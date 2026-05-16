#include "normality_function.hpp"
#include "distributions.hpp"

#include "duckdb/function/aggregate_function.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

#include <algorithm>
#include <cmath>
#include <limits>
#include <vector>

namespace duckdb {

// Jarque-Bera normality test.
//
// JB = (n / 6) * (S^2 + (K_excess)^2 / 4)
// where S is sample skewness and K_excess is sample excess kurtosis.
//
// Under H0 (normality), JB is asymptotically chi-square(2). The approximation
// is known to be poor for small n (say n < 30), where the test tends to be
// conservative. We report the statistic regardless so Bedevere Wise's UI can
// display it alongside a small-sample warning when appropriate.
//
// Moments are accumulated with the streaming 4-moment Welford algorithm, so
// the test runs in a single pass, supports GROUP BY, and parallelizes via a
// parallel Combine step (Pébay 2008 / Wikipedia).

namespace {

// ── Result STRUCT ──────────────────────────────────────────────────────────

static LogicalType JarqueBeraResultType() {
	child_list_t<LogicalType> children;
	children.emplace_back("test_type", LogicalType::VARCHAR);
	children.emplace_back("jb_statistic", LogicalType::DOUBLE);
	children.emplace_back("skewness", LogicalType::DOUBLE);
	children.emplace_back("excess_kurtosis", LogicalType::DOUBLE);
	children.emplace_back("df", LogicalType::DOUBLE);
	children.emplace_back("p_value", LogicalType::DOUBLE);
	children.emplace_back("n", LogicalType::BIGINT);
	return LogicalType::STRUCT(std::move(children));
}

// ── 4-moment Welford state ─────────────────────────────────────────────────

struct JBState {
	int64_t n;
	double mean;
	double m2; // sum((x - mean)^2)
	double m3; // sum((x - mean)^3)
	double m4; // sum((x - mean)^4)
};

static void JBInit(const AggregateFunction &, data_ptr_t state_p) {
	auto &s = *reinterpret_cast<JBState *>(state_p);
	s.n = 0;
	s.mean = 0.0;
	s.m2 = 0.0;
	s.m3 = 0.0;
	s.m4 = 0.0;
}

//! Single-element update: online 4-moment Welford (Terriberry 2007).
static inline void JBUpdateOne(JBState &s, double x) {
	int64_t n1 = s.n;
	s.n++;
	int64_t n = s.n;
	double dn = static_cast<double>(n);
	double delta = x - s.mean;
	double delta_n = delta / dn;
	double delta_n2 = delta_n * delta_n;
	double term1 = delta * delta_n * static_cast<double>(n1);
	s.mean += delta_n;
	s.m4 += term1 * delta_n2 * (dn * dn - 3.0 * dn + 3.0) + 6.0 * delta_n2 * s.m2 - 4.0 * delta_n * s.m3;
	s.m3 += term1 * delta_n * (dn - 2.0) - 3.0 * delta_n * s.m2;
	s.m2 += term1;
}

static void JBUpdate(Vector inputs[], AggregateInputData &, idx_t, Vector &state_vector, idx_t count) {
	UnifiedVectorFormat idata;
	inputs[0].ToUnifiedFormat(count, idata);

	auto states = FlatVector::GetData<JBState *>(state_vector);
	auto values = UnifiedVectorFormat::GetData<double>(idata);

	for (idx_t i = 0; i < count; i++) {
		auto idx = idata.sel->get_index(i);
		if (!idata.validity.RowIsValid(idx)) {
			continue;
		}
		double v = values[idx];
		if (std::isnan(v)) {
			continue;
		}
		JBUpdateOne(*states[i], v);
	}
}

//! Parallel combine of two 4-moment Welford states (Pébay 2008).
static void JBCombine(Vector &source, Vector &target, AggregateInputData &, idx_t count) {
	auto src = FlatVector::GetData<JBState *>(source);
	auto tgt = FlatVector::GetData<JBState *>(target);
	for (idx_t i = 0; i < count; i++) {
		auto &a = *tgt[i];
		auto &b = *src[i];
		if (b.n == 0) {
			continue;
		}
		if (a.n == 0) {
			a = b;
			continue;
		}
		int64_t n = a.n + b.n;
		double dn = static_cast<double>(n);
		double na = static_cast<double>(a.n);
		double nb = static_cast<double>(b.n);
		double delta = b.mean - a.mean;
		double delta2 = delta * delta;
		double delta3 = delta * delta2;
		double delta4 = delta2 * delta2;

		double mean = a.mean + delta * nb / dn;
		double m2 = a.m2 + b.m2 + delta2 * na * nb / dn;
		double m3 = a.m3 + b.m3 + delta3 * na * nb * (na - nb) / (dn * dn) +
		            3.0 * delta * (na * b.m2 - nb * a.m2) / dn;
		double m4 = a.m4 + b.m4 +
		            delta4 * na * nb * (na * na - na * nb + nb * nb) / (dn * dn * dn) +
		            6.0 * delta2 * (na * na * b.m2 + nb * nb * a.m2) / (dn * dn) +
		            4.0 * delta * (na * b.m3 - nb * a.m3) / dn;

		a.n = n;
		a.mean = mean;
		a.m2 = m2;
		a.m3 = m3;
		a.m4 = m4;
	}
}

static void JBFinalize(Vector &state_vector, AggregateInputData &, Vector &result, idx_t count, idx_t offset) {
	auto states = FlatVector::GetData<JBState *>(state_vector);
	auto &children = StructVector::GetEntries(result);

	for (idx_t i = 0; i < count; i++) {
		auto &state = *states[i];
		auto idx = i + offset;

		// Need at least 4 non-null observations for meaningful skewness/kurtosis.
		if (state.n < 4 || state.m2 <= 0.0) {
			FlatVector::SetNull(result, idx, true);
			continue;
		}

		double dn = static_cast<double>(state.n);
		// Central moments g1 (population skewness) and g2 (population kurtosis).
		double m2_over_n = state.m2 / dn;
		double m3_over_n = state.m3 / dn;
		double m4_over_n = state.m4 / dn;
		double g1 = m3_over_n / std::pow(m2_over_n, 1.5);
		double g2 = m4_over_n / (m2_over_n * m2_over_n) - 3.0;

		// Jarque-Bera uses the population-moment form (not the bias-corrected one)
		// so that the statistic is directly comparable across implementations
		// (R's tseries::jarque.bera.test uses the same convention).
		double jb = (dn / 6.0) * (g1 * g1 + 0.25 * g2 * g2);
		double p_value = 1.0 - stats_duck::ChiSquareCDF(jb, 2.0);

		FlatVector::GetData<string_t>(*children[0])[idx] =
		    StringVector::AddString(*children[0], "Jarque-Bera");
		FlatVector::GetData<double>(*children[1])[idx] = jb;
		FlatVector::GetData<double>(*children[2])[idx] = g1;
		FlatVector::GetData<double>(*children[3])[idx] = g2;
		FlatVector::GetData<double>(*children[4])[idx] = 2.0;
		FlatVector::GetData<double>(*children[5])[idx] = p_value;
		FlatVector::GetData<int64_t>(*children[6])[idx] = state.n;
	}
}

} // namespace

// =============================================================================
// Anderson-Darling
// =============================================================================
//
// EDF goodness-of-fit test against the fitted normal distribution (case 3:
// both mean and variance estimated from the sample, which is what's wanted
// for a "did this data come from a normal distribution?" question).
//
//     A² = -n - (1/n) * Σ_{i=1..n} (2i-1) * [ln Φ(z_(i)) + ln(1 - Φ(z_(n+1-i)))]
//
// where z_(i) are the sorted standardised values and Φ is the standard
// normal CDF. The size-adjusted statistic
//
//     A²* = A² * (1 + 0.75/n + 2.25/n²)
//
// is the input to Stephens (1986)'s p-value table, which is what every
// downstream implementation (R nortest::ad.test, scipy.stats.anderson) uses.
//
// Requires sorting and so cannot be a streaming aggregate: state buffers
// values; the sort + scan happen in Finalize.

namespace {

static LogicalType ADResultType() {
	child_list_t<LogicalType> children;
	children.emplace_back("test_type", LogicalType::VARCHAR);
	children.emplace_back("a_squared", LogicalType::DOUBLE);
	children.emplace_back("a_squared_adjusted", LogicalType::DOUBLE);
	children.emplace_back("p_value", LogicalType::DOUBLE);
	children.emplace_back("n", LogicalType::BIGINT);
	return LogicalType::STRUCT(std::move(children));
}

struct ADState {
	std::vector<double> *values;
};

static void ADInit(const AggregateFunction &, data_ptr_t state_p) {
	auto &state = *reinterpret_cast<ADState *>(state_p);
	state.values = nullptr;
}

static void ADUpdate(Vector inputs[], AggregateInputData &, idx_t, Vector &state_vector, idx_t count) {
	UnifiedVectorFormat idata;
	inputs[0].ToUnifiedFormat(count, idata);

	auto states = FlatVector::GetData<ADState *>(state_vector);
	auto values = UnifiedVectorFormat::GetData<double>(idata);

	for (idx_t i = 0; i < count; i++) {
		auto idx = idata.sel->get_index(i);
		if (!idata.validity.RowIsValid(idx)) {
			continue;
		}
		double v = values[idx];
		if (std::isnan(v)) {
			continue;
		}
		auto &state = *states[i];
		if (!state.values) {
			state.values = new std::vector<double>();
		}
		state.values->push_back(v);
	}
}

static void ADCombine(Vector &source, Vector &target, AggregateInputData &, idx_t count) {
	auto src = FlatVector::GetData<ADState *>(source);
	auto tgt = FlatVector::GetData<ADState *>(target);
	for (idx_t i = 0; i < count; i++) {
		auto &s = *src[i];
		auto &t = *tgt[i];
		if (!s.values || s.values->empty()) {
			continue;
		}
		if (!t.values) {
			t.values = new std::vector<double>();
		}
		t.values->insert(t.values->end(), s.values->begin(), s.values->end());
	}
}

static void ADDestroy(Vector &state_vector, AggregateInputData &, idx_t count) {
	auto states = FlatVector::GetData<ADState *>(state_vector);
	for (idx_t i = 0; i < count; i++) {
		delete states[i]->values;
	}
}

//! Stephens (1986) four-segment polynomial p-value approximation for the
//! Anderson-Darling normality test with mean and variance both estimated
//! from the sample (case 3). Valid for n >= 8.
static double ADPValue(double a2_adj) {
	if (a2_adj < 0.20) {
		return 1.0 - std::exp(-13.436 + 101.14 * a2_adj - 223.73 * a2_adj * a2_adj);
	}
	if (a2_adj < 0.34) {
		return 1.0 - std::exp(-8.318 + 42.796 * a2_adj - 59.938 * a2_adj * a2_adj);
	}
	if (a2_adj < 0.60) {
		return std::exp(0.9177 - 4.279 * a2_adj - 1.38 * a2_adj * a2_adj);
	}
	return std::exp(1.2937 - 5.709 * a2_adj + 0.0186 * a2_adj * a2_adj);
}

static void ADFinalize(Vector &state_vector, AggregateInputData &, Vector &result, idx_t count, idx_t offset) {
	auto states = FlatVector::GetData<ADState *>(state_vector);
	auto &children = StructVector::GetEntries(result);

	for (idx_t i = 0; i < count; i++) {
		auto &state = *states[i];
		auto idx = i + offset;

		// Stephens' p-value approximation is calibrated for n >= 8; R's
		// nortest::ad.test enforces the same minimum.
		if (!state.values || state.values->size() < 8) {
			FlatVector::SetNull(result, idx, true);
			continue;
		}

		auto &values = *state.values;
		idx_t n = values.size();
		double n_d = static_cast<double>(n);

		double mean = 0.0;
		for (double v : values) {
			mean += v;
		}
		mean /= n_d;

		double m2 = 0.0;
		for (double v : values) {
			double d = v - mean;
			m2 += d * d;
		}
		double sd = std::sqrt(m2 / (n_d - 1.0));
		if (sd <= 0.0) {
			// All-identical input: standardisation undefined.
			FlatVector::SetNull(result, idx, true);
			continue;
		}

		std::sort(values.begin(), values.end());
		// Numerical safety: clamp Φ away from 0 and 1 so log doesn't blow up
		// for extreme z values (relevant when n is large enough to have
		// |z| > 8 or so).
		const double tiny = std::numeric_limits<double>::min();
		double a2_sum = 0.0;
		for (idx_t j = 0; j < n; j++) {
			double z_lo = (values[j] - mean) / sd;
			double z_hi = (values[n - 1 - j] - mean) / sd;
			double cdf_lo = stats_duck::NormalCDF(z_lo);
			double cdf_hi = stats_duck::NormalCDF(z_hi);
			if (cdf_lo < tiny) {
				cdf_lo = tiny;
			}
			if (cdf_hi > 1.0 - tiny) {
				cdf_hi = 1.0 - tiny;
			}
			double weight = 2.0 * static_cast<double>(j + 1) - 1.0;
			a2_sum += weight * (std::log(cdf_lo) + std::log(1.0 - cdf_hi));
		}
		double a2 = -n_d - a2_sum / n_d;
		double a2_adj = a2 * (1.0 + 0.75 / n_d + 2.25 / (n_d * n_d));
		double p_value = ADPValue(a2_adj);
		// p_value from the Stephens formula can exceed 1 or go negative right
		// at the boundaries; clamp to [0, 1] for sanity.
		if (p_value < 0.0) {
			p_value = 0.0;
		} else if (p_value > 1.0) {
			p_value = 1.0;
		}

		FlatVector::GetData<string_t>(*children[0])[idx] =
		    StringVector::AddString(*children[0], "Anderson-Darling");
		FlatVector::GetData<double>(*children[1])[idx] = a2;
		FlatVector::GetData<double>(*children[2])[idx] = a2_adj;
		FlatVector::GetData<double>(*children[3])[idx] = p_value;
		FlatVector::GetData<int64_t>(*children[4])[idx] = static_cast<int64_t>(n);
	}
}

} // namespace

// =============================================================================
// Shapiro-Wilk (Royston 1995, AS R94)
// =============================================================================
//
// W = (Σ a_i x_(i))² / Σ (x_i - x̄)²
//
// where a_i are coefficients antisymmetric around the middle (a_i = -a_{n+1-i})
// derived from the normal-quantile plotting positions m_i = Φ^-1((i - 3/8) /
// (n + 1/4)) plus a polynomial correction for the top two coefficients. The
// polynomial replaces the Shapiro-Wilk 1965 coefficient tables and extends
// the valid range to n in [3, 5000]. AS R94 is what scipy.stats.shapiro and
// R's shapiro.test ship.
//
// Reference: Royston, P. (1995). "Algorithm AS R94: A Remark on Algorithm
// AS 181: The W-test for Normality". J. R. Statist. Soc. C 44 (4): 547-551.

namespace {

// Polynomial coefficients. Constants verified against scipy/stats/
// _ansari_swilk_statistics.pyx (algorithm only; no source code copied).
//
//   C1, C2 — corrections to a_n and a_{n-1}; argument is 1/sqrt(n).
//   C3, C4 — small-n (4..11) p-value transform; argument is n.
//   C5, C6 — large-n (12..5000) p-value transform; argument is log(n).
//   G      — small-n γ for the log-of-log transform; argument is n.
static const double C1[6] = {0.0, 0.221157, -0.147981, -2.07119, 4.434685, -2.706056};
static const double C2[6] = {0.0, 0.042981, -0.293762, -1.752461, 5.682633, -3.582633};
static const double C3[4] = {0.5440, -0.39978, 0.025054, -0.0006714};
static const double C4[4] = {1.3822, -0.77857, 0.062767, -0.0020322};
static const double C5[4] = {-1.5861, -0.31082, -0.083751, 0.0038915};
static const double C6[3] = {-0.4803, -0.082676, 0.0030302};
static const double G[2] = {-2.273, 0.459};

//! Horner-rule polynomial evaluator: coef[0] + coef[1]*x + ... + coef[n-1]*x^(n-1).
static double Polyval(const double *coef, int len, double x) {
	double r = coef[len - 1];
	for (int i = len - 2; i >= 0; i--) {
		r = r * x + coef[i];
	}
	return r;
}

static LogicalType SWResultType() {
	child_list_t<LogicalType> children;
	children.emplace_back("test_type", LogicalType::VARCHAR);
	children.emplace_back("w_statistic", LogicalType::DOUBLE);
	children.emplace_back("p_value", LogicalType::DOUBLE);
	children.emplace_back("n", LogicalType::BIGINT);
	return LogicalType::STRUCT(std::move(children));
}

struct SWState {
	std::vector<double> *values;
};

static void SWInit(const AggregateFunction &, data_ptr_t state_p) {
	auto &state = *reinterpret_cast<SWState *>(state_p);
	state.values = nullptr;
}

static void SWUpdate(Vector inputs[], AggregateInputData &, idx_t, Vector &state_vector, idx_t count) {
	UnifiedVectorFormat idata;
	inputs[0].ToUnifiedFormat(count, idata);
	auto states = FlatVector::GetData<SWState *>(state_vector);
	auto values = UnifiedVectorFormat::GetData<double>(idata);
	for (idx_t i = 0; i < count; i++) {
		auto idx = idata.sel->get_index(i);
		if (!idata.validity.RowIsValid(idx)) {
			continue;
		}
		double v = values[idx];
		if (std::isnan(v)) {
			continue;
		}
		auto &state = *states[i];
		if (!state.values) {
			state.values = new std::vector<double>();
		}
		state.values->push_back(v);
	}
}

static void SWCombine(Vector &source, Vector &target, AggregateInputData &, idx_t count) {
	auto src = FlatVector::GetData<SWState *>(source);
	auto tgt = FlatVector::GetData<SWState *>(target);
	for (idx_t i = 0; i < count; i++) {
		auto &s = *src[i];
		auto &t = *tgt[i];
		if (!s.values || s.values->empty()) {
			continue;
		}
		if (!t.values) {
			t.values = new std::vector<double>();
		}
		t.values->insert(t.values->end(), s.values->begin(), s.values->end());
	}
}

static void SWDestroy(Vector &state_vector, AggregateInputData &, idx_t count) {
	auto states = FlatVector::GetData<SWState *>(state_vector);
	for (idx_t i = 0; i < count; i++) {
		delete states[i]->values;
	}
}

static void SWFinalize(Vector &state_vector, AggregateInputData &, Vector &result, idx_t count, idx_t offset) {
	auto states = FlatVector::GetData<SWState *>(state_vector);
	auto &children = StructVector::GetEntries(result);

	constexpr double SMALL_P = 1.0e-19;

	for (idx_t i = 0; i < count; i++) {
		auto &state = *states[i];
		auto idx = i + offset;
		if (!state.values || state.values->size() < 3 || state.values->size() > 5000) {
			FlatVector::SetNull(result, idx, true);
			continue;
		}

		auto &values = *state.values;
		idx_t n = values.size();
		double n_d = static_cast<double>(n);
		std::sort(values.begin(), values.end());

		// All-equal input: degenerate sample, W = 1 / p = 1 (vacuously normal).
		if (values.front() == values.back()) {
			FlatVector::GetData<string_t>(*children[0])[idx] =
			    StringVector::AddString(*children[0], "Shapiro-Wilk");
			FlatVector::GetData<double>(*children[1])[idx] = 1.0;
			FlatVector::GetData<double>(*children[2])[idx] = 1.0;
			FlatVector::GetData<int64_t>(*children[3])[idx] = static_cast<int64_t>(n);
			continue;
		}

		double mean = 0.0;
		for (double v : values) {
			mean += v;
		}
		mean /= n_d;
		double sx2 = 0.0;
		for (double v : values) {
			double d = v - mean;
			sx2 += d * d;
		}

		double w;
		if (n == 3) {
			// Exact n=3 case: a = (-1/√2, 0, +1/√2), so (Σa·x)² = ½ · (x₃ − x₁)².
			// W is theoretically bounded in [0.75, 1].
			double range = values[n - 1] - values[0];
			w = 0.5 * range * range / sx2;
			if (w < 0.75) {
				w = 0.75;
			}
		} else {
			std::vector<double> m(n);
			for (idx_t k = 0; k < n; k++) {
				double frac = (static_cast<double>(k + 1) - 0.375) / (n_d + 0.25);
				m[k] = stats_duck::NormalQuantile(frac);
			}
			double m_sum_sq = 0.0;
			for (double mv : m) {
				m_sum_sq += mv * mv;
			}

			double rsn = 1.0 / std::sqrt(n_d);
			double a_n = Polyval(C1, 6, rsn) + m[n - 1] / std::sqrt(m_sum_sq);
			std::vector<double> a(n);
			a[n - 1] = a_n;

			if (n >= 6) {
				double a_nm1 = Polyval(C2, 6, rsn) + m[n - 2] / std::sqrt(m_sum_sq);
				a[n - 2] = a_nm1;
				double denom = 1.0 - 2.0 * a_n * a_n - 2.0 * a_nm1 * a_nm1;
				double eps2 = (m_sum_sq - 2.0 * m[n - 1] * m[n - 1] - 2.0 * m[n - 2] * m[n - 2]) / denom;
				double se = std::sqrt(eps2);
				for (idx_t k = 2; k + 2 < n; k++) {
					a[k] = m[k] / se;
				}
			} else {
				// n in {4, 5}: only a_n gets the polynomial correction.
				double eps2 = (m_sum_sq - 2.0 * m[n - 1] * m[n - 1]) / (1.0 - 2.0 * a_n * a_n);
				double se = std::sqrt(eps2);
				for (idx_t k = 1; k + 1 < n; k++) {
					a[k] = m[k] / se;
				}
			}
			for (idx_t k = 0; k < n / 2; k++) {
				a[k] = -a[n - 1 - k];
			}

			double linear = 0.0;
			for (idx_t k = 0; k < n; k++) {
				linear += a[k] * values[k];
			}
			w = linear * linear / sx2;
		}

		if (w > 1.0) {
			w = 1.0;
		} else if (w < 0.0) {
			w = 0.0;
		}

		double pw;
		if (n == 3) {
			// AS R94 exact: pw = 1 - (6/π) · acos(√W).
			constexpr double SIX_OVER_PI = 6.0 / 3.14159265358979323846;
			double pw_raw = 1.0 - SIX_OVER_PI * std::acos(std::sqrt(w));
			pw = pw_raw < 0.0 ? 0.0 : (pw_raw > 1.0 ? 1.0 : pw_raw);
		} else {
			double y = std::log(1.0 - w);
			bool saturated = false;
			double mu = 0.0, sigma = 1.0;
			if (n <= 11) {
				double gamma_n = Polyval(G, 2, n_d);
				if (y >= gamma_n) {
					// Small-n y-transform saturates: report a floor instead of NaN.
					pw = SMALL_P;
					saturated = true;
				} else {
					y = -std::log(gamma_n - y);
					mu = Polyval(C3, 4, n_d);
					sigma = std::exp(Polyval(C4, 4, n_d));
				}
			} else {
				double log_n = std::log(n_d);
				mu = Polyval(C5, 4, log_n);
				sigma = std::exp(Polyval(C6, 3, log_n));
			}
			if (!saturated) {
				double z = (y - mu) / sigma;
				pw = 1.0 - stats_duck::NormalCDF(z);
				if (pw < 0.0) {
					pw = 0.0;
				}
				if (pw > 1.0) {
					pw = 1.0;
				}
			}
		}

		FlatVector::GetData<string_t>(*children[0])[idx] =
		    StringVector::AddString(*children[0], "Shapiro-Wilk");
		FlatVector::GetData<double>(*children[1])[idx] = w;
		FlatVector::GetData<double>(*children[2])[idx] = pw;
		FlatVector::GetData<int64_t>(*children[3])[idx] = static_cast<int64_t>(n);
	}
}

} // namespace

void RegisterJarqueBera(ExtensionLoader &loader) {
	AggregateFunction fn("jarque_bera", {LogicalType::DOUBLE}, JarqueBeraResultType(),
	                     AggregateFunction::StateSize<JBState>, JBInit, JBUpdate, JBCombine, JBFinalize,
	                     FunctionNullHandling::DEFAULT_NULL_HANDLING);
	loader.RegisterFunction(fn);
}

void RegisterShapiroWilk(ExtensionLoader &loader) {
	AggregateFunction fn("shapiro_wilk", {LogicalType::DOUBLE}, SWResultType(),
	                     AggregateFunction::StateSize<SWState>, SWInit, SWUpdate, SWCombine, SWFinalize,
	                     FunctionNullHandling::DEFAULT_NULL_HANDLING, nullptr, nullptr, SWDestroy);
	loader.RegisterFunction(fn);
}

void RegisterAndersonDarling(ExtensionLoader &loader) {
	AggregateFunction fn("anderson_darling", {LogicalType::DOUBLE}, ADResultType(),
	                     AggregateFunction::StateSize<ADState>, ADInit, ADUpdate, ADCombine, ADFinalize,
	                     FunctionNullHandling::DEFAULT_NULL_HANDLING, nullptr, nullptr, ADDestroy);
	loader.RegisterFunction(fn);
}

} // namespace duckdb
