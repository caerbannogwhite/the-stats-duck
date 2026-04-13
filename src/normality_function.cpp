#include "normality_function.hpp"
#include "distributions.hpp"

#include "duckdb/function/aggregate_function.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

#include <cmath>

namespace duckdb {

// Jarque-Bera normality test.
//
// JB = (n / 6) * (S^2 + (K_excess)^2 / 4)
// where S is sample skewness and K_excess is sample excess kurtosis.
//
// Under H0 (normality), JB is asymptotically chi-square(2). The approximation
// is known to be poor for small n (say n < 30), where the test tends to be
// conservative. We report the statistic regardless so Brian's UI can display
// it alongside a small-sample warning when appropriate.
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

void RegisterJarqueBera(ExtensionLoader &loader) {
	AggregateFunction fn("jarque_bera", {LogicalType::DOUBLE}, JarqueBeraResultType(),
	                     AggregateFunction::StateSize<JBState>, JBInit, JBUpdate, JBCombine, JBFinalize,
	                     FunctionNullHandling::DEFAULT_NULL_HANDLING);
	loader.RegisterFunction(fn);
}

} // namespace duckdb
