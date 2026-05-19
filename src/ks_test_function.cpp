#include "ks_test_function.hpp"

#include "distributions.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/function/aggregate_function.hpp"
#include "duckdb/function/function_set.hpp"

#include <algorithm>
#include <cmath>
#include <vector>

namespace duckdb {

namespace {

//===--------------------------------------------------------------------===//
// Shared: asymptotic Kolmogorov distribution p-value
//===--------------------------------------------------------------------===//

// Kolmogorov distribution survival function:
//   Q(λ) = 2 · Σ_{k≥1} (-1)^(k-1) · exp(-2 · k² · λ²)
// Returns the two-sided KS p-value. Converges rapidly; we cap at 100 terms
// and bail when an additional term contributes less than 1e-12 of the running
// sum (terms alternate signs and decay super-exponentially).
double KolmogorovQ(double lambda) {
	if (lambda <= 0.0) {
		return 1.0;
	}
	double sum = 0.0;
	double last_term = 0.0;
	int sign = 1;
	for (int k = 1; k < 101; k++) {
		double term = std::exp(-2.0 * k * k * lambda * lambda);
		sum += sign * term;
		if (k > 1 && std::fabs(term) < 1e-12 * std::fabs(sum) && term < last_term) {
			break;
		}
		last_term = term;
		sign = -sign;
	}
	double p = 2.0 * sum;
	if (p < 0.0) {
		p = 0.0;
	} else if (p > 1.0) {
		p = 1.0;
	}
	return p;
}

// Stephens (1970) small-sample correction to the asymptotic λ. Same form used
// by Numerical Recipes' kstest / kstwo: λ_eff = (√en + 0.12 + 0.11/√en) · D.
// `en` is the effective sample size (n for 1-sample, n1*n2/(n1+n2) for 2-sample).
double KsPValue(double d_statistic, double en) {
	double sqrt_en = std::sqrt(en);
	double lambda = (sqrt_en + 0.12 + 0.11 / sqrt_en) * d_statistic;
	return KolmogorovQ(lambda);
}

//===--------------------------------------------------------------------===//
// Result struct schemas
//===--------------------------------------------------------------------===//

LogicalType KsTest1SampResultType() {
	child_list_t<LogicalType> children;
	children.push_back({"test_type", LogicalType::VARCHAR});
	children.push_back({"d_statistic", LogicalType::DOUBLE});
	children.push_back({"p_value", LogicalType::DOUBLE});
	children.push_back({"n", LogicalType::BIGINT});
	return LogicalType::STRUCT(std::move(children));
}

LogicalType KsTest2SampResultType() {
	child_list_t<LogicalType> children;
	children.push_back({"test_type", LogicalType::VARCHAR});
	children.push_back({"d_statistic", LogicalType::DOUBLE});
	children.push_back({"p_value", LogicalType::DOUBLE});
	children.push_back({"n_x", LogicalType::BIGINT});
	children.push_back({"n_y", LogicalType::BIGINT});
	return LogicalType::STRUCT(std::move(children));
}

//===--------------------------------------------------------------------===//
// One-sample KS — vs fitted N(mean, sd)
//===--------------------------------------------------------------------===//

struct Ks1SampState {
	std::vector<double> *values;
};

static void Ks1Init(const AggregateFunction &, data_ptr_t state_p) {
	auto &state = *reinterpret_cast<Ks1SampState *>(state_p);
	state.values = nullptr;
}

static void Ks1Update(Vector inputs[], AggregateInputData &, idx_t, Vector &state_vector, idx_t count) {
	UnifiedVectorFormat idata;
	inputs[0].ToUnifiedFormat(count, idata);
	auto states = FlatVector::GetData<Ks1SampState *>(state_vector);
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

static void Ks1Combine(Vector &source, Vector &target, AggregateInputData &, idx_t count) {
	auto src = FlatVector::GetData<Ks1SampState *>(source);
	auto tgt = FlatVector::GetData<Ks1SampState *>(target);
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

static void Ks1Destroy(Vector &state_vector, AggregateInputData &, idx_t count) {
	auto states = FlatVector::GetData<Ks1SampState *>(state_vector);
	for (idx_t i = 0; i < count; i++) {
		delete states[i]->values;
	}
}

static void Ks1Finalize(Vector &state_vector, AggregateInputData &, Vector &result, idx_t count, idx_t offset) {
	auto states = FlatVector::GetData<Ks1SampState *>(state_vector);
	auto &children = StructVector::GetEntries(result);

	for (idx_t i = 0; i < count; i++) {
		auto &state = *states[i];
		auto idx = i + offset;
		// Need at least 3 points to estimate mean and sd meaningfully and
		// have anything sensible to compare.
		if (!state.values || state.values->size() < 3) {
			FlatVector::SetNull(result, idx, true);
			continue;
		}

		auto &values = *state.values;
		idx_t n = values.size();
		double n_d = static_cast<double>(n);
		std::sort(values.begin(), values.end());

		double mean = 0.0;
		for (double v : values) {
			mean += v;
		}
		mean /= n_d;
		double sx2 = 0.0;
		for (double v : values) {
			double diff = v - mean;
			sx2 += diff * diff;
		}
		// Sample standard deviation (n-1 denominator); zero variance ⇒
		// degenerate sample, can't compare to a normal.
		if (sx2 <= 0.0) {
			FlatVector::SetNull(result, idx, true);
			continue;
		}
		double sd = std::sqrt(sx2 / (n_d - 1.0));

		// KS statistic: at each sorted x_(i+1), the ECDF jumps from i/n to
		// (i+1)/n. The KS sup-norm is the larger of the "before-jump" and
		// "after-jump" gaps from the reference CDF.
		double d = 0.0;
		for (idx_t k = 0; k < n; k++) {
			double f = stats_duck::NormalCDF(values[k], mean, sd);
			double f_n_lower = static_cast<double>(k) / n_d;
			double f_n_upper = static_cast<double>(k + 1) / n_d;
			double gap_lo = std::fabs(f - f_n_lower);
			double gap_hi = std::fabs(f_n_upper - f);
			if (gap_lo > d) {
				d = gap_lo;
			}
			if (gap_hi > d) {
				d = gap_hi;
			}
		}

		double p = KsPValue(d, n_d);

		FlatVector::GetData<string_t>(*children[0])[idx] =
		    StringVector::AddString(*children[0], "Kolmogorov-Smirnov (one-sample, fitted normal)");
		FlatVector::GetData<double>(*children[1])[idx] = d;
		FlatVector::GetData<double>(*children[2])[idx] = p;
		FlatVector::GetData<int64_t>(*children[3])[idx] = static_cast<int64_t>(n);
	}
}

//===--------------------------------------------------------------------===//
// Two-sample KS — compare two ECDFs
//===--------------------------------------------------------------------===//

struct Ks2SampState {
	std::vector<double> *x;
	std::vector<double> *y;
};

static void Ks2Init(const AggregateFunction &, data_ptr_t state_p) {
	auto &state = *reinterpret_cast<Ks2SampState *>(state_p);
	state.x = nullptr;
	state.y = nullptr;
}

static void Ks2Update(Vector inputs[], AggregateInputData &, idx_t, Vector &state_vector, idx_t count) {
	UnifiedVectorFormat x_data, y_data;
	inputs[0].ToUnifiedFormat(count, x_data);
	inputs[1].ToUnifiedFormat(count, y_data);
	auto states = FlatVector::GetData<Ks2SampState *>(state_vector);
	auto xs = UnifiedVectorFormat::GetData<double>(x_data);
	auto ys = UnifiedVectorFormat::GetData<double>(y_data);
	for (idx_t i = 0; i < count; i++) {
		auto &state = *states[i];
		auto xi = x_data.sel->get_index(i);
		auto yi = y_data.sel->get_index(i);
		// Each column contributes independently — NULL in x doesn't drop the
		// row from y. Matches ttest_2samp's per-column NULL handling.
		if (x_data.validity.RowIsValid(xi)) {
			double v = xs[xi];
			if (!std::isnan(v)) {
				if (!state.x) {
					state.x = new std::vector<double>();
				}
				state.x->push_back(v);
			}
		}
		if (y_data.validity.RowIsValid(yi)) {
			double v = ys[yi];
			if (!std::isnan(v)) {
				if (!state.y) {
					state.y = new std::vector<double>();
				}
				state.y->push_back(v);
			}
		}
	}
}

static void Ks2Combine(Vector &source, Vector &target, AggregateInputData &, idx_t count) {
	auto src = FlatVector::GetData<Ks2SampState *>(source);
	auto tgt = FlatVector::GetData<Ks2SampState *>(target);
	for (idx_t i = 0; i < count; i++) {
		auto &s = *src[i];
		auto &t = *tgt[i];
		if (s.x && !s.x->empty()) {
			if (!t.x) {
				t.x = new std::vector<double>();
			}
			t.x->insert(t.x->end(), s.x->begin(), s.x->end());
		}
		if (s.y && !s.y->empty()) {
			if (!t.y) {
				t.y = new std::vector<double>();
			}
			t.y->insert(t.y->end(), s.y->begin(), s.y->end());
		}
	}
}

static void Ks2Destroy(Vector &state_vector, AggregateInputData &, idx_t count) {
	auto states = FlatVector::GetData<Ks2SampState *>(state_vector);
	for (idx_t i = 0; i < count; i++) {
		delete states[i]->x;
		delete states[i]->y;
	}
}

static void Ks2Finalize(Vector &state_vector, AggregateInputData &, Vector &result, idx_t count, idx_t offset) {
	auto states = FlatVector::GetData<Ks2SampState *>(state_vector);
	auto &children = StructVector::GetEntries(result);

	for (idx_t i = 0; i < count; i++) {
		auto &state = *states[i];
		auto idx = i + offset;
		idx_t nx = state.x ? state.x->size() : 0;
		idx_t ny = state.y ? state.y->size() : 0;
		// Need at least one observation in each sample.
		if (nx == 0 || ny == 0) {
			FlatVector::SetNull(result, idx, true);
			continue;
		}

		auto &xs = *state.x;
		auto &ys = *state.y;
		std::sort(xs.begin(), xs.end());
		std::sort(ys.begin(), ys.end());

		// Standard two-pointer scan over the merged order. At each step we
		// advance whichever pointer points to the smaller value (ties advance
		// both, with one ECDF cycling through the tie group before D is
		// re-evaluated).
		double d = 0.0;
		idx_t i_x = 0, i_y = 0;
		double f_x = 0.0, f_y = 0.0;
		double inv_nx = 1.0 / static_cast<double>(nx);
		double inv_ny = 1.0 / static_cast<double>(ny);
		while (i_x < nx && i_y < ny) {
			double v_x = xs[i_x];
			double v_y = ys[i_y];
			if (v_x <= v_y) {
				do {
					i_x++;
					f_x += inv_nx;
				} while (i_x < nx && xs[i_x] == v_x);
			}
			if (v_y <= v_x) {
				do {
					i_y++;
					f_y += inv_ny;
				} while (i_y < ny && ys[i_y] == v_y);
			}
			double gap = std::fabs(f_x - f_y);
			if (gap > d) {
				d = gap;
			}
		}
		// Tail: one ECDF is at 1, the other still climbing — the maximum gap
		// can only have grown earlier, but recheck to be safe against
		// floating-point drift.
		// (The loop above also covers this: once one pointer hits the end,
		// we exit, and the other ECDF being below 1 means the prior gap is
		// already accounted for.)

		double nx_d = static_cast<double>(nx);
		double ny_d = static_cast<double>(ny);
		double en = nx_d * ny_d / (nx_d + ny_d);
		double p = KsPValue(d, en);

		FlatVector::GetData<string_t>(*children[0])[idx] =
		    StringVector::AddString(*children[0], "Kolmogorov-Smirnov (two-sample)");
		FlatVector::GetData<double>(*children[1])[idx] = d;
		FlatVector::GetData<double>(*children[2])[idx] = p;
		FlatVector::GetData<int64_t>(*children[3])[idx] = static_cast<int64_t>(nx);
		FlatVector::GetData<int64_t>(*children[4])[idx] = static_cast<int64_t>(ny);
	}
}

} // namespace

void RegisterKsTest1Samp(ExtensionLoader &loader) {
	AggregateFunction fn("ks_test_1samp", {LogicalType::DOUBLE}, KsTest1SampResultType(),
	                     AggregateFunction::StateSize<Ks1SampState>, Ks1Init, Ks1Update, Ks1Combine,
	                     Ks1Finalize, FunctionNullHandling::DEFAULT_NULL_HANDLING, nullptr, nullptr,
	                     Ks1Destroy);
	loader.RegisterFunction(fn);
}

void RegisterKsTest2Samp(ExtensionLoader &loader) {
	AggregateFunction fn("ks_test_2samp", {LogicalType::DOUBLE, LogicalType::DOUBLE},
	                     KsTest2SampResultType(), AggregateFunction::StateSize<Ks2SampState>, Ks2Init,
	                     Ks2Update, Ks2Combine, Ks2Finalize, FunctionNullHandling::DEFAULT_NULL_HANDLING,
	                     nullptr, nullptr, Ks2Destroy);
	loader.RegisterFunction(fn);
}

} // namespace duckdb
