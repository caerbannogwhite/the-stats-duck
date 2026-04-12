#include "summary_stats_function.hpp"

#include "duckdb/function/aggregate_function.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/main/extension_util.hpp"

#include <algorithm>
#include <cmath>
#include <vector>

namespace duckdb {

// summary_stats(column) — exact descriptive statistics.
//
// Approach: we buffer the non-null values per aggregate state in a
// heap-allocated std::vector, then compute all statistics (moments + order
// stats) in Finalize. This gives exact quantiles without a sketch data
// structure, at the cost of memory proportional to the number of rows per
// group. For typical descriptive-stats workflows (a few million rows, a
// handful of groups) this is fine; billion-row global aggregations would
// want a t-digest, which is a future optimization.
//
// Quantiles use the R type 7 / Excel INC definition:
//   position = 1 + q * (n - 1)  (1-based)
// with linear interpolation between adjacent order statistics.

namespace {

// ── Result struct type ─────────────────────────────────────────────────────

static LogicalType SummaryStatsResultType() {
	child_list_t<LogicalType> children;
	children.emplace_back("n", LogicalType::BIGINT);
	children.emplace_back("n_missing", LogicalType::BIGINT);
	children.emplace_back("mean", LogicalType::DOUBLE);
	children.emplace_back("sd", LogicalType::DOUBLE);
	children.emplace_back("variance", LogicalType::DOUBLE);
	children.emplace_back("min", LogicalType::DOUBLE);
	children.emplace_back("q1", LogicalType::DOUBLE);
	children.emplace_back("median", LogicalType::DOUBLE);
	children.emplace_back("q3", LogicalType::DOUBLE);
	children.emplace_back("max", LogicalType::DOUBLE);
	children.emplace_back("iqr", LogicalType::DOUBLE);
	children.emplace_back("skewness", LogicalType::DOUBLE);
	children.emplace_back("kurtosis", LogicalType::DOUBLE);
	return LogicalType::STRUCT(std::move(children));
}

// ── Quantile helper (R type 7) ──────────────────────────────────────────────

static double Quantile(const std::vector<double> &sorted, double q) {
	// Precondition: `sorted` is non-empty and already sorted ascending.
	idx_t n = sorted.size();
	if (n == 1) {
		return sorted[0];
	}
	double pos = 1.0 + q * static_cast<double>(n - 1); // 1-based
	double floor_pos = std::floor(pos);
	double frac = pos - floor_pos;
	idx_t lo = static_cast<idx_t>(floor_pos) - 1; // 0-based
	if (lo >= n - 1) {
		return sorted[n - 1];
	}
	return sorted[lo] + frac * (sorted[lo + 1] - sorted[lo]);
}

// ── Aggregate state ────────────────────────────────────────────────────────

struct SummaryStatsState {
	std::vector<double> *values;
	int64_t n_missing;
};

static void SummaryStatsInit(const AggregateFunction &, data_ptr_t state_p) {
	auto &state = *reinterpret_cast<SummaryStatsState *>(state_p);
	state.values = nullptr;
	state.n_missing = 0;
}

static void SummaryStatsUpdate(Vector inputs[], AggregateInputData &, idx_t, Vector &state_vector, idx_t count) {
	auto &input = inputs[0];
	UnifiedVectorFormat idata;
	input.ToUnifiedFormat(count, idata);

	auto states = FlatVector::GetData<SummaryStatsState *>(state_vector);
	auto values = UnifiedVectorFormat::GetData<double>(idata);

	for (idx_t i = 0; i < count; i++) {
		auto &state = *states[i];
		auto idx = idata.sel->get_index(i);
		if (!idata.validity.RowIsValid(idx)) {
			state.n_missing++;
			continue;
		}
		double v = values[idx];
		if (std::isnan(v)) {
			// NaN inputs are treated as missing (same as R's na.rm behavior).
			state.n_missing++;
			continue;
		}
		if (!state.values) {
			state.values = new std::vector<double>();
		}
		state.values->push_back(v);
	}
}

static void SummaryStatsCombine(Vector &source, Vector &target, AggregateInputData &, idx_t count) {
	auto src = FlatVector::GetData<SummaryStatsState *>(source);
	auto tgt = FlatVector::GetData<SummaryStatsState *>(target);
	for (idx_t i = 0; i < count; i++) {
		auto &s = *src[i];
		auto &t = *tgt[i];
		t.n_missing += s.n_missing;
		if (s.values && !s.values->empty()) {
			if (!t.values) {
				t.values = new std::vector<double>();
			}
			t.values->insert(t.values->end(), s.values->begin(), s.values->end());
		}
	}
}

static void SummaryStatsDestroy(Vector &state_vector, AggregateInputData &, idx_t count) {
	auto states = FlatVector::GetData<SummaryStatsState *>(state_vector);
	for (idx_t i = 0; i < count; i++) {
		delete states[i]->values;
		states[i]->values = nullptr;
	}
}

static void SummaryStatsFinalize(Vector &state_vector, AggregateInputData &, Vector &result, idx_t count, idx_t offset) {
	auto states = FlatVector::GetData<SummaryStatsState *>(state_vector);
	auto &children = StructVector::GetEntries(result);

	for (idx_t i = 0; i < count; i++) {
		auto &state = *states[i];
		auto idx = i + offset;

		if (!state.values || state.values->empty()) {
			FlatVector::SetNull(result, idx, true);
			continue;
		}

		auto &values = *state.values;
		idx_t n = values.size();
		double dn = static_cast<double>(n);

		// Moments ─────────────────────────────────────────────────────
		double mean = 0.0;
		for (double v : values) {
			mean += v;
		}
		mean /= dn;

		double m2 = 0.0; // central moment order 2
		double m3 = 0.0; // central moment order 3
		double m4 = 0.0; // central moment order 4
		for (double v : values) {
			double d = v - mean;
			double d2 = d * d;
			m2 += d2;
			m3 += d2 * d;
			m4 += d2 * d2;
		}

		double variance = 0.0;
		double sd = 0.0;
		if (n >= 2) {
			variance = m2 / (dn - 1.0); // unbiased (Bessel-corrected)
			sd = std::sqrt(variance);
		}

		// Fisher-Pearson adjusted skewness (NaN for n < 3 or zero variance).
		double skewness = std::numeric_limits<double>::quiet_NaN();
		if (n >= 3 && m2 > 0.0) {
			double m2n = m2 / dn;
			double m3n = m3 / dn;
			double g1 = m3n / std::pow(m2n, 1.5);
			skewness = std::sqrt(dn * (dn - 1.0)) / (dn - 2.0) * g1;
		}

		// Sample excess kurtosis (NaN for n < 4 or zero variance).
		double kurtosis = std::numeric_limits<double>::quiet_NaN();
		if (n >= 4 && m2 > 0.0) {
			double m2n = m2 / dn;
			double m4n = m4 / dn;
			double g2 = m4n / (m2n * m2n) - 3.0;
			// Unbiased adjustment per R's e1071 / Excel KURT
			kurtosis = ((dn - 1.0) / ((dn - 2.0) * (dn - 3.0))) * ((dn + 1.0) * g2 + 6.0);
		}

		// Order stats ─────────────────────────────────────────────────
		std::sort(values.begin(), values.end());
		double vmin = values.front();
		double vmax = values.back();
		double q1 = Quantile(values, 0.25);
		double median = Quantile(values, 0.50);
		double q3 = Quantile(values, 0.75);
		double iqr = q3 - q1;

		// Write out ──────────────────────────────────────────────────
		FlatVector::GetData<int64_t>(*children[0])[idx] = static_cast<int64_t>(n);
		FlatVector::GetData<int64_t>(*children[1])[idx] = state.n_missing;
		FlatVector::GetData<double>(*children[2])[idx] = mean;
		FlatVector::GetData<double>(*children[3])[idx] = sd;
		FlatVector::GetData<double>(*children[4])[idx] = variance;
		FlatVector::GetData<double>(*children[5])[idx] = vmin;
		FlatVector::GetData<double>(*children[6])[idx] = q1;
		FlatVector::GetData<double>(*children[7])[idx] = median;
		FlatVector::GetData<double>(*children[8])[idx] = q3;
		FlatVector::GetData<double>(*children[9])[idx] = vmax;
		FlatVector::GetData<double>(*children[10])[idx] = iqr;
		FlatVector::GetData<double>(*children[11])[idx] = skewness;
		FlatVector::GetData<double>(*children[12])[idx] = kurtosis;
	}
}

} // namespace

void RegisterSummaryStats(DatabaseInstance &db) {
	AggregateFunction fn("summary_stats", {LogicalType::DOUBLE}, SummaryStatsResultType(),
	                     AggregateFunction::StateSize<SummaryStatsState>, SummaryStatsInit, SummaryStatsUpdate,
	                     SummaryStatsCombine, SummaryStatsFinalize, FunctionNullHandling::SPECIAL_HANDLING,
	                     nullptr, nullptr, SummaryStatsDestroy);
	ExtensionUtil::RegisterFunction(db, fn);
}

} // namespace duckdb
