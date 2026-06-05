#include "bootstrap_function.hpp"

#include "duckdb/common/types/vector.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/aggregate_function.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"

#include <algorithm>
#include <cmath>
#include <numeric>
#include <random>
#include <string>
#include <vector>

namespace duckdb {

namespace {

//===--------------------------------------------------------------------===//
// Statistic kinds.
//===--------------------------------------------------------------------===//

enum class BootstrapStat {
	MEAN,
	MEDIAN,
	SUM,
	STDDEV,
	VARIANCE,
	MIN,
	MAX,
};

static BootstrapStat ParseStat(const std::string &name) {
	if (name == "mean") return BootstrapStat::MEAN;
	if (name == "median") return BootstrapStat::MEDIAN;
	if (name == "sum") return BootstrapStat::SUM;
	if (name == "stddev" || name == "sd") return BootstrapStat::STDDEV;
	if (name == "variance" || name == "var") return BootstrapStat::VARIANCE;
	if (name == "min") return BootstrapStat::MIN;
	if (name == "max") return BootstrapStat::MAX;
	throw BinderException(
	    "bootstrap: unknown statistic '%s' — supported: 'mean', 'median', 'sum', "
	    "'stddev' (alias 'sd'), 'variance' (alias 'var'), 'min', 'max'",
	    name);
}

//===--------------------------------------------------------------------===//
// Bind data (carries the constant statistic, n_iters, seed through Finalize).
//===--------------------------------------------------------------------===//

struct BootstrapBindData : public FunctionData {
	BootstrapStat stat = BootstrapStat::MEAN;
	int64_t n_iters = 0;
	int64_t seed = 0;
	bool has_seed = false;

	unique_ptr<FunctionData> Copy() const override {
		auto copy = make_uniq<BootstrapBindData>();
		copy->stat = stat;
		copy->n_iters = n_iters;
		copy->seed = seed;
		copy->has_seed = has_seed;
		return std::move(copy);
	}
	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<BootstrapBindData>();
		return stat == other.stat && n_iters == other.n_iters && seed == other.seed &&
		       has_seed == other.has_seed;
	}
};

//! Pull the constant statistic name + n_iters off the bound arguments. The
//! statistic and n_iters are erased from the function signature so the
//! aggregate runs as a single-input aggregate over the value column.
static unique_ptr<FunctionData>
BootstrapBindCommon(ClientContext &context, AggregateFunction &function,
                    vector<unique_ptr<Expression>> &arguments, bool has_seed) {
	auto bd = make_uniq<BootstrapBindData>();
	bd->has_seed = has_seed;

	if (!arguments[1]->IsFoldable()) {
		throw BinderException("bootstrap: 'statistic' must be a constant string");
	}
	Value stat_val = ExpressionExecutor::EvaluateScalar(context, *arguments[1]);
	if (stat_val.IsNull()) {
		throw BinderException("bootstrap: 'statistic' must not be NULL");
	}
	bd->stat = ParseStat(StringUtil::Lower(stat_val.GetValue<string>()));

	if (!arguments[2]->IsFoldable()) {
		throw BinderException("bootstrap: 'n_iters' must be a constant integer");
	}
	Value n_val = ExpressionExecutor::EvaluateScalar(context, *arguments[2]);
	if (n_val.IsNull()) {
		throw BinderException("bootstrap: 'n_iters' must not be NULL");
	}
	bd->n_iters = n_val.GetValue<int64_t>();
	if (bd->n_iters <= 0) {
		throw BinderException("bootstrap: 'n_iters' must be > 0 (got %lld)",
		                      static_cast<long long>(bd->n_iters));
	}

	if (has_seed) {
		if (!arguments[3]->IsFoldable()) {
			throw BinderException("bootstrap: 'seed' must be a constant integer");
		}
		Value seed_val = ExpressionExecutor::EvaluateScalar(context, *arguments[3]);
		if (seed_val.IsNull()) {
			throw BinderException("bootstrap: 'seed' must not be NULL");
		}
		bd->seed = seed_val.GetValue<int64_t>();
	}

	// Erase the constant args from highest to lowest index so positions remain
	// valid as each erase shifts the tail down.
	if (has_seed) {
		Function::EraseArgument(function, arguments, 3);
	}
	Function::EraseArgument(function, arguments, 2);
	Function::EraseArgument(function, arguments, 1);
	return std::move(bd);
}

static unique_ptr<FunctionData>
BootstrapBindNoSeed(ClientContext &context, AggregateFunction &function,
                    vector<unique_ptr<Expression>> &arguments) {
	return BootstrapBindCommon(context, function, arguments, false);
}

static unique_ptr<FunctionData>
BootstrapBindSeeded(ClientContext &context, AggregateFunction &function,
                    vector<unique_ptr<Expression>> &arguments) {
	return BootstrapBindCommon(context, function, arguments, true);
}

//===--------------------------------------------------------------------===//
// State (buffer-based — lazy alloc, heap-owned vector).
//===--------------------------------------------------------------------===//

struct BootstrapState {
	std::vector<double> *values;
};

static void BootstrapInit(const AggregateFunction &, data_ptr_t state_p) {
	auto &state = *reinterpret_cast<BootstrapState *>(state_p);
	state.values = nullptr;
}

static void BootstrapUpdate(Vector inputs[], AggregateInputData &, idx_t,
                            Vector &state_vector, idx_t count) {
	UnifiedVectorFormat idata;
	inputs[0].ToUnifiedFormat(count, idata);
	auto states = FlatVector::GetData<BootstrapState *>(state_vector);
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

static void BootstrapCombine(Vector &source, Vector &target, AggregateInputData &, idx_t count) {
	auto src = FlatVector::GetData<BootstrapState *>(source);
	auto tgt = FlatVector::GetData<BootstrapState *>(target);
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

static void BootstrapDestroy(Vector &state_vector, AggregateInputData &, idx_t count) {
	auto states = FlatVector::GetData<BootstrapState *>(state_vector);
	for (idx_t i = 0; i < count; i++) {
		delete states[i]->values;
	}
}

//===--------------------------------------------------------------------===//
// Statistic computation on a resample-index view of the source values.
//===--------------------------------------------------------------------===//

//! Compute the requested summary statistic from a resample described by indices
//! into the original `values` vector. The resample is the multiset
//! { values[indices[i]] : i ∈ [0, n) } — duplicates allowed, sample size n.
//! For MEDIAN we copy the resample into a scratch buffer so we can nth_element
//! in place without disturbing the indices.
static double ComputeStat(BootstrapStat stat, const std::vector<double> &values,
                          const std::vector<size_t> &indices, std::vector<double> &scratch) {
	size_t n = indices.size();
	switch (stat) {
	case BootstrapStat::MEAN: {
		double s = 0.0;
		for (size_t i = 0; i < n; i++) {
			s += values[indices[i]];
		}
		return s / static_cast<double>(n);
	}
	case BootstrapStat::SUM: {
		double s = 0.0;
		for (size_t i = 0; i < n; i++) {
			s += values[indices[i]];
		}
		return s;
	}
	case BootstrapStat::MIN: {
		double m = values[indices[0]];
		for (size_t i = 1; i < n; i++) {
			double v = values[indices[i]];
			if (v < m) m = v;
		}
		return m;
	}
	case BootstrapStat::MAX: {
		double m = values[indices[0]];
		for (size_t i = 1; i < n; i++) {
			double v = values[indices[i]];
			if (v > m) m = v;
		}
		return m;
	}
	case BootstrapStat::VARIANCE:
	case BootstrapStat::STDDEV: {
		if (n < 2) {
			return std::numeric_limits<double>::quiet_NaN();
		}
		// Two-pass mean+SS for numerical safety on heavy-tailed bootstrap draws.
		double mean = 0.0;
		for (size_t i = 0; i < n; i++) {
			mean += values[indices[i]];
		}
		mean /= static_cast<double>(n);
		double sq = 0.0;
		for (size_t i = 0; i < n; i++) {
			double d = values[indices[i]] - mean;
			sq += d * d;
		}
		double var = sq / static_cast<double>(n - 1);
		return stat == BootstrapStat::STDDEV ? std::sqrt(var) : var;
	}
	case BootstrapStat::MEDIAN: {
		scratch.resize(n);
		for (size_t i = 0; i < n; i++) {
			scratch[i] = values[indices[i]];
		}
		if (n % 2 == 1) {
			auto mid = scratch.begin() + n / 2;
			std::nth_element(scratch.begin(), mid, scratch.end());
			return *mid;
		}
		auto hi = scratch.begin() + n / 2;
		std::nth_element(scratch.begin(), hi, scratch.end());
		double right = *hi;
		auto lo = scratch.begin() + n / 2 - 1;
		std::nth_element(scratch.begin(), lo, hi);
		return 0.5 * (*lo + right);
	}
	}
	return std::numeric_limits<double>::quiet_NaN(); // unreachable
}

//===--------------------------------------------------------------------===//
// Finalize — emit LIST<DOUBLE> of n_iters resampled statistic values.
//===--------------------------------------------------------------------===//

static void BootstrapFinalize(Vector &state_vector, AggregateInputData &input_data, Vector &result,
                              idx_t count, idx_t offset) {
	auto &bd = input_data.bind_data->Cast<BootstrapBindData>();
	auto states = FlatVector::GetData<BootstrapState *>(state_vector);
	auto list_entries = FlatVector::GetData<list_entry_t>(result);

	// Pre-allocate the seed-derived RNG once; per-row we re-seed deterministically
	// from (seed, row index) when a seed is supplied so each group in a GROUP BY
	// gets a stable but distinct stream.
	std::vector<std::vector<double>> per_row(count);
	std::vector<bool> per_row_null(count, false);
	idx_t total_new_elements = 0;

	for (idx_t i = 0; i < count; i++) {
		auto &state = *states[i];
		if (!state.values || state.values->empty()) {
			per_row_null[i] = true;
			continue;
		}
		auto &vals = *state.values;
		size_t n = vals.size();

		std::mt19937_64 rng;
		if (bd.has_seed) {
			// Mix the per-row index into the seed so multi-group bootstraps
			// (one row per GROUP BY key) don't all produce identical streams.
			rng.seed(static_cast<uint64_t>(bd.seed) ^
			         (static_cast<uint64_t>(i + offset) * 0x9E3779B97F4A7C15ULL));
		} else {
			std::random_device rd;
			rng.seed((static_cast<uint64_t>(rd()) << 32) | rd());
		}
		std::uniform_int_distribution<size_t> dist(0, n - 1);

		auto &out = per_row[i];
		out.reserve(static_cast<size_t>(bd.n_iters));
		std::vector<size_t> indices(n);
		std::vector<double> scratch;
		for (int64_t it = 0; it < bd.n_iters; it++) {
			for (size_t k = 0; k < n; k++) {
				indices[k] = dist(rng);
			}
			out.push_back(ComputeStat(bd.stat, vals, indices, scratch));
		}
		total_new_elements += out.size();
	}

	idx_t child_anchor = ListVector::GetListSize(result);
	ListVector::Reserve(result, child_anchor + total_new_elements);
	auto &child = ListVector::GetEntry(result);
	auto child_data = FlatVector::GetData<double>(child);
	idx_t out_offset = child_anchor;
	for (idx_t i = 0; i < count; i++) {
		auto idx = i + offset;
		if (per_row_null[i]) {
			FlatVector::SetNull(result, idx, true);
			list_entries[idx] = list_entry_t(out_offset, 0);
			continue;
		}
		auto &samples = per_row[i];
		list_entries[idx] = list_entry_t(out_offset, samples.size());
		for (idx_t j = 0; j < samples.size(); j++) {
			child_data[out_offset + j] = samples[j];
		}
		out_offset += samples.size();
	}
	ListVector::SetListSize(result, out_offset);
}

} // namespace

void RegisterBootstrap(ExtensionLoader &loader) {
	auto list_double = LogicalType::LIST(LogicalType::DOUBLE);
	auto make_fn = [&](vector<LogicalType> args, bind_aggregate_function_t bind) {
		AggregateFunction fn("bootstrap", std::move(args), list_double, AggregateFunction::StateSize<BootstrapState>,
		                     BootstrapInit, BootstrapUpdate, BootstrapCombine, BootstrapFinalize,
		                     nullptr, bind, BootstrapDestroy);
		return fn;
	};

	AggregateFunctionSet set("bootstrap");
	set.AddFunction(make_fn({LogicalType::DOUBLE, LogicalType::VARCHAR, LogicalType::BIGINT},
	                        BootstrapBindNoSeed));
	set.AddFunction(make_fn({LogicalType::DOUBLE, LogicalType::VARCHAR, LogicalType::BIGINT,
	                         LogicalType::BIGINT},
	                        BootstrapBindSeeded));
	loader.RegisterFunction(set);
}

} // namespace duckdb
