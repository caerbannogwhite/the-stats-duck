#include "auto_bin_function.hpp"

#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector_operations/binary_executor.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/aggregate_function.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"

#include <algorithm>
#include <cmath>
#include <cstdio>
#include <numeric>
#include <string>
#include <vector>

namespace duckdb {

namespace {

//===--------------------------------------------------------------------===//
// Methods
//===--------------------------------------------------------------------===//

enum class BinMethod {
	STURGES,
	FREEDMAN_DIACONIS,
	SCOTT,
	SQRT,
	RICE,
	AUTO,
};

static BinMethod ParseMethod(const std::string &name) {
	if (name == "sturges") return BinMethod::STURGES;
	if (name == "fd") return BinMethod::FREEDMAN_DIACONIS;
	if (name == "scott") return BinMethod::SCOTT;
	if (name == "sqrt") return BinMethod::SQRT;
	if (name == "rice") return BinMethod::RICE;
	if (name == "auto") return BinMethod::AUTO;
	throw BinderException(
	    "bin_edges: unknown method '%s' — supported: 'sturges', 'fd', "
	    "'scott', 'sqrt', 'rice', 'auto'",
	    name);
}

//===--------------------------------------------------------------------===//
// Bind data (carries the constant method through to Finalize)
//===--------------------------------------------------------------------===//

struct BinEdgesBindData : public FunctionData {
	BinMethod method = BinMethod::STURGES;

	unique_ptr<FunctionData> Copy() const override {
		auto copy = make_uniq<BinEdgesBindData>();
		copy->method = method;
		return std::move(copy);
	}
	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<BinEdgesBindData>();
		return method == other.method;
	}
};

static unique_ptr<FunctionData>
BinEdgesBindNoArg(ClientContext &, AggregateFunction &,
                  vector<unique_ptr<Expression>> &) {
	return make_uniq<BinEdgesBindData>();
}

static unique_ptr<FunctionData>
BinEdgesBindMethod(ClientContext &context, AggregateFunction &function,
                   vector<unique_ptr<Expression>> &arguments) {
	if (!arguments[1]->IsFoldable()) {
		throw BinderException("bin_edges: 'method' must be a constant string");
	}
	Value method_val = ExpressionExecutor::EvaluateScalar(context, *arguments[1]);
	if (method_val.IsNull()) {
		throw BinderException("bin_edges: 'method' must not be NULL");
	}
	auto bd = make_uniq<BinEdgesBindData>();
	std::string method_str = StringUtil::Lower(method_val.GetValue<string>());
	bd->method = ParseMethod(method_str);
	Function::EraseArgument(function, arguments, 1);
	return std::move(bd);
}

//===--------------------------------------------------------------------===//
// State (buffer-based, lazy alloc — same shape as anderson_darling)
//===--------------------------------------------------------------------===//

struct BinEdgesState {
	std::vector<double> *values;
};

static void BinEdgesInit(const AggregateFunction &, data_ptr_t state_p) {
	auto &state = *reinterpret_cast<BinEdgesState *>(state_p);
	state.values = nullptr;
}

static void BinEdgesUpdate(Vector inputs[], AggregateInputData &, idx_t,
                            Vector &state_vector, idx_t count) {
	UnifiedVectorFormat idata;
	inputs[0].ToUnifiedFormat(count, idata);
	auto states = FlatVector::GetData<BinEdgesState *>(state_vector);
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

static void BinEdgesCombine(Vector &source, Vector &target, AggregateInputData &, idx_t count) {
	auto src = FlatVector::GetData<BinEdgesState *>(source);
	auto tgt = FlatVector::GetData<BinEdgesState *>(target);
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

static void BinEdgesDestroy(Vector &state_vector, AggregateInputData &, idx_t count) {
	auto states = FlatVector::GetData<BinEdgesState *>(state_vector);
	for (idx_t i = 0; i < count; i++) {
		delete states[i]->values;
	}
}

//===--------------------------------------------------------------------===//
// Bin-count rules
//===--------------------------------------------------------------------===//

//! Type-7 (R/Excel-INC) quantile from an already-sorted vector. Linear
//! interpolation between adjacent order statistics; same formula used in
//! summary_stats. Assumes n >= 1.
static double Quantile7(const std::vector<double> &sorted, double p) {
	idx_t n = sorted.size();
	if (n == 1) {
		return sorted[0];
	}
	double pos = p * (static_cast<double>(n) - 1.0);
	idx_t lo = static_cast<idx_t>(std::floor(pos));
	idx_t hi = static_cast<idx_t>(std::ceil(pos));
	double frac = pos - static_cast<double>(lo);
	return sorted[lo] + frac * (sorted[hi] - sorted[lo]);
}

//! Compute the bin count k for a sorted, non-degenerate sample (n >= 2,
//! range > 0). FD and Scott silently fall back to Sturges when their
//! width estimate is non-positive — same convention as numpy / R.
static int ComputeBinCount(BinMethod method, const std::vector<double> &sorted) {
	idx_t n = sorted.size();
	double n_d = static_cast<double>(n);
	double range = sorted.back() - sorted.front();

	auto sturges = [&]() -> int {
		return std::max(1, static_cast<int>(std::ceil(std::log2(n_d) + 1.0)));
	};
	auto fd = [&]() -> int {
		double iqr = Quantile7(sorted, 0.75) - Quantile7(sorted, 0.25);
		if (iqr <= 0.0) {
			return sturges();
		}
		double width = 2.0 * iqr * std::pow(n_d, -1.0 / 3.0);
		if (width <= 0.0) {
			return sturges();
		}
		return std::max(1, static_cast<int>(std::ceil(range / width)));
	};
	auto scott = [&]() -> int {
		double mean = std::accumulate(sorted.begin(), sorted.end(), 0.0) / n_d;
		double sq = 0.0;
		for (double v : sorted) {
			double d = v - mean;
			sq += d * d;
		}
		if (n < 2) {
			return sturges();
		}
		double sd = std::sqrt(sq / (n_d - 1.0));
		if (sd <= 0.0) {
			return sturges();
		}
		double width = 3.5 * sd * std::pow(n_d, -1.0 / 3.0);
		if (width <= 0.0) {
			return sturges();
		}
		return std::max(1, static_cast<int>(std::ceil(range / width)));
	};
	auto sqrt_rule = [&]() -> int {
		return std::max(1, static_cast<int>(std::ceil(std::sqrt(n_d))));
	};
	auto rice = [&]() -> int {
		return std::max(1, static_cast<int>(std::ceil(2.0 * std::pow(n_d, 1.0 / 3.0))));
	};

	switch (method) {
	case BinMethod::STURGES:
		return sturges();
	case BinMethod::FREEDMAN_DIACONIS:
		return fd();
	case BinMethod::SCOTT:
		return scott();
	case BinMethod::SQRT:
		return sqrt_rule();
	case BinMethod::RICE:
		return rice();
	case BinMethod::AUTO:
		return std::max(sturges(), fd());
	}
	return sturges(); // unreachable, keep MSVC happy
}

//===--------------------------------------------------------------------===//
// Finalize — emit LIST<DOUBLE> per state
//===--------------------------------------------------------------------===//

static void BinEdgesFinalize(Vector &state_vector, AggregateInputData &input_data, Vector &result,
                              idx_t count, idx_t offset) {
	auto &bd = input_data.bind_data->Cast<BinEdgesBindData>();
	auto states = FlatVector::GetData<BinEdgesState *>(state_vector);
	auto list_entries = FlatVector::GetData<list_entry_t>(result);

	// First pass: compute edges per row + total child elements.
	std::vector<std::vector<double>> per_row(count);
	std::vector<bool> per_row_null(count, false);
	idx_t total_new_elements = 0;
	for (idx_t i = 0; i < count; i++) {
		auto &state = *states[i];
		if (!state.values || state.values->size() < 2) {
			// n=0 → null; n=1 → null (can't form a meaningful range)
			per_row_null[i] = true;
			continue;
		}
		auto &vals = *state.values;
		std::sort(vals.begin(), vals.end());
		double lo = vals.front();
		double hi = vals.back();
		if (hi <= lo) {
			// All values equal — degenerate range, emit a single-point
			// degenerate edge list so bin_label still has something to chew on.
			per_row[i] = {lo, lo};
			total_new_elements += 2;
			continue;
		}
		int k = ComputeBinCount(bd.method, vals);
		if (k < 1) {
			k = 1;
		}
		per_row[i].reserve(static_cast<idx_t>(k) + 1);
		for (int j = 0; j <= k; j++) {
			per_row[i].push_back(lo + static_cast<double>(j) * (hi - lo) / static_cast<double>(k));
		}
		total_new_elements += per_row[i].size();
	}

	// Second pass: append child elements and stamp list_entry_t. Anchor the
	// new child entries past whatever the result vector already holds (so
	// repeated Finalize calls into the same chunk compose safely).
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
		auto &edges = per_row[i];
		list_entries[idx] = list_entry_t(out_offset, edges.size());
		for (idx_t j = 0; j < edges.size(); j++) {
			child_data[out_offset + j] = edges[j];
		}
		out_offset += edges.size();
	}
	ListVector::SetListSize(result, out_offset);
}

//===--------------------------------------------------------------------===//
// bin_label scalar
//===--------------------------------------------------------------------===//

//! Format a double for use in a bin label: %g chops trailing zeros and uses
//! scientific notation only for very large / very small values. 6 sig figs
//! is enough for typical clinical / scientific ranges.
static std::string FormatEdge(double v) {
	char buf[64];
	std::snprintf(buf, sizeof(buf), "%g", v);
	return buf;
}

static void BinLabelExec(DataChunk &args, ExpressionState &, Vector &result) {
	idx_t row_count = args.size();
	auto &x_input = args.data[0];
	auto &edges_input = args.data[1];

	UnifiedVectorFormat x_fmt, edges_fmt;
	x_input.ToUnifiedFormat(row_count, x_fmt);
	edges_input.ToUnifiedFormat(row_count, edges_fmt);
	auto x_data = UnifiedVectorFormat::GetData<double>(x_fmt);
	auto edges_entries = UnifiedVectorFormat::GetData<list_entry_t>(edges_fmt);

	auto &edges_child = ListVector::GetEntry(edges_input);
	UnifiedVectorFormat child_fmt;
	edges_child.ToUnifiedFormat(ListVector::GetListSize(edges_input), child_fmt);
	auto child_data = UnifiedVectorFormat::GetData<double>(child_fmt);

	auto result_data = FlatVector::GetData<string_t>(result);
	auto &result_validity = FlatVector::Validity(result);

	for (idx_t i = 0; i < row_count; i++) {
		auto x_idx = x_fmt.sel->get_index(i);
		auto edges_idx = edges_fmt.sel->get_index(i);

		if (!x_fmt.validity.RowIsValid(x_idx) || !edges_fmt.validity.RowIsValid(edges_idx)) {
			result_validity.SetInvalid(i);
			continue;
		}
		double x = x_data[x_idx];
		if (std::isnan(x)) {
			result_validity.SetInvalid(i);
			continue;
		}

		auto entry = edges_entries[edges_idx];
		// Need at least one full bin (two edges).
		if (entry.length < 2) {
			result_validity.SetInvalid(i);
			continue;
		}

		// Pull edges into a small local — typical k is <50 so the copy is cheap.
		std::vector<double> edges(entry.length);
		bool has_null_edge = false;
		for (idx_t j = 0; j < entry.length; j++) {
			auto child_idx = child_fmt.sel->get_index(entry.offset + j);
			if (!child_fmt.validity.RowIsValid(child_idx)) {
				has_null_edge = true;
				break;
			}
			edges[j] = child_data[child_idx];
		}
		if (has_null_edge) {
			result_validity.SetInvalid(i);
			continue;
		}

		double lo = edges.front();
		double hi = edges.back();
		// Out-of-range → NULL. The user can COALESCE if they want a sentinel.
		if (x < lo || x > hi) {
			result_validity.SetInvalid(i);
			continue;
		}

		// Locate the bin. The right edge is closed for the LAST bin only,
		// so values equal to the maximum land in the final bin rather than
		// triggering "no bin found".
		idx_t n_bins = entry.length - 1;
		idx_t bin = 0;
		bool found = false;
		for (idx_t j = 0; j < n_bins; j++) {
			bool last = (j == n_bins - 1);
			bool in_bin = last ? (x >= edges[j] && x <= edges[j + 1])
			                   : (x >= edges[j] && x < edges[j + 1]);
			if (in_bin) {
				bin = j;
				found = true;
				break;
			}
		}
		if (!found) {
			result_validity.SetInvalid(i);
			continue;
		}

		bool last = (bin == n_bins - 1);
		std::string lo_s = FormatEdge(edges[bin]);
		std::string hi_s = FormatEdge(edges[bin + 1]);
		std::string label;
		label.reserve(lo_s.size() + hi_s.size() + 4);
		label.push_back('[');
		label += lo_s;
		label += ", ";
		label += hi_s;
		label.push_back(last ? ']' : ')');
		result_data[i] = StringVector::AddString(result, label);
	}
}

//===--------------------------------------------------------------------===//
// Factory helper
//===--------------------------------------------------------------------===//

static AggregateFunction MakeBinEdges(vector<LogicalType> args, bind_aggregate_function_t bind_fn) {
	return AggregateFunction(
	    "bin_edges", std::move(args), LogicalType::LIST(LogicalType::DOUBLE),
	    AggregateFunction::StateSize<BinEdgesState>, BinEdgesInit, BinEdgesUpdate, BinEdgesCombine,
	    BinEdgesFinalize, FunctionNullHandling::DEFAULT_NULL_HANDLING, nullptr, bind_fn,
	    BinEdgesDestroy);
}

} // namespace

void RegisterBinEdges(ExtensionLoader &loader) {
	AggregateFunctionSet set("bin_edges");
	set.AddFunction(MakeBinEdges({LogicalType::DOUBLE}, BinEdgesBindNoArg));
	set.AddFunction(MakeBinEdges({LogicalType::DOUBLE, LogicalType::VARCHAR}, BinEdgesBindMethod));
	loader.RegisterFunction(set);
}

void RegisterBinLabel(ExtensionLoader &loader) {
	ScalarFunction fn("bin_label",
	                  {LogicalType::DOUBLE, LogicalType::LIST(LogicalType::DOUBLE)},
	                  LogicalType::VARCHAR, BinLabelExec);
	loader.RegisterFunction(fn);
}

} // namespace duckdb
