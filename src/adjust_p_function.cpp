#include "adjust_p_function.hpp"

#include "duckdb/function/scalar_function.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

#include <algorithm>
#include <cmath>
#include <limits>
#include <numeric>
#include <string>
#include <vector>

namespace duckdb {

// Multiple-testing corrections — adjusted p-values from a vector of raw p-values.
// Methods follow R's p.adjust naming and formulas. For sorted ascending
// p_(1) ≤ … ≤ p_(n) the canonical step-down / step-up adjustments are:
//
//   bonferroni:  q_(i) = min(1, n · p_(i))                          ← per-row, trivial
//   holm:        q_(i) = min(1, max_{j≤i} (n−j+1) · p_(j))          ← step-down cumulative max
//   hochberg:    q_(i) = min(1, min_{j≥i} (n−j+1) · p_(j))          ← step-up cumulative min
//   BH (fdr):    q_(i) = min(1, min_{j≥i} (n/j) · p_(j))            ← step-up cumulative min
//   BY:          q_(i) = min(1, min_{j≥i} (n·H_n/j) · p_(j))        ← BH with harmonic factor
//   none:        q_(i) = p_(i)
//
// NULLs in the input list are passed through to the output at the same
// position and excluded from `n` and from the rank computation.

namespace {

enum class AdjustMethod {
	BONFERRONI,
	HOLM,
	HOCHBERG,
	BENJAMINI_HOCHBERG,
	BENJAMINI_YEKUTIELI,
	NONE,
};

static AdjustMethod ParseMethod(const std::string &name) {
	if (name == "bonferroni") return AdjustMethod::BONFERRONI;
	if (name == "holm") return AdjustMethod::HOLM;
	if (name == "hochberg") return AdjustMethod::HOCHBERG;
	if (name == "BH" || name == "fdr") return AdjustMethod::BENJAMINI_HOCHBERG;
	if (name == "BY") return AdjustMethod::BENJAMINI_YEKUTIELI;
	if (name == "none") return AdjustMethod::NONE;
	throw InvalidInputException(
	    "adjust_p: unknown method '%s' — supported: 'bonferroni', 'holm', "
	    "'hochberg', 'BH' (or 'fdr'), 'BY', 'none'",
	    name);
}

//! Apply the adjustment in place. `p_in` holds raw p-values; `valid`
//! marks which entries are non-NULL. Results are written to `p_out`
//! (same length); NULL slots in the input become NaN in the output and
//! the caller marks them as invalid.
static void AdjustInPlace(const std::vector<double> &p_in, const std::vector<bool> &valid,
                          AdjustMethod method, std::vector<double> &p_out) {
	idx_t total = p_in.size();
	double nan = std::numeric_limits<double>::quiet_NaN();
	p_out.assign(total, nan);

	// Collect non-NULL indices.
	std::vector<idx_t> idx_valid;
	idx_valid.reserve(total);
	for (idx_t i = 0; i < total; i++) {
		if (valid[i]) {
			idx_valid.push_back(i);
		}
	}
	idx_t n = idx_valid.size();
	if (n == 0) {
		return;
	}

	if (method == AdjustMethod::NONE) {
		for (idx_t k : idx_valid) {
			p_out[k] = p_in[k];
		}
		return;
	}

	if (method == AdjustMethod::BONFERRONI) {
		double n_d = static_cast<double>(n);
		for (idx_t k : idx_valid) {
			double q = n_d * p_in[k];
			p_out[k] = q > 1.0 ? 1.0 : q;
		}
		return;
	}

	// Step-down / step-up methods: sort the valid indices by raw p value.
	std::vector<idx_t> order = idx_valid;
	std::sort(order.begin(), order.end(),
	          [&](idx_t a, idx_t b) { return p_in[a] < p_in[b]; });

	double n_d = static_cast<double>(n);

	switch (method) {
	case AdjustMethod::HOLM: {
		// Cumulative max from the smallest p upward; multiplier (n - i + 1) for
		// 1-based rank i. In 0-based form: multiplier (n - i) for i in [0, n).
		double cummax = 0.0;
		for (idx_t i = 0; i < n; i++) {
			double q = (n_d - static_cast<double>(i)) * p_in[order[i]];
			if (q > cummax) {
				cummax = q;
			}
			p_out[order[i]] = cummax > 1.0 ? 1.0 : cummax;
		}
		break;
	}
	case AdjustMethod::HOCHBERG: {
		// Cumulative min from the largest p downward.
		double cummin = std::numeric_limits<double>::infinity();
		for (idx_t i = n; i-- > 0;) {
			double q = (n_d - static_cast<double>(i)) * p_in[order[i]];
			if (q < cummin) {
				cummin = q;
			}
			p_out[order[i]] = cummin > 1.0 ? 1.0 : cummin;
		}
		break;
	}
	case AdjustMethod::BENJAMINI_HOCHBERG: {
		// Step-up FDR: q_(i) = min over j ≥ i of (n / rank_j) · p_(j).
		double cummin = std::numeric_limits<double>::infinity();
		for (idx_t i = n; i-- > 0;) {
			double rank = static_cast<double>(i + 1); // 1-based
			double q = (n_d / rank) * p_in[order[i]];
			if (q < cummin) {
				cummin = q;
			}
			p_out[order[i]] = cummin > 1.0 ? 1.0 : cummin;
		}
		break;
	}
	case AdjustMethod::BENJAMINI_YEKUTIELI: {
		// Like BH but with the harmonic-number penalty for arbitrary dependence.
		double harmonic = 0.0;
		for (idx_t k = 1; k <= n; k++) {
			harmonic += 1.0 / static_cast<double>(k);
		}
		double cummin = std::numeric_limits<double>::infinity();
		for (idx_t i = n; i-- > 0;) {
			double rank = static_cast<double>(i + 1);
			double q = (n_d * harmonic / rank) * p_in[order[i]];
			if (q < cummin) {
				cummin = q;
			}
			p_out[order[i]] = cummin > 1.0 ? 1.0 : cummin;
		}
		break;
	}
	default:
		break; // unreachable: covered above
	}
}

static void AdjustPExec(DataChunk &args, ExpressionState &, Vector &result) {
	idx_t row_count = args.size();
	auto &list_input = args.data[0];
	auto &method_input = args.data[1];

	UnifiedVectorFormat list_fmt, method_fmt;
	list_input.ToUnifiedFormat(row_count, list_fmt);
	method_input.ToUnifiedFormat(row_count, method_fmt);
	auto list_entries = UnifiedVectorFormat::GetData<list_entry_t>(list_fmt);
	auto method_data = UnifiedVectorFormat::GetData<string_t>(method_fmt);

	// Child vector of the input list (the flat array of doubles).
	auto &input_child = ListVector::GetEntry(list_input);
	UnifiedVectorFormat child_fmt;
	input_child.ToUnifiedFormat(ListVector::GetListSize(list_input), child_fmt);
	auto child_data = UnifiedVectorFormat::GetData<double>(child_fmt);

	// Compute total output size = total input size (NULLs preserved as NULLs).
	idx_t total_output = 0;
	for (idx_t i = 0; i < row_count; i++) {
		auto idx = list_fmt.sel->get_index(i);
		if (list_fmt.validity.RowIsValid(idx)) {
			total_output += list_entries[idx].length;
		}
	}
	ListVector::Reserve(result, total_output);
	ListVector::SetListSize(result, total_output);

	auto result_list_entries = FlatVector::GetData<list_entry_t>(result);
	auto &result_child = ListVector::GetEntry(result);
	auto result_child_data = FlatVector::GetData<double>(result_child);
	auto &result_child_validity = FlatVector::Validity(result_child);

	std::vector<double> p_in_buf;
	std::vector<bool> valid_buf;
	std::vector<double> p_out_buf;
	idx_t out_offset = 0;

	for (idx_t i = 0; i < row_count; i++) {
		auto list_idx = list_fmt.sel->get_index(i);
		auto method_idx = method_fmt.sel->get_index(i);

		if (!list_fmt.validity.RowIsValid(list_idx) ||
		    !method_fmt.validity.RowIsValid(method_idx)) {
			FlatVector::SetNull(result, i, true);
			result_list_entries[i] = list_entry_t(out_offset, 0);
			continue;
		}

		auto entry = list_entries[list_idx];
		auto method_str = method_data[method_idx];
		std::string method_s(method_str.GetData(), method_str.GetSize());
		AdjustMethod method = ParseMethod(method_s);

		// Extract input values + validity.
		p_in_buf.assign(entry.length, 0.0);
		valid_buf.assign(entry.length, false);
		for (idx_t j = 0; j < entry.length; j++) {
			auto child_idx = child_fmt.sel->get_index(entry.offset + j);
			if (child_fmt.validity.RowIsValid(child_idx)) {
				double v = child_data[child_idx];
				if (!std::isnan(v)) {
					p_in_buf[j] = v;
					valid_buf[j] = true;
				}
			}
		}

		AdjustInPlace(p_in_buf, valid_buf, method, p_out_buf);

		// Emit results.
		result_list_entries[i] = list_entry_t(out_offset, entry.length);
		for (idx_t j = 0; j < entry.length; j++) {
			if (valid_buf[j]) {
				result_child_data[out_offset + j] = p_out_buf[j];
			} else {
				result_child_validity.SetInvalid(out_offset + j);
			}
		}
		out_offset += entry.length;
	}
}

} // namespace

void RegisterAdjustP(ExtensionLoader &loader) {
	ScalarFunction fn("adjust_p",
	                  {LogicalType::LIST(LogicalType::DOUBLE), LogicalType::VARCHAR},
	                  LogicalType::LIST(LogicalType::DOUBLE), AdjustPExec);
	loader.RegisterFunction(fn);
}

} // namespace duckdb
