#include "poibin_function.hpp"

#include "duckdb/common/types/vector.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

#include <cmath>
#include <vector>

namespace duckdb {

// Poisson Binomial distribution CDF — the distribution of X = Σᵢ Bᵢ where
// each Bᵢ ∼ Bernoulli(pᵢ) is independent but the pᵢ may differ.
//
// Algorithm: direct convolution (Hong 2013, "On computing the distribution
// function for the Poisson binomial distribution"). Start with pmf = [1, 0, …]
// (no trials → P(X=0) = 1) and fold each trial in by replacing
//   new_pmf[j] = pmf[j-1] · pᵢ + pmf[j] · (1 - pᵢ)
// updating top-down in place so we never read a stale slot. O(n²) time,
// O(n) memory; stable for moderate n.

namespace {

//! Returns P(X ≤ k) or NaN to signal an invalid input that should surface as
//! NULL. `probs_validity` parallels `probs`: any false entry indicates a NULL
//! probability and short-circuits to NaN.
static double PoibinCdf(const std::vector<double> &probs,
                        const std::vector<bool> &probs_validity, int64_t k) {
	idx_t n = probs.size();
	// Validate the probabilities first — one bad entry invalidates the whole
	// sum, since we have no principled way to "skip" a Bernoulli.
	for (idx_t i = 0; i < n; i++) {
		if (!probs_validity[i]) {
			return std::nan("");
		}
		double p = probs[i];
		if (std::isnan(p) || p < 0.0 || p > 1.0) {
			return std::nan("");
		}
	}

	if (k < 0) {
		return 0.0;
	}
	if (n == 0) {
		// Empty sum → X = 0 deterministically, so P(X ≤ k) = 1 for any k ≥ 0.
		return 1.0;
	}
	if (static_cast<idx_t>(k) >= n) {
		return 1.0;
	}

	// pmf[j] = P(X = j) after the trials processed so far. Allocate one slot
	// per possible value (0..n).
	std::vector<double> pmf(n + 1, 0.0);
	pmf[0] = 1.0;

	for (idx_t i = 0; i < n; i++) {
		double p = probs[i];
		double q = 1.0 - p;
		// After trial i, max occupied index is i+1. Update top-down so each
		// slot reads the pre-update value from j-1 before being overwritten.
		idx_t max_j = i + 1;
		pmf[max_j] = pmf[max_j - 1] * p;
		for (idx_t j = max_j - 1; j >= 1; j--) {
			pmf[j] = pmf[j - 1] * p + pmf[j] * q;
		}
		pmf[0] *= q;
	}

	double cdf = 0.0;
	idx_t k_cap = static_cast<idx_t>(k);
	for (idx_t j = 0; j <= k_cap; j++) {
		cdf += pmf[j];
	}
	// Clamp away tiny floating-point overshoot from the convolution.
	if (cdf > 1.0) {
		cdf = 1.0;
	} else if (cdf < 0.0) {
		cdf = 0.0;
	}
	return cdf;
}

static void PoibinCdfExec(DataChunk &args, ExpressionState &, Vector &result) {
	idx_t row_count = args.size();
	auto &list_input = args.data[0];
	auto &k_input = args.data[1];

	UnifiedVectorFormat list_fmt, k_fmt;
	list_input.ToUnifiedFormat(row_count, list_fmt);
	k_input.ToUnifiedFormat(row_count, k_fmt);
	auto list_entries = UnifiedVectorFormat::GetData<list_entry_t>(list_fmt);
	auto k_data = UnifiedVectorFormat::GetData<int64_t>(k_fmt);

	auto &input_child = ListVector::GetEntry(list_input);
	UnifiedVectorFormat child_fmt;
	input_child.ToUnifiedFormat(ListVector::GetListSize(list_input), child_fmt);
	auto child_data = UnifiedVectorFormat::GetData<double>(child_fmt);

	auto result_data = FlatVector::GetData<double>(result);
	auto &result_validity = FlatVector::Validity(result);

	std::vector<double> probs_buf;
	std::vector<bool> validity_buf;

	for (idx_t i = 0; i < row_count; i++) {
		auto list_idx = list_fmt.sel->get_index(i);
		auto k_idx = k_fmt.sel->get_index(i);

		if (!list_fmt.validity.RowIsValid(list_idx) ||
		    !k_fmt.validity.RowIsValid(k_idx)) {
			result_validity.SetInvalid(i);
			result_data[i] = 0.0;
			continue;
		}

		auto entry = list_entries[list_idx];
		int64_t k = k_data[k_idx];

		probs_buf.assign(entry.length, 0.0);
		validity_buf.assign(entry.length, true);
		for (idx_t j = 0; j < entry.length; j++) {
			auto child_idx = child_fmt.sel->get_index(entry.offset + j);
			if (!child_fmt.validity.RowIsValid(child_idx)) {
				validity_buf[j] = false;
			} else {
				probs_buf[j] = child_data[child_idx];
			}
		}

		double r = PoibinCdf(probs_buf, validity_buf, k);
		if (std::isnan(r)) {
			result_validity.SetInvalid(i);
			result_data[i] = 0.0;
		} else {
			result_data[i] = r;
		}
	}
}

} // namespace

void RegisterPoibinCdf(ExtensionLoader &loader) {
	ScalarFunction fn("poibin_cdf",
	                  {LogicalType::LIST(LogicalType::DOUBLE), LogicalType::BIGINT},
	                  LogicalType::DOUBLE, PoibinCdfExec);
	loader.RegisterFunction(fn);
}

} // namespace duckdb
