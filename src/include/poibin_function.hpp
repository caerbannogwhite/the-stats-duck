#pragma once

#include "duckdb.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {

// Register `poibin_cdf(probs LIST<DOUBLE>, k BIGINT) → DOUBLE` — CDF of the
// Poisson Binomial distribution: the sum X = Σᵢ Bᵢ where each Bᵢ is an
// independent Bernoulli(pᵢ) with its own success probability.
//
// Returns P(X ≤ k). Computed by Hong (2013)'s direct convolution method —
// O(n²) time, O(n) memory, numerically stable for the n that show up in
// biostat-scale queries (typically n ≤ a few hundred).
//
// NULL handling: NULL in either argument → NULL output. NULL inside the
// `probs` list → NULL output (one bad probability invalidates the whole
// sum). Any pᵢ outside [0, 1] → NULL.
void RegisterPoibinCdf(ExtensionLoader &loader);

} // namespace duckdb
