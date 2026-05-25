#pragma once

#include "duckdb.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {

// bin_edges(x [, method]) → LIST<DOUBLE>
//   Buffer-based aggregate that computes the bin-edge vector for a numeric
//   column under one of six classical rules. Returns the edges as a sorted
//   LIST<DOUBLE> of length k+1 (so the caller can inspect / plot the
//   boundaries directly). Methods (case-insensitive, default 'sturges'):
//     'sturges'   — k = ceil(log2(n) + 1)        (R's hist() default)
//     'fd'        — Freedman-Diaconis: bin_width = 2·IQR·n^(-1/3)
//     'scott'     — bin_width = 3.5·sd·n^(-1/3)  (normal-reference rule)
//     'sqrt'      — k = ceil(sqrt(n))            (Excel default)
//     'rice'      — k = ceil(2·n^(1/3))
//     'auto'      — max(sturges, fd)             (numpy's default)
//
//   Degenerate inputs (n < 2, or all values equal, or IQR/sd = 0 in FD/Scott)
//   silently fall back to Sturges. Empty / all-NULL group → NULL output.
void RegisterBinEdges(ExtensionLoader &loader);

// bin_label(x, edges) → VARCHAR
//   Maps a value to the formatted label `[lo, hi)` for the bin containing x,
//   where the bin is defined by adjacent entries of the `edges` vector
//   (typically produced by bin_edges). The right-most bin is closed on the
//   right (`[lo, hi]`) so the maximum value is never dropped. Returns NULL
//   for x outside the [edges[0], edges[last]] range or for any NULL input.
void RegisterBinLabel(ExtensionLoader &loader);

} // namespace duckdb
