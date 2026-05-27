#pragma once

#include "duckdb.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {

// corr_matrix(tbl, variables := [...] [, method := 'pearson']) → TABLE
//
// Long-format pairwise correlation matrix as a DuckDB table function.
// Emits one row per ordered pair (row_var, col_var) — including self-pairs on
// the diagonal and both (a, b) / (b, a) mirrors — so the result feeds the
// ggsql `heatmap` mark directly without a CROSS JOIN.
//
// Schema:  row_var VARCHAR, col_var VARCHAR, coef DOUBLE, p_value DOUBLE, n BIGINT
//
// `coef` is the chosen method's correlation coefficient: Pearson r, Spearman
// rho, or Kendall tau — the column name stays method-agnostic so a single
// downstream pipeline works across all three. NULLs in the source columns
// are excluded pairwise.
void RegisterCorrMatrix(ExtensionLoader &loader);

} // namespace duckdb
