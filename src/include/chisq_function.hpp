#pragma once

#include "duckdb.hpp"

namespace duckdb {

//! Registers chi-square tests:
//!   chisq_independence(row, col) — test of independence on a contingency
//!     table built from (row, col) VARCHAR pairs. Returns STRUCT(test_type,
//!     chi_square, df, p_value, n, n_rows, n_cols).
//!   chisq_goodness_of_fit(category) — goodness-of-fit test against the
//!     uniform distribution over observed categories. Returns STRUCT(test_type,
//!     chi_square, df, p_value, n, n_categories).
void RegisterChiSquareTests(DatabaseInstance &db);

} // namespace duckdb
