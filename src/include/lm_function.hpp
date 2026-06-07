#pragma once

#include "duckdb.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {

//! Register the `lm` and `lm_summary` table functions.
//!
//!   lm('table_name', y := 'col', x := ['c1', 'c2', ...])
//!     → (term VARCHAR, estimate DOUBLE, std_error DOUBLE,
//!        t_statistic DOUBLE, p_value DOUBLE)
//!
//!   lm_summary('table_name', y := 'col', x := ['c1', 'c2', ...])
//!     → (r_squared DOUBLE, adj_r_squared DOUBLE, f_statistic DOUBLE,
//!        f_p_value DOUBLE, df_model BIGINT, df_residual BIGINT,
//!        sigma DOUBLE, n BIGINT)
//!
//! Both fit an OLS model y = β₀ + Σ βᵢ·xᵢ via Cholesky decomposition of X'X.
//! Rows with NULL in y or any x are excluded pairwise-complete style.
void RegisterLm(ExtensionLoader &loader);

} // namespace duckdb
