#pragma once

#include "duckdb.hpp"

namespace duckdb {

//! Registers sign_test_1samp(x, [mu], [alternative]) — one-sample sign test for
//! the median. Counts signs of (x - mu), excludes ties (x == mu), and computes
//! a binomial p-value against p=0.5. Returns STRUCT(test_type, m_statistic,
//! n_pos, n_neg, n_zero, p_value, alternative, n).
//!
//! Registers sign_test_paired(x, y, [alternative]) — paired sign test on the
//! signs of (x - y). Same result-struct shape.
void RegisterSignTest(ExtensionLoader &loader);

} // namespace duckdb
