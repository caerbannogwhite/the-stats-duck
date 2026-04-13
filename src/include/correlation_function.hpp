#pragma once

#include "duckdb.hpp"

namespace duckdb {

//! Registers pearson_test(x, y, [alpha], [alternative]) — Pearson's product-moment
//! correlation with significance test (t-distribution) and Fisher-z confidence
//! interval. Returns STRUCT(test_type, r, t_statistic, df, p_value, alternative,
//! ci_lower, ci_upper, n).
void RegisterPearsonTest(ExtensionLoader &loader);

} // namespace duckdb
