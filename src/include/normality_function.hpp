#pragma once

#include "duckdb.hpp"

namespace duckdb {

//! Registers jarque_bera(column) — a normality test based on sample skewness
//! and kurtosis. Under H0 (the data comes from a normal distribution), the
//! test statistic is asymptotically chi-square with 2 degrees of freedom.
//! Returns STRUCT(test_type, jb_statistic, skewness, excess_kurtosis, df,
//! p_value, n).
void RegisterJarqueBera(ExtensionLoader &loader);

} // namespace duckdb
