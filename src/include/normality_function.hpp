#pragma once

#include "duckdb.hpp"

namespace duckdb {

//! Registers jarque_bera(column) — a normality test based on sample skewness
//! and kurtosis. Under H0 (the data comes from a normal distribution), the
//! test statistic is asymptotically chi-square with 2 degrees of freedom.
//! Returns STRUCT(test_type, jb_statistic, skewness, excess_kurtosis, df,
//! p_value, n).
void RegisterJarqueBera(ExtensionLoader &loader);

//! Registers shapiro_wilk(column) — the Shapiro-Wilk normality test via
//! Royston's AS R94 (1995) polynomial approximation. Valid for n in [3, 5000].
//! W is the squared correlation of the sorted standardised values against the
//! normal-quantile plotting positions; under H0 (normality), W is close to 1
//! and small values are evidence against normality. Returns STRUCT(test_type,
//! w_statistic, p_value, n).
void RegisterShapiroWilk(ExtensionLoader &loader);

//! Registers anderson_darling(column) — Anderson-Darling normality test
//! against the fitted normal (mean and variance estimated from the sample).
//! Buffer-based aggregate: state holds the values, sort + scan happen in
//! Finalize. p-value via Stephens (1986)'s four-segment approximation; valid
//! for n >= 8 (returns NULL otherwise). Returns STRUCT(test_type, a_squared,
//! a_squared_adjusted, p_value, n).
void RegisterAndersonDarling(ExtensionLoader &loader);

} // namespace duckdb
