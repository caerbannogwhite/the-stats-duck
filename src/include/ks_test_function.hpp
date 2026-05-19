#pragma once

#include "duckdb.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {

// Register `ks_test_1samp(x)` — one-sample Kolmogorov-Smirnov test against the
// fitted normal N(mean(x), sd(x)). Returns
// STRUCT(test_type, d_statistic, p_value, n). P-value via the asymptotic
// Kolmogorov distribution with Stephens (1970) small-sample correction;
// because the parameters are estimated from the sample, the p-value is
// conservative — pair with shapiro_wilk / anderson_darling when normality
// is the primary question.
void RegisterKsTest1Samp(ExtensionLoader &loader);

// Register `ks_test_2samp(x, y)` — two-sample Kolmogorov-Smirnov test on
// whether x and y come from the same underlying distribution. Returns
// STRUCT(test_type, d_statistic, p_value, n_x, n_y). P-value via the
// asymptotic Kolmogorov distribution evaluated at the effective sample
// size n_x*n_y/(n_x+n_y).
void RegisterKsTest2Samp(ExtensionLoader &loader);

} // namespace duckdb
