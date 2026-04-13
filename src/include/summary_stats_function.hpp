#pragma once

#include "duckdb.hpp"

namespace duckdb {

//! Registers summary_stats(column) — a single aggregate returning n, n_missing,
//! mean, sd, variance, min, q1, median, q3, max, iqr, skewness, and excess
//! kurtosis as a STRUCT. This is the "describe this column" button.
void RegisterSummaryStats(ExtensionLoader &loader);

} // namespace duckdb
