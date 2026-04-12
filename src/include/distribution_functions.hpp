#pragma once

#include "duckdb.hpp"

namespace duckdb {

//! Registers scalar PDF / CDF / quantile functions for the normal, Student's t,
//! chi-square, and F distributions. Function names mirror R's convention:
//! d* = density, p* = CDF, q* = quantile.
void RegisterDistributionFunctions(DatabaseInstance &db);

} // namespace duckdb
