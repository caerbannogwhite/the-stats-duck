#pragma once

#include "duckdb.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {

//! Register per-row random-sampling functions (`rnorm`, `rt`, `rchisq`, `rf`,
//! `rgamma`, `rbeta`, `rexp`, `rweibull`, `rlnorm`, `rpois`). Each function is
//! VOLATILE, draws a uniform sample from a thread-local std::mt19937_64 seeded
//! from std::random_device, and returns the inverse-CDF transform.
void RegisterRandomSampling(ExtensionLoader &loader);

} // namespace duckdb
