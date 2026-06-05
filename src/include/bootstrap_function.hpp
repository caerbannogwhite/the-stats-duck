#pragma once

#include "duckdb.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {

//! Register `bootstrap(value DOUBLE, statistic VARCHAR, n_iters BIGINT[, seed BIGINT])
//! → LIST<DOUBLE>`. Buffer-based aggregate that resamples the input with
//! replacement n_iters times and emits the chosen summary statistic per
//! resample.
void RegisterBootstrap(ExtensionLoader &loader);

} // namespace duckdb
