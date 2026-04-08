#pragma once

#include "duckdb.hpp"

namespace duckdb {

void RegisterMannWhitneyU(ExtensionLoader &loader);
void RegisterWilcoxonSignedRank(ExtensionLoader &loader);

} // namespace duckdb
