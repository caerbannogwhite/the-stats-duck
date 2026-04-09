#pragma once

#include "duckdb.hpp"

namespace duckdb {

void RegisterMannWhitneyU(DatabaseInstance &db);
void RegisterWilcoxonSignedRank(DatabaseInstance &db);

} // namespace duckdb
