#pragma once

#include "duckdb.hpp"

namespace duckdb {

void RegisterTTest1SampAgg(DatabaseInstance &db);
void RegisterTTest2SampAgg(DatabaseInstance &db);
void RegisterTTestPairedAgg(DatabaseInstance &db);

} // namespace duckdb
