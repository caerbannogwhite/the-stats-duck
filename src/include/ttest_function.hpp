#pragma once

#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"

namespace duckdb {

void RegisterTTest1Samp(DatabaseInstance &db);
void RegisterTTest2Samp(DatabaseInstance &db);
void RegisterTTestPaired(DatabaseInstance &db);

} // namespace duckdb
