#pragma once

#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"

namespace duckdb {

void RegisterTTest1Samp(ExtensionLoader &loader);
void RegisterTTest2Samp(ExtensionLoader &loader);
void RegisterTTestPaired(ExtensionLoader &loader);

} // namespace duckdb
