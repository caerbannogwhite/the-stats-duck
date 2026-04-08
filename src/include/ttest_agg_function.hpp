#pragma once

#include "duckdb.hpp"

namespace duckdb {

void RegisterTTest1SampAgg(ExtensionLoader &loader);
void RegisterTTest2SampAgg(ExtensionLoader &loader);
void RegisterTTestPairedAgg(ExtensionLoader &loader);

} // namespace duckdb
