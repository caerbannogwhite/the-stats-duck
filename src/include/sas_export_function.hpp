#pragma once

#include "duckdb.hpp"

namespace duckdb {

class ExtensionLoader;

void RegisterSasExport(ExtensionLoader &loader);

} // namespace duckdb
