#pragma once

#include "duckdb.hpp"

namespace duckdb {

class ExtensionLoader;

void RegisterGgsqlParserExtension(ExtensionLoader &loader);

} // namespace duckdb
