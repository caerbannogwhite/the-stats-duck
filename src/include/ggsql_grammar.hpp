#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "ggsql_marks.hpp"

namespace duckdb {
namespace ggsql {

struct DrawLayer {
	string mark;
};

struct VisualizeStatement {
	vector<AestheticMapping> aesthetics;
	string from_table;
	vector<DrawLayer> layers;
};

struct ParseResult {
	bool success = false;
	VisualizeStatement stmt;
	string error;
};

ParseResult ParseGgsql(const string &query);

} // namespace ggsql
} // namespace duckdb
