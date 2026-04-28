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

struct ScaleSpec {
	string aesthetic; // channel name to scale (color, fill, ...)
	string scheme;    // Vega-Lite scheme name (accent, viridis, category10, ...)
};

struct TypeOverride {
	string aesthetic; // channel name (color, x, ...)
	string type;      // quantitative | ordinal | nominal | temporal
};

struct VisualizeStatement {
	vector<AestheticMapping> aesthetics;
	string from_table;
	vector<DrawLayer> layers;
	vector<ScaleSpec> scales;
	vector<TypeOverride> type_overrides;
};

struct ParseResult {
	bool success = false;
	VisualizeStatement stmt;
	string error;
};

ParseResult ParseGgsql(const string &query);

} // namespace ggsql
} // namespace duckdb
