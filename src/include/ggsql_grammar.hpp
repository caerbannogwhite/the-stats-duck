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
	string aesthetic;  // channel to scale (x, y, color, ...)
	string property;   // Vega-Lite scale property: "scheme", "domain", "zero", ...
	string value_json; // value formatted as JSON ('"viridis"', '[0,100]', 'false', ...)
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
	string facet_layout; // "" (grid), "rows", or "cols" — only meaningful when a facet aesthetic is present
};

struct ParseResult {
	bool success = false;
	VisualizeStatement stmt;
	string error;
};

ParseResult ParseGgsql(const string &query);

} // namespace ggsql
} // namespace duckdb
