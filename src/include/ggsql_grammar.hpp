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
	string aesthetic;             // channel to scale (x, y, color, ...)
	string sub_object = "scale";  // channel sub-object: "scale" (TO/ZERO/DOMAIN) or "axis" (LABEL)
	string property;              // sub-object property: "scheme", "domain", "zero", "title", ...
	string value_json;            // value formatted as JSON ('"viridis"', '[0,100]', 'false', '"My label"', ...)
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
	string title;        // "" if not set; raw text (caller JSON-escapes for output)
	string subtitle;     // "" if not set; SUBTITLE alone (no TITLE) is rejected at parse time
};

// Strip surrounding single quotes from a SQL string literal and collapse '' → '.
// Tokens without surrounding single quotes are returned unchanged (so bare
// identifiers like `viridis` remain usable for operators that accept either).
string UnquoteSqlString(const string &raw);

// Escape a raw string for embedding inside a JSON string literal.
string JsonEscape(const string &s);

struct ParseResult {
	bool success = false;
	VisualizeStatement stmt;
	string error;
};

ParseResult ParseGgsql(const string &query);

} // namespace ggsql
} // namespace duckdb
