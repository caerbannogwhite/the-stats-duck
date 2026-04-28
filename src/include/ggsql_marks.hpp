// ggsql_marks.hpp — public ABI for plugging chart marks into the ggsql parser
// extension at runtime.
//
// This header is the boundary between stats_duck (which hosts the VISUALIZE
// parser extension) and any other extension that wants to register additional
// marks — e.g. a "bio-stats" extension shipping Kaplan-Meier or forest-plot
// marks. Such an extension should vendor a copy of THIS header, link only
// against DuckDB (never against stats_duck), and rendezvous via the catalog.
//
// ABI stability rules (read these before changing anything):
//   - The layout of MarkInfo is FROZEN for ABI v1. New optional fields go in
//     a parallel MarkInfoV2 (with a new prefix `ggsql_mark_v2_`), never as
//     bare struct members.
//   - The first member of MarkInfo is `abi_version`. LookupMark verifies it.
//   - The function-name prefix `ggsql_mark_v1_` encodes the version: a
//     consumer targeting v1 simply does not find v2 marks, and vice versa.
//   - Bumping the ABI is a major-version event; coordinate cross-extension.

#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/function/scalar_function.hpp"

#include <cstdint>

namespace duckdb {
namespace ggsql {

#define GGSQL_MARK_ABI_VERSION 1u

constexpr const char *GGSQL_MARK_PREFIX = "ggsql_mark_v1_";

struct AestheticMapping {
	string expression; // SQL expression text (phase 1: identifier only)
	string aesthetic;  // "x", "y", "color", ...
};

struct MarkContext {
	const vector<AestheticMapping> &aesthetics;
	const string &projected_sql; // already-built "SELECT <expr> AS <aesth>... FROM <table>"
};

struct MarkResult {
	string layer_body; // Vega-Lite layer body, e.g. "\"mark\":\"point\",\"encoding\":{...}"
	string data_sql;   // SQL whose Arrow rows feed this layer's `data` block
};

typedef MarkResult (*mark_render_t)(const MarkContext &);

// Stashed on ScalarFunction.function_info for catalog-mediated dispatch.
// FROZEN layout for ABI v1. Do not reorder, insert, or grow.
struct MarkInfo : public ScalarFunctionInfo {
	uint32_t abi_version = GGSQL_MARK_ABI_VERSION;
	string name;
	vector<string> required_aesthetics;
	mark_render_t render;
};

} // namespace ggsql
} // namespace duckdb
