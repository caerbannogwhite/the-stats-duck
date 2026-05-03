// ggsql_marks_internal.hpp — stats_duck-internal helpers used by ggsql.cpp to
// register and look up marks via the catalog. NOT part of the public ABI;
// downstream extensions (bio-stats etc.) should NOT include this header.

#pragma once

#include "ggsql_marks.hpp"

namespace duckdb {

class ClientContext;
class ExtensionLoader;

namespace ggsql {

// Registers a no-arg scalar function `ggsql_mark_v1_<name>` whose
// function_info carries a MarkInfo. The function's executable callback is
// only a debug stub: invoking it via `SELECT ggsql_mark_v1_point()` returns a
// (name, required_aesthetics) introspection struct.
void RegisterMark(ExtensionLoader &loader, const string &name,
                  vector<string> required_aesthetics, mark_render_t render);

// Resolves a mark by name through the catalog. Throws InvalidInputException
// if the mark is not registered or carries an incompatible ABI version.
const MarkInfo &LookupMark(ClientContext &context, const string &name);

// Registers the four built-in marks (point, line, bar, histogram).
void RegisterBuiltinMarks(ExtensionLoader &loader);

} // namespace ggsql
} // namespace duckdb
