// marks_demo_extension.cpp — throwaway spike to verify cross-WASM-module mark
// dispatch in DuckDB-WASM. Registers a single mark "demo" with a sentinel
// renderer, so we can VISUALIZE ... DRAW demo from a stats_duck-loaded
// session and confirm:
//   1. Two of our own DuckDB extensions can co-load (catalog merge works).
//   2. stats_duck's plan_function can find a mark registered by a DIFFERENT
//      extension via Catalog::GetEntry + ScalarFunction::function_info.
//   3. The function pointer cross-module dispatch (mark_render_t) hits the
//      right code in marks_demo.wasm.
//
// In bio-stats this header would be vendored. Here we include from src/include
// because the spike is in-tree.

#define DUCKDB_EXTENSION_MAIN

#include "ggsql_marks.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {

namespace {

ggsql::MarkResult RenderDemo(const ggsql::MarkContext &ctx) {
	ggsql::MarkResult r;
	// Sentinel string lets the Bedevere agent verify the dispatch went through
	// THIS extension's code (and not, e.g., a stale stats_duck binding).
	r.layer_body =
	    R"("mark":"point","encoding":{"x":{"field":"x","type":"quantitative"},"y":{"field":"y","type":"quantitative"}},"description":"GGSQL_MARKS_DEMO_SENTINEL_v1")";
	r.data_sql = ctx.projected_sql;
	return r;
}

void RegisterDemoMark(ExtensionLoader &loader) {
	// Re-implement a minimal version of stats_duck's RegisterMark, so that
	// marks_demo has zero link-time dependency on stats_duck's symbols.
	ScalarFunction func("ggsql_mark_v1_demo", {}, LogicalType::VARCHAR,
	                    [](DataChunk &args, ExpressionState &state, Vector &result) {
		                    (void)args;
		                    (void)state;
		                    result.SetVectorType(VectorType::CONSTANT_VECTOR);
		                    auto data = ConstantVector::GetData<string_t>(result);
		                    data[0] = StringVector::AddString(
		                        result, "{\"name\":\"demo\",\"required_aesthetics\":[\"x\",\"y\"]}");
	                    });

	auto info = make_shared_ptr<ggsql::MarkInfo>();
	info->abi_version = GGSQL_MARK_ABI_VERSION;
	info->name = "demo";
	info->required_aesthetics = {"x", "y"};
	info->render = RenderDemo;
	func.SetExtraFunctionInfo(std::move(info));

	loader.RegisterFunction(std::move(func));
}

} // namespace

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(marks_demo, loader) {
	duckdb::RegisterDemoMark(loader);
}
}
