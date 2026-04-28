#include "ggsql_marks_internal.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

namespace duckdb {
namespace ggsql {

namespace {

string CatalogName(const string &mark_name) {
	return GGSQL_MARK_PREFIX + StringUtil::Lower(mark_name);
}

string BuildIntrospectionString(const string &name, const vector<string> &required_aesthetics) {
	string out = "{\"name\":\"" + name + "\",\"required_aesthetics\":[";
	for (size_t i = 0; i < required_aesthetics.size(); i++) {
		if (i > 0) {
			out += ",";
		}
		out += "\"" + required_aesthetics[i] + "\"";
	}
	out += "]}";
	return out;
}

void MarkIntrospectionStub(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	auto &info = func_expr.function.GetExtraFunctionInfo().Cast<MarkInfo>();
	auto desc = BuildIntrospectionString(info.name, info.required_aesthetics);
	result.SetVectorType(VectorType::CONSTANT_VECTOR);
	auto data = ConstantVector::GetData<string_t>(result);
	data[0] = StringVector::AddString(result, desc);
	(void)args;
}

//===--------------------------------------------------------------------===//
// Built-in mark renderers (ported verbatim from phase 2 RenderMark).
//===--------------------------------------------------------------------===//

MarkResult RenderPoint(const MarkContext &ctx) {
	MarkResult r;
	r.layer_body =
	    R"("mark":"point","encoding":{"x":{"field":"x","type":"quantitative"},"y":{"field":"y","type":"quantitative"}})";
	r.data_sql = ctx.projected_sql;
	return r;
}

MarkResult RenderLine(const MarkContext &ctx) {
	MarkResult r;
	r.layer_body =
	    R"("mark":"line","encoding":{"x":{"field":"x","type":"quantitative"},"y":{"field":"y","type":"quantitative"}})";
	r.data_sql = "SELECT * FROM (" + ctx.projected_sql + ") ORDER BY x";
	return r;
}

MarkResult RenderBar(const MarkContext &ctx) {
	MarkResult r;
	r.layer_body =
	    R"("mark":"bar","encoding":{"x":{"field":"x","type":"ordinal"},"y":{"field":"y","type":"quantitative"}})";
	r.data_sql = "SELECT x, SUM(y) AS y FROM (" + ctx.projected_sql + ") GROUP BY x ORDER BY x";
	return r;
}

MarkResult RenderHistogram(const MarkContext &ctx) {
	MarkResult r;
	r.layer_body =
	    R"("mark":"bar","encoding":{"x":{"field":"x","type":"quantitative","bin":true},"y":{"aggregate":"count","type":"quantitative"}})";
	r.data_sql = ctx.projected_sql;
	return r;
}

} // namespace

void RegisterMark(ExtensionLoader &loader, const string &name,
                  vector<string> required_aesthetics, mark_render_t render) {
	ScalarFunction func(CatalogName(name), {}, LogicalType::VARCHAR, MarkIntrospectionStub);

	auto info = make_shared_ptr<MarkInfo>();
	info->abi_version = GGSQL_MARK_ABI_VERSION;
	info->name = name;
	info->required_aesthetics = std::move(required_aesthetics);
	info->render = render;
	func.SetExtraFunctionInfo(std::move(info));

	loader.RegisterFunction(std::move(func));
}

const MarkInfo &LookupMark(ClientContext &context, const string &name) {
	auto &catalog = Catalog::GetSystemCatalog(context);
	auto entry = catalog.GetEntry(context, CatalogType::SCALAR_FUNCTION_ENTRY, DEFAULT_SCHEMA,
	                              CatalogName(name), OnEntryNotFound::RETURN_NULL);
	if (!entry) {
		throw InvalidInputException("ggsql: unknown mark '%s'", name);
	}
	auto &func_entry = entry->Cast<ScalarFunctionCatalogEntry>();
	if (func_entry.functions.Size() == 0) {
		throw InvalidInputException("ggsql: unknown mark '%s'", name);
	}
	auto &func = func_entry.functions.GetFunctionReferenceByOffset(0);
	if (!func.HasExtraFunctionInfo()) {
		throw InvalidInputException("ggsql: '%s' is not a registered mark", name);
	}
	auto &info = func.GetExtraFunctionInfo().Cast<MarkInfo>();
	if (info.abi_version != GGSQL_MARK_ABI_VERSION) {
		throw InvalidInputException(
		    "ggsql: mark '%s' uses ABI version %u but this build expects %u", name,
		    info.abi_version, GGSQL_MARK_ABI_VERSION);
	}
	return info;
}

void RegisterBuiltinMarks(ExtensionLoader &loader) {
	RegisterMark(loader, "point", {"x", "y"}, RenderPoint);
	RegisterMark(loader, "line", {"x", "y"}, RenderLine);
	RegisterMark(loader, "bar", {"x", "y"}, RenderBar);
	RegisterMark(loader, "histogram", {"x"}, RenderHistogram);
}

} // namespace ggsql
} // namespace duckdb
