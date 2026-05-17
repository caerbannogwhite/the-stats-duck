#include "ggsql_marks_internal.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

#include <map>

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
// Encoding helpers (Phase 4b).
//===--------------------------------------------------------------------===//

struct ChannelDefault {
	const char *name;
	const char *vega_type;
};

// Canonical encoding-channel order used for byte-stable output. New channels
// must be appended (not inserted) to keep fixtures stable.
static const vector<ChannelDefault> kStandardChannels = {
    {"x", "quantitative"},       {"y", "quantitative"},     {"color", "nominal"},
    {"fill", "nominal"},         {"stroke", "nominal"},     {"shape", "nominal"},
    {"size", "quantitative"},    {"opacity", "quantitative"}, {"tooltip", "nominal"},
    {"text", "nominal"},         {"x2", "quantitative"},    {"y2", "quantitative"},
};

bool HasAesthetic(const MarkContext &ctx, const string &channel) {
	for (const auto &a : ctx.aesthetics) {
		if (StringUtil::CIEquals(a.aesthetic, channel)) {
			return true;
		}
	}
	return false;
}

// Returns "" if the user did not map this aesthetic, otherwise:
//   "<channel>":{"field":"<channel>","type":"<vega_type>"<extras>}
// `extras` is appended verbatim (must already start with "," when non-empty).
string BuildChannel(const MarkContext &ctx, const string &channel,
                    const string &vega_type, const string &extras) {
	if (!HasAesthetic(ctx, channel)) {
		return "";
	}
	string out = "\"" + channel + "\":{\"field\":\"" + channel + "\",\"type\":\"" + vega_type + "\"";
	out += extras;
	out += "}";
	return out;
}

// Builds a complete "{...}" encoding object from a channel-default table,
// applying per-mark overrides for type and for inserted extras.
string BuildEncoding(const MarkContext &ctx, const vector<ChannelDefault> &channel_table,
                     const std::map<string, string> &type_overrides = {},
                     const std::map<string, string> &extras_overrides = {}) {
	string out = "{";
	bool first = true;
	for (const auto &ch : channel_table) {
		string vega_type = ch.vega_type;
		auto type_it = type_overrides.find(ch.name);
		if (type_it != type_overrides.end()) {
			vega_type = type_it->second;
		}
		string extras;
		auto extras_it = extras_overrides.find(ch.name);
		if (extras_it != extras_overrides.end()) {
			extras = extras_it->second;
		}
		string fragment = BuildChannel(ctx, ch.name, vega_type, extras);
		if (fragment.empty()) {
			continue;
		}
		if (!first) {
			out += ",";
		}
		out += fragment;
		first = false;
	}
	out += "}";
	return out;
}

//===--------------------------------------------------------------------===//
// Built-in mark renderers.
//===--------------------------------------------------------------------===//

MarkResult RenderPoint(const MarkContext &ctx) {
	MarkResult r;
	r.layer_body = "\"mark\":\"point\",\"encoding\":" + BuildEncoding(ctx, kStandardChannels);
	r.data_sql = ctx.projected_sql;
	return r;
}

MarkResult RenderLine(const MarkContext &ctx) {
	MarkResult r;
	r.layer_body = "\"mark\":\"line\",\"encoding\":" + BuildEncoding(ctx, kStandardChannels);
	r.data_sql = "SELECT * FROM (" + ctx.projected_sql + ") ORDER BY x";
	return r;
}

MarkResult RenderBar(const MarkContext &ctx) {
	// When faceting is active, the `facet` column rides through projected_sql
	// and must be carried through bar's GROUP BY so each facet partition keeps
	// its own ordinal x bins.
	string group_by = HasAesthetic(ctx, "facet") ? string("x, facet") : string("x");
	MarkResult r;
	r.layer_body =
	    "\"mark\":\"bar\",\"encoding\":" + BuildEncoding(ctx, kStandardChannels, {{"x", "ordinal"}});
	r.data_sql = "SELECT " + group_by + ", SUM(y) AS y FROM (" + ctx.projected_sql + ") GROUP BY " +
	             group_by + " ORDER BY x";
	return r;
}

MarkResult RenderText(const MarkContext &ctx) {
	MarkResult r;
	r.layer_body = "\"mark\":\"text\",\"encoding\":" + BuildEncoding(ctx, kStandardChannels);
	r.data_sql = ctx.projected_sql;
	return r;
}

MarkResult RenderArea(const MarkContext &ctx) {
	MarkResult r;
	r.layer_body = "\"mark\":\"area\",\"encoding\":" + BuildEncoding(ctx, kStandardChannels);
	r.data_sql = "SELECT * FROM (" + ctx.projected_sql + ") ORDER BY x";
	return r;
}

MarkResult RenderRule(const MarkContext &ctx) {
	MarkResult r;
	r.layer_body = "\"mark\":\"rule\",\"encoding\":" + BuildEncoding(ctx, kStandardChannels);
	r.data_sql = ctx.projected_sql;
	return r;
}

MarkResult RenderTick(const MarkContext &ctx) {
	MarkResult r;
	r.layer_body = "\"mark\":\"tick\",\"encoding\":" + BuildEncoding(ctx, kStandardChannels);
	r.data_sql = ctx.projected_sql;
	return r;
}

MarkResult RenderErrorbar(const MarkContext &ctx) {
	MarkResult r;
	r.layer_body = "\"mark\":\"errorbar\",\"encoding\":" + BuildEncoding(ctx, kStandardChannels);
	r.data_sql = ctx.projected_sql;
	return r;
}

MarkResult RenderErrorband(const MarkContext &ctx) {
	MarkResult r;
	r.layer_body = "\"mark\":\"errorband\",\"encoding\":" + BuildEncoding(ctx, kStandardChannels);
	r.data_sql = "SELECT * FROM (" + ctx.projected_sql + ") ORDER BY x";
	return r;
}

MarkResult RenderBoxplot(const MarkContext &ctx) {
	MarkResult r;
	r.layer_body = "\"mark\":\"boxplot\",\"encoding\":" + BuildEncoding(ctx, kStandardChannels);
	r.data_sql = ctx.projected_sql;
	return r;
}

MarkResult RenderHeatmap(const MarkContext &ctx) {
	// Rect mark with categorical-by-default x/y and quantitative color. Both
	// axes override to ordinal because the typical heatmap (correlation matrix,
	// contingency table, factor-by-factor counts) uses discrete bins; users
	// who want a continuous-binned heatmap can type-override the channel back.
	MarkResult r;
	r.layer_body = "\"mark\":\"rect\",\"encoding\":" +
	               BuildEncoding(ctx, kStandardChannels,
	                             {{"x", "ordinal"}, {"y", "ordinal"}, {"color", "quantitative"}});
	r.data_sql = ctx.projected_sql;
	return r;
}

// Build a "groupby":["color"] fragment iff the user mapped the color aesthetic.
// Used by density / regression transforms so multi-curve overlays work without
// an extra clause.
string BuildGroupbyExtra(const MarkContext &ctx) {
	if (HasAesthetic(ctx, "color")) {
		return ",\"groupby\":[\"color\"]";
	}
	return "";
}

// Build optional-channel fragments (color, opacity, ...) for marks that splice
// their own x/y encoding (histogram / density / regression). Returns "" if no
// optional channels are mapped; otherwise the leading "," is included so the
// caller can concatenate directly.
string BuildOptionalChannels(const MarkContext &ctx) {
	string out;
	for (const auto &ch : kStandardChannels) {
		string name = ch.name;
		if (name == "x" || name == "y") {
			continue;
		}
		string fragment = BuildChannel(ctx, name, ch.vega_type, "");
		if (!fragment.empty()) {
			out += "," + fragment;
		}
	}
	return out;
}

MarkResult RenderDensity(const MarkContext &ctx) {
	// Vega-Lite density transform consumes the underlying field `x` and emits
	// fields `value` (x-axis) and `density` (y-axis). When the user mapped a
	// color aesthetic, group by it to produce one density curve per level.
	string transform = "\"transform\":[{\"density\":\"x\"" + BuildGroupbyExtra(ctx) + "}]";
	string encoding = "{\"x\":{\"field\":\"value\",\"type\":\"quantitative\"},"
	                  "\"y\":{\"field\":\"density\",\"type\":\"quantitative\"}";
	encoding += BuildOptionalChannels(ctx);
	encoding += "}";
	MarkResult r;
	r.layer_body = transform + ",\"mark\":\"area\",\"encoding\":" + encoding;
	r.data_sql = ctx.projected_sql;
	return r;
}

MarkResult RenderRegression(const MarkContext &ctx) {
	// Vega-Lite regression transform fits y ~ x and emits the same field names
	// back as the smoothed line. Grouped by color when mapped so each category
	// gets its own fit line.
	string transform =
	    "\"transform\":[{\"regression\":\"y\",\"on\":\"x\"" + BuildGroupbyExtra(ctx) + "}]";
	string encoding = "{\"x\":{\"field\":\"x\",\"type\":\"quantitative\"},"
	                  "\"y\":{\"field\":\"y\",\"type\":\"quantitative\"}";
	encoding += BuildOptionalChannels(ctx);
	encoding += "}";
	MarkResult r;
	r.layer_body = transform + ",\"mark\":\"line\",\"encoding\":" + encoding;
	r.data_sql = ctx.projected_sql;
	return r;
}

MarkResult RenderHistogram(const MarkContext &ctx) {
	// Histogram's x is binned and y is computed (aggregate:count, no field).
	// Optional channels (color, opacity, ...) come from kStandardChannels but x
	// and y are spliced manually.
	string encoding = "{\"x\":{\"field\":\"x\",\"type\":\"quantitative\",\"bin\":true},"
	                  "\"y\":{\"aggregate\":\"count\",\"type\":\"quantitative\"}";
	for (const auto &ch : kStandardChannels) {
		string name = ch.name;
		if (name == "x" || name == "y") {
			continue;
		}
		string fragment = BuildChannel(ctx, name, ch.vega_type, "");
		if (!fragment.empty()) {
			encoding += "," + fragment;
		}
	}
	encoding += "}";
	MarkResult r;
	r.layer_body = "\"mark\":\"bar\",\"encoding\":" + encoding;
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
	RegisterMark(loader, "text", {"x", "y", "text"}, RenderText);
	RegisterMark(loader, "area", {"x", "y"}, RenderArea);
	// `rule` and `tick` accept any subset of x/y so a single mark serves the
	// vline / hline / segment / rug-plot use cases. Vega-Lite renders sensibly
	// with whatever the user maps; aesthetic validation stays minimal.
	RegisterMark(loader, "rule", {}, RenderRule);
	RegisterMark(loader, "tick", {}, RenderTick);
	// Stats marks: errorbar/errorband consume the y2 channel for the upper
	// bound; boxplot's quartile/outlier computation is server-side via
	// Vega-Lite once x and y are mapped.
	RegisterMark(loader, "errorbar", {"x", "y"}, RenderErrorbar);
	RegisterMark(loader, "errorband", {"x", "y"}, RenderErrorband);
	RegisterMark(loader, "boxplot", {"x", "y"}, RenderBoxplot);
	// Heatmap (categorical-axis rect), density (KDE area), and regression
	// (linear-fit line). Density and regression rely on Vega-Lite transforms,
	// so they ignore the underlying row order — safe to emit projected_sql
	// directly. Color is the grouping aesthetic for both transforms.
	RegisterMark(loader, "heatmap", {"x", "y", "color"}, RenderHeatmap);
	RegisterMark(loader, "density", {"x"}, RenderDensity);
	RegisterMark(loader, "regression", {"x", "y"}, RenderRegression);
}

} // namespace ggsql
} // namespace duckdb
