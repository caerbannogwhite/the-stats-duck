#include "ggsql.hpp"

#include "ggsql_grammar.hpp"
#include "ggsql_marks_internal.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/insertion_order_preserving_map.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parser/parser_extension.hpp"

#include <algorithm>

namespace duckdb {
namespace ggsql {

namespace {

string BuildProjectedSql(const VisualizeStatement &stmt) {
	string sql;
	// Prepend any leading WITH clause so the FROM and aesthetic expressions
	// can resolve CTE-bound names. CTEs are scoped to the inner query when a
	// mark wraps the projection (e.g. `SELECT * FROM (<projected>) ORDER BY x`),
	// so passing the WITH through verbatim composes cleanly with line / bar /
	// area / errorband / regression wraps without any extra plumbing.
	if (!stmt.with_clause.empty()) {
		sql += stmt.with_clause + " ";
	}
	sql += "SELECT ";
	for (size_t i = 0; i < stmt.aesthetics.size(); i++) {
		if (i > 0) {
			sql += ", ";
		}
		sql += stmt.aesthetics[i].expression + " AS " + stmt.aesthetics[i].aesthetic;
	}
	sql += " FROM " + stmt.from_table;
	return sql;
}

bool HasAesthetic(const VisualizeStatement &stmt, const string &name) {
	for (const auto &a : stmt.aesthetics) {
		if (StringUtil::CIEquals(a.aesthetic, name)) {
			return true;
		}
	}
	return false;
}

struct CompiledResult {
	string spec_json;
	vector<pair<string, string>> layer_sqls;
};

// Apply the layer's STAT modifier to a freshly-rendered MarkResult.
//   identity / "" : no-op
//   smooth        : prepend a Vega-Lite loess transform on (x, y), grouped by
//                   color if mapped. Rejected if the mark already emitted its
//                   own `"transform":` block (regression / density / violin /
//                   histogram), since two transform keys would be ambiguous.
//   summary       : rewrite data_sql to `SELECT x [, color, facet, facet2],
//                   AVG(y) AS y FROM (projected) GROUP BY ... ORDER BY x`.
void ApplyStat(MarkResult &rendered, const VisualizeStatement &stmt, const DrawLayer &layer,
               const string &projected_sql) {
	if (layer.stat.empty() || layer.stat == "identity") {
		return;
	}
	if (layer.stat == "smooth") {
		if (rendered.layer_body.compare(0, 12, "\"transform\":") == 0) {
			throw InvalidInputException(
			    "ggsql: STAT smooth is not allowed on '%s' (mark already emits a transform)",
			    layer.mark);
		}
		string loess = "\"transform\":[{\"loess\":\"y\",\"on\":\"x\"";
		if (HasAesthetic(stmt, "color")) {
			loess += ",\"groupby\":[\"color\"]";
		}
		loess += "}],";
		rendered.layer_body = loess + rendered.layer_body;
		return;
	}
	if (layer.stat == "summary") {
		string group_by = "x";
		if (HasAesthetic(stmt, "color")) {
			group_by += ", color";
		}
		if (HasAesthetic(stmt, "facet")) {
			group_by += ", facet";
		}
		if (HasAesthetic(stmt, "facet2")) {
			group_by += ", facet2";
		}
		rendered.data_sql = "SELECT " + group_by + ", AVG(y) AS y FROM (" + projected_sql +
		                    ") GROUP BY " + group_by + " ORDER BY x";
		return;
	}
}

MarkResult RenderLayer(ClientContext &context, const VisualizeStatement &stmt,
                       const DrawLayer &layer, const string &projected_sql) {
	const auto &info = LookupMark(context, layer.mark);
	for (const auto &req : info.required_aesthetics) {
		if (!HasAesthetic(stmt, req)) {
			throw InvalidInputException("ggsql: '%s' requires '%s' aesthetic", layer.mark, req);
		}
	}
	MarkContext ctx{stmt.aesthetics, projected_sql};
	MarkResult rendered = info.render(ctx);
	ApplyStat(rendered, stmt, layer, projected_sql);
	return rendered;
}

bool HasFacet(const VisualizeStatement &stmt) {
	for (const auto &a : stmt.aesthetics) {
		if (StringUtil::CIEquals(a.aesthetic, "facet")) {
			return true;
		}
	}
	return false;
}

bool Has2DFacet(const VisualizeStatement &stmt) {
	for (const auto &a : stmt.aesthetics) {
		if (StringUtil::CIEquals(a.aesthetic, "facet2")) {
			return true;
		}
	}
	return false;
}

// Inject channel sub-objects (e.g. `,"scale":{...}` for TO/ZERO/DOMAIN,
// `,"axis":{...}` for LABEL) into each encoding channel that has at least one
// matching ScaleSpec. Relies on the current encoding format using only
// primitive sub-properties at this point (no nested objects), so the first '}'
// after the channel's opening '{' is its closing brace. Multiple ScaleSpec
// entries on the same (channel, sub_object) merge into one block; multiple
// sub_objects on the same channel each produce their own block.
string ApplyScales(string layer_body, const vector<ScaleSpec> &scales) {
	// channel → sub_object → "k1":v1,"k2":v2  (within-block insertion order preserved)
	std::map<string, std::map<string, string>> by_channel;
	std::vector<string> channel_order;
	std::map<string, std::vector<string>> sub_obj_order; // per-channel sub_object insertion order
	for (const auto &scale : scales) {
		auto &props = by_channel[scale.aesthetic][scale.sub_object];
		if (!props.empty()) {
			props += ",";
		}
		props += "\"" + scale.property + "\":" + scale.value_json;
		auto &ch_subs = sub_obj_order[scale.aesthetic];
		if (std::find(ch_subs.begin(), ch_subs.end(), scale.sub_object) == ch_subs.end()) {
			ch_subs.push_back(scale.sub_object);
		}
		if (std::find(channel_order.begin(), channel_order.end(), scale.aesthetic) ==
		    channel_order.end()) {
			channel_order.push_back(scale.aesthetic);
		}
	}
	for (const auto &channel : channel_order) {
		string needle = "\"" + channel + "\":{";
		size_t pos = layer_body.find(needle);
		if (pos == string::npos) {
			continue; // channel not mapped → silently ignore
		}
		size_t open_brace = pos + needle.size() - 1;
		size_t close_brace = layer_body.find('}', open_brace + 1);
		if (close_brace == string::npos) {
			continue;
		}
		string injection;
		for (const auto &sub_obj : sub_obj_order[channel]) {
			injection += ",\"" + sub_obj + "\":{" + by_channel[channel][sub_obj] + "}";
		}
		layer_body.insert(close_brace, injection);
	}
	return layer_body;
}

// Build the Vega-Lite title block. Returns "" if no title is set. The block is
// always emitted as a TitleParams object (not a bare string) so that subtitle
// can be added without changing the shape; this keeps fixture output stable.
string BuildTitleBlock(const VisualizeStatement &stmt) {
	if (stmt.title.empty()) {
		return "";
	}
	string out = ",\"title\":{\"text\":\"" + JsonEscape(stmt.title) + "\"";
	if (!stmt.subtitle.empty()) {
		out += ",\"subtitle\":\"" + JsonEscape(stmt.subtitle) + "\"";
	}
	out += "}";
	return out;
}

// Replace the rendered "type":"..." inside the targeted channel with the user's
// override. Same lookup pattern as ApplyScales — find the channel block, locate
// the type field within it, swap the value.
string ApplyTypeOverrides(string layer_body, const vector<TypeOverride> &overrides) {
	for (const auto &ov : overrides) {
		string needle = "\"" + ov.aesthetic + "\":{";
		size_t pos = layer_body.find(needle);
		if (pos == string::npos) {
			continue; // channel not mapped → silently ignore
		}
		size_t open_brace = pos + needle.size() - 1;
		size_t close_brace = layer_body.find('}', open_brace + 1);
		if (close_brace == string::npos) {
			continue;
		}
		const string type_key = "\"type\":\"";
		size_t type_pos = layer_body.find(type_key, open_brace);
		if (type_pos == string::npos || type_pos >= close_brace) {
			continue;
		}
		size_t value_start = type_pos + type_key.size();
		size_t value_end = layer_body.find('"', value_start);
		if (value_end == string::npos || value_end >= close_brace) {
			continue;
		}
		layer_body.replace(value_start, value_end - value_start, ov.type);
	}
	return layer_body;
}

CompiledResult Compile(ClientContext &context, const VisualizeStatement &stmt) {
	if (stmt.layers.empty()) {
		throw InvalidInputException("ggsql: at least one DRAW clause is required");
	}
	string projected_sql = BuildProjectedSql(stmt);
	bool faceted = HasFacet(stmt);
	bool faceted_2d = Has2DFacet(stmt);
	string facet_block;
	if (faceted_2d) {
		// 2D grid: row × column. Vega-Lite's facet operator with both `row` and
		// `column` sub-channels.
		facet_block = "\"facet\":{\"row\":{\"field\":\"facet\",\"type\":\"nominal\"},"
		              "\"column\":{\"field\":\"facet2\",\"type\":\"nominal\"}},\"spec\":";
	} else if (faceted) {
		if (stmt.facet_layout == "rows") {
			facet_block = "\"facet\":{\"row\":{\"field\":\"facet\",\"type\":\"nominal\"}},\"spec\":";
		} else if (stmt.facet_layout == "cols") {
			facet_block =
			    "\"facet\":{\"column\":{\"field\":\"facet\",\"type\":\"nominal\"}},\"spec\":";
		} else {
			facet_block = "\"facet\":{\"field\":\"facet\",\"type\":\"nominal\"},\"spec\":";
		}
	}
	CompiledResult out;

	string title_block = BuildTitleBlock(stmt);

	if (stmt.layers.size() == 1) {
		// Single layer: emit canonical Vega-Lite single-view shape (top-level
		// data + mark + encoding). When faceted, wrap mark/encoding inside a
		// `spec` sibling of `facet`.
		const string layer_name = "layer_0";
		MarkResult rendered = RenderLayer(context, stmt, stmt.layers[0], projected_sql);
		rendered.layer_body = ApplyTypeOverrides(std::move(rendered.layer_body), stmt.type_overrides);
		rendered.layer_body = ApplyScales(std::move(rendered.layer_body), stmt.scales);

		string spec =
		    R"({"$schema":"https://vega.github.io/schema/vega-lite/v5.json","data":{"name":")";
		spec += layer_name;
		spec += R"("},)";
		if (faceted) {
			spec += facet_block;
			spec += "{";
			spec += rendered.layer_body;
			spec += "}";
		} else {
			spec += rendered.layer_body;
		}
		spec += title_block;
		spec += "}";

		out.spec_json = std::move(spec);
		out.layer_sqls.emplace_back(layer_name, std::move(rendered.data_sql));
		return out;
	}

	// Multi-layer: emit Vega-Lite layer:[...] array. When faceted, wrap the
	// layer array in a `spec` sibling of `facet`.
	string layer_array = R"("layer":[)";
	for (size_t i = 0; i < stmt.layers.size(); i++) {
		if (i > 0) {
			layer_array += ",";
		}
		string layer_name = "layer_" + std::to_string(i);
		MarkResult rendered = RenderLayer(context, stmt, stmt.layers[i], projected_sql);
		rendered.layer_body = ApplyTypeOverrides(std::move(rendered.layer_body), stmt.type_overrides);
		rendered.layer_body = ApplyScales(std::move(rendered.layer_body), stmt.scales);
		layer_array += R"({"data":{"name":")";
		layer_array += layer_name;
		layer_array += R"("},)";
		layer_array += rendered.layer_body;
		layer_array += "}";
		out.layer_sqls.emplace_back(layer_name, std::move(rendered.data_sql));
	}
	layer_array += "]";

	string spec = R"({"$schema":"https://vega.github.io/schema/vega-lite/v5.json",)";
	if (faceted) {
		spec += facet_block;
		spec += "{";
		spec += layer_array;
		spec += "}";
	} else {
		spec += layer_array;
	}
	spec += title_block;
	spec += "}";
	out.spec_json = std::move(spec);
	return out;
}

Value BuildLayerSqlsMap(const vector<pair<string, string>> &pairs) {
	InsertionOrderPreservingMap<string> kv;
	for (const auto &p : pairs) {
		kv.insert(p.first, p.second);
	}
	return Value::MAP(kv);
}

//===--------------------------------------------------------------------===//
// Parser extension: parse_function -> plan_function -> result TableFunction
//===--------------------------------------------------------------------===//
struct GgsqlParseData : public ParserExtensionParseData {
	VisualizeStatement stmt;

	explicit GgsqlParseData(VisualizeStatement stmt_p) : stmt(std::move(stmt_p)) {
	}

	unique_ptr<ParserExtensionParseData> Copy() const override {
		return make_uniq<GgsqlParseData>(stmt);
	}
	string ToString() const override {
		return "ggsql statement";
	}
};

struct GgsqlResultBindData : public TableFunctionData {
	string spec;
	vector<pair<string, string>> layer_sqls;

	GgsqlResultBindData(string spec_p, vector<pair<string, string>> layer_sqls_p)
	    : spec(std::move(spec_p)), layer_sqls(std::move(layer_sqls_p)) {
	}
};

struct GgsqlResultState : public GlobalTableFunctionState {
	bool emitted = false;
};

unique_ptr<FunctionData> GgsqlResultBind(ClientContext &, TableFunctionBindInput &input,
                                         vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("spec");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("layer_sqls");
	return_types.emplace_back(LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR));

	string spec = input.inputs[0].GetValue<string>();

	vector<pair<string, string>> pairs;
	auto &map_value = input.inputs[1];
	auto &map_entries = ListValue::GetChildren(map_value);
	for (auto &entry : map_entries) {
		auto &kv = StructValue::GetChildren(entry);
		pairs.emplace_back(kv[0].GetValue<string>(), kv[1].GetValue<string>());
	}
	return make_uniq<GgsqlResultBindData>(std::move(spec), std::move(pairs));
}

unique_ptr<GlobalTableFunctionState> GgsqlResultInit(ClientContext &, TableFunctionInitInput &) {
	return make_uniq<GgsqlResultState>();
}

void GgsqlResultFunc(ClientContext &, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind = data_p.bind_data->Cast<GgsqlResultBindData>();
	auto &state = data_p.global_state->Cast<GgsqlResultState>();
	if (state.emitted) {
		output.SetCardinality(0);
		return;
	}
	output.SetValue(0, 0, Value(bind.spec));
	output.SetValue(1, 0, BuildLayerSqlsMap(bind.layer_sqls));
	output.SetCardinality(1);
	state.emitted = true;
}

bool StartsWithIdentifier(const string &lower, const string &keyword) {
	if (lower.size() < keyword.size()) {
		return false;
	}
	if (lower.compare(0, keyword.size(), keyword) != 0) {
		return false;
	}
	if (lower.size() == keyword.size()) {
		return true;
	}
	auto next = lower[keyword.size()];
	return next == ' ' || next == '\t' || next == '\n' || next == '\r' || next == ';';
}

// Strip leading whitespace and SQL comments (`-- to end of line`, `/* ... */`)
// from the front of `s`. DuckDB hands the parser extension the raw statement
// text including any comments, so we need to skip past them before deciding
// whether the statement is ours.
static void SkipLeadingCommentsAndWhitespace(string &s) {
	size_t i = 0;
	while (i < s.size()) {
		char c = s[i];
		if (c == ' ' || c == '\t' || c == '\n' || c == '\r') {
			i++;
			continue;
		}
		if (c == '-' && i + 1 < s.size() && s[i + 1] == '-') {
			i += 2;
			while (i < s.size() && s[i] != '\n') {
				i++;
			}
			continue;
		}
		if (c == '/' && i + 1 < s.size() && s[i + 1] == '*') {
			i += 2;
			while (i + 1 < s.size() && !(s[i] == '*' && s[i + 1] == '/')) {
				i++;
			}
			if (i + 1 < s.size()) {
				i += 2; // past `*/`
			}
			continue;
		}
		break;
	}
	if (i > 0) {
		s.erase(0, i);
	}
}

ParserExtensionParseResult GgsqlParse(ParserExtensionInfo *, const string &query) {
	string trimmed = query;
	SkipLeadingCommentsAndWhitespace(trimmed);
	StringUtil::Trim(trimmed); // trailing whitespace too
	auto lower = StringUtil::Lower(trimmed);
	if (StartsWithIdentifier(lower, "visualize")) {
		// Direct path — bare VISUALIZE statement.
	} else if (StartsWithIdentifier(lower, "with")) {
		// Could be ours (WITH … VISUALIZE …) or DuckDB's (WITH … SELECT …).
		// Only claim it if a top-level VISUALIZE keyword exists in the
		// input — case-insensitive, whole-word match, ignoring strings /
		// quoted identifiers / parenthesised expressions. The cheap
		// approach is a manual scan rather than a full tokenize: keep
		// track of paren depth and skip over string/quoted-ident content,
		// then look for `visualize` as a standalone token at depth 0.
		bool has_visualize = false;
		int depth = 0;
		char quote = 0;
		auto is_ident_part = [](char c) {
			return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
			       (c >= '0' && c <= '9') || c == '_';
		};
		for (size_t i = 0; i < trimmed.size(); i++) {
			char c = trimmed[i];
			if (quote != 0) {
				if (c == quote) {
					if (i + 1 < trimmed.size() && trimmed[i + 1] == quote) {
						i++; // SQL '' / "" escape
					} else {
						quote = 0;
					}
				}
				continue;
			}
			if (c == '\'' || c == '"') {
				quote = c;
				continue;
			}
			if (c == '(') {
				depth++;
				continue;
			}
			if (c == ')') {
				depth--;
				continue;
			}
			if (depth > 0) {
				continue;
			}
			// At depth 0: try to match the whole-word "visualize" case-insensitively.
			if ((c == 'v' || c == 'V') && i + 9 <= trimmed.size()) {
				static const char kKeyword[] = "visualize";
				bool match = true;
				for (size_t k = 0; k < 9; k++) {
					char a = trimmed[i + k];
					char b = kKeyword[k];
					char a_lower = (a >= 'A' && a <= 'Z') ? (a - 'A' + 'a') : a;
					if (a_lower != b) {
						match = false;
						break;
					}
				}
				if (match) {
					// Must be a standalone token — surrounded by non-identifier chars.
					bool left_ok = (i == 0) || !is_ident_part(trimmed[i - 1]);
					bool right_ok = (i + 9 == trimmed.size()) ||
					                !is_ident_part(trimmed[i + 9]);
					if (left_ok && right_ok) {
						has_visualize = true;
						break;
					}
				}
			}
		}
		if (!has_visualize) {
			return ParserExtensionParseResult(); // hand off to DuckDB
		}
	} else {
		return ParserExtensionParseResult();
	}
	auto parsed = ParseGgsql(trimmed);
	if (!parsed.success) {
		return ParserExtensionParseResult(parsed.error);
	}
	return ParserExtensionParseResult(make_uniq<GgsqlParseData>(std::move(parsed.stmt)));
}

ParserExtensionPlanResult GgsqlPlan(ParserExtensionInfo *, ClientContext &context,
                                    unique_ptr<ParserExtensionParseData> parse_data) {
	auto &data = (GgsqlParseData &)*parse_data;
	auto compiled = Compile(context, data.stmt);

	TableFunction tf;
	tf.name = "ggsql_result";
	tf.arguments.push_back(LogicalType::VARCHAR);
	tf.arguments.push_back(LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR));
	tf.bind = GgsqlResultBind;
	tf.init_global = GgsqlResultInit;
	tf.function = GgsqlResultFunc;

	ParserExtensionPlanResult result;
	result.function = std::move(tf);
	result.parameters.emplace_back(Value(compiled.spec_json));
	result.parameters.emplace_back(BuildLayerSqlsMap(compiled.layer_sqls));
	result.requires_valid_transaction = false;
	result.return_type = StatementReturnType::QUERY_RESULT;
	return result;
}

class GgsqlParserExtension : public ParserExtension {
public:
	GgsqlParserExtension() {
		parse_function = GgsqlParse;
		plan_function = GgsqlPlan;
	}
};

} // namespace

} // namespace ggsql

void RegisterGgsql(ExtensionLoader &loader) {
	auto &db = loader.GetDatabaseInstance();
	auto &config = DBConfig::GetConfig(db);
	ParserExtension::Register(config, ggsql::GgsqlParserExtension());
}

} // namespace duckdb
