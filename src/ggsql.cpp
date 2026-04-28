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

namespace duckdb {
namespace ggsql {

namespace {

string BuildProjectedSql(const VisualizeStatement &stmt) {
	string sql = "SELECT ";
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

MarkResult RenderLayer(ClientContext &context, const VisualizeStatement &stmt,
                       const DrawLayer &layer, const string &projected_sql) {
	const auto &info = LookupMark(context, layer.mark);
	for (const auto &req : info.required_aesthetics) {
		if (!HasAesthetic(stmt, req)) {
			throw InvalidInputException("ggsql: '%s' requires '%s' aesthetic", layer.mark, req);
		}
	}
	MarkContext ctx{stmt.aesthetics, projected_sql};
	return info.render(ctx);
}

bool HasFacet(const VisualizeStatement &stmt) {
	for (const auto &a : stmt.aesthetics) {
		if (StringUtil::CIEquals(a.aesthetic, "facet")) {
			return true;
		}
	}
	return false;
}

// Inject `,"scale":{<merged-properties>}` into each encoding channel that has
// at least one matching ScaleSpec. Relies on the current encoding format using
// only primitive sub-properties (no nested objects), so the first '}' after the
// channel's opening '{' is the closing brace. Multiple SCALE clauses on the
// same channel merge into one scale object.
string ApplyScales(string layer_body, const vector<ScaleSpec> &scales) {
	// Preserve user-specified order while grouping by channel.
	std::map<string, string> merged; // channel → comma-separated "key":value list
	std::vector<string> order;
	for (const auto &scale : scales) {
		auto it = merged.find(scale.aesthetic);
		if (it == merged.end()) {
			merged[scale.aesthetic] = "\"" + scale.property + "\":" + scale.value_json;
			order.push_back(scale.aesthetic);
		} else {
			it->second += ",\"" + scale.property + "\":" + scale.value_json;
		}
	}
	for (const auto &channel : order) {
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
		string injection = ",\"scale\":{" + merged[channel] + "}";
		layer_body.insert(close_brace, injection);
	}
	return layer_body;
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
	string facet_block;
	if (faceted) {
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

ParserExtensionParseResult GgsqlParse(ParserExtensionInfo *, const string &query) {
	string trimmed = query;
	StringUtil::Trim(trimmed);
	auto lower = StringUtil::Lower(trimmed);
	if (!StartsWithIdentifier(lower, "visualize")) {
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
