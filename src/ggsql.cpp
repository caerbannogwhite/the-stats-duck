#include "ggsql.hpp"

#include "ggsql_grammar.hpp"

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

// Hardcoded mark table. Phase 3 will replace this with a proper registry so
// external extensions (e.g., bio-stats) can plug in their own marks. Each
// entry returns the Vega-Lite layer fragment (without $schema or data keys)
// plus the SQL that computes that layer's data.
struct MarkResult {
	string layer_json_body; // the layer fragment keys AFTER the opening '{'
	string data_sql;        // the SQL that produces this layer's data
};

bool HasAesthetic(const VisualizeStatement &stmt, const char *name) {
	for (const auto &a : stmt.aesthetics) {
		if (StringUtil::CIEquals(a.aesthetic, name)) {
			return true;
		}
	}
	return false;
}

void RequireAesthetic(const VisualizeStatement &stmt, const string &mark, const char *aesthetic) {
	if (!HasAesthetic(stmt, aesthetic)) {
		throw InvalidInputException("ggsql: '%s' requires '%s' aesthetic", mark, aesthetic);
	}
}

MarkResult RenderMark(const string &mark, const VisualizeStatement &stmt,
                      const string &projected_sql) {
	if (StringUtil::CIEquals(mark, "point")) {
		RequireAesthetic(stmt, mark, "x");
		RequireAesthetic(stmt, mark, "y");
		MarkResult r;
		r.layer_json_body =
		    R"("mark":"point","encoding":{"x":{"field":"x","type":"quantitative"},"y":{"field":"y","type":"quantitative"}})";
		r.data_sql = projected_sql;
		return r;
	}
	if (StringUtil::CIEquals(mark, "line")) {
		RequireAesthetic(stmt, mark, "x");
		RequireAesthetic(stmt, mark, "y");
		MarkResult r;
		r.layer_json_body =
		    R"("mark":"line","encoding":{"x":{"field":"x","type":"quantitative"},"y":{"field":"y","type":"quantitative"}})";
		r.data_sql = "SELECT * FROM (" + projected_sql + ") ORDER BY x";
		return r;
	}
	if (StringUtil::CIEquals(mark, "bar")) {
		RequireAesthetic(stmt, mark, "x");
		RequireAesthetic(stmt, mark, "y");
		MarkResult r;
		r.layer_json_body =
		    R"("mark":"bar","encoding":{"x":{"field":"x","type":"ordinal"},"y":{"field":"y","type":"quantitative"}})";
		r.data_sql = "SELECT x, SUM(y) AS y FROM (" + projected_sql + ") GROUP BY x ORDER BY x";
		return r;
	}
	if (StringUtil::CIEquals(mark, "histogram")) {
		RequireAesthetic(stmt, mark, "x");
		MarkResult r;
		r.layer_json_body =
		    R"("mark":"bar","encoding":{"x":{"field":"x","type":"quantitative","bin":true},"y":{"aggregate":"count","type":"quantitative"}})";
		r.data_sql = projected_sql;
		return r;
	}
	throw InvalidInputException(
	    "ggsql: unknown mark '%s' (supported: point, line, bar, histogram)", mark);
}

struct CompiledResult {
	string spec_json;
	vector<pair<string, string>> layer_sqls;
};

CompiledResult Compile(const VisualizeStatement &stmt) {
	if (stmt.layers.size() != 1) {
		throw InvalidInputException("ggsql phase 1 supports exactly one DRAW clause (got %llu)",
		                            static_cast<unsigned long long>(stmt.layers.size()));
	}
	string projected_sql = BuildProjectedSql(stmt);
	const auto &layer = stmt.layers[0];
	string layer_name = "layer_0";
	auto rendered = RenderMark(layer.mark, stmt, projected_sql);

	string spec =
	    R"({"$schema":"https://vega.github.io/schema/vega-lite/v5.json","data":{"name":")";
	spec += layer_name;
	spec += R"("},)";
	spec += rendered.layer_json_body;
	spec += "}";

	CompiledResult out;
	out.spec_json = std::move(spec);
	out.layer_sqls.emplace_back(layer_name, std::move(rendered.data_sql));
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

ParserExtensionPlanResult GgsqlPlan(ParserExtensionInfo *, ClientContext &,
                                    unique_ptr<ParserExtensionParseData> parse_data) {
	auto &data = (GgsqlParseData &)*parse_data;
	auto compiled = Compile(data.stmt);

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
