#include "ggsql_parser_extension.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parser/parser_extension.hpp"

namespace duckdb {

struct GgsqlParseData : public ParserExtensionParseData {
	explicit GgsqlParseData(string command_p) : command(std::move(command_p)) {
	}

	string command;

	unique_ptr<ParserExtensionParseData> Copy() const override {
		return make_uniq<GgsqlParseData>(command);
	}
	string ToString() const override {
		return "GGSQL " + command;
	}
};

struct GgsqlSpikeBindData : public TableFunctionData {
	explicit GgsqlSpikeBindData(string reply_p) : reply(std::move(reply_p)) {
	}

	string reply;
};

struct GgsqlSpikeGlobalState : public GlobalTableFunctionState {
	GgsqlSpikeGlobalState() : emitted(false) {
	}

	bool emitted;
};

static unique_ptr<FunctionData> GgsqlSpikeBind(ClientContext &, TableFunctionBindInput &input,
                                               vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("result");
	return_types.emplace_back(LogicalType::VARCHAR);
	auto reply = input.inputs.empty() ? string("pong") : input.inputs[0].GetValue<string>();
	return make_uniq<GgsqlSpikeBindData>(std::move(reply));
}

static unique_ptr<GlobalTableFunctionState> GgsqlSpikeInit(ClientContext &, TableFunctionInitInput &) {
	return make_uniq<GgsqlSpikeGlobalState>();
}

static void GgsqlSpikeFunc(ClientContext &, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<GgsqlSpikeBindData>();
	auto &state = data_p.global_state->Cast<GgsqlSpikeGlobalState>();
	if (state.emitted) {
		output.SetCardinality(0);
		return;
	}
	output.SetValue(0, 0, Value(bind_data.reply));
	output.SetCardinality(1);
	state.emitted = true;
}

static bool StartsWithIdentifier(const string &lower, const string &keyword) {
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

static ParserExtensionParseResult GgsqlParse(ParserExtensionInfo *, const string &query) {
	string trimmed = query;
	StringUtil::Trim(trimmed);
	while (!trimmed.empty() && trimmed.back() == ';') {
		trimmed.pop_back();
		StringUtil::Trim(trimmed);
	}
	auto lower = StringUtil::Lower(trimmed);
	if (!StartsWithIdentifier(lower, "ggsql")) {
		return ParserExtensionParseResult();
	}
	auto rest = trimmed.substr(5);
	StringUtil::Trim(rest);
	if (rest.empty()) {
		return ParserExtensionParseResult("GGSQL requires a command (e.g. GGSQL PING)");
	}
	auto rest_lower = StringUtil::Lower(rest);
	if (rest_lower != "ping") {
		return ParserExtensionParseResult("Unknown GGSQL command: " + rest);
	}
	return ParserExtensionParseResult(make_uniq<GgsqlParseData>("ping"));
}

static ParserExtensionPlanResult GgsqlPlan(ParserExtensionInfo *, ClientContext &,
                                           unique_ptr<ParserExtensionParseData> parse_data) {
	auto &data = (GgsqlParseData &)*parse_data;
	string reply = data.command == "ping" ? "pong" : data.command;

	TableFunction ggsql_spike;
	ggsql_spike.name = "ggsql_spike";
	ggsql_spike.arguments.push_back(LogicalType::VARCHAR);
	ggsql_spike.bind = GgsqlSpikeBind;
	ggsql_spike.init_global = GgsqlSpikeInit;
	ggsql_spike.function = GgsqlSpikeFunc;

	ParserExtensionPlanResult result;
	result.function = std::move(ggsql_spike);
	result.parameters.emplace_back(Value(reply));
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

void RegisterGgsqlParserExtension(ExtensionLoader &loader) {
	auto &db = loader.GetDatabaseInstance();
	auto &config = DBConfig::GetConfig(db);
	ParserExtension::Register(config, GgsqlParserExtension());
}

} // namespace duckdb
