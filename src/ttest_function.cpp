#include "ttest_function.hpp"
#include "ttest_compute.hpp"

#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb {

// ─── Bind data ──────────────────────────────────────────────────────────────

struct TTestBindData : public FunctionData {
	stats_duck::TTestResult result;

	unique_ptr<FunctionData> Copy() const override {
		auto copy = make_uniq<TTestBindData>();
		copy->result = result;
		return std::move(copy);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<TTestBindData>();
		return result.t_statistic == other.result.t_statistic && result.df == other.result.df &&
		       result.p_value == other.result.p_value;
	}
};

struct TTestGlobalState : public GlobalTableFunctionState {
	bool finished = false;
};

static unique_ptr<GlobalTableFunctionState> TTestInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<TTestGlobalState>();
}

// ─── Helpers ────────────────────────────────────────────────────────────────

static vector<double> ExtractDoubleList(const Value &val) {
	if (val.IsNull()) {
		throw InvalidInputException("t-test input list cannot be NULL");
	}
	auto &children = ListValue::GetChildren(val);
	vector<double> result;
	result.reserve(children.size());
	for (auto &child : children) {
		if (child.IsNull()) {
			continue; // skip NULLs (listwise deletion)
		}
		result.push_back(child.GetValue<double>());
	}
	return result;
}

static double GetNamedDouble(const named_parameter_map_t &params, const string &name, double default_val) {
	auto it = params.find(name);
	if (it == params.end()) {
		return default_val;
	}
	return it->second.GetValue<double>();
}

static string GetNamedString(const named_parameter_map_t &params, const string &name, const string &default_val) {
	auto it = params.find(name);
	if (it == params.end()) {
		return default_val;
	}
	return it->second.ToString();
}

static bool GetNamedBool(const named_parameter_map_t &params, const string &name, bool default_val) {
	auto it = params.find(name);
	if (it == params.end()) {
		return default_val;
	}
	return it->second.GetValue<bool>();
}

static void ValidateAlpha(double alpha) {
	if (alpha <= 0.0 || alpha >= 1.0) {
		throw InvalidInputException("alpha must be between 0 and 1 (exclusive), got %g", alpha);
	}
}

static void ValidateAlternative(const string &alt) {
	if (alt != "two-sided" && alt != "less" && alt != "greater") {
		throw InvalidInputException("alternative must be 'two-sided', 'less', or 'greater', got '%s'", alt);
	}
}

// ─── Output schema ──────────────────────────────────────────────────────────

static void SetTTestOutputSchema(vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("test_type");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("t_statistic");
	return_types.emplace_back(LogicalType::DOUBLE);

	names.emplace_back("df");
	return_types.emplace_back(LogicalType::DOUBLE);

	names.emplace_back("p_value");
	return_types.emplace_back(LogicalType::DOUBLE);

	names.emplace_back("alternative");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("mean_diff");
	return_types.emplace_back(LogicalType::DOUBLE);

	names.emplace_back("ci_lower");
	return_types.emplace_back(LogicalType::DOUBLE);

	names.emplace_back("ci_upper");
	return_types.emplace_back(LogicalType::DOUBLE);

	names.emplace_back("cohens_d");
	return_types.emplace_back(LogicalType::DOUBLE);
}

// ─── Execute (shared) ───────────────────────────────────────────────────────

static void TTestExecute(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<TTestGlobalState>();
	if (state.finished) {
		return;
	}
	state.finished = true;

	auto &r = data.bind_data->Cast<TTestBindData>().result;
	output.SetCardinality(1);
	output.SetValue(0, 0, Value(r.test_type));
	output.SetValue(1, 0, Value(r.t_statistic));
	output.SetValue(2, 0, Value(r.df));
	output.SetValue(3, 0, Value(r.p_value));
	output.SetValue(4, 0, Value(r.alternative));
	output.SetValue(5, 0, Value(r.mean_diff));
	output.SetValue(6, 0, Value(r.ci_lower));
	output.SetValue(7, 0, Value(r.ci_upper));
	output.SetValue(8, 0, Value(r.cohens_d));
}

// ─── One-sample t-test ──────────────────────────────────────────────────────

static unique_ptr<FunctionData> TTest1SampBind(ClientContext &context, TableFunctionBindInput &input,
                                               vector<LogicalType> &return_types, vector<string> &names) {
	SetTTestOutputSchema(return_types, names);

	auto sample = ExtractDoubleList(input.inputs[0]);
	double mu = GetNamedDouble(input.named_parameters, "mu", 0.0);
	double alpha = GetNamedDouble(input.named_parameters, "alpha", 0.05);
	string alternative = GetNamedString(input.named_parameters, "alternative", "two-sided");

	ValidateAlpha(alpha);
	ValidateAlternative(alternative);

	auto bind_data = make_uniq<TTestBindData>();
	bind_data->result = stats_duck::ComputeTTest1Samp(sample, mu, alpha, alternative);
	return std::move(bind_data);
}

void RegisterTTest1Samp(ExtensionLoader &loader) {
	TableFunction func("ttest_1samp", {LogicalType::LIST(LogicalType::DOUBLE)}, TTestExecute, TTest1SampBind,
	                   TTestInitGlobal);
	func.named_parameters["mu"] = LogicalType::DOUBLE;
	func.named_parameters["alpha"] = LogicalType::DOUBLE;
	func.named_parameters["alternative"] = LogicalType::VARCHAR;
	loader.RegisterFunction(func);
}

// ─── Two-sample t-test ──────────────────────────────────────────────────────

static unique_ptr<FunctionData> TTest2SampBind(ClientContext &context, TableFunctionBindInput &input,
                                               vector<LogicalType> &return_types, vector<string> &names) {
	SetTTestOutputSchema(return_types, names);

	auto sample1 = ExtractDoubleList(input.inputs[0]);
	auto sample2 = ExtractDoubleList(input.inputs[1]);
	bool equal_var = GetNamedBool(input.named_parameters, "equal_var", false);
	double alpha = GetNamedDouble(input.named_parameters, "alpha", 0.05);
	string alternative = GetNamedString(input.named_parameters, "alternative", "two-sided");

	ValidateAlpha(alpha);
	ValidateAlternative(alternative);

	auto bind_data = make_uniq<TTestBindData>();
	bind_data->result = stats_duck::ComputeTTest2Samp(sample1, sample2, equal_var, alpha, alternative);
	return std::move(bind_data);
}

void RegisterTTest2Samp(ExtensionLoader &loader) {
	TableFunction func("ttest_2samp", {LogicalType::LIST(LogicalType::DOUBLE), LogicalType::LIST(LogicalType::DOUBLE)},
	                   TTestExecute, TTest2SampBind, TTestInitGlobal);
	func.named_parameters["equal_var"] = LogicalType::BOOLEAN;
	func.named_parameters["alpha"] = LogicalType::DOUBLE;
	func.named_parameters["alternative"] = LogicalType::VARCHAR;
	loader.RegisterFunction(func);
}

// ─── Paired t-test ──────────────────────────────────────────────────────────

static unique_ptr<FunctionData> TTestPairedBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names) {
	SetTTestOutputSchema(return_types, names);

	auto sample1 = ExtractDoubleList(input.inputs[0]);
	auto sample2 = ExtractDoubleList(input.inputs[1]);
	double alpha = GetNamedDouble(input.named_parameters, "alpha", 0.05);
	string alternative = GetNamedString(input.named_parameters, "alternative", "two-sided");

	ValidateAlpha(alpha);
	ValidateAlternative(alternative);

	auto bind_data = make_uniq<TTestBindData>();
	bind_data->result = stats_duck::ComputeTTestPaired(sample1, sample2, alpha, alternative);
	return std::move(bind_data);
}

void RegisterTTestPaired(ExtensionLoader &loader) {
	TableFunction func("ttest_paired",
	                   {LogicalType::LIST(LogicalType::DOUBLE), LogicalType::LIST(LogicalType::DOUBLE)}, TTestExecute,
	                   TTestPairedBind, TTestInitGlobal);
	func.named_parameters["alpha"] = LogicalType::DOUBLE;
	func.named_parameters["alternative"] = LogicalType::VARCHAR;
	loader.RegisterFunction(func);
}

} // namespace duckdb
