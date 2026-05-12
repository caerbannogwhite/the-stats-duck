#include "sign_test_function.hpp"
#include "distributions.hpp"
#include "stats_validation.hpp"

#include "duckdb/function/aggregate_function.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/planner/expression.hpp"

#include <algorithm>
#include <cmath>

namespace duckdb {

namespace {

// ── Bind data ──────────────────────────────────────────────────────────────

struct SignTestBindData : public FunctionData {
	double mu = 0.0; // Reference value for 1-sample; unused (and 0) for paired.
	std::string alternative = "two-sided";

	unique_ptr<FunctionData> Copy() const override {
		auto copy = make_uniq<SignTestBindData>();
		copy->mu = mu;
		copy->alternative = alternative;
		return std::move(copy);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<SignTestBindData>();
		return mu == other.mu && alternative == other.alternative;
	}
};

static double ExtractConstantDouble(ClientContext &context, AggregateFunction &function,
                                    vector<unique_ptr<Expression>> &arguments, idx_t arg_idx,
                                    const char *fn_name) {
	if (!arguments[arg_idx]->IsFoldable()) {
		throw BinderException("%s: parameter must be a constant value", fn_name);
	}
	Value val = ExpressionExecutor::EvaluateScalar(context, *arguments[arg_idx]);
	double result = val.GetValue<double>();
	Function::EraseArgument(function, arguments, arg_idx);
	return result;
}

static std::string ExtractConstantString(ClientContext &context, AggregateFunction &function,
                                         vector<unique_ptr<Expression>> &arguments, idx_t arg_idx,
                                         const char *fn_name) {
	if (!arguments[arg_idx]->IsFoldable()) {
		throw BinderException("%s: parameter must be a constant value", fn_name);
	}
	Value val = ExpressionExecutor::EvaluateScalar(context, *arguments[arg_idx]);
	std::string result = val.GetValue<std::string>();
	Function::EraseArgument(function, arguments, arg_idx);
	return result;
}

// 1-sample binders
static unique_ptr<FunctionData> Bind1Samp1(ClientContext &, AggregateFunction &,
                                           vector<unique_ptr<Expression>> &) {
	return make_uniq<SignTestBindData>();
}

static unique_ptr<FunctionData> Bind1Samp2(ClientContext &context, AggregateFunction &function,
                                           vector<unique_ptr<Expression>> &arguments) {
	auto bd = make_uniq<SignTestBindData>();
	bd->mu = ExtractConstantDouble(context, function, arguments, 1, "sign_test_1samp");
	return std::move(bd);
}

static unique_ptr<FunctionData> Bind1Samp3(ClientContext &context, AggregateFunction &function,
                                           vector<unique_ptr<Expression>> &arguments) {
	auto bd = make_uniq<SignTestBindData>();
	bd->alternative = ExtractConstantString(context, function, arguments, 2, "sign_test_1samp");
	sdv::ValidateAlternative(bd->alternative, "sign_test_1samp");
	bd->mu = ExtractConstantDouble(context, function, arguments, 1, "sign_test_1samp");
	return std::move(bd);
}

// paired binders
static unique_ptr<FunctionData> BindPaired2(ClientContext &, AggregateFunction &,
                                            vector<unique_ptr<Expression>> &) {
	return make_uniq<SignTestBindData>();
}

static unique_ptr<FunctionData> BindPaired3(ClientContext &context, AggregateFunction &function,
                                            vector<unique_ptr<Expression>> &arguments) {
	auto bd = make_uniq<SignTestBindData>();
	bd->alternative = ExtractConstantString(context, function, arguments, 2, "sign_test_paired");
	sdv::ValidateAlternative(bd->alternative, "sign_test_paired");
	return std::move(bd);
}

// ── Result struct ──────────────────────────────────────────────────────────

static LogicalType SignTestResultType() {
	child_list_t<LogicalType> children;
	children.emplace_back("test_type", LogicalType::VARCHAR);
	children.emplace_back("m_statistic", LogicalType::DOUBLE);
	children.emplace_back("n_pos", LogicalType::BIGINT);
	children.emplace_back("n_neg", LogicalType::BIGINT);
	children.emplace_back("n_zero", LogicalType::BIGINT);
	children.emplace_back("p_value", LogicalType::DOUBLE);
	children.emplace_back("alternative", LogicalType::VARCHAR);
	children.emplace_back("n", LogicalType::BIGINT);
	return LogicalType::STRUCT(std::move(children));
}

// ── Aggregate state (POD: just three counters) ────────────────────────────

struct SignTestState {
	int64_t n_pos;
	int64_t n_neg;
	int64_t n_zero;
};

static void SignTestInit(const AggregateFunction &, data_ptr_t state_p) {
	auto &state = *reinterpret_cast<SignTestState *>(state_p);
	state.n_pos = 0;
	state.n_neg = 0;
	state.n_zero = 0;
}

// 1-sample Update: classify x - mu
static void SignTest1SampUpdate(Vector inputs[], AggregateInputData &aggr_input_data, idx_t, Vector &state_vector,
                                idx_t count) {
	double mu = 0.0;
	if (aggr_input_data.bind_data) {
		mu = aggr_input_data.bind_data->Cast<SignTestBindData>().mu;
	}

	UnifiedVectorFormat xdata;
	inputs[0].ToUnifiedFormat(count, xdata);

	auto states = FlatVector::GetData<SignTestState *>(state_vector);
	auto xv = UnifiedVectorFormat::GetData<double>(xdata);

	for (idx_t i = 0; i < count; i++) {
		auto x_idx = xdata.sel->get_index(i);
		if (!xdata.validity.RowIsValid(x_idx)) {
			continue;
		}
		double x = xv[x_idx];
		if (std::isnan(x)) {
			continue;
		}
		auto &state = *states[i];
		double d = x - mu;
		if (d > 0.0) {
			state.n_pos++;
		} else if (d < 0.0) {
			state.n_neg++;
		} else {
			state.n_zero++;
		}
	}
}

// Paired Update: classify x - y
static void SignTestPairedUpdate(Vector inputs[], AggregateInputData &, idx_t, Vector &state_vector, idx_t count) {
	UnifiedVectorFormat xdata, ydata;
	inputs[0].ToUnifiedFormat(count, xdata);
	inputs[1].ToUnifiedFormat(count, ydata);

	auto states = FlatVector::GetData<SignTestState *>(state_vector);
	auto xv = UnifiedVectorFormat::GetData<double>(xdata);
	auto yv = UnifiedVectorFormat::GetData<double>(ydata);

	for (idx_t i = 0; i < count; i++) {
		auto x_idx = xdata.sel->get_index(i);
		auto y_idx = ydata.sel->get_index(i);
		if (!xdata.validity.RowIsValid(x_idx) || !ydata.validity.RowIsValid(y_idx)) {
			continue;
		}
		double x = xv[x_idx];
		double y = yv[y_idx];
		if (std::isnan(x) || std::isnan(y)) {
			continue;
		}
		auto &state = *states[i];
		double d = x - y;
		if (d > 0.0) {
			state.n_pos++;
		} else if (d < 0.0) {
			state.n_neg++;
		} else {
			state.n_zero++;
		}
	}
}

static void SignTestCombine(Vector &source, Vector &target, AggregateInputData &, idx_t count) {
	auto src = FlatVector::GetData<SignTestState *>(source);
	auto tgt = FlatVector::GetData<SignTestState *>(target);
	for (idx_t i = 0; i < count; i++) {
		tgt[i]->n_pos += src[i]->n_pos;
		tgt[i]->n_neg += src[i]->n_neg;
		tgt[i]->n_zero += src[i]->n_zero;
	}
}

// ── Binomial CDF helpers for p=0.5 (via the regularized incomplete beta) ──

// P(X <= k | n, 0.5) = I_{0.5}(n-k, k+1)
static double BinomCdfLower(int64_t k, int64_t n) {
	if (k < 0) {
		return 0.0;
	}
	if (k >= n) {
		return 1.0;
	}
	return stats_duck::BetaIncomplete(static_cast<double>(n - k), static_cast<double>(k + 1), 0.5);
}

// P(X >= k | n, 0.5) = I_{0.5}(k, n-k+1)
static double BinomCdfUpper(int64_t k, int64_t n) {
	if (k <= 0) {
		return 1.0;
	}
	if (k > n) {
		return 0.0;
	}
	return stats_duck::BetaIncomplete(static_cast<double>(k), static_cast<double>(n - k + 1), 0.5);
}

static double SignTestPValue(int64_t n_pos, int64_t n_eff, const std::string &alternative) {
	if (alternative == "two-sided") {
		double lower = BinomCdfLower(n_pos, n_eff);
		double upper = BinomCdfUpper(n_pos, n_eff);
		double p = 2.0 * std::min(lower, upper);
		return std::min(p, 1.0);
	}
	if (alternative == "less") {
		// H1: median < mu. Reject for small n_pos.
		return BinomCdfLower(n_pos, n_eff);
	}
	// alternative == "greater". H1: median > mu. Reject for large n_pos.
	return BinomCdfUpper(n_pos, n_eff);
}

// ── Finalize ───────────────────────────────────────────────────────────────

template <const char *TEST_TYPE>
static void SignTestFinalize(Vector &state_vector, AggregateInputData &aggr_input_data, Vector &result, idx_t count,
                             idx_t offset) {
	auto states = FlatVector::GetData<SignTestState *>(state_vector);
	auto &children = StructVector::GetEntries(result);

	std::string alternative = "two-sided";
	if (aggr_input_data.bind_data) {
		alternative = aggr_input_data.bind_data->Cast<SignTestBindData>().alternative;
	}

	for (idx_t i = 0; i < count; i++) {
		auto &state = *states[i];
		auto idx = i + offset;

		int64_t n_eff = state.n_pos + state.n_neg;
		int64_t n_total = n_eff + state.n_zero;
		if (n_eff < 1) {
			// All ties (or no data): test is undefined.
			FlatVector::SetNull(result, idx, true);
			continue;
		}

		double m = (static_cast<double>(state.n_pos) - static_cast<double>(state.n_neg)) / 2.0;
		double p_value = SignTestPValue(state.n_pos, n_eff, alternative);

		FlatVector::GetData<string_t>(*children[0])[idx] = StringVector::AddString(*children[0], TEST_TYPE);
		FlatVector::GetData<double>(*children[1])[idx] = m;
		FlatVector::GetData<int64_t>(*children[2])[idx] = state.n_pos;
		FlatVector::GetData<int64_t>(*children[3])[idx] = state.n_neg;
		FlatVector::GetData<int64_t>(*children[4])[idx] = state.n_zero;
		FlatVector::GetData<double>(*children[5])[idx] = p_value;
		FlatVector::GetData<string_t>(*children[6])[idx] = StringVector::AddString(*children[6], alternative);
		FlatVector::GetData<int64_t>(*children[7])[idx] = n_total;
	}
}

static const char kSignTest1SampLabel[] = "Sign Test (1-sample)";
static const char kSignTestPairedLabel[] = "Sign Test (paired)";

static AggregateFunction MakeSignTest1Samp(vector<LogicalType> args, bind_aggregate_function_t bind_fn) {
	return AggregateFunction("sign_test_1samp", std::move(args), SignTestResultType(),
	                         AggregateFunction::StateSize<SignTestState>, SignTestInit, SignTest1SampUpdate,
	                         SignTestCombine, SignTestFinalize<kSignTest1SampLabel>,
	                         FunctionNullHandling::DEFAULT_NULL_HANDLING, nullptr, bind_fn);
}

static AggregateFunction MakeSignTestPaired(vector<LogicalType> args, bind_aggregate_function_t bind_fn) {
	return AggregateFunction("sign_test_paired", std::move(args), SignTestResultType(),
	                         AggregateFunction::StateSize<SignTestState>, SignTestInit, SignTestPairedUpdate,
	                         SignTestCombine, SignTestFinalize<kSignTestPairedLabel>,
	                         FunctionNullHandling::DEFAULT_NULL_HANDLING, nullptr, bind_fn);
}

} // namespace

void RegisterSignTest(ExtensionLoader &loader) {
	AggregateFunctionSet one_samp("sign_test_1samp");
	one_samp.AddFunction(MakeSignTest1Samp({LogicalType::DOUBLE}, Bind1Samp1));
	one_samp.AddFunction(MakeSignTest1Samp({LogicalType::DOUBLE, LogicalType::DOUBLE}, Bind1Samp2));
	one_samp.AddFunction(
	    MakeSignTest1Samp({LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::VARCHAR}, Bind1Samp3));
	loader.RegisterFunction(one_samp);

	AggregateFunctionSet paired("sign_test_paired");
	paired.AddFunction(MakeSignTestPaired({LogicalType::DOUBLE, LogicalType::DOUBLE}, BindPaired2));
	paired.AddFunction(
	    MakeSignTestPaired({LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::VARCHAR}, BindPaired3));
	loader.RegisterFunction(paired);
}

} // namespace duckdb
