#include "correlation_function.hpp"
#include "distributions.hpp"
#include "stats_validation.hpp"

#include "duckdb/function/aggregate_function.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/main/extension_util.hpp"

#include <cmath>
#include <limits>

namespace duckdb {

// Pearson product-moment correlation as a streaming aggregate.
//
// Uses a two-variable Welford-style online algorithm (parallel variant for
// Combine) so the aggregate runs in a single pass, supports GROUP BY, and
// handles distributed / parallel execution without buffering rows.
//
// Significance testing uses the classic t-statistic:
//     t = r * sqrt((n - 2) / (1 - r^2)),   df = n - 2
// p-values are obtained from Student's t distribution.
//
// Confidence intervals use the Fisher z-transformation:
//     z = atanh(r),  SE = 1 / sqrt(n - 3)
// then back-transformed with tanh. This is preferred over the symmetric
// "normal" CI because it respects the [-1, 1] bounds on r.

namespace {

// ── Bind data ──────────────────────────────────────────────────────────────

struct PearsonBindData : public FunctionData {
	double alpha = 0.05;
	std::string alternative = "two-sided";

	unique_ptr<FunctionData> Copy() const override {
		auto copy = make_uniq<PearsonBindData>();
		copy->alpha = alpha;
		copy->alternative = alternative;
		return std::move(copy);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<PearsonBindData>();
		return alpha == other.alpha && alternative == other.alternative;
	}
};

// ── Bind-parameter extractors ──────────────────────────────────────────────

static double ExtractConstantDouble(ClientContext &context, AggregateFunction &function,
                                     vector<unique_ptr<Expression>> &arguments, idx_t arg_idx) {
	if (!arguments[arg_idx]->IsFoldable()) {
		throw BinderException("pearson_test: parameter must be a constant value");
	}
	Value val = ExpressionExecutor::EvaluateScalar(context, *arguments[arg_idx]);
	double result = val.GetValue<double>();
	Function::EraseArgument(function, arguments, arg_idx);
	return result;
}

static std::string ExtractConstantString(ClientContext &context, AggregateFunction &function,
                                          vector<unique_ptr<Expression>> &arguments, idx_t arg_idx) {
	if (!arguments[arg_idx]->IsFoldable()) {
		throw BinderException("pearson_test: parameter must be a constant value");
	}
	Value val = ExpressionExecutor::EvaluateScalar(context, *arguments[arg_idx]);
	std::string result = val.GetValue<std::string>();
	Function::EraseArgument(function, arguments, arg_idx);
	return result;
}

// Overload bindings: (x, y), (x, y, alpha), (x, y, alpha, alternative).

static unique_ptr<FunctionData> PearsonBind2(ClientContext &, AggregateFunction &,
                                              vector<unique_ptr<Expression>> &) {
	return make_uniq<PearsonBindData>();
}

static unique_ptr<FunctionData> PearsonBind3(ClientContext &context, AggregateFunction &function,
                                              vector<unique_ptr<Expression>> &arguments) {
	auto bd = make_uniq<PearsonBindData>();
	bd->alpha = ExtractConstantDouble(context, function, arguments, 2);
	sdv::ValidateAlpha(bd->alpha, "pearson_test");
	return std::move(bd);
}

static unique_ptr<FunctionData> PearsonBind4(ClientContext &context, AggregateFunction &function,
                                              vector<unique_ptr<Expression>> &arguments) {
	auto bd = make_uniq<PearsonBindData>();
	bd->alternative = ExtractConstantString(context, function, arguments, 3);
	sdv::ValidateAlternative(bd->alternative, "pearson_test");
	bd->alpha = ExtractConstantDouble(context, function, arguments, 2);
	sdv::ValidateAlpha(bd->alpha, "pearson_test");
	return std::move(bd);
}

// ── Result STRUCT ──────────────────────────────────────────────────────────

static LogicalType PearsonResultType() {
	child_list_t<LogicalType> children;
	children.emplace_back("test_type", LogicalType::VARCHAR);
	children.emplace_back("r", LogicalType::DOUBLE);
	children.emplace_back("t_statistic", LogicalType::DOUBLE);
	children.emplace_back("df", LogicalType::DOUBLE);
	children.emplace_back("p_value", LogicalType::DOUBLE);
	children.emplace_back("alternative", LogicalType::VARCHAR);
	children.emplace_back("ci_lower", LogicalType::DOUBLE);
	children.emplace_back("ci_upper", LogicalType::DOUBLE);
	children.emplace_back("n", LogicalType::BIGINT);
	return LogicalType::STRUCT(std::move(children));
}

// ── Aggregate state (two-variable Welford) ─────────────────────────────────

struct PearsonState {
	int64_t n;
	double mean_x;
	double mean_y;
	double c2_x; // sum_i (x_i - mean_x)^2
	double c2_y; // sum_i (y_i - mean_y)^2
	double cxy;  // sum_i (x_i - mean_x) * (y_i - mean_y)
};

static void PearsonInit(const AggregateFunction &, data_ptr_t state_p) {
	auto &state = *reinterpret_cast<PearsonState *>(state_p);
	state.n = 0;
	state.mean_x = 0.0;
	state.mean_y = 0.0;
	state.c2_x = 0.0;
	state.c2_y = 0.0;
	state.cxy = 0.0;
}

static inline void PearsonUpdateOne(PearsonState &state, double x, double y) {
	state.n++;
	double n_d = static_cast<double>(state.n);
	double dx = x - state.mean_x;
	double dy = y - state.mean_y;
	state.mean_x += dx / n_d;
	state.mean_y += dy / n_d;
	// Use the updated means for the second factor.
	state.c2_x += dx * (x - state.mean_x);
	state.c2_y += dy * (y - state.mean_y);
	state.cxy += dx * (y - state.mean_y);
}

static void PearsonUpdate(Vector inputs[], AggregateInputData &, idx_t, Vector &state_vector, idx_t count) {
	UnifiedVectorFormat xdata, ydata;
	inputs[0].ToUnifiedFormat(count, xdata);
	inputs[1].ToUnifiedFormat(count, ydata);

	auto states = FlatVector::GetData<PearsonState *>(state_vector);
	auto xv = UnifiedVectorFormat::GetData<double>(xdata);
	auto yv = UnifiedVectorFormat::GetData<double>(ydata);

	for (idx_t i = 0; i < count; i++) {
		auto x_idx = xdata.sel->get_index(i);
		auto y_idx = ydata.sel->get_index(i);
		// Drop any pair where either side is NULL (pairwise complete).
		if (!xdata.validity.RowIsValid(x_idx) || !ydata.validity.RowIsValid(y_idx)) {
			continue;
		}
		double x = xv[x_idx];
		double y = yv[y_idx];
		if (std::isnan(x) || std::isnan(y)) {
			continue;
		}
		PearsonUpdateOne(*states[i], x, y);
	}
}

static void PearsonCombine(Vector &source, Vector &target, AggregateInputData &, idx_t count) {
	auto src = FlatVector::GetData<PearsonState *>(source);
	auto tgt = FlatVector::GetData<PearsonState *>(target);
	for (idx_t i = 0; i < count; i++) {
		auto &a = *tgt[i];
		auto &b = *src[i];
		if (b.n == 0) {
			continue;
		}
		if (a.n == 0) {
			a = b;
			continue;
		}
		int64_t n = a.n + b.n;
		double dn = static_cast<double>(n);
		double n_a = static_cast<double>(a.n);
		double n_b = static_cast<double>(b.n);
		double dx = b.mean_x - a.mean_x;
		double dy = b.mean_y - a.mean_y;
		// Combined central moments via the parallel algorithm:
		a.c2_x += b.c2_x + dx * dx * n_a * n_b / dn;
		a.c2_y += b.c2_y + dy * dy * n_a * n_b / dn;
		a.cxy += b.cxy + dx * dy * n_a * n_b / dn;
		a.mean_x += dx * n_b / dn;
		a.mean_y += dy * n_b / dn;
		a.n = n;
	}
}

// ── Finalize ───────────────────────────────────────────────────────────────

static double ComputePValue(double t_stat, double df, const std::string &alternative) {
	if (alternative == "two-sided") {
		return 2.0 * (1.0 - stats_duck::StudentTCDF(std::abs(t_stat), df));
	}
	if (alternative == "less") {
		return stats_duck::StudentTCDF(t_stat, df);
	}
	return 1.0 - stats_duck::StudentTCDF(t_stat, df);
}

static void PearsonFinalize(Vector &state_vector, AggregateInputData &aggr_input_data, Vector &result, idx_t count,
                              idx_t offset) {
	auto states = FlatVector::GetData<PearsonState *>(state_vector);
	auto &children = StructVector::GetEntries(result);

	double alpha = 0.05;
	std::string alternative = "two-sided";
	if (aggr_input_data.bind_data) {
		auto &bd = aggr_input_data.bind_data->Cast<PearsonBindData>();
		alpha = bd.alpha;
		alternative = bd.alternative;
	}

	for (idx_t i = 0; i < count; i++) {
		auto &state = *states[i];
		auto idx = i + offset;

		// Need at least 3 pairs for t-test and 4 for a Fisher-z CI.
		if (state.n < 3 || state.c2_x <= 0.0 || state.c2_y <= 0.0) {
			FlatVector::SetNull(result, idx, true);
			continue;
		}

		double n_d = static_cast<double>(state.n);
		double r = state.cxy / std::sqrt(state.c2_x * state.c2_y);
		// Clamp for numerical safety (ties / floating-point drift can push |r| > 1).
		if (r > 1.0) {
			r = 1.0;
		} else if (r < -1.0) {
			r = -1.0;
		}

		double df = n_d - 2.0;
		double denom = 1.0 - r * r;
		double t_stat;
		if (denom <= 0.0) {
			t_stat = std::copysign(std::numeric_limits<double>::infinity(), r);
		} else {
			t_stat = r * std::sqrt(df / denom);
		}

		double p_value;
		if (std::isinf(t_stat)) {
			p_value = 0.0;
		} else {
			p_value = ComputePValue(t_stat, df, alternative);
		}

		// Fisher-z confidence interval (two-sided regardless of `alternative`,
		// matching R's cor.test behavior).
		double ci_lower = std::numeric_limits<double>::quiet_NaN();
		double ci_upper = std::numeric_limits<double>::quiet_NaN();
		if (state.n >= 4 && std::abs(r) < 1.0) {
			double z = std::atanh(r);
			double se = 1.0 / std::sqrt(n_d - 3.0);
			double z_crit = stats_duck::NormalQuantile(1.0 - alpha / 2.0);
			ci_lower = std::tanh(z - z_crit * se);
			ci_upper = std::tanh(z + z_crit * se);
		}

		FlatVector::GetData<string_t>(*children[0])[idx] =
		    StringVector::AddString(*children[0], "Pearson Correlation");
		FlatVector::GetData<double>(*children[1])[idx] = r;
		FlatVector::GetData<double>(*children[2])[idx] = t_stat;
		FlatVector::GetData<double>(*children[3])[idx] = df;
		FlatVector::GetData<double>(*children[4])[idx] = p_value;
		FlatVector::GetData<string_t>(*children[5])[idx] = StringVector::AddString(*children[5], alternative);
		FlatVector::GetData<double>(*children[6])[idx] = ci_lower;
		FlatVector::GetData<double>(*children[7])[idx] = ci_upper;
		FlatVector::GetData<int64_t>(*children[8])[idx] = state.n;
	}
}

static AggregateFunction MakePearson(vector<LogicalType> args, bind_aggregate_function_t bind_fn) {
	return AggregateFunction("pearson_test", std::move(args), PearsonResultType(),
	                         AggregateFunction::StateSize<PearsonState>, PearsonInit, PearsonUpdate, PearsonCombine,
	                         PearsonFinalize, FunctionNullHandling::DEFAULT_NULL_HANDLING, nullptr, bind_fn);
}

} // namespace

void RegisterPearsonTest(DatabaseInstance &db) {
	AggregateFunctionSet set("pearson_test");
	set.AddFunction(MakePearson({LogicalType::DOUBLE, LogicalType::DOUBLE}, PearsonBind2));
	set.AddFunction(MakePearson({LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE}, PearsonBind3));
	set.AddFunction(MakePearson(
	    {LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::VARCHAR}, PearsonBind4));
	ExtensionUtil::RegisterFunction(db, set);
}

} // namespace duckdb
