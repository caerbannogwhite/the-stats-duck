#include "nonparametric_function.hpp"

#include "duckdb/function/aggregate_function.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression.hpp"

#include <algorithm>
#include <cmath>
#include <vector>

namespace duckdb {

// ─── Helpers ────────────────────────────────────────────────────────────────────

static double NormalCDF(double x) {
	return 0.5 * (1.0 + std::erf(x / std::sqrt(2.0)));
}

static double NormalPValue(double z, const string &alternative) {
	if (alternative == "two-sided") {
		return 2.0 * (1.0 - NormalCDF(std::abs(z)));
	} else if (alternative == "less") {
		return NormalCDF(z);
	} else {
		return 1.0 - NormalCDF(z);
	}
}

struct RankEntry {
	double value;
	idx_t group; // 0 = sample A, 1 = sample B
	double rank;
};

//! Sort entries by value and assign average ranks for ties.
//! Returns sum(t^3 - t) for tie correction.
static double AssignRanksAndTieCorrection(std::vector<RankEntry> &entries) {
	std::sort(entries.begin(), entries.end(),
	          [](const RankEntry &a, const RankEntry &b) { return a.value < b.value; });

	double tie_correction = 0.0;
	idx_t n = entries.size();
	idx_t i = 0;
	while (i < n) {
		idx_t j = i;
		while (j < n && entries[j].value == entries[i].value) {
			j++;
		}
		double avg_rank = (static_cast<double>(i + 1) + static_cast<double>(j)) / 2.0;
		for (idx_t k = i; k < j; k++) {
			entries[k].rank = avg_rank;
		}
		double t = static_cast<double>(j - i);
		if (t > 1.0) {
			tie_correction += t * t * t - t;
		}
		i = j;
	}
	return tie_correction;
}

static void ValidateAlternative(const string &alt) {
	if (alt != "two-sided" && alt != "less" && alt != "greater") {
		throw InvalidInputException("alternative must be 'two-sided', 'less', or 'greater'");
	}
}

static string ExtractConstantString(ClientContext &context, AggregateFunction &function,
                                     vector<unique_ptr<Expression>> &arguments, idx_t arg_idx) {
	if (!arguments[arg_idx]->IsFoldable()) {
		throw BinderException("Parameter must be a constant value");
	}
	Value val = ExpressionExecutor::EvaluateScalar(context, *arguments[arg_idx]);
	string result = val.GetValue<string>();
	Function::EraseArgument(function, arguments, arg_idx);
	return result;
}

// ─── Bind Data ──────────────────────────────────────────────────────────────────

struct NonParamBindData : public FunctionData {
	string alternative = "two-sided";

	unique_ptr<FunctionData> Copy() const override {
		auto copy = make_uniq<NonParamBindData>();
		copy->alternative = alternative;
		return std::move(copy);
	}

	bool Equals(const FunctionData &other_p) const override {
		return alternative == other_p.Cast<NonParamBindData>().alternative;
	}
};

static unique_ptr<FunctionData> NonParamBindDefault(ClientContext &, AggregateFunction &,
                                                     vector<unique_ptr<Expression>> &) {
	return make_uniq<NonParamBindData>();
}

static unique_ptr<FunctionData> NonParamBindAlt(ClientContext &context, AggregateFunction &function,
                                                 vector<unique_ptr<Expression>> &arguments) {
	auto bd = make_uniq<NonParamBindData>();
	bd->alternative = ExtractConstantString(context, function, arguments, arguments.size() - 1);
	ValidateAlternative(bd->alternative);
	return std::move(bd);
}

// =============================================================================
// Mann-Whitney U
// =============================================================================

static LogicalType MannWhitneyResultType() {
	child_list_t<LogicalType> children;
	children.emplace_back("test_type", LogicalType::VARCHAR);
	children.emplace_back("u_statistic", LogicalType::DOUBLE);
	children.emplace_back("z_statistic", LogicalType::DOUBLE);
	children.emplace_back("p_value", LogicalType::DOUBLE);
	children.emplace_back("alternative", LogicalType::VARCHAR);
	children.emplace_back("rank_biserial", LogicalType::DOUBLE);
	return LogicalType::STRUCT(std::move(children));
}

struct MannWhitneyState {
	std::vector<double> *data_a;
	std::vector<double> *data_b;
};

static void MannWhitneyInit(const AggregateFunction &, data_ptr_t state_p) {
	auto &state = *reinterpret_cast<MannWhitneyState *>(state_p);
	state.data_a = nullptr;
	state.data_b = nullptr;
}

static void MannWhitneyUpdate(Vector inputs[], AggregateInputData &, idx_t, Vector &state_vector, idx_t count) {
	UnifiedVectorFormat idata1, idata2;
	inputs[0].ToUnifiedFormat(count, idata1);
	inputs[1].ToUnifiedFormat(count, idata2);

	auto states = FlatVector::GetData<MannWhitneyState *>(state_vector);
	auto v1 = UnifiedVectorFormat::GetData<double>(idata1);
	auto v2 = UnifiedVectorFormat::GetData<double>(idata2);

	for (idx_t i = 0; i < count; i++) {
		auto &state = *states[i];

		auto idx1 = idata1.sel->get_index(i);
		if (idata1.validity.RowIsValid(idx1)) {
			if (!state.data_a) {
				state.data_a = new std::vector<double>();
			}
			state.data_a->push_back(v1[idx1]);
		}

		auto idx2 = idata2.sel->get_index(i);
		if (idata2.validity.RowIsValid(idx2)) {
			if (!state.data_b) {
				state.data_b = new std::vector<double>();
			}
			state.data_b->push_back(v2[idx2]);
		}
	}
}

static void MannWhitneyCombine(Vector &source, Vector &target, AggregateInputData &, idx_t count) {
	auto src = FlatVector::GetData<MannWhitneyState *>(source);
	auto tgt = FlatVector::GetData<MannWhitneyState *>(target);
	for (idx_t i = 0; i < count; i++) {
		auto &s = *src[i];
		auto &t = *tgt[i];
		if (s.data_a && !s.data_a->empty()) {
			if (!t.data_a) {
				t.data_a = new std::vector<double>();
			}
			t.data_a->insert(t.data_a->end(), s.data_a->begin(), s.data_a->end());
		}
		if (s.data_b && !s.data_b->empty()) {
			if (!t.data_b) {
				t.data_b = new std::vector<double>();
			}
			t.data_b->insert(t.data_b->end(), s.data_b->begin(), s.data_b->end());
		}
	}
}

static void MannWhitneyDestroy(Vector &state_vector, AggregateInputData &, idx_t count) {
	auto states = FlatVector::GetData<MannWhitneyState *>(state_vector);
	for (idx_t i = 0; i < count; i++) {
		delete states[i]->data_a;
		delete states[i]->data_b;
	}
}

static void MannWhitneyFinalize(Vector &state_vector, AggregateInputData &aggr_input_data, Vector &result,
                                 idx_t count, idx_t offset) {
	auto states = FlatVector::GetData<MannWhitneyState *>(state_vector);
	auto &children = StructVector::GetEntries(result);

	string alternative = "two-sided";
	if (aggr_input_data.bind_data) {
		alternative = aggr_input_data.bind_data->Cast<NonParamBindData>().alternative;
	}

	for (idx_t i = 0; i < count; i++) {
		auto &state = *states[i];
		auto idx = i + offset;

		if (!state.data_a || !state.data_b || state.data_a->size() < 2 || state.data_b->size() < 2) {
			FlatVector::SetNull(result, idx, true);
			continue;
		}

		auto n1 = state.data_a->size();
		auto n2 = state.data_b->size();
		double dn1 = static_cast<double>(n1);
		double dn2 = static_cast<double>(n2);
		double N = dn1 + dn2;

		// Combine samples and assign ranks
		std::vector<RankEntry> entries;
		entries.reserve(n1 + n2);
		for (idx_t j = 0; j < n1; j++) {
			entries.push_back({(*state.data_a)[j], 0, 0.0});
		}
		for (idx_t j = 0; j < n2; j++) {
			entries.push_back({(*state.data_b)[j], 1, 0.0});
		}

		double tie_correction = AssignRanksAndTieCorrection(entries);

		// Rank sum for sample A
		double R1 = 0.0;
		for (auto &e : entries) {
			if (e.group == 0) {
				R1 += e.rank;
			}
		}

		// U statistic for sample A
		double U1 = R1 - dn1 * (dn1 + 1.0) / 2.0;

		// Normal approximation
		double mu_U = dn1 * dn2 / 2.0;
		double sigma_U = std::sqrt(dn1 * dn2 / 12.0 * (N + 1.0 - tie_correction / (N * (N - 1.0))));

		double z = (sigma_U > 0.0) ? (U1 - mu_U) / sigma_U : 0.0;
		double p_value = NormalPValue(z, alternative);
		double rank_biserial = 1.0 - 2.0 * U1 / (dn1 * dn2);

		FlatVector::GetData<string_t>(*children[0])[idx] = StringVector::AddString(*children[0], "Mann-Whitney U");
		FlatVector::GetData<double>(*children[1])[idx] = U1;
		FlatVector::GetData<double>(*children[2])[idx] = z;
		FlatVector::GetData<double>(*children[3])[idx] = p_value;
		FlatVector::GetData<string_t>(*children[4])[idx] = StringVector::AddString(*children[4], alternative);
		FlatVector::GetData<double>(*children[5])[idx] = rank_biserial;
	}
}

static AggregateFunction MakeMannWhitneyAgg(vector<LogicalType> args, bind_aggregate_function_t bind_fn) {
	return AggregateFunction("mann_whitney_u", std::move(args), MannWhitneyResultType(),
	                         AggregateFunction::StateSize<MannWhitneyState>, MannWhitneyInit, MannWhitneyUpdate,
	                         MannWhitneyCombine, MannWhitneyFinalize, FunctionNullHandling::DEFAULT_NULL_HANDLING,
	                         nullptr, bind_fn, MannWhitneyDestroy);
}

void RegisterMannWhitneyU(DatabaseInstance &db) {
	AggregateFunctionSet set("mann_whitney_u");
	set.AddFunction(MakeMannWhitneyAgg({LogicalType::DOUBLE, LogicalType::DOUBLE}, NonParamBindDefault));
	set.AddFunction(
	    MakeMannWhitneyAgg({LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::VARCHAR}, NonParamBindAlt));
	ExtensionUtil::RegisterFunction(db, set);
}

// =============================================================================
// Wilcoxon Signed-Rank
// =============================================================================

static LogicalType WilcoxonResultType() {
	child_list_t<LogicalType> children;
	children.emplace_back("test_type", LogicalType::VARCHAR);
	children.emplace_back("w_statistic", LogicalType::DOUBLE);
	children.emplace_back("z_statistic", LogicalType::DOUBLE);
	children.emplace_back("p_value", LogicalType::DOUBLE);
	children.emplace_back("alternative", LogicalType::VARCHAR);
	children.emplace_back("effect_size_r", LogicalType::DOUBLE);
	return LogicalType::STRUCT(std::move(children));
}

struct WilcoxonState {
	std::vector<double> *diffs; // Non-zero differences
};

static void WilcoxonInit(const AggregateFunction &, data_ptr_t state_p) {
	auto &state = *reinterpret_cast<WilcoxonState *>(state_p);
	state.diffs = nullptr;
}

static void WilcoxonUpdate(Vector inputs[], AggregateInputData &, idx_t, Vector &state_vector, idx_t count) {
	UnifiedVectorFormat idata1, idata2;
	inputs[0].ToUnifiedFormat(count, idata1);
	inputs[1].ToUnifiedFormat(count, idata2);

	auto states = FlatVector::GetData<WilcoxonState *>(state_vector);
	auto v1 = UnifiedVectorFormat::GetData<double>(idata1);
	auto v2 = UnifiedVectorFormat::GetData<double>(idata2);

	for (idx_t i = 0; i < count; i++) {
		auto idx1 = idata1.sel->get_index(i);
		auto idx2 = idata2.sel->get_index(i);
		if (!idata1.validity.RowIsValid(idx1) || !idata2.validity.RowIsValid(idx2)) {
			continue;
		}
		double diff = v1[idx1] - v2[idx2];
		if (diff == 0.0) {
			continue; // Zero differences are excluded
		}
		auto &state = *states[i];
		if (!state.diffs) {
			state.diffs = new std::vector<double>();
		}
		state.diffs->push_back(diff);
	}
}

static void WilcoxonCombine(Vector &source, Vector &target, AggregateInputData &, idx_t count) {
	auto src = FlatVector::GetData<WilcoxonState *>(source);
	auto tgt = FlatVector::GetData<WilcoxonState *>(target);
	for (idx_t i = 0; i < count; i++) {
		auto &s = *src[i];
		auto &t = *tgt[i];
		if (s.diffs && !s.diffs->empty()) {
			if (!t.diffs) {
				t.diffs = new std::vector<double>();
			}
			t.diffs->insert(t.diffs->end(), s.diffs->begin(), s.diffs->end());
		}
	}
}

static void WilcoxonDestroy(Vector &state_vector, AggregateInputData &, idx_t count) {
	auto states = FlatVector::GetData<WilcoxonState *>(state_vector);
	for (idx_t i = 0; i < count; i++) {
		delete states[i]->diffs;
	}
}

static void WilcoxonFinalize(Vector &state_vector, AggregateInputData &aggr_input_data, Vector &result,
                              idx_t count, idx_t offset) {
	auto states = FlatVector::GetData<WilcoxonState *>(state_vector);
	auto &children = StructVector::GetEntries(result);

	string alternative = "two-sided";
	if (aggr_input_data.bind_data) {
		alternative = aggr_input_data.bind_data->Cast<NonParamBindData>().alternative;
	}

	for (idx_t i = 0; i < count; i++) {
		auto &state = *states[i];
		auto idx = i + offset;

		if (!state.diffs || state.diffs->size() < 2) {
			FlatVector::SetNull(result, idx, true);
			continue;
		}

		auto &diffs = *state.diffs;
		auto n_r = diffs.size();
		double dn = static_cast<double>(n_r);

		// Rank absolute differences
		std::vector<RankEntry> entries;
		entries.reserve(n_r);
		for (idx_t j = 0; j < n_r; j++) {
			entries.push_back({std::abs(diffs[j]), 0, 0.0});
		}
		double tie_correction = AssignRanksAndTieCorrection(entries);

		// W+ = sum of ranks where original difference is positive
		double w_plus = 0.0;
		for (idx_t j = 0; j < n_r; j++) {
			if (diffs[j] > 0.0) {
				w_plus += entries[j].rank;
			}
		}

		// Normal approximation
		double mu_W = dn * (dn + 1.0) / 4.0;
		double sigma_W = std::sqrt(dn * (dn + 1.0) * (2.0 * dn + 1.0) / 24.0 - tie_correction / 48.0);

		double z = (sigma_W > 0.0) ? (w_plus - mu_W) / sigma_W : 0.0;
		double p_value = NormalPValue(z, alternative);
		double effect_r = z / std::sqrt(dn);

		FlatVector::GetData<string_t>(*children[0])[idx] =
		    StringVector::AddString(*children[0], "Wilcoxon Signed-Rank");
		FlatVector::GetData<double>(*children[1])[idx] = w_plus;
		FlatVector::GetData<double>(*children[2])[idx] = z;
		FlatVector::GetData<double>(*children[3])[idx] = p_value;
		FlatVector::GetData<string_t>(*children[4])[idx] = StringVector::AddString(*children[4], alternative);
		FlatVector::GetData<double>(*children[5])[idx] = effect_r;
	}
}

static AggregateFunction MakeWilcoxonAgg(vector<LogicalType> args, bind_aggregate_function_t bind_fn) {
	return AggregateFunction("wilcoxon_signed_rank", std::move(args), WilcoxonResultType(),
	                         AggregateFunction::StateSize<WilcoxonState>, WilcoxonInit, WilcoxonUpdate,
	                         WilcoxonCombine, WilcoxonFinalize, FunctionNullHandling::DEFAULT_NULL_HANDLING,
	                         nullptr, bind_fn, WilcoxonDestroy);
}

void RegisterWilcoxonSignedRank(DatabaseInstance &db) {
	AggregateFunctionSet set("wilcoxon_signed_rank");
	set.AddFunction(MakeWilcoxonAgg({LogicalType::DOUBLE, LogicalType::DOUBLE}, NonParamBindDefault));
	set.AddFunction(
	    MakeWilcoxonAgg({LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::VARCHAR}, NonParamBindAlt));
	ExtensionUtil::RegisterFunction(db, set);
}

} // namespace duckdb
