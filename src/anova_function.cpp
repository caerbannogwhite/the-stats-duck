#include "anova_function.hpp"
#include "distributions.hpp"

#include "duckdb/function/aggregate_function.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/main/extension_util.hpp"

#include <cmath>
#include <string>
#include <unordered_map>

namespace duckdb {

// anova_oneway(value, group) — one-way ANOVA.
//
// Accumulates a Welford-style (n, mean, M2) per group using a heap-allocated
// std::unordered_map. At Finalize we compute:
//   grand_mean = sum(n_g * mean_g) / n
//   SS_between = sum(n_g * (mean_g - grand_mean)^2)
//   SS_within  = sum(m2_g)                               // M2 = sum((x - mean_g)^2)
//   df_between = k - 1
//   df_within  = n - k
//   MS_between = SS_between / df_between
//   MS_within  = SS_within  / df_within
//   F          = MS_between / MS_within
//   p_value    = 1 - F_CDF(F, df_between, df_within)
//   eta^2      = SS_between / (SS_between + SS_within)
//
// Groups are keyed by VARCHAR (the most common case in practice). Users with
// numeric group IDs can always CAST them to VARCHAR.

namespace {

// ── Result STRUCT ──────────────────────────────────────────────────────────

static LogicalType AnovaResultType() {
	child_list_t<LogicalType> children;
	children.emplace_back("test_type", LogicalType::VARCHAR);
	children.emplace_back("f_statistic", LogicalType::DOUBLE);
	children.emplace_back("df_between", LogicalType::DOUBLE);
	children.emplace_back("df_within", LogicalType::DOUBLE);
	children.emplace_back("p_value", LogicalType::DOUBLE);
	children.emplace_back("ss_between", LogicalType::DOUBLE);
	children.emplace_back("ss_within", LogicalType::DOUBLE);
	children.emplace_back("eta_squared", LogicalType::DOUBLE);
	children.emplace_back("n_groups", LogicalType::BIGINT);
	children.emplace_back("n", LogicalType::BIGINT);
	return LogicalType::STRUCT(std::move(children));
}

// ── State ──────────────────────────────────────────────────────────────────

struct GroupMoments {
	int64_t n;
	double mean;
	double m2; // sum((x - mean)^2)
};

struct AnovaState {
	std::unordered_map<std::string, GroupMoments> *groups;
};

static void AnovaInit(const AggregateFunction &, data_ptr_t state_p) {
	auto &state = *reinterpret_cast<AnovaState *>(state_p);
	state.groups = nullptr;
}

static inline void GroupUpdate(GroupMoments &g, double x) {
	g.n++;
	double dn = static_cast<double>(g.n);
	double delta = x - g.mean;
	g.mean += delta / dn;
	double delta2 = x - g.mean;
	g.m2 += delta * delta2;
}

static inline void GroupCombine(GroupMoments &a, const GroupMoments &b) {
	if (b.n == 0) {
		return;
	}
	if (a.n == 0) {
		a = b;
		return;
	}
	int64_t n = a.n + b.n;
	double dn = static_cast<double>(n);
	double na = static_cast<double>(a.n);
	double nb = static_cast<double>(b.n);
	double delta = b.mean - a.mean;
	a.m2 += b.m2 + delta * delta * na * nb / dn;
	a.mean += delta * nb / dn;
	a.n = n;
}

static void AnovaUpdate(Vector inputs[], AggregateInputData &, idx_t, Vector &state_vector, idx_t count) {
	UnifiedVectorFormat vdata, gdata;
	inputs[0].ToUnifiedFormat(count, vdata);
	inputs[1].ToUnifiedFormat(count, gdata);

	auto states = FlatVector::GetData<AnovaState *>(state_vector);
	auto values = UnifiedVectorFormat::GetData<double>(vdata);
	auto groups = UnifiedVectorFormat::GetData<string_t>(gdata);

	for (idx_t i = 0; i < count; i++) {
		auto v_idx = vdata.sel->get_index(i);
		auto g_idx = gdata.sel->get_index(i);
		if (!vdata.validity.RowIsValid(v_idx) || !gdata.validity.RowIsValid(g_idx)) {
			continue;
		}
		double v = values[v_idx];
		if (std::isnan(v)) {
			continue;
		}
		auto &state = *states[i];
		if (!state.groups) {
			state.groups = new std::unordered_map<std::string, GroupMoments>();
		}
		// Copy the string out of the DuckDB string_t to use as a map key.
		std::string key(groups[g_idx].GetData(), groups[g_idx].GetSize());
		auto &g = (*state.groups)[key];
		GroupUpdate(g, v);
	}
}

static void AnovaCombine(Vector &source, Vector &target, AggregateInputData &, idx_t count) {
	auto src = FlatVector::GetData<AnovaState *>(source);
	auto tgt = FlatVector::GetData<AnovaState *>(target);
	for (idx_t i = 0; i < count; i++) {
		auto &s = *src[i];
		auto &t = *tgt[i];
		if (!s.groups) {
			continue;
		}
		if (!t.groups) {
			t.groups = new std::unordered_map<std::string, GroupMoments>();
		}
		for (auto &kv : *s.groups) {
			auto &target_g = (*t.groups)[kv.first];
			GroupCombine(target_g, kv.second);
		}
	}
}

static void AnovaDestroy(Vector &state_vector, AggregateInputData &, idx_t count) {
	auto states = FlatVector::GetData<AnovaState *>(state_vector);
	for (idx_t i = 0; i < count; i++) {
		delete states[i]->groups;
		states[i]->groups = nullptr;
	}
}

static void AnovaFinalize(Vector &state_vector, AggregateInputData &, Vector &result, idx_t count, idx_t offset) {
	auto states = FlatVector::GetData<AnovaState *>(state_vector);
	auto &children = StructVector::GetEntries(result);

	for (idx_t i = 0; i < count; i++) {
		auto &state = *states[i];
		auto idx = i + offset;

		if (!state.groups || state.groups->size() < 2) {
			// ANOVA is undefined for fewer than 2 groups.
			FlatVector::SetNull(result, idx, true);
			continue;
		}

		int64_t k = static_cast<int64_t>(state.groups->size());
		int64_t n_total = 0;
		double weighted_mean_sum = 0.0;
		for (auto &kv : *state.groups) {
			n_total += kv.second.n;
			weighted_mean_sum += static_cast<double>(kv.second.n) * kv.second.mean;
		}
		if (n_total - k <= 0) {
			// Not enough observations to estimate within-group variance.
			FlatVector::SetNull(result, idx, true);
			continue;
		}
		double grand_mean = weighted_mean_sum / static_cast<double>(n_total);

		double ss_between = 0.0;
		double ss_within = 0.0;
		for (auto &kv : *state.groups) {
			double n_g = static_cast<double>(kv.second.n);
			double diff = kv.second.mean - grand_mean;
			ss_between += n_g * diff * diff;
			ss_within += kv.second.m2;
		}

		double df_between = static_cast<double>(k - 1);
		double df_within = static_cast<double>(n_total - k);
		double ms_between = ss_between / df_between;
		double ms_within = ss_within / df_within;

		double f_stat;
		double p_value;
		if (ms_within <= 0.0) {
			// All within-group variance is zero — either every group has
			// one observation, or every value within a group is identical.
			// If ss_between > 0 the F statistic is effectively infinite.
			if (ss_between > 0.0) {
				f_stat = std::numeric_limits<double>::infinity();
				p_value = 0.0;
			} else {
				f_stat = 0.0;
				p_value = 1.0;
			}
		} else {
			f_stat = ms_between / ms_within;
			p_value = 1.0 - stats_duck::FCDF(f_stat, df_between, df_within);
		}

		double ss_total = ss_between + ss_within;
		double eta_squared = ss_total > 0.0 ? ss_between / ss_total : 0.0;

		FlatVector::GetData<string_t>(*children[0])[idx] =
		    StringVector::AddString(*children[0], "One-way ANOVA");
		FlatVector::GetData<double>(*children[1])[idx] = f_stat;
		FlatVector::GetData<double>(*children[2])[idx] = df_between;
		FlatVector::GetData<double>(*children[3])[idx] = df_within;
		FlatVector::GetData<double>(*children[4])[idx] = p_value;
		FlatVector::GetData<double>(*children[5])[idx] = ss_between;
		FlatVector::GetData<double>(*children[6])[idx] = ss_within;
		FlatVector::GetData<double>(*children[7])[idx] = eta_squared;
		FlatVector::GetData<int64_t>(*children[8])[idx] = k;
		FlatVector::GetData<int64_t>(*children[9])[idx] = n_total;
	}
}

} // namespace

void RegisterAnovaOneway(DatabaseInstance &db) {
	AggregateFunction fn("anova_oneway", {LogicalType::DOUBLE, LogicalType::VARCHAR}, AnovaResultType(),
	                     AggregateFunction::StateSize<AnovaState>, AnovaInit, AnovaUpdate, AnovaCombine, AnovaFinalize,
	                     FunctionNullHandling::SPECIAL_HANDLING, nullptr, nullptr, AnovaDestroy);
	ExtensionUtil::RegisterFunction(db, fn);
}

} // namespace duckdb
