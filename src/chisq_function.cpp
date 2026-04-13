#include "chisq_function.hpp"
#include "distributions.hpp"

#include "duckdb/function/aggregate_function.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

#include <cmath>
#include <string>
#include <unordered_map>
#include <unordered_set>

namespace duckdb {

// Chi-square tests.
//
// chisq_independence(row, col):
//   Builds a contingency table (row_label, col_label) -> count and computes
//       chi^2 = sum_{i,j} (O_ij - E_ij)^2 / E_ij
//   where E_ij = row_total_i * col_total_j / n.
//   df = (n_rows - 1) * (n_cols - 1).
//
// chisq_goodness_of_fit(category):
//   Counts occurrences of each category label and tests the hypothesis that
//   the data are drawn uniformly at random over the k observed categories.
//       chi^2 = sum_k (O_k - n/k)^2 / (n/k)
//   df = k - 1.
//
// Both tests are weak for very small expected frequencies (< 5); Brian's UI
// can surface a warning based on the `n` and counts fields returned.

namespace {

// ── chisq_independence ─────────────────────────────────────────────────────

static LogicalType ChiSqIndependenceResultType() {
	child_list_t<LogicalType> children;
	children.emplace_back("test_type", LogicalType::VARCHAR);
	children.emplace_back("chi_square", LogicalType::DOUBLE);
	children.emplace_back("df", LogicalType::DOUBLE);
	children.emplace_back("p_value", LogicalType::DOUBLE);
	children.emplace_back("n", LogicalType::BIGINT);
	children.emplace_back("n_rows", LogicalType::BIGINT);
	children.emplace_back("n_cols", LogicalType::BIGINT);
	return LogicalType::STRUCT(std::move(children));
}

struct ChiSqIndependenceState {
	// row_label -> col_label -> count
	std::unordered_map<std::string, std::unordered_map<std::string, int64_t>> *table;
};

static void ChiSqIndInit(const AggregateFunction &, data_ptr_t state_p) {
	auto &state = *reinterpret_cast<ChiSqIndependenceState *>(state_p);
	state.table = nullptr;
}

static void ChiSqIndUpdate(Vector inputs[], AggregateInputData &, idx_t, Vector &state_vector, idx_t count) {
	UnifiedVectorFormat rdata, cdata;
	inputs[0].ToUnifiedFormat(count, rdata);
	inputs[1].ToUnifiedFormat(count, cdata);

	auto states = FlatVector::GetData<ChiSqIndependenceState *>(state_vector);
	auto rvals = UnifiedVectorFormat::GetData<string_t>(rdata);
	auto cvals = UnifiedVectorFormat::GetData<string_t>(cdata);

	for (idx_t i = 0; i < count; i++) {
		auto r_idx = rdata.sel->get_index(i);
		auto c_idx = cdata.sel->get_index(i);
		if (!rdata.validity.RowIsValid(r_idx) || !cdata.validity.RowIsValid(c_idx)) {
			continue;
		}
		auto &state = *states[i];
		if (!state.table) {
			state.table = new std::unordered_map<std::string, std::unordered_map<std::string, int64_t>>();
		}
		std::string r_key(rvals[r_idx].GetData(), rvals[r_idx].GetSize());
		std::string c_key(cvals[c_idx].GetData(), cvals[c_idx].GetSize());
		(*state.table)[r_key][c_key]++;
	}
}

static void ChiSqIndCombine(Vector &source, Vector &target, AggregateInputData &, idx_t count) {
	auto src = FlatVector::GetData<ChiSqIndependenceState *>(source);
	auto tgt = FlatVector::GetData<ChiSqIndependenceState *>(target);
	for (idx_t i = 0; i < count; i++) {
		auto &s = *src[i];
		auto &t = *tgt[i];
		if (!s.table) {
			continue;
		}
		if (!t.table) {
			t.table = new std::unordered_map<std::string, std::unordered_map<std::string, int64_t>>();
		}
		for (auto &row_kv : *s.table) {
			auto &t_row = (*t.table)[row_kv.first];
			for (auto &col_kv : row_kv.second) {
				t_row[col_kv.first] += col_kv.second;
			}
		}
	}
}

static void ChiSqIndDestroy(Vector &state_vector, AggregateInputData &, idx_t count) {
	auto states = FlatVector::GetData<ChiSqIndependenceState *>(state_vector);
	for (idx_t i = 0; i < count; i++) {
		delete states[i]->table;
		states[i]->table = nullptr;
	}
}

static void ChiSqIndFinalize(Vector &state_vector, AggregateInputData &, Vector &result, idx_t count, idx_t offset) {
	auto states = FlatVector::GetData<ChiSqIndependenceState *>(state_vector);
	auto &children = StructVector::GetEntries(result);

	for (idx_t i = 0; i < count; i++) {
		auto &state = *states[i];
		auto idx = i + offset;

		if (!state.table || state.table->empty()) {
			FlatVector::SetNull(result, idx, true);
			continue;
		}

		// Gather the union of column labels and compute row totals.
		std::unordered_map<std::string, int64_t> row_totals;
		std::unordered_map<std::string, int64_t> col_totals;
		int64_t n = 0;
		for (auto &row_kv : *state.table) {
			int64_t row_sum = 0;
			for (auto &col_kv : row_kv.second) {
				row_sum += col_kv.second;
				col_totals[col_kv.first] += col_kv.second;
			}
			row_totals[row_kv.first] = row_sum;
			n += row_sum;
		}

		int64_t n_rows = static_cast<int64_t>(row_totals.size());
		int64_t n_cols = static_cast<int64_t>(col_totals.size());
		if (n_rows < 2 || n_cols < 2 || n <= 0) {
			// Chi-square independence is undefined for a 1xN or Nx1 table.
			FlatVector::SetNull(result, idx, true);
			continue;
		}

		double chi_square = 0.0;
		double n_d = static_cast<double>(n);
		for (auto &row_kv : *state.table) {
			double row_tot = static_cast<double>(row_totals[row_kv.first]);
			for (auto &col_kv : col_totals) {
				auto it = row_kv.second.find(col_kv.first);
				double observed = it != row_kv.second.end() ? static_cast<double>(it->second) : 0.0;
				double expected = row_tot * static_cast<double>(col_kv.second) / n_d;
				if (expected > 0.0) {
					double diff = observed - expected;
					chi_square += diff * diff / expected;
				}
			}
		}

		double df = static_cast<double>((n_rows - 1) * (n_cols - 1));
		double p_value = 1.0 - stats_duck::ChiSquareCDF(chi_square, df);

		FlatVector::GetData<string_t>(*children[0])[idx] =
		    StringVector::AddString(*children[0], "Chi-square Independence");
		FlatVector::GetData<double>(*children[1])[idx] = chi_square;
		FlatVector::GetData<double>(*children[2])[idx] = df;
		FlatVector::GetData<double>(*children[3])[idx] = p_value;
		FlatVector::GetData<int64_t>(*children[4])[idx] = n;
		FlatVector::GetData<int64_t>(*children[5])[idx] = n_rows;
		FlatVector::GetData<int64_t>(*children[6])[idx] = n_cols;
	}
}

// ── chisq_goodness_of_fit (uniform) ────────────────────────────────────────

static LogicalType ChiSqGoFResultType() {
	child_list_t<LogicalType> children;
	children.emplace_back("test_type", LogicalType::VARCHAR);
	children.emplace_back("chi_square", LogicalType::DOUBLE);
	children.emplace_back("df", LogicalType::DOUBLE);
	children.emplace_back("p_value", LogicalType::DOUBLE);
	children.emplace_back("n", LogicalType::BIGINT);
	children.emplace_back("n_categories", LogicalType::BIGINT);
	return LogicalType::STRUCT(std::move(children));
}

struct ChiSqGoFState {
	std::unordered_map<std::string, int64_t> *counts;
};

static void ChiSqGoFInit(const AggregateFunction &, data_ptr_t state_p) {
	auto &state = *reinterpret_cast<ChiSqGoFState *>(state_p);
	state.counts = nullptr;
}

static void ChiSqGoFUpdate(Vector inputs[], AggregateInputData &, idx_t, Vector &state_vector, idx_t count) {
	UnifiedVectorFormat cdata;
	inputs[0].ToUnifiedFormat(count, cdata);

	auto states = FlatVector::GetData<ChiSqGoFState *>(state_vector);
	auto cvals = UnifiedVectorFormat::GetData<string_t>(cdata);

	for (idx_t i = 0; i < count; i++) {
		auto c_idx = cdata.sel->get_index(i);
		if (!cdata.validity.RowIsValid(c_idx)) {
			continue;
		}
		auto &state = *states[i];
		if (!state.counts) {
			state.counts = new std::unordered_map<std::string, int64_t>();
		}
		std::string key(cvals[c_idx].GetData(), cvals[c_idx].GetSize());
		(*state.counts)[key]++;
	}
}

static void ChiSqGoFCombine(Vector &source, Vector &target, AggregateInputData &, idx_t count) {
	auto src = FlatVector::GetData<ChiSqGoFState *>(source);
	auto tgt = FlatVector::GetData<ChiSqGoFState *>(target);
	for (idx_t i = 0; i < count; i++) {
		auto &s = *src[i];
		auto &t = *tgt[i];
		if (!s.counts) {
			continue;
		}
		if (!t.counts) {
			t.counts = new std::unordered_map<std::string, int64_t>();
		}
		for (auto &kv : *s.counts) {
			(*t.counts)[kv.first] += kv.second;
		}
	}
}

static void ChiSqGoFDestroy(Vector &state_vector, AggregateInputData &, idx_t count) {
	auto states = FlatVector::GetData<ChiSqGoFState *>(state_vector);
	for (idx_t i = 0; i < count; i++) {
		delete states[i]->counts;
		states[i]->counts = nullptr;
	}
}

static void ChiSqGoFFinalize(Vector &state_vector, AggregateInputData &, Vector &result, idx_t count, idx_t offset) {
	auto states = FlatVector::GetData<ChiSqGoFState *>(state_vector);
	auto &children = StructVector::GetEntries(result);

	for (idx_t i = 0; i < count; i++) {
		auto &state = *states[i];
		auto idx = i + offset;

		if (!state.counts || state.counts->size() < 2) {
			// GoF requires at least 2 categories.
			FlatVector::SetNull(result, idx, true);
			continue;
		}

		int64_t k = static_cast<int64_t>(state.counts->size());
		int64_t n = 0;
		for (auto &kv : *state.counts) {
			n += kv.second;
		}
		if (n <= 0) {
			FlatVector::SetNull(result, idx, true);
			continue;
		}
		double expected = static_cast<double>(n) / static_cast<double>(k);
		double chi_square = 0.0;
		for (auto &kv : *state.counts) {
			double diff = static_cast<double>(kv.second) - expected;
			chi_square += diff * diff / expected;
		}
		double df = static_cast<double>(k - 1);
		double p_value = 1.0 - stats_duck::ChiSquareCDF(chi_square, df);

		FlatVector::GetData<string_t>(*children[0])[idx] =
		    StringVector::AddString(*children[0], "Chi-square Goodness-of-Fit (uniform)");
		FlatVector::GetData<double>(*children[1])[idx] = chi_square;
		FlatVector::GetData<double>(*children[2])[idx] = df;
		FlatVector::GetData<double>(*children[3])[idx] = p_value;
		FlatVector::GetData<int64_t>(*children[4])[idx] = n;
		FlatVector::GetData<int64_t>(*children[5])[idx] = k;
	}
}

} // namespace

void RegisterChiSquareTests(ExtensionLoader &loader) {
	AggregateFunction ind_fn("chisq_independence", {LogicalType::VARCHAR, LogicalType::VARCHAR},
	                          ChiSqIndependenceResultType(), AggregateFunction::StateSize<ChiSqIndependenceState>,
	                          ChiSqIndInit, ChiSqIndUpdate, ChiSqIndCombine, ChiSqIndFinalize,
	                          FunctionNullHandling::SPECIAL_HANDLING, nullptr, nullptr, ChiSqIndDestroy);
	loader.RegisterFunction(ind_fn);

	AggregateFunction gof_fn("chisq_goodness_of_fit", {LogicalType::VARCHAR}, ChiSqGoFResultType(),
	                          AggregateFunction::StateSize<ChiSqGoFState>, ChiSqGoFInit, ChiSqGoFUpdate, ChiSqGoFCombine,
	                          ChiSqGoFFinalize, FunctionNullHandling::SPECIAL_HANDLING, nullptr, nullptr,
	                          ChiSqGoFDestroy);
	loader.RegisterFunction(gof_fn);
}

} // namespace duckdb
