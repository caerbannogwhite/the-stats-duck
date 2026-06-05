#include "table_one_function.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

#include <algorithm>
#include <cmath>
#include <cstdio>
#include <sstream>
#include <string>
#include <vector>

namespace duckdb {

// table_one — Table-1 style descriptives summary, MVP shape (v0.4).
//
// At bind time we look up the source table in the catalog, classify each
// declared variable as numeric or categorical from its column type, and
// stash a (mostly schema-only) BindData. At init-global time we open an
// internal Connection and run one summary query per (variable, group)
// pair, formatting results into pre-built output rows. The execute step
// just streams those rows out STANDARD_VECTOR_SIZE at a time.
//
// Long-format output makes the schema static (DuckDB table functions
// don't love dynamic columns) and lets clients pivot to wide for display.

namespace {

// ── BindData / state ───────────────────────────────────────────────────────

struct TableOneRow {
	std::string variable;
	std::string level; // empty → emitted as NULL for numeric rows
	std::string statistic;
	std::string stratum; // "Overall" or a `by`-column value
	std::string display;
	// Between-group p-value: NaN means "no test applicable" → emitted as NULL.
	// The same value is stamped on every row of the same variable so a PIVOT
	// can pick it up with FIRST(p_value). NULL when `by` is unset / single
	// stratum or when the underlying test fails (e.g. zero variance, too few
	// samples).
	double p_value;
	// Between-group effect size: η² for numeric (from ANOVA's eta_squared);
	// Cramér's V for categorical (sqrt(χ²/(n · (min(r,c)-1)))). NaN under the
	// same conditions as p_value. Same uniform name across both — the
	// variable's row's `statistic` field implicitly tells the consumer which.
	double effect_size;

	// Explicit constructor so 5-arg brace-init calls (the existing pattern in
	// EmitNumericRows / EmitCategoricalRows) keep working with p_value and
	// effect_size defaulted to NaN. A C++17 NSDMI default would also work
	// under MSVC, but emscripten's libc++ rejects brace-init of an aggregate
	// with NSDMIs when the brace list is shorter than the field count — the
	// constructor form is portable across both toolchains.
	TableOneRow(std::string variable, std::string level, std::string statistic,
	            std::string stratum, std::string display,
	            double p_value = std::nan(""),
	            double effect_size = std::nan(""))
	    : variable(std::move(variable)), level(std::move(level)),
	      statistic(std::move(statistic)), stratum(std::move(stratum)),
	      display(std::move(display)), p_value(p_value), effect_size(effect_size) {
	}
};

struct TableOneBindData : public TableFunctionData {
	// Parsed inputs
	std::string data_table;            // unqualified table name (catalog resolution happens here)
	std::vector<std::string> variables;
	std::vector<std::string> by_columns; // empty → no grouping; "Overall" only.
	                                     // Multi-column = Cartesian product of distinct
	                                     // value tuples; stratum label joins with " / ".
	// Classified per variable (filled at bind time)
	std::vector<bool> is_numeric;
};

struct TableOneGlobalState : public GlobalTableFunctionState {
	std::vector<TableOneRow> rows;
	idx_t cursor = 0;
};

// ── Helpers ────────────────────────────────────────────────────────────────

//! Identifier quoting: wrap an identifier in double quotes and double up any
//! existing double quotes inside. Used for both table and column names so
//! catalog lookups with special characters (or names that shadow keywords)
//! survive the round-trip through generated SQL.
static std::string QuoteIdent(const std::string &raw) {
	std::string out;
	out.reserve(raw.size() + 2);
	out.push_back('"');
	for (char c : raw) {
		if (c == '"') {
			out.push_back('"');
		}
		out.push_back(c);
	}
	out.push_back('"');
	return out;
}

//! Single-quote a value to embed inside a SQL literal. Doubles up any
//! single quotes already present.
static std::string QuoteString(const std::string &raw) {
	std::string out;
	out.reserve(raw.size() + 2);
	out.push_back('\'');
	for (char c : raw) {
		if (c == '\'') {
			out.push_back('\'');
		}
		out.push_back(c);
	}
	out.push_back('\'');
	return out;
}

//! Classify a DuckDB LogicalType as numeric for table_one's purposes.
//! INTEGER/FLOAT family is numeric; everything else (VARCHAR, BOOLEAN,
//! ENUM, BLOB, dates, etc.) gets the categorical treatment.
static bool IsNumericKind(LogicalTypeId tid) {
	switch (tid) {
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::HUGEINT:
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::UHUGEINT:
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
	case LogicalTypeId::DECIMAL:
		return true;
	default:
		return false;
	}
}

static std::string FormatInt(int64_t v) {
	return std::to_string(v);
}

static std::string FormatDouble1(double v) {
	char buf[64];
	std::snprintf(buf, sizeof(buf), "%.1f", v);
	return buf;
}

static std::string FormatPercent(double n, double total) {
	if (total <= 0.0) {
		return "0 (0.0%)";
	}
	char buf[64];
	std::snprintf(buf, sizeof(buf), "%.0f (%.1f%%)", n, 100.0 * n / total);
	return buf;
}

// ── Bind ───────────────────────────────────────────────────────────────────

static unique_ptr<FunctionData> TableOneBind(ClientContext &context, TableFunctionBindInput &input,
                                             vector<LogicalType> &return_types,
                                             vector<string> &names) {
	auto bd = make_uniq<TableOneBindData>();

	// Positional arg 0: data table name (VARCHAR).
	if (input.inputs.empty() || input.inputs[0].IsNull()) {
		throw BinderException("table_one: 'data' (source table name) is required");
	}
	bd->data_table = input.inputs[0].GetValue<string>();

	// Named param: variables LIST<VARCHAR> (required).
	auto it_vars = input.named_parameters.find("variables");
	if (it_vars == input.named_parameters.end() || it_vars->second.IsNull()) {
		throw BinderException("table_one: 'variables' list is required");
	}
	auto &vars_val = it_vars->second;
	auto &vars_children = ListValue::GetChildren(vars_val);
	for (auto &v : vars_children) {
		bd->variables.push_back(v.GetValue<string>());
	}
	if (bd->variables.empty()) {
		throw BinderException("table_one: 'variables' must not be empty");
	}

	// Named param: by LIST<VARCHAR> (optional). One element → single-column
	// stratification (the v0.4 behaviour, now spelled `by := ['arm']`). Multiple
	// elements → Cartesian product of distinct value tuples across the columns;
	// the stratum label joins values with " / " in the declared column order.
	auto it_by = input.named_parameters.find("by");
	if (it_by != input.named_parameters.end() && !it_by->second.IsNull()) {
		auto &by_children = ListValue::GetChildren(it_by->second);
		for (auto &c : by_children) {
			if (c.IsNull()) {
				throw BinderException("table_one: 'by' list entries must not be NULL");
			}
			bd->by_columns.push_back(c.GetValue<string>());
		}
	}

	// Named params: force_categorical / force_numerical — per-variable
	// classification overrides. Integer columns that are really categorical
	// (`stage ∈ {1,2,3,4}`, treatment codes, etc.) need force_categorical;
	// VARCHAR columns holding numeric strings need force_numerical. Each entry
	// must appear in `variables` so typos surface as bind errors rather than
	// silent no-ops; the two lists must not overlap.
	std::vector<std::string> force_categorical;
	std::vector<std::string> force_numerical;
	auto read_override = [&](const char *param_name, std::vector<std::string> &out) {
		auto it = input.named_parameters.find(param_name);
		if (it == input.named_parameters.end() || it->second.IsNull()) {
			return;
		}
		auto &children = ListValue::GetChildren(it->second);
		for (auto &c : children) {
			if (c.IsNull()) {
				throw BinderException("table_one: '%s' list entries must not be NULL",
				                      param_name);
			}
			out.push_back(c.GetValue<string>());
		}
	};
	read_override("force_categorical", force_categorical);
	read_override("force_numerical", force_numerical);
	for (auto &v : force_categorical) {
		if (std::find(force_numerical.begin(), force_numerical.end(), v) !=
		    force_numerical.end()) {
			throw BinderException("table_one: '%s' is listed in both force_categorical "
			                      "and force_numerical", v);
		}
		if (std::find(bd->variables.begin(), bd->variables.end(), v) == bd->variables.end()) {
			throw BinderException("table_one: force_categorical entry '%s' is not in 'variables'",
			                      v);
		}
	}
	for (auto &v : force_numerical) {
		if (std::find(bd->variables.begin(), bd->variables.end(), v) == bd->variables.end()) {
			throw BinderException("table_one: force_numerical entry '%s' is not in 'variables'",
			                      v);
		}
	}

	// Catalog lookup — get column types so we can classify each variable.
	auto &catalog = Catalog::GetCatalog(context, INVALID_CATALOG);
	auto &schema = DEFAULT_SCHEMA;
	auto entry = catalog.GetEntry(context, CatalogType::TABLE_ENTRY, schema, bd->data_table,
	                              OnEntryNotFound::THROW_EXCEPTION);
	auto &tbl = entry->Cast<TableCatalogEntry>();
	auto &cols = tbl.GetColumns();

	for (auto &v : bd->variables) {
		if (!cols.ColumnExists(v)) {
			throw BinderException("table_one: column '%s' not found in table '%s'",
			                      v, bd->data_table);
		}
		auto &col = cols.GetColumn(v);
		bool numeric = IsNumericKind(col.GetType().id());
		if (std::find(force_categorical.begin(), force_categorical.end(), v) !=
		    force_categorical.end()) {
			numeric = false;
		} else if (std::find(force_numerical.begin(), force_numerical.end(), v) !=
		           force_numerical.end()) {
			numeric = true;
		}
		bd->is_numeric.push_back(numeric);
	}
	for (auto &b : bd->by_columns) {
		if (!cols.ColumnExists(b)) {
			throw BinderException("table_one: 'by' column '%s' not found in table '%s'",
			                      b, bd->data_table);
		}
	}

	// Output schema (static — see header comment).
	// p_value is the between-group test result for the variable; it is
	// repeated on every row of the same variable so a PIVOT can grab it via
	// FIRST(p_value). NULL when no `by` is set, when there's only one
	// stratum, or when the test failed.
	// effect_size is the matching effect-size magnitude (η² for numeric,
	// Cramér's V for categorical) — same repetition and NULL handling.
	names = {"variable", "level", "statistic", "stratum", "display", "p_value", "effect_size"};
	return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
	                LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::DOUBLE,
	                LogicalType::DOUBLE};
	return std::move(bd);
}

// ── Per-variable row generation (runs via an internal Connection) ──────────

//! Build the per-stratum WHERE fragment (without the leading " WHERE "). Empty
//! when `by_cols` is empty (the Overall stratum) — caller should skip emitting
//! a WHERE clause in that case.
static std::string BuildStratumPredicate(const std::vector<std::string> &by_cols,
                                         const std::vector<std::string> &group_values) {
	std::string out;
	for (size_t i = 0; i < by_cols.size(); i++) {
		if (i > 0) {
			out += " AND ";
		}
		out += QuoteIdent(by_cols[i]) + " = " + QuoteString(group_values[i]);
	}
	return out;
}

//! For a numeric variable, fetch summary stats per stratum and append the
//! formatted rows. If `group_values` is empty, the stratum label is "Overall"
//! and the WHERE clause is omitted; otherwise we restrict to rows where each
//! by-column equals its parallel value in `group_values`.
static void EmitNumericRows(Connection &conn, const std::string &table,
                            const std::string &var, const std::vector<std::string> &by_cols,
                            const std::vector<std::string> &group_values,
                            const std::string &stratum_label,
                            std::vector<TableOneRow> &out) {
	std::stringstream sql;
	sql << "SELECT (s).n, (s).n_missing, (s).mean, (s).sd, "
	       "(s).median, (s).q1, (s).q3, (s).min, (s).max "
	    << "FROM (SELECT summary_stats(" << QuoteIdent(var) << "::DOUBLE) AS s "
	    << "FROM " << QuoteIdent(table);
	if (!group_values.empty()) {
		sql << " WHERE " << BuildStratumPredicate(by_cols, group_values);
	}
	sql << ")";

	auto result = conn.Query(sql.str());
	if (result->HasError()) {
		throw InvalidInputException("table_one: failed to summarise '%s' (%s)", var,
		                            result->GetError());
	}
	auto chunk = result->Fetch();
	if (!chunk || chunk->size() == 0) {
		return; // empty group — skip silently
	}

	auto n = chunk->GetValue(0, 0).GetValue<int64_t>();
	auto n_missing = chunk->GetValue(1, 0).GetValue<int64_t>();
	bool stats_null = chunk->GetValue(2, 0).IsNull();

	out.push_back({var, "", "n", stratum_label, FormatInt(n)});
	out.push_back({var, "", "missing", stratum_label, FormatInt(n_missing)});
	if (stats_null) {
		// Not enough non-NULL data for moments / quantiles.
		out.push_back({var, "", "mean (sd)", stratum_label, "."});
		out.push_back({var, "", "median [q1, q3]", stratum_label, "."});
		out.push_back({var, "", "min, max", stratum_label, "."});
		return;
	}

	double mean = chunk->GetValue(2, 0).GetValue<double>();
	double sd = chunk->GetValue(3, 0).GetValue<double>();
	double median = chunk->GetValue(4, 0).GetValue<double>();
	double q1 = chunk->GetValue(5, 0).GetValue<double>();
	double q3 = chunk->GetValue(6, 0).GetValue<double>();
	double vmin = chunk->GetValue(7, 0).GetValue<double>();
	double vmax = chunk->GetValue(8, 0).GetValue<double>();

	out.push_back({var, "", "mean (sd)", stratum_label,
	               FormatDouble1(mean) + " (" + FormatDouble1(sd) + ")"});
	out.push_back({var, "", "median [q1, q3]", stratum_label,
	               FormatDouble1(median) + " [" + FormatDouble1(q1) + ", " +
	                   FormatDouble1(q3) + "]"});
	out.push_back({var, "", "min, max", stratum_label,
	               FormatDouble1(vmin) + ", " + FormatDouble1(vmax)});
}

//! For a categorical variable, emit one row per observed non-NULL level plus a
//! "Missing" level row at the end. The Missing row is always emitted (even when
//! the count is zero) so that downstream PIVOTs see a stable shape; users who
//! don't want it can filter with `WHERE level <> 'Missing'`. All `n (%)`
//! percentages share the same denominator — total rows in the stratum,
//! including the missing count — so the per-stratum level percentages sum to
//! 100%.
static void EmitCategoricalRows(Connection &conn, const std::string &table,
                                const std::string &var,
                                const std::vector<std::string> &by_cols,
                                const std::vector<std::string> &group_values,
                                const std::string &stratum_label,
                                std::vector<TableOneRow> &out) {
	std::string stratum_pred =
	    group_values.empty() ? std::string() : BuildStratumPredicate(by_cols, group_values);
	std::stringstream sql;
	sql << "SELECT " << QuoteIdent(var) << "::VARCHAR AS lvl, COUNT(*) AS n "
	    << "FROM " << QuoteIdent(table) << " WHERE " << QuoteIdent(var) << " IS NOT NULL";
	if (!stratum_pred.empty()) {
		sql << " AND " << stratum_pred;
	}
	sql << " GROUP BY 1 ORDER BY 1";

	std::stringstream missing_sql;
	missing_sql << "SELECT COUNT(*) FROM " << QuoteIdent(table) << " WHERE "
	            << QuoteIdent(var) << " IS NULL";
	if (!stratum_pred.empty()) {
		missing_sql << " AND " << stratum_pred;
	}

	auto result = conn.Query(sql.str());
	if (result->HasError()) {
		throw InvalidInputException("table_one: failed to tabulate '%s' (%s)", var,
		                            result->GetError());
	}

	struct Cell {
		std::string level;
		int64_t n;
	};
	std::vector<Cell> cells;
	int64_t nonmissing_total = 0;
	while (auto chunk = result->Fetch()) {
		for (idx_t i = 0; i < chunk->size(); i++) {
			std::string lvl = chunk->GetValue(0, i).ToString();
			int64_t n = chunk->GetValue(1, i).GetValue<int64_t>();
			cells.push_back({lvl, n});
			nonmissing_total += n;
		}
	}

	int64_t miss = 0;
	auto miss_result = conn.Query(missing_sql.str());
	if (miss_result->HasError()) {
		throw InvalidInputException("table_one: failed to count missing for '%s' (%s)", var,
		                            miss_result->GetError());
	}
	auto miss_chunk = miss_result->Fetch();
	if (miss_chunk && miss_chunk->size() > 0) {
		miss = miss_chunk->GetValue(0, 0).GetValue<int64_t>();
	}

	double denom = static_cast<double>(nonmissing_total + miss);
	for (auto &c : cells) {
		out.push_back({var, c.level, "n (%)", stratum_label,
		               FormatPercent(static_cast<double>(c.n), denom)});
	}
	out.push_back({var, "Missing", "n (%)", stratum_label,
	               FormatPercent(static_cast<double>(miss), denom)});
}

// ── Between-group p-value ──────────────────────────────────────────────────

//! Compute the between-group p-value for a single variable. Returns NaN
//! ("no result") when no test applies (no by-columns, fewer than 2 strata)
//! or when the underlying aggregate failed (e.g. zero variance, too few
//! samples for ANOVA / chi-square).
//!
//! Numeric variables → one-way ANOVA via `anova_oneway(value, group)`. Works
//! for 2 groups (where F = t² pooled) and 3+ groups uniformly.
//!
//! Categorical variables → chi-square independence via
//! `chisq_independence(row, col)` with the variable as rows and the (possibly
//! concatenated) `by` columns as cols.
//!
//! Multi-column `by` is folded into a single grouping string by concatenating
//! the columns with " / " — same separator used for stratum labels.
struct BetweenGroupStats {
	double p_value = std::nan("");
	double effect_size = std::nan("");
};

static BetweenGroupStats ComputeBetweenGroupStats(Connection &conn, const std::string &table,
                                                  const std::string &var, bool numeric,
                                                  const std::vector<std::string> &by_cols,
                                                  idx_t n_strata) {
	BetweenGroupStats stats;
	if (by_cols.empty() || n_strata < 2) {
		return stats;
	}

	// Build the grouping expression: single column verbatim, multi-column
	// concatenated with the same " / " separator used in stratum labels.
	std::string group_expr;
	if (by_cols.size() == 1) {
		group_expr = QuoteIdent(by_cols[0]) + "::VARCHAR";
	} else {
		for (size_t i = 0; i < by_cols.size(); i++) {
			if (i > 0) {
				group_expr += " || ' / ' || ";
			}
			group_expr += QuoteIdent(by_cols[i]) + "::VARCHAR";
		}
	}

	// Common WHERE: variable + every by-column non-NULL.
	std::string where_clause = QuoteIdent(var) + " IS NOT NULL";
	for (auto &b : by_cols) {
		where_clause += " AND " + QuoteIdent(b) + " IS NOT NULL";
	}

	// One SELECT pulls p_value and effect_size from the same aggregate struct,
	// so the aggregate runs once per variable rather than twice.
	//   Numeric: anova_oneway exposes p_value and eta_squared directly.
	//   Categorical: chisq_independence exposes chi_square, n, n_rows, n_cols;
	//     Cramér's V is √(χ²/(n · (min(r,c)-1))) computed inline.
	std::stringstream sql;
	if (numeric) {
		sql << "SELECT (s).p_value, (s).eta_squared FROM ("
		    << "SELECT anova_oneway(" << QuoteIdent(var) << "::DOUBLE, " << group_expr << ") AS s "
		    << "FROM " << QuoteIdent(table) << " WHERE " << where_clause << ")";
	} else {
		sql << "SELECT (s).p_value, "
		    << "CASE WHEN (s).n > 0 AND LEAST((s).n_rows, (s).n_cols) > 1 "
		    << "THEN sqrt((s).chi_square / ((s).n * (LEAST((s).n_rows, (s).n_cols) - 1))) "
		    << "ELSE NULL END "
		    << "FROM (SELECT chisq_independence(" << QuoteIdent(var) << "::VARCHAR, " << group_expr
		    << ") AS s FROM " << QuoteIdent(table) << " WHERE " << where_clause << ")";
	}

	auto result = conn.Query(sql.str());
	if (result->HasError()) {
		return stats; // test infeasible (e.g. zero variance, sparse 2x2) → NULL
	}
	auto chunk = result->Fetch();
	if (!chunk || chunk->size() == 0) {
		return stats;
	}
	auto p_v = chunk->GetValue(0, 0);
	if (!p_v.IsNull()) {
		stats.p_value = p_v.GetValue<double>();
	}
	auto eff = chunk->GetValue(1, 0);
	if (!eff.IsNull()) {
		stats.effect_size = eff.GetValue<double>();
	}
	return stats;
}

// ── InitGlobal: pre-compute every row ──────────────────────────────────────

static unique_ptr<GlobalTableFunctionState> TableOneInitGlobal(ClientContext &context,
                                                                TableFunctionInitInput &input) {
	auto &bd = input.bind_data->Cast<TableOneBindData>();
	auto state = make_uniq<TableOneGlobalState>();

	Connection conn(*context.db);

	// If `by` is set, discover the distinct tuples of by-column values up front
	// so we can iterate strata in a stable order. Rows where any by-column is
	// NULL are excluded from the breakdown (matching the v0.4 single-column
	// behaviour). Each tuple becomes its own stratum, labelled by joining the
	// values with " / " in declared order.
	std::vector<std::vector<std::string>> by_tuples;
	if (!bd.by_columns.empty()) {
		std::stringstream sql;
		sql << "SELECT DISTINCT ";
		for (size_t i = 0; i < bd.by_columns.size(); i++) {
			if (i > 0) {
				sql << ", ";
			}
			sql << QuoteIdent(bd.by_columns[i]) << "::VARCHAR";
		}
		sql << " FROM " << QuoteIdent(bd.data_table) << " WHERE ";
		for (size_t i = 0; i < bd.by_columns.size(); i++) {
			if (i > 0) {
				sql << " AND ";
			}
			sql << QuoteIdent(bd.by_columns[i]) << " IS NOT NULL";
		}
		sql << " ORDER BY ";
		for (size_t i = 0; i < bd.by_columns.size(); i++) {
			if (i > 0) {
				sql << ", ";
			}
			sql << (i + 1);
		}
		auto result = conn.Query(sql.str());
		if (result->HasError()) {
			throw InvalidInputException("table_one: failed to enumerate `by` values (%s)",
			                            result->GetError());
		}
		while (auto chunk = result->Fetch()) {
			for (idx_t i = 0; i < chunk->size(); i++) {
				std::vector<std::string> tup;
				for (idx_t c = 0; c < bd.by_columns.size(); c++) {
					tup.push_back(chunk->GetValue(c, i).ToString());
				}
				by_tuples.push_back(std::move(tup));
			}
		}
	}

	auto join_tuple = [](const std::vector<std::string> &tup) {
		std::string out;
		for (size_t i = 0; i < tup.size(); i++) {
			if (i > 0) {
				out += " / ";
			}
			out += tup[i];
		}
		return out;
	};

	// Group iteration order: Overall first, then each by-tuple in distinct order.
	auto emit_var = [&](idx_t var_idx) {
		const std::string &var = bd.variables[var_idx];
		bool numeric = bd.is_numeric[var_idx];

		size_t first_row = state->rows.size();

		// Overall
		std::vector<std::string> empty_values;
		if (numeric) {
			EmitNumericRows(conn, bd.data_table, var, bd.by_columns, empty_values, "Overall",
			                state->rows);
		} else {
			EmitCategoricalRows(conn, bd.data_table, var, bd.by_columns, empty_values, "Overall",
			                    state->rows);
		}
		// Per-stratum breakdown.
		for (auto &tup : by_tuples) {
			std::string label = join_tuple(tup);
			if (numeric) {
				EmitNumericRows(conn, bd.data_table, var, bd.by_columns, tup, label, state->rows);
			} else {
				EmitCategoricalRows(conn, bd.data_table, var, bd.by_columns, tup, label,
				                    state->rows);
			}
		}

		// Compute the between-group p-value and effect size once per variable,
		// then stamp both on every row we just emitted for this variable. NaN
		// sentinels → Execute will SetNull on those cells.
		auto stats = ComputeBetweenGroupStats(conn, bd.data_table, var, numeric,
		                                       bd.by_columns, by_tuples.size());
		for (size_t i = first_row; i < state->rows.size(); i++) {
			state->rows[i].p_value = stats.p_value;
			state->rows[i].effect_size = stats.effect_size;
		}
	};

	for (idx_t i = 0; i < bd.variables.size(); i++) {
		emit_var(i);
	}

	return std::move(state);
}

// ── Execute: stream pre-computed rows ──────────────────────────────────────

static void TableOneExecute(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &state = input.global_state->Cast<TableOneGlobalState>();
	idx_t emitted = 0;
	while (state.cursor < state.rows.size() && emitted < STANDARD_VECTOR_SIZE) {
		auto &row = state.rows[state.cursor++];
		output.SetValue(0, emitted, Value(row.variable));
		// level: empty string → NULL (numeric variables have no level)
		if (row.level.empty()) {
			FlatVector::SetNull(output.data[1], emitted, true);
		} else {
			output.SetValue(1, emitted, Value(row.level));
		}
		output.SetValue(2, emitted, Value(row.statistic));
		output.SetValue(3, emitted, Value(row.stratum));
		output.SetValue(4, emitted, Value(row.display));
		if (std::isnan(row.p_value)) {
			FlatVector::SetNull(output.data[5], emitted, true);
		} else {
			output.SetValue(5, emitted, Value::DOUBLE(row.p_value));
		}
		if (std::isnan(row.effect_size)) {
			FlatVector::SetNull(output.data[6], emitted, true);
		} else {
			output.SetValue(6, emitted, Value::DOUBLE(row.effect_size));
		}
		emitted++;
	}
	output.SetCardinality(emitted);
}

} // namespace

void RegisterTableOne(ExtensionLoader &loader) {
	TableFunction fn("table_one", {LogicalType::VARCHAR}, TableOneExecute, TableOneBind,
	                  TableOneInitGlobal);
	fn.named_parameters["variables"] = LogicalType::LIST(LogicalType::VARCHAR);
	fn.named_parameters["by"] = LogicalType::LIST(LogicalType::VARCHAR);
	fn.named_parameters["force_categorical"] = LogicalType::LIST(LogicalType::VARCHAR);
	fn.named_parameters["force_numerical"] = LogicalType::LIST(LogicalType::VARCHAR);
	loader.RegisterFunction(fn);
}

} // namespace duckdb
