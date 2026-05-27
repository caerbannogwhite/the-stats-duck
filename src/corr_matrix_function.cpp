#include "corr_matrix_function.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

#include <cmath>
#include <sstream>
#include <string>
#include <vector>

namespace duckdb {

namespace {

//===--------------------------------------------------------------------===//
// Method
//===--------------------------------------------------------------------===//

enum class CorrMethod {
	PEARSON,
	SPEARMAN,
	KENDALL,
};

static CorrMethod ParseMethod(const std::string &name) {
	if (name == "pearson") return CorrMethod::PEARSON;
	if (name == "spearman") return CorrMethod::SPEARMAN;
	if (name == "kendall") return CorrMethod::KENDALL;
	throw BinderException(
	    "corr_matrix: unknown method '%s' — supported: 'pearson', 'spearman', 'kendall'",
	    name);
}

//! Aggregate function name + the struct field that holds the correlation
//! coefficient for the chosen method. Pearson exposes it as `.r`, Spearman
//! as `.rho`, Kendall as `.tau` — we paper over the difference by aliasing
//! into a common `coef` output column.
struct MethodSql {
	const char *func;
	const char *coef_field;
};

static MethodSql MethodSqlOf(CorrMethod m) {
	switch (m) {
	case CorrMethod::PEARSON:
		return {"pearson_test", "r"};
	case CorrMethod::SPEARMAN:
		return {"spearman_test", "rho"};
	case CorrMethod::KENDALL:
		return {"kendall_test", "tau"};
	}
	return {"pearson_test", "r"}; // unreachable
}

//===--------------------------------------------------------------------===//
// State + bind data
//===--------------------------------------------------------------------===//

struct CorrMatrixRow {
	std::string row_var;
	std::string col_var;
	double coef;     // NaN → emitted as NULL
	double p_value;  // NaN → emitted as NULL
	int64_t n;       // negative → NULL (use -1 sentinel)

	CorrMatrixRow(std::string row_var, std::string col_var, double coef, double p_value, int64_t n)
	    : row_var(std::move(row_var)), col_var(std::move(col_var)), coef(coef), p_value(p_value),
	      n(n) {
	}
};

struct CorrMatrixBindData : public TableFunctionData {
	std::string data_table;
	std::vector<std::string> variables;
	CorrMethod method = CorrMethod::PEARSON;
};

struct CorrMatrixGlobalState : public GlobalTableFunctionState {
	std::vector<CorrMatrixRow> rows;
	idx_t cursor = 0;
};

//===--------------------------------------------------------------------===//
// Helpers
//===--------------------------------------------------------------------===//

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

//! Classification mirrors table_one's IsNumericKind. Anything we can cast to
//! DOUBLE safely is fair game for a correlation; everything else is rejected
//! at bind time so the user sees a clear error rather than a runtime cast.
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

//===--------------------------------------------------------------------===//
// Bind
//===--------------------------------------------------------------------===//

static unique_ptr<FunctionData> CorrMatrixBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types,
                                                vector<string> &names) {
	auto bd = make_uniq<CorrMatrixBindData>();

	if (input.inputs.empty() || input.inputs[0].IsNull()) {
		throw BinderException("corr_matrix: 'data' (source table name) is required");
	}
	bd->data_table = input.inputs[0].GetValue<string>();

	auto it_vars = input.named_parameters.find("variables");
	if (it_vars == input.named_parameters.end() || it_vars->second.IsNull()) {
		throw BinderException("corr_matrix: 'variables' list is required");
	}
	auto &vars_children = ListValue::GetChildren(it_vars->second);
	for (auto &v : vars_children) {
		if (v.IsNull()) {
			throw BinderException("corr_matrix: 'variables' list entries must not be NULL");
		}
		bd->variables.push_back(v.GetValue<string>());
	}
	if (bd->variables.size() < 2) {
		throw BinderException("corr_matrix: 'variables' must contain at least 2 columns");
	}

	auto it_method = input.named_parameters.find("method");
	if (it_method != input.named_parameters.end() && !it_method->second.IsNull()) {
		std::string method_str = StringUtil::Lower(it_method->second.GetValue<string>());
		bd->method = ParseMethod(method_str);
	}

	// Catalog lookup — confirm each variable exists and is numeric.
	auto &catalog = Catalog::GetCatalog(context, INVALID_CATALOG);
	auto &schema = DEFAULT_SCHEMA;
	auto entry = catalog.GetEntry(context, CatalogType::TABLE_ENTRY, schema, bd->data_table,
	                              OnEntryNotFound::THROW_EXCEPTION);
	auto &tbl = entry->Cast<TableCatalogEntry>();
	auto &cols = tbl.GetColumns();
	for (auto &v : bd->variables) {
		if (!cols.ColumnExists(v)) {
			throw BinderException("corr_matrix: column '%s' not found in table '%s'", v,
			                      bd->data_table);
		}
		auto &col = cols.GetColumn(v);
		if (!IsNumericKind(col.GetType().id())) {
			throw BinderException(
			    "corr_matrix: column '%s' is not numeric (type %s); "
			    "correlation requires numeric columns",
			    v, col.GetType().ToString());
		}
	}

	names = {"row_var", "col_var", "coef", "p_value", "n"};
	return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::DOUBLE,
	                LogicalType::DOUBLE, LogicalType::BIGINT};
	return std::move(bd);
}

//===--------------------------------------------------------------------===//
// InitGlobal — compute upper triangle once, mirror to fill the matrix
//===--------------------------------------------------------------------===//

static unique_ptr<GlobalTableFunctionState>
CorrMatrixInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	auto &bd = input.bind_data->Cast<CorrMatrixBindData>();
	auto state = make_uniq<CorrMatrixGlobalState>();

	Connection conn(*context.db);
	MethodSql msql = MethodSqlOf(bd.method);

	idx_t k = bd.variables.size();

	// Provisional storage: index by [row_idx][col_idx]. Filled on the upper
	// triangle and mirrored to the lower so emission order is deterministic.
	struct Cell {
		double coef;
		double p_value;
		int64_t n;
		bool set;
	};
	std::vector<std::vector<Cell>> grid(k, std::vector<Cell>(k, {std::nan(""), std::nan(""), -1, false}));

	auto run_pair = [&](idx_t i, idx_t j) {
		std::stringstream sql;
		sql << "SELECT (s)." << msql.coef_field << " AS coef, "
		    << "(s).p_value AS p, "
		    << "(s).n AS n "
		    << "FROM (SELECT " << msql.func << "(" << QuoteIdent(bd.variables[i])
		    << "::DOUBLE, " << QuoteIdent(bd.variables[j]) << "::DOUBLE) AS s "
		    << "FROM " << QuoteIdent(bd.data_table) << ")";
		auto result = conn.Query(sql.str());
		Cell cell{std::nan(""), std::nan(""), -1, true};
		if (!result->HasError()) {
			auto chunk = result->Fetch();
			if (chunk && chunk->size() > 0) {
				auto v_coef = chunk->GetValue(0, 0);
				auto v_p = chunk->GetValue(1, 0);
				auto v_n = chunk->GetValue(2, 0);
				if (!v_coef.IsNull()) {
					cell.coef = v_coef.GetValue<double>();
				}
				if (!v_p.IsNull()) {
					cell.p_value = v_p.GetValue<double>();
				}
				if (!v_n.IsNull()) {
					cell.n = v_n.GetValue<int64_t>();
				}
			}
		}
		grid[i][j] = cell;
		if (i != j) {
			// Correlations are symmetric for all three supported methods —
			// no need to recompute.
			grid[j][i] = cell;
		}
	};

	for (idx_t i = 0; i < k; i++) {
		for (idx_t j = i; j < k; j++) {
			run_pair(i, j);
		}
	}

	// Emit in declaration order so a PIVOT lands the rows / cols in the
	// user's chosen sequence.
	state->rows.reserve(k * k);
	for (idx_t i = 0; i < k; i++) {
		for (idx_t j = 0; j < k; j++) {
			auto &c = grid[i][j];
			state->rows.emplace_back(bd.variables[i], bd.variables[j], c.coef, c.p_value, c.n);
		}
	}

	return std::move(state);
}

//===--------------------------------------------------------------------===//
// Execute
//===--------------------------------------------------------------------===//

static void CorrMatrixExecute(ClientContext &, TableFunctionInput &input, DataChunk &output) {
	auto &state = input.global_state->Cast<CorrMatrixGlobalState>();
	idx_t emitted = 0;
	while (state.cursor < state.rows.size() && emitted < STANDARD_VECTOR_SIZE) {
		auto &row = state.rows[state.cursor++];
		output.SetValue(0, emitted, Value(row.row_var));
		output.SetValue(1, emitted, Value(row.col_var));
		if (std::isnan(row.coef)) {
			FlatVector::SetNull(output.data[2], emitted, true);
		} else {
			output.SetValue(2, emitted, Value::DOUBLE(row.coef));
		}
		if (std::isnan(row.p_value)) {
			FlatVector::SetNull(output.data[3], emitted, true);
		} else {
			output.SetValue(3, emitted, Value::DOUBLE(row.p_value));
		}
		if (row.n < 0) {
			FlatVector::SetNull(output.data[4], emitted, true);
		} else {
			output.SetValue(4, emitted, Value::BIGINT(row.n));
		}
		emitted++;
	}
	output.SetCardinality(emitted);
}

} // namespace

void RegisterCorrMatrix(ExtensionLoader &loader) {
	TableFunction fn("corr_matrix", {LogicalType::VARCHAR}, CorrMatrixExecute, CorrMatrixBind,
	                  CorrMatrixInitGlobal);
	fn.named_parameters["variables"] = LogicalType::LIST(LogicalType::VARCHAR);
	fn.named_parameters["method"] = LogicalType::VARCHAR;
	loader.RegisterFunction(fn);
}

} // namespace duckdb
