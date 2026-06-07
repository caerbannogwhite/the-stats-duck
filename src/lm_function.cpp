#include "lm_function.hpp"

#include "distributions.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

#include <cmath>
#include <limits>
#include <sstream>
#include <string>
#include <vector>

namespace duckdb {

namespace {

//===--------------------------------------------------------------------===//
// Helpers shared by both lm and lm_summary.
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

//! Same numeric kind check used by corr_matrix / table_one — everything we can
//! cast to DOUBLE without losing the intent of the column.
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
// Linear-algebra primitives (small dense — p is rarely above a few dozen).
// Row-major storage: M[i,j] = M[i*ncols + j].
//===--------------------------------------------------------------------===//

//! In-place Cholesky decomposition of a symmetric positive-definite matrix A
//! (size n × n). On success returns true and overwrites the lower triangle
//! with L such that A = L·L'. On failure (A not positive-definite, e.g. due
//! to perfectly collinear predictors) returns false.
static bool CholeskyDecompose(std::vector<double> &A, idx_t n) {
	for (idx_t j = 0; j < n; j++) {
		double diag = A[j * n + j];
		for (idx_t k = 0; k < j; k++) {
			diag -= A[j * n + k] * A[j * n + k];
		}
		if (diag <= 0.0 || !std::isfinite(diag)) {
			return false;
		}
		A[j * n + j] = std::sqrt(diag);
		for (idx_t i = j + 1; i < n; i++) {
			double s = A[i * n + j];
			for (idx_t k = 0; k < j; k++) {
				s -= A[i * n + k] * A[j * n + k];
			}
			A[i * n + j] = s / A[j * n + j];
		}
	}
	return true;
}

//! Solve LL'·x = b given L (lower triangle of A in row-major form). Forward
//! then backward substitution.
static void CholeskySolve(const std::vector<double> &L, const std::vector<double> &b,
                          std::vector<double> &x, idx_t n) {
	std::vector<double> z(n, 0.0);
	// Forward: L·z = b
	for (idx_t i = 0; i < n; i++) {
		double s = b[i];
		for (idx_t j = 0; j < i; j++) {
			s -= L[i * n + j] * z[j];
		}
		z[i] = s / L[i * n + i];
	}
	// Backward: L'·x = z
	x.assign(n, 0.0);
	for (idx_t ii = 0; ii < n; ii++) {
		idx_t i = n - 1 - ii;
		double s = z[i];
		for (idx_t j = i + 1; j < n; j++) {
			s -= L[j * n + i] * x[j];
		}
		x[i] = s / L[i * n + i];
	}
}

//===--------------------------------------------------------------------===//
// Fit results.
//===--------------------------------------------------------------------===//

struct LmFit {
	idx_t n;                          // number of complete-case rows
	idx_t p;                          // number of predictors (excluding intercept)
	std::vector<std::string> terms;   // length p + 1 — "(Intercept)" then each x
	std::vector<double> beta;         // length p + 1
	std::vector<double> std_error;    // length p + 1
	std::vector<double> t_statistic;  // length p + 1
	std::vector<double> p_value;      // length p + 1
	double r_squared;
	double adj_r_squared;
	double f_statistic;
	double f_p_value;
	double sigma;                     // residual standard error
	idx_t df_residual;                // n - p - 1
	idx_t df_model;                   // p
	bool ok;                          // false → fit failed (insufficient data / singular)
	std::string error;                // populated when ok = false
};

//===--------------------------------------------------------------------===//
// Materialize y + X by streaming the source table once via an internal
// Connection. Filters all rows with NULL in y or any x (complete-case).
//===--------------------------------------------------------------------===//

struct YXBuffers {
	std::vector<double> y;
	std::vector<std::vector<double>> x; // x[j] is column j of X (size n)
};

static YXBuffers MaterializeYX(Connection &conn, const std::string &table, const std::string &y_col,
                               const std::vector<std::string> &x_cols) {
	std::stringstream sql;
	sql << "SELECT " << QuoteIdent(y_col) << "::DOUBLE";
	for (auto &xc : x_cols) {
		sql << ", " << QuoteIdent(xc) << "::DOUBLE";
	}
	sql << " FROM " << QuoteIdent(table);
	sql << " WHERE " << QuoteIdent(y_col) << " IS NOT NULL";
	for (auto &xc : x_cols) {
		sql << " AND " << QuoteIdent(xc) << " IS NOT NULL";
	}
	auto result = conn.Query(sql.str());
	if (result->HasError()) {
		throw InvalidInputException("lm: failed to materialize data — %s", result->GetError());
	}
	YXBuffers buf;
	buf.x.assign(x_cols.size(), {});
	while (true) {
		auto chunk = result->Fetch();
		if (!chunk || chunk->size() == 0) {
			break;
		}
		auto y_vec = FlatVector::GetData<double>(chunk->data[0]);
		for (idx_t i = 0; i < chunk->size(); i++) {
			buf.y.push_back(y_vec[i]);
		}
		for (idx_t j = 0; j < x_cols.size(); j++) {
			auto x_vec = FlatVector::GetData<double>(chunk->data[1 + j]);
			for (idx_t i = 0; i < chunk->size(); i++) {
				buf.x[j].push_back(x_vec[i]);
			}
		}
	}
	return buf;
}

//===--------------------------------------------------------------------===//
// FitOls — OLS fit via Cholesky on X'X. Populates every field of LmFit.
//===--------------------------------------------------------------------===//

static LmFit FitOls(Connection &conn, const std::string &table, const std::string &y_col,
                    const std::vector<std::string> &x_cols) {
	LmFit fit;
	fit.ok = false;
	fit.p = x_cols.size();
	fit.terms.reserve(fit.p + 1);
	fit.terms.emplace_back("(Intercept)");
	for (auto &xc : x_cols) {
		fit.terms.push_back(xc);
	}

	auto buf = MaterializeYX(conn, table, y_col, x_cols);
	fit.n = buf.y.size();
	idx_t k = fit.p + 1; // number of parameters

	if (fit.n <= k) {
		fit.error = StringUtil::Format(
		    "lm: need n > p + 1 (got n=%llu, p=%llu); not enough complete-case rows",
		    static_cast<unsigned long long>(fit.n), static_cast<unsigned long long>(fit.p));
		return fit;
	}

	// Build A = X'X and b = X'y. X has an implicit intercept column (all 1s)
	// at index 0; predictor columns occupy indices 1..p. We use row-major
	// storage: A[i*k + j] is the (i, j) entry.
	std::vector<double> A(k * k, 0.0);
	std::vector<double> b(k, 0.0);
	for (idx_t row = 0; row < fit.n; row++) {
		// row vector x_row of length k: [1, x_1[row], x_2[row], ...]
		// Update upper triangle (then symmetrize below).
		double y_v = buf.y[row];
		// First: A[0][*] and b[0]
		A[0 * k + 0] += 1.0;
		b[0] += y_v;
		for (idx_t j = 1; j < k; j++) {
			double xj = buf.x[j - 1][row];
			A[0 * k + j] += xj;
			A[j * k + j] += xj * xj;
			b[j] += xj * y_v;
			for (idx_t i = 1; i < j; i++) {
				double xi = buf.x[i - 1][row];
				A[i * k + j] += xi * xj;
			}
		}
	}
	// Symmetrize A (only upper triangle was populated above).
	for (idx_t i = 0; i < k; i++) {
		for (idx_t j = 0; j < i; j++) {
			A[i * k + j] = A[j * k + i];
		}
	}

	// Cholesky decompose A. A copy is kept around so we can recover the
	// variance-covariance matrix below — the decomposition writes over A.
	std::vector<double> L = A;
	if (!CholeskyDecompose(L, k)) {
		fit.error =
		    "lm: design matrix is singular (X'X not positive-definite); "
		    "check for perfectly collinear predictors or a constant column";
		return fit;
	}

	// Solve LL'·β = b
	CholeskySolve(L, b, fit.beta, k);

	// Compute residuals e = y - X·β and RSS.
	double rss = 0.0;
	double y_sum = 0.0;
	for (idx_t row = 0; row < fit.n; row++) {
		double yhat = fit.beta[0];
		for (idx_t j = 1; j < k; j++) {
			yhat += fit.beta[j] * buf.x[j - 1][row];
		}
		double e = buf.y[row] - yhat;
		rss += e * e;
		y_sum += buf.y[row];
	}
	double y_mean = y_sum / static_cast<double>(fit.n);
	double tss = 0.0;
	for (idx_t row = 0; row < fit.n; row++) {
		double dy = buf.y[row] - y_mean;
		tss += dy * dy;
	}

	fit.df_model = fit.p;
	fit.df_residual = fit.n - k;
	double sigma2 = rss / static_cast<double>(fit.df_residual);
	fit.sigma = std::sqrt(sigma2);

	// Compute the diagonal of A⁻¹ by solving A·v_i = e_i for each i, taking
	// v_i[i]. Same Cholesky factor; O(k) solves of O(k²) each.
	fit.std_error.assign(k, 0.0);
	fit.t_statistic.assign(k, 0.0);
	fit.p_value.assign(k, 0.0);
	std::vector<double> ei(k, 0.0);
	std::vector<double> vi(k, 0.0);
	for (idx_t i = 0; i < k; i++) {
		std::fill(ei.begin(), ei.end(), 0.0);
		ei[i] = 1.0;
		CholeskySolve(L, ei, vi, k);
		double var_ii = sigma2 * vi[i];
		fit.std_error[i] = var_ii > 0.0 ? std::sqrt(var_ii) : 0.0;
		if (fit.std_error[i] > 0.0) {
			fit.t_statistic[i] = fit.beta[i] / fit.std_error[i];
			double abs_t = std::fabs(fit.t_statistic[i]);
			// Two-sided t-test p-value: 2 · P(T > |t|) on df_residual.
			double tail = 1.0 - stats_duck::StudentTCDF(abs_t, static_cast<double>(fit.df_residual));
			fit.p_value[i] = 2.0 * tail;
		} else {
			fit.t_statistic[i] = std::numeric_limits<double>::quiet_NaN();
			fit.p_value[i] = std::numeric_limits<double>::quiet_NaN();
		}
	}

	// Model-level statistics.
	if (tss > 0.0) {
		fit.r_squared = 1.0 - rss / tss;
		double n_d = static_cast<double>(fit.n);
		fit.adj_r_squared =
		    1.0 - (1.0 - fit.r_squared) * (n_d - 1.0) / static_cast<double>(fit.df_residual);
	} else {
		fit.r_squared = std::numeric_limits<double>::quiet_NaN();
		fit.adj_r_squared = std::numeric_limits<double>::quiet_NaN();
	}
	// F-statistic for the joint hypothesis β_1 = ... = β_p = 0.
	if (fit.df_model > 0 && fit.df_residual > 0 && rss > 0.0 && tss > 0.0) {
		double ess = tss - rss;
		fit.f_statistic = (ess / static_cast<double>(fit.df_model)) /
		                  (rss / static_cast<double>(fit.df_residual));
		fit.f_p_value = 1.0 - stats_duck::FCDF(fit.f_statistic,
		                                       static_cast<double>(fit.df_model),
		                                       static_cast<double>(fit.df_residual));
	} else {
		fit.f_statistic = std::numeric_limits<double>::quiet_NaN();
		fit.f_p_value = std::numeric_limits<double>::quiet_NaN();
	}

	fit.ok = true;
	return fit;
}

//===--------------------------------------------------------------------===//
// Shared bind data — both lm and lm_summary need the same trio (table, y, x).
//===--------------------------------------------------------------------===//

struct LmBindData : public TableFunctionData {
	std::string data_table;
	std::string y_col;
	std::vector<std::string> x_cols;
	bool is_summary;
};

static unique_ptr<FunctionData> LmBindCommon(ClientContext &context, TableFunctionBindInput &input,
                                              bool is_summary) {
	auto bd = make_uniq<LmBindData>();
	bd->is_summary = is_summary;
	const char *fname = is_summary ? "lm_summary" : "lm";

	if (input.inputs.empty() || input.inputs[0].IsNull()) {
		throw BinderException("%s: 'data' (source table name) is required", fname);
	}
	bd->data_table = input.inputs[0].GetValue<string>();

	auto it_y = input.named_parameters.find("y");
	if (it_y == input.named_parameters.end() || it_y->second.IsNull()) {
		throw BinderException("%s: 'y' (response column name) is required", fname);
	}
	bd->y_col = it_y->second.GetValue<string>();

	auto it_x = input.named_parameters.find("x");
	if (it_x == input.named_parameters.end() || it_x->second.IsNull()) {
		throw BinderException("%s: 'x' (predictor list) is required", fname);
	}
	auto &x_children = ListValue::GetChildren(it_x->second);
	for (auto &v : x_children) {
		if (v.IsNull()) {
			throw BinderException("%s: 'x' list entries must not be NULL", fname);
		}
		bd->x_cols.push_back(v.GetValue<string>());
	}
	if (bd->x_cols.empty()) {
		throw BinderException("%s: 'x' must contain at least one predictor column", fname);
	}

	// Catalog lookup — y and every x must exist and be numeric.
	auto &catalog = Catalog::GetCatalog(context, INVALID_CATALOG);
	auto entry = catalog.GetEntry(context, CatalogType::TABLE_ENTRY, DEFAULT_SCHEMA,
	                              bd->data_table, OnEntryNotFound::THROW_EXCEPTION);
	auto &tbl = entry->Cast<TableCatalogEntry>();
	auto &cols = tbl.GetColumns();

	auto check_col = [&](const std::string &c, const char *role) {
		if (!cols.ColumnExists(c)) {
			throw BinderException("%s: %s column '%s' not found in table '%s'", fname, role, c,
			                      bd->data_table);
		}
		auto &col = cols.GetColumn(c);
		if (!IsNumericKind(col.GetType().id())) {
			throw BinderException(
			    "%s: %s column '%s' is not numeric (type %s); OLS requires numeric columns",
			    fname, role, c, col.GetType().ToString());
		}
	};
	check_col(bd->y_col, "response");
	for (auto &xc : bd->x_cols) {
		check_col(xc, "predictor");
	}
	return std::move(bd);
}

static unique_ptr<FunctionData> LmBind(ClientContext &context, TableFunctionBindInput &input,
                                       vector<LogicalType> &return_types, vector<string> &names) {
	auto bd = LmBindCommon(context, input, /*is_summary=*/false);
	names = {"term", "estimate", "std_error", "t_statistic", "p_value"};
	return_types = {LogicalType::VARCHAR, LogicalType::DOUBLE, LogicalType::DOUBLE,
	                LogicalType::DOUBLE, LogicalType::DOUBLE};
	return bd;
}

static unique_ptr<FunctionData> LmSummaryBind(ClientContext &context, TableFunctionBindInput &input,
                                              vector<LogicalType> &return_types,
                                              vector<string> &names) {
	auto bd = LmBindCommon(context, input, /*is_summary=*/true);
	names = {"r_squared",   "adj_r_squared", "f_statistic", "f_p_value",
	         "df_model",    "df_residual",   "sigma",       "n"};
	return_types = {LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE,
	                LogicalType::DOUBLE, LogicalType::BIGINT, LogicalType::BIGINT,
	                LogicalType::DOUBLE, LogicalType::BIGINT};
	return bd;
}

//===--------------------------------------------------------------------===//
// Global state — holds the fit result and a cursor over emitted rows.
//===--------------------------------------------------------------------===//

struct LmGlobalState : public GlobalTableFunctionState {
	LmFit fit;
	idx_t cursor = 0;
};

static unique_ptr<GlobalTableFunctionState> LmInitGlobal(ClientContext &context,
                                                         TableFunctionInitInput &input) {
	auto &bd = input.bind_data->Cast<LmBindData>();
	auto state = make_uniq<LmGlobalState>();
	Connection conn(*context.db);
	state->fit = FitOls(conn, bd.data_table, bd.y_col, bd.x_cols);
	if (!state->fit.ok) {
		throw InvalidInputException(state->fit.error);
	}
	return std::move(state);
}

//===--------------------------------------------------------------------===//
// Execute — stream pre-computed rows.
//===--------------------------------------------------------------------===//

static inline void SetDoubleOrNull(DataChunk &output, idx_t col, idx_t row, double v) {
	if (std::isnan(v)) {
		FlatVector::SetNull(output.data[col], row, true);
	} else {
		output.SetValue(col, row, Value::DOUBLE(v));
	}
}

static void LmExecute(ClientContext &, TableFunctionInput &input, DataChunk &output) {
	auto &state = input.global_state->Cast<LmGlobalState>();
	idx_t emitted = 0;
	while (state.cursor < state.fit.terms.size() && emitted < STANDARD_VECTOR_SIZE) {
		idx_t i = state.cursor++;
		output.SetValue(0, emitted, Value(state.fit.terms[i]));
		SetDoubleOrNull(output, 1, emitted, state.fit.beta[i]);
		SetDoubleOrNull(output, 2, emitted, state.fit.std_error[i]);
		SetDoubleOrNull(output, 3, emitted, state.fit.t_statistic[i]);
		SetDoubleOrNull(output, 4, emitted, state.fit.p_value[i]);
		emitted++;
	}
	output.SetCardinality(emitted);
}

static void LmSummaryExecute(ClientContext &, TableFunctionInput &input, DataChunk &output) {
	auto &state = input.global_state->Cast<LmGlobalState>();
	if (state.cursor > 0) {
		output.SetCardinality(0);
		return;
	}
	state.cursor = 1;
	SetDoubleOrNull(output, 0, 0, state.fit.r_squared);
	SetDoubleOrNull(output, 1, 0, state.fit.adj_r_squared);
	SetDoubleOrNull(output, 2, 0, state.fit.f_statistic);
	SetDoubleOrNull(output, 3, 0, state.fit.f_p_value);
	output.SetValue(4, 0, Value::BIGINT(static_cast<int64_t>(state.fit.df_model)));
	output.SetValue(5, 0, Value::BIGINT(static_cast<int64_t>(state.fit.df_residual)));
	SetDoubleOrNull(output, 6, 0, state.fit.sigma);
	output.SetValue(7, 0, Value::BIGINT(static_cast<int64_t>(state.fit.n)));
	output.SetCardinality(1);
}

} // namespace

void RegisterLm(ExtensionLoader &loader) {
	{
		TableFunction fn("lm", {LogicalType::VARCHAR}, LmExecute, LmBind, LmInitGlobal);
		fn.named_parameters["y"] = LogicalType::VARCHAR;
		fn.named_parameters["x"] = LogicalType::LIST(LogicalType::VARCHAR);
		loader.RegisterFunction(fn);
	}
	{
		TableFunction fn("lm_summary", {LogicalType::VARCHAR}, LmSummaryExecute, LmSummaryBind,
		                 LmInitGlobal);
		fn.named_parameters["y"] = LogicalType::VARCHAR;
		fn.named_parameters["x"] = LogicalType::LIST(LogicalType::VARCHAR);
		loader.RegisterFunction(fn);
	}
}

} // namespace duckdb
