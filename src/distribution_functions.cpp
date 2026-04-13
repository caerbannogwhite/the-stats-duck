#include "distribution_functions.hpp"
#include "distributions.hpp"

#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/common/vector_operations/binary_executor.hpp"
#include "duckdb/common/vector_operations/ternary_executor.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {

// Scalar wrappers around stats_duck::NormalPDF / CDF / Quantile etc.
// Each distribution gets three SQL functions: d{name}, p{name}, q{name}
// following R's convention.
//
// Invalid parameters (e.g., negative sd, df <= 0, p outside [0,1]) are
// surfaced as NULL rather than raised errors so that a single bad input row
// doesn't abort an entire query.

namespace {

// Helper: run a stats_duck call safely. On exception, mark the output slot as
// NULL via the ValidityMask and return a zero sentinel. On NaN, also NULL.
template <typename CALL>
inline double SafeCall(CALL &&f, ValidityMask &mask, idx_t idx) {
	try {
		double r = f();
		if (std::isnan(r)) {
			mask.SetInvalid(idx);
			return 0.0;
		}
		return r;
	} catch (...) {
		mask.SetInvalid(idx);
		return 0.0;
	}
}

// ── Normal ──────────────────────────────────────────────────────────────────

static void DNormStdExec(DataChunk &args, ExpressionState &, Vector &result) {
	UnaryExecutor::ExecuteWithNulls<double, double>(
	    args.data[0], result, args.size(),
	    [](double x, ValidityMask &mask, idx_t idx) {
		    return SafeCall([&] { return stats_duck::NormalPDF(x); }, mask, idx);
	    });
}

static void DNorm2Exec(DataChunk &args, ExpressionState &, Vector &result) {
	BinaryExecutor::ExecuteWithNulls<double, double, double>(
	    args.data[0], args.data[1], result, args.size(),
	    [](double x, double mean, ValidityMask &mask, idx_t idx) {
		    return SafeCall([&] { return stats_duck::NormalPDF(x, mean); }, mask, idx);
	    });
}

static void DNorm3Exec(DataChunk &args, ExpressionState &, Vector &result) {
	TernaryExecutor::ExecuteWithNulls<double, double, double, double>(
	    args.data[0], args.data[1], args.data[2], result, args.size(),
	    [](double x, double mean, double sd, ValidityMask &mask, idx_t idx) {
		    return SafeCall([&] { return stats_duck::NormalPDF(x, mean, sd); }, mask, idx);
	    });
}

static void PNormStdExec(DataChunk &args, ExpressionState &, Vector &result) {
	UnaryExecutor::ExecuteWithNulls<double, double>(
	    args.data[0], result, args.size(),
	    [](double x, ValidityMask &mask, idx_t idx) {
		    return SafeCall([&] { return stats_duck::NormalCDF(x); }, mask, idx);
	    });
}

static void PNorm2Exec(DataChunk &args, ExpressionState &, Vector &result) {
	BinaryExecutor::ExecuteWithNulls<double, double, double>(
	    args.data[0], args.data[1], result, args.size(),
	    [](double x, double mean, ValidityMask &mask, idx_t idx) {
		    return SafeCall([&] { return stats_duck::NormalCDF(x, mean); }, mask, idx);
	    });
}

static void PNorm3Exec(DataChunk &args, ExpressionState &, Vector &result) {
	TernaryExecutor::ExecuteWithNulls<double, double, double, double>(
	    args.data[0], args.data[1], args.data[2], result, args.size(),
	    [](double x, double mean, double sd, ValidityMask &mask, idx_t idx) {
		    return SafeCall([&] { return stats_duck::NormalCDF(x, mean, sd); }, mask, idx);
	    });
}

static void QNormStdExec(DataChunk &args, ExpressionState &, Vector &result) {
	UnaryExecutor::ExecuteWithNulls<double, double>(
	    args.data[0], result, args.size(),
	    [](double p, ValidityMask &mask, idx_t idx) {
		    return SafeCall([&] { return stats_duck::NormalQuantile(p); }, mask, idx);
	    });
}

static void QNorm2Exec(DataChunk &args, ExpressionState &, Vector &result) {
	BinaryExecutor::ExecuteWithNulls<double, double, double>(
	    args.data[0], args.data[1], result, args.size(),
	    [](double p, double mean, ValidityMask &mask, idx_t idx) {
		    return SafeCall([&] { return stats_duck::NormalQuantile(p, mean); }, mask, idx);
	    });
}

static void QNorm3Exec(DataChunk &args, ExpressionState &, Vector &result) {
	TernaryExecutor::ExecuteWithNulls<double, double, double, double>(
	    args.data[0], args.data[1], args.data[2], result, args.size(),
	    [](double p, double mean, double sd, ValidityMask &mask, idx_t idx) {
		    return SafeCall([&] { return stats_duck::NormalQuantile(p, mean, sd); }, mask, idx);
	    });
}

// ── Student's t ─────────────────────────────────────────────────────────────

static void DTExec(DataChunk &args, ExpressionState &, Vector &result) {
	BinaryExecutor::ExecuteWithNulls<double, double, double>(
	    args.data[0], args.data[1], result, args.size(),
	    [](double x, double df, ValidityMask &mask, idx_t idx) {
		    return SafeCall([&] { return stats_duck::StudentTPDF(x, df); }, mask, idx);
	    });
}

static void PTExec(DataChunk &args, ExpressionState &, Vector &result) {
	BinaryExecutor::ExecuteWithNulls<double, double, double>(
	    args.data[0], args.data[1], result, args.size(),
	    [](double x, double df, ValidityMask &mask, idx_t idx) {
		    return SafeCall([&] { return stats_duck::StudentTCDF(x, df); }, mask, idx);
	    });
}

static void QTExec(DataChunk &args, ExpressionState &, Vector &result) {
	BinaryExecutor::ExecuteWithNulls<double, double, double>(
	    args.data[0], args.data[1], result, args.size(),
	    [](double p, double df, ValidityMask &mask, idx_t idx) {
		    return SafeCall([&] { return stats_duck::StudentTQuantile(p, df); }, mask, idx);
	    });
}

// ── Chi-square ──────────────────────────────────────────────────────────────

static void DChiSqExec(DataChunk &args, ExpressionState &, Vector &result) {
	BinaryExecutor::ExecuteWithNulls<double, double, double>(
	    args.data[0], args.data[1], result, args.size(),
	    [](double x, double df, ValidityMask &mask, idx_t idx) {
		    return SafeCall([&] { return stats_duck::ChiSquarePDF(x, df); }, mask, idx);
	    });
}

static void PChiSqExec(DataChunk &args, ExpressionState &, Vector &result) {
	BinaryExecutor::ExecuteWithNulls<double, double, double>(
	    args.data[0], args.data[1], result, args.size(),
	    [](double x, double df, ValidityMask &mask, idx_t idx) {
		    return SafeCall([&] { return stats_duck::ChiSquareCDF(x, df); }, mask, idx);
	    });
}

static void QChiSqExec(DataChunk &args, ExpressionState &, Vector &result) {
	BinaryExecutor::ExecuteWithNulls<double, double, double>(
	    args.data[0], args.data[1], result, args.size(),
	    [](double p, double df, ValidityMask &mask, idx_t idx) {
		    return SafeCall([&] { return stats_duck::ChiSquareQuantile(p, df); }, mask, idx);
	    });
}

// ── F ───────────────────────────────────────────────────────────────────────

static void DFExec(DataChunk &args, ExpressionState &, Vector &result) {
	TernaryExecutor::ExecuteWithNulls<double, double, double, double>(
	    args.data[0], args.data[1], args.data[2], result, args.size(),
	    [](double x, double df1, double df2, ValidityMask &mask, idx_t idx) {
		    return SafeCall([&] { return stats_duck::FPDF(x, df1, df2); }, mask, idx);
	    });
}

static void PFExec(DataChunk &args, ExpressionState &, Vector &result) {
	TernaryExecutor::ExecuteWithNulls<double, double, double, double>(
	    args.data[0], args.data[1], args.data[2], result, args.size(),
	    [](double x, double df1, double df2, ValidityMask &mask, idx_t idx) {
		    return SafeCall([&] { return stats_duck::FCDF(x, df1, df2); }, mask, idx);
	    });
}

static void QFExec(DataChunk &args, ExpressionState &, Vector &result) {
	TernaryExecutor::ExecuteWithNulls<double, double, double, double>(
	    args.data[0], args.data[1], args.data[2], result, args.size(),
	    [](double p, double df1, double df2, ValidityMask &mask, idx_t idx) {
		    return SafeCall([&] { return stats_duck::FQuantile(p, df1, df2); }, mask, idx);
	    });
}

} // namespace

void RegisterDistributionFunctions(ExtensionLoader &loader) {
	const auto DBL = LogicalType::DOUBLE;

	// ── Normal ──────────────────────────────────────────────────────────────
	{
		ScalarFunctionSet dnorm("dnorm");
		dnorm.AddFunction(ScalarFunction({DBL}, DBL, DNormStdExec));
		dnorm.AddFunction(ScalarFunction({DBL, DBL}, DBL, DNorm2Exec));
		dnorm.AddFunction(ScalarFunction({DBL, DBL, DBL}, DBL, DNorm3Exec));
		loader.RegisterFunction(dnorm);
	}
	{
		ScalarFunctionSet pnorm("pnorm");
		pnorm.AddFunction(ScalarFunction({DBL}, DBL, PNormStdExec));
		pnorm.AddFunction(ScalarFunction({DBL, DBL}, DBL, PNorm2Exec));
		pnorm.AddFunction(ScalarFunction({DBL, DBL, DBL}, DBL, PNorm3Exec));
		loader.RegisterFunction(pnorm);
	}
	{
		ScalarFunctionSet qnorm("qnorm");
		qnorm.AddFunction(ScalarFunction({DBL}, DBL, QNormStdExec));
		qnorm.AddFunction(ScalarFunction({DBL, DBL}, DBL, QNorm2Exec));
		qnorm.AddFunction(ScalarFunction({DBL, DBL, DBL}, DBL, QNorm3Exec));
		loader.RegisterFunction(qnorm);
	}

	// ── Student's t ─────────────────────────────────────────────────────────
	loader.RegisterFunction(ScalarFunction("dt", {DBL, DBL}, DBL, DTExec));
	loader.RegisterFunction(ScalarFunction("pt", {DBL, DBL}, DBL, PTExec));
	loader.RegisterFunction(ScalarFunction("qt", {DBL, DBL}, DBL, QTExec));

	// ── Chi-square ──────────────────────────────────────────────────────────
	loader.RegisterFunction(ScalarFunction("dchisq", {DBL, DBL}, DBL, DChiSqExec));
	loader.RegisterFunction(ScalarFunction("pchisq", {DBL, DBL}, DBL, PChiSqExec));
	loader.RegisterFunction(ScalarFunction("qchisq", {DBL, DBL}, DBL, QChiSqExec));

	// ── F ───────────────────────────────────────────────────────────────────
	loader.RegisterFunction(ScalarFunction("df", {DBL, DBL, DBL}, DBL, DFExec));
	loader.RegisterFunction(ScalarFunction("pf", {DBL, DBL, DBL}, DBL, PFExec));
	loader.RegisterFunction(ScalarFunction("qf", {DBL, DBL, DBL}, DBL, QFExec));
}

} // namespace duckdb
