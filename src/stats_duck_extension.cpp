#define DUCKDB_EXTENSION_MAIN

#include "stats_duck_extension.hpp"
#include "ttest_function.hpp"
#include "ttest_agg_function.hpp"
#include "nonparametric_function.hpp"
#include "distribution_functions.hpp"
#include "summary_stats_function.hpp"
#include "correlation_function.hpp"
#include "normality_function.hpp"
#include "anova_function.hpp"
#include "chisq_function.hpp"
#include "read_stat_function.hpp"
#include "ggsql.hpp"
#include "ggsql_marks_internal.hpp"

#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {

static void LoadInternal(ExtensionLoader &loader) {
	// Hypothesis tests
	RegisterTTest1SampAgg(loader);
	RegisterTTest2SampAgg(loader);
	RegisterTTestPairedAgg(loader);
	RegisterMannWhitneyU(loader);
	RegisterWilcoxonSignedRank(loader);
	RegisterPearsonTest(loader);
	RegisterJarqueBera(loader);
	RegisterAnovaOneway(loader);
	RegisterChiSquareTests(loader);

	// Descriptive statistics
	RegisterSummaryStats(loader);

	// Scalar distribution functions (dnorm/pnorm/qnorm/dt/pt/qt/dchisq/...)
	RegisterDistributionFunctions(loader);

	// Data import
	RegisterReadStat(loader);

	// ggsql: Grammar of Graphics for SQL (parser extension + marks)
	RegisterGgsql(loader);
	ggsql::RegisterBuiltinMarks(loader);
}

void StatsDuckExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

std::string StatsDuckExtension::Name() {
	return "stats_duck";
}

std::string StatsDuckExtension::Version() const {
#ifdef EXT_VERSION_STATS_DUCK
	return EXT_VERSION_STATS_DUCK;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(stats_duck, loader) {
	duckdb::LoadInternal(loader);
}

}
