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

#include "duckdb/main/extension_util.hpp"

namespace duckdb {

static void LoadInternal(DuckDB &db) {
	auto &instance = *db.instance;

	// Hypothesis tests
	RegisterTTest1SampAgg(instance);
	RegisterTTest2SampAgg(instance);
	RegisterTTestPairedAgg(instance);
	RegisterMannWhitneyU(instance);
	RegisterWilcoxonSignedRank(instance);
	RegisterPearsonTest(instance);
	RegisterJarqueBera(instance);
	RegisterAnovaOneway(instance);
	RegisterChiSquareTests(instance);

	// Descriptive statistics
	RegisterSummaryStats(instance);

	// Scalar distribution functions (dnorm/pnorm/qnorm/dt/pt/qt/dchisq/...)
	RegisterDistributionFunctions(instance);

	// Data import
	RegisterReadStat(instance);
}

void StatsDuckExtension::Load(DuckDB &db) {
	LoadInternal(db);
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

DUCKDB_EXTENSION_API void stats_duck_init(duckdb::DatabaseInstance &db) {
	duckdb::DuckDB db_wrapper(db);
	duckdb::LoadInternal(db_wrapper);
}

DUCKDB_EXTENSION_API const char *stats_duck_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
