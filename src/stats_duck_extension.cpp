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
#include "ggsql_parser_extension.hpp"

#include "duckdb/main/extension/extension_loader.hpp"

#include <cstdio>

namespace duckdb {

#define TRY_REGISTER(call)                                                                                              \
	do {                                                                                                                \
		std::fprintf(stderr, "[stats_duck] before " #call "\n");                                                        \
		std::fflush(stderr);                                                                                            \
		call;                                                                                                           \
		std::fprintf(stderr, "[stats_duck] after  " #call "\n");                                                        \
		std::fflush(stderr);                                                                                            \
	} while (0)

static void LoadInternal(ExtensionLoader &loader) {
	// Hypothesis tests
	TRY_REGISTER(RegisterTTest1SampAgg(loader));
	TRY_REGISTER(RegisterTTest2SampAgg(loader));
	TRY_REGISTER(RegisterTTestPairedAgg(loader));
	TRY_REGISTER(RegisterMannWhitneyU(loader));
	TRY_REGISTER(RegisterWilcoxonSignedRank(loader));
	TRY_REGISTER(RegisterPearsonTest(loader));
	TRY_REGISTER(RegisterJarqueBera(loader));
	TRY_REGISTER(RegisterAnovaOneway(loader));
	TRY_REGISTER(RegisterChiSquareTests(loader));

	// Descriptive statistics
	TRY_REGISTER(RegisterSummaryStats(loader));

	// Scalar distribution functions (dnorm/pnorm/qnorm/dt/pt/qt/dchisq/...)
	TRY_REGISTER(RegisterDistributionFunctions(loader));

	// Data import
	TRY_REGISTER(RegisterReadStat(loader));

	// Parser extension spike (ggsql)
	TRY_REGISTER(RegisterGgsqlParserExtension(loader));
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
