#define DUCKDB_EXTENSION_MAIN

#include "stats_duck_extension.hpp"
#include "ttest_function.hpp"
#include "ttest_agg_function.hpp"

#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {

void StatsDuckExtension::Load(ExtensionLoader &loader) {
	RegisterTTest1SampAgg(loader);
	RegisterTTest2SampAgg(loader);
	RegisterTTestPairedAgg(loader);
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

DUCKDB_CPP_EXTENSION_ENTRY(stats_duck, loader) {
	duckdb::RegisterTTest1SampAgg(loader);
	duckdb::RegisterTTest2SampAgg(loader);
	duckdb::RegisterTTestPairedAgg(loader);
}
