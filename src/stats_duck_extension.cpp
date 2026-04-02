#define DUCKDB_EXTENSION_MAIN

#include "stats_duck_extension.hpp"
#include "ttest_function.hpp"

#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {

void StatsDuckExtension::Load(ExtensionLoader &loader) {
	RegisterTTest1Samp(loader);
	RegisterTTest2Samp(loader);
	RegisterTTestPaired(loader);
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
	duckdb::RegisterTTest1Samp(loader);
	duckdb::RegisterTTest2Samp(loader);
	duckdb::RegisterTTestPaired(loader);
}
