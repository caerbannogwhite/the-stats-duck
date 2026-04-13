#pragma once

#include "duckdb.hpp"

namespace duckdb {

//! Registers anova_oneway(value, group) — one-way analysis of variance as a
//! streaming aggregate. Returns STRUCT(test_type, f_statistic, df_between,
//! df_within, p_value, ss_between, ss_within, eta_squared, n_groups, n).
void RegisterAnovaOneway(ExtensionLoader &loader);

} // namespace duckdb
