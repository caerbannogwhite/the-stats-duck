#pragma once

#include "duckdb.hpp"

namespace duckdb {

//! Registers adjust_p(pvals LIST<DOUBLE>, method VARCHAR) → LIST<DOUBLE>.
//! Returns adjusted p-values in input order. Supported methods (case-
//! sensitive, matching R's p.adjust): 'bonferroni', 'holm', 'hochberg',
//! 'BH' (also accepts 'fdr'), 'BY', 'none'. NULL entries in the input
//! list are passed through as NULL and are not counted in the multiple-
//! testing denominator.
void RegisterAdjustP(ExtensionLoader &loader);

} // namespace duckdb
