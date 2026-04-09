#pragma once

#include "duckdb.hpp"

extern "C" {
#include "readstat.h"
}

namespace duckdb {

// Epoch offsets (days from Unix epoch 1970-01-01)
constexpr int32_t SAS_EPOCH_OFFSET = -3653;   // 1960-01-01
constexpr int32_t SPSS_EPOCH_OFFSET = -141428; // 1582-10-14
constexpr int32_t STATA_EPOCH_OFFSET = -3653;  // 1960-01-01

//! Map a ReadStat variable type + format string to a DuckDB LogicalType
LogicalType MapReadStatType(readstat_type_t type, const char *format);

//! Write a ReadStat value into a DuckDB vector at the given row, handling type conversion.
//! Caller must check for missing values before calling this.
void WriteReadStatValue(readstat_value_t value, readstat_variable_t *variable, const LogicalType &target_type,
                        const string &format, Vector &vec, idx_t row);

} // namespace duckdb
