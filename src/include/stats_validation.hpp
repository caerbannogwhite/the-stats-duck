#pragma once

#include "duckdb.hpp"

#include <cmath>
#include <string>

namespace duckdb {

// Shared validation helpers used across stats_duck functions.
//
// Goals:
//   - Consistent error messages (Brian's UI displays these directly to users)
//   - A single place to improve the wording
//   - Cheap inline checks that compile down to nothing when the input is valid
//
// All validators throw a DuckDB-native exception type (BinderException for
// constant-parameter issues detected at plan time, InvalidInputException for
// per-row runtime data issues).

namespace stats_duck_validation {

//! Throw BinderException if `alternative` is not one of the accepted values.
//! Accepts: "two-sided", "less", "greater". Case-sensitive by convention.
inline void ValidateAlternative(const std::string &alternative, const char *function_name) {
	if (alternative != "two-sided" && alternative != "less" && alternative != "greater") {
		throw BinderException(
		    "%s: alternative must be 'two-sided', 'less', or 'greater' (got '%s')",
		    function_name, alternative);
	}
}

//! Throw BinderException if `alpha` is not strictly in (0, 1).
inline void ValidateAlpha(double alpha, const char *function_name) {
	if (!(alpha > 0.0 && alpha < 1.0) || std::isnan(alpha)) {
		throw BinderException("%s: alpha must be in the open interval (0, 1) (got %g)", function_name, alpha);
	}
}

//! Throw BinderException if `df` is not a positive finite number.
inline void ValidateDf(double df, const char *function_name) {
	if (!(df > 0.0) || !std::isfinite(df)) {
		throw BinderException("%s: degrees of freedom must be positive (got %g)", function_name, df);
	}
}

//! Throw BinderException if a standard deviation / variance argument is not positive.
inline void ValidatePositive(double value, const char *parameter_name, const char *function_name) {
	if (!(value > 0.0) || !std::isfinite(value)) {
		throw BinderException("%s: %s must be positive (got %g)", function_name, parameter_name, value);
	}
}

//! Throw BinderException if `p` is outside [0, 1].
inline void ValidateProbability(double p, const char *function_name) {
	if (!(p >= 0.0 && p <= 1.0) || std::isnan(p)) {
		throw BinderException("%s: probability must be in [0, 1] (got %g)", function_name, p);
	}
}

} // namespace stats_duck_validation

// Convenience alias so call sites stay short.
namespace sdv = stats_duck_validation;

} // namespace duckdb
