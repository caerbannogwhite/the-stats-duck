#include "read_stat_types.hpp"

#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/vector.hpp"

#include <cmath>
#include <unordered_set>

namespace duckdb {

// ─── Format string tables ───────────────────────────────────────────────────────

static const std::unordered_set<string> DATE_FORMATS = {
    // SAS
    "WEEKDATE", "WEEKDATX", "MMDDYY", "DDMMYY", "YYMMDD", "DATE", "DATE7", "DATE9", "YYMMDD10",
    "DDMMYYB", "DDMMYYB10", "DDMMYYC", "DDMMYYC10", "DDMMYYD", "DDMMYYD10",
    "DDMMYYN6", "DDMMYYN8", "DDMMYYP", "DDMMYYP10", "DDMMYYS", "DDMMYYS10",
    "MMDDYYB", "MMDDYYB10", "MMDDYYC", "MMDDYYC10", "MMDDYYD", "MMDDYYD10",
    "MMDDYYN6", "MMDDYYN8", "MMDDYYP", "MMDDYYP10", "MMDDYYS", "MMDDYYS10",
    "DTDATE", "IS8601DA", "E8601DA", "B8601DA",
    "YYMMDDB", "YYMMDDD", "YYMMDDN", "YYMMDDP", "YYMMDDS",
    "WORDDATE", "WORDDATX", "JULDAY", "JULIAN", "MONYY", "YYQ",
    // SPSS
    "DATE8", "DATE11", "DATE12", "ADATE", "ADATE8", "ADATE10",
    "EDATE", "EDATE8", "EDATE10", "JDATE", "JDATE5", "JDATE7",
    "SDATE", "SDATE8", "SDATE10",
};

static const std::unordered_set<string> DATETIME_FORMATS = {
    // SAS
    "DATETIME", "DATETIME17", "DATETIME18", "DATETIME19", "DATETIME20",
    "DATETIME21", "DATETIME22", "E8601DT", "DATEAMPM", "MDYAMPM",
    "IS8601DT", "B8601DT", "B8601DN",
    // SPSS
    "DATETIME8", "DATETIME17", "DATETIME20", "DATETIME23.2",
    "YMDHMS16", "YMDHMS19", "YMDHMS19.2", "YMDHMS20",
};

static const std::unordered_set<string> TIME_FORMATS = {
    // SAS
    "TIME", "HHMM", "HHMMSS", "TIME5", "TIME8", "TIME20", "TIME20.3",
    "TOD", "TIMEAMPM", "IS8601TM", "E8601TM", "B8601TM",
    // SPSS
    "DTIME", "TIME11.2",
};

enum class TemporalKind { NONE, DATE, TIMESTAMP, TIME };

//! Determine if a format string represents a temporal type.
//! Checks Stata prefix formats first, then exact matches.
static TemporalKind ClassifyFormat(const char *format) {
	if (!format || format[0] == '\0') {
		return TemporalKind::NONE;
	}
	string fmt(format);

	// Stata prefix formats — check TIME before TIMESTAMP since both use %tc
	if (fmt.rfind("%td", 0) == 0 || fmt.rfind("%d", 0) == 0) {
		return TemporalKind::DATE;
	}
	// Stata time-only: %tc with only HH/MM/SS components (no date parts)
	if (fmt.find("HH") != string::npos && fmt.find("DD") == string::npos && fmt.find("CC") == string::npos &&
	    fmt.find("YY") == string::npos && fmt.find("NN") == string::npos) {
		if (fmt.rfind("%tc", 0) == 0 || fmt.rfind("%tC", 0) == 0) {
			return TemporalKind::TIME;
		}
	}
	if (fmt.rfind("%tC", 0) == 0 || fmt.rfind("%tc", 0) == 0) {
		return TemporalKind::TIMESTAMP;
	}

	// Strip trailing width/decimals for matching (e.g., "DATE9." → "DATE9")
	auto dot_pos = fmt.find('.');
	string base = (dot_pos != string::npos) ? fmt.substr(0, dot_pos) : fmt;

	// Check exact matches (order: timestamp before date, since some share prefixes)
	if (DATETIME_FORMATS.count(base) || DATETIME_FORMATS.count(fmt)) {
		return TemporalKind::TIMESTAMP;
	}
	if (DATE_FORMATS.count(base) || DATE_FORMATS.count(fmt)) {
		return TemporalKind::DATE;
	}
	if (TIME_FORMATS.count(base) || TIME_FORMATS.count(fmt)) {
		return TemporalKind::TIME;
	}

	return TemporalKind::NONE;
}

// ─── Type mapping ───────────────────────────────────────────────────────────────

LogicalType MapReadStatType(readstat_type_t type, const char *format) {
	switch (type) {
	case READSTAT_TYPE_STRING:
	case READSTAT_TYPE_STRING_REF:
		return LogicalType::VARCHAR;
	case READSTAT_TYPE_INT8:
		return LogicalType::TINYINT;
	case READSTAT_TYPE_INT16:
		return LogicalType::SMALLINT;
	case READSTAT_TYPE_INT32:
		return LogicalType::INTEGER;
	case READSTAT_TYPE_FLOAT:
		return LogicalType::FLOAT;
	case READSTAT_TYPE_DOUBLE: {
		auto kind = ClassifyFormat(format);
		switch (kind) {
		case TemporalKind::DATE:
			return LogicalType::DATE;
		case TemporalKind::TIMESTAMP:
			return LogicalType::TIMESTAMP;
		case TemporalKind::TIME:
			return LogicalType::TIME;
		default:
			return LogicalType::DOUBLE;
		}
	}
	default:
		return LogicalType::VARCHAR;
	}
}

// ─── Value conversion helpers ───────────────────────────────────────────────────

//! Detect which epoch system the format uses
enum class EpochSystem { SAS, SPSS, STATA };

static EpochSystem DetectEpochSystem(const string &format) {
	// Stata formats start with %
	if (!format.empty() && format[0] == '%') {
		return EpochSystem::STATA;
	}
	// SPSS date formats
	if (format == "ADATE" || format == "ADATE8" || format == "ADATE10" || format == "EDATE" ||
	    format == "EDATE8" || format == "EDATE10" || format == "JDATE" || format == "JDATE5" ||
	    format == "JDATE7" || format == "SDATE" || format == "SDATE8" || format == "SDATE10" ||
	    format == "DATE8" || format == "DATE11" || format == "DATE12" || format == "DTIME" ||
	    format == "DATETIME8" || format == "DATETIME17" || format == "DATETIME20" ||
	    format == "DATETIME23.2" || format == "YMDHMS16" || format == "YMDHMS19" ||
	    format == "YMDHMS19.2" || format == "YMDHMS20" || format == "TIME11.2") {
		return EpochSystem::SPSS;
	}
	return EpochSystem::SAS;
}

static void WriteDateValue(double raw, const string &format, Vector &vec, idx_t row) {
	auto epoch = DetectEpochSystem(format);
	int32_t days;
	switch (epoch) {
	case EpochSystem::SPSS:
		// SPSS: raw is seconds since 1582-10-14
		days = static_cast<int32_t>(raw / 86400.0) + SPSS_EPOCH_OFFSET;
		break;
	case EpochSystem::STATA:
	case EpochSystem::SAS:
	default:
		// SAS/Stata: raw is days since 1960-01-01
		days = static_cast<int32_t>(raw) + SAS_EPOCH_OFFSET;
		break;
	}
	FlatVector::GetData<int32_t>(vec)[row] = days;
}

static void WriteTimestampValue(double raw, const string &format, Vector &vec, idx_t row) {
	auto epoch = DetectEpochSystem(format);
	double seconds;
	int32_t epoch_offset;

	switch (epoch) {
	case EpochSystem::SPSS:
		// SPSS: raw is seconds since 1582-10-14
		seconds = raw;
		epoch_offset = SPSS_EPOCH_OFFSET;
		break;
	case EpochSystem::STATA:
		// Stata %tc/%tC: raw is milliseconds since 1960-01-01
		seconds = raw / 1000.0;
		epoch_offset = STATA_EPOCH_OFFSET;
		break;
	case EpochSystem::SAS:
	default:
		// SAS: raw is seconds since 1960-01-01
		seconds = raw;
		epoch_offset = SAS_EPOCH_OFFSET;
		break;
	}

	auto total_days = static_cast<int64_t>(seconds / 86400.0);
	double remaining_secs = seconds - static_cast<double>(total_days) * 86400.0;
	int64_t micros =
	    (total_days + epoch_offset) * 86400LL * 1000000LL + static_cast<int64_t>(remaining_secs * 1000000.0);
	FlatVector::GetData<int64_t>(vec)[row] = micros;
}

static void WriteTimeValue(double raw, const string &format, Vector &vec, idx_t row) {
	double secs;
	if (!format.empty() && format[0] == '%') {
		// Stata: raw is milliseconds since 1960-01-01, extract time-of-day
		secs = std::fmod(raw / 1000.0, 86400.0);
	} else {
		// SAS/SPSS: raw is seconds since midnight
		secs = std::fmod(raw, 86400.0);
	}
	if (secs < 0.0) {
		secs += 86400.0;
	}
	FlatVector::GetData<int64_t>(vec)[row] = static_cast<int64_t>(secs * 1000000.0);
}

// ─── Public API ─────────────────────────────────────────────────────────────────

void WriteReadStatValue(readstat_value_t value, readstat_variable_t *variable, const LogicalType &target_type,
                        const string &format, Vector &vec, idx_t row) {
	auto type_id = target_type.id();

	switch (type_id) {
	case LogicalTypeId::VARCHAR: {
		const char *str = readstat_string_value(value);
		if (str) {
			FlatVector::GetData<string_t>(vec)[row] = StringVector::AddString(vec, str);
		} else {
			FlatVector::SetNull(vec, row, true);
		}
		break;
	}
	case LogicalTypeId::TINYINT:
		FlatVector::GetData<int8_t>(vec)[row] = readstat_int8_value(value);
		break;
	case LogicalTypeId::SMALLINT:
		FlatVector::GetData<int16_t>(vec)[row] = readstat_int16_value(value);
		break;
	case LogicalTypeId::INTEGER:
		FlatVector::GetData<int32_t>(vec)[row] = readstat_int32_value(value);
		break;
	case LogicalTypeId::FLOAT:
		FlatVector::GetData<float>(vec)[row] = readstat_float_value(value);
		break;
	case LogicalTypeId::DOUBLE:
		FlatVector::GetData<double>(vec)[row] = readstat_double_value(value);
		break;
	case LogicalTypeId::DATE:
		WriteDateValue(readstat_double_value(value), format, vec, row);
		break;
	case LogicalTypeId::TIMESTAMP:
		WriteTimestampValue(readstat_double_value(value), format, vec, row);
		break;
	case LogicalTypeId::TIME:
		WriteTimeValue(readstat_double_value(value), format, vec, row);
		break;
	default:
		FlatVector::SetNull(vec, row, true);
		break;
	}
}

} // namespace duckdb
