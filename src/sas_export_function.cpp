#include "sas_export_function.hpp"
#include "read_stat_types.hpp"

#include "duckdb/function/copy_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/execution/execution_context.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/file_open_flags.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/types/column/column_data_scan_states.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/hugeint.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/parser/column_list.hpp"
#include "duckdb/parser/column_definition.hpp"

extern "C" {
#include "readstat.h"
}

namespace duckdb {

enum class SasFormat { XPT, SAS7BDAT, SAV, POR };

// SAV and POR share SPSS-side semantics: SPSS epoch (1582-10-14), SPSS format
// strings (DATE11/DATETIME20/TIME8), spss_format_for_variable() handling.
static inline bool IsSpssFamily(SasFormat f) {
	return f == SasFormat::SAV || f == SasFormat::POR;
}

// ─── Per-format column-name validation ─────────────────────────────────────────
// XPT v5 / POR: ≤ 8 chars, uppercased, ASCII alphanumeric or underscore, must
// start with a letter or underscore. Names are uppercased before validation —
// silent truncation would create silent collisions on round-trip. (POR allows
// '.', '@', '#', '$' too, but we keep the stricter A-Z 0-9 _ subset for
// consistency with XPT.)
// XPT v8 / SAS7BDAT / SAV: ≤ 32 chars, otherwise the same character rules. XPT
// v8 still uppercases (XPT-family files are uppercase by convention); SAS7BDAT
// and SAV preserve the user's casing.

static size_t MaxNameLen(SasFormat format, uint8_t xpt_version) {
	switch (format) {
	case SasFormat::XPT:
		return xpt_version == 8 ? 32 : 8;
	case SasFormat::POR:
		return 8;
	case SasFormat::SAS7BDAT:
	case SasFormat::SAV:
		return 32;
	}
	return 8;
}

static const char *FormatLabelWithVersion(SasFormat format, uint8_t xpt_version) {
	switch (format) {
	case SasFormat::XPT:
		return xpt_version == 8 ? "XPT v8" : "XPT v5";
	case SasFormat::SAS7BDAT:
		return "SAS7BDAT";
	case SasFormat::SAV:
		return "SAV";
	case SasFormat::POR:
		return "POR";
	}
	return "?";
}

static string ValidateColumnName(const string &name, SasFormat format, const char *role, uint8_t xpt_version = 5) {
	const bool uppercase = format == SasFormat::XPT || format == SasFormat::POR;
	const size_t max_len = MaxNameLen(format, xpt_version);
	const char *format_label = FormatLabelWithVersion(format, xpt_version);

	if (name.empty()) {
		throw BinderException("Empty %s not allowed in %s export", role, format_label);
	}
	if (name.size() > max_len) {
		throw BinderException("%s '%s' is too long for %s (max %llu chars). Rename in your SELECT.", role, name,
		                      format_label, static_cast<uint64_t>(max_len));
	}
	string out = uppercase ? StringUtil::Upper(name) : name;
	for (char c : out) {
		bool ok = (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_';
		if (!ok) {
			throw BinderException("%s '%s' contains characters not allowed in %s (only A-Z a-z 0-9 _ permitted)",
			                      role, name, format_label);
		}
	}
	if (out[0] >= '0' && out[0] <= '9') {
		throw BinderException("%s '%s' starts with a digit, not allowed in %s", role, name, format_label);
	}
	return out;
}

// ─── Type mapping (DuckDB → ReadStat) ──────────────────────────────────────────

struct ColumnSpec {
	string original_name;
	string mangled_name;
	string label; // ReadStat variable label — sourced from DuckDB column comments
	LogicalType duckdb_type;
	readstat_type_t rs_type;
	string rs_format;     // SAS-style "DATE9." / SPSS-style "DATE11" / "" for non-temporal
	size_t storage_width; // ReadStat's storage_width (only used for STRING in non-XPT)
};

static void MapDuckDBType(const LogicalType &dt, SasFormat fmt, ColumnSpec &spec) {
	const bool is_spss = IsSpssFamily(fmt);

	switch (dt.id()) {
	case LogicalTypeId::BOOLEAN:
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
		spec.rs_type = READSTAT_TYPE_INT32;
		spec.storage_width = 8;
		return;
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::HUGEINT:
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::UHUGEINT:
	case LogicalTypeId::DECIMAL:
	case LogicalTypeId::DOUBLE:
	case LogicalTypeId::FLOAT:
		spec.rs_type = READSTAT_TYPE_DOUBLE;
		spec.storage_width = 8;
		return;
	case LogicalTypeId::VARCHAR:
		spec.rs_type = READSTAT_TYPE_STRING;
		spec.storage_width = 1;
		return;
	case LogicalTypeId::DATE:
		spec.rs_type = READSTAT_TYPE_DOUBLE;
		// SPSS format strings have no trailing period and use widths from the SPSS
		// docs; SAS format strings end with a period. Reader's epoch detection keys
		// off the format-string text, so the writer must match the format family.
		// Picks "DATETIME19." (SAS-only, dodges the reader's SPSS-claims-DATETIME20 bug).
		spec.rs_format = is_spss ? "DATE11" : "DATE9.";
		spec.storage_width = 8;
		return;
	case LogicalTypeId::TIMESTAMP:
		spec.rs_type = READSTAT_TYPE_DOUBLE;
		spec.rs_format = is_spss ? "DATETIME20" : "DATETIME19.";
		spec.storage_width = 8;
		return;
	case LogicalTypeId::TIME:
		spec.rs_type = READSTAT_TYPE_DOUBLE;
		spec.rs_format = is_spss ? "TIME8" : "TIME8.";
		spec.storage_width = 8;
		return;
	default:
		throw BinderException("stats_duck: column '%s' has unsupported type %s for export",
		                      spec.original_name, dt.ToString());
	}
}

// ─── BindData / GlobalState / LocalState ───────────────────────────────────────

struct SasExportBindData : public FunctionData {
	SasFormat format = SasFormat::XPT;
	uint8_t xpt_version = 5;                                    // XPT only — 5 (default) or 8
	string label;
	string table_name;                                          // XPT only
	readstat_compress_t compression = READSTAT_COMPRESS_NONE;   // SAS7BDAT and SAV only
	vector<ColumnSpec> columns;

	unique_ptr<FunctionData> Copy() const override {
		auto c = make_uniq<SasExportBindData>();
		c->format = format;
		c->xpt_version = xpt_version;
		c->label = label;
		c->table_name = table_name;
		c->compression = compression;
		c->columns = columns;
		return std::move(c);
	}
	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<SasExportBindData>();
		if (columns.size() != other.columns.size()) {
			return false;
		}
		for (idx_t i = 0; i < columns.size(); i++) {
			if (columns[i].mangled_name != other.columns[i].mangled_name ||
			    columns[i].duckdb_type != other.columns[i].duckdb_type) {
				return false;
			}
		}
		return format == other.format && xpt_version == other.xpt_version && label == other.label &&
		       table_name == other.table_name && compression == other.compression;
	}
};

struct SasExportGlobalState : public GlobalFunctionData {
	unique_ptr<FileHandle> file_handle;
	unique_ptr<ColumnDataCollection> buffer;
	ColumnDataAppendState append_state;
	vector<size_t> max_string_len; // 0 for non-VARCHAR columns
};

struct SasExportLocalState : public LocalFunctionData {};

// ─── Bind ──────────────────────────────────────────────────────────────────────

static SasFormat FormatFromInfo(const CopyInfo &info) {
	auto fmt = StringUtil::Lower(info.format);
	if (fmt == "xpt") {
		return SasFormat::XPT;
	}
	if (fmt == "sas7bdat") {
		return SasFormat::SAS7BDAT;
	}
	if (fmt == "sav") {
		return SasFormat::SAV;
	}
	if (fmt == "por") {
		return SasFormat::POR;
	}
	throw BinderException("Unknown stats_duck export format '%s' (expected 'xpt', 'sas7bdat', 'sav', or 'por')",
	                      info.format);
}

static const char *FormatLabel(SasFormat f) {
	switch (f) {
	case SasFormat::XPT:
		return "XPT";
	case SasFormat::SAS7BDAT:
		return "SAS7BDAT";
	case SasFormat::SAV:
		return "SAV";
	case SasFormat::POR:
		return "POR";
	}
	return "?";
}

static unique_ptr<FunctionData> SasExportBind(ClientContext &context, CopyFunctionBindInput &input,
                                              const vector<string> &names, const vector<LogicalType> &sql_types) {
	auto bind_data = make_uniq<SasExportBindData>();
	bind_data->format = FormatFromInfo(input.info);
	const auto fmt = bind_data->format;
	const auto *fmt_label = FormatLabel(fmt);

	// XPT-only: derive a default internal dataset name from the path basename.
	// SAS7BDAT ignores readstat_writer_set_table_name, so we only build it for XPT.
	// Truncation length follows xpt_version (set below from the VERSION option,
	// defaulting to 5).
	string xpt_base;
	if (fmt == SasFormat::XPT) {
		auto path = input.info.file_path;
		auto slash = path.find_last_of("/\\");
		xpt_base = slash == string::npos ? path : path.substr(slash + 1);
		auto dot = xpt_base.find_last_of('.');
		if (dot != string::npos) {
			xpt_base = xpt_base.substr(0, dot);
		}
		if (xpt_base.empty()) {
			xpt_base = "DATASET";
		}
	}

	bool table_name_user_provided = false;
	for (auto &option : input.info.options) {
		auto loption = StringUtil::Lower(option.first);
		if (option.second.size() != 1) {
			throw BinderException("%s %s requires exactly one argument", fmt_label, StringUtil::Upper(loption));
		}
		if (loption == "label") {
			bind_data->label = option.second[0].ToString();
		} else if (loption == "version") {
			if (fmt != SasFormat::XPT) {
				throw BinderException("VERSION option is only meaningful for XPT export, not %s", fmt_label);
			}
			auto v = option.second[0].GetValue<int64_t>();
			if (v != 5 && v != 8) {
				throw BinderException("XPT VERSION must be 5 or 8, got %lld", static_cast<long long>(v));
			}
			bind_data->xpt_version = static_cast<uint8_t>(v);
		} else if (loption == "table_name") {
			if (fmt != SasFormat::XPT) {
				throw BinderException("TABLE_NAME option is only meaningful for XPT export, not %s", fmt_label);
			}
			xpt_base = option.second[0].ToString();
			table_name_user_provided = true;
		} else if (loption == "compression") {
			if (fmt != SasFormat::SAS7BDAT && fmt != SasFormat::SAV) {
				// POR is text-based; SPSS's spec defines no compression for it.
				// XPT also has no compression in either v5 or v8.
				throw BinderException("COMPRESSION option is only meaningful for SAS7BDAT and SAV export, not %s",
				                      fmt_label);
			}
			auto roption = StringUtil::Lower(option.second[0].ToString());
			if (roption == "none") {
				bind_data->compression = READSTAT_COMPRESS_NONE;
			} else if (roption == "rows" || roption == "rle") {
				bind_data->compression = READSTAT_COMPRESS_ROWS;
			} else {
				throw BinderException("%s COMPRESSION must be 'none' or 'rows', got '%s'", fmt_label, roption);
			}
		} else {
			throw BinderException("Unrecognized %s option: %s", fmt_label, option.first);
		}
	}

	if (fmt == SasFormat::XPT) {
		// Truncate auto-derived path basenames to the version-appropriate width
		// before validation. User-supplied TABLE_NAME is left intact and must
		// validate as-is.
		if (!table_name_user_provided) {
			auto cap = MaxNameLen(SasFormat::XPT, bind_data->xpt_version);
			if (xpt_base.size() > cap) {
				xpt_base = xpt_base.substr(0, cap);
			}
		}
		try {
			bind_data->table_name = ValidateColumnName(xpt_base, fmt, "TABLE_NAME", bind_data->xpt_version);
		} catch (const BinderException &) {
			if (table_name_user_provided) {
				throw;
			}
			// Auto-derived name from path didn't validate; fall back to "DATASET".
			bind_data->table_name = "DATASET";
		}
	}

	bind_data->columns.reserve(names.size());
	for (idx_t i = 0; i < names.size(); i++) {
		ColumnSpec spec;
		spec.original_name = names[i];
		spec.duckdb_type = sql_types[i];
		MapDuckDBType(sql_types[i], fmt, spec);
		spec.mangled_name = ValidateColumnName(names[i], fmt, "column name", bind_data->xpt_version);
		bind_data->columns.push_back(std::move(spec));
	}

	// Pull DuckDB column comments into ReadStat variable labels for `COPY tbl TO ...`.
	// CopyInfo.table is empty for `COPY (subquery) TO ...`, in which case there's
	// no source-table catalog entry to consult and labels stay empty. This is
	// intentional: a SELECT has no comment metadata of its own, only its source
	// columns do, and reaching through the projection would mislabel computed
	// columns.
	if (!input.info.table.empty()) {
		auto &catalog_name = input.info.catalog;
		auto &schema_name = input.info.schema;
		auto entry = Catalog::GetEntry<TableCatalogEntry>(context, catalog_name, schema_name, input.info.table,
		                                                 OnEntryNotFound::RETURN_NULL);
		if (entry) {
			auto &columns = entry->GetColumns();
			for (auto &spec : bind_data->columns) {
				if (!columns.ColumnExists(spec.original_name)) {
					continue;
				}
				const auto &col = columns.GetColumn(spec.original_name);
				const Value &comment = col.Comment();
				if (!comment.IsNull()) {
					spec.label = comment.ToString();
				}
			}
		}
	}

	return std::move(bind_data);
}

// ─── InitializeGlobal ──────────────────────────────────────────────────────────

static unique_ptr<GlobalFunctionData> SasExportInitGlobal(ClientContext &context, FunctionData &bind_data_p,
                                                          const string &file_path) {
	auto &bind_data = bind_data_p.Cast<SasExportBindData>();
	auto state = make_uniq<SasExportGlobalState>();

	auto &fs = FileSystem::GetFileSystem(context);
	state->file_handle = fs.OpenFile(file_path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE_NEW);

	vector<LogicalType> types;
	types.reserve(bind_data.columns.size());
	for (auto &col : bind_data.columns) {
		types.push_back(col.duckdb_type);
	}
	state->buffer = make_uniq<ColumnDataCollection>(context, std::move(types));
	state->buffer->InitializeAppend(state->append_state);
	state->max_string_len.assign(bind_data.columns.size(), 0);
	return std::move(state);
}

// ─── InitializeLocal ───────────────────────────────────────────────────────────

static unique_ptr<LocalFunctionData> SasExportInitLocal(ExecutionContext &, FunctionData &) {
	return make_uniq<SasExportLocalState>();
}

// ─── Sink ──────────────────────────────────────────────────────────────────────
// Single-threaded: append each chunk to the global buffer. Track running max
// VARCHAR length per column — needed at finalize for `readstat_add_variable`'s
// storage_width.

static void SasExportSink(ExecutionContext &, FunctionData &bind_data_p, GlobalFunctionData &gstate_p,
                          LocalFunctionData &, DataChunk &input) {
	auto &bind_data = bind_data_p.Cast<SasExportBindData>();
	auto &gstate = gstate_p.Cast<SasExportGlobalState>();

	auto row_count = input.size();
	for (idx_t c = 0; c < bind_data.columns.size(); c++) {
		if (bind_data.columns[c].rs_type != READSTAT_TYPE_STRING) {
			continue;
		}
		auto &vec = input.data[c];
		UnifiedVectorFormat fmt;
		vec.ToUnifiedFormat(row_count, fmt);
		auto data = UnifiedVectorFormat::GetData<string_t>(fmt);
		for (idx_t r = 0; r < row_count; r++) {
			auto idx = fmt.sel->get_index(r);
			if (!fmt.validity.RowIsValid(idx)) {
				continue;
			}
			auto sz = data[idx].GetSize();
			if (sz > gstate.max_string_len[c]) {
				gstate.max_string_len[c] = sz;
			}
		}
	}

	gstate.buffer->Append(gstate.append_state, input);
}

// ─── Combine (no-op for single-threaded) ───────────────────────────────────────

static void SasExportCombine(ExecutionContext &, FunctionData &, GlobalFunctionData &, LocalFunctionData &) {
}

// ─── ReadStat write callback ───────────────────────────────────────────────────
// Routes ReadStat's writer output through DuckDB's FileSystem so registered/remote
// paths and WASM file buffers work the same as the reader's VFS shim.

static ssize_t SasExportDataWriter(const void *bytes, size_t len, void *ctx) {
	auto *handle = static_cast<FileHandle *>(ctx);
	try {
		handle->Write(const_cast<void *>(bytes), len);
	} catch (...) {
		return -1;
	}
	return static_cast<ssize_t>(len);
}

// ─── Per-cell value insertion ──────────────────────────────────────────────────

static void InsertValue(readstat_writer_t *writer, const readstat_variable_t *var, const ColumnSpec &spec,
                        SasFormat sas_format, const Vector &vec_p, idx_t row, UnifiedVectorFormat &fmt) {
	const int32_t epoch_offset = IsSpssFamily(sas_format) ? SPSS_EPOCH_OFFSET : SAS_EPOCH_OFFSET;
	auto idx = fmt.sel->get_index(row);
	if (!fmt.validity.RowIsValid(idx)) {
		readstat_insert_missing_value(writer, var);
		return;
	}

	switch (spec.duckdb_type.id()) {
	case LogicalTypeId::BOOLEAN: {
		int32_t v = UnifiedVectorFormat::GetData<bool>(fmt)[idx] ? 1 : 0;
		readstat_insert_int32_value(writer, var, v);
		break;
	}
	case LogicalTypeId::TINYINT:
		readstat_insert_int32_value(writer, var,
		                            static_cast<int32_t>(UnifiedVectorFormat::GetData<int8_t>(fmt)[idx]));
		break;
	case LogicalTypeId::SMALLINT:
		readstat_insert_int32_value(writer, var,
		                            static_cast<int32_t>(UnifiedVectorFormat::GetData<int16_t>(fmt)[idx]));
		break;
	case LogicalTypeId::INTEGER:
		readstat_insert_int32_value(writer, var, UnifiedVectorFormat::GetData<int32_t>(fmt)[idx]);
		break;
	case LogicalTypeId::BIGINT:
		readstat_insert_double_value(writer, var,
		                             static_cast<double>(UnifiedVectorFormat::GetData<int64_t>(fmt)[idx]));
		break;
	case LogicalTypeId::HUGEINT:
		readstat_insert_double_value(writer, var,
		                             Hugeint::Cast<double>(UnifiedVectorFormat::GetData<hugeint_t>(fmt)[idx]));
		break;
	case LogicalTypeId::UTINYINT:
		readstat_insert_double_value(writer, var,
		                             static_cast<double>(UnifiedVectorFormat::GetData<uint8_t>(fmt)[idx]));
		break;
	case LogicalTypeId::USMALLINT:
		readstat_insert_double_value(writer, var,
		                             static_cast<double>(UnifiedVectorFormat::GetData<uint16_t>(fmt)[idx]));
		break;
	case LogicalTypeId::UINTEGER:
		readstat_insert_double_value(writer, var,
		                             static_cast<double>(UnifiedVectorFormat::GetData<uint32_t>(fmt)[idx]));
		break;
	case LogicalTypeId::UBIGINT:
		readstat_insert_double_value(writer, var,
		                             static_cast<double>(UnifiedVectorFormat::GetData<uint64_t>(fmt)[idx]));
		break;
	case LogicalTypeId::FLOAT:
		readstat_insert_double_value(writer, var,
		                             static_cast<double>(UnifiedVectorFormat::GetData<float>(fmt)[idx]));
		break;
	case LogicalTypeId::DOUBLE:
		readstat_insert_double_value(writer, var, UnifiedVectorFormat::GetData<double>(fmt)[idx]);
		break;
	case LogicalTypeId::DECIMAL: {
		// DECIMAL → DOUBLE via the value API (handles all internal storage widths).
		Value v = vec_p.GetValue(row);
		readstat_insert_double_value(writer, var, v.GetValue<double>());
		break;
	}
	case LogicalTypeId::VARCHAR: {
		auto str = UnifiedVectorFormat::GetData<string_t>(fmt)[idx];
		std::string buf(str.GetData(), str.GetSize());
		readstat_insert_string_value(writer, var, buf.c_str());
		break;
	}
	case LogicalTypeId::DATE: {
		// DuckDB date_t::days = days since 1970-01-01.
		// Target epoch days = duckdb_days - epoch_offset.
		// SAS DATE: days since 1960-01-01 (epoch_offset = SAS_EPOCH_OFFSET = -3653).
		// SPSS DATE: SECONDS since 1582-10-14 (epoch_offset = SPSS_EPOCH_OFFSET = -141428,
		// then × 86400 to convert days→seconds).
		auto days = UnifiedVectorFormat::GetData<date_t>(fmt)[idx].days;
		double offset_days = static_cast<double>(days - epoch_offset);
		double v = IsSpssFamily(sas_format) ? offset_days * 86400.0 : offset_days;
		readstat_insert_double_value(writer, var, v);
		break;
	}
	case LogicalTypeId::TIMESTAMP: {
		auto micros = UnifiedVectorFormat::GetData<timestamp_t>(fmt)[idx].value;
		double secs = static_cast<double>(micros) / 1e6 - static_cast<double>(epoch_offset) * 86400.0;
		readstat_insert_double_value(writer, var, secs);
		break;
	}
	case LogicalTypeId::TIME: {
		auto micros = UnifiedVectorFormat::GetData<dtime_t>(fmt)[idx].micros;
		readstat_insert_double_value(writer, var, static_cast<double>(micros) / 1e6);
		break;
	}
	default:
		throw InternalException("Unexpected type during SAS export sink (should be caught at bind)");
	}
}

// ─── Finalize ──────────────────────────────────────────────────────────────────

static void SasExportFinalize(ClientContext &, FunctionData &bind_data_p, GlobalFunctionData &gstate_p) {
	auto &bind_data = bind_data_p.Cast<SasExportBindData>();
	auto &gstate = gstate_p.Cast<SasExportGlobalState>();

	auto *writer = readstat_writer_init();
	readstat_set_data_writer(writer, SasExportDataWriter);

	if (bind_data.format == SasFormat::XPT) {
		// XPT v5 (default) — most universally readable. v8 lifts the column-name
		// width to 32 chars and is read transparently by ReadStat-family readers
		// (this extension's read_stat(), pyreadstat, haven, R), but some legacy
		// SAS toolchains still expect v5.
		readstat_writer_set_file_format_version(writer, bind_data.xpt_version);
		if (!bind_data.table_name.empty()) {
			readstat_writer_set_table_name(writer, bind_data.table_name.c_str());
		}
	} else {
		// SAS7BDAT: optional row-level RLE compression (matches `proc copy ... compress=`).
		// SAV: optional row compression (SPSS's "compressed save").
		if (bind_data.compression != READSTAT_COMPRESS_NONE) {
			readstat_writer_set_compression(writer, bind_data.compression);
		}
	}

	if (!bind_data.label.empty()) {
		readstat_writer_set_file_label(writer, bind_data.label.c_str());
	}

	vector<readstat_variable_t *> vars(bind_data.columns.size(), nullptr);
	for (idx_t i = 0; i < bind_data.columns.size(); i++) {
		auto &col = bind_data.columns[i];
		size_t storage_width = col.storage_width;
		if (col.rs_type == READSTAT_TYPE_STRING) {
			storage_width = std::max<size_t>(1, gstate.max_string_len[i]);
		}
		vars[i] = readstat_add_variable(writer, col.mangled_name.c_str(), col.rs_type, storage_width);
		if (!col.rs_format.empty()) {
			readstat_variable_set_format(vars[i], col.rs_format.c_str());
		}
		if (!col.label.empty()) {
			readstat_variable_set_label(vars[i], col.label.c_str());
		}
	}

	long row_count = static_cast<long>(gstate.buffer->Count());
	readstat_error_t err;
	switch (bind_data.format) {
	case SasFormat::XPT:
		err = readstat_begin_writing_xport(writer, gstate.file_handle.get(), row_count);
		break;
	case SasFormat::SAS7BDAT:
		err = readstat_begin_writing_sas7bdat(writer, gstate.file_handle.get(), row_count);
		break;
	case SasFormat::SAV:
		err = readstat_begin_writing_sav(writer, gstate.file_handle.get(), row_count);
		break;
	case SasFormat::POR:
		err = readstat_begin_writing_por(writer, gstate.file_handle.get(), row_count);
		break;
	}
	if (err != READSTAT_OK) {
		auto msg = readstat_error_message(err);
		readstat_writer_free(writer);
		throw IOException("Failed to begin %s write: %s", FormatLabel(bind_data.format), msg ? msg : "unknown");
	}

	DataChunk chunk;
	gstate.buffer->InitializeScanChunk(chunk);
	ColumnDataScanState scan_state;
	gstate.buffer->InitializeScan(scan_state);

	while (gstate.buffer->Scan(scan_state, chunk)) {
		idx_t n = chunk.size();
		vector<UnifiedVectorFormat> formats(bind_data.columns.size());
		for (idx_t c = 0; c < bind_data.columns.size(); c++) {
			chunk.data[c].ToUnifiedFormat(n, formats[c]);
		}
		for (idx_t r = 0; r < n; r++) {
			err = readstat_begin_row(writer);
			if (err != READSTAT_OK) {
				auto msg = readstat_error_message(err);
				readstat_writer_free(writer);
				throw IOException("%s begin_row failed: %s", FormatLabel(bind_data.format), msg ? msg : "unknown");
			}
			for (idx_t c = 0; c < bind_data.columns.size(); c++) {
				InsertValue(writer, vars[c], bind_data.columns[c], bind_data.format, chunk.data[c], r,
				            formats[c]);
			}
			err = readstat_end_row(writer);
			if (err != READSTAT_OK) {
				auto msg = readstat_error_message(err);
				readstat_writer_free(writer);
				throw IOException("%s end_row failed: %s", FormatLabel(bind_data.format), msg ? msg : "unknown");
			}
		}
		chunk.Reset();
	}

	err = readstat_end_writing(writer);
	readstat_writer_free(writer);
	if (err != READSTAT_OK) {
		auto msg = readstat_error_message(err);
		throw IOException("%s end_writing failed: %s", FormatLabel(bind_data.format), msg ? msg : "unknown");
	}

	gstate.file_handle->Close();
	gstate.file_handle.reset();
}

// ─── Execution mode ────────────────────────────────────────────────────────────

static CopyFunctionExecutionMode SasExportExecutionMode(bool, bool) {
	return CopyFunctionExecutionMode::REGULAR_COPY_TO_FILE;
}

// ─── Registration ──────────────────────────────────────────────────────────────

static void WireCopyFunction(CopyFunction &fn) {
	fn.copy_to_bind = SasExportBind;
	fn.copy_to_initialize_global = SasExportInitGlobal;
	fn.copy_to_initialize_local = SasExportInitLocal;
	fn.copy_to_sink = SasExportSink;
	fn.copy_to_combine = SasExportCombine;
	fn.copy_to_finalize = SasExportFinalize;
	fn.execution_mode = SasExportExecutionMode;
}

void RegisterSasExport(ExtensionLoader &loader) {
	CopyFunction xpt("xpt");
	WireCopyFunction(xpt);
	xpt.extension = "xpt";
	loader.RegisterFunction(xpt);

	// SAS7BDAT: round-trips through this extension's read_stat() and other
	// ReadStat-based readers (pyreadstat, haven). A long-standing upstream
	// limitation means real SAS / SAS Universal Viewer cannot open these files.
	// CHANGELOG flags this caveat for users.
	CopyFunction sas7bdat("sas7bdat");
	WireCopyFunction(sas7bdat);
	sas7bdat.extension = "sas7bdat";
	loader.RegisterFunction(sas7bdat);

	// SPSS .sav — same writer family, no SAS-side caveat (real SPSS / PSPP read
	// these files cleanly).
	CopyFunction sav("sav");
	WireCopyFunction(sav);
	sav.extension = "sav";
	loader.RegisterFunction(sav);

	// SPSS .por — Portable file format. ASCII-encoded sibling of SAV designed
	// for cross-platform interchange in pre-Unicode SPSS workflows. Strict
	// XPT-style 8-char uppercase column names and no compression. Variable
	// labels are written via the same readstat_variable_set_label path.
	CopyFunction por("por");
	WireCopyFunction(por);
	por.extension = "por";
	loader.RegisterFunction(por);
}

} // namespace duckdb
