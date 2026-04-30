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

extern "C" {
#include "readstat.h"
}

namespace duckdb {

enum class SasFormat { XPT, SAS7BDAT };

// ─── Per-format column-name validation ─────────────────────────────────────────
// XPT v5: ≤ 8 chars, uppercased, ASCII alphanumeric or underscore, must start
// with a letter or underscore. Names are uppercased before validation —
// silent truncation would create silent collisions on round-trip.
// SAS7BDAT: ≤ 32 chars, otherwise the same character rules. Case is preserved
// (real SAS is case-insensitive but stores the user's casing).

static string ValidateColumnName(const string &name, SasFormat format, const char *role) {
	const size_t max_len = (format == SasFormat::XPT) ? 8 : 32;
	const char *format_label = (format == SasFormat::XPT) ? "XPT v5" : "SAS7BDAT";

	if (name.empty()) {
		throw BinderException("Empty %s not allowed in %s export", role, format_label);
	}
	if (name.size() > max_len) {
		throw BinderException("%s '%s' is too long for %s (max %llu chars). Rename in your SELECT.", role, name,
		                      format_label, static_cast<uint64_t>(max_len));
	}
	string out = (format == SasFormat::XPT) ? StringUtil::Upper(name) : name;
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
	LogicalType duckdb_type;
	readstat_type_t rs_type;
	string rs_format;     // "DATE9.", "DATETIME20.", "TIME8.", or ""
	size_t storage_width; // ReadStat's storage_width (only used for STRING in non-XPT)
};

static void MapDuckDBType(const LogicalType &dt, ColumnSpec &spec) {
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
		spec.rs_format = "DATE9.";
		spec.storage_width = 8;
		return;
	case LogicalTypeId::TIMESTAMP:
		spec.rs_type = READSTAT_TYPE_DOUBLE;
		// DATETIME19. (not DATETIME20.) — both are valid SAS datetime widths, but
		// "DATETIME20" is ambiguous in our reader's epoch heuristic (it appears in
		// both the SAS and SPSS format tables, and the SPSS branch wins). DATETIME19.
		// is unambiguously SAS, so XPT round-trip lands on the right epoch.
		spec.rs_format = "DATETIME19.";
		spec.storage_width = 8;
		return;
	case LogicalTypeId::TIME:
		spec.rs_type = READSTAT_TYPE_DOUBLE;
		spec.rs_format = "TIME8.";
		spec.storage_width = 8;
		return;
	default:
		throw BinderException("stats_duck: column '%s' has unsupported type %s for SAS export",
		                      spec.original_name, dt.ToString());
	}
}

// ─── BindData / GlobalState / LocalState ───────────────────────────────────────

struct SasExportBindData : public FunctionData {
	SasFormat format = SasFormat::XPT;
	string label;
	string table_name;                                          // XPT only
	readstat_compress_t compression = READSTAT_COMPRESS_NONE;   // SAS7BDAT only
	vector<ColumnSpec> columns;

	unique_ptr<FunctionData> Copy() const override {
		auto c = make_uniq<SasExportBindData>();
		c->format = format;
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
		return format == other.format && label == other.label && table_name == other.table_name &&
		       compression == other.compression;
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
	throw BinderException("Unknown SAS export format '%s' (expected 'xpt' or 'sas7bdat')", info.format);
}

static const char *FormatLabel(SasFormat f) {
	return f == SasFormat::XPT ? "XPT" : "SAS7BDAT";
}

static unique_ptr<FunctionData> SasExportBind(ClientContext &context, CopyFunctionBindInput &input,
                                              const vector<string> &names, const vector<LogicalType> &sql_types) {
	auto bind_data = make_uniq<SasExportBindData>();
	bind_data->format = FormatFromInfo(input.info);
	const auto fmt = bind_data->format;
	const auto *fmt_label = FormatLabel(fmt);

	// XPT-only: derive a default internal dataset name from the path basename.
	// SAS7BDAT ignores readstat_writer_set_table_name, so we only build it for XPT.
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
		if (xpt_base.size() > 8) {
			xpt_base = xpt_base.substr(0, 8);
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
		} else if (loption == "table_name") {
			if (fmt != SasFormat::XPT) {
				throw BinderException("TABLE_NAME option is only meaningful for XPT export, not %s", fmt_label);
			}
			xpt_base = option.second[0].ToString();
			table_name_user_provided = true;
		} else if (loption == "compression") {
			if (fmt != SasFormat::SAS7BDAT) {
				throw BinderException("COMPRESSION option is only meaningful for SAS7BDAT export, not %s", fmt_label);
			}
			auto roption = StringUtil::Lower(option.second[0].ToString());
			if (roption == "none") {
				bind_data->compression = READSTAT_COMPRESS_NONE;
			} else if (roption == "rows" || roption == "rle") {
				bind_data->compression = READSTAT_COMPRESS_ROWS;
			} else {
				throw BinderException("SAS7BDAT COMPRESSION must be 'none' or 'rows', got '%s'", roption);
			}
		} else {
			throw BinderException("Unrecognized %s option: %s", fmt_label, option.first);
		}
	}

	if (fmt == SasFormat::XPT) {
		try {
			bind_data->table_name = ValidateColumnName(xpt_base, fmt, "TABLE_NAME");
		} catch (const BinderException &) {
			if (table_name_user_provided) {
				throw;
			}
			// Auto-derived name from path didn't fit XPT v5; fall back to "DATASET".
			bind_data->table_name = "DATASET";
		}
	}

	bind_data->columns.reserve(names.size());
	for (idx_t i = 0; i < names.size(); i++) {
		ColumnSpec spec;
		spec.original_name = names[i];
		spec.duckdb_type = sql_types[i];
		MapDuckDBType(sql_types[i], spec);
		spec.mangled_name = ValidateColumnName(names[i], fmt, "column name");
		bind_data->columns.push_back(std::move(spec));
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
                        const Vector &vec_p, idx_t row, UnifiedVectorFormat &fmt) {
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
		// DuckDB date_t::days is days since 1970-01-01 (Unix epoch).
		// SAS date is days since 1960-01-01. SAS_EPOCH_OFFSET = -3653 (days from
		// 1970 back to 1960), so SAS days = duckdb_days - SAS_EPOCH_OFFSET.
		auto days = UnifiedVectorFormat::GetData<date_t>(fmt)[idx].days;
		double sas_days = static_cast<double>(days - SAS_EPOCH_OFFSET);
		readstat_insert_double_value(writer, var, sas_days);
		break;
	}
	case LogicalTypeId::TIMESTAMP: {
		auto micros = UnifiedVectorFormat::GetData<timestamp_t>(fmt)[idx].value;
		double secs = static_cast<double>(micros) / 1e6 - static_cast<double>(SAS_EPOCH_OFFSET) * 86400.0;
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
		// XPT v5 — most universally readable XPT version. v8 (longer names, wider
		// strings) is a future phase.
		readstat_writer_set_file_format_version(writer, 5);
		if (!bind_data.table_name.empty()) {
			readstat_writer_set_table_name(writer, bind_data.table_name.c_str());
		}
	} else {
		// SAS7BDAT: optional row-level RLE compression (matches `proc copy ... compress=`).
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
	}

	long row_count = static_cast<long>(gstate.buffer->Count());
	readstat_error_t err;
	if (bind_data.format == SasFormat::XPT) {
		err = readstat_begin_writing_xport(writer, gstate.file_handle.get(), row_count);
	} else {
		err = readstat_begin_writing_sas7bdat(writer, gstate.file_handle.get(), row_count);
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
				InsertValue(writer, vars[c], bind_data.columns[c], chunk.data[c], r, formats[c]);
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
}

} // namespace duckdb
