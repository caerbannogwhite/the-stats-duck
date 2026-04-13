#include "read_stat_function.hpp"
#include "read_stat_types.hpp"

#include "duckdb/function/table_function.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"

extern "C" {
#include "readstat.h"
}

namespace duckdb {

// ─── Format detection ───────────────────────────────────────────────────────────

static string DetectFormat(const string &path) {
	auto dot = path.rfind('.');
	if (dot == string::npos) {
		return "";
	}
	auto ext = StringUtil::Lower(path.substr(dot + 1));
	if (ext == "sas7bdat" || ext == "xpt" || ext == "sav" || ext == "zsav" || ext == "por" || ext == "dta") {
		return ext;
	}
	return "";
}

static readstat_error_t ParseWithFormat(readstat_parser_t *parser, const string &path, const string &format,
                                        void *ctx) {
	if (format == "sas7bdat") {
		return readstat_parse_sas7bdat(parser, path.c_str(), ctx);
	}
	if (format == "xpt") {
		return readstat_parse_xport(parser, path.c_str(), ctx);
	}
	if (format == "sav" || format == "zsav") {
		return readstat_parse_sav(parser, path.c_str(), ctx);
	}
	if (format == "por") {
		return readstat_parse_por(parser, path.c_str(), ctx);
	}
	if (format == "dta") {
		return readstat_parse_dta(parser, path.c_str(), ctx);
	}
	return READSTAT_ERROR_PARSE;
}

// ─── DuckDB FileSystem I/O handlers ─────────────────────────────────────────────
// Route ReadStat's file reads through DuckDB's VFS so that remote files
// (httpfs/s3), registered WASM file buffers, and other virtual file systems
// work transparently. The default unistd handlers call raw POSIX open()/read(),
// which bypass DuckDB's VFS and fail in WASM.

struct DuckDBIOContext {
	FileSystem *fs;
	unique_ptr<FileHandle> handle;
	idx_t file_size;
};

static int DuckDBOpenHandler(const char *path, void *io_ctx_p) {
	auto &io_ctx = *static_cast<DuckDBIOContext *>(io_ctx_p);
	try {
		io_ctx.handle = io_ctx.fs->OpenFile(path, FileFlags::FILE_FLAGS_READ);
		io_ctx.file_size = static_cast<idx_t>(io_ctx.handle->GetFileSize());
		return 0; // fake non-negative fd; ReadStat only checks for < 0
	} catch (...) {
		return -1;
	}
}

static int DuckDBCloseHandler(void *io_ctx_p) {
	auto &io_ctx = *static_cast<DuckDBIOContext *>(io_ctx_p);
	io_ctx.handle.reset();
	return 0;
}

static readstat_off_t DuckDBSeekHandler(readstat_off_t offset, readstat_io_flags_t whence, void *io_ctx_p) {
	auto &io_ctx = *static_cast<DuckDBIOContext *>(io_ctx_p);
	if (!io_ctx.handle) {
		return -1;
	}
	int64_t new_pos;
	switch (whence) {
	case READSTAT_SEEK_SET:
		new_pos = offset;
		break;
	case READSTAT_SEEK_CUR:
		new_pos = static_cast<int64_t>(io_ctx.handle->SeekPosition()) + offset;
		break;
	case READSTAT_SEEK_END:
		new_pos = static_cast<int64_t>(io_ctx.file_size) + offset;
		break;
	default:
		return -1;
	}
	if (new_pos < 0) {
		return -1;
	}
	try {
		io_ctx.handle->Seek(static_cast<idx_t>(new_pos));
	} catch (...) {
		return -1;
	}
	return static_cast<readstat_off_t>(new_pos);
}

static ssize_t DuckDBReadHandler(void *buf, size_t nbyte, void *io_ctx_p) {
	auto &io_ctx = *static_cast<DuckDBIOContext *>(io_ctx_p);
	if (!io_ctx.handle) {
		return -1;
	}
	try {
		return static_cast<ssize_t>(io_ctx.handle->Read(buf, nbyte));
	} catch (...) {
		return -1;
	}
}

static void InstallDuckDBIO(readstat_parser_t *parser, DuckDBIOContext &io_ctx) {
	readstat_set_open_handler(parser, DuckDBOpenHandler);
	readstat_set_close_handler(parser, DuckDBCloseHandler);
	readstat_set_seek_handler(parser, DuckDBSeekHandler);
	readstat_set_read_handler(parser, DuckDBReadHandler);
	readstat_set_io_ctx(parser, &io_ctx);
}

// ─── Bind data ──────────────────────────────────────────────────────────────────

struct ReadStatBindData : public FunctionData {
	string path;
	string format;
	string encoding;
	// -1 when the format doesn't report a row count (e.g., XPT).
	int64_t row_count = 0;
	vector<string> column_names;
	vector<LogicalType> column_types;
	vector<string> column_formats;
	string error_message;

	unique_ptr<FunctionData> Copy() const override {
		auto copy = make_uniq<ReadStatBindData>();
		copy->path = path;
		copy->format = format;
		copy->encoding = encoding;
		copy->row_count = row_count;
		copy->column_names = column_names;
		copy->column_types = column_types;
		copy->column_formats = column_formats;
		return std::move(copy);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<ReadStatBindData>();
		return path == other.path && format == other.format;
	}
};

// ─── Bind-phase ReadStat callbacks ──────────────────────────────────────────────

static int BindMetadataHandler(readstat_metadata_t *metadata, void *ctx) {
	auto &bind_data = *static_cast<ReadStatBindData *>(ctx);
	// Some formats (notably XPT) report -1 for unknown row count.
	// We preserve the -1 so Execute can detect this and rely on
	// ReadStat's own EOF signal instead of a pre-known bound.
	bind_data.row_count = static_cast<int64_t>(readstat_get_row_count(metadata));
	return READSTAT_HANDLER_OK;
}

static int BindVariableHandler(int index, readstat_variable_t *variable, const char *val_labels, void *ctx) {
	auto &bind_data = *static_cast<ReadStatBindData *>(ctx);
	const char *name = readstat_variable_get_name(variable);
	readstat_type_t type = readstat_variable_get_type(variable);
	const char *format = readstat_variable_get_format(variable);

	bind_data.column_names.emplace_back(name ? name : "");
	bind_data.column_formats.emplace_back(format ? format : "");
	bind_data.column_types.push_back(MapReadStatType(type, format));

	return READSTAT_HANDLER_OK;
}

static void BindErrorHandler(const char *error_message, void *ctx) {
	auto &bind_data = *static_cast<ReadStatBindData *>(ctx);
	bind_data.error_message = error_message ? error_message : "Unknown ReadStat error";
}

// ─── Bind function ──────────────────────────────────────────────────────────────

static unique_ptr<FunctionData> ReadStatBind(ClientContext &context, TableFunctionBindInput &input,
                                              vector<LogicalType> &return_types, vector<string> &names) {
	auto bind_data = make_uniq<ReadStatBindData>();
	bind_data->path = input.inputs[0].GetValue<string>();

	// Named parameters
	for (auto &kv : input.named_parameters) {
		if (kv.first == "format") {
			bind_data->format = StringUtil::Lower(kv.second.GetValue<string>());
		} else if (kv.first == "encoding") {
			bind_data->encoding = kv.second.GetValue<string>();
		}
	}

	// Auto-detect format if not specified
	if (bind_data->format.empty()) {
		bind_data->format = DetectFormat(bind_data->path);
	}
	if (bind_data->format.empty()) {
		throw InvalidInputException("Cannot detect file format. Specify it with: read_stat('file', format := 'sas7bdat')");
	}

	// Parse metadata only
	readstat_parser_t *parser = readstat_parser_init();
	readstat_set_metadata_handler(parser, BindMetadataHandler);
	readstat_set_variable_handler(parser, BindVariableHandler);
	readstat_set_error_handler(parser, BindErrorHandler);
	readstat_set_row_limit(parser, 0);

	DuckDBIOContext io_ctx;
	io_ctx.fs = &FileSystem::GetFileSystem(context);
	InstallDuckDBIO(parser, io_ctx);

	if (!bind_data->encoding.empty()) {
		readstat_set_file_character_encoding(parser, bind_data->encoding.c_str());
	}

	auto error = ParseWithFormat(parser, bind_data->path, bind_data->format, bind_data.get());
	readstat_set_io_ctx(parser, nullptr); // detach stack ctx before free
	readstat_parser_free(parser);

	if (error != READSTAT_OK) {
		string msg = bind_data->error_message.empty() ? readstat_error_message(error) : bind_data->error_message;
		throw IOException("Failed to read '%s': %s", bind_data->path, msg);
	}

	// Set result schema
	for (idx_t i = 0; i < bind_data->column_names.size(); i++) {
		names.push_back(bind_data->column_names[i]);
		return_types.push_back(bind_data->column_types[i]);
	}

	return std::move(bind_data);
}

// ─── Global state ───────────────────────────────────────────────────────────────

struct ReadStatGlobalState : public GlobalTableFunctionState {
	idx_t offset = 0;
	bool done = false; // true once ReadStat produces 0 rows (EOF)
};

static unique_ptr<GlobalTableFunctionState> ReadStatInitGlobal(ClientContext &context,
                                                                TableFunctionInitInput &input) {
	return make_uniq<ReadStatGlobalState>();
}

// ─── Execute-phase ReadStat callbacks ───────────────────────────────────────────

struct ReadStatExecContext {
	DataChunk *output;
	const ReadStatBindData *bind_data;
	idx_t rows_read;
	string error_message;
};

static int ExecValueHandler(int obs_index, readstat_variable_t *variable, readstat_value_t value, void *ctx) {
	auto &exec = *static_cast<ReadStatExecContext *>(ctx);
	int col = readstat_variable_get_index(variable);
	auto &vec = exec.output->data[col];

	if (readstat_value_is_system_missing(value) || readstat_value_is_tagged_missing(value) ||
	    readstat_value_is_defined_missing(value, variable)) {
		FlatVector::SetNull(vec, static_cast<idx_t>(obs_index), true);
	} else {
		WriteReadStatValue(value, variable, exec.bind_data->column_types[col],
		                   exec.bind_data->column_formats[col], vec, static_cast<idx_t>(obs_index));
	}

	auto row = static_cast<idx_t>(obs_index + 1);
	if (row > exec.rows_read) {
		exec.rows_read = row;
	}
	return READSTAT_HANDLER_OK;
}

static int ExecMetadataHandler(readstat_metadata_t *metadata, void *ctx) {
	return READSTAT_HANDLER_OK;
}

static int ExecVariableHandler(int index, readstat_variable_t *variable, const char *val_labels, void *ctx) {
	return READSTAT_HANDLER_OK;
}

static void ExecErrorHandler(const char *error_message, void *ctx) {
	auto &exec = *static_cast<ReadStatExecContext *>(ctx);
	exec.error_message = error_message ? error_message : "Unknown ReadStat error";
}

// ─── Execute function ───────────────────────────────────────────────────────────

static void ReadStatExecute(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &bind_data = input.bind_data->Cast<ReadStatBindData>();
	auto &state = input.global_state->Cast<ReadStatGlobalState>();

	// If the format reported a known row count, check the bound.
	// If row_count is -1 (unknown, e.g. XPT), we rely on ReadStat
	// returning 0 rows to signal EOF (handled below via SetCardinality(0)).
	if (bind_data.row_count >= 0 && state.offset >= static_cast<idx_t>(bind_data.row_count)) {
		return; // Done — empty output signals EOF
	}

	// Guard against re-reading after ReadStat already signaled EOF in a
	// previous call (rows_read was 0). Without this, unknown-row-count
	// formats would loop forever.
	if (state.done) {
		return;
	}

	ReadStatExecContext exec;
	exec.output = &output;
	exec.bind_data = &bind_data;
	exec.rows_read = 0;

	readstat_parser_t *parser = readstat_parser_init();
	readstat_set_metadata_handler(parser, ExecMetadataHandler);
	readstat_set_variable_handler(parser, ExecVariableHandler);
	readstat_set_value_handler(parser, ExecValueHandler);
	readstat_set_error_handler(parser, ExecErrorHandler);
	readstat_set_row_offset(parser, static_cast<long>(state.offset));
	readstat_set_row_limit(parser, static_cast<long>(STANDARD_VECTOR_SIZE));

	DuckDBIOContext io_ctx;
	io_ctx.fs = &FileSystem::GetFileSystem(context);
	InstallDuckDBIO(parser, io_ctx);

	if (!bind_data.encoding.empty()) {
		readstat_set_file_character_encoding(parser, bind_data.encoding.c_str());
	}

	auto error = ParseWithFormat(parser, bind_data.path, bind_data.format, &exec);
	readstat_set_io_ctx(parser, nullptr); // detach stack ctx before free
	readstat_parser_free(parser);

	if (error != READSTAT_OK && exec.rows_read == 0) {
		string msg = exec.error_message.empty() ? readstat_error_message(error) : exec.error_message;
		throw IOException("Failed to read '%s' at offset %llu: %s", bind_data.path, state.offset, msg);
	}

	output.SetCardinality(exec.rows_read);
	state.offset += exec.rows_read;

	// If ReadStat produced no rows in this call, the file is exhausted.
	if (exec.rows_read == 0) {
		state.done = true;
	}
}

// ─── Replacement scan ───────────────────────────────────────────────────────────

static unique_ptr<TableRef> ReadStatReplacementScan(ClientContext &context, ReplacementScanInput &input,
                                                     optional_ptr<ReplacementScanData> data) {
	auto table_name = ReplacementScan::GetFullPath(input);
	auto format = DetectFormat(table_name);
	if (format.empty()) {
		return nullptr;
	}

	auto table_function = make_uniq<TableFunctionRef>();
	vector<unique_ptr<ParsedExpression>> children;
	children.push_back(make_uniq<ConstantExpression>(Value(table_name)));
	table_function->function = make_uniq<FunctionExpression>("read_stat", std::move(children));
	return std::move(table_function);
}

// ─── Registration ───────────────────────────────────────────────────────────────

void RegisterReadStat(ExtensionLoader &loader) {
	TableFunction func("read_stat", {LogicalType::VARCHAR}, ReadStatExecute, ReadStatBind, ReadStatInitGlobal);
	func.named_parameters["format"] = LogicalType::VARCHAR;
	func.named_parameters["encoding"] = LogicalType::VARCHAR;
	loader.RegisterFunction(func);

	auto &db = loader.GetDatabaseInstance();
	auto &config = DBConfig::GetConfig(db);
	config.replacement_scans.emplace_back(ReadStatReplacementScan);
}

} // namespace duckdb
