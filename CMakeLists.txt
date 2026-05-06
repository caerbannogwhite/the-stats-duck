cmake_minimum_required(VERSION 2.8.12...3.29)

set(TARGET_NAME stats_duck)
set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})

include_directories(src/include)

# ── ReadStat third-party library ─────────────────────────────────────────────

set(READSTAT_DIR ${CMAKE_CURRENT_SOURCE_DIR}/third_party/readstat/src)

include_directories(${READSTAT_DIR})
include_directories(${READSTAT_DIR}/sas)
include_directories(${READSTAT_DIR}/spss)
include_directories(${READSTAT_DIR}/stata)

set(READSTAT_SOURCES
    ${READSTAT_DIR}/CKHashTable.c
    ${READSTAT_DIR}/readstat_bits.c
    ${READSTAT_DIR}/readstat_convert.c
    ${READSTAT_DIR}/readstat_error.c
    ${READSTAT_DIR}/readstat_io_unistd.c
    ${READSTAT_DIR}/readstat_malloc.c
    ${READSTAT_DIR}/readstat_metadata.c
    ${READSTAT_DIR}/readstat_parser.c
    ${READSTAT_DIR}/readstat_value.c
    ${READSTAT_DIR}/readstat_variable.c
    ${READSTAT_DIR}/readstat_writer.c
    ${READSTAT_DIR}/sas/ieee.c
    ${READSTAT_DIR}/sas/readstat_sas.c
    ${READSTAT_DIR}/sas/readstat_sas_rle.c
    ${READSTAT_DIR}/sas/readstat_sas7bdat_read.c
    ${READSTAT_DIR}/sas/readstat_sas7bdat_write.c
    ${READSTAT_DIR}/sas/readstat_xport.c
    ${READSTAT_DIR}/sas/readstat_xport_parse_format.c
    ${READSTAT_DIR}/sas/readstat_xport_read.c
    ${READSTAT_DIR}/sas/readstat_xport_write.c
    ${READSTAT_DIR}/spss/readstat_por.c
    ${READSTAT_DIR}/spss/readstat_por_parse.c
    ${READSTAT_DIR}/spss/readstat_por_read.c
    ${READSTAT_DIR}/spss/readstat_por_write.c
    ${READSTAT_DIR}/spss/readstat_sav.c
    ${READSTAT_DIR}/spss/readstat_sav_compress.c
    ${READSTAT_DIR}/spss/readstat_sav_parse.c
    ${READSTAT_DIR}/spss/readstat_sav_parse_mr_name.c
    ${READSTAT_DIR}/spss/readstat_sav_parse_timestamp.c
    ${READSTAT_DIR}/spss/readstat_sav_read.c
    ${READSTAT_DIR}/spss/readstat_sav_write.c
    ${READSTAT_DIR}/spss/readstat_spss.c
    ${READSTAT_DIR}/spss/readstat_spss_parse.c
    ${READSTAT_DIR}/spss/readstat_zsav_compress.c
    ${READSTAT_DIR}/spss/readstat_zsav_read.c
    ${READSTAT_DIR}/spss/readstat_zsav_write.c
    ${READSTAT_DIR}/stata/readstat_dta.c
    ${READSTAT_DIR}/stata/readstat_dta_parse_timestamp.c
    ${READSTAT_DIR}/stata/readstat_dta_read.c)

# ── Extension sources ────────────────────────────────────────────────────────

set(EXTENSION_SOURCES
    src/stats_duck_extension.cpp
    src/ttest_function.cpp
    src/ttest_compute.cpp
    src/ttest_agg_function.cpp
    src/nonparametric_function.cpp
    src/distribution_functions.cpp
    src/summary_stats_function.cpp
    src/correlation_function.cpp
    src/normality_function.cpp
    src/anova_function.cpp
    src/chisq_function.cpp
    src/read_stat_function.cpp
    src/read_stat_types.cpp
    src/sas_export_function.cpp
    src/ggsql.cpp
    src/ggsql_grammar.cpp
    src/ggsql_marks.cpp
    ${READSTAT_SOURCES})

# ── Platform-specific iconv/zlib handling ────────────────────────────────────

find_package(ZLIB)

# On Windows, always use vendored win_iconv (avoids MinGW libiconv build issues)
if(WIN32)
    set(WIN_ICONV_DIR ${CMAKE_CURRENT_SOURCE_DIR}/third_party/win_iconv)
    include_directories(${WIN_ICONV_DIR})
    list(APPEND EXTENSION_SOURCES ${WIN_ICONV_DIR}/win_iconv.c)
else()
    find_package(Iconv)
endif()

# Exclude zsav sources when zlib is unavailable
if(NOT ZLIB_FOUND)
    list(REMOVE_ITEM EXTENSION_SOURCES
        ${READSTAT_DIR}/spss/readstat_zsav_compress.c
        ${READSTAT_DIR}/spss/readstat_zsav_read.c
        ${READSTAT_DIR}/spss/readstat_zsav_write.c)
endif()

# ── Build targets ────────────────────────────────────────────────────────────

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
if(ZLIB_FOUND)
    target_compile_definitions(${EXTENSION_NAME} PRIVATE HAVE_ZLIB)
    target_link_libraries(${EXTENSION_NAME} ZLIB::ZLIB)
endif()
if(Iconv_FOUND AND NOT WIN32)
    target_link_libraries(${EXTENSION_NAME} Iconv::Iconv)
endif()
# Align readstat's iconv inbuf typedef with win_iconv's prototype on Windows.
# readstat's readstat_iconv_inbuf_t is `ICONV_CONST char **`, win_iconv's iconv()
# takes `WINICONV_CONST char **` which defaults to `const char **`. Without
# ICONV_CONST=const, the cast in readstat_convert.c mismatches by one `const`
# and newer GCCs (≥ windows-latest runner image, May 2026) reject it.
if(WIN32)
    target_compile_definitions(${EXTENSION_NAME} PRIVATE ICONV_CONST=const)
endif()

set(PARAMETERS "-warnings")
# Force i64 → (i32,i32) legalization at the WASM ABI boundary. Emscripten ≥ 4.0
# defaults to -sWASM_BIGINT=1 (native i64), but @duckdb/duckdb-wasm 1.33.x and
# the deployed phase-2 extension speak the legalized ABI. Mismatched modules
# fail to link with a TemplatedValidityMask::Copy LinkError on instantiation.
# Read by build_loadable_extension_directory at duckdb/extension/extension_build_tools.cmake:186
# and spliced into the post-build emcc command.
if(EMSCRIPTEN)
    string(TOUPPER ${TARGET_NAME} _STATS_DUCK_UPPER)
    set(DUCKDB_EXTENSION_${_STATS_DUCK_UPPER}_LINKED_LIBS "-sWASM_BIGINT=0")
endif()
build_loadable_extension(${TARGET_NAME} ${PARAMETERS} ${EXTENSION_SOURCES})
if(ZLIB_FOUND)
    target_compile_definitions(${LOADABLE_EXTENSION_NAME} PRIVATE HAVE_ZLIB)
    target_link_libraries(${LOADABLE_EXTENSION_NAME} ZLIB::ZLIB)
endif()
if(Iconv_FOUND AND NOT WIN32)
    target_link_libraries(${LOADABLE_EXTENSION_NAME} Iconv::Iconv)
endif()
if(WIN32)
    target_compile_definitions(${LOADABLE_EXTENSION_NAME} PRIVATE ICONV_CONST=const)
endif()

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")

# ── Phase 3 cross-extension WASM spike ──────────────────────────────────────
# Builds a second loadable extension `marks_demo` that registers
# `ggsql_mark_v1_demo`. Lets us verify cross-WASM-module mark dispatch (a
# stats_duck.wasm session looking up + invoking a mark registered by
# marks_demo.wasm). Throwaway — do not ship.
if(BUILD_GGSQL_SPIKE OR EMSCRIPTEN)
    set(MARKS_DEMO_NAME marks_demo)
    set(MARKS_DEMO_SOURCES spike/marks_demo/marks_demo_extension.cpp)
    if(EMSCRIPTEN)
        set(DUCKDB_EXTENSION_MARKS_DEMO_LINKED_LIBS "-sWASM_BIGINT=0")
    endif()
    build_loadable_extension(${MARKS_DEMO_NAME} "-warnings" ${MARKS_DEMO_SOURCES})
endif()
