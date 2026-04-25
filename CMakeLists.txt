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
    src/ggsql.cpp
    src/ggsql_grammar.cpp
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

set(PARAMETERS "-warnings")
build_loadable_extension(${TARGET_NAME} ${PARAMETERS} ${EXTENSION_SOURCES})
if(ZLIB_FOUND)
    target_compile_definitions(${LOADABLE_EXTENSION_NAME} PRIVATE HAVE_ZLIB)
    target_link_libraries(${LOADABLE_EXTENSION_NAME} ZLIB::ZLIB)
endif()
if(Iconv_FOUND AND NOT WIN32)
    target_link_libraries(${LOADABLE_EXTENSION_NAME} Iconv::Iconv)
endif()

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
