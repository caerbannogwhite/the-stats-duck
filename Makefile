PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

EXT_NAME=STATS_DUCK
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile

# ─── MinGW build (Windows) ────────────────────────────────────────────────────
# Produces a stats_duck.duckdb_extension stamped with platform string
# `windows_amd64_mingw` so it loads into mingw-built DuckDB hosts (e.g.
# the zig-bundled DuckDB inside the sassy SAS interpreter, or DuckDB's
# own duckdb_cli-windows-amd64-mingw.zip releases). The default `release`
# target on Windows uses MSVC and stamps `windows_amd64`; both binaries
# can coexist — this target only writes under `build/mingw_release/`.
#
# Toolchain: takes gcc/g++ from $PATH. On a typical Windows dev box that's
# either TDM-GCC, MSYS2's mingw-w64-x86_64-toolchain, or rtools' bundled
# mingw. Anything ABI-compatible with x86_64-w64-mingw32 works.
#
# vcpkg is intentionally not consulted — its Windows default triplet is
# MSVC and would not work here. zlib (the only optional vcpkg dep, used
# for SPSS .zsav read/write) falls back to disabled if not found in the
# system include paths, see CMakeLists.txt:97.
.PHONY: mingw_release

mingw_release: export DUCKDB_PLATFORM=windows_amd64_mingw
mingw_release: export CC=gcc
mingw_release: export CXX=g++
# DuckDB's local_file_system.cpp calls Vista-era APIs (GetFinalPathNameByHandleW).
# Some mingw distributions (notably TDM-GCC 10.3) default to _WIN32_WINNT=0x0502
# which guards those declarations out — bump to Windows 10 (0x0A00). MSYS2's
# mingw-w64 already defaults to 0x0A00; the flag is harmless there.
MINGW_WIN32_API_FLAG := -D_WIN32_WINNT=0x0A00
MINGW_JOBS ?= 8
mingw_release:
	mkdir -p build/mingw_release
	cmake -G "MinGW Makefiles" \
	      $(BUILD_FLAGS) $(EXT_RELEASE_FLAGS) \
	      -DCMAKE_BUILD_TYPE=Release \
	      -DDUCKDB_EXPLICIT_PLATFORM=windows_amd64_mingw \
	      -DCMAKE_C_FLAGS="$(MINGW_WIN32_API_FLAG)" \
	      -DCMAKE_CXX_FLAGS="$(MINGW_WIN32_API_FLAG)" \
	      -S $(DUCKDB_SRCDIR) -B build/mingw_release
	cmake --build build/mingw_release --config Release --parallel $(MINGW_JOBS)
