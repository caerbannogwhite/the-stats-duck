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

# ─── zig + libc++ build (Windows) ─────────────────────────────────────────────
# Same `windows_amd64_mingw` platform stamp as `mingw_release`, but uses zig's
# bundled clang and **libc++** (LLVM's C++ runtime) instead of mingw-gcc's
# **libstdc++** (GNU's). The platform string doesn't distinguish the two C++
# runtimes, so a `mingw_release` artifact loaded into a libc++-linked DuckDB
# host (e.g. zig-built `sassy` with `link_libcpp = true`) will pass the
# platform check and segfault at function-registration time — std::map and
# friends have ABI-incompatible layouts across the two STLs.
#
# Zig as a compiler is invoked via two cmd shims (`scripts/zig-shims/`) that
# prepend the `-target x86_64-windows-gnu` triple. CMake invokes them like
# any other compiler binary; zig's clang frontend handles all the usual
# flags (warnings, -E preprocessing, etc.).
#
# vcpkg is intentionally not consulted (its triplets don't match this
# toolchain). zlib falls back to disabled if absent — same degradation as
# the gcc-mingw target.
.PHONY: zig_mingw_release

# zig's clang frontend promotes a handful of warnings to errors by default
# that DuckDB's third_party tree trips. Demote them back to warnings:
#   -Wdate-time      pcg_random's __DATE__/__TIME__ in header (reproducibility)
#   -Wmacro-redefined miniz / re2 / etc. redefining macros across vendored TUs
ZIG_CFLAGS := -Wno-error=date-time -Wno-error=macro-redefined

zig_mingw_release: export CC=$(PROJ_DIR)scripts/zig-shims/zig-cc.cmd
zig_mingw_release: export CXX=$(PROJ_DIR)scripts/zig-shims/zig-cxx.cmd
zig_mingw_release: export DUCKDB_PLATFORM=windows_amd64_mingw
zig_mingw_release:
	mkdir -p build/zig_mingw_release
	cmake -G "MinGW Makefiles" \
	      $(BUILD_FLAGS) $(EXT_RELEASE_FLAGS) \
	      -DCMAKE_BUILD_TYPE=Release \
	      -DDUCKDB_EXPLICIT_PLATFORM=windows_amd64_mingw \
	      -DCMAKE_C_FLAGS="$(ZIG_CFLAGS)" \
	      -DCMAKE_CXX_FLAGS="$(ZIG_CFLAGS)" \
	      -S $(DUCKDB_SRCDIR) -B build/zig_mingw_release
	# Build only the extension + the repository copy step. The default `all`
	# target also builds duckdb's shell tool (linenoise), which calls a few
	# POSIX-only APIs (`getpid`) that zig's mingw sysroot doesn't expose. We
	# don't ship the shell, so skip it.
	cmake --build build/zig_mingw_release --config Release --parallel $(MINGW_JOBS) \
	      --target stats_duck_loadable_extension duckdb_local_extension_repo
