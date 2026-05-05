@echo off
REM CMake-compatible C++ compiler shim that delegates arg filtering to a
REM Python helper before forwarding to `zig c++`. The Python shim strips
REM GNU-only linker flags (-Wl,--exclude-libs,ALL etc.) that DuckDB's
REM extension build pipeline emits when CMAKE_CXX_COMPILER_ID is "Clang"
REM but that lld-for-PE/COFF doesn't accept. See zig-cxx.py.
python "%~dp0zig-cxx.py" %*
