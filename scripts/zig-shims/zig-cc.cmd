@echo off
REM CMake-compatible C compiler shim. Forwards all args to `zig cc`
REM with -target x86_64-windows-gnu so the build links against zig's
REM bundled mingw-w64 sysroot (matching sassy's link_libcpp=true).
zig cc -target x86_64-windows-gnu %*
