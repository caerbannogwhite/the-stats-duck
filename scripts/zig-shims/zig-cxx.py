#!/usr/bin/env python3
"""CMake-compatible C++ compiler shim that forwards to `zig c++`.

The plain `zig-cxx.cmd` shim works for compilation but trips at link time:
DuckDB's `extension_build_tools.cmake` adds `-Wl,--exclude-libs,ALL` for any
Clang or GCC compiler on a non-Apple, non-z/OS host (line 141, 152). The
Windows-with-Clang case falls through to the GNU-on-Linux branch because
there's no escape hatch for a clang-but-on-Windows scenario.

`lld` for PE/COFF — what zig invokes on the `*-windows-gnu` target — does
not implement `--exclude-libs`, so the link fails. Fortunately the flag is
purely a symbol-hiding optimization on ELF; on Windows, symbol export is
controlled by `__declspec(dllexport)` / `dllimport` and the absence of
`--exclude-libs` is a no-op. Strip it.

This shim also strips a couple of other GNU-only linker flags that DuckDB
adds in the same code path (`--gc-sections`) — also redundant on PE/COFF
where the linker discards unreferenced code by default.

CMake on Windows passes long argument lists via response files (`@file.rsp`).
The flags we care about are inside those files, not on the command line, so
the shim expands `@file` references inline before filtering.
"""
import shlex
import subprocess
import sys

# GNU/ELF-only linker flags that lld for PE/COFF rejects but that have no
# functional impact on the Windows build. Any -Wl,arg matching these prefixes
# is dropped before the args reach `zig c++`.
DROP_LINKER_PREFIXES = (
    "-Wl,--exclude-libs",
    "-Wl,--gc-sections",
)


def expand_response_files(args):
    out = []
    for a in args:
        if a.startswith("@"):
            with open(a[1:], "r", encoding="utf-8") as f:
                # Response files use shell-style quoting. shlex.split handles
                # the simple cases CMake emits (paths with spaces, double-quoted
                # tokens, no environment variable expansion).
                out.extend(shlex.split(f.read(), posix=False))
        else:
            out.append(a)
    return out


def main() -> int:
    args = expand_response_files(sys.argv[1:])
    args = [a for a in args if not a.startswith(DROP_LINKER_PREFIXES)]
    cmd = ["zig", "c++", "-target", "x86_64-windows-gnu", *args]
    return subprocess.call(cmd)


if __name__ == "__main__":
    sys.exit(main())
