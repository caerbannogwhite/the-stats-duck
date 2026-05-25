# Local DuckDB submodule patches

This directory holds small patches that need to be applied to the pinned
`duckdb/` submodule before it will compile in some environments. They are
candidates for upstream contribution; the parent repository keeps them here
so the submodule pointer stays clean.

## How to apply

From the repository root:

```sh
cd duckdb && git apply ../patches/<patch-file>.patch
```

The submodule will appear as "modified content" in `git status` from the
parent repo — that's expected and means the patch is in place.

To revert (e.g. before updating the submodule):

```sh
cd duckdb && git checkout -- third_party/fmt/include/fmt/format.h
```

## Inventory

### `0001-fmt-secure-scl-msvc-v145.patch`

**Affects:** MSVC v145 toolset (VS2026) builds only. POSIX / older MSVC builds
compile fine without it.

**Symptom (without the patch):**
```
duckdb\third_party\fmt\include\fmt\format.h(326): error C2653:
  'stdext': is not a class or namespace name
```

**Cause:** upstream fmt at this version checks `#ifdef _SECURE_SCL` to decide
whether to wrap pointers in `stdext::checked_array_iterator`. Microsoft's CRT
auto-defines `_SECURE_SCL` to `0` in Release builds (so `#ifdef` is true even
when the value is `0`). The v145 toolset then removed
`stdext::checked_array_iterator` entirely, so the wrapped-pointer branch
fails to compile in Release.

**Fix:** check the value, not just defined-ness — `#if defined(_SECURE_SCL) &&
_SECURE_SCL > 0`. Release builds (`_SECURE_SCL=0`) correctly take the
plain-pointer `else` branch.
