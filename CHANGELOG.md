# Changelog

All notable changes to **The Stats Duck** are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

The extension installs and loads in DuckDB under the technical name `stats_duck` —
that name is preserved across releases for backward compatibility.

## [Unreleased]

### Added

- Apache 2.0 license, citation file, and changelog.

## [0.1.0] - TBD

First public release.

### Added

- `ttest_1samp(column, [mu], [alpha], [alternative])` — one-sample t-test as a streaming aggregate.
- `ttest_2samp(column1, column2, [equal_var], [alpha], [alternative])` — two-sample t-test (Welch's by default, Student's pooled when `equal_var := true`).
- `ttest_paired(column1, column2, [alpha], [alternative])` — paired t-test.
- `mann_whitney_u(column1, column2, [alternative])` — Mann–Whitney U test (Wilcoxon rank-sum).
- `wilcoxon_signed_rank(column1, column2, [alternative])` — Wilcoxon signed-rank test.
- `read_stat(path, [format], [encoding])` — read SAS (`.sas7bdat`, `.xpt`), SPSS (`.sav`, `.zsav`, `.por`) and Stata (`.dta`) files via [ReadStat](https://github.com/WizardMac/ReadStat).
- Replacement scan so `SELECT * FROM 'data.sas7bdat'` works without an explicit `read_stat()` call.
- Automatic detection of date, datetime, and time columns from format metadata, converted to DuckDB temporal types.
- All ReadStat I/O is routed through DuckDB's `FileSystem`, enabling reads from `httpfs://`, `s3://`, registered WASM file buffers, and any other DuckDB-registered virtual filesystem.

### Build & CI

- Targets DuckDB v1.2.2.
- Builds for `linux_amd64`, `osx_amd64`, `osx_arm64`, `windows_amd64`, `windows_amd64_mingw`, `wasm_mvp`, `wasm_eh`, `wasm_threads`.
- Vendored `win_iconv` is always used on Windows; `find_package(Iconv)` is only consulted on non-Windows platforms.
- `linux_amd64_musl` is excluded due to a known upstream issue in the v1.2.2 extension-ci-tools Alpine Dockerfile.

[Unreleased]: https://github.com/caerbannogwhite/the-stats-duck/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/caerbannogwhite/the-stats-duck/releases/tag/v0.1.0
