# Changelog

All notable changes to **The Stats Duck** are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

The extension installs and loads in DuckDB under the technical name `stats_duck` —
that name is preserved across releases for backward compatibility.

## [Unreleased]

### Changed

- Target DuckDB v1.5.1 (up from v1.2.2). Extensions built for 0.1.x are
  not ABI-compatible and must be reinstalled.
- Switched from the `ExtensionUtil::RegisterFunction(db, ...)` API to the
  newer `ExtensionLoader::RegisterFunction(...)` pattern required by
  DuckDB v1.5.x.
- Re-enabled `linux_amd64_musl` CI job (the upstream Alpine Dockerfile bug
  that affected v1.2.2 is fixed in v1.5.1's extension-ci-tools).

## [0.1.0-mortician] - 2026-04-13

First public release.

### Hypothesis tests (aggregate functions)

- `ttest_1samp(column, [mu], [alpha], [alternative])` — one-sample t-test.
- `ttest_2samp(column1, column2, [equal_var], [alpha], [alternative])` — two-sample t-test (Welch's by default, Student's pooled when `equal_var := true`).
- `ttest_paired(column1, column2, [alpha], [alternative])` — paired t-test.
- `mann_whitney_u(column1, column2, [alternative])` — Mann–Whitney U test (Wilcoxon rank-sum).
- `wilcoxon_signed_rank(column1, column2, [alternative])` — Wilcoxon signed-rank test.
- `pearson_test(x, y, [alpha], [alternative])` — Pearson correlation with t-test significance and Fisher-z confidence interval.
- `anova_oneway(value, group)` — one-way ANOVA with F-statistic, p-value, SS, and eta-squared.
- `chisq_independence(row, col)` — chi-square test of independence on a contingency table.
- `chisq_goodness_of_fit(category)` — chi-square goodness-of-fit against the uniform distribution.
- `jarque_bera(column)` — Jarque–Bera normality test based on skewness and kurtosis.

### Descriptive statistics (aggregate functions)

- `summary_stats(column)` — returns n, n_missing, mean, sd, variance, min, q1, median, q3, max, iqr, skewness, and excess kurtosis as a STRUCT.

### Distribution functions (scalar)

- `dnorm` / `pnorm` / `qnorm` — normal distribution PDF, CDF, and quantile (1–3 argument overloads).
- `dt` / `pt` / `qt` — Student's t distribution.
- `dchisq` / `pchisq` / `qchisq` — chi-square distribution.
- `df` / `pf` / `qf` — F distribution.

### Data import (table function)

- `read_stat(path, [format], [encoding])` — read SAS (`.sas7bdat`, `.xpt`), SPSS (`.sav`, `.zsav`, `.por`) and Stata (`.dta`) files via [ReadStat](https://github.com/WizardMac/ReadStat).
- Replacement scan so `SELECT * FROM 'data.sas7bdat'` works without an explicit `read_stat()` call.
- Automatic detection of date, datetime, and time columns from format metadata, converted to DuckDB temporal types.
- All ReadStat I/O is routed through DuckDB's `FileSystem`, enabling reads from `httpfs://`, `s3://`, registered WASM file buffers, and any other DuckDB-registered virtual filesystem.
- Non-UTF-8 strings (e.g., Windows-1252 in XPT files) are sanitized to valid UTF-8 with U+FFFD replacement.

### Build & CI

- Targets DuckDB v1.2.2.
- Builds for `linux_amd64`, `osx_amd64`, `osx_arm64`, `windows_amd64`, `windows_amd64_mingw`, `wasm_mvp`, `wasm_eh`, `wasm_threads`.
- Vendored `win_iconv` is always used on Windows; `find_package(Iconv)` is only consulted on non-Windows platforms.
- `linux_amd64_musl` is excluded due to a known upstream issue in the v1.2.2 extension-ci-tools Alpine Dockerfile.
- WASM binaries served via GitHub Pages at `https://caerbannogwhite.github.io/the-stats-duck`.

[0.1.0-mortician]: https://github.com/caerbannogwhite/the-stats-duck/releases/tag/v0.1.0
