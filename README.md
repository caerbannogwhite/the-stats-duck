# The Stats Duck

A statistical computing toolkit for DuckDB.

The Stats Duck brings statistical workflows — descriptive statistics, hypothesis
tests, grammar-of-graphics visualization, and direct readers/writers for SAS,
SPSS, and Stata files — into SQL. Functions are implemented as streaming
aggregates and scalar primitives, so they scale from local notebooks to
billion-row warehouses and also run inside DuckDB-WASM in the browser.

> The extension installs and loads in DuckDB under the technical name
> `stats_duck` (matching the binary, the SQL function namespace, and the
> `INSTALL` keyword). "The Stats Duck" is the project / brand name; `stats_duck`
> is what you type at the SQL prompt.

## Scope

The Stats Duck is meant to cover the everyday work of a general-purpose
statistician without leaving SQL. The current release covers four areas:

- **Hypothesis tests** — parametric and non-parametric, with effect sizes and
  confidence intervals returned alongside the test statistic.
- **Visualizations (ggsql)** — `VISUALIZE … FROM <table> DRAW <mark>`, a
  Posit-published Grammar-of-Graphics SQL dialect compiled to Vega-Lite v5.
  No server-side rendering: the extension emits a spec + per-layer SQL, and
  the client (browser, notebook, …) runs the SQL and feeds the rows to vega.
- **Statistical file I/O** — first-class readers AND writers for SAS, SPSS, and
  Stata files, integrated with DuckDB's virtual file system so they work
  transparently with local paths, `httpfs://`, `s3://`, and registered WASM
  file buffers.
- **Streaming aggregates** — every test is a single-pass aggregate over the
  data, so it composes naturally with `GROUP BY`, window frames, and DuckDB's
  parallel execution.

Future releases will add Spearman/Kendall correlations, regression with full
diagnostics, multiple-testing corrections, and more distribution families.

## Functions

### Hypothesis tests (aggregate)

| Function                                                             | Description                              |
| -------------------------------------------------------------------- | ---------------------------------------- |
| `ttest_1samp(column, [mu], [alpha], [alternative])`                  | One-sample t-test                        |
| `ttest_2samp(column1, column2, [equal_var], [alpha], [alternative])` | Two-sample t-test (Welch's or Student's) |
| `ttest_paired(column1, column2, [alpha], [alternative])`             | Paired t-test                            |
| `mann_whitney_u(column1, column2, [alternative])`                    | Mann-Whitney U test (Wilcoxon rank-sum)  |
| `wilcoxon_signed_rank(column1, column2, [alternative])`              | Wilcoxon signed-rank test                |
| `pearson_test(x, y, [alpha], [alternative])`                         | Pearson correlation with significance    |
| `anova_oneway(value, group)`                                         | One-way ANOVA                            |
| `chisq_independence(row, col)`                                       | Chi-square test of independence          |
| `chisq_goodness_of_fit(category)`                                    | Chi-square goodness-of-fit (uniform)     |
| `jarque_bera(column)`                                                | Jarque-Bera normality test               |

All tests return a `STRUCT` with the test statistic, degrees of freedom,
p-value, and relevant effect sizes / confidence intervals.

### Descriptive statistics (aggregate)

| Function                | Description                                                            |
| ----------------------- | ---------------------------------------------------------------------- |
| `summary_stats(column)` | n, n_missing, mean, sd, variance, min, q1, median, q3, max, iqr, skewness, kurtosis |

### Distribution functions (scalar)

| Function                          | Description                  |
| --------------------------------- | ---------------------------- |
| `dnorm(x, [mean], [sd])`         | Normal PDF                   |
| `pnorm(x, [mean], [sd])`         | Normal CDF                   |
| `qnorm(p, [mean], [sd])`         | Normal quantile              |
| `dt(x, df)`                      | Student's t PDF              |
| `pt(x, df)`                      | Student's t CDF              |
| `qt(p, df)`                      | Student's t quantile         |
| `dchisq(x, df)`                  | Chi-square PDF               |
| `pchisq(x, df)`                  | Chi-square CDF               |
| `qchisq(p, df)`                  | Chi-square quantile          |
| `df(x, df1, df2)`                | F distribution PDF           |
| `pf(x, df1, df2)`                | F distribution CDF           |
| `qf(p, df1, df2)`                | F distribution quantile      |

### Data import (table function)

| Function                                | Description                   |
| --------------------------------------- | ----------------------------- |
| `read_stat(path, [format], [encoding])` | Read SAS / SPSS / Stata files |

### Data export (COPY function)

| Statement                                  | Description                      |
| ------------------------------------------ | -------------------------------- |
| `COPY <table> TO 'file.xpt'`               | Write SAS Transport (XPT v5)     |
| `COPY <table> TO 'file.sas7bdat'`          | Write SAS7BDAT (see caveat below)|
| `COPY <table> TO 'file.sav'`               | Write SPSS SAV                   |

> **SAS7BDAT caveat.** ReadStat's SAS7BDAT writer is reverse-engineered: files
> round-trip through ReadStat-family readers (this extension's `read_stat()`,
> pyreadstat, haven, R) but are **not opened by real SAS / SAS Universal
> Viewer / SAS OnDemand**. Use XPT for SAS-native readability.

### Visualizations (ggsql parser extension)

A Grammar-of-Graphics SQL dialect: `VISUALIZE` returns a single row with two
columns — `spec` (a complete Vega-Lite v5 JSON spec) and `layer_sqls` (a
`MAP(VARCHAR, VARCHAR)` of named SQL strings, one per layer). The client
runs each layer's SQL and feeds the rows to vega-embed via the `datasets` API.

```
VISUALIZE <expr> AS <aesthetic> [: <type>] (, <expr> AS <aesthetic> ...)
FROM <table>
DRAW <mark> (DRAW <mark>)*
[FACET BY <expr> [ROWS | COLS]]
[SCALE <channel> {TO <scheme> | ZERO true|false | DOMAIN <lo> <hi>}]*
```

**Marks:** `point`, `line`, `bar`, `histogram`, `text`, `area`, `rule`, `tick`,
`errorbar`, `errorband`, `boxplot`. Custom marks register as `ggsql_mark_v1_<name>`
scalar functions and are discovered via DuckDB's catalog, so other extensions can
ship their own marks without modifying stats_duck.

**Aesthetic channels:** `x`, `y`, `color`, `fill`, `stroke`, `shape`, `size`,
`opacity`, `tooltip`, `text`, `x2`, `y2`. Unknown channels are silently dropped.

**Type overrides:** append `:quantitative`, `:ordinal`, `:nominal`, or
`:temporal` to an aesthetic to force its Vega-Lite type
(e.g. `year AS color:ordinal`).

**t-test result fields:** `test_type`, `t_statistic`, `df`, `p_value`, `alternative`, `mean_diff`, `ci_lower`, `ci_upper`, `cohens_d`

**Mann-Whitney result fields:** `test_type`, `u_statistic`, `z_statistic`, `p_value`, `alternative`, `rank_biserial`

**Wilcoxon result fields:** `test_type`, `w_statistic`, `z_statistic`, `p_value`, `alternative`, `effect_size_r`

**Pearson result fields:** `test_type`, `r`, `t_statistic`, `df`, `p_value`, `alternative`, `ci_lower`, `ci_upper`, `n`

**ANOVA result fields:** `test_type`, `f_statistic`, `df_between`, `df_within`, `p_value`, `ss_between`, `ss_within`, `eta_squared`, `n_groups`, `n`

**Chi-square result fields:** `test_type`, `chi_square`, `df`, `p_value`, `n`, `n_rows`/`n_cols` or `n_categories`

**Jarque-Bera result fields:** `test_type`, `jb_statistic`, `skewness`, `excess_kurtosis`, `df`, `p_value`, `n`

### Common parameters

| Parameter     | Type      | Default       | Description                                                      |
| ------------- | --------- | ------------- | ---------------------------------------------------------------- |
| `mu`          | `DOUBLE`  | `0.0`         | Hypothesized population mean (one-sample t-test only)            |
| `equal_var`   | `BOOLEAN` | `false`       | Assume equal variances — Student's pooled test (two-sample only) |
| `alpha`       | `DOUBLE`  | `0.05`        | Significance level for confidence intervals (t-tests only)       |
| `alternative` | `VARCHAR` | `'two-sided'` | `'two-sided'`, `'less'`, or `'greater'`                          |

## Examples

### Hypothesis tests

#### One-sample t-test

```sql
-- Test whether the mean of v3 differs from zero
SELECT ttest_1samp(v3) FROM measurements;

-- Test against a specific mean
SELECT ttest_1samp(v3, 5.0) FROM measurements;

-- One-sided test with 99% confidence interval
SELECT ttest_1samp(v3, 0.0, 0.01, 'greater') FROM measurements;
```

#### Two-sample t-test

```sql
-- Welch's t-test (default, does not assume equal variances)
SELECT ttest_2samp(group_a, group_b) FROM experiment;

-- Student's t-test (assumes equal variances)
SELECT ttest_2samp(group_a, group_b, true) FROM experiment;
```

#### Paired t-test

```sql
-- Compare before/after measurements
SELECT ttest_paired(before, after) FROM patients;
```

#### Mann-Whitney U test

Non-parametric alternative to the two-sample t-test:

```sql
-- Compare two independent samples
SELECT mann_whitney_u(group_a, group_b) FROM experiment;

-- One-sided test
SELECT mann_whitney_u(group_a, group_b, 'less') FROM experiment;
```

#### Wilcoxon signed-rank test

Non-parametric alternative to the paired t-test:

```sql
-- Compare paired measurements
SELECT wilcoxon_signed_rank(before, after) FROM patients;
```

### Working with results

The result of any hypothesis test is a `STRUCT` — access individual fields with
dot notation:

```sql
SELECT (ttest_1samp(v3)).t_statistic,
       (ttest_1samp(v3)).p_value
FROM measurements;
```

Or unpack all fields:

```sql
SELECT (r).*
FROM (SELECT ttest_1samp(v3) AS r FROM measurements);
```

#### Group-by

Run a test per group with no extra plumbing:

```sql
SELECT id3,
       (ttest_1samp(v3)).t_statistic,
       (ttest_1samp(v3)).p_value
FROM measurements
GROUP BY id3;
```

#### Inline data

Use `VALUES` for quick experiments with literal data:

```sql
SELECT (r).*
FROM (SELECT ttest_1samp(v) AS r FROM (VALUES (2.0), (4.0), (6.0), (8.0), (10.0)) AS t(v));
```

### Data import

#### Reading statistical file formats

Read SAS (`.sas7bdat`, `.xpt`), SPSS (`.sav`, `.zsav`, `.por`), and Stata
(`.dta`) files:

```sql
-- Auto-detect format from file extension
SELECT * FROM read_stat('data.sas7bdat');

-- Explicit format
SELECT * FROM read_stat('data.dat', format := 'dta');

-- Replacement scan: query files directly
SELECT * FROM 'survey.sav';
```

Date, datetime, and time columns are automatically detected from format
metadata and converted to DuckDB's native temporal types.

`read_stat` is wired through DuckDB's virtual file system, so the same call
works against:

- local paths: `read_stat('/data/survey.sav')`
- remote files (with the `httpfs` extension loaded): `read_stat('https://…/survey.sav')`
- object stores: `read_stat('s3://bucket/survey.sav')`
- in-browser DuckDB-WASM file buffers registered via `registerFileBuffer()`

#### Writing statistical file formats

The same VFS-backed writers, exposed as DuckDB `COPY` functions:

```sql
-- SAS Transport (XPT v5) — universally readable, FDA / pharma standard
COPY survey TO 'survey.xpt';

-- SAS7BDAT — round-trips through stats_duck and pyreadstat (NOT real SAS, see caveat)
COPY survey TO 'survey.sas7bdat' (COMPRESSION 'rows');

-- SPSS SAV
COPY survey TO 'survey.sav';

-- COPY <subquery> form works too
COPY (SELECT * FROM huge_table WHERE region = 'EU') TO 'eu.xpt';
```

NULL handling differs by storage format: numeric NULLs round-trip as
SAS/SPSS system-missing, but VARCHAR NULLs collapse to empty strings (these
formats have no NULL/empty distinction for character columns).

### Visualizations (ggsql)

```sql
-- Set up — penguin morphology, classic ggplot2 demo data
CREATE TABLE penguins AS SELECT * FROM (VALUES
    (39.1, 18.7, 'Adelie',    2018), (39.5, 17.4, 'Adelie',    2019),
    (44.0, 18.0, 'Gentoo',    2020), (45.2, 14.5, 'Chinstrap', 2021)
) AS t(bill_len, bill_dep, species, year);
```

#### Scatter

```sql
VISUALIZE bill_len AS x, bill_dep AS y FROM penguins DRAW point;
```

#### Scatter colored by species, with a viridis scale

```sql
VISUALIZE bill_len AS x, bill_dep AS y, species AS color
FROM penguins
DRAW point
SCALE color TO viridis;
```

#### Multi-layer (scatter + line overlay)

```sql
VISUALIZE bill_len AS x, bill_dep AS y FROM penguins DRAW point DRAW line;
```

#### Faceted plot — one panel per species, vertically stacked

```sql
VISUALIZE bill_len AS x, bill_dep AS y FROM penguins
DRAW point FACET BY species ROWS;
```

#### SQL expressions in mappings

```sql
VISUALIZE bill_len * 2 AS x, log(bill_dep) AS y FROM penguins DRAW point;
```

#### Type-annotated aesthetic (year is INTEGER but should be ordinal here)

```sql
VISUALIZE bill_len AS x, bill_dep AS y, year AS color:ordinal
FROM penguins
DRAW point
SCALE color TO viridis;
```

The output of any `VISUALIZE` query is a single row with `(spec, layer_sqls)`.
The client side (a browser app, a notebook renderer, …) is responsible for
running each layer's SQL and feeding the rows into vega-embed via its
`datasets` API. See [Bedevere Wise](https://github.com/caerbannogwhite/bedevere-wise)
for a reference DuckDB-WASM consumer.

## Building

```bash
git submodule update --init --recursive
make release
```

### Building with MinGW

DuckDB extensions are tagged at build time with a *platform string* that the
host process uses to validate ABI compatibility before loading. The default
`make release` on Windows uses MSVC and stamps `windows_amd64`. A consumer
DuckDB built with mingw-w64 (e.g. via zig's bundled clang + mingw-w64
sysroot, or the rtools/MSYS2 toolchains) will refuse to load that binary
with:

```
Failed to load 'stats_duck.duckdb_extension'. The file was built for the
platform 'windows_amd64', but we can only load extensions built for
platform 'windows_amd64_mingw'.
```

For those hosts, build with:

```bash
make mingw_release
```

This drives a CMake build with `-G "MinGW Makefiles"`, sets `CC=gcc`,
`CXX=g++`, and stamps `DUCKDB_EXPLICIT_PLATFORM=windows_amd64_mingw`. The
binary lands at:

```
build/mingw_release/extension/stats_duck/stats_duck.duckdb_extension
build/mingw_release/repository/<duckdb_version>/windows_amd64_mingw/stats_duck.duckdb_extension
```

The MSVC `windows_amd64` binary at `build/release/...` is left alone — both
flavors can coexist on disk and ship side by side. CI already produces both
on every release; this target is for local-only verification or for
distributing to consumers that bundle their own DuckDB and need an exact
ABI match.

**Toolchain requirements.** Anything ABI-compatible with `x86_64-w64-mingw32`
works. Tested with TDM-GCC 10.3 and MSYS2's `mingw-w64-x86_64-toolchain`. On
TDM-GCC the Makefile passes `-D_WIN32_WINNT=0x0A00` because TDM-GCC's default
predates the Vista-era APIs DuckDB uses; MSYS2 already defaults to that
value, where the flag is harmless.

**vcpkg is intentionally not used** for the MinGW build — its Windows
default triplet is MSVC and would not match the toolchain. zlib (the only
external dep, used for SPSS `.zsav` read/write) becomes optional and
gracefully degrades if absent (see `CMakeLists.txt:97`); install zlib via
your mingw package manager if you need `.zsav` support.

### Building with zig as the C++ toolchain (matches sassy)

DuckDB's platform string `windows_amd64_mingw` does not distinguish which C++
runtime the binary is linked against. Two `windows_amd64_mingw` extensions can
both pass the platform check on load and then segfault during function
registration if their `std::map` / `std::shared_ptr` / etc. layouts differ —
which they do across **libstdc++** (GNU's STL, what mingw-w64 GCC uses) and
**libc++** (LLVM's STL, what zig's bundled clang uses with `link_libcpp`).

The downstream [sassy](https://github.com/caerbannogwhite/sassy) SAS
interpreter builds DuckDB with zig + `link_libcpp = true`, so its DuckDB
links against libc++. The `make mingw_release` target above produces a
libstdc++-linked binary and is *not* compatible — it'll segfault inside
sassy. Use the zig variant instead:

```bash
make zig_mingw_release
```

This drives the same CMake configuration as `mingw_release` but routes
`CC` / `CXX` through `scripts/zig-shims/zig-cc.cmd` / `zig-cxx.cmd`, which
forward to `zig cc -target x86_64-windows-gnu` and
`zig c++ -target x86_64-windows-gnu`. Output:

```
build/zig_mingw_release/extension/stats_duck/stats_duck.duckdb_extension
build/zig_mingw_release/repository/<duckdb_version>/windows_amd64_mingw/stats_duck.duckdb_extension
```

Same platform stamp (`windows_amd64_mingw`), different C++ runtime. On a
correctly-built artifact, `objdump -p stats_duck.duckdb_extension | grep DLL`
shows only Windows system DLLs (`KERNEL32.dll`, `api-ms-win-crt-*`) — no
`libstdc++-6.dll` or `libgcc_s_seh-1.dll`. If any GNU runtime DLL appears,
the build slipped back to GCC.

**Requirements.** zig 0.16+ on `PATH`. Tested with the `zig.zig`
WinGet-installed copy.

## Testing

```bash
make release && make test
```

Or on Windows where the Makefile test runner may not work:

```bash
./build/release/test/Release/unittest.exe "test/sql/*"
```

## License

The Stats Duck is released under the [Apache License 2.0](LICENSE). If you use
it in academic work, see [`CITATION.cff`](CITATION.cff) for citation metadata.
