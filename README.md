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

| Function                                                              | Description                                  |
| --------------------------------------------------------------------- | -------------------------------------------- |
| `ttest_1samp(column, [mu], [alpha], [alternative])`                   | One-sample t-test                            |
| `ttest_2samp(column1, column2, [equal_var], [alpha], [alternative])`  | Two-sample t-test (Welch's or Student's)     |
| `ttest_paired(column1, column2, [alpha], [alternative])`              | Paired t-test                                |
| `mann_whitney_u(column1, column2, [alternative], [continuity])`       | Mann-Whitney U test (Wilcoxon rank-sum)      |
| `wilcoxon_signed_rank(column1, column2, [alternative], [continuity])` | Wilcoxon signed-rank test                    |
| `pearson_test(x, y, [alpha], [alternative])`                          | Pearson correlation with significance        |
| `spearman_test(x, y, [alpha], [alternative])`                         | Spearman rank correlation                    |
| `kendall_test(x, y, [alternative])`                                   | Kendall's tau-b rank correlation             |
| `anova_oneway(value, group)`                                          | One-way ANOVA                                |
| `chisq_independence(row, col, [continuity])`                          | Chi-square test of independence              |
| `chisq_goodness_of_fit(category)`                                     | Chi-square goodness-of-fit (uniform)         |
| `jarque_bera(column)`                                                 | Jarque-Bera normality test                   |
| `shapiro_wilk(column)`                                                | Shapiro-Wilk normality test (Royston AS R94) |
| `anderson_darling(column)`                                            | Anderson-Darling normality test              |
| `ks_test_1samp(column)`                                               | Kolmogorov-Smirnov one-sample (vs fitted normal) |
| `ks_test_2samp(column1, column2)`                                     | Kolmogorov-Smirnov two-sample                |
| `sign_test_1samp(column, [mu], [alternative])`                        | Sign test on the median                      |
| `sign_test_paired(column1, column2, [alternative])`                   | Paired sign test                             |

All tests return a `STRUCT` with the test statistic, degrees of freedom,
p-value, and relevant effect sizes / confidence intervals.

#### Common parameters

| Parameter     | Type      | Default       | Description                                                      |
| ------------- | --------- | ------------- | ---------------------------------------------------------------- |
| `mu`          | `DOUBLE`  | `0.0`         | Hypothesized population mean (one-sample t-test only)            |
| `equal_var`   | `BOOLEAN` | `false`       | Assume equal variances — Student's pooled test (two-sample only) |
| `alpha`       | `DOUBLE`  | `0.05`        | Significance level for confidence intervals (t-tests only)       |
| `alternative` | `VARCHAR` | `'two-sided'` | `'two-sided'`, `'less'`, or `'greater'`                          |
| `continuity`  | `BOOLEAN` | `false`       | Apply continuity correction (mann_whitney_u, chisq_independence) |

#### Result struct fields

**t-test:** `test_type`, `t_statistic`, `df`, `p_value`, `alternative`, `mean_diff`, `ci_lower`, `ci_upper`, `cohens_d`

**Mann-Whitney:** `test_type`, `u_statistic`, `z_statistic`, `p_value`, `alternative`, `rank_biserial`

**Wilcoxon:** `test_type`, `w_statistic`, `z_statistic`, `p_value`, `alternative`, `effect_size_r`

**Pearson:** `test_type`, `r`, `t_statistic`, `df`, `p_value`, `alternative`, `ci_lower`, `ci_upper`, `n`

**Spearman:** `test_type`, `rho`, `t_statistic`, `df`, `p_value`, `alternative`, `ci_lower`, `ci_upper`, `n`

**Kendall:** `test_type`, `tau`, `z_statistic`, `p_value`, `alternative`, `n`

**ANOVA:** `test_type`, `f_statistic`, `df_between`, `df_within`, `p_value`, `ss_between`, `ss_within`, `eta_squared`, `n_groups`, `n`

**Chi-square:** `test_type`, `chi_square`, `df`, `p_value`, `n`, `n_rows`/`n_cols` or `n_categories`

**Jarque-Bera:** `test_type`, `jb_statistic`, `skewness`, `excess_kurtosis`, `df`, `p_value`, `n`

**Anderson-Darling:** `test_type`, `a_squared`, `a_squared_adjusted`, `p_value`, `n`

**Shapiro-Wilk:** `test_type`, `w_statistic`, `p_value`, `n`

**KS one-sample:** `test_type`, `d_statistic`, `p_value`, `n`

**KS two-sample:** `test_type`, `d_statistic`, `p_value`, `n_x`, `n_y`

**Sign test:** `test_type`, `m_statistic`, `n_pos`, `n_neg`, `n_zero`, `p_value`, `alternative`, `n`

### Descriptive statistics (aggregate)

| Function                                                    | Description                                                                                                              |
| ----------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------ |
| `summary_stats(column, [bias_correction], [quantile_type])` | n, n_missing, mean, sd, variance, min, q1, median, q3, max, iqr, skewness, kurtosis, mode, mode_frequency, is_multimodal |

`bias_correction` (BOOLEAN, default `true`) toggles the skewness/kurtosis
formulas. With `true` the output matches SAS PROC MEANS, scipy with
`bias=False`, and Excel SKEW/KURT. With `false` the population formulas
`m3/m2^1.5` and `m4/m2² - 3` are used, matching R's default. Mean, SD,
variance, quantiles, and IQR are unaffected.

`mode` is the smallest modal value (when more than one value shares the
maximum frequency, the smallest of them is returned and `is_multimodal`
is set to `true`). For all-distinct input — every value appears exactly
once — `mode` is `NaN` and `mode_frequency` is `0`, matching SAS PROC
UNIVARIATE's "Mode ." output.

`quantile_type` (INTEGER, default `7`) picks the Hyndman & Fan (1996)
quantile algorithm used for `q1`, `median`, and `q3`. Supported values:

- `7` — R / Excel INC default. `position = 1 + q * (n - 1)`.
- `5` — SAS PROC UNIVARIATE default. `position = q * n + 0.5`.

For `x = [1, 2, 3, 4]`: type 7 gives Q1=1.75 / Q3=3.25 (matching R); type 5
gives Q1=1.5 / Q3=3.5 (matching SAS PROC MEANS).

### Distribution functions (scalar)

| Function                 | Description             |
| ------------------------ | ----------------------- |
| `dnorm(x, [mean], [sd])` | Normal PDF              |
| `pnorm(x, [mean], [sd])` | Normal CDF              |
| `qnorm(p, [mean], [sd])` | Normal quantile         |
| `dt(x, df)`              | Student's t PDF         |
| `pt(x, df)`              | Student's t CDF         |
| `qt(p, df)`              | Student's t quantile    |
| `dchisq(x, df)`          | Chi-square PDF          |
| `pchisq(x, df)`          | Chi-square CDF          |
| `qchisq(p, df)`          | Chi-square quantile     |
| `df(x, df1, df2)`        | F distribution PDF      |
| `pf(x, df1, df2)`        | F distribution CDF      |
| `qf(p, df1, df2)`        | F distribution quantile |
| `dgamma(x, shape, [rate])` | Gamma PDF (rate=1 default) |
| `pgamma(x, shape, [rate])` | Gamma CDF             |
| `qgamma(p, shape, [rate])` | Gamma quantile        |
| `dbeta(x, alpha, beta)`  | Beta PDF on [0, 1]      |
| `pbeta(x, alpha, beta)`  | Beta CDF                |
| `qbeta(p, alpha, beta)`  | Beta quantile           |
| `dexp(x, [rate])`        | Exponential PDF (rate=1 default) |
| `pexp(x, [rate])`        | Exponential CDF         |
| `qexp(p, [rate])`        | Exponential quantile (closed form) |
| `poibin_cdf(probs LIST<DOUBLE>, k BIGINT)` | Poisson Binomial CDF — `P(X ≤ k)` for `X = Σᵢ Bᵢ`, `Bᵢ ∼ Bernoulli(pᵢ)` |
| `bin_edges(x [, method])` *(aggregate)* | Auto bin-edge vector for `x` — `sturges` (default), `fd`, `scott`, `sqrt`, `rice`, `auto` |
| `bin_label(x, edges)` | Label for the bin containing `x` given an edge vector (typically from `bin_edges`) |

### Table 1 summary (table function)

| Function                                          | Description                                                |
| ------------------------------------------------- | ---------------------------------------------------------- |
| `table_one(data, variables [, by])`               | Long-format descriptives table for mixed variable types    |
| `corr_matrix(data, variables [, method])`         | Long-format pairwise correlation matrix (`pearson` / `spearman` / `kendall`) |

```sql
SELECT * FROM table_one(
    'patients',
    variables := ['age', 'sex', 'bmi'],
    by := ['arm']         -- optional; pass multiple columns for cross-stratification
);
```

Output columns (long format, fixed schema):
`variable`, `level`, `statistic`, `stratum`, `display`, `p_value`

- Each numeric variable yields rows for `n`, `missing`, `mean (sd)`,
  `median [q1, q3]`, `min, max` — `level` is NULL.
- Each categorical variable yields one row per level with `n (%)` plus a
  trailing `Missing` level row that is always emitted (filter with
  `WHERE level <> 'Missing'` if you don't want it). All level percentages
  share the stratum-total denominator so they sum to 100%.
- `stratum` is `'Overall'` when `by` is unset / empty; otherwise the Cartesian
  product of distinct value tuples across the listed by-columns, labelled
  by joining values with `' / '` in declared order (e.g. `'Adelie / female'`
  for `by := ['species', 'sex']`). Rows where any by-column is NULL are
  excluded from the stratum breakdown.
- `p_value` is the between-group test result, repeated on every row of the
  same variable so a PIVOT can grab it with `FIRST(p_value)`. NULL when
  `by` is unset or has only one stratum. Numeric variables use one-way
  ANOVA (`anova_oneway`); categorical variables use chi-square independence
  (`chisq_independence`). NULL when the underlying test is infeasible (zero
  variance, too few samples).
- Variable types are auto-classified from the catalog: integer / floating-
  point types are numeric, everything else (VARCHAR, BOOLEAN, ENUM,
  date/time) is categorical. Override per-variable with
  `force_categorical := ['stage']` (integer column that's really a
  category) or `force_numerical := ['height']` (VARCHAR column holding
  numeric strings). Entries must appear in `variables`, and the two lists
  must not overlap.

Pivot to wide for display:

```sql
PIVOT table_one('patients', variables := ['age', 'sex'], by := ['arm'])
    ON stratum USING first(display)
    GROUP BY variable, level, statistic;
```

### Multiple-testing correction (scalar)

| Function                                | Description                                                                |
| --------------------------------------- | -------------------------------------------------------------------------- |
| `adjust_p(pvals, method)`               | Apply a multiple-testing correction to a list of p-values                  |

`adjust_p` takes a `LIST<DOUBLE>` of raw p-values and a method name, and
returns adjusted p-values in input order. Methods (case-sensitive, matching
R's `p.adjust`):

- `'bonferroni'` — `min(1, n · p_i)`.
- `'holm'` — Holm step-down (1979).
- `'hochberg'` — Hochberg step-up (1988).
- `'BH'` (alias `'fdr'`) — Benjamini-Hochberg FDR (1995).
- `'BY'` — Benjamini-Yekutieli FDR (2001) for arbitrary dependence.
- `'none'` — pass-through, returns the input unchanged.

NULLs in the input list are passed through to the output at the same
position and are excluded from `n`.

### Data import (table function)

| Function                                | Description                   |
| --------------------------------------- | ----------------------------- |
| `read_stat(path, [format], [encoding])` | Read SAS / SPSS / Stata files |

### Data export (COPY function)

| Statement                         | Description                       |
| --------------------------------- | --------------------------------- |
| `COPY <table> TO 'file.xpt'`      | Write SAS Transport (XPT v5)      |
| `COPY <table> TO 'file.sas7bdat'` | Write SAS7BDAT (see caveat below) |
| `COPY <table> TO 'file.sav'`      | Write SPSS SAV                    |

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
[WITH [RECURSIVE] <cte> AS (...) [, <cte> AS (...)]*]
VISUALIZE <expr> AS <aesthetic> [: <type>] (, <expr> AS <aesthetic> ...)
FROM <table>
DRAW <mark> (DRAW <mark>)*
[FACET BY <expr> [ROWS | COLS]]
[SCALE <channel> {TO <scheme> | ZERO true|false | DOMAIN <lo> <hi> | LABEL '<text>'}]*
[TITLE '<text>' [SUBTITLE '<text>']]
```

A leading `WITH` clause is supported; CTEs are scoped to each layer's
projected SQL so they compose with wrapping marks (`line`, `bar`, `area`,
`errorband`, `regression`) without extra work. `WITH … SELECT …` statements
without a top-level `VISUALIZE` keyword fall through to DuckDB's normal SQL
parser unchanged.

**Marks:** `point`, `line`, `bar`, `histogram`, `text`, `area`, `rule`, `tick`,
`errorbar`, `errorband`, `boxplot`, `heatmap`, `density`, `regression`. Custom
marks register as `ggsql_mark_v1_<name>` scalar functions and are discovered
via DuckDB's catalog, so other extensions can ship their own marks without
modifying stats_duck.

`heatmap` is a `rect` mark with ordinal x/y and quantitative color (correlation
matrices, contingency tables). `density` runs Vega-Lite's KDE on the `x`
aesthetic, grouped by `color` if mapped (one curve per category). `regression`
fits a linear `y ~ x` model server-side via Vega-Lite's regression transform,
also grouped by `color`. Use `DRAW point DRAW regression` for a scatter-with-
fit overlay.

**Aesthetic channels:** `x`, `y`, `color`, `fill`, `stroke`, `shape`, `size`,
`opacity`, `tooltip`, `text`, `x2`, `y2`. Unknown channels are silently dropped.

**Type overrides:** append `:quantitative`, `:ordinal`, `:nominal`, or
`:temporal` to an aesthetic to force its Vega-Lite type
(e.g. `year AS color:ordinal`).

**Axis labels:** `SCALE x LABEL 'Bill length (mm)'` injects an `axis.title` into
the channel; pairs with `TO` / `ZERO` / `DOMAIN` on the same channel.

**Titles:** `TITLE 'Plot title' [SUBTITLE 'Plot subtitle']` appears once per
spec, after `SCALE` clauses. Always emitted as a Vega-Lite `TitleParams` object
so a subtitle can be added without reshaping consumer code.

## SAS compatibility

stats_duck defaults follow modern statistical conventions (scipy /
R with `correct=FALSE` / `var.equal=FALSE`). The one exception is
`summary_stats` — its skewness/kurtosis formulas are the Fisher-Pearson
bias-corrected ones, which is what SAS PROC MEANS, pandas, and Excel
report. To reproduce SAS PROC output exactly, use the toggles below.

| SAS procedure / statistic                      | stats_duck call                                                     |
| ---------------------------------------------- | ------------------------------------------------------------------- |
| PROC MEANS — mean, SD, skewness, kurtosis      | `summary_stats(x)` _(default already matches SAS)_                  |
| PROC TTEST — Pooled (equal variances)          | `ttest_2samp(x, y, true)`                                           |
| PROC TTEST — Satterthwaite (default)           | `ttest_2samp(x, y)` _(default Welch's matches Satterthwaite)_       |
| PROC NPAR1WAY — Wilcoxon two-sample Z          | `mann_whitney_u(x, y, 'two-sided', true)` _(continuity correction)_ |
| PROC FREQ — Continuity Adj. χ² (2x2 only)      | `chisq_independence(row, col, true)` _(Yates' correction)_          |
| PROC FREQ — Chi-Square (no adjustment)         | `chisq_independence(row, col)` _(default)_                          |
| PROC CORR — Pearson / Spearman / Kendall       | `pearson_test(x, y)` / `spearman_test(x, y)` / `kendall_test(x, y)` |
| PROC GLM — one-way ANOVA F-test                | `anova_oneway(value, group)`                                        |
| PROC UNIVARIATE — Signed Rank (sign-rank test) | `wilcoxon_signed_rank(x, 0)` _(against a 0 column or constant)_     |
| PROC UNIVARIATE — Sign test (M statistic)      | `sign_test_1samp(x, [mu_0])`                                        |
| PROC UNIVARIATE — quantiles (Type 5)           | `summary_stats(x, true, 5)`                                         |

Defaults preserve modern conventions so users on the modern side of the
fence get sensible numbers without touching the API; SAS users add the
appropriate flag during migration and validation.

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

DuckDB extensions are tagged at build time with a _platform string_ that the
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
libstdc++-linked binary and is _not_ compatible — it'll segfault inside
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
