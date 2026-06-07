# Changelog

All notable changes to **The Stats Duck** are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

The extension installs and loads in DuckDB under the technical name `stats_duck` —
that name is preserved across releases for backward compatibility.

## [Unreleased]

### Added

- **`lm(table, y := 'col', x := ['c1', 'c2', ...])` and `lm_summary(...)`
  table functions** — OLS linear regression via Cholesky decomposition of
  `X'X`. `lm` returns one row per term (`(Intercept)` followed by each
  predictor in user-supplied order) with columns
  `(term VARCHAR, estimate, std_error, t_statistic, p_value)`. `lm_summary`
  returns a single-row model summary
  `(r_squared, adj_r_squared, f_statistic, f_p_value, df_model BIGINT,
  df_residual BIGINT, sigma, n BIGINT)`. Both share a bind path and a fit
  routine — calling `lm` and `lm_summary` with the same arguments fits the
  model twice; collapse with a CTE if you need both shapes from one fit.
  Complete-case row filtering (any NULL in y or any x drops the row) at
  the SQL level so the OLS solver always sees a complete design matrix.
  Cross-verified against R on the built-in `cars` (simple) and `mtcars`
  (multi-predictor) datasets to ≥4 decimals on coefficients / standard
  errors / R² / F. Singular `X'X` (e.g. perfectly collinear predictors) or
  insufficient data (`n ≤ p + 1`) raises a clear bind error rather than
  silently NaN'ing.

- **Random sampling functions** — completes the d/p/q/**r** quartet for every
  distribution family currently in the extension: `rnorm([mean=0, sd=1])`,
  `rt(df)`, `rchisq(df)`, `rf(df1, df2)`, `rgamma(shape, [rate=1])`,
  `rbeta(alpha, beta)`, `rexp([rate=1])`, `rweibull(shape, [scale=1])`,
  `rlnorm([meanlog=0, sdlog=1])`, `rpois(lambda)`. Per-row inverse-CDF on a
  thread-local `std::mt19937_64` (seeded from `std::random_device` on first
  use). The functions are marked VOLATILE and use explicit per-row loops
  rather than `UnaryExecutor` / `BinaryExecutor` so that constant-vector
  parameter inputs (`rnorm(0, 1)`) don't trip the per-chunk-cache fast path
  that would otherwise produce only ~N/vector_size unique values across an
  N-row scan. Uniform draws use the bit-shift method on the top 53 bits of a
  single mt19937_64 draw (MSVC's `uniform_real_distribution` /
  `generate_canonical` have biased tails on the platform we ship today).

- **`bootstrap(value DOUBLE, statistic VARCHAR, n_iters BIGINT [, seed BIGINT])`
  → LIST<DOUBLE>** — buffer-based aggregate that resamples the input with
  replacement `n_iters` times and emits the chosen summary statistic for
  each resample. `statistic` ∈ `{mean, median, sum, stddev (alias sd),
  variance (alias var), min, max}`. When `seed` is provided the RNG
  (std::mt19937_64) is seeded deterministically — mixed with the per-row
  index so multi-group `GROUP BY` bootstraps produce stable but distinct
  streams. Empty input → NULL list. Composes naturally with `list_quantile`
  for percentile CIs:
  ```
  WITH b AS (SELECT bootstrap(price, 'mean', 1000, 42) AS samples FROM t)
  SELECT list_quantile(samples, 0.025) AS lo,
         list_quantile(samples, 0.975) AS hi
  FROM b;
  ```

- `table_one`: new `effect_size` output column carrying the between-group
  magnitude alongside `p_value`. Numeric variables get **η² (eta-squared)**
  from `anova_oneway` (`ss_between / ss_total`); categorical variables
  get **Cramér's V** computed inline as `√(χ²/(n · (min(rows, cols) - 1)))`
  from `chisq_independence`. Both are in [0, 1] and larger means stronger
  association, so a single uniform column name works across kinds — the
  variable's `statistic` rows tell the consumer which is which. NULL under
  the same conditions as `p_value` (no `by`, single stratum, test
  infeasible). Repeated on every row of a variable so a downstream PIVOT
  can grab it via `FIRST(effect_size)`.

- **Weibull, log-normal, and Poisson distribution functions** — d/p/q
  triples in the same R-style API as the existing distribution families.
  `dweibull(x, shape, [scale])` / `pweibull` / `qweibull` (scale defaults
  to 1; closed-form quantile). `dlnorm(x, [meanlog, [sdlog]])` / `plnorm`
  / `qlnorm` (meanlog defaults to 0, sdlog to 1; reuses NormalCDF /
  NormalQuantile). `dpois(k, lambda)` / `ppois` / `qpois` (discrete; CDF
  uses the regularized upper incomplete gamma identity from Numerical
  Recipes §6.2, quantile is integer search seeded from the normal
  approximation). Non-integer `k` in `dpois` returns 0 (matches R's
  warn-and-zero). Cross-verified against R to 6 decimal places.

- ggsql: per-layer `STAT smooth | summary | identity` modifier — appended
  after a mark name as `DRAW <mark> STAT <name>`. `smooth` injects a
  Vega-Lite loess transform on `(x, y)` and groups by `color` when mapped
  (rejected on marks that already emit their own transform — `regression`,
  `density`, `violin`, `histogram`). `summary` rewrites the layer's data
  SQL to `AVG(y) GROUP BY x [, color, facet, facet2] ORDER BY x` so each
  cell collapses to its mean. `identity` is the explicit no-op. Multi-
  layer composition like `DRAW point DRAW line STAT smooth` produces the
  canonical scatter-with-LOESS overlay without a separate transform
  function.

- ggsql: 2D `FACET BY <row_expr>, <col_expr>` — two comma-separated
  expressions produce a row × column grid via vega-lite's facet operator
  with both `row` and `column` sub-channels. Composes with `SCALE`,
  `TITLE`, multi-layer `DRAW`, and `WITH … VISUALIZE`. The 1D form
  (`FACET BY <expr>` with optional `ROWS` / `COLS` layout) is unchanged.
  `ROWS` / `COLS` is rejected in the 2D form since the layout is fixed.
  The bar mark's GROUP BY extends to both facet columns so per-cell
  ordinal bins compute correctly.

- ggsql: `violin` mark — per-category density rendered as horizontal
  Vega-Lite area marks via the canonical `density` transform + `column`
  facet idiom. Required aesthetics `x` (categorical) and `y` (numeric);
  optional channels (`color`, `opacity`, ...) propagate through the
  encoding. Composes with `FACET BY ... ROWS` (vega `row` channel) but
  conflicts with `FACET BY ... COLS` because both would request the
  `column` channel. Pairs naturally with `boxplot` for distribution
  comparison overlays.

## [0.5.0-dead-person] - 2026-05-31

### Added

- **`corr_matrix(data, variables [, method])` table function** — long-format
  pairwise correlation matrix as `(row_var, col_var, coef, p_value, n)` rows.
  `method` is `'pearson'` (default), `'spearman'`, or `'kendall'`; the `coef`
  column carries the method-appropriate coefficient (r / rho / tau) under one
  uniform name so downstream pipelines compose without method-specific casing.
  All n² rows emitted (full symmetric matrix including diagonal and mirror
  pairs) so the result drops straight into a `DRAW heatmap` without a
  CROSS JOIN. Upper triangle is computed once in C++ and mirrored — for k
  variables that's k·(k+1)/2 aggregate runs instead of k². Pairwise-complete
  NULL handling per the underlying `pearson_test` / `spearman_test` /
  `kendall_test` aggregates. Non-numeric columns are rejected at bind time.

- **ggsql: `WITH … VISUALIZE`** — a leading CTE clause is now accepted in
  front of a `VISUALIZE` statement. The captured `WITH [RECURSIVE] <cte> AS
  (...) [, <cte> AS (...)]*` block is prepended to each layer's projected
  SQL, so the `FROM` clause and aesthetic expressions can reference
  CTE-bound names. Composes with every existing modifier (`FACET BY`,
  `SCALE`, `TITLE`, multi-layer `DRAW`) — wrapping marks like `line` and
  `bar` keep the CTE scoped to the inner subquery via standard SQL CTE
  scoping rules. Leading SQL comments (`--`, `/* … */`) before the `WITH`
  are skipped during keyword detection. Statements that start with `WITH`
  but contain no top-level `VISUALIZE` keyword fall through to DuckDB's
  regular SQL parser unchanged, so existing CTE-using queries are
  unaffected.

- **`bin_edges` / `bin_label` — auto-binning helpers.**
  `bin_edges(x [, method]) → LIST<DOUBLE>` is a buffer-based aggregate that
  returns the edge vector for the chosen rule: `'sturges'` (default,
  `k = ⌈log₂(n)+1⌉`, R's `hist()` default), `'fd'` (Freedman-Diaconis), `'scott'`
  (normal-reference), `'sqrt'`, `'rice'`, or `'auto'` (numpy's `max(sturges, fd)`).
  Methods that depend on robust scale (FD, Scott) silently fall back to
  Sturges when IQR/sd is zero. `bin_label(x, edges) → VARCHAR` formats the
  bin containing `x` as `'[lo, hi)'` (or `'[lo, hi]'` for the rightmost bin so
  the maximum is never dropped). Pairs naturally with `table_one` — bin a
  numeric column up front, then summarise the binned column categorically.

- **Local DuckDB submodule patch (`duckdb/third_party/fmt/include/fmt/format.h`).**
  Upstream fmt checks `#ifdef _SECURE_SCL` to decide whether to use
  `stdext::checked_array_iterator`, but Microsoft's CRT auto-defines
  `_SECURE_SCL=0` in Release builds — and the v145 MSVC toolset (VS2026)
  removed `stdext::checked_array_iterator` entirely, so the `#ifdef` branch
  now fails to compile. Patched to `#if defined(_SECURE_SCL) && _SECURE_SCL > 0`,
  which correctly takes the plain-pointer branch in Release. The patch is
  localized and a candidate for upstream contribution; re-apply if the
  DuckDB submodule moves.

- `poibin_cdf(probs LIST<DOUBLE>, k BIGINT) → DOUBLE` — Poisson Binomial CDF.
  Returns `P(X ≤ k)` where `X = Σᵢ Bᵢ`, each `Bᵢ ∼ Bernoulli(pᵢ)` independent
  with its own success probability. Computed by Hong (2013)'s direct
  convolution: O(n²) time, O(n) memory, numerically stable for the n that
  show up in biostat-scale queries. Reduces exactly to `pbinom(k, n, p)` when
  all probabilities are equal. NULL handling: NULL in either argument or
  inside the list → NULL; any `pᵢ` outside `[0, 1]` → NULL.

- **Gamma / Beta / Exponential distribution functions.** d/p/q triples in the
  same R-style API as the existing `d/p/qnorm` / `d/p/qt` / `d/p/qchisq` /
  `d/p/qf` families. `dgamma(x, shape, [rate])` / `pgamma` / `qgamma` (rate
  defaults to 1, matching R); `dbeta(x, alpha, beta)` / `pbeta` / `qbeta` on
  the [0, 1] support; `dexp(x, [rate])` / `pexp` / `qexp` (rate defaults to 1;
  `qexp` is closed-form `-log(1-p)/rate`). All implemented on top of the
  existing regularized incomplete gamma / beta infrastructure in
  `distributions.hpp`. Cross-verified against R to 6+ decimal places.

- **Kolmogorov-Smirnov tests.** Two new aggregates:

  - `ks_test_1samp(x)` — one-sample KS against the fitted normal
    `N(mean(x), sd(x))`. Returns `STRUCT(test_type, d_statistic, p_value, n)`.
    Buffer-based aggregate; valid for `n >= 3`. P-value via the asymptotic
    Kolmogorov distribution with Stephens (1970) small-sample correction.
    Because mu/sigma are estimated from the sample the p-value is
    **conservative** — pair with `shapiro_wilk` / `anderson_darling` when
    normality is the primary question.
  - `ks_test_2samp(x, y)` — two-sample KS on whether `x` and `y` come from
    the same underlying distribution. Returns
    `STRUCT(test_type, d_statistic, p_value, n_x, n_y)`. Asymptotic p-value
    evaluated at the effective sample size `n_x*n_y/(n_x+n_y)`. Per-column
    NULL handling (NULL in `x` doesn't drop the value from `y` on the same
    row), matching `ttest_2samp` semantics.

- `table_one`: new `p_value` output column carrying the between-group test
  result. NULL when no `by` is set or there's only one stratum. Numeric
  variables use one-way ANOVA via `anova_oneway(value, group)`; categorical
  variables use chi-square independence via `chisq_independence(var, group)`.
  Multi-column `by` folds into a single grouping by concatenating columns
  with `' / '` (the same separator used for stratum labels). The same
  p_value is stamped on every row of a given variable so a PIVOT can grab
  it via `FIRST(p_value)`. Test failures (zero variance, too few samples
  for a 2×2, etc.) → NULL rather than throwing — keeps the whole table
  rendering even when one variable's test can't run.
- `table_one`: `force_categorical := [...]` and `force_numerical := [...]`
  named parameters to override the per-variable auto-classification. Useful
  for integer columns that are really categorical (e.g. `stage ∈ {1,2,3,4}`,
  treatment codes) and VARCHAR columns holding numeric strings. Each entry
  must appear in `variables` (typos surface as bind errors); the two lists
  must not overlap.

### Changed

- `table_one`: `by` named parameter is now `LIST<VARCHAR>` instead of
  `VARCHAR`. Single-column callers need to wrap the column name in a list:
  `by := ['arm']` instead of `by := 'arm'`. Multi-column stratification
  (Cartesian product of distinct value tuples across the listed columns,
  e.g. `by := ['species', 'sex']`) is the motivation — the v0.4 single-
  column form had no place to grow. Stratum labels join values with
  `' / '` in declared order (e.g. `'Adelie / female'`); rows where any
  by-column is NULL are excluded from the breakdown.

### Added

- ggsql: three new marks — `heatmap`, `density`, `regression`. `heatmap` is a
  `rect` mark with ordinal x/y and quantitative color (correlation matrices,
  contingency tables). `density` is a KDE via Vega-Lite's `density` transform
  on the `x` aesthetic; groups by `color` if mapped (one curve per level).
  `regression` is a `line` mark via Vega-Lite's `regression` transform fitting
  `y ~ x`; groups by `color` if mapped. Pairs naturally with `DRAW point
  DRAW regression` for scatter-with-fit overlays.
- ggsql: `TITLE '<text>' [SUBTITLE '<text>']` clause appended after any
  `SCALE` clauses. Emitted as a Vega-Lite `TitleParams` object (always object
  form, even without subtitle) so consumers don't need to handle both string
  and object shapes. `SUBTITLE` without a preceding `TITLE` is a parse error.
- ggsql: `SCALE <channel> LABEL '<text>'` operator injects an `axis.title`
  block alongside the channel's existing `scale` block. Composes with
  `TO` / `ZERO` / `DOMAIN` on the same channel and with `FACET BY` and
  multi-layer specs. LABEL on an unmapped channel is a silent no-op, matching
  the existing `SCALE` semantics.

## [0.4.0-nine-pence] - 2026-05-19

Two themes for v0.4: rounding out the normality-test family that v0.3
left at jarque_bera only, and shipping the first table-function helper
oriented at clinical-trial reporting workflows. Plus a multiple-testing
correction primitive that the rest of the test family was missing.

### Added

- **Normality tests.** Two new aggregates complete the family alongside
  the existing `jarque_bera`:

  - `anderson_darling(x)` — Anderson-Darling normality test against the
    fitted normal (mean and variance estimated from the sample, "case 3").
    Buffer-based aggregate: state holds the values; sort + scan happen in
    Finalize. Statistic is the standard A² with Stephens' size adjustment
    `(1 + 0.75/n + 2.25/n²)`, p-value via Stephens (1986)'s four-segment
    polynomial approximation. Valid for `n >= 8` (returns NULL otherwise,
    matching R's `nortest::ad.test`). Returns
    `STRUCT(test_type, a_squared, a_squared_adjusted, p_value, n)`.

  - `shapiro_wilk(x)` — Shapiro-Wilk normality test via Royston (1995)'s
    AS R94 polynomial approximation. Valid for n in [3, 5000]. The same
    algorithm shipped by scipy.stats.shapiro and R's shapiro.test, with
    no exact coefficient table to keep around — modern best practice for
    any n. W and p-values agree with R to 7+ decimal places on R's
    reference cases (W = 0.96038, p = 0.5514 for `shapiro.test(1:20)`,
    exact match). Special-cased for n = 3 (exact arcsine formula) and
    the small-n branch n in [4, 11] (log-of-log transformation per AS R94).
    Returns `STRUCT(test_type, w_statistic, p_value, n)`.

- **Multiple-testing correction.** `adjust_p(pvals LIST<DOUBLE>, method
  VARCHAR) → LIST<DOUBLE>` — methods match R's `p.adjust`: `'bonferroni'`,
  `'holm'`, `'hochberg'`, `'BH'` (alias `'fdr'`), `'BY'`, `'none'`. Returns
  adjusted p-values in input order; NULLs pass through and are excluded
  from `n`. R-verified to all displayed digits across all five methods on
  a standard reference set. Pairs with any of the per-row tests via
  `LIST(p_value)` over a `GROUP BY` analysis.

- **Table 1 helper.** `table_one(data, variables [, by])` — Table-1-style
  descriptives summary as a DuckDB table function. Auto-classifies each
  variable from its catalog column type (integer / floating-point →
  numeric, everything else → categorical) and emits long-format rows
  `(variable, level, statistic, stratum, display)` with `level` = NULL
  for numeric variables. Numeric stats: `n`, `missing`, `mean (sd)`,
  `median [q1, q3]`, `min, max`. Categorical: per-level `n (%)` followed
  by a `Missing` level row that is always emitted (even when its count
  is zero) so downstream PIVOTs see a stable shape — filter with `WHERE
  level <> 'Missing'` if you don't want it. All categorical-level
  percentages share the same denominator (stratum total including
  missings) so they sum to 100%. With `by`, emits an `Overall` stratum
  plus one stratum per distinct `by` value. v0.4 MVP — between-group
  p-value column, `force_categorical` / `force_numerical` overrides, and
  the `overall := false` toggle land in a follow-up.

## [0.3.2-here-s-one] - 2026-05-13

### Fixed

- `scripts/zig-shims/zig-cxx.py`: switched `shlex.split` from non-POSIX
  to POSIX mode when expanding CMake response files. The previous
  non-POSIX variant preserved each token's surrounding double quotes
  literally, so zig clang received `-I"C:/path"` with the quote
  characters embedded in the path and failed to find every header in
  `src/include` (and similar). POSIX mode strips the quotes. CMake's
  MinGW Makefiles generator emits forward slashes inside the response
  files, so the POSIX backslash-as-escape rule doesn't bite. `make
zig_mingw_release` now completes from a clean state.

## [0.3.1-customer] - 2026-05-13

### Fixed

- CI: excluded `windows_amd64` from the build / deploy matrix to work
  around [duckdb/extension-ci-tools#371][371]. The hardcoded
  `vcvars64.bat` path in v1.5.1 of the reusable workflow no longer
  resolves on the current `windows-latest` image, so cmake silently
  fell back to mingw and the link failed against the MSVC-built
  `x64-windows-static-release` vcpkg libs. Identical code to v0.3.0
  otherwise. Windows users without the MSVC artifact should use the
  `windows_amd64_mingw` build (which uses mingw natively and is
  unaffected). Revisit once upstream ships the fix.

[371]: https://github.com/duckdb/extension-ci-tools/issues/371

## [0.3.0-customer] - 2026-05-13

Focused on closing the gap with SAS PROC UNIVARIATE / PROC MEANS /
PROC FREQ / PROC CORR while keeping modern statistical defaults. Every
new optional parameter ships with a `false` / population / no-correction
default; existing callers see no behavioural change.

### Added

- **Rank correlations.** `spearman_test(x, y, [alpha], [alternative])`
  and `kendall_test(x, y, [alternative])` complete the correlation family
  started in v0.1. Spearman: midrank tie correction, Pearson-on-ranks
  Student's t (df = n-2), Fisher-z CI on rho. Kendall: tau-b only, O(n²)
  pair counting with Knight's tie-corrected variance and a normal-
  approximation z. Result-struct shape mirrors `pearson_test`. SAS-
  verified against PROC CORR.

- **Sign tests.** `sign_test_1samp(x, [mu], [alternative])` and
  `sign_test_paired(x, y, [alternative])` complete PROC UNIVARIATE's
  "Tests for Location" trio (alongside Student's t and Wilcoxon
  signed-rank). M = (n_pos - n_neg) / 2 matches SAS exactly (M = -4.5,
  p = 0.0039 on R's sleep dataset). Binomial p-value via the
  regularized incomplete beta identity.

- **`summary_stats` gets three new fields and two options.**
  - `mode` (smallest modal value), `mode_frequency`, `is_multimodal`
    fields. For all-distinct input → `NaN / 0 / false`, matching SAS
    PROC UNIVARIATE's "Mode ." convention.
  - `bias_correction` (BOOLEAN, default `true`) toggles between
    bias-corrected sample formulas for skewness/kurtosis (SAS / pandas
    / Excel) and population formulas `m3/m2^1.5` / `m4/m2² - 3` (R /
    scipy default). Mean, SD, variance, quantiles, IQR unaffected.
  - `quantile_type` (INTEGER, default `7`) picks the Hyndman & Fan
    quantile algorithm. Type 7 (R / Excel INC) is the default; type 5
    matches SAS PROC UNIVARIATE — `Q1 = 1.5` / `Q3 = 3.5` on `[1,2,3,4]`
    vs Type 7's `1.75` / `3.25`.

- **Continuity-correction toggles** on three nonparametric tests, all
  defaulting to `false` (matching scipy / R `correct=FALSE`):
  - `mann_whitney_u(x, y, [alternative], [continuity])` — set `true`
    for SAS PROC NPAR1WAY (Z = -2.5067, p = 0.0122 on `[1..5]` vs
    `[6..10]`).
  - `wilcoxon_signed_rank(x, y, [alternative], [continuity])` — set
    `true` for SAS PROC UNIVARIATE's asymptotic signed-rank Z.
  - `chisq_independence(row, col, [continuity])` — set `true` for
    Yates' (2x2 only; silently dropped on larger tables, matching
    `chisq.test(correct=TRUE)`). SAS-verified: χ² = 9.9556 on the
    (30, 10, 15, 25) table.

- **SPSS Portable (POR) export**: `COPY tbl TO 'file.por'`. Same type
  and NULL semantics as the SAV writer, with XPT-style strict
  column-name rules (≤ 8 chars uppercase, A-Z 0-9 \_) and no compression
  (POR is plain ASCII). The reader has supported `.por` since v0.1.0.

- **XPT version-8 export**: `COPY tbl TO 'file.xpt' (VERSION 8)` lifts
  the v5 column-name limit (8 chars) to 32 chars and raises the dataset
  name limit similarly. Default remains v5 for backward compatibility.
  v8 files round-trip through `read_stat()`, pyreadstat, haven, and R;
  some legacy SAS toolchains still expect v5.

- **Column-label propagation on export**: `COPY tbl TO
'file.{xpt,sas7bdat,sav,por}'` picks up DuckDB column comments
  (`COMMENT ON COLUMN tbl.col IS '...'`) and writes them as ReadStat
  variable labels — visible in SAS, SPSS, pyreadstat, haven, and R as
  the column's descriptive label. Only applies when the COPY source is
  a named base table; `COPY (SELECT …) TO …` writes empty labels. XPT
  v8 stores labels longer than 40 chars in the `LABELV8` long-label
  subrecord.

- **`read_stat_metadata(path, [format], [encoding])`** — table function
  returning one row per variable with `(column_name, type, format,
label)`, without scanning the data. Useful for inspecting SAS / SPSS
  / Stata files before importing, and for verifying that column
  comments propagated through to ReadStat variable labels on export.

- **Local mingw build targets**: `make mingw_release` produces a
  `windows_amd64_mingw`-stamped extension for loading into mingw-built
  DuckDB hosts. `make zig_mingw_release` does the same with zig's
  bundled clang + libc++ for libc++-linked DuckDB hosts (e.g. the
  zig-bundled DuckDB inside `sassy` with `link_libcpp = true`). See the
  README's "Building with MinGW" / "Building with zig" sections.

- **SAS compatibility section** in the README — single-table reference
  for every per-call toggle and how to set it to reproduce SAS PROC
  output exactly.

- **Test suite expanded from 32 to 39 cases / 848 to 1117 assertions.**
  New `.test` files for `anova_oneway`, `chisq_independence`,
  `chisq_goodness_of_fit`, `jarque_bera`, `summary_stats`,
  `distribution_functions`, `sign_test`, plus the rank-correlation
  tests. Reference values are R 4.5.3-verified via an out-of-tree
  script. Backfills `pearson_test.test`, which was previously missing.

### Fixed

- `pearson_test` and `spearman_test` no longer return `p_value = 0`
  when `|r| = 1` produces an infinite t-statistic. The hard-coded
  short-circuit was wrong for one-sided alternatives (`r = 1` with
  `alternative = 'less'` must give `p = 1`, not `0`). `StudentTCDF`
  handles `±inf` correctly; the short-circuit is removed.

- Readstat's `iconv` cast in `readstat_convert.c` mismatched
  `win_iconv`'s `WINICONV_CONST char **` by one `const`, which mingw
  GCC ≥ 15 (the toolchain on `windows-latest` after early-May 2026)
  rejects as an error. CMakeLists now defines `ICONV_CONST=const` on
  Windows so the typedef chain lines up.

## [0.2.0-bring-out-your-dead] - 2026-05-03

### Added

- **ggsql — Grammar of Graphics for SQL.** New `VISUALIZE … FROM <table> DRAW <mark>`
  parser extension that compiles at plan time into a Vega-Lite v5 spec plus one SQL
  string per layer, returned as `(spec VARCHAR, layer_sqls MAP(VARCHAR, VARCHAR))`.
  Clients (e.g. Bedevere Wise in DuckDB-WASM) execute each layer's SQL and feed Arrow
  rows into vega-embed via the `datasets` API — no server-side rendering. Supports
  11 marks (`point`, `line`, `bar`, `histogram`, `text`, `area`, `rule`, `tick`,
  `errorbar`, `errorband`, `boxplot`), 12 aesthetic channels, multi-layer overlays
  (`DRAW point DRAW line`), `FACET BY <expr> [ROWS|COLS]`, `SCALE` clauses
  (`TO <scheme>` for color schemes, `ZERO true|false`, `DOMAIN <lo> <hi>`),
  type-annotated aesthetics (`year AS color:ordinal`), and SQL expressions in
  mappings (`bill_len * 2 AS x`, `coalesce(a, b) AS x`). Marks are registered
  through a catalog-mediated registry (`ggsql_mark_v1_<name>` scalar functions),
  so other extensions can ship custom marks without modifying stats_duck.
- `COPY tbl TO 'file.xpt'` — write SAS Transport (XPT v5) files via ReadStat.
  Supports BOOLEAN, all integer/float/decimal types, VARCHAR, DATE, TIMESTAMP, and
  TIME columns. Numeric NULLs round-trip as SAS system-missing; VARCHAR NULLs
  collapse to empty strings (SAS character columns have no NULL/empty distinction).
  Output goes through DuckDB's `FileSystem` so registered/remote/WASM paths work
  the same as `read_stat()`. Optional `LABEL` and `TABLE_NAME` options. XPT v5
  column-name rules (≤ 8 chars uppercase, A-Z 0-9 \_) are validated at bind — no
  silent truncation. Buffers the full result set in a `ColumnDataCollection`
  (spillable via DuckDB's buffer manager) before emitting, since ReadStat's writer
  requires the row count up front.
- `COPY tbl TO 'file.sas7bdat'` — write SAS7BDAT files via ReadStat. Same type and
  NULL semantics as XPT, but allows ≤ 32-char mixed-case column names. Optional
  `LABEL` and `COMPRESSION` (`'none'` / `'rows'`) options. **Caveat:** ReadStat's
  SAS7BDAT writer is reverse-engineered, and the produced files round-trip through
  ReadStat-family readers (this extension's `read_stat()`, pyreadstat, haven, R)
  but **are not opened by real SAS / SAS Universal Viewer / SAS OnDemand**. This
  is a long-standing upstream limitation. Use XPT if you need SAS-native readability.
- `COPY tbl TO 'file.sav'` — write SPSS SAV files via ReadStat. Same scaffolding
  as the SAS exports; uses SPSS-native epoch (seconds since 1582-10-14) and SPSS
  format strings (`DATE11`, `DATETIME20`, `TIME8`) for temporal columns. Optional
  `LABEL` and `COMPRESSION` (`'none'` / `'rows'`) options.

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

[0.3.2-here-s-one]: https://github.com/caerbannogwhite/the-stats-duck/releases/tag/v0.3.2
[0.3.1-customer]: https://github.com/caerbannogwhite/the-stats-duck/releases/tag/v0.3.1
[0.3.0-customer]: https://github.com/caerbannogwhite/the-stats-duck/releases/tag/v0.3.0
[0.2.0-bring-out-your-dead]: https://github.com/caerbannogwhite/the-stats-duck/releases/tag/v0.2.0
[0.1.0-mortician]: https://github.com/caerbannogwhite/the-stats-duck/releases/tag/v0.1.0
