# The Stats Duck

A statistical computing toolkit for DuckDB.

The Stats Duck brings statistical workflows — descriptive statistics, hypothesis
tests, and direct readers for SAS, SPSS, and Stata files — into SQL. Functions
are implemented as streaming aggregates and scalar primitives, so they scale
from local notebooks to billion-row warehouses and also run inside DuckDB-WASM
in the browser.

> The extension installs and loads in DuckDB under the technical name
> `stats_duck` (matching the binary, the SQL function namespace, and the
> `INSTALL` keyword). "The Stats Duck" is the project / brand name; `stats_duck`
> is what you type at the SQL prompt.

## Scope

The Stats Duck is meant to cover the everyday work of a general-purpose
statistician without leaving SQL. The current release focuses on three areas:

- **Hypothesis tests** — parametric and non-parametric, with effect sizes and
  confidence intervals returned alongside the test statistic.
- **Statistical file I/O** — first-class readers for SAS, SPSS, and Stata
  files, integrated with DuckDB's virtual file system so they work transparently
  with local paths, `httpfs://`, `s3://`, and registered WASM file buffers.
- **Streaming aggregates** — every test is a single-pass aggregate over the
  data, so it composes naturally with `GROUP BY`, window frames, and DuckDB's
  parallel execution.

Future releases will broaden the toolkit with descriptive statistics helpers,
distribution functions (PDF/CDF/quantile), correlation tests, ANOVA, normality
tests, multiple-testing corrections, and regression with full diagnostics. See
the project roadmap on GitHub for the current plan.

## Functions

| Function                                                             | Category        | Description                              |
| -------------------------------------------------------------------- | --------------- | ---------------------------------------- |
| `ttest_1samp(column, [mu], [alpha], [alternative])`                  | Hypothesis test | One-sample t-test                        |
| `ttest_2samp(column1, column2, [equal_var], [alpha], [alternative])` | Hypothesis test | Two-sample t-test (Welch's or Student's) |
| `ttest_paired(column1, column2, [alpha], [alternative])`             | Hypothesis test | Paired t-test                            |
| `mann_whitney_u(column1, column2, [alternative])`                    | Hypothesis test | Mann-Whitney U test (Wilcoxon rank-sum)  |
| `wilcoxon_signed_rank(column1, column2, [alternative])`              | Hypothesis test | Wilcoxon signed-rank test                |
| `read_stat(path, [format], [encoding])`                              | Data import     | Read SAS / SPSS / Stata files            |

The hypothesis tests are **aggregate functions** that return a `STRUCT`. The
data import function is a **table function**.

**t-test result fields:** `test_type`, `t_statistic`, `df`, `p_value`, `alternative`, `mean_diff`, `ci_lower`, `ci_upper`, `cohens_d`

**Mann-Whitney result fields:** `test_type`, `u_statistic`, `z_statistic`, `p_value`, `alternative`, `rank_biserial`

**Wilcoxon result fields:** `test_type`, `w_statistic`, `z_statistic`, `p_value`, `alternative`, `effect_size_r`

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

## Building

```bash
git submodule update --init --recursive
make release
```

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
