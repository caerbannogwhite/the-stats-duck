# Two RNG footguns we hit shipping the `r*` random-sampling functions

*June 2026, while landing `rnorm` / `rt` / `rchisq` / `rf` / `rgamma` / `rbeta` / `rexp` / `rweibull` / `rlnorm` / `rpois` for v0.6.*

This is the kind of bug that produces plausible-looking output for a long time
before someone runs a careful Monte Carlo and notices the numbers don't quite
match theory. We hit two such bugs back-to-back. Both are worth writing down
because the diagnostic path was a small adventure and the fixes are non-obvious.

## TL;DR

1. **DuckDB's `UnaryExecutor` / `BinaryExecutor` short-circuit constant-vector
   inputs by invoking the user lambda once per chunk and broadcasting the
   result.** That is correct for pure functions and catastrophic for sampling
   functions. Marking the function `VOLATILE` stops constant-folding at the
   planner level but does not disable the executor-level fast path. Fix: drop
   the helpers and run an explicit per-row loop via `UnifiedVectorFormat`.
2. **MSVC's `std::uniform_real_distribution<double>` and
   `std::generate_canonical<double, 53>` have biased tails in this toolchain.**
   They produce uniform values whose CDF is visibly off from `U ~ Uniform(0, 1)`
   when you feed them through an inverse-CDF transform with a heavy right tail
   (exponential, gamma). Fix: take the top 53 bits of a single `mt19937_64`
   draw and scale by `2^-53` manually.

## What we shipped, then what we noticed

The naive first draft of `rexp(rate)` looked like every other distribution
scalar we'd written:

```cpp
static void RExp1Exec(DataChunk &args, ExpressionState &, Vector &result) {
    UnaryExecutor::ExecuteWithNulls<double, double>(
        args.data[0], result, args.size(),
        [](double rate, ValidityMask &mask, idx_t idx) {
            return SafeSample(
                [&](double u) { return stats_duck::ExponentialQuantile(u, rate); },
                mask, idx);
        });
}
```

We then ran a smoke test:

```sql
SELECT round(avg(rexp(1.0)), 2) AS mean,
       round(max(rexp(1.0)), 1) AS mx
FROM range(500000);
```

The exponential distribution with rate λ=1 has mean 1.0 and, for a sample of
500k iid draws, an expected maximum near `log(500_000) + γ ≈ 13.7`. We got
mean ≈ 0.92 to 1.1 (varying across runs) and max around 8. The mean was
clearly biased and the right tail was conspicuously truncated.

`rnorm()` (the zero-argument form) gave correct moments. `rgamma(2, 3)` was
also slightly off. So the problem correlated with **which executor pattern we
used**, not which distribution the quantile function was for.

## Bug 1 — `UnaryExecutor` caches the lambda result for constant inputs

The killer diagnostic was counting distinct values:

```sql
SELECT count(*) AS total, count(DISTINCT v) AS distinct_cnt
FROM (SELECT rexp(1.0) AS v FROM range(500000));
```

```
total: 500000
distinct_cnt: 245
```

500,000 rows, **245 unique values**. The ratio `500000 / 245 ≈ 2041` is
DuckDB's `STANDARD_VECTOR_SIZE` of 2048 — meaning each chunk got exactly one
`u` value, replicated 2048 times.

The compare-and-contrast was `rnorm()`, which used a hand-written per-row loop
(it took no arguments so we couldn't use `UnaryExecutor` anyway). That one
returned all distinct values across 500k rows. The difference wasn't the
distribution — it was the executor.

### Why this happens

`UnaryExecutor::ExecuteWithNulls` has a fast path for `ConstantVector` inputs:
when `args.data[0]` is a constant `1.0`, the executor reads the scalar once,
invokes the user lambda once, and broadcasts the single result across the
chunk. That is a sound optimisation for pure scalar functions — and DuckDB's
expression framework has every reason to assume scalars are pure unless told
otherwise.

### Why `VOLATILE` doesn't save you

We had already set:

```cpp
ScalarFunction f(...);
f.stability = FunctionStability::VOLATILE;
```

`FunctionStability::VOLATILE` tells the **planner** "don't constant-fold or
common-subexpression-eliminate calls to this function." It does not propagate
to the executor's per-vector code path. The executor's constant-input fast
path is purely a runtime optimisation, and it will fire whether the function
is volatile or not.

So `VOLATILE` does the job of stopping
`SELECT rnorm(0, 1) + rnorm(0, 1) FROM range(N)` from being folded into
`2 * rnorm(0, 1)`, but it doesn't stop `rnorm(0, 1)` itself from being
broadcast inside one vector.

### The fix

Drop `UnaryExecutor` / `BinaryExecutor` and write the per-row loop ourselves
through `UnifiedVectorFormat` (which still handles constant / flat /
dictionary input encodings, but in a loop we control):

```cpp
template <typename Quantile>
static inline void RunPerRow1(DataChunk &args, Vector &result, Quantile &&q) {
    idx_t count = args.size();
    UnifiedVectorFormat a0;
    args.data[0].ToUnifiedFormat(count, a0);
    auto v0 = UnifiedVectorFormat::GetData<double>(a0);
    result.SetVectorType(VectorType::FLAT_VECTOR);
    auto out = FlatVector::GetData<double>(result);
    auto &mask = FlatVector::Validity(result);
    for (idx_t i = 0; i < count; i++) {
        auto i0 = a0.sel->get_index(i);
        if (!a0.validity.RowIsValid(i0)) {
            mask.SetInvalid(i);
            (void)NextUniform(); // keep RNG advance independent of NULLs
            out[i] = 0.0;
            continue;
        }
        out[i] = SafeSampleOne(
            [&](double u) { return q(u, v0[i0]); }, mask, i);
    }
}
```

The result: 500,000 rows, 500,000 distinct values, mean ≈ 0.997, max ≈ 14.
The numbers now match `Exp(1)` theory tightly.

### Takeaways

- The `UnaryExecutor` constant-input optimisation is a property of the
  **executor**, not the function metadata. Volatility flags do not turn it
  off.
- If a scalar function genuinely needs side-effecting per-row state — RNG
  draws, counters, time samples — assume the helpers will broadcast and
  write the loop yourself.
- The single most useful diagnostic for this class of bug is
  `count(*) = count(DISTINCT v)`. If the function is supposed to vary per
  row and that equation fails, you have an executor caching issue, full stop.

## Bug 2 — MSVC's uniform-real distribution clips the tail

Once the per-row loop was in place, the broad shape of the distribution was
correct but `rexp(1.0)` still had a too-light right tail. The diagnostic:

| Statistic                      | Expected (Exp(1), n=500k) | Observed (first NextUniform) |
| ------------------------------ | ------------------------- | ---------------------------- |
| `count(v > 5)`                 | ≈ 3370                    | 4096                         |
| `count(v > 8)`                 | ≈ 167                     | 0                            |
| `count(v > 10)`                | ≈ 23                      | 0                            |
| Implied `max(u)` (= 1 - exp(-max(v))) | ≈ 1 - 1.5×10⁻⁶     | ≈ 0.999665                   |

We were drawing 500k uniforms but the largest one was around 0.99966. The
probability of *no* draw exceeding 0.999665 in 500k is `(0.999665)^500000 ≈
e^-167 ≈ 10⁻⁷²`. Something was capping `u` from above.

The first NextUniform was:

```cpp
std::uniform_real_distribution<double> dist(
    std::nextafter(0.0, 1.0),
    std::nextafter(1.0, 0.0));
double u = dist(rng);
```

This was a deliberate "open interval (0, 1)" — we picked it so the inverse-CDF
transforms below (`log(1-u)`, `qnorm(u)`) couldn't hit the endpoints. On the
MSVC STL we ship against, that distribution does not produce uniformly
distributed draws across its declared support: the upper tail is squeezed
in. The same is true of `std::generate_canonical<double, 53>` on this
toolchain.

We did not chase the MSVC implementation to find exactly why. The bias is
known — there are multi-year-old reports against MSVC STL's
`uniform_real_distribution` for similar symptoms — and we needed a working
RNG more than a culprit.

### The fix

Generate the uniform double manually from a single 64-bit `mt19937_64` draw:

```cpp
static inline double NextUniform() {
    auto &rng = TLSRng();
    uint64_t bits = rng() >> 11;            // keep the top 53 bits
    double u = static_cast<double>(bits) * (1.0 / static_cast<double>(1ULL << 53));
    if (u <= 0.0) {
        u = std::numeric_limits<double>::min();
    }
    return u;
}
```

This is the bit-shift method (sometimes called "Vigna's"): every IEEE 754
double in `[0, 1)` with 53-bit precision gets equal probability mass. The
upper bound is `1 - 2^-53`, so we don't need to nudge that side; we still
nudge the lower side to keep `log(0)` and `qnorm(0)` out of reach.

After the fix, the tail counts match Exp(1) theory at three-significant-figure
precision out to the 10⁻⁵ quantile.

### Takeaways

- `std::uniform_real_distribution<double>` and `std::generate_canonical` are
  fine for "give me a roughly uniform double" but not for **inverse-CDF
  sampling**, where tail accuracy matters disproportionately.
- For inverse-CDF, prefer the explicit bit-shift method from a 64-bit
  generator. It costs one `rng()` call, one shift, one multiply.
- Validating an RNG by checking moments alone is not enough — a distribution
  that clips its upper tail can keep mean and standard deviation close to
  theory while drastically misreporting the tail behaviour that actual
  Monte-Carlo workloads care about. Always look at tail counts (e.g.
  `count(v > threshold)`) against theoretical probabilities.

## Diagnostic checklist for the next time

If a sampling or perturbation function is producing weird-looking output:

1. **`count(*) = count(DISTINCT v)`** — catches per-chunk replication.
2. **Tail counts against theoretical probabilities** — catches uniform-RNG
   bias that moment checks miss.
3. **Inverse round-trip** for sanity (`pnorm(rnorm())` should have mean 0.5,
   stddev `1/√12`).
4. **Run the smoke test more than once** — the thread-local RNG seeds from
   `std::random_device` once per thread, so symptoms drift between runs. A
   single suspicious number could be sampling noise; a moving target across
   reruns is a strong signal of the kind of bug we hit here.

## Where this lives in the code

- `src/random_sampling_function.cpp`
  - `NextUniform()` — bit-shift uniform.
  - `RunPerRow0/1/2` — generic per-row loops that bypass the broadcast
    fast path.
- `src/include/distributions.hpp` — quantile functions that the r* family
  composes with (unchanged by this work — the bugs were upstream of them).

If you ever need to add another VOLATILE scalar function that has side
effects per row (counters, timestamps with sub-millisecond resolution, more
sampling routines), reuse the `RunPerRow*` pattern. It is the only safe
way today.
