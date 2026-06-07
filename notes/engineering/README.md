# Engineering notes

Short writeups about non-obvious decisions and bugs that future contributors
(or future-us) will want to know about. Each note is dated and self-contained;
they are not living documentation, they are postmortems / decision records.

If you hit a subtle bug while developing on stats_duck and the fix was longer
than the diagnosis, please add a note here. It is much easier to find a thing
that has been written down than to re-derive it.

## Inventory

- [`2026-06-volatile-and-rng-bias.md`](2026-06-volatile-and-rng-bias.md) — two
  RNG footguns hit while landing the `r*` random-sampling family: DuckDB's
  `UnaryExecutor` caches the lambda result for constant-vector inputs (the
  `VOLATILE` flag does not disable this), and MSVC's
  `std::uniform_real_distribution<double>` clips the upper tail. Fixes and
  diagnostic checklist.
