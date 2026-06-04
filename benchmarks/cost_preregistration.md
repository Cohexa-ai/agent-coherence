# Cost-sweep pre-registration

Falsifiability scaffolding for the change-rate × answer-sensitivity cost sweep
(`tools/run_cost_sweep.py`, third benchmark lane — temporal simulation). It
fixes the three verdicts **and their decision rule** before the full sweep is
run, so the result cannot be reverse-justified after the numbers land.

The sweep emits, per `(change-rate r, answer-sensitivity s)` cell, gating's
`savings_ratio` (1 − gated_fetches / always_fetches) and `refetches_avoided`
(always_fetches − gated_fetches), every figure provenance-labeled
`temporal-sim (third lane)`. Gating buys re-fetch savings; the question this
pre-registration answers is **whether that saving is worth the coordination
dependency at realistic change-rates**, or whether it collapses to noise.

> **Threshold VALUES (`X`, `R`, `W`, `S`) are a founder input set _before_ the
> full sweep run — they are NOT part of this file's acceptance.** This file is
> complete when the three verdicts, the distinguishers, and the placeholder
> thresholds are present. The committed CI baseline lives in
> `benchmarks/expected_cost.json` and the drift gate in
> `tools/cost_drift_check.py`; those guard reproducibility, not the verdict.

## Metrics under test

| Metric | Source field | Meaning |
| --- | --- | --- |
| Savings | `savings_ratio` | Fraction of always-re-fetch cost gating avoids. |
| Avoided | `refetches_avoided` | Absolute re-fetches gating skips vs. always-read. |
| Waste | `wasted_refetches` | Gating re-fetches that were answer-irrelevant churn. |

## Verdicts

### PASS — gating's savings are materially worth the dependency

There exists a realistic change-rate regime where gating saves enough re-fetch
cost, with low enough waste, to justify the coordination layer.

- **Rule (placeholder):** `savings_ratio ≥ X%` **sustained across all rates
  `r ≤ R`**, with `wasted_refetches ≤ W` in that same regime.
- Placeholders to be set by founder before the run: `X` (min savings, e.g.
  `30%`), `R` (the calm-source ceiling savings must hold below, e.g. `0.25`),
  `W` (max tolerated wasted re-fetches, e.g. `5`).
- Interpretation: the savings curve is real and lands in the change-rate band
  real workloads actually occupy — not only at the degenerate `r = 0` endpoint.

### NULL — savings are trivial across realistic change-rates (honest falsification)

Across the realistic-change-rate band, gating's savings never clear the PASS
bar; the dependency does not pay for itself. This is the honest negative result
and is reported as such — **not** softened into INCONCLUSIVE.

- **Rule (placeholder):** for every rate `r` in the realistic band
  (`r_min … R`), `savings_ratio < X%` (or `savings_ratio ≥ X%` only at
  unrealistic `r ≈ 0`), i.e. savings decay to noise before the realistic band.
- Declared **only after** every INCONCLUSIVE distinguisher below has been ruled
  out — otherwise the apparent null may be a measurement artifact.

### INCONCLUSIVE — the run cannot adjudicate PASS vs. NULL yet

The sweep as run does not let us separate "no real effect" from "effect hidden
by how we measured it". Triage **all** distinguishers below and re-run before
declaring NULL.

#### Distinguishers (triage each before declaring NULL)

- **gating-dominates-everywhere** — `savings_ratio` high (and `refetches_avoided`
  large) at *every* rate including `r = 1.0`. A genuine cost floor should let
  savings fall toward 0 as the source churns constantly; flat-high savings
  signals the always-read ceiling is mis-instrumented, not that gating wins
  everywhere.
- **grid-too-coarse** — savings drop from high→trivial across one rate step
  (e.g. `0.0 → 0.5`), so the realistic band `R` falls *between* grid points and
  the crossover is unobserved. Refine the `rates` grid around `R` and re-run.
- **degenerate-sensitivity-model** — `savings_ratio` is identical across all
  `answer-sensitivity` values (sensitivity moves only `wasted_refetches`, never
  savings). If the verdict depends on sensitivity but the model can't express
  it, the sensitivity axis is degenerate for this question — fix the model
  before reading savings.
- **baseline-mis-modeled** — the always-read ceiling or blind floor is wrong
  (e.g. `always_fetches ≈ gated_fetches`, collapsing `savings_ratio → 0`
  spuriously, or `blind_fetches > gated_fetches`). The denominator is broken;
  no savings verdict is valid until the baselines bracket gating correctly.

## Decision flow

```
run full sweep
  └─ any INCONCLUSIVE distinguisher fires?
        ├─ yes → INCONCLUSIVE: fix instrumentation/grid/model, re-run
        └─ no  → PASS rule satisfied?
                    ├─ yes → PASS
                    └─ no  → NULL (honest falsification)
```
