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

**Axis note (by design).** `savings_ratio` and `refetches_avoided` are **invariant
along the answer-sensitivity axis**: gating re-fetches on *every* source change
regardless of whether the change was answer-relevant, so the fetch counts depend
only on change-rate. `wasted_refetches` is therefore the only metric that moves
with answer-sensitivity, and it is drift-gated in `tools/cost_drift_check.py` so
the second axis is not left unguarded in CI. A savings PASS/NULL verdict must not
depend on sensitivity (see the `degenerate-sensitivity-model` distinguisher).

## Verdicts

### PASS — gating's savings are materially worth the dependency

There exists a realistic change-rate regime where gating saves enough re-fetch
cost, with low enough waste, to justify the coordination layer.

- **Rule (LOCKED 2026-06-05, before the run):** `savings_ratio ≥ 30%`
  **sustained across all rates `r ≤ 0.30`**, with `wasted_refetches ≤
  refetches_avoided` (evaluated at answer-sensitivity `s = 0.5`) in that same
  regime.
- Rationale (fixed before seeing refined numbers): `X = 30%` is the
  material-improvement bar below which a coordination dependency isn't worth
  adopting; real RAG sources drift *occasionally* (≤30% of turns), so `R = 0.30`
  is the realistic-change-rate ceiling; the over-fetch guard requires gating's
  waste not exceed its savings in-band. (This file's earlier scaffolding gave
  `R = 0.25` as the conservative example — see § Result: PASS holds at both.)
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

## Result — PASS (thresholds locked 2026-06-05; confirmed at n=50 on 2026-06-14)

Thresholds were LOCKED 2026-06-05 (`X = 30%`, `R = 0.30`, `W`: `wasted ≤ avoided
@ s = 0.5`) before any refined run. The prior refined run lived only in an
uncommitted worktree at `runs_per_point = 10`; **this canonical commit
(2026-06-14) promotes that verdict into the tracked repo, makes it reproducible
from committed code, and re-runs the refined grid at `runs_per_point = 50`** to
confirm the PASS at higher power.

**Reproduce (from committed code):**

```
python tools/run_cost_sweep.py \
  --rates 0,0.05,0.1,0.15,0.2,0.25,0.3,0.35,0.5,0.75,1.0 \
  --sensitivities 0,0.5,1.0 --runs 50 \
  --output benchmarks/results/cost_sweep_published.json
python tools/plot_cost_sweep.py   # → benchmarks/results/cost_sweep_savings_curve.svg
```

Committed artifacts: `benchmarks/results/cost_sweep_published.json` (raw) +
`benchmarks/results/cost_sweep_savings_curve.svg` (figure). The sweep is seeded
(`SEED_START = 20260318`), so the numbers below reproduce exactly.

**Refined curve (n=50; savings is answer-sensitivity-invariant — `wasted` shown @ s=0.5):**

| rate r | savings | avoided | wasted@s0.5 | in-band (r ≤ 0.30) | W ok |
|---|---|---|---|---|---|
| 0.00 | 79.3% | 91.3 | 0.0 | ✓ | ✓ |
| 0.05 | 66.4% | 76.9 | 8.3 | ✓ | ✓ |
| 0.10 | 56.4% | 65.7 | 12.9 | ✓ | ✓ |
| 0.15 | 48.8% | 57.1 | 15.4 | ✓ | ✓ |
| 0.20 | 42.1% | 49.4 | 17.7 | ✓ | ✓ |
| 0.25 | 36.6% | 42.9 | 19.5 | ✓ | ✓ |
| 0.30 | 32.1% | 37.8 | 20.7 | ✓ | ✓ |
| 0.35 | 27.7% | 32.7 | 22.3 | — | — |
| 0.50 | 17.8% | 21.1 | 23.9 | — | — |
| 0.75 |  7.3% |  8.7 | 22.6 | — | — |
| 1.00 |  0.0% |  0.0 | 21.6 | — | — |

**Verdict: PASS.** `savings_ratio ≥ 30%` holds across every in-band rate
`r ≤ 0.30` (the 30% crossover is at `r ≈ 0.31`), and `wasted ≤ avoided @ s = 0.5`
holds at every in-band rate. The boundary margin at `r = 0.30` is **2.1pp**
(32.1% vs 30%) — thin but real, and at the *pre-registered* threshold, not a
reverse-fit one. PASS also holds at the conservative example ceiling `R = 0.25`
(savings 36.6%, a 6.6pp margin).

**Distinguishers triaged (all ruled out):**

- *grid-too-coarse* — resolved: the refined grid samples `r ∈ {0.05 … 0.35}`, so
  the 30% crossover (`r ≈ 0.31`) is directly observed, not interpolated across a gap.
- *gating-dominates-everywhere* — ruled out: savings decays monotonically to
  **0.0% at r = 1.0** (a proper cost floor), not flat-high.
- *degenerate-sensitivity-model* — handled by design: savings is **exactly**
  sensitivity-invariant (max spread `0.0000` across `s ∈ {0, 0.5, 1}`), so the
  savings verdict doesn't depend on `s`; the `W` guard is read at `s = 0.5` as specified.
- *baseline-mis-modeled* — ruled out: blind floor (`12.0`) < gated < always-read
  ceiling (`~115–120`) at every rate; the baselines bracket gating correctly.

**Honest caveats (carried, not hidden):** the metric is **re-fetches avoided**
(fetch counts) — a proxy for the headline "token-spend + prompt-cache
preservation" cost function, **not** a token/cache-dollar model (that modeling is
a tracked follow-up). The source is synthetic; `R` (the realistic change-rate
band) is an assumption, not a field measurement. The PASS is therefore scoped to
*"gating's re-fetch savings clear 30% across the assumed realistic band,"*
verified reproducibly — a regime map, not a measured dollar figure.
