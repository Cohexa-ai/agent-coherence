---
title: v0.9.0 crash-recovery default-flip benchmark attestation
date: 2026-06-03
requirement: R10 (D-bench three-step)
plan: docs/plans/2026-05-28-001-feat-c-flip-crash-recovery-default-on-plan.md (Unit 10)
---

# v0.9.0 Benchmark Validation Attestation

This attests that the v0.9.0 crash-recovery default flip (`CrashRecoveryConfig.enabled`
`False` → `True`, with retuned `heartbeat_timeout_ticks=120` / `max_hold_ticks=900`,
plus the rate-limited `_maybe_sweep` wiring in `CoherenceAdapterCore`) introduces
**zero unexplained drift** in `benchmarks/expected.json` and **zero unexplained
`reclaim_*` events** under the real-workload benchmarks.

## 1. Baseline (main) vs C-flip branch — zero drift

The committed `benchmarks/expected.json` is the frozen main baseline. The benchmark
generator (`tools/run_benchmarks.py`) is fully deterministic — no RNG seeding variance,
no wall-clock dependence — so a single capture suffices for the noise check (the
"two identical runs" step of D-bench is trivially satisfied by determinism, and a second
run was confirmed identical during development).

- Raw C-flip-branch run: [`c_flip_branch_run.txt`](c_flip_branch_run.txt)
- Drift check vs committed baseline: [`drift_check_vs_expected.txt`](drift_check_vs_expected.txt)

| Workload | Expected | Actual | Delta |
|---|---|---|---|
| 4-agent planning pipeline (read-heavy) | 68.73% | 68.73% | **0.00pp** |
| 3-agent code review (write-moderate) | 46.71% | 46.71% | **0.00pp** |
| 4-agent high-churn (write-heavy) | 28.71% | 28.71% | **0.00pp** |

`expected.json` is therefore **unchanged** — no re-anchoring was performed, and none
was warranted (ADV-07 / RM-3: never silently rebaseline).

## 2. Why there is zero drift (source attribution)

The flip makes the simulation engine and the bare adapters enabled-by-default, so the
engine now emits a per-tick heartbeat for every *alive* agent and runs the reclamation
sweep. Two reasons this does not move the benchmark token counts:

1. **Heartbeats are not coherence traffic.** The benchmark measures `ccs_tokens` =
   invalidation signals + pointer updates. `record_heartbeat` is coordinator-internal
   state and emits no inter-agent message, so it contributes zero tokens.
2. **Zero reclamations in the benchmark scenarios.** The benchmark workloads contain no
   agent kills or stalls; every agent is heartbeated every tick, so no heartbeat gap ever
   reaches the 120-tick timeout and no grant is held past the 900-tick max-hold. The
   sweep runs but reclaims nothing — no `reclaim_*` event is emitted on any of
   `bench_planner.py`, `bench_code_review.py`, `bench_high_churn.py`.

## 3. Reclamation correctness (empirical, OOM-kill shape — RM-10 fallback)

The contributed `cron-fan-out` fixture is not yet in-repo (RM-10). Its OOM-kill fallback —
"construct one M/E grant-holder, kill it mid-run so its heartbeat gaps past the timeout,
assert the sweep reclaims exactly that agent and no live agent" — is exercised
deterministically and as CI-runnable tests rather than as a standalone benchmark fixture:

- `tests/test_adapter_crash_recovery.py::TestMaybeSweepEndToEnd::test_stale_grant_reclaimed_on_read_diagnostic_once`
  — a stalled MODIFIED grant-holder is reclaimed on a peer's read; the diagnostic emits once.
- `tests/adapters/test_ccsstore.py::test_ccsstore_default_thresholds_no_false_reclaim_then_reclaims`
  — with the **default** 120/900 thresholds, a held grant is NOT reclaimed within the
  heartbeat window (gap 60 < 120) and IS reclaimed once past it (gap 120 ≥ 120).
- `tests/test_engine.py::test_engine_flag_on_diverges_from_explicit_false_via_reclaim`
  — a killed grant-holder is reclaimed by the engine sweep after its heartbeat gaps past
  the timeout; the divergence from the disabled path is attributable to a `reclaim_*` event.

Each asserts reclamation fires on exactly the stale grant and never on a live agent.

## 4. Sign-off

Zero unexplained `expected.json` drift; zero unexplained reclamations under the
real-workload benchmarks; reclamation correctness covered by the CI-runnable tests above.

Author: Vlad (hipvlady) — prepared ahead of the v0.9.0 release gate (≥ 2026-06-09, R13).
