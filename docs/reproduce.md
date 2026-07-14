# Reproducing CCS Simulation Results

Results in [Token Coherence: Adapting MESI Cache Protocols to Minimize Synchronization Overhead in Multi-Agent LLM Systems](https://arxiv.org/abs/2603.15183) §8 are reproducible from this repository.

> **Scope:** this page reproduces the **cost-proxy pipeline** — the cost/simulation axis (re-fetches avoided under coherence-gating). It does not exercise the correctness properties; for those, see [Reproducing the correctness claims](#reproducing-the-correctness-claims) below.

## Requirements

- Python 3.11+
- ~3-6 min runtime on a modern laptop

## Quick start

```bash
git clone https://github.com/Cohexa-ai/agent-coherence
cd agent-coherence
bash scripts/reproduce.sh
```

## Output files

| File | Corresponds to |
|------|----------------|
| `benchmarks/results/step5/read_heavy.json` | Table 1, §8.2 - read-heavy workload |
| `benchmarks/results/step5/write_heavy.json` | Table 1, §8.2 - write-heavy workload |
| `benchmarks/results/step5/parallel_editing.json` | Table 1, §8.2 - parallel editing |
| `benchmarks/results/step5/large_artifact_reasoning.json` | Table 1, §8.2 - large artifact workload |
| `benchmarks/results/step5/access_*.json` | §8 access semantics comparison |
| `benchmarks/results/step5/SUMMARY.md` | Full scenario summary table |
| `benchmarks/results/step_scaling.json` | §8.5 Table 4 - S-scaling |
| `benchmarks/results/artifact_scaling.json` | §8.5 Table 5 - |d|-scaling |

## Committed baseline

`benchmarks/results/step5/` contains the committed canonical baseline (see `generated_on` in `SUMMARY.md` / `manifest.json`; 10 runs per strategy, `eager` + `lazy`).

`scripts/reproduce.sh` re-runs all scenarios and verifies output against `SUMMARY.md` within ±0.5% tolerance using `tools/verify_baseline.py`.

## Temporal-cost sweep (TC-1)

A separate, pre-registered benchmark for the **temporal / source-drift** dimension — how many re-fetches coherence-gating avoids as a *single* agent's source changes between turns, versus an always-re-fetch baseline. Distinct from the spatial workloads above (more agents sharing one artifact); do not splice the two.

```bash
python tools/run_cost_sweep.py \
  --rates 0,0.05,0.1,0.15,0.2,0.25,0.3,0.35,0.5,0.75,1.0 \
  --sensitivities 0,0.5,1.0 --runs 50 \
  --output benchmarks/results/cost_sweep_published.json
python tools/plot_cost_sweep.py    # → benchmarks/results/cost_sweep_savings_curve.svg
python tools/cost_to_tokens.py     # token/$ translation, under stated assumptions
```

The seeded sweep reproduces the committed `benchmarks/results/cost_sweep_published.json` byte-for-byte. The pre-registered verdict (PASS at n=50; savings ≥ 30% across all `r ≤ 0.30`; crossover `r ≈ 0.31`) and the distinguisher triage live in [`../benchmarks/cost_preregistration.md`](../benchmarks/cost_preregistration.md). The metric is **re-fetches-avoided** — a proxy / regime map, **not** a token-dollar invoice; `cost_to_tokens.py`'s dollar figures are explicitly assumption-parameterized. Shipped in `v0.9.3` (#116).

## Simulation scope

The simulation models token transmission accounting, MESI state transitions, write-frequency effects, and artifact volatility.

It does not model LLM inference latency, real framework scheduler jitter, or event bus network RTT outside the configured simulator latency/loss parameters.

## Reproducing the correctness claims

The numbers above are a **cost/simulation** proxy, not a correctness proof. The coherence properties are shown by runnable examples and machine-checked models — all single-host:

| What it shows | Where |
|---------------|-------|
| A stale write-back is denied (write-side lost-update prevention) | [`examples/coherent_volume`](../examples/coherent_volume/README.md) |
| Concurrent writers to one artifact — the stale writer is rejected | [`examples/concurrent_writers`](../examples/concurrent_writers/README.md) |
| `gate()` orders side effects behind a fresh read (it orders effects; it does not roll back) | [`examples/effect_gate`](../examples/effect_gate/README.md) |
| Machine-checked TLA+ safety models (the CI-run specs and their named invariants) | [`formal/tla/`](../formal/tla/README.md) |

Enforcement is single-host (one coordinator). Cross-host coordination is on the roadmap, demand-gated.
