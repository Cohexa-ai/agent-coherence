# Reproducing CCS Simulation Results

Results in [Token Coherence: Adapting MESI Cache Protocols to Minimize Synchronization Overhead in Multi-Agent LLM Systems](https://arxiv.org/abs/2603.15183) §8 are reproducible from this repository.

## Requirements

- Python 3.11+
- ~3-6 min runtime on a modern laptop

## Quick start

```bash
git clone https://github.com/hipvlady/agent-coherence
cd agent-coherence
bash reproduce.sh
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

`reproduce.sh` re-runs all scenarios and verifies output against `SUMMARY.md` within ±0.5% tolerance using `tools/verify_baseline.py`.

## Simulation scope

The simulation models token transmission accounting, MESI state transitions, write-frequency effects, and artifact volatility.

It does not model LLM inference latency, real framework scheduler jitter, or event bus network RTT outside the configured simulator latency/loss parameters.
