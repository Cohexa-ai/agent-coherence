# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""S-scaling sweep: vary duration_ticks with broadcast/eager/lazy baselines."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from ccs.simulation.engine import run_strategy_comparison
from ccs.simulation.scenarios import load_scenario

STEP_VALUES = [5, 10, 20, 40, 50, 100]
RUNS_PER_POINT = 10
STRATEGIES = ["broadcast", "eager", "lazy"]
SEED_START = 20260305


def _make_scenario(duration_ticks: int) -> dict:
    scenario = load_scenario(str(REPO_ROOT / "benchmarks" / "scenarios" / "planning_canonical.yaml"))
    scenario["simulation"]["duration_ticks"] = duration_ticks
    scenario["simulation"]["seed"] = SEED_START
    scenario["scenario"]["name"] = f"step-scaling-S{duration_ticks}"
    return scenario


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="S-scaling sweep for CCS benchmarks.")
    parser.add_argument("--output", default="benchmarks/results/step_scaling.json")
    args = parser.parse_args(argv)

    rows: list[dict[str, float | int]] = []
    for step_count in STEP_VALUES:
        scenario = _make_scenario(step_count)
        report = run_strategy_comparison(
            scenario,
            strategies=STRATEGIES,
            runs=RUNS_PER_POINT,
            seed_start=SEED_START,
        )
        aggregated = {item["strategy"]: item for item in report.aggregated}
        broadcast_sync = float(aggregated["broadcast"]["synchronization_tokens_mean"])
        eager_sync = float(aggregated["eager"]["synchronization_tokens_mean"])
        lazy_sync = float(aggregated["lazy"]["synchronization_tokens_mean"])
        savings_vs_broadcast = 0.0 if broadcast_sync == 0 else max(0.0, 1.0 - lazy_sync / broadcast_sync)
        savings_vs_eager = 0.0 if eager_sync == 0 else max(0.0, 1.0 - lazy_sync / eager_sync)
        rows.append(
            {
                "S": step_count,
                "broadcast_sync_tokens_mean": broadcast_sync,
                "eager_sync_tokens_mean": eager_sync,
                "lazy_sync_tokens_mean": lazy_sync,
                "lazy_savings_vs_broadcast": round(savings_vs_broadcast, 4),
                "lazy_savings_vs_eager": round(savings_vs_eager, 4),
            }
        )
        print(
            f"  S={step_count:4d}: broadcast={broadcast_sync:>12.1f} "
            f"eager={eager_sync:>12.1f} lazy={lazy_sync:>12.1f} "
            f"savings_vs_bcast={savings_vs_broadcast:.1%} savings_vs_eager={savings_vs_eager:.1%}"
        )

    payload = {
        "sweep": "steps",
        "runs_per_point": RUNS_PER_POINT,
        "strategies": STRATEGIES,
        "rows": rows,
    }
    output_path = REPO_ROOT / args.output
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"Wrote {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
