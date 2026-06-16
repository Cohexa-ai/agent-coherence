# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Benchmark regression smoke check for CI hardening."""

from __future__ import annotations

from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from ccs.simulation.engine import run_strategy_comparison


def _scenario() -> dict:
    return {
        "simulation": {"duration_ticks": 16, "num_agents": 4, "seed": 21, "action_probability": 0.6},
        "network": {"latency_ticks": 1, "message_loss_rate": 0.0},
        "scenario": {
            "name": "benchmark-smoke",
            "workload": "custom",
            "action_probability": 0.6,
            "write_probability": 0.25,
            "revocation_tick": None,
        },
        "artifacts": [
            {"id": "plan.md", "size_tokens": 512, "volatility": 0.2, "initial_version": 1, "mutable": True},
            {"id": "facts.json", "size_tokens": 1024, "volatility": 0.1, "initial_version": 1, "mutable": True},
        ],
        "strategies": {
            "eager": {},
            "lazy": {"check_interval_ticks": 2},
            "lease": {"default_ttl_ticks": 5},
            "access_count": {"max_accesses": 3},
            "exec_count": {"max_operations": 3},
        },
        "transient": {"timeout_ticks": 5},
        "context_semantics": {"model": "pointer"},
    }


def main() -> int:
    report = run_strategy_comparison(
        _scenario(),
        strategies=["eager", "lazy"],
        runs=2,
        seed_start=300,
    )
    payload = report.to_dict()

    if len(payload["runs"]) != 4:
        print("Expected 4 runs in smoke benchmark payload.")
        return 1
    if len(payload["aggregated"]) != 2:
        print("Expected 2 aggregated strategy rows.")
        return 1
    print("Benchmark regression smoke passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
