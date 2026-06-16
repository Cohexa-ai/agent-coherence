# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Tests for canonical benchmark scenario fixtures."""

from __future__ import annotations

from pathlib import Path

from ccs.simulation.scenarios import load_scenario


def test_canonical_workload_fixtures_load_and_validate() -> None:
    root = Path(__file__).resolve().parents[1] / "benchmarks" / "scenarios"
    expected = [
        "read_heavy.yaml",
        "write_heavy.yaml",
        "parallel_editing.yaml",
        "large_artifact_reasoning.yaml",
    ]
    for name in expected:
        scenario = load_scenario(str(root / name))
        assert scenario["scenario"]["workload"] in {
            "read_heavy",
            "write_heavy",
            "parallel_editing",
            "large_artifact_reasoning",
        }


def test_access_model_fixtures_cover_all_context_semantics() -> None:
    root = Path(__file__).resolve().parents[1] / "benchmarks" / "scenarios"
    models = set()
    for name in [
        "access_always_read.yaml",
        "access_conditional_injection.yaml",
        "access_pointer.yaml",
    ]:
        scenario = load_scenario(str(root / name))
        models.add(scenario["context_semantics"]["model"])
    assert models == {"always_read", "conditional_injection", "pointer"}
