# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""Tests for architecture boundary and cycle checks."""

from __future__ import annotations

from pathlib import Path

from ccs.hardening.architecture import find_boundary_violations, find_cycles, run_architecture_checks


def test_project_architecture_has_no_boundary_or_cycle_violations() -> None:
    src_root = Path(__file__).resolve().parents[1] / "src"
    report = run_architecture_checks(src_root)

    assert report.boundary_violations == []
    assert report.cycles == []


def test_cycle_detection_finds_strongly_connected_component() -> None:
    graph = {
        "ccs.core.a": {"ccs.core.b"},
        "ccs.core.b": {"ccs.core.c"},
        "ccs.core.c": {"ccs.core.a"},
        "ccs.core.d": set(),
    }
    cycles = find_cycles(graph)
    assert cycles == [["ccs.core.a", "ccs.core.b", "ccs.core.c"]]


def test_boundary_violation_detection_reports_forbidden_edge() -> None:
    graph = {
        "ccs.core.types": {"ccs.simulation.engine"},
        "ccs.simulation.engine": set(),
    }
    violations = find_boundary_violations(graph)
    assert len(violations) == 1
    assert "ccs.core.types" in violations[0]
    assert "ccs.simulation.engine" in violations[0]


# ---- v0.9.0 Unit 10: benchmark-attestation merge gate (Finding #2) ----------
#
# The C-flip plan requires a machine-checkable artifact set so a v0.9.0 release
# cannot merge without the documented benchmark validation. This is the
# enforceable complement to the reviewer-checked attestation content.


def test_v090_benchmark_attestation_artifacts_present() -> None:
    """The v0.9.0 benchmark attestation directory exists with non-empty raw
    runs and an attestation.md covering the zero-drift + zero-reclaim claims."""
    repo_root = Path(__file__).resolve().parents[1]
    attestation_dir = repo_root / "benchmarks" / "results" / "v0.9.0"
    assert attestation_dir.is_dir(), "missing benchmarks/results/v0.9.0/ attestation dir"

    required = [
        "attestation.md",
        "c_flip_branch_run.txt",
        "drift_check_vs_expected.txt",
    ]
    for name in required:
        path = attestation_dir / name
        assert path.is_file(), f"missing attestation artifact: {name}"
        assert path.stat().st_size > 0, f"empty attestation artifact: {name}"

    attestation = (attestation_dir / "attestation.md").read_text(encoding="utf-8").lower()
    # Required sections / claims (kept loose so prose can evolve).
    assert "drift" in attestation
    assert "reclaim" in attestation
    assert "0.00pp" in attestation
