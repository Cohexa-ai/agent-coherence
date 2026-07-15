# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Smoke test for the CI merge-gate demo (examples/ci_merge_gate).

Runs the demo in --baseline mode (which exercises BOTH the no-gate negative
control and the with-gate hold-then-fire path) and asserts the honest contract
holds (exit 0), plus exact final values, determinism, and the broken-vs-fixed
divergence, so the demo cannot silently rot.
"""

from __future__ import annotations

import tempfile
from pathlib import Path

STALE_MERGE = "merge PR#1 onto a1f9c04 (CI green vs a1f9c04)"
FRESH_MERGE = "merge PR#1 onto b7e2d55 (CI green vs b7e2d55)"


def test_ci_merge_gate_demo_exits_zero() -> None:
    from examples.ci_merge_gate.main import main

    assert main(["--baseline"]) == 0


def test_baseline_fires_merge_on_stale_base() -> None:
    from examples.ci_merge_gate.main import run_baseline

    with tempfile.TemporaryDirectory() as d:
        trace = run_baseline(Path(d))

    assert trace["fired_stale"] is True
    assert trace["merged"] == STALE_MERGE  # validated vs SHA-A...
    assert trace["base_now"] == "b7e2d55"  # ...while the base already moved
    assert trace["ledger"] == [STALE_MERGE]


def test_gated_holds_then_merges_on_fresh_base() -> None:
    from examples.ci_merge_gate.main import run_gated

    with tempfile.TemporaryDirectory() as d:
        trace = run_gated(Path(d))

    assert trace["held"] is True
    assert trace["merged"] == FRESH_MERGE
    # The stale merge never lands: the only ledger entry is the fresh one.
    assert trace["ledger"] == [FRESH_MERGE]


def test_broken_vs_fixed_divergence_and_determinism() -> None:
    from examples.ci_merge_gate.main import run_baseline, run_gated

    with tempfile.TemporaryDirectory() as d:
        baseline = run_baseline(Path(d))
    with tempfile.TemporaryDirectory() as d:
        gated_1 = run_gated(Path(d))
    with tempfile.TemporaryDirectory() as d:
        gated_2 = run_gated(Path(d))

    # Divergence: no gate merges on the stale validation, the gate on the fresh one.
    assert baseline["merged"] != gated_1["merged"]
    assert baseline["merged"] == STALE_MERGE
    assert gated_1["merged"] == FRESH_MERGE

    # Determinism: the gated path lands the same trace on every run.
    assert gated_1 == gated_2
