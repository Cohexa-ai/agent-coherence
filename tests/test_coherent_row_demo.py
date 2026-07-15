# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Smoke test for the CoherentRow cross-agent demo (examples/coherent_row).

Runs the demo in both modes and asserts the honest contract holds (exit 0), so
the demo cannot silently rot: with the binding the stale act is denied before the
row is touched and A recovers; with --baseline the un-coordinated agent acts on a
stale cache (the failure the binding catches).
"""

from __future__ import annotations


def test_coherent_row_demo_exits_zero() -> None:
    from examples.coherent_row.main import main

    assert main([]) == 0


def test_coherent_row_demo_baseline_exits_zero() -> None:
    from examples.coherent_row.main import main

    assert main(["--baseline"]) == 0
