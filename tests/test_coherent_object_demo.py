# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Smoke test for the CoherentObject cross-agent demo (examples/coherent_object).

Runs the demo in both modes and asserts the honest contract holds (exit 0), so
the demo cannot silently rot: with the binding the stale act is denied before the
object is touched and A recovers; with --baseline the un-coordinated agent acts on
a stale cache (the failure the binding catches).
"""

from __future__ import annotations


def test_coherent_object_demo_exits_zero() -> None:
    from examples.coherent_object.main import main

    assert main([]) == 0


def test_coherent_object_demo_baseline_exits_zero() -> None:
    from examples.coherent_object.main import main

    assert main(["--baseline"]) == 0
