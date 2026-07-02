# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Smoke test for the effect-ordering gate demo (examples/effect_gate).

Runs the demo in --baseline mode (which exercises BOTH the no-gate negative
control and the with-gate hold-then-fire path) and asserts the honest contract
holds (exit 0), so the demo cannot silently rot.
"""

from __future__ import annotations


def test_effect_gate_demo_exits_zero() -> None:
    from examples.effect_gate.main import main

    assert main(["--baseline"]) == 0
