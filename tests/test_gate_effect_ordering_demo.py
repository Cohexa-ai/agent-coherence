# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Smoke test for the deploy-on-moved-base demo (examples/gate_effect_ordering).

Runs the demo, which exercises BOTH the no-gate negative control (fires the
deploy on the stale base) and the with-gate hold-then-fire-on-fresh path, and
asserts the honest contract holds (exit 0), so the demo cannot silently rot.
"""

from __future__ import annotations


def test_gate_effect_ordering_demo_exits_zero() -> None:
    from examples.gate_effect_ordering.main import main

    assert main([]) == 0
