# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents
"""Smoke test for the atomic multi-file publish demo (examples/atomic_publish).

Runs the demo in --baseline mode (which exercises BOTH the file-by-file negative
control that lands a torn pair AND the atomic-publish hold-then-land path) and
asserts the honest contract holds (exit 0), so the demo cannot silently rot.
"""
from __future__ import annotations


def test_atomic_publish_demo_exits_zero() -> None:
    from examples.atomic_publish.main import main

    assert main(["--baseline"]) == 0
