# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Unit tests for the logical clock primitive."""

import pytest

from ccs.core.clock import LogicalClock


def test_clock_advances_and_now() -> None:
    clock = LogicalClock()
    assert clock.now() == 0
    assert clock.advance() == 1
    assert clock.advance(3) == 4
    assert clock.now() == 4


def test_clock_rejects_negative_advance() -> None:
    clock = LogicalClock()
    with pytest.raises(ValueError):
        clock.advance(-1)


def test_elapsed_since_rejects_future_tick() -> None:
    clock = LogicalClock(tick=2)
    with pytest.raises(ValueError):
        clock.elapsed_since(3)

