# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Logical clock primitives for deterministic simulation."""

from dataclasses import dataclass


@dataclass
class LogicalClock:
    """Monotonic tick clock used by simulation and tests."""

    tick: int = 0

    def advance(self, n: int = 1) -> int:
        """Advance the clock by a non-negative number of ticks."""
        if n < 0:
            raise ValueError("Cannot advance clock backwards")
        self.tick += n
        return self.tick

    def now(self) -> int:
        """Return current tick."""
        return self.tick

    def elapsed_since(self, past_tick: int) -> int:
        """Return elapsed ticks relative to a past tick."""
        if past_tick > self.tick:
            raise ValueError("past_tick cannot be in the future")
        return self.tick - past_tick

