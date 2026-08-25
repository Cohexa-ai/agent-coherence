# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Logical clock primitives for deterministic simulation.

Also home to :func:`monotonic_seconds` — the ONE wall-clock tick basis shared
by the coordinator's HTTP handlers, the sweep loops, and the workspace capture
engine (WV plan Unit 3 promoted it here from
``ccs.adapters.claude_code.coordinator_server`` so the capture engine never
imports from an adapter's server module; the server imports it back under the
same name, so every existing import site is unchanged).
"""

import time
from dataclasses import dataclass


def monotonic_seconds() -> int:
    """Tick basis for the coordinator. 1 tick = 1 second of wall clock.

    Deliberately ``int(time.time())`` and NOT ``time.monotonic()``: the grant
    heartbeat handlers, the session heartbeat, ``begin_session``'s
    ``created_at_tick``, the sweep loops, and the workspace-checkpoint capture
    window are ALL seeded from this ONE basis. A boot-relative monotonic clock
    (~1.7e9 below wall-clock) would make every staleness diff permanently
    negative (the F1 sweep bug), and it resets on reboot — which the durable
    ``deadline_tick`` / session-ceiling arithmetic cannot survive.
    ``tests/test_session_wire_and_boundary.py`` pins the basis.
    """
    return int(time.time())


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

