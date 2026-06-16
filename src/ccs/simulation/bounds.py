# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Helpers for enforcing strategy-specific staleness bounds."""

from __future__ import annotations

from ccs.core.invariants import check_bounded_staleness
from ccs.strategies.base import SyncStrategy


def strategy_staleness_bound(strategy: SyncStrategy) -> int | None:
    """Return strategy-defined stale-step bound if one exists."""
    return strategy.staleness_bound()


def enforce_strategy_staleness_bound(strategy: SyncStrategy, *, steps_on_stale: int) -> None:
    """Validate stale-step count against strategy bound when configured."""
    bound = strategy_staleness_bound(strategy)
    if bound is None:
        return
    check_bounded_staleness(steps_on_stale, bound)
