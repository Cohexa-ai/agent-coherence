# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Tests for strategy-aware bounded staleness checks."""

from __future__ import annotations

import pytest

from ccs.core.exceptions import InvariantViolationError
from ccs.simulation.bounds import enforce_strategy_staleness_bound, strategy_staleness_bound
from ccs.strategies.access_count import AccessCountStrategy
from ccs.strategies.eager import EagerStrategy
from ccs.strategies.lazy import LazyStrategy
from ccs.strategies.lease import LeaseStrategy


def test_bound_helper_returns_expected_values() -> None:
    assert strategy_staleness_bound(EagerStrategy()) == 0
    assert strategy_staleness_bound(LeaseStrategy(ttl_ticks=4)) == 4
    assert strategy_staleness_bound(AccessCountStrategy(max_accesses=3)) == 3
    assert strategy_staleness_bound(LazyStrategy()) is None


def test_eager_bound_allows_only_zero_stale_steps() -> None:
    strategy = EagerStrategy()
    enforce_strategy_staleness_bound(strategy, steps_on_stale=0)
    with pytest.raises(InvariantViolationError):
        enforce_strategy_staleness_bound(strategy, steps_on_stale=1)


def test_lease_bound_enforced_by_ttl_ticks() -> None:
    strategy = LeaseStrategy(ttl_ticks=2)
    enforce_strategy_staleness_bound(strategy, steps_on_stale=2)
    with pytest.raises(InvariantViolationError):
        enforce_strategy_staleness_bound(strategy, steps_on_stale=3)


def test_access_count_bound_enforced_by_max_accesses() -> None:
    strategy = AccessCountStrategy(max_accesses=5)
    enforce_strategy_staleness_bound(strategy, steps_on_stale=5)
    with pytest.raises(InvariantViolationError):
        enforce_strategy_staleness_bound(strategy, steps_on_stale=6)


def test_lazy_has_no_strategy_level_bound() -> None:
    strategy = LazyStrategy()
    enforce_strategy_staleness_bound(strategy, steps_on_stale=10000)
