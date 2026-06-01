# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""Behavior tests for synchronization strategy implementations."""

from __future__ import annotations

from uuid import uuid4

import pytest

from ccs.core.states import MESIState
from ccs.simulation.engine import SimulationEngine
from ccs.simulation.scenarios import load_scenario
from ccs.strategies.access_count import AccessCountStrategy
from ccs.strategies.broadcast import BroadcastStrategy
from ccs.strategies.eager import EagerStrategy
from ccs.strategies.lazy import LazyStrategy
from ccs.strategies.lease import LeaseStrategy
from ccs.strategies.selector import build_strategy, select_strategy_name_for_role


def test_eager_broadcasts_full_content_and_has_zero_staleness_bound() -> None:
    strategy = EagerStrategy()
    entry = strategy.on_fetch(
        artifact_id=uuid4(),
        version=7,
        state=MESIState.SHARED,
        now_tick=10,
    )

    assert strategy.broadcasts_content_on_commit() is True
    assert strategy.staleness_bound() == 0
    assert strategy.requires_refresh(entry, now_tick=11) is False
    assert strategy.on_read(entry, now_tick=11).access_count == 1


def test_lazy_invalidates_and_fetches_on_demand() -> None:
    strategy = LazyStrategy()
    valid_entry = strategy.on_fetch(
        artifact_id=uuid4(),
        version=3,
        state=MESIState.SHARED,
        now_tick=2,
    )
    invalid_entry = strategy.on_fetch(
        artifact_id=uuid4(),
        version=3,
        state=MESIState.INVALID,
        now_tick=2,
    )

    assert strategy.broadcasts_content_on_commit() is False
    assert strategy.requires_refresh(valid_entry, now_tick=3) is False
    assert strategy.requires_refresh(invalid_entry, now_tick=3) is True
    assert strategy.staleness_bound() is None


def test_lease_refreshes_after_ttl_expiry() -> None:
    strategy = LeaseStrategy(ttl_ticks=5)
    entry = strategy.on_fetch(
        artifact_id=uuid4(),
        version=1,
        state=MESIState.SHARED,
        now_tick=10,
    )

    assert entry.expires_at_tick == 15
    assert strategy.staleness_bound() == 5
    assert strategy.requires_refresh(entry, now_tick=14) is False
    assert strategy.requires_refresh(entry, now_tick=15) is True


def test_access_count_refreshes_after_max_accesses() -> None:
    strategy = AccessCountStrategy(max_accesses=2)
    entry = strategy.on_fetch(
        artifact_id=uuid4(),
        version=9,
        state=MESIState.SHARED,
        now_tick=1,
    )

    assert strategy.requires_refresh(entry, now_tick=2) is False
    entry = strategy.on_read(entry, now_tick=2)
    assert entry.access_count == 1
    assert strategy.requires_refresh(entry, now_tick=3) is False
    entry = strategy.on_read(entry, now_tick=3)
    assert entry.access_count == 2
    assert strategy.requires_refresh(entry, now_tick=4) is True
    assert strategy.staleness_bound() == 2


def test_selector_uses_role_override_or_default() -> None:
    selected = select_strategy_name_for_role(
        "reviewer",
        role_overrides={"reviewer": "lease"},
        default="lazy",
    )
    assert selected == "lease"
    assert select_strategy_name_for_role("planner", default="lazy") == "lazy"


def test_build_strategy_constructs_expected_types() -> None:
    assert isinstance(build_strategy("broadcast"), BroadcastStrategy)
    assert isinstance(build_strategy("eager"), EagerStrategy)
    assert isinstance(build_strategy("lazy"), LazyStrategy)
    lease = build_strategy("lease", lease_ttl_ticks=9)
    assert isinstance(lease, LeaseStrategy)
    assert lease.ttl_ticks == 9
    access = build_strategy("access-count", access_count_max_accesses=7)
    assert isinstance(access, AccessCountStrategy)
    assert access.max_accesses == 7


def test_build_strategy_rejects_unknown_name() -> None:
    with pytest.raises(ValueError):
        build_strategy("unknown")


def test_broadcast_strategy_broadcasts_every_tick() -> None:
    strategy = BroadcastStrategy()
    assert strategy.broadcasts_every_tick() is True
    assert strategy.broadcasts_content_on_commit() is True
    assert strategy.staleness_bound() == 0


def test_broadcast_baseline_token_cost() -> None:
    scenario = load_scenario("benchmarks/scenarios/planning_canonical.yaml")
    metrics = SimulationEngine(scenario, strategy_name="broadcast", seed=20260305).run()

    n = int(scenario["simulation"]["num_agents"])
    s = int(scenario["simulation"]["duration_ticks"])
    total_artifact_tokens = sum(int(artifact["size_tokens"]) for artifact in scenario["artifacts"])
    expected = n * s * total_artifact_tokens
    ratio_delta = abs(metrics.tokens_broadcast - expected) / float(expected)
    assert ratio_delta < 0.05
