# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Behavior tests for synchronization strategy implementations."""

from __future__ import annotations

from uuid import uuid4

import pytest

from ccs.core.states import MESIState
from ccs.simulation.engine import SimulationEngine
from ccs.simulation.scenarios import load_scenario
from ccs.strategies.access_count import AccessCountStrategy
from ccs.strategies.base import SyncStrategy
from ccs.strategies.blind_cache import BlindCacheStrategy
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


def test_blind_cache_never_refreshes_even_when_invalid() -> None:
    strategy = BlindCacheStrategy()
    invalid_entry = strategy.on_fetch(
        artifact_id=uuid4(),
        version=4,
        state=MESIState.INVALID,
        now_tick=2,
    )

    # The point of the cost floor: refuse to refetch even a stale/invalid entry.
    assert strategy.requires_refresh(invalid_entry, now_tick=99) is False
    assert strategy.broadcasts_content_on_commit() is False
    assert strategy.staleness_bound() is None
    assert strategy.on_read(invalid_entry, now_tick=3).access_count == 1


def test_build_strategy_constructs_blind_cache() -> None:
    assert isinstance(build_strategy("blind"), BlindCacheStrategy)
    with pytest.raises(ValueError):
        build_strategy("nope")


def test_blind_cache_fetches_once_per_agent_artifact_pair() -> None:
    scenario = load_scenario("benchmarks/scenarios/planning_canonical.yaml")
    blind = SimulationEngine(scenario, strategy_name="blind", seed=20260305).run()
    lazy = SimulationEngine(scenario, strategy_name="lazy", seed=20260305).run()

    num_agents = int(scenario["simulation"]["num_agents"])
    artifact_count = len(scenario["artifacts"])
    # Cost floor: each (agent, artifact) pair fills its cache at most once, then
    # every later access is a hit. Never refetching on INVALID keeps blind
    # strictly below a strategy (lazy) that does refetch stale entries.
    assert blind.fetch_actions <= num_agents * artifact_count
    assert blind.cache_misses == blind.fetch_actions
    assert blind.fetch_actions < lazy.fetch_actions


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


# --- OCC retry policy seam (Unit 4) -----------------------------------------
#
# These cover ONLY the policy surface on ``SyncStrategy``: the retry knobs are
# concrete-with-default (not abstract, which would break all six concretes), and
# nothing here adds an actor hook or an auto-escalation path. The "loop obeys the
# knob" integration test belongs to Unit 5 (AgentRuntime), where the loop exists.

# All concrete strategies a caller can construct, including the benchmark-only
# blind cost-floor (reachable via ``build_strategy("blind")``).
_ALL_CONCRETE_STRATEGIES = (
    LazyStrategy(),
    LeaseStrategy(),
    EagerStrategy(),
    AccessCountStrategy(),
    BroadcastStrategy(),
    BlindCacheStrategy(),
)

# Every name ``build_strategy`` accepts (mirrors the selector's branches).
_REGISTERED_STRATEGY_NAMES = (
    "blind",
    "broadcast",
    "eager",
    "lazy",
    "lease",
    "access_count",
    "access-count",
    "accesscount",
)


def test_default_strategy_returns_sane_cas_retry_policy() -> None:
    # Use the production default strategy as the representative for the base
    # concrete-with-default policy.
    strategy = LazyStrategy()

    assert strategy.max_cas_retries() > 0
    # Backoff is non-negative and deterministic for the full attempt window.
    for attempt in range(strategy.max_cas_retries() + 1):
        assert strategy.cas_backoff_ticks(attempt) >= 0
        assert strategy.cas_backoff_ticks(attempt) == strategy.cas_backoff_ticks(attempt)


def test_cas_backoff_first_retry_is_immediate_and_monotonic_nondecreasing() -> None:
    strategy = LazyStrategy()

    # First retry (attempt 0) is immediate so a lone transient conflict re-reads
    # with no delay; a negative attempt clamps to no delay rather than erroring.
    assert strategy.cas_backoff_ticks(0) == 0
    assert strategy.cas_backoff_ticks(-1) == 0

    # Schedule never decreases across the retry window (bounds livelock).
    backoffs = [strategy.cas_backoff_ticks(attempt) for attempt in range(8)]
    assert backoffs == sorted(backoffs)


def test_no_auto_escalation_surface_on_any_strategy() -> None:
    # D5: a CAS conflict must NEVER auto-escalate to EXCLUSIVE. Assert no method
    # or attribute hinting at escalation exists on the ABC or any concrete.
    forbidden_substrings = ("escalat", "exclusive", "auto_acquire", "force_acquire")
    candidates = (SyncStrategy, *(type(s) for s in _ALL_CONCRETE_STRATEGIES))
    for strategy_type in candidates:
        for member in dir(strategy_type):
            lowered = member.lower()
            assert not any(token in lowered for token in forbidden_substrings), (
                f"{strategy_type.__name__} exposes possible auto-escalation member '{member}'"
            )


def test_all_concrete_strategies_construct_and_inherit_default_cas_policy() -> None:
    # No abstractmethod break: every concrete still instantiates, and each
    # inherits the base concrete-with-default retry policy.
    assert len(_ALL_CONCRETE_STRATEGIES) == 6
    for strategy in _ALL_CONCRETE_STRATEGIES:
        assert isinstance(strategy, SyncStrategy)
        # Inherited (not overridden) -> identical to the base default.
        assert strategy.max_cas_retries() == SyncStrategy.max_cas_retries(strategy)
        assert strategy.max_cas_retries() > 0
        assert strategy.cas_backoff_ticks(0) == 0
        assert strategy.cas_backoff_ticks(1) >= 0


@pytest.mark.parametrize("name", _REGISTERED_STRATEGY_NAMES)
def test_build_strategy_still_builds_each_registered_name_with_cas_policy(name: str) -> None:
    strategy = build_strategy(name)
    assert isinstance(strategy, SyncStrategy)
    # The new policy surface is reachable on every selector-built strategy.
    assert strategy.max_cas_retries() > 0
    assert strategy.cas_backoff_ticks(0) >= 0
