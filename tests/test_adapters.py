# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""Tests for framework adapter integration helpers."""

from __future__ import annotations

import warnings
from uuid import NAMESPACE_URL, uuid5

import pytest

from ccs.adapters.autogen import AutoGenAdapter
from ccs.adapters.base import CoherenceAdapterCore
from ccs.adapters.crewai import CrewAIAdapter
from ccs.adapters.langgraph import LangGraphAdapter
from ccs.coordinator.service import CrashRecoveryConfig
from ccs.core.states import MESIState


def test_langgraph_adapter_propagates_invalidation_then_refresh() -> None:
    adapter = LangGraphAdapter(strategy_name="lazy")
    artifact = adapter.register_artifact(name="plan.md", content="v1", size_tokens=128)
    adapter.register_agent("planner")
    adapter.register_agent("researcher")

    planner_context = adapter.before_node(agent_name="planner", artifact_ids=[artifact.id], now_tick=1)
    assert planner_context[artifact.id]["version"] == 1
    assert planner_context[artifact.id]["content"] == "v1"
    adapter.before_node(agent_name="researcher", artifact_ids=[artifact.id], now_tick=1)

    versions = adapter.commit_outputs(
        agent_name="planner",
        writes={artifact.id: "v2"},
        now_tick=2,
    )
    assert versions[artifact.id] == 2

    researcher_entry = adapter.core.runtime("researcher").cache.get(artifact.id)
    assert researcher_entry is not None
    assert researcher_entry.state == MESIState.INVALID

    refreshed = adapter.before_node(agent_name="researcher", artifact_ids=[artifact.id], now_tick=3)
    assert refreshed[artifact.id]["version"] == 2
    assert refreshed[artifact.id]["content"] == "v2"


def test_crewai_adapter_task_hooks_roundtrip_content() -> None:
    adapter = CrewAIAdapter(strategy_name="lazy")
    artifact = adapter.register_artifact(name="analysis.json", content='{"summary":"v1"}')
    adapter.register_agent("author")
    adapter.register_agent("reviewer")

    initial = adapter.prepare_task_context(agent_name="author", artifact_ids=[artifact.id], now_tick=1)
    assert initial[artifact.id] == '{"summary":"v1"}'

    new_version = adapter.commit_task_artifact(
        agent_name="author",
        artifact_id=artifact.id,
        content='{"summary":"v2"}',
        now_tick=2,
    )
    assert new_version == 2

    reviewer_context = adapter.prepare_task_context(
        agent_name="reviewer",
        artifact_ids=[artifact.id],
        now_tick=3,
    )
    assert reviewer_context[artifact.id] == '{"summary":"v2"}'


def test_autogen_adapter_turn_hooks_roundtrip_content() -> None:
    adapter = AutoGenAdapter(strategy_name="lazy")
    artifact = adapter.register_artifact(name="facts.md", content="v1")
    adapter.register_agent("assistant")
    adapter.register_agent("planner")

    pre_turn = adapter.pre_turn_context(agent_name="assistant", artifact_ids=[artifact.id], now_tick=1)
    assert pre_turn[artifact.id] == "v1"

    versions = adapter.post_turn_commit(
        agent_name="assistant",
        updates={artifact.id: "v2"},
        now_tick=2,
    )
    assert versions[artifact.id] == 2

    planner_view = adapter.pre_turn_context(agent_name="planner", artifact_ids=[artifact.id], now_tick=3)
    assert planner_view[artifact.id] == "v2"


def test_coherence_adapter_core_default_strategy_unchanged() -> None:
    core = CoherenceAdapterCore(strategy_name="lazy")
    core.register_agent("planner")
    artifact = core.register_artifact(name="plan.md", content="v1")
    resp = core.read(agent_name="planner", artifact_id=artifact.id, now_tick=1)
    assert resp.content == "v1"


def test_coherence_adapter_core_explicit_strategy_param_still_works() -> None:
    core = CoherenceAdapterCore(strategy_name="lease", lease_ttl_ticks=50)
    core.register_agent("agent")
    artifact = core.register_artifact(name="a.md", content="x")
    resp = core.read(agent_name="agent", artifact_id=artifact.id, now_tick=1)
    assert resp.content == "x"


def test_coherence_adapter_core_unknown_strategy_kwarg_does_not_raise() -> None:
    # Unknown kwargs are absorbed and silently ignored — forward-compat escape hatch.
    core = CoherenceAdapterCore(strategy_name="lazy", some_future_kwarg=99)
    assert core is not None


def test_agent_id_for_returns_deterministic_uuid() -> None:
    core = CoherenceAdapterCore(strategy_name="lazy")
    core.register_agent("planner")
    expected = uuid5(NAMESPACE_URL, "ccs-agent:planner")
    assert core.agent_id_for("planner") == expected


def test_agent_id_for_unknown_name_raises_key_error() -> None:
    core = CoherenceAdapterCore(strategy_name="lazy")
    try:
        core.agent_id_for("nobody")
        assert False, "expected KeyError"
    except KeyError:
        pass


# --- Public registry snapshot accessors (Gated #17) ---


def test_agent_names_snapshot_empty_when_no_registrations() -> None:
    core = CoherenceAdapterCore(strategy_name="lazy")
    assert core.agent_names_snapshot() == {}


def test_agent_names_snapshot_reflects_registrations() -> None:
    core = CoherenceAdapterCore(strategy_name="lazy")
    core.register_agent("planner")
    core.register_agent("reviewer")
    snapshot = core.agent_names_snapshot()
    assert set(snapshot.values()) == {"planner", "reviewer"}
    assert all(isinstance(k, type(core.agent_id_for("planner"))) for k in snapshot)


def test_agent_names_snapshot_mutation_does_not_affect_core() -> None:
    # Snapshot is a fresh dict — mutating it must not corrupt the
    # coordinator's internal registry.
    core = CoherenceAdapterCore(strategy_name="lazy")
    core.register_agent("planner")
    snapshot = core.agent_names_snapshot()
    snapshot.clear()
    snapshot["bogus"] = "injected"  # type: ignore[index]
    fresh = core.agent_names_snapshot()
    assert "injected" not in fresh.values()
    assert "planner" in fresh.values()


def test_artifact_names_snapshot_empty_when_no_artifacts() -> None:
    core = CoherenceAdapterCore(strategy_name="lazy")
    assert core.artifact_names_snapshot() == {}


def test_artifact_names_snapshot_reflects_registrations() -> None:
    core = CoherenceAdapterCore(strategy_name="lazy")
    a = core.register_artifact(name="outline.md", content="x")
    b = core.register_artifact(name="draft.md", content="y")
    snapshot = core.artifact_names_snapshot()
    assert snapshot[a.id] == "outline.md"
    assert snapshot[b.id] == "draft.md"


def test_artifact_names_snapshot_mutation_does_not_affect_core() -> None:
    core = CoherenceAdapterCore(strategy_name="lazy")
    core.register_artifact(name="outline.md", content="x")
    snapshot = core.artifact_names_snapshot()
    snapshot.clear()
    fresh = core.artifact_names_snapshot()
    assert "outline.md" in fresh.values()


# --- Crash recovery adapter tests (Unit 2) ---


def _core_enabled(**overrides: object) -> CoherenceAdapterCore:
    defaults = {
        "strategy_name": "lazy",
        "crash_recovery": CrashRecoveryConfig(enabled=True, heartbeat_timeout_ticks=10, max_hold_ticks=1000),
    }
    defaults.update(overrides)
    return CoherenceAdapterCore(**defaults)  # type: ignore[arg-type]


def test_piggyback_heartbeat_on_read() -> None:
    core = _core_enabled()
    core.register_agent("A", now_tick=0)
    artifact = core.register_artifact(name="x.md", content="v1")

    core.read(agent_name="A", artifact_id=artifact.id, now_tick=10)

    assert core.registry.last_heartbeat_tick(core.agent_id_for("A")) == 10


def test_piggyback_heartbeat_on_write() -> None:
    core = _core_enabled()
    core.register_agent("A", now_tick=0)
    artifact = core.register_artifact(name="x.md", content="v1")
    core.read(agent_name="A", artifact_id=artifact.id, now_tick=1)

    core.write(agent_name="A", artifact_id=artifact.id, content="v2", now_tick=20)

    assert core.registry.last_heartbeat_tick(core.agent_id_for("A")) == 20


def test_explicit_heartbeat_updates_tick() -> None:
    core = _core_enabled()
    core.register_agent("A", now_tick=0)

    core.heartbeat(agent_name="A", now_tick=42)

    assert core.registry.last_heartbeat_tick(core.agent_id_for("A")) == 42


def test_explicit_heartbeat_out_of_order_uses_max() -> None:
    core = _core_enabled()
    core.register_agent("A", now_tick=0)

    core.heartbeat(agent_name="A", now_tick=50)
    core.heartbeat(agent_name="A", now_tick=30)

    assert core.registry.last_heartbeat_tick(core.agent_id_for("A")) == 50


def test_recover_invalidates_cache_and_heartbeats() -> None:
    core = _core_enabled()
    core.register_agent("A", now_tick=0)
    artifact = core.register_artifact(name="x.md", content="v1")
    core.read(agent_name="A", artifact_id=artifact.id, now_tick=1)

    core.recover(agent_name="A", now_tick=200)

    entry = core.runtime("A").cache.get(artifact.id)
    assert entry is not None
    assert entry.state == MESIState.INVALID
    assert core.registry.last_heartbeat_tick(core.agent_id_for("A")) == 200


def test_recover_invalidates_cache_before_heartbeat() -> None:
    """Ordering matters: cache must be invalidated before heartbeat re-seeds liveness."""
    core = _core_enabled()
    core.register_agent("A", now_tick=0)
    artifact = core.register_artifact(name="x.md", content="v1")
    core.read(agent_name="A", artifact_id=artifact.id, now_tick=1)

    call_order: list[str] = []
    orig_invalidate = core.runtime("A").invalidate_all_cache
    orig_heartbeat = core.coordinator.record_heartbeat

    def tracked_invalidate(**kwargs: object) -> None:
        call_order.append("invalidate")
        orig_invalidate(**kwargs)

    def tracked_heartbeat(**kwargs: object) -> None:
        call_order.append("heartbeat")
        orig_heartbeat(**kwargs)

    core.runtime("A").invalidate_all_cache = tracked_invalidate  # type: ignore[assignment]
    core.coordinator.record_heartbeat = tracked_heartbeat  # type: ignore[assignment]

    core.recover(agent_name="A", now_tick=200)

    assert call_order == ["invalidate", "heartbeat"]


def test_flag_off_write_does_not_heartbeat() -> None:
    # Explicit enabled=False: post-v0.9.0 bare construction enables the sweep,
    # so the "flag off" contract requires opting out explicitly.
    core = CoherenceAdapterCore(
        strategy_name="lazy", crash_recovery=CrashRecoveryConfig(enabled=False)
    )
    core.register_agent("A")
    artifact = core.register_artifact(name="x.md", content="v1")
    core.read(agent_name="A", artifact_id=artifact.id, now_tick=1)

    core.write(agent_name="A", artifact_id=artifact.id, content="v2", now_tick=10)

    assert core.registry.last_heartbeat_tick(core.agent_id_for("A")) is None


def test_recover_anti_trap_after_recovery_not_immediately_reclaimed() -> None:
    core = _core_enabled(crash_recovery=CrashRecoveryConfig(
        enabled=True, heartbeat_timeout_ticks=10, max_hold_ticks=1000,
    ))
    core.register_agent("A", now_tick=0)
    artifact = core.register_artifact(name="x.md", content="v1")

    core.recover(agent_name="A", now_tick=200)
    core.read(agent_name="A", artifact_id=artifact.id, now_tick=201)

    reclaimed = core.coordinator.enforce_stable_grant_timeouts(
        current_tick=205, heartbeat_timeout_ticks=10, max_hold_ticks=1000,
    )
    assert reclaimed == 0


def test_recover_flag_off_invalidates_cache_no_heartbeat() -> None:
    core = CoherenceAdapterCore(
        strategy_name="lazy", crash_recovery=CrashRecoveryConfig(enabled=False)
    )
    core.register_agent("A")
    artifact = core.register_artifact(name="x.md", content="v1")
    core.read(agent_name="A", artifact_id=artifact.id, now_tick=1)

    core.recover(agent_name="A", now_tick=200)

    entry = core.runtime("A").cache.get(artifact.id)
    assert entry.state == MESIState.INVALID
    assert core.registry.last_heartbeat_tick(core.agent_id_for("A")) is None


def test_register_agent_seeds_heartbeat_when_enabled() -> None:
    core = _core_enabled()
    core.register_agent("A", now_tick=500)

    assert core.registry.last_heartbeat_tick(core.agent_id_for("A")) == 500


def test_register_agent_no_seed_when_disabled() -> None:
    core = CoherenceAdapterCore(
        strategy_name="lazy", crash_recovery=CrashRecoveryConfig(enabled=False)
    )
    core.register_agent("A")

    assert core.registry.last_heartbeat_tick(core.agent_id_for("A")) is None


def test_failfast_lease_ttl_equal_max_hold_raises() -> None:
    with pytest.raises(ValueError, match="composition violation"):
        CoherenceAdapterCore(
            strategy_name="lease",
            lease_ttl_ticks=300,
            crash_recovery=CrashRecoveryConfig(enabled=True, max_hold_ticks=300),
        )


def test_failfast_lease_ttl_above_max_hold_raises() -> None:
    with pytest.raises(ValueError, match="composition violation"):
        CoherenceAdapterCore(
            strategy_name="lease",
            lease_ttl_ticks=500,
            crash_recovery=CrashRecoveryConfig(enabled=True, max_hold_ticks=300),
        )


def test_failfast_lease_ttl_below_max_hold_accepts() -> None:
    core = CoherenceAdapterCore(
        strategy_name="lease",
        lease_ttl_ticks=200,
        crash_recovery=CrashRecoveryConfig(enabled=True, max_hold_ticks=300),
    )
    assert core is not None


def test_failfast_non_lease_strategy_no_warning_for_builtin() -> None:
    # Built-in non-lease strategies (lazy, eager, access_count) have no ttl_ticks
    # and should NOT emit a warning (silent-accept path).
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        core = CoherenceAdapterCore(
            strategy_name="lazy",
            crash_recovery=CrashRecoveryConfig(enabled=True, max_hold_ticks=300),
        )
    assert core is not None


def test_framework_adapter_passthrough_langgraph_raises() -> None:
    with pytest.raises(ValueError, match="composition violation"):
        LangGraphAdapter(
            strategy_name="lease",
            lease_ttl_ticks=300,
            crash_recovery=CrashRecoveryConfig(enabled=True, max_hold_ticks=300),
        )


def test_framework_adapter_passthrough_crewai_raises() -> None:
    with pytest.raises(ValueError, match="composition violation"):
        CrewAIAdapter(
            strategy_name="lease",
            lease_ttl_ticks=300,
            crash_recovery=CrashRecoveryConfig(enabled=True, max_hold_ticks=300),
        )


def test_framework_adapter_passthrough_autogen_raises() -> None:
    with pytest.raises(ValueError, match="composition violation"):
        AutoGenAdapter(
            strategy_name="lease",
            lease_ttl_ticks=300,
            crash_recovery=CrashRecoveryConfig(enabled=True, max_hold_ticks=300),
        )


def test_framework_adapter_external_core_ignores_crash_recovery_kwarg() -> None:
    # External core built with enabled=False so it is distinct from the kwarg's
    # enabled=True; the assertion below proves the adapter uses the external
    # core's config and ignores its own crash_recovery= kwarg.
    external_core = CoherenceAdapterCore(
        strategy_name="lazy", crash_recovery=CrashRecoveryConfig(enabled=False)
    )
    adapter = LangGraphAdapter(
        core=external_core,
        crash_recovery=CrashRecoveryConfig(enabled=True, max_hold_ticks=300),
    )
    assert adapter.core is external_core
    assert adapter.core._crash_recovery.enabled is False


def test_flag_off_read_does_not_heartbeat() -> None:
    core = CoherenceAdapterCore(
        strategy_name="lazy", crash_recovery=CrashRecoveryConfig(enabled=False)
    )
    core.register_agent("A")
    artifact = core.register_artifact(name="x.md", content="v1")

    core.read(agent_name="A", artifact_id=artifact.id, now_tick=10)

    assert core.registry.last_heartbeat_tick(core.agent_id_for("A")) is None


def test_flag_off_heartbeat_is_noop() -> None:
    core = CoherenceAdapterCore(
        strategy_name="lazy", crash_recovery=CrashRecoveryConfig(enabled=False)
    )
    core.register_agent("A")

    core.heartbeat(agent_name="A", now_tick=42)

    assert core.registry.last_heartbeat_tick(core.agent_id_for("A")) is None


# ---- v0.9.0 C-flip — bare adapter adopts enabled-by-default -----------------
#
# See docs/plans/2026-05-28-001-feat-c-flip-crash-recovery-default-on-plan.md
# Units 5 + 6. Post-flip, a bare CoherenceAdapterCore() / CCSStore() (no
# crash_recovery= argument) adopts the enabled-by-default crash recovery. The
# v0.8.3 DeprecationWarning is gone; at most one v0.9.0 transitional
# RuntimeWarning may surface (suppressed suite-wide by the conftest neutralizer
# except in the dedicated tests/test_coordinator.py assertions).


def test_bare_coherence_adapter_core_adopts_enabled_default() -> None:
    """v0.9.0: bare CoherenceAdapterCore() emits no DeprecationWarning and adopts
    the enabled-by-default crash recovery; at most one transitional
    RuntimeWarning may surface."""
    from ccs.coordinator import service as _service_mod

    # Reset the once-per-process flag so we deterministically observe the
    # transitional warning here (the conftest neutralizer otherwise pre-sets it).
    _service_mod._V090_FIRST_USE_WARNED = False
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        core = CoherenceAdapterCore(strategy_name="lazy")
    deprecation_warnings = [
        w for w in caught if issubclass(w.category, DeprecationWarning)
    ]
    assert deprecation_warnings == [], (
        f"bare CoherenceAdapterCore construction must not emit "
        f"DeprecationWarning, got: {[str(w.message) for w in deprecation_warnings]}"
    )
    runtime_warnings = [
        w for w in caught if issubclass(w.category, RuntimeWarning)
    ]
    assert len(runtime_warnings) <= 1
    # The flip: bare construction now enables crash recovery.
    assert core._crash_recovery.enabled is True


def test_explicit_crash_recovery_kwarg_emits_no_deprecation_warning() -> None:
    """Passing an explicit CrashRecoveryConfig must not surface a DeprecationWarning."""
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        core = CoherenceAdapterCore(
            strategy_name="lazy",
            crash_recovery=CrashRecoveryConfig(enabled=True),
        )
    deprecation_warnings = [
        w for w in caught if issubclass(w.category, DeprecationWarning)
    ]
    assert deprecation_warnings == []
    assert core._crash_recovery.enabled is True
