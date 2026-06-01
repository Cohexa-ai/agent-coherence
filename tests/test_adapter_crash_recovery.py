# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""End-to-end integration tests: adapter heartbeat + reclamation + recovery.

Each test drives the coordinator directly through an adapter — no SimulationEngine.
Reclamation is triggered by manually advancing ticks and calling
enforce_stable_grant_timeouts between adapter calls.
"""

from __future__ import annotations

import pytest

from ccs.adapters.autogen import AutoGenAdapter
from ccs.adapters.crewai import CrewAIAdapter
from ccs.adapters.langgraph import LangGraphAdapter
from ccs.coordinator.service import CrashRecoveryConfig
from ccs.core.exceptions import CoherenceError
from ccs.core.states import MESIState

CR_CFG = CrashRecoveryConfig(enabled=True, heartbeat_timeout_ticks=10, max_hold_ticks=1000)


def _langgraph(**overrides):
    defaults = dict(strategy_name="lazy", crash_recovery=CR_CFG)
    defaults.update(overrides)
    return LangGraphAdapter(**defaults)


def _reclaim(adapter, *, current_tick: int, cfg: CrashRecoveryConfig = CR_CFG) -> int:
    return adapter.core.coordinator.enforce_stable_grant_timeouts(
        current_tick=current_tick,
        heartbeat_timeout_ticks=cfg.heartbeat_timeout_ticks,
        max_hold_ticks=cfg.max_hold_ticks,
    )


# ---------------------------------------------------------------------------
# OOM-kill shape (jingchang0623): agent acquires grant, goes silent, gets reclaimed
# ---------------------------------------------------------------------------


class TestOOMKill:
    """Agent A acquires EXCLUSIVE, stops heartbeating, gets reclaimed by sweep."""

    def test_langgraph_oom_kill(self) -> None:
        adapter = _langgraph()
        artifact = adapter.register_artifact(name="plan.md", content="v1")
        adapter.register_agent("A")
        adapter.register_agent("B")

        adapter.before_node(agent_name="A", artifact_ids=[artifact.id], now_tick=1)
        adapter.commit_outputs(agent_name="A", writes={artifact.id: "v2"}, now_tick=2)

        # A goes silent — no more adapter calls. Advance past heartbeat timeout.
        reclaimed = _reclaim(adapter, current_tick=13)
        assert reclaimed == 1

        # A's grant is now INVALID at coordinator
        state = adapter.core.registry.get_agent_state(artifact.id, adapter.core.agent_id_for("A"))
        assert state == MESIState.INVALID

        # B can now acquire and commit
        adapter.before_node(agent_name="B", artifact_ids=[artifact.id], now_tick=14)
        versions = adapter.commit_outputs(agent_name="B", writes={artifact.id: "v3"}, now_tick=15)
        assert versions[artifact.id] == 3

    def test_crewai_oom_kill(self) -> None:
        adapter = CrewAIAdapter(strategy_name="lazy", crash_recovery=CR_CFG)
        artifact = adapter.register_artifact(name="analysis.md", content="v1")
        adapter.register_agent("A")
        adapter.register_agent("B")

        adapter.prepare_task_context(agent_name="A", artifact_ids=[artifact.id], now_tick=1)
        adapter.commit_task_artifact(agent_name="A", artifact_id=artifact.id, content="v2", now_tick=2)

        reclaimed = adapter.core.coordinator.enforce_stable_grant_timeouts(
            current_tick=13, heartbeat_timeout_ticks=10, max_hold_ticks=1000,
        )
        assert reclaimed == 1

        adapter.prepare_task_context(agent_name="B", artifact_ids=[artifact.id], now_tick=14)
        version = adapter.commit_task_artifact(agent_name="B", artifact_id=artifact.id, content="v3", now_tick=15)
        assert version == 3

    def test_autogen_oom_kill(self) -> None:
        adapter = AutoGenAdapter(strategy_name="lazy", crash_recovery=CR_CFG)
        artifact = adapter.register_artifact(name="facts.md", content="v1")
        adapter.register_agent("A")
        adapter.register_agent("B")

        adapter.pre_turn_context(agent_name="A", artifact_ids=[artifact.id], now_tick=1)
        adapter.post_turn_commit(agent_name="A", updates={artifact.id: "v2"}, now_tick=2)

        reclaimed = adapter.core.coordinator.enforce_stable_grant_timeouts(
            current_tick=13, heartbeat_timeout_ticks=10, max_hold_ticks=1000,
        )
        assert reclaimed == 1

        adapter.pre_turn_context(agent_name="B", artifact_ids=[artifact.id], now_tick=14)
        versions = adapter.post_turn_commit(agent_name="B", updates={artifact.id: "v3"}, now_tick=15)
        assert versions[artifact.id] == 3


# ---------------------------------------------------------------------------
# Checkpoint-restore shape (jessieibarra): agent crashes, recovers, stale commit fails
# ---------------------------------------------------------------------------


class TestCheckpointRestore:
    """Agent A acquires MODIFIED, is killed, recovers, stale commit raises."""

    def test_stale_commit_after_reclamation_raises_with_context(self) -> None:
        adapter = _langgraph()
        artifact = adapter.register_artifact(name="plan.md", content="v1")
        adapter.register_agent("A")

        adapter.before_node(agent_name="A", artifact_ids=[artifact.id], now_tick=1)
        adapter.commit_outputs(agent_name="A", writes={artifact.id: "v2"}, now_tick=2)

        # A is "killed" — sweep reclaims at coordinator level
        _reclaim(adapter, current_tick=13)

        # Simulate stale process trying to commit directly on coordinator
        # (bypassing the runtime's fetch-before-write to exercise the reclamation error).
        agent_id = adapter.core.agent_id_for("A")
        with pytest.raises(CoherenceError, match="reclaimed_by"):
            adapter.core.coordinator.commit(
                agent_id=agent_id,
                artifact_id=artifact.id,
                content="v3",
                issued_at_tick=21,
            )

    def test_recover_then_fresh_acquire_succeeds(self) -> None:
        adapter = _langgraph()
        artifact = adapter.register_artifact(name="plan.md", content="v1")
        adapter.register_agent("A")

        adapter.before_node(agent_name="A", artifact_ids=[artifact.id], now_tick=1)
        adapter.commit_outputs(agent_name="A", writes={artifact.id: "v2"}, now_tick=2)

        _reclaim(adapter, current_tick=13)

        # A recovers and re-acquires through normal adapter flow
        adapter.core.recover(agent_name="A", now_tick=20)
        adapter.before_node(agent_name="A", artifact_ids=[artifact.id], now_tick=21)
        versions = adapter.commit_outputs(agent_name="A", writes={artifact.id: "v3"}, now_tick=22)
        assert versions[artifact.id] == 3


# ---------------------------------------------------------------------------
# Live-but-stuck shape: agent heartbeats but never commits, max_hold reclaims
# ---------------------------------------------------------------------------


class TestLiveButStuck:
    """Agent A heartbeats via before_node but never commits — max_hold reclaims."""

    def test_max_hold_reclaim(self) -> None:
        cfg = CrashRecoveryConfig(enabled=True, heartbeat_timeout_ticks=10, max_hold_ticks=50)
        adapter = _langgraph(crash_recovery=cfg)
        artifact = adapter.register_artifact(name="plan.md", content="v1")
        adapter.register_agent("A")

        adapter.before_node(agent_name="A", artifact_ids=[artifact.id], now_tick=1)
        adapter.commit_outputs(agent_name="A", writes={artifact.id: "v2"}, now_tick=2)

        # Keep heartbeating (no staleness) but never release the grant
        for t in range(10, 50, 5):
            adapter.before_node(agent_name="A", artifact_ids=[artifact.id], now_tick=t)

        # Sweep at tick 52 — grant held since tick 2, which is 50 ticks
        reclaimed = adapter.core.coordinator.enforce_stable_grant_timeouts(
            current_tick=52, heartbeat_timeout_ticks=10, max_hold_ticks=50,
        )
        assert reclaimed == 1

        state = adapter.core.registry.get_agent_state(artifact.id, adapter.core.agent_id_for("A"))
        assert state == MESIState.INVALID


# ---------------------------------------------------------------------------
# Long-compute false-reclaim: no heartbeat bridge → reclaimed
# ---------------------------------------------------------------------------


class TestLongComputeFalseReclaim:
    """Agent acquires EXCLUSIVE, does no adapter calls, gets false-reclaimed."""

    def test_no_heartbeat_bridge_causes_reclaim(self) -> None:
        adapter = _langgraph()
        artifact = adapter.register_artifact(name="plan.md", content="v1")
        adapter.register_agent("A")

        adapter.before_node(agent_name="A", artifact_ids=[artifact.id], now_tick=1)
        adapter.commit_outputs(agent_name="A", writes={artifact.id: "v2"}, now_tick=2)

        # No adapter calls for heartbeat_timeout_ticks + 1 ticks
        reclaimed = _reclaim(adapter, current_tick=13)
        assert reclaimed == 1

    def test_explicit_heartbeat_prevents_reclaim(self) -> None:
        adapter = _langgraph()
        artifact = adapter.register_artifact(name="plan.md", content="v1")
        adapter.register_agent("A")

        adapter.before_node(agent_name="A", artifact_ids=[artifact.id], now_tick=1)
        adapter.commit_outputs(agent_name="A", writes={artifact.id: "v2"}, now_tick=2)

        # Bridge the compute window with explicit heartbeat
        adapter.core.heartbeat(agent_name="A", now_tick=7)

        reclaimed = _reclaim(adapter, current_tick=13)
        assert reclaimed == 0


# ---------------------------------------------------------------------------
# Recovery anti-trap: recover() auto-heartbeats so agent isn't re-reclaimed
# ---------------------------------------------------------------------------


class TestRecoveryAntiTrap:
    """After recover(now_tick=200), agent isn't immediately reclaim-eligible."""

    def test_recovered_agent_not_re_reclaimed(self) -> None:
        adapter = _langgraph()
        a1 = adapter.register_artifact(name="plan.md", content="v1")
        a2 = adapter.register_artifact(name="other.md", content="v1")
        adapter.register_agent("A")

        # A acquires and is reclaimed
        adapter.before_node(agent_name="A", artifact_ids=[a1.id], now_tick=1)
        adapter.commit_outputs(agent_name="A", writes={a1.id: "v2"}, now_tick=2)
        _reclaim(adapter, current_tick=100)

        # A restarts and recovers at tick 200
        adapter.core.recover(agent_name="A", now_tick=200)

        # A acquires EXCLUSIVE on a fresh artifact at tick 201
        adapter.before_node(agent_name="A", artifact_ids=[a2.id], now_tick=201)
        adapter.commit_outputs(agent_name="A", writes={a2.id: "new"}, now_tick=202)

        # Sweep at tick 205 with timeout=10 — should NOT reclaim (last hb was 202 via piggyback)
        reclaimed = _reclaim(adapter, current_tick=205)
        assert reclaimed == 0


# ---------------------------------------------------------------------------
# CCSStore parity
# ---------------------------------------------------------------------------


class TestCCSStoreParity:
    """CCSStore heartbeat/recover works the same as framework adapters."""

    def test_ccsstore_oom_kill(self) -> None:
        pytest.importorskip("langgraph.store.base")
        from langgraph.store.base import PutOp

        from ccs.adapters.ccsstore import CCSStore

        store = CCSStore(
            strategy="lazy",
            crash_recovery=CR_CFG,
        )
        store.batch([PutOp(namespace=("A", "shared"), key="plan", value={"v": 1})])
        store.batch([PutOp(namespace=("A", "shared"), key="plan", value={"v": 2})])

        # A goes silent
        reclaimed = store.core.coordinator.enforce_stable_grant_timeouts(
            current_tick=13, heartbeat_timeout_ticks=10, max_hold_ticks=1000,
        )
        assert reclaimed == 1

        # B can now write
        store.batch([PutOp(namespace=("B", "shared"), key="plan", value={"v": 3})])


# ---------------------------------------------------------------------------
# Composition fail-fast at adapter level (R5/R8)
# ---------------------------------------------------------------------------


class TestCompositionFailFast:
    """Framework adapter constructor passthrough reaches core fail-fast."""

    def test_langgraph_raises(self) -> None:
        with pytest.raises(ValueError, match="composition violation"):
            LangGraphAdapter(
                strategy_name="lease",
                lease_ttl_ticks=300,
                crash_recovery=CrashRecoveryConfig(enabled=True, max_hold_ticks=300),
            )
