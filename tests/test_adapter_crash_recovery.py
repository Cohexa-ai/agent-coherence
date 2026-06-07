# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""End-to-end integration tests: adapter heartbeat + reclamation + recovery.

Each test drives the coordinator directly through an adapter — no SimulationEngine.
Reclamation is triggered by manually advancing ticks and calling
enforce_stable_grant_timeouts between adapter calls.
"""

from __future__ import annotations

import asyncio
import logging
import threading
from unittest.mock import Mock
from uuid import uuid4

import pytest

from ccs.adapters.autogen import AutoGenAdapter
from ccs.adapters.base import CoherenceAdapterCore
from ccs.adapters.crewai import CrewAIAdapter
from ccs.adapters.langgraph import LangGraphAdapter
from ccs.adapters.openai_agents import OpenAIAgentsAdapter
from ccs.coordinator.service import CrashRecoveryConfig
from ccs.core.exceptions import CoherenceError
from ccs.core.states import MESIState

CR_CFG = CrashRecoveryConfig(enabled=True, heartbeat_timeout_ticks=10, max_hold_ticks=1000)


class _FakeSession:
    """Minimal in-memory Session (the four async methods) for adapter parity tests."""

    def __init__(self):
        self._items: list = []

    async def get_items(self, limit=None):
        return list(self._items)

    async def add_items(self, items):
        self._items.extend(items)

    async def pop_item(self):
        return self._items.pop() if self._items else None

    async def clear_session(self):
        self._items.clear()


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
# OpenAIAgentsAdapter parity: heartbeat piggyback, reclamation, recover flush
# ---------------------------------------------------------------------------


class TestOpenAIAgentsParity:
    """OpenAIAgentsAdapter heartbeat/recover/reclamation behaves like the others.

    The adapter is SDK-free (CoherenceSession composes a caller-provided Session),
    so these run in the default offline suite with a _FakeSession.
    """

    def test_openai_agents_oom_kill(self) -> None:
        adapter = OpenAIAgentsAdapter(crash_recovery=CR_CFG)
        a = adapter.wrap_session(_FakeSession(), agent_name="A", session_id="s")
        b = adapter.wrap_session(_FakeSession(), agent_name="B", session_id="s")
        artifact_id = adapter._session_artifact_id("s")

        # A acquires a grant (MODIFIED) and piggybacks a heartbeat via the write.
        asyncio.run(a.add_items([{"v": 1}]))

        # A goes silent; the sweep reclaims its stale grant.
        reclaimed = _reclaim(adapter, current_tick=13)
        assert reclaimed == 1
        # A's grant is now INVALID at the coordinator (matches the sibling adapter tests).
        assert adapter.core.registry.get_agent_state(artifact_id, adapter.core.agent_id_for("A")) == MESIState.INVALID

        # B can now write where A's stranded grant would otherwise have blocked it,
        # and B holds the grant (write committed, not silently degraded).
        asyncio.run(b.add_items([{"v": 2}]))
        assert adapter.core.registry.get_agent_state(artifact_id, adapter.core.agent_id_for("B")) == MESIState.MODIFIED

    def test_openai_agents_recover_flushes_cache(self) -> None:
        adapter = OpenAIAgentsAdapter(crash_recovery=CR_CFG)
        a = adapter.wrap_session(_FakeSession(), agent_name="A", session_id="s")
        asyncio.run(a.get_items())  # A caches the session artifact
        artifact_id = adapter._session_artifact_id("s")
        assert adapter.core.runtime("A").cache.get(artifact_id) is not None
        adapter.recover(agent_name="A", now_tick=5)  # post-restart flush
        # recover invalidates A's local view: the next read is a coordinator miss.
        entry = adapter.core.runtime("A").cache.get(artifact_id)
        assert entry is None or entry.state == MESIState.INVALID


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

    def test_openai_agents_raises(self) -> None:
        with pytest.raises(ValueError, match="composition violation"):
            OpenAIAgentsAdapter(
                strategy_name="lease",
                lease_ttl_ticks=300,
                crash_recovery=CrashRecoveryConfig(enabled=True, max_hold_ticks=300),
            )


# ---- v0.9.0 Unit 8: rate-limited _maybe_sweep + structured-log diagnostic ----
#
# See docs/plans/2026-05-28-001-feat-c-flip-crash-recovery-default-on-plan.md
# Unit 8. CR_CFG uses heartbeat_timeout_ticks=10, so the rate-limit gate is
# ``10 // 2 == 5`` ticks. Diagnostic is logged on the ``ccs.adapters.base``
# logger at WARNING with structured ``extra`` fields.

_GATE = CR_CFG.heartbeat_timeout_ticks // 2  # == 5


def _core(**overrides) -> CoherenceAdapterCore:
    defaults = dict(strategy_name="lazy", crash_recovery=CR_CFG)
    defaults.update(overrides)
    return CoherenceAdapterCore(**defaults)


def _reclaiming_enforce(*, current_tick, heartbeat_timeout_ticks, max_hold_ticks, on_reclaim):
    """Mock side_effect simulating the sweep reclaiming exactly one grant."""
    on_reclaim(uuid4(), uuid4(), "reclaim_heartbeat")
    return 1


class TestMaybeSweepRateLimit:
    """KD-4 rate-limit gate + monotonicity defense + disabled short-circuit."""

    def test_first_call_fires_then_gate_skips_within_window(self) -> None:
        core = _core()
        core.coordinator.enforce_stable_grant_timeouts = Mock(return_value=0)
        core._maybe_sweep(now_tick=0)  # first call fires (never-swept sentinel)
        core._maybe_sweep(now_tick=_GATE - 1)  # within window -> skip
        assert core.coordinator.enforce_stable_grant_timeouts.call_count == 1

    def test_fires_again_on_second_eligible_tick(self) -> None:
        core = _core()
        core.coordinator.enforce_stable_grant_timeouts = Mock(return_value=0)
        core._maybe_sweep(now_tick=0)
        core._maybe_sweep(now_tick=_GATE)  # exactly at the gate -> fire
        assert core.coordinator.enforce_stable_grant_timeouts.call_count == 2
        assert core._last_sweep_tick == _GATE

    def test_disabled_short_circuits(self) -> None:
        core = _core(crash_recovery=CrashRecoveryConfig(enabled=False))
        core.coordinator.enforce_stable_grant_timeouts = Mock(return_value=0)
        core._maybe_sweep(now_tick=100)
        core.coordinator.enforce_stable_grant_timeouts.assert_not_called()

    def test_non_monotonic_tick_defense(self) -> None:
        core = _core()
        core.coordinator.enforce_stable_grant_timeouts = Mock(return_value=0)
        core._maybe_sweep(now_tick=100)  # fire -> _last_sweep_tick = 100
        core._maybe_sweep(now_tick=50)  # backward jump (50 - 100 < gate) -> skip
        assert core._last_sweep_tick == 100
        assert core.coordinator.enforce_stable_grant_timeouts.call_count == 1

    def test_read_invokes_maybe_sweep(self) -> None:
        core = _core()
        core.register_agent("A")
        artifact = core.register_artifact(name="x.md", content="v1")
        core._maybe_sweep = Mock()  # type: ignore[method-assign]
        core.read(agent_name="A", artifact_id=artifact.id, now_tick=7)
        core._maybe_sweep.assert_called_once_with(7)

    def test_write_invokes_maybe_sweep(self) -> None:
        core = _core()
        core.register_agent("A")
        artifact = core.register_artifact(name="x.md", content="v1")
        core._maybe_sweep = Mock()  # type: ignore[method-assign]
        core.write(agent_name="A", artifact_id=artifact.id, content="v2", now_tick=9)
        core._maybe_sweep.assert_called_once_with(9)


class TestMaybeSweepDiagnostic:
    """R9 first-reclamation diagnostic: logger.warning + structured extras."""

    def test_first_reclamation_emits_structured_diagnostic(self, caplog) -> None:
        core = _core()
        aid, agid = uuid4(), uuid4()

        def _enforce(*, current_tick, heartbeat_timeout_ticks, max_hold_ticks, on_reclaim):
            on_reclaim(aid, agid, "reclaim_heartbeat")
            return 1

        core.coordinator.enforce_stable_grant_timeouts = Mock(side_effect=_enforce)
        with caplog.at_level(logging.WARNING, logger="ccs.adapters.base"):
            core._maybe_sweep(now_tick=10)
        records = [
            r for r in caplog.records
            if r.name == "ccs.adapters.base" and r.levelno == logging.WARNING
        ]
        assert len(records) == 1
        rec = records[0]
        assert rec.trigger == "reclaim_heartbeat"
        assert rec.agent_id_short == str(agid)[:8]
        assert rec.artifact_id_short == str(aid)[:8]
        assert rec.reclaim_count == 1
        assert "sweep reclaimed 1 agent(s)" in rec.getMessage()

    def test_diagnostic_dedupes_per_instance(self, caplog) -> None:
        core = _core()
        core.coordinator.enforce_stable_grant_timeouts = Mock(side_effect=_reclaiming_enforce)
        with caplog.at_level(logging.WARNING, logger="ccs.adapters.base"):
            core._maybe_sweep(now_tick=0)  # fires + reclaims + emits
            core._maybe_sweep(now_tick=10)  # fires + reclaims, but no second emit
        records = [
            r for r in caplog.records
            if r.name == "ccs.adapters.base" and r.levelno == logging.WARNING
        ]
        assert len(records) == 1

    def test_diagnostic_is_per_instance_not_per_process(self, caplog) -> None:
        c1, c2 = _core(), _core()
        c1.coordinator.enforce_stable_grant_timeouts = Mock(side_effect=_reclaiming_enforce)
        c2.coordinator.enforce_stable_grant_timeouts = Mock(side_effect=_reclaiming_enforce)
        with caplog.at_level(logging.WARNING, logger="ccs.adapters.base"):
            c1._maybe_sweep(now_tick=10)
            c2._maybe_sweep(now_tick=10)
        records = [
            r for r in caplog.records
            if r.name == "ccs.adapters.base" and r.levelno == logging.WARNING
        ]
        assert len(records) == 2

    def test_sweep_failure_is_best_effort_and_rate_limits_retry(self, caplog) -> None:
        core = _core()  # heartbeat_timeout=10 -> gate=5
        core.register_agent("A")
        artifact = core.register_artifact(name="x.md", content="v1")
        core.coordinator.enforce_stable_grant_timeouts = Mock(
            side_effect=RuntimeError("synthetic")
        )
        with caplog.at_level(logging.ERROR, logger="ccs.adapters.base"):
            # read must complete despite the sweep raising.
            core.read(agent_name="A", artifact_id=artifact.id, now_tick=10)
        assert any(r.levelno == logging.ERROR for r in caplog.records)
        # The slot was CLAIMED in Phase 1, so the failed sweep is rate-limited:
        # _last_sweep_tick advanced to 10, and a call within the gate window
        # does NOT retry (no hot loop against a persistently-failing coordinator).
        assert core._last_sweep_tick == 10
        core._maybe_sweep(now_tick=11)  # 11 - 10 = 1 < gate(5) -> skip
        assert core.coordinator.enforce_stable_grant_timeouts.call_count == 1
        # A call past the gate window does retry.
        core._maybe_sweep(now_tick=20)  # 20 - 10 = 10 >= gate -> retry
        assert core.coordinator.enforce_stable_grant_timeouts.call_count == 2


class TestMaybeSweepConcurrency:
    """Finding #6 + ce:review ADV-01 — the Phase-1 claim under _sweep_lock
    serializes concurrent callers at the same tick to a SINGLE coordinator
    sweep per gate window (and therefore a single diagnostic). A start-barrier
    maximizes lock contention; the inside-sweep barrier of the old design would
    now deadlock, since only one thread reaches the coordinator call."""

    def test_concurrent_sweep_invokes_coordinator_and_emits_exactly_once(self, caplog) -> None:
        core = _core()
        start = threading.Barrier(2)
        calls: list[int] = []
        calls_lock = threading.Lock()

        def _enforce(*, current_tick, heartbeat_timeout_ticks, max_hold_ticks, on_reclaim):
            with calls_lock:
                calls.append(current_tick)
            on_reclaim(uuid4(), uuid4(), "reclaim_heartbeat")
            return 1

        core.coordinator.enforce_stable_grant_timeouts = Mock(side_effect=_enforce)

        def worker() -> None:
            start.wait()  # release both threads together to contend on _sweep_lock
            core._maybe_sweep(now_tick=10)

        with caplog.at_level(logging.WARNING, logger="ccs.adapters.base"):
            threads = [threading.Thread(target=worker) for _ in range(2)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()
        records = [
            r for r in caplog.records
            if r.name == "ccs.adapters.base" and r.levelno == logging.WARNING
        ]
        # The Phase-1 claim serializes: exactly one thread sweeps the coordinator
        # and exactly one diagnostic is emitted — no double sweep, no double emit.
        assert len(calls) == 1
        assert len(records) == 1

    def test_concurrent_gate_invokes_sweep_exactly_once(self) -> None:
        core = _core()
        start = threading.Barrier(2)

        def _enforce(*, current_tick, heartbeat_timeout_ticks, max_hold_ticks, on_reclaim):
            return 0

        core.coordinator.enforce_stable_grant_timeouts = Mock(side_effect=_enforce)

        def worker() -> None:
            start.wait()  # contend on _sweep_lock at the same tick
            core._maybe_sweep(now_tick=10)

        threads = [threading.Thread(target=worker) for _ in range(2)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        # The Phase-1 claim (advancing _last_sweep_tick under the lock) means the
        # second caller sees the advanced tick and skips: exactly one sweep.
        assert core.coordinator.enforce_stable_grant_timeouts.call_count == 1
        assert core._last_sweep_tick == 10


class TestMaybeSweepEndToEnd:
    """Integration: a real stale grant is reclaimed on the next read, the
    diagnostic emits exactly once, and subsequent reads do not re-emit."""

    def test_stale_grant_reclaimed_on_read_diagnostic_once(self, caplog) -> None:
        core = _core()  # heartbeat_timeout_ticks=10, gate=5
        core.register_agent("A", now_tick=0)
        core.register_agent("B", now_tick=0)
        artifact = core.register_artifact(name="x.md", content="v1")
        # A acquires MODIFIED then goes silent (no further heartbeat).
        core.write(agent_name="A", artifact_id=artifact.id, content="v2", now_tick=0)
        with caplog.at_level(logging.WARNING, logger="ccs.adapters.base"):
            # B reads far past A's heartbeat timeout -> sweep reclaims A.
            core.read(agent_name="B", artifact_id=artifact.id, now_tick=50)
            # A second read does not re-reclaim or re-emit.
            core.read(agent_name="B", artifact_id=artifact.id, now_tick=100)
        records = [
            r for r in caplog.records
            if r.name == "ccs.adapters.base" and r.levelno == logging.WARNING
        ]
        assert len(records) == 1
        assert records[0].trigger in ("reclaim_heartbeat", "reclaim_max_hold")
