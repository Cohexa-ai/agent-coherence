# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""Tests for the OpenAI Agents SDK coherence adapter (Units 4 + 6).

Scaffolding + Session-coherence are exercised offline with a fake Session (the
four-method async protocol), so they run in the default ``pytest -q`` loop with
no ``openai-agents`` install. One integration test wraps a real ``SQLiteSession``
behind ``pytest.importorskip`` to prove delegation against the actual SDK.
"""

from __future__ import annotations

import asyncio
from unittest.mock import patch

import pytest

from ccs.adapters.openai_agents import (
    CoherenceDegradedWarning,
    CoherenceSession,
    OpenAIAgentsAdapter,
)
from ccs.core.exceptions import CoherenceError


class FakeSession:
    """In-memory Session sharing a backing store across agents on one session_id."""

    def __init__(self, store: dict[str, list], session_id: str) -> None:
        self._store = store
        self._sid = session_id
        store.setdefault(session_id, [])

    async def get_items(self, limit=None):
        items = self._store[self._sid]
        return list(items[-limit:]) if limit else list(items)

    async def add_items(self, items):
        self._store[self._sid].extend(items)

    async def pop_item(self):
        return self._store[self._sid].pop() if self._store[self._sid] else None

    async def clear_session(self):
        self._store[self._sid].clear()


# --- Unit 4: scaffolding ---------------------------------------------------


def test_constructs_with_fresh_core_and_registers_agent():
    adapter = OpenAIAgentsAdapter()
    agent_id = adapter.register_agent("planner")
    assert agent_id is not None
    assert "planner" in adapter.core.agent_names()


def test_invalid_on_error_raises():
    with pytest.raises(ValueError):
        OpenAIAgentsAdapter(on_error="bogus")


def test_tick_is_monotonic_under_lock():
    adapter = OpenAIAgentsAdapter()
    ticks = [adapter._next_tick() for _ in range(5)]
    assert ticks == sorted(ticks) and len(set(ticks)) == 5


def test_wrap_session_returns_coherence_session_with_shared_artifact():
    adapter = OpenAIAgentsAdapter()
    store: dict[str, list] = {}
    a = adapter.wrap_session(FakeSession(store, "conv-1"), agent_name="a", session_id="conv-1")
    b = adapter.wrap_session(FakeSession(store, "conv-1"), agent_name="b", session_id="conv-1")
    assert isinstance(a, CoherenceSession)
    # Both agents on the same session_id resolve to the same coherence artifact.
    assert a._artifact_id == b._artifact_id


# --- Unit 6: Session coherence ---------------------------------------------


def test_peer_add_invalidates_other_agents_cache():
    adapter = OpenAIAgentsAdapter()
    store: dict[str, list] = {}
    a = adapter.wrap_session(FakeSession(store, "conv-1"), agent_name="a", session_id="conv-1")
    b = adapter.wrap_session(FakeSession(store, "conv-1"), agent_name="b", session_id="conv-1")

    async def scenario():
        await b.get_items()  # B reads → caches (SHARED)
        assert b.peer_mutated_since_read() is False
        await a.add_items([{"role": "user", "content": "v2"}])  # A writes → invalidates B
        assert b.peer_mutated_since_read() is True  # B's cache is now INVALID
        items = await b.get_items()  # B re-reads → fresh, sees A's item
        return items

    items = asyncio.run(scenario())
    assert items == [{"role": "user", "content": "v2"}]


def test_pop_and_clear_invalidate_peers():
    adapter = OpenAIAgentsAdapter()
    store: dict[str, list] = {"conv-1": [{"x": 1}]}
    a = adapter.wrap_session(FakeSession(store, "conv-1"), agent_name="a", session_id="conv-1")
    b = adapter.wrap_session(FakeSession(store, "conv-1"), agent_name="b", session_id="conv-1")

    async def scenario():
        await b.get_items()
        popped = await a.pop_item()  # A mutates → invalidates B
        assert b.peer_mutated_since_read() is True
        await b.get_items()  # refresh
        await a.clear_session()  # A mutates again → invalidates B
        assert b.peer_mutated_since_read() is True
        return popped

    assert asyncio.run(scenario()) == {"x": 1}


def test_first_read_before_any_write_is_not_stale():
    adapter = OpenAIAgentsAdapter()
    store: dict[str, list] = {}
    b = adapter.wrap_session(FakeSession(store, "conv-1"), agent_name="b", session_id="conv-1")

    async def scenario():
        await b.get_items()  # fresh-registered agent: state is None, not INVALID
        return b.peer_mutated_since_read()

    assert asyncio.run(scenario()) is False


def test_peer_mutated_is_true_before_any_read_baseline():
    # Conservative contract: with no baseline read, the only safe answer is "read first".
    adapter = OpenAIAgentsAdapter()
    store: dict[str, list] = {}
    b = adapter.wrap_session(FakeSession(store, "conv-1"), agent_name="b", session_id="conv-1")
    assert b.peer_mutated_since_read() is True  # never called get_items yet


# --- degrade contract ------------------------------------------------------


def test_strict_mode_reraises_coherence_error():
    adapter = OpenAIAgentsAdapter(on_error="strict")
    store: dict[str, list] = {}
    a = adapter.wrap_session(FakeSession(store, "conv-1"), agent_name="a", session_id="conv-1")

    async def scenario():
        with patch.object(adapter.core, "write", side_effect=CoherenceError("boom")):
            with pytest.raises(CoherenceError):
                await a.add_items([{"x": 1}])

    asyncio.run(scenario())


def test_degrade_mode_swallows_and_warns_once_but_underlying_write_persists():
    adapter = OpenAIAgentsAdapter(on_error="degrade")
    store: dict[str, list] = {}
    a = adapter.wrap_session(FakeSession(store, "conv-1"), agent_name="a", session_id="conv-1")

    async def scenario():
        with patch.object(adapter.core, "write", side_effect=CoherenceError("boom")):
            with pytest.warns(CoherenceDegradedWarning):
                await a.add_items([{"x": 1}])  # coherence degraded...
                await a.add_items([{"y": 2}])  # ...warns only once
        # The underlying Session write still happened — degrade never drops real work.
        return store["conv-1"]

    items = asyncio.run(scenario())
    assert items == [{"x": 1}, {"y": 2}]
    assert adapter.is_degraded is True
    assert adapter.degradation_count == 2


# --- shared-warning identity (regression for the dual-class export bug) ------


def test_degraded_warning_is_catchable_via_package_export():
    # `from ccs.adapters import CoherenceDegradedWarning` must catch what THIS
    # adapter emits — both resolve to the canonical ccs.core.exceptions class.
    import ccs.adapters as adapters_pkg

    assert adapters_pkg.CoherenceDegradedWarning is CoherenceDegradedWarning
    adapter = OpenAIAgentsAdapter(on_error="degrade")
    store: dict[str, list] = {}
    a = adapter.wrap_session(FakeSession(store, "c"), agent_name="a", session_id="c")

    async def scenario():
        with patch.object(adapter.core, "write", side_effect=CoherenceError("boom")):
            with pytest.warns(adapters_pkg.CoherenceDegradedWarning):  # package-level class
                await a.add_items([{"x": 1}])

    asyncio.run(scenario())


# --- degrade/strict on the read path (get_items) ----------------------------


def test_strict_mode_reraises_on_read_failure():
    adapter = OpenAIAgentsAdapter(on_error="strict")
    store: dict[str, list] = {}
    a = adapter.wrap_session(FakeSession(store, "c"), agent_name="a", session_id="c")

    async def scenario():
        with patch.object(adapter.core, "read", side_effect=CoherenceError("read boom")):
            with pytest.raises(CoherenceError):
                await a.get_items()

    asyncio.run(scenario())


def test_degrade_mode_swallows_read_failure_and_still_returns_items():
    adapter = OpenAIAgentsAdapter(on_error="degrade")
    store: dict[str, list] = {"c": [{"x": 1}]}
    a = adapter.wrap_session(FakeSession(store, "c"), agent_name="a", session_id="c")

    async def scenario():
        with patch.object(adapter.core, "read", side_effect=CoherenceError("read boom")):
            with pytest.warns(CoherenceDegradedWarning):
                items = await a.get_items()  # coherence degraded, underlying read still served
        return items

    assert asyncio.run(scenario()) == [{"x": 1}]


# --- crash-recovery passthrough parity --------------------------------------


def test_heartbeat_and_recover_passthrough_are_noops_when_disabled():
    # Parity with the other adapters: forward to core; no-op heartbeat when crash
    # recovery is disabled, recover always flushes the local cache.
    adapter = OpenAIAgentsAdapter()
    adapter.register_agent("a")
    adapter.heartbeat(agent_name="a", now_tick=1)  # must not raise
    adapter.recover(agent_name="a", now_tick=2)  # must not raise


# --- integration: real SQLiteSession ---------------------------------------


def test_wraps_a_real_sqlite_session():
    agents = pytest.importorskip("agents")  # requires the openai-agents extra
    adapter = OpenAIAgentsAdapter()
    underlying = agents.SQLiteSession("conv-real")
    session = adapter.wrap_session(underlying, agent_name="a", session_id="conv-real")

    async def scenario():
        await session.add_items([{"role": "user", "content": "hello"}])
        items = await session.get_items()
        await underlying.clear_session()  # cleanup shared in-memory db
        return items

    items = asyncio.run(scenario())
    assert items and items[-1]["content"] == "hello"
