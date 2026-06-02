# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""Live-API smoke tests for the OpenAI Agents adapter (Unit 8).

Gated three ways so the default ``pytest -q`` loop stays fast, offline, and free:
  1. ``pytest.importorskip("agents"/"openai")`` — skips on a bare ``[dev]`` install.
  2. module ``pytestmark = live_api`` — excluded from the default ``addopts``.
  3. ``skipif(not OPENAI_API_KEY)`` — skips when no key is present.

Run explicitly: ``pytest -m live_api`` with ``OPENAI_API_KEY`` set and
``pip install "agent-coherence[openai-agents]"``.

Scope: proves the adapter's Session-cache coherence works over a *real*
Conversations-API-backed Session (``OpenAIConversationsSession``). The adapter
wraps a caller-provided Session and governs coherence accounting only — it does
not wrap the underlying Session's own SDK/network errors (the caller owns their
Session), so there is no adapter-error-wrapping scenario to assert here.
"""

from __future__ import annotations

import asyncio
import os

import pytest

pytest.importorskip("openai")
agents = pytest.importorskip("agents")

from ccs.adapters.openai_agents import OpenAIAgentsAdapter  # noqa: E402

pytestmark = [
    pytest.mark.live_api,
    pytest.mark.skipif(not os.environ.get("OPENAI_API_KEY"), reason="OPENAI_API_KEY not set"),
]


def test_round_trip_over_real_conversations_session():
    """A wrapped real Conversations Session round-trips an item through the live API."""
    adapter = OpenAIAgentsAdapter()
    underlying = agents.OpenAIConversationsSession()  # creates a real conversation_id
    session = adapter.wrap_session(underlying, agent_name="a", session_id="live-round-trip")

    async def scenario():
        try:
            await session.add_items([{"role": "user", "content": "coherence live smoke"}])
            return await session.get_items()
        finally:
            await underlying.clear_session()  # cleanup server-side state even on failure

    items = asyncio.run(scenario())
    assert items and items[-1]["content"] == "coherence live smoke"


def test_cross_agent_invalidation_over_one_real_conversation():
    """Two agents on one live conversation_id: A's write invalidates B's cached view."""
    adapter = OpenAIAgentsAdapter()

    async def scenario():
        # OpenAIConversationsSession.session_id raises until the session is lazily
        # initialized by a method call, so seed one item first to obtain the id.
        underlying_a = agents.OpenAIConversationsSession()
        await underlying_a.add_items([{"role": "user", "content": "seed"}])
        conversation_id = underlying_a.session_id
        underlying_b = agents.OpenAIConversationsSession(conversation_id=conversation_id)
        a = adapter.wrap_session(underlying_a, agent_name="a", session_id=conversation_id)
        b = adapter.wrap_session(underlying_b, agent_name="b", session_id=conversation_id)
        try:
            await b.get_items()  # B establishes a baseline over the live session
            assert b.peer_mutated_since_read() is False
            await a.add_items([{"role": "user", "content": "A revises the plan"}])  # invalidates B
            stale = b.peer_mutated_since_read()  # B's cached view is now INVALID
            fresh = await b.get_items()  # re-fetch from the live conversation
            return stale, fresh
        finally:
            await underlying_a.clear_session()
            await underlying_b.clear_session()

    stale, fresh = asyncio.run(scenario())
    assert stale is True
    assert any(item.get("content") == "A revises the plan" for item in fresh)
