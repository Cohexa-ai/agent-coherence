# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""OpenAI Agents SDK coherence adapter.

The adapter's primary durable surface (post-Q6, 2026-05-31) is the **Session
cache**: the OpenAI Agents SDK's ``Session`` is local conversation memory
(``get_items``/``add_items``/``pop_item``/``clear_session``), and a peer that
mutates a shared session leaves this agent's cached view stale — regardless of
how consistent the underlying durable store is. The Q6 probe found the
Conversations *server* consistent, so the coherence value lives here, on the
readers' caches.

The SDK exposes no Session hook/middleware API, so interception is by
*composition*: ``CoherenceSession`` wraps a caller-provided Session, overrides
the four async methods, and routes coherence accounting through
``CoherenceAdapterCore``. Because the underlying Session is supplied by the
caller, this module imports no ``agents`` symbol — it works against anything
implementing the four-method protocol.

Scope (v1): in-process multi-agent coherence — peers registered on one
``CoherenceAdapterCore``/process (the same boundary as the LangGraph/CrewAI/
AutoGen adapters). Cross-service coherence needs the out-of-process coordinator.
"""

from __future__ import annotations

import logging
import threading
import warnings
from typing import Any
from uuid import UUID

from ccs.adapters.base import CoherenceAdapterCore
from ccs.coordinator.service import CrashRecoveryConfig
from ccs.core.exceptions import CoherenceError
from ccs.core.states import MESIState
from ccs.core.types import Artifact

logger = logging.getLogger(__name__)

__all__ = ["OpenAIAgentsAdapter", "CoherenceSession", "CoherenceDegradedWarning"]


class CoherenceDegradedWarning(UserWarning):
    """Emitted once when the adapter falls back under ``on_error='degrade'``."""


class OpenAIAgentsAdapter:
    """Coherence facade for the OpenAI Agents SDK, parity with the other adapters.

    Mirrors the LangGraph/CrewAI/AutoGen constructor shape and composes with
    ``CrashRecoveryConfig``; mints its own monotonic tick (the SDK has no step
    counter) under a lock, following the ``CCSStore`` pattern.
    """

    def __init__(
        self,
        *,
        strategy_name: str = "lazy",
        core: CoherenceAdapterCore | None = None,
        crash_recovery: CrashRecoveryConfig | None = None,
        on_error: str = "degrade",
        **kwargs: Any,
    ) -> None:
        if on_error not in ("strict", "degrade"):
            raise ValueError(f"on_error must be 'strict' or 'degrade', got {on_error!r}")
        self._on_error = on_error
        self.core = (
            core
            if core is not None
            else CoherenceAdapterCore(strategy_name=strategy_name, crash_recovery=crash_recovery, **kwargs)
        )
        self._lock = threading.Lock()
        self._tick = 0
        self._sessions: dict[str, UUID] = {}
        self._degradation_count = 0

    # --- parity passthroughs ------------------------------------------------

    def register_agent(self, name: str) -> UUID:
        """Register one agent identity (idempotent)."""
        return self.core.register_agent(name)

    def register_artifact(self, *, name: str, content: str, size_tokens: int | None = None) -> Artifact:
        """Register a shared artifact in the coordinator directory."""
        return self.core.register_artifact(name=name, content=content, size_tokens=size_tokens)

    # --- Session coherence (the primary surface) ----------------------------

    def wrap_session(self, underlying: Any, *, agent_name: str, session_id: str) -> CoherenceSession:
        """Wrap a caller-provided Session so peer mutations invalidate this agent.

        All agents wrapping the same ``session_id`` through this adapter share one
        coherence artifact (single registration, shared id) — no deterministic
        artifact-id trick required because the adapter coordinates registration.
        """
        self.core.register_agent(agent_name)
        with self._lock:
            artifact_id = self._sessions.get(session_id)
            if artifact_id is None:
                artifact = self.core.register_artifact(name=f"session:{session_id}", content="")
                artifact_id = artifact.id
                self._sessions[session_id] = artifact_id
        return CoherenceSession(
            underlying=underlying,
            adapter=self,
            agent_name=agent_name,
            artifact_id=artifact_id,
            session_id=session_id,
        )

    # --- degradation contract ----------------------------------------------

    @property
    def is_degraded(self) -> bool:
        return self._degradation_count > 0

    @property
    def degradation_count(self) -> int:
        return self._degradation_count

    def _next_tick(self) -> int:
        with self._lock:
            self._tick += 1
            return self._tick

    def _record_degraded(self, exc: CoherenceError) -> None:
        if self._degradation_count == 0:
            warnings.warn(f"OpenAI Agents adapter degraded: {exc}", CoherenceDegradedWarning, stacklevel=3)
            logger.warning("OpenAI Agents adapter degraded under on_error='degrade': %s", exc)
        self._degradation_count += 1


class CoherenceSession:
    """Coherence-tracking wrapper over an OpenAI Agents SDK ``Session``.

    Implements the four-method async protocol by composition. A mutation
    (``add_items``/``pop_item``/``clear_session``) commits through the coordinator
    and invalidates peer agents' cached views; ``get_items`` refreshes this
    agent's coherence state so a prior peer mutation surfaces as a cache miss.
    The underlying Session remains the durable source of truth for the items
    themselves — the coherence layer governs *awareness*, not storage.
    """

    def __init__(
        self,
        *,
        underlying: Any,
        adapter: OpenAIAgentsAdapter,
        agent_name: str,
        artifact_id: UUID,
        session_id: str,
    ) -> None:
        self._underlying = underlying
        self._adapter = adapter
        self._agent_name = agent_name
        self._artifact_id = artifact_id
        self.session_id = session_id

    async def get_items(self, limit: int | None = None) -> list[Any]:
        """Read items; refresh coherence state first so peer writes show as misses."""
        self._coherence_read()
        return await self._underlying.get_items(limit)

    async def add_items(self, items: list[Any]) -> None:
        """Persist items to the underlying Session, then invalidate peers."""
        await self._underlying.add_items(items)  # durable write first
        self._coherence_commit("add")

    async def pop_item(self) -> Any:
        """Pop from the underlying Session, then invalidate peers."""
        item = await self._underlying.pop_item()
        self._coherence_commit("pop")
        return item

    async def clear_session(self) -> None:
        """Clear the underlying Session, then invalidate peers."""
        await self._underlying.clear_session()
        self._coherence_commit("clear")

    def peer_mutated_since_read(self) -> bool:
        """True if a peer mutated this session since this agent last read it.

        Observable coherence signal: the agent's cached entry is INVALID because a
        peer's commit invalidated it. ``None``/fresh-state reads as not-mutated.
        """
        entry = self._adapter.core.runtime(self._agent_name).cache.get(self._artifact_id)
        return entry is not None and entry.state == MESIState.INVALID

    # --- coherence plumbing with the degrade contract -----------------------

    def _coherence_read(self) -> None:
        try:
            self._adapter.core.read(
                agent_name=self._agent_name,
                artifact_id=self._artifact_id,
                now_tick=self._adapter._next_tick(),
            )
        except CoherenceError as exc:
            self._degrade_or_raise(exc)

    def _coherence_commit(self, op: str) -> None:
        try:
            self._adapter.core.write(
                agent_name=self._agent_name,
                artifact_id=self._artifact_id,
                content=f"mutated:{op}",
                now_tick=self._adapter._next_tick(),
            )
        except CoherenceError as exc:
            self._degrade_or_raise(exc)

    def _degrade_or_raise(self, exc: CoherenceError) -> None:
        # The underlying Session op already succeeded — under degrade, coherence is
        # best-effort and we never swallow the real work, only the accounting.
        if self._adapter._on_error == "strict":
            raise exc
        self._adapter._record_degraded(exc)
