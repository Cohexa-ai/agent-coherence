# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""In-memory event bus for invalidation and update fan-out."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Iterable
from uuid import UUID

from ccs.core.types import InvalidationSignal


@dataclass(frozen=True)
class ArtifactUpdateEvent:
    """Content update event used for eager update propagation."""

    artifact_id: UUID
    version: int
    content: str
    issued_at_tick: int
    issuer_agent_id: UUID


class InMemoryEventBus:
    """Simple synchronous pub/sub bus keyed by recipient agent id."""

    def __init__(self) -> None:
        self._invalidation_handlers: dict[UUID, Callable[[InvalidationSignal], None]] = {}
        self._update_handlers: dict[UUID, Callable[[ArtifactUpdateEvent], None]] = {}

    def subscribe(
        self,
        *,
        agent_id: UUID,
        on_invalidation: Callable[[InvalidationSignal], None],
        on_update: Callable[[ArtifactUpdateEvent], None] | None = None,
    ) -> None:
        """Register handlers for one agent identity."""
        self._invalidation_handlers[agent_id] = on_invalidation
        if on_update is not None:
            self._update_handlers[agent_id] = on_update

    def unsubscribe(self, *, agent_id: UUID) -> None:
        """Remove handlers for one agent identity."""
        self._invalidation_handlers.pop(agent_id, None)
        self._update_handlers.pop(agent_id, None)

    def publish_invalidation(self, signal: InvalidationSignal, *, recipients: Iterable[UUID]) -> int:
        """Deliver invalidation signal to subscribed recipients."""
        delivered = 0
        for recipient in recipients:
            handler = self._invalidation_handlers.get(recipient)
            if handler is None:
                continue
            handler(signal)
            delivered += 1
        return delivered

    def publish_update(self, event: ArtifactUpdateEvent, *, recipients: Iterable[UUID]) -> int:
        """Deliver update event to subscribed recipients."""
        delivered = 0
        for recipient in recipients:
            handler = self._update_handlers.get(recipient)
            if handler is None:
                continue
            handler(event)
            delivered += 1
        return delivered

    def subscribers(self) -> list[UUID]:
        """Return sorted list of subscribed agent ids."""
        return sorted(self._invalidation_handlers.keys(), key=str)
