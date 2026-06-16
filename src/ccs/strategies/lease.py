# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Lease strategy with TTL-based local refresh."""

from __future__ import annotations

from dataclasses import replace
from uuid import UUID

from ccs.core.states import MESIState
from ccs.core.types import ArtifactCacheEntry

from .base import SyncStrategy


class LeaseStrategy(SyncStrategy):
    """Allow local use until TTL expires, then force refresh."""

    name = "lease"

    def __init__(self, *, ttl_ticks: int = 300):
        if ttl_ticks < 1:
            raise ValueError("ttl_ticks must be >= 1")
        self.ttl_ticks = ttl_ticks

    def staleness_bound(self) -> int:
        return self.ttl_ticks

    def requires_refresh(self, entry: ArtifactCacheEntry, *, now_tick: int) -> bool:
        if entry.state == MESIState.INVALID:
            return True
        if entry.expires_at_tick is None:
            return True
        return now_tick >= entry.expires_at_tick

    def on_read(self, entry: ArtifactCacheEntry, *, now_tick: int) -> ArtifactCacheEntry:
        expires_at_tick = entry.expires_at_tick
        if expires_at_tick is None:
            expires_at_tick = now_tick + self.ttl_ticks
        return replace(
            self._touch(entry),
            expires_at_tick=expires_at_tick,
        )

    def on_fetch(
        self,
        *,
        artifact_id: UUID,
        version: int,
        state: MESIState,
        now_tick: int,
    ) -> ArtifactCacheEntry:
        return replace(
            self._new_entry(artifact_id=artifact_id, version=version, state=state, now_tick=now_tick),
            expires_at_tick=now_tick + self.ttl_ticks,
        )
