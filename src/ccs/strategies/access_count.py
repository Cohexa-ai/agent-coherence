# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Access-count bounded strategy for clock-independent staleness control."""

from __future__ import annotations

from uuid import UUID

from ccs.core.states import MESIState
from ccs.core.types import ArtifactCacheEntry

from .base import SyncStrategy


class AccessCountStrategy(SyncStrategy):
    """Refresh after a configured number of local accesses."""

    name = "access_count"

    def __init__(self, *, max_accesses: int = 100):
        if max_accesses < 1:
            raise ValueError("max_accesses must be >= 1")
        self.max_accesses = max_accesses

    def staleness_bound(self) -> int:
        return self.max_accesses

    def requires_refresh(self, entry: ArtifactCacheEntry, *, now_tick: int) -> bool:
        del now_tick
        if entry.state == MESIState.INVALID:
            return True
        return entry.access_count >= self.max_accesses

    def on_read(self, entry: ArtifactCacheEntry, *, now_tick: int) -> ArtifactCacheEntry:
        del now_tick
        return self._touch(entry)

    def on_fetch(
        self,
        *,
        artifact_id: UUID,
        version: int,
        state: MESIState,
        now_tick: int,
    ) -> ArtifactCacheEntry:
        return self._new_entry(artifact_id=artifact_id, version=version, state=state, now_tick=now_tick)
