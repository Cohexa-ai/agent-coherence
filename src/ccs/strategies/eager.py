# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Eager broadcast strategy with zero permitted staleness."""

from __future__ import annotations

from uuid import UUID

from ccs.core.states import MESIState
from ccs.core.types import ArtifactCacheEntry

from .base import SyncStrategy


class EagerStrategy(SyncStrategy):
    """Consistency-agnostic strategy that broadcasts full content on commit."""

    name = "eager"

    def broadcasts_content_on_commit(self) -> bool:
        return True

    def invalidates_peers_on_commit(self) -> bool:
        return False

    def staleness_bound(self) -> int:
        return 0

    def requires_refresh(self, entry: ArtifactCacheEntry, *, now_tick: int) -> bool:
        del now_tick
        return entry.state == MESIState.INVALID

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
