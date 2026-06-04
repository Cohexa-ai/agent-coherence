# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""Blind cache strategy that never refetches after the first fill."""

from __future__ import annotations

from uuid import UUID

from ccs.core.states import MESIState
from ccs.core.types import ArtifactCacheEntry

from .base import SyncStrategy


class BlindCacheStrategy(SyncStrategy):
    """Fetch once on first fill, then serve the local copy forever.

    This is the cost floor for the temporal benchmark: it deliberately ignores
    ``MESIState.INVALID`` so it never re-fetches a stale entry. Realistic
    strategies trade tokens for freshness; this one pays the minimum token cost
    and accepts unbounded staleness as the comparison baseline.
    """

    name = "blind"

    def requires_refresh(self, entry: ArtifactCacheEntry, *, now_tick: int) -> bool:
        del entry, now_tick
        return False

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
