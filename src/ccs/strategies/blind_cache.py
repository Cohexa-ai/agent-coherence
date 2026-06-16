# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Blind cache strategy — BENCHMARK COST-FLOOR ONLY, never for production.

``requires_refresh`` always returns ``False``: it serves the first-fetched copy
forever and never honors ``MESIState.INVALID``, modelling the exact failure the
coherence protocol exists to prevent (unbounded staleness). It is intentionally
NOT part of the public ``ccs.strategies`` API (absent from ``__all__``) and is
reachable only via ``build_strategy("blind")`` so the temporal cost benchmark can
resolve it by name.
"""

from __future__ import annotations

from uuid import UUID

from ccs.core.states import MESIState
from ccs.core.types import ArtifactCacheEntry

from .base import SyncStrategy


class BlindCacheStrategy(SyncStrategy):
    """BENCHMARK COST-FLOOR — UNBOUNDED STALENESS, NEVER FOR PRODUCTION.

    Fetches once on first fill, then serves the local copy forever: it
    deliberately ignores ``MESIState.INVALID`` so it never re-fetches a stale
    entry. This models the exact failure the coherence protocol exists to
    prevent, and exists only as the comparison baseline for the temporal cost
    benchmark. It is intentionally excluded from ``ccs.strategies.__all__`` and
    reachable only via ``build_strategy("blind")``. Do not use it in production —
    it has no freshness guarantee whatsoever.
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
