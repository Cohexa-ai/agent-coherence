# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""Local artifact cache used by agent runtime."""

from __future__ import annotations

from dataclasses import replace
from uuid import UUID

from ccs.core.states import MESIState
from ccs.core.types import ArtifactCacheEntry


class ArtifactCache:
    """In-memory per-agent cache for artifact entries."""

    def __init__(self) -> None:
        self._entries: dict[UUID, ArtifactCacheEntry] = {}

    def get(self, artifact_id: UUID) -> ArtifactCacheEntry | None:
        """Return cache entry for artifact if present."""
        return self._entries.get(artifact_id)

    def put(self, artifact_id: UUID, entry: ArtifactCacheEntry) -> None:
        """Insert or replace cache entry for artifact."""
        self._entries[artifact_id] = entry

    def invalidate(
        self,
        artifact_id: UUID,
        *,
        invalidated_version: int | None = None,
        issued_at_tick: int = 0,
    ) -> None:
        """Set entry state to INVALID, creating placeholder if missing."""
        entry = self._entries.get(artifact_id)
        if entry is None:
            self._entries[artifact_id] = ArtifactCacheEntry(
                artifact_id=artifact_id,
                state=MESIState.INVALID,
                local_version=max(invalidated_version or 0, 0),
                acquired_at_tick=issued_at_tick,
            )
            return

        next_version = entry.local_version
        if invalidated_version is not None:
            next_version = min(entry.local_version, invalidated_version)
        self._entries[artifact_id] = replace(
            entry,
            state=MESIState.INVALID,
            local_version=max(next_version, 0),
        )

    def invalidate_all(
        self,
        *,
        invalidated_version: int | None = None,
        issued_at_tick: int = 0,
    ) -> None:
        """Set all cached entries to INVALID without creating placeholders."""
        for artifact_id, entry in list(self._entries.items()):
            next_version = entry.local_version
            if invalidated_version is not None:
                next_version = min(entry.local_version, invalidated_version)
            self._entries[artifact_id] = replace(
                entry,
                state=MESIState.INVALID,
                local_version=max(next_version, 0),
            )

    def has_valid(self, artifact_id: UUID) -> bool:
        """Return whether artifact is cached in non-invalid state."""
        entry = self._entries.get(artifact_id)
        return entry is not None and entry.state != MESIState.INVALID

    def entries(self) -> dict[UUID, ArtifactCacheEntry]:
        """Return shallow copy of cached entries."""
        return dict(self._entries)
