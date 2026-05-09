# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""Agent runtime encapsulating read/write/invalidation protocol flows."""

from __future__ import annotations

from typing import Any, Callable, Optional
from uuid import UUID

from ccs.coordinator.service import CoordinatorService
from ccs.core.hashing import compute_content_hash
from ccs.core.states import MESIState
from ccs.core.types import Artifact, FetchRequest, FetchResponse, InvalidationSignal
from ccs.strategies.base import SyncStrategy

from .cache import ArtifactCache

CCS_CONTENT_AUDIT_LOG_SCHEMA_VERSION = "ccs.content_audit.v1"


class AgentRuntime:
    """Reusable protocol participant for one agent identity."""

    def __init__(
        self,
        *,
        agent_id: UUID,
        coordinator: CoordinatorService,
        strategy: SyncStrategy,
        cache: Optional[ArtifactCache] = None,
        content_audit_log: Callable[[dict[str, Any]], None] | None = None,
        audit_seq: list[int] | None = None,
        agent_name: str | None = None,
        instance_id: str | None = None,
    ) -> None:
        self.agent_id = agent_id
        self.coordinator = coordinator
        self.strategy = strategy
        self.cache = cache if cache is not None else ArtifactCache()
        self._content_by_artifact: dict[UUID, str] = {}
        self._content_audit_log = content_audit_log
        self._audit_seq = audit_seq if audit_seq is not None else [0]
        self._agent_name = agent_name
        self._instance_id = instance_id

    def read(self, artifact_id: UUID, *, now_tick: int) -> FetchResponse:
        """Return artifact view from cache or coordinator."""
        entry = self.cache.get(artifact_id)
        if entry is None or self.strategy.requires_refresh(entry, now_tick=now_tick):
            return self._fetch(artifact_id, now_tick=now_tick)

        touched = self.strategy.on_read(entry, now_tick=now_tick)
        self.cache.put(artifact_id, touched)
        cached_content = self._content_by_artifact.get(artifact_id, "")
        self._record_content_view(
            artifact_id=artifact_id,
            version=touched.local_version,
            content=cached_content,
            source="cache_hit",
            now_tick=now_tick,
        )
        return FetchResponse(
            artifact_id=artifact_id,
            version=touched.local_version,
            content=cached_content,
            state_grant=touched.state,
        )

    def write(
        self,
        artifact_id: UUID,
        *,
        content: str,
        now_tick: int,
        content_hash: str | None = None,
        size_tokens: int | None = None,
    ) -> tuple[Artifact, list[InvalidationSignal]]:
        """Write new artifact content through coordinator protocol."""
        if content_hash is not None:
            computed = compute_content_hash(content)
            if content_hash != computed:
                raise ValueError(
                    f"content_hash mismatch: caller provided {content_hash!r}, "
                    f"computed {computed!r}"
                )

        entry = self.cache.get(artifact_id)
        if entry is None or self.strategy.requires_refresh(entry, now_tick=now_tick):
            self._fetch(artifact_id, now_tick=now_tick)

        write_signals = self.coordinator.write(
            agent_id=self.agent_id,
            artifact_id=artifact_id,
            issued_at_tick=now_tick,
        )
        updated, commit_signals = self.coordinator.commit(
            agent_id=self.agent_id,
            artifact_id=artifact_id,
            content=content,
            issued_at_tick=now_tick,
            content_hash=content_hash,
            size_tokens=size_tokens,
        )
        self.cache.put(
            artifact_id,
            self.strategy.on_fetch(
                artifact_id=artifact_id,
                version=updated.version,
                state=MESIState.MODIFIED,
                now_tick=now_tick,
            ),
        )
        self._record_content_view(
            artifact_id=artifact_id,
            version=updated.version,
            content=content,
            source="write",
            now_tick=now_tick,
        )
        return updated, [*write_signals, *commit_signals]

    def invalidate_all_cache(
        self,
        *,
        invalidated_version: int | None = None,
        issued_at_tick: int = 0,
    ) -> None:
        """Invalidate all local cache entries (recovery use). Does not touch coordinator state."""
        self.cache.invalidate_all(
            invalidated_version=invalidated_version,
            issued_at_tick=issued_at_tick,
        )

    def handle_invalidation(self, signal: InvalidationSignal) -> None:
        """Apply invalidation event from coordinator/event bus."""
        self.cache.invalidate(
            signal.artifact_id,
            invalidated_version=max(signal.new_version - 1, 0),
            issued_at_tick=signal.issued_at_tick,
        )
        self.coordinator.invalidate(
            agent_id=self.agent_id,
            artifact_id=signal.artifact_id,
            new_version=signal.new_version,
            issuer_agent_id=signal.issuer_agent_id,
            issued_at_tick=signal.issued_at_tick,
        )

    def handle_update(
        self,
        *,
        artifact_id: UUID,
        version: int,
        content: str,
        now_tick: int,
        writer_agent_id: UUID | None = None,
    ) -> None:
        """Apply eager-broadcast content update from peer/coordinator."""
        self.cache.put(
            artifact_id,
            self.strategy.on_fetch(
                artifact_id=artifact_id,
                version=version,
                state=MESIState.SHARED,
                now_tick=now_tick,
            ),
        )
        self._record_content_view(
            artifact_id=artifact_id,
            version=version,
            content=content,
            source="broadcast",
            now_tick=now_tick,
        )
        self.coordinator.registry.set_agent_state(
            artifact_id, self.agent_id, MESIState.SHARED, trigger="update", tick=now_tick
        )
        if writer_agent_id is not None:
            self.coordinator.registry.set_agent_state(
                artifact_id, writer_agent_id, MESIState.SHARED, trigger="update", tick=now_tick
            )

    def content(self, artifact_id: UUID) -> str | None:
        """Return locally cached content body for artifact if present."""
        return self._content_by_artifact.get(artifact_id)

    def _fetch(self, artifact_id: UUID, *, now_tick: int) -> FetchResponse:
        response = self.coordinator.fetch(
            FetchRequest(
                artifact_id=artifact_id,
                requesting_agent_id=self.agent_id,
                requested_at_tick=now_tick,
            )
        )
        self.cache.put(
            artifact_id,
            self.strategy.on_fetch(
                artifact_id=artifact_id,
                version=response.version,
                state=response.state_grant,
                now_tick=now_tick,
            ),
        )
        self._record_content_view(
            artifact_id=artifact_id,
            version=response.version,
            content=response.content,
            source="fetch",
            now_tick=now_tick,
        )
        return response

    def _record_content_view(
        self,
        *,
        artifact_id: UUID,
        version: int | None,
        content: str | None,
        source: str,
        now_tick: int,
    ) -> str | None:
        """Record a content delivery event and update local content dict.

        Returns the computed content_hash, or None on error/empty outcomes.
        """
        if content is not None and source != "cache_hit":
            self._content_by_artifact[artifact_id] = content

        if content is None:
            outcome = "error"
            record_version = None
            content_hash = None
        elif version is None or version == 0:
            outcome = "empty"
            record_version = None
            content_hash = None
        else:
            outcome = "content"
            record_version = version
            content_hash = compute_content_hash(content)

        if self._content_audit_log is not None:
            self._audit_seq[0] += 1
            entry: dict[str, Any] = {
                "tick": now_tick,
                "agent_id": str(self.agent_id),
                "agent_name": self._agent_name,
                "artifact_id": str(artifact_id),
                "version": record_version,
                "content_hash": content_hash,
                "source": source,
                "outcome": outcome,
                "sequence_number": self._audit_seq[0],
                "instance_id": self._instance_id,
                "schema_version": CCS_CONTENT_AUDIT_LOG_SCHEMA_VERSION,
            }
            try:
                self._content_audit_log(entry)
            except Exception:
                self._audit_seq[0] -= 1
                raise

        return content_hash

