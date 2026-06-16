# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Base strategy contract for synchronization policy decisions."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import replace
from uuid import UUID

from ccs.core.states import MESIState
from ccs.core.types import ArtifactCacheEntry


class SyncStrategy(ABC):
    """Interface for policy-specific cache refresh and propagation rules."""

    name: str

    def invalidates_peers_on_commit(self) -> bool:
        """Whether peers should receive invalidation when writer commits."""
        return True

    def max_cas_retries(self) -> int:
        """Max retry attempts the OCC caller may make after a commit-CAS conflict.

        Policy knob only: the retry *loop* (re-read -> recompute -> commit_cas)
        lives in the caller/``AgentRuntime`` (the actor), never in the strategy
        (D1 "retry is policy, strategy-layer"; strategies are policy, not actors).
        Total commit_cas attempts is ``max_cas_retries() + 1`` (initial + retries).
        Conservative default; a strategy MAY override (e.g. to tune retries vs a
        lease TTL). There is deliberately NO auto-escalation to EXCLUSIVE (D5).
        """
        return 3

    def cas_backoff_ticks(self, attempt: int) -> int:
        """Deterministic, non-negative backoff (in ticks) before retry ``attempt``.

        ``attempt`` is 0-based (0 == the first retry after the initial attempt).
        Default is a capped exponential schedule (0, 1, 2, 4, 8, ... up to 16):
        the first retry is immediate so a single transient conflict re-reads with
        no delay, then growth bounds livelock under sustained contention. Pure
        function of ``attempt`` (no wall clock, no RNG) so behavior is testable
        and reproducible. Negative ``attempt`` clamps to no delay.
        """
        if attempt <= 0:
            return 0
        return min(1 << (attempt - 1), 16)

    def broadcasts_content_on_commit(self) -> bool:
        """Whether the strategy pushes full content on commit."""
        return False

    def broadcasts_every_tick(self) -> bool:
        """Whether the strategy injects full content to all agents each tick."""
        return False

    def staleness_bound(self) -> int | None:
        """Max stale steps permitted by strategy; None means unbounded by strategy."""
        return None

    @abstractmethod
    def requires_refresh(self, entry: ArtifactCacheEntry, *, now_tick: int) -> bool:
        """Return whether runtime must fetch before local read."""

    @abstractmethod
    def on_read(self, entry: ArtifactCacheEntry, *, now_tick: int) -> ArtifactCacheEntry:
        """Return updated cache entry after local read access."""

    @abstractmethod
    def on_fetch(
        self,
        *,
        artifact_id: UUID,
        version: int,
        state: MESIState,
        now_tick: int,
    ) -> ArtifactCacheEntry:
        """Return refreshed cache entry after coordinator fetch."""

    def _new_entry(
        self,
        *,
        artifact_id: UUID,
        version: int,
        state: MESIState,
        now_tick: int,
    ) -> ArtifactCacheEntry:
        return ArtifactCacheEntry(
            artifact_id=artifact_id,
            state=state,
            local_version=version,
            access_count=0,
            acquired_at_tick=now_tick,
            expires_at_tick=None,
            transient_state=None,
            transient_entered_tick=None,
        )

    def _touch(self, entry: ArtifactCacheEntry) -> ArtifactCacheEntry:
        """Increment local access counter for telemetry and policy checks."""
        return replace(entry, access_count=entry.access_count + 1)
