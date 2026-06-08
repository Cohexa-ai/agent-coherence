# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""Core domain dataclasses for artifact coherence."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal, Optional
from uuid import UUID, uuid4

from .states import MESIState, TransientState


@dataclass(frozen=True)
class Artifact:
    """Named shared artifact tracked by the coherence coordinator."""

    id: UUID = field(default_factory=uuid4)
    name: str = ""
    version: int = 0
    content_hash: Optional[str] = None
    size_tokens: Optional[int] = None
    depends_on: tuple[UUID, ...] = ()


@dataclass(frozen=True)
class ArtifactCacheEntry:
    """Per-agent cached view of one artifact."""

    artifact_id: UUID
    state: MESIState
    local_version: int
    access_count: int = 0
    acquired_at_tick: int = 0
    expires_at_tick: Optional[int] = None
    transient_state: Optional[TransientState] = None
    transient_entered_tick: Optional[int] = None


@dataclass(frozen=True)
class ConflictDetail:
    """Typed result of an optimistic-concurrency commit that did NOT mutate.

    Returned (never raised) by the registry ``commit_cas`` primitive and the
    service ``commit_cas`` orchestration when a compare-and-swap loses the
    race (OCC write API, plan R2 / R-OCC-2). The two retry-eligible reasons:

    - ``"version_mismatch"`` — the caller's ``expected_version`` is *behind*
      the registry's current version (another writer committed first). Two
      concurrent OCC writers (both SHARED) are arbitrated here: the serialized
      transaction lets the first win and the second observes this.
    - ``"other_holder"`` — the version matched, but a *pessimistic* peer holds
      MODIFIED or EXCLUSIVE during the OCC compute window (OCC-vs-pessimistic
      coexistence guard). Not how two OCC writers are arbitrated.

    Both are retry-eligible (re-read → recompute → retry). ``current_version``
    is the registry's authoritative version at the point the conflict was
    detected, so the caller can re-seed its retry. Corruption
    (``expected_version > current``) is signalled separately (it is never a
    ``ConflictDetail``) and the service layer raises ``CoherenceError`` for it.
    """

    reason: Literal["version_mismatch", "other_holder"]
    current_version: int


@dataclass(frozen=True)
class CasCorruption:
    """Typed registry signal that an OCC compare-and-swap saw an impossible
    state: ``expected_version > current_version``.

    A correct single-coordinator system cannot produce this — an honest writer
    only ever observes a version ≤ the current one. It indicates corruption or
    a second coordinator writing the same store. Kept DISTINCT from
    ``ConflictDetail`` (a retry-eligible conflict) so the service layer can map
    it to a non-retryable ``CoherenceError`` (plan R2). The registry returns
    this sentinel rather than raising so all in-transaction outcomes are typed
    returns and the ``BEGIN IMMEDIATE`` region stays a single clean commit path.
    """

    current_version: int


@dataclass(frozen=True)
class InvalidationSignal:
    """Lightweight invalidation signal sent to agents."""

    artifact_id: UUID
    new_version: int
    issued_at_tick: int
    issuer_agent_id: UUID


@dataclass(frozen=True)
class FetchRequest:
    """Request to fetch canonical artifact content/version."""

    artifact_id: UUID
    requesting_agent_id: UUID
    requested_at_tick: int


@dataclass(frozen=True)
class FetchResponse:
    """Fetch response containing granted state and content payload."""

    artifact_id: UUID
    version: int
    content: str
    state_grant: MESIState

