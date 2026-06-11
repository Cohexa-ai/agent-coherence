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
    race (OCC write API, plan R2 / R-OCC-2). The three retry-eligible reasons:

    - ``"version_mismatch"`` — the caller's ``expected_version`` is *behind*
      the registry's current version (another writer committed first). Two
      concurrent OCC writers (both SHARED) are arbitrated here: the serialized
      transaction lets the first win and the second observes this.
    - ``"other_holder"`` — the version matched, but a *pessimistic* peer holds
      MODIFIED or EXCLUSIVE during the OCC compute window (OCC-vs-pessimistic
      coexistence guard). Not how two OCC writers are arbitrated.
    - ``"stale_read_generation"`` — the version matched and no peer holds M/E,
      but the committer's CAPTURED read_generation was superseded by a sweep
      reclamation — the read-generation fence (Piece #2). Only the generation
      catches this; version-CAS cannot (the version is unchanged on a
      no-successor reclaim). An absent read_generation is NOT this conflict
      (a plain OCC writer is arbitrated by version-CAS).

    Both are retry-eligible (re-read → recompute → retry). ``current_version``
    is the registry's authoritative version at the point the conflict was
    detected, so the caller can re-seed its retry. Corruption
    (``expected_version > current``) is signalled separately (it is never a
    ``ConflictDetail``) and the service layer raises ``CoherenceError`` for it.
    """

    reason: Literal["version_mismatch", "other_holder", "stale_read_generation"]
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
class VersionedContent:
    """A successfully resolved retained version (plan item N v1, Unit 4 / R5).

    The WIN return of :meth:`CoordinatorService.read_at_version` — a typed
    return (the ``ConflictDetail`` discipline), never an exception. Carries the
    exact retained body and the forensic metadata a consumer needs:

    - ``content`` is the registry-committed body with its ORIGINAL Python type
      (``str`` for a TEXT row, ``bytes`` for a BLOB row — the sqlite
      affinity-NONE column round-trips by value; the in-memory record stores the
      body as-supplied). It is "the content the registry committed", not what
      reached any client's disk (the watchdog-ack honesty boundary).
    - ``captured_at`` is the wall-clock ``time.time()`` capture timestamp — kept
      on the success surface because forensic consumers need it (omitting it
      would bake a surface change into the first consumer's contract) and it is
      also the T-expiry reference.
    - ``coordinator_epoch`` is the store's epoch, stamped on every answer so a
      consumer can pin which store-incarnation served the bytes.

    The current version is never served here (``read_at_version`` rejects
    ``version == current`` with ``current_version``): this is a HISTORY surface.
    """

    artifact_id: UUID
    version: int
    content: str | bytes
    captured_at: float
    coordinator_epoch: str


@dataclass(frozen=True)
class VersionedReadRejection:
    """A typed read-at-version rejection (plan item N v1, Unit 4 / R5).

    The non-WIN return of :meth:`CoordinatorService.read_at_version` — a typed
    return (the ``ConflictDetail`` discipline), never an exception. ``reason`` is
    exactly one of the six wire-stable constants in
    :mod:`ccs.core.exceptions` (:data:`~ccs.core.exceptions.READ_AT_VERSION_REASONS`);
    consumers match with ``==`` against that set, never on a human message.

    **Carries NO content, NO content hash, NO body material** — only the reason,
    the requested/current versions, and the epoch. This is enforced by a test
    that pins the exact field set: a rejection is the one surface that must never
    leak bytes (e.g. an ``epoch_mismatch`` from a different store-incarnation, or
    a ``future_version`` that hints at a second coordinator), so the dataclass
    structurally cannot.

    Field semantics:

    - ``current_version`` is the registry's authoritative current version at the
      point the reason was decided, or ``None`` when it genuinely cannot be known
      (``unknown_artifact`` — there is no current version for a missing
      artifact). The single-scope read means a racing commit cannot mislabel it.
    - ``coordinator_epoch`` is the store's epoch (always populated — the registry
      always has one), so even a rejection tells the consumer which store
      answered.
    """

    reason: str
    artifact_id: UUID
    requested_version: int
    current_version: int | None
    coordinator_epoch: str | None


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

