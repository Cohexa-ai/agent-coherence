# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Structural Protocols for the coordinator's artifact registries.

This module holds the registry CONTRACT the service layer depends on, extracted
as a pure refactor (zero behavior change). Before this extraction the two
registries (:class:`ccs.coordinator.registry.ArtifactRegistry` in-memory and
:class:`ccs.coordinator.sqlite_registry.SqliteArtifactRegistry` durable) were
duck-typed with NO shared interface; their parity was asserted piecemeal in
tests.

The two Protocols below name that shared surface explicitly (the authoritative
method set is the one pinned by ``tests/test_registry_protocol_parity.py``):

- :class:`RegistryBase` — the methods :class:`CoordinatorService` (the service
  layer) depends on. ``ArtifactRegistry`` and ``SqliteArtifactRegistry`` both
  satisfy it.
- :class:`SqliteExtended` — ``RegistryBase`` plus the SQLite-backed methods
  ``coordinator_server.py`` depends on (preemption notices, prefix lookups,
  ``resolve_or_register``, ``status_snapshot``, connection ``close``). Only
  ``SqliteArtifactRegistry`` satisfies it today.

Both are :func:`~typing.runtime_checkable` so ``isinstance`` (structural
presence-of-methods only) and the parity test can verify conformance. The
registries do NOT inherit these Protocols at runtime — conformance is structural,
backed by a ``TYPE_CHECKING``-guarded static assertion in each registry module
and by ``tests/test_registry_protocol_parity.py``.

To avoid an import cycle, this module imports ONLY domain types — never the
registry classes themselves (the registries import this module's Protocols under
``TYPE_CHECKING`` for the conformance assertion).
"""

from __future__ import annotations

from contextlib import AbstractContextManager
from dataclasses import dataclass
from threading import Event
from typing import (
    Any,
    Iterable,
    Mapping,
    Optional,
    Protocol,
    Sequence,
    TypeAlias,
    runtime_checkable,
)
from uuid import UUID

from ccs.core.states import MESIState, TransientState
from ccs.core.types import (
    Artifact,
    CasCorruption,
    CommitAllEntry,
    ConflictDetail,
    MultiCommitConflict,
    MultiCommitResult,
    VersionedReadRejection,
)

from .retention import RetentionPolicy

# Contract return types — DEFINED here in the contract module and re-exported by
# the concrete registries (registry.py / sqlite_registry.py import them back).
# They ARE part of the contract (the return types of its methods); this module
# deduplicated them here (each registry previously defined its own identical copy).
ReclamationSlot: TypeAlias = tuple[str, int]  # (trigger, tick)
# WIN = (updated_artifact, invalidated_agent_ids); loss = ConflictDetail;
# impossible state = CasCorruption. None is raised by the registry.
CasResult: TypeAlias = "tuple[Artifact, list[UUID]] | ConflictDetail | CasCorruption"
# Atomic multi-artifact publish (SB-18 / commit_all): WIN = MultiCommitResult
# (per-member new versions + the aggregated invalidated set); any member blocked =
# MultiCommitConflict (per-member ConflictDetail); any member corrupt = CasCorruption.
# All-or-nothing — never a partial batch. None is raised by the registry.
MultiCasResult: TypeAlias = "MultiCommitResult | MultiCommitConflict | CasCorruption"
# Snapshot consistent-cut capture: WIN = the pinned cut
# {artifact_id: version}; a read_set with an unknown id = VersionedReadRejection,
# NO pins inserted. Neither is raised by the registry.
CaptureResult: TypeAlias = "dict[UUID, int] | VersionedReadRejection"


@dataclass(frozen=True)
class CheckpointRecord:
    """One workspace-checkpoint manifest header (WV plan Unit 2 / R1, R9).

    The contract row both registries store and return — DEFINED here (the
    contract module) like the other contract return types. Frozen: a manifest
    record is a fact; updates go through the targeted registry mutators
    (:meth:`RegistryBase.set_checkpoint_restore_status` /
    :meth:`RegistryBase.adjust_checkpoint_pin_refcount`), never in-place edits.

    ``owner`` is REQUIRED metadata (fail-closed: ``create_checkpoint`` raises on
    an absent owner — an ownerless manifest is unrepresentable). ``window_min`` /
    ``window_max`` are the skew-declared cut window's endpoints (monotonic
    seconds as captured by the engine). ``restore_status`` +
    ``restore_updated_at`` are the checkpoint-level restore-progress fields
    (durable because restore is crash-resumable); the vocabulary is the service
    layer's — the registry stores the string. ``pin_refcount`` is the Unit-6 GC
    pin bookkeeping (never negative; adjusted only through the registry).
    """

    checkpoint_id: str
    name: str
    owner: UUID
    created_at: float
    created_at_tick: int
    window_min: float
    window_max: float
    restore_status: str = "none"
    restore_updated_at: float | None = None
    pin_refcount: int = 0


@dataclass(frozen=True)
class CheckpointMember:
    """One member row of a workspace-checkpoint manifest (WV plan Unit 2 / R1).

    Fixed-width capture facts only — tokens, fingerprints, flags, tiers,
    timestamps; NEVER content bytes. ``member_path`` keys the member within its
    checkpoint; ``artifact_id`` is the optional coordinator artifact ref (an S3
    member has none). ``native_token`` is the member's substrate CAS token as
    captured (opaque here). ``absent`` records absent-at-capture (ABSENT is a
    fact distinct from empty); ``dirty_during_window`` is the torn-cut flag.
    ``arbitration_tier`` (``native-cas`` / ``no-arbiter``) and ``restore_tier``
    (``restorable`` / ``restorable-unpinned`` / ``forward_only``) default to the
    WEAKEST claims — honesty is the default, upgrades are explicit.
    ``restore_outcome`` + ``deleted_at_restore`` are the per-member
    restore-progress columns: the typed terminal outcome a crash-resumable
    restore records, and the manifest-side delete record (``commit_all`` has no
    delete semantics, so delete legs are recorded here per the plan's
    restore-registration design).
    """

    member_path: str
    artifact_id: UUID | None
    native_token: str | None
    fingerprint: str | None
    captured_at: float
    absent: bool = False
    dirty_during_window: bool = False
    arbitration_tier: str = "no-arbiter"
    restore_tier: str = "forward_only"
    pin_state: str = "unpinned"
    restore_outcome: str | None = None
    deleted_at_restore: float | None = None

# Shared registry trigger constants — DEFINED here (canonical) and re-exported by
# both registries, which previously each kept an identical copy pinned equal by
# the dual-registry parity test.
#
# RECLAIM_TRIGGERS: the coordinator-side EVICTION triggers (the stable-grant
# sweep's reclaim_heartbeat / reclaim_max_hold + the transient-timeout fail-safe
# "timeout"). An M/E -> INVALID transition carrying one of these bumps the
# artifact's owner_generation (the read-generation fence): the claim was revoked
# WITHOUT a version move, which version-CAS cannot see. Any other
# (peer-invalidation) INVALID does NOT bump — that path moves the version, so
# version-CAS already catches a stale write.
RECLAIM_TRIGGERS: frozenset[str] = frozenset(
    {"reclaim_heartbeat", "reclaim_max_hold", "timeout"}
)
# CLAIM_CAPTURE_TRIGGERS: triggers marking a GENUINE content read for
# read-generation capture (the E/M-acquire capture is keyed on the state
# transition, not the trigger). Service.fetch() emits "fetch"; renaming it
# without updating this would silently disable capture on reads.
CLAIM_CAPTURE_TRIGGERS: frozenset[str] = frozenset({"fetch"})


@runtime_checkable
class RegistryBase(Protocol):
    """The registry contract the service layer (:class:`CoordinatorService`)
    depends on — the methods shared by both the in-memory and SQLite-backed
    registries.

    Extracted as a pure refactor (no behavior change): it names the
    previously-implicit duck-type both registries already satisfied. The
    in-memory :class:`~ccs.coordinator.registry.ArtifactRegistry` is the
    canonical shape; the durable
    :class:`~ccs.coordinator.sqlite_registry.SqliteArtifactRegistry` mirrors it.

    Note on :meth:`get_content`: the in-memory registry returns ``Optional[str]``
    while the SQLite registry returns ``Optional[bytes]`` (it returns ``b""`` for
    known artifacts). The honest union return type is therefore
    ``str | bytes | None``.
    """

    def abort_guard(self, abort: "Event | None" = None) -> AbstractContextManager[None]:
        ...

    def adjust_checkpoint_pin_refcount(self, checkpoint_id: str, delta: int) -> int:
        """Atomically add ``delta`` to a checkpoint's pin refcount and return the
        new value. Raises ``KeyError`` for an unknown checkpoint and
        ``ValueError`` if the result would go negative (a release without a
        matching pin is a bookkeeping bug, fail-closed)."""
        ...

    def all_session_meta(self) -> "dict[str, tuple[UUID, int]]":
        ...

    def artifact_ids(self) -> list[UUID]:
        ...

    def capture_version_vector(
        self,
        read_set: "Iterable[UUID]",
        session_token: str,
        *,
        owner: "UUID | None" = None,
        created_at_tick: int | None = None,
    ) -> CaptureResult:
        ...

    def clear_agent_transient(self, artifact_id: UUID, agent_id: UUID) -> None:
        ...

    def commit_cas(
        self,
        artifact_id: UUID,
        agent_id: UUID,
        *,
        expected_version: int,
        content_hash: str,
        size_tokens: int | None = None,
        content: bytes | str | None = None,
        tick: int = 0,
        trigger: str = "commit_cas",
    ) -> CasResult:
        ...

    def commit_all(
        self,
        agent_id: UUID,
        writes: Mapping[UUID, CommitAllEntry],
        *,
        tick: int = 0,
        trigger: str = "commit_all",
    ) -> MultiCasResult:
        """Atomic multi-artifact publish (SB-18 / commit_all): commit ``writes``
        all-or-nothing — every member advances to its next version or none do and
        the batch is HELD. A genuinely new atomic multi-row op, never a loop of
        :meth:`commit_cas`. Implemented on BOTH backends with identical outcomes
        (parity)."""
        ...

    def create_checkpoint(
        self,
        checkpoint: CheckpointRecord,
        members: Sequence[CheckpointMember],
    ) -> None:
        """Persist a checkpoint manifest — the header row (owner metadata
        INCLUDED, same transaction) plus every member row — atomically: all rows
        land or none do. Raises ``ValueError`` on an absent owner (fail-closed:
        an ownerless manifest is never persisted), on a duplicate
        ``checkpoint_id``, and on duplicate member paths within the manifest."""
        ...

    @property
    def coordinator_epoch(self) -> str:
        """Fence token identifying this coordinator incarnation. A ``@property``
        on both registries; read by :class:`CoordinatorService` on the read-fence
        and session paths (``read_at_version`` / ``begin_session`` /
        ``session_read`` / ``session_commit``). Declared here so a backend typed
        against ``RegistryBase`` cannot omit it and pass ``isinstance`` yet fail
        at the first fence read."""
        ...

    def get_agent_state(self, artifact_id: UUID, agent_id: UUID) -> MESIState | None:
        ...

    def get_agent_transient(self, artifact_id: UUID, agent_id: UUID) -> TransientState | None:
        ...

    def get_artifact(self, artifact_id: UUID) -> Optional[Artifact]:
        ...

    def get_checkpoint(self, checkpoint_id: str) -> CheckpointRecord | None:
        ...

    def get_checkpoint_members(self, checkpoint_id: str) -> list[CheckpointMember]:
        """Return the manifest's member rows ordered by ``member_path`` (empty
        list for an unknown checkpoint — the header getter tells known from
        unknown)."""
        ...

    def get_content(self, artifact_id: UUID) -> str | bytes | None:
        ...

    def get_content_at_version(self, artifact_id: UUID, version: int) -> str | bytes | None:
        ...

    def get_last_reclamation(
        self, agent_id: UUID, artifact_id: UUID
    ) -> ReclamationSlot | None:
        ...

    def get_owner_generation(self, artifact_id: UUID) -> int:
        ...

    def get_read_generation(self, artifact_id: UUID, agent_id: UUID) -> int | None:
        ...

    def get_session_cut(self, session_token: str) -> dict[UUID, int] | None:
        ...

    def get_session_meta(self, session_token: str) -> "tuple[UUID, int] | None":
        ...

    def get_state_map(self, artifact_id: UUID) -> dict[UUID, MESIState]:
        ...

    def get_transient_map(self, artifact_id: UUID) -> dict[UUID, TransientState]:
        ...

    def get_transient_tick(self, artifact_id: UUID, agent_id: UUID) -> int | None:
        ...

    def get_artifact_and_generation(
        self, artifact_id: UUID
    ) -> "tuple[Artifact, int] | None":
        """Return ``(artifact, owner_generation)`` from ONE snapshot, or None if
        the artifact is absent. The pair MUST have coexisted at a single
        instant: a backend serving it as two independent reads lets a concurrent
        sweep reclamation (which bumps the generation WITHOUT a version move)
        tear the pair, silently reopening the reclaim-zombie EFFECT hole
        downstream (see ``adapters.effect_gate``). Any caller needing a
        version and its ownership epoch together must use this, never two
        separate accessors.
        """
        ...

    def get_version_record(
        self, artifact_id: UUID, version: int
    ) -> tuple[str | bytes, float] | None:
        ...

    def granted_at_tick(self, agent_id: UUID, artifact_id: UUID) -> int | None:
        ...

    def has_artifact(self, artifact_id: UUID) -> bool:
        ...

    def last_heartbeat_tick(self, agent_id: UUID) -> int | None:
        ...

    def last_observed_version_for(self, artifact_id: UUID, agent_id: UUID) -> int | None:
        """Return the artifact version whose bytes this agent last observed
        (SB-10: recorded atomically with every non-INVALID grant/commit upsert),
        or None when the pair was never observed. Absence semantics are part of
        the contract: never a 0-sentinel, and a transition to INVALID preserves
        the prior recorded value — this is the durable comparand the
        post-compaction stale flag is computed from."""
        ...

    def list_checkpoints(self) -> list[CheckpointRecord]:
        """Return every checkpoint header, ordered by ``(created_at,
        checkpoint_id)`` (deterministic for the CLI ``list`` verb)."""
        ...

    def record_heartbeat(self, agent_id: UUID, now_tick: int) -> None:
        ...

    def record_last_reclamation(
        self, agent_id: UUID, artifact_id: UUID, trigger: str, tick: int
    ) -> None:
        ...

    def register_artifact(self, artifact: Artifact, content: str) -> None:
        ...

    def release_session(self, session_token: str) -> None:
        ...

    def remove_artifact(self, artifact_id: UUID) -> None:
        ...

    def retention_meta(self) -> tuple[bool, RetentionPolicy | None]:
        ...

    def session_count(self) -> int:
        ...

    def set_agent_state(
        self,
        artifact_id: UUID,
        agent_id: UUID,
        state: MESIState,
        *,
        trigger: str = "unknown",
        tick: int = 0,
        content_hash: str | None = None,
    ) -> None:
        ...

    def set_agent_transient(
        self,
        artifact_id: UUID,
        agent_id: UUID,
        transient_state: TransientState,
        *,
        entered_tick: int,
    ) -> None:
        ...

    def set_checkpoint_member_pin(
        self,
        checkpoint_id: str,
        member_path: str,
        *,
        pin_state: str,
        restore_tier: str | None = None,
    ) -> None:
        """Update a member's pin state; ``restore_tier`` (when given) rewrites
        the member's restore tier in the same step — the Unit-6 loud tier
        downgrade (``restorable`` -> ``restorable-unpinned`` on a failed pin
        leg). ``restore_tier=None`` leaves the tier untouched. Raises
        ``KeyError`` for an unknown (checkpoint, member) pair."""
        ...

    def set_checkpoint_member_restore(
        self,
        checkpoint_id: str,
        member_path: str,
        *,
        restore_outcome: str | None,
        deleted_at_restore: float | None = None,
    ) -> None:
        """Record a member's restore progress: BOTH columns are written to the
        given values (a full member-restore-state write — a new restore run's
        first write for a member resets any prior run's delete record). Raises
        ``KeyError`` for an unknown (checkpoint, member) pair."""
        ...

    def set_checkpoint_restore_status(
        self, checkpoint_id: str, status: str, *, updated_at: float
    ) -> None:
        """Update the checkpoint-level restore status + its ``updated_at``
        stamp. Raises ``KeyError`` for an unknown checkpoint."""
        ...

    def set_artifact_and_content(
        self,
        artifact_id: UUID,
        artifact: Artifact,
        content: str,
        *,
        last_writer: Optional[UUID] = None,
        fence_agent_id: Optional[UUID] = None,
    ) -> None:
        ...

    def valid_holders(self, artifact_id: UUID) -> list[UUID]:
        ...


@runtime_checkable
class SqliteExtended(RegistryBase, Protocol):
    """The extended registry surface ``coordinator_server.py`` depends on —
    :class:`RegistryBase` plus the methods that only the SQLite-backed
    registry (:class:`~ccs.coordinator.sqlite_registry.SqliteArtifactRegistry`)
    provides today.

    These cover the durable-store-only concerns: connection ``close``, durable
    name/prefix lookups, the preemption-notice surface (record/peek/pop/evict),
    ``resolve_or_register`` first-observation seeding, and the ``status_snapshot``
    batch. Extracted as a pure refactor (no behavior change).
    """

    def artifact_names_under_prefix(self, prefix: str) -> list[str]:
        ...

    def artifacts_held_by_agent(
        self, agent_id: UUID, states: Iterable[MESIState]
    ) -> list[UUID]:
        ...

    def close(self) -> None:
        ...

    def evict_stale_notices(
        self, *, max_age_sec: float, now_unix: Optional[float] = None
    ) -> int:
        ...

    def get_artifact_updated_at(self, artifact_id: UUID) -> Optional[float]:
        ...

    def last_writer_for(self, artifact_id: UUID) -> Optional[UUID]:
        ...

    def lookup_artifact_id_by_name(self, parent_rel_path: str) -> UUID | None:
        ...

    def peek_preemption_notice(
        self, agent_id: UUID, artifact_id: UUID
    ) -> Optional[tuple[UUID, float]]:
        ...

    def pop_pending_notices(
        self, agent_id: UUID
    ) -> list[tuple[UUID, UUID, float]]:
        ...

    def pop_preemption_notice(
        self, agent_id: UUID, artifact_id: UUID
    ) -> Optional[tuple[UUID, float]]:
        ...

    def record_preemption_notice(
        self,
        *,
        victim_agent_id: UUID,
        artifact_id: UUID,
        preempter_agent_id: UUID,
        preempted_at_unix_ts: float,
    ) -> None:
        ...

    def resolve_or_register(
        self,
        parent_rel_path: str,
        content_hash: str,
        *,
        initial_owner: Optional[UUID] = None,
    ) -> UUID:
        ...

    def status_snapshot(
        self,
    ) -> tuple[
        dict[UUID, dict[str, Any]],
        dict[UUID, dict[UUID, MESIState]],
    ]:
        ...
