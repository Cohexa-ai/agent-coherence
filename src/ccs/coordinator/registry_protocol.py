# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Structural Protocols for the coordinator's artifact registries.

This module holds the registry CONTRACT the service layer depends on, extracted
in Phase 1 of the cross-host production-HA journey (PURE REFACTOR — zero
behavior change). Before this extraction the two registries
(:class:`ccs.coordinator.registry.ArtifactRegistry` in-memory and
:class:`ccs.coordinator.sqlite_registry.SqliteArtifactRegistry` durable) were
duck-typed with NO shared interface; their parity was asserted piecemeal in
tests.

The two Protocols below name that shared surface explicitly:

- :class:`RegistryBase` — the 34 methods :class:`CoordinatorService` (the
  service layer) depends on. ``ArtifactRegistry`` and ``SqliteArtifactRegistry``
  both satisfy it.
- :class:`SqliteExtended` — ``RegistryBase`` plus the 13 SQLite-backed methods
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
from threading import Event
from typing import Any, Iterable, Optional, Protocol, TypeAlias, runtime_checkable
from uuid import UUID

from ccs.core.states import MESIState, TransientState
from ccs.core.types import Artifact, CasCorruption, ConflictDetail, VersionedReadRejection

from .retention import RetentionPolicy

# Contract return types — DEFINED here in the contract module and re-exported by
# the concrete registries (registry.py / sqlite_registry.py import them back).
# They ARE part of the contract (the return types of its methods); Phase 1
# deduplicated them here (each registry previously defined its own identical copy).
ReclamationSlot: TypeAlias = tuple[str, int]  # (trigger, tick)
# WIN = (updated_artifact, invalidated_agent_ids); loss = ConflictDetail;
# impossible state = CasCorruption. None is raised by the registry.
CasResult: TypeAlias = "tuple[Artifact, list[UUID]] | ConflictDetail | CasCorruption"
# Snapshot consistent-cut capture (SB-17 / TX-1): WIN = the pinned cut
# {artifact_id: version}; a read_set with an unknown id = VersionedReadRejection,
# NO pins inserted. Neither is raised by the registry.
CaptureResult: TypeAlias = "dict[UUID, int] | VersionedReadRejection"


@runtime_checkable
class RegistryBase(Protocol):
    """The registry contract the service layer (:class:`CoordinatorService`)
    depends on — the 34 methods shared by both the in-memory and SQLite-backed
    registries.

    Extracted in Phase 1 as a PURE REFACTOR (no behavior change): it names the
    previously-implicit duck-type both registries already satisfied. The
    in-memory :class:`~ccs.coordinator.registry.ArtifactRegistry` is the
    canonical shape; the durable
    :class:`~ccs.coordinator.sqlite_registry.SqliteArtifactRegistry` mirrors it.

    Note on :meth:`get_content`: the in-memory registry returns ``Optional[str]``
    while the SQLite registry returns ``Optional[bytes]`` (the KTD-13 contract
    divergence — SQLite returns ``b""`` for known artifacts). The honest union
    return type is therefore ``str | bytes | None``.
    """

    def abort_guard(self, abort: "Event | None" = None) -> AbstractContextManager[None]:
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

    def get_agent_state(self, artifact_id: UUID, agent_id: UUID) -> MESIState | None:
        ...

    def get_agent_transient(self, artifact_id: UUID, agent_id: UUID) -> TransientState | None:
        ...

    def get_artifact(self, artifact_id: UUID) -> Optional[Artifact]:
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
    :class:`RegistryBase` plus the 13 methods that only the SQLite-backed
    registry (:class:`~ccs.coordinator.sqlite_registry.SqliteArtifactRegistry`)
    provides today.

    These cover the durable-store-only concerns: connection ``close``, durable
    name/prefix lookups, the preemption-notice surface (record/peek/pop/evict),
    ``resolve_or_register`` first-observation seeding, and the ``status_snapshot``
    batch. Extracted in Phase 1 as a PURE REFACTOR (no behavior change).
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
