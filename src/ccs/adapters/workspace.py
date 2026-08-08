# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""WorkspaceVersioner — the Workspace-Versioning capture engine (WV Unit 3).

A workspace is a set of heterogeneous MEMBERS: files on a
:class:`~ccs.adapters.coherent_volume.CoherentVolume` (or any source speaking
its ``read_with_version`` surface), S3 objects on a
:class:`~ccs.adapters.coherent_object.CoherentObject`, and declared
forward-only members (actions/effects with no state to capture).
:meth:`WorkspaceVersioner.checkpoint` takes a **skew-declared cut** over them:

- **Per-member capture** — one read per member yields the restore POINTER
  (``native_token``: the S3 versionId / the coordinator content-state version
  for a file), a fixed-width fingerprint, and a
  :func:`~ccs.core.clock.monotonic_seconds` timestamp. The CAS comparand (the
  S3 ETag / the live coordinator version at restore time) is NEVER manifested
  — it is re-read live when a restore leg runs (the F4 pointer-vs-comparand
  split).
- **ABSENT is a fact, not an empty body** — a missing member is recorded
  ``absent=True`` with no token and no fingerprint, distinct from a present
  empty member (whose fingerprint is ``sha256(b"")``).
- **Honest tiers per member** — derived through
  :func:`~ccs.core.substrate.derive_restore_tier`, never asserted: a versioned
  S3 member is ``restorable`` (the substrate offers history + a per-version
  pin; the Unit-6 pin leg establishes the hold and downgrades LOUDLY on
  failure), an unversioned S3 member is ``forward_only`` (the typed
  :class:`~ccs.adapters.coherent_object.VersionPointerUnconfirmed` refusal IS
  the discovery — no pre-probe), a file member is ``restorable-unpinned``
  (coordinator retention holds history; the retention pin is Unit 6), and a
  member whose pointer cannot be confirmed is ``forward_only`` (an unconfirmed
  pointer never lands in a manifest — the Sentinel rule).
- **Torn-cut detection** — after the capture window closes, EVERY captured
  member is re-read once; any observed movement (token, fingerprint, or
  presence) marks that member ``dirty_during_window=True``. Conservative by
  design: the verification read runs after ``window_max``, so a write landing
  between the window close and the verify read still flags — dirty means "not
  verified quiescent across the window", never "verified torn".
- **The window** — ``[window_min, window_max]`` is the min/max of the members'
  capture timestamps (whole-second wall-clock ticks; the skew is DECLARED, not
  hidden).
- **One registration** — the manifest persists through the coordinator
  service's ``create_workspace_checkpoint`` (the Unit-2 registry API: header +
  owner + every member in ONE transaction). Any persist failure raises the
  typed :class:`CheckpointPersistFailed`; the single-transaction registration
  means a raise leaves NO partial manifest.

v1 limitation (typed, capture-time): a file member whose bytes are not UTF-8
text is refused with :class:`BinaryFileMemberRefused` BEFORE anything persists
— the file restore leg rides the snapshot-session text wire (the same
constraint ``CoherentVolume.atomic_publish`` enforces), so capturing a member
that could never be restored would be a silent over-claim.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Final, Protocol, Sequence, runtime_checkable
from uuid import UUID

from ccs.adapters.coherent_object import CoherentObject, VersionPointerUnconfirmed
from ccs.coordinator.registry_protocol import CheckpointMember, CheckpointRecord
from ccs.core.clock import monotonic_seconds
from ccs.core.exceptions import CoherenceError
from ccs.core.substrate import ArbitrationTier, derive_restore_tier
from ccs.core.substrate import sha256_hex as _sha256_hex

if TYPE_CHECKING:
    from typing import Callable

__all__ = [
    "BINARY_FILE_MEMBER_REASON",
    "BinaryFileMemberRefused",
    "CHECKPOINT_NOT_PERSISTED_REASON",
    "CheckpointPersistFailed",
    "CheckpointPersistence",
    "FileMemberSource",
    "WorkspaceCheckpoint",
    "WorkspaceVersioner",
]


# --- typed-reason vocabulary (identity-matched constants; add, never rename) ----

# A file member's bytes are not UTF-8 text: the file restore leg rides the
# snapshot-session TEXT wire, so the member could never be restored — refuse at
# capture (typed), never silently capture an unrestorable member.
BINARY_FILE_MEMBER_REASON: Final[str] = "binary_file_member_unsupported"

# The checkpoint manifest was NOT persisted: the coordinator/registry raised
# during the single-transaction registration, so no header and no member row
# landed (no partial manifest — the registration is all-or-nothing).
CHECKPOINT_NOT_PERSISTED_REASON: Final[str] = "checkpoint_not_persisted"


class BinaryFileMemberRefused(CoherenceError):
    """A file member holds non-UTF-8 bytes — the typed capture-time refusal.

    The v1 file restore leg rides the snapshot-session text wire
    (``CoherentVolume.atomic_publish`` enforces the same UTF-8 constraint), so
    a binary member can never be restored; capturing it would over-claim.
    Raised BEFORE anything persists. Carries
    :data:`BINARY_FILE_MEMBER_REASON`, matched by identity.
    """

    reason = BINARY_FILE_MEMBER_REASON
    #: The member the refusal names (the operator removes it or accepts that
    #: this workspace cannot be checkpointed in v1).
    member_path: str = ""

    def __init__(self, message: str, *, member_path: str) -> None:
        super().__init__(message)
        self.member_path = member_path


class CheckpointPersistFailed(CoherenceError):
    """The manifest registration raised — the checkpoint was NOT persisted.

    The Unit-2 registration is a single transaction (header + owner + every
    member land together or not at all), so this failure guarantees NO partial
    manifest. The operational cause chains as ``__cause__``. Carries
    :data:`CHECKPOINT_NOT_PERSISTED_REASON`, matched by identity.
    """

    reason = CHECKPOINT_NOT_PERSISTED_REASON


# --- the member-source seams (structural; CoherentVolume / CoordinatorService
# --- satisfy them without importing this module) --------------------------------


@runtime_checkable
class FileMemberSource(Protocol):
    """The file-member read surface: ``CoherentVolume.read_with_version``.

    One call returns ``(bytes, coordinator_version)`` — the bytes and the
    coordinator's authoritative version from the SAME read path, so the
    fingerprint and the restore pointer always describe one observation.
    Raises ``FileNotFoundError`` for an absent member (the ABSENT fact).
    A version ``< 1`` means the pointer could not be resolved (no coordinator
    / degraded) — the capture treats it as UNCONFIRMED (never manifested).
    """

    def read_with_version(self, path: str) -> tuple[bytes, int]:
        ...


@runtime_checkable
class CheckpointPersistence(Protocol):
    """The persist seam: ``CoordinatorService.create_workspace_checkpoint``.

    ONE call registers the whole manifest (header + owner + members) in one
    transaction via the Unit-2 registry API and returns the minted header.
    """

    def create_workspace_checkpoint(
        self,
        *,
        name: str,
        owner: UUID,
        members: Sequence[CheckpointMember],
        window_min: float,
        window_max: float,
        issued_at_tick: int = 0,
    ) -> CheckpointRecord:
        ...


# --- declared members (registration-time facts) ---------------------------------


@dataclass(frozen=True)
class _FileMember:
    member_path: str
    source: FileMemberSource


@dataclass(frozen=True)
class _ObjectMember:
    member_path: str
    binding: CoherentObject
    key: str


@dataclass(frozen=True)
class _ForwardOnlyMember:
    member_path: str


# --- the capture result ---------------------------------------------------------


@dataclass(frozen=True)
class WorkspaceCheckpoint:
    """One persisted checkpoint: the minted header + the member rows as stored.

    ``record.checkpoint_id`` keys later restore/status calls (Units 4–5);
    ``members`` are the exact :class:`CheckpointMember` rows the registry
    holds — capture facts only, never content.
    """

    record: CheckpointRecord
    members: tuple[CheckpointMember, ...]


@dataclass(frozen=True)
class _Observation:
    """One member's live observation — capture pass and verify pass share it.

    ``token`` is the restore pointer (or ``None`` when absent/unconfirmed);
    ``fingerprint`` is the fixed-width content digest (``None`` when absent);
    ``restorable``/``pinnable`` feed :func:`derive_restore_tier` on the
    capture pass (the verify pass compares only presence/token/fingerprint).
    """

    absent: bool
    token: str | None
    fingerprint: str | None
    versioned: bool = False
    pinnable: bool = False


# --- the engine -----------------------------------------------------------------


class WorkspaceVersioner:
    """Skew-declared checkpoint capture over heterogeneous workspace members.

    Declare members first (:meth:`add_file_member` / :meth:`add_object_member`
    / :meth:`add_forward_only_member` — member paths must be unique), then
    :meth:`checkpoint` captures, verifies, and persists ONE manifest through
    the coordinator service. Thread-safe: registration and capture serialize
    on one lock (a checkpoint is a single logical operation; two overlapping
    captures of one versioner would interleave their windows).

    ``clock`` defaults to :func:`~ccs.core.clock.monotonic_seconds` — the ONE
    coordinator tick basis; injectable for deterministic tests only.
    """

    def __init__(
        self,
        *,
        service: CheckpointPersistence,
        owner: UUID,
        clock: "Callable[[], int]" = monotonic_seconds,
    ) -> None:
        if owner is None:
            raise ValueError(
                "WorkspaceVersioner needs an owner: an ownerless manifest is "
                "unrepresentable (fail-closed; the registry enforces it too)"
            )
        self._service = service
        self._owner = owner
        self._clock = clock
        self._lock = threading.Lock()
        self._members: list[_FileMember | _ObjectMember | _ForwardOnlyMember] = []

    # --- member registration --------------------------------------------------

    def add_file_member(self, source: FileMemberSource, path: str) -> None:
        """Declare one file member: ``path`` read through ``source``
        (a :class:`CoherentVolume` or anything speaking its
        ``read_with_version`` surface). ``path`` is the member's manifest key.
        """
        with self._lock:
            self._register(_FileMember(member_path=self._require_new_path(path), source=source))

    def add_object_member(
        self,
        binding: CoherentObject,
        key: str,
        *,
        member_path: str | None = None,
    ) -> None:
        """Declare one S3 object member: ``key`` read through ``binding``.

        ``member_path`` keys the member in the manifest; it is REQUIRED to be
        explicit or defaulted to ``s3://<key>`` — the bucket lives inside the
        binding, and the manifest key only needs to be unique + stable within
        this workspace.
        """
        path = member_path if member_path is not None else f"s3://{key}"
        with self._lock:
            self._register(
                _ObjectMember(
                    member_path=self._require_new_path(path), binding=binding, key=key
                )
            )

    def add_forward_only_member(self, member_path: str) -> None:
        """Declare one forward-only member (an action/effect surface).

        Enumerated in every manifest — the checkpoint DESCRIBES it so a
        restore can say "skipped, forward-only" per member — but never
        token-captured: there is no state to capture and nothing to compare.
        """
        with self._lock:
            self._register(_ForwardOnlyMember(member_path=self._require_new_path(member_path)))

    # --- the capture ----------------------------------------------------------

    def checkpoint(self, name: str) -> WorkspaceCheckpoint:
        """Capture a named checkpoint: cut → verify → persist (one registration).

        Raises :class:`BinaryFileMemberRefused` (typed, BEFORE any persist) for
        a non-UTF-8 file member, and :class:`CheckpointPersistFailed` when the
        coordinator/registry raises during the single-transaction registration
        (no partial manifest either way). ``ValueError`` for an empty member
        set or a blank name (nothing to checkpoint is a caller bug, not a cut).
        """
        if not isinstance(name, str) or not name.strip():
            raise ValueError("checkpoint needs a non-empty name")
        with self._lock:
            if not self._members:
                raise ValueError(
                    "checkpoint needs at least one declared member (an empty "
                    "workspace manifest would describe nothing)"
                )
            rows = self._capture_all(name)
            rows = self._verify_window(rows)
            return self._persist(name, rows)

    # --- capture pass ---------------------------------------------------------

    def _capture_all(self, name: str) -> list[CheckpointMember]:
        """Phase 1 — one capture read per member, in registration order."""
        rows: list[CheckpointMember] = []
        for member in self._members:
            captured_at = float(self._clock())
            if isinstance(member, _ForwardOnlyMember):
                # Enumerated, never token-captured: the weakest-claim defaults
                # (no-arbiter / forward_only) are already the member's truth.
                rows.append(
                    CheckpointMember(
                        member_path=member.member_path,
                        artifact_id=None,
                        native_token=None,
                        fingerprint=None,
                        captured_at=captured_at,
                    )
                )
                continue
            observed = self._observe(member, refuse_binary=True)
            rows.append(
                CheckpointMember(
                    member_path=member.member_path,
                    artifact_id=None,
                    native_token=observed.token,
                    fingerprint=observed.fingerprint,
                    captured_at=captured_at,
                    absent=observed.absent,
                    arbitration_tier=self._arbitration_tier(member),
                    restore_tier=derive_restore_tier(
                        versioned=observed.versioned, pinnable=observed.pinnable
                    ).value,
                )
            )
        return rows

    # --- verification pass (torn-cut detection) -------------------------------

    def _verify_window(self, rows: list[CheckpointMember]) -> list[CheckpointMember]:
        """Phase 2 — after the window closes, re-read every captured member.

        Any movement (presence, token, or fingerprint) marks THAT member
        ``dirty_during_window=True``. Forward-only members carry no state and
        are never verified. Conservative: a write landing after ``window_max``
        but before this re-read still flags (dirty = "not verified quiescent").
        """
        verified: list[CheckpointMember] = []
        member_by_path = {m.member_path: m for m in self._members}
        for row in rows:
            member = member_by_path[row.member_path]
            if isinstance(member, _ForwardOnlyMember):
                verified.append(row)
                continue
            live = self._observe(member, refuse_binary=False)
            dirty = (
                live.absent != row.absent
                or live.token != row.native_token
                or live.fingerprint != row.fingerprint
            )
            if dirty:
                # Frozen rows are replaced, never mutated (the manifest-record
                # discipline); only the torn-cut flag moves.
                row = replace(row, dirty_during_window=True)
            verified.append(row)
        return verified

    # --- one member observation (shared by both passes) -----------------------

    def _observe(
        self, member: _FileMember | _ObjectMember, *, refuse_binary: bool
    ) -> _Observation:
        if isinstance(member, _FileMember):
            return self._observe_file(member, refuse_binary=refuse_binary)
        return self._observe_object(member)

    def _observe_file(self, member: _FileMember, *, refuse_binary: bool) -> _Observation:
        try:
            data, version = member.source.read_with_version(member.member_path)
        except FileNotFoundError:
            return _Observation(absent=True, token=None, fingerprint=None)
        if refuse_binary:
            self._require_utf8_text(member.member_path, data)
        if version < 1:
            # Pointer UNCONFIRMED (no coordinator / degraded resolution): the
            # Sentinel rule — an unconfirmed pointer never lands in a manifest,
            # and the member can never claim a restore tier above forward_only.
            return _Observation(
                absent=False, token=None, fingerprint=_sha256_hex(data), versioned=False
            )
        # File members: coordinator retention holds the version history
        # (versioned=True) but no retention pin exists yet (Unit 6), so the
        # honest tier is restorable-unpinned — derive, never assert.
        return _Observation(
            absent=False,
            token=str(version),
            fingerprint=_sha256_hex(data),
            versioned=True,
            pinnable=False,
        )

    def _observe_object(self, member: _ObjectMember) -> _Observation:
        try:
            read = member.binding.read_versioned(member.key)
        except KeyError:
            return _Observation(absent=True, token=None, fingerprint=None)
        except VersionPointerUnconfirmed:
            # Unversioned bucket — the typed refusal IS the discovery (no
            # pre-probe). The member is still DESCRIBED (fingerprint from a
            # second consistent read) but holds no pointer and can never be
            # restorable: derive_restore_tier(versioned=False) → forward_only.
            try:
                data, _etag = member.binding.read(member.key)
            except KeyError:
                return _Observation(absent=True, token=None, fingerprint=None)
            return _Observation(
                absent=False, token=None, fingerprint=_sha256_hex(data), versioned=False
            )
        # Versioned bucket: the versionId is the manifest pointer (the ETag —
        # the CAS comparand — is deliberately NOT manifested; a restore leg
        # re-reads it live). S3 offers a per-version pin (Object Lock legal
        # hold), so pinnable=True → restorable; the Unit-6 pin leg establishes
        # the hold at capture and downgrades LOUDLY (restorable-unpinned via
        # set_checkpoint_member_pin) where the bucket offers no lock.
        return _Observation(
            absent=False,
            token=read.version_id,
            fingerprint=_sha256_hex(read.data),
            versioned=True,
            pinnable=True,
        )

    # --- persist (one registration) -------------------------------------------

    def _persist(self, name: str, rows: list[CheckpointMember]) -> WorkspaceCheckpoint:
        window_min = min(row.captured_at for row in rows)
        window_max = max(row.captured_at for row in rows)
        try:
            record = self._service.create_workspace_checkpoint(
                name=name,
                owner=self._owner,
                members=rows,
                window_min=window_min,
                window_max=window_max,
                issued_at_tick=int(self._clock()),
            )
        except Exception as exc:
            raise CheckpointPersistFailed(
                f"checkpoint {name!r} was NOT persisted: the coordinator "
                f"registration raised ({type(exc).__name__}). The registration "
                "is a single transaction, so no partial manifest exists — "
                "retry once the coordinator is reachable."
            ) from exc
        return WorkspaceCheckpoint(record=record, members=tuple(rows))

    # --- internals ------------------------------------------------------------

    def _register(self, member: _FileMember | _ObjectMember | _ForwardOnlyMember) -> None:
        self._members.append(member)

    def _require_new_path(self, member_path: str) -> str:
        if not isinstance(member_path, str) or not member_path.strip():
            raise ValueError("member path must be a non-empty string")
        if any(member_path == m.member_path for m in self._members):
            raise ValueError(
                f"duplicate member path {member_path!r}: member paths key the "
                "manifest and must be unique within a workspace"
            )
        return member_path

    @staticmethod
    def _arbitration_tier(member: _FileMember | _ObjectMember) -> str:
        """Who arbitrates a foreign writer racing this member's restore leg.

        S3 members: the substrate itself (an If-Match put — native CAS). File
        members: NOBODY — the volume can only DETECT a foreign edit
        adapter-locally, and the manifest must never present detection as
        substrate arbitration (the cross-host carve-out).
        """
        if isinstance(member, _ObjectMember):
            return ArbitrationTier.NATIVE_CAS.value
        return ArbitrationTier.NO_ARBITER.value

    @staticmethod
    def _require_utf8_text(member_path: str, data: bytes) -> None:
        try:
            data.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise BinaryFileMemberRefused(
                f"file member {member_path!r} holds non-UTF-8 bytes and cannot "
                "be checkpointed (v1 limitation: the file restore leg rides the "
                f"UTF-8 snapshot-session wire; {exc}). Nothing was persisted.",
                member_path=member_path,
            ) from None
