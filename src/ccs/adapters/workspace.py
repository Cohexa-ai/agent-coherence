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

**Restore (WV Unit 4 / R3)** — :meth:`WorkspaceVersioner.restore` drives one
conditional leg per DURABLE member row under a TERMINATION CONTRACT:

- **Every member reaches exactly one terminal outcome** from the closed
  :data:`~ccs.core.exceptions.RESTORE_MEMBER_OUTCOMES` vocabulary — success
  (``restored`` / ``converged``), absorbing (``conflict`` / ``target_lost`` /
  ``forward_only_skipped``), or hold (``held_unconfirmed``). A restore that
  starts always CONCLUDES with a frozen per-member report; per-member failures
  are absorbed into the report, never raised.
- **Bounded re-drive** — each contended leg re-drives at most
  :data:`MAX_RESTORE_LEG_REDRIVES` times (the ``MAX_CAS_REACQUIRES`` twin);
  sustained foreign-writer contention exhausts the budget into the absorbing
  ``conflict``, never a livelock.
- **Per-member honesty** — the S3 leg is NATIVE-CAS (an If-Match put; the
  substrate arbitrates a racing foreign writer); the file leg is
  **no-arbiter**: a version-checked CAS whose foreign-edit signal is
  adapter-local DETECTION only, and every file outcome is labeled so — never
  presented as substrate arbitration (the cross-host carve-out).
- **Crash-resumable from durable state** — progress rides the registry
  (checkpoint ``restore_status``: ``none`` → ``in_progress`` → ``concluded``;
  per-member ``restore_outcome`` rows). A fresh engine restoring an
  ``in_progress`` checkpoint RESUMES: members with a terminal outcome are
  skipped (reported ``resumed_from_prior_run``), the rest are re-driven
  idempotently — a member whose live state already matches the manifest
  concludes ``converged`` WITHOUT a write, so a crash between a landed leg and
  its durable outcome record can never double-apply.
- **Delete legs restore the ABSENT fact** — a member captured ABSENT that
  exists live is deleted (S3: an unconditional-latest ``delete``, minting a
  marker on a versioned bucket; the pre-delete race window is a documented
  residual). Coordinator registration of restored/deleted members is Unit 5 —
  see :meth:`WorkspaceVersioner._registration_seam`.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Final, Protocol, Sequence, runtime_checkable
from uuid import UUID

from ccs.adapters.coherent_object import (
    CREATE_IF_ABSENT,
    CoherentObject,
    VersionedCasWritten,
    VersionPointerUnconfirmed,
)
from ccs.adapters.substrate import CasConflict, ReconcileVerdict
from ccs.coordinator.registry_protocol import CheckpointMember, CheckpointRecord
from ccs.core.clock import monotonic_seconds
from ccs.core.exceptions import (
    RESTORE_MEMBER_OUTCOMES,
    RESTORE_OUTCOME_CONFLICT,
    RESTORE_OUTCOME_CONVERGED,
    RESTORE_OUTCOME_FORWARD_ONLY_SKIPPED,
    RESTORE_OUTCOME_HELD_UNCONFIRMED,
    RESTORE_OUTCOME_RESTORED,
    RESTORE_OUTCOME_TARGET_LOST,
    RESTORE_STATUS_CONCLUDED,
    RESTORE_STATUS_IN_PROGRESS,
    RESTORE_STATUSES,
    CasRetriesExhausted,
    CasVersionConflict,
    CheckpointUnknown,
    CoherenceError,
    CommitUnconfirmed,
    ViewWedged,
)
from ccs.core.substrate import ArbitrationTier, RestoreTier, derive_restore_tier
from ccs.core.substrate import sha256_hex as _sha256_hex

if TYPE_CHECKING:
    from typing import Callable

__all__ = [
    "BINARY_FILE_MEMBER_REASON",
    "BinaryFileMemberRefused",
    "CHECKPOINT_NOT_PERSISTED_REASON",
    "CheckpointPersistFailed",
    "CheckpointPersistence",
    "CheckpointRestoreStore",
    "FileContentResolver",
    "FileMemberSource",
    "FileRestoreTarget",
    "MAX_RESTORE_LEG_REDRIVES",
    "MemberRestoreOutcome",
    "WorkspaceCheckpoint",
    "WorkspaceRestoreReport",
    "WorkspaceVersioner",
]

# Per-leg re-drive budget for a contended restore leg (WV Unit 4 / R3): total
# leg iterations allowed is MAX_RESTORE_LEG_REDRIVES + 1 (the initial attempt
# plus the re-drives) — the twin of ``coherent_volume.MAX_CAS_REACQUIRES`` and
# ``coherent_object.MAX_RETRYABLE_PUT_ATTEMPTS`` (one budget discipline across
# the family). Exhaustion is the absorbing ``conflict`` outcome, never a raise
# and never a livelock: a restore leg racing a sustained foreign writer loses
# honestly and the restore still concludes.
MAX_RESTORE_LEG_REDRIVES: Final[int] = 8


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


@runtime_checkable
class CheckpointRestoreStore(Protocol):
    """The restore engine's durable-store seam (WV Unit 4 / R3):
    ``CoordinatorService``'s workspace-checkpoint read + progress surface.

    Restore is driven FROM these durable reads and records progress THROUGH
    these durable writes — never from an in-memory capture return — so a fresh
    engine (post-crash) resumes from exactly the state the registry holds.
    :meth:`WorkspaceVersioner.restore` verifies its service speaks this surface
    BEFORE touching any state (fail-fast on a capture-only service).
    """

    def get_workspace_checkpoint(self, checkpoint_id: str) -> CheckpointRecord | None:
        ...

    def get_workspace_checkpoint_members(
        self, checkpoint_id: str
    ) -> "list[CheckpointMember]":
        ...

    def set_workspace_checkpoint_restore_status(
        self, checkpoint_id: str, status: str, *, updated_at: float
    ) -> None:
        ...

    def set_workspace_checkpoint_member_restore(
        self,
        checkpoint_id: str,
        member_path: str,
        *,
        restore_outcome: str | None,
        deleted_at_restore: float | None = None,
    ) -> None:
        ...


@runtime_checkable
class FileContentResolver(Protocol):
    """The file-member HISTORY seam: the captured bytes for ``(path, version)``.

    The restore engine reads a file member's pinned bytes through THIS Protocol
    only. Unit 5 wires the real resolution (coordinator retention's
    ``get_content_at_version`` + member-path→artifact-id mapping); Unit-4 tests
    inject a fake. Raises :class:`KeyError` when the version is not retained —
    the engine maps it to the absorbing ``target_lost`` outcome (an expired
    retention window is the file twin of an expired S3 pin).
    """

    def content_at(self, member_path: str, version: int) -> bytes:
        ...


@runtime_checkable
class FileRestoreTarget(FileMemberSource, Protocol):
    """The file-member RESTORE surface: the capture read plus the single-shot
    version-checked CAS write — ``CoherentVolume`` speaks both natively.

    The write leg is **no-arbiter**: ``write_cas_at`` commits iff the
    coordinator's current version equals ``expected_version``, which DETECTS a
    foreign edit adapter-locally (typed
    :class:`~ccs.core.exceptions.CasVersionConflict`) — it is never substrate
    arbitration, and no restore outcome may present it as such (the cross-host
    carve-out). A confirmed win lands ``new_content`` and advances the version
    deterministically to ``expected_version + 1``.
    """

    def write_cas_at(
        self, path: str, expected_version: int, new_content: bytes
    ) -> None:
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


# --- the restore report (frozen facts; durably mirrored in the registry) --------


@dataclass(frozen=True)
class MemberRestoreOutcome:
    """One member's TERMINAL restore outcome — the termination contract's unit.

    ``outcome`` is one of the closed
    :data:`~ccs.core.exceptions.RESTORE_MEMBER_OUTCOMES` (matched by identity —
    ``detail`` is the human line and is never matched). ``attempts`` counts the
    budgeted leg iterations consumed (0 for skips and pre-leg absorbing
    outcomes). ``new_native_token`` is the restore pointer a LANDED write
    minted (S3 versionId / the file's new coordinator version) — the Unit-5
    registration seam consumes it; ``None`` on every non-write path (a delete
    is recorded via ``deleted_at_restore``, its marker id is never a content
    pointer). ``resumed_from_prior_run`` marks a member whose terminal outcome
    a crashed run already recorded durably: reported, never re-driven.
    """

    member_path: str
    outcome: str
    attempts: int
    detail: str
    new_native_token: str | None = None
    deleted_at_restore: float | None = None
    resumed_from_prior_run: bool = False


@dataclass(frozen=True)
class WorkspaceRestoreReport:
    """The complete per-member terminal report one restore run concludes with.

    Returned to the caller AND durably mirrored (each member's ``outcome`` in
    its registry row; ``status`` — always ``concluded`` — on the checkpoint
    header), so the same report is reconstructible from durable state alone.
    """

    checkpoint_id: str
    status: str
    members: tuple[MemberRestoreOutcome, ...]

    @property
    def members_by_path(self) -> "dict[str, MemberRestoreOutcome]":
        """The report keyed by member path (paths are unique per manifest)."""
        return {m.member_path: m for m in self.members}


class _LegBudget:
    """One restore leg's bounded re-drive budget (the ``MAX_CAS_REACQUIRES`` twin).

    :meth:`try_consume` admits at most ``limit + 1`` iterations (the initial
    attempt plus ``limit`` re-drives) and then answers ``None`` forever — the
    caller's absorbing-``conflict`` signal. The increment AND the decision run
    in ONE ``threading.RLock`` critical section (the GIL-TOCTOU discipline: a
    check-then-increment split across the lock could admit an extra attempt
    when paths interleave at a bytecode boundary). The counter is RUN-LOCAL by
    design — the durable truth is the member's terminal ``restore_outcome``,
    so a crash-resumed run re-arms a fresh budget for a member that never
    reached one: each run is individually bounded, and no run can livelock.
    """

    def __init__(self, limit: int) -> None:
        self._lock = threading.RLock()
        self._limit = limit
        self._attempts = 0

    def try_consume(self) -> int | None:
        """Admit one leg iteration: its 1-based number, or ``None`` when spent."""
        with self._lock:
            if self._attempts >= self._limit + 1:
                return None
            self._attempts += 1
            return self._attempts

    @property
    def attempts(self) -> int:
        """Iterations consumed so far (read under the same lock)."""
        with self._lock:
            return self._attempts


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
        file_resolver: FileContentResolver | None = None,
    ) -> None:
        if owner is None:
            raise ValueError(
                "WorkspaceVersioner needs an owner: an ownerless manifest is "
                "unrepresentable (fail-closed; the registry enforces it too)"
            )
        self._service = service
        self._owner = owner
        self._clock = clock
        # The file-member history seam (restore only): pinned bytes for
        # (path, version). Capture never needs it; restore pre-flight requires
        # it for every actionable file member — fail-fast, before any status
        # write (Unit 5 wires the real coordinator-retention resolver).
        self._file_resolver = file_resolver
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

    # --- the restore (WV Unit 4 / R3) -----------------------------------------

    def restore(self, checkpoint_id: str) -> WorkspaceRestoreReport:
        """Restore a checkpoint: one conditional leg per durable member row,
        under the TERMINATION CONTRACT (module docstring, "Restore").

        A restore that starts always CONCLUDES: every member reaches exactly
        one terminal outcome from the closed
        :data:`~ccs.core.exceptions.RESTORE_MEMBER_OUTCOMES` vocabulary, the
        checkpoint's ``restore_status`` moves ``in_progress`` → ``concluded``,
        and the frozen :class:`WorkspaceRestoreReport` is returned AND durably
        mirrored. Per-member failures are ABSORBED into the report — the only
        raises are pre-flight, before any status write: the typed
        :class:`~ccs.core.exceptions.CheckpointUnknown` for an unknown id,
        ``TypeError`` for a service that lacks the restore surface, and
        ``ValueError`` for missing member bindings / a missing file resolver
        (caller misconfiguration must never mint an ``in_progress`` record it
        cannot drive).

        Crash-resume: a checkpoint found ``in_progress`` is RESUMED — members
        whose durable ``restore_outcome`` is already terminal are skipped
        (reported ``resumed_from_prior_run``), the rest re-driven idempotently
        (a live state already matching the manifest concludes ``converged``
        with NO write — no double-apply). A ``concluded`` checkpoint returns
        its report rebuilt from durable rows, driving nothing.
        """
        if not isinstance(checkpoint_id, str) or not checkpoint_id.strip():
            raise ValueError("restore needs a non-empty checkpoint id")
        store = self._require_restore_store()
        with self._lock:
            return self._restore_locked(store, checkpoint_id)

    def _restore_locked(
        self, store: CheckpointRestoreStore, checkpoint_id: str
    ) -> WorkspaceRestoreReport:
        record = store.get_workspace_checkpoint(checkpoint_id)
        if record is None:
            raise CheckpointUnknown(checkpoint_id)
        rows = store.get_workspace_checkpoint_members(checkpoint_id)
        self._require_known_restore_state(record, rows)
        if record.restore_status == RESTORE_STATUS_CONCLUDED:
            # Idempotent re-conclude: the durable rows ARE the report.
            return self._report_from_durable_rows(checkpoint_id, rows)
        pending = [row for row in rows if row.restore_outcome is None]
        self._require_drivable(pending)
        store.set_workspace_checkpoint_restore_status(
            checkpoint_id, RESTORE_STATUS_IN_PROGRESS, updated_at=float(self._clock())
        )
        outcomes: list[MemberRestoreOutcome] = []
        for row in rows:
            if row.restore_outcome is not None:
                # Terminal from a prior (crashed) run: skip, never re-drive.
                outcomes.append(self._outcome_from_durable_row(row))
                continue
            outcome = self._drive_member(row)
            # Durable BEFORE the next member: a crash after this write leaves a
            # terminal row the resuming run skips; a crash before it leaves a
            # pending row the resuming run re-drives idempotently.
            store.set_workspace_checkpoint_member_restore(
                checkpoint_id,
                row.member_path,
                restore_outcome=outcome.outcome,
                deleted_at_restore=outcome.deleted_at_restore,
            )
            outcomes.append(outcome)
        self._registration_seam(checkpoint_id, outcomes)
        store.set_workspace_checkpoint_restore_status(
            checkpoint_id, RESTORE_STATUS_CONCLUDED, updated_at=float(self._clock())
        )
        return WorkspaceRestoreReport(
            checkpoint_id=checkpoint_id,
            status=RESTORE_STATUS_CONCLUDED,
            members=tuple(outcomes),
        )

    def _registration_seam(
        self, checkpoint_id: str, outcomes: Sequence[MemberRestoreOutcome]
    ) -> None:
        """THE UNIT-5 REGISTRATION HOOK POINT — deliberately inert in Unit 4.

        Runs after every member holds a terminal outcome and BEFORE the
        ``concluded`` status lands. Unit 5 replaces this no-op with the
        coordinator registration split (the plan's restore-registration
        design), consuming exactly what it receives here — the checkpoint id
        plus the full per-member outcome sequence:

        - WRITTEN members (``outcome == restored`` with ``new_native_token``
          set) register via ``commit_all`` — all-or-nothing, an S/I caller;
        - DELETED members (``deleted_at_restore`` set) are recorded
          manifest-side in the same service transaction (``commit_all`` has no
          delete semantics);
        - an EMPTY write-set (every member skipped/absorbed/converged/deleted)
          concludes via the status update alone — ``commit_all`` is never
          called (it raises on an empty set).

        Must stay side-effect-free until Unit 5 lands.
        """

    # --- restore pre-flight (fail-fast, before any status write) --------------

    def _require_restore_store(self) -> CheckpointRestoreStore:
        if not isinstance(self._service, CheckpointRestoreStore):
            raise TypeError(
                "restore needs a service speaking the CheckpointRestoreStore "
                "surface (checkpoint read + durable restore progress); this "
                f"service ({type(self._service).__name__}) does not — a "
                "capture-only seam cannot record a crash-resumable restore"
            )
        return self._service

    @staticmethod
    def _require_known_restore_state(
        record: CheckpointRecord, rows: Sequence[CheckpointMember]
    ) -> None:
        """Fail closed on durable state outside the closed vocabularies.

        An unknown status/outcome string would silently break crash-resume
        (which classifies terminality by identity), so it is refused loudly
        rather than guessed at.
        """
        if record.restore_status not in RESTORE_STATUSES:
            raise CoherenceError(
                f"checkpoint {record.checkpoint_id!r} carries unknown "
                f"restore_status {record.restore_status!r} (closed vocabulary: "
                f"{sorted(RESTORE_STATUSES)}) — refusing to drive legs from "
                "unclassifiable durable state"
            )
        for row in rows:
            if (
                row.restore_outcome is not None
                and row.restore_outcome not in RESTORE_MEMBER_OUTCOMES
            ):
                raise CoherenceError(
                    f"member {row.member_path!r} carries unknown restore_outcome "
                    f"{row.restore_outcome!r} — refusing to classify it as "
                    "terminal or pending (fail-closed)"
                )
        if record.restore_status == RESTORE_STATUS_CONCLUDED and any(
            row.restore_outcome is None for row in rows
        ):
            raise CoherenceError(
                f"checkpoint {record.checkpoint_id!r} is 'concluded' but holds "
                "outcome-less members — inconsistent durable state (a concluded "
                "restore records a terminal outcome for EVERY member)"
            )

    def _require_drivable(self, pending: Sequence[CheckpointMember]) -> None:
        """Every leg this run will drive must be reachable BEFORE ``in_progress``.

        A missing binding or resolver is caller misconfiguration: raising here
        (ValueError) keeps an undrivable restore from minting progress state.
        Skip legs (declared forward-only / capture-refused tiers) need nothing.
        """
        declared = {m.member_path: m for m in self._members}
        problems: list[str] = []
        for row in pending:
            if not row.absent and row.restore_tier == RestoreTier.FORWARD_ONLY.value:
                continue  # forward_only_skipped needs no binding
            member = declared.get(row.member_path)
            if member is None or isinstance(member, _ForwardOnlyMember):
                problems.append(
                    f"{row.member_path}: no declared file/object member binding "
                    "(re-declare the member on this versioner before restore)"
                )
                continue
            if isinstance(member, _FileMember) and not row.absent:
                if self._file_resolver is None:
                    problems.append(
                        f"{row.member_path}: file restore needs a "
                        "FileContentResolver (pass file_resolver= at construction)"
                    )
                if not isinstance(member.source, FileRestoreTarget):
                    problems.append(
                        f"{row.member_path}: the member's source lacks the "
                        "write_cas_at restore leg (FileRestoreTarget surface)"
                    )
        if problems:
            raise ValueError(
                "restore pre-flight failed (nothing was started): "
                + "; ".join(problems)
            )

    # --- one member's leg (terminal outcome, always) --------------------------

    def _drive_member(self, row: CheckpointMember) -> MemberRestoreOutcome:
        # ABSENT-fact FIRST: an absent-at-capture member restores to ABSENCE
        # via a delete leg. Its restore_tier is forward_only only because
        # there was no STATE to tier (derive_restore_tier's absent default) —
        # the tier speaks to state restorability, and absence needs no
        # history; checking the tier first would dead-code every delete leg.
        if row.absent:
            return self._drive_absent_member(row, self._require_binding(row.member_path))
        if row.restore_tier == RestoreTier.FORWARD_ONLY.value:
            return MemberRestoreOutcome(
                member_path=row.member_path,
                outcome=RESTORE_OUTCOME_FORWARD_ONLY_SKIPPED,
                attempts=0,
                detail=(
                    "forward-only member: enumerated and skipped — no captured "
                    "state to bring back (declared action surface, or a member "
                    "whose pointer could not be confirmed at capture)"
                ),
            )
        member = self._require_binding(row.member_path)
        if isinstance(member, _ObjectMember):
            return self._drive_object_leg(row, member)
        assert isinstance(member, _FileMember)  # pre-flight guaranteed
        return self._drive_file_leg(row, member)

    def _require_binding(self, member_path: str) -> "_FileMember | _ObjectMember":
        member = next(
            (m for m in self._members if m.member_path == member_path), None
        )
        if member is None or isinstance(member, _ForwardOnlyMember):
            # Pre-flight already refused this shape; kept as a hard invariant.
            raise CoherenceError(
                f"no declared binding for member {member_path!r} at leg time"
            )
        return member

    # --- delete legs (the ABSENT fact restored) -------------------------------

    def _drive_absent_member(
        self, row: CheckpointMember, member: "_FileMember | _ObjectMember"
    ) -> MemberRestoreOutcome:
        if isinstance(member, _ObjectMember):
            return self._drive_object_delete_leg(row, member)
        return self._drive_file_absent_leg(row, member)

    def _drive_object_delete_leg(
        self, row: CheckpointMember, member: _ObjectMember
    ) -> MemberRestoreOutcome:
        try:
            # Presence probe via the plain read (works on versioned AND
            # unversioned buckets — read_versioned would refuse the latter).
            member.binding.read(member.key)
        except KeyError:
            return MemberRestoreOutcome(
                member_path=row.member_path,
                outcome=RESTORE_OUTCOME_CONVERGED,
                attempts=0,
                detail=(
                    "manifest records ABSENT and the object is already absent "
                    "live — no delete issued"
                ),
            )
        # Unconditional-latest delete: between the presence probe and this
        # request a foreign writer may land a version the delete then covers.
        # A DOCUMENTED residual (plan Unit 4): on a versioned bucket the
        # minted marker preserves that write as a noncurrent version
        # (recoverable); on an unversioned bucket the window is the member's
        # declared no-history reality. S3 offers no If-Match DELETE to close it.
        deletion = member.binding.delete(member.key)
        kind = (
            "delete marker minted (history survives)"
            if deletion.delete_marker
            else "permanent unversioned delete"
        )
        return MemberRestoreOutcome(
            member_path=row.member_path,
            outcome=RESTORE_OUTCOME_RESTORED,
            attempts=1,
            detail=(
                f"live object deleted to match the manifest's ABSENT fact — "
                f"{kind}; unconditional-latest (the pre-delete race window is "
                "a documented residual)"
            ),
            deleted_at_restore=float(self._clock()),
        )

    def _drive_file_absent_leg(
        self, row: CheckpointMember, member: _FileMember
    ) -> MemberRestoreOutcome:
        try:
            member.source.read_with_version(row.member_path)
        except FileNotFoundError:
            return MemberRestoreOutcome(
                member_path=row.member_path,
                outcome=RESTORE_OUTCOME_CONVERGED,
                attempts=0,
                detail=(
                    "manifest records ABSENT and the file is already absent "
                    "live — nothing to delete"
                ),
            )
        # v1 residual: the file seam offers no delete leg, so a live file the
        # manifest records ABSENT is a divergence this engine cannot converge
        # — absorbed as conflict (detection only; no-arbiter), never a raise
        # and never a silent skip.
        return MemberRestoreOutcome(
            member_path=row.member_path,
            outcome=RESTORE_OUTCOME_CONFLICT,
            attempts=0,
            detail=(
                "manifest records ABSENT but the file exists live; the v1 file "
                "leg has no delete surface (no-arbiter: adapter-local detection "
                "only) — divergence not converged"
            ),
        )

    # --- the S3 CAS leg (native-cas: the substrate arbitrates) ----------------

    def _drive_object_leg(
        self, row: CheckpointMember, member: _ObjectMember
    ) -> MemberRestoreOutcome:
        if row.native_token is None:
            # A present, restorable-tiered member always manifests a real
            # pointer (capture's Sentinel rule); its absence means the pinned
            # target is unreachable — absorbing, the run must still conclude.
            return MemberRestoreOutcome(
                member_path=row.member_path,
                outcome=RESTORE_OUTCOME_TARGET_LOST,
                attempts=0,
                detail="no manifested restore pointer — the pinned target is unreachable",
            )
        budget = _LegBudget(MAX_RESTORE_LEG_REDRIVES)
        # Pinned bytes resolve LAZILY, after the converged check: a member
        # whose live state already matches the manifest concludes ``converged``
        # even when its pin has since expired (the token-identity rule — a
        # crash-resumed, already-landed leg must never report target_lost).
        pinned_bytes: bytes | None = None
        while True:
            if budget.try_consume() is None:
                return MemberRestoreOutcome(
                    member_path=row.member_path,
                    outcome=RESTORE_OUTCOME_CONFLICT,
                    attempts=budget.attempts,
                    detail=(
                        f"re-drive budget exhausted ({budget.attempts} attempts) "
                        "under sustained live-writer contention — no write "
                        "landed (native-CAS: every If-Match attempt lost its race)"
                    ),
                )
            view = self._object_live_view(row, member, budget)
            if isinstance(view, MemberRestoreOutcome):
                return view
            live_token, live_hash = view
            if live_hash == row.fingerprint:
                return MemberRestoreOutcome(
                    member_path=row.member_path,
                    outcome=RESTORE_OUTCOME_CONVERGED,
                    attempts=budget.attempts,
                    detail=(
                        "live object already byte-identical to the manifest — "
                        "no write issued (authorship not claimed)"
                    ),
                )
            if pinned_bytes is None:
                resolved = self._resolve_pinned_object(row, member)
                if isinstance(resolved, MemberRestoreOutcome):
                    return resolved
                pinned_bytes = resolved
            outcome = self._object_cas_attempt(row, member, pinned_bytes, live_token, budget)
            if outcome is not None:
                return outcome

    def _object_live_view(
        self, row: CheckpointMember, member: _ObjectMember, budget: _LegBudget
    ) -> "MemberRestoreOutcome | tuple[str, str | None]":
        """One live read → ``(comparand, content_hash)``, or a terminal outcome.

        The (bytes, ETag) pair comes from ONE response (the split-comparand
        rule); the manifested versionId is only ever the POINTER, never the
        comparand (the F4 split). Live-absent yields the explicit
        :data:`CREATE_IF_ABSENT` comparand — the create leg loses to any
        concurrent re-creation, never overwrites one.
        """
        try:
            live = member.binding.read_versioned(member.key)
        except KeyError:
            return (CREATE_IF_ABSENT, None)
        except VersionPointerUnconfirmed:
            # The live pointer axis broke mid-restore (versioning suspended
            # since capture): what a write would mint can no longer be
            # confirmed — UNCONFIRMED → HOLD, never best-effort.
            return MemberRestoreOutcome(
                member_path=row.member_path,
                outcome=RESTORE_OUTCOME_HELD_UNCONFIRMED,
                attempts=budget.attempts,
                detail=(
                    "live version pointer unconfirmed (bucket versioning "
                    "suspended since capture) — HELD, never best-effort"
                ),
            )
        return (live.etag, _sha256_hex(live.data))

    def _resolve_pinned_object(
        self, row: CheckpointMember, member: _ObjectMember
    ) -> "bytes | MemberRestoreOutcome":
        """The pinned target's bytes (an immutable S3 version), or target_lost."""
        assert row.native_token is not None  # guarded by the leg entry
        try:
            pinned = member.binding.read_pinned(member.key, version_id=row.native_token)
        except KeyError:
            return MemberRestoreOutcome(
                member_path=row.member_path,
                outcome=RESTORE_OUTCOME_TARGET_LOST,
                attempts=0,
                detail=(
                    f"pinned version {row.native_token!r} no longer resolves "
                    "(expired or raced pin) — the restore target is gone"
                ),
            )
        return pinned.data

    def _object_cas_attempt(
        self,
        row: CheckpointMember,
        member: _ObjectMember,
        pinned_bytes: bytes,
        live_token: str,
        budget: _LegBudget,
    ) -> MemberRestoreOutcome | None:
        """One conditional put under the freshly read comparand; ``None`` = re-drive.

        ``row.fingerprint`` doubles as the intended hash: a pinned S3 version
        is immutable, so its bytes always hash to the captured fingerprint.
        """
        binding, key = member.binding, member.key
        intended_hash = row.fingerprint or _sha256_hex(pinned_bytes)
        try:
            result = binding.cas_write_versioned(
                key, expected_token=live_token, new_bytes=pinned_bytes
            )
        except CasRetriesExhausted:
            # The binding's OWN 409-transient budget (this leg's in-binding
            # twin) exhausted: terminal, absorbing — no write landed.
            return MemberRestoreOutcome(
                member_path=row.member_path,
                outcome=RESTORE_OUTCOME_CONFLICT,
                attempts=budget.attempts,
                detail=(
                    "the binding's conditional-put transient budget exhausted "
                    "— no write landed"
                ),
            )
        except VersionPointerUnconfirmed:
            # The put LANDED durably (its ETag was captured) but minted no
            # pointer — the bucket lost versioning mid-restore. The bytes are
            # back; the new state simply cannot be pinned or registered (the
            # Unit-5 seam receives no token for it).
            return MemberRestoreOutcome(
                member_path=row.member_path,
                outcome=RESTORE_OUTCOME_RESTORED,
                attempts=budget.attempts,
                detail=(
                    "pinned bytes landed but the write minted no version "
                    "pointer (bucket unversioned mid-restore) — the restored "
                    "state cannot be re-pinned"
                ),
            )
        if isinstance(result, VersionedCasWritten):
            return MemberRestoreOutcome(
                member_path=row.member_path,
                outcome=RESTORE_OUTCOME_RESTORED,
                attempts=budget.attempts,
                detail=(
                    "pinned bytes landed via the native-CAS If-Match put "
                    f"(attempt {budget.attempts})"
                ),
                new_native_token=result.version_id,
            )
        if isinstance(result, CasConflict):
            # A foreign writer moved the comparand (412 / raced delete): the
            # substrate arbitrated and this attempt lost — re-drive from a
            # fresh live read, bounded by the leg budget.
            return None
        # CasUnknown: ONE reconciliation read decides; still-unknown → HOLD.
        decision = binding.reconcile_after_unknown(
            key, expected_token=live_token, intended_hash=intended_hash
        )
        if decision.verdict is ReconcileVerdict.CONVERGE:
            return MemberRestoreOutcome(
                member_path=row.member_path,
                outcome=RESTORE_OUTCOME_CONVERGED,
                attempts=budget.attempts,
                detail=(
                    "unconfirmed write reconciled CONVERGE: the live object is "
                    "byte-identical to the manifest (authorship not claimed)"
                ),
            )
        if decision.verdict in (ReconcileVerdict.RE_DRIVE, ReconcileVerdict.CONFLICT):
            # Knowledge either way — an unmoved comparand (retry the intent)
            # or a real peer write (re-derive from fresh state): both re-drive
            # under the leg budget.
            return None
        # HOLD (and any future verdict): the outcome stayed unconfirmable
        # after the reconciliation read — HELD, never best-effort. (On the
        # create path a HOLD can also mean "still absent": re-driving blind
        # would risk a second landing of a two-world put, so HOLD wins.)
        return MemberRestoreOutcome(
            member_path=row.member_path,
            outcome=RESTORE_OUTCOME_HELD_UNCONFIRMED,
            attempts=budget.attempts,
            detail=(
                "write outcome still UNCONFIRMED after the reconciliation "
                "read — HELD, never best-effort"
            ),
        )

    # --- the file CAS leg (no-arbiter: detection-guarded ONLY) ----------------

    def _drive_file_leg(
        self, row: CheckpointMember, member: _FileMember
    ) -> MemberRestoreOutcome:
        budget = _LegBudget(MAX_RESTORE_LEG_REDRIVES)
        # Lazily resolved, after the converged check (token-identity first —
        # an already-matching live state concludes ``converged`` even when
        # retention has since expired; the resolver is consulted only when a
        # write is actually needed).
        pinned: bytes | None = None
        while True:
            if budget.try_consume() is None:
                return MemberRestoreOutcome(
                    member_path=row.member_path,
                    outcome=RESTORE_OUTCOME_CONFLICT,
                    attempts=budget.attempts,
                    detail=(
                        f"re-drive budget exhausted ({budget.attempts} attempts) "
                        "under live-editor contention — no write landed "
                        "(no-arbiter: detection-guarded only, never substrate "
                        "arbitration)"
                    ),
                )
            try:
                live_bytes, live_version = member.source.read_with_version(row.member_path)
            except FileNotFoundError:
                # v1 residual: recreation needs the coordinator artifact
                # identity, which lands with Unit-5 registration — absorbed,
                # never silent.
                return MemberRestoreOutcome(
                    member_path=row.member_path,
                    outcome=RESTORE_OUTCOME_CONFLICT,
                    attempts=budget.attempts,
                    detail=(
                        "member present in the manifest but absent live; the "
                        "v1 file leg cannot recreate it (artifact registration "
                        "lands in Unit 5) — divergence not converged (no-arbiter)"
                    ),
                )
            if _sha256_hex(live_bytes) == row.fingerprint:
                return MemberRestoreOutcome(
                    member_path=row.member_path,
                    outcome=RESTORE_OUTCOME_CONVERGED,
                    attempts=budget.attempts,
                    detail=(
                        "live file already byte-identical to the manifest — "
                        "no write issued"
                    ),
                )
            if pinned is None:
                resolved = self._resolve_pinned_file(row)
                if isinstance(resolved, MemberRestoreOutcome):
                    return resolved
                pinned = resolved
            outcome = self._file_cas_attempt(row, member, pinned, live_version, budget)
            if outcome is not None:
                return outcome

    def _resolve_pinned_file(
        self, row: CheckpointMember
    ) -> "bytes | MemberRestoreOutcome":
        """The retained bytes for the manifested version, or target_lost.

        The fingerprint cross-check is load-bearing: unlike an immutable S3
        version, a resolver is a SEAM — bytes that do not hash to the captured
        fingerprint are not the captured state, and restoring them would be a
        silent wrong-content restore.
        """
        resolver = self._file_resolver
        assert resolver is not None  # pre-flight guaranteed
        try:
            pinned = resolver.content_at(row.member_path, int(row.native_token or 0))
        except KeyError:
            return MemberRestoreOutcome(
                member_path=row.member_path,
                outcome=RESTORE_OUTCOME_TARGET_LOST,
                attempts=0,
                detail=(
                    f"retained version {row.native_token} no longer resolves "
                    "(retention expired) — the restore target is gone"
                ),
            )
        if _sha256_hex(pinned) != row.fingerprint:
            return MemberRestoreOutcome(
                member_path=row.member_path,
                outcome=RESTORE_OUTCOME_TARGET_LOST,
                attempts=0,
                detail=(
                    "retained bytes do not match the captured fingerprint — "
                    "the restore target is gone"
                ),
            )
        return bytes(pinned)

    def _file_cas_attempt(
        self,
        row: CheckpointMember,
        member: _FileMember,
        pinned: bytes,
        live_version: int,
        budget: _LegBudget,
    ) -> MemberRestoreOutcome | None:
        """One version-checked CAS write; ``None`` means re-drive.

        Detection-guarded ONLY: the CAS detects a foreign edit adapter-locally
        (typed :class:`~ccs.core.exceptions.CasVersionConflict`) — nothing here
        is, or may ever be labeled, substrate arbitration (no-arbiter).
        """
        path = row.member_path
        source = member.source
        assert isinstance(source, FileRestoreTarget)  # pre-flight guaranteed
        try:
            source.write_cas_at(path, live_version, pinned)
        except CasVersionConflict:
            # DETECTION fired: a foreign edit moved the version between the
            # read and the CAS — re-drive from a fresh read (leg-budgeted).
            return None
        except ViewWedged:
            return MemberRestoreOutcome(
                member_path=path,
                outcome=RESTORE_OUTCOME_CONFLICT,
                attempts=budget.attempts,
                detail=(
                    "the comparand view stayed strict-denied (wedged) — no "
                    "write landed (no-arbiter: detection-guarded only)"
                ),
            )
        except CommitUnconfirmed:
            return MemberRestoreOutcome(
                member_path=path,
                outcome=RESTORE_OUTCOME_HELD_UNCONFIRMED,
                attempts=budget.attempts,
                detail=(
                    "the version-CAS commit could not be confirmed (transport "
                    "failed mid-commit) — HELD, never best-effort"
                ),
            )
        return MemberRestoreOutcome(
            member_path=path,
            outcome=RESTORE_OUTCOME_RESTORED,
            attempts=budget.attempts,
            # write_cas_at advances deterministically to expected+1 on a
            # confirmed win — the pointer the Unit-5 registration consumes.
            new_native_token=str(live_version + 1),
            detail=(
                "pinned bytes landed via the detection-guarded version-CAS "
                f"(attempt {budget.attempts}; no-arbiter: adapter-local "
                "detection, never substrate arbitration)"
            ),
        )

    # --- restore report reconstruction (durable rows → report) ----------------

    @staticmethod
    def _outcome_from_durable_row(row: CheckpointMember) -> MemberRestoreOutcome:
        assert row.restore_outcome is not None  # callers filtered / validated
        return MemberRestoreOutcome(
            member_path=row.member_path,
            outcome=row.restore_outcome,
            attempts=0,
            detail=(
                "terminal outcome recorded by a prior run (resumed: reported, "
                "not re-driven)"
            ),
            deleted_at_restore=row.deleted_at_restore,
            resumed_from_prior_run=True,
        )

    def _report_from_durable_rows(
        self, checkpoint_id: str, rows: Sequence[CheckpointMember]
    ) -> WorkspaceRestoreReport:
        return WorkspaceRestoreReport(
            checkpoint_id=checkpoint_id,
            status=RESTORE_STATUS_CONCLUDED,
            members=tuple(self._outcome_from_durable_row(row) for row in rows),
        )

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
