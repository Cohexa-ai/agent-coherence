# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""``agent-coherence-workspace`` — checkpoint / list / restore / status (WV R6, R8).

Operator surface over the Workspace-Versioning engine
(:class:`ccs.adapters.workspace.WorkspaceVersioner`). The CLI drives an
IN-PROCESS :class:`~ccs.coordinator.service.CoordinatorService` over a durable
workspace-scoped SQLite registry (``<root>/.coherence/workspace.db``, retention
on) — pin legs run adapter-side and have no HTTP surface, so the whole verb set
runs in-process rather than through the hook coordinator's HTTP routes. The
db is OWNED by this CLI (the hook coordinator's ``state.db`` is untouched).

Verbs:

- ``checkpoint NAME --file P [--forward-only Q] [--no-pin]`` — capture a
  skew-declared cut over the named members and persist ONE manifest (pin legs
  on by default; ``--no-pin`` leaves the documented claimed-but-not-yet-backed
  ``(tier, unpinned)`` pair).
- ``list`` — every persisted checkpoint header.
- ``status ID`` — header + per-member honesty pairs ``(restore_tier,
  pin_state)``, torn-cut flags, restore outcomes.
- ``restore ID`` — drive the restore engine over the durable member rows;
  the per-member terminal report is the output, absorbing outcomes included.

File members ride a working-tree bridge (:class:`WorkingTreeSource`): one read
observes the disk bytes AND their version in the CLI's registry (first
observation mints v1; observed drift mints the next version with the bytes
retained), so the restore pointer and the fingerprint always describe one
observation and restore reads resolve through coordinator retention
(:class:`RetainedContentResolver`). The write leg is DETECTION-ONLY
(``no-arbiter``): a foreign edit is detected by hash/version comparison and
surfaced as a typed conflict, never presented as substrate arbitration.
S3 object members are captured/restored via the Python API (bindings carry
credentials the CLI cannot reconstruct) — ``status``/``list`` still render
them honestly.

Exit codes:
- 0: verb succeeded (restore: concluded with no absorbing/hold outcome)
- 1: not in a git repo / validation error (bad path, no members, pre-flight)
- 2: typed coherence refusal (binary member, unknown checkpoint, persist failure)
- 3: restore CONCLUDED but at least one member ended in an absorbing/hold
     outcome (``conflict`` / ``target_lost`` / ``held_unconfirmed``) or the
     registration was refused — the report on stdout carries the per-member truth
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Sequence
from uuid import NAMESPACE_URL, UUID, uuid5

from ccs.adapters.claude_code.resolver import find_coordinator_root
from ccs.adapters.workspace import (
    BinaryFileMemberRefused,
    CheckpointPersistFailed,
    MemberRestoreOutcome,
    WorkspaceCheckpoint,
    WorkspaceRestoreReport,
    WorkspaceVersioner,
)
from ccs.cli._coherence_client import err, normalize_workspace_path
from ccs.coordinator.registry_protocol import CheckpointMember, CheckpointRecord
from ccs.coordinator.service import CoordinatorService
from ccs.coordinator.sqlite_registry import SqliteArtifactRegistry
from ccs.core.exceptions import (
    PIN_STATE_UNPINNED,
    RESTORE_OUTCOME_CONFLICT,
    RESTORE_OUTCOME_HELD_UNCONFIRMED,
    RESTORE_OUTCOME_TARGET_LOST,
    RESTORE_STATUS_CONCLUDED,
    WORKSPACE_REGISTRATION_REFUSED,
    CasVersionConflict,
    CheckpointUnknown,
    CoherenceError,
    CommitUnconfirmed,
)
from ccs.core.substrate import ArbitrationTier, RestoreTier, sha256_hex
from ccs.core.types import Artifact

_PROG = "agent-coherence-workspace"

#: The CLI's own durable registry, relative to the workspace root. Deliberately
#: NOT the hook coordinator's ``state.db``: pin legs and checkpoint manifests
#: are driven in-process by THIS CLI, and sharing a live server's db would
#: couple two writers the architecture keeps separate.
_WORKSPACE_DB_RELPATH = ".coherence/workspace.db"

#: Stable identity namespace: the checkpoint owner and the file-bridge writer
#: are minted per-workspace so re-invocations are the SAME principal.
_IDENTITY_NAMESPACE = uuid5(NAMESPACE_URL, "ccs-workspace-cli")

#: Honesty placement #2 (--help constraint note). Single tokens are asserted
#: by tests, so keep "single-host", "no-arbiter" and "detection-only" intact.
_SCOPE_NOTE = (
    "Scope: single-host coordinator only — checkpoints, pins and restore\n"
    "progress live in this workspace's local coordinator state and make NO\n"
    "cross-host claims. File members are detection-only ('no-arbiter'): a\n"
    "foreign edit racing a restore is detected adapter-locally and reported\n"
    "as a typed conflict, never arbitrated by the substrate. S3 object\n"
    "members are captured/restored via the Python API (see\n"
    "examples/workspace_versioning); this CLI still renders them honestly\n"
    "in 'status' and 'list'."
)

#: Honesty placement #4 (checkpoint output retention caveat).
FILE_RETENTION_CAVEAT = (
    "file members ride bounded coordinator retention (a K/T policy with no "
    "per-version hold in v1): a 'held' pin on a file member is a VERIFICATION "
    "of the retained bytes, not a retention guarantee — the tier stays "
    "restorable-unpinned, and an expired retention window surfaces as "
    "target_lost at restore time."
)

#: Honesty placement #3 (the claimed-but-not-yet-backed pair label).
CLAIMED_NOT_BACKED_LABEL = "claimed-but-not-yet-backed"

_ABSORBING_OUTCOMES = frozenset(
    {
        RESTORE_OUTCOME_CONFLICT,
        RESTORE_OUTCOME_TARGET_LOST,
        RESTORE_OUTCOME_HELD_UNCONFIRMED,
    }
)

#: read_with_version's observe-commit loop bound: a racing second CLI process
#: can move the ledger between the lookup and the CAS; three attempts absorb
#: any realistic interleave without risking a livelock.
_OBSERVE_ATTEMPTS = 3


# --- the working-tree bridge (FileMemberSource + FileRestoreTarget) -------------


class WorkingTreeSource:
    """Disk files bridged to the CLI's registry — the file-member binding.

    ``read_with_version`` returns ``(bytes, version)`` from ONE observation:
    the disk bytes are read, then the registry row for ``path`` is resolved —
    first observation mints v1 with the bytes retained; observed drift (disk
    hash != ledger hash) mints the next version via the registry's OCC
    ``commit_cas``, again with the bytes retained. The returned version's
    retained content therefore always hashes to the returned bytes, which is
    what makes :class:`RetainedContentResolver` a REAL resolver rather than a
    cache.

    ``write_cas_at`` is the restore leg: DETECTION-ONLY (``no-arbiter``). It
    refuses (typed :class:`~ccs.core.exceptions.CasVersionConflict`) when the
    ledger moved past ``expected_version`` OR when the disk bytes no longer
    hash to the ledger's content at ``expected_version`` (a foreign edit the
    ledger has not observed). A win writes the disk FIRST, then commits the
    ledger — a crash between the two leaves the disk restored and the ledger
    behind, which the next read re-observes (self-healing in the safe
    direction). Documented v1 residuals: a delete landing between the read and
    the CAS is recreated undetected, and two concurrent CLI processes could
    both pass detection (single-writer CLI assumption).

    Non-UTF-8 disk bytes are returned with version 0 (pointer UNCONFIRMED) and
    are never minted into the ledger — the versioner's typed
    :class:`~ccs.adapters.workspace.BinaryFileMemberRefused` fires at capture.
    """

    def __init__(self, root: Path, registry: SqliteArtifactRegistry, agent: UUID) -> None:
        self._root = root
        self._registry = registry
        self._agent = agent

    def _abs(self, path: str) -> Path:
        return self._root / path

    def read_with_version(self, path: str) -> tuple[bytes, int]:
        data = self._abs(path).read_bytes()  # FileNotFoundError = the ABSENT fact
        try:
            text = data.decode("utf-8")
        except UnicodeDecodeError:
            # Never minted: the version stays UNCONFIRMED (0) and capture
            # refuses the member with the typed binary refusal.
            return (data, 0)
        digest = sha256_hex(data)
        for _ in range(_OBSERVE_ATTEMPTS):
            artifact_id = self._registry.lookup_artifact_id_by_name(path)
            if artifact_id is None:
                artifact = Artifact(name=path, version=1, content_hash=digest)
                self._registry.register_artifact(artifact, text)
                return (data, 1)
            current = self._registry.get_artifact(artifact_id)
            if current is None:  # pragma: no cover — removed between the two reads
                continue
            if current.content_hash == digest:
                return (data, current.version)
            result = self._registry.commit_cas(
                artifact_id,
                self._agent,
                expected_version=current.version,
                content_hash=digest,
                content=text,
            )
            if isinstance(result, tuple):
                return (data, result[0].version)
            # ConflictDetail: another observer moved the ledger — re-resolve.
        raise CoherenceError(
            f"file member {path!r}: could not record the observed content "
            f"state after {_OBSERVE_ATTEMPTS} attempts (another writer keeps "
            "moving the workspace ledger)"
        )

    def write_cas_at(self, path: str, expected_version: int, new_content: bytes) -> None:
        text = new_content.decode("utf-8")  # captured members are UTF-8 by the typed refusal
        artifact_id = self._registry.lookup_artifact_id_by_name(path)
        current = (
            self._registry.get_artifact(artifact_id) if artifact_id is not None else None
        )
        if artifact_id is None or current is None or current.version != expected_version:
            raise CasVersionConflict(
                path, expected_version, current.version if current is not None else 0
            )
        live = self._abs(path)
        try:
            disk = live.read_bytes()
        except FileNotFoundError:
            disk = None
        if disk is not None and sha256_hex(disk) != current.content_hash:
            # DETECTION fired: the disk moved without the ledger observing it —
            # a foreign edit. no-arbiter: typed conflict, never arbitration.
            raise CasVersionConflict(path, expected_version, expected_version)
        live.parent.mkdir(parents=True, exist_ok=True)
        live.write_bytes(new_content)  # disk first: the safe crash direction
        result = self._registry.commit_cas(
            artifact_id,
            self._agent,
            expected_version=expected_version,
            content_hash=sha256_hex(new_content),
            content=text,
        )
        if not isinstance(result, tuple):
            raise CommitUnconfirmed(
                f"file member {path!r}: the restored bytes landed on disk but "
                f"the version-CAS ledger commit was refused ({result!r}) — "
                "outcome unconfirmed, never best-effort"
            )


class RetainedContentResolver:
    """``FileContentResolver`` over the CLI registry's retention (real, not a cache)."""

    def __init__(self, registry: SqliteArtifactRegistry) -> None:
        self._registry = registry

    def content_at(self, member_path: str, version: int) -> bytes:
        artifact_id = self._registry.lookup_artifact_id_by_name(member_path)
        if artifact_id is None:
            raise KeyError(member_path)
        body = self._registry.get_content_at_version(artifact_id, version)
        if body is None:
            raise KeyError((member_path, version))
        return body.encode("utf-8") if isinstance(body, str) else bytes(body)


# --- parser ----------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog=_PROG,
        description=(
            "Workspace checkpoint / list / restore / status over heterogeneous "
            "members (files + S3 objects + declared forward-only surfaces)."
        ),
        epilog=_SCOPE_NOTE,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument(
        "--root",
        type=Path,
        default=None,
        help="Override the workspace root (default: walk up from cwd to git root).",
    )
    common.add_argument(
        "--json",
        action="store_true",
        help="Emit machine-parseable JSON instead of the human rendering.",
    )
    sub = parser.add_subparsers(dest="verb", required=True)

    p_ckpt = sub.add_parser(
        "checkpoint",
        parents=[common],
        help="Capture a named checkpoint over the declared members.",
        description=(
            "Capture a skew-declared cut over the named members and persist ONE "
            "manifest. Pin legs run by default; file-member pins are "
            "verification-only (see the retention caveat printed with the result)."
        ),
    )
    p_ckpt.add_argument("name", help="Checkpoint name (non-empty; not an id).")
    p_ckpt.add_argument(
        "--file",
        action="append",
        default=[],
        metavar="PATH",
        help=(
            "Declare one file member (repeatable). Workspace-relative or an "
            "absolute path inside the root; no '..' traversal. Non-UTF-8 files "
            "are refused with a typed error (v1 limitation)."
        ),
    )
    p_ckpt.add_argument(
        "--forward-only",
        action="append",
        default=[],
        metavar="NAME",
        help=(
            "Declare one forward-only member (repeatable): enumerated in the "
            "manifest, skipped at restore — an action/effect surface with no "
            "state to capture."
        ),
    )
    p_ckpt.add_argument(
        "--no-pin",
        action="store_true",
        help=(
            "Capture only — skip the pin legs. Members then persist the "
            "documented claimed-but-not-yet-backed (tier, unpinned) pair; "
            "'agent-coherence-workspace status' renders it explicitly."
        ),
    )

    sub.add_parser(
        "list",
        parents=[common],
        help="List every persisted checkpoint header.",
    )

    p_status = sub.add_parser(
        "status",
        parents=[common],
        help="Show one checkpoint's header + per-member honesty pairs.",
        description=(
            "Every member is labeled with its (restore_tier, pin_state) pair — "
            "(restorable, unpinned) is rendered as claimed-but-not-yet-backed — "
            "plus torn-cut flags and restore outcomes when present."
        ),
    )
    p_status.add_argument("checkpoint_id", help="The persisted checkpoint id.")

    p_restore = sub.add_parser(
        "restore",
        parents=[common],
        help="Restore a checkpoint (per-member terminal report).",
        description=(
            "Drive one conditional leg per durable member row under the "
            "termination contract. Absorbing outcomes (conflict / target_lost / "
            "held_unconfirmed) are REPORT content: the restore still concludes "
            "and the exit code distinguishes a clean restore (0) from a "
            "concluded-with-absorbed-outcomes one (3)."
        ),
    )
    p_restore.add_argument("checkpoint_id", help="The persisted checkpoint id.")
    return parser


# --- rendering helpers -----------------------------------------------------------


def _member_payload(row: CheckpointMember) -> dict[str, Any]:
    return {
        "member_path": row.member_path,
        "restore_tier": row.restore_tier,
        "pin_state": row.pin_state,
        "pair": f"({row.restore_tier}, {row.pin_state})",
        "claimed_not_backed": _claimed_not_backed(row),
        "absent": row.absent,
        "dirty_during_window": row.dirty_during_window,
        "arbitration_tier": row.arbitration_tier,
        "native_token": row.native_token,
        "fingerprint": row.fingerprint,
        "restore_outcome": row.restore_outcome,
        "deleted_at_restore": row.deleted_at_restore,
    }


def _claimed_not_backed(row: CheckpointMember) -> bool:
    return (
        row.restore_tier == RestoreTier.RESTORABLE.value
        and row.pin_state == PIN_STATE_UNPINNED
    )


def _member_line(row: CheckpointMember) -> str:
    """One member's honesty line: the (tier, pin) PAIR plus every flag present."""
    bits = [f"  {row.member_path}  ({row.restore_tier}, {row.pin_state})"]
    if _claimed_not_backed(row):
        # Honesty placement #3: (restorable, unpinned) is a CLAIM nothing backs
        # yet — never rendered as a plain restorable state.
        bits.append(CLAIMED_NOT_BACKED_LABEL)
    if row.absent:
        bits.append("ABSENT-at-capture")
    if row.dirty_during_window:
        bits.append("dirty-during-window")
    if row.restore_outcome is not None:
        bits.append(f"outcome={row.restore_outcome}")
    if row.deleted_at_restore is not None:
        bits.append("deleted-at-restore")
    return "  ".join(bits)


def _record_payload(record: CheckpointRecord) -> dict[str, Any]:
    return {
        "checkpoint_id": record.checkpoint_id,
        "name": record.name,
        "restore_status": record.restore_status,
        "pin_refcount": record.pin_refcount,
        "window_min": record.window_min,
        "window_max": record.window_max,
        "created_at_tick": record.created_at_tick,
    }


def _outcome_payload(outcome: MemberRestoreOutcome) -> dict[str, Any]:
    return {
        "member_path": outcome.member_path,
        "outcome": outcome.outcome,
        "attempts": outcome.attempts,
        "detail": outcome.detail,
        "new_native_token": outcome.new_native_token,
        "deleted_at_restore": outcome.deleted_at_restore,
        "resumed_from_prior_run": outcome.resumed_from_prior_run,
    }


def _print_json(payload: dict[str, Any]) -> None:
    print(json.dumps(payload, indent=2, sort_keys=True), flush=True)


# --- the in-process stack --------------------------------------------------------


class _Stack:
    """One verb invocation's in-process service stack (context-managed)."""

    def __init__(self, root: Path) -> None:
        self.root = root
        db_path = root / _WORKSPACE_DB_RELPATH
        db_path.parent.mkdir(parents=True, exist_ok=True)
        # Retention ON: the file-member resolver reads captured bytes back
        # through get_content_at_version — without it every file restore
        # would honestly (and uselessly) report target_lost.
        self.registry = SqliteArtifactRegistry(db_path, retain_versions=True)
        self.service = CoordinatorService(self.registry)
        self.owner = uuid5(_IDENTITY_NAMESPACE, f"owner:{root}")
        self.source = WorkingTreeSource(
            root, self.registry, uuid5(_IDENTITY_NAMESPACE, f"file-bridge:{root}")
        )
        self.resolver = RetainedContentResolver(self.registry)

    def versioner(self) -> WorkspaceVersioner:
        return WorkspaceVersioner(
            service=self.service, owner=self.owner, file_resolver=self.resolver
        )

    def close(self) -> None:
        self.registry.close()


# --- verbs -----------------------------------------------------------------------


def _cmd_checkpoint(stack: _Stack, args: argparse.Namespace) -> int:
    invalid: list[tuple[str, str]] = []
    files: list[str] = []
    for raw in args.file:
        normalized, reason = normalize_workspace_path(raw, stack.root)
        if reason is not None:
            invalid.append((raw, reason))
        else:
            files.append(normalized)
    if invalid:
        for raw, reason in invalid:
            err(f"{_PROG}: rejected {raw!r}: {reason}")
        return 1
    if not files and not args.forward_only:
        err(
            f"{_PROG}: checkpoint needs at least one member "
            "(--file and/or --forward-only)"
        )
        return 1

    versioner = stack.versioner()
    for path in files:
        versioner.add_file_member(stack.source, path)
    for name in args.forward_only:
        versioner.add_forward_only_member(name)
    result: WorkspaceCheckpoint = versioner.checkpoint(args.name, pin=not args.no_pin)

    if args.json:
        _print_json(
            {
                "checkpoint": _record_payload(result.record),
                "members": [_member_payload(row) for row in result.members],
                "retention_caveat": FILE_RETENTION_CAVEAT,
            }
        )
        return 0
    record = result.record
    print(f"checkpoint {record.name!r} persisted: {record.checkpoint_id}")
    print(
        f"window: [{record.window_min:.0f}, {record.window_max:.0f}] "
        "(monotonic seconds; the skew is declared, not hidden)"
    )
    print("members:")
    for row in result.members:
        print(_member_line(row))
    # Honesty placement #4: the retention caveat rides EVERY checkpoint output.
    print(f"retention caveat: {FILE_RETENTION_CAVEAT}")
    return 0


def _cmd_list(stack: _Stack, args: argparse.Namespace) -> int:
    records = stack.service.list_workspace_checkpoints()
    if args.json:
        _print_json({"checkpoints": [_record_payload(r) for r in records]})
        return 0
    if not records:
        print("no checkpoints persisted for this workspace")
        return 0
    for record in records:
        print(
            f"{record.checkpoint_id}  {record.name!r}  "
            f"restore_status={record.restore_status}  "
            f"pin_refcount={record.pin_refcount}"
        )
    return 0


def _cmd_status(stack: _Stack, args: argparse.Namespace) -> int:
    record = stack.service.get_workspace_checkpoint(args.checkpoint_id)
    if record is None:
        raise CheckpointUnknown(args.checkpoint_id)
    members = stack.service.get_workspace_checkpoint_members(args.checkpoint_id)
    if args.json:
        _print_json(
            {
                "checkpoint": _record_payload(record),
                "members": [_member_payload(row) for row in members],
            }
        )
        return 0
    print(f"checkpoint {record.checkpoint_id}  {record.name!r}")
    # Honesty placement #3 (header half): restore_status + pin_refcount.
    print(
        f"restore_status={record.restore_status}  pin_refcount={record.pin_refcount}"
    )
    print("members:  (restore_tier, pin_state) per member")
    for row in members:
        print(_member_line(row))
    return 0


def _cmd_restore(stack: _Stack, args: argparse.Namespace) -> int:
    record = stack.service.get_workspace_checkpoint(args.checkpoint_id)
    if record is None:
        raise CheckpointUnknown(args.checkpoint_id)
    rows = stack.service.get_workspace_checkpoint_members(args.checkpoint_id)
    versioner = stack.versioner()
    undrivable = [
        row.member_path
        for row in rows
        if row.restore_outcome is None
        and row.arbitration_tier == ArbitrationTier.NATIVE_CAS.value
    ]
    if undrivable and record.restore_status != RESTORE_STATUS_CONCLUDED:
        err(
            f"{_PROG}: cannot restore {args.checkpoint_id}: S3 object members "
            f"({', '.join(sorted(undrivable))}) need their bindings/credentials, "
            "which live in the Python API (see examples/workspace_versioning) — "
            "nothing was started"
        )
        return 1
    for row in rows:
        if row.arbitration_tier != ArbitrationTier.NO_ARBITER.value:
            continue
        needs_binding = row.absent or row.restore_tier != RestoreTier.FORWARD_ONLY.value
        if needs_binding:
            versioner.add_file_member(stack.source, row.member_path)
    report: WorkspaceRestoreReport = versioner.restore(args.checkpoint_id)

    absorbed = [m for m in report.members if m.outcome in _ABSORBING_OUTCOMES]
    registration_refused = (
        report.registration is not None
        and report.registration.status == WORKSPACE_REGISTRATION_REFUSED
    )
    rc = 3 if absorbed or registration_refused else 0

    if args.json:
        payload: dict[str, Any] = {
            "checkpoint_id": report.checkpoint_id,
            "status": report.status,
            "members": [_outcome_payload(m) for m in report.members],
        }
        if report.registration is not None:
            payload["registration"] = {
                "status": report.registration.status,
                "detail": report.registration.detail,
                "registered": dict(report.registration.registered),
                "skipped": list(report.registration.skipped),
                "substrate_registered": list(report.registration.substrate_registered),
                "deleted_recorded": list(report.registration.deleted_recorded),
                "refused": dict(report.registration.refused),
            }
        _print_json(payload)
        return rc

    print(f"restore of checkpoint {report.checkpoint_id} {report.status.upper()}")
    print("members:")
    for outcome in report.members:
        line = f"  {outcome.member_path}  outcome={outcome.outcome}  attempts={outcome.attempts}"
        if outcome.resumed_from_prior_run:
            line += "  resumed-from-prior-run"
        print(line)
        print(f"    {outcome.detail}")
    if report.registration is not None:
        print(
            f"registration: {report.registration.status} — {report.registration.detail}"
        )
    if absorbed:
        print(
            "note: absorbed outcomes above are the honest per-member truth — "
            "the restore concluded, it did not silently succeed."
        )
    return rc


# --- entry point -----------------------------------------------------------------


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    root_arg = args.root if args.root is not None else find_coordinator_root()
    if root_arg is None:
        err(f"{_PROG}: not in a git repository")
        return 1
    root = Path(root_arg).resolve()

    stack = _Stack(root)
    try:
        if args.verb == "checkpoint":
            return _cmd_checkpoint(stack, args)
        if args.verb == "list":
            return _cmd_list(stack, args)
        if args.verb == "status":
            return _cmd_status(stack, args)
        return _cmd_restore(stack, args)
    except BinaryFileMemberRefused as exc:
        # Honesty placement #1: the typed refusal names the UTF-8 limitation.
        err(f"{_PROG}: refused ({exc.reason}): {exc}")
        return 2
    except CheckpointUnknown as exc:
        err(f"{_PROG}: refused ({exc.reason}): {exc}")
        return 2
    except CheckpointPersistFailed as exc:
        err(f"{_PROG}: failed ({exc.reason}): {exc}")
        return 2
    except ValueError as exc:
        err(f"{_PROG}: {exc}")
        return 1
    finally:
        stack.close()


if __name__ == "__main__":
    raise SystemExit(main())
