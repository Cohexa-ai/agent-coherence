# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents
"""Workspace Versioning & Restore demo — checkpoint a heterogeneous workspace, let a
failed attempt and a foreign writer trash it, then bring it back honestly.

A workspace spans a file on disk, three S3 objects, and one declared forward-only
action surface. The agent checkpoints it (skew-declared cut + GC pins), a failed
migration attempt plus a foreign S3 writer interfere, and the restore engine drives
one conditional leg per member to a TERMINAL per-member outcome:

- the file member comes back via the detection-guarded version-CAS (``no-arbiter``);
- one S3 object comes back via the native If-Match CAS;
- an object ABSENT at capture but present live is deleted (a delete marker — the
  delete leg restores the ABSENT fact);
- an object raced by a SUSTAINED foreign writer exhausts the bounded re-drive
  budget into the honest absorbing ``conflict`` — the foreign writer's state
  survives (native CAS arbitrated; nothing is blindly clobbered);
- the forward-only member is enumerated and skipped, never invented.

Offline, deterministic, no API keys — a local in-process coordinator service over a
temporary SQLite registry plus the deterministic local S3 fake.

    python -m examples.workspace_versioning.main             # the guarded demo
    python -m examples.workspace_versioning.main --baseline  # loss-first, then guarded

Exit 0 iff the contract holds: the guarded arm restores the restorable members,
reports the absorbing outcomes honestly, and concludes; with ``--baseline`` the
unprotected arm must FIRST demonstrate the loss (no version history, no pointer —
the original bytes are unrecoverable), so the guarantee is measured against its
absence rather than asserted.

Builder demo, NOT an enterprise product. Same shape serious infra ships as
point-in-time recovery — brought to the mixed file+object workspace an agent
actually mutates, with per-member honesty about what can and cannot come back.
"""

from __future__ import annotations

import argparse
import sys
import tempfile
from pathlib import Path
from typing import Any
from uuid import NAMESPACE_URL, uuid5

REPO_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from ccs.adapters.coherent_object import (  # noqa: E402  (after sys.path setup)
    CoherentObject,
    VersionPointerUnconfirmed,
)
from ccs.adapters.workspace import WorkspaceVersioner  # noqa: E402
from ccs.cli.workspace import (  # noqa: E402
    RetainedContentResolver,
    WorkingTreeSource,
)
from ccs.coordinator.service import CoordinatorService  # noqa: E402
from ccs.coordinator.sqlite_registry import SqliteArtifactRegistry  # noqa: E402
from ccs.core.exceptions import (  # noqa: E402
    RESTORE_OUTCOME_CONFLICT,
    RESTORE_OUTCOME_FORWARD_ONLY_SKIPPED,
    RESTORE_OUTCOME_RESTORED,
    RESTORE_STATUS_CONCLUDED,
)
from ccs.testing.s3_local import LocalS3Client  # noqa: E402

BUCKET = "demo"
NOTES = "ws/notes.md"
SUMMARY_KEY = "reports/summary.txt"
FEED_KEY = "data/feed.txt"
LEFTOVER_KEY = "scratch/leftover.txt"
ACTION = "actions/deploy-step"

NOTES_V1 = b"notes: plan the migration\n"
SUMMARY_V1 = b"summary: all systems nominal\n"
FEED_V1 = b"feed: rows 1..100\n"

OWNER = uuid5(NAMESPACE_URL, "ccs-workspace-versioning-demo")


class _TickClock:
    """Deterministic clock for the versioner (whole-second ticks)."""

    def __init__(self, start: int = 100) -> None:
        self._now = start

    def __call__(self) -> int:
        self._now += 1
        return self._now


class _SustainedForeignWriter:
    """Wrap the S3 fake: every LIVE read of ``key`` is followed by a fresh
    foreign write, so the reader's comparand is stale by the time it CASes —
    sustained live-writer contention that exhausts the leg budget into the
    honest absorbing ``conflict``. Pinned reads (VersionId) pass through."""

    def __init__(self, inner: LocalS3Client, bucket: str, key: str) -> None:
        self._inner = inner
        self._bucket = bucket
        self._key = key
        self._n = 0

    def get_object(self, **kwargs: Any) -> Any:
        resp = self._inner.get_object(**kwargs)
        if (
            kwargs.get("Bucket") == self._bucket
            and kwargs.get("Key") == self._key
            and "VersionId" not in kwargs
        ):
            self._n += 1
            self._inner.put_object(
                Bucket=self._bucket,
                Key=self._key,
                Body=f"feed: foreign write {self._n}\n".encode(),
            )
        return resp

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)


def _seed_tree(root: Path) -> None:
    (root / "ws").mkdir(parents=True, exist_ok=True)
    (root / NOTES).write_bytes(NOTES_V1)


def run_guarded(root: Path) -> dict:
    """Checkpoint → interference → restore; the guarantees measured, not asserted."""
    registry = SqliteArtifactRegistry(
        root / ".coherence" / "workspace.db", retain_versions=True
    )
    try:
        service = CoordinatorService(registry)
        s3 = LocalS3Client()
        s3.create_bucket(BUCKET, versioned=True, object_lock=True)
        s3.put_object(Bucket=BUCKET, Key=SUMMARY_KEY, Body=SUMMARY_V1)
        s3.put_object(Bucket=BUCKET, Key=FEED_KEY, Body=FEED_V1)
        _seed_tree(root)

        source = WorkingTreeSource(root, registry, uuid5(OWNER, "file-bridge"))
        resolver = RetainedContentResolver(registry)
        clock = _TickClock()

        capture = WorkspaceVersioner(
            service=service, owner=OWNER, clock=clock, file_resolver=resolver
        )
        binding = CoherentObject(BUCKET, client=s3)
        capture.add_file_member(source, NOTES)
        capture.add_object_member(binding, SUMMARY_KEY)
        capture.add_object_member(binding, FEED_KEY)
        capture.add_object_member(binding, LEFTOVER_KEY)  # ABSENT at capture
        capture.add_forward_only_member(ACTION)

        print("  Agent checkpoints the workspace (pins on by default)...")
        checkpoint = capture.checkpoint("before-migration", pin=True)
        for row in checkpoint.members:
            absent = "  ABSENT-at-capture" if row.absent else ""
            print(
                f"    {row.member_path}  ({row.restore_tier}, {row.pin_state})"
                f"{absent}"
            )

        print("\n  A failed attempt + a foreign writer trash the workspace:")
        (root / NOTES).write_bytes(b"notes: half-migrated garbage\n")
        s3.put_object(Bucket=BUCKET, Key=SUMMARY_KEY, Body=b"summary: half-migrated\n")
        s3.put_object(Bucket=BUCKET, Key=FEED_KEY, Body=b"feed: foreign write 0\n")
        s3.put_object(Bucket=BUCKET, Key=LEFTOVER_KEY, Body=b"stray temp output\n")
        print("    file overwritten / summary overwritten / stray object created")
        print("    ...and a foreign writer keeps racing the feed object.")

        racing = _SustainedForeignWriter(s3, BUCKET, FEED_KEY)
        restore = WorkspaceVersioner(
            service=service, owner=OWNER, clock=clock, file_resolver=resolver
        )
        racing_binding = CoherentObject(BUCKET, client=racing)
        restore.add_file_member(source, NOTES)
        restore.add_object_member(racing_binding, SUMMARY_KEY)
        restore.add_object_member(racing_binding, FEED_KEY)
        restore.add_object_member(racing_binding, LEFTOVER_KEY)
        restore.add_forward_only_member(ACTION)

        print("\n  Restore drives one conditional leg per member:")
        report = restore.restore(checkpoint.record.checkpoint_id)
        for member in report.members:
            print(f"    {member.member_path}  outcome={member.outcome}  "
                  f"attempts={member.attempts}")
            print(f"      {member.detail}")
        if report.registration is not None:
            print(f"    registration: {report.registration.status} — "
                  f"{report.registration.detail}")

        by_path = report.members_by_path
        notes_back = (root / NOTES).read_bytes() == NOTES_V1
        summary_back = binding.read(SUMMARY_KEY)[0] == SUMMARY_V1
        try:
            binding.read(LEFTOVER_KEY)
            leftover_absent = False
        except KeyError:
            leftover_absent = True
        feed_member = by_path[f"s3://{FEED_KEY}"]
        feed_live = binding.read(FEED_KEY)[0]
        feed_honest = (
            feed_member.outcome == RESTORE_OUTCOME_CONFLICT
            and feed_live.startswith(b"feed: foreign write")
        )
        delete_leg = by_path[f"s3://{LEFTOVER_KEY}"]
        delete_ok = (
            delete_leg.outcome == RESTORE_OUTCOME_RESTORED
            and delete_leg.deleted_at_restore is not None
        )
        skip_ok = by_path[ACTION].outcome == RESTORE_OUTCOME_FORWARD_ONLY_SKIPPED
        file_ok = by_path[NOTES].outcome == RESTORE_OUTCOME_RESTORED and notes_back

        print("")
        print(f"  {'ok' if file_ok else 'x'} file member restored "
              "(detection-guarded version-CAS; no-arbiter)")
        print(f"  {'ok' if summary_back else 'x'} S3 summary restored "
              "(native If-Match CAS)")
        print(f"  {'ok' if delete_ok and leftover_absent else 'x'} delete leg "
              "restored the ABSENT fact (delete marker; history survives)")
        print(f"  {'ok' if feed_honest else 'x'} contended feed reported honestly: "
              "conflict after a bounded re-drive; the foreign writer's state "
              "survives (arbitrated, never clobbered)")
        print(f"  {'ok' if skip_ok else 'x'} forward-only member enumerated and "
              "skipped")

        return {
            "prevented": (
                file_ok
                and summary_back
                and leftover_absent
                and delete_ok
                and feed_honest
                and skip_ok
                and report.status == RESTORE_STATUS_CONCLUDED
            ),
        }
    finally:
        registry.close()


def run_baseline(root: Path) -> dict:
    """Negative control: same interference, NO checkpoint — the loss lands."""
    s3 = LocalS3Client()
    s3.create_bucket(BUCKET, versioned=False)  # no history axis at all
    s3.put_object(Bucket=BUCKET, Key=SUMMARY_KEY, Body=SUMMARY_V1)
    _seed_tree(root)
    binding = CoherentObject(BUCKET, client=s3)

    print("  No checkpoint taken. The same interference lands:")
    (root / NOTES).write_bytes(b"notes: half-migrated garbage\n")
    s3.put_object(Bucket=BUCKET, Key=SUMMARY_KEY, Body=b"summary: half-migrated\n")
    print("    file overwritten / summary overwritten (unversioned: replaced)")

    print("  The operator tries to get the workspace back:")
    try:
        binding.read_versioned(SUMMARY_KEY)
        pointer_refused = False
    except VersionPointerUnconfirmed:
        pointer_refused = True
        print(
            "    x S3: VersionPointerUnconfirmed — the unversioned bucket holds "
            "no history and no pointer; the old bytes are GONE"
        )
    notes_lost = (root / NOTES).read_bytes() != NOTES_V1
    summary_lost = binding.read(SUMMARY_KEY)[0] != SUMMARY_V1
    if notes_lost:
        print("    x file: no retained copy anywhere — the original bytes are GONE")
    lost = notes_lost and summary_lost and pointer_refused
    print(f"  {'x LOSS demonstrated' if lost else 'ok (unexpected: no loss?)'} — "
          "this is the failure the checkpoint prevents.")
    return {"lost": lost}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Workspace Versioning & Restore demo (single-host, offline)."
    )
    parser.add_argument(
        "--baseline",
        action="store_true",
        help="run the unprotected loss-first arm before the guarded arm",
    )
    args = parser.parse_args(argv)

    print("Workspace Versioning & Restore — checkpoint a mixed file+S3 workspace,")
    print("then bring it back with per-member honesty.\n")

    baseline_ok = True
    if args.baseline:
        print("Negative control (no checkpoint):")
        with tempfile.TemporaryDirectory() as d:
            baseline_ok = bool(run_baseline(Path(d))["lost"])
        print("")

    print("With a workspace checkpoint:")
    with tempfile.TemporaryDirectory() as d:
        guarded = run_guarded(Path(d))

    print("\nTakeaway: the checkpoint is a skew-declared cut with per-member honesty")
    print("(restorable / restorable-unpinned / forward_only), and the restore is a")
    print("bounded, terminating engine: restorable members come back, a raced member")
    print("loses honestly to the substrate's own arbitration, deletes restore the")
    print("ABSENT fact, and forward-only members are named, never invented.")
    print("Single-host coordinator scope; file members are detection-only (no-arbiter).")

    ok = bool(guarded["prevented"]) and baseline_ok
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())

# Comparing notes on multi-agent coherence?
# https://github.com/Cohexa-ai/agent-coherence/discussions
