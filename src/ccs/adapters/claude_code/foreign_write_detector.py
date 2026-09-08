# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""The sweep-side foreign-write detection pass.

The coordinator sees only writes routed through it. The shipped guards catch a
foreign edit at the boundaries — the next read denies on a hash mismatch, the
next write denies on a content mismatch — but nothing observes the window
between them, and that window is where a stale value propagates into derived
artifacts the guards never re-touch.

This pass observes it. Once per sweep tick it asks git which covered artifacts
are dirty, re-hashes only those, and records each newly observed content once as
``foreign`` / ``mediated`` / ``lag_suppressed``. It NEVER enforces: it denies
nothing, invalidates nothing, and writes no coordinator state outside the two
detection tables the registry owns.

Four properties are load-bearing and each one is a defect this repo has already
shipped in some form:

**It is a pure observation.** The re-hash never advances a baseline, never calls
``resolve_or_register`` (which would seed a row from whatever content it finds,
registering the foreign bytes as the new truth and hiding the very write being
detected), and never touches the comparand a safety check reads.

**It counts edges, not levels.** ``git status`` reports a path dirty against the
index on every invocation until it is staged and committed, and the canonical
hash only advances on a mediated commit — so counting on every tick would record
one unreconciled edit forever. The registry gates on the disk hash it last
counted.

**It cannot take the tick down with it.** A deleted or replaced artifact raises
on the READ, not on the git call; unguarded, git would keep reporting it every
tick and detection would stop for good while the liveness count kept advancing.
Every per-artifact step is guarded individually, and the whole pass is guarded
again so it can never abort the four sweep passes that run before it.

**A broken poll is never a quiet month.** The shipped git helper maps a non-zero
exit and a clean tree to the same value; here a failed poll raises and the tick
is NOT recorded, so the offline report shows the gap instead of a false zero.
"""

from __future__ import annotations

import logging
import os
import stat
import subprocess
from pathlib import Path
from typing import Iterable
from uuid import UUID

from ccs.adapters.claude_code.coordinator_server import _F_SENTINEL_CONTENT_HASH
from ccs.core.substrate import sha256_hex

logger = logging.getLogger(__name__)

# Conservative per-invocation budget for the pathspec list. Windows caps a
# process command line at 32,767 characters; the poll stays well under that so
# a large registry is polled in several batches rather than raising E2BIG.
# `git status` rejects --pathspec-from-file, so batching is the only lever.
_MAX_PATHSPEC_BYTES = 24_000

# Beyond this, a single file is not re-hashed on a periodic pass: an
# observability instrument must never be the reason a sweep tick stalls. A skip
# is logged rather than counted, because a size cap is a coverage gap and
# recording it as an outcome would inflate a number an operator reads as writes.
_MAX_HASH_BYTES = 64 * 1024 * 1024

# The no-claim sentinel is IMPORTED, not re-typed: this comparison and the
# shipped strict-mode one must exclude the same value, and a second literal is
# exactly how they would drift apart.


class GitPollError(RuntimeError):
    """The poll could not be completed, so this tick observed nothing.

    Distinct from "git ran and reported no dirty artifact": that is a real
    observation and advances the run's tick count. This is not, and must not.
    """


def _git_dirty_paths(root: Path, names: list[str]) -> set[str]:
    """Return the subset of ``names`` git reports as dirty in ``root``.

    ``GIT_OPTIONAL_LOCKS=0`` is the documented way to stop ``git status`` from
    refreshing and REWRITING the index, which would take ``index.lock`` and
    contend with the user's own git commands — unacceptable for a background
    poll. ``--untracked-files=no`` keeps the cost scoped to tracked files, and
    porcelain v2 with NUL termination is the stable machine format.

    Any non-zero exit raises. The shipped ``_git`` helper maps a clean non-zero
    exit to the same value as an empty result, which is right for "am I in a
    repo?" and wrong here: it would make a corrupt index or a bad pathspec
    indistinguishable from a clean tree.
    """
    dirty: set[str] = set()
    env = {**os.environ, "GIT_OPTIONAL_LOCKS": "0"}
    for batch in _batched_pathspecs(names):
        try:
            result = subprocess.run(
                [
                    "git", "-C", str(root), "status",
                    "--porcelain=v2", "-z", "--untracked-files=no", "--", *batch,
                ],
                check=False,
                capture_output=True,
                timeout=30.0,
                env=env,
            )
        except subprocess.TimeoutExpired as exc:
            raise GitPollError(f"git status timed out in {root}") from exc
        except FileNotFoundError as exc:
            raise GitPollError("git is not on PATH") from exc
        if result.returncode != 0:
            raise GitPollError(
                f"git status exited {result.returncode} in {root}: "
                f"{result.stderr.decode('utf-8', 'replace').strip()[:200]}"
            )
        dirty.update(_parse_porcelain_v2(result.stdout.decode("utf-8", "replace")))
    return dirty


def _batched_pathspecs(names: list[str]) -> Iterable[list[str]]:
    """Split ``names`` into command-line-sized batches, preserving all of them."""
    batch: list[str] = []
    size = 0
    for name in names:
        cost = len(name.encode("utf-8")) + 1
        if batch and size + cost > _MAX_PATHSPEC_BYTES:
            yield batch
            batch, size = [], 0
        batch.append(name)
        size += cost
    if batch:
        yield batch


def _parse_porcelain_v2(payload: str) -> set[str]:
    """Extract changed paths from ``git status --porcelain=v2 -z`` output.

    Two record kinds carry a path. A ``1`` record is an ordinary change and ends
    in one path. A ``2`` record is a rename or copy and carries TWO NUL-separated
    paths plus a similarity column; both sides are reported, because either can
    be the registered artifact. ``#`` header lines and every other record kind
    are ignored, per the format's own extensibility rule.
    """
    fields = payload.split("\0")
    paths: set[str] = set()
    index = 0
    while index < len(fields):
        record = fields[index]
        index += 1
        if not record or record.startswith("#"):
            continue
        kind, _, rest = record.partition(" ")
        if kind == "1":
            path = rest.split(" ", 7)[-1] if rest.count(" ") >= 7 else None
            if path:
                paths.add(path)
        elif kind == "2":
            path = rest.split(" ", 8)[-1] if rest.count(" ") >= 8 else None
            if path:
                paths.add(path)
            # The rename's origin path is the next NUL-separated field.
            if index < len(fields) and fields[index]:
                paths.add(fields[index])
            index += 1
    return paths


def _disk_hash(path: Path) -> str | None:
    """Hash the file's raw bytes, or return None when it is not observable.

    Raw bytes and the one canonical helper: no decode, no newline translation,
    no git filter. A normalization here would be the first in the system and
    would diverge this comparison from every writer that produced a stored hash.

    None covers every way an artifact can stop being a readable regular file —
    deleted, renamed away, replaced by a directory, permission-revoked — and
    also an oversized one. All of those are coverage gaps, not detections.

    The regular-file check stays even though it looks like a TOCTOU pre-check to
    be replaced by open-and-handle-the-error. It is load-bearing rather than
    defensive: opening a FIFO blocks until a writer connects, which would hang
    this thread indefinitely if a coordinated path were ever replaced by a named
    pipe. One stat answers both that question and the size cap.

    The whole file is read before hashing rather than streamed. That is bounded
    by the cap above and only happens for a file git already reported dirty, so
    it buys less than routing every hash through the one canonical helper does.
    """
    try:
        info = path.stat()
        if not stat.S_ISREG(info.st_mode):
            return None
        if info.st_size > _MAX_HASH_BYTES:
            logger.debug("detection: %s exceeds the per-tick hash cap; skipped", path)
            return None
        with open(path, "rb") as handle:
            return sha256_hex(handle.read())
    except OSError:
        return None


def _classify_mismatch(
    *,
    updated_at: float | None,
    has_mediated_writer: bool,
    now_unix: float,
    window_sec: float,
) -> str:
    """Name an observation whose disk bytes differ from the canonical hash.

    The caller has already established the mismatch — disk agreeing with the
    canonical hash means the coordinator holds these bytes and the write was one
    it mediated, which needs no store lookups at all. This is the other branch,
    and it is the only one that costs two extra reads.

    The store may still explain the mismatch: in both shipped write paths the
    bytes reach disk BEFORE the registry commit lands, so a commit observed
    mid-flight looks like a divergence. That excuse is only available when the
    store actually witnesses a mediated commit for the artifact, which is what
    ``has_mediated_writer`` asks — the timestamp alone is also stamped by
    first-observation registration, with no writer behind it, and would suppress
    a genuine foreign write on a freshly registered artifact.

    The window's admitted false-negative is not hidden: a suppression is its own
    counted outcome, so an operator can size the exposure rather than trust it.
    """
    if (
        has_mediated_writer
        and updated_at is not None
        and (now_unix - updated_at) <= window_sec
    ):
        return "lag_suppressed"
    return "foreign"


def run_detection_pass(
    coordinator,
    *,
    now_unix: float,
    window_sec: float,
) -> int:
    """Run one detection tick. Returns the number of observations counted.

    Raises nothing: every failure is logged and the tick ends. The caller is the
    sweep loop, whose four shipped passes must not be able to fail because an
    observability instrument did.
    """
    try:
        return _detect(coordinator, now_unix=now_unix, window_sec=window_sec)
    except Exception as exc:  # noqa: BLE001 — an instrument may never break the sweep
        logger.exception("foreign-write detection tick failed: %s", exc)
        return 0


def _detect(coordinator, *, now_unix: float, window_sec: float) -> int:
    # Bind both to locals for the whole tick. `/policy/track` swaps the policy
    # object atomically while the coordinator runs, and registration is
    # continuous, so a tick that re-read either mid-pass could reason about two
    # different worlds. Never cached across ticks, for the same reason.
    registry = coordinator.registry
    policy = coordinator.policy
    root = Path(coordinator.coordinator_root)

    # The registered names are NOT already policy-filtered: `/session/begin`
    # registers every client-supplied read-set path without consulting the
    # tracked gate, and an artifact untracked afterwards keeps its row. The
    # intersection is what makes the reported coverage true.
    covered = [
        name for name in registry.artifact_names_under_prefix("") if policy.is_tracked(name)
    ]
    if not covered:
        registry.record_detection_tick(now_unix)
        return 0

    dirty = _git_dirty_paths(root, covered)
    counted = 0
    for name in sorted(dirty & set(covered)):
        try:
            counted += _observe(
                registry, root, name, now_unix=now_unix, window_sec=window_sec
            )
        except Exception:  # noqa: BLE001 — one bad artifact never ends the tick
            logger.exception("detection: skipping %s after an error", name)

    # Only now, and only because the poll completed. A tick counted over a
    # failed poll would let the offline report call a broken instrument a quiet
    # month, which is the one reading the liveness row exists to prevent.
    registry.record_detection_tick(now_unix)
    return counted


def _observe(
    registry,
    root: Path,
    name: str,
    *,
    now_unix: float,
    window_sec: float,
) -> int:
    """Classify and count one dirty artifact. Returns 1 if counted, else 0."""
    artifact_id: UUID | None = registry.lookup_artifact_id_by_name(name)
    if artifact_id is None:
        return 0  # git saw a path the coordinator has never observed
    artifact = registry.get_artifact(artifact_id)
    if artifact is None:
        return 0
    canonical = artifact.content_hash
    if not canonical or canonical == _F_SENTINEL_CONTENT_HASH:
        return 0  # a no-claim hash; a mismatch against it means nothing
    disk_hash = _disk_hash(root / name)
    if disk_hash is None:
        return 0  # not an observable regular file this tick

    # Settle the cheap branch first. Disk agreeing with the canonical hash is
    # the common case on a tick that sees anything at all, and it needs no
    # further reads — asking the store for the timestamp and last writer before
    # comparing would spend two queries and two lock acquisitions per artifact
    # to answer a question the comparison already closed.
    if disk_hash == canonical:
        outcome = "mediated"
    else:
        outcome = _classify_mismatch(
            updated_at=registry.get_artifact_updated_at(artifact_id),
            has_mediated_writer=registry.last_writer_for(artifact_id) is not None,
            now_unix=now_unix,
            window_sec=window_sec,
        )
    return 1 if registry.record_foreign_write(artifact_id, outcome, disk_hash) else 0
