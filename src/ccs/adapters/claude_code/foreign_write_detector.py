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

Several properties are load-bearing, and each is a defect this repository has
shipped in some form:

**It is a pure observation.** The re-hash never advances a baseline, never calls
``resolve_or_register`` (which would seed a row from whatever content it finds,
registering the foreign bytes as the new truth and hiding the very write being
detected), and never touches the comparand a safety check reads.

**It counts edges, not levels.** ``git status`` reports a path dirty against the
index on every invocation until it is staged and committed, and the canonical
hash only advances on a mediated commit — so counting on every tick would record
one unreconciled edit forever. The registry gates on the content and outcome it
last counted, and this pass additionally skips the read entirely when the file's
size and modification time have not moved.

**It cannot take the tick down with it.** A deleted or replaced artifact raises
on the READ, not on the git call; unguarded, git would keep reporting it every
tick and detection would stop for good while the liveness count kept advancing.
Every per-artifact step is guarded individually, and the whole pass is guarded
again so it can never abort the four sweep passes that run before it.

**A broken poll is never a quiet month.** The shipped git helper maps a non-zero
exit and a clean tree to the same value; here a failed poll raises, the tick is
NOT recorded, and the run's observed interval is closed so the offline report
shows a hole rather than interpolating across it.

**It asks git for literal paths.** A stored artifact name is data, and git reads
pathspec magic in a path even after ``--``. A name beginning with a colon would
otherwise re-scope or silently exclude the rest of the poll.
"""

from __future__ import annotations

import logging
import os
import stat
import subprocess
import sys
import time
from pathlib import Path
from typing import Iterable, Protocol
from uuid import UUID

from ccs.adapters.claude_code.coordinator_server import _F_SENTINEL_CONTENT_HASH
from ccs.core.states import MESIState
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

# Git environment variables that would redirect the poll at a different
# repository than the one `-C <root>` names. A coordinator started from a
# context that exports these would otherwise poll the wrong tree while still
# recording ticks — the same false-clean shape a swallowed error produces.
_GIT_REDIRECT_VARS = (
    "GIT_DIR",
    "GIT_WORK_TREE",
    "GIT_INDEX_FILE",
    "GIT_COMMON_DIR",
    "GIT_OBJECT_DIRECTORY",
    "GIT_ALTERNATE_OBJECT_DIRECTORIES",
    "GIT_CEILING_DIRECTORIES",
    "GIT_NAMESPACE",
)

# An outstanding write grant is the registry's own evidence that a mediated
# write is in flight, which the timestamp alone cannot supply — see _classify.
_WRITE_GRANT_STATES = (MESIState.MODIFIED, MESIState.EXCLUSIVE)


class DetectionTarget(Protocol):
    """The three attributes this pass reads off the coordinator server.

    Named rather than left implicit so the surface it touches is checkable: the
    pass claims to write nothing outside the detection tables, and a reader can
    only confirm that against a stated set of collaborators.
    """

    coordinator_root: Path
    registry: object
    policy: object


class GitPollError(RuntimeError):
    """The poll could not be completed, so this tick observed nothing.

    Distinct from "git ran and reported no dirty artifact": that is a real
    observation and advances the run's tick count. This is not, and must not.
    """


def _poll_env() -> dict[str, str]:
    """The environment the poll runs under.

    ``GIT_OPTIONAL_LOCKS=0`` stops ``git status`` from refreshing and REWRITING
    the index, which would take ``index.lock`` and contend with the user's own
    git commands — unacceptable for a background poll.

    ``GIT_LITERAL_PATHSPECS=1`` makes every stored name a literal path. Git
    parses pathspec magic inside a path even after ``--``, so without this a
    registered name beginning with a colon can exclude or re-scope the whole
    poll, blinding detection for every other artifact in the same batch.
    """
    env = {k: v for k, v in os.environ.items() if k not in _GIT_REDIRECT_VARS}
    env["GIT_OPTIONAL_LOCKS"] = "0"
    env["GIT_LITERAL_PATHSPECS"] = "1"
    return env


def _git_dirty_paths(root: Path, names: list[str], *, budget_sec: float) -> set[str]:
    """Return the subset of ``names`` git reports as dirty in ``root``.

    ``budget_sec`` bounds the WHOLE poll, not each batch. A per-batch timeout
    lets a large path list stall the sweep thread for the sum of its batches,
    and detection runs in the same loop as grant reclamation — so an instrument
    that overruns delays the next tick's safety work. Exhausting the budget
    raises, which correctly leaves the tick unrecorded.

    ``-c status.relativePaths=true`` is forced rather than assumed: porcelain
    v2 honours that setting, so a checkout configured otherwise would return
    repository-root-relative paths while the registry holds coordinator-root
    ones, and the two would never intersect — a silent permanent zero.

    Any non-zero exit raises. The shipped ``_git`` helper maps a clean non-zero
    exit to the same value as an empty result, which is right for "am I in a
    repo?" and wrong here: it would make a corrupt index or a bad pathspec
    indistinguishable from a clean tree.
    """
    dirty: set[str] = set()
    env = _poll_env()
    deadline = time.monotonic() + budget_sec
    for batch in _batched_pathspecs(names):
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise GitPollError(
                f"git poll exceeded its {budget_sec:.1f}s budget in {root}"
            )
        try:
            result = subprocess.run(
                [
                    "git", "-C", str(root),
                    "-c", "status.relativePaths=true",
                    "status", "--porcelain=v2", "-z", "--untracked-files=no",
                    "--", *batch,
                ],
                check=False,
                capture_output=True,
                timeout=remaining,
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

    Three record kinds carry a path. A ``1`` record is an ordinary change and
    ends in one path. A ``2`` record is a rename or copy, carrying a similarity
    column and TWO NUL-separated paths; both are reported, because either can be
    the registered artifact. A ``u`` record is an unmerged path — a file in a
    merge or rebase conflict — and dropping it would report a conflicted
    coordinated artifact as clean.

    Header lines and every other record kind are ignored, per the format's own
    extensibility rule.
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
            # <XY> <sub> <mH> <mI> <mW> <hH> <hI> <path>
            path = rest.split(" ", 7)[-1] if rest.count(" ") >= 7 else None
            if path:
                paths.add(path)
        elif kind == "u":
            # <XY> <sub> <m1> <m2> <m3> <mW> <h1> <h2> <h3> <path>
            path = rest.split(" ", 9)[-1] if rest.count(" ") >= 9 else None
            if path:
                paths.add(path)
        elif kind == "2":
            # <XY> <sub> <mH> <mI> <mW> <hH> <hI> <X><score> <path>
            path = rest.split(" ", 8)[-1] if rest.count(" ") >= 8 else None
            if path:
                paths.add(path)
            # The rename's origin path is the next NUL-separated field.
            if index < len(fields) and fields[index]:
                paths.add(fields[index])
            index += 1
    return paths


def _argv_encodable(name: str) -> bool:
    """Whether ``name`` can reach a subprocess argument list at all.

    A stored name holding a lone surrogate raises when the argv is encoded —
    outside the two failures the poll maps, so unguarded it would escape to the
    outer guard and, because the name stays in the registry, kill every later
    tick. One such name is a coverage gap for that artifact, never a reason to
    stop watching the rest.
    """
    try:
        name.encode(sys.getfilesystemencoding(), "strict")
    except (UnicodeEncodeError, LookupError):
        return False
    return True


def _disk_hash(path: Path) -> str | None:
    """Hash the file's raw bytes, or return None when it is not observable.

    Raw bytes and the one canonical helper: no decode, no newline translation,
    no git filter. A normalization here would be the first in the system and
    would diverge this comparison from every writer that produced a stored hash.

    One descriptor answers every question, which closes three gaps a
    stat-then-open sequence leaves open. ``O_NOFOLLOW`` refuses a symlink leaf,
    so a coordinated name cannot be pointed at a file outside the workspace.
    ``O_NONBLOCK`` means a path replaced by a named pipe fails instead of
    blocking this thread forever. And checking the size on the open descriptor,
    then reading one byte past the cap, means a file that grows between the
    check and the read is still refused rather than read unbounded.

    None covers every way an artifact can stop being an observable regular file
    — deleted, renamed away, replaced by a directory or a link, permission
    revoked, or oversized. All of those are coverage gaps, not detections.
    """
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0)
    try:
        fd = os.open(path, flags)
    except OSError:
        return None
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode):
            return None
        if info.st_size > _MAX_HASH_BYTES:
            logger.debug("detection: %s exceeds the per-tick hash cap; skipped", path)
            return None
        with os.fdopen(fd, "rb", closefd=True) as handle:
            fd = -1  # ownership moved to the file object
            data = handle.read(_MAX_HASH_BYTES + 1)
        if len(data) > _MAX_HASH_BYTES:
            logger.debug("detection: %s grew past the per-tick hash cap; skipped", path)
            return None
        return sha256_hex(data)
    except OSError:
        return None
    finally:
        if fd >= 0:
            os.close(fd)


def _stat_signature(path: Path) -> tuple[int, int] | None:
    """A cheap ``(size, mtime_ns)`` fingerprint, or None when unobservable.

    Used only to skip work: an artifact whose signature has not moved since the
    last observation usually has no new content to count. A missed skip costs
    one extra read, never a wrong count, so trusting size and modification time
    here carries none of the risk it would carry on a correctness path.

    "Usually" is doing real work in that sentence. One outcome is a function of
    time as well as content — see the cache guard in :func:`_observe`.
    """
    try:
        info = os.stat(path)
    except OSError:
        return None
    return (info.st_size, info.st_mtime_ns)


def _classify_mismatch(
    *,
    updated_at: float | None,
    has_mediated_writer: bool,
    has_write_grant: bool,
    now_unix: float,
    window_sec: float,
) -> str:
    """Name an observation whose disk bytes differ from the canonical hash.

    The caller has already established the mismatch — disk agreeing with the
    canonical hash means the coordinator holds these bytes and the write was one
    it mediated, which needs no store lookups at all. This is the other branch.

    The store can still explain the mismatch two ways, and it needs both. In
    every shipped write path the bytes reach disk BEFORE the registry commit
    lands, so a commit caught mid-flight looks like a divergence. The timestamp
    catches that only when a PREVIOUS commit happens to be recent — during the
    gap it still holds the previous commit's time, so a mediated write to an
    artifact untouched for an hour would read as foreign. An outstanding write
    grant is the registry's own evidence that a write is in flight right now,
    and it covers exactly the case the timestamp misses.

    The timestamp leg additionally requires a witnessed writer: ``updated_at``
    is also stamped by first-observation registration, with no writer behind it,
    and would otherwise suppress a genuine foreign write on a freshly
    registered artifact.

    What either leg admits is not hidden — a suppression is its own counted
    outcome, so an operator can size the exposure rather than trust it.
    """
    if has_write_grant:
        return "lag_suppressed"
    if (
        has_mediated_writer
        and updated_at is not None
        and (now_unix - updated_at) <= window_sec
    ):
        return "lag_suppressed"
    return "foreign"


def run_detection_pass(
    coordinator: DetectionTarget,
    *,
    now_unix: float,
    window_sec: float,
    poll_budget_sec: float,
    stat_cache: dict[str, tuple[tuple[int, int], str]],
) -> int:
    """Run one detection tick. Returns the number of observations counted.

    Raises nothing: every failure is logged and the tick ends. The caller is the
    sweep loop, whose four shipped passes must not be able to fail because an
    observability instrument did.

    ``stat_cache`` is the caller's, held across ticks for one coordinator. It
    holds no coordination state and no safety comparand — only which files are
    worth re-reading, and what each was last called.
    """
    try:
        return _detect(
            coordinator,
            now_unix=now_unix,
            window_sec=window_sec,
            poll_budget_sec=poll_budget_sec,
            stat_cache=stat_cache,
        )
    except Exception as exc:  # noqa: BLE001 — an instrument may never break the sweep
        logger.exception("foreign-write detection tick failed: %s", exc)
        # The run's observed interval ends here. Without this a later successful
        # tick would extend the same interval across the outage, and a coverage
        # question answered from that interval would claim a span nothing
        # watched. The next success opens a new interval and the hole shows.
        try:
            coordinator.registry.close_detection_run()
        except Exception:  # noqa: BLE001 — best effort; never mask the original
            logger.exception("detection: could not close the observed run interval")
        return 0


def _detect(
    coordinator: DetectionTarget,
    *,
    now_unix: float,
    window_sec: float,
    poll_budget_sec: float,
    stat_cache: dict[str, tuple[tuple[int, int], str]],
) -> int:
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
        name
        for name in registry.artifact_names_under_prefix("")
        if policy.is_tracked(name) and _argv_encodable(name)
    ]
    if not covered:
        # Deliberately no tick. A run that watched nothing must not read as a
        # clean zero — that is the reading this instrument exists to prevent,
        # and it is reachable whenever the tracked patterns match no registered,
        # git-tracked artifact.
        return 0

    dirty = _git_dirty_paths(root, covered, budget_sec=poll_budget_sec)
    counted = 0
    for name in sorted(dirty & set(covered)):
        try:
            counted += _observe(
                registry,
                root,
                name,
                now_unix=now_unix,
                window_sec=window_sec,
                stat_cache=stat_cache,
            )
        except Exception:  # noqa: BLE001 — one bad artifact never ends the tick
            logger.exception("detection: skipping %s after an error", name)

    # An artifact git now reports clean has been reconciled, so the content the
    # edge gate is holding is no longer the current divergence. Leaving it
    # would make an identical later edit look already-counted.
    _release_clean_edges(registry, covered, dirty, stat_cache)

    # Only now, and only because the poll completed. A tick counted over a
    # failed poll would let the offline report call a broken instrument a quiet
    # month, which is the one reading the liveness row exists to prevent.
    registry.record_detection_tick(now_unix, covered_count=len(covered))
    return counted


def _release_clean_edges(
    registry,
    covered: list[str],
    dirty: set[str],
    stat_cache: dict[str, tuple[tuple[int, int], str]],
) -> None:
    """Clear the edge gate for covered artifacts git now reports clean.

    Scoped to artifacts that actually carry an edge, which is the small set the
    detector has ever counted — not every clean artifact on every tick.
    """
    clean = set(covered) - dirty
    if not clean:
        return
    for name in clean:
        stat_cache.pop(name, None)
    counted_ids = registry.artifacts_with_detection_edge()
    if not counted_ids:
        return
    to_clear = [
        artifact_id
        for name in clean
        if (artifact_id := registry.lookup_artifact_id_by_name(name)) in counted_ids
    ]
    if to_clear:
        registry.clear_detection_edges(to_clear)


def _observe(
    registry,
    root: Path,
    name: str,
    *,
    now_unix: float,
    window_sec: float,
    stat_cache: dict[str, tuple[tuple[int, int], str]],
) -> int:
    """Classify and count one dirty artifact. Returns 1 if counted, else 0."""
    path = root / name
    signature = _stat_signature(path)
    cached = stat_cache.get(name)
    # Skipping an unchanged file is safe for every outcome EXCEPT a suppression,
    # which is a claim about time as much as content: a mismatch excused because
    # a commit looked in flight has to be re-examined once the window closes, and
    # the bytes do not move while that happens. Caching it would make the first
    # benefit of the doubt permanent — the very defect the outcome-keyed edge
    # gate exists to prevent.
    if (
        signature is not None
        and cached is not None
        and cached[0] == signature
        and cached[1] != "lag_suppressed"
    ):
        return 0  # unchanged since the last observation; nothing new to read

    artifact_id: UUID | None = registry.lookup_artifact_id_by_name(name)
    if artifact_id is None:
        return 0  # git saw a path the coordinator has never observed
    artifact = registry.get_artifact(artifact_id)
    if artifact is None:
        return 0
    canonical = artifact.content_hash
    if not canonical or canonical == _F_SENTINEL_CONTENT_HASH:
        return 0  # a no-claim hash; a mismatch against it means nothing
    disk_hash = _disk_hash(path)
    if disk_hash is None:
        return 0  # not an observable regular file this tick

    # Settle the cheap branch first. Disk agreeing with the canonical hash is
    # the common case on a tick that sees anything at all, and it needs no
    # further reads — asking the store for the grant state, the timestamp, and
    # the last writer before comparing would spend three queries and three lock
    # acquisitions per artifact to answer a question the comparison closed.
    if disk_hash == canonical:
        outcome = "mediated"
    else:
        states = registry.get_state_map(artifact_id).values()
        outcome = _classify_mismatch(
            updated_at=registry.get_artifact_updated_at(artifact_id),
            has_mediated_writer=registry.last_writer_for(artifact_id) is not None,
            has_write_grant=any(state in _WRITE_GRANT_STATES for state in states),
            now_unix=now_unix,
            window_sec=window_sec,
        )
    counted = registry.record_foreign_write(artifact_id, outcome, disk_hash)
    if signature is not None:
        stat_cache[name] = (signature, outcome)
    return 1 if counted else 0
