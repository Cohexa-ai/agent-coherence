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

**A run row is a claim about a span, so every way of not watching ends it.**
There are three, and the first two are the only ones this pass can see: a poll
that failed, and a tick that found nothing in scope. The third is the pass not
running at all — a suspended host, or the four safety passes ahead of detection
stuck on the store — which leaves no trace here by construction. So a tick that
arrives further from the last one than the sweep cadence can explain opens a
new interval instead of joining the old, and the stall reads as the hole it was.

**An inert workspace is not a broken poll.** A workspace with no repository at
or above it cannot be polled at all: the condition is permanent, no operator
action clears it, and every tick would otherwise log the same traceback until
the coordinator stops. That one condition is reported once at debug and then
latched; every other poll failure keeps its traceback, because it may be
transient and it may be actionable. Both paths record no tick and close the
observed interval, so the offline report shows the same honest hole either way
— only the log volume differs.

**And the quiet state is earned from the filesystem, never from the message.**
Git answers a gutted repository — ``.git`` present, ``HEAD`` or ``objects`` or
``refs`` gone — with the byte-identical "fatal: not a git repository (or any of
the parent directories): .git" and the same exit 128 it gives an absent one.
Classifying on that sentence alone would quiet a corrupt repository, which is
both actionable and precisely the false reassurance this pass exists to refuse.

**What the quiet state cannot tell apart, and why no cheap memory fixes it.** A
workspace whose filesystem drops back to an empty directory satisfies all three
terms: git exits 128 with the sentence, and the walk genuinely finds no
``.git``. That outage can be transient and actionable, and it is quieted as if
permanent. The obvious discriminator — "this root completed a poll once, so a
repository existed" — already exists in ``detection_runs`` and does NOT work
here: the store IS ``<coordinator_root>/.coherence/state.db``, so the outage
that hides the repository hides the memory with it. Measured on a real second
filesystem: a forced unmount under a running coordinator kills the process at
the registry read that PRECEDES the poll, a graceful one is refused while the
store is open, and a coordinator that starts during the outage opens a fresh
store whose ``detection_runs()`` is empty. What is lost is the log line only.
Every branch here records no tick and closes the observed interval, so the
offline report still shows the hole.

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
import threading
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

# The ONLY `git ls-files -v` tags `git status --untracked-files=no` provably
# cannot report on. `S` is skip-worktree: git is told to treat the worktree
# entry as matching the index, so status diffs it never. A LOWERCASE tag is the
# assume-unchanged bit, which buys the same silence by a different mechanism.
# Both were established by mutating the file and watching the poll stay quiet,
# not by reading the manual.
#
# Every other tag is RETAINED, including `M` (unmerged, which the poll reports
# as a `u` record the shipped parser keeps deliberately) and any tag a later git
# emits that this module has never seen. An allowlist of known-good tags would
# instead drop every artifact in the repository for the whole of every merge
# conflict, and would go blind again the day git grows a letter — so the
# fail-closed direction here is the one that keeps an artifact in scope.
_STATUS_BLIND_TAGS = frozenset({"S"})

# The one poll fault reported once rather than per tick. A key rather than a
# bare bool so the caller holds one latch whatever else later joins it.
_FAULT_NOT_A_REPOSITORY = "not-a-git-repository"

# What the OFFLINE report is told when the poll can never read this workspace.
# An opaque stable token, not a message: written here and read back across a
# process and a release boundary by ``ccs.diagnose.foreign_writes``.
_REASON_NO_WORK_TREE = "no-git-work-tree"

# The CEILING on how long the repository walk may take. A handful of `lstat`
# calls is microseconds whenever the filesystem answers at all; the bound is for
# the case where it never does. It is a cap and not the operand — the caller
# passes whatever is left of the poll's own budget, because that budget is
# documented to bound the WHOLE poll, and a walk spent on top of it would
# overrun the sweep interval this pass shares with grant reclamation.
_WALK_BUDGET_SEC = 2.0

# At most one walk may be outstanding per process. A walk that never returns
# leaves an unkillable thread behind — `os.lstat` on a wedged mount cannot be
# interrupted — so starting a fresh one per sweep tick would accumulate threads
# until the process could create none at all. That is a worse failure than the
# one the bound exists to prevent. While a walk is still out, the answer is
# already "present", so a second one would buy nothing.
_walk_lock = threading.Lock()
_walk_in_flight: threading.Thread | None = None

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


class NotAGitRepositoryError(GitPollError):
    """No repository exists at or above the workspace, so the poll never can.

    A subclass rather than a flag: a caller that only cares that the tick
    observed nothing keeps catching ``GitPollError`` and is unaffected, while a
    caller that wants to say something different about a permanent,
    non-actionable condition can name it.

    No tick is recorded and the observed interval is closed, exactly as for
    any other failed poll. What it changes is two things. The log: this one is
    reported once at debug rather than as a traceback per sweep tick, because
    repeating it buries the failures an operator can actually act on. And the
    store: once per time the condition arrives, a note is recorded so the
    offline report can separate "nothing here can be watched" from "the
    instrument never ran" — see ``_record_uncoverable``.

    Which is why it is earned from the FILESYSTEM and not from what git said.
    A repository whose ``.git`` survives but whose ``HEAD``, ``objects`` or
    ``refs`` does not exits 128 with the byte-identical "fatal: not a git
    repository (or any of the parent directories): .git" — verified across all
    three. That repository is broken, an operator can fix it, and quieting it
    would hand out exactly the reassurance this instrument exists to earn.
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

    ``LC_ALL=C`` pins the language of git's own diagnostics, which this module
    READS: "not a git repository" is the signature that separates a permanently
    inert workspace from a poll worth a traceback, and git translates that
    sentence under a localized locale (verified: an ``fr_FR.UTF-8`` operator
    gets "ni ceci ni aucun de ses répertoires parents n'est un dépôt git").
    Pinned per-invocation for the same reason ``status.relativePaths`` is —
    an output shape this code parses must not be inherited from the ambient
    environment. It is a message-catalog setting only: ``-z`` already suppresses
    path quoting, so the porcelain bytes are unchanged.

    The literal ``C`` is load-bearing. ``POSIX``, which reads as a synonym, does
    NOT suppress ``LANGUAGE`` — gettext special-cases only ``C``/``C.UTF-8`` —
    and on macOS an unset locale is not the C locale either, because libintl
    falls back to CoreFoundation's preferred languages. Setting no locale is
    therefore not equivalent to setting this one.
    """
    env = {k: v for k, v in os.environ.items() if k not in _GIT_REDIRECT_VARS}
    env["GIT_OPTIONAL_LOCKS"] = "0"
    env["GIT_LITERAL_PATHSPECS"] = "1"
    env["LC_ALL"] = "C"
    return env


def _git_dirty_paths(root: Path, names: list[str], *, deadline: float) -> set[str]:
    """Return the subset of ``names`` git reports as dirty in ``root``.

    ``deadline`` is a monotonic INSTANT, not a duration, and the caller mints
    exactly one of them per tick. That is the whole mechanism by which the
    budget bounds the tick rather than each helper: a second git call handed a
    copied duration would mint a second full budget, and one tick could run for
    twice ``poll_budget_sec`` on the sweep thread that also reclaims grants and
    reaps dead sessions. The same reasoning rules out a per-batch timeout, which
    would let a large path list stall that thread for the sum of its batches.
    Exhausting the deadline raises, which correctly leaves the tick unrecorded.

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
    for batch in _batched_pathspecs(names):
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise GitPollError(f"git poll exceeded its budget in {root}")
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
            stderr = result.stderr.decode("utf-8", "replace").strip()
            message = f"git status exited {result.returncode} in {root}: {stderr[:200]}"
            # Two halves, and NEITHER is sufficient alone. 128 is git's generic
            # fatal exit — a bad object, a corrupt index and a dubious-ownership
            # refusal all land on it — so the sentence is what says this was a
            # repository-discovery failure (`_poll_env` pins the language so the
            # match holds under any locale). And the sentence is what git also
            # says about a repository whose `.git` is present but gutted, so the
            # filesystem is what says the repository is absent rather than
            # broken. Quiet requires both; anything else keeps its traceback.
            # The walk's own bound is a ceiling; what it actually gets is
            # whatever is left of the budget that bounds this WHOLE poll, so a
            # failed classification cannot push the tick past the sweep
            # interval it shares with the coordinator's grant and session work.
            walk_budget = min(_WALK_BUDGET_SEC, max(0.0, deadline - time.monotonic()))
            if (
                result.returncode == 128
                and "not a git repository" in stderr.lower()
                and _repository_is_absent(root, budget_sec=walk_budget)
            ):
                raise NotAGitRepositoryError(message)
            raise GitPollError(message)
        dirty.update(_parse_porcelain_v2(result.stdout.decode("utf-8", "replace")))
    return dirty


def _git_visible_names(root: Path, *, deadline: float) -> set[str]:
    """Return every name in ``root``'s index that the status poll can report on.

    The pass claims coverage of what it polls, and the poll runs with
    ``--untracked-files=no``. A registered artifact git never tracked, or one an
    exclude rule hides, is therefore reported clean on every tick forever;
    counting it as covered manufactures the quiet month this whole module exists
    to refuse. So the claimed scope has to be intersected with what git can
    speak about at all, and this is the read that says which names those are.

    ONE invocation over the whole index, with NO pathspecs. That is not an
    optimisation. Handing it the stored names would reinstate pathspec magic on
    data, let one out-of-repo name fail an entire batch, let a registered
    directory name match its children instead of itself, and drag the argv
    batching along with all three. None of it can happen to a command that takes
    no paths at all.

    ``--full-name`` is required for the opposite reason to the obvious one.
    Without it ``ls-files`` prints names relative to the invocation directory;
    with it they are pinned to the repository top — the shape porcelain v2
    already emits, and the shape the registry holds, so the two intersect. The
    poll's ``-c status.relativePaths=true`` is deliberately NOT copied across:
    ``ls-files`` does not honour it, so it would read as a pin while pinning
    nothing, and a name shape this code compares must never be inherited.

    Any non-zero exit RAISES, for the same reason the poll's does and one more.
    An empty visible set is about to mean "nothing here can be watched", so
    returning it for a read that failed would hand out that verdict on no
    evidence — a swallowed error reading as a narrowed scope is the same defect
    as a swallowed poll reading as a clean tree.

    It never classifies. The exit-128 ``NotAGitRepositoryError`` split, its
    bounded filesystem walk and its once-per-arrival latch belong to the status
    poll, which runs FIRST. A second classifier here would make this call the
    one that meets a non-repository, and put the entire no-work-tree story
    behind a classifier nothing else has ever exercised.
    """
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        # Checked before the spawn, not after: a helper that started git anyway
        # and let the timeout catch it would still cost a process launch on a
        # budget that is already gone.
        raise GitPollError(f"git poll exceeded its budget before ls-files in {root}")
    try:
        result = subprocess.run(
            ["git", "-C", str(root), "ls-files", "-z", "-v", "--full-name"],
            check=False,
            capture_output=True,
            timeout=remaining,
            env=_poll_env(),
        )
    except subprocess.TimeoutExpired as exc:
        raise GitPollError(f"git ls-files timed out in {root}") from exc
    except FileNotFoundError as exc:
        raise GitPollError("git is not on PATH") from exc
    if result.returncode != 0:
        stderr = result.stderr.decode("utf-8", "replace").strip()
        raise GitPollError(
            f"git ls-files exited {result.returncode} in {root}: {stderr[:200]}"
        )
    return _parse_ls_files_v(result.stdout.decode("utf-8", "replace"))


def _repository_is_absent(root: Path, *, budget_sec: float = _WALK_BUDGET_SEC) -> bool:
    """Whether NO ``.git`` exists at ``root`` or at any directory above it.

    The walk runs on a bounded daemon thread because its own calls cannot be
    interrupted: ``os.lstat`` on a wedged network or FUSE mount never returns,
    and this runs on the sweep thread, whose other four passes reclaim grants
    and reap dead sessions. ``_disk_hash`` states the same rule for the read
    path — a path replaced by a named pipe "fails instead of blocking this
    thread forever" — and this is that rule on the classification path. A walk
    that has not answered within the budget says present, like every ambiguity
    in the walk itself, so a wedged mount stays loud instead of going quiet.

    One walk at a time per process, for the same reason the bound exists: the
    thread left behind by a wedged mount cannot be killed, so a fresh one per
    tick would accumulate. A walk still in flight means the last one did not
    answer, which is already the present/loud reading.
    """
    global _walk_in_flight

    answer: list[bool] = []

    def _answer() -> None:
        # Guarded here and not only in the pass: an exception raised on this
        # thread never reaches ``run_detection_pass``, it reaches
        # ``threading.excepthook`` — which prints the per-tick traceback this
        # pass exists to stop printing. An unanswered walk is already the loud
        # reading, so the guard costs nothing but the noise.
        try:
            answer.append(_walk_for_repository(root))
        except Exception:  # noqa: BLE001 — a thread's raise escapes the pass
            logger.exception("detection: the repository walk failed")

    walker = threading.Thread(target=_answer, name="coord-fwd-walk", daemon=True)
    with _walk_lock:
        if _walk_in_flight is not None and _walk_in_flight.is_alive():
            return False
        try:
            walker.start()
        except RuntimeError:
            # The process cannot create a thread. Saying present keeps this
            # loud, where raising would reintroduce the per-tick traceback.
            return False
        _walk_in_flight = walker
    walker.join(budget_sec)
    return answer[0] if answer else False


def _walk_for_repository(root: Path) -> bool:
    """The walk itself. Separated so the bound above has something to bound.

    The half of the missing-repository signature that git cannot supply. Git's
    message is identical for an absent repository and for a corrupt one, so the
    quiet state is earned here or not at all.

    It walks ``-C <root>`` and upward, like git — but it does NOT mirror git,
    and the difference is deliberate. Git stops at a filesystem boundary unless
    ``GIT_DISCOVERY_ACROSS_FILESYSTEM`` is set; this walk crosses. So a
    workspace on its own mount under a repository keeps its per-tick traceback,
    which is only noise.

    Do not "fix" that by stopping when ``st_dev`` changes. Measured on a real
    second filesystem: a GUTTED repository across a mount boundary produces the
    byte-identical "fatal: not a git repository (or any of the parent
    directories): .git", with no boundary line to tell it apart — so a walk
    that stopped at the boundary would answer absent and quiet a BROKEN
    repository, which is the one reading the third term exists to refuse.
    Crossing is what keeps that case loud. Doing it safely would additionally
    mean reimplementing git's boolean grammar for that variable and stat-ing a
    second time per level, every failure of which would have to resolve loud —
    new branches on the one path whose whole job is refusing a false quiet,
    bought with log lines.

    Every asymmetry runs toward the loud answer, because a wrong "absent" is the
    expensive one — it is the reading that turns a broken repository into a
    clean-looking silence:

    * ``lstat``, not ``exists`` — a dangling ``.git`` symlink is a BROKEN
      repository, not an absent one, and must keep its traceback.
    * ``lstat``, not ``os.path.lexists`` — that helper folds a stat ERROR into
      the same ``False`` it gives a missing file, so an unsearchable parent
      directory would read as "no repository here" and be quieted. Only
      ``FileNotFoundError`` keeps the walk going; anything else says present.
    * a root that cannot even be resolved says present, for the same reason.
    """
    # Intentionally no I/O bound here — the caller owns it.
    try:
        resolved = root.resolve()
    except OSError:
        return False
    for directory in (resolved, *resolved.parents):
        try:
            os.lstat(directory / ".git")
        except FileNotFoundError:
            continue  # this level has none; git looks higher, so do we
        except OSError:
            return False  # cannot tell — stay loud
        return False  # something is there, even a dangling symlink
    return True


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


def _parse_ls_files_v(payload: str) -> set[str]:
    """Extract the status-visible names from ``git ls-files -z -v`` output.

    Each NUL-separated record is ``<tag> <path>``. Under ``-z`` git applies no
    quoting and no C-escaping, so the bytes after that first space ARE the name:
    running an unescape helper over them would corrupt every path holding a
    space, a quote or a non-ASCII byte into a registry key that matches nothing,
    and the artifact would drop out of scope with no error anywhere.

    A set, because an unmerged path appears once per index stage — three records
    naming one artifact.

    Only ``_STATUS_BLIND_TAGS`` and the lowercase assume-unchanged tags are
    dropped; every other tag is kept, recognised or not. A record carrying no
    space carries no path either, so there is nothing to keep and it is skipped.
    """
    names: set[str] = set()
    for record in payload.split("\0"):
        if not record:
            continue
        tag, separator, path = record.partition(" ")
        if not separator or not path:
            continue
        if tag in _STATUS_BLIND_TAGS or tag.islower():
            continue
        names.add(path)
    return names


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
    reported_faults: set[str],
    tick_clock: dict[str, float],
    max_gap_sec: float,
) -> int:
    """Run one detection tick. Returns the number of observations counted.

    Raises nothing: every failure is logged and the tick ends. The caller is the
    sweep loop, whose four shipped passes must not be able to fail because an
    observability instrument did.

    ``stat_cache`` is the caller's, held across ticks for one coordinator. It
    holds no coordination state and no safety comparand — only which files are
    worth re-reading, and what each was last called.

    ``reported_faults`` is the caller's too, and holds strictly less: which
    permanent, non-actionable conditions have already been reported for this
    coordinator, so a workspace that can never be polled says so once instead of
    once per tick. Nothing reads it but the log, and a completed poll clears it
    — the latch tracks the condition, so a workspace that later becomes a
    repository and then is not one again reports the second time too.

    ``tick_clock`` is the caller's third piece and holds one number: when this
    pass last recorded a tick. It is what lets a stall be seen at all — the
    pass cannot observe the interval in which it did not run, so the only
    evidence is the distance back to the tick before it. ``max_gap_sec`` is how
    far apart two ticks may be and still belong to one continuously observed
    span; the caller owns it because the caller owns the cadence. Both reset
    with the process, which is correct: a restart opens a fresh run anyway.
    """
    try:
        return _detect(
            coordinator,
            now_unix=now_unix,
            window_sec=window_sec,
            poll_budget_sec=poll_budget_sec,
            stat_cache=stat_cache,
            reported_faults=reported_faults,
            tick_clock=tick_clock,
            max_gap_sec=max_gap_sec,
        )
    except NotAGitRepositoryError as exc:
        # Not an error to hand an operator every five seconds: the instrument is
        # inert here and no reading of the log changes that. Reported without a
        # traceback, because the stack says nothing the sentence does not, and
        # a per-tick traceback buries the failures that ARE actionable — the
        # demo whose temp workspace surfaced this printed one per tick over its
        # own stdout. No tick is recorded and the interval closes, as for any
        # other failure — and, once per time the condition arrives, a note is
        # written so the offline report can say WHY there are no ticks.
        _close_observed_interval(coordinator)
        if _FAULT_NOT_A_REPOSITORY not in reported_faults:
            logger.debug("foreign-write detection is inert: %s", exc)
            if _record_uncoverable(coordinator, now_unix):
                reported_faults.add(_FAULT_NOT_A_REPOSITORY)
        return 0
    except Exception as exc:  # noqa: BLE001 — an instrument may never break the sweep
        logger.exception("foreign-write detection tick failed: %s", exc)
        _close_observed_interval(coordinator)
        return 0


def _close_observed_interval(coordinator: DetectionTarget) -> None:
    """End the run's observed interval after a tick that observed nothing.

    Without this a later successful tick would extend the same interval across
    the outage, and a coverage question answered from that interval would claim
    a span nothing watched. The next success opens a new interval and the hole
    shows. Every tick that observed nothing closes it — a failed poll, the
    inert workspace, a tick with nothing in scope, and a tick that arrives too
    late to join the interval before it. The inert-workspace case closes it
    too, and then records why — see ``_record_uncoverable``.
    """
    try:
        coordinator.registry.close_detection_run()
    except Exception:  # noqa: BLE001 — best effort; never mask the original
        logger.exception("detection: could not close the observed run interval")


def _record_uncoverable(coordinator: DetectionTarget, now_unix: float) -> bool:
    """Record that this workspace has nothing the poll can ever read, so the
    OFFLINE report can say so. True once the note is durable.

    Without the row the store reads as not-instrumented — the same answer a
    store gets when the sweep was off or the instrument failed every tick,
    which is precisely the collapse the report's states exist to prevent.

    The caller gates this on the latch that also gates the log, and closes the
    latch only on True. Once per transition rather than once per tick because
    the poll retries every tick and ``_close_observed_interval`` rotates the
    run id on every failure, so a per-tick record keyed on that id would write
    one row per sweep interval. And only on True so a store that refuses the
    note — locked, out of disk — is retried on the next tick rather than left
    holding a report that accuses the instrument of never running; that
    failure is a real fault and stays loud. The latch re-arms when a poll
    completes, so a workspace that becomes a repository and later stops being
    one is noted again: that is a second fact.

    The note lands on a run id of its own. The caller has just retired any
    interval that was open, so no tick precedes it on this id; the close here
    retires the id so no tick can follow it — the poll can succeed on the very
    next tick, and a run must never read as both an interval the detector
    watched and a workspace with nothing to watch.
    """
    try:
        coordinator.registry.record_detection_uncoverable(_REASON_NO_WORK_TREE, now_unix)
        coordinator.registry.close_detection_run()
    except Exception:  # noqa: BLE001 — an instrument may never break the sweep
        logger.exception("detection: could not record that the workspace has no work tree")
        return False
    return True


def _detect(
    coordinator: DetectionTarget,
    *,
    now_unix: float,
    window_sec: float,
    poll_budget_sec: float,
    stat_cache: dict[str, tuple[tuple[int, int], str]],
    reported_faults: set[str],
    tick_clock: dict[str, float],
    max_gap_sec: float,
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
        #
        # And the interval closes, exactly as it does on a failed poll. Not
        # recording the tick is only half of it: the run row this window sits
        # inside is read as CONTINUOUSLY observed, so leaving it open lets the
        # next successful tick extend the same interval across the window and
        # `covers()` answer True for a span nothing was polled in. That is the
        # false clean the whole four-state report exists to refuse, and it
        # needs no corruption, no mount and no locale to reach — `/policy/track`
        # swaps the tracked set while the coordinator runs, and the shipped
        # untrack command is one way an operator empties it.
        _close_observed_interval(coordinator)
        return 0

    # Minted once, here, and shared by every git call this tick makes. A helper
    # handed the DURATION instead would mint a second full budget, letting one
    # tick run for twice `poll_budget_sec` on the thread that also reclaims
    # grants and reaps dead sessions.
    deadline = time.monotonic() + poll_budget_sec
    dirty = _git_dirty_paths(root, covered, deadline=deadline)
    # The poll completed, so whatever this workspace was when the latch was set,
    # it is pollable now: re-arm, and a workspace that stops being a repository
    # again is reported the second time too. Only a completed poll clears it —
    # the early returns above observed nothing about the condition.
    reported_faults.discard(_FAULT_NOT_A_REPOSITORY)

    # The claimed scope, narrowed to what the poll above can actually speak
    # about. `--untracked-files=no` means a registered artifact git never
    # tracked — or one an exclude rule, `--skip-worktree` or `--assume-unchanged`
    # hides — is reported clean on every tick for the life of the workspace, so
    # counting it as covered manufactures exactly the quiet month this module
    # exists to refuse.
    #
    # The SAME deadline the status poll used, deliberately: one budget bounds
    # the whole tick, and a second instant minted here would let one pass run
    # for twice `poll_budget_sec` on the thread that also reclaims grants and
    # reaps dead sessions.
    #
    # It raises like the poll does, and is not caught here. A visibility read
    # that failed must never be read as a narrowed scope — that is the poll's
    # "swallowed error reads as a clean tree" defect wearing a different hat,
    # and an unrecorded tick is the honest answer to a scope nobody could read.
    #
    # `covered` stays bound. The eviction pass needs the difference between the
    # two sets, which overwriting the name here would destroy.
    visible = set(covered) & _git_visible_names(root, deadline=deadline)

    counted = 0
    for name in sorted(dirty & visible):
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
    _release_clean_edges(registry, visible, dirty, stat_cache)

    # Only now, and only because the poll completed. A tick counted over a
    # failed poll would let the offline report call a broken instrument a quiet
    # month, which is the one reading the liveness row exists to prevent.
    #
    # And only into an interval this tick can honestly join. The run row says
    # its span was continuously observed, so a tick that arrives further from
    # the last one than the cadence explains would stretch that claim over a
    # stretch nothing watched — the same false clean as a failed poll, reached
    # by the pass not running rather than by the pass failing. Closing first
    # makes this tick open a new interval and leaves the stall as a hole. A
    # clock that steps backwards shortens the apparent gap rather than
    # lengthening it, so it can only under-split, never invent a hole.
    last_tick = tick_clock.get("last_tick_unix")
    if last_tick is not None and (now_unix - last_tick) > max_gap_sec:
        _close_observed_interval(coordinator)
    # The visible set, not the claimed one: the number a report reads has to
    # describe the same population the observe loop and the edge pass just ran
    # over, or a coverage question is answered about one scope with a count
    # taken over another.
    registry.record_detection_tick(now_unix, covered_count=len(visible))
    tick_clock["last_tick_unix"] = now_unix
    return counted


def _release_clean_edges(
    registry,
    visible: set[str],
    dirty: set[str],
    stat_cache: dict[str, tuple[tuple[int, int], str]],
) -> None:
    """Clear the edge gate for visible artifacts git now reports clean.

    Scoped to artifacts that actually carry an edge, which is the small set the
    detector has ever counted — not every clean artifact on every tick.

    ``visible`` and not the whole claimed scope, for the reason the count is
    narrowed too: an artifact the poll cannot report on is absent from ``dirty``
    on every tick regardless of its bytes, so reading that absence as "git now
    reports it clean" would call it reconciled on the strength of a question
    never asked.
    """
    clean = visible - dirty
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
