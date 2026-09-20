# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""U1/U4 — the sweep-side detection pass, driven one tick at a time.

Every test here calls ``run_detection_pass`` directly rather than sleeping past
a sweep interval: the unit under test is "does one tick classify correctly", not
"does a timer fire". Assertions are before-and-after DELTAS, never absolute
post-state, so an increment that never happens cannot pass vacuously.

Fixtures commit their own artifacts. This repository's own default tracked
patterns match zero git-tracked files — ``docs/*`` is git-ignored and
``CLAUDE.md`` sits in the local exclude file — so a fixture leaning on the
defaults would exercise nothing.
"""

from __future__ import annotations

import shutil
import subprocess
import time
from pathlib import Path

import pytest

from ccs.adapters.claude_code.foreign_write_detector import (
    GitPollError,
    NotAGitRepositoryError,
    _classify_mismatch,
    _git_dirty_paths,
    _git_visible_names,
    _parse_ls_files_v,
    _parse_porcelain_v2,
    _poll_env,
    _repository_is_absent,
    run_detection_pass,
)
from ccs.adapters.claude_code.policy import TrackedArtifactPolicy
from ccs.coordinator.sqlite_registry import SqliteArtifactRegistry
from ccs.core.substrate import sha256_hex
from ccs.diagnose.foreign_writes import NOT_COVERABLE, NOT_INSTRUMENTED, read_foreign_write_report

WINDOW = 10.0
POLL_BUDGET = 30.0
# Generous by default so the existing tests, which step now_unix by whatever
# suits them, never trip the staleness split; the tests that care pass their own.
MAX_GAP = 1_000_000.0


def _deadline(budget_sec: float = POLL_BUDGET) -> float:
    """The poll's one monotonic deadline, minted the way ``_detect`` mints it.

    ``_git_dirty_paths`` takes the INSTANT rather than the duration so that
    every git call in a tick shares one budget; a helper handed a duration
    would mint a second full one.
    """
    return time.monotonic() + budget_sec


def _tick(
    coordinator,
    *,
    now_unix: float,
    window_sec: float = WINDOW,
    cache=None,
    faults=None,
    clock=None,
    max_gap_sec: float = MAX_GAP,
) -> int:
    """One tick with the two caller-owned pieces the sweep loop holds across
    ticks: the stat cache, and the latch of already-reported permanent faults.
    A test that cares about either passes its own and reuses it."""
    return run_detection_pass(
        coordinator,
        now_unix=now_unix,
        window_sec=window_sec,
        poll_budget_sec=POLL_BUDGET,
        stat_cache=cache if cache is not None else {},
        reported_faults=faults if faults is not None else set(),
        tick_clock=clock if clock is not None else {},
        max_gap_sec=max_gap_sec,
    )


class _Coordinator:
    """The three attributes the pass reads off the real coordinator server."""

    def __init__(self, root: Path, registry, policy) -> None:
        self.coordinator_root = root
        self.registry = registry
        self.policy = policy


def _git(root: Path, *args: str) -> None:
    subprocess.run(["git", "-C", str(root), *args], check=True, capture_output=True)


def _policy(root: Path, *patterns: str) -> TrackedArtifactPolicy:
    """A policy covering exactly ``patterns``. Constructed directly rather than
    via ``load`` so a test states its tracked set inline; the loader's YAML and
    traversal guard are covered by the policy's own suite."""
    return TrackedArtifactPolicy(coordinator_root=root, tracked_patterns=patterns)


@pytest.fixture
def repo(tmp_path: Path):
    """A real git repo whose tracked artifact is actually committed."""
    root = tmp_path / "repo"
    root.mkdir()
    _git(root, "init", "-q")
    _git(root, "config", "user.email", "t@example.com")
    _git(root, "config", "user.name", "t")
    (root / "notes.md").write_text("v1\n")
    _git(root, "add", "notes.md")
    _git(root, "commit", "-qm", "seed")
    return root


@pytest.fixture
def coordinator(repo: Path):
    registry = SqliteArtifactRegistry(repo / ".coherence" / "state.db")
    policy = _policy(repo, "notes.md")
    yield _Coordinator(repo, registry, policy)
    registry.close()


def _register(coordinator, name: str, content: str):
    """Register the artifact with the content currently on disk as canonical."""
    return coordinator.registry.resolve_or_register(name, sha256_hex(content.encode()))


def _totals(coordinator, artifact_id):
    return coordinator.registry.foreign_write_totals().get(artifact_id, {})


# ---------------------------------------------------------------------------
# Classification (R5, R6)
# ---------------------------------------------------------------------------


def test_a_foreign_edit_is_counted_foreign(coordinator, repo: Path) -> None:
    art = _register(coordinator, "notes.md", "v1\n")
    before = _totals(coordinator, art)
    (repo / "notes.md").write_text("edited by something else\n")

    _tick(coordinator, now_unix=1000.0)

    after = _totals(coordinator, art)
    assert before.get("foreign", 0) == 0
    assert after.get("foreign", 0) == 1


def test_disk_matching_the_canonical_hash_is_mediated(coordinator, repo: Path) -> None:
    """Git says the file moved; the coordinator holds these exact bytes, so the
    write was one it mediated."""
    (repo / "notes.md").write_text("v2 through the coordinator\n")
    art = _register(coordinator, "notes.md", "v2 through the coordinator\n")

    _tick(coordinator, now_unix=1000.0)

    assert _totals(coordinator, art) == {"mediated": 1}


def test_a_recent_mediated_commit_suppresses_rather_than_accuses(
    coordinator, repo: Path
) -> None:
    """The bytes reach disk before the ledger commit lands, so a commit caught
    mid-flight looks like a divergence. Suppressed, and counted as suppressed."""
    art = _register(coordinator, "notes.md", "v1\n")
    coordinator.registry.commit_cas(
        art,
        __import__("uuid").uuid4(),
        expected_version=coordinator.registry.get_artifact(art).version,
        content_hash=sha256_hex(b"in flight\n"),
        content="in flight",
    )
    (repo / "notes.md").write_text("newer bytes not yet committed\n")
    updated_at = coordinator.registry.get_artifact_updated_at(art)

    _tick(coordinator, now_unix=updated_at + 1.0)

    totals = _totals(coordinator, art)
    assert totals.get("lag_suppressed", 0) == 1
    assert totals.get("foreign", 0) == 0


def test_outside_the_window_the_same_mismatch_is_foreign(
    coordinator, repo: Path
) -> None:
    art = _register(coordinator, "notes.md", "v1\n")
    coordinator.registry.commit_cas(
        art,
        __import__("uuid").uuid4(),
        expected_version=coordinator.registry.get_artifact(art).version,
        content_hash=sha256_hex(b"in flight\n"),
        content="in flight",
    )
    (repo / "notes.md").write_text("newer bytes not yet committed\n")
    updated_at = coordinator.registry.get_artifact_updated_at(art)

    _tick(coordinator, now_unix=updated_at + WINDOW + 5.0)

    assert _totals(coordinator, art).get("foreign", 0) == 1


def test_a_never_written_artifact_is_not_suppressed_by_its_registration(
    coordinator, repo: Path
) -> None:
    """First-observation registration stamps updated_at with NO writer behind
    it. Keying suppression on the timestamp alone would excuse a genuine
    foreign write on a freshly registered artifact."""
    art = _register(coordinator, "notes.md", "v1\n")
    (repo / "notes.md").write_text("foreign, moments after registration\n")
    updated_at = coordinator.registry.get_artifact_updated_at(art)

    _tick(coordinator, now_unix=updated_at + 0.1)

    assert _totals(coordinator, art).get("foreign", 0) == 1
    assert _totals(coordinator, art).get("lag_suppressed", 0) == 0


# ---------------------------------------------------------------------------
# Edge-triggering (R15)
# ---------------------------------------------------------------------------


def test_an_unreconciled_edit_is_counted_once_not_once_per_tick(
    coordinator, repo: Path
) -> None:
    """Git keeps reporting the file dirty until it is committed. Ten more ticks
    must not turn one edit into eleven."""
    art = _register(coordinator, "notes.md", "v1\n")
    (repo / "notes.md").write_text("foreign\n")

    for tick in range(11):
        _tick(coordinator, now_unix=1000.0 + tick)

    assert _totals(coordinator, art) == {"foreign": 1}


def test_a_second_distinct_edit_counts_again(coordinator, repo: Path) -> None:
    art = _register(coordinator, "notes.md", "v1\n")
    (repo / "notes.md").write_text("foreign one\n")
    _tick(coordinator, now_unix=1000.0)
    (repo / "notes.md").write_text("foreign two\n")
    _tick(coordinator, now_unix=1001.0)

    assert _totals(coordinator, art) == {"foreign": 2}


# ---------------------------------------------------------------------------
# Coverage (R14) and non-mutation (R9)
# ---------------------------------------------------------------------------


def test_an_untracked_registered_artifact_is_outside_the_instrument(
    coordinator, repo: Path
) -> None:
    """The registered-name list is not policy-filtered — session/begin registers
    client-supplied paths without consulting the tracked gate."""
    (repo / "other.md").write_text("x\n")
    _git(repo, "add", "other.md")
    _git(repo, "commit", "-qm", "other")
    art = _register(coordinator, "other.md", "x\n")
    (repo / "other.md").write_text("foreign\n")

    _tick(coordinator, now_unix=1000.0)

    assert _totals(coordinator, art) == {}


def test_a_git_ignored_artifact_is_outside_the_instrument(
    coordinator, repo: Path
) -> None:
    (repo / ".gitignore").write_text("secret.md\n")
    (repo / "secret.md").write_text("x\n")
    coordinator.policy = _policy(repo, "secret.md")
    art = _register(coordinator, "secret.md", "x\n")
    (repo / "secret.md").write_text("foreign\n")

    _tick(coordinator, now_unix=1000.0)

    assert _totals(coordinator, art) == {}


def test_a_dirty_path_with_no_artifact_row_seeds_nothing(
    coordinator, repo: Path
) -> None:
    """R9 and the resolve_or_register trap: looking a path up must not create
    it, or the foreign bytes become the new baseline and hide the write."""
    art = _register(coordinator, "notes.md", "v1\n")
    (repo / "extra.md").write_text("never registered\n")
    _git(repo, "add", "extra.md")
    _git(repo, "commit", "-qm", "extra")
    (repo / "extra.md").write_text("changed\n")
    coordinator.policy = _policy(repo, "notes.md", "extra.md")
    before = coordinator.registry.artifact_names_under_prefix("")

    _tick(coordinator, now_unix=1000.0)

    assert coordinator.registry.artifact_names_under_prefix("") == before
    assert coordinator.registry.lookup_artifact_id_by_name("extra.md") is None
    assert art is not None


def test_the_canonical_hash_is_never_advanced_by_a_detection(
    coordinator, repo: Path
) -> None:
    """A verification read must not heal what it checks: after detection the
    write-time guard must still see the same divergence."""
    art = _register(coordinator, "notes.md", "v1\n")
    canonical_before = coordinator.registry.get_artifact(art).content_hash
    version_before = coordinator.registry.get_artifact(art).version
    (repo / "notes.md").write_text("foreign\n")

    _tick(coordinator, now_unix=1000.0)

    artifact = coordinator.registry.get_artifact(art)
    assert artifact.content_hash == canonical_before
    assert artifact.version == version_before


# ---------------------------------------------------------------------------
# Failure containment (R9 / KTD15) and liveness honesty
# ---------------------------------------------------------------------------


def test_a_deleted_artifact_is_skipped_and_its_tick_mates_still_count(
    coordinator, repo: Path
) -> None:
    """Unguarded, this raises on the READ, git reports it every tick, and
    detection stops for good while the liveness count keeps advancing."""
    (repo / "second.md").write_text("v1\n")
    _git(repo, "add", "second.md")
    _git(repo, "commit", "-qm", "second")
    coordinator.policy = _policy(repo, "notes.md", "second.md")
    gone = _register(coordinator, "notes.md", "v1\n")
    survivor = _register(coordinator, "second.md", "v1\n")
    (repo / "notes.md").unlink()
    (repo / "second.md").write_text("foreign\n")

    _tick(coordinator, now_unix=1000.0)

    assert _totals(coordinator, gone) == {}
    assert _totals(coordinator, survivor) == {"foreign": 1}
    assert len(coordinator.registry.detection_runs()) == 1


def test_an_artifact_replaced_by_a_directory_is_skipped(
    coordinator, repo: Path
) -> None:
    art = _register(coordinator, "notes.md", "v1\n")
    (repo / "notes.md").unlink()
    (repo / "notes.md").mkdir()

    _tick(coordinator, now_unix=1000.0)

    assert _totals(coordinator, art) == {}


def test_a_failed_poll_does_not_advance_the_tick_count(
    coordinator, repo: Path
) -> None:
    """A broken poll must never read as a quiet month. Removing the git dir
    makes the poll fail; the run must record no observed tick."""
    _register(coordinator, "notes.md", "v1\n")
    import shutil

    shutil.rmtree(repo / ".git")

    _tick(coordinator, now_unix=1000.0)

    assert coordinator.registry.detection_runs() == []


def test_a_clean_tick_still_records_an_observation(coordinator) -> None:
    """Zero is only readable as zero because a clean tick is recorded."""
    _register(coordinator, "notes.md", "v1\n")

    _tick(coordinator, now_unix=1000.0)

    runs = coordinator.registry.detection_runs()
    assert len(runs) == 1 and runs[0].tick_count == 1
    assert coordinator.registry.foreign_write_totals() == {}


def test_a_workspace_with_nothing_in_scope_records_no_tick(coordinator) -> None:
    """A run that watched nothing must not read as a clean zero.

    This is reachable, not hypothetical: in this repository every default
    tracked pattern matches zero git-tracked files, so a window opened here
    would otherwise report a healthy instrumented zero having observed nothing.
    The registry holds no artifacts at all in this fixture."""
    _tick(coordinator, now_unix=1000.0)
    assert coordinator.registry.detection_runs() == []


def test_a_window_with_nothing_in_scope_is_a_hole_not_a_covered_span(
    coordinator, repo: Path
) -> None:
    """Not recording the tick is only half of what a blind window needs.

    A run row is read as continuously observed, so a window with nothing in
    scope that leaves the interval OPEN lets the next successful tick extend
    the same interval across it — and `covers()` then answers True for a span
    the detector polled nothing in. That is the false clean the three-state
    report exists to refuse, and unlike the other ways to blind this pass it
    needs no corruption, no mount and no locale: `/policy/track` swaps the
    tracked set while the coordinator runs, and untracking is a shipped command.
    """
    _register(coordinator, "notes.md", "v1\n")
    _tick(coordinator, now_unix=100.0)

    coordinator.policy = _policy(repo)  # tracked set narrowed to nothing
    _tick(coordinator, now_unix=5000.0)
    coordinator.policy = _policy(repo, "notes.md")  # and restored
    _tick(coordinator, now_unix=10000.0)

    from ccs.diagnose.foreign_writes import read_foreign_write_report

    db = Path(coordinator.registry._db_path)  # noqa: SLF001 — the store under test
    coordinator.registry.close()
    report = read_foreign_write_report(db)

    # Asserted through the offline report rather than the live run rows,
    # because `covers` is what an operator actually asks and what this fix is
    # about; the row boundaries are only how it comes to be true.
    assert len(report.runs) == 2, "the blind window did not close the observed interval"
    assert report.covers(100.0, 100.0) is True
    assert report.covers(10000.0, 10000.0) is True
    assert report.covers(100.0, 10000.0) is False


def test_a_late_tick_opens_a_new_interval_rather_than_stretching_the_old(
    coordinator, repo: Path
) -> None:
    """The other way a run row comes to span time nothing watched.

    Closing the interval when a tick observes nothing only covers the windows
    this pass can SEE. A sweep thread that arrives late — the host suspended,
    the four safety passes ahead of detection stuck on the store — observes
    nothing in between and says nothing about it, so the next successful tick
    would extend the same interval across the stall and `covers()` would answer
    True for it. A tick further from the last one than the sweep can explain
    therefore starts a new interval instead of joining the old.
    """
    from ccs.diagnose.foreign_writes import read_foreign_write_report

    _register(coordinator, "notes.md", "v1\n")
    clock: dict = {}
    _tick(coordinator, now_unix=100.0, clock=clock, max_gap_sec=15.0)
    _tick(coordinator, now_unix=110.0, clock=clock, max_gap_sec=15.0)  # on cadence
    _tick(coordinator, now_unix=9000.0, clock=clock, max_gap_sec=15.0)  # a stall

    db = Path(coordinator.registry._db_path)  # noqa: SLF001 — the store under test
    coordinator.registry.close()
    report = read_foreign_write_report(db)

    assert len(report.runs) == 2, "the stall did not end the observed interval"
    assert report.covers(100.0, 110.0) is True  # the on-cadence pair is one span
    assert report.covers(110.0, 9000.0) is False  # the stall is a hole
    assert report.covers(9000.0, 9000.0) is True


def test_narrowing_the_tracked_set_short_of_empty_keeps_one_interval(
    coordinator, repo: Path
) -> None:
    """The boundary the blind-window fix rests on.

    Only a scope that reaches ZERO ends the observed interval — a tracked set
    that merely shrank is still being watched, and splitting the run there
    would report a hole where there was none. Pinned because the sentence in
    the guide is precise about it, and because the fix's own trigger is
    `if not covered`, one edit away from `if len(covered) < previous`.
    """
    (repo / "second.md").write_text("v1\n")
    _git(repo, "add", "second.md")
    _git(repo, "commit", "-qm", "second")
    coordinator.policy = _policy(repo, "notes.md", "second.md")
    _register(coordinator, "notes.md", "v1\n")
    _register(coordinator, "second.md", "v1\n")

    _tick(coordinator, now_unix=100.0)
    coordinator.policy = _policy(repo, "notes.md")  # narrowed, but not to nothing
    _tick(coordinator, now_unix=110.0)

    runs = coordinator.registry.detection_runs()
    assert len(runs) == 1, "a narrowed-but-non-empty scope must not split the run"
    assert runs[0].tick_count == 2


def test_a_tick_records_how_much_was_in_scope(coordinator, repo: Path) -> None:
    """A tick count alone cannot separate "watched 500, all clean" from
    "watched nothing"."""
    _register(coordinator, "notes.md", "v1\n")
    _tick(coordinator, now_unix=1000.0)
    runs = coordinator.registry.detection_runs()
    assert len(runs) == 1 and runs[0].covered_count == 1


# ---------------------------------------------------------------------------
# The git layer in isolation
# ---------------------------------------------------------------------------


def test_a_non_zero_git_exit_raises_rather_than_reading_clean(tmp_path: Path) -> None:
    """The shipped helper maps a clean tree and a failed git to the same value.
    Here they must differ, or a corrupt index reads as no foreign writes."""
    not_a_repo = tmp_path / "plain"
    not_a_repo.mkdir()
    with pytest.raises(GitPollError):
        _git_dirty_paths(not_a_repo, ["notes.md"], deadline=_deadline())


def test_a_missing_repository_is_typed_apart_from_a_genuine_failure(
    tmp_path: Path, repo: Path
) -> None:
    """128 is git's generic fatal exit, so the code alone cannot separate a
    workspace that can never be polled from a corrupt index, a bad object or a
    dubious-ownership refusal. Only the first is permanent and non-actionable,
    and only the first may ever be quieted."""
    not_a_repo = tmp_path / "plain"
    not_a_repo.mkdir()
    with pytest.raises(NotAGitRepositoryError):
        _git_dirty_paths(not_a_repo, ["notes.md"], deadline=_deadline())

    # A real repository, a real fatal, the same exit code — and not the quiet
    # one: git refuses a pathspec that resolves outside the work tree.
    outside = tmp_path / "outside.txt"
    outside.write_text("x")
    with pytest.raises(GitPollError) as caught:
        _git_dirty_paths(repo, [str(outside)], deadline=_deadline())
    assert not isinstance(caught.value, NotAGitRepositoryError)


@pytest.mark.parametrize("gutted", [".git/HEAD", ".git/objects", ".git/refs"])
def test_a_corrupt_repository_is_never_mistaken_for_an_absent_one(
    repo: Path, gutted: str
) -> None:
    """The message cannot tell these apart, so the filesystem has to.

    A repository whose ``.git`` survives but whose ``HEAD``, ``objects`` or
    ``refs`` does not answers with the BYTE-IDENTICAL "fatal: not a git
    repository (or any of the parent directories): .git" and the same exit 128
    an absent one gives. That repository is broken and an operator can fix it;
    quieting it would hand out the reassurance this instrument exists to earn.
    """
    import shutil

    target = repo / gutted
    shutil.rmtree(target) if target.is_dir() else target.unlink()

    with pytest.raises(GitPollError) as caught:
        _git_dirty_paths(repo, ["notes.md"], deadline=_deadline())

    # The sentence IS there. The classification still is not — which is the
    # whole point: it is decided by `.git` existing, not by what git said.
    assert "not a git repository" in str(caught.value).lower()
    assert not isinstance(caught.value, NotAGitRepositoryError)


def test_a_workspace_that_vanished_is_broken_rather_than_absent(
    tmp_path: Path
) -> None:
    """The other half of the guard, forced by the case that reaches it.

    A coordinator root deleted or unmounted under a running coordinator has no
    ``.git`` anywhere above it, so the filesystem walk alone would call it
    absent and quiet it forever. Git exits 128 there too, but says "cannot
    change to" rather than "not a git repository" — and that sentence is the
    only thing standing between a vanished workspace and a permanent silence.
    """
    from ccs.adapters.claude_code.foreign_write_detector import _repository_is_absent

    gone = tmp_path / "gone"  # never created
    assert _repository_is_absent(gone) is True  # the walk says absent ...

    with pytest.raises(GitPollError) as caught:
        _git_dirty_paths(gone, ["notes.md"], deadline=_deadline())
    assert not isinstance(caught.value, NotAGitRepositoryError)  # ... git does not
    assert "not a git repository" not in str(caught.value).lower()


def test_the_walk_crosses_a_filesystem_boundary_git_would_stop_at(
    repo: Path, monkeypatch
) -> None:
    """Crossing is deliberate, and narrowing it would quiet a broken repository.

    Git stops discovery at a filesystem boundary unless
    ``GIT_DISCOVERY_ACROSS_FILESYSTEM`` is set. Mirroring that here looks like
    a faithfulness fix and is the opposite: a GUTTED repository across a mount
    boundary gives the byte-identical "not a git repository" with no boundary
    line to distinguish it, so a walk that stopped at the boundary would answer
    absent and quiet it. This pins the crossing so that change fails here
    instead of shipping.

    The boundary is staged by faking ``st_dev`` rather than mounting anything,
    so it runs anywhere — and the staging is asserted first, because a fake
    that silently stopped staging a boundary would let this pass against the
    very mutation it exists to kill.
    """
    import os as os_module

    from ccs.adapters.claude_code.foreign_write_detector import _walk_for_repository

    nested = repo / "on" / "its" / "own" / "mount"
    nested.mkdir(parents=True)
    real_lstat = os_module.lstat
    boundary = nested.resolve()

    class _Stat:
        def __init__(self, st, dev):
            self._st, self.st_dev = st, dev

        def __getattr__(self, name):
            return getattr(self._st, name)

    def _faked(path, *args, **kwargs):
        st = real_lstat(path, *args, **kwargs)
        # Everything at or below the workspace is on "its own device".
        target = Path(path)
        on_far_side = target == boundary or boundary in target.parents
        return _Stat(st, st.st_dev if on_far_side else st.st_dev + 1)

    monkeypatch.setattr(os_module, "lstat", _faked)

    # The fake really does stage a device boundary between the two.
    assert os_module.lstat(nested).st_dev != os_module.lstat(repo).st_dev, (
        "the fake staged no device boundary, so this test proves nothing"
    )
    # And the walk crosses it anyway, finding the repository above.
    assert _walk_for_repository(nested) is False


def test_a_dangling_git_symlink_is_broken_rather_than_absent(
    tmp_path: Path
) -> None:
    """``lexists``, not ``exists``: a ``.git`` pointing at nothing is a
    repository someone has to repair, and the walk must not read it as a
    workspace that never had one."""
    workspace = tmp_path / "dangling"
    workspace.mkdir()
    (workspace / ".git").symlink_to(tmp_path / "gone")

    with pytest.raises(GitPollError) as caught:
        _git_dirty_paths(workspace, ["notes.md"], deadline=_deadline())
    assert not isinstance(caught.value, NotAGitRepositoryError)


def test_a_repository_above_the_workspace_still_counts_as_present(
    tmp_path: Path, repo: Path
) -> None:
    """The walk mirrors git's own discovery, which searches upward. A
    coordinator rooted in a subdirectory is inside a repository even though its
    own directory holds no ``.git``."""
    from ccs.adapters.claude_code.foreign_write_detector import _repository_is_absent

    nested = repo / "sub" / "deeper"
    nested.mkdir(parents=True)
    assert _repository_is_absent(nested) is False
    assert _repository_is_absent(tmp_path / "nowhere-near-a-repo") is True


@pytest.mark.parametrize("failure", [PermissionError, OSError])
def test_a_walk_that_cannot_answer_says_present_rather_than_absent(
    tmp_path: Path, monkeypatch, failure
) -> None:
    """Driven through the guard function itself, because the conditions that
    reach these branches — an unsearchable parent, a root that will not resolve
    — cannot be staged portably, and a branch no test can see reports green.

    ``os.path.lexists`` is what this rules out: it folds a stat ERROR into the
    same ``False`` it gives a missing file, so the walk would read an
    unreadable directory as "no repository here" and quiet it.
    """
    import os as os_module

    from ccs.adapters.claude_code.foreign_write_detector import _repository_is_absent

    workspace = tmp_path / "workspace"
    workspace.mkdir()
    assert _repository_is_absent(workspace) is True  # control: really absent

    def _refuse(*_args, **_kwargs):
        raise failure(13, "refused")

    monkeypatch.setattr(os_module, "lstat", _refuse)
    assert _repository_is_absent(workspace) is False


def test_a_walk_that_never_answers_cannot_hold_the_sweep_thread(
    tmp_path: Path, monkeypatch
) -> None:
    """The walk runs on the sweep thread, whose other four passes reclaim
    grants and reap dead sessions. ``os.lstat`` on a wedged network or FUSE
    mount never returns and cannot be interrupted, so an unbounded walk would
    stall the coordinator's safety work for as long as the mount stays wedged.

    ``_disk_hash`` states the same rule for the read path — a path replaced by
    a named pipe "fails instead of blocking this thread forever" — and this is
    that rule on the classification path. An unanswered walk says present, like
    every other ambiguity here.
    """
    import threading
    import time

    import ccs.adapters.claude_code.foreign_write_detector as detector

    # Released in the finally: only one walk may be in flight per process, so a
    # test that left its own wedged walker running would hold that slot and
    # quietly turn every later test's workspace loud.
    release = threading.Event()

    def _never_answers(_root):
        release.wait(30)
        return True  # would quiet the workspace, if it were ever reached

    monkeypatch.setattr(detector, "_walk_for_repository", _never_answers)

    started = time.monotonic()
    try:
        answer = detector._repository_is_absent(tmp_path, budget_sec=0.2)
    finally:
        release.set()
    elapsed = time.monotonic() - started

    assert answer is False  # unanswered means present, which stays loud
    # Tight enough that the budget is the thing being honoured: a hard-coded
    # wait substituted for `budget_sec` has to fail here, not sail through on
    # a margin wide enough to hide it.
    assert elapsed < 1.0


def test_a_wedged_walk_is_never_started_twice_over(tmp_path: Path, monkeypatch) -> None:
    """The thread a wedged mount leaves behind cannot be killed, so one per
    sweep tick would accumulate until the process could create none at all —
    a worse failure than the stall the bound exists to prevent.

    While a walk is still out, its answer is already present/loud, so the
    second caller needs no thread of its own.
    """
    import threading
    import time

    import ccs.adapters.claude_code.foreign_write_detector as detector

    release = threading.Event()
    monkeypatch.setattr(
        detector, "_walk_for_repository", lambda _root: release.wait(30) or True
    )

    def _live_walkers() -> int:
        return sum(1 for t in threading.enumerate() if t.name == "coord-fwd-walk")

    def _settle_walkers() -> None:
        """Wait out any walker a neighbouring test released a moment ago, so
        this test counts its own threads and not the previous one's."""
        for _ in range(100):
            if _live_walkers() == 0:
                return
            time.sleep(0.01)

    _settle_walkers()
    before = _live_walkers()
    try:
        for _ in range(5):  # five ticks against a filesystem that never answers
            assert detector._repository_is_absent(tmp_path, budget_sec=0.05) is False
        assert _live_walkers() - before == 1
    finally:
        release.set()
        time.sleep(0.1)  # let the one walker retire so it leaks nothing


def test_a_process_that_cannot_spawn_a_walker_says_present(
    tmp_path: Path, monkeypatch
) -> None:
    """Thread exhaustion is exactly when this must not raise. A raise here
    reaches the pass as an unexpected failure and prints the per-tick traceback
    the change exists to stop printing, so the unstartable walk takes the same
    loud-but-quiet-logged answer every other ambiguity takes."""
    import threading

    import ccs.adapters.claude_code.foreign_write_detector as detector

    def _refuse(_self):
        raise RuntimeError("can't start new thread")

    monkeypatch.setattr(threading.Thread, "start", _refuse)

    assert detector._repository_is_absent(tmp_path, budget_sec=0.2) is False


def test_the_walk_is_bounded_by_what_is_left_of_the_poll_budget(
    tmp_path: Path, monkeypatch
) -> None:
    """``budget_sec`` is documented to bound the WHOLE poll. A walk spent on
    top of it would push the tick past the sweep interval this pass shares with
    grant reclamation and session liveness, so the walk gets the remainder —
    never its own ceiling on top."""
    import ccs.adapters.claude_code.foreign_write_detector as detector

    seen: list[float] = []

    def _capture(root, *, budget_sec):
        seen.append(budget_sec)
        return True

    monkeypatch.setattr(detector, "_repository_is_absent", _capture)

    not_a_repo = tmp_path / "plain"
    not_a_repo.mkdir()
    with pytest.raises(NotAGitRepositoryError):
        _git_dirty_paths(not_a_repo, ["notes.md"], deadline=_deadline(0.5))

    assert seen and seen[0] <= 0.5
    assert seen[0] < detector._WALK_BUDGET_SEC  # the ceiling did not win


def test_a_root_that_will_not_resolve_says_present(tmp_path: Path, monkeypatch) -> None:
    """Same asymmetry one step earlier."""
    from ccs.adapters.claude_code.foreign_write_detector import _repository_is_absent

    def _refuse(*_args, **_kwargs):
        raise OSError(62, "too many levels of symbolic links")

    monkeypatch.setattr(Path, "resolve", _refuse)
    assert _repository_is_absent(tmp_path / "workspace") is False


def test_the_pinned_environment_reaches_the_git_process(
    tmp_path: Path, monkeypatch
) -> None:
    """``_poll_env`` returning the right dict proves nothing if the dict never
    reaches the child. Dropping ``env=env`` from the ``subprocess.run`` call
    unpins the message language this module classifies on, restores pathspec
    magic, re-takes the index lock, AND reinstates the ``GIT_DIR`` redirect the
    poll scrubs — and the whole shipped suite stays green. That last one is not
    safe-direction: an inherited ``GIT_DIR`` makes the poll succeed against a
    different repository, report nothing dirty, exit 0 and RECORD THE TICK,
    which is the silent permanent zero ``_GIT_REDIRECT_VARS`` exists to stop.

    This is the assertion at the end of the wire rather than at the start of
    it. It needs no locale, no catalogs and no git binary, so unlike a test
    that reads git's translated output it cannot report green while blind.
    """
    import ccs.adapters.claude_code.foreign_write_detector as detector

    monkeypatch.setenv("GIT_DIR", "/tmp/somewhere-else/.git")
    handed: dict = {}
    real_run = subprocess.run

    def _capture(cmd, **kwargs):
        handed["passed_env"] = kwargs.get("env") is not None
        handed["env"] = dict(kwargs.get("env") or {})
        return real_run(cmd, **kwargs)

    monkeypatch.setattr(detector.subprocess, "run", _capture)

    workspace = tmp_path / "plain"
    workspace.mkdir()
    with pytest.raises(GitPollError):
        _git_dirty_paths(workspace, ["notes.md"], deadline=_deadline())

    # A subscript, not a .get(): a poll that never reached subprocess.run at
    # all must fail here rather than pass vacuously.
    assert handed["passed_env"] is True, "subprocess.run was called without env="
    env = handed["env"]
    assert env["LC_ALL"] == "C"
    assert env["GIT_LITERAL_PATHSPECS"] == "1"
    assert env["GIT_OPTIONAL_LOCKS"] == "0"
    assert "GIT_DIR" not in env


def test_the_poll_pins_the_language_of_the_diagnostics_it_reads() -> None:
    """The missing-repository signature is a sentence git TRANSLATES: under
    ``fr_FR.UTF-8`` it reads "ni ceci ni aucun de ses répertoires parents n'est
    un dépôt git". Without ``LC_ALL=C`` an operator's locale would decide
    whether a workspace reads as inert or as broken."""
    assert _poll_env()["LC_ALL"] == "C"


def test_the_poll_never_takes_the_index_lock(repo: Path) -> None:
    """A background poll that rewrites the index would contend with the user's
    own git commands. Proven by the index file being byte-identical after."""
    index = repo / ".git" / "index"
    before = index.read_bytes()
    (repo / "notes.md").write_text("dirty\n")

    assert _git_dirty_paths(repo, ["notes.md"], deadline=_deadline()) == {"notes.md"}

    assert index.read_bytes() == before


def test_a_large_path_set_is_polled_in_batches(repo: Path) -> None:
    """Ten thousand literal pathspecs exceed the Windows command-line limit and
    would raise before git ran; batching keeps every one of them in scope."""
    names = [f"generated/file_{n:05d}.md" for n in range(10_000)]
    (repo / "generated").mkdir()
    for name in names[:3]:
        (repo / name).write_text("x\n")
    _git(repo, "add", "generated")
    _git(repo, "commit", "-qm", "generated")
    (repo / names[1]).write_text("changed\n")

    assert _git_dirty_paths(repo, names, deadline=_deadline()) == {names[1]}


def test_porcelain_v2_rename_records_report_both_paths() -> None:
    """A rename is a `2` record carrying two NUL-separated paths; a parser
    written for `1` records alone would miss a renamed artifact."""
    payload = (
        "# branch.oid abc\0"
        "1 .M N... 100644 100644 100644 aaa bbb notes.md\0"
        "2 R. N... 100644 100644 100644 ccc ddd R100 new.md\0old.md\0"
    )
    assert _parse_porcelain_v2(payload) == {"notes.md", "new.md", "old.md"}


def test_classify_mismatch_is_a_pure_function_of_its_inputs() -> None:
    """Only the mismatch branch reaches here; a hash match is settled by the
    caller without touching the store at all."""
    assert _classify_mismatch(
        updated_at=1.0, has_mediated_writer=True, has_write_grant=False,
        now_unix=2.0, window_sec=5.0,
    ) == "lag_suppressed"
    assert _classify_mismatch(
        updated_at=1.0, has_mediated_writer=False, has_write_grant=False,
        now_unix=2.0, window_sec=5.0,
    ) == "foreign"
    assert _classify_mismatch(
        updated_at=None, has_mediated_writer=True, has_write_grant=False,
        now_unix=2.0, window_sec=5.0,
    ) == "foreign"
    assert _classify_mismatch(
        updated_at=1.0, has_mediated_writer=True, has_write_grant=False,
        now_unix=99.0, window_sec=5.0,
    ) == "foreign"


def test_an_outstanding_write_grant_suppresses_on_its_own() -> None:
    """The timestamp cannot see a write that is landing right now.

    Disk is written before the ledger commit, so during that gap updated_at
    still holds the PREVIOUS commit's time. On an artifact untouched for an
    hour the timestamp leg says foreign about a perfectly ordinary mediated
    write. An outstanding grant is the registry's own evidence."""
    assert _classify_mismatch(
        updated_at=0.0, has_mediated_writer=True, has_write_grant=True,
        now_unix=100_000.0, window_sec=5.0,
    ) == "lag_suppressed"
    assert _classify_mismatch(
        updated_at=None, has_mediated_writer=False, has_write_grant=True,
        now_unix=100_000.0, window_sec=5.0,
    ) == "lag_suppressed"


def test_the_lag_window_boundary_is_inclusive() -> None:
    assert _classify_mismatch(
        updated_at=0.0, has_mediated_writer=True, has_write_grant=False,
        now_unix=5.0, window_sec=5.0,
    ) == "lag_suppressed"
    assert _classify_mismatch(
        updated_at=0.0, has_mediated_writer=True, has_write_grant=False,
        now_unix=5.001, window_sec=5.0,
    ) == "foreign"


def test_the_poll_asks_git_for_literal_paths_and_drops_repo_redirects() -> None:
    """Pathspec magic in a stored name would otherwise re-scope the whole poll,
    and an inherited GIT_DIR would point it at a different repository."""
    import os

    os.environ["GIT_DIR"] = "/tmp/somewhere-else/.git"
    try:
        env = _poll_env()
    finally:
        del os.environ["GIT_DIR"]
    assert env["GIT_LITERAL_PATHSPECS"] == "1"
    assert env["GIT_OPTIONAL_LOCKS"] == "0"
    assert "GIT_DIR" not in env


# ---------------------------------------------------------------------------
# Regressions for the defects code review found
# ---------------------------------------------------------------------------


def test_a_suppressed_mismatch_is_re_examined_once_the_window_expires(
    coordinator, repo: Path
) -> None:
    """The benefit of the doubt must expire.

    A mismatch inside the window is suppressed on the theory that a commit is
    still landing. If none ever lands, the bytes are foreign — and keying the
    edge gate on content alone would have frozen that first benign label
    forever, recording a real foreign write as benign and never revisiting it.
    """
    art = _register(coordinator, "notes.md", "v1\n")
    coordinator.registry.commit_cas(
        art,
        __import__("uuid").uuid4(),
        expected_version=coordinator.registry.get_artifact(art).version,
        content_hash=sha256_hex(b"in flight\n"),
        content="in flight",
    )
    (repo / "notes.md").write_text("never actually committed\n")
    updated_at = coordinator.registry.get_artifact_updated_at(art)
    cache: dict = {}

    _tick(coordinator, now_unix=updated_at + 1.0, cache=cache)
    assert _totals(coordinator, art) == {"lag_suppressed": 1}

    # Same bytes, same artifact — only the window has passed.
    _tick(coordinator, now_unix=updated_at + WINDOW + 60.0, cache=cache)
    totals = _totals(coordinator, art)
    assert totals["foreign"] == 1
    assert totals["lag_suppressed"] == 1


def test_a_write_landing_on_a_long_idle_artifact_is_not_called_foreign(
    coordinator, repo: Path
) -> None:
    """Disk is written before the ledger commit. During that gap updated_at
    still holds the previous commit's time, so on an artifact untouched for
    hours the timestamp alone would accuse an ordinary mediated write."""
    from ccs.core.states import MESIState

    art = _register(coordinator, "notes.md", "v1\n")
    holder = __import__("uuid").uuid4()
    coordinator.registry.set_agent_state(
        art, holder, MESIState.MODIFIED, trigger="write", tick=0
    )
    (repo / "notes.md").write_text("bytes on disk, commit not yet landed\n")
    updated_at = coordinator.registry.get_artifact_updated_at(art)

    _tick(coordinator, now_unix=updated_at + 100_000.0)

    assert _totals(coordinator, art) == {"lag_suppressed": 1}


def test_a_reconciled_artifact_re_arms_for_an_identical_later_edit(
    coordinator, repo: Path
) -> None:
    """Going clean clears the gate. Otherwise a revert followed by the same
    edit again reads as already-counted and the second write is never seen."""
    art = _register(coordinator, "notes.md", "v1\n")
    cache: dict = {}
    (repo / "notes.md").write_text("foreign\n")
    _tick(coordinator, now_unix=1000.0, cache=cache)
    assert _totals(coordinator, art) == {"foreign": 1}

    (repo / "notes.md").write_text("v1\n")  # reconciled: git reports it clean
    _tick(coordinator, now_unix=1001.0, cache=cache)

    (repo / "notes.md").write_text("foreign\n")  # byte-identical to the first
    _tick(coordinator, now_unix=1002.0, cache=cache)
    assert _totals(coordinator, art) == {"foreign": 2}


def test_a_failed_tick_closes_the_observed_interval(coordinator, repo: Path) -> None:
    """Coverage must not interpolate across an outage.

    Without closing the interval, a run that ticked, went blind, then ticked
    again would answer a coverage question for the whole blind period.

    This one goes blind by losing its repository, so it rides the quiet branch;
    its sibling below rides the loud one. Both branches close the interval, and
    each needs its own test — typing one failure apart moved this test onto the
    new branch and left the old one covered by nothing."""
    import shutil

    from ccs.diagnose.foreign_writes import read_foreign_write_report

    _register(coordinator, "notes.md", "v1\n")
    cache: dict = {}
    _tick(coordinator, now_unix=100.0, cache=cache)

    git_dir = repo / ".git"
    stashed = repo.parent / "git-stashed"
    shutil.move(str(git_dir), str(stashed))
    _tick(coordinator, now_unix=200.0, cache=cache)  # poll fails
    shutil.move(str(stashed), str(git_dir))
    _tick(coordinator, now_unix=300.0, cache=cache)

    db = Path(coordinator.registry._db_path)  # noqa: SLF001 — the store under test
    coordinator.registry.close()
    report = read_foreign_write_report(db)

    assert len(report.runs) == 2
    assert report.covers(100.0, 100.0) is True
    assert report.covers(300.0, 300.0) is True
    assert report.covers(100.0, 300.0) is False


def test_an_ordinary_failed_tick_closes_the_observed_interval_too(
    coordinator, repo: Path, monkeypatch
) -> None:
    """The same coverage guarantee on the branch that keeps its traceback.

    A corrupt index or a dubious-ownership refusal blinds the instrument just
    as completely as a missing repository, and interpolating a clean span
    across THAT outage is the reading the three-state report exists to refuse.
    Driven through the generic branch by a plain ``GitPollError``, because the
    quiet branch is what a removed ``.git`` now reaches."""
    import ccs.adapters.claude_code.foreign_write_detector as detector
    from ccs.diagnose.foreign_writes import read_foreign_write_report

    _register(coordinator, "notes.md", "v1\n")
    cache: dict = {}
    _tick(coordinator, now_unix=100.0, cache=cache)

    real = detector._git_dirty_paths  # noqa: SLF001 — the pass under test

    def _boom(*_args, **_kwargs):
        raise GitPollError("git status exited 128 in X: fatal: bad object HEAD")

    monkeypatch.setattr(detector, "_git_dirty_paths", _boom)
    _tick(coordinator, now_unix=200.0, cache=cache)  # poll fails, loudly
    monkeypatch.setattr(detector, "_git_dirty_paths", real)
    _tick(coordinator, now_unix=300.0, cache=cache)

    db = Path(coordinator.registry._db_path)  # noqa: SLF001 — the store under test
    coordinator.registry.close()
    report = read_foreign_write_report(db)

    assert len(report.runs) == 2
    assert report.covers(100.0, 300.0) is False


def test_an_unencodable_name_is_filtered_out_of_the_poll() -> None:
    """A name that cannot reach argv would escape the poll's two handled
    failures and, because it stays in the registry, kill every later tick.

    The sqlite registry refuses such a name at registration, so this guard is
    the second line rather than the first — but the detector must not depend on
    a store's encoding behaviour to keep watching everything else.
    """
    from ccs.adapters.claude_code.foreign_write_detector import _argv_encodable

    assert _argv_encodable("docs/plan.md") is True
    assert _argv_encodable("bad\udcff.md") is False


def test_a_per_artifact_error_does_not_end_the_tick(
    coordinator, repo: Path, monkeypatch
) -> None:
    """The per-artifact guard, exercised by an actual raise rather than by
    _disk_hash's graceful None."""
    (repo / "second.md").write_text("v1\n")
    _git(repo, "add", "second.md")
    _git(repo, "commit", "-qm", "second")
    coordinator.policy = _policy(repo, "notes.md", "second.md")
    exploding = _register(coordinator, "notes.md", "v1\n")
    survivor = _register(coordinator, "second.md", "v1\n")
    (repo / "notes.md").write_text("foreign one\n")
    (repo / "second.md").write_text("foreign two\n")

    real = coordinator.registry.get_artifact

    def _boom(artifact_id):
        if artifact_id == exploding:
            raise RuntimeError("registry blew up for this artifact")
        return real(artifact_id)

    monkeypatch.setattr(coordinator.registry, "get_artifact", _boom)
    _tick(coordinator, now_unix=1000.0)

    assert _totals(coordinator, exploding) == {}
    assert _totals(coordinator, survivor) == {"foreign": 1}
    assert len(coordinator.registry.detection_runs()) == 1


def test_an_oversized_file_is_a_coverage_gap_not_an_outcome(
    coordinator, repo: Path, monkeypatch
) -> None:
    import ccs.adapters.claude_code.foreign_write_detector as detector

    art = _register(coordinator, "notes.md", "v1\n")
    (repo / "notes.md").write_text("x" * 4096)
    monkeypatch.setattr(detector, "_MAX_HASH_BYTES", 16)

    _tick(coordinator, now_unix=1000.0)
    assert _totals(coordinator, art) == {}

    monkeypatch.setattr(detector, "_MAX_HASH_BYTES", 1_000_000)
    _tick(coordinator, now_unix=1001.0)
    assert _totals(coordinator, art) == {"foreign": 1}


def test_an_unchanged_file_is_not_re_read_on_a_later_tick(
    coordinator, repo: Path, monkeypatch
) -> None:
    """The edge gate stops double-counting; the stat cache stops double-work."""
    import ccs.adapters.claude_code.foreign_write_detector as detector

    _register(coordinator, "notes.md", "v1\n")
    (repo / "notes.md").write_text("foreign\n")
    cache: dict = {}

    reads: list[str] = []
    real = detector._disk_hash
    monkeypatch.setattr(
        detector, "_disk_hash", lambda p: (reads.append(p.name), real(p))[1]
    )

    _tick(coordinator, now_unix=1000.0, cache=cache)
    assert reads == ["notes.md"]
    for tick in range(5):
        _tick(coordinator, now_unix=1001.0 + tick, cache=cache)
    assert reads == ["notes.md"]


def test_a_conflicted_artifact_is_visible(coordinator, repo: Path) -> None:
    """An unmerged path is a `u` record; dropping it would report a file in a
    merge conflict as clean."""
    payload = (
        "# branch.oid abc\0"
        "u UU N... 100644 100644 100644 100644 aaa bbb ccc notes.md\0"
    )
    assert _parse_porcelain_v2(payload) == {"notes.md"}


def test_a_symlinked_artifact_is_not_followed(coordinator, repo: Path) -> None:
    """A coordinated name pointed outside the workspace is unobservable, not a
    licence to hash whatever it targets."""
    outside = repo.parent / "outside.md"
    outside.write_text("secrets\n")
    art = _register(coordinator, "notes.md", "v1\n")
    (repo / "notes.md").unlink()
    (repo / "notes.md").symlink_to(outside)

    _tick(coordinator, now_unix=1000.0)

    assert _totals(coordinator, art) == {}


def test_the_poll_budget_bounds_the_whole_pass_not_each_batch(repo: Path) -> None:
    """A per-batch timeout lets a large path list stall the sweep for the sum
    of its batches, and detection shares the loop with grant reclamation."""
    names = [f"generated/file_{n:05d}.md" for n in range(10_000)]
    (repo / "generated").mkdir()
    (repo / names[0]).write_text("x\n")
    _git(repo, "add", "generated")
    _git(repo, "commit", "-qm", "generated")

    with pytest.raises(GitPollError, match="budget"):
        _git_dirty_paths(repo, names, deadline=_deadline(0.0))


def test_the_last_batch_is_queried_too(repo: Path) -> None:
    names = [f"generated/file_{n:05d}.md" for n in range(10_000)]
    (repo / "generated").mkdir()
    for name in (names[1], names[-1]):
        (repo / name).write_text("x\n")
    _git(repo, "add", "generated")
    _git(repo, "commit", "-qm", "generated")
    (repo / names[1]).write_text("changed\n")
    (repo / names[-1]).write_text("changed\n")

    assert _git_dirty_paths(repo, names, deadline=_deadline()) == {
        names[1],
        names[-1],
    }


# ---------------------------------------------------------------------------
# The git-visibility read (U1) — which registered names git can report on
# ---------------------------------------------------------------------------


def _status_reports(root: Path, *names: str) -> set[str]:
    """Which of ``names`` the SHIPPED poll reports right now.

    The visibility helper's entire claim is that it agrees with this call, so
    every exclusion below is justified by asking git rather than by restating
    what git is believed to do.
    """
    return _git_dirty_paths(root, list(names), deadline=_deadline())


def _conflict(root: Path, name: str) -> None:
    """Leave ``name`` genuinely unmerged — a real three-stage index entry.

    Hand-written porcelain would prove nothing here: the point is what ``git
    ls-files -v`` actually tags a conflicted path, which only git can say.
    """
    base = subprocess.run(
        ["git", "-C", str(root), "rev-parse", "--abbrev-ref", "HEAD"],
        check=True,
        capture_output=True,
    ).stdout.decode().strip()
    _git(root, "branch", "other")
    (root / name).write_text("ours\n")
    _git(root, "commit", "-qam", "ours")
    _git(root, "checkout", "-q", "other")
    (root / name).write_text("theirs\n")
    _git(root, "commit", "-qam", "theirs")
    _git(root, "checkout", "-q", base)
    merged = subprocess.run(
        ["git", "-C", str(root), "merge", "other"], capture_output=True
    )
    assert merged.returncode != 0, "the fixture produced no conflict to observe"


def test_only_index_entries_are_visible(repo: Path) -> None:
    """A tracked file is in scope; a git-ignored one and a never-added one are
    not — and the poll cannot report either even once they are MUTATED, so
    dropping them narrows nothing the instrument could ever have watched."""
    (repo / ".gitignore").write_text("ignored.md\n")
    _git(repo, "add", ".gitignore")
    _git(repo, "commit", "-qm", "ignore")
    (repo / "ignored.md").write_text("edited\n")
    (repo / "never.md").write_text("edited\n")

    visible = _git_visible_names(repo, deadline=_deadline())

    assert "notes.md" in visible
    assert "ignored.md" not in visible
    assert "never.md" not in visible
    assert _status_reports(repo, "ignored.md", "never.md", "notes.md") == set()


def test_a_staged_and_a_worktree_deleted_entry_stay_visible(repo: Path) -> None:
    """Both are index entries the poll DOES report — a file added but not yet
    committed, and one removed from the worktree while still in the index.
    Narrowing either away would drop an observable artifact out of the coverage
    claim, which is the one direction R3 forbids."""
    (repo / "staged.md").write_text("new\n")
    _git(repo, "add", "staged.md")
    (repo / "notes.md").unlink()

    visible = _git_visible_names(repo, deadline=_deadline())

    assert {"staged.md", "notes.md"} <= visible
    assert _status_reports(repo, "staged.md", "notes.md") == {"staged.md", "notes.md"}


def test_skip_worktree_and_assume_unchanged_are_excluded(repo: Path) -> None:
    """The only two exclusions, earned from the same evidence the rule rests on.

    Both files are MUTATED and the shipped poll still reports neither: git has
    been told to stop looking at them, so claiming coverage of them would be
    the false clean this instrument exists to refuse. Asserting the exclusion
    without asserting git's silence would leave the rule resting on belief.
    """
    for name in ("skipped.md", "assumed.md"):
        (repo / name).write_text("v1\n")
    _git(repo, "add", "skipped.md", "assumed.md")
    _git(repo, "commit", "-qm", "two more")
    _git(repo, "update-index", "--skip-worktree", "skipped.md")
    _git(repo, "update-index", "--assume-unchanged", "assumed.md")
    for name in ("skipped.md", "assumed.md"):
        (repo / name).write_text("MUTATED\n")

    visible = _git_visible_names(repo, deadline=_deadline())

    assert "skipped.md" not in visible
    assert "assumed.md" not in visible
    assert _status_reports(repo, "skipped.md", "assumed.md") == set()


def test_an_unmerged_path_is_visible_and_collapses_to_one_member(repo: Path) -> None:
    """``git ls-files -v`` tags a conflicted path ``M`` and repeats it once per
    stage. An allowlist of known-good tags would drop it, and the poll reports
    it as a ``u`` record — so every merge conflict would go blind in exactly
    the window an operator most wants watched. Consumed as a set, so the three
    stage lines are one member; the raw listing is asserted to really repeat,
    or the set claim would pass vacuously."""
    _conflict(repo, "notes.md")

    listing = subprocess.run(
        ["git", "-C", str(repo), "ls-files", "-z", "-v", "--full-name"],
        check=True,
        capture_output=True,
    ).stdout.decode()
    assert listing.split("\0").count("M notes.md") > 1

    visible = _git_visible_names(repo, deadline=_deadline())
    assert [name for name in visible if name == "notes.md"] == ["notes.md"]

    reported = subprocess.run(
        ["git", "-C", str(repo), "status", "--porcelain=v2", "-z", "--untracked-files=no"],
        check=True,
        capture_output=True,
    ).stdout.decode()
    assert any(
        record.startswith("u ") and record.endswith(" notes.md")
        for record in reported.split("\0")
    )


def test_an_unrecognized_tag_is_retained_rather_than_dropped() -> None:
    """R3's fail-closed direction applied to the tag vocabulary. No shipped git
    emits this tag, so only the parser can be asked: an allowlist would go
    silently blind the day git grows a letter, and the artifact it dropped
    would still be one the poll reports."""
    payload = "H notes.md\0Z future.md\0S skipped.md\0h assumed.md\0"

    assert _parse_ls_files_v(payload) == {"notes.md", "future.md"}


def test_the_visibility_read_pins_its_own_path_shape(repo: Path, monkeypatch) -> None:
    """``--full-name`` pins names to the repository top — the shape the registry
    holds and the shape porcelain v2 emits. Omitting it prints them relative to
    the invocation directory, which would intersect with the registry nowhere:
    the silent permanent zero this module's own learning records. And the poll's
    ``-c status.relativePaths=true`` must NOT be copied across, because
    ``ls-files`` ignores it — copying it would read as a pin while pinning
    nothing.

    Both are asserted on the argv, as the ``GIT_LITERAL_PATHSPECS`` scenario is:
    at a coordinator root that IS the repository top — every fixture in these
    suites — the two forms emit identical names, so no behavioural test can see
    the difference.
    """
    import ccs.adapters.claude_code.foreign_write_detector as detector

    seen: dict = {}
    real_run = subprocess.run

    def _capture(cmd, **kwargs):
        seen["argv"] = list(cmd)
        return real_run(cmd, **kwargs)

    monkeypatch.setattr(detector.subprocess, "run", _capture)

    _git_visible_names(repo, deadline=_deadline())

    argv = seen["argv"]
    assert "--full-name" in argv
    assert not any("relativePaths" in part for part in argv)
    assert "ls-files" in argv and "-z" in argv and "-v" in argv


def test_a_name_beginning_with_a_colon_is_visible_and_read_literally(
    repo: Path, monkeypatch
) -> None:
    """The pathspec-magic regression this module has already shipped once. The
    read passes no pathspecs, so nothing can be re-scoped here today — but the
    env is pinned at the end of the wire anyway, because a later change that
    adds one must not reintroduce it silently."""
    import ccs.adapters.claude_code.foreign_write_detector as detector

    (repo / ":colon.md").write_text("v1\n")
    # `add -A` rather than a pathspec: the fixture's own git runs WITHOUT
    # `GIT_LITERAL_PATHSPECS`, so naming this file would trip the very magic
    # the read under test pins against.
    _git(repo, "add", "-A")
    _git(repo, "commit", "-qm", "colon")

    handed: dict = {}
    real_run = subprocess.run

    def _capture(cmd, **kwargs):
        handed["env"] = dict(kwargs.get("env") or {})
        handed["passed_env"] = kwargs.get("env") is not None
        return real_run(cmd, **kwargs)

    monkeypatch.setattr(detector.subprocess, "run", _capture)

    visible = _git_visible_names(repo, deadline=_deadline())

    assert ":colon.md" in visible
    assert handed["passed_env"] is True, "subprocess.run was called without env="
    assert handed["env"]["GIT_LITERAL_PATHSPECS"] == "1"
    assert handed["env"]["LC_ALL"] == "C"
    assert handed["env"]["GIT_OPTIONAL_LOCKS"] == "0"


def test_awkward_names_round_trip_unchanged(repo: Path) -> None:
    """``-z`` means no quoting and no C-escaping, so no unescape helper may be
    applied — one would corrupt exactly these names into registry keys that
    match nothing."""
    for name in ("a file.md", "café.md"):
        (repo / name).write_text("v1\n")
    _git(repo, "add", "a file.md", "café.md")
    _git(repo, "commit", "-qm", "awkward")

    visible = _git_visible_names(repo, deadline=_deadline())

    assert {"a file.md", "café.md"} <= visible


def test_a_non_zero_exit_raises_rather_than_returning_an_empty_set(
    tmp_path: Path,
) -> None:
    """An empty visible set is about to mean "nothing here can be watched".
    Returning it for a failed read would turn a broken instrument into that
    verdict — the same swallow the status poll already refuses."""
    not_a_repo = tmp_path / "plain"
    not_a_repo.mkdir()

    with pytest.raises(GitPollError):
        _git_visible_names(not_a_repo, deadline=_deadline())


def test_a_read_that_never_classifies_leaves_that_to_the_poll(
    tmp_path: Path,
) -> None:
    """The status poll owns the exit-128 classification and the latch re-arm.
    A second classifier here would put the whole no-work-tree story behind it,
    so this one raises the plain type even where the poll would not."""
    not_a_repo = tmp_path / "plain"
    not_a_repo.mkdir()

    with pytest.raises(GitPollError) as caught:
        _git_visible_names(not_a_repo, deadline=_deadline())

    assert not isinstance(caught.value, NotAGitRepositoryError)


def test_a_deadline_already_past_raises_rather_than_spawning(
    repo: Path, monkeypatch
) -> None:
    """The shared deadline is the whole mechanism: a helper that spawned anyway
    would let one tick run for twice ``poll_budget_sec`` on the thread that also
    reclaims grants. Asserted by the absence of the spawn, not by the raise —
    a raise after a spawn would look identical."""
    import ccs.adapters.claude_code.foreign_write_detector as detector

    spawned: list = []

    def _record(cmd, **kwargs):
        # A CLEAN result, deliberately: a stub that blew up would kill the
        # no-pre-check mutant by accident, on the exception rather than on the
        # spawn. This one lets that mutant return a perfectly good empty set,
        # so the raise below is what catches it.
        spawned.append(list(cmd))
        return subprocess.CompletedProcess(cmd, 0, stdout=b"", stderr=b"")

    monkeypatch.setattr(detector.subprocess, "run", _record)

    with pytest.raises(GitPollError):
        _git_visible_names(repo, deadline=time.monotonic() - 1.0)

    assert spawned == []


# ---------------------------------------------------------------------------
# A workspace the pass can never poll — the shipped demos' temp directory
# ---------------------------------------------------------------------------


def test_a_non_git_workspace_is_reported_once_and_without_a_traceback(
    coordinator, repo: Path, caplog
) -> None:
    """``tempfile.mkdtemp()`` is not a repository, so every shipped demo runs
    the pass against a workspace it can never poll — and before this, printed a
    full traceback per sweep tick over the demo's own stdout and over CI.

    The condition is permanent and no operator action clears it, so it is worth
    exactly one line. Quieting the LOG must not quiet the REPORT: the ticks are
    still not recorded, which is what makes the coverage hole visible offline.
    """
    import logging
    import shutil

    _register(coordinator, "notes.md", "v1\n")
    shutil.rmtree(repo / ".git")
    faults: set[str] = set()

    with caplog.at_level(logging.DEBUG):
        for tick in range(3):
            _tick(coordinator, now_unix=1000.0 + tick, faults=faults)

    logged = [r for r in caplog.records if r.name.endswith("foreign_write_detector")]
    assert len(logged) == 1
    assert logged[0].levelno == logging.DEBUG
    assert logged[0].exc_info is None  # no traceback
    assert "inert" in logged[0].getMessage()
    assert coordinator.registry.detection_runs() == []


def test_the_latch_re_arms_once_the_workspace_can_be_polled_again(
    coordinator, repo: Path, caplog
) -> None:
    """The latch tracks the condition, not the coordinator's lifetime. A
    workspace can become a repository (``git init``) and a repository can stop
    being one, and the second outage is as worth one line as the first."""
    import logging
    import shutil

    _register(coordinator, "notes.md", "v1\n")
    git_dir = repo / ".git"
    stashed = repo.parent / "git-stashed"
    faults: set[str] = set()

    with caplog.at_level(logging.DEBUG):
        shutil.move(str(git_dir), str(stashed))
        _tick(coordinator, now_unix=100.0, faults=faults)
        shutil.move(str(stashed), str(git_dir))
        _tick(coordinator, now_unix=200.0, faults=faults)  # a completed poll
        shutil.move(str(git_dir), str(stashed))
        _tick(coordinator, now_unix=300.0, faults=faults)
        shutil.move(str(stashed), str(git_dir))

    logged = [r for r in caplog.records if r.name.endswith("foreign_write_detector")]
    assert len(logged) == 2
    assert all(r.levelno == logging.DEBUG and r.exc_info is None for r in logged)


def test_a_genuine_poll_failure_keeps_its_traceback_every_tick(
    coordinator, repo: Path, caplog, monkeypatch
) -> None:
    """Only the one permanent condition is quieted. A corrupt index, a locked
    repository or a dubious-ownership refusal is neither permanent nor
    non-actionable, and stays exactly as loud as it was — every tick, with the
    stack that says where it came from.

    The raise deliberately carries the missing-repository SENTENCE while being
    the base type: the quiet path keys on the type the git layer chose, never on
    a substring re-sniffed at the top.
    """
    import logging

    import ccs.adapters.claude_code.foreign_write_detector as detector

    _register(coordinator, "notes.md", "v1\n")

    def _boom(*_args, **_kwargs):
        raise GitPollError("git status exited 128: fatal: not a git repository")

    monkeypatch.setattr(detector, "_git_dirty_paths", _boom)
    faults: set[str] = set()

    with caplog.at_level(logging.DEBUG):
        for tick in range(2):
            _tick(coordinator, now_unix=1000.0 + tick, faults=faults)

    logged = [r for r in caplog.records if r.name.endswith("foreign_write_detector")]
    assert len(logged) == 2  # never latched
    assert all(r.levelno == logging.ERROR for r in logged)
    assert all(r.exc_info is not None for r in logged)  # traceback kept


def test_a_corrupt_repository_stays_loud_on_every_tick(
    coordinator, repo: Path, caplog
) -> None:
    """The end-to-end half of the same defect. A gutted ``.git`` reaches the
    pass as an ordinary poll failure, so it keeps its traceback every tick and
    never enters the latch — an operator who can fix this has to hear it."""
    import logging
    import shutil

    _register(coordinator, "notes.md", "v1\n")
    shutil.rmtree(repo / ".git" / "objects")
    faults: set[str] = set()

    with caplog.at_level(logging.DEBUG):
        for tick in range(2):
            _tick(coordinator, now_unix=1000.0 + tick, faults=faults)

    logged = [r for r in caplog.records if r.name.endswith("foreign_write_detector")]
    assert len(logged) == 2
    assert all(r.levelno == logging.ERROR for r in logged)
    assert all(r.exc_info is not None for r in logged)
    assert faults == set()  # nothing latched
    assert coordinator.registry.detection_runs() == []


# ---------------------------------------------------------------------------
# A workspace nothing can ever watch (#207)
# ---------------------------------------------------------------------------


@pytest.fixture
def bare_workspace(tmp_path: Path) -> Path:
    """A coordinator root that is NOT a git work tree — the examples' shape.

    Every example spawns a coordinator over ``tempfile.mkdtemp()``, and a temp
    directory is not a repository. Checked rather than assumed: a stray ``.git``
    anywhere above ``tmp_path`` would silently turn this into a repository
    fixture, and every assertion below would then pass for the wrong reason.
    """
    root = tmp_path / "bare"
    root.mkdir()
    (root / "notes.md").write_text("v1\n")
    assert _repository_is_absent(root), "fixture is inside a repository"
    return root


@pytest.fixture
def bare_coordinator(bare_workspace: Path):
    registry = SqliteArtifactRegistry(bare_workspace / ".coherence" / "state.db")
    policy = _policy(bare_workspace, "notes.md")
    coordinator = _Coordinator(bare_workspace, registry, policy)
    # Tracked AND registered, so the tick reaches the git poll. With nothing in
    # scope the pass returns before git is asked, which is a different fact.
    _register(coordinator, "notes.md", "v1\n")
    yield coordinator
    registry.close()


def test_a_workspace_with_no_work_tree_is_noted_once_and_never_ticked(
    bare_coordinator,
) -> None:
    """The note is what lets the offline report say "nothing here can be
    watched" instead of accusing the instrument of never having run — and it
    is recorded ONCE per became-uncoverable transition, not once per tick: the
    poll retries every tick and the run id rotates on every failure, so a
    per-tick record keyed on that id would write one row per sweep interval.
    """
    faults: set[str] = set()
    for tick in range(3):
        assert _tick(bare_coordinator, now_unix=100.0 + tick, faults=faults) == 0
    registry = bare_coordinator.registry
    assert registry.detection_runs() == []  # never a tick: the note is not an interval
    notes = registry.detection_uncoverable()
    assert len(notes) == 1, f"one note per transition, not per tick: {notes}"
    assert notes[0].reason == "no-git-work-tree"
    assert notes[0].observed_at_unix == 100.0  # the tick the latch closed on
    assert faults == {"not-a-git-repository"}

    db = bare_coordinator.coordinator_root / ".coherence" / "state.db"
    report = read_foreign_write_report(db)
    assert report.state == NOT_COVERABLE
    assert report.instrumented is False
    assert report.covers(100.0, 102.0) is False


def test_a_workspace_that_regains_and_loses_its_repository_is_noted_twice(
    bare_coordinator,
) -> None:
    """The note follows the latch, and the latch re-arms on a completed poll —
    so a workspace that becomes a repository and later stops being one is a
    second fact, recorded a second time, with the watched interval between."""
    root = bare_coordinator.coordinator_root
    faults: set[str] = set()
    _tick(bare_coordinator, now_unix=100.0, faults=faults)
    _git(root, "init", "-q")
    _tick(bare_coordinator, now_unix=200.0, faults=faults)  # completes: re-arms, ticks
    assert faults == set()
    shutil.rmtree(root / ".git")
    _tick(bare_coordinator, now_unix=300.0, faults=faults)

    registry = bare_coordinator.registry
    assert [n.observed_at_unix for n in registry.detection_uncoverable()] == [100.0, 300.0]
    assert [r.first_tick_unix for r in registry.detection_runs()] == [200.0]


def test_the_note_never_shares_a_run_id_with_a_tick(bare_coordinator) -> None:
    """No single run id may read as both an interval the detector watched and
    a workspace with nothing to watch. Here the poll can succeed on the very
    next tick, so the note's id has to be retired on BOTH sides of the note."""
    root = bare_coordinator.coordinator_root
    faults: set[str] = set()
    _git(root, "init", "-q")
    _tick(bare_coordinator, now_unix=100.0, faults=faults)  # a tick, on id A
    shutil.rmtree(root / ".git")
    _tick(bare_coordinator, now_unix=200.0, faults=faults)  # the note, on its own id
    _git(root, "init", "-q")
    _tick(bare_coordinator, now_unix=300.0, faults=faults)  # a tick, on a third id

    registry = bare_coordinator.registry
    tick_ids = {r.run_id for r in registry.detection_runs()}
    note_ids = {n.run_id for n in registry.detection_uncoverable()}
    assert len(tick_ids) == 2 and len(note_ids) == 1, (tick_ids, note_ids)
    assert not (tick_ids & note_ids), "a run id reads as both watched and unwatchable"


class _RefusingUncoverable:
    """A registry whose note write fails ``refusals`` times, then succeeds."""

    def __init__(self, inner, refusals: int) -> None:
        self._inner = inner
        self.refusals = refusals
        self.attempts = 0

    def record_detection_uncoverable(self, reason: str, now_unix: float) -> None:
        self.attempts += 1
        if self.attempts <= self.refusals:
            raise RuntimeError("store refused the note")
        self._inner.record_detection_uncoverable(reason, now_unix)

    def __getattr__(self, name: str):
        return getattr(self._inner, name)


def test_a_refused_note_keeps_the_latch_open_and_is_retried(
    bare_coordinator, caplog
) -> None:
    """A store that refuses the note — locked, out of disk — must not leave the
    report accusing the instrument of never running. The latch closes only
    once the note is durable, so the next tick tries again; and the refusal is
    a real fault, so unlike the condition it interrupts it stays loud."""
    import logging

    refusing = _RefusingUncoverable(bare_coordinator.registry, refusals=2)
    bare_coordinator.registry = refusing
    faults: set[str] = set()
    with caplog.at_level(logging.ERROR):
        _tick(bare_coordinator, now_unix=100.0, faults=faults)
        _tick(bare_coordinator, now_unix=101.0, faults=faults)
        assert faults == set()  # nothing durable yet, so nothing latched
        _tick(bare_coordinator, now_unix=102.0, faults=faults)

    assert faults == {"not-a-git-repository"}
    assert refusing.attempts == 3
    assert [n.observed_at_unix for n in refusing.detection_uncoverable()] == [102.0]
    refused = [r for r in caplog.records if "could not record" in r.getMessage()]
    assert len(refused) == 2 and all(r.exc_info is not None for r in refused)


def test_a_bare_root_with_nothing_registered_stays_not_instrumented(bare_workspace: Path) -> None:
    """The guide's precondition, pinned: ``not-coverable`` needs at least one
    artifact the coordinator already knows AND tracks to have reached git.
    The policy assertion fixes which half is missing here — the pattern does
    track ``notes.md``; nothing ever registered it — so the empty scope is
    provably non-registration, not a policy that tracks nothing. With nothing
    registered the pass returns before the poll ("Deliberately no tick"), so no
    fault is latched, no note is written, and the store reads
    ``not-instrumented`` exactly as it did before the fourth state existed —
    the shape of every shipped example until its first ``/session/begin``.
    Removing that early return makes this fail: with an empty scope the tick
    then records a covered_count=0 run."""
    db = bare_workspace / ".coherence" / "state.db"
    registry = SqliteArtifactRegistry(db)
    coordinator = _Coordinator(bare_workspace, registry, _policy(bare_workspace, "notes.md"))
    assert coordinator.policy.is_tracked("notes.md")
    faults: set[str] = set()
    try:
        for tick in range(3):
            assert _tick(coordinator, now_unix=100.0 + tick, faults=faults) == 0
        assert faults == set()
        assert registry.detection_runs() == []
        assert registry.detection_uncoverable() == []
    finally:
        registry.close()
    report = read_foreign_write_report(db)
    assert report.state == NOT_INSTRUMENTED
    assert report.uncoverable == ()
