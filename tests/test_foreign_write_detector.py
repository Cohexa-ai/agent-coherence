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

import subprocess
from pathlib import Path

import pytest

from ccs.adapters.claude_code.foreign_write_detector import (
    GitPollError,
    _classify_mismatch,
    _git_dirty_paths,
    _parse_porcelain_v2,
    _poll_env,
    run_detection_pass,
)
from ccs.adapters.claude_code.policy import TrackedArtifactPolicy
from ccs.coordinator.sqlite_registry import SqliteArtifactRegistry
from ccs.core.substrate import sha256_hex

WINDOW = 10.0
POLL_BUDGET = 30.0


def _tick(coordinator, *, now_unix: float, window_sec: float = WINDOW, cache=None) -> int:
    """One tick with the caller-owned stat cache the sweep loop holds."""
    return run_detection_pass(
        coordinator,
        now_unix=now_unix,
        window_sec=window_sec,
        poll_budget_sec=POLL_BUDGET,
        stat_cache=cache if cache is not None else {},
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
        _git_dirty_paths(not_a_repo, ["notes.md"], budget_sec=POLL_BUDGET)


def test_the_poll_never_takes_the_index_lock(repo: Path) -> None:
    """A background poll that rewrites the index would contend with the user's
    own git commands. Proven by the index file being byte-identical after."""
    index = repo / ".git" / "index"
    before = index.read_bytes()
    (repo / "notes.md").write_text("dirty\n")

    assert _git_dirty_paths(repo, ["notes.md"], budget_sec=POLL_BUDGET) == {"notes.md"}

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

    assert _git_dirty_paths(repo, names, budget_sec=POLL_BUDGET) == {names[1]}


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
    again would answer a coverage question for the whole blind period."""
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
        _git_dirty_paths(repo, names, budget_sec=0.0)


def test_the_last_batch_is_queried_too(repo: Path) -> None:
    names = [f"generated/file_{n:05d}.md" for n in range(10_000)]
    (repo / "generated").mkdir()
    for name in (names[1], names[-1]):
        (repo / name).write_text("x\n")
    _git(repo, "add", "generated")
    _git(repo, "commit", "-qm", "generated")
    (repo / names[1]).write_text("changed\n")
    (repo / names[-1]).write_text("changed\n")

    assert _git_dirty_paths(repo, names, budget_sec=POLL_BUDGET) == {
        names[1],
        names[-1],
    }
