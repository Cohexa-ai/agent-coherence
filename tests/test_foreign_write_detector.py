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

import logging
import os
import shutil
import sqlite3
import subprocess
import time
from pathlib import Path
from unittest import mock

import pytest

from ccs.adapters.claude_code import foreign_write_detector
from ccs.adapters.claude_code.foreign_write_detector import (
    _MAX_PROBE_ATTEMPTS,
    NO_WORK_TREE_REASON,
    Coverage,
    DetectionState,
    GitPollError,
    _classify_mismatch,
    _git_dirty_paths,
    _has_git_entry,
    _parse_porcelain_v2,
    _poll_env,
    _probe_work_tree,
    run_detection_pass,
)
from ccs.adapters.claude_code.policy import TrackedArtifactPolicy
from ccs.coordinator.sqlite_registry import SqliteArtifactRegistry
from ccs.core.substrate import sha256_hex

WINDOW = 10.0
POLL_BUDGET = 30.0
_DETECTOR_LOGGER = "ccs.adapters.claude_code.foreign_write_detector"


def _tick(
    coordinator,
    *,
    now_unix: float,
    window_sec: float = WINDOW,
    cache=None,
    state: DetectionState | None = None,
) -> int:
    """One tick with the caller-owned cross-tick state the sweep loop holds.

    ``cache`` stays a stat-cache dict so the tests that thread one across ticks
    keep reading as "the same cache, two ticks"; it is wrapped in a fresh state
    here rather than at each call site."""
    return run_detection_pass(
        coordinator,
        now_unix=now_unix,
        window_sec=window_sec,
        poll_budget_sec=POLL_BUDGET,
        state=state
        if state is not None
        else DetectionState(stat_cache=cache if cache is not None else {}),
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


# ---------------------------------------------------------------------------
# A workspace outside any git work tree: permanently uncoverable, not an outage
# ---------------------------------------------------------------------------


@pytest.fixture
def bare_workspace(tmp_path: Path):
    """A coordinator root that is NOT a git work tree — the examples' shape.

    Every example spawns a coordinator over ``tempfile.mkdtemp()``, and a temp
    directory is not a repository. Nested under ``tmp_path`` and checked, rather
    than assumed: a stray ``.git`` anywhere above would silently turn this into
    a repository fixture and every assertion below would still pass vacuously.
    """
    root = tmp_path / "bare"
    root.mkdir()
    (root / "notes.md").write_text("v1\n")
    assert not _has_git_entry(root), "fixture is inside a repository"
    return root


@pytest.fixture
def bare_coordinator(bare_workspace: Path):
    registry = SqliteArtifactRegistry(bare_workspace / ".coherence" / "state.db")
    policy = _policy(bare_workspace, "notes.md")
    yield _Coordinator(bare_workspace, registry, policy)
    registry.close()


def test_a_workspace_with_no_work_tree_is_probed_once_and_then_left_alone(
    bare_coordinator, caplog
) -> None:
    """The defect: one traceback per tick, forever, for a permanent condition.

    Asserts the WHOLE shape rather than the log alone — a fix that only quieted
    the logger while still shelling out to a doomed `git status` every tick
    would pass a log-only assertion.
    """
    _register(bare_coordinator, "notes.md", "v1\n")
    state = DetectionState()

    calls: list[list[str]] = []
    real_run = subprocess.run

    def _counting_run(argv, *args, **kwargs):
        calls.append(list(argv))
        return real_run(argv, *args, **kwargs)

    with caplog.at_level(logging.DEBUG, logger=_DETECTOR_LOGGER):
        with mock.patch.object(subprocess, "run", _counting_run):
            for tick in range(5):
                assert _tick(bare_coordinator, now_unix=100.0 + tick, state=state) == 0

    assert state.coverage is Coverage.NO_WORK_TREE
    # One probe across five ticks, and never a `git status` — the poll that can
    # only fail here is not attempted at all.
    assert len(calls) == 1, calls
    assert "rev-parse" in calls[0]
    assert not [argv for argv in calls if "status" in argv]

    # Said once, at INFO, with no traceback — and actionable.
    detector_records = [
        r for r in caplog.records if r.name == _DETECTOR_LOGGER
    ]
    assert len(detector_records) == 1
    record = detector_records[0]
    assert record.levelno == logging.INFO
    assert record.exc_info is None
    assert "git init" in record.getMessage()


def test_an_uncoverable_workspace_records_no_tick(bare_coordinator) -> None:
    """The reason the disable cannot simply return early and say nothing.

    A tick asserts the detector LOOKED. Recording one here would let a workspace
    nothing can watch answer a coverage question with a clean span — the exact
    false-clean reading the liveness row exists to prevent.
    """
    _register(bare_coordinator, "notes.md", "v1\n")
    _tick(bare_coordinator, now_unix=100.0, state=DetectionState())

    assert bare_coordinator.registry.detection_runs() == []
    uncoverable = bare_coordinator.registry.detection_uncoverable()
    assert [(u.reason, u.observed_at_unix) for u in uncoverable] == [
        (NO_WORK_TREE_REASON, 100.0)
    ]


def test_the_offline_report_separates_uncoverable_from_never_instrumented(
    bare_coordinator,
) -> None:
    """Success criterion: a zero is never ambiguous.

    Both stores have no ticks and no counts. Before this, both read
    ``not-instrumented`` — "the detector never ran" — which is a false
    accusation against an instrument that ran and correctly reported that there
    is nothing here to watch.
    """
    from ccs.diagnose.foreign_writes import (
        NOT_COVERABLE,
        NOT_INSTRUMENTED,
        read_foreign_write_report,
    )

    _register(bare_coordinator, "notes.md", "v1\n")
    _tick(bare_coordinator, now_unix=100.0, state=DetectionState())
    db = Path(bare_coordinator.registry._db_path)  # noqa: SLF001 — the store under test
    bare_coordinator.registry.close()

    report = read_foreign_write_report(db)
    assert report.state == NOT_COVERABLE
    assert report.state != NOT_INSTRUMENTED
    assert report.instrumented is False
    assert report.covers(100.0, 100.0) is False  # never a coverage claim
    assert [u.reason for u in report.uncoverable] == [NO_WORK_TREE_REASON]


def test_a_workspace_with_nothing_in_scope_is_never_even_probed(
    bare_coordinator,
) -> None:
    """The empty-scope branch keeps its meaning, and keeps its cost.

    Registering nothing leaves the pass with no covered artifact, and that
    already returns without a tick. It must also return without a subprocess:
    probing a workspace the detector has no reason to look at would spend a
    process on every coordinator that never registers anything.

    Asserts on the OBSERVED argv list rather than on a raising sentinel. A
    sentinel is unusable here and the first version of this test used one: the
    pass is contractually required to swallow every exception, so an
    ``AssertionError`` side effect is caught by ``run_detection_pass`` and none
    of the assertions below can see it. Moving the probe ahead of the
    empty-scope check — deleting the guarantee this test is named for — left
    that version green.
    """
    state = DetectionState()
    calls: list[list[str]] = []
    real_run = subprocess.run

    def _recording_run(argv, *args, **kwargs):
        calls.append(list(argv))
        return real_run(argv, *args, **kwargs)

    with mock.patch.object(subprocess, "run", _recording_run):
        assert _tick(bare_coordinator, now_unix=100.0, state=state) == 0
    assert calls == [], calls
    assert state.coverage is None
    assert state.probe_attempts == 0
    assert bare_coordinator.registry.detection_uncoverable() == []


def test_a_repository_that_is_broken_rather_than_absent_stays_loud(
    coordinator, repo: Path, caplog
) -> None:
    """The distinction message-matching cannot make.

    Git reports a corrupt ``HEAD`` with the SAME ``fatal: not a git repository``
    text and the SAME exit 128 as a directory outside any repository (verified
    against git 2.48). A substring check would file this under "not covered" and
    retire the alarm on a genuine fault, so the quiet state is earned from the
    filesystem instead: a ``.git`` is present here, so the poll speaks and it
    raises.
    """
    _register(coordinator, "notes.md", "v1\n")
    (repo / ".git" / "HEAD").write_text("garbage\n")
    state = DetectionState()

    with caplog.at_level(logging.DEBUG, logger=_DETECTOR_LOGGER):
        assert _tick(coordinator, now_unix=100.0, state=state) == 0

    assert state.coverage is not Coverage.NO_WORK_TREE
    assert coordinator.registry.detection_uncoverable() == []
    assert coordinator.registry.detection_runs() == []  # no tick over a failed poll
    errors = [r for r in caplog.records if r.levelno >= logging.ERROR]
    assert errors and errors[0].exc_info is not None


def test_a_probe_that_cannot_run_still_earns_the_quiet_state_from_the_filesystem(
    bare_coordinator,
) -> None:
    """A timeout and a missing git binary say nothing about the workspace.

    The filesystem still does, and it is the evidence the quiet state rests on —
    which is what keeps this fix working on the CI machine where a 0.1s sweep
    interval leaves a cold `git` invocation no room.
    """
    _register(bare_coordinator, "notes.md", "v1\n")
    state = DetectionState()
    with mock.patch.object(
        subprocess, "run", side_effect=subprocess.TimeoutExpired("git", 5.0)
    ):
        assert _tick(bare_coordinator, now_unix=100.0, state=state) == 0
    assert state.coverage is Coverage.NO_WORK_TREE


def test_a_probe_that_cannot_run_over_a_real_repository_does_not(
    coordinator,
) -> None:
    """The other half of the same rule: no evidence, no quiet state."""
    _register(coordinator, "notes.md", "v1\n")
    state = DetectionState()
    with mock.patch.object(
        subprocess, "run", side_effect=subprocess.TimeoutExpired("git", 5.0)
    ):
        assert _tick(coordinator, now_unix=100.0, state=state) == 0
    assert state.coverage is None  # undetermined is never cached
    assert coordinator.registry.detection_uncoverable() == []


def test_a_root_inside_a_git_directory_has_no_work_tree(repo: Path) -> None:
    """Git answers ``false`` on exit 0 here — a positive, definitive negative.

    Worth pinning separately from the absent-repository case: a ``.git`` IS
    present, so the filesystem disambiguator would withhold the quiet state.
    Git's own ``false`` is what grants it.
    """
    assert _probe_work_tree(repo, budget_sec=POLL_BUDGET) is Coverage.COVERED
    assert (
        _probe_work_tree(repo / ".git", budget_sec=POLL_BUDGET)
        is Coverage.NO_WORK_TREE
    )


def test_losing_the_repository_after_a_tick_ends_that_interval(
    coordinator, repo: Path
) -> None:
    """The narrow path on which an interval IS open when the probe first lands.

    Normally the probe precedes every tick, so no interval can be open the first
    time it answers. But an UNDETERMINED probe is deliberately not cached: it
    lets the poll speak, the poll can succeed, and a tick is then recorded with
    ``coverage`` still unset. The next tick probes for real — and by then the
    repository can be gone.

    Two properties, and the second is why the note is given its own run id: the
    watched interval must END here rather than be extended across the blind
    window by any later tick, and no single run may appear both as an interval
    the detector watched and as one that had no workspace to watch. A store read
    offline cannot see this control flow and must not be able to hold that pair.
    """
    _register(coordinator, "notes.md", "v1\n")
    state = DetectionState()

    real_run = subprocess.run

    def _probe_times_out(argv, *args, **kwargs):
        if "rev-parse" in argv:
            raise subprocess.TimeoutExpired("git", 5.0)
        return real_run(argv, *args, **kwargs)

    with mock.patch.object(subprocess, "run", _probe_times_out):
        _tick(coordinator, now_unix=100.0, state=state)
    assert state.coverage is None  # undetermined, so the next tick re-probes
    watched = coordinator.registry.detection_runs()
    assert [(r.first_tick_unix, r.last_tick_unix) for r in watched] == [(100.0, 100.0)]

    shutil.rmtree(repo / ".git")
    assert _tick(coordinator, now_unix=200.0, state=state) == 0
    assert state.coverage is Coverage.NO_WORK_TREE

    after = coordinator.registry.detection_runs()
    assert [(r.first_tick_unix, r.last_tick_unix) for r in after] == [(100.0, 100.0)]
    uncoverable = coordinator.registry.detection_uncoverable()
    assert [u.reason for u in uncoverable] == [NO_WORK_TREE_REASON]
    assert {u.run_id for u in uncoverable}.isdisjoint({r.run_id for r in after})


# ---------------------------------------------------------------------------
# The quiet state is earned from a successful NEGATIVE observation, never from
# a lookup that failed to produce one
# ---------------------------------------------------------------------------


def _unreadable(path: Path):
    """Make ``path`` untraversable, restoring it even if the test fails."""
    import contextlib

    @contextlib.contextmanager
    def _ctx():
        original = path.stat().st_mode
        os.chmod(path, 0o000)
        try:
            yield
        finally:
            os.chmod(path, original)

    return _ctx()


def test_a_root_that_cannot_be_resolved_stays_loud(tmp_path: Path) -> None:
    """Kills `resolve(strict=True)` -> `resolve()`.

    The non-strict form cheerfully returns a path for a root that does not
    exist; the walk then finds no `.git` above it and the workspace reads as one
    we OBSERVED to be bare. It was never seen at all.
    """
    # Precondition, matching the bare_workspace fixture's own policy: if a .git
    # sat above the pytest temp root, the non-strict mutant would answer True
    # for that reason and this test would pass without testing anything.
    assert _has_git_entry(tmp_path) is False, "temp root is inside a repository"
    assert _has_git_entry(tmp_path / "never-existed") is True
    assert (
        _probe_work_tree(tmp_path / "never-existed", budget_sec=POLL_BUDGET)
        is Coverage.UNDETERMINED
    )


@pytest.mark.skipif(
    hasattr(os, "geteuid") and os.geteuid() == 0,
    reason="chmod 000 denies nothing to root, so this guard would be vacuous",
)
def test_a_repository_whose_root_became_unreadable_stays_loud(repo: Path) -> None:
    """Kills a revert to `os.path.lexists`, and the whole finding's point.

    `lexists` swallows the permission error and answers "absent", so a real
    repository whose root stopped being readable earned the permanent quiet
    state and a durable note asserting something false — on an ordinary local
    filesystem, with no network share and no unmount.
    """
    with _unreadable(repo):
        assert _has_git_entry(repo) is True
        assert _probe_work_tree(repo, budget_sec=POLL_BUDGET) is Coverage.UNDETERMINED


def test_a_symlink_loop_root_stays_loud(tmp_path: Path) -> None:
    """Kills `except (OSError, RuntimeError)` -> `except OSError`.

    VERSION-SENSITIVE BY DESIGN, and that is the finding: `Path.resolve` raises
    RuntimeError — not OSError — for a symlink loop on Python 3.11 and 3.12,
    this project's CI matrix, while 3.13 raises OSError. An OSError-only catch
    therefore makes the verdict depend on the interpreter: a traceback per tick
    on the supported versions, a permanently quiet false note on 3.13. Do not
    "simplify" the tuple away on a 3.13 laptop and watch this stay green.
    """
    loop = tmp_path / "loop"
    os.symlink(str(loop), str(loop))
    assert _has_git_entry(loop) is True
    assert _probe_work_tree(loop, budget_sec=POLL_BUDGET) is Coverage.UNDETERMINED

    # The arm that bites on EVERY interpreter, including the 3.13 most of this
    # repo is developed on, where the real loop raises OSError and the assertion
    # above therefore survives the narrowed catch.
    with mock.patch.object(Path, "resolve", side_effect=RuntimeError("loop")):
        assert _has_git_entry(tmp_path) is True


def test_an_ordinary_bare_directory_is_still_quiet(bare_workspace: Path) -> None:
    """Kills an over-broad fix that re-arms the noise this whole change retires.

    Withhold the quiet state from a root we could not SEE; never from one we
    saw and found bare. This is the case the feature exists for.
    """
    assert _has_git_entry(bare_workspace) is False
    assert (
        _probe_work_tree(bare_workspace, budget_sec=POLL_BUDGET)
        is Coverage.NO_WORK_TREE
    )


def test_an_unseeable_root_records_no_note_and_caches_nothing(
    bare_coordinator, bare_workspace: Path, caplog
) -> None:
    """The store, not just the enum — a fix that only moves `Coverage` is not one.

    A root renamed away before the first probe must leave the detector with
    nothing settled and the store with nothing asserted, and the poll must be
    the thing that speaks (loudly).
    """
    _register(bare_coordinator, "notes.md", "v1\n")
    state = DetectionState()
    bare_workspace.rename(bare_workspace.parent / "moved-away")
    try:
        with caplog.at_level(logging.DEBUG, logger=_DETECTOR_LOGGER):
            assert _tick(bare_coordinator, now_unix=100.0, state=state) == 0
        assert state.coverage is None
        assert bare_coordinator.registry.detection_uncoverable() == []
        assert bare_coordinator.registry.detection_runs() == []
        errors = [r for r in caplog.records if r.levelno >= logging.ERROR]
        assert errors and errors[0].exc_info is not None
    finally:
        (bare_workspace.parent / "moved-away").rename(bare_workspace)


# ---------------------------------------------------------------------------
# The verdict may never outrun the record that makes it readable
# ---------------------------------------------------------------------------


class _FailingUncoverable:
    """Registry proxy whose uncoverable write fails the first ``fail_times``."""

    def __init__(self, inner, fail_times: int) -> None:
        self._inner = inner
        self._remaining = fail_times
        self.attempts = 0

    def __getattr__(self, name):
        return getattr(self._inner, name)

    def record_detection_uncoverable(self, reason: str, now_unix: float) -> None:
        self.attempts += 1
        if self._remaining > 0:
            self._remaining -= 1
            raise sqlite3.OperationalError("database is locked")
        self._inner.record_detection_uncoverable(reason, now_unix)


@pytest.mark.parametrize("fail_times", [1, _MAX_PROBE_ATTEMPTS, _MAX_PROBE_ATTEMPTS + 2])
def test_a_refused_note_leaves_the_verdict_unsettled_and_retries(
    bare_coordinator, fail_times: int
) -> None:
    """Kills caching the verdict before the two registry writes.

    One transient sqlite failure used to be permanent: the cache is what ends
    the retries, so setting it first made a refused write the LAST attempt the
    coordinator ever made — no note, no tick, and an offline report reading
    `not-instrumented`, the instrument accused of never having run.
    """
    _register(bare_coordinator, "notes.md", "v1\n")
    guarded = _FailingUncoverable(bare_coordinator.registry, fail_times=fail_times)
    bare_coordinator.registry = guarded
    state = DetectionState()
    argv: list[list[str]] = []
    real_run = subprocess.run

    def _recording_run(a, *args, **kwargs):
        argv.append(list(a))
        return real_run(a, *args, **kwargs)

    with mock.patch.object(subprocess, "run", _recording_run):
        for tick in range(fail_times + 3):
            assert _tick(bare_coordinator, now_unix=100.0 + tick, state=state) == 0
            if tick < fail_times:
                assert state.coverage is None, "a refused note must not settle"
                assert guarded._inner.detection_uncoverable() == []

    # The note lands on the first tick the store accepts it, however many it
    # refused first. The retry is the store's, so it must not be bounded by the
    # probe's fork budget: counting a refused write as a spent probe attempt
    # retired the probe after three of them and let `git status` run in a
    # non-git workspace on every tick after that — the exact defect this whole
    # change exists to remove, re-armed by a transient sqlite error.
    assert state.coverage is Coverage.NO_WORK_TREE
    assert [u.reason for u in guarded._inner.detection_uncoverable()] == [
        NO_WORK_TREE_REASON
    ]
    assert guarded.attempts == fail_times + 1
    assert not [a for a in argv if "status" in a], argv
    # One fork and one INFO for the verdict, no matter how long the store
    # refused it: the retry is of the note alone.
    assert len([a for a in argv if "rev-parse" in a]) == 1, argv
    assert state.probe_attempts == 1


# ---------------------------------------------------------------------------
# The probe and the poll are one pass billed against one tick
# ---------------------------------------------------------------------------


def test_the_probe_and_the_poll_share_one_tick_budget(coordinator) -> None:
    """Kills billing the probe its own floor on top of the poll's budget.

    Detection is the fifth pass in a loop whose first four are safety work
    (grant reclamation, session liveness). A pass that can occupy two sweep
    intervals delays that work, and two comments in this codebase promised it
    could not.
    """
    _register(coordinator, "notes.md", "v1\n")
    budget, probe_cost = 1.0, 0.5
    timeouts: list[float] = []
    real_run = subprocess.run

    def _recording_run(argv, *args, **kwargs):
        timeouts.append(kwargs["timeout"])
        if "rev-parse" in argv:
            time.sleep(probe_cost)  # a probe that spends half the tick
        return real_run(argv, *args, **kwargs)

    with mock.patch.object(subprocess, "run", _recording_run):
        run_detection_pass(
            coordinator,
            now_unix=100.0,
            window_sec=WINDOW,
            poll_budget_sec=budget,
            state=DetectionState(),
        )

    assert len(timeouts) == 2, timeouts  # the probe, then the poll
    # Asserting the ALLOWANCES, not their sum: a fast probe legitimately leaves
    # the poll most of the interval, so a summed bound would fail on healthy
    # behaviour. What must hold is that neither is granted more than the tick
    # has left — the probe no floor of its own, the poll only the remainder.
    assert timeouts[0] <= budget, f"probe billed its own floor: {timeouts}"
    assert timeouts[1] <= budget - probe_cost + 0.05, (
        f"poll billed the full interval after the probe spent half: {timeouts}"
    )


def test_a_never_resolving_probe_stops_forking_but_never_caches(
    coordinator, repo: Path
) -> None:
    """Kills removing the fork cap, and kills "fixing" it by caching UNDETERMINED.

    A nested mount inside a checkout answers UNDETERMINED forever (git's
    discovery stops at the boundary, the filesystem walk climbs past it), so an
    uncapped probe forks every tick for an answer it can never get. The cap
    bounds the FORK; the verdict must stay unsettled so a repository that is
    merely mid-clone is still re-probed by the poll's own honest failure.
    """
    _register(coordinator, "notes.md", "v1\n")
    state = DetectionState()
    probes = 0
    real_run = subprocess.run

    def _undetermined_probe(argv, *args, **kwargs):
        nonlocal probes
        if "rev-parse" in argv:
            probes += 1
            raise subprocess.TimeoutExpired("git", 5.0)
        return real_run(argv, *args, **kwargs)

    def _ticks_recorded() -> int:
        return sum(r.tick_count for r in coordinator.registry.detection_runs())

    with mock.patch.object(
        foreign_write_detector, "_has_git_entry", lambda root: True
    ), mock.patch.object(subprocess, "run", _undetermined_probe):
        for tick in range(_MAX_PROBE_ATTEMPTS):
            _tick(coordinator, now_unix=100.0 + tick, state=state)
        assert probes == _MAX_PROBE_ATTEMPTS, probes
        at_cap = _ticks_recorded()
        for tick in range(5):
            _tick(coordinator, now_unix=200.0 + tick, state=state)

    assert probes == _MAX_PROBE_ATTEMPTS, "the cap must stop the fork"
    assert state.coverage is None, "the cap bounds the fork, never the verdict"
    # And the poll must keep running PAST the cap. Comparing against the count
    # at the cap is the whole point: an `_ensure_coverable` that returned False
    # there would leave the runs recorded BEFORE it in place, so merely
    # asserting that some run exists passes while detection is silently off —
    # the wrong-direction regression the docstring promises against.
    assert _ticks_recorded() == at_cap + 5, (
        f"the poll stopped at the cap: {at_cap} -> {_ticks_recorded()}"
    )
    assert coordinator.registry.detection_uncoverable() == []
