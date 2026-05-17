# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""Tests for the four agent-coherence-* console scripts (Unit 6).

Covers happy paths, argparse + path validation, graceful-no-coordinator
behavior, and HTTP error propagation. End-to-end smoke (real spawned
coordinator) is exercised by the lifecycle tests; here we mostly hit the
control-flow / output-rendering paths.
"""

from __future__ import annotations

import json
import os
import urllib.error
from io import BytesIO
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

from ccs.adapters.claude_code import lifecycle
from ccs.adapters.claude_code.lifecycle import ensure_coordinator, stop_coordinator, LifecycleConfig
from ccs.cli import (
    coherence_coordinator,
    coherence_status,
    coherence_track,
    coherence_untrack,
)
from ccs.cli._coherence_client import (
    CoordinatorUnavailable,
    resolve_endpoint,
)


@pytest.fixture
def fast_cfg() -> LifecycleConfig:
    return LifecycleConfig(
        idle_shutdown_sec=0,
        sweep_interval_sec=0,
        port_file_retry_attempts=10,
        port_file_retry_interval_sec=0.05,
        connect_retry_attempts=10,
        connect_retry_interval_sec=0.05,
        spawn_self_probe_attempts=20,
    )


@pytest.fixture
def git_workspace(tmp_path: Path) -> Path:
    """A tmp_path with a minimal .git/ marker so find_coordinator_root resolves it."""
    (tmp_path / ".git").mkdir()
    return tmp_path


@pytest.fixture
def live_coordinator(git_workspace: Path, fast_cfg: LifecycleConfig):
    """Spawn a real coordinator and yield (workspace, port)."""
    port = ensure_coordinator(git_workspace, config=fast_cfg)
    assert port > 0
    yield git_workspace, port
    stop_coordinator(git_workspace)


# ----------------------------------------------------------------------
# coherence_coordinator
# ----------------------------------------------------------------------


def test_coordinator_not_in_git_repo_exits_1(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Edge case: no .git/ ancestor → exit 1 with a clear message.

    Uses ``monkeypatch.chdir`` (not raw ``os.chdir``) so the working
    directory is restored on test teardown — otherwise the leak breaks
    every subsequent test that relies on relative paths or tmp_path.
    """
    no_git = tmp_path / "no_git"
    no_git.mkdir()
    monkeypatch.chdir(no_git)
    rc = coherence_coordinator.main([])
    captured = capsys.readouterr()
    assert rc == 1
    assert "not in a git repository" in captured.out


def test_coordinator_spawns_and_prints_port(
    git_workspace: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Happy path: agent-coherence-coordinator --root <git> → exits 0,
    prints 'port=NNNN'."""
    try:
        rc = coherence_coordinator.main(["--root", str(git_workspace)])
        captured = capsys.readouterr()
        assert rc == 0
        assert captured.out.startswith("port=")
        port = int(captured.out.strip().split("=")[1])
        assert 1024 <= port <= 65535
    finally:
        stop_coordinator(git_workspace)


def test_coordinator_quiet_flag_suppresses_port_line(
    git_workspace: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """--quiet suppresses stdout but still exits 0."""
    try:
        rc = coherence_coordinator.main(["--root", str(git_workspace), "--quiet"])
        captured = capsys.readouterr()
        assert rc == 0
        assert captured.out == ""
    finally:
        stop_coordinator(git_workspace)


# ----------------------------------------------------------------------
# coherence_status
# ----------------------------------------------------------------------


def test_status_no_coordinator_exits_0_with_graceful_message(
    git_workspace: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Edge case: no coordinator running → exit 0 (NOT an error)."""
    rc = coherence_status.main(["--root", str(git_workspace)])
    captured = capsys.readouterr()
    assert rc == 0
    assert "no coordinator running" in captured.out


def test_status_renders_table_against_live_coordinator(
    live_coordinator, capsys: pytest.CaptureFixture[str]
) -> None:
    """Happy path: agent-coherence-status against a live coordinator → table."""
    workspace, port = live_coordinator
    rc = coherence_status.main(["--root", str(workspace)])
    captured = capsys.readouterr()
    assert rc == 0
    assert "Coordinator:" in captured.out
    assert f"pid={os.getpid()}" in captured.out
    assert "No tracked artifacts" in captured.out or "Tracked artifacts:" in captured.out


def test_status_json_mode_emits_raw_payload(
    live_coordinator, capsys: pytest.CaptureFixture[str]
) -> None:
    """--json mode prints the raw response so external tools can parse it."""
    workspace, port = live_coordinator
    rc = coherence_status.main(["--root", str(workspace), "--json"])
    captured = capsys.readouterr()
    assert rc == 0
    data = json.loads(captured.out)
    assert "tracked_artifacts" in data
    assert "sessions" in data
    assert data["coordinator_pid"] == os.getpid()


# ----------------------------------------------------------------------
# coherence_track + coherence_untrack — validation + happy path
# ----------------------------------------------------------------------


@pytest.mark.parametrize("bad_path,reason_substr", [
    ("/etc/passwd", "must be relative"),
    ("../../../etc/passwd", "'..'"),
    ("", "empty"),
])
def test_track_rejects_invalid_paths_without_network(
    git_workspace: Path, capsys: pytest.CaptureFixture[str],
    bad_path: str, reason_substr: str,
) -> None:
    """Pre-validation: invalid paths exit 1 without a network round-trip."""
    rc = coherence_track.main(["--root", str(git_workspace), bad_path])
    captured = capsys.readouterr()
    assert rc == 1
    assert reason_substr in captured.out


def test_track_no_coordinator_running_exits_2(
    git_workspace: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Valid path but coordinator down → exit 2."""
    rc = coherence_track.main(["--root", str(git_workspace), "docs/plan.md"])
    captured = capsys.readouterr()
    assert rc == 2
    assert "no coordinator running" in captured.out


def test_track_against_live_coordinator(
    live_coordinator, capsys: pytest.CaptureFixture[str]
) -> None:
    """Happy path: track a valid path → exit 0, /policy/track returns added."""
    workspace, port = live_coordinator
    # Create the file so the "does not exist on disk yet" warning doesn't fire
    target = workspace / "docs" / "plan.md"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("plan v1")

    rc = coherence_track.main(["--root", str(workspace), "docs/plan.md"])
    captured = capsys.readouterr()
    assert rc == 0
    assert "tracked docs/plan.md" in captured.out
    # tracked.yaml should now exist with the path
    tracked_yaml = workspace / ".coherence" / "tracked.yaml"
    assert tracked_yaml.is_file()
    assert "docs/plan.md" in tracked_yaml.read_text()


def test_track_warns_on_path_not_on_disk(
    live_coordinator, capsys: pytest.CaptureFixture[str]
) -> None:
    """Happy-ish path: tracking a path that doesn't exist on disk yet warns
    but still exits 0 (path will be seeded on first Read)."""
    workspace, port = live_coordinator
    rc = coherence_track.main(["--root", str(workspace), "docs/future.md"])
    captured = capsys.readouterr()
    assert rc == 0
    assert "does not exist on disk yet" in captured.out


def test_untrack_against_live_coordinator(
    live_coordinator, capsys: pytest.CaptureFixture[str]
) -> None:
    """Happy path: untrack a path → ignored.yaml updated, response carries removed list."""
    workspace, port = live_coordinator
    rc = coherence_untrack.main(["--root", str(workspace), "docs/draft.md"])
    captured = capsys.readouterr()
    assert rc == 0
    assert "untracked docs/draft.md" in captured.out
    ignored_yaml = workspace / ".coherence" / "ignored.yaml"
    assert ignored_yaml.is_file()
    assert "docs/draft.md" in ignored_yaml.read_text()


@pytest.mark.parametrize("bad_path,reason_substr", [
    ("/etc/passwd", "must be relative"),
    ("../escape", "'..'"),
    ("", "empty"),
])
def test_untrack_rejects_invalid_paths_without_network(
    git_workspace: Path, capsys: pytest.CaptureFixture[str],
    bad_path: str, reason_substr: str,
) -> None:
    rc = coherence_untrack.main(["--root", str(git_workspace), bad_path])
    captured = capsys.readouterr()
    assert rc == 1
    assert reason_substr in captured.out


# ----------------------------------------------------------------------
# _coherence_client — endpoint resolution edge cases
# ----------------------------------------------------------------------


def test_resolve_endpoint_missing_pid_file(git_workspace: Path) -> None:
    """No port file → CoordinatorUnavailable with operator-friendly message."""
    with pytest.raises(CoordinatorUnavailable) as excinfo:
        resolve_endpoint(git_workspace)
    assert "no coordinator running" in str(excinfo.value)


def test_resolve_endpoint_missing_secret_file(git_workspace: Path) -> None:
    """Port file present but hook.secret missing → CoordinatorUnavailable."""
    (git_workspace / ".coherence").mkdir(parents=True, exist_ok=True, mode=0o700)
    (git_workspace / ".coherence" / "server.pid").write_text("12345\n50000\n")
    with pytest.raises(CoordinatorUnavailable) as excinfo:
        resolve_endpoint(git_workspace)
    assert "authentication unavailable" in str(excinfo.value)


def test_resolve_endpoint_empty_secret(git_workspace: Path) -> None:
    """hook.secret exists but is empty → CoordinatorUnavailable."""
    coh = git_workspace / ".coherence"
    coh.mkdir(parents=True, exist_ok=True, mode=0o700)
    (coh / "server.pid").write_text("12345\n50000\n")
    (coh / "hook.secret").write_text("")
    with pytest.raises(CoordinatorUnavailable) as excinfo:
        resolve_endpoint(git_workspace)
    assert "empty" in str(excinfo.value)
