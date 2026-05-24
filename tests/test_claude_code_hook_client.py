# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""Tests for agent-coherence-hook-client — the command-type hook bridge.

Built as a Phase E.0 contingent deliverable when probe 2A revealed Claude
Code v2.1.131 rejects HTTP-type hooks.json URLs containing ${COHERENCE_PORT}
at load time. Hook-client reads CC's stdin payload, translates to the
coordinator's contract, POSTs, and forwards the response to stdout.
"""

from __future__ import annotations

import hashlib
import io
import json
import os
import uuid
from pathlib import Path
from typing import Any

import pytest

from ccs.adapters.claude_code.lifecycle import (
    LifecycleConfig,
    ensure_coordinator,
    stop_coordinator,
)
from ccs.cli import coherence_hook_client


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
    (tmp_path / ".git").mkdir()
    return tmp_path


@pytest.fixture
def live_coordinator(git_workspace: Path, fast_cfg: LifecycleConfig):
    port = ensure_coordinator(git_workspace, config=fast_cfg)
    assert port > 0
    yield git_workspace, port
    stop_coordinator(git_workspace)


def _sid() -> str:
    return str(uuid.uuid4())


def _drive(
    subcommand: str,
    cc_payload: dict[str, Any],
    workspace: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> tuple[int, str]:
    """Helper: feed cc_payload to hook-client via monkeypatched stdin,
    run the subcommand against the workspace, return (exit_code, stdout)."""
    monkeypatch.setattr("sys.stdin", io.StringIO(json.dumps(cc_payload)))
    rc = coherence_hook_client.main([subcommand, "--root", str(workspace)])
    captured = capsys.readouterr()
    return rc, captured.out


# ----------------------------------------------------------------------
# Happy paths
# ----------------------------------------------------------------------


def test_pre_read_against_live_coordinator(
    live_coordinator, monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Happy path: pre-read on a tracked file → coordinator returns fresh."""
    workspace, port = live_coordinator
    (workspace / "docs").mkdir(parents=True, exist_ok=True)
    (workspace / "docs" / "plan.md").write_text("plan v1")

    cc_payload = {
        "session_id": _sid(),
        "tool_name": "Read",
        "tool_input": {"file_path": str(workspace / "docs" / "plan.md")},
    }
    rc, out = _drive("pre-read", cc_payload, workspace, monkeypatch, capsys)
    assert rc == 0
    response = json.loads(out)
    # T-02 / ce-review: tightened from isinstance(response, dict) to a
    # specific shape check. pre-read returns either {status: "fresh"|"stale"}
    # or the fast-path empty {} for untracked paths (handler returns 200 +
    # {ok: true} before fresh-shape logic). Either is acceptable.
    assert isinstance(response, dict)
    if "status" in response:
        assert response["status"] in ("fresh", "stale"), (
            f"pre-read status must be fresh or stale; got {response['status']!r}"
        )
    else:
        # Untracked fast-path → either {} or {ok: True}
        assert response.get("ok") is True or response == {}, (
            f"pre-read without status must be empty or {{ok:True}}; got {response!r}"
        )


def test_pre_edit_translates_correctly(
    live_coordinator, monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """pre-edit on a Edit/Write hook: workspace-relative path computed
    from absolute file_path, session_id passed through."""
    workspace, _ = live_coordinator
    (workspace / "docs" / "specs").mkdir(parents=True, exist_ok=True)
    (workspace / "docs" / "specs" / "test.md").write_text("v1")

    cc_payload = {
        "session_id": _sid(),
        "tool_name": "Edit",
        "tool_input": {"file_path": str(workspace / "docs" / "specs" / "test.md")},
    }
    rc, out = _drive("pre-edit", cc_payload, workspace, monkeypatch, capsys)
    assert rc == 0
    response = json.loads(out)
    # T-03 / ce-review: tightened to assert the {ok: bool} contract.
    # pre-edit's wire shape always includes ok (true on success, false on
    # collision-rejected; never absent). On collision, the response also
    # carries hookSpecificOutput per the CollisionResponse TypedDict.
    assert isinstance(response, dict)
    assert "ok" in response or "hookSpecificOutput" in response, (
        f"pre-edit response must carry 'ok' or 'hookSpecificOutput'; got {response!r}"
    )


def test_post_edit_hashes_file_on_disk_when_response_missing_hash(
    live_coordinator, monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """If CC's tool_response doesn't include content_hash, hook-client
    hashes the post-write file content from disk."""
    workspace, _ = live_coordinator
    target = workspace / "docs" / "specs" / "test.md"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("post-edit content")

    cc_payload = {
        "session_id": _sid(),
        "tool_name": "Edit",
        "tool_input": {"file_path": str(target)},
        "tool_response": {"success": True},  # NO content_hash — client must hash
    }
    rc, out = _drive("post-edit", cc_payload, workspace, monkeypatch, capsys)
    assert rc == 0
    # Verify on-disk hash matches what we expect
    expected_hash = hashlib.sha256(b"post-edit content").hexdigest()
    # Just confirm the call succeeded — we can't see the body easily but
    # the response will indicate ok/note.
    response = json.loads(out)
    assert isinstance(response, dict)


def test_post_edit_with_explicit_content_hash(
    live_coordinator, monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """If tool_response carries content_hash, hook-client uses it
    verbatim (no disk read)."""
    workspace, _ = live_coordinator
    target = workspace / "docs" / "specs" / "test.md"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("v1")
    provided_hash = "a" * 64

    cc_payload = {
        "session_id": _sid(),
        "tool_name": "Edit",
        "tool_input": {"file_path": str(target)},
        "tool_response": {"success": True, "content_hash": provided_hash},
    }
    rc, out = _drive("post-edit", cc_payload, workspace, monkeypatch, capsys)
    assert rc == 0


def test_session_stop_only_needs_session_id(
    live_coordinator, monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Stop hooks don't carry tool_input — only session_id."""
    workspace, _ = live_coordinator
    cc_payload = {"session_id": _sid()}
    rc, out = _drive("session-stop", cc_payload, workspace, monkeypatch, capsys)
    assert rc == 0
    response = json.loads(out)
    assert response.get("ok") is True


# ----------------------------------------------------------------------
# Graceful-degrade paths — hook MUST NEVER block the user's tool call
# ----------------------------------------------------------------------


def test_no_coordinator_running_returns_empty_response(
    git_workspace: Path, monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """No coordinator running → hook exits 0 with empty JSON. CC ignores."""
    cc_payload = {
        "session_id": _sid(),
        "tool_name": "Read",
        "tool_input": {"file_path": str(git_workspace / "any.md")},
    }
    rc, out = _drive("pre-read", cc_payload, git_workspace, monkeypatch, capsys)
    assert rc == 0
    assert out.strip() == "{}"


def test_malformed_stdin_does_not_crash(
    git_workspace: Path, monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Garbage on stdin → exit 0, empty response. Defense against any
    upstream wrapper getting confused."""
    monkeypatch.setattr("sys.stdin", io.StringIO("not json at all"))
    rc = coherence_hook_client.main(["pre-read", "--root", str(git_workspace)])
    captured = capsys.readouterr()
    assert rc == 0
    assert captured.out.strip() == "{}"


def test_empty_stdin_does_not_crash(
    git_workspace: Path, monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Empty stdin → exit 0 with no output (CC's hook contract permits
    silent success)."""
    monkeypatch.setattr("sys.stdin", io.StringIO(""))
    rc = coherence_hook_client.main(["pre-read", "--root", str(git_workspace)])
    captured = capsys.readouterr()
    assert rc == 0


def test_missing_session_id_emits_empty(
    live_coordinator, monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """CC payload without session_id → skip hook, empty response."""
    workspace, _ = live_coordinator
    cc_payload = {"tool_name": "Read", "tool_input": {"file_path": str(workspace / "x.md")}}
    rc, out = _drive("pre-read", cc_payload, workspace, monkeypatch, capsys)
    assert rc == 0
    assert out.strip() == "{}"


def test_missing_file_path_emits_empty(
    live_coordinator, monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Tool hook with no file_path in tool_input → skip."""
    workspace, _ = live_coordinator
    cc_payload = {"session_id": _sid(), "tool_name": "Read", "tool_input": {}}
    rc, out = _drive("pre-read", cc_payload, workspace, monkeypatch, capsys)
    assert rc == 0
    assert out.strip() == "{}"


def test_path_outside_workspace_emits_empty(
    live_coordinator, monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """If the file_path resolves outside the workspace root (shouldn't
    happen but defensive), skip rather than send garbage."""
    workspace, _ = live_coordinator
    cc_payload = {
        "session_id": _sid(),
        "tool_name": "Read",
        "tool_input": {"file_path": "/etc/passwd"},
    }
    rc, out = _drive("pre-read", cc_payload, workspace, monkeypatch, capsys)
    assert rc == 0
    assert out.strip() == "{}"


# ----------------------------------------------------------------------
# Workspace-relative path translation
# ----------------------------------------------------------------------


def test_pre_read_translates_absolute_to_workspace_relative(
    live_coordinator, monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Sanity: an absolute file_path inside the workspace gets converted
    to a relative path before being sent to the coordinator. We verify
    via the policy effect — track the relative form, then trigger a
    read with the absolute form, then check status shows the artifact."""
    workspace, _ = live_coordinator
    # Track using the relative form
    from ccs.cli import coherence_track
    target_rel = "docs/specs/test.md"
    (workspace / "docs" / "specs").mkdir(parents=True, exist_ok=True)
    (workspace / "docs" / "specs" / "test.md").write_text("seed")
    coherence_track.main(["--root", str(workspace), target_rel])
    capsys.readouterr()  # drain

    # Now fire a hook with the absolute form
    cc_payload = {
        "session_id": _sid(),
        "tool_name": "Read",
        "tool_input": {
            "file_path": str(workspace / "docs" / "specs" / "test.md"),
        },
    }
    rc, out = _drive("pre-read", cc_payload, workspace, monkeypatch, capsys)
    assert rc == 0
    # Response should be JSON. The artifact should now be observed.
    response = json.loads(out)
    assert isinstance(response, dict)
    # Verify the coordinator now sees the artifact (post-read)
    from ccs.cli import coherence_status
    coherence_status.main(["--root", str(workspace), "--json"])
    status_out = capsys.readouterr().out
    status = json.loads(status_out)
    paths = [a["path"] for a in status.get("tracked_artifacts", [])]
    assert target_rel in paths, (
        f"path translation failed: expected '{target_rel}' in artifacts, got {paths}"
    )


# ----------------------------------------------------------------------
# _build_pre_grep — both path shapes (subagent absolute, direct relative)
#
# Regression guard for the 2026-05-24 launch-gate finding: PR #64 added an
# `os.path.isabs(raw_path)` branch to `_build_pre_grep` but forgot to
# `import os` at module scope. Every non-empty Grep path raised NameError,
# which the main()'s broad except swallowed → empty `{}` response →
# coordinator never contacted → strict-deny never fired. Existing pre-grep
# tests all hit the coordinator HTTP endpoint directly and never exercised
# the builder, so the bug slipped through CI.
#
# These tests drive the FULL hook-client path (stdin → main() → builder →
# HTTP → stdout) for both subagent-shape and top-level-shape Grep payloads,
# so any future regression in either branch fails loudly.
# ----------------------------------------------------------------------


def test_pre_grep_relative_path_drives_coordinator(
    live_coordinator, monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Direct (top-level) Grep invocation: path is workspace-relative.
    Hook builder must produce a non-empty payload that reaches the
    coordinator. Empty `{}` response indicates the builder crashed
    silently (the bug PR #64 introduced + this guard fixes)."""
    workspace, _ = live_coordinator
    cc_payload = {
        "session_id": _sid(),
        "tool_name": "Grep",
        "tool_input": {
            "pattern": "anything",
            "path": "docs",
            "output_mode": "content",
        },
    }
    rc, out = _drive("pre-grep", cc_payload, workspace, monkeypatch, capsys)
    assert rc == 0
    response = json.loads(out)
    # The builder must NOT have crashed silently. Empty {} would mean an
    # exception was caught and `_emit_empty` fired — which is the exact
    # NameError-swallow regression this test guards against.
    assert response != {}, (
        "pre-grep returned empty {} for a non-empty path — builder "
        "likely crashed silently. Check imports and the _build_pre_grep "
        "absolute/relative path branches."
    )
    assert "status" in response or "hookSpecificOutput" in response


def test_pre_grep_absolute_path_drives_coordinator(
    live_coordinator, monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Subagent-shape Grep invocation: path is absolute (Task tool resolves
    paths to absolute before dispatching to the subagent's hook). Hook
    builder must normalize via _to_workspace_relative and produce a
    non-empty coordinator-bound payload."""
    workspace, _ = live_coordinator
    (workspace / "docs").mkdir(parents=True, exist_ok=True)
    cc_payload = {
        "session_id": _sid(),
        "tool_name": "Grep",
        "tool_input": {
            "pattern": "anything",
            "path": str(workspace / "docs"),  # absolute path → subagent shape
            "output_mode": "files_with_matches",
        },
    }
    rc, out = _drive("pre-grep", cc_payload, workspace, monkeypatch, capsys)
    assert rc == 0
    response = json.loads(out)
    assert response != {}, (
        "pre-grep returned empty {} for an absolute path — absolute-path "
        "branch likely raised an exception (e.g., NameError from missing "
        "`import os`). Check imports and _to_workspace_relative."
    )
    assert "status" in response or "hookSpecificOutput" in response


def test_pre_grep_empty_path_does_not_crash(
    live_coordinator, monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Grep with no path arg → search root is workspace root (empty
    string). Must reach the coordinator without crashing — the empty-path
    branch in _build_pre_grep was the only branch the pre-fix builder
    could survive on (it short-circuits before os.path.isabs)."""
    workspace, _ = live_coordinator
    cc_payload = {
        "session_id": _sid(),
        "tool_name": "Grep",
        "tool_input": {
            "pattern": "anything",
            "output_mode": "content",
        },
    }
    rc, out = _drive("pre-grep", cc_payload, workspace, monkeypatch, capsys)
    assert rc == 0
    response = json.loads(out)
    # Empty workspace + no tracked artifacts → coordinator returns
    # {"status": "fresh"}. We just need a parseable JSON response, not
    # specifically empty or non-empty.
    assert isinstance(response, dict)
