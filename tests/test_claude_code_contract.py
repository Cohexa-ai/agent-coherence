# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""Hook-payload contract test for the Claude Code adapter (Unit 8).

CI early-warning system for Claude Code version drift. The fixtures
in ``tests/fixtures/cc_hook_stdin/`` are verbatim stdin payloads
recorded from a live ``claude`` v2.1.131 session. This test asserts
that ``agent-coherence-hook-client`` parses each fixture without
errors and produces the coordinator-bound payload shape the
``coordinator_server`` endpoints expect.

When Claude Code ships a payload-shape change (renamed field, removed
field, type change), this test fails — flagging drift before users
hit it in production.

To re-record after a v2.1.x bump:
1. Replace fixtures with newly captured ones (see fixtures README for
   the capture procedure)
2. Re-run this test; failures indicate the parser needs updates
3. Update ``coherence_hook_client.py`` + ``coordinator_server.py``
   payload contracts to match new shapes
4. Commit fixture + parser updates together so the CI gate stays meaningful
"""

from __future__ import annotations

import io
import json
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

FIXTURE_DIR = Path(__file__).parent / "fixtures" / "cc_hook_stdin"


def _load_fixture(name: str) -> dict[str, Any]:
    return json.loads((FIXTURE_DIR / name).read_text())


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
def live_coordinator(tmp_path: Path, fast_cfg: LifecycleConfig):
    """Spawn a real coordinator so the contract test exercises the full
    parse → translate → POST → respond path."""
    (tmp_path / ".git").mkdir()
    port = ensure_coordinator(tmp_path, config=fast_cfg)
    assert port > 0
    yield tmp_path, port
    stop_coordinator(tmp_path)


def _drive(
    subcommand: str,
    cc_payload: dict[str, Any],
    workspace: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> tuple[int, dict[str, Any]]:
    """Feed a CC fixture to hook-client and return (exit_code, parsed_response)."""
    # The fixture's `cwd` doesn't exist on this machine — rewrite the
    # file_path / cwd to be inside our test workspace so the path-
    # translation step doesn't reject as "outside workspace".
    if "tool_input" in cc_payload and "file_path" in cc_payload["tool_input"]:
        cc_payload = {**cc_payload, "tool_input": {
            **cc_payload["tool_input"],
            "file_path": str(workspace / "docs" / "specs" / "test.md"),
        }}
    if "tool_response" in cc_payload and "filePath" in cc_payload.get("tool_response", {}):
        cc_payload = {**cc_payload, "tool_response": {
            **cc_payload["tool_response"],
            "filePath": str(workspace / "docs" / "specs" / "test.md"),
        }}
    cc_payload = {**cc_payload, "cwd": str(workspace),
                  "session_id": str(uuid.uuid4())}  # fresh UUID per test
    monkeypatch.setattr("sys.stdin", io.StringIO(json.dumps(cc_payload)))
    rc = coherence_hook_client.main([subcommand, "--root", str(workspace)])
    captured = capsys.readouterr()
    try:
        response = json.loads(captured.out) if captured.out.strip() else {}
    except json.JSONDecodeError:
        response = {"_raw_stdout": captured.out}
    return rc, response


# ----------------------------------------------------------------------
# Shape assertions on the fixtures themselves (sanity — flags shape drift
# in the fixture file before the parser even runs)
# ----------------------------------------------------------------------


@pytest.mark.parametrize("fixture_name,required_fields", [
    ("pre_read.json", {"session_id", "tool_name", "tool_input", "hook_event_name"}),
    ("pre_edit.json", {"session_id", "tool_name", "tool_input", "hook_event_name"}),
    ("post_read.json", {"session_id", "tool_name", "tool_input", "tool_response"}),
    ("post_edit.json", {"session_id", "tool_name", "tool_input", "tool_response"}),
    ("session_start.json", {"session_id", "hook_event_name"}),
    ("stop.json", {"session_id", "hook_event_name"}),
])
def test_fixture_has_required_fields(
    fixture_name: str, required_fields: set
) -> None:
    """If CC ever stops sending a required field, this test fails before
    the parser test does — gives a clearer signal."""
    fixture = _load_fixture(fixture_name)
    missing = required_fields - set(fixture.keys())
    assert not missing, f"{fixture_name} missing required fields: {missing}"


@pytest.mark.parametrize("fixture_name", ["pre_read.json", "pre_edit.json", "post_read.json", "post_edit.json"])
def test_tool_input_has_file_path(fixture_name: str) -> None:
    """Tool hooks must carry tool_input.file_path so we can translate to
    a workspace-relative path."""
    fixture = _load_fixture(fixture_name)
    assert "file_path" in fixture["tool_input"]
    assert isinstance(fixture["tool_input"]["file_path"], str)
    assert fixture["tool_input"]["file_path"]


def test_post_edit_tool_response_uses_camelcase() -> None:
    """Observed in v2.1.131: tool_response uses camelCase (filePath,
    oldString, newString) while the outer payload uses snake_case. Our
    parser must not assume snake_case inside tool_response. If CC ever
    normalizes the casing, this test fails — and our parser still works
    because we don't read from tool_response (we hash the file on disk)."""
    fixture = _load_fixture("post_edit.json")
    response = fixture["tool_response"]
    # The actual contract — these specific fields are what we observed
    assert "filePath" in response, (
        "tool_response.filePath missing — CC may have normalized to snake_case"
    )


# ----------------------------------------------------------------------
# End-to-end contract: fixture → hook-client → coordinator → response
# ----------------------------------------------------------------------


def test_pre_read_fixture_parses_and_calls_coordinator(
    live_coordinator, monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    workspace, _ = live_coordinator
    (workspace / "docs" / "specs").mkdir(parents=True, exist_ok=True)
    (workspace / "docs" / "specs" / "test.md").write_text("seed")
    fixture = _load_fixture("pre_read.json")
    rc, response = _drive("pre-read", fixture, workspace, monkeypatch, capsys)
    assert rc == 0, f"hook-client exited non-zero: {response}"
    # Untracked path → fresh response (acceptable). Tracked path → stale or
    # fresh with hookSpecificOutput. Either way, the parse path didn't error.
    assert isinstance(response, dict)


def test_pre_edit_fixture_parses_and_calls_coordinator(
    live_coordinator, monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    workspace, _ = live_coordinator
    (workspace / "docs" / "specs").mkdir(parents=True, exist_ok=True)
    (workspace / "docs" / "specs" / "test.md").write_text("seed")
    fixture = _load_fixture("pre_edit.json")
    rc, response = _drive("pre-edit", fixture, workspace, monkeypatch, capsys)
    assert rc == 0, f"hook-client exited non-zero: {response}"
    assert isinstance(response, dict)


def test_post_edit_fixture_parses_and_calls_coordinator(
    live_coordinator, monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Critical contract: post_edit fixture has NO `success` field in
    tool_response — we observed CC's contract uses `userModified` and
    `structuredPatch` instead. Our hook-client defaults success=True
    when the field is missing, which matches the observed semantics
    (PostToolUse only fires when the edit actually applied)."""
    workspace, _ = live_coordinator
    target = workspace / "docs" / "specs" / "test.md"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("seed")
    fixture = _load_fixture("post_edit.json")
    rc, response = _drive("post-edit", fixture, workspace, monkeypatch, capsys)
    assert rc == 0
    assert isinstance(response, dict)


def test_session_stop_fixture_parses_and_calls_coordinator(
    live_coordinator, monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Stop fixture has stop_hook_active, last_assistant_message —
    fields we don't read. Verify the parser doesn't choke on the
    extra fields."""
    workspace, _ = live_coordinator
    fixture = _load_fixture("stop.json")
    rc, response = _drive("session-stop", fixture, workspace, monkeypatch, capsys)
    assert rc == 0
    assert response.get("ok") is True


# ----------------------------------------------------------------------
# Defensive: any future field additions must not break the parser
# ----------------------------------------------------------------------


def test_parser_tolerates_unknown_fields(
    live_coordinator, monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Forward-compat: if CC adds new fields to the hook payload, our
    parser must ignore them, not crash."""
    workspace, _ = live_coordinator
    (workspace / "docs" / "specs").mkdir(parents=True, exist_ok=True)
    (workspace / "docs" / "specs" / "test.md").write_text("seed")
    fixture = _load_fixture("pre_read.json")
    fixture["_future_field_we_dont_know_about"] = "value"
    fixture["tool_input"]["_another_one"] = ["a", "b", "c"]
    rc, response = _drive("pre-read", fixture, workspace, monkeypatch, capsys)
    assert rc == 0
