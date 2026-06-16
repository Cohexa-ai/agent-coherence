# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""End-to-end tests for the Claude Code adapter (Unit 8).

Codifies the AS-phpmac install walkthrough as automated tests. Covers
the scenarios from plan §Unit 8 that can be verified without a live
``claude`` CLI:

- Bootstrap permissions on first spawn (0700/.coherence + 0600/secret)
- Shared-secret Bearer auth (KTD-12) — 401 without, 200 with, 401 with wrong
- DNS-rebinding mitigation — non-127.0.0.1 Host header → 403
- KTD-13 — state.db schema has no `content` column on `artifacts` table
- Coordinator-down graceful degradation — kill the process, next hook no-ops
- Subprocess-spawn integration — full pipeline via real ``agent-coherence-*``
  binaries and a detached coordinator

Scenarios that require a live ``claude`` CLI session (multi-process
stale-read e2e, claude-agents probe) are exercised by the Phase E.0
probe procedure doc and re-verified manually on each Claude Code
minor-version bump.
"""

from __future__ import annotations

import json
import os
import signal
import sqlite3
import stat
import subprocess
import urllib.error
import urllib.request
import uuid
from pathlib import Path
from typing import Any

import pytest

from ccs.adapters.claude_code.lifecycle import (
    LifecycleConfig,
    ensure_coordinator,
    stop_coordinator,
)

# ----------------------------------------------------------------------
# Fixtures
# ----------------------------------------------------------------------


@pytest.fixture
def fast_cfg() -> LifecycleConfig:
    return LifecycleConfig(
        idle_shutdown_sec=0,
        sweep_interval_sec=0,
        port_file_retry_attempts=10,
        port_file_retry_interval_sec=0.05,
        connect_retry_attempts=10,
        connect_retry_interval_sec=0.05,
        spawn_self_probe_attempts=30,
    )


@pytest.fixture
def workspace(tmp_path: Path) -> Path:
    (tmp_path / ".git").mkdir()
    return tmp_path


@pytest.fixture
def live(workspace: Path, fast_cfg: LifecycleConfig):
    """In-process spawned coordinator. Yields (workspace, port, secret)."""
    port = ensure_coordinator(workspace, config=fast_cfg)
    assert port > 0
    secret = (workspace / ".coherence" / "hook.secret").read_text().strip()
    yield workspace, port, secret
    stop_coordinator(workspace)


# ----------------------------------------------------------------------
# Bootstrap permissions (plan §Unit 8 happy-path #7)
# ----------------------------------------------------------------------


def test_bootstrap_creates_coherence_dir_with_0700(
    live,
) -> None:
    """First-spawn creates .coherence/ with mode 0700 (owner rwx only)."""
    workspace, _, _ = live
    coh_dir = workspace / ".coherence"
    assert coh_dir.is_dir()
    mode = stat.S_IMODE(coh_dir.stat().st_mode)
    assert mode == 0o700, f"expected 0700, got {oct(mode)}"


def test_bootstrap_creates_secret_with_0600(live) -> None:
    """hook.secret is mode 0600 per KTD-12 — only owner can read."""
    workspace, _, _ = live
    secret_file = workspace / ".coherence" / "hook.secret"
    assert secret_file.is_file()
    mode = stat.S_IMODE(secret_file.stat().st_mode)
    assert mode == 0o600, f"expected 0600, got {oct(mode)}"


def test_bootstrap_creates_state_db(live) -> None:
    """state.db must exist after first spawn."""
    workspace, _, _ = live
    assert (workspace / ".coherence" / "state.db").is_file()


def test_bootstrap_creates_gitignore_with_wildcard(live) -> None:
    """KTD-13: .coherence/.gitignore must contain ``*`` so a careless
    ``git add .`` doesn't accidentally commit state.db (containing
    MESI state + agent UUIDs) or hook.secret (a credential). The README
    claims these are 'auto-gitignored' — this verifies the implementation."""
    workspace, _, _ = live
    gitignore = workspace / ".coherence" / ".gitignore"
    assert gitignore.is_file(), (
        ".coherence/.gitignore missing — `git add .` would commit state.db + secret"
    )
    assert "*" in gitignore.read_text()


def test_bootstrap_gitignore_not_clobbered_on_respawn(workspace: Path, fast_cfg: LifecycleConfig) -> None:
    """If an operator customized .coherence/.gitignore, a respawn must NOT
    overwrite their changes (the implementation only writes when missing)."""
    coh_dir = workspace / ".coherence"
    coh_dir.mkdir(mode=0o700)
    custom_content = "# operator-customized\n*\n# end of custom\n"
    (coh_dir / ".gitignore").write_text(custom_content)
    port = ensure_coordinator(workspace, config=fast_cfg)
    try:
        assert port > 0
        assert (coh_dir / ".gitignore").read_text() == custom_content
    finally:
        stop_coordinator(workspace)


def test_bootstrap_creates_server_pid_with_pid_and_port(live) -> None:
    """server.pid carries <pid>\\n<port>\\n in that order."""
    workspace, port, _ = live
    pid_file = workspace / ".coherence" / "server.pid"
    lines = pid_file.read_text().splitlines()
    assert len(lines) >= 2
    assert int(lines[0]) == os.getpid()
    assert int(lines[1]) == port


# ----------------------------------------------------------------------
# Shared-secret Bearer auth (KTD-12, plan §Unit 8 happy-path #4)
# ----------------------------------------------------------------------


def _request(
    port: int,
    path: str,
    bearer: str | None,
    body: dict[str, Any] | None = None,
    host: str = "127.0.0.1",
) -> tuple[int, str]:
    """Make a raw authenticated request. Returns (status_code, body_text)."""
    headers: dict[str, str] = {"Host": host}
    if bearer is not None:
        headers["Authorization"] = f"Bearer {bearer}"
    data = None
    if body is not None:
        headers["Content-Type"] = "application/json"
        data = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(
        f"http://127.0.0.1:{port}{path}",
        method="POST" if body is not None else "GET",
        headers=headers,
        data=data,
    )
    try:
        with urllib.request.urlopen(req, timeout=3) as resp:
            return resp.status, resp.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read().decode("utf-8")


def test_auth_rejects_request_without_bearer(live) -> None:
    """No Authorization header → 401."""
    _, port, _ = live
    status, body = _request(port, "/status", bearer=None)
    assert status == 401, f"expected 401, got {status}: {body}"


def test_auth_rejects_request_with_wrong_bearer(live) -> None:
    """Wrong token → 401."""
    _, port, _ = live
    status, body = _request(port, "/status", bearer="wrong-token-here")
    assert status == 401


def test_auth_accepts_request_with_correct_bearer(live) -> None:
    """Correct token → 200 + valid JSON status. R12 (Unit 6) + P1 #7
    revision: minimal tier surfaces ``coordinator_pid`` (pid is public
    on POSIX; operators rely on it) but the absolute workspace root
    stays sentinel'd ("."). The full tier (gated behind
    ``Coherence-Local-Operator: true``) is what exposes the absolute
    root."""
    _, port, secret = live
    status, body = _request(port, "/status", bearer=secret)
    assert status == 200
    payload = json.loads(body)
    assert payload["detail"] == "minimal"
    assert "tracked_artifacts" in payload
    # P1 #7: pid is in the minimal tier.
    assert payload["coordinator_pid"] == os.getpid()
    # Absolute root still sentinel'd at this tier.
    assert payload["coordinator_root"] == "."


# ----------------------------------------------------------------------
# DNS-rebinding mitigation (plan §Unit 8 integration security #1)
# ----------------------------------------------------------------------


def test_dns_rebind_request_with_attacker_host_rejected(live) -> None:
    """Host header check: anything other than 127.0.0.1 / localhost → 403.
    Defends against DNS-rebinding attacks where attacker.example.com
    resolves to 127.0.0.1 and a victim's browser makes credentialed
    requests against our local coordinator."""
    _, port, secret = live
    status, body = _request(
        port, "/status", bearer=secret, host="attacker.example.com",
    )
    assert status == 403, f"expected 403, got {status}: {body}"


def test_dns_rebind_localhost_alias_accepted(live) -> None:
    """`localhost` should still be accepted (legitimate alias for 127.0.0.1)."""
    _, port, secret = live
    status, _ = _request(
        port, "/status", bearer=secret, host="localhost",
    )
    assert status == 200


# ----------------------------------------------------------------------
# KTD-13: state.db must NOT have a content column on artifacts (plan
# §Unit 8 integration security #2 — disclosure surface defense)
# ----------------------------------------------------------------------


def test_state_db_artifacts_has_no_content_column(live) -> None:
    """KTD-13: storing raw file content in state.db expands the disclosure
    surface if .coherence/ leaks (e.g. accidentally committed). The
    schema must store only content_hash, never the bytes."""
    workspace, _, _ = live
    db_path = workspace / ".coherence" / "state.db"
    conn = sqlite3.connect(str(db_path))
    try:
        cols = conn.execute("PRAGMA table_info(artifacts)").fetchall()
        col_names = {c[1] for c in cols}
    finally:
        conn.close()
    assert "content" not in col_names, (
        f"KTD-13 violation: artifacts table has 'content' column. "
        f"Columns: {col_names}"
    )
    # And content_hash MUST be present (that's what we DO store)
    assert "content_hash" in col_names


# ----------------------------------------------------------------------
# Coordinator-down graceful degradation (plan §Unit 8 edge case #6)
# ----------------------------------------------------------------------


def test_coordinator_killed_mid_session_hook_client_no_ops(
    workspace: Path, fast_cfg: LifecycleConfig,
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    """If the coordinator is killed between two hook events, the next
    hook-client invocation must exit 0 with empty JSON (CC's hook
    contract permits silent success). The user's tool call proceeds."""
    import io
    port = ensure_coordinator(workspace, config=fast_cfg)
    assert port > 0
    pid = int((workspace / ".coherence" / "server.pid").read_text().splitlines()[0])

    # Kill the coordinator. In-process coordinator is this same pid — we can't
    # actually kill ourselves. Instead: shut down via stop_coordinator (which
    # is the in-process equivalent) and confirm hook-client degrades.
    stop_coordinator(workspace)
    # And remove the port from the pid file (simulating crash)
    (workspace / ".coherence" / "server.pid").write_text(f"{pid}\n")

    from ccs.cli import coherence_hook_client
    cc_payload = {
        "session_id": str(uuid.uuid4()),
        "tool_name": "Read",
        "tool_input": {"file_path": str(workspace / "any.md")},
    }
    monkeypatch.setattr("sys.stdin", io.StringIO(json.dumps(cc_payload)))
    rc = coherence_hook_client.main(["pre-read", "--root", str(workspace)])
    captured = capsys.readouterr()
    assert rc == 0
    assert captured.out.strip() == "{}"


# ----------------------------------------------------------------------
# Subprocess-spawn integration — what the AS-phpmac walkthrough does
# without a live claude CLI. Verifies the full agent-coherence-* binary
# pipeline as a stranger would invoke it.
# ----------------------------------------------------------------------


def _venv_bin(name: str) -> str:
    """Resolve a console script from this repo's venv."""
    repo_root = Path(__file__).parent.parent
    candidate = repo_root / ".venv" / "bin" / name
    if candidate.is_file():
        return str(candidate)
    # Fallback to PATH lookup (in CI the venv is set up differently)
    import shutil
    found = shutil.which(name)
    assert found is not None, f"binary not on PATH: {name}"
    return found


def test_e2e_subprocess_spawn_then_status(workspace: Path) -> None:
    """The full AS-phpmac chain via real subprocess invocations:
    1. `agent-coherence-coordinator` spawns a detached coordinator
    2. `agent-coherence-track` adds a path to the policy
    3. `agent-coherence-status` shows the policy state

    This is the regression test for the manual smoke that surfaced the
    daemon-thread bug (8015f80) — must run real subprocesses to exercise
    the parent-exits-but-coordinator-survives path."""
    coordinator_bin = _venv_bin("agent-coherence-coordinator")
    status_bin = _venv_bin("agent-coherence-status")
    track_bin = _venv_bin("agent-coherence-track")

    # Step 1: spawn
    spawn = subprocess.run(
        [coordinator_bin, "--root", str(workspace)],
        capture_output=True, text=True, timeout=30,
    )
    assert spawn.returncode == 0, f"spawn failed: {spawn.stderr}"
    assert spawn.stdout.startswith("port="), spawn.stdout
    port = int(spawn.stdout.strip().split("=")[1])

    try:
        # Step 2: track a path
        (workspace / "docs" / "specs").mkdir(parents=True, exist_ok=True)
        (workspace / "docs" / "specs" / "test.md").write_text("v1")
        track = subprocess.run(
            [track_bin, "--root", str(workspace), "docs/specs/test.md"],
            capture_output=True, text=True, timeout=10,
        )
        assert track.returncode == 0, f"track failed: {track.stderr}"

        # Step 3: status
        status = subprocess.run(
            [status_bin, "--root", str(workspace), "--json"],
            capture_output=True, text=True, timeout=10,
        )
        assert status.returncode == 0
        payload = json.loads(status.stdout)
        assert "policy_summary" in payload
        assert payload["policy_summary"]["user_added_pattern_count"] >= 1
    finally:
        # Cleanup — kill the detached coordinator process
        pid_file = workspace / ".coherence" / "server.pid"
        if pid_file.is_file():
            try:
                detached_pid = int(pid_file.read_text().splitlines()[0])
                os.kill(detached_pid, signal.SIGTERM)
            except (ValueError, ProcessLookupError, IndexError):
                pass


def test_e2e_subprocess_idempotent_second_spawn(workspace: Path) -> None:
    """Second `agent-coherence-coordinator` invocation must short-circuit
    to the existing port via the live-probe check, not re-fork."""
    coordinator_bin = _venv_bin("agent-coherence-coordinator")
    first = subprocess.run(
        [coordinator_bin, "--root", str(workspace)],
        capture_output=True, text=True, timeout=30,
    )
    assert first.returncode == 0
    port_1 = int(first.stdout.strip().split("=")[1])

    try:
        second = subprocess.run(
            [coordinator_bin, "--root", str(workspace)],
            capture_output=True, text=True, timeout=10,
        )
        assert second.returncode == 0
        port_2 = int(second.stdout.strip().split("=")[1])
        assert port_2 == port_1, "second spawn should reuse existing port"
    finally:
        pid_file = workspace / ".coherence" / "server.pid"
        if pid_file.is_file():
            try:
                detached_pid = int(pid_file.read_text().splitlines()[0])
                os.kill(detached_pid, signal.SIGTERM)
            except (ValueError, ProcessLookupError, IndexError):
                pass
