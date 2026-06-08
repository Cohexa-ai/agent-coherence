# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""Integration tests for v0.2 strict-mode handler decision-flip (plan Unit 2).

Covers (per plan Unit 2 Test scenarios):

- Happy path: strict + tracked + stale on each of the 4 PreToolUse handlers
  (Read, Edit/Write via pre-edit, Bash, Grep) returns ``permissionDecision:
  "deny"`` with the static reason text.
- Negative: strict + tracked + FRESH → allow (no stale-read).
- Negative: tracked + NOT strict + stale → warn (v0.1.1 behavior unchanged).
- Negative: NOT tracked + stale → allow passthrough (fast-path).
- Edge: Bash multi-path where ONLY one path is strict → deny (any strict
  match triggers).
- Edge: static deny reason byte-stable across N retries (KTD-T; H1
  falsification regression guard).
- KTD-U structural invariant: parameterized over allow-emitting call sites;
  each call site refuses to convert a TERMINAL_DENIAL_CLASSES-member input
  to allow.
- KTD-U coverage meta-test: the parameter list covers every call site of
  ``emit_allow`` in ``coordinator_server.py`` + ``hook_payloads.py``
  (static grep + count check).
"""

from __future__ import annotations

import hashlib
import json
import re
import time
import uuid
from pathlib import Path
from typing import Optional
from urllib import error as urlerror
from urllib import request as urlrequest

import pytest

from ccs.adapters.claude_code.auth import load_secret
from ccs.adapters.claude_code.coordinator_server import CoordinatorHTTPServer
from ccs.adapters.claude_code.hook_payloads import (
    STRICT_MODE_DENY_REASON_TEMPLATE,
    TERMINAL_DENIAL_CLASSES,
    emit_allow,
)

# ----------------------------------------------------------------------
# Test plumbing — mirrors tests/test_claude_code_coordinator_server.py
# ----------------------------------------------------------------------


_TEST_SESSION_NS = uuid.UUID("11111111-1111-4111-8111-111111111111")


def _sid(label: str) -> str:
    return str(uuid.uuid5(_TEST_SESSION_NS, f"strict-mode-test:{label}"))


def _hash(label: str) -> str:
    return hashlib.sha256(label.encode()).hexdigest()


class _Client:
    """Tiny urllib client mirroring test_claude_code_coordinator_server._Client."""

    def __init__(self, host: str, port: int, secret: str) -> None:
        self.base = f"http://{host}:{port}"
        self.headers = {
            "Authorization": f"Bearer {secret}",
            "Host": "127.0.0.1",
            "Content-Type": "application/json",
        }

    def request(
        self,
        method: str,
        path: str,
        body: Optional[dict] = None,
    ) -> tuple[int, dict]:
        url = self.base + path
        data = json.dumps(body).encode("utf-8") if body is not None else b""
        req = urlrequest.Request(
            url,
            data=data if method == "POST" else None,
            method=method,
            headers=self.headers,
        )
        try:
            with urlrequest.urlopen(req, timeout=10) as resp:
                return resp.status, json.loads(resp.read().decode("utf-8") or "{}")
        except urlerror.HTTPError as e:
            return e.code, json.loads(e.read().decode("utf-8") or "{}")

    def post(self, path: str, body: dict) -> tuple[int, dict]:
        return self.request("POST", path, body)

    def get(self, path: str) -> tuple[int, dict]:
        return self.request("GET", path)


def _write_policy(workspace: Path, *, tracked: list[str], strict: list[str]) -> None:
    """Materialize tracked.yaml + strict_mode.yaml under .coherence/."""
    coherence_dir = workspace / ".coherence"
    coherence_dir.mkdir(exist_ok=True, mode=0o700)
    if tracked:
        (coherence_dir / "tracked.yaml").write_text(
            "\n".join(f"- {p}" for p in tracked) + "\n"
        )
    if strict:
        (coherence_dir / "strict_mode.yaml").write_text(
            "\n".join(f"- {p}" for p in strict) + "\n"
        )


@pytest.fixture
def workspace(tmp_path: Path) -> Path:
    """A fresh workspace per test."""
    return tmp_path


@pytest.fixture
def strict_coordinator(workspace: Path):
    """Coordinator with CLAUDE.md tracked + strict, plan.md tracked + strict,
    docs/plans/x.md tracked + warn-mode (NOT strict). Matches the most common
    test scenario shape across this file."""
    _write_policy(
        workspace,
        tracked=["CLAUDE.md", "plan.md", "docs/plans/x.md"],
        strict=["CLAUDE.md", "plan.md"],
    )
    server = CoordinatorHTTPServer(workspace, port=0, instance_id="strict-mode-test")
    server.serve_in_thread()
    time.sleep(0.05)
    try:
        yield server
    finally:
        server.shutdown()


@pytest.fixture
def strict_client(strict_coordinator) -> _Client:
    secret = load_secret(strict_coordinator.coordinator_root)
    assert secret is not None
    return _Client("127.0.0.1", strict_coordinator.port, secret)


# ----------------------------------------------------------------------
# Happy path — strict deny on each handler (4 surfaces)
# ----------------------------------------------------------------------


def _setup_stale(client: _Client, path: str) -> None:
    """Set up the canonical stale scenario: sessions A and B both read path
    (each takes SHARED), B edits + commits (acquires EXCLUSIVE then MODIFIED,
    invalidates A), leaving A in INVALID state on path. The caller's NEXT
    operation on path from session A is the stale event under test.

    Both sessions must pre-read first because v0.2 strict-mode pre-edit
    requires the editor to have a fresh grant — an editor without prior
    read on a strict-mode artifact gets strict-deny (correct behavior;
    matches the operator's intent "agent MUST re-read before edit")."""
    client.post("/hooks/pre-read",
                {"session_id": _sid("A"), "path": path, "content_hash": _hash("v1")})
    client.post("/hooks/pre-read",
                {"session_id": _sid("B"), "path": path, "content_hash": _hash("v1")})
    client.post("/hooks/pre-edit",
                {"session_id": _sid("B"), "path": path})
    client.post("/hooks/post-edit",
                {"session_id": _sid("B"), "path": path,
                 "content_hash": _hash("v2"), "success": True})


def test_pre_read_strict_tracked_stale_denies(strict_client: _Client) -> None:
    """Read on a strict + tracked + stale artifact returns deny + static reason."""
    _setup_stale(strict_client, "CLAUDE.md")
    status, body = strict_client.post(
        "/hooks/pre-read",
        {"session_id": _sid("A"), "path": "CLAUDE.md", "content_hash": _hash("v1")},
    )
    assert status == 200
    out = body["hookSpecificOutput"]
    assert out["permissionDecision"] == "deny"
    assert out["hookEventName"] == "PreToolUse"
    assert "permissionDecisionReason" in out
    reason = out["permissionDecisionReason"]
    assert "CLAUDE.md" in reason
    assert "Re-read" in reason


def test_pre_edit_strict_tracked_stale_denies(strict_client: _Client) -> None:
    """Edit on a strict + tracked + stale artifact returns deny (the Edit/Write
    surface — pre-edit handles both per the hooks.json matcher Edit|Write)."""
    _setup_stale(strict_client, "plan.md")
    # Session A tries to edit plan.md without re-reading.
    status, body = strict_client.post(
        "/hooks/pre-edit",
        {"session_id": _sid("A"), "path": "plan.md"},
    )
    assert status == 200
    out = body["hookSpecificOutput"]
    assert out["permissionDecision"] == "deny"
    assert out["hookEventName"] == "PreToolUse"
    assert "plan.md" in out["permissionDecisionReason"]


def test_pre_bash_strict_tracked_stale_denies(strict_client: _Client) -> None:
    """Bash command that reads a strict + tracked + stale artifact denies."""
    _setup_stale(strict_client, "plan.md")
    status, body = strict_client.post(
        "/hooks/pre-bash",
        {"session_id": _sid("A"), "command": "cat plan.md"},
    )
    assert status == 200
    out = body["hookSpecificOutput"]
    assert out["permissionDecision"] == "deny"
    assert "plan.md" in out["permissionDecisionReason"]


def test_pre_grep_strict_tracked_stale_denies(strict_client: _Client) -> None:
    """Grep over a directory containing a strict + tracked + stale artifact
    denies the whole grep command."""
    _setup_stale(strict_client, "plan.md")
    status, body = strict_client.post(
        "/hooks/pre-grep",
        {"session_id": _sid("A"), "search_root": ""},
    )
    assert status == 200
    out = body["hookSpecificOutput"]
    assert out["permissionDecision"] == "deny"
    assert "plan.md" in out["permissionDecisionReason"]


# ----------------------------------------------------------------------
# Negative paths — preserve v0.1.1 warn-mode behavior for non-strict
# ----------------------------------------------------------------------


def test_pre_read_strict_tracked_fresh_allows(strict_client: _Client) -> None:
    """Strict + tracked + FRESH (no stale event) returns the fresh allow shape
    — no deny when the artifact is up to date for this session."""
    status, body = strict_client.post(
        "/hooks/pre-read",
        {"session_id": _sid("A"), "path": "CLAUDE.md", "content_hash": _hash("v1")},
    )
    assert status == 200
    # First observation seeds SHARED → fresh response, no hookSpecificOutput.
    # Unit 6: the fresh response additively carries the seeded version.
    assert body == {"status": "fresh", "version": 1}


def test_pre_read_tracked_not_strict_stale_returns_warn(strict_client: _Client) -> None:
    """Tracked + NOT strict + stale returns warn-mode allow (v0.1.1 behavior
    preserved for warn-mode artifacts even when strict_mode is configured for
    other artifacts in the same workspace)."""
    # docs/plans/x.md is tracked but NOT in strict_mode_paths.
    _setup_stale(strict_client, "docs/plans/x.md")
    status, body = strict_client.post(
        "/hooks/pre-read",
        {"session_id": _sid("A"), "path": "docs/plans/x.md", "content_hash": _hash("v1")},
    )
    assert status == 200
    out = body["hookSpecificOutput"]
    assert out["permissionDecision"] == "allow"
    assert "Stale read" in out["additionalContext"]


def test_pre_read_untracked_path_strict_irrelevant(strict_client: _Client) -> None:
    """Untracked path takes the policy fast-path; strict mode never applies."""
    status, body = strict_client.post(
        "/hooks/pre-read",
        {"session_id": _sid("A"), "path": "untracked.txt"},
    )
    assert status == 200
    assert body == {"status": "fresh"}


# ----------------------------------------------------------------------
# Edge cases
# ----------------------------------------------------------------------


def test_pre_bash_multi_path_one_strict_one_warn_denies(strict_client: _Client) -> None:
    """`cat plan.md docs/plans/x.md` — plan.md is strict, docs/plans/x.md is
    warn-only. ANY strict-stale match triggers deny for the whole command."""
    _setup_stale(strict_client, "plan.md")           # strict
    _setup_stale(strict_client, "docs/plans/x.md")  # warn-only
    status, body = strict_client.post(
        "/hooks/pre-bash",
        {"session_id": _sid("A"), "command": "cat plan.md docs/plans/x.md"},
    )
    assert status == 200
    out = body["hookSpecificOutput"]
    assert out["permissionDecision"] == "deny"
    assert "plan.md" in out["permissionDecisionReason"]


def test_pre_bash_only_warn_paths_still_allows(strict_client: _Client) -> None:
    """If a Bash command touches only warn-mode tracked artifacts (none in
    strict), the v0.1.1 warn-mode allow shape is preserved."""
    _setup_stale(strict_client, "docs/plans/x.md")
    status, body = strict_client.post(
        "/hooks/pre-bash",
        {"session_id": _sid("A"), "command": "cat docs/plans/x.md"},
    )
    assert status == 200
    assert body["hookSpecificOutput"]["permissionDecision"] == "allow"


def test_strict_deny_reason_byte_stable_across_retries(strict_client: _Client) -> None:
    """KTD-T (H1 falsification regression guard): the deny reason MUST be
    byte-identical across N retries of the same (session, path) staleness
    event. Per-invocation timestamp variation would re-introduce the opus
    prompt-injection retry hazard the Phase 0 falsifiability experiment
    surfaced."""
    _setup_stale(strict_client, "CLAUDE.md")
    reasons: list[str] = []
    for _ in range(5):
        _, body = strict_client.post(
            "/hooks/pre-read",
            {"session_id": _sid("A"), "path": "CLAUDE.md", "content_hash": _hash("v1")},
        )
        reasons.append(body["hookSpecificOutput"]["permissionDecisionReason"])
    assert len(set(reasons)) == 1, (
        f"Deny reason rotated across retries — KTD-P invariant violated. "
        f"Unique reasons: {set(reasons)}"
    )


# ----------------------------------------------------------------------
# KTD-U structural invariant: emit_allow refuses terminal-denial conversion
# ----------------------------------------------------------------------


# Parameter list: ALL call sites of emit_allow in coordinator_server.py +
# hook_payloads.py. The meta-test below grep-counts emit_allow calls in those
# files and asserts this list has the same length. Adding a new emit_allow
# call site MUST extend this parameter list — that's the structural guarantee
# Unit 2's KTD-U design provides.
ALLOW_EMISSION_SOURCES: list[str] = [
    "stale_response_builder",        # hook_payloads.build_stale_response
    "collision_response_builder",    # hook_payloads.build_collision_response
    "pre_read_fresh_with_notice",    # coordinator_server._handle_pre_read
    "pre_edit_notice_only",          # coordinator_server._handle_pre_edit
    "pre_bash_stale_warn",           # coordinator_server._handle_pre_bash
    "pre_grep_stale_warn",           # coordinator_server._handle_pre_grep
]


@pytest.mark.parametrize("source", ALLOW_EMISSION_SOURCES)
def test_emit_allow_refuses_terminal_denial_class(source: str) -> None:
    """KTD-U invariant (structural, not behavioral): for every allow-emitting
    call site, ``emit_allow`` with a TERMINAL_DENIAL_CLASSES-member denial_class
    raises AssertionError. A future contributor cannot satisfy the test
    trivially by adding a new allow path — they must extend the parameter
    list and therefore think about the invariant."""
    terminal_class = next(iter(TERMINAL_DENIAL_CLASSES))
    with pytest.raises(AssertionError, match="TERMINAL_DENIAL_CLASSES"):
        emit_allow(source=source, denial_class=terminal_class)


def test_emit_allow_passes_for_non_terminal_class() -> None:
    """Sanity: emit_allow returns the allow envelope when denial_class is
    None or not in the terminal set."""
    out = emit_allow(source="sanity_test", additional_context="hello")
    assert out["permissionDecision"] == "allow"
    assert out["hookEventName"] == "PreToolUse"
    assert out["additionalContext"] == "hello"


def test_terminal_denial_classes_includes_strict_mode_deny() -> None:
    """The strict-mode deny class is in TERMINAL_DENIAL_CLASSES. This is the
    security marker the deny path uses."""
    assert "permissions_deny_strict_mode" in TERMINAL_DENIAL_CLASSES


# ----------------------------------------------------------------------
# KTD-U coverage meta-test: parameter list covers every emit_allow call
# ----------------------------------------------------------------------


_REPO_ROOT = Path(__file__).resolve().parents[2]
_SCANNED_FILES = (
    _REPO_ROOT / "src" / "ccs" / "adapters" / "claude_code" / "coordinator_server.py",
    _REPO_ROOT / "src" / "ccs" / "adapters" / "claude_code" / "hook_payloads.py",
)


def _count_emit_allow_call_sites(path: Path) -> int:
    """AST-based call-site counter. Matches only real ``emit_allow(...)``
    Call nodes — not docstring mentions, error-message strings, or the
    function definition itself."""
    import ast

    tree = ast.parse(path.read_text())
    count = 0
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            func = node.func
            if isinstance(func, ast.Name) and func.id == "emit_allow":
                count += 1
            elif isinstance(func, ast.Attribute) and func.attr == "emit_allow":
                count += 1
    return count


def test_ktd_u_emit_allow_call_sites_covered_by_parameter_list() -> None:
    """AST scan: total real ``emit_allow(...)`` call sites across
    coordinator_server.py + hook_payloads.py must equal
    ``len(ALLOW_EMISSION_SOURCES)``. Adding a new emit_allow call site
    requires extending the parameter list — that's the structural guarantee
    that future code reviews catch the invariant discussion before the new
    site can land."""
    call_count = sum(_count_emit_allow_call_sites(p) for p in _SCANNED_FILES)
    assert call_count == len(ALLOW_EMISSION_SOURCES), (
        f"emit_allow() call sites in scanned files: {call_count}; "
        f"ALLOW_EMISSION_SOURCES parameter list: {len(ALLOW_EMISSION_SOURCES)}. "
        f"Extend ALLOW_EMISSION_SOURCES or audit the new call site for KTD-U "
        f"invariant compliance."
    )


# ----------------------------------------------------------------------
# Static reason template format string sanity
# ----------------------------------------------------------------------


def test_strict_mode_deny_reason_template_is_static() -> None:
    """KTD-P (static deny text, NO template rotation). The template is a
    module-level constant; this test guards against accidental
    timestamp-of-rendering interpolation that would violate byte-stability."""
    expected_placeholders = {"path", "last_writer_short", "last_writer_ts_iso"}
    # Extract format-string placeholders.
    actual = set(
        m.group(1) for m in re.finditer(r"\{([a-z_]+)\}", STRICT_MODE_DENY_REASON_TEMPLATE)
    )
    assert actual == expected_placeholders, (
        f"STRICT_MODE_DENY_REASON_TEMPLATE placeholders changed: "
        f"expected {expected_placeholders}, got {actual}. Adding a "
        f"per-invocation field (e.g., warning_generated_at) violates "
        f"KTD-P byte-stability."
    )
