# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""Tests for the coordinator HTTP server (plan Unit 4).

Covers the seven endpoint contracts + auth + Host check + watchdog +
per-invocation warning-template variation.
"""

from __future__ import annotations

import hashlib
import json
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Optional
from urllib import error as urlerror
from urllib import request as urlrequest

import pytest

from ccs.adapters.claude_code.auth import load_secret
from ccs.adapters.claude_code.coordinator_server import (
    CoordinatorHTTPServer,
    HANDLER_TIMEOUT_SEC,
    MAX_POLICY_PATHS_PER_REQUEST,
    session_to_agent_id,
)
from ccs.adapters.claude_code import hook_payloads as _payloads
from ccs.core.states import MESIState


# Test helper: deterministic UUID4-shaped strings for short test labels.
# Sessions now must be UUIDs (A3 validation); tests use this to keep label
# semantics while satisfying the wire-contract validator.
_TEST_SESSION_NS = uuid.UUID("11111111-1111-4111-8111-111111111111")


def _sid(label: str) -> str:
    return str(uuid.uuid5(_TEST_SESSION_NS, f"test-session:{label}"))


# Realistic sha-256 hex strings for tests (A8 requires 64-hex content_hash).
def _hash(label: str) -> str:
    return hashlib.sha256(label.encode()).hexdigest()


# ----------------------------------------------------------------------
# Test client
# ----------------------------------------------------------------------


class _Client:
    """Tiny urllib-based client. Returns (status, body_dict)."""

    def __init__(self, host: str, port: int, secret: str) -> None:
        self.base = f"http://{host}:{port}"
        self.headers = {
            "Authorization": f"Bearer {secret}",
            "Host": "127.0.0.1",  # explicit — we want to assert behavior
            "Content-Type": "application/json",
        }

    def request(
        self,
        method: str,
        path: str,
        body: Optional[dict] = None,
        *,
        headers_override: Optional[dict] = None,
    ) -> tuple[int, dict]:
        url = self.base + path
        data = json.dumps(body).encode("utf-8") if body is not None else b""
        headers = dict(self.headers)
        if headers_override:
            headers.update(headers_override)
        req = urlrequest.Request(url, data=data if method == "POST" else None,
                                 method=method, headers=headers)
        try:
            with urlrequest.urlopen(req, timeout=10) as resp:
                return resp.status, json.loads(resp.read().decode("utf-8") or "{}")
        except urlerror.HTTPError as e:
            return e.code, json.loads(e.read().decode("utf-8") or "{}")

    def post(self, path: str, body: dict, **kw) -> tuple[int, dict]:
        return self.request("POST", path, body, **kw)

    def get(self, path: str, **kw) -> tuple[int, dict]:
        return self.request("GET", path, **kw)


@pytest.fixture
def coordinator(tmp_path: Path):
    """A live coordinator on a random port, with secret extracted for tests."""
    server = CoordinatorHTTPServer(tmp_path, port=0, instance_id="test-instance")
    server.serve_in_thread()
    # Small delay so server is accepting before tests fire.
    time.sleep(0.05)
    try:
        yield server
    finally:
        server.shutdown()


@pytest.fixture
def client(coordinator) -> _Client:
    secret = load_secret(coordinator.coordinator_root)
    assert secret is not None
    return _Client("127.0.0.1", coordinator.port, secret)


# ----------------------------------------------------------------------
# Auth + Host check (KTD-12)
# ----------------------------------------------------------------------


def test_missing_authorization_returns_401(coordinator) -> None:
    url = f"http://127.0.0.1:{coordinator.port}/hooks/pre-read"
    req = urlrequest.Request(url, data=b"{}", method="POST",
                             headers={"Host": "127.0.0.1", "Content-Type": "application/json"})
    try:
        urlrequest.urlopen(req, timeout=5)
        assert False, "expected 401"
    except urlerror.HTTPError as e:
        assert e.code == 401


def test_wrong_bearer_returns_401(coordinator) -> None:
    url = f"http://127.0.0.1:{coordinator.port}/hooks/pre-read"
    req = urlrequest.Request(url, data=b"{}", method="POST",
                             headers={
                                 "Authorization": "Bearer not-the-secret",
                                 "Host": "127.0.0.1",
                                 "Content-Type": "application/json",
                             })
    try:
        urlrequest.urlopen(req, timeout=5)
        assert False, "expected 401"
    except urlerror.HTTPError as e:
        assert e.code == 401


def test_bad_host_returns_403(client: _Client) -> None:
    """DNS-rebind mitigation: a request whose Host header is attacker.com
    must be rejected even if the Bearer is valid."""
    status, body = client.post("/hooks/pre-read", {"session_id": _sid("s1"), "path": "CLAUDE.md"},
                                headers_override={"Host": "attacker.example.com"})
    assert status == 403
    assert "host" in body["error"].lower()


def test_localhost_host_accepted(coordinator) -> None:
    """Host: localhost (not just 127.0.0.1) must also be accepted."""
    secret = load_secret(coordinator.coordinator_root)
    url = f"http://127.0.0.1:{coordinator.port}/hooks/pre-read"
    body = json.dumps({"session_id": _sid("s1"), "path": "CLAUDE.md"}).encode("utf-8")
    req = urlrequest.Request(
        url, data=body, method="POST",
        headers={
            "Authorization": f"Bearer {secret}",
            "Host": "localhost",
            "Content-Type": "application/json",
        },
    )
    with urlrequest.urlopen(req, timeout=5) as resp:
        assert resp.status == 200


# ----------------------------------------------------------------------
# Routing
# ----------------------------------------------------------------------


def test_unknown_route_returns_404(client: _Client) -> None:
    status, body = client.post("/does-not-exist", {})
    assert status == 404


def test_get_on_post_route_returns_404(client: _Client) -> None:
    """/hooks/pre-read is POST-only; GET should 404."""
    status, body = client.get("/hooks/pre-read")
    assert status == 404


# ----------------------------------------------------------------------
# /hooks/pre-read
# ----------------------------------------------------------------------


def test_pre_read_first_observation_returns_fresh(client: _Client) -> None:
    """KTD-9: first observation of a tracked artifact seeds v1 + grants
    SHARED + returns fresh."""
    status, body = client.post("/hooks/pre-read",
                                {"session_id": _sid("s1"), "path": "CLAUDE.md",
                                 "content_hash": _hash("abc")})
    assert status == 200
    assert body == {"status": "fresh"}


def test_pre_read_repeat_from_same_session_stays_fresh(client: _Client) -> None:
    """A second read from the same session on the same artifact is fresh."""
    client.post("/hooks/pre-read", {"session_id": _sid("s1"), "path": "CLAUDE.md", "content_hash": _hash("h1")})
    status, body = client.post("/hooks/pre-read",
                                {"session_id": _sid("s1"), "path": "CLAUDE.md", "content_hash": _hash("h1")})
    assert status == 200
    assert body == {"status": "fresh"}


def test_pre_read_after_peer_write_returns_stale(client: _Client) -> None:
    """Two sessions: A reads, B writes, A's next read returns stale."""
    # Session A first-reads to seed v1 + take SHARED.
    client.post("/hooks/pre-read", {"session_id": _sid("A"), "path": "plan.md", "content_hash": _hash("h1")})
    # Session B pre-edits (acquires E, invalidates A) and post-edits (commits v2).
    client.post("/hooks/pre-edit", {"session_id": _sid("B"), "path": "plan.md"})
    client.post("/hooks/post-edit",
                {"session_id": _sid("B"), "path": "plan.md", "content_hash": _hash("h2"), "success": True})
    # Session A's next read now sees stale.
    status, body = client.post("/hooks/pre-read",
                                {"session_id": _sid("A"), "path": "plan.md", "content_hash": _hash("h1")})
    assert status == 200
    assert "hookSpecificOutput" in body
    out = body["hookSpecificOutput"]
    assert out["hookEventName"] == "PreToolUse"
    assert out["permissionDecision"] == "allow"  # v0.1 WARN, NEVER deny
    assert "Stale read" in out["additionalContext"]
    assert "plan.md" in out["additionalContext"]
    # Summary metadata present and respects KTD-12 no-content constraint.
    summary = body["summary"]
    assert summary["path"] == "plan.md"
    assert summary["current_version"] == 2
    assert summary["hash_differs"] is True
    # No raw content / no hash bytes in the prose
    assert "h1" not in out["additionalContext"]
    assert "h2" not in out["additionalContext"]


def test_pre_read_warn_mode_never_returns_deny(client: _Client) -> None:
    """Belt-and-suspenders invariant: v0.1 pre-read MUST NOT return deny."""
    client.post("/hooks/pre-read", {"session_id": _sid("A"), "path": "plan.md", "content_hash": _hash("h1")})
    client.post("/hooks/pre-edit", {"session_id": _sid("B"), "path": "plan.md"})
    client.post("/hooks/post-edit",
                {"session_id": _sid("B"), "path": "plan.md", "content_hash": _hash("h2"), "success": True})
    status, body = client.post("/hooks/pre-read",
                                {"session_id": _sid("A"), "path": "plan.md", "content_hash": _hash("h1")})
    assert body["hookSpecificOutput"]["permissionDecision"] != "deny"


def test_pre_read_untracked_path_fastpath(coordinator, client: _Client) -> None:
    """An untracked path returns fresh WITHOUT touching SQLite (R8)."""
    artifact_count_before = len(coordinator.registry.artifact_ids())
    status, body = client.post("/hooks/pre-read",
                                {"session_id": _sid("A"), "path": "src/random.py",
                                 "content_hash": _hash("h1")})
    assert body == {"status": "fresh"}
    artifact_count_after = len(coordinator.registry.artifact_ids())
    assert artifact_count_after == artifact_count_before, (
        "untracked path must not create an artifact row"
    )


def test_pre_read_missing_session_id_400(client: _Client) -> None:
    status, body = client.post("/hooks/pre-read", {"path": "CLAUDE.md"})
    assert status == 400
    assert "session_id" in body["error"]


def test_pre_read_empty_path_400(client: _Client) -> None:
    status, body = client.post("/hooks/pre-read", {"session_id": _sid("s"), "path": ""})
    assert status == 400


# ----------------------------------------------------------------------
# /hooks/pre-edit + post-edit (KTD-1 cycle)
# ----------------------------------------------------------------------


def test_full_edit_cycle(coordinator, client: _Client) -> None:
    """pre-edit acquires E → post-edit commits + bumps version."""
    client.post("/hooks/pre-read", {"session_id": _sid("A"), "path": "plan.md", "content_hash": _hash("h0")})
    s, b = client.post("/hooks/pre-edit", {"session_id": _sid("A"), "path": "plan.md"})
    assert s == 200 and b == {"ok": True}
    s, b = client.post("/hooks/post-edit",
                        {"session_id": _sid("A"), "path": "plan.md",
                         "content_hash": _hash("h1"), "success": True})
    assert s == 200 and b == {"ok": True}
    # Version bumped
    artifact_id = coordinator.registry.lookup_artifact_id_by_name("plan.md")
    art = coordinator.registry.get_artifact(artifact_id)
    assert art.version == 2  # seeded at 1, committed once → 2


def test_failed_edit_releases_grant_without_bump(coordinator, client: _Client) -> None:
    """KTD-1 release-on-failure: post-edit success:false releases E without bumping."""
    client.post("/hooks/pre-read", {"session_id": _sid("A"), "path": "plan.md"})
    client.post("/hooks/pre-edit", {"session_id": _sid("A"), "path": "plan.md"})
    artifact_id = coordinator.registry.lookup_artifact_id_by_name("plan.md")
    version_before = coordinator.registry.get_artifact(artifact_id).version
    s, b = client.post("/hooks/post-edit",
                        {"session_id": _sid("A"), "path": "plan.md",
                         "content_hash": _hash("ignored"), "success": False})
    assert s == 200
    assert b.get("released") is True
    # Version NOT bumped on failure
    version_after = coordinator.registry.get_artifact(artifact_id).version
    assert version_after == version_before
    # Agent state is no longer EXCLUSIVE (some non-M/E state)
    agent_id = session_to_agent_id("A")
    state = coordinator.registry.get_agent_state(artifact_id, agent_id)
    assert state not in (MESIState.EXCLUSIVE, MESIState.MODIFIED)
    # Another session can now acquire immediately
    s2, b2 = client.post("/hooks/pre-edit", {"session_id": _sid("B"), "path": "plan.md"})
    assert s2 == 200 and b2.get("ok") is True


def test_collision_surfaces_via_additional_context(coordinator, client: _Client) -> None:
    """KTD-9 same-hash-blindness mitigation: when another session holds E,
    pre-edit returns hookSpecificOutput with collision warning."""
    # Session A holds E
    a_sid = _sid("A")
    client.post("/hooks/pre-edit", {"session_id": a_sid, "path": "plan.md"})
    # Session B attempts edit → collision response
    status, body = client.post("/hooks/pre-edit", {"session_id": _sid("B"), "path": "plan.md"})
    assert status == 200
    assert body.get("collision") is True
    out = body["hookSpecificOutput"]
    assert out["permissionDecision"] == "allow"  # v0.1 warn only
    assert "Concurrent edit detected" in out["additionalContext"]
    assert "plan.md" in out["additionalContext"]
    # The collision msg contains the holder's short session id (first 8 chars of A's UUID)
    assert a_sid[:8] in out["additionalContext"]


# ----------------------------------------------------------------------
# /hooks/session-stop (KTD-11)
# ----------------------------------------------------------------------


def test_session_stop_releases_uncommitted_grants(coordinator, client: _Client) -> None:
    """KTD-11: end-of-turn Stop releases any uncommitted EXCLUSIVE grants."""
    client.post("/hooks/pre-edit", {"session_id": _sid("A"), "path": "plan.md"})
    client.post("/hooks/pre-edit", {"session_id": _sid("A"), "path": "spec.md"})
    # Stop fires
    s, b = client.post("/hooks/session-stop", {"session_id": _sid("A")})
    assert s == 200 and b["ok"] is True
    released = set(b["released_artifacts"])
    assert released == {"plan.md", "spec.md"}
    # Neither artifact is held in M∪E by A anymore
    agent_id = session_to_agent_id("A")
    for path in ("plan.md", "spec.md"):
        art_id = coordinator.registry.lookup_artifact_id_by_name(path)
        state = coordinator.registry.get_agent_state(art_id, agent_id)
        assert state not in (MESIState.EXCLUSIVE, MESIState.MODIFIED)


def test_session_stop_idempotent(client: _Client) -> None:
    """Calling Stop twice in a row is safe — second call returns empty release list."""
    client.post("/hooks/pre-edit", {"session_id": _sid("A"), "path": "plan.md"})
    s1, b1 = client.post("/hooks/session-stop", {"session_id": _sid("A")})
    assert s1 == 200 and "plan.md" in b1["released_artifacts"]
    s2, b2 = client.post("/hooks/session-stop", {"session_id": _sid("A")})
    assert s2 == 200 and b2["released_artifacts"] == []


# ----------------------------------------------------------------------
# /policy/track + /policy/untrack
# ----------------------------------------------------------------------


def test_policy_track_persists_to_yaml(coordinator, client: _Client) -> None:
    s, b = client.post("/policy/track", {"paths": ["runbook.md", "architecture.md"]})
    assert s == 200
    assert b["ok"] is True
    assert sorted(b["added"]) == ["architecture.md", "runbook.md"]
    yaml_path = coordinator.coordinator_root / ".coherence" / "tracked.yaml"
    assert yaml_path.is_file()
    content = yaml_path.read_text()
    assert "runbook.md" in content
    assert "architecture.md" in content
    # Live policy now matches runbook.md (untracked-by-default earlier)
    assert coordinator.policy.is_tracked("runbook.md")


def test_policy_track_rejects_traversal(client: _Client) -> None:
    s, b = client.post("/policy/track",
                        {"paths": ["../../.env", "/etc/passwd", "runbook.md"]})
    assert s == 200
    assert b["added"] == ["runbook.md"]
    rejected = {r["path"] for r in b["rejected"]}
    assert "../../.env" in rejected
    assert "/etc/passwd" in rejected


def test_policy_track_cap_enforced(client: _Client) -> None:
    too_many = [f"f{i}.md" for i in range(MAX_POLICY_PATHS_PER_REQUEST + 1)]
    s, b = client.post("/policy/track", {"paths": too_many})
    assert s == 400
    assert "max" in b["error"].lower()


def test_policy_untrack_persists_to_ignored_yaml(coordinator, client: _Client) -> None:
    s, b = client.post("/policy/untrack", {"paths": ["docs/brainstorms/draft.md"]})
    assert s == 200
    assert b["removed"] == ["docs/brainstorms/draft.md"]
    yaml_path = coordinator.coordinator_root / ".coherence" / "ignored.yaml"
    assert yaml_path.is_file()
    assert "docs/brainstorms/draft.md" in yaml_path.read_text()
    # Default-matching draft now ignored
    assert not coordinator.policy.is_tracked("docs/brainstorms/draft.md")


# ----------------------------------------------------------------------
# /status
# ----------------------------------------------------------------------


def test_status_includes_tracked_artifacts_and_sessions(client: _Client) -> None:
    a_sid = _sid("A")
    client.post("/hooks/pre-read", {"session_id": a_sid, "path": "plan.md"})
    client.post("/hooks/pre-edit", {"session_id": a_sid, "path": "spec.md"})
    s, b = client.get("/status")
    assert s == 200
    tracked_paths = {a["path"] for a in b["tracked_artifacts"]}
    assert "plan.md" in tracked_paths
    assert "spec.md" in tracked_paths
    sessions = {sess["agent_name"] for sess in b["sessions"]}
    assert f"claude-session-{a_sid}" in sessions
    assert b["coordinator_uptime_s"] > 0
    assert isinstance(b["coordinator_pid"], int)
    assert "policy_summary" in b


# ----------------------------------------------------------------------
# Malformed input
# ----------------------------------------------------------------------


def test_malformed_json_body_returns_400(coordinator) -> None:
    secret = load_secret(coordinator.coordinator_root)
    url = f"http://127.0.0.1:{coordinator.port}/hooks/pre-read"
    req = urlrequest.Request(
        url, data=b"{not json", method="POST",
        headers={
            "Authorization": f"Bearer {secret}",
            "Host": "127.0.0.1",
            "Content-Type": "application/json",
        },
    )
    try:
        urlrequest.urlopen(req, timeout=5)
        assert False, "expected 400"
    except urlerror.HTTPError as e:
        assert e.code == 400


def test_body_not_object_returns_400(client: _Client) -> None:
    # JSON list at top level, not object
    url = f"http://127.0.0.1:{client.base.rsplit(':', 1)[1]}/hooks/pre-read"  # rebuild
    secret = client.headers["Authorization"][len("Bearer "):]
    req = urlrequest.Request(
        url, data=b"[1,2,3]", method="POST",
        headers={
            "Authorization": f"Bearer {secret}",
            "Host": "127.0.0.1",
            "Content-Type": "application/json",
        },
    )
    try:
        urlrequest.urlopen(req, timeout=5)
        assert False, "expected 400"
    except urlerror.HTTPError as e:
        assert e.code == 400


# ----------------------------------------------------------------------
# Heartbeat invariant
# ----------------------------------------------------------------------


def test_every_endpoint_records_heartbeat(coordinator, client: _Client) -> None:
    """KTD-2: every hook POST records the calling session's heartbeat."""
    hb_sid = _sid("hb-session")
    agent_id = session_to_agent_id(hb_sid)
    assert coordinator.registry.last_heartbeat_tick(agent_id) is None
    client.post("/hooks/pre-read",
                {"session_id": hb_sid, "path": "CLAUDE.md"})
    after_pre_read = coordinator.registry.last_heartbeat_tick(agent_id)
    assert after_pre_read is not None
    time.sleep(1.1)  # ensure monotonic tick advances
    client.post("/hooks/pre-edit", {"session_id": hb_sid, "path": "CLAUDE.md"})
    after_pre_edit = coordinator.registry.last_heartbeat_tick(agent_id)
    assert after_pre_edit >= after_pre_read


# ----------------------------------------------------------------------
# Per-invocation variation in warning templates (strict-mode future-proofing)
# ----------------------------------------------------------------------


def test_stale_warnings_vary_per_invocation(client: _Client) -> None:
    """Two back-to-back stale-read responses for the same artifact must
    have DIFFERENT additionalContext text (timestamp varies). This is the
    strict-mode-future-proofing constraint — when v0.2 flips allow → deny,
    the varying reason structurally prevents the §13.5 retry loop."""
    client.post("/hooks/pre-read", {"session_id": _sid("A"), "path": "plan.md"})
    client.post("/hooks/pre-edit", {"session_id": _sid("B"), "path": "plan.md"})
    client.post("/hooks/post-edit",
                {"session_id": _sid("B"), "path": "plan.md",
                 "content_hash": _hash("h2"), "success": True})

    _, body1 = client.post("/hooks/pre-read",
                           {"session_id": _sid("A"), "path": "plan.md"})
    time.sleep(0.05)  # ensure clock advances
    # Invalidate A again so the second response is also stale
    client.post("/hooks/pre-edit", {"session_id": _sid("B"), "path": "plan.md"})
    client.post("/hooks/post-edit",
                {"session_id": _sid("B"), "path": "plan.md",
                 "content_hash": _hash("h3"), "success": True})
    _, body2 = client.post("/hooks/pre-read",
                           {"session_id": _sid("A"), "path": "plan.md"})

    msg1 = body1["hookSpecificOutput"]["additionalContext"]
    msg2 = body2["hookSpecificOutput"]["additionalContext"]
    # Same shape, different text — the version delta differs at minimum.
    assert msg1 != msg2, "warning templates must vary per invocation for v0.2 strict-mode safety"


# ----------------------------------------------------------------------
# Concurrency
# ----------------------------------------------------------------------


# ----------------------------------------------------------------------
# A1 — Preemption notice (silent-grant-revocation surfacing)
# ----------------------------------------------------------------------


def test_a1_preemption_surfaces_on_victim_next_pre_read(client: _Client) -> None:
    """A1 load-bearing test (canonical phpmac scenario).

    Sequence: X pre-edits (holds E) → Y pre-edits (silently invalidates X) →
    X's next pre-read MUST surface a preemption notice naming Y + the
    artifact + when. Without this, X never learns its grant was revoked
    and X's content silently fails to land in the coordinator's view."""
    x = _sid("X"); y = _sid("Y")
    # X acquires EXCLUSIVE
    s, _ = client.post("/hooks/pre-edit", {"session_id": x, "path": "plan.md"})
    assert s == 200
    # Y preempts X (Y now holds E, X is INVALID, X received NO notification)
    s, _ = client.post("/hooks/pre-edit", {"session_id": y, "path": "plan.md"})
    assert s == 200
    # X's NEXT pre-read MUST surface the preemption via hookSpecificOutput
    status, body = client.post("/hooks/pre-read", {"session_id": x, "path": "plan.md"})
    assert status == 200, body
    assert "hookSpecificOutput" in body, (
        "X's next hook MUST inject the preemption notice into additionalContext; "
        f"got {body}"
    )
    out = body["hookSpecificOutput"]
    msg = out["additionalContext"]
    msg_lower = msg.lower()
    assert any(word in msg_lower for word in ("preempted", "revoked", "acquired by")), (
        f"prose should name the preemption explicitly; got: {msg}"
    )
    assert "plan.md" in msg
    assert y[:8] in msg, f"prose should name the preempter session prefix; got: {msg}"


def test_a1_preemption_surfaces_on_victim_next_pre_edit(client: _Client) -> None:
    """A1: surface preemption even when X's next hook is pre-edit, not pre-read."""
    x = _sid("X"); y = _sid("Y")
    client.post("/hooks/pre-edit", {"session_id": x, "path": "plan.md"})
    client.post("/hooks/pre-edit", {"session_id": y, "path": "plan.md"})
    status, body = client.post("/hooks/pre-edit", {"session_id": x, "path": "plan.md"})
    assert status == 200
    # pre-edit response shape: either {ok: true} OR hookSpecificOutput for collision/preemption
    assert "hookSpecificOutput" in body, (
        "pre-edit after being preempted MUST inject the notice; got {}"
    ).format(body)
    msg = body["hookSpecificOutput"]["additionalContext"]
    msg_lower = msg.lower()
    assert any(w in msg_lower for w in ("preempted", "revoked")), (
        f"prose should name the preemption; got: {msg}"
    )


def test_a1_preemption_surfaces_in_post_edit_failure_reason(client: _Client) -> None:
    """A1: when X tries to post-edit after being silently preempted, the
    failure response MUST name the preempter (not just generic CoherenceError)."""
    x = _sid("X"); y = _sid("Y")
    client.post("/hooks/pre-edit", {"session_id": x, "path": "plan.md"})
    client.post("/hooks/pre-edit", {"session_id": y, "path": "plan.md"})  # preempts X
    s, body = client.post("/hooks/post-edit",
                           {"session_id": x, "path": "plan.md",
                            "content_hash": _hash("h"), "success": True})
    assert s == 200
    assert body.get("ok") is False, f"post-edit on preempted grant must fail; got {body}"
    reason = body.get("reason", "")
    reason_lower = reason.lower()
    assert any(w in reason_lower for w in ("preempted", "revoked", "acquired by")), (
        f"failure reason must name the preemption; got: {reason}"
    )
    assert y[:8] in reason, f"reason should name preempter session prefix; got: {reason}"


def test_a1_preemption_notice_consumed_after_one_surface(client: _Client) -> None:
    """A1: preemption notices are pop-and-clear — the victim sees the notice
    on their NEXT hook, but a subsequent hook (without a fresh preemption)
    sees fresh/normal response."""
    x = _sid("X"); y = _sid("Y")
    client.post("/hooks/pre-edit", {"session_id": x, "path": "plan.md"})
    client.post("/hooks/pre-edit", {"session_id": y, "path": "plan.md"})
    # First X hook after preemption: surfaces notice
    _, body1 = client.post("/hooks/pre-read", {"session_id": x, "path": "plan.md"})
    assert "hookSpecificOutput" in body1
    # Second X hook: notice is consumed; response is normal stale-read shape
    # (X is still INVALID on plan.md, so this will be stale, but NOT carry the
    # preemption notice text anymore — that was popped).
    _, body2 = client.post("/hooks/pre-read", {"session_id": x, "path": "plan.md"})
    if "hookSpecificOutput" in body2:
        msg2 = body2["hookSpecificOutput"]["additionalContext"]
        # The second message can carry a stale-read warning, but should NOT
        # repeat the preemption notice text.
        assert "preempted" not in msg2.lower() and "revoked" not in msg2.lower(), (
            f"preemption notice should be consumed after first surface; got: {msg2}"
        )


def test_a1_no_preemption_no_notice(client: _Client) -> None:
    """A1 negative: a session that's never been preempted gets no notice."""
    x = _sid("X")
    # X never preempted — pre-edit just works
    s, body = client.post("/hooks/pre-edit", {"session_id": x, "path": "plan.md"})
    assert s == 200
    if "hookSpecificOutput" in body:
        msg = body["hookSpecificOutput"]["additionalContext"]
        assert "preempted" not in msg.lower() and "revoked" not in msg.lower()


# ----------------------------------------------------------------------
# Boundary validators (A2 + A3 + A8 — adversarial review hardening)
# ----------------------------------------------------------------------


@pytest.mark.parametrize("bad_session", [
    "A",            # not UUID
    "12345",        # not UUID
    "11111111-1111-4111-8111",  # too short
    "11111111-1111-4111-8111-1111111111111",  # too long
    "ggggggg1-1111-4111-8111-111111111111",   # non-hex
    "",
    None,
    123,
])
def test_invalid_session_id_returns_400(client: _Client, bad_session: Any) -> None:
    """A3: every handler rejects non-UUID session_id with 400."""
    status, body = client.post("/hooks/pre-read",
                                {"session_id": bad_session, "path": "CLAUDE.md"})
    assert status == 400


@pytest.mark.parametrize("bad_path", [
    "/etc/passwd",                  # absolute
    "../../.env",                   # traversal
    "subdir/../../etc/passwd",      # traversal mid-string
    "plan.md\n[SYSTEM] inject",     # newline injection (Adv #11)
    "plan.md\rrogue",               # carriage return
    "plan.md\x1b[31mred",            # ANSI escape
    "plan.md\x00null",               # null byte
    "x" * 2000,                      # over MAX_PATH_LEN
])
def test_invalid_path_returns_400(client: _Client, bad_path: str) -> None:
    """A2: every handler rejects invalid paths with 400."""
    status, body = client.post("/hooks/pre-read",
                                {"session_id": _sid("s"), "path": bad_path})
    assert status == 400


def test_pre_edit_rejects_invalid_path(client: _Client) -> None:
    status, body = client.post("/hooks/pre-edit",
                                {"session_id": _sid("s"), "path": "/etc/passwd"})
    assert status == 400


def test_post_edit_requires_valid_content_hash_on_success(client: _Client) -> None:
    """A8: post-edit with success:true MUST have a valid 64-hex content_hash."""
    s, _ = client.post("/hooks/pre-edit", {"session_id": _sid("Q"), "path": "plan.md"})
    assert s == 200
    # Missing content_hash → 400
    status, body = client.post("/hooks/post-edit",
                                {"session_id": _sid("Q"), "path": "plan.md", "success": True})
    assert status == 400
    # Malformed content_hash → 400
    status, body = client.post("/hooks/post-edit",
                                {"session_id": _sid("Q"), "path": "plan.md",
                                 "content_hash": "lol-not-a-hash", "success": True})
    assert status == 400
    # Empty content_hash → 400
    status, body = client.post("/hooks/post-edit",
                                {"session_id": _sid("Q"), "path": "plan.md",
                                 "content_hash": "", "success": True})
    assert status == 400


def test_post_edit_allows_missing_hash_on_failure(client: _Client) -> None:
    """A8: post-edit with success:false does NOT require content_hash —
    the release path doesn't use the hash."""
    client.post("/hooks/pre-edit", {"session_id": _sid("F"), "path": "plan.md"})
    status, body = client.post("/hooks/post-edit",
                                {"session_id": _sid("F"), "path": "plan.md", "success": False})
    assert status == 200
    assert body.get("released") is True


def test_pre_read_rejects_malformed_content_hash(client: _Client) -> None:
    """A8: content_hash is optional on pre-read but if present must be 64 hex."""
    status, body = client.post("/hooks/pre-read",
                                {"session_id": _sid("X"), "path": "plan.md",
                                 "content_hash": "garbage"})
    assert status == 400


def test_pre_read_allows_missing_content_hash(client: _Client) -> None:
    """A8: missing content_hash is permitted on pre-read."""
    status, body = client.post("/hooks/pre-read",
                                {"session_id": _sid("X"), "path": "plan.md"})
    assert status == 200


def test_secret_file_mode_is_0600_atomically(tmp_path: Path) -> None:
    """Bonus 1: secret is created with mode 0600 atomically (O_CREAT|O_EXCL),
    no mode-0644 window between write and chmod. Exercises ensure_secret
    directly to avoid the HTTPServer.shutdown deadlock when serve_in_thread
    was never called."""
    import os, stat
    from ccs.adapters.claude_code.auth import ensure_secret

    token = ensure_secret(tmp_path)
    assert token
    assert len(token) == 64  # 32-byte hex
    secret_file = tmp_path / ".coherence" / "hook.secret"
    assert secret_file.is_file()
    mode = stat.S_IMODE(os.stat(secret_file).st_mode)
    assert mode == 0o600, f"expected 0o600, got {oct(mode)}"
    # Idempotent on second call: returns same token, no exception.
    assert ensure_secret(tmp_path) == token


def test_warning_includes_warning_generated_at_field(client: _Client) -> None:
    """A5: stale-read summary includes both last_writer_at_unix_ts (real
    write tick from registry) AND warning_generated_at_unix_ts (handler now())."""
    a = _sid("A"); b = _sid("B")
    client.post("/hooks/pre-read", {"session_id": a, "path": "plan.md"})
    client.post("/hooks/pre-edit", {"session_id": b, "path": "plan.md"})
    client.post("/hooks/post-edit", {"session_id": b, "path": "plan.md",
                                       "content_hash": _hash("h2"), "success": True})
    _, body = client.post("/hooks/pre-read", {"session_id": a, "path": "plan.md"})
    summary = body["summary"]
    assert "last_writer_at_unix_ts" in summary
    assert "warning_generated_at_unix_ts" in summary
    # Generated-at is AFTER writer-at (handler runs after commit)
    assert summary["warning_generated_at_unix_ts"] >= summary["last_writer_at_unix_ts"]


def test_first_observation_prose_distinguishes_from_invalidated(client: _Client) -> None:
    """F1: first-observation warning prose says 'first time your session has
    observed' rather than 'you haven't read this version yet' (which falsely
    implies the session was previously behind)."""
    # Seed plan.md via session A at v1
    a = _sid("A")
    client.post("/hooks/pre-read", {"session_id": a, "path": "plan.md"})
    # Session B has never seen plan.md — first-pre-read returns stale because
    # B has no prior agent_state. Prose must reflect "first observation" not "previously behind".
    b = _sid("B")
    # Make plan.md v2 first so B's first observation is genuinely stale
    client.post("/hooks/pre-edit", {"session_id": a, "path": "plan.md"})
    client.post("/hooks/post-edit", {"session_id": a, "path": "plan.md",
                                       "content_hash": _hash("h1"), "success": True})
    _, body = client.post("/hooks/pre-read", {"session_id": b, "path": "plan.md"})
    if "hookSpecificOutput" in body:
        msg = body["hookSpecificOutput"]["additionalContext"]
        assert "first time your session has observed" in msg, (
            f"prose should distinguish first-observation from invalidation; got: {msg}"
        )
        # And NOT the old misleading framing
        assert "you haven't read this version yet" not in msg


def test_status_no_deadlock_under_concurrent_registration(client: _Client) -> None:
    """A4: /status iteration won't raise 'dict changed size during iteration'
    when concurrent pre-reads from new sessions are firing."""
    stop = threading.Event()
    errors: list[Exception] = []

    def churner() -> None:
        i = 0
        while not stop.is_set():
            try:
                client.post("/hooks/pre-read",
                            {"session_id": _sid(f"churn-{i}"), "path": "plan.md"})
            except Exception as e:
                errors.append(e)
            i += 1

    threads = [threading.Thread(target=churner) for _ in range(4)]
    for t in threads: t.start()
    try:
        for _ in range(20):
            s, _ = client.get("/status")
            assert s == 200, "status returned non-200 under churn"
    finally:
        stop.set()
        for t in threads: t.join(timeout=2.0)
    assert not errors, f"churner threads hit errors: {errors[:3]}"


def test_concurrent_pre_read_no_deadlock(client: _Client) -> None:
    """10 concurrent pre-read requests on distinct sessions — all succeed."""
    results: list[int] = []

    def fire(i: int) -> None:
        s, _ = client.post("/hooks/pre-read",
                            {"session_id": _sid(f"conc-{i}"), "path": "CLAUDE.md"})
        results.append(s)

    threads = [threading.Thread(target=fire, args=(i,)) for i in range(10)]
    for t in threads: t.start()
    for t in threads: t.join()
    assert results == [200] * 10
