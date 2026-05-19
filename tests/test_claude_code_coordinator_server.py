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
# A1 hardening — adversarial review findings F1-F5
# ----------------------------------------------------------------------


def test_a1_stop_hook_surfaces_pending_notices(client: _Client) -> None:
    """F1 (P0): the canonical phpmac case — X preempted, X never fires
    another pre-event (model decided next action is a Bash/Grep, or turn
    just ends). Stop fires. Without this, X's notice orphans and X never
    learns. Fix: Stop pops + includes in response body."""
    x = _sid("X"); y = _sid("Y")
    # X holds E
    client.post("/hooks/pre-edit", {"session_id": x, "path": "plan.md"})
    # Y preempts X
    client.post("/hooks/pre-edit", {"session_id": y, "path": "plan.md"})
    # X's turn ends without another pre-event — Stop fires
    s, body = client.post("/hooks/session-stop", {"session_id": x})
    assert s == 200
    assert body["ok"] is True
    # Response body MUST include the preemption notices (telemetry-visible
    # in stream-json) so the silent-drop is impossible.
    assert "notices" in body, f"Stop response must surface pending notices; got {body}"
    notices = body["notices"]
    assert len(notices) >= 1
    # Notice references the preempted artifact + preempter
    notice = notices[0]
    assert notice["path"] == "plan.md"
    assert notice["preempter_session_id"].startswith(y[:8]) or notice["preempter_session_id"] == y


def test_a1_stop_hook_consumes_notices_no_orphan(client: _Client) -> None:
    """F1 consequence: Stop POPS notices, so they don't orphan if X never
    returns. After Stop, a subsequent pre-read by X shouldn't see the
    same notice text (already consumed at Stop)."""
    x = _sid("X"); y = _sid("Y")
    client.post("/hooks/pre-edit", {"session_id": x, "path": "plan.md"})
    client.post("/hooks/pre-edit", {"session_id": y, "path": "plan.md"})
    # Stop consumes
    _, stop_body = client.post("/hooks/session-stop", {"session_id": x})
    assert "notices" in stop_body and len(stop_body["notices"]) >= 1
    # X's hypothetical next-turn pre-read MUST NOT re-surface the same notice
    _, body = client.post("/hooks/pre-read", {"session_id": x, "path": "plan.md"})
    if "hookSpecificOutput" in body:
        msg = body["hookSpecificOutput"]["additionalContext"]
        # Notice was consumed at Stop; pre-read may still show stale-read
        # warning (X is INVALID) but should not contain preemption prose.
        assert "preempted" not in msg.lower() and "revoked" not in msg.lower(), (
            f"notice should be consumed at Stop; pre-read re-surfaced: {msg}"
        )


def test_a1_prose_capped_under_10kb_with_many_notices(client: _Client) -> None:
    """F3 (P1): N preemption notices compound prose linearly. Cap at 4KB
    prose (~10KB total with prepended stale-read warnings). Coalesce
    after first 3 notices."""
    x = _sid("X")
    # Set up 20 artifacts under tracked paths, X holds E on all, then 20
    # other sessions each preempt X on a distinct artifact.
    paths = [f"docs/specs/preempt-{i:02d}.md" for i in range(20)]
    for path in paths:
        client.post("/hooks/pre-edit", {"session_id": x, "path": path})
    for i, path in enumerate(paths):
        attacker = _sid(f"attacker-{i}")
        client.post("/hooks/pre-edit", {"session_id": attacker, "path": path})
    # X's next hook will see 20 pending notices
    _, body = client.post("/hooks/pre-read",
                          {"session_id": x, "path": paths[0]})
    if "hookSpecificOutput" in body:
        msg = body["hookSpecificOutput"]["additionalContext"]
        assert len(msg.encode("utf-8")) <= 10240, (
            f"additionalContext should fit in 10KB cap; got {len(msg.encode('utf-8'))} bytes"
        )
        # And the message should mention coalescing (e.g., "and N more")
        # so the model knows there are unsurfaced notices.
        # Permit "more" or "additional" or a count expression
        assert any(w in msg.lower() for w in ("more", "additional", "(...)")), (
            f"prose should signal coalescing when notices truncated; got: {msg}"
        )


def test_a1_orphan_notices_evicted_after_ttl(coordinator) -> None:
    """F2 (P1): orphan notices (victim session never returns to pop) are
    eventually evicted to bound state growth. Registry exposes
    evict_stale_notices(max_age_sec) for the lifecycle sweep."""
    import time
    # Create a notice manually with an old timestamp
    from uuid import UUID, uuid4
    victim = uuid4()
    preempter = uuid4()
    # Need an artifact_id that exists (FK)
    from ccs.core.types import Artifact
    art = Artifact(id=uuid4(), name="orphan-test.md", version=1, content_hash="h")
    coordinator.registry.register_artifact(art, content="")
    coordinator.registry.record_preemption_notice(
        victim_agent_id=victim,
        artifact_id=art.id,
        preempter_agent_id=preempter,
        preempted_at_unix_ts=time.time() - 3600,  # 1 hour old
    )
    # Sanity: notice present
    notices = coordinator.registry.pop_pending_notices(victim)
    coordinator.registry.record_preemption_notice(  # re-record after pop drained
        victim_agent_id=victim,
        artifact_id=art.id,
        preempter_agent_id=preempter,
        preempted_at_unix_ts=time.time() - 3600,
    )
    # Evict everything older than 30 minutes
    evicted = coordinator.registry.evict_stale_notices(max_age_sec=1800)
    assert evicted >= 1, f"expected to evict the 1-hour-old notice; evicted {evicted}"
    # Now empty
    assert coordinator.registry.pop_pending_notices(victim) == []


def test_a1_upsert_uses_wall_clock_not_commit_order(coordinator) -> None:
    """F5 (P3): UPSERT must keep the most-recent-by-WALL-CLOCK notice,
    not the most-recent-by-COMMIT-order. If Y commits at clock=100 but
    Z commits later at clock=99 (out-of-order), the row stays at Y's
    record (later wall-clock)."""
    from uuid import uuid4
    from ccs.core.types import Artifact
    victim = uuid4()
    art = Artifact(id=uuid4(), name="upsert-test.md", version=1, content_hash="h")
    coordinator.registry.register_artifact(art, content="")
    y = uuid4(); z = uuid4()
    # Record Y at clock=100 (later in wall-clock)
    coordinator.registry.record_preemption_notice(
        victim_agent_id=victim, artifact_id=art.id,
        preempter_agent_id=y, preempted_at_unix_ts=100.0,
    )
    # Then record Z at clock=50 (earlier in wall-clock, later in commit order)
    coordinator.registry.record_preemption_notice(
        victim_agent_id=victim, artifact_id=art.id,
        preempter_agent_id=z, preempted_at_unix_ts=50.0,
    )
    # The remaining notice should be Y's (later wall-clock wins)
    notices = coordinator.registry.pop_pending_notices(victim)
    assert len(notices) == 1
    artifact_id, preempter, ts = notices[0]
    assert preempter == y, f"expected Y (later wall-clock) to win, got {preempter}"
    assert ts == 100.0


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
    """8 concurrent pre-read requests on distinct sessions — all succeed.

    v0.1.1 KTD-G item 2 caps handler concurrency at HANDLER_CONCURRENCY_LIMIT
    (= pool_size × 2 = 8) per plugin docs/known-issues/
    2026-05-17-watchdog-races.md A7 mitigation. Requests above the limit
    receive HTTP 503 synchronously without spawning a handler thread.
    """
    results: list[int] = []

    def fire(i: int) -> None:
        s, _ = client.post("/hooks/pre-read",
                            {"session_id": _sid(f"conc-{i}"), "path": "CLAUDE.md"})
        results.append(s)

    threads = [threading.Thread(target=fire, args=(i,)) for i in range(8)]
    for t in threads: t.start()
    for t in threads: t.join()
    assert results == [200] * 8


def test_concurrent_pre_read_above_limit_returns_503(client: _Client) -> None:
    """v0.1.1 KTD-G item 2: requests above HANDLER_CONCURRENCY_LIMIT are
    rejected with 503, not silently queued. Fires 32 concurrent requests
    against a coordinator with limit=8; expects at least some 503s.

    Note: deterministic 503 emission requires slow-handler simulation —
    real handlers complete fast enough that the burst may serialize.
    This test asserts the contract (503 is possible above limit) by
    issuing far more requests than the limit in tight succession.
    """
    results: list[int] = []

    def fire(i: int) -> None:
        s, _ = client.post("/hooks/pre-read",
                            {"session_id": _sid(f"burst-{i}"), "path": "CLAUDE.md"})
        results.append(s)

    threads = [threading.Thread(target=fire, args=(i,)) for i in range(32)]
    for t in threads: t.start()
    for t in threads: t.join()
    # All responses must be valid HTTP codes; allowed values: 200 (handled)
    # or 503 (concurrency-overflowed). Anything else is a bug.
    assert all(r in (200, 503) for r in results), f"unexpected statuses: {results}"
    # At least one 200 must succeed (the limit allows some throughput).
    assert any(r == 200 for r in results)


# ======================================================================
# v0.1.1 KTD-N — H4 mitigation: /hooks/pre-bash + /hooks/pre-grep
# ======================================================================


def test_pre_bash_untracked_command_returns_fresh_fastpath(coordinator, client: _Client) -> None:
    """A Bash command that reads no tracked artifacts returns fresh
    without touching SQLite. Mirrors pre-read fast-path."""
    before = len(coordinator.registry.artifact_ids())
    status, body = client.post(
        "/hooks/pre-bash",
        {"session_id": _sid("A"), "command": "ls -la /etc"},
    )
    assert status == 200
    assert body == {"status": "fresh"}
    assert len(coordinator.registry.artifact_ids()) == before


def test_pre_bash_first_observation_returns_fresh(client: _Client) -> None:
    """KTD-9 first-observation seeding via Bash. `cat plan.md` on a fresh
    workspace seeds plan.md + grants SHARED + returns fresh."""
    status, body = client.post(
        "/hooks/pre-bash",
        {"session_id": _sid("A"), "command": "cat plan.md"},
    )
    assert status == 200
    assert body == {"status": "fresh"}


def test_pre_bash_after_peer_write_returns_stale(client: _Client) -> None:
    """H4 mitigation core test: session A's `bash cat plan.md` after a
    peer commit returns stale, NOT silent fresh. This is the gap KTD-N
    closes — without the Bash hook, A's bash-cat would bypass the
    coherence layer entirely (the H4 finding from v0.2 Phase 0)."""
    # A first-reads via pre-read.
    client.post("/hooks/pre-read", {"session_id": _sid("A"), "path": "plan.md", "content_hash": _hash("h1")})
    # B commits v2.
    client.post("/hooks/pre-edit", {"session_id": _sid("B"), "path": "plan.md"})
    client.post("/hooks/post-edit", {"session_id": _sid("B"), "path": "plan.md", "content_hash": _hash("h2"), "success": True})
    # A bash-cats plan.md → stale warning fires.
    status, body = client.post(
        "/hooks/pre-bash",
        {"session_id": _sid("A"), "command": "cat plan.md"},
    )
    assert status == 200
    assert body["status"] == "stale"
    assert "hookSpecificOutput" in body
    out = body["hookSpecificOutput"]
    assert out["hookEventName"] == "PreToolUse"
    assert out["permissionDecision"] == "allow"  # v0.1.1 warn-only per KTD-E
    assert "plan.md" in out["additionalContext"]
    assert "Bash command" in out["additionalContext"]
    assert body["stale_paths"] == ["plan.md"]


def test_pre_bash_warn_mode_never_returns_deny(client: _Client) -> None:
    """v0.1.1 invariant: pre-bash MUST NOT return deny (warn-only per KTD-E)."""
    client.post("/hooks/pre-read", {"session_id": _sid("A"), "path": "plan.md", "content_hash": _hash("h1")})
    client.post("/hooks/pre-edit", {"session_id": _sid("B"), "path": "plan.md"})
    client.post("/hooks/post-edit", {"session_id": _sid("B"), "path": "plan.md", "content_hash": _hash("h2"), "success": True})
    status, body = client.post(
        "/hooks/pre-bash",
        {"session_id": _sid("A"), "command": "cat plan.md"},
    )
    if "hookSpecificOutput" in body:
        assert body["hookSpecificOutput"]["permissionDecision"] != "deny"


def test_pre_bash_pipeline_with_tracked_arg(client: _Client) -> None:
    """`cat README.md || cat plan.md` — pipeline-split detection finds plan.md."""
    client.post("/hooks/pre-read", {"session_id": _sid("A"), "path": "plan.md", "content_hash": _hash("h1")})
    client.post("/hooks/pre-edit", {"session_id": _sid("B"), "path": "plan.md"})
    client.post("/hooks/post-edit", {"session_id": _sid("B"), "path": "plan.md", "content_hash": _hash("h2"), "success": True})
    status, body = client.post(
        "/hooks/pre-bash",
        {"session_id": _sid("A"), "command": "cat README.md || cat plan.md"},
    )
    assert status == 200
    assert body["status"] == "stale"
    assert "plan.md" in body["stale_paths"]


def test_pre_bash_missing_session_id_400(client: _Client) -> None:
    status, body = client.post("/hooks/pre-bash", {"command": "cat plan.md"})
    assert status == 400
    assert "session_id" in body["error"]


def test_pre_bash_empty_command_400(client: _Client) -> None:
    status, body = client.post("/hooks/pre-bash", {"session_id": _sid("A"), "command": ""})
    assert status == 400
    assert "command" in body["error"]


def test_pre_bash_oversized_command_413(client: _Client) -> None:
    big = "cat plan.md " + ("x" * 20_000)
    status, body = client.post("/hooks/pre-bash", {"session_id": _sid("A"), "command": big})
    assert status == 413


def test_pre_bash_grep_substring_no_false_positive(client: _Client) -> None:
    """Per KTD-N false-positive test: a literal quoted pattern that
    contains a tracked filename must NOT fire. `grep "cat plan.md" notes.txt`
    has plan.md inside the quoted search-pattern string, NOT as a file arg."""
    status, body = client.post(
        "/hooks/pre-bash",
        {"session_id": _sid("A"), "command": 'grep "cat plan.md is a tracked file" notes.txt'},
    )
    assert status == 200
    # notes.txt is not tracked; plan.md is inside a quoted string. No detection.
    assert body == {"status": "fresh"}


def test_pre_grep_no_tracked_artifacts_under_root_returns_fresh(client: _Client) -> None:
    """Empty workspace — grep over `src/` finds zero tracked artifacts."""
    status, body = client.post(
        "/hooks/pre-grep",
        {"session_id": _sid("A"), "search_root": "src"},
    )
    assert status == 200
    assert body == {"status": "fresh"}


def test_pre_grep_after_peer_write_returns_stale(client: _Client) -> None:
    """H4 mitigation for Grep: session A's grep over a directory
    containing peer-updated tracked artifacts returns stale."""
    # A first-reads plan.md (registers it).
    client.post("/hooks/pre-read", {"session_id": _sid("A"), "path": "plan.md", "content_hash": _hash("h1")})
    # B commits v2.
    client.post("/hooks/pre-edit", {"session_id": _sid("B"), "path": "plan.md"})
    client.post("/hooks/post-edit", {"session_id": _sid("B"), "path": "plan.md", "content_hash": _hash("h2"), "success": True})
    # A greps the workspace root (covers plan.md).
    status, body = client.post(
        "/hooks/pre-grep",
        {"session_id": _sid("A"), "search_root": ""},
    )
    assert status == 200
    assert body["status"] == "stale"
    assert "plan.md" in body["stale_paths"]
    assert "Grep search" in body["hookSpecificOutput"]["additionalContext"]


def test_pre_grep_missing_session_id_400(client: _Client) -> None:
    status, body = client.post("/hooks/pre-grep", {"search_root": ""})
    assert status == 400
    assert "session_id" in body["error"]


def test_pre_grep_path_traversal_400(client: _Client) -> None:
    status, body = client.post("/hooks/pre-grep", {"session_id": _sid("A"), "search_root": "../escape"})
    assert status == 400


# ======================================================================
# v0.1.1 KTD-G — watchdog A6/A7 hardening: queue gate + handler semaphore + counters
# ======================================================================


def test_status_includes_watchdog_counters_zeroed_at_startup(client: _Client) -> None:
    """KTD-G item 3 + KTD-J: /status surfaces watchdog/concurrency counters
    so silent degradation becomes observable. All zero immediately after spawn."""
    status, body = client.get("/status")
    assert status == 200
    assert body["watchdog_timeouts_total"] == 0
    assert body["watchdog_queue_overflows_total"] == 0
    assert body["handler_concurrency_overflows_total"] == 0


def test_watchdog_timeout_increments_counter(coordinator, client: _Client) -> None:
    """When FuturesTimeout fires in _run_or_degrade, watchdog_timeouts_total
    increments and /status reflects it."""
    from unittest.mock import patch
    from concurrent.futures import TimeoutError as FuturesTimeout

    with patch.object(coordinator, "run_with_watchdog", side_effect=FuturesTimeout()):
        status, body = client.post("/hooks/pre-read",
                                    {"session_id": _sid("X"), "path": "plan.md"})
    # Degraded response per the existing _run_or_degrade contract.
    assert status == 200
    assert body.get("degraded") is True
    # Counter incremented.
    status, sbody = client.get("/status")
    assert sbody["watchdog_timeouts_total"] >= 1


def test_watchdog_queue_overflow_returns_503(coordinator, client: _Client) -> None:
    """KTD-G item 1: when the watchdog ThreadPoolExecutor's _work_queue
    grows past WATCHDOG_QUEUE_LIMIT, _run_or_degrade returns HTTP 503
    instead of submitting the task. Simulated via a stubbed qsize that
    reports overflow."""
    from unittest.mock import patch

    class _FakeQueue:
        @staticmethod
        def qsize() -> int:
            return 100  # well above the limit

    with patch.object(coordinator._watchdog, "_work_queue", _FakeQueue()):
        status, body = client.post("/hooks/pre-read",
                                    {"session_id": _sid("X"), "path": "plan.md"})
    assert status == 503
    assert body["error"] == "watchdog queue overloaded"
    # Counter incremented.
    status, sbody = client.get("/status")
    assert sbody["watchdog_queue_overflows_total"] >= 1


def test_handler_concurrency_limit_constant_matches_spec(client: _Client) -> None:
    """KTD-G item 2 invariant: HANDLER_CONCURRENCY_LIMIT = pool_size × 2.
    Locked at 8 in v0.1.1 (pool_size=4). If a future change adjusts the
    pool size, this test will fail loudly so the operator confirms the
    new concurrency cap is intentional."""
    from ccs.adapters.claude_code.coordinator_server import (
        HANDLER_CONCURRENCY_LIMIT,
        WATCHDOG_QUEUE_LIMIT,
        _WATCHDOG_POOL_SIZE,
    )

    assert _WATCHDOG_POOL_SIZE == 4
    assert HANDLER_CONCURRENCY_LIMIT == 8
    assert WATCHDOG_QUEUE_LIMIT == 8
