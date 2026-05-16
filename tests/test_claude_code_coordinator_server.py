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
    status, body = client.post("/hooks/pre-read", {"session_id": "s1", "path": "CLAUDE.md"},
                                headers_override={"Host": "attacker.example.com"})
    assert status == 403
    assert "host" in body["error"].lower()


def test_localhost_host_accepted(coordinator) -> None:
    """Host: localhost (not just 127.0.0.1) must also be accepted."""
    secret = load_secret(coordinator.coordinator_root)
    url = f"http://127.0.0.1:{coordinator.port}/hooks/pre-read"
    req = urlrequest.Request(
        url, data=b'{"session_id":"s1","path":"CLAUDE.md"}', method="POST",
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
                                {"session_id": "s1", "path": "CLAUDE.md",
                                 "content_hash": "abc"})
    assert status == 200
    assert body == {"status": "fresh"}


def test_pre_read_repeat_from_same_session_stays_fresh(client: _Client) -> None:
    """A second read from the same session on the same artifact is fresh."""
    client.post("/hooks/pre-read", {"session_id": "s1", "path": "CLAUDE.md", "content_hash": "h1"})
    status, body = client.post("/hooks/pre-read",
                                {"session_id": "s1", "path": "CLAUDE.md", "content_hash": "h1"})
    assert status == 200
    assert body == {"status": "fresh"}


def test_pre_read_after_peer_write_returns_stale(client: _Client) -> None:
    """Two sessions: A reads, B writes, A's next read returns stale."""
    # Session A first-reads to seed v1 + take SHARED.
    client.post("/hooks/pre-read", {"session_id": "A", "path": "plan.md", "content_hash": "h1"})
    # Session B pre-edits (acquires E, invalidates A) and post-edits (commits v2).
    client.post("/hooks/pre-edit", {"session_id": "B", "path": "plan.md"})
    client.post("/hooks/post-edit",
                {"session_id": "B", "path": "plan.md", "content_hash": "h2", "success": True})
    # Session A's next read now sees stale.
    status, body = client.post("/hooks/pre-read",
                                {"session_id": "A", "path": "plan.md", "content_hash": "h1"})
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
    client.post("/hooks/pre-read", {"session_id": "A", "path": "plan.md", "content_hash": "h1"})
    client.post("/hooks/pre-edit", {"session_id": "B", "path": "plan.md"})
    client.post("/hooks/post-edit",
                {"session_id": "B", "path": "plan.md", "content_hash": "h2", "success": True})
    status, body = client.post("/hooks/pre-read",
                                {"session_id": "A", "path": "plan.md", "content_hash": "h1"})
    assert body["hookSpecificOutput"]["permissionDecision"] != "deny"


def test_pre_read_untracked_path_fastpath(coordinator, client: _Client) -> None:
    """An untracked path returns fresh WITHOUT touching SQLite (R8)."""
    artifact_count_before = len(coordinator.registry.artifact_ids())
    status, body = client.post("/hooks/pre-read",
                                {"session_id": "A", "path": "src/random.py",
                                 "content_hash": "h1"})
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
    status, body = client.post("/hooks/pre-read", {"session_id": "s", "path": ""})
    assert status == 400


# ----------------------------------------------------------------------
# /hooks/pre-edit + post-edit (KTD-1 cycle)
# ----------------------------------------------------------------------


def test_full_edit_cycle(coordinator, client: _Client) -> None:
    """pre-edit acquires E → post-edit commits + bumps version."""
    client.post("/hooks/pre-read", {"session_id": "A", "path": "plan.md", "content_hash": "h0"})
    s, b = client.post("/hooks/pre-edit", {"session_id": "A", "path": "plan.md"})
    assert s == 200 and b == {"ok": True}
    s, b = client.post("/hooks/post-edit",
                        {"session_id": "A", "path": "plan.md",
                         "content_hash": "h1", "success": True})
    assert s == 200 and b == {"ok": True}
    # Version bumped
    artifact_id = coordinator.registry.lookup_artifact_id_by_name("plan.md")
    art = coordinator.registry.get_artifact(artifact_id)
    assert art.version == 2  # seeded at 1, committed once → 2


def test_failed_edit_releases_grant_without_bump(coordinator, client: _Client) -> None:
    """KTD-1 release-on-failure: post-edit success:false releases E without bumping."""
    client.post("/hooks/pre-read", {"session_id": "A", "path": "plan.md"})
    client.post("/hooks/pre-edit", {"session_id": "A", "path": "plan.md"})
    artifact_id = coordinator.registry.lookup_artifact_id_by_name("plan.md")
    version_before = coordinator.registry.get_artifact(artifact_id).version
    s, b = client.post("/hooks/post-edit",
                        {"session_id": "A", "path": "plan.md",
                         "content_hash": "ignored", "success": False})
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
    s2, b2 = client.post("/hooks/pre-edit", {"session_id": "B", "path": "plan.md"})
    assert s2 == 200 and b2.get("ok") is True


def test_collision_surfaces_via_additional_context(coordinator, client: _Client) -> None:
    """KTD-9 same-hash-blindness mitigation: when another session holds E,
    pre-edit returns hookSpecificOutput with collision warning."""
    # Session A holds E
    client.post("/hooks/pre-edit", {"session_id": "A", "path": "plan.md"})
    # Session B attempts edit → collision response
    status, body = client.post("/hooks/pre-edit", {"session_id": "B", "path": "plan.md"})
    assert status == 200
    assert body.get("collision") is True
    out = body["hookSpecificOutput"]
    assert out["permissionDecision"] == "allow"  # v0.1 warn only
    assert "Concurrent edit detected" in out["additionalContext"]
    assert "plan.md" in out["additionalContext"]
    # The collision msg contains the holder's short session id (A's prefix)
    assert "A" in out["additionalContext"]


# ----------------------------------------------------------------------
# /hooks/session-stop (KTD-11)
# ----------------------------------------------------------------------


def test_session_stop_releases_uncommitted_grants(coordinator, client: _Client) -> None:
    """KTD-11: end-of-turn Stop releases any uncommitted EXCLUSIVE grants."""
    client.post("/hooks/pre-edit", {"session_id": "A", "path": "plan.md"})
    client.post("/hooks/pre-edit", {"session_id": "A", "path": "spec.md"})
    # Stop fires
    s, b = client.post("/hooks/session-stop", {"session_id": "A"})
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
    client.post("/hooks/pre-edit", {"session_id": "A", "path": "plan.md"})
    s1, b1 = client.post("/hooks/session-stop", {"session_id": "A"})
    assert s1 == 200 and "plan.md" in b1["released_artifacts"]
    s2, b2 = client.post("/hooks/session-stop", {"session_id": "A"})
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
    client.post("/hooks/pre-read", {"session_id": "A", "path": "plan.md"})
    client.post("/hooks/pre-edit", {"session_id": "A", "path": "spec.md"})
    s, b = client.get("/status")
    assert s == 200
    tracked_paths = {a["path"] for a in b["tracked_artifacts"]}
    assert "plan.md" in tracked_paths
    assert "spec.md" in tracked_paths
    sessions = {sess["agent_name"] for sess in b["sessions"]}
    assert "claude-session-A" in sessions
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
    agent_id = session_to_agent_id("hb-session")
    assert coordinator.registry.last_heartbeat_tick(agent_id) is None
    client.post("/hooks/pre-read",
                {"session_id": "hb-session", "path": "CLAUDE.md"})
    after_pre_read = coordinator.registry.last_heartbeat_tick(agent_id)
    assert after_pre_read is not None
    time.sleep(1.1)  # ensure monotonic tick advances
    client.post("/hooks/pre-edit", {"session_id": "hb-session", "path": "CLAUDE.md"})
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
    client.post("/hooks/pre-read", {"session_id": "A", "path": "plan.md"})
    client.post("/hooks/pre-edit", {"session_id": "B", "path": "plan.md"})
    client.post("/hooks/post-edit",
                {"session_id": "B", "path": "plan.md",
                 "content_hash": "h2", "success": True})

    _, body1 = client.post("/hooks/pre-read",
                           {"session_id": "A", "path": "plan.md"})
    time.sleep(0.05)  # ensure clock advances
    # Invalidate A again so the second response is also stale
    client.post("/hooks/pre-edit", {"session_id": "B", "path": "plan.md"})
    client.post("/hooks/post-edit",
                {"session_id": "B", "path": "plan.md",
                 "content_hash": "h3", "success": True})
    _, body2 = client.post("/hooks/pre-read",
                           {"session_id": "A", "path": "plan.md"})

    msg1 = body1["hookSpecificOutput"]["additionalContext"]
    msg2 = body2["hookSpecificOutput"]["additionalContext"]
    # Same shape, different text — the version delta differs at minimum.
    assert msg1 != msg2, "warning templates must vary per invocation for v0.2 strict-mode safety"


# ----------------------------------------------------------------------
# Concurrency
# ----------------------------------------------------------------------


def test_concurrent_pre_read_no_deadlock(client: _Client) -> None:
    """10 concurrent pre-read requests on distinct sessions — all succeed."""
    results: list[int] = []

    def fire(i: int) -> None:
        s, _ = client.post("/hooks/pre-read",
                            {"session_id": f"conc-{i}", "path": "CLAUDE.md"})
        results.append(s)

    threads = [threading.Thread(target=fire, args=(i,)) for i in range(10)]
    for t in threads: t.start()
    for t in threads: t.join()
    assert results == [200] * 10
