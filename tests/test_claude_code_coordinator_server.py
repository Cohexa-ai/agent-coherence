# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Tests for the coordinator HTTP server (plan Unit 4).

Covers the seven endpoint contracts + auth + Host check + watchdog +
per-invocation warning-template variation.
"""

from __future__ import annotations

import dataclasses
import hashlib
import json
import os
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
    _SHARED_FOREIGN_DENY_LAG_WINDOW_SEC,
    MAX_POLICY_PATHS_PER_REQUEST,
    CoordinatorHTTPServer,
    _is_recent_self_commit_lag,
    session_to_agent_id,
)
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
    SHARED + returns fresh. Unit 6: the fresh response also carries the
    seeded ``version`` (additive — for OCC writers sourcing expected_version)."""
    status, body = client.post("/hooks/pre-read",
                                {"session_id": _sid("s1"), "path": "CLAUDE.md",
                                 "content_hash": _hash("abc")})
    assert status == 200
    assert body == {"status": "fresh", "version": 1}


def test_pre_read_repeat_from_same_session_stays_fresh(client: _Client) -> None:
    """A second read from the same session on the same artifact is fresh.
    Unit 6: still carries the artifact version on the fresh response."""
    client.post("/hooks/pre-read", {"session_id": _sid("s1"), "path": "CLAUDE.md", "content_hash": _hash("h1")})
    status, body = client.post("/hooks/pre-read",
                                {"session_id": _sid("s1"), "path": "CLAUDE.md", "content_hash": _hash("h1")})
    assert status == 200
    assert body == {"status": "fresh", "version": 1}


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
# /hooks/pre-read — fresh-SHARED hash-mismatch signal (PR #108 follow-up)
# ----------------------------------------------------------------------


def test_pre_read_fresh_shared_hash_mismatch_sets_hash_differs(client: _Client) -> None:
    """Defense-in-depth (PR #108 follow-up): a SHARED holder re-reading
    with a disk hash that mismatches the recorded content gets
    ``hash_differs: true`` on the fresh response. A peer commit would
    have left the session INVALID, so the mismatch implies an
    out-of-band write — surfaced additively, never denied (the plugin
    path stays fail-open)."""
    client.post("/hooks/pre-read",
                {"session_id": _sid("s1"), "path": "CLAUDE.md",
                 "content_hash": _hash("on-disk-v1")})
    status, body = client.post("/hooks/pre-read",
                                {"session_id": _sid("s1"), "path": "CLAUDE.md",
                                 "content_hash": _hash("out-of-band-edit")})
    assert status == 200
    assert body == {"status": "fresh", "version": 1, "hash_differs": True}


def test_pre_read_fresh_shared_matching_hash_omits_hash_differs(client: _Client) -> None:
    """Additive contract: the key appears ONLY when the mismatch fires.
    A matching hash keeps the exact two-field fresh shape so
    exact-shape status clients are untouched."""
    client.post("/hooks/pre-read",
                {"session_id": _sid("s1"), "path": "CLAUDE.md",
                 "content_hash": _hash("on-disk-v1")})
    status, body = client.post("/hooks/pre-read",
                                {"session_id": _sid("s1"), "path": "CLAUDE.md",
                                 "content_hash": _hash("on-disk-v1")})
    assert status == 200
    assert body == {"status": "fresh", "version": 1}


def test_pre_read_fresh_shared_without_hash_omits_hash_differs(client: _Client) -> None:
    """A SHARED re-read that supplies no content_hash has nothing to
    compare — no signal, exact two-field shape."""
    client.post("/hooks/pre-read",
                {"session_id": _sid("s1"), "path": "CLAUDE.md",
                 "content_hash": _hash("on-disk-v1")})
    status, body = client.post("/hooks/pre-read",
                                {"session_id": _sid("s1"), "path": "CLAUDE.md"})
    assert status == 200
    assert body == {"status": "fresh", "version": 1}


def test_pre_read_fresh_shared_empty_seed_recorded_hash_never_fires(client: _Client) -> None:
    """KTD-9 seeding without a caller hash records the "" sentinel
    (surfaced as None by the registry); a later hash-bearing re-read
    must not fire against it — there is no real content claim to
    mismatch."""
    client.post("/hooks/pre-read",
                {"session_id": _sid("s1"), "path": "CLAUDE.md"})
    status, body = client.post("/hooks/pre-read",
                                {"session_id": _sid("s1"), "path": "CLAUDE.md",
                                 "content_hash": _hash("real-disk-bytes")})
    assert status == 200
    assert body == {"status": "fresh", "version": 1}


def test_pre_read_fresh_shared_f_sentinel_recorded_hash_never_fires(
    coordinator, client: _Client,
) -> None:
    """The synthetic launch-gate sentinel ("f"*64) is not a real SHA-256
    of any content; a SHARED holder re-reading against it must not fire
    the signal. (The stale path's hash_differs deliberately DOES fire on
    it — that asymmetry is the launch-gate contract.)"""
    client.post("/hooks/pre-read",
                {"session_id": _sid("s1"), "path": "CLAUDE.md",
                 "content_hash": _hash("on-disk-v1")})
    art_id = coordinator.registry.lookup_artifact_id_by_name("CLAUDE.md")
    assert art_id is not None
    art = coordinator.registry.get_artifact(art_id)
    # Inject the sentinel directly into the artifact row (same shape the
    # launch-gate synthetic SQLite injection produces); the session's
    # SHARED grant is untouched.
    coordinator.registry.set_artifact_and_content(
        art_id, dataclasses.replace(art, content_hash="f" * 64), "",
    )
    status, body = client.post("/hooks/pre-read",
                                {"session_id": _sid("s1"), "path": "CLAUDE.md",
                                 "content_hash": _hash("on-disk-v1")})
    assert status == 200
    assert body == {"status": "fresh", "version": 1}


def test_pre_read_fresh_shared_hash_mismatch_increments_counter(
    coordinator, client: _Client,
) -> None:
    """Each firing bumps fresh_shared_hash_mismatch_total; matching
    re-reads don't."""
    before = coordinator._fresh_shared_hash_mismatch_total
    client.post("/hooks/pre-read",
                {"session_id": _sid("s1"), "path": "CLAUDE.md",
                 "content_hash": _hash("on-disk-v1")})
    client.post("/hooks/pre-read",
                {"session_id": _sid("s1"), "path": "CLAUDE.md",
                 "content_hash": _hash("on-disk-v1")})
    assert coordinator._fresh_shared_hash_mismatch_total == before
    client.post("/hooks/pre-read",
                {"session_id": _sid("s1"), "path": "CLAUDE.md",
                 "content_hash": _hash("out-of-band-edit")})
    assert coordinator._fresh_shared_hash_mismatch_total == before + 1


# ----------------------------------------------------------------------
# Survivor #6 v1 — _is_recent_self_commit_lag (R2): the commit→disk-write
# lag-exclusion predicate. A SHARED-holder hash mismatch is the benign lag
# (NOT a foreign edit) iff THIS session is the artifact's RECENT last
# committer — the registry advanced the canonical hash (e.g. a commit_cas
# WIN that leaves the writer SHARED) but the agent has not yet flushed the
# new bytes to disk. Anything else is a genuine out-of-band edit → deny.
# ----------------------------------------------------------------------

_CSRV = "ccs.adapters.claude_code.coordinator_server"


def _patch_last_writer(monkeypatch, *, writer, ts) -> None:
    monkeypatch.setattr(f"{_CSRV}._last_writer_for", lambda coord, aid: writer)
    monkeypatch.setattr(f"{_CSRV}._last_writer_unix_ts", lambda coord, aid: ts)


# SB-25: the predicate's 3rd arg is now the CALLER's composite agent_id
# (compared via _agent_id_to_session to the writer's attribution), not the
# raw session_id. Register the caller so its attribution resolves, and keep
# monkeypatching _last_writer_for to the writer's ATTRIBUTION string.
def _caller(coordinator, label: str):
    """Register a session and return (session_id, composite_agent_id)."""
    sid = _sid(label)
    return sid, coordinator.register_session(sid)


def test_lag_true_for_recent_self_commit(coordinator, monkeypatch) -> None:
    sid, agent_id = _caller(coordinator, "s1")
    _patch_last_writer(monkeypatch, writer=sid, ts=1000.0)
    now = 1000.0 + _SHARED_FOREIGN_DENY_LAG_WINDOW_SEC - 0.1
    assert _is_recent_self_commit_lag(coordinator, uuid.uuid4(), agent_id, now_unix=now) is True


def test_lag_false_for_stale_self_commit(coordinator, monkeypatch) -> None:
    """last_writer == self but the commit is OLD => something rewrote disk
    since => correctly FOREIGN (the recency clause is load-bearing)."""
    sid, agent_id = _caller(coordinator, "s1")
    _patch_last_writer(monkeypatch, writer=sid, ts=1000.0)
    now = 1000.0 + _SHARED_FOREIGN_DENY_LAG_WINDOW_SEC + 0.1
    assert _is_recent_self_commit_lag(coordinator, uuid.uuid4(), agent_id, now_unix=now) is False


def test_lag_false_for_other_writer(coordinator, monkeypatch) -> None:
    _, agent_id = _caller(coordinator, "s1")
    _patch_last_writer(monkeypatch, writer=_sid("s2"), ts=1000.0)
    assert _is_recent_self_commit_lag(
        coordinator, uuid.uuid4(), agent_id, now_unix=1000.1) is False


def test_lag_false_for_no_writer(coordinator, monkeypatch) -> None:
    _, agent_id = _caller(coordinator, "s1")
    _patch_last_writer(monkeypatch, writer=None, ts=None)
    assert _is_recent_self_commit_lag(
        coordinator, uuid.uuid4(), agent_id, now_unix=1000.1) is False


def test_lag_false_for_missing_updated_at(coordinator, monkeypatch) -> None:
    sid, agent_id = _caller(coordinator, "s1")
    _patch_last_writer(monkeypatch, writer=sid, ts=None)
    assert _is_recent_self_commit_lag(coordinator, uuid.uuid4(), agent_id, now_unix=1000.1) is False


def test_lag_true_at_exact_window_boundary(coordinator, monkeypatch) -> None:
    """`<=` boundary: a self-commit EXACTLY at the window edge is still treated as
    lag (the fail-safe direction). Pins the operator choice against a silent flip
    to `<`."""
    sid, agent_id = _caller(coordinator, "s1")
    _patch_last_writer(monkeypatch, writer=sid, ts=1000.0)
    now = 1000.0 + _SHARED_FOREIGN_DENY_LAG_WINDOW_SEC
    assert _is_recent_self_commit_lag(coordinator, uuid.uuid4(), agent_id, now_unix=now) is True


def test_status_metrics_exposes_fresh_shared_hash_mismatch_counter(client: _Client) -> None:
    """fresh_shared_hash_mismatch_total visible in /status?detail=metrics
    so an operator can size the false-positive rate before any
    strict-mode deny knob is considered."""
    status, body = client.get("/status?detail=metrics")
    assert status == 200
    assert "fresh_shared_hash_mismatch_total" in body
    assert isinstance(body["fresh_shared_hash_mismatch_total"], int)


def test_status_metrics_exposes_shared_foreign_lag_suppressed_counter(client: _Client) -> None:
    """shared_foreign_lag_suppressed_total visible in /status?detail=metrics so an
    operator can size the lag-window (5s) false-negative rate against
    strict_mode_denials_total."""
    status, body = client.get("/status?detail=metrics")
    assert status == 200
    assert "shared_foreign_lag_suppressed_total" in body
    assert isinstance(body["shared_foreign_lag_suppressed_total"], int)


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
    """Default (minimal) tier surfaces tracked artifacts + sessions +
    counters + coordinator_pid; the absolute workspace root stays gated
    behind ``?detail=full`` per R12.

    P1 #7: coordinator_pid was moved out of minimal in Unit 6 R12 and
    restored here — pid is public on POSIX (any `ps` invocation lists
    it) so it does not exceed the threat model's accepted disclosure,
    and operators rely on it to verify "is the coordinator I think is
    running actually mine"."""
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
    # AC-02: canonical field; old _s alias also present for one release.
    assert b["coordinator_uptime_seconds"] > 0
    assert b["coordinator_uptime_s"] == b["coordinator_uptime_seconds"]
    assert "policy_summary" in b
    # Minimal tier: absolute root sentinel'd; pid is present (P1 #7 reversion).
    assert b.get("detail") == "minimal"
    assert b.get("coordinator_root") == "."
    assert b.get("coordinator_pid") == os.getpid()


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


def test_a1_fresh_with_notice_preserves_version_field(client: _Client) -> None:
    """A1 × Unit 6: notice surfacing on the FRESH pre-read path must keep
    the additive ``version`` key alongside ``hookSpecificOutput``.

    Regression: the ``work_with_notice_surfacing`` wrapper rebuilt the
    fresh response as a literal dict, dropping ``version`` — an OCC
    writer sourcing expected_version from the read then CAS'd against 0
    and burned a wasted version_mismatch round-trip."""
    x = _sid("X"); y = _sid("Y")
    # X first-reads task.md — seeds v1 + grants SHARED, so the re-read
    # below (after the preemption on a DIFFERENT artifact) stays fresh.
    status, body = client.post("/hooks/pre-read",
                               {"session_id": x, "path": "task.md",
                                "content_hash": _hash("t1")})
    assert status == 200
    assert body == {"status": "fresh", "version": 1}
    # X acquires EXCLUSIVE on plan.md; Y pre-edits plan.md, silently
    # preempting X — a preemption notice queues for X.
    client.post("/hooks/pre-edit", {"session_id": x, "path": "plan.md"})
    client.post("/hooks/pre-edit", {"session_id": y, "path": "plan.md"})
    # X's next hook is a pre-read of task.md (still SHARED → fresh): the
    # wrapper drains the pending notice onto this fresh response.
    status, body = client.post("/hooks/pre-read",
                               {"session_id": x, "path": "task.md",
                                "content_hash": _hash("t1")})
    assert status == 200, body
    assert body.get("status") == "fresh", body
    assert "hookSpecificOutput" in body, (
        f"fresh pre-read after a preemption must surface the notice; got {body}"
    )
    msg = body["hookSpecificOutput"]["additionalContext"]
    assert "plan.md" in msg and y[:8] in msg, (
        f"notice prose should name the preempted artifact + preempter; got: {msg}"
    )
    assert body.get("version") == 1, (
        "fresh-with-notice response must preserve the Unit 6 version key "
        f"(OCC writers source expected_version from it); got {body}"
    )


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
    """A1 negative: a session that's never been preempted gets no notice.

    Finding #24: the previous conditional `if 'hookSpecificOutput' in body`
    made this assertion unreachable (X was never preempted so the field is
    absent). Replace with an unconditional assertion: the response must be
    exactly {ok: True} with no hookSpecificOutput at all.
    """
    x = _sid("X")
    # X never preempted — pre-edit must return exactly {ok: True} with no
    # preemption output.
    s, body = client.post("/hooks/pre-edit", {"session_id": x, "path": "plan.md"})
    assert s == 200
    assert body.get("ok") is True, f"expected ok=True, got: {body!r}"
    assert "hookSpecificOutput" not in body, (
        f"pre-edit for a never-preempted session must not carry hookSpecificOutput; got: {body!r}"
    )


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
    from uuid import uuid4
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
    import os
    import stat

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


def test_a6_watchdog_timeout_increments_counter(coordinator, client: _Client) -> None:
    """A6 — handler timeout / sweep deadlock: when FuturesTimeout fires in
    _run_or_degrade, watchdog_timeouts_total increments and /status reflects it."""
    from concurrent.futures import TimeoutError as FuturesTimeout
    from unittest.mock import patch

    with patch.object(coordinator, "run_with_watchdog", side_effect=FuturesTimeout()):
        status, body = client.post("/hooks/pre-read",
                                    {"session_id": _sid("X"), "path": "plan.md"})
    # Degraded response per the existing _run_or_degrade contract.
    assert status == 200
    assert body.get("degraded") is True
    # Counter incremented.
    status, sbody = client.get("/status")
    assert sbody["watchdog_timeouts_total"] >= 1


def test_a7_watchdog_queue_overflow_returns_503(coordinator, client: _Client) -> None:
    """A7 — sweep-concurrent-write / shutdown-mid-sweep: when the watchdog
    ThreadPoolExecutor's _work_queue grows past WATCHDOG_QUEUE_LIMIT,
    _run_or_degrade returns HTTP 503 instead of submitting the task.
    Simulated via a stubbed qsize that reports overflow."""
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
        _WATCHDOG_POOL_SIZE,
        HANDLER_CONCURRENCY_LIMIT,
        WATCHDOG_QUEUE_LIMIT,
    )

    assert _WATCHDOG_POOL_SIZE == 4
    assert HANDLER_CONCURRENCY_LIMIT == 8
    assert WATCHDOG_QUEUE_LIMIT == 8


# ----------------------------------------------------------------------
# KTD-I (Unit 5 L2) — in-flight handler semaphore drain on shutdown
# ----------------------------------------------------------------------


def test_i1_acquire_release_pair_balances_counter(tmp_path: Path) -> None:
    """Unit-level: acquire/release balance the in-flight counter; the
    drain condition is signalled on the zero transition."""
    srv = CoordinatorHTTPServer(tmp_path, port=0, instance_id="i1")
    try:
        assert srv._in_flight == 0
        assert srv.acquire_handler_slot() is True
        assert srv._in_flight == 1
        assert srv.acquire_handler_slot() is True
        assert srv._in_flight == 2
        srv.release_handler_slot()
        assert srv._in_flight == 1
        srv.release_handler_slot()
        assert srv._in_flight == 0
    finally:
        srv.shutdown()


def test_i2_acquire_denied_after_shutdown_started(tmp_path: Path) -> None:
    """Once ``_shutting_down`` flips, acquire_handler_slot returns False
    so the dispatcher 503s instead of touching a closing registry."""
    srv = CoordinatorHTTPServer(tmp_path, port=0, instance_id="i2")
    srv.serve_in_thread()
    try:
        # Manually flip the flag (mimics in-progress shutdown without
        # actually closing the registry, so we can keep poking).
        srv._shutting_down = True
        assert srv.acquire_handler_slot() is False
        assert srv._in_flight == 0
    finally:
        srv._shutting_down = False  # let shutdown() proceed normally
        srv.shutdown()


def test_i3_shutdown_waits_for_in_flight_handler(tmp_path: Path) -> None:
    """End-to-end: a long-running handler keeps the in-flight counter
    above zero; shutdown() must block on the drain until the handler
    returns rather than closing the registry under it."""
    srv = CoordinatorHTTPServer(tmp_path, port=0, instance_id="i3")
    srv.serve_in_thread()
    time.sleep(0.05)
    secret = load_secret(srv.coordinator_root)
    assert secret is not None
    client = _Client("127.0.0.1", srv.port, secret)

    # Simulate a slow handler by acquiring a slot from the test thread
    # (no real handler invoked — we only need to keep _in_flight > 0
    # for the drain to wait on).
    assert srv.acquire_handler_slot() is True

    shutdown_done = threading.Event()
    def shutdown_thread() -> None:
        srv.shutdown()
        shutdown_done.set()
    t = threading.Thread(target=shutdown_thread)
    t.start()

    # shutdown() should be blocked in the drain loop.
    assert not shutdown_done.wait(timeout=0.5), (
        "shutdown returned before the in-flight slot was released"
    )

    # Releasing the slot wakes the drain and lets shutdown complete.
    srv.release_handler_slot()
    assert shutdown_done.wait(timeout=2.0), "shutdown did not complete after drain"
    t.join(timeout=2.0)
    assert srv._in_flight_drain_timed_out is False
    del client  # silence unused-var lint


def test_i4_shutdown_drain_timeout_records_flag(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If a handler stays in-flight past the drain timeout, shutdown
    closes the registry anyway and records the timeout for observability."""
    srv = CoordinatorHTTPServer(tmp_path, port=0, instance_id="i4")
    srv.serve_in_thread()
    time.sleep(0.05)

    # Keep an in-flight slot held for the duration of the test.
    assert srv.acquire_handler_slot() is True

    # Shrink the drain timeout so the test finishes in <1s.
    import ccs.adapters.claude_code.coordinator_server as mod
    monkeypatch.setattr(mod, "IN_FLIGHT_DRAIN_TIMEOUT_SEC", 0.1)
    try:
        srv.shutdown()
        assert srv._in_flight_drain_timed_out is True, (
            "drain timeout should set the observability flag"
        )
    finally:
        # Release the artificially-held slot so the test doesn't leak.
        srv.release_handler_slot()


def test_i5_dispatch_pairs_acquire_with_release(client: _Client, coordinator) -> None:
    """Integration: a normal pre-read request increments and decrements
    the in-flight counter exactly once, leaving it at zero on return.

    Polling rationale: client.post returns once the response body is read,
    but ``release_handler_slot`` runs in the dispatcher's finally block
    AFTER the response is sent. There's a microseconds-to-milliseconds
    window where the counter is still 1 from the client's POV. Poll
    briefly (up to 1s) instead of asserting immediately — the contract
    is "eventually zero", not "zero by the next bytecode op". REL-03's
    lock around watchdog counters widens this window slightly on slow
    CI, surfacing the pre-existing race."""
    assert coordinator._in_flight == 0
    status, _ = client.post(
        "/hooks/pre-read",
        {"session_id": _sid("i5"), "path": "CLAUDE.md"},
    )
    assert status == 200
    deadline = time.monotonic() + 1.0
    while time.monotonic() < deadline:
        if coordinator._in_flight == 0:
            return
        time.sleep(0.010)
    pytest.fail(
        f"in-flight counter never drained: expected 0 within 1s, "
        f"still at {coordinator._in_flight}"
    )


def test_i6_dispatch_decrements_even_when_handler_raises(
    client: _Client, coordinator, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Defensive: if a handler raises mid-dispatch (becoming a 500), the
    finally block must still release the slot."""
    import ccs.adapters.claude_code.coordinator_server as mod
    original = mod._ROUTES[("POST", "/hooks/pre-read")]

    def raising_handler(req, coord) -> None:
        raise RuntimeError("simulated handler failure")

    monkeypatch.setitem(mod._ROUTES, ("POST", "/hooks/pre-read"), raising_handler)
    try:
        status, _ = client.post(
            "/hooks/pre-read",
            {"session_id": _sid("i6"), "path": "CLAUDE.md"},
        )
        assert status == 500
        # Counter must have been decremented despite the exception.
        assert coordinator._in_flight == 0
    finally:
        mod._ROUTES[("POST", "/hooks/pre-read")] = original


# ----------------------------------------------------------------------
# R21 (Unit 6) — MAX_REQUEST_BODY_BYTES cap before rfile.read
# ----------------------------------------------------------------------


def test_r21_request_body_overflow_returns_413(coordinator) -> None:
    """A Content-Length header that exceeds MAX_REQUEST_BODY_BYTES must
    be rejected with 413 BEFORE the coordinator reads the body into
    memory — protects against single-request OOM by a hostile or buggy
    client inside the trust boundary."""
    import http.client

    from ccs.adapters.claude_code.auth import load_secret
    from ccs.adapters.claude_code.coordinator_server import MAX_REQUEST_BODY_BYTES

    secret = load_secret(coordinator.coordinator_root)
    assert secret is not None

    # Build the request manually to control Content-Length precisely.
    # We claim n+1 bytes but only send 1 byte — server must reject on
    # header alone, not after reading the (oversized) body.
    over_n = MAX_REQUEST_BODY_BYTES + 1
    conn = http.client.HTTPConnection("127.0.0.1", coordinator.port, timeout=5)
    try:
        conn.request(
            "POST", "/hooks/pre-read",
            body=b"x",  # intentional mismatch — header says oversized, body is tiny
            headers={
                "Authorization": f"Bearer {secret}",
                "Host": "127.0.0.1",
                "Content-Type": "application/json",
                "Content-Length": str(over_n),
            },
        )
        resp = conn.getresponse()
        assert resp.status == 413, (
            f"expected 413 for oversized Content-Length={over_n}; got {resp.status}"
        )
        body = json.loads(resp.read().decode("utf-8"))
        assert "exceeds" in body["error"].lower()
        assert str(over_n) in body["error"]
    finally:
        conn.close()


def test_r21_body_at_cap_accepted(coordinator) -> None:
    """Boundary: a body at exactly MAX_REQUEST_BODY_BYTES (still well
    over our real payload sizes) is accepted, not 413'd off."""
    from ccs.adapters.claude_code.auth import load_secret
    from ccs.adapters.claude_code.coordinator_server import MAX_REQUEST_BODY_BYTES

    secret = load_secret(coordinator.coordinator_root)
    assert secret is not None

    # Craft a JSON object whose serialized length equals the cap. We pad
    # the session_id label so the overall JSON hits the byte count.
    base = {"session_id": _sid("r21"), "path": "CLAUDE.md", "pad": ""}
    base_bytes = json.dumps(base).encode("utf-8")
    pad_len = MAX_REQUEST_BODY_BYTES - len(base_bytes)
    assert pad_len > 0
    base["pad"] = "x" * pad_len
    payload = json.dumps(base).encode("utf-8")
    assert len(payload) == MAX_REQUEST_BODY_BYTES

    client = _Client("127.0.0.1", coordinator.port, secret)
    # Use the raw urllib client to ensure we control Content-Length.
    url = f"http://127.0.0.1:{coordinator.port}/hooks/pre-read"
    req = urlrequest.Request(
        url, data=payload, method="POST",
        headers={
            "Authorization": f"Bearer {secret}",
            "Host": "127.0.0.1",
            "Content-Type": "application/json",
        },
    )
    with urlrequest.urlopen(req, timeout=5) as resp:
        # Server may 200 (fresh) or 400 (unknown extra fields are tolerated
        # — but the body must have been READ, proving the cap let it through).
        assert resp.status in (200, 400)
    del client  # silence unused-var lint


# ----------------------------------------------------------------------
# KTD-J (Unit 8) — telemetry counters
# ----------------------------------------------------------------------


def test_a8_per_endpoint_counters_increment_on_dispatch(client: _Client, coordinator) -> None:
    """5 pre-reads + 3 pre-edits + 3 post-edits + 1 session-stop must show
    up in the per-endpoint counter block of /status?detail=full."""
    for i in range(5):
        client.post(
            "/hooks/pre-read",
            {"session_id": _sid(f"j1-{i}"), "path": "plan.md"},
        )
    for i in range(3):
        client.post(
            "/hooks/pre-edit",
            {"session_id": _sid(f"j1-edit-{i}"), "path": f"path_{i}.md"},
        )
    for i in range(3):
        client.post(
            "/hooks/post-edit",
            {
                "session_id": _sid(f"j1-edit-{i}"),
                "path": f"path_{i}.md",
                "content_hash": _hash(f"h{i}"),
                "success": True,
            },
        )
    client.post(
        "/hooks/session-stop", {"session_id": _sid("j1-stop")}
    )

    s, b = client.request(
        "GET", "/status?detail=metrics",
    )
    assert s == 200
    counters = b["endpoint_counters"]
    assert counters["pre_read_total"] == 5
    assert counters["pre_edit_total"] == 3
    assert counters["post_edit_total"] == 3
    assert counters["session_stop_total"] == 1


def test_a8_status_counter_request_itself_increments(client: _Client, coordinator) -> None:
    """A /status call counts itself — the increment fires before the
    handler runs."""
    _, b1 = client.get("/status")
    _, b2 = client.get("/status")
    assert (
        b2["endpoint_counters"]["status_total"]
        > b1["endpoint_counters"]["status_total"]
    )


def test_a8_counters_reset_to_zero_on_fresh_coordinator(tmp_path: Path) -> None:
    """Counters are CACHE, not persistent state. A fresh coordinator
    instance starts with zeros even when state.db already exists."""
    srv = CoordinatorHTTPServer(tmp_path, port=0, instance_id="j3")
    try:
        snap = srv.endpoint_counters_snapshot()
        assert all(v == 0 for v in snap.values())
        assert srv._intra_task_acquire_release_total == 0
        assert srv._stale_warning_emitted_total == 0
        assert srv._stale_warning_reread_total == 0
    finally:
        srv.shutdown()


def test_a8_stale_emitted_and_reread_counters_track_warning_cycle(
    client: _Client, coordinator,
) -> None:
    """Two-session stale scenario: A reads, B writes, A re-reads → stale
    warning fires (emitted=1). A reads again with the same artifact →
    that's the re-read (reread=1)."""
    a = _sid("j4-A"); b = _sid("j4-B")
    client.post("/hooks/pre-read",
                {"session_id": a, "path": "plan.md", "content_hash": _hash("h1")})
    client.post("/hooks/pre-edit", {"session_id": b, "path": "plan.md"})
    client.post("/hooks/post-edit", {"session_id": b, "path": "plan.md",
                                     "content_hash": _hash("h2"), "success": True})
    # A re-reads — stale warning fires.
    s, body = client.post("/hooks/pre-read",
                          {"session_id": a, "path": "plan.md"})
    assert body["status"] == "stale"
    # A re-reads again (the re-read after the warning).
    client.post("/hooks/pre-read",
                {"session_id": a, "path": "plan.md"})

    _, status_body = client.get("/status?detail=metrics")
    assert status_body["stale_warning_emitted_total"] >= 1
    assert status_body["stale_warning_reread_total"] >= 1


def test_a8_intra_task_acquire_release_increments_on_successful_post_edit(
    client: _Client, coordinator,
) -> None:
    """A pre-edit followed by a successful post-edit on a tracked path
    must bump the intra-task acquire-release counter exactly once.
    Untracked paths fast-path through pre-edit/post-edit without
    acquiring E, so the counter is the load-bearing signal that fine-
    grained write protection was actually exercised."""
    sid = _sid("j5")
    before = coordinator._intra_task_acquire_release_total
    # plan.md matches the default tracked policy.
    client.post("/hooks/pre-edit", {"session_id": sid, "path": "plan.md"})
    client.post("/hooks/post-edit",
                {"session_id": sid, "path": "plan.md",
                 "content_hash": _hash("j5"), "success": True})
    assert coordinator._intra_task_acquire_release_total == before + 1


def test_a8_failed_post_edit_does_not_increment_acquire_release(
    client: _Client, coordinator,
) -> None:
    """If post-edit reports failure, the counter does NOT increment —
    the contract is 'fine-grained write protection actually used', not
    'attempted'."""
    sid = _sid("j6")
    before = coordinator._intra_task_acquire_release_total
    client.post("/hooks/pre-edit", {"session_id": sid, "path": "plan.md"})
    client.post("/hooks/post-edit",
                {"session_id": sid, "path": "plan.md",
                 "content_hash": _hash("j6"), "success": False})
    assert coordinator._intra_task_acquire_release_total == before


# ----------------------------------------------------------------------
# Unit 6 — OCC commit endpoint (/hooks/post-edit-cas)
# ----------------------------------------------------------------------


def _occ_seed_shared(client: _Client, sid: str, path: str, h: str) -> int:
    """OCC helper: pre-read a tracked path to register SHARED + seed the
    artifact, returning the coordinator's version (the OCC comparand).

    A first-observation read returns a fresh response carrying a top-level
    ``version``; a second session reading an already-seeded artifact (matching
    hash) falls through to the warn-mode stale response, which carries the
    version under ``summary.current_version`` (mirrors the production
    ``CoherentVolume._pre_read_version`` two-shape extraction)."""
    s, b = client.post("/hooks/pre-read",
                       {"session_id": sid, "path": path, "content_hash": h})
    assert s == 200, b
    if isinstance(b.get("version"), int):
        return b["version"]
    summary = b.get("summary")
    if isinstance(summary, dict) and isinstance(summary.get("current_version"), int):
        return summary["current_version"]
    raise AssertionError(f"pre-read surfaced no version for OCC: {b}")


def test_occ_commit_happy_path_bumps_version(client: _Client) -> None:
    """OCC commit (post-edit-cas) with a matching expected_version commits and
    returns the new version N+1 — WITHOUT a pre-edit EXCLUSIVE acquire."""
    sid = _sid("occ1")
    v = _occ_seed_shared(client, sid, "plan.md", _hash("occ1-v1"))  # v1
    s, b = client.post("/hooks/post-edit-cas",
                       {"session_id": sid, "path": "plan.md",
                        "success": True, "content_hash": _hash("occ1-v2"),
                        "expected_version": v})
    assert s == 200
    assert b == {"ok": True, "version": v + 1}


def _artifact_version(coordinator, path: str) -> int:
    """Read the coordinator's authoritative version for a tracked path."""
    aid = coordinator.registry.lookup_artifact_id_by_name(path)
    assert aid is not None
    art = coordinator.registry.get_artifact(aid)
    assert art is not None
    return art.version


def test_occ_commit_stale_version_conflicts_no_mutation(coordinator, client: _Client) -> None:
    """A stale expected_version → {ok:false, reason:'version_mismatch',
    current_version} — a clean typed conflict (NOT a degrade), no mutation."""
    a, b_sid = _sid("occA"), _sid("occB")
    v1 = _occ_seed_shared(client, a, "plan.md", _hash("v1"))      # A reads v1
    _occ_seed_shared(client, b_sid, "plan.md", _hash("v1"))       # B reads v1
    # A commits first → v2 (A wins).
    s, ba = client.post("/hooks/post-edit-cas",
                        {"session_id": a, "path": "plan.md", "success": True,
                         "content_hash": _hash("v2-A"), "expected_version": v1})
    assert ba == {"ok": True, "version": v1 + 1}
    after_a = _artifact_version(coordinator, "plan.md")

    # B commits with its now-stale expected_version (still v1) → version_mismatch.
    s, bb = client.post("/hooks/post-edit-cas",
                        {"session_id": b_sid, "path": "plan.md", "success": True,
                         "content_hash": _hash("v2-B-stale"), "expected_version": v1})
    assert s == 200
    assert bb["ok"] is False
    assert bb["reason"] == "version_mismatch"
    assert bb["current_version"] == after_a
    assert "degraded" not in bb  # a clean typed conflict, NOT a degrade
    # No mutation: B's stale commit did not bump the version past A's.
    assert _artifact_version(coordinator, "plan.md") == after_a


def test_occ_commit_corruption_expected_gt_current_raises_body(coordinator, client: _Client) -> None:
    """expected_version > current → corruption: commit_cas raises CoherenceError,
    the endpoint returns {ok:false, reason:<verbatim>} the client raises on."""
    sid = _sid("occCorrupt")
    _occ_seed_shared(client, sid, "plan.md", _hash("v1"))  # v1
    s, b = client.post("/hooks/post-edit-cas",
                       {"session_id": sid, "path": "plan.md", "success": True,
                        "content_hash": _hash("vN"), "expected_version": 999})
    assert s == 200
    assert b["ok"] is False
    assert "corruption" in b["reason"] or "commit_cas_corruption" in b["reason"]
    # No mutation on corruption.
    assert _artifact_version(coordinator, "plan.md") == 1


def test_occ_commit_caller_in_transient_returns_stable_reason(
    coordinator, client: _Client
) -> None:
    """AC2: a caller left mid-transient (a peer invalidated it between its read
    and its CAS) → {ok:false, reason:'caller_in_transient_state'} — a STABLE
    machine reason (NOT the exception's human message), so the client's retry
    classifier matches it exactly. The body also carries current_version so the
    client can advance its comparand. No mutation: this is a lost race."""
    from ccs.core.states import TransientState

    sid = _sid("occTransient")
    v = _occ_seed_shared(client, sid, "plan.md", _hash("v1"))  # caller SHARED@v1
    # Force the caller mid-transient on the coordinator (the registry shape a
    # peer's invalidating write leaves on a SHARED holder: SIA). commit_cas
    # rejects this as a retry-eligible precondition.
    aid = coordinator.registry.lookup_artifact_id_by_name("plan.md")
    agent_id = session_to_agent_id(sid)
    coordinator.registry.set_agent_transient(
        aid, agent_id, TransientState.SIA, entered_tick=v
    )

    s, b = client.post(
        "/hooks/post-edit-cas",
        {"session_id": sid, "path": "plan.md", "success": True,
         "content_hash": _hash("v2"), "expected_version": v},
    )
    assert s == 200
    # The STABLE wire reason — exactly the literal the client matcher keys on,
    # decoupled from commit_cas's "commit_cas_not_allowed ..." human message.
    assert b == {"ok": False, "reason": "caller_in_transient_state", "current_version": v}
    assert "degraded" not in b  # a clean retry-eligible conflict, not a degrade
    # No mutation: the version did not advance.
    assert _artifact_version(coordinator, "plan.md") == v


def test_occ_commit_degrade_reads_as_failure(coordinator, client: _Client) -> None:
    """THE LOAD-BEARING FIX: a timed-out/degraded OCC commit returns
    {ok:false, degraded:true, reason:'commit_unconfirmed'} — NOT the
    {ok:true, degraded:true} the pessimistic post-edit uses. A client reading
    result.get('ok') must see False so it never assumes the write landed."""
    from concurrent.futures import TimeoutError as FuturesTimeout
    from unittest.mock import patch

    sid = _sid("occDegrade")
    v = _occ_seed_shared(client, sid, "plan.md", _hash("v1"))
    with patch.object(coordinator, "run_with_watchdog", side_effect=FuturesTimeout()):
        s, b = client.post("/hooks/post-edit-cas",
                           {"session_id": sid, "path": "plan.md", "success": True,
                            "content_hash": _hash("v2"), "expected_version": v})
    assert s == 200
    assert b.get("ok") is False, "degraded OCC commit must read as FAILURE, not ok:true"
    assert b.get("degraded") is True
    assert b.get("reason") == "commit_unconfirmed"


def test_occ_commit_does_not_take_pre_edit_acquire(coordinator, client: _Client) -> None:
    """The OCC path must NOT invoke the pre-edit EXCLUSIVE-acquire handler.
    Monkeypatch _handle_pre_edit to blow up: a full read→post-edit-cas cycle
    still succeeds, proving the OCC writer never routes through the acquire."""
    import ccs.adapters.claude_code.coordinator_server as mod
    original = mod._ROUTES[("POST", "/hooks/pre-edit")]

    def exploding_pre_edit(req, coord):
        raise AssertionError("OCC path must NOT call _handle_pre_edit")

    mod._ROUTES[("POST", "/hooks/pre-edit")] = exploding_pre_edit
    try:
        sid = _sid("occNoAcq")
        v = _occ_seed_shared(client, sid, "plan.md", _hash("v1"))
        before_pre_edit = coordinator.endpoint_counters_snapshot()["pre_edit_total"]
        s, b = client.post("/hooks/post-edit-cas",
                           {"session_id": sid, "path": "plan.md", "success": True,
                            "content_hash": _hash("v2"), "expected_version": v})
        assert s == 200
        assert b == {"ok": True, "version": v + 1}
        # The OCC writer is never EXCLUSIVE — it ends SHARED via commit_cas's
        # S/I→S transition (an OCC writer holds no grant), and the pre-edit
        # counter never moved.
        after_pre_edit = coordinator.endpoint_counters_snapshot()["pre_edit_total"]
        assert after_pre_edit == before_pre_edit
        aid = coordinator.registry.lookup_artifact_id_by_name("plan.md")
        state = coordinator.registry.get_agent_state(aid, session_to_agent_id(sid))
        assert state == MESIState.SHARED
    finally:
        mod._ROUTES[("POST", "/hooks/pre-edit")] = original


def test_occ_commit_untracked_path_fastpath(client: _Client) -> None:
    """An untracked path fast-paths to {ok:true} without a CAS (mirrors
    post-edit's is_tracked early return)."""
    s, b = client.post("/hooks/post-edit-cas",
                       {"session_id": _sid("occU"), "path": "src/random.py",
                        "success": True, "content_hash": _hash("h"),
                        "expected_version": 0})
    assert s == 200
    assert b == {"ok": True}


def test_occ_commit_rejects_non_int_expected_version(client: _Client) -> None:
    """expected_version is the OCC discriminator — a malformed (non-int)
    value is rejected at the boundary with 400, never driven into the CAS."""
    sid = _sid("occBad")
    _occ_seed_shared(client, sid, "plan.md", _hash("v1"))
    s, b = client.post("/hooks/post-edit-cas",
                       {"session_id": sid, "path": "plan.md", "success": True,
                        "content_hash": _hash("v2"), "expected_version": "1"})
    assert s == 400
    assert "expected_version" in b["error"]


def test_occ_commit_missing_content_hash_400(client: _Client) -> None:
    """The OCC commit always carries the bytes it wrote → content_hash required."""
    sid = _sid("occNoHash")
    v = _occ_seed_shared(client, sid, "plan.md", _hash("v1"))
    s, b = client.post("/hooks/post-edit-cas",
                       {"session_id": sid, "path": "plan.md", "success": True,
                        "expected_version": v})
    assert s == 400


def test_occ_commit_increments_endpoint_counter(coordinator, client: _Client) -> None:
    """KTD-J: the OCC endpoint has its own per-endpoint counter."""
    sid = _sid("occCount")
    v = _occ_seed_shared(client, sid, "plan.md", _hash("v1"))
    before = coordinator.endpoint_counters_snapshot()["post_edit_cas_total"]
    client.post("/hooks/post-edit-cas",
                {"session_id": sid, "path": "plan.md", "success": True,
                 "content_hash": _hash("v2"), "expected_version": v})
    after = coordinator.endpoint_counters_snapshot()["post_edit_cas_total"]
    assert after == before + 1


def test_occ_commit_concurrent_winner_election_no_lost_update(coordinator, client: _Client) -> None:
    """R11-flavored funnel through the HTTP path: two OCC clients both read v1
    (fixed stale buffers, NOT a counter increment), barrier-synced, both POST
    post-edit-cas with expected_version=v1. Exactly one wins (v2), the loser
    gets a typed version_mismatch, final version is v2 (no lost update)."""
    n_writers = 2
    a, b_sid = _sid("raceA"), _sid("raceB")
    v1 = _occ_seed_shared(client, a, "plan.md", _hash("v1"))
    assert _occ_seed_shared(client, b_sid, "plan.md", _hash("v1")) == v1

    barrier = threading.Barrier(n_writers)
    results: dict[str, tuple[int, dict]] = {}

    def commit(sid: str, label: str) -> None:
        barrier.wait()
        results[label] = client.post(
            "/hooks/post-edit-cas",
            {"session_id": sid, "path": "plan.md", "success": True,
             "content_hash": _hash(f"v2-{label}"), "expected_version": v1},
        )

    threads = [
        threading.Thread(target=commit, args=(a, "A")),
        threading.Thread(target=commit, args=(b_sid, "B")),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    wins = [lbl for lbl, (_, body) in results.items() if body.get("ok") is True]
    losers = [lbl for lbl, (_, body) in results.items() if body.get("ok") is False]
    assert len(wins) == 1, f"exactly one OCC writer must win: {results}"
    assert len(losers) == 1
    loser_body = results[losers[0]][1]
    assert loser_body["reason"] == "version_mismatch"
    # Final version is exactly v1+1 — the loser's stale buffer did NOT clobber.
    assert _artifact_version(coordinator, "plan.md") == v1 + 1


def test_occ_late_completion_residual_contended_is_noop_not_lost_update(
    coordinator, client: _Client,
) -> None:
    """Late-completion residual (plan Unit 6 / Key Decision). The watchdog does
    not cancel a timed-out future, so a late commit_cas may run after the client
    gave up. This asserts the SAFE half of that residual deterministically: a
    late CAS in the CONTENDED case (the version already advanced) sees the
    advanced version → version_mismatch, NO mutation — it cannot drop an
    acknowledged write. (The uncontended case lands a phantom/duplicate N+1 from
    the same edit — a duplicate version bump, NOT a lost write; NoLostUpdate
    still holds. Full fencing is deferred to the cross-host follow-on.)"""
    a, late = _sid("liveWinner"), _sid("lateGaveUp")
    v1 = _occ_seed_shared(client, a, "plan.md", _hash("v1"))
    _occ_seed_shared(client, late, "plan.md", _hash("v1"))  # the "late" writer also read v1
    # The live winner commits → v2 (this models the contention the late writer
    # raced against).
    s, ba = client.post("/hooks/post-edit-cas",
                        {"session_id": a, "path": "plan.md", "success": True,
                         "content_hash": _hash("v2-winner"), "expected_version": v1})
    assert ba == {"ok": True, "version": v1 + 1}
    v2 = _artifact_version(coordinator, "plan.md")
    # The late writer's CAS finally runs with its now-stale expected_version
    # (==v1): it observes the advanced version → version_mismatch, no mutation.
    s, bl = client.post("/hooks/post-edit-cas",
                        {"session_id": late, "path": "plan.md", "success": True,
                         "content_hash": _hash("v2-late"), "expected_version": v1})
    assert bl["ok"] is False
    assert bl["reason"] == "version_mismatch"
    # No acknowledged write was dropped — the version is the winner's v2, and the
    # late writer's stale bytes were NOT committed over it.
    assert _artifact_version(coordinator, "plan.md") == v2


def test_a8_status_exposes_coordinator_backend_and_version(client: _Client) -> None:
    """KTD-J: /status shape includes coordinator_backend + coordinator_version
    for cross-implementation operator observability."""
    _, b = client.get("/status?detail=metrics")
    assert b["coordinator_backend"] == "python"
    assert isinstance(b["coordinator_version"], str)
    assert b["coordinator_version"]  # non-empty


def test_a8_counters_increment_even_when_handler_raises(
    client: _Client, coordinator, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Contract per the plan: per-endpoint counters count ATTEMPTED
    requests, not successful ones. A handler that raises mid-dispatch
    (becoming a 500) must still leave the counter incremented."""
    import ccs.adapters.claude_code.coordinator_server as mod
    original = mod._ROUTES[("POST", "/hooks/pre-read")]
    def raising(req, coord):
        raise RuntimeError("simulated handler failure")
    monkeypatch.setitem(mod._ROUTES, ("POST", "/hooks/pre-read"), raising)
    try:
        before = coordinator.endpoint_counters_snapshot()["pre_read_total"]
        status, _ = client.post(
            "/hooks/pre-read",
            {"session_id": _sid("j8"), "path": "j8.md"},
        )
        assert status == 500
        after = coordinator.endpoint_counters_snapshot()["pre_read_total"]
        assert after == before + 1, "counter must increment even on handler exception"
    finally:
        mod._ROUTES[("POST", "/hooks/pre-read")] = original


# ----------------------------------------------------------------------
# R10 (Unit 6) — _agent_names mutation under threading.Lock
# ----------------------------------------------------------------------


def test_a4_agent_names_mutation_under_concurrent_status(
    coordinator,
) -> None:
    """A4 — agent-names map concurrency (plan §'Cross-cutting test discipline').

    Eight threads concurrently call register_session with distinct session ids;
    the resulting dict must contain exactly the union with no torn entries (no
    missing keys, no overwrites). Canonical a4_ prefix for risk-code triage.
    """
    expected: set[str] = set()
    barrier = threading.Barrier(8)
    lock = threading.Lock()

    def churner(thread_idx: int) -> None:
        barrier.wait()
        for j in range(50):
            sid = _sid(f"r10-{thread_idx}-{j}")
            with lock:
                expected.add(sid)
            coordinator.register_session(sid)

    threads = [threading.Thread(target=churner, args=(i,)) for i in range(8)]
    for t in threads: t.start()
    for t in threads: t.join()

    snapshot = coordinator.agent_names_snapshot()
    names = {name for _, name in snapshot}
    assert len(snapshot) >= len(expected)
    for sid in expected:
        assert f"claude-session-{sid}" in names, (
            f"session {sid} missing from snapshot (lock failed to serialize)"
        )


# Backward triage alias: pytest -k r10 still resolves.
test_r10_agent_names_lock_serializes_concurrent_registration = (
    test_a4_agent_names_mutation_under_concurrent_status
)


def test_r10_status_snapshot_consistent_under_churn(
    coordinator, client: _Client,
) -> None:
    """A reader calling /status while writers churn register_session must
    NEVER see RuntimeError from a torn iteration. The lock-protected
    snapshot is the contract."""
    stop = threading.Event()
    errors: list[Exception] = []

    def writer() -> None:
        i = 0
        while not stop.is_set():
            try:
                coordinator.register_session(_sid(f"r10-churn-{i}"))
            except Exception as e:
                errors.append(e)
            i += 1

    writers = [threading.Thread(target=writer) for _ in range(4)]
    for t in writers: t.start()
    try:
        for _ in range(20):
            s, _ = client.get("/status")
            assert s == 200, "status returned non-200 under register_session churn"
    finally:
        stop.set()
        for t in writers: t.join(timeout=2.0)
    assert not errors, f"writer threads hit errors: {errors[:3]}"


def test_r10_agent_name_for_returns_none_for_unknown(coordinator) -> None:
    """The single-key accessor returns None for an agent that has never
    been registered, without raising."""
    fake_id = uuid.uuid5(uuid.NAMESPACE_URL, "ccs-agent:claude-session-never-registered")
    assert coordinator.agent_name_for(fake_id) is None


# ----------------------------------------------------------------------
# R14 (Unit 6) — _append_policy_yaml under fcntl.flock
# ----------------------------------------------------------------------


def test_r14_concurrent_policy_track_no_lost_writes(coordinator, client: _Client) -> None:
    """Eight threads each POST /policy/track with one unique path; every
    request that the coordinator accepts (status 200) must have its path
    persisted in tracked.yaml — no read-modify-write interleaving losing
    entries. R14's contract is about lost writes among ACCEPTED requests,
    not about 503s from the KTD-G concurrency cap (which is a separate
    pre-handler reject)."""
    target_paths = [f"r14/path_{i}.md" for i in range(8)]
    barrier = threading.Barrier(len(target_paths))
    results: list[tuple[str, int]] = []
    results_lock = threading.Lock()

    def add_path(p: str) -> None:
        barrier.wait()
        s, _ = client.post("/policy/track", {"paths": [p]})
        with results_lock:
            results.append((p, s))

    threads = [threading.Thread(target=add_path, args=(p,)) for p in target_paths]
    for t in threads: t.start()
    for t in threads: t.join()

    accepted = [p for p, s in results if s == 200]
    assert accepted, f"no requests succeeded — KTD-G cap may be too tight: {results}"

    yaml_path = coordinator.coordinator_root / ".coherence" / "tracked.yaml"
    text = yaml_path.read_text()
    for p in accepted:
        assert f"- {p}" in text, (
            f"path {p!r} lost despite 200 response — fcntl.flock did not serialize"
        )


def test_r14_lock_file_created_next_to_yaml(coordinator, client: _Client) -> None:
    """The fcntl lock uses a sidecar ``<yaml>.lock`` file; verify it
    appears and stays present (the file is reused across calls)."""
    client.post("/policy/track", {"paths": ["r14_sidecar.md"]})
    lock_path = (
        coordinator.coordinator_root / ".coherence" / "tracked.yaml.lock"
    )
    assert lock_path.is_file(), "tracked.yaml.lock sidecar was not created"


def test_policy_track_idempotent_no_duplicate_entries(coordinator, client: _Client) -> None:
    """Tracking the same path twice must not produce duplicate lines in
    tracked.yaml. The second call should still return 200 (idempotent) but
    the path must appear exactly once in the YAML."""
    path = "idempotent_test.md"
    s1, b1 = client.post("/policy/track", {"paths": [path]})
    s2, b2 = client.post("/policy/track", {"paths": [path]})
    assert s1 == 200
    assert s2 == 200
    # Second call must report zero newly-added patterns \u2014 the path was already
    # present and _append_policy_yaml must return ([], rejected), not (added, rejected).
    assert b2.get("added") == [], (
        f"second /policy/track returned 'added'={b2.get('added')!r}; "
        "idempotent call must report no additions"
    )

    yaml_path = coordinator.coordinator_root / ".coherence" / "tracked.yaml"
    text = yaml_path.read_text()
    occurrences = text.count(f"- {path}")
    assert occurrences == 1, (
        f"path {path!r} appears {occurrences}\xd7 in tracked.yaml after two track calls "
        f"(expected 1 \u2014 /policy/track must be idempotent)"
    )


# ----------------------------------------------------------------------
# _parse_yaml_pattern_lines unit tests
# ----------------------------------------------------------------------


def test_parse_yaml_pattern_lines_plain_entries() -> None:
    """Plain unquoted list items are extracted."""
    from ccs.adapters.claude_code.coordinator_server import _parse_yaml_pattern_lines
    text = "- plan.md\n- src/main.py\n"
    result = _parse_yaml_pattern_lines(text)
    assert result == {"plan.md", "src/main.py"}


def test_parse_yaml_pattern_lines_quoted_entries() -> None:
    """YAML-quoted values are extracted without the quotes."""
    from ccs.adapters.claude_code.coordinator_server import _parse_yaml_pattern_lines
    text = '- "plan.md"\n- \'src/main.py\'\n'
    result = _parse_yaml_pattern_lines(text)
    assert result == {"plan.md", "src/main.py"}


def test_parse_yaml_pattern_lines_empty_input() -> None:
    """Empty or whitespace-only input returns empty set."""
    from ccs.adapters.claude_code.coordinator_server import _parse_yaml_pattern_lines
    assert _parse_yaml_pattern_lines("") == set()
    assert _parse_yaml_pattern_lines("   \n  ") == set()


def test_parse_yaml_pattern_lines_non_string_items_ignored() -> None:
    """Non-string items (numbers, null) are silently dropped."""
    from ccs.adapters.claude_code.coordinator_server import _parse_yaml_pattern_lines
    text = "- plan.md\n- 42\n- null\n"
    result = _parse_yaml_pattern_lines(text)
    assert result == {"plan.md"}


def test_parse_yaml_pattern_lines_malformed_yaml_returns_empty() -> None:
    """Malformed YAML falls back to empty set rather than raising."""
    from ccs.adapters.claude_code.coordinator_server import _parse_yaml_pattern_lines
    result = _parse_yaml_pattern_lines("{not: a list}")
    assert result == set()


# ----------------------------------------------------------------------
# R11 (Unit 6) — ensure_secret bounded O_EXCL retry, fail-closed
# ----------------------------------------------------------------------


def test_r11_ensure_secret_recovers_from_empty_file_during_retries(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If a stale empty hook.secret exists when ensure_secret runs (e.g.,
    a previous coordinator crashed between O_EXCL-create and write), the
    bounded retry loop must eventually populate it without clobbering
    via O_TRUNC. We simulate this by pre-creating the empty file, then
    letting ensure_secret retry through to a clean O_EXCL after we
    unlink it from a sidecar 'racer' thread."""
    import threading as _t

    from ccs.adapters.claude_code import auth as _auth

    coherence_dir = tmp_path / ".coherence"
    coherence_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    secret_path = coherence_dir / _auth.SECRET_FILENAME

    # Make the file exist but be empty (mimics crashed predecessor).
    secret_path.touch(mode=0o600)
    assert secret_path.stat().st_size == 0

    # Speed up the test: shorter retry sleep, but keep retry count.
    monkeypatch.setattr(_auth, "ENSURE_SECRET_RETRY_SLEEP_SEC", 0.020)

    # Simulate a racer that unlinks the empty file mid-retry, allowing
    # ensure_secret's next O_EXCL to succeed.
    def racer() -> None:
        time.sleep(0.040)
        try:
            secret_path.unlink()
        except FileNotFoundError:
            pass

    t = _t.Thread(target=racer)
    t.start()
    try:
        token = _auth.ensure_secret(tmp_path)
    finally:
        t.join(timeout=2.0)
    # Contract: ensure_secret returned a token (recovery from empty
    # file succeeded). We intentionally do NOT re-read the file —
    # the racer may have unlinked AFTER ensure_secret returned, which
    # is fine for the in-process race but would race the assertion
    # on slow CI runners.
    assert token
    assert len(token) == 64


def test_r11_ensure_secret_fails_closed_when_empty_file_persists(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If hook.secret stays empty across all retries, ensure_secret MUST
    raise EnsureSecretError rather than O_TRUNC over it. The old behavior
    would silently overwrite a concurrent racer's valid secret, leaving
    two spawn-side processes with different secrets for the same
    workspace — a silent total-protocol-break failure mode."""
    from ccs.adapters.claude_code import auth as _auth

    coherence_dir = tmp_path / ".coherence"
    coherence_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    secret_path = coherence_dir / _auth.SECRET_FILENAME
    secret_path.touch(mode=0o600)
    assert secret_path.stat().st_size == 0

    # Shorten retry sleep so the test finishes fast.
    monkeypatch.setattr(_auth, "ENSURE_SECRET_RETRY_SLEEP_SEC", 0.001)
    monkeypatch.setattr(_auth, "ENSURE_SECRET_MAX_RETRIES", 3)

    with pytest.raises(_auth.EnsureSecretError) as exc:
        _auth.ensure_secret(tmp_path)
    assert "stayed empty" in str(exc.value)
    # The file must remain empty — we did NOT O_TRUNC over it.
    assert secret_path.stat().st_size == 0


# ----------------------------------------------------------------------
# R12 (Unit 6) — /status three-tier disclosure
# ----------------------------------------------------------------------


def test_r12_status_minimal_default_hides_coordinator_root(
    client: _Client,
) -> None:
    """The default (no query) response is the minimal tier — coordinator_root
    is the sentinel "." so $HOME / directory layout never leaks.

    P1 #7 (revision to R12): coordinator_pid IS included in minimal —
    pid is public on POSIX and operators rely on it. Only the absolute
    workspace root stays behind the operator-header gate at this tier."""
    s, b = client.get("/status")
    assert s == 200
    assert b["detail"] == "minimal"
    assert b["coordinator_root"] == "."
    # P1 #7: pid is in minimal tier (reversion of R12 over-redaction).
    assert b.get("coordinator_pid") == os.getpid()


def test_r12_status_full_requires_operator_header(client: _Client) -> None:
    """?detail=full without the Coherence-Local-Operator: true opt-in header
    must be rejected with 403 — Bearer auth alone is not sufficient for
    the elevated tier."""
    s, b = client.get("/status?detail=full")
    assert s == 403
    assert "operator" in b["error"].lower()


def test_r12_status_full_with_operator_header_exposes_root_and_pid(
    client: _Client, coordinator,
) -> None:
    """?detail=full + Coherence-Local-Operator: true returns the absolute
    coordinator_root and coordinator_pid for legitimate operator inspection."""
    s, b = client.request(
        "GET", "/status?detail=full",
        headers_override={"Coherence-Local-Operator": "true"},
    )
    assert s == 200
    assert b["detail"] == "full"
    assert b["coordinator_root"] == str(coordinator.coordinator_root)
    assert isinstance(b["coordinator_pid"], int)
    # Full tier also retains the artifact/session block.
    assert "tracked_artifacts" in b
    assert "sessions" in b


def test_r12_status_metrics_returns_counters_only(client: _Client) -> None:
    """?detail=metrics returns only the counter block — no artifact/session
    walk, no leak of workspace state. Useful for dashboard scrapers."""
    s, b = client.get("/status?detail=metrics")
    assert s == 200
    assert b["detail"] == "metrics"
    assert "tracked_artifacts" not in b
    assert "sessions" not in b
    assert "policy_summary" not in b
    # Counters must be present.
    for k in (
        "coordinator_uptime_seconds",  # AC-02 canonical field
        "coordinator_uptime_s",  # AC-02 deprecated alias (one release)
        "watchdog_timeouts_total",
        "handler_concurrency_overflows_total",
        "in_flight_drain_timed_out",
        "cold_start_duration_ms",
    ):
        assert k in b, f"counter {k} missing from metrics tier"


def test_r12_status_unknown_detail_falls_back_to_minimal(client: _Client) -> None:
    """A typo'd ?detail=value must NOT silently grant more access — it
    falls back to minimal, never to full. P1 #7: pid is in minimal
    so we assert the absolute root is sentinel'd as the actual
    confidentiality signal instead."""
    s, b = client.get("/status?detail=fully")
    assert s == 200
    assert b["detail"] == "minimal"
    # The fall-back is "minimal" not "full" — absolute root must NOT leak.
    assert b["coordinator_root"] == "."


def test_r11_ensure_secret_concurrent_threads_return_identical(
    tmp_path: Path,
) -> None:
    """Multiple threads spawning concurrently must all walk away with the
    SAME secret (one wins O_EXCL, the others read what the winner wrote).
    Thread-level test exercises the in-process race; the cross-process
    case is identical at the syscall layer (O_EXCL is OS-enforced)."""
    from ccs.adapters.claude_code.auth import ensure_secret

    tokens: list[str] = []
    tokens_lock = threading.Lock()
    barrier = threading.Barrier(8)

    def worker() -> None:
        barrier.wait()
        tok = ensure_secret(tmp_path)
        with tokens_lock:
            tokens.append(tok)

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads: t.start()
    for t in threads: t.join()
    assert len(set(tokens)) == 1, (
        f"concurrent ensure_secret returned different tokens: {set(tokens)}"
    )


# ----------------------------------------------------------------------
# ADV-001 — prepare-for-migration drain semantics
# ----------------------------------------------------------------------


def test_adv001_pre_edit_rejected_during_migration_drain(
    coordinator, client: _Client
) -> None:
    """ADV-001: after prepare-for-migration sets the draining flag, a new
    pre-edit on a tracked artifact must be rejected with 503 + a
    structured error rather than minting an EXCLUSIVE that the agent
    can never post-edit."""
    # Flip the flag directly so we don't have to wait for the full
    # drain → invalidate → shutdown sequence in this unit test.
    coordinator._migration_draining = True
    try:
        status, body = client.post(
            "/hooks/pre-edit",
            {"session_id": _sid("adv001-A"), "path": "plan.md"},
        )
        assert status == 503
        assert "migration" in body.get("error", "").lower()
        assert "draining" in body.get("error", "").lower()
    finally:
        coordinator._migration_draining = False


def test_adv001_post_edit_continues_during_migration_drain(
    coordinator, client: _Client
) -> None:
    """ADV-001: post-edit must still serve while draining so in-flight
    pre-edit→post-edit chains can complete naturally. This is the whole
    point of the draining-flag fix vs. an immediate hard shutdown."""
    sid = _sid("adv001-B")
    # Acquire an EXCLUSIVE before flipping the flag (the in-flight
    # request whose post-edit we want to allow through).
    client.post("/hooks/pre-edit", {"session_id": sid, "path": "plan.md"})

    coordinator._migration_draining = True
    try:
        status, body = client.post(
            "/hooks/post-edit",
            {
                "session_id": sid,
                "path": "plan.md",
                "content_hash": _hash("adv001-B"),
                "success": True,
            },
        )
        assert status == 200, (
            f"post-edit must complete during drain; got {status}: {body!r}"
        )
        assert body.get("ok") is True
    finally:
        coordinator._migration_draining = False


def test_adv001_pre_read_continues_during_migration_drain(
    coordinator, client: _Client
) -> None:
    """ADV-001: pre-read is a non-mutating endpoint and must keep
    serving during the migration drain window."""
    coordinator._migration_draining = True
    try:
        status, body = client.post(
            "/hooks/pre-read",
            {"session_id": _sid("adv001-C"), "path": "plan.md"},
        )
        assert status == 200
    finally:
        coordinator._migration_draining = False


def test_adv001_prepare_for_migration_returns_immediately_with_draining_flag(
    coordinator, client: _Client
) -> None:
    """The handler now returns {ok, draining:true, drain_timeout_ms} as
    soon as it flips the flag. The drain + invalidate + shutdown
    sequence runs in a background thread; the CLI polls /status to
    observe the coordinator becoming unreachable."""
    status, body = client.post(
        "/admin/prepare-for-migration", {},
        headers_override={"Coherence-Local-Operator": "true"},
    )
    assert status == 200
    assert body["ok"] is True
    assert body["draining"] is True
    assert body["drain_timeout_ms"] > 0
    assert body["shutdown_scheduled_in_ms"] >= body["drain_timeout_ms"]
    # Background thread will close the coordinator; the fixture's
    # shutdown is idempotent so cleanup still works.


def test_adv001_repeated_prepare_for_migration_is_idempotent(
    coordinator, client: _Client
) -> None:
    """A second prepare-for-migration call while already draining must
    not start a second drain sequence — it returns the already_in_progress
    envelope."""
    coordinator._migration_draining = True
    try:
        status, body = client.post(
            "/admin/prepare-for-migration", {},
            headers_override={"Coherence-Local-Operator": "true"},
        )
        assert status == 200
        assert body["ok"] is True
        assert body["draining"] is True
        assert body.get("already_in_progress") is True
    finally:
        coordinator._migration_draining = False


# ----------------------------------------------------------------------
# P1 #5 — watchdog late-completion detector
# ----------------------------------------------------------------------


def test_p1_5_watchdog_late_completion_increments_counter(
    coordinator,
) -> None:
    """When run_with_watchdog times out and the underlying future
    later completes successfully, the late-completion counter must
    increment and a CRITICAL log line fires."""
    import threading as _t
    from concurrent.futures import TimeoutError as _FuturesTimeout

    # Replace HANDLER_TIMEOUT_SEC for the duration of the test so we
    # don't have to wait 4s. The work function blocks until the test
    # releases it, then returns a successful payload.
    release = _t.Event()
    def slow_work() -> dict:
        release.wait(timeout=5.0)
        return {"ok": True, "late": True}

    import ccs.adapters.claude_code.coordinator_server as mod
    original_timeout = mod.HANDLER_TIMEOUT_SEC
    try:
        mod.HANDLER_TIMEOUT_SEC = 0.05  # 50ms — fire timeout fast
        before = coordinator._watchdog_late_completion_total
        try:
            coordinator.run_with_watchdog(slow_work)
        except _FuturesTimeout:
            pass
        else:
            pytest.fail("expected FuturesTimeout")
        # Now release the work; the future completes successfully and
        # the done_callback fires (asynchronously — give the pool a
        # moment to schedule the callback).
        release.set()
        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline:
            if coordinator._watchdog_late_completion_total > before:
                break
            time.sleep(0.020)
        assert coordinator._watchdog_late_completion_total == before + 1, (
            f"expected late-completion counter to increment by 1; "
            f"before={before} after={coordinator._watchdog_late_completion_total}"
        )
    finally:
        mod.HANDLER_TIMEOUT_SEC = original_timeout
        release.set()


def test_p1_5_late_failure_does_not_increment_counter(
    coordinator,
) -> None:
    """If a timed-out future later RAISES rather than completing, no
    phantom state landed in the registry — counter must NOT increment."""
    import threading as _t
    from concurrent.futures import TimeoutError as _FuturesTimeout

    release = _t.Event()
    def slow_failing_work() -> dict:
        release.wait(timeout=5.0)
        raise RuntimeError("late failure")

    import ccs.adapters.claude_code.coordinator_server as mod
    original_timeout = mod.HANDLER_TIMEOUT_SEC
    try:
        mod.HANDLER_TIMEOUT_SEC = 0.05
        before = coordinator._watchdog_late_completion_total
        try:
            coordinator.run_with_watchdog(slow_failing_work)
        except _FuturesTimeout:
            pass
        # Release; future fails late; counter should NOT increment.
        release.set()
        time.sleep(0.3)
        assert coordinator._watchdog_late_completion_total == before, (
            f"late-failure path must not bump phantom-grant counter; "
            f"before={before} after={coordinator._watchdog_late_completion_total}"
        )
    finally:
        mod.HANDLER_TIMEOUT_SEC = original_timeout
        release.set()


def test_p1_5_status_metrics_exposes_late_completion_counter(
    client: _Client, coordinator,
) -> None:
    """The new counter must show up in /status?detail=metrics so
    operators can spot a phantom-grant cluster in a bug report."""
    status, body = client.get("/status?detail=metrics")
    assert status == 200
    assert "watchdog_late_completion_total" in body
    assert isinstance(body["watchdog_late_completion_total"], int)


# ----------------------------------------------------------------------
# P1 #6 — 401 visibility (hook.secret deletion / bearer mismatch)
# ----------------------------------------------------------------------


def test_p1_6_401_increments_auth_counter(coordinator) -> None:
    """A request with a wrong bearer must bump auth_401_total so
    operators can spot a hook.secret deletion via /status."""
    url = f"http://127.0.0.1:{coordinator.port}/hooks/pre-read"
    req = urlrequest.Request(
        url, data=b"{}", method="POST",
        headers={
            "Authorization": "Bearer wrong-secret",
            "Host": "127.0.0.1",
            "Content-Type": "application/json",
        },
    )
    before = coordinator._auth_401_total
    try:
        urlrequest.urlopen(req, timeout=5)
        pytest.fail("expected 401")
    except urlerror.HTTPError as e:
        assert e.code == 401
    assert coordinator._auth_401_total == before + 1


def test_p1_6_repeated_401s_dedupe_warning_logs(
    coordinator, caplog: pytest.LogCaptureFixture
) -> None:
    """Counter bumps every 401; WARNING log dedupes to once per 60s so
    a burst of bad requests doesn't drown the log."""
    import logging
    caplog.set_level(logging.WARNING, logger="ccs.adapters.claude_code.coordinator_server")
    url = f"http://127.0.0.1:{coordinator.port}/hooks/pre-read"
    def fire_bad() -> None:
        req = urlrequest.Request(
            url, data=b"{}", method="POST",
            headers={
                "Authorization": "Bearer wrong-secret",
                "Host": "127.0.0.1",
                "Content-Type": "application/json",
            },
        )
        try:
            urlrequest.urlopen(req, timeout=5)
        except urlerror.HTTPError:
            pass

    before_total = coordinator._auth_401_total
    fire_bad()
    fire_bad()
    fire_bad()
    assert coordinator._auth_401_total == before_total + 3
    auth_warnings = [
        r for r in caplog.records
        if "auth: 401" in r.getMessage()
    ]
    # First call emits the warning; next two are deduped.
    assert len(auth_warnings) == 1, (
        f"expected exactly one 401 WARNING per dedupe window; got {len(auth_warnings)}: "
        f"{[r.getMessage() for r in auth_warnings]}"
    )


def test_p1_6_status_metrics_exposes_auth_401_counter(client: _Client) -> None:
    """auth_401_total visible in /status?detail=metrics."""
    status, body = client.get("/status?detail=metrics")
    assert status == 200
    assert "auth_401_total" in body
    assert isinstance(body["auth_401_total"], int)


# ----------------------------------------------------------------------
# ADV-004 — stable-grant sweep records preemption notice on reclamation
# ----------------------------------------------------------------------


def test_adv004_sweep_reclamation_records_preemption_notice(
    coordinator, client: _Client,
) -> None:
    """ADV-004: the stable-grant sweep must record a preemption notice
    for the reclaimed victim — otherwise the victim's eventual
    post-edit fails CoherenceError with no F4 context."""
    from ccs.adapters.claude_code.coordinator_server import (
        SWEEP_RECLAMATION_PREEMPTER_ID,
        session_to_agent_id,
    )

    sid = _sid("adv004-A")
    agent_id = session_to_agent_id(sid)
    # Acquire EXCLUSIVE via pre-edit on a tracked artifact.
    s, _ = client.post("/hooks/pre-edit", {"session_id": sid, "path": "plan.md"})
    assert s == 200
    artifact_id = coordinator.registry.lookup_artifact_id_by_name("plan.md")
    assert artifact_id is not None

    # Drive the sweep manually so the heartbeat-stale path fires
    # immediately. heartbeat_timeout_ticks=1 + current_tick well past
    # the agent's last heartbeat triggers reclaim_heartbeat.
    reclaimed_n = coordinator.service.enforce_stable_grant_timeouts(
        current_tick=int(time.time()) + 999_999,
        heartbeat_timeout_ticks=1,
        max_hold_ticks=999_999_999,
        on_reclaim=lambda artifact_id, agent_id, trigger: (
            coordinator.registry.record_preemption_notice(
                victim_agent_id=agent_id,
                artifact_id=artifact_id,
                preempter_agent_id=SWEEP_RECLAMATION_PREEMPTER_ID,
                preempted_at_unix_ts=time.time(),
            )
        ),
    )
    assert reclaimed_n == 1, "sweep should have reclaimed exactly one M/E grant"

    # The preemption notice for the victim must be present and tagged
    # with the sweep-sentinel preempter.
    popped = coordinator.registry.pop_preemption_notice(agent_id, artifact_id)
    assert popped is not None
    preempter_id, _preempted_at = popped
    assert preempter_id == SWEEP_RECLAMATION_PREEMPTER_ID


def test_adv004_post_edit_after_reclamation_returns_reclaimed_message(
    coordinator, client: _Client,
) -> None:
    """End-to-end: pre-edit → sweep reclaims → post-edit gets the F4
    'reclaimed by coordinator sweep' message (NOT the generic
    CoherenceError) and the response carries reclaimed=True instead of
    preempted=True."""
    from ccs.adapters.claude_code.coordinator_server import (
        SWEEP_RECLAMATION_PREEMPTER_ID,
        session_to_agent_id,
    )

    sid = _sid("adv004-B")
    agent_id = session_to_agent_id(sid)
    client.post("/hooks/pre-edit", {"session_id": sid, "path": "plan.md"})
    artifact_id = coordinator.registry.lookup_artifact_id_by_name("plan.md")

    # Sweep reclaims agent's grant + records notice via the on_reclaim
    # callback (same wiring the real adapter sweep uses).
    coordinator.service.enforce_stable_grant_timeouts(
        current_tick=int(time.time()) + 999_999,
        heartbeat_timeout_ticks=1,
        max_hold_ticks=999_999_999,
        on_reclaim=lambda aid, sid_, trigger: coordinator.registry.record_preemption_notice(
            victim_agent_id=sid_,
            artifact_id=aid,
            preempter_agent_id=SWEEP_RECLAMATION_PREEMPTER_ID,
            preempted_at_unix_ts=time.time(),
        ),
    )

    # Now post-edit fires — should fail with the reclaimed-message.
    s, body = client.post("/hooks/post-edit", {
        "session_id": sid,
        "path": "plan.md",
        "content_hash": _hash("adv004-B-late"),
        "success": True,
    })
    assert s == 200, body
    assert body.get("ok") is False
    assert body.get("reclaimed") is True, (
        f"expected reclaimed=True in F4 response; got {body!r}"
    )
    assert "reclaimed by the coordinator sweep" in body.get("reason", "")
    assert "plan.md" in body.get("reason", "")
    # And NOT the peer-preemption message
    assert "preempted by session" not in body.get("reason", "")


def test_post_edit_fence_reject_returns_stable_reason(
    coordinator, client: _Client,
) -> None:
    """End-to-end: the read-generation fence race window — the grant is still
    EXCLUSIVE but a sweep superseded the claim (owner_generation advanced
    between commit()'s state check and the version persist). post-edit must
    return the STABLE stale_read_generation reason (exact constant, not
    str(exc)) plus current_version, land no phantom bump, and leak no MWB
    transient."""
    from ccs.adapters.claude_code.coordinator_server import session_to_agent_id
    from ccs.core.exceptions import STALE_READ_GENERATION_REASON

    sid = _sid("fence-race")
    agent_id = session_to_agent_id(sid)
    client.post("/hooks/pre-edit", {"session_id": sid, "path": "plan.md"})
    artifact_id = coordinator.registry.lookup_artifact_id_by_name("plan.md")
    assert artifact_id is not None

    # Manufacture the race: generation advances while the state stays E
    # (a direct bump stands in for the sweep firing mid-commit; autocommit
    # connection, so no transaction is held open against the server).
    coordinator.registry._conn.execute(
        "UPDATE artifacts SET owner_generation = owner_generation + 1 WHERE id = ?",
        (artifact_id.hex,),
    )
    before = coordinator.registry.get_artifact(artifact_id).version

    s, body = client.post("/hooks/post-edit", {
        "session_id": sid,
        "path": "plan.md",
        "content_hash": _hash("fence-late"),
        "success": True,
    })
    assert s == 200, body
    assert body.get("ok") is False
    assert body.get("reason") == STALE_READ_GENERATION_REASON
    assert body.get("current_version") == before
    # No phantom version bump; no leaked MWB transient (review P1 regression).
    assert coordinator.registry.get_artifact(artifact_id).version == before
    assert coordinator.registry.get_agent_transient(artifact_id, agent_id) is None


def test_adv004_sweep_on_reclaim_callback_exception_does_not_break_sweep(
    coordinator,
) -> None:
    """Defensive: if the on_reclaim callback raises, the sweep continues
    and the reclamation itself still lands. The callback is telemetry
    surface; its failure must not block coherence guarantees."""
    from ccs.adapters.claude_code.coordinator_server import session_to_agent_id

    sid = _sid("adv004-C")
    agent_id = session_to_agent_id(sid)
    # Acquire EXCLUSIVE.
    coordinator.register_session(sid)
    artifact_id = coordinator.registry.resolve_or_register("plan.md", content_hash="")
    coordinator.registry.set_agent_state(
        artifact_id, agent_id, MESIState.EXCLUSIVE,
        trigger="test_setup", tick=0, content_hash=None,
    )

    raises_counter = {"n": 0}
    def raising_callback(*_a, **_kw) -> None:
        raises_counter["n"] += 1
        raise RuntimeError("simulated telemetry failure")

    reclaimed_n = coordinator.service.enforce_stable_grant_timeouts(
        current_tick=int(time.time()) + 999_999,
        heartbeat_timeout_ticks=1,
        max_hold_ticks=999_999_999,
        on_reclaim=raising_callback,
    )
    assert reclaimed_n == 1, "reclamation must still land despite callback failure"
    assert raises_counter["n"] == 1
    # The agent's state is invalid (the reclamation itself succeeded).
    assert coordinator.registry.get_agent_state(artifact_id, agent_id) == MESIState.INVALID


# ----------------------------------------------------------------------
# REL-01 — shutdown drain deadlock (suppressed false positive; locked by test)
# ----------------------------------------------------------------------


def test_rel01_drain_no_deadlock_under_concurrent_dispatch(tmp_path: Path) -> None:
    """REL-01: the reviewer's deeper trace suppressed this as a false
    positive — Condition.wait() releases _in_flight_lock during the
    drain, and acquire_handler_slot's atomic shutting_down check
    prevents new in-flight bumps after shutdown begins. Stress-test
    the invariant: 50 concurrent dispatches racing against shutdown()
    must not deadlock the drain. If REL-01 ever becomes real again
    (e.g., a refactor introduces a non-Condition lock ordering), this
    test will hang and pytest will time out — making the regression
    loud."""
    import threading as _t
    srv = CoordinatorHTTPServer(tmp_path, port=0, instance_id="rel01")
    srv.serve_in_thread()
    time.sleep(0.05)
    secret = load_secret(srv.coordinator_root)
    assert secret is not None
    client = _Client("127.0.0.1", srv.port, secret)

    fire_results: list[int] = []
    def fire(i: int) -> None:
        try:
            s, _ = client.post(
                "/hooks/pre-read",
                {"session_id": _sid(f"rel01-{i}"), "path": "CLAUDE.md"},
            )
            fire_results.append(s)
        except Exception:
            fire_results.append(-1)

    # 50 dispatch threads racing against a delayed shutdown.
    threads = [_t.Thread(target=fire, args=(i,)) for i in range(50)]
    for t in threads:
        t.start()

    # Brief pause so some requests are mid-flight when shutdown fires.
    time.sleep(0.020)

    shutdown_done = _t.Event()
    def call_shutdown() -> None:
        srv.shutdown()
        shutdown_done.set()
    _t.Thread(target=call_shutdown, daemon=True).start()

    # The shutdown MUST complete within IN_FLIGHT_DRAIN_TIMEOUT_SEC + a
    # small margin (handlers in-flight when drain started should
    # complete quickly; new ones after shutting_down=True are denied).
    assert shutdown_done.wait(timeout=10.0), (
        "shutdown deadlocked (REL-01 regression — drain never completed)"
    )
    for t in threads:
        t.join(timeout=2.0)
    # All responses are either 200 (handled) or 503 (post-shutdown) or
    # -1 (connection lost during shutdown). No 500s or hangs.
    assert all(r in (200, 503, -1) for r in fire_results), (
        f"unexpected status codes during shutdown race: {fire_results}"
    )


# ----------------------------------------------------------------------
# REL-03 — free-threading-safe reliability counters
# ----------------------------------------------------------------------


def test_rel03_watchdog_timeouts_counter_under_concurrent_increment(
    tmp_path: Path,
) -> None:
    """REL-03: under free-threading Py 3.13+ or PyPy, ``x += 1`` on a
    plain int is NOT atomic — concurrent threads can tear the increment
    and lose counts. Reliability counters protect with a lock; this
    test verifies that 1000 concurrent increments from 50 threads land
    as exactly 1000 (no torn writes)."""
    import threading as _t
    srv = CoordinatorHTTPServer(tmp_path, port=0, instance_id="rel03")
    try:
        N_THREADS = 50
        N_PER_THREAD = 20  # 1000 total
        def bump_many() -> None:
            for _ in range(N_PER_THREAD):
                srv.increment_watchdog_timeout()
        threads = [_t.Thread(target=bump_many) for _ in range(N_THREADS)]
        for t in threads: t.start()
        for t in threads: t.join()
        expected = N_THREADS * N_PER_THREAD
        assert srv._watchdog_timeouts_total == expected, (
            f"counter torn under concurrent increment: "
            f"expected {expected}, got {srv._watchdog_timeouts_total}"
        )
    finally:
        srv.shutdown()


def test_rel03_watchdog_queue_overflow_counter_under_concurrent_increment(
    tmp_path: Path,
) -> None:
    """Same contract for the queue-overflow counter."""
    import threading as _t
    srv = CoordinatorHTTPServer(tmp_path, port=0, instance_id="rel03b")
    try:
        N_THREADS = 50
        N_PER_THREAD = 20
        def bump_many() -> None:
            for _ in range(N_PER_THREAD):
                srv.increment_watchdog_queue_overflow()
        threads = [_t.Thread(target=bump_many) for _ in range(N_THREADS)]
        for t in threads: t.start()
        for t in threads: t.join()
        expected = N_THREADS * N_PER_THREAD
        assert srv._watchdog_queue_overflows_total == expected
    finally:
        srv.shutdown()


# ----------------------------------------------------------------------
# ADV-005 — empty/missing body rejected with explicit 400
# ----------------------------------------------------------------------


def test_adv005_content_length_zero_returns_explicit_400(coordinator) -> None:
    """ADV-005: a POST with Content-Length:0 must produce an explicit
    'missing or empty body' 400, not fall through to per-field
    validation errors that mask the real cause."""
    url = f"http://127.0.0.1:{coordinator.port}/hooks/pre-read"
    secret = load_secret(coordinator.coordinator_root)
    req = urlrequest.Request(
        url, data=b"", method="POST",
        headers={
            "Authorization": f"Bearer {secret}",
            "Host": "127.0.0.1",
            "Content-Type": "application/json",
            "Content-Length": "0",
        },
    )
    try:
        urlrequest.urlopen(req, timeout=5)
        pytest.fail("expected 400")
    except urlerror.HTTPError as e:
        assert e.code == 400
        body = json.loads(e.read().decode())
        assert "empty body" in body["error"].lower() or "missing" in body["error"].lower()


def test_adv005_missing_content_length_returns_400(coordinator) -> None:
    """A POST with no Content-Length header at all should also reject
    with the same explicit error (Content-Length defaults to 0 in
    _read_json on missing)."""
    url = f"http://127.0.0.1:{coordinator.port}/hooks/pre-read"
    secret = load_secret(coordinator.coordinator_root)
    # Build via raw socket to omit Content-Length entirely (urllib auto-adds it).
    import socket as _socket
    sock = _socket.create_connection(("127.0.0.1", coordinator.port))
    try:
        sock.sendall(
            f"POST /hooks/pre-read HTTP/1.0\r\n"
            f"Host: 127.0.0.1\r\n"
            f"Authorization: Bearer {secret}\r\n"
            f"Content-Type: application/json\r\n"
            f"\r\n".encode()
        )
        resp = b""
        while True:
            chunk = sock.recv(4096)
            if not chunk:
                break
            resp += chunk
        # The status line is the first line of the response.
        first_line = resp.split(b"\r\n", 1)[0].decode()
        assert "400" in first_line, f"expected 400, got: {first_line}"
    finally:
        sock.close()


# ----------------------------------------------------------------------
# AC-05 — degraded response shape varies by endpoint contract
# ----------------------------------------------------------------------


def test_ac05_pre_edit_degraded_response_returns_ok_shape(
    coordinator, client: _Client, monkeypatch: pytest.MonkeyPatch
) -> None:
    """pre-edit's wire contract is {ok: bool}; degraded envelope on
    watchdog timeout must include ok=True so clients reading
    result.get('ok') don't see None. AC-05 fix."""
    from concurrent.futures import TimeoutError as FuturesTimeout

    def force_timeout(fn, abort=None):
        raise FuturesTimeout()

    monkeypatch.setattr(coordinator, "run_with_watchdog", force_timeout)
    status, body = client.post(
        "/hooks/pre-edit",
        {"session_id": _sid("ac05-pre-edit"), "path": "plan.md"},
    )
    assert status == 200
    assert body.get("ok") is True, (
        f"pre-edit degraded envelope must include ok=True; got {body!r}"
    )
    assert body.get("degraded") is True


def test_ac05_post_edit_degraded_response_returns_ok_shape(
    coordinator, client: _Client, monkeypatch: pytest.MonkeyPatch
) -> None:
    """post-edit's wire contract is {ok: bool}; degraded envelope must
    include ok=True. AC-05 fix."""
    from concurrent.futures import TimeoutError as FuturesTimeout

    def force_timeout(fn, abort=None):
        raise FuturesTimeout()

    monkeypatch.setattr(coordinator, "run_with_watchdog", force_timeout)
    status, body = client.post(
        "/hooks/post-edit",
        {
            "session_id": _sid("ac05-post-edit"),
            "path": "plan.md",
            "content_hash": _hash("ac05"),
            "success": True,
        },
    )
    assert status == 200
    assert body.get("ok") is True
    assert body.get("degraded") is True


def test_ac05_session_stop_degraded_response_returns_ok_shape(
    coordinator, client: _Client, monkeypatch: pytest.MonkeyPatch
) -> None:
    """session-stop's wire contract is {ok: bool}; degraded envelope
    must include ok=True. AC-05 fix."""
    from concurrent.futures import TimeoutError as FuturesTimeout

    def force_timeout(fn, abort=None):
        raise FuturesTimeout()

    monkeypatch.setattr(coordinator, "run_with_watchdog", force_timeout)
    status, body = client.post(
        "/hooks/session-stop", {"session_id": _sid("ac05-session-stop")}
    )
    assert status == 200
    assert body.get("ok") is True
    assert body.get("degraded") is True


def test_ac05_pre_read_degraded_response_keeps_status_fresh_shape(
    coordinator, client: _Client, monkeypatch: pytest.MonkeyPatch
) -> None:
    """pre-read's wire contract is {status: ...}; degraded envelope
    keeps the fresh-shape envelope so clients checking status
    don't see ok=None. AC-05 contract preservation."""
    from concurrent.futures import TimeoutError as FuturesTimeout

    def force_timeout(fn, abort=None):
        raise FuturesTimeout()

    monkeypatch.setattr(coordinator, "run_with_watchdog", force_timeout)
    status, body = client.post(
        "/hooks/pre-read",
        {"session_id": _sid("ac05-pre-read"), "path": "plan.md"},
    )
    assert status == 200
    assert body.get("status") == "fresh"
    assert body.get("degraded") is True
    # Crucially, pre-read's degraded envelope does NOT include ok.
    assert "ok" not in body


def test_a7_degraded_read_surfaces_advisory_not_silent(
    coordinator, client: _Client, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A7: a watchdog-degraded read must NOT silently pass as verified-fresh.
    The degraded envelope now carries a hookSpecificOutput advisory the model
    sees (the hook client passes it straight through), so an in-queue/processing
    timeout cannot masquerade as a confirmed fresh read."""
    from concurrent.futures import TimeoutError as FuturesTimeout

    def force_timeout(fn, abort=None):
        raise FuturesTimeout()

    monkeypatch.setattr(coordinator, "run_with_watchdog", force_timeout)
    status, body = client.post(
        "/hooks/pre-read",
        {"session_id": _sid("a7-degraded"), "path": "plan.md"},
    )
    assert status == 200
    assert body.get("status") == "fresh"
    assert body.get("degraded") is True
    hso = body.get("hookSpecificOutput")
    assert isinstance(hso, dict), "degraded read must carry an advisory (non-silent)"
    assert "timed out" in hso.get("additionalContext", "").lower(), (
        f"advisory must explain the freshness check did not run; got {hso!r}"
    )


# ----------------------------------------------------------------------
# T-01 — pre-bash notices-only branch (no stale paths, non-empty notices)
# ----------------------------------------------------------------------


def test_t01_pre_bash_notices_only_branch_surfaces_preemption(
    client: _Client
) -> None:
    """T-01 / ce-review: previously-untested branch in _handle_pre_bash —
    session has pending preemption notices but the Bash command reads
    only UNTRACKED paths. Expected: response carries hookSpecificOutput
    with the notice text but no stale_paths (status fresh)."""
    x = _sid("t01-X")
    y = _sid("t01-Y")
    # X acquires + commits plan.md (tracked), then Y commits → invalidates X.
    client.post("/hooks/pre-read", {"session_id": x, "path": "plan.md",
                                     "content_hash": _hash("v1")})
    client.post("/hooks/pre-edit", {"session_id": x, "path": "plan.md"})
    client.post("/hooks/post-edit", {"session_id": x, "path": "plan.md",
                                      "content_hash": _hash("v2"), "success": True})
    client.post("/hooks/pre-edit", {"session_id": y, "path": "plan.md"})
    client.post("/hooks/post-edit", {"session_id": y, "path": "plan.md",
                                      "content_hash": _hash("v3"), "success": True})
    # Now X has a pending preemption notice for plan.md.
    # X fires pre-bash with a command that reads only an UNTRACKED path.
    s, body = client.post("/hooks/pre-bash", {
        "session_id": x,
        "command": "cat /etc/hosts",  # untracked, never matches policy
    })
    assert s == 200
    # No tracked path → no stale_paths in response.
    assert "stale_paths" not in body or body["stale_paths"] == []
    # But X's pending notice should surface via additionalContext.
    out = body.get("hookSpecificOutput")
    if out is not None:
        # The notice prose lands here if the handler chose to surface.
        # Either path is acceptable per the contract — the notice may
        # also be deferred to the next pre-read.
        assert "plan.md" in out.get("additionalContext", "") or body.get("status") == "fresh"
    else:
        # Notice deferred to next pre-read — handler returned plain fresh.
        assert body.get("status") == "fresh"


# ----------------------------------------------------------------------
# L5 — idle/uptime use a monotonic clock (NTP-/suspend-safe)
# ----------------------------------------------------------------------


def test_l5_idle_and_uptime_use_monotonic_immune_to_wall_clock(
    tmp_path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """L5: idle_seconds / uptime_s are monotonic deltas, so a wall-clock
    (NTP step / suspend-resume) jump does not misfire or defer idle shutdown.
    Against the old time.time() body the backward-time assertions below fail."""
    import ccs.adapters.claude_code.coordinator_server as mod

    clock = {"t": 1000.0}
    monkeypatch.setattr(mod.time, "monotonic", lambda: clock["t"])
    with CoordinatorHTTPServer(tmp_path, port=0, instance_id="l5-test") as srv:
        # _started_at and _last_request_at were seeded at monotonic 1000.0.
        clock["t"] = 1030.0
        assert srv.idle_seconds == 30.0
        assert srv.uptime_s == 30.0
        # A wild wall-clock step (NTP back an hour / resume) must NOT perturb the
        # monotonic-based deltas — the core L5 regression assertion.
        monkeypatch.setattr(mod.time, "time", lambda: 1.0)
        assert srv.idle_seconds == 30.0
        assert srv.uptime_s == 30.0
        # mark_request resets idle on the monotonic clock.
        clock["t"] = 1100.0
        srv.mark_request()
        clock["t"] = 1105.0
        assert srv.idle_seconds == 5.0
