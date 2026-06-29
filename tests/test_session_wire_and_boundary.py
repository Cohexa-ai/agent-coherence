# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Wire-stability, server-capture boundary lock, and endpoint-auth tests for
the snapshot-session HTTP endpoints (SB-17 / TX-1, Unit 8 — R7 / R9 / R10a).

Covers:
- Endpoint auth: every ``/session/*`` route rides the central ``_ROUTES``
  ``verify_bearer`` + ``verify_host`` seam (401 no/bad bearer, 403 bad Host) —
  NOT a parallel router.
- Boundary lock (R9): a client-supplied pinned-version / cut / forged-or-
  replayed token / client-asserted owner CANNOT forge or bypass the
  server-side capture. The "client carries the cut" path FAILS the guard.
- Wire (R7): new session reasons are ADDITIVE; existing reason sets unchanged.
- Audit (R10a): begin / commit / invalidate emit content-free JSONL records.
- Happy-path round trip: begin → read → commit over HTTP with a valid
  bearer/host + authenticated caller.
"""

from __future__ import annotations

import json
import time
import uuid
from pathlib import Path
from typing import Optional
from urllib import error as urlerror
from urllib import request as urlrequest

import pytest

from ccs.adapters.claude_code.auth import load_secret
from ccs.adapters.claude_code.coordinator_server import (
    _ROUTES,
    CoordinatorHTTPServer,
)
from ccs.adapters.claude_code.session_audit_log import (
    _resolve_session_audit_log_path,
)
from ccs.core.exceptions import (
    READ_AT_VERSION_REASONS,
    SESSION_BEGIN_CAP_REASONS,
    SESSION_COMMIT_REASONS,
    SESSION_READ_REASONS,
)

_TEST_SESSION_NS = uuid.UUID("22222222-2222-4222-8222-222222222222")


def _sid(label: str) -> str:
    return str(uuid.uuid5(_TEST_SESSION_NS, f"session-wire:{label}"))


class _Client:
    """Tiny urllib client returning (status, body_dict)."""

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
        *,
        headers_override: Optional[dict] = None,
    ) -> tuple[int, dict]:
        url = self.base + path
        data = json.dumps(body).encode("utf-8") if body is not None else b""
        headers = dict(self.headers)
        if headers_override:
            headers.update(headers_override)
        req = urlrequest.Request(
            url, data=data if method == "POST" else None, method=method, headers=headers
        )
        try:
            with urlrequest.urlopen(req, timeout=10) as resp:
                return resp.status, json.loads(resp.read().decode("utf-8") or "{}")
        except urlerror.HTTPError as e:
            return e.code, json.loads(e.read().decode("utf-8") or "{}")

    def post(self, path: str, body: dict, **kw) -> tuple[int, dict]:
        return self.request("POST", path, body, **kw)


@pytest.fixture
def coordinator(tmp_path: Path):
    server = CoordinatorHTTPServer(tmp_path, port=0, instance_id="test-instance")
    server.serve_in_thread()
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


_SESSION_ROUTES = [
    "/session/begin",
    "/session/read",
    "/session/commit",
    "/session/heartbeat",
]


# ----------------------------------------------------------------------
# Endpoint auth — rides the central _ROUTES seam (NOT a parallel router)
# ----------------------------------------------------------------------


def test_session_routes_registered_in_central_routes() -> None:
    """All four session routes are in the central _ROUTES table, so the
    dispatcher's verify_bearer + verify_host run before any handler — there
    is no parallel router that could bypass auth."""
    for route in _SESSION_ROUTES:
        assert ("POST", route) in _ROUTES


@pytest.mark.parametrize("route", _SESSION_ROUTES)
def test_session_endpoint_no_bearer_returns_401(coordinator, route: str) -> None:
    url = f"http://127.0.0.1:{coordinator.port}{route}"
    req = urlrequest.Request(
        url, data=b"{}", method="POST",
        headers={"Host": "127.0.0.1", "Content-Type": "application/json"},
    )
    try:
        urlrequest.urlopen(req, timeout=5)
        raise AssertionError("expected 401")
    except urlerror.HTTPError as e:
        assert e.code == 401


@pytest.mark.parametrize("route", _SESSION_ROUTES)
def test_session_endpoint_bad_bearer_returns_401(coordinator, route: str) -> None:
    url = f"http://127.0.0.1:{coordinator.port}{route}"
    req = urlrequest.Request(
        url, data=b"{}", method="POST",
        headers={
            "Authorization": "Bearer not-the-secret",
            "Host": "127.0.0.1",
            "Content-Type": "application/json",
        },
    )
    try:
        urlrequest.urlopen(req, timeout=5)
        raise AssertionError("expected 401")
    except urlerror.HTTPError as e:
        assert e.code == 401


@pytest.mark.parametrize("route", _SESSION_ROUTES)
def test_session_endpoint_bad_host_returns_403(client: _Client, route: str) -> None:
    status, _body = client.post(
        route, {"session_id": _sid("h")},
        headers_override={"Host": "attacker.example.com"},
    )
    assert status == 403


# ----------------------------------------------------------------------
# Happy-path round trip (begin → read → commit)
# ----------------------------------------------------------------------


def _begin(client: _Client, sid: str, read_set: list[str]) -> dict:
    status, body = client.post(
        "/session/begin", {"session_id": sid, "read_set": read_set}
    )
    assert status == 200, body
    return body


def test_happy_path_begin_read_commit(client: _Client) -> None:
    sid = _sid("happy")
    # First-observation seeds v1 for the path.
    begin = _begin(client, sid, ["plan.md"])
    assert begin["ok"] is True
    assert "session_token" in begin
    token = begin["session_token"]
    assert begin["cut"] == {"plan.md": 1}
    assert "coordinator_epoch" in begin and "retain_versions" in begin

    # Read the pinned version.
    status, read = client.post(
        "/session/read", {"session_id": sid, "session_token": token, "path": "plan.md"}
    )
    assert status == 200, read
    assert read["ok"] is True
    assert read["version"] == 1
    # Default registry retains no bodies (retain_versions=False) → EAGER branch
    # → the coordinator defers byte-serving to the data plane (typed, not a
    # crash). The LAZY content-serve branch is exercised at the service layer
    # in tests/test_session_read.py (the HTTP server hardwires retain=False).
    assert read["served"] == "data_plane_deferred"
    assert begin["retain_versions"] is False

    # Commit against the pinned base → WIN, version bumps to 2.
    status, commit = client.post(
        "/session/commit",
        {"session_id": sid, "session_token": token, "path": "plan.md", "content": "new"},
    )
    assert status == 200, commit
    assert commit["ok"] is True
    assert commit["version"] == 2

    # A SECOND commit at the same pin is HELD (R11 exactly-one-commit).
    status, commit2 = client.post(
        "/session/commit",
        {"session_id": sid, "session_token": token, "path": "plan.md", "content": "again"},
    )
    assert status == 200
    assert commit2["ok"] is False
    assert commit2["reason"] == "version_mismatch"


def test_heartbeat_refreshes_owned_session(client: _Client) -> None:
    sid = _sid("hb")
    begin = _begin(client, sid, ["hb.md"])
    token = begin["session_token"]
    status, hb = client.post(
        "/session/heartbeat", {"session_id": sid, "session_token": token}
    )
    assert status == 200
    assert hb == {"ok": True, "refreshed": True}


# ----------------------------------------------------------------------
# Boundary lock (R9) — no client can forge / bypass the server-side capture
# ----------------------------------------------------------------------


def test_client_supplied_cut_is_ignored_not_trusted(client: _Client) -> None:
    """A client that smuggles a ``cut`` / ``pinned_version`` / ``expected_version``
    into /session/read or /session/commit MUST NOT have it honored — the server
    is authoritative. The forged fields are ignored; the server reads the pinned
    base from the registry by token. (The day a client legitimately carries the
    cut is cross-host — this guard FAILS if anyone wires that in here.)"""
    sid = _sid("forge-cut")
    begin = _begin(client, sid, ["target.md"])  # pins target.md@v1
    token = begin["session_token"]

    # Forge a pinned_version=999 + a fake cut in the read request. The server
    # ignores them and serves the REAL pinned v1.
    status, read = client.post(
        "/session/read",
        {
            "session_id": sid, "session_token": token, "path": "target.md",
            "pinned_version": 999, "cut": {"target.md": 999}, "version": 999,
        },
    )
    assert status == 200, read
    assert read["ok"] is True
    assert read["version"] == 1  # the server-captured pin, NOT the forged 999.

    # Forge expected_version=999 on commit. The server uses the real pinned v1
    # as the comparand → WIN bumps to v2 (a forged 999 would corrupt-error).
    status, commit = client.post(
        "/session/commit",
        {
            "session_id": sid, "session_token": token, "path": "target.md",
            "content": "x", "expected_version": 999, "pinned_version": 999,
        },
    )
    assert status == 200, commit
    assert commit["ok"] is True
    assert commit["version"] == 2  # pinned-base CAS, forged comparand ignored.


def test_forged_session_token_cannot_bypass_capture(client: _Client) -> None:
    """A made-up / never-minted session token has no server-side cut. A read or
    commit against it fails CLOSED (typed reason), NEVER served live HEAD."""
    sid = _sid("forged-token")
    # Seed the artifact so it exists (so the failure is about the TOKEN, not the
    # path).
    _begin(client, sid, ["existing.md"])

    forged = "totally-not-a-real-server-minted-token"
    status, read = client.post(
        "/session/read",
        {"session_id": sid, "session_token": forged, "path": "existing.md"},
    )
    assert status == 200, read
    assert read["ok"] is False
    assert read["reason"] in SESSION_READ_REASONS  # fail-closed, not live HEAD

    status, commit = client.post(
        "/session/commit",
        {"session_id": sid, "session_token": forged, "path": "existing.md", "content": "x"},
    )
    assert status == 200
    assert commit["ok"] is False
    assert commit["reason"] in SESSION_COMMIT_REASONS


def test_replayed_token_after_release_fails_closed(coordinator, client: _Client) -> None:
    """A token whose session has been reaped (its cut released) cannot be
    replayed to read/commit — it fails closed, never serves the (now stale)
    cut from live HEAD."""
    sid = _sid("replay2")
    begin = _begin(client, sid, ["replay2.md"])
    token = begin["session_token"]
    # Release the session's pins directly on the wrapped registry (what the
    # liveness sweep does when a heartbeat goes stale).
    coordinator.registry.release_session(token)
    status, read = client.post(
        "/session/read", {"session_id": sid, "session_token": token, "path": "replay2.md"}
    )
    assert status == 200, read
    assert read["ok"] is False
    assert read["reason"] in SESSION_READ_REASONS  # fail-closed


def test_foreign_caller_cannot_read_anothers_cut(coordinator, client: _Client) -> None:
    """R13 owner isolation surfaced at the wire: a DIFFERENT authenticated
    session_id (a sibling) cannot read another session's cut even with the
    leaked token — it fails closed with session_invalidated."""
    owner_sid = _sid("owner")
    begin = _begin(client, owner_sid, ["owned.md"])
    token = begin["session_token"]

    foreign_sid = _sid("foreign")  # a different authenticated caller
    status, read = client.post(
        "/session/read",
        {"session_id": foreign_sid, "session_token": token, "path": "owned.md"},
    )
    assert status == 200, read
    assert read["ok"] is False
    assert read["reason"] == "session_invalidated"


def test_client_cannot_assert_owner_field(coordinator, client: _Client) -> None:
    """A client-asserted ``owner`` / ``caller`` field MUST NOT bind or rebind the
    session owner — the owner is derived from the authenticated session_id only.
    A foreign caller supplying the real owner's id as ``owner``/``caller`` still
    fails closed (the server ignores those fields)."""
    owner_sid = _sid("owner-bind")
    begin = _begin(client, owner_sid, ["bound.md"])
    token = begin["session_token"]

    foreign_sid = _sid("foreign-bind")
    # Try to impersonate the owner via a client-supplied owner/caller field.
    status, read = client.post(
        "/session/read",
        {
            "session_id": foreign_sid, "session_token": token, "path": "bound.md",
            "owner": owner_sid, "caller": owner_sid,
        },
    )
    assert status == 200, read
    assert read["ok"] is False
    assert read["reason"] == "session_invalidated"  # owner came from AUTH, not the field


# ----------------------------------------------------------------------
# Wire (R7) — new reasons are additive; existing sets unchanged
# ----------------------------------------------------------------------


def test_session_reason_sets_are_disjoint_and_additive() -> None:
    """The session reason sets are NET-NEW closed sets, disjoint from the
    bare read_at_version contract — additive, never folded in (R7)."""
    assert SESSION_READ_REASONS.isdisjoint(READ_AT_VERSION_REASONS)
    assert SESSION_COMMIT_REASONS.isdisjoint(READ_AT_VERSION_REASONS)
    assert SESSION_BEGIN_CAP_REASONS.isdisjoint(READ_AT_VERSION_REASONS)
    # The pre-Unit-2 frozen read_at_version reasons are unchanged (6 reasons).
    assert "current_version" in READ_AT_VERSION_REASONS
    assert "unknown_artifact" in READ_AT_VERSION_REASONS


def test_begin_unknown_artifact_reason_stays_in_read_at_version_set(client: _Client) -> None:
    """begin_session's unknown-id rejection reuses the existing unknown_artifact
    reason (not a parallel one) — but unknown PATHS get seeded as first
    observations, so this asserts the cap-reason additive surface instead via a
    too-large read_set."""
    # An over-cap read_set is rejected at the WIRE (400) before the service —
    # the service-level read_set_too_large stays the authoritative cap.
    sid = _sid("cap")
    big = [f"f{i}.md" for i in range(65)]  # > MAX_SESSION_READ_SET_PATHS (64)
    status, body = client.post("/session/begin", {"session_id": sid, "read_set": big})
    assert status == 400
    assert "read_set" in body["error"]


# ----------------------------------------------------------------------
# Audit (R10a) — begin / commit / invalidate emit content-free records
# ----------------------------------------------------------------------


def test_audit_emits_begin_commit_invalidate_content_free(coordinator, client: _Client) -> None:
    sid = _sid("audit")
    begin = _begin(client, sid, ["audit.md"])
    token = begin["session_token"]
    # Commit → WIN (emits a session_commit audit event).
    client.post(
        "/session/commit",
        {"session_id": sid, "session_token": token, "path": "audit.md", "content": "v2"},
    )
    # Reap then read with the dead token → fail-closed → session_invalidate event.
    coordinator.registry.release_session(token)
    client.post(
        "/session/read", {"session_id": sid, "session_token": token, "path": "audit.md"}
    )

    audit_path = _resolve_session_audit_log_path(coordinator.coordinator_root)
    records = [json.loads(line) for line in audit_path.read_text().strip().splitlines()]
    events = {r["event"] for r in records}
    assert {"session_begin", "session_commit", "session_invalidate"} <= events

    # Content-free: no record carries body / hash / prose / raw token.
    forbidden = {"content", "content_hash", "body", "command", "token", "session_token"}
    raw = audit_path.read_text()
    assert token not in raw  # raw token never logged (only its hash)
    assert "v2" not in raw  # the committed body bytes never logged
    for record in records:
        assert not (set(record.keys()) & forbidden)

    begin_rec = next(r for r in records if r["event"] == "session_begin")
    # ids + versions only.
    assert set(begin_rec.keys()) == {"ts", "event", "session", "cut"}
    commit_rec = next(r for r in records if r["event"] == "session_commit")
    assert set(commit_rec.keys()) == {
        "ts", "event", "session", "artifact", "pinned_version", "committed_version",
    }
