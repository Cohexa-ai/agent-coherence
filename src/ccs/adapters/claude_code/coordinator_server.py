# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""Local HTTP coordinator for the Claude Code coherence plugin (Unit 4).

stdlib HTTP server (KTD-4) that wraps :class:`CoordinatorService` driven
by :class:`SqliteArtifactRegistry`. Exposes seven endpoints behind
shared-secret Bearer auth + Host-header check (KTD-12):

- ``POST /hooks/pre-read``      — stale-read check; KTD-9 first-observation
- ``POST /hooks/pre-edit``      — acquire EXCLUSIVE; KTD-1 single-writer
- ``POST /hooks/post-edit``     — commit on success, release on failure
- ``POST /hooks/session-stop``  — KTD-11 release on end-of-turn
- ``POST /policy/track``        — Unit 6 CLI hot-add to tracked.yaml
- ``POST /policy/untrack``      — Unit 6 CLI hot-add to ignored.yaml
- ``GET  /status``              — Unit 6 status CLI

Every handler:
- Verifies ``Authorization: Bearer <secret>`` (constant-time)
- Verifies ``Host`` header is localhost / 127.0.0.1 (DNS-rebind guard)
- Records the calling session's heartbeat (KTD-2)
- Runs the coordinator call under a 4s ThreadPoolExecutor timeout
  (handler-side watchdog — keeps us under the 5s hook timeout even when
  SQLite contention exceeds busy_timeout=2000)
- Converts ``CoherenceError`` to 200 ``{ok: false, reason}`` (NOT 500 — we
  want hooks to proceed gracefully on protocol violations, not block)
- Logs request/response at DEBUG, errors at WARNING

Lifecycle (spawn, port-file, idle-shutdown, sweep) lives in :mod:`lifecycle`
(Unit 5) and consumes ``CoordinatorHTTPServer.from_root(...)``.
"""

from __future__ import annotations

import http.server
import json
import logging
import re
import socketserver
import threading
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout
from pathlib import Path
from typing import Any, Callable, Optional
from uuid import NAMESPACE_URL, UUID, uuid5

from ccs.adapters.claude_code import hook_payloads as _payloads
from ccs.adapters.claude_code.auth import (
    ensure_secret,
    verify_bearer,
    verify_host,
)
from ccs.adapters.claude_code.policy import TrackedArtifactPolicy
from ccs.adapters.claude_code.resolver import find_coordinator_root
from ccs.coordinator.service import CoordinatorService
from ccs.coordinator.sqlite_registry import SqliteArtifactRegistry
from ccs.core.exceptions import CoherenceError
from ccs.core.states import MESIState

logger = logging.getLogger(__name__)


HANDLER_TIMEOUT_SEC = 4.0
"""Each endpoint's coordinator call is bounded to 4s by the watchdog;
leaves 1s of margin under the 5s Claude Code hook timeout (KTD-12 / Unit 4)."""

MAX_POLICY_PATHS_PER_REQUEST = 20
"""Cap on the number of paths /policy/track and /policy/untrack accept
in one request body (security-lens P1)."""

MAX_POLICY_YAML_BYTES = 64 * 1024
"""Cap on the resulting tracked.yaml / ignored.yaml file size (security-lens P1)."""

MAX_PATH_LEN = 1024
"""Cap on inbound path length to defend against memory/DoS and prose-injection
attacks. 1024 covers nested-deep paths in any realistic project."""

_SESSION_ID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
                            re.IGNORECASE)
"""UUID4 shape. CC v2.1.131 fixtures (cc_hook_stdin/) confirm session_ids
are always UUIDs. Rejecting non-UUIDs closes the unbounded-agent_names
abuse vector (Adv #13)."""

_CONTENT_HASH_RE = re.compile(r"^[0-9a-f]{64}$", re.IGNORECASE)
"""SHA-256 hex shape. Rejecting malformed hashes closes the
caller-supplied-hash abuse vector (Adv #6) where an authenticated client
could mint v1 with attacker-chosen hash strings."""


# ----------------------------------------------------------------------
# Session → agent UUID derivation (matches src/ccs/adapters/base.py:82)
# ----------------------------------------------------------------------


def session_to_agent_id(session_id: str) -> UUID:
    """Deterministic UUID for a Claude Code session, matching the convention
    in :class:`CoherenceAdapterCore` so other adapters and the in-process
    library see the same agent identity."""
    return uuid5(NAMESPACE_URL, f"ccs-agent:claude-session-{session_id}")


def session_to_agent_name(session_id: str) -> str:
    """Human-readable agent name for state_log and status display."""
    return f"claude-session-{session_id}"


def monotonic_seconds() -> int:
    """Tick basis for the coordinator. 1 tick = 1 second of wall clock."""
    return int(time.time())


# ----------------------------------------------------------------------
# Boundary validators (Adv-review hardening A2 + A3 + A8)
# ----------------------------------------------------------------------


def validate_session_id(session_id: Any) -> Optional[str]:
    """Return reason if invalid, None if valid. UUID-shape required."""
    if not isinstance(session_id, str):
        return "session_id must be a string"
    if not _SESSION_ID_RE.match(session_id):
        return "session_id must be a UUID (8-4-4-4-12 hex with hyphens)"
    return None


def validate_path(path: Any) -> Optional[str]:
    """Return reason if invalid, None if valid. The coordinator stores paths
    as parent-repo-relative — KTD-7 normalization happens client-side (the
    hook script must compute repo-relative from CC's absolute tool_input.
    file_path). The boundary validation here is defense-in-depth against
    bad hook clients AND prose-injection abuse (Adv #4 + Adv #11)."""
    if not isinstance(path, str):
        return "path must be a string"
    if not path:
        return "path is empty"
    if len(path) > MAX_PATH_LEN:
        return f"path exceeds {MAX_PATH_LEN} chars"
    if path.startswith("/"):
        return "path must be relative (no leading /)"
    if path.startswith("\\"):
        return "path must be relative (no leading \\)"
    # ANY control character rejected — guards against newline injection into
    # additionalContext prose (Adv #11) and other terminal-control mischief.
    if any(ord(c) < 0x20 or ord(c) == 0x7F for c in path):
        return "path contains control characters"
    # .. traversal at any segment boundary
    parts = path.replace("\\", "/").split("/")
    if ".." in parts:
        return "path contains '..' traversal"
    return None


def validate_content_hash(content_hash: Any, *, required: bool) -> Optional[str]:
    """Return reason if invalid, None if valid. content_hash is OPTIONAL on
    pre-read (the caller may not have it yet) but REQUIRED on post-edit
    (the caller just wrote the file and computed it). Empty string is
    always rejected to avoid the silent-record-known-wrong-hash anti-pattern."""
    if content_hash is None and not required:
        return None
    if not isinstance(content_hash, str):
        return "content_hash must be a string"
    if not content_hash:
        return "content_hash is empty (omit the field instead if unknown)"
    if not _CONTENT_HASH_RE.match(content_hash):
        return "content_hash must be 64 hex characters (sha-256)"
    return None


# ----------------------------------------------------------------------
# Server shell
# ----------------------------------------------------------------------


class CoordinatorHTTPServer:
    """Holds the wired-up coordinator state and a stdlib ThreadingHTTPServer.

    Caller (Unit 5 ``lifecycle.ensure_coordinator``) instantiates this then
    calls :meth:`serve_in_thread` to start a daemon-thread serving loop. The
    ``port`` attribute is populated once the OS picks one (port=0 binding)."""

    def __init__(
        self,
        coordinator_root: Path,
        *,
        port: int = 0,  # 0 = OS picks
        bind_host: str = "127.0.0.1",
        agent_names: Optional[dict[UUID, str]] = None,
        state_log: Optional[Callable[[dict[str, Any]], None]] = None,
        instance_id: Optional[str] = None,
    ) -> None:
        self.coordinator_root = Path(coordinator_root).resolve()
        self.bind_host = bind_host
        self._started_at = time.time()
        self._last_request_at = self._started_at
        self._shutting_down = False
        self._agent_names: dict[UUID, str] = dict(agent_names or {})

        # Wire storage + coordinator service.
        db_path = self.coordinator_root / ".coherence" / "state.db"
        self.registry = SqliteArtifactRegistry(
            db_path,
            state_log=state_log,
            agent_names=self._agent_names,
            instance_id=instance_id,
        )
        self.service = CoordinatorService(self.registry)
        self.policy = TrackedArtifactPolicy.load(self.coordinator_root)

        # Shared secret (auth) — generated on first spawn, persisted across restarts.
        self.secret = ensure_secret(self.coordinator_root)

        # Handler watchdog executor — bounded to a small pool, the work is
        # SQLite-bound and we want timeouts not parallelism.
        self._watchdog = ThreadPoolExecutor(max_workers=4, thread_name_prefix="coord-wd")

        # ThreadingHTTPServer — handlers see this instance via .server.coordinator
        handler_cls = _make_handler_class(self)
        self._server = _ThreadingHTTPServer((bind_host, port), handler_cls)
        self.port = self._server.server_port
        self._serve_thread: Optional[threading.Thread] = None

    @classmethod
    def from_root(cls, coordinator_root: Path, **kwargs: Any) -> "CoordinatorHTTPServer":
        """Convenience for the lifecycle module."""
        return cls(coordinator_root, **kwargs)

    def serve_in_thread(self) -> None:
        """Start the serving loop in a daemon thread."""
        if self._serve_thread is not None:
            return
        self._serve_thread = threading.Thread(
            target=self._server.serve_forever, name="coord-http", daemon=True
        )
        self._serve_thread.start()

    def shutdown(self) -> None:
        """Stop the server, drain in-flight handlers, close storage."""
        if self._shutting_down:
            return
        self._shutting_down = True
        try:
            self._server.shutdown()
            self._server.server_close()
        finally:
            self._watchdog.shutdown(wait=True, cancel_futures=False)
            self.registry.close()

    def __enter__(self) -> "CoordinatorHTTPServer":
        self.serve_in_thread()
        return self

    def __exit__(self, *exc_info: Any) -> None:
        self.shutdown()

    # ------------------------------------------------------------------
    # Server-side state & metrics surfaced to handlers
    # ------------------------------------------------------------------

    def mark_request(self) -> None:
        self._last_request_at = time.time()

    @property
    def uptime_s(self) -> float:
        return time.time() - self._started_at

    @property
    def shutting_down(self) -> bool:
        return self._shutting_down

    def register_session(self, session_id: str) -> UUID:
        """Idempotent session registration. Returns the deterministic agent UUID."""
        agent_id = session_to_agent_id(session_id)
        if agent_id not in self._agent_names:
            self._agent_names[agent_id] = session_to_agent_name(session_id)
        return agent_id

    def run_with_watchdog(self, fn: Callable[[], Any]) -> Any:
        """Run a callable under the 4s handler-side timeout. Raises
        :class:`FuturesTimeout` on timeout (caller decides degradation)."""
        future = self._watchdog.submit(fn)
        return future.result(timeout=HANDLER_TIMEOUT_SEC)


class _ThreadingHTTPServer(socketserver.ThreadingMixIn, http.server.HTTPServer):
    """ThreadingMixIn + HTTPServer — concurrent hook handling per request."""

    daemon_threads = True
    allow_reuse_address = True


# ----------------------------------------------------------------------
# Request handler factory
# ----------------------------------------------------------------------


def _make_handler_class(coordinator: CoordinatorHTTPServer) -> type:
    """Build a BaseHTTPRequestHandler subclass closed over the coordinator
    instance. This is the stdlib idiom for handing per-server state to
    handlers without subclassing the server class."""

    class _Handler(http.server.BaseHTTPRequestHandler):
        # Use HTTP/1.1 with Connection: close per response (keep-alive is
        # not worth the complexity at hook-call rate).
        protocol_version = "HTTP/1.0"

        # ----------------------------------------------------------
        # stdlib hook overrides
        # ----------------------------------------------------------

        def log_message(self, fmt: str, *args: Any) -> None:
            # Silence stdout/stderr — we have our own logger.
            logger.debug("http %s - " + fmt, self.address_string(), *args)

        # ----------------------------------------------------------
        # Routing
        # ----------------------------------------------------------

        def do_POST(self) -> None:
            self._dispatch("POST")

        def do_GET(self) -> None:
            self._dispatch("GET")

        def _dispatch(self, method: str) -> None:
            if coordinator.shutting_down:
                self._json(503, {"error": "coordinator shutting down"})
                return
            coordinator.mark_request()

            # Auth + Host check on every endpoint
            if not verify_host(self.headers.get("Host")):
                self._json(403, {"error": "host header not allowlisted"})
                logger.warning("rejected request with bad Host: %r", self.headers.get("Host"))
                return
            if not verify_bearer(self.headers.get("Authorization"), coordinator.secret):
                self._json(401, {"error": "missing or invalid bearer token"})
                return

            # Route
            try:
                handler = _ROUTES.get((method, self.path))
                if handler is None:
                    self._json(404, {"error": f"unknown route {method} {self.path}"})
                    return
                handler(self, coordinator)
            except Exception as exc:
                logger.exception("unhandled error in handler for %s %s", method, self.path)
                self._json(500, {"error": f"internal: {type(exc).__name__}"})

        # ----------------------------------------------------------
        # Helpers
        # ----------------------------------------------------------

        def _read_json(self) -> Optional[dict]:
            try:
                n = int(self.headers.get("Content-Length", "0"))
            except ValueError:
                self._json(400, {"error": "invalid Content-Length"})
                return None
            if n <= 0:
                return {}
            raw = self.rfile.read(n)
            try:
                obj = json.loads(raw.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError):
                self._json(400, {"error": "invalid json"})
                return None
            if not isinstance(obj, dict):
                self._json(400, {"error": "body must be a JSON object"})
                return None
            return obj

        def _json(self, status: int, body: dict) -> None:
            data = json.dumps(body).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.send_header("Connection", "close")
            self.end_headers()
            self.wfile.write(data)

    return _Handler


# ----------------------------------------------------------------------
# Endpoint implementations
# ----------------------------------------------------------------------


def _handle_pre_read(req, coordinator: CoordinatorHTTPServer) -> None:
    """POST /hooks/pre-read — stale-read check + KTD-9 first-observation seeding."""
    body = req._read_json()
    if body is None:
        return
    session_id = body.get("session_id")
    path = body.get("path", "")
    content_hash = body.get("content_hash") or None

    err = validate_session_id(session_id)
    if err:
        req._json(400, {"error": f"missing session_id" if err.startswith("session_id must be a string") else err})
        return
    err = validate_path(path)
    if err:
        # Mirror the prior "missing or empty path" message shape for empty/missing
        # to keep client-side error-handling stable.
        msg = "missing or empty path" if err in ("path is empty", "path must be a string") else err
        req._json(400, {"error": msg})
        return
    err = validate_content_hash(content_hash, required=False)
    if err:
        req._json(400, {"error": err})
        return

    # Tracked-policy gate: untracked paths fast-path to {fresh} without
    # touching SQLite (R8 false-positive budget protection).
    if not coordinator.policy.is_tracked(path):
        req._json(200, {"status": "fresh"})
        return

    agent_id = coordinator.register_session(session_id)
    now = monotonic_seconds()

    def work() -> dict:
        coordinator.service.record_heartbeat(agent_id=agent_id, now_tick=now)
        artifact_id = coordinator.registry.lookup_artifact_id_by_name(path)

        if artifact_id is None:
            # First observation per KTD-9 — seed v1 with the on-disk hash if
            # the caller supplied one, else use a sentinel.
            seed_hash = content_hash or ""
            artifact_id = coordinator.registry.resolve_or_register(
                path, content_hash=seed_hash
            )
            # Grant SHARED to the first reader so subsequent reads see
            # themselves as known-fresh.
            coordinator.registry.set_agent_state(
                artifact_id, agent_id, MESIState.SHARED,
                trigger="first_read", tick=now, content_hash=seed_hash,
            )
            return {"status": "fresh"}

        artifact = coordinator.registry.get_artifact(artifact_id)
        agent_state = coordinator.registry.get_agent_state(artifact_id, agent_id)

        if agent_state is not None and agent_state != MESIState.INVALID:
            # Reader has a valid grant on the current version.
            return {"status": "fresh"}

        # Stale: either first time this session sees the artifact OR they
        # were invalidated by a peer commit.
        prior_seen = None
        if agent_state == MESIState.INVALID:
            prior_seen = artifact.version - 1 if artifact.version > 0 else 0

        # Compute hash_differs against the caller's last-observed hash, if any.
        # Per KTD-9 we track filesystem-state; a content_hash from the caller's
        # current Read attempt could differ from what's persisted.
        hash_differs = bool(
            content_hash
            and artifact.content_hash
            and content_hash != artifact.content_hash
        )

        last_writer_id = _last_writer_for(coordinator, artifact_id)
        # last_writer_at_unix_ts is REAL — from the artifact's updated_at
        # in the registry (semantically honest, A5). warning_generated_at
        # is now() to guarantee per-invocation variation (A5 + structural
        # defense for v0.2 strict-mode flip).
        last_writer_ts = _last_writer_unix_ts(coordinator, artifact_id) or _payloads.now_unix()
        summary: _payloads.StaleSummary = {
            "path": path,
            "current_version": artifact.version,
            "prior_version_seen_by_session": prior_seen,
            "last_writer_session_id": last_writer_id or "<unknown>",
            "last_writer_at_unix_ts": last_writer_ts,
            "warning_generated_at_unix_ts": _payloads.now_unix(),
            "hash_differs": hash_differs,
        }

        # Re-grant SHARED so this read doesn't re-fire stale on every call.
        coordinator.registry.set_agent_state(
            artifact_id, agent_id, MESIState.SHARED,
            trigger="post_stale_read", tick=now, content_hash=content_hash,
        )
        resp = _payloads.build_stale_response(summary)
        # A1: if THIS session has pending preemption notices, prepend them
        # to the additionalContext so X learns about Y's revocation alongside
        # the stale-read warning.
        notices = coordinator.registry.pop_pending_notices(agent_id)
        if notices:
            notice_text = _build_preemption_text(coordinator, notices)
            resp["hookSpecificOutput"]["additionalContext"] = (
                notice_text + "\n\n" + resp["hookSpecificOutput"]["additionalContext"]
            )
        return resp

    # Wrap _run_or_degrade so we can also pop notices for the FRESH-response
    # path (work() returned {status: "fresh"} without going through stale logic).
    def work_with_notice_surfacing() -> dict:
        result = work()
        if result.get("status") == "fresh" and "hookSpecificOutput" not in result:
            notices = coordinator.registry.pop_pending_notices(agent_id)
            if notices:
                notice_text = _build_preemption_text(coordinator, notices)
                return {
                    "status": "fresh",
                    "hookSpecificOutput": {
                        "hookEventName": "PreToolUse",
                        "permissionDecision": "allow",
                        "additionalContext": notice_text,
                    },
                }
        return result

    _run_or_degrade(req, coordinator, work_with_notice_surfacing)


def _handle_pre_edit(req, coordinator: CoordinatorHTTPServer) -> None:
    """POST /hooks/pre-edit — acquire EXCLUSIVE (KTD-1) + KTD-9 collision surfacing."""
    body = req._read_json()
    if body is None:
        return
    session_id = body.get("session_id")
    path = body.get("path", "")
    err = validate_session_id(session_id) or validate_path(path)
    if err:
        req._json(400, {"error": "missing session_id or path" if "missing" in err or "empty" in err else err})
        return
    if not coordinator.policy.is_tracked(path):
        req._json(200, {"ok": True})
        return

    agent_id = coordinator.register_session(session_id)
    now = monotonic_seconds()

    def work() -> dict:
        coordinator.service.record_heartbeat(agent_id=agent_id, now_tick=now)
        # Seed the artifact row if this is the first Edit on a fresh path.
        artifact_id = coordinator.registry.lookup_artifact_id_by_name(path)
        if artifact_id is None:
            artifact_id = coordinator.registry.resolve_or_register(path, content_hash="")

        # A1: snapshot peers in M∪E BEFORE write so we can record preemption
        # notices for victims after the side-effecting invalidation.
        peers_in_me = _peers_in_me_excluding(coordinator, artifact_id, agent_id)
        # Detect collision BEFORE acquiring: is any other session in M∪E?
        holder_id, holder_ts = _exclusive_holder(coordinator, artifact_id, exclude_agent=agent_id)

        # Acquire EXCLUSIVE — this invalidates peers (KTD-1 single-writer).
        try:
            coordinator.service.write(agent_id=agent_id, artifact_id=artifact_id, issued_at_tick=now)
        except CoherenceError as exc:
            return {"ok": False, "reason": str(exc)}

        # A1: record preemption notices for the agents whose M∪E grants we
        # just silently revoked via the write() side effect. The victims
        # will see these on their next pre-read / pre-edit hook.
        for victim_id in peers_in_me:
            coordinator.registry.record_preemption_notice(
                victim_agent_id=victim_id,
                artifact_id=artifact_id,
                preempter_agent_id=agent_id,
                preempted_at_unix_ts=_payloads.now_unix(),
            )

        # Pop any notices for THIS session (the caller of pre-edit) and
        # merge into the response (A1: surface on the victim's next hook
        # of any kind).
        notices = coordinator.registry.pop_pending_notices(agent_id)
        notice_text = _build_preemption_text(coordinator, notices) if notices else None

        if holder_id is not None:
            # Existing collision surfacing path. If we also have preemption
            # notices for this session, prepend them.
            holder_session = _agent_id_to_session(coordinator, holder_id)
            resp = _payloads.build_collision_response(
                holder_session_id=holder_session or "<unknown>",
                holder_acquired_at_unix_ts=float(holder_ts or _payloads.now_unix()),
                path=path,
            )
            if notice_text:
                resp["hookSpecificOutput"]["additionalContext"] = (
                    notice_text + "\n\n" + resp["hookSpecificOutput"]["additionalContext"]
                )
            return resp

        if notice_text:
            # No collision, but THIS session was preempted previously —
            # promote {ok: true} into a hookSpecificOutput.
            return {
                "ok": True,
                "hookSpecificOutput": {
                    "hookEventName": "PreToolUse",
                    "permissionDecision": "allow",
                    "additionalContext": notice_text,
                },
            }
        return {"ok": True}

    _run_or_degrade(req, coordinator, work)


def _handle_post_edit(req, coordinator: CoordinatorHTTPServer) -> None:
    """POST /hooks/post-edit — commit on success, release on failure (KTD-1)."""
    body = req._read_json()
    if body is None:
        return
    session_id = body.get("session_id")
    path = body.get("path", "")
    content_hash = body.get("content_hash")  # required when success=true
    success = bool(body.get("success", True))
    err = validate_session_id(session_id) or validate_path(path)
    if err:
        req._json(400, {"error": "missing session_id or path" if "missing" in err or "empty" in err else err})
        return
    # content_hash is required only on success — if the tool succeeded, the
    # hook script computed it from the worktree's post-write state.
    err = validate_content_hash(content_hash, required=bool(success))
    if err:
        req._json(400, {"error": err})
        return
    if not coordinator.policy.is_tracked(path):
        req._json(200, {"ok": True})
        return

    agent_id = coordinator.register_session(session_id)
    now = monotonic_seconds()

    def work() -> dict:
        coordinator.service.record_heartbeat(agent_id=agent_id, now_tick=now)
        artifact_id = coordinator.registry.lookup_artifact_id_by_name(path)
        if artifact_id is None:
            # No prior pre-edit / pre-read; nothing to commit against.
            return {"ok": True, "note": "untracked-at-commit"}

        if not success:
            # Tool failure path — release the EXCLUSIVE grant without bumping version.
            artifact = coordinator.registry.get_artifact(artifact_id)
            if artifact is not None:
                try:
                    coordinator.service.invalidate(
                        agent_id=agent_id,
                        artifact_id=artifact_id,
                        new_version=artifact.version,
                        issuer_agent_id=agent_id,
                        issued_at_tick=now,
                    )
                except CoherenceError as exc:
                    return {"ok": False, "reason": str(exc)}
            return {"ok": True, "released": True}

        # Success path — commit and bump version.
        try:
            coordinator.service.commit(
                agent_id=agent_id,
                artifact_id=artifact_id,
                content="",  # KTD-13 — registry stores only the hash
                issued_at_tick=now,
                content_hash=content_hash,
            )
        except CoherenceError as exc:
            # A1: if this commit failed because the grant was preempted
            # silently, enrich the reason with the preemption context so
            # the caller (and any stream-json telemetry) sees WHO took the
            # grant and WHEN — not just the generic CoherenceError text.
            notice = coordinator.registry.peek_preemption_notice(agent_id, artifact_id)
            if notice is not None:
                preempter_id, preempted_at = notice
                preempter_session = _agent_id_to_session(coordinator, preempter_id) or "<unknown>"
                reason = (
                    f"commit_not_allowed: your EXCLUSIVE grant on {path} was "
                    f"preempted by session {preempter_session[:8]} at "
                    f"{_iso_utc(preempted_at)}. Your edit landed in your local "
                    f"worktree but will not be reflected in the coordinator's "
                    f"version. Underlying coordinator error: {exc}"
                )
                return {"ok": False, "reason": reason, "preempted": True}
            return {"ok": False, "reason": str(exc)}
        return {"ok": True}

    _run_or_degrade(req, coordinator, work)


def _handle_session_stop(req, coordinator: CoordinatorHTTPServer) -> None:
    """POST /hooks/session-stop — release uncommitted EXCLUSIVE grants (KTD-11)."""
    body = req._read_json()
    if body is None:
        return
    session_id = body.get("session_id")
    err = validate_session_id(session_id)
    if err:
        req._json(400, {"error": "missing session_id" if err.startswith("session_id must be a string") else err})
        return

    agent_id = coordinator.register_session(session_id)
    now = monotonic_seconds()

    def work() -> dict:
        coordinator.service.record_heartbeat(agent_id=agent_id, now_tick=now)
        held = coordinator.registry.artifacts_held_by_agent(
            agent_id, {MESIState.EXCLUSIVE, MESIState.MODIFIED}
        )
        released: list[str] = []
        for artifact_id in held:
            artifact = coordinator.registry.get_artifact(artifact_id)
            if artifact is None:
                continue
            try:
                coordinator.service.invalidate(
                    agent_id=agent_id,
                    artifact_id=artifact_id,
                    new_version=artifact.version,
                    issuer_agent_id=agent_id,
                    issued_at_tick=now,
                )
                released.append(artifact.name)
            except CoherenceError as exc:
                logger.warning("session-stop release failed for %s: %s", artifact_id, exc)
        return {"ok": True, "released_artifacts": released}

    _run_or_degrade(req, coordinator, work)


def _handle_policy_track(req, coordinator: CoordinatorHTTPServer) -> None:
    """POST /policy/track — Unit 6 CLI add to tracked.yaml."""
    body = req._read_json()
    if body is None:
        return
    paths = body.get("paths")
    if not isinstance(paths, list) or not all(isinstance(p, str) for p in paths):
        req._json(400, {"error": "paths must be a list of strings"})
        return
    if len(paths) > MAX_POLICY_PATHS_PER_REQUEST:
        req._json(400, {"error": f"max {MAX_POLICY_PATHS_PER_REQUEST} paths per request"})
        return
    yaml_path = coordinator.coordinator_root / ".coherence" / "tracked.yaml"
    added, rejected = _append_policy_yaml(yaml_path, paths)
    # Reload the live policy so subsequent hook calls see the additions.
    coordinator.policy = TrackedArtifactPolicy.load(coordinator.coordinator_root)
    req._json(200, {"ok": True, "added": added, "rejected": rejected})


def _handle_policy_untrack(req, coordinator: CoordinatorHTTPServer) -> None:
    """POST /policy/untrack — Unit 6 CLI add to ignored.yaml."""
    body = req._read_json()
    if body is None:
        return
    paths = body.get("paths")
    if not isinstance(paths, list) or not all(isinstance(p, str) for p in paths):
        req._json(400, {"error": "paths must be a list of strings"})
        return
    if len(paths) > MAX_POLICY_PATHS_PER_REQUEST:
        req._json(400, {"error": f"max {MAX_POLICY_PATHS_PER_REQUEST} paths per request"})
        return
    yaml_path = coordinator.coordinator_root / ".coherence" / "ignored.yaml"
    added, _ = _append_policy_yaml(yaml_path, paths)
    coordinator.policy = TrackedArtifactPolicy.load(coordinator.coordinator_root)
    req._json(200, {"ok": True, "removed": added})


def _handle_status(req, coordinator: CoordinatorHTTPServer) -> None:
    """GET /status — drives the agent-coherence-status console script."""
    # A4: snapshot artifact_ids and _agent_names BEFORE iterating, to avoid
    # `RuntimeError: dictionary changed size during iteration` racing against
    # a concurrent register_session in another handler.
    artifact_ids_snapshot = list(coordinator.registry.artifact_ids())
    agent_names_snapshot = list(coordinator._agent_names.items())

    tracked: list[dict] = []
    for artifact_id in artifact_ids_snapshot:
        art = coordinator.registry.get_artifact(artifact_id)
        if art is None:
            continue
        tracked.append({
            "path": art.name,
            "version": art.version,
            "id": str(artifact_id),
        })
    # Per-session state map: from agent_names keys we know who's registered.
    sessions: list[dict] = []
    for agent_id, name in agent_names_snapshot:
        per_artifact: dict[str, str] = {}
        for artifact_id in artifact_ids_snapshot:
            state = coordinator.registry.get_agent_state(artifact_id, agent_id)
            if state is not None and state != MESIState.INVALID:
                art = coordinator.registry.get_artifact(artifact_id)
                if art is not None:
                    per_artifact[art.name] = state.name
        sessions.append({
            "agent_name": name,
            "agent_id": str(agent_id),
            "states": per_artifact,
        })
    req._json(200, {
        "tracked_artifacts": tracked,
        "sessions": sessions,
        "coordinator_uptime_s": coordinator.uptime_s,
        "coordinator_pid": _os_pid(),
        "policy_summary": coordinator.policy.summary(),
    })


_ROUTES: dict[tuple[str, str], Callable] = {
    ("POST", "/hooks/pre-read"): _handle_pre_read,
    ("POST", "/hooks/pre-edit"): _handle_pre_edit,
    ("POST", "/hooks/post-edit"): _handle_post_edit,
    ("POST", "/hooks/session-stop"): _handle_session_stop,
    ("POST", "/policy/track"): _handle_policy_track,
    ("POST", "/policy/untrack"): _handle_policy_untrack,
    ("GET", "/status"): _handle_status,
}


# ----------------------------------------------------------------------
# Internals
# ----------------------------------------------------------------------


def _run_or_degrade(req, coordinator: CoordinatorHTTPServer, work: Callable[[], dict]) -> None:
    """Run ``work`` under the handler-side watchdog. On timeout, log WARNING
    and return 200 {status:"fresh"} so the user's tool call proceeds."""
    try:
        result = coordinator.run_with_watchdog(work)
    except FuturesTimeout:
        logger.warning("handler watchdog timeout after %ss; degrading to fresh", HANDLER_TIMEOUT_SEC)
        req._json(200, {"status": "fresh", "degraded": True})
        return
    except Exception as exc:
        logger.exception("handler work failed: %s", exc)
        req._json(200, {"ok": False, "reason": f"internal: {type(exc).__name__}"})
        return
    req._json(200, result)


def _exclusive_holder(
    coordinator: CoordinatorHTTPServer,
    artifact_id: UUID,
    *,
    exclude_agent: UUID,
) -> tuple[Optional[UUID], Optional[int]]:
    """Return (agent_id, granted_at_tick) of any current M∪E holder OTHER
    than the given agent. Used for KTD-9 collision detection in pre-edit."""
    state_map = coordinator.registry.get_state_map(artifact_id)
    for other_id, state in state_map.items():
        if other_id == exclude_agent:
            continue
        if state in (MESIState.EXCLUSIVE, MESIState.MODIFIED):
            granted_at = coordinator.registry.granted_at_tick(other_id, artifact_id)
            return other_id, granted_at
    return None, None


def _peers_in_me_excluding(
    coordinator: CoordinatorHTTPServer,
    artifact_id: UUID,
    agent_id: UUID,
) -> list[UUID]:
    """A1: return the list of agents currently in MODIFIED or EXCLUSIVE on
    the given artifact, EXCLUDING the given agent. Used to snapshot victims
    BEFORE service.write side-effects invalidate them, so the plugin can
    record preemption notices for each."""
    state_map = coordinator.registry.get_state_map(artifact_id)
    return [
        peer_id
        for peer_id, state in state_map.items()
        if peer_id != agent_id and state in (MESIState.EXCLUSIVE, MESIState.MODIFIED)
    ]


def _iso_utc(unix_ts: float) -> str:
    """Format a unix timestamp as an ISO 8601 UTC string for prose."""
    from datetime import datetime, timezone
    return datetime.fromtimestamp(unix_ts, tz=timezone.utc).isoformat()


def _build_preemption_text(
    coordinator: CoordinatorHTTPServer,
    notices: list[tuple[UUID, UUID, float]],
) -> str:
    """A1: render pending preemption notices as additionalContext prose.

    notices: list of (artifact_id, preempter_agent_id, preempted_at_unix_ts).
    Variance per invocation comes from the timestamps (real preemption time)
    + the session-id prefixes. Multiple notices → multi-line prose.
    """
    lines: list[str] = ["⚠ Coordinator notice: your EXCLUSIVE grant was preempted:"]
    for artifact_id, preempter_id, ts in notices:
        artifact = coordinator.registry.get_artifact(artifact_id)
        path = artifact.name if artifact else "<unknown-artifact>"
        preempter_session = _agent_id_to_session(coordinator, preempter_id) or "<unknown>"
        lines.append(
            f"  • {path} — preempted/revoked by session {preempter_session[:8]} "
            f"at {_iso_utc(ts)}. Any local edit you made to this file will land "
            f"in your worktree but is NOT reflected in the coordinator's version."
        )
    lines.append(
        "Re-read affected files before continuing if you need the latest "
        "coordinator-tracked version, or proceed knowing your edits remain "
        "local-only until you re-acquire and commit."
    )
    return "\n".join(lines)


def _last_writer_for(coordinator: CoordinatorHTTPServer, artifact_id: UUID) -> Optional[str]:
    """Return the session_id (not agent UUID) of the artifact's last writer, if any."""
    # The registry's _fetch_artifact_row returns last_writer_id; we expose it
    # via lookup. For now derive from agent_names cache.
    state_map = coordinator.registry.get_state_map(artifact_id)
    # Best signal: an agent currently in MODIFIED state.
    for agent_id, state in state_map.items():
        if state == MESIState.MODIFIED:
            return _agent_id_to_session(coordinator, agent_id)
    # Fall back: any known agent (better than nothing).
    if state_map:
        first_agent = next(iter(state_map))
        return _agent_id_to_session(coordinator, first_agent)
    return None


def _last_writer_unix_ts(
    coordinator: CoordinatorHTTPServer, artifact_id: UUID
) -> Optional[float]:
    """Return the REAL wall-clock time the artifact was last written, from
    `artifacts.updated_at` in the registry. Reads directly via the registry's
    private connection because this is a plugin-internal projection. None if
    the artifact is unknown."""
    with coordinator.registry._lock:
        row = coordinator.registry._conn.execute(
            "SELECT updated_at FROM artifacts WHERE id = ?",
            (artifact_id.hex,),
        ).fetchone()
    return float(row[0]) if row else None


def _agent_id_to_session(coordinator: CoordinatorHTTPServer, agent_id: UUID) -> Optional[str]:
    """Reverse the session_to_agent_id mapping via agent_names."""
    name = coordinator._agent_names.get(agent_id)
    if name and name.startswith("claude-session-"):
        return name[len("claude-session-"):]
    return None


def _append_policy_yaml(yaml_path: Path, new_paths: list[str]) -> tuple[list[str], list[dict]]:
    """Append valid patterns to a YAML file. Returns (added, rejected).
    Honors MAX_POLICY_YAML_BYTES — raises ValueError if the resulting file
    would exceed the cap."""
    yaml_path.parent.mkdir(parents=True, exist_ok=True)
    # Validate each path the same way TrackedArtifactPolicy does.
    added: list[str] = []
    rejected: list[dict] = []
    for p in new_paths:
        if not p:
            rejected.append({"path": p, "reason": "empty"})
            continue
        if p.startswith("/"):
            rejected.append({"path": p, "reason": "absolute path"})
            continue
        if ".." in Path(p).parts:
            rejected.append({"path": p, "reason": "contains '..'"})
            continue
        added.append(p)

    if not added:
        return added, rejected

    existing = ""
    if yaml_path.is_file():
        existing = yaml_path.read_text()
    new_lines = "\n".join(f"- {p}" for p in added)
    new_content = (existing.rstrip("\n") + "\n" + new_lines + "\n") if existing else (new_lines + "\n")
    if len(new_content.encode("utf-8")) > MAX_POLICY_YAML_BYTES:
        raise ValueError(f"policy YAML cap of {MAX_POLICY_YAML_BYTES} bytes would be exceeded")
    yaml_path.write_text(new_content)
    return added, rejected


def _os_pid() -> int:
    import os as _os
    return _os.getpid()
