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
import os
import re
import socketserver
import threading
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Optional
from uuid import NAMESPACE_URL, UUID, uuid5

from ccs.adapters.claude_code import hook_payloads as _payloads
from ccs.adapters.claude_code.bash_path_detector import detect_tracked_paths
from ccs.adapters.claude_code.auth import (
    ensure_secret,
    verify_bearer,
    verify_host,
)
from ccs.adapters.claude_code.policy import TrackedArtifactPolicy
from ccs.coordinator.service import CoordinatorService
from ccs.coordinator.sqlite_registry import SqliteArtifactRegistry
from ccs.core.exceptions import CoherenceError
from ccs.core.states import MESIState

logger = logging.getLogger(__name__)


HANDLER_TIMEOUT_SEC = 4.0

# v0.1.1 KTD-G concurrency limits per plugin docs/known-issues/
# 2026-05-17-watchdog-races.md A7 fix. Watchdog pool size × 2 is the
# upper bound on both (i) work queue depth before we reject with 503,
# and (ii) concurrent HTTP handler threads. Two layers because:
#   - The queue-depth gate (item 1) catches submit-time overflow.
#   - The handler semaphore (item 2) caps thread creation upstream of
#     the watchdog pool, preventing a session that's slow-rolling N
#     overlapping requests from starving the watchdog pool's queue.
# Both are independently effective; running both is defense in depth.
_WATCHDOG_POOL_SIZE = 4
WATCHDOG_QUEUE_LIMIT = _WATCHDOG_POOL_SIZE * 2
HANDLER_CONCURRENCY_LIMIT = _WATCHDOG_POOL_SIZE * 2
"""Each endpoint's coordinator call is bounded to 4s by the watchdog;
leaves 1s of margin under the 5s Claude Code hook timeout (KTD-12 / Unit 4)."""

IN_FLIGHT_DRAIN_TIMEOUT_SEC = 5.0
"""KTD-I (Unit 5 L2): ``shutdown()`` waits up to this many seconds for
in-flight handlers to complete before closing the SQLite registry. After
the deadline, any still-running handlers are abandoned — they may raise
``sqlite3.ProgrammingError`` and return HTTP 500 to their clients. Better
a 500 than a wedged shutdown (silent hang vs observable failure)."""

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
        # v0.1.1 KTD-G item 3: surface watchdog/concurrency degradation
        # rather than letting it stay silent. Counters are read by
        # _handle_status; incremented in _run_or_degrade (timeouts +
        # queue overflows) and _ThreadingHTTPServer.process_request
        # (handler concurrency overflows). Plain ints — CPython GIL
        # guarantees atomicity for the single-attribute increment idiom.
        self._watchdog_timeouts_total: int = 0
        self._watchdog_queue_overflows_total: int = 0

        # KTD-I (Unit 5 L2) — in-flight handler counter. Incremented at
        # dispatch entry via :meth:`acquire_handler_slot`, decremented in
        # the handler's finally via :meth:`release_handler_slot`.
        # :meth:`shutdown` blocks on the counter reaching zero for up to
        # IN_FLIGHT_DRAIN_TIMEOUT_SEC before closing the SQLite registry,
        # so a handler mid-write doesn't see ProgrammingError on a closed
        # connection (silent hang → observable 500 at worst).
        self._in_flight_lock = threading.Lock()
        self._in_flight_zero = threading.Condition(self._in_flight_lock)
        self._in_flight = 0
        self._in_flight_drain_timed_out = False

        # KTD-H/I/L3 (Unit 5 L3) — cold-start timing populated by the
        # lifecycle winner path after self-probe completes. Telemetry
        # surface for the future /status endpoint (Unit 8). 0.0 until the
        # lifecycle module sets it; remains 0.0 when the server is
        # constructed directly in tests.
        self.cold_start_duration_ms: float = 0.0

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
        # SQLite-bound and we want timeouts not parallelism. Size matches
        # _WATCHDOG_POOL_SIZE; KTD-G concurrency limits derive from this.
        self._watchdog = ThreadPoolExecutor(
            max_workers=_WATCHDOG_POOL_SIZE,
            thread_name_prefix="coord-wd",
        )

        # ThreadingHTTPServer — handlers see this instance via .server.coordinator.
        # KTD-G item 2: concurrency_limit caps handler threads at
        # HANDLER_CONCURRENCY_LIMIT (= pool_size × 2); requests above the
        # limit get a synchronous 503 without spawning a handler thread.
        handler_cls = _make_handler_class(self)
        self._server = _ThreadingHTTPServer(
            (bind_host, port),
            handler_cls,
            concurrency_limit=HANDLER_CONCURRENCY_LIMIT,
        )
        self.port = self._server.server_port
        self._serve_thread: Optional[threading.Thread] = None

    def serve_in_thread(self) -> None:
        """Start the serving loop in a daemon thread."""
        if self._serve_thread is not None:
            return
        self._serve_thread = threading.Thread(
            target=self._server.serve_forever, name="coord-http", daemon=True
        )
        self._serve_thread.start()

    def shutdown(self) -> None:
        """Stop the server, drain in-flight handlers, close storage.

        KTD-I (Unit 5 L2): waits up to ``IN_FLIGHT_DRAIN_TIMEOUT_SEC`` for
        the in-flight counter to reach zero before closing the SQLite
        registry. Sets ``shutting_down`` BEFORE the drain so new dispatch
        attempts 503 immediately and don't replenish the counter. After
        the deadline, closes regardless — handlers still mid-write may
        raise ``sqlite3.ProgrammingError`` (becoming HTTP 500 to clients),
        which is observable. The alternative — wedging shutdown waiting
        for a stuck handler — is silent and worse.
        """
        if self._shutting_down:
            return
        self._shutting_down = True
        try:
            # http.server.HTTPServer.shutdown() blocks on an Event set by
            # serve_forever's exit. If serve_in_thread was never called,
            # serve_forever never ran, and the event was never set —
            # shutdown() would wait forever. Guard against that so unit
            # tests that construct a server purely for state manipulation
            # can still tear it down cleanly.
            if self._serve_thread is not None:
                self._server.shutdown()
            self._server.server_close()
        finally:
            self._drain_in_flight(IN_FLIGHT_DRAIN_TIMEOUT_SEC)
            self._watchdog.shutdown(wait=True, cancel_futures=False)
            self.registry.close()

    def acquire_handler_slot(self) -> bool:
        """KTD-I L2: atomic shutting_down check + counter increment.

        Returns False if shutdown has started between the dispatcher's
        outer ``shutting_down`` check and this call (race window of a few
        microseconds). Returns True iff the slot was acquired and the
        caller MUST pair with :meth:`release_handler_slot`."""
        with self._in_flight_lock:
            if self._shutting_down:
                return False
            self._in_flight += 1
            return True

    def release_handler_slot(self) -> None:
        """KTD-I L2: decrement the counter and notify drain waiters when
        it reaches zero. Safe to call from any handler thread's finally."""
        with self._in_flight_lock:
            self._in_flight -= 1
            if self._in_flight <= 0:
                self._in_flight = 0  # defensive — never go negative
                self._in_flight_zero.notify_all()

    def _drain_in_flight(self, timeout_sec: float) -> None:
        """Wait up to ``timeout_sec`` for in-flight handlers to complete.

        Sets :attr:`_in_flight_drain_timed_out` if the deadline elapses
        with handlers still running, so operators can observe the event
        via the eventual /status surface (deferred to Unit 8)."""
        if timeout_sec <= 0:
            return
        deadline = time.monotonic() + timeout_sec
        with self._in_flight_lock:
            while self._in_flight > 0:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    self._in_flight_drain_timed_out = True
                    logger.warning(
                        "shutdown drain timed out after %.1fs with %d handler(s) still in-flight; "
                        "closing registry anyway (KTD-I — observable 500 > wedged shutdown)",
                        timeout_sec, self._in_flight,
                    )
                    return
                self._in_flight_zero.wait(timeout=remaining)

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
    def last_request_at(self) -> float:
        """Wall-clock timestamp of the most recent request, or the
        server start time if no requests have hit it yet.

        P3 ce-review fix #39 (kieran-python): the idle-shutdown loop in
        lifecycle.py previously reached into the private
        ``_last_request_at`` with a ``# type: ignore[attr-defined]``.
        This public property removes the cross-module private access."""
        return self._last_request_at

    @property
    def idle_seconds(self) -> float:
        """Wall-clock seconds since the most recent request."""
        return time.time() - self._last_request_at

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
    """ThreadingMixIn + HTTPServer — concurrent hook handling per request,
    with v0.1.1 KTD-G item 2 handler concurrency semaphore.

    Per plugin docs/known-issues/2026-05-17-watchdog-races.md A7: without
    an upper bound on concurrent handler threads, a same-secret client
    issuing slow-rolling overlapping requests can saturate the watchdog
    pool's _work_queue. KTD-G item 2 caps thread creation upstream of
    the watchdog pool by acquiring a BoundedSemaphore (limit =
    HANDLER_CONCURRENCY_LIMIT = pool_size × 2) BEFORE spawning the
    handler thread. Excess requests receive HTTP 503 synchronously
    without spawning a thread.

    The semaphore is bounded so over-release surfaces as ValueError —
    catches the bug where a handler exit path forgets the release
    rather than silently allowing extra concurrent handlers.
    """

    daemon_threads = True
    allow_reuse_address = True

    def __init__(
        self,
        server_address: tuple[str, int],
        RequestHandlerClass: type,
        *,
        concurrency_limit: int,
    ) -> None:
        super().__init__(server_address, RequestHandlerClass)
        self._concurrency_sem = threading.BoundedSemaphore(concurrency_limit)
        # KTD-G item 3: surfaced in /status. Plain int + GIL-atomic increment.
        self.handler_concurrency_overflows_total: int = 0

    def process_request(self, request: Any, client_address: Any) -> None:
        """Override ThreadingMixIn.process_request to gate handler spawn
        on the concurrency semaphore. If at limit, send 503 directly
        without spawning a thread."""
        if not self._concurrency_sem.acquire(blocking=False):
            self.handler_concurrency_overflows_total += 1
            self._send_concurrency_503(request)
            self.shutdown_request(request)
            return
        try:
            super().process_request(request, client_address)
        except BaseException:
            # If thread spawn fails (rare), release so we don't leak a slot.
            self._concurrency_sem.release()
            raise

    def process_request_thread(self, request: Any, client_address: Any) -> None:
        """Override to release the concurrency semaphore in the handler
        thread's finally block, so the slot is freed when the handler
        completes (NOT when process_request returns — that happens
        immediately after thread spawn)."""
        try:
            super().process_request_thread(request, client_address)
        finally:
            self._concurrency_sem.release()

    @staticmethod
    def _send_concurrency_503(request: Any) -> None:
        """Send a minimal 503 response without going through the full
        BaseHTTPRequestHandler pipeline (which would spawn a thread).
        Conforms to KTD-B.3 C1: single-key {error: lowercase phrase}.
        """
        body = b'{"error":"handler concurrency exceeded"}'
        response = (
            b"HTTP/1.1 503 Service Unavailable\r\n"
            b"Content-Type: application/json; charset=utf-8\r\n"
            b"Content-Length: " + str(len(body)).encode("ascii") + b"\r\n"
            b"Connection: close\r\n"
            b"\r\n"
            + body
        )
        try:
            request.sendall(response)
        except (OSError, BrokenPipeError):
            # Client gave up before we could respond; nothing to recover.
            pass


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
            # KTD-I L2: acquire_handler_slot does the atomic shutting_down
            # check + counter increment. If shutdown started between this
            # call and the dispatcher entry, the slot is denied and we 503
            # without touching the SQLite registry (which may already be
            # mid-close).
            if not coordinator.acquire_handler_slot():
                self._json(503, {"error": "coordinator shutting down"})
                return
            try:
                coordinator.mark_request()

                # Auth + Host check on every endpoint
                if not verify_host(self.headers.get("Host")):
                    self._json(403, {"error": "host header not allowlisted"})
                    logger.warning(
                        "rejected request with bad Host: %r", self.headers.get("Host")
                    )
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
            finally:
                coordinator.release_handler_slot()

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
            # A1 + F4: if this commit failed because the grant was preempted
            # silently, enrich the reason with the preemption context so
            # the caller (and any stream-json telemetry) sees WHO took the
            # grant and WHEN — not just the generic CoherenceError text.
            #
            # F4 (P2): consume the notice here (single-consumer semantics) so
            # the next pre-event for this (agent, artifact) does NOT re-emit
            # the same preemption prose. The subagent flagged this as a
            # double-emit hazard — the post-edit-failure response IS the
            # surfacing channel for this specific case.
            popped = coordinator.registry.pop_preemption_notice(agent_id, artifact_id)
            if popped is not None:
                preempter_id, preempted_at = popped
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

        # F1 (P0): pop any pending preemption notices for the ending session.
        # The phpmac canonical case: X was preempted, but X's next action was
        # a Bash/Grep (not a tracked file op), or the turn just ended — so
        # no pre-read / pre-edit / post-edit fires to drain the notice queue.
        # Without this drain, notices orphan indefinitely (or until F2 evict).
        # We surface them in the response body (telemetry-visible via
        # stream-json `--include-hook-events`) AND, opportunistically, as
        # `additionalContext` so any post-Stop processing or human-readable
        # log still carries the signal.
        pending = coordinator.registry.pop_pending_notices(agent_id)
        notices_payload: list[dict] = []
        for art_id, preempter_id, ts in pending:
            art = coordinator.registry.get_artifact(art_id)
            preempter_session = _agent_id_to_session(coordinator, preempter_id) or ""
            notices_payload.append({
                "path": art.name if art else "<unknown-artifact>",
                "preempter_session_id": preempter_session,
                "preempter_session_short": (preempter_session[:8] if preempter_session else "<unknown>"),
                "preempted_at_unix_ts": ts,
                "preempted_at_iso": _iso_utc(ts),
            })

        response: dict = {"ok": True, "released_artifacts": released}
        if notices_payload:
            response["notices"] = notices_payload
            # Render prose for stream-json consumers / human inspection.
            response["hookSpecificOutput"] = {
                "hookEventName": "Stop",
                "additionalContext": _build_preemption_text(coordinator, pending),
            }
        return response

    _run_or_degrade(req, coordinator, work)


def _handle_policy_track(req, coordinator: CoordinatorHTTPServer) -> None:
    """POST /policy/track — Unit 6 CLI add to tracked.yaml.

    P2 ce-review fixes:
    - #4 (security YAML injection): every path passes validate_path() which
      rejects control chars (newlines), absolute paths, and ../ traversal
      before being appended to tracked.yaml. Without this, an authenticated
      caller could POST {"paths":["real.md\\n- injected.yaml"]} and inject
      additional patterns.
    - #11 (correctness 500→400): _append_policy_yaml's ValueError on YAML
      cap overflow is caught and returned as HTTP 400 instead of falling
      through the catch-all as a 500.
    """
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
    # Pre-validate each path: filter out malformed entries (path traversal,
    # absolute paths, control chars including the newlines that previously
    # allowed YAML injection). Invalid paths join the response's `rejected`
    # list — preserves partial-accept semantics while defending against
    # injection into tracked.yaml.
    safe_paths: list[str] = []
    pre_rejected: list[dict] = []
    for p in paths:
        v_err = validate_path(p)
        if v_err is not None:
            pre_rejected.append({"path": p, "reason": v_err})
        else:
            safe_paths.append(p)
    yaml_path = coordinator.coordinator_root / ".coherence" / "tracked.yaml"
    try:
        added, rejected = _append_policy_yaml(yaml_path, safe_paths)
    except ValueError as exc:
        req._json(400, {"error": str(exc)})
        return
    # Reload the live policy so subsequent hook calls see the additions.
    coordinator.policy = TrackedArtifactPolicy.load(coordinator.coordinator_root)
    req._json(200, {
        "ok": True, "added": added, "rejected": rejected + pre_rejected,
    })


def _handle_policy_untrack(req, coordinator: CoordinatorHTTPServer) -> None:
    """POST /policy/untrack — Unit 6 CLI add to ignored.yaml.

    Same hardening as /policy/track: per-path validate_path call + ValueError
    → HTTP 400 mapping.
    """
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
    # Same defense-in-depth + partial-accept as /policy/track.
    safe_paths: list[str] = []
    for p in paths:
        v_err = validate_path(p)
        if v_err is None:
            safe_paths.append(p)
    yaml_path = coordinator.coordinator_root / ".coherence" / "ignored.yaml"
    try:
        added, _ = _append_policy_yaml(yaml_path, safe_paths)
    except ValueError as exc:
        req._json(400, {"error": str(exc)})
        return
    coordinator.policy = TrackedArtifactPolicy.load(coordinator.coordinator_root)
    req._json(200, {"ok": True, "removed": added})


def _handle_pre_bash(req, coordinator: CoordinatorHTTPServer) -> None:
    """POST /hooks/pre-bash — KTD-N H4 mitigation.

    The v0.2 Phase 0 falsifiability experiment (see
    ``docs/probes/2026-05-19-ktd-e-falsifiability/REPORT.md``) confirmed
    that when a stale-read warning fires on Read, the model retries 2-5
    times then routes around via `Bash cat plan.md` — bypassing the
    coherence layer entirely if Bash is unhooked. KTD-N closes that gap
    for v0.1.1's warn mode (without this, marketplace cohort sees silent
    stale-read misses on the common Bash routing pattern).

    Detects tracked-artifact READS in the Bash command via
    ``bash_path_detector.detect_tracked_paths``. For each detected path,
    runs the same stale-vs-fresh logic as ``/hooks/pre-read``. False
    negatives are acceptable (adversarial obfuscation, command
    substitution, etc. are OUT of scope per KTD-N).

    Request: ``{session_id, command}``.
    Response:
      - ``{status: "fresh"}`` if no tracked paths detected (fast path)
      - ``{status: "fresh"}`` if all detected paths are fresh
      - ``{status: "stale", hookSpecificOutput: {...}, stale_paths: [...]}``
        if any detected path is stale; ``additionalContext`` lists the
        affected paths and prepends any pending preemption notices
    """
    body = req._read_json()
    if body is None:
        return
    session_id = body.get("session_id")
    command = body.get("command")

    err = validate_session_id(session_id)
    if err:
        req._json(400, {"error": "missing session_id" if err.startswith("session_id must be a string") else err})
        return
    if not isinstance(command, str) or not command.strip():
        req._json(400, {"error": "missing or empty command"})
        return
    if len(command) > 16384:
        # Bash commands beyond 16K are pathological; reject rather than
        # spend CPU on the regex pipeline. Matches MAX_REQUEST_BODY_BYTES
        # spirit (KTD-K item 4 / R21 — defense in depth).
        req._json(413, {"error": "command too long"})
        return

    # Detect tracked paths the command would read. is_tracked is the
    # policy gate — handler never touches SQLite for an untracked workspace.
    tracked_paths = detect_tracked_paths(command, coordinator.policy.is_tracked)
    if not tracked_paths:
        req._json(200, {"status": "fresh"})
        return

    agent_id = coordinator.register_session(session_id)
    now = monotonic_seconds()

    def work() -> dict:
        coordinator.service.record_heartbeat(agent_id=agent_id, now_tick=now)
        stale_summaries: list[dict] = []
        for path in tracked_paths:
            artifact_id = coordinator.registry.lookup_artifact_id_by_name(path)
            if artifact_id is None:
                # First observation per KTD-9 — seed v1 + grant SHARED so
                # subsequent reads see fresh.
                artifact_id = coordinator.registry.resolve_or_register(
                    path, content_hash=""
                )
                coordinator.registry.set_agent_state(
                    artifact_id, agent_id, MESIState.SHARED,
                    trigger="first_bash_read", tick=now,
                )
                continue
            agent_state = coordinator.registry.get_agent_state(artifact_id, agent_id)
            if agent_state is not None and agent_state != MESIState.INVALID:
                continue  # fresh on this path
            # Stale. Record summary; re-grant SHARED to suppress repeat fires.
            artifact = coordinator.registry.get_artifact(artifact_id)
            stale_summaries.append({
                "path": path,
                "current_version": artifact.version,
            })
            coordinator.registry.set_agent_state(
                artifact_id, agent_id, MESIState.SHARED,
                trigger="post_stale_bash", tick=now,
            )

        notices = coordinator.registry.pop_pending_notices(agent_id)

        if not stale_summaries and not notices:
            return {"status": "fresh"}

        # Build merged additionalContext: notices first (most-urgent),
        # then bash-multipath stale warning.
        parts: list[str] = []
        if notices:
            parts.append(_build_preemption_text(coordinator, notices))
        if stale_summaries:
            paths_str = ", ".join(
                f"{s['path']} (current v{s['current_version']})"
                for s in stale_summaries
            )
            parts.append(
                f"⚠ Bash command reads tracked artifacts that have been "
                f"updated since your session's last fresh read: {paths_str}. "
                f"The command will still execute (v0.1.1 is warn-only), but "
                f"consider re-reading via the Read tool before relying on "
                f"the output as ground truth."
            )

        resp: dict[str, Any] = {
            "hookSpecificOutput": {
                "hookEventName": "PreToolUse",
                "permissionDecision": "allow",  # v0.1.1 warn-only per KTD-E
                "additionalContext": "\n\n".join(parts),
            },
        }
        if stale_summaries:
            resp["status"] = "stale"
            resp["stale_paths"] = [s["path"] for s in stale_summaries]
        else:
            resp["status"] = "fresh"
        return resp

    _run_or_degrade(req, coordinator, work)


def _handle_pre_grep(req, coordinator: CoordinatorHTTPServer) -> None:
    """POST /hooks/pre-grep — KTD-N H4 mitigation, Grep variant.

    Same threat model as ``/hooks/pre-bash`` but for the Grep tool:
    when the model uses Grep over a directory containing tracked
    artifacts, surface stale-read warnings for any artifacts the
    session has not freshened since peer commits.

    Request: ``{session_id, search_root}`` where ``search_root`` is
    the parent-repo-relative path Grep is scanning (== Grep tool's
    ``path`` arg, empty string for workspace root).

    Response shape mirrors /hooks/pre-bash.
    """
    body = req._read_json()
    if body is None:
        return
    session_id = body.get("session_id")
    search_root = body.get("search_root", "")

    err = validate_session_id(session_id)
    if err:
        req._json(400, {"error": "missing session_id" if err.startswith("session_id must be a string") else err})
        return
    # search_root may be "" (workspace root). If non-empty, apply path validator.
    if search_root != "":
        v = validate_path(search_root)
        if v is not None:
            req._json(400, {"error": v})
            return

    # Find registry-known tracked artifacts under the search root.
    tracked_paths = coordinator.registry.artifact_names_under_prefix(search_root)
    if not tracked_paths:
        req._json(200, {"status": "fresh"})
        return

    agent_id = coordinator.register_session(session_id)
    now = monotonic_seconds()

    def work() -> dict:
        coordinator.service.record_heartbeat(agent_id=agent_id, now_tick=now)
        stale_summaries: list[dict] = []
        for path in tracked_paths:
            artifact_id = coordinator.registry.lookup_artifact_id_by_name(path)
            if artifact_id is None:
                continue  # registry-listed but raced away; skip
            agent_state = coordinator.registry.get_agent_state(artifact_id, agent_id)
            if agent_state is not None and agent_state != MESIState.INVALID:
                continue
            artifact = coordinator.registry.get_artifact(artifact_id)
            stale_summaries.append({
                "path": path,
                "current_version": artifact.version,
            })
            coordinator.registry.set_agent_state(
                artifact_id, agent_id, MESIState.SHARED,
                trigger="post_stale_grep", tick=now,
            )

        notices = coordinator.registry.pop_pending_notices(agent_id)

        if not stale_summaries and not notices:
            return {"status": "fresh"}

        parts: list[str] = []
        if notices:
            parts.append(_build_preemption_text(coordinator, notices))
        if stale_summaries:
            paths_str = ", ".join(
                f"{s['path']} (current v{s['current_version']})"
                for s in stale_summaries
            )
            parts.append(
                f"⚠ Grep search over tracked artifacts your session has "
                f"not freshened since peer commits: {paths_str}. The "
                f"results may reflect outdated content. Consider re-reading "
                f"via Read before acting on Grep output."
            )

        resp: dict[str, Any] = {
            "hookSpecificOutput": {
                "hookEventName": "PreToolUse",
                "permissionDecision": "allow",
                "additionalContext": "\n\n".join(parts),
            },
        }
        if stale_summaries:
            resp["status"] = "stale"
            resp["stale_paths"] = [s["path"] for s in stale_summaries]
        else:
            resp["status"] = "fresh"
        return resp

    _run_or_degrade(req, coordinator, work)


def _handle_status(req, coordinator: CoordinatorHTTPServer) -> None:
    """GET /status — drives the agent-coherence-status console script.

    P2 ce-review fix #18 (kieran-python): batched the per-(session,
    artifact) query loop. The previous version did
    ``get_agent_state(artifact_id, agent_id)`` and a SECOND
    ``get_artifact(artifact_id)`` per inner iteration — O(sessions ×
    artifacts) SQLite round-trips per /status request. Now we
    build a single ``{artifact_id: Artifact}`` lookup outside the loop
    and call ``get_state_map(artifact_id)`` once per artifact (which
    returns ``{agent_id: MESIState}`` in one query). Per-session
    iteration becomes a dict lookup — O(artifacts) total queries.
    """
    # A4: snapshot artifact_ids and _agent_names BEFORE iterating, to avoid
    # `RuntimeError: dictionary changed size during iteration` racing against
    # a concurrent register_session in another handler.
    artifact_ids_snapshot = list(coordinator.registry.artifact_ids())
    agent_names_snapshot = list(coordinator._agent_names.items())

    # Single-pass artifact resolution: one get_artifact per artifact id,
    # cached in artifact_by_id for the inner-loop lookups below.
    artifact_by_id = {}
    for artifact_id in artifact_ids_snapshot:
        art = coordinator.registry.get_artifact(artifact_id)
        if art is not None:
            artifact_by_id[artifact_id] = art

    tracked: list[dict] = [
        {"path": art.name, "version": art.version, "id": str(artifact_id)}
        for artifact_id, art in artifact_by_id.items()
    ]

    # Single-pass per-artifact state map: one get_state_map per artifact
    # (returns all agents' states for that artifact in one query). The
    # session/artifact cross-product becomes a dict lookup.
    state_by_artifact = {
        artifact_id: coordinator.registry.get_state_map(artifact_id)
        for artifact_id in artifact_by_id  # only artifacts we actually have
    }

    sessions: list[dict] = []
    for agent_id, name in agent_names_snapshot:
        per_artifact: dict[str, str] = {}
        for artifact_id, art in artifact_by_id.items():
            state = state_by_artifact[artifact_id].get(agent_id)
            if state is not None and state != MESIState.INVALID:
                per_artifact[art.name] = state.name
        sessions.append({
            "agent_name": name,
            "agent_id": str(agent_id),
            "states": per_artifact,
        })

    # v0.1.1 KTD-G item 3 + KTD-J: surface watchdog / concurrency degradation
    # via /status so silent degradation is observable. agent-coherence-status
    # CLI consumes this block; operators reading bug reports can spot
    # watchdog saturation immediately.
    req._json(200, {
        "tracked_artifacts": tracked,
        "sessions": sessions,
        "coordinator_uptime_s": coordinator.uptime_s,
        "coordinator_pid": os.getpid(),
        "policy_summary": coordinator.policy.summary(),
        "watchdog_timeouts_total": coordinator._watchdog_timeouts_total,
        "watchdog_queue_overflows_total": coordinator._watchdog_queue_overflows_total,
        "handler_concurrency_overflows_total": coordinator._server.handler_concurrency_overflows_total,
    })


_ROUTES: dict[tuple[str, str], Callable] = {
    ("POST", "/hooks/pre-read"): _handle_pre_read,
    ("POST", "/hooks/pre-edit"): _handle_pre_edit,
    ("POST", "/hooks/post-edit"): _handle_post_edit,
    ("POST", "/hooks/session-stop"): _handle_session_stop,
    # v0.1.1 KTD-N — H4 mitigation: catch model routing-around-via-Bash/Grep
    # to bypass the Read-only stale-read warning. Per the v0.2 Phase 0
    # falsifiability experiment, the model retries Read 2-5 times then
    # routes via `bash cat plan.md`; unhooked Bash means silent stale miss.
    ("POST", "/hooks/pre-bash"): _handle_pre_bash,
    ("POST", "/hooks/pre-grep"): _handle_pre_grep,
    ("POST", "/policy/track"): _handle_policy_track,
    ("POST", "/policy/untrack"): _handle_policy_untrack,
    ("GET", "/status"): _handle_status,
}


# ----------------------------------------------------------------------
# Internals
# ----------------------------------------------------------------------


def _run_or_degrade(req, coordinator: CoordinatorHTTPServer, work: Callable[[], dict]) -> None:
    """Run ``work`` under the handler-side watchdog. On timeout, log WARNING
    and return 200 {status:"fresh"} so the user's tool call proceeds.

    v0.1.1 KTD-G:
      - Item 1: queue-depth gate. Reject with HTTP 503 if the watchdog
        ThreadPoolExecutor's _work_queue is past WATCHDOG_QUEUE_LIMIT
        items. Prevents the silent-degradation cascade documented in
        plugin docs/known-issues/2026-05-17-watchdog-races.md A7 where
        queued tasks wait long enough in the executor queue that they
        race the future's timeout on submit-side.
      - Item 3: increment ``_watchdog_timeouts_total`` on FuturesTimeout
        so silent degradation becomes observable via /status.

    Item 2 (handler concurrency semaphore) lives in
    _ThreadingHTTPServer.process_request — gates BEFORE this function
    is reached.
    """
    # KTD-G item 1: queue-depth gate. Use a defensive try because
    # ThreadPoolExecutor's _work_queue attribute is technically private
    # — guard against future stdlib changes that would break this.
    try:
        qsize = coordinator._watchdog._work_queue.qsize()  # type: ignore[attr-defined]
    except AttributeError:
        qsize = 0
    if qsize > WATCHDOG_QUEUE_LIMIT:
        coordinator._watchdog_queue_overflows_total += 1
        logger.warning(
            "watchdog queue at %d items (limit %d); rejecting with 503",
            qsize,
            WATCHDOG_QUEUE_LIMIT,
        )
        req._json(503, {"error": "watchdog queue overloaded"})
        return

    try:
        result = coordinator.run_with_watchdog(work)
    except FuturesTimeout:
        # KTD-G item 3: surface watchdog degradation via /status counter.
        coordinator._watchdog_timeouts_total += 1
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
    return datetime.fromtimestamp(unix_ts, tz=timezone.utc).isoformat()


#: F3 hardening — render up to this many notices verbatim; coalesce the
#: rest into a single "Plus K more …" line that points at the status surface.
#: Chosen so the rendered prose stays comfortably under Claude Code's 10KB
#: additionalContext cap even with the prepended stale-read warning (each
#: verbatim line ≈ 250 bytes; budget of 3 × 250 + header/footer keeps total
#: well under 4KB before any stale-read prepend).
_PREEMPTION_PROSE_VERBATIM_CAP = 3


def _build_preemption_text(
    coordinator: CoordinatorHTTPServer,
    notices: list[tuple[UUID, UUID, float]],
) -> str:
    """A1 + F3: render pending preemption notices as additionalContext prose.

    notices: list of (artifact_id, preempter_agent_id, preempted_at_unix_ts).
    Variance per invocation comes from the timestamps (real preemption time)
    + the session-id prefixes.

    F3 hardening: render newest-first up to ``_PREEMPTION_PROSE_VERBATIM_CAP``
    notices in full. If more remain, coalesce them into a single overflow line
    pointing at the ``/agent-coherence status`` console for the full list.
    This bounds the prose to a constant-size block regardless of N, sidesteps
    Claude Code's 10KB additionalContext cap, and uses the status surface as
    the overflow channel rather than silently truncating.
    """
    # Sort newest first — the most recent preemption is the most informative
    # signal for the agent's next decision.
    sorted_notices = sorted(notices, key=lambda n: n[2], reverse=True)
    verbatim = sorted_notices[:_PREEMPTION_PROSE_VERBATIM_CAP]
    overflow = sorted_notices[_PREEMPTION_PROSE_VERBATIM_CAP:]

    lines: list[str] = ["⚠ Coordinator notice: your EXCLUSIVE grant was preempted:"]
    for artifact_id, preempter_id, ts in verbatim:
        artifact = coordinator.registry.get_artifact(artifact_id)
        path = artifact.name if artifact else "<unknown-artifact>"
        preempter_session = _agent_id_to_session(coordinator, preempter_id) or "<unknown>"
        lines.append(
            f"  • {path} — preempted/revoked by session {preempter_session[:8]} "
            f"at {_iso_utc(ts)}. Any local edit you made to this file will land "
            f"in your worktree but is NOT reflected in the coordinator's version."
        )
    if overflow:
        lines.append(
            f"  • Plus {len(overflow)} more preemptions since your last activity; "
            f"run `/agent-coherence status` (or query GET /status on the coordinator) "
            f"for the full list."
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
    `artifacts.updated_at` in the registry. None if the artifact is unknown.

    P2 ce-review fix #16: uses the public ``get_artifact_updated_at()``
    accessor on SqliteArtifactRegistry instead of reaching into ``_conn``
    + ``_lock`` directly. Layer violation closed."""
    return coordinator.registry.get_artifact_updated_at(artifact_id)


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
