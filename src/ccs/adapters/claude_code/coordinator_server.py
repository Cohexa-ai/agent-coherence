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
from typing import Any, Callable, Literal, Protocol
from typing import runtime_checkable
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


# Finding #30 — typed interface for the `req` parameter passed to all
# endpoint handlers. The concrete implementation lives inside the
# _make_handler_class closure as a BaseHTTPRequestHandler subclass; the
# Protocol lets handlers declare the interface they need without coupling
# to the concrete class or breaking the closure structure.
@runtime_checkable
class _RequestProtocol(Protocol):
    """Minimal interface every endpoint handler expects from `req`."""

    headers: Any  # http.client.HTTPMessage (Mapping-like)
    path: str

    def _read_json(self) -> dict | None:
        """Read + parse the request body as JSON. Returns None on error."""
        ...

    def _json(self, status: int, body: dict) -> None:
        """Write a JSON response with the given HTTP status code."""
        ...


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

# ADV-004: a sentinel preempter UUID the stable-grant sweep uses when
# recording a preemption notice for an agent whose M/E grant it just
# reclaimed. The F4 enrichment in ``_handle_post_edit`` compares against
# this constant to distinguish "your grant was reclaimed by the sweep"
# from "your grant was preempted by another session" — both surface via
# the same notice table but communicate distinct failure modes to the
# model. UUID derived from a stable namespace string so it stays the
# same across processes, restarts, and instances.
SWEEP_RECLAMATION_PREEMPTER_ID: UUID = uuid5(
    NAMESPACE_URL, "ccs-coordinator-sweep:stable-grant-reclamation"
)

MAX_POLICY_PATHS_PER_REQUEST = 20
"""Cap on the number of paths /policy/track and /policy/untrack accept
in one request body (security-lens P1)."""

MAX_POLICY_YAML_BYTES = 64 * 1024
"""Cap on the resulting tracked.yaml / ignored.yaml file size (security-lens P1)."""

MAX_PATH_LEN = 1024
"""Cap on inbound path length to defend against memory/DoS and prose-injection
attacks. 1024 covers nested-deep paths in any realistic project."""

MAX_REQUEST_BODY_BYTES = 64 * 1024
"""R21 (Unit 6): hard cap on the Content-Length the HTTP server is willing
to read into memory. Matches MAX_POLICY_YAML_BYTES — generous for the
~1 KB hook payloads we actually expect, tight enough that a hostile or
buggy client cannot OOM the coordinator with a single oversized body.
Validated BEFORE rfile.read so we never allocate the offending buffer."""


def _resolve_coordinator_version() -> str:
    """KTD-J (Unit 8): surface the package version via /status so operators
    pasting status output into bug reports always include which build
    they're running. Reads ``ccs.__version__`` lazily so test fixtures
    that import this module before ``ccs`` is fully loaded don't crash."""
    try:
        from ccs import __version__ as _v
        return _v
    except Exception:  # pragma: no cover — defensive against import order
        return "unknown"


_COORDINATOR_VERSION: str = _resolve_coordinator_version()

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


def validate_session_id(
    session_id: Any,
) -> tuple[Literal["MISSING", "MALFORMED"], str] | None:
    """Return (error_kind, reason) if invalid, None if valid. UUID-shape required.

    AC-51 / finding #51: structured error kind lets callers branch on MISSING vs
    MALFORMED without string-prefix matching (``err.startswith(...)``).
    """
    if not isinstance(session_id, str):
        return ("MISSING", "missing session_id")
    if not _SESSION_ID_RE.match(session_id):
        return ("MALFORMED", "session_id must be a UUID (8-4-4-4-12 hex with hyphens)")
    return None


def validate_path(path: Any) -> str | None:
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


def validate_content_hash(content_hash: Any, *, required: bool) -> str | None:
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
        agent_names: dict[UUID, str] | None = None,
        state_log: Callable[[dict[str, Any]], None] | None = None,
        instance_id: str | None = None,
    ) -> None:
        self.coordinator_root = Path(coordinator_root).resolve()
        self.bind_host = bind_host
        self._started_at = time.time()
        self._last_request_at = self._started_at
        self._shutting_down = False
        # ADV-001 (fix): "migration draining" is a halfway state between
        # serving and shutting_down. While true, new pre-edit requests are
        # rejected (would mint an EXCLUSIVE that the agent can never
        # post-edit since shutdown is imminent). All other endpoints —
        # pre-read, post-edit, session-stop, status — continue to be
        # served so in-flight pre-edit→post-edit chains can complete
        # naturally. Set by /admin/prepare-for-migration; cleared only
        # at process exit (no rollback path).
        self._migration_draining = False
        self._agent_names: dict[UUID, str] = dict(agent_names or {})
        # R10 (Unit 6): explicit lock around _agent_names mutation. CPython's
        # GIL makes single-key dict assignment effectively atomic today, but
        # the project standard is "don't rely on GIL" so the contract holds
        # on PyPy / future free-threading builds and so the snapshot pattern
        # at _handle_status (list(items()) under the lock) sees a consistent
        # view. threading.Lock per pattern (NOT RLock — no nested acquisition).
        self._agent_names_lock = threading.Lock()
        # v0.1.1 KTD-G item 3: surface watchdog/concurrency degradation
        # rather than letting it stay silent. Counters are read by
        # _handle_status; incremented in _run_or_degrade (timeouts +
        # queue overflows) and _ThreadingHTTPServer.process_request
        # (handler concurrency overflows).
        #
        # REL-03 (free-threading-safe): under CPython's traditional
        # build, ``x += 1`` on an int attribute is effectively atomic
        # because the GIL serializes bytecode execution. Under Python
        # 3.13+ free-threaded builds (PEP 703) and on PyPy, that
        # guarantee is gone — concurrent threads can read-modify-write
        # the same counter and tear increments. These three counters
        # are operator-facing reliability signals (a missed bump means
        # an under-reported degradation event in a bug report), so
        # protect their mutation with a lock. Product-signal counters
        # (intra_task_acquire_release_total, stale_warning_*_total)
        # stay GIL-reliant per the reviewer's recommendation: they're
        # advisory ratios, not absolute counts.
        self._reliability_counter_lock = threading.Lock()
        self._watchdog_timeouts_total: int = 0
        self._watchdog_queue_overflows_total: int = 0
        # P1 #6: silent 401 surface. If hook.secret is deleted out from
        # under a running coordinator (operator misclick, accidental
        # rm in .coherence), every subsequent hook request from any
        # session 401s. The client treats 401 as "no coordinator
        # available" and degrades silently — agents lose the coherence
        # layer with zero operator signal. Counter + WARNING log
        # surface the symptom; 60s dedupe avoids spamming the log when
        # a real burst hits. ``self._last_401_warn_at`` is the
        # monotonic timestamp of the last warning emission.
        self._auth_401_total: int = 0
        self._last_401_warn_at: float = 0.0
        self._auth_401_warn_lock = threading.Lock()
        # P1 #5 detection-only: when ``run_with_watchdog`` raises
        # FuturesTimeout, the handler returns degraded — but the
        # underlying ``work()`` future is left running in the pool
        # (cancel_futures=False). If it eventually completes
        # successfully, any state it mutated (EXCLUSIVE grant from
        # ``service.write``, for instance) lands in the registry AFTER
        # the agent received a degraded response — a phantom grant the
        # agent will never post-edit. We can't cancel the future
        # without invasive cancel-token plumbing through service.write,
        # but we CAN detect it: every timed-out future gets a
        # done_callback that bumps this counter + logs CRITICAL if the
        # future eventually finished without exception. Operators see
        # the symptom via ``/status?detail=metrics`` even when the
        # cause is rare (4s deadline + 1.5s busy_timeout = real-world
        # only hits with a wedged SQLite or contended drive).
        self._watchdog_late_completion_total: int = 0

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

        # KTD-J (Unit 8) — telemetry counters. CACHE, not persistent
        # state: reset to 0 on coordinator respawn (do NOT persist in
        # state.db per the plan rationale). Plain ints — CPython's
        # GIL guarantees atomicity for ``+= 1``; the contract is
        # "advisory, not auditable", so a missed increment on a
        # future free-threading build is acceptable.
        #
        # Per-endpoint request counters — drive operator visibility
        # into which hooks fire and how often. Surfaced via
        # /status?detail=full and /status?detail=metrics.
        self._endpoint_counters: dict[str, int] = {
            "pre_read_total": 0,
            "pre_edit_total": 0,
            "post_edit_total": 0,
            "session_stop_total": 0,
            "pre_bash_total": 0,
            "pre_grep_total": 0,
            "policy_track_total": 0,
            "policy_untrack_total": 0,
            "status_total": 0,
        }
        self._endpoint_counters_lock = threading.Lock()

        # KTD-J product-signal counters. These shape v0.2 / hosted-tier
        # decisions, so each has documented economic meaning.
        #
        # intra_task_acquire_release_total: how often a session acquired
        # EXCLUSIVE and released within the same dispatch chain. Sizes
        # the hosted-tier upsell argument (signal that fine-grained
        # write protection is exercised, not idle).
        #
        # stale_warning_emitted_total: how often /hooks/pre-read or
        # /hooks/pre-bash returned a stale-summary response. Denominator
        # for the operator-computed re-read rate.
        #
        # stale_warning_reread_total: how often the agent re-read after
        # a stale warning (heuristic: same session re-reads same path
        # within HANDLER_TIMEOUT_SEC × 4 of receiving a stale warning).
        # Numerator for re-read rate; operator computes the ratio at
        # query time.
        self._intra_task_acquire_release_total: int = 0
        self._stale_warning_emitted_total: int = 0
        self._stale_warning_reread_total: int = 0

        # KTD-J re-read detection: a stale warning marks an (agent, artifact)
        # pair as "warned"; the next pre-read on that pair consumes the
        # marker and bumps :attr:`_stale_warning_reread_total`. The set
        # cannot grow without bound — any subsequent pre-read clears the
        # entry, and a worst-case scenario of one entry per active
        # (agent, artifact) pair is the same upper bound as the registry's
        # own state map.
        self._stale_warned_pairs: set[tuple[UUID, UUID]] = set()
        self._stale_warned_pairs_lock = threading.Lock()

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
        self._serve_thread: threading.Thread | None = None

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

        COR-07: actual wall-clock shutdown time can EXCEED
        ``IN_FLIGHT_DRAIN_TIMEOUT_SEC`` when watchdog timeouts have
        fired. The in-flight counter decrements when the handler
        thread returns (after FuturesTimeout), but the corresponding
        watchdog-pool future may still be running. The subsequent
        ``self._watchdog.shutdown(wait=True, cancel_futures=False)``
        waits for those orphaned futures to complete. Worst-case
        addition: one extra ``HANDLER_TIMEOUT_SEC`` (4s) per orphaned
        future. ``cancel_futures=True`` would shorten shutdown but
        risk aborting a SQLite write mid-transaction; documented
        trade-off, not a bug.
        """
        # REL-05 / finding #42: protect the check-then-set with a lock so
        # concurrent callers (idle thread + stop_coordinator) cannot both
        # observe _shutting_down=False and both enter the shutdown body.
        # _in_flight_lock is the right granularity: shutdown is what drives
        # the drain, so no new lock is needed.
        with self._in_flight_lock:
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
            #
            # COR-01: order is shutdown → drain → server_close → registry.close.
            # _server.shutdown() stops the serve_forever accept loop but does
            # NOT close in-flight handler threads (they own their accepted
            # sockets and finish writing on their own). Drain those slots
            # BEFORE server_close + registry.close, so the registry remains
            # open while a handler is still mid-transaction. Closing the
            # listening socket is independent of accepted-connection sockets
            # but keeping it open through the drain matches the canonical
            # shutdown ordering and removes a refactor footgun.
            if self._serve_thread is not None:
                self._server.shutdown()
        finally:
            self._drain_in_flight(IN_FLIGHT_DRAIN_TIMEOUT_SEC)
            try:
                self._server.server_close()
            except Exception:  # noqa: BLE001 - best-effort cleanup
                logger.exception("server_close raised during shutdown")
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
    def migration_draining(self) -> bool:
        """ADV-001: True between the prepare-for-migration trigger and the
        scheduled shutdown. The dispatcher uses this to reject NEW write
        initiations (pre-edit) while still serving in-flight chains'
        completions (post-edit) and all read/observability endpoints."""
        return self._migration_draining

    @property
    def shutting_down(self) -> bool:
        return self._shutting_down

    def register_session(self, session_id: str) -> UUID:
        """Idempotent session registration. Returns the deterministic agent UUID.

        R10 (Unit 6): mutation is wrapped in ``_agent_names_lock`` so the
        check-then-set is atomic w.r.t. concurrent registrations AND so
        :meth:`agent_name_for` snapshots see a consistent dict — relying
        on the GIL is forbidden by the project standard."""
        agent_id = session_to_agent_id(session_id)
        with self._agent_names_lock:
            if agent_id not in self._agent_names:
                self._agent_names[agent_id] = session_to_agent_name(session_id)
        return agent_id

    def agent_names_snapshot(self) -> list[tuple[UUID, str]]:
        """R10 (Unit 6): return a stable list snapshot of (agent_id, name)
        pairs taken under the lock — callers iterate over the snapshot
        rather than the live dict so a concurrent register_session cannot
        invalidate the iteration (RuntimeError: dictionary changed size)."""
        with self._agent_names_lock:
            return list(self._agent_names.items())

    def agent_name_for(self, agent_id: UUID) -> str | None:
        """R10 (Unit 6): single-key read under the lock. Returns None if
        the agent has never been registered."""
        with self._agent_names_lock:
            return self._agent_names.get(agent_id)

    def increment_endpoint_counter(self, name: str) -> None:
        """KTD-J (Unit 8): bump a per-endpoint counter. Names match the
        keys in ``_endpoint_counters`` (e.g., ``pre_read_total``). Unknown
        names are silently ignored — counters are advisory; a typo in a
        future endpoint dispatch must not crash the request."""
        with self._endpoint_counters_lock:
            if name in self._endpoint_counters:
                self._endpoint_counters[name] += 1

    def endpoint_counters_snapshot(self) -> dict[str, int]:
        """KTD-J (Unit 8): stable snapshot of per-endpoint counters.
        Taken under the lock so concurrent increments cannot tear the
        view."""
        with self._endpoint_counters_lock:
            return dict(self._endpoint_counters)

    def increment_intra_task_acquire_release(self) -> None:
        """KTD-J product-signal counter. Increment when a session's
        EXCLUSIVE grant is released within the same dispatch chain that
        acquired it (post-edit on the same artifact as the pre-edit).
        Documents how often fine-grained write protection actually fires
        — feeds the v0.2 / hosted-tier upsell case."""
        # CPython GIL guarantees atomicity of += on a plain int; no
        # explicit lock needed for this advisory counter.
        self._intra_task_acquire_release_total += 1

    def increment_stale_warning_emitted(self) -> None:
        """KTD-J: denominator counter for operator-computed re-read rate."""
        self._stale_warning_emitted_total += 1

    def increment_stale_warning_reread(self) -> None:
        """KTD-J: numerator counter for operator-computed re-read rate."""
        self._stale_warning_reread_total += 1

    # Finding #31 — infrastructure counters now use the same public-method
    # pattern as product-signal counters. _run_or_degrade calls these
    # instead of reaching into private attributes directly.

    def increment_watchdog_timeout(self) -> None:
        """M-03 / finding #31: increment the watchdog-timeout operator counter.
        Mirrors the increment_* pattern for product-signal counters.
        REL-03: locked so free-threading Py 3.13+ and PyPy don't tear."""
        with self._reliability_counter_lock:
            self._watchdog_timeouts_total += 1

    def increment_watchdog_queue_overflow(self) -> None:
        """M-03 / finding #31: increment the watchdog queue-overflow counter.
        REL-03: locked."""
        with self._reliability_counter_lock:
            self._watchdog_queue_overflows_total += 1

    def increment_watchdog_late_completion(self) -> None:
        """P1 #5: a watchdog-timed-out future eventually completed
        successfully — any state it mutated (e.g., an EXCLUSIVE grant
        from ``service.write``) is now in the registry without the
        agent's knowledge, since the handler had already returned a
        degraded response. Operator-visible via
        ``/status?detail=metrics`` so a phantom-grant cluster is
        diagnosable from a bug report."""
        self._watchdog_late_completion_total += 1

    def record_401(self) -> None:
        """P1 #6: bump ``auth_401_total`` and (deduped to once per 60s)
        emit a WARNING log explaining the most common cause — operator
        deleted ``hook.secret`` while the coordinator was running, so
        every hook request from every session now 401s and the client
        treats it as a coordinator-unavailable degrade. Without this
        signal an operator sees no symptom except "coherence stopped
        working" with no log line to point at. We deliberately do NOT
        shut down the coordinator on 401 — the secret may be restored,
        or this may be a single bad request rather than a system
        misconfig."""
        self._auth_401_total += 1
        now = time.monotonic()
        with self._auth_401_warn_lock:
            if now - self._last_401_warn_at < 60.0:
                return
            self._last_401_warn_at = now
        logger.warning(
            "auth: 401 on request — bearer mismatch or hook.secret missing. "
            "If this is the first 401 after a healthy period, check that "
            "%s/.coherence/hook.secret exists and matches the client's "
            "bearer. Subsequent 401s within 60s suppressed; total: %d.",
            self.coordinator_root,
            self._auth_401_total,
        )

    def counters_snapshot(self) -> dict[str, Any]:
        """M-03 / finding #31: stable snapshot of ALL coordinator counters
        (per-endpoint + product-signal + infrastructure + watchdog).

        ``_handle_status`` uses this instead of reaching into private attrs,
        giving a single source-of-truth for the counter set.
        """
        return {
            "watchdog_timeouts_total": self._watchdog_timeouts_total,
            "watchdog_queue_overflows_total": self._watchdog_queue_overflows_total,
            "watchdog_late_completion_total": self._watchdog_late_completion_total,
            "handler_concurrency_overflows_total": (
                self._server.handler_concurrency_overflows_total
                if self._server is not None else 0
            ),
            "in_flight_drain_timed_out": self._in_flight_drain_timed_out,
            "cold_start_duration_ms": self.cold_start_duration_ms,
            "endpoint_counters": self.endpoint_counters_snapshot(),
            "intra_task_acquire_release_total": self._intra_task_acquire_release_total,
            "stale_warning_emitted_total": self._stale_warning_emitted_total,
            "stale_warning_reread_total": self._stale_warning_reread_total,
            "auth_401_total": self._auth_401_total,
        }

    def mark_stale_warned(self, agent_id: UUID, artifact_id: UUID) -> None:
        """KTD-J: stamp an (agent, artifact) pair as having received a
        stale warning. The next pre-read on the same pair consumes the
        marker via :meth:`consume_stale_marker` and bumps the re-read
        counter."""
        with self._stale_warned_pairs_lock:
            self._stale_warned_pairs.add((agent_id, artifact_id))

    def consume_stale_marker(self, agent_id: UUID, artifact_id: UUID) -> bool:
        """KTD-J: returns True (and clears the marker) if a stale warning
        had been emitted for this (agent, artifact) pair since the last
        consumption. Returns False otherwise. Used by pre-read entry to
        bump the re-read counter exactly once per warning cycle."""
        with self._stale_warned_pairs_lock:
            pair = (agent_id, artifact_id)
            if pair in self._stale_warned_pairs:
                self._stale_warned_pairs.remove(pair)
                return True
            return False

    def run_with_watchdog(self, fn: Callable[[], Any]) -> Any:
        """Run a callable under the 4s handler-side timeout. Raises
        :class:`FuturesTimeout` on timeout (caller decides degradation).

        P1 #5 (detection-only): when the future times out, ``cancel_futures``
        is not set so the underlying work continues running in the pool.
        Attach a done_callback that fires when that runaway work
        eventually finishes — if it completed successfully, the
        registry now holds state the agent never saw (phantom EXCLUSIVE
        grant being the canonical worry). The callback bumps
        ``watchdog_late_completion_total`` and logs CRITICAL so the
        operator can correlate a phantom-grant cluster in a bug report
        with the rate at which late completions are firing.
        """
        future = self._watchdog.submit(fn)
        try:
            return future.result(timeout=HANDLER_TIMEOUT_SEC)
        except FuturesTimeout:
            future.add_done_callback(self._on_watchdog_future_done_after_timeout)
            raise

    def _on_watchdog_future_done_after_timeout(self, future: Any) -> None:
        """Callback wired by :meth:`run_with_watchdog` when its future
        timed out. Fires later (microseconds to many seconds) when the
        underlying work actually completes. We only count + log when
        the late completion produced state — i.e., the future finished
        without raising. A future that ultimately raised was a no-op
        in the registry; nothing phantom there."""
        if future.cancelled():
            return
        try:
            future.result(timeout=0)
        except Exception:
            # Late failure — no phantom state landed in the registry.
            return
        self.increment_watchdog_late_completion()
        logger.critical(
            "watchdog late completion: a handler future timed out but the "
            "underlying work completed successfully afterwards. Any state "
            "it mutated (e.g., an EXCLUSIVE grant) is in the registry "
            "without the agent's knowledge. Check /status?detail=metrics "
            "for watchdog_late_completion_total and consider running "
            "agent-coherence-status --detail=full to inspect orphaned "
            "M/E grants. Counter total now: %d.",
            self._watchdog_late_completion_total,
        )


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
        # KTD-G item 3: surfaced in /status. REL-03 (free-threading-safe):
        # increment under a lock so concurrent process_request calls
        # don't tear the counter on Py 3.13+ free-threaded builds or
        # PyPy. The lock is on the hot path — but only the over-limit
        # case fires it (cold path), so the overhead is negligible.
        self.handler_concurrency_overflows_total: int = 0
        self._overflow_counter_lock = threading.Lock()

    def process_request(self, request: Any, client_address: Any) -> None:
        """Override ThreadingMixIn.process_request to gate handler spawn
        on the concurrency semaphore. If at limit, send 503 directly
        without spawning a thread."""
        if not self._concurrency_sem.acquire(blocking=False):
            with self._overflow_counter_lock:
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
                    coordinator.record_401()
                    self._json(401, {"error": "missing or invalid bearer token"})
                    return

                # Route. R12 (Unit 6): query string is intentionally
                # separated from the route key so /status?detail=full
                # dispatches to the same handler as /status, with the
                # handler reading the detail parameter for tier gating.
                raw_path = self.path
                if "?" in raw_path:
                    route_path, query = raw_path.split("?", 1)
                else:
                    route_path, query = raw_path, ""
                self._query_string = query  # consumed by status handler
                try:
                    handler = _ROUTES.get((method, route_path))
                    if handler is None:
                        self._json(404, {"error": f"unknown route {method} {route_path}"})
                        return
                    # ADV-001: while the coordinator is draining for migration,
                    # reject NEW write-initiation requests (pre-edit) with a
                    # structured error the agent can see. Existing in-flight
                    # pre-edit→post-edit chains are allowed to complete (the
                    # post-edit endpoint is NOT in this set), and all read +
                    # observability endpoints continue to serve. Without this
                    # gate, a pre-edit landing mid-migration mints an
                    # EXCLUSIVE grant that gets immediately invalidated by
                    # the migration handler, and the agent's matching
                    # post-edit hits a dead coordinator (silent failure).
                    if coordinator.migration_draining and (method, route_path) in _MIGRATION_REJECTED_ROUTES:
                        self._json(503, {
                            "error": (
                                "coordinator is draining for backend migration; "
                                "this write was rejected. Retry after the migration "
                                "completes and the coordinator restarts."
                            ),
                        })
                        return
                    # KTD-J (Unit 8): bump the per-endpoint counter BEFORE
                    # invoking the handler so timeouts/exceptions still
                    # show up in operator-visible counters. Contract:
                    # counts attempted requests, not successful ones.
                    counter_name = _ENDPOINT_COUNTER_NAMES.get((method, route_path))
                    if counter_name is not None:
                        coordinator.increment_endpoint_counter(counter_name)
                    handler(self, coordinator)
                except Exception as exc:
                    logger.exception("unhandled error in handler for %s %s", method, route_path)
                    self._json(500, {"error": f"internal: {type(exc).__name__}"})
            finally:
                coordinator.release_handler_slot()

        # ----------------------------------------------------------
        # Helpers
        # ----------------------------------------------------------

        def _read_json(self) -> dict | None:
            try:
                n = int(self.headers.get("Content-Length", "0"))
            except ValueError:
                self._json(400, {"error": "invalid Content-Length"})
                return None
            # ADV-005 (defensive): reject Content-Length:0 or missing with
            # an explicit 400 rather than silently returning {}. Every POST
            # endpoint expects a body with required fields; a missing body
            # used to fall through to per-field validate_* errors ("missing
            # session_id" etc.) which mask the actual cause. Loud-at-the-
            # right-layer fails fast for hook-client serialization bugs.
            if n <= 0:
                self._json(400, {"error": "missing or empty body (Content-Length:0)"})
                return None
            # R21 (Unit 6): cap the body BEFORE rfile.read so a hostile or
            # buggy client cannot allocate an oversized buffer in the
            # coordinator process. Validates Content-Length only — chunked
            # transfer encoding is not supported by http.server in any
            # case, so a missing/zero Content-Length already short-circuits
            # above.
            if n > MAX_REQUEST_BODY_BYTES:
                self._json(
                    413,
                    {
                        "error": (
                            f"request body exceeds {MAX_REQUEST_BODY_BYTES} bytes "
                            f"(Content-Length={n})"
                        )
                    },
                )
                return None
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


def _handle_pre_read(req: _RequestProtocol, coordinator: CoordinatorHTTPServer) -> None:
    """POST /hooks/pre-read — stale-read check + KTD-9 first-observation seeding.

    Notice-drain contract (COR-03 — fragile but correct; documented here
    so future refactors don't break the two-path discipline):

    A pre-read response can shape one of three ways:

    1. **Fresh (first observation OR already-seen valid grant)** —
       returns ``{"status": "fresh"}`` (no ``hookSpecificOutput``).
       The ``work_with_notice_surfacing`` wrapper at the bottom of
       this handler pops pending notices on this exact shape and
       attaches ``hookSpecificOutput.additionalContext`` if any
       notices were pending. Fresh response → wrapper drains.

    2. **Stale (peer commit invalidated us)** — the stale branch
       inside ``work()`` builds a ``hookSpecificOutput`` envelope
       AND pops + prepends pending notices itself (line ~890). The
       wrapper sees ``hookSpecificOutput`` present and SKIPS its own
       notice drain — single-consumer semantics, no double-pop.

    3. **Fresh with already-drained notices** — if ``work()`` itself
       drained (the stale path's behaviour), the wrapper's
       ``status == 'fresh' and 'hookSpecificOutput' not in result``
       check is False (hookSpecificOutput present) so no re-pop.

    The KTD-J ``consume_stale_marker`` call (re-read counter) fires
    BEFORE the fresh-path short-circuit so a re-read that returns
    fresh still bumps ``stale_warning_reread_total``. Moving the
    marker check after the short-circuit would silently break the
    counter; do not refactor without preserving this ordering.
    """
    body = req._read_json()
    if body is None:
        return
    session_id = body.get("session_id")
    path = body.get("path", "")
    content_hash = body.get("content_hash") or None

    sid_err = validate_session_id(session_id)
    if sid_err:
        req._json(400, {"error": sid_err[1]})
        return
    path_err = validate_path(path)
    if path_err:
        # Mirror the prior "missing or empty path" message shape for empty/missing
        # to keep client-side error-handling stable.
        msg = "missing or empty path" if path_err in ("path is empty", "path must be a string") else path_err
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

        # KTD-J (Unit 8): if a prior pre-read on this exact (agent,
        # artifact) pair emitted a stale warning, count THIS call as the
        # re-read. Increment whether the re-read returns fresh or stale —
        # the agent attempted the read either way.
        if coordinator.consume_stale_marker(agent_id, artifact_id):
            coordinator.increment_stale_warning_reread()

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
        # KTD-J (Unit 8): bump the stale-warning emission counter +
        # mark the pair so a follow-up pre-read counts as a re-read.
        coordinator.increment_stale_warning_emitted()
        coordinator.mark_stale_warned(agent_id, artifact_id)
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
    #
    # COR-08: graceful-degradation note — if work() raises before this
    # wrapper can drain notices, pop_pending_notices never runs and the
    # notice stays in the DB. That's intentional: the notice will surface
    # on the next successful pre-read for the same (agent, artifact) OR
    # on session-stop OR via the F2 sweep eviction at
    # notice_evict_max_age_sec. A transient SQLite error delays — but
    # does not lose — the notice surface.
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


def _handle_pre_edit(req: _RequestProtocol, coordinator: CoordinatorHTTPServer) -> None:
    """POST /hooks/pre-edit — acquire EXCLUSIVE (KTD-1) + KTD-9 collision surfacing."""
    body = req._read_json()
    if body is None:
        return
    session_id = body.get("session_id")
    path = body.get("path", "")
    sid_err = validate_session_id(session_id)
    if sid_err:
        req._json(400, {"error": sid_err[1]})
        return
    path_err = validate_path(path)
    if path_err:
        req._json(400, {"error": "missing or empty path" if path_err in ("path is empty", "path must be a string") else path_err})
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

    # AC-05: pre-edit's wire contract is {ok: bool}, not {status: ...}.
    # On watchdog timeout, return the ok-shape degraded envelope so a
    # client doing result.get("ok") sees True rather than None.
    _run_or_degrade(req, coordinator, work, degraded_response=_OK_DEGRADED_RESPONSE)


def _handle_post_edit(req: _RequestProtocol, coordinator: CoordinatorHTTPServer) -> None:
    """POST /hooks/post-edit — commit on success, release on failure (KTD-1)."""
    body = req._read_json()
    if body is None:
        return
    session_id = body.get("session_id")
    path = body.get("path", "")
    content_hash = body.get("content_hash")  # required when success=true
    success = bool(body.get("success", True))
    sid_err = validate_session_id(session_id)
    if sid_err:
        req._json(400, {"error": sid_err[1]})
        return
    path_err = validate_path(path)
    if path_err:
        req._json(400, {"error": "missing or empty path" if path_err in ("path is empty", "path must be a string") else path_err})
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
                # ADV-004: distinguish sweep reclamation from peer preemption.
                # The sweep uses SWEEP_RECLAMATION_PREEMPTER_ID; matching it
                # means "your heartbeat went stale (or you held the grant past
                # max-hold) and the coordinator pulled the grant back" — a
                # different failure mode than "another session committed".
                if preempter_id == SWEEP_RECLAMATION_PREEMPTER_ID:
                    reason = (
                        f"commit_not_allowed: your M/E grant on {path} was "
                        f"reclaimed by the coordinator sweep (heartbeat "
                        f"timeout or max-hold ceiling) at "
                        f"{_iso_utc(preempted_at)}. Your edit landed in your "
                        f"local worktree but the coordinator's version was "
                        f"not bumped. Re-fetch the latest via pre-read and "
                        f"retry. Underlying coordinator error: {exc}"
                    )
                    return {"ok": False, "reason": reason, "reclaimed": True}
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
        # KTD-J (Unit 8): successful commit means an EXCLUSIVE grant was
        # acquired (pre-edit) and released (post-edit) within this same
        # turn → sizes the hosted-tier upsell case.
        coordinator.increment_intra_task_acquire_release()
        return {"ok": True}

    # AC-05: post-edit's wire contract is {ok: bool}; ok-shape degraded
    # envelope keeps clients reading result.get("ok") safe on timeout.
    _run_or_degrade(req, coordinator, work, degraded_response=_OK_DEGRADED_RESPONSE)


def _handle_session_stop(req: _RequestProtocol, coordinator: CoordinatorHTTPServer) -> None:
    """POST /hooks/session-stop — release uncommitted EXCLUSIVE grants (KTD-11)."""
    body = req._read_json()
    if body is None:
        return
    session_id = body.get("session_id")
    sid_err = validate_session_id(session_id)
    if sid_err:
        req._json(400, {"error": sid_err[1]})
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

    # AC-05: session-stop's wire contract is {ok: bool}; ok-shape degraded
    # envelope keeps clients reading result.get("ok") safe on timeout.
    _run_or_degrade(req, coordinator, work, degraded_response=_OK_DEGRADED_RESPONSE)


def _handle_policy_track(req: _RequestProtocol, coordinator: CoordinatorHTTPServer) -> None:
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
    #
    # COR-05: this is an atomic-swap-via-local-variable pattern. The RHS
    # evaluates fully (TrackedArtifactPolicy.load returns a new object)
    # before the attribute assignment fires. Single PyObject* write is
    # atomic on CPython, and even on free-threading builds the per-object
    # lock makes the swap visible to other threads as a single edge.
    # Handlers reading coordinator.policy bind it to a local at entry
    # (see pre-read / pre-edit / pre-bash / pre-grep) so a mid-handler
    # swap can't change which policy object the handler reasons about.
    new_policy = TrackedArtifactPolicy.load(coordinator.coordinator_root)
    coordinator.policy = new_policy
    req._json(200, {
        "ok": True, "added": added, "rejected": rejected + pre_rejected,
    })


def _handle_policy_untrack(req: _RequestProtocol, coordinator: CoordinatorHTTPServer) -> None:
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
    # AC-06 / finding #27: collect pre_rejected so the response is symmetric
    # with /policy/track's {ok, removed, rejected} shape. Previously, invalid
    # paths were silently dropped with no rejected field in the response.
    safe_paths: list[str] = []
    pre_rejected: list[dict] = []
    for p in paths:
        v_err = validate_path(p)
        if v_err is None:
            safe_paths.append(p)
        else:
            pre_rejected.append({"path": p, "reason": v_err})
    yaml_path = coordinator.coordinator_root / ".coherence" / "ignored.yaml"
    try:
        added, yaml_rejected = _append_policy_yaml(yaml_path, safe_paths)
    except ValueError as exc:
        req._json(400, {"error": str(exc)})
        return
    coordinator.policy = TrackedArtifactPolicy.load(coordinator.coordinator_root)
    req._json(200, {"ok": True, "removed": added, "rejected": yaml_rejected + pre_rejected})


def _handle_pre_bash(req: _RequestProtocol, coordinator: CoordinatorHTTPServer) -> None:
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

    sid_err = validate_session_id(session_id)
    if sid_err:
        req._json(400, {"error": sid_err[1]})
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
            # KTD-J (Unit 8): one increment per stale RESPONSE, regardless
            # of how many paths the response summarizes.
            coordinator.increment_stale_warning_emitted()
        else:
            resp["status"] = "fresh"
        return resp

    _run_or_degrade(req, coordinator, work)


def _handle_pre_grep(req: _RequestProtocol, coordinator: CoordinatorHTTPServer) -> None:
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

    sid_err = validate_session_id(session_id)
    if sid_err:
        req._json(400, {"error": sid_err[1]})
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
            # KTD-J (Unit 8): one increment per stale pre-grep response.
            coordinator.increment_stale_warning_emitted()
        else:
            resp["status"] = "fresh"
        return resp

    _run_or_degrade(req, coordinator, work)


def _handle_status(req: _RequestProtocol, coordinator: CoordinatorHTTPServer) -> None:
    """GET /status — drives the agent-coherence-status console script.

    R12 (Unit 6): three-tier disclosure model.

    | tier      | query                | extra requirement              |
    |-----------|----------------------|--------------------------------|
    | minimal   | (none) or detail=minimal | none                       |
    | metrics   | ?detail=metrics      | none — telemetry block only    |
    | full      | ?detail=full         | ``Coherence-Local-Operator: true`` header opt-in |

    ``minimal`` is the default and includes no absolute paths (workspace
    root is reported as a sentinel ``.``). ``metrics`` returns only the
    counter block — useful for operators scraping /status into a
    dashboard without leaking workspace state. ``full`` is the legacy
    everything-block plus absolute ``coordinator_root`` and ``coordinator_pid``,
    gated by the explicit ``Coherence-Local-Operator: true`` header so a
    same-user adversary (Adversary 1 in auth.py) cannot trivially grab
    the operator's home-directory path. Bearer auth is enforced by the
    dispatcher; the header is a SECOND factor specifically for the
    elevated tier.

    AC-07 — metrics-tier stability contract (operator-facing):

      Fields PRESENT in the metrics tier are stable within a major
      version. ``coordinator_uptime_seconds``, ``coordinator_backend``,
      ``coordinator_version``, ``watchdog_timeouts_total``,
      ``watchdog_queue_overflows_total``,
      ``handler_concurrency_overflows_total``,
      ``in_flight_drain_timed_out``, ``cold_start_duration_ms``,
      ``endpoint_counters``, ``intra_task_acquire_release_total``,
      ``stale_warning_emitted_total``, ``stale_warning_reread_total``.

      Fields may be ADDED in minor versions (additive change is
      non-breaking for dashboards using selective key access).

      Fields are REMOVED only in major versions and only after at
      least one minor-version release where the field is emitted
      ALONGSIDE its replacement as a deprecated alias (see AC-02
      for the ``coordinator_uptime_s`` → ``coordinator_uptime_seconds``
      precedent — alias ships through v0.1.x; remove at v0.2.0).

      Fields EXPLICITLY OMITTED from metrics tier vs. full tier:
      ``tracked_artifacts``, ``sessions``, ``policy_summary``,
      ``coordinator_root``, ``coordinator_pid``. Operators wanting
      these for a dashboard must call ``?detail=full`` with the
      ``Coherence-Local-Operator: true`` header.
    """
    detail = _parse_detail_query(getattr(req, "_query_string", ""))
    if detail == "full":
        if req.headers.get("Coherence-Local-Operator", "").lower() != "true":
            req._json(403, {
                "error": (
                    "detail=full requires the Coherence-Local-Operator: true "
                    "opt-in header in addition to the Bearer secret (R12)."
                ),
            })
            return

    # Counter block — present at every tier so the telemetry-only
    # consumer (?detail=metrics) doesn't pay for the artifact/session
    # walk. KTD-J (Unit 8) adds per-endpoint + product-signal counters
    # alongside the existing watchdog/concurrency counters.
    # M-03 / finding #31: use counters_snapshot() instead of reaching into
    # private attrs directly — single source of truth for the counter set.
    # AC-02 cross-backend parity: KTD-J naming convention locks the
    # full-word ``_seconds`` suffix for duration fields. Node emits
    # ``coordinator_uptime_seconds``; Python now matches. The old
    # ``coordinator_uptime_s`` field is emitted ALONGSIDE the new one
    # for one release as a backward-compat alias (consumers can detect
    # which field to read by checking which is present, or just read
    # the canonical name). Deprecation note in docs/metrics.md (TODO).
    _uptime = coordinator.uptime_s
    counters = {
        "coordinator_uptime_seconds": _uptime,
        "coordinator_uptime_s": _uptime,  # AC-02: deprecated alias, removed in v0.2
        "coordinator_backend": "python",
        "coordinator_version": _COORDINATOR_VERSION,
        **coordinator.counters_snapshot(),
    }
    if detail == "metrics":
        req._json(200, {"detail": "metrics", **counters})
        return

    # PERF-1: single batched snapshot — replaces 2N SELECTs (one
    # get_artifact + one get_state_map per artifact) with 2 SELECTs total
    # held under one registry lock so the view is consistent.
    artifact_by_id, state_by_artifact = coordinator.registry.status_snapshot()
    agent_names_snapshot = coordinator.agent_names_snapshot()

    tracked: list[dict] = [
        {"path": meta["name"], "version": meta["version"], "id": str(artifact_id)}
        for artifact_id, meta in artifact_by_id.items()
    ]
    sessions: list[dict] = []
    for agent_id, name in agent_names_snapshot:
        per_artifact: dict[str, str] = {}
        for artifact_id, meta in artifact_by_id.items():
            state = state_by_artifact[artifact_id].get(agent_id)
            if state is not None and state != MESIState.INVALID:
                per_artifact[meta["name"]] = state.name
        sessions.append({
            "agent_name": name,
            "agent_id": str(agent_id),
            "states": per_artifact,
        })

    base = {
        "detail": detail,
        "tracked_artifacts": tracked,
        "sessions": sessions,
        "policy_summary": coordinator.policy.summary(),
        # P1 #7: coordinator_pid is in the minimal tier too. Process IDs
        # are public on POSIX (anyone with `ps` sees them) so this is
        # not a disclosure beyond the trust boundary the threat model
        # already accepts. Operators use this field to verify "is the
        # coordinator I think is running actually mine" — restoring it
        # closes the regression Unit 6 R12 introduced when it moved pid
        # behind the operator-header gate. The contract is also
        # documented in CLAUDE.md and used by status-rendering CLIs.
        "coordinator_pid": os.getpid(),
        **counters,
    }
    if detail == "full":
        # Full tier still adds the absolute workspace root — that DOES
        # leak $HOME / directory layout and stays gated behind the
        # Coherence-Local-Operator: true header.
        base["coordinator_root"] = str(coordinator.coordinator_root)
    else:
        # Minimal: replace absolute workspace path with sentinel "." so the
        # default tier never leaks $HOME or directory layout.
        base["coordinator_root"] = "."
    req._json(200, base)


def _parse_detail_query(query: str) -> str:
    """R12 (Unit 6): map a raw ``?detail=...`` query string to one of
    ``{minimal, metrics, full}``. Unknown values fall back to ``minimal``
    so a typo never exposes more than the default tier."""
    if not query:
        return "minimal"
    for part in query.split("&"):
        if "=" not in part:
            continue
        key, _, value = part.partition("=")
        if key.strip() == "detail":
            v = value.strip().lower()
            if v in ("minimal", "metrics", "full"):
                return v
            return "minimal"
    return "minimal"


MIGRATION_DRAIN_TIMEOUT_SEC = 5.0
"""ADV-001: how long the migration handler waits for in-flight non-pre-edit
handlers (post-edit, pre-read, etc.) to complete before invalidating
remaining grants + scheduling shutdown. Same magnitude as
``IN_FLIGHT_DRAIN_TIMEOUT_SEC`` since the drain semantics are the same;
keeping them as separate constants documents intent."""


def _handle_prepare_for_migration(req: _RequestProtocol, coordinator: CoordinatorHTTPServer) -> None:
    """POST /admin/prepare-for-migration — drain → release-all-grants → shutdown.

    Unit 8 (Decision 1, locked 2026-05-18): operator runs
    ``agent-coherence-coordinator --prepare-for-migration`` before
    switching the Python/Node backend.

    ADV-001 (fix): the prior implementation invalidated grants
    synchronously then scheduled shutdown 100ms later. That race let a
    pre-edit landing at T=50ms mint an EXCLUSIVE grant that the
    invalidation step at T=51ms revoked — and the agent's matching
    post-edit at T=150ms hit a dead coordinator (silent failure).

    New sequence:

    1. Flip ``coordinator._migration_draining = True``. Dispatcher
       starts rejecting NEW pre-edit requests with HTTP 503 + a
       structured "migration in progress" error visible to the model.
       Other endpoints (post-edit, pre-read, session-stop, /status,
       policy mutations) continue to serve so in-flight chains can
       finish naturally.
    2. Background thread waits up to ``MIGRATION_DRAIN_TIMEOUT_SEC``
       for the in-flight handler counter to reach zero. In-flight
       pre-edit→post-edit pairs complete normally during this window.
    3. After drain, snapshot every (agent, artifact) pair still in
       MODIFIED or EXCLUSIVE state — these are orphaned grants from
       sessions that pre-edited but never post-edited (already broken).
       Invalidate each so the new backend doesn't inherit them.
    4. Schedule ``coordinator.shutdown()`` ~100ms later (kernel send
       buffer flush window for the response).

    Returns immediately with ``{ok:true, draining:true,
    drain_timeout_ms}``. The CLI polls /status until the coordinator
    becomes TCP-unreachable; counts/errors land in the coordinator log
    rather than the HTTP response (the response goes out before the
    drain completes).

    Requires the same elevated-tier signal as /status?detail=full:
    Bearer + ``Coherence-Local-Operator: true`` header.

    Security note (SEC-01): the ``Coherence-Local-Operator: true``
    header value is a static, well-known string embedded in public
    source — it does NOT constitute a second factor against Adversary
    1 (same OS user who can read the 0600 hook.secret file). This
    endpoint is a DoS surface within the Adversary 1 boundary: a
    same-UID process with hook.secret access can force coordinator
    shutdown. Accepted per the v0.1 threat model. The header serves as
    an explicit opt-in signal for operator-automation tooling, not as
    a security gate.
    """
    if req.headers.get("Coherence-Local-Operator", "").lower() != "true":
        req._json(403, {
            "error": (
                "prepare-for-migration requires the Coherence-Local-Operator: "
                "true opt-in header (operator-only endpoint)."
            ),
        })
        return

    # Idempotent: a second call while already draining returns the same
    # accepted-but-already-running envelope.
    if coordinator.migration_draining:
        req._json(200, {
            "ok": True,
            "draining": True,
            "already_in_progress": True,
        })
        return

    coordinator._migration_draining = True
    SHUTDOWN_DELAY_MS = 100

    def _drain_invalidate_and_shutdown() -> None:
        """Background sequence: drain in-flight handlers (during which
        pre-edit→post-edit pairs complete naturally), invalidate any
        remaining M/E grants (orphans from sessions that pre-edited
        but never post-edited), schedule shutdown.
        """
        # Step 1: wait for in-flight handlers to drain. The current
        # handler holds one slot itself, so account for that by
        # comparing against 1 rather than 0. After this handler
        # returns, the counter drops to its true in-flight value
        # which the second pass observes.
        deadline = time.monotonic() + MIGRATION_DRAIN_TIMEOUT_SEC
        while time.monotonic() < deadline:
            with coordinator._in_flight_lock:
                # Strictly less than 2 means: just this handler still
                # in flight (1), or fewer (handler already returned).
                if coordinator._in_flight <= 1:
                    break
            time.sleep(0.020)

        # Step 2: invalidate any remaining M/E grants (orphans).
        now = monotonic_seconds()
        released = 0
        errors: list[dict[str, str]] = []
        for artifact_id in list(coordinator.registry.artifact_ids()):
            state_map = coordinator.registry.get_state_map(artifact_id)
            for agent_id, state in list(state_map.items()):
                if state not in (MESIState.MODIFIED, MESIState.EXCLUSIVE):
                    continue
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
                    released += 1
                except CoherenceError as exc:
                    errors.append({
                        "artifact_id": str(artifact_id),
                        "agent_id": str(agent_id),
                        "reason": str(exc),
                    })
        logger.info(
            "prepare-for-migration drained: released=%d errors=%d",
            released, len(errors),
        )
        if errors:
            for e in errors:
                logger.warning("prepare-for-migration invalidate error: %s", e)

        # Step 3: schedule shutdown ~100ms later so any further status
        # polls from the CLI see the draining state once before TCP
        # becomes unreachable.
        time.sleep(SHUTDOWN_DELAY_MS / 1000.0)
        try:
            coordinator.shutdown()
        except Exception:  # pragma: no cover — best-effort cleanup
            logger.exception("scheduled shutdown after prepare-for-migration failed")

    threading.Thread(
        target=_drain_invalidate_and_shutdown,
        name="prepare-for-migration-drain",
        daemon=True,
    ).start()

    req._json(200, {
        "ok": True,
        "draining": True,
        "drain_timeout_ms": int(MIGRATION_DRAIN_TIMEOUT_SEC * 1000),
        "shutdown_scheduled_in_ms": int(MIGRATION_DRAIN_TIMEOUT_SEC * 1000) + SHUTDOWN_DELAY_MS,
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
    ("POST", "/admin/prepare-for-migration"): _handle_prepare_for_migration,
}


# ADV-001: routes the dispatcher rejects with 503 while
# ``coordinator.migration_draining`` is True. Only NEW write-initiation
# requests belong here — post-edit must still serve (so in-flight chains
# can complete), policy mutations must still serve (operator may be
# clearing tracked artifacts as part of migration prep), and all reads
# + observability always serve. pre-bash / pre-grep don't initiate
# writes, so they stay out of this set.
_MIGRATION_REJECTED_ROUTES: set[tuple[str, str]] = {
    ("POST", "/hooks/pre-edit"),
}


# KTD-J (Unit 8): route → counter-name lookup. Used by ``_dispatch`` to
# bump per-endpoint counters BEFORE invoking the handler (contract:
# counts attempted requests, not successful ones, so a timeout or
# exception still shows up in operator-visible /status output).
_ENDPOINT_COUNTER_NAMES: dict[tuple[str, str], str] = {
    ("POST", "/hooks/pre-read"): "pre_read_total",
    ("POST", "/hooks/pre-edit"): "pre_edit_total",
    ("POST", "/hooks/post-edit"): "post_edit_total",
    ("POST", "/hooks/session-stop"): "session_stop_total",
    ("POST", "/hooks/pre-bash"): "pre_bash_total",
    ("POST", "/hooks/pre-grep"): "pre_grep_total",
    ("POST", "/policy/track"): "policy_track_total",
    ("POST", "/policy/untrack"): "policy_untrack_total",
    ("GET", "/status"): "status_total",
    # /admin/prepare-for-migration intentionally not counted — it
    # initiates shutdown, so counting it would never be observable via
    # subsequent /status calls (coordinator is already down).
}


# ----------------------------------------------------------------------
# Internals
# ----------------------------------------------------------------------


_DEFAULT_DEGRADED_RESPONSE: dict = {"status": "fresh", "degraded": True}
"""AC-05: pre-read / pre-bash / pre-grep degrade to a fresh-shape envelope
because their wire contract uses ``{status: "fresh"|"stale"}``. Endpoints
whose contract is ``{ok: bool}`` (pre-edit, post-edit, session-stop) pass
``OK_DEGRADED_RESPONSE`` instead so the client doesn't see ``None`` from
``result.get("ok")``."""

_OK_DEGRADED_RESPONSE: dict = {"ok": True, "degraded": True}
"""AC-05: degraded envelope for {ok: bool}-shape endpoints (pre-edit,
post-edit, session-stop). Pairs with ``_DEFAULT_DEGRADED_RESPONSE``."""


def _run_or_degrade(
    req: _RequestProtocol,
    coordinator: CoordinatorHTTPServer,
    work: Callable[[], dict],
    *,
    degraded_response: dict | None = None,
) -> None:
    """Run ``work`` under the handler-side watchdog. On timeout, log WARNING
    and return 200 with ``degraded_response`` (or the default fresh-shape
    envelope) so the user's tool call proceeds.

    AC-05: callers from ``{ok: bool}``-shape endpoints (pre-edit,
    post-edit, session-stop) pass ``degraded_response=_OK_DEGRADED_RESPONSE``
    so clients reading ``result.get("ok")`` see ``True`` rather than
    ``None``. Callers from ``{status: ...}``-shape endpoints (pre-read,
    pre-bash, pre-grep) accept the default fresh-shape envelope.

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
        coordinator.increment_watchdog_queue_overflow()  # finding #31
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
        coordinator.increment_watchdog_timeout()  # finding #31
        logger.warning("handler watchdog timeout after %ss; degrading", HANDLER_TIMEOUT_SEC)
        req._json(200, degraded_response if degraded_response is not None else _DEFAULT_DEGRADED_RESPONSE)
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
) -> tuple[UUID | None, int | None]:
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
        # ADV-004: render sweep-reclaimed notices with a clear cause rather
        # than the awkward truncated sentinel UUID.
        if preempter_id == SWEEP_RECLAMATION_PREEMPTER_ID:
            lines.append(
                f"  • {path} — reclaimed by the coordinator sweep "
                f"(heartbeat timeout or max-hold ceiling) at {_iso_utc(ts)}. "
                f"Any local edit you made to this file will land in your "
                f"worktree but is NOT reflected in the coordinator's version. "
                f"Re-fetch via pre-read and retry."
            )
            continue
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


def _last_writer_for(coordinator: CoordinatorHTTPServer, artifact_id: UUID) -> str | None:
    """Return the session_id (not agent UUID) of the artifact's last writer, if any.

    Best signal: an agent currently in MODIFIED state (they are the current writer).

    Fallback caveat (COR-09): if no agent holds MODIFIED, we fall back to the
    first entry in the state_map by insertion order. This agent may be in SHARED
    or EXCLUSIVE state — it is not necessarily the actual last writer. In the
    common case the querying agent itself may be in state_map (e.g. in SHARED),
    meaning the stale-read warning could attribute the file to the very session
    receiving the warning. This is a known limitation of the in-memory fallback;
    a future improvement should use ``artifacts.last_writer_id`` from the DB
    (via ``_fetch_artifact_row``) instead of the state_map fallback.
    """
    # The registry's _fetch_artifact_row returns last_writer_id; we expose it
    # via lookup. For now derive from agent_names cache.
    state_map = coordinator.registry.get_state_map(artifact_id)
    # Best signal: an agent currently in MODIFIED state.
    for agent_id, state in state_map.items():
        if state == MESIState.MODIFIED:
            return _agent_id_to_session(coordinator, agent_id)
    # Fallback: first known agent by insertion order (see docstring caveat).
    if state_map:
        first_agent = next(iter(state_map))
        return _agent_id_to_session(coordinator, first_agent)
    return None


def _last_writer_unix_ts(
    coordinator: CoordinatorHTTPServer, artifact_id: UUID
) -> float | None:
    """Return the REAL wall-clock time the artifact was last written, from
    `artifacts.updated_at` in the registry. None if the artifact is unknown.

    P2 ce-review fix #16: uses the public ``get_artifact_updated_at()``
    accessor on SqliteArtifactRegistry instead of reaching into ``_conn``
    + ``_lock`` directly. Layer violation closed."""
    return coordinator.registry.get_artifact_updated_at(artifact_id)


def _agent_id_to_session(coordinator: CoordinatorHTTPServer, agent_id: UUID) -> str | None:
    """Reverse the session_to_agent_id mapping via agent_names. R10 (Unit 6):
    routes through the lock-aware public accessor instead of reaching into
    the private dict directly."""
    name = coordinator.agent_name_for(agent_id)
    if name and name.startswith("claude-session-"):
        return name[len("claude-session-"):]
    return None


def _append_policy_yaml(yaml_path: Path, new_paths: list[str]) -> tuple[list[str], list[dict]]:
    """Append valid patterns to a YAML file. Returns (added, rejected).
    Honors MAX_POLICY_YAML_BYTES — raises ValueError if the resulting file
    would exceed the cap.

    R14 (Unit 6): the read-modify-write is wrapped in an ``fcntl.flock``
    exclusive lock on a sidecar ``<yaml_path>.lock`` file so two concurrent
    /policy/track or /policy/untrack requests cannot interleave their
    reads-and-writes and corrupt the YAML (e.g., both read the same
    pre-state, both compute "previous + my_paths", second write loses the
    first writer's additions). fcntl is POSIX-only; on the deferred
    Windows path this is a no-op (lifecycle already disables the
    coordinator on Windows per _FCNTL_AVAILABLE).

    COR-06: callers pre-validate via ``validate_path`` (or equivalent)
    and pass only safe paths. The local re-check below is a defensive
    second pass that catches accidentally-bypassed validation BUT in
    the normal flow the ``rejected`` list it returns from this branch
    is always empty (everything passes the caller's check already).
    Kept as defense-in-depth — removing it would couple this helper to
    the caller's validation discipline, which is a tighter contract
    than the function's current "self-contained validate-and-write"
    behaviour. Tests should assert callers reject before reaching here.
    """
    yaml_path.parent.mkdir(parents=True, exist_ok=True)
    # Validate each path the same way TrackedArtifactPolicy does. This
    # validation is pure (no I/O) so it lives outside the lock window.
    # Defense-in-depth per COR-06: callers pre-validate, but this loop
    # ensures the YAML write is never reached with traversal patterns.
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

    lock_path = yaml_path.with_suffix(yaml_path.suffix + ".lock")
    try:
        import fcntl as _fcntl
        lock_fd: int | None = os.open(str(lock_path), os.O_CREAT | os.O_RDWR, 0o600)
        try:
            _fcntl.flock(lock_fd, _fcntl.LOCK_EX)
            existing = yaml_path.read_text() if yaml_path.is_file() else ""
            new_lines = "\n".join(f"- {p}" for p in added)
            new_content = (
                (existing.rstrip("\n") + "\n" + new_lines + "\n") if existing
                else (new_lines + "\n")
            )
            if len(new_content.encode("utf-8")) > MAX_POLICY_YAML_BYTES:
                raise ValueError(
                    f"policy YAML cap of {MAX_POLICY_YAML_BYTES} bytes would be exceeded"
                )
            yaml_path.write_text(new_content)
            return added, rejected
        finally:
            try:
                _fcntl.flock(lock_fd, _fcntl.LOCK_UN)
            except OSError:
                pass
            try:
                os.close(lock_fd)
            except OSError:
                pass
    except ImportError:
        # Windows fallback: no fcntl. Lifecycle already disables the
        # coordinator on Windows, but degrade defensively if reached.
        existing = yaml_path.read_text() if yaml_path.is_file() else ""
        new_lines = "\n".join(f"- {p}" for p in added)
        new_content = (
            (existing.rstrip("\n") + "\n" + new_lines + "\n") if existing
            else (new_lines + "\n")
        )
        if len(new_content.encode("utf-8")) > MAX_POLICY_YAML_BYTES:
            raise ValueError(
                f"policy YAML cap of {MAX_POLICY_YAML_BYTES} bytes would be exceeded"
            )
        yaml_path.write_text(new_content)
        return added, rejected
