# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""Race-safe lazy spawn, idle shutdown, and background sweep for the
Claude Code coordinator HTTP server (Unit 5 per the v0.1 plan).

The plugin's hook scripts call :func:`connect_or_spawn` on every hook
event. The first call from any session in a workspace lazily spawns the
coordinator process; subsequent calls (from the same or other sessions)
read the existing port file and skip spawn.

Key correctness properties:
- **Single binder per workspace.** `fcntl.flock(server.pid, LOCK_EX|LOCK_NB)`
  ensures exactly one process at a time owns the coordinator. POSIX-only;
  Windows fallback (KTD-5) is deferred to v0.1.1.
- **No port-file TOCTOU.** The holder binds the ``ThreadingHTTPServer``
  FIRST (port=0 lets the OS pick), reads ``server.server_port``, writes
  ``<pid>\\n<port>\\n`` to ``server.pid``, fsyncs, THEN starts the serving
  loop and the sweep thread. Losers' bounded retry reads the port once
  it appears.
- **Race-safe idle shutdown.** Shutdown rewrites the port file to drop
  the port line BEFORE releasing the flock, so a concurrent
  :func:`ensure_coordinator` that acquires the flock right after release
  sees a port-less file and re-spawns cleanly instead of returning a
  dead port.
"""

from __future__ import annotations

import errno
import logging
import os
import socket
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from ccs.adapters.claude_code.coordinator_server import CoordinatorHTTPServer

logger = logging.getLogger(__name__)


# G8 fix (subagent finding #8): fcntl is POSIX-only. On Windows, import-time
# failure would crash hook handlers with a stack trace on every hook event.
# Guard the import and provide a stub that degrades gracefully — hook handlers
# already treat -1 as "no coordinator available" and no-op.
try:
    import fcntl  # type: ignore[import-not-found]
    _FCNTL_AVAILABLE = True
except ImportError:  # pragma: no cover — exercised only on Windows
    fcntl = None  # type: ignore[assignment]
    _FCNTL_AVAILABLE = False
    logger.warning(
        "fcntl not available (platform=%s); agent-coherence coordinator is disabled. "
        "Use WSL2 on Windows. Native Windows support tracked in v0.1.1.",
        sys.platform,
    )


# ----------------------------------------------------------------------
# Configuration
# ----------------------------------------------------------------------


@dataclass(frozen=True)
class LifecycleConfig:
    """Tunables for the spawn / sweep / shutdown loops.

    Defaults are chosen for a single-developer interactive session on
    macOS; the 10-process race test in :mod:`test_claude_code_lifecycle`
    measures real cold-start p99 and confirms the port-file retry budget
    covers it.
    """

    #: Wall-clock seconds of inactivity before the coordinator self-stops.
    #: 0 disables idle shutdown (tests, long-running benchmarks).
    idle_shutdown_sec: float = 900.0

    #: How often the idle-shutdown watcher and grant-timeout sweep tick.
    #: 0 disables the sweep entirely.
    sweep_interval_sec: float = 5.0

    #: F2 hardening — orphan preemption notices older than this are
    #: evicted by the sweep. 30 min default: long enough for a model to
    #: pause and come back, short enough to bound state on a dead session.
    notice_evict_max_age_sec: float = 1800.0

    #: Loser's bounded retry when reading the port file. Bumped from 30 to
    #: 60 (G9) so a 30-process thundering herd with cold Python imports
    #: and SQLite WAL setup has time to settle before losers degrade.
    #: Full mitigation requires measuring real-world p99 cold-start —
    #: deferred to v0.1.1 (docs/known-issues).
    port_file_retry_attempts: int = 60
    port_file_retry_interval_sec: float = 0.050  # × 60 = 3000ms budget

    #: Hook handler's TCP connect retry before falling back to spawn.
    connect_retry_attempts: int = 3
    connect_retry_interval_sec: float = 0.100

    #: G2 fix — the spawn-side self-probe budget is independent and much
    #: larger because the spawning process knows it just bound the socket
    #: and can afford to wait for serve_forever to reach accept(). Cold
    #: start can take well over 300ms on a slow disk.
    spawn_self_probe_attempts: int = 50  # × 100ms = 5000ms budget
    spawn_self_probe_interval_sec: float = 0.100

    #: Grant-timeout sweep thresholds. With v0.1's CrashRecoveryConfig
    #: shipping disabled-by-default, these effectively define the sweep's
    #: safety net for genuinely orphaned grants. Generous so a thinking
    #: session is never reclaimed under interactive load.
    grant_heartbeat_timeout_sec: int = 600
    grant_max_hold_sec: int = 1800

    #: Transient-state timeout (fail-safe for unfinished M↔E protocol steps).
    transient_timeout_sec: int = 60


_DEFAULT_CONFIG = LifecycleConfig()


# ----------------------------------------------------------------------
# Public API: spawn / connect / shutdown
# ----------------------------------------------------------------------


def ensure_coordinator(
    coordinator_root: Path,
    *,
    config: Optional[LifecycleConfig] = None,
    bind_host: str = "127.0.0.1",
) -> int:
    """Lazy-spawn entry point.

    Acquires the fcntl exclusive lock on ``<root>/.coherence/server.pid``.
    If acquired, binds the HTTP server, writes the port file, starts
    serving in a daemon thread, and starts the sweep + idle-shutdown
    threads. If not acquired, reads the existing port file (with bounded
    retry for the brief window where the holder hasn't written it yet).

    Returns the port the coordinator is bound to. Returns ``-1`` if the
    parent repo is read-only and the coordinator cannot be spawned — the
    caller (hook handler) treats this as "no coordinator available" and
    degrades gracefully.
    """
    if not _FCNTL_AVAILABLE:
        return -1

    cfg = config or _DEFAULT_CONFIG
    coherence_dir = _ensure_coherence_dir(coordinator_root)
    if coherence_dir is None:
        return -1

    # G3 entry short-circuit: if this process already spawned a coordinator
    # for this workspace and it's still healthy, return its port directly.
    # Prevents fd / sweep-thread leaks from accidental re-entrant calls.
    resolved_key = str(coordinator_root.resolve())
    existing = _SPAWNED_REGISTRY.get(resolved_key)
    if existing is not None and not existing.shutdown_done.is_set():
        existing_port = existing.coordinator.port
        if _tcp_probe(existing_port, cfg, bind_host=bind_host):
            return existing_port
        # Existing entry not actually reachable — fall through to respawn.
        logger.warning(
            "ensure_coordinator: existing entry for %s port=%d not reachable; respawning",
            resolved_key, existing_port,
        )

    pid_file = coherence_dir / "server.pid"
    fd = _open_pidfile(pid_file)
    if fd is None:
        return -1

    # Unified spawn-or-join loop (G1 fix per Unit 5 §5.654 — the
    # idle-shutdown-vs-spawn race). On each attempt:
    #   1. Try to acquire the flock (non-blocking). If acquired, we're
    #      the winner — bind, write port, serve, return.
    #   2. If contended, try to read a valid port from the file. If
    #      present, we're a clean loser — return the holder's port.
    #   3. Otherwise the holder is either mid-bind (will write the port
    #      shortly) OR mid-shutdown (will release the lock shortly).
    #      Sleep one interval and retry both checks.
    #
    # This handles both the cold-start thundering herd (holder is
    # mid-bind; port appears) and the idle-shutdown race (holder is
    # mid-shutdown; flock releases) without baking in an order.
    for attempt in range(cfg.port_file_retry_attempts):
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            if exc.errno not in (errno.EWOULDBLOCK, errno.EACCES, errno.EAGAIN):
                os.close(fd)
                raise
            # Contended — try to read the port.
            port = _read_port_from_file(pid_file)
            if port is not None:
                os.close(fd)
                return port
            # Port file empty (holder mid-bind or mid-shutdown). Wait
            # and try the whole loop again — on the next iteration we
            # may either see the port populated OR acquire the released
            # lock ourselves.
            time.sleep(cfg.port_file_retry_interval_sec)
            continue

        # Winner — we hold the lock. Bind, write port, serve.
        try:
            coordinator = CoordinatorHTTPServer(coordinator_root, port=0, bind_host=bind_host)
            port = coordinator.port
            _write_pidfile(fd, os.getpid(), port)
            coordinator.serve_in_thread()
            entry = _SpawnedEntry(
                coordinator=coordinator,
                lock_fd=fd,
                coherence_dir=coherence_dir,
                shutdown_lock=threading.Lock(),
                shutdown_done=threading.Event(),
            )
            _SPAWNED_REGISTRY[resolved_key] = entry
            _start_background_threads(entry, cfg)
            # G2 fix: self-probe with generous spawn-side budget before
            # returning so the caller is guaranteed a coordinator that
            # is actually accepting. Cold-start (Python interpreter +
            # SQLite WAL rehydration) can take well past the loser-side
            # connect_retry budget.
            if not _self_probe(port, cfg, bind_host=bind_host):
                logger.warning(
                    "coordinator bound port=%d but self-probe exhausted after %dms; returning anyway",
                    port,
                    int(cfg.spawn_self_probe_attempts * cfg.spawn_self_probe_interval_sec * 1000),
                )
            logger.info(
                "coordinator spawned: pid=%d port=%d root=%s",
                os.getpid(), port, coordinator_root,
            )
            return port
        except Exception:
            try:
                fcntl.flock(fd, fcntl.LOCK_UN)
            finally:
                os.close(fd)
            raise

    # Retry budget exhausted without acquiring the lock or seeing a
    # populated port. Operationally this means the holder is wedged
    # between flock-hold and port-write for the full retry budget —
    # which would itself be a bug in this module — OR the holder is
    # mid-shutdown for longer than the retry budget. Either way, return
    # -1 so the caller degrades gracefully.
    os.close(fd)
    logger.warning(
        "ensure_coordinator: %d attempts exhausted without acquiring lock or reading port",
        cfg.port_file_retry_attempts,
    )
    return -1


def connect_or_spawn(
    coordinator_root: Path,
    *,
    config: Optional[LifecycleConfig] = None,
    bind_host: str = "127.0.0.1",
) -> int:
    """Hook-handler entry point.

    Read the port file → TCP-probe the port → on connect failure, call
    :func:`ensure_coordinator` once and retry the probe.
    """
    if not _FCNTL_AVAILABLE:
        return -1

    cfg = config or _DEFAULT_CONFIG
    pid_file = coordinator_root / ".coherence" / "server.pid"
    port = _read_port_from_file(pid_file)
    if port is not None and _tcp_probe(port, cfg, bind_host=bind_host):
        return port

    # Stale or absent — spawn (or join existing holder via fcntl race).
    port = ensure_coordinator(coordinator_root, config=cfg, bind_host=bind_host)
    if port == -1:
        return -1
    # ensure_coordinator already self-probes on the spawn path; here we
    # only re-probe if the caller landed in the loser-read path (where
    # the just-read port may belong to a coordinator mid-shutdown).
    if not _tcp_probe(port, cfg, bind_host=bind_host):
        logger.warning("coordinator spawned at port=%d but TCP probe failed", port)
        return -1
    return port


def stop_coordinator(coordinator_root: Path) -> bool:
    """Race-safe in-process shutdown.

    Returns True if a coordinator was running in *this* process and was
    cleanly stopped (the caller's invocation actually executed the
    sequence). Returns False if no such coordinator exists locally OR
    if idle-shutdown already completed it (caller's intent is fulfilled
    either way; the return distinguishes who did the work).
    """
    key = str(Path(coordinator_root).resolve())
    entry = _SPAWNED_REGISTRY.get(key)
    if entry is None:
        return False
    ran = _shutdown_sequence(entry)
    # Only pop the registry if the shutdown actually completed successfully.
    # If shutdown_sequence aborted (e.g. coordinator.shutdown raised) we leave
    # the entry in place so a subsequent stop_coordinator call can retry.
    if entry.shutdown_done.is_set():
        _SPAWNED_REGISTRY.pop(key, None)
    return ran


# ----------------------------------------------------------------------
# Internals — pid file, port file, sockets
# ----------------------------------------------------------------------


@dataclass
class _SpawnedEntry:
    """Per-spawn state kept by the spawn-side process.

    The shutdown_lock + shutdown_done pair (G6 fix per subagent finding #6)
    mutexes the shutdown sequence so concurrent triggers (stop_coordinator +
    _idle_shutdown_loop) cannot interleave pid-file writes or double-close
    the lock_fd. The first caller acquires the lock, runs the sequence,
    sets shutdown_done; subsequent callers acquire the lock, see done=True,
    return immediately.
    """

    coordinator: CoordinatorHTTPServer
    lock_fd: int
    coherence_dir: Path
    shutdown_lock: threading.Lock
    shutdown_done: threading.Event


#: Maps coordinator_root → _SpawnedEntry. Only the spawn-side ever populates
#: this; loser-side and other-process paths don't have a Coordinator instance
#: to manage.
_SPAWNED_REGISTRY: dict[str, _SpawnedEntry] = {}


def _ensure_coherence_dir(coordinator_root: Path) -> Optional[Path]:
    """Create ``<root>/.coherence/`` with mode 0700 if missing. Returns
    the dir path, or None if the parent repo is read-only.

    Also writes ``.coherence/.gitignore`` containing ``*`` per KTD-13 so
    a careless ``git add .`` doesn't accidentally commit the SQLite
    state.db (containing MESI state + agent UUIDs), the hook.secret
    (a credential), or the server.pid file. The README claims these are
    auto-gitignored — this is the implementation.
    """
    coherence_dir = coordinator_root / ".coherence"
    try:
        coherence_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    except OSError as exc:
        logger.warning(
            "cannot create .coherence directory under %s: %s — coordinator disabled",
            coordinator_root, exc,
        )
        return None
    # Write .gitignore (idempotent — only write if missing to avoid
    # clobbering any operator customization)
    gitignore = coherence_dir / ".gitignore"
    if not gitignore.exists():
        try:
            gitignore.write_text("*\n")
        except OSError as exc:
            logger.warning(
                "could not write %s: %s — workspace data risks being committed",
                gitignore, exc,
            )
    return coherence_dir


def _open_pidfile(pid_file: Path) -> Optional[int]:
    """Open (creating if needed) the pid file with mode 0600 for fcntl use."""
    try:
        fd = os.open(pid_file, os.O_RDWR | os.O_CREAT, 0o600)
    except OSError as exc:
        logger.warning("cannot open pid file %s: %s", pid_file, exc)
        return None
    return fd


def _write_pidfile(fd: int, pid: int, port: int) -> None:
    """Replace the pid file's contents with ``<pid>\\n<port>\\n`` and fsync.
    The fd must hold the exclusive flock."""
    os.lseek(fd, 0, os.SEEK_SET)
    os.ftruncate(fd, 0)
    payload = f"{pid}\n{port}\n".encode("utf-8")
    written = 0
    while written < len(payload):
        n = os.write(fd, payload[written:])
        if n == 0:  # defensive — write should always make progress
            break
        written += n
    os.fsync(fd)


def _rewrite_pidfile_drop_port(fd: int, pid: int) -> None:
    """Idle-shutdown step: rewrite the pid file with just ``<pid>\\n``.
    Callers must hold the flock. A subsequent ensure_coordinator call
    that acquires the lock will see an empty port and re-spawn."""
    os.lseek(fd, 0, os.SEEK_SET)
    os.ftruncate(fd, 0)
    os.write(fd, f"{pid}\n".encode("utf-8"))
    os.fsync(fd)


def _read_port_from_file(pid_file: Path) -> Optional[int]:
    """Read the port line from the pid file. Returns None if absent,
    empty, malformed, or the file doesn't exist."""
    try:
        text = pid_file.read_text(encoding="utf-8")
    except FileNotFoundError:
        return None
    except OSError:
        return None
    lines = text.splitlines()
    if len(lines) < 2:
        return None
    try:
        port = int(lines[1].strip())
    except ValueError:
        return None
    if not (1 <= port <= 65535):
        return None
    return port


def _read_port_with_retry(pid_file: Path, cfg: LifecycleConfig) -> int:
    """Bounded retry for the brief window where the holder has the lock
    but hasn't written the port yet. Returns -1 if the retry exhausts."""
    for _ in range(cfg.port_file_retry_attempts):
        port = _read_port_from_file(pid_file)
        if port is not None:
            return port
        time.sleep(cfg.port_file_retry_interval_sec)
    logger.warning(
        "loser path: port file %s never populated within %dms",
        pid_file,
        int(cfg.port_file_retry_attempts * cfg.port_file_retry_interval_sec * 1000),
    )
    return -1


def _tcp_probe(port: int, cfg: LifecycleConfig, *, bind_host: str = "127.0.0.1") -> bool:
    """Loser-side / generic TCP probe with the connect_retry budget. Used
    by hook-handler-style callers that have already paid a port-read."""
    return _probe_with_budget(
        port, bind_host, cfg.connect_retry_attempts, cfg.connect_retry_interval_sec
    )


def _self_probe(port: int, cfg: LifecycleConfig, *, bind_host: str = "127.0.0.1") -> bool:
    """Spawn-side self-probe with the much larger spawn budget. G2 fix:
    the spawning process knows it just bound the socket and can afford to
    wait for the daemon thread to reach serve_forever's accept loop."""
    return _probe_with_budget(
        port, bind_host, cfg.spawn_self_probe_attempts, cfg.spawn_self_probe_interval_sec
    )


def _probe_with_budget(port: int, bind_host: str, attempts: int, interval_sec: float) -> bool:
    """Shared TCP-probe implementation. residual[3] fix: bind_host is no
    longer hardcoded to 127.0.0.1 — important if a caller ever opts into
    a non-loopback bind."""
    for _ in range(attempts):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(0.250)
        try:
            sock.connect((bind_host, port))
            return True
        except OSError:
            pass
        finally:
            sock.close()
        time.sleep(interval_sec)
    return False


# ----------------------------------------------------------------------
# Background threads — sweep + idle shutdown
# ----------------------------------------------------------------------


def _start_background_threads(entry: _SpawnedEntry, cfg: LifecycleConfig) -> None:
    """Start the sweep + idle-shutdown daemon threads."""
    if cfg.sweep_interval_sec > 0:
        sweep_thread = threading.Thread(
            target=_sweep_loop,
            args=(entry, cfg),
            name="coord-sweep",
            daemon=True,
        )
        sweep_thread.start()
    if cfg.idle_shutdown_sec > 0:
        idle_thread = threading.Thread(
            target=_idle_shutdown_loop,
            args=(entry, cfg),
            name="coord-idle",
            daemon=True,
        )
        idle_thread.start()


def _sweep_loop(entry: _SpawnedEntry, cfg: LifecycleConfig) -> None:
    """Periodic sweep: transient → stable grant → notice eviction.

    Order matters per R4: transient sweep first so the stable sweep does
    not race entries that are mid-protocol. F2 notice eviction last —
    it's a pure storage reclaim and doesn't interact with grants.
    """
    coordinator = entry.coordinator
    while not coordinator.shutting_down:
        time.sleep(cfg.sweep_interval_sec)
        if coordinator.shutting_down:
            break
        now_tick = int(time.monotonic())
        try:
            coordinator.service.enforce_transient_timeouts(
                current_tick=now_tick,
                timeout_ticks=cfg.transient_timeout_sec,
            )
            coordinator.service.enforce_stable_grant_timeouts(
                current_tick=now_tick,
                heartbeat_timeout_ticks=cfg.grant_heartbeat_timeout_sec,
                max_hold_ticks=cfg.grant_max_hold_sec,
            )
            evicted = coordinator.registry.evict_stale_notices(
                max_age_sec=cfg.notice_evict_max_age_sec,
            )
            if evicted:
                logger.info("sweep evicted %d stale preemption notice(s)", evicted)
        except Exception as exc:
            # Sweep is best-effort — never crash the coordinator.
            logger.exception("sweep tick failed: %s", exc)


def _idle_shutdown_loop(entry: _SpawnedEntry, cfg: LifecycleConfig) -> None:
    """Wall-clock idle watcher. When ``time.time() - last_request_at >=
    idle_shutdown_sec``, runs the race-safe shutdown sequence and pops
    the registry entry."""
    coordinator = entry.coordinator
    while not coordinator.shutting_down:
        time.sleep(cfg.sweep_interval_sec)
        if coordinator.shutting_down:
            break
        idle_for = time.time() - coordinator._last_request_at  # type: ignore[attr-defined]
        if idle_for >= cfg.idle_shutdown_sec:
            logger.info(
                "coordinator idle for %.0fs (>= %ss threshold) — shutting down",
                idle_for, cfg.idle_shutdown_sec,
            )
            _shutdown_sequence(entry)
            if entry.shutdown_done.is_set():
                _SPAWNED_REGISTRY.pop(str(coordinator.coordinator_root), None)
            return


def _shutdown_sequence(entry: _SpawnedEntry) -> bool:
    """Race-safe, mutexed shutdown.

    Concurrent triggers (stop_coordinator + idle-shutdown thread) are
    serialized by ``entry.shutdown_lock``. The first caller runs the
    sequence; subsequent callers see ``shutdown_done`` set and return
    immediately without touching pid file or fd.

    Ordering (revised per subagent findings G4 + G5):
      1. Drop the port from the pid file FIRST. This closes the cascade
         window in G5: loser readers immediately see "no port" instead
         of a port pointing at a coordinator that's about to die.
      2. Set the coordinator's shutting_down flag (handlers 503).
      3. Run coordinator.shutdown() — blocks on serve_forever exit and
         drains in-flight handlers. If THIS raises (G4), we ABORT: the
         lock stays held, the pid file stays port-empty, and the next
         spawn-side caller will retry. Stable-grant reclamation
         (max_hold_ticks) provides the long-term safety net.
      4. Release the flock.
      5. Close the fd.

    Returns True if this caller actually executed the sequence; False if
    the sequence was already done by a previous caller.
    """
    with entry.shutdown_lock:
        if entry.shutdown_done.is_set():
            return False

        coordinator = entry.coordinator
        lock_fd = entry.lock_fd

        # Step 1 (G5): drop port FIRST so loser readers don't get a
        # stale-but-live port during the shutdown drain window.
        try:
            _rewrite_pidfile_drop_port(lock_fd, os.getpid())
        except OSError as exc:
            logger.warning(
                "could not drop port from pid file during shutdown: %s — "
                "loser readers may still see stale port",
                exc,
            )

        # Step 2 + 3: shut down the HTTP server. If this raises, abort the
        # sequence — leave the lock held so no new coordinator can spawn
        # while in-flight handlers are still running against a partially
        # torn-down state.
        try:
            coordinator.shutdown()
        except Exception as exc:
            logger.critical(
                "coordinator.shutdown failed; aborting shutdown sequence with "
                "lock still held. Stable-grant reclamation (max_hold_ticks=%d) "
                "is the recovery path. Underlying error: %s",
                # config thresholds not on entry; using a generic mention
                1800, exc,
            )
            # Note: shutdown_done remains UNSET so a retry is possible.
            return True

        # Step 4 + 5: release the flock + close fd. Wrap each in try/except
        # so a failure at this stage (rare — lock already validly held)
        # doesn't crash but still marks the sequence done.
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_UN)
        except OSError as exc:
            logger.warning("could not release flock during shutdown: %s", exc)
        try:
            os.close(lock_fd)
        except OSError:
            pass

        entry.shutdown_done.set()
        return True
