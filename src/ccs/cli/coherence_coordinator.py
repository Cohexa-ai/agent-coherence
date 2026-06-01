# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""``agent-coherence-coordinator`` — lazy-spawn the workspace coordinator.

Invoked by the plugin's ``bin/ensure-coordinator`` shim from the
``SessionStart`` hook. Cheap to re-invoke: if a coordinator is already
running for the workspace, returns immediately.

## Detach model

The coordinator must outlive this CLI invocation — Claude Code's
SessionStart hook expects the shim to exit promptly so the session can
proceed. We can't simply call :func:`ensure_coordinator` and return,
because :func:`ensure_coordinator` starts the HTTP server in a *daemon*
thread that dies the moment the parent process exits.

Pattern:

1. Parent process probes ``.coherence/server.pid``. If a coordinator is
   already up and TCP-reachable, print its port and exit 0 — no fork.
2. Otherwise, parent spawns a detached child via :class:`subprocess.Popen`
   with ``start_new_session=True``. The child runs in ``--_daemonized``
   mode: calls :func:`ensure_coordinator`, then blocks indefinitely so
   its daemon threads stay alive.
3. Parent polls ``.coherence/server.pid`` for the port to appear (the
   child writes it inside :func:`ensure_coordinator`). Once the port is
   visible and TCP-reachable, parent prints it and exits 0.

The ``--_daemonized`` flag is intentionally underscore-prefixed: this is
the internal worker mode, not part of the user-facing CLI contract.

Exit codes:
- 0: coordinator is up and reachable (port printed to stdout)
- 1: not in a git repository (no coordinator root resolved)
- 2: coordinator failed to spawn (platform unsupported, FS read-only,
     port never appeared in the file, or detached child crashed)
"""

from __future__ import annotations

import argparse
import logging
import subprocess
import sys
import time
from pathlib import Path
from typing import Sequence

from ccs.adapters.claude_code.lifecycle import (
    LifecycleConfig,
    ensure_coordinator,
    stop_coordinator,
)
from ccs.adapters.claude_code.lifecycle import (
    read_port_from_file as _read_port_from_file,
)
from ccs.adapters.claude_code.lifecycle import (
    tcp_probe as _tcp_probe,
)
from ccs.adapters.claude_code.resolver import find_coordinator_root
from ccs.cli._coherence_client import err

logger = logging.getLogger(__name__)

#: How long the parent polls for the detached child to write the port
#: file. Generous: cold Python interpreter + SQLite WAL setup + bind
#: can take several seconds on slow disks. Matches the lifecycle module's
#: spawn_self_probe budget × 1.5 for the inter-process round trip.
_DETACH_PORT_WAIT_ATTEMPTS = 100  # × 0.1s = 10s
_DETACH_PORT_WAIT_INTERVAL_SEC = 0.1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="agent-coherence-coordinator",
        description="Lazy-spawn the Claude Code coherence coordinator for this workspace.",
    )
    parser.add_argument(
        "--root",
        type=Path,
        default=None,
        help="Override the coordinator root (default: walk up from cwd to git root).",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress the 'port=N' line on stdout (still exits 0 on success).",
    )
    parser.add_argument(
        "--prepare-for-migration",
        action="store_true",
        help=(
            "Atomically release all EXCLUSIVE/MODIFIED grants and shut "
            "down the coordinator for this workspace. RUN THIS before "
            "switching the coherence backend between Python and Node — "
            "see the v0.1.1 migration runbook."
        ),
    )
    parser.add_argument(
        "--_daemonized",
        action="store_true",
        help=argparse.SUPPRESS,  # hidden — internal detached-worker flag
    )
    parser.add_argument(
        "--no-detach",
        action="store_true",
        help=argparse.SUPPRESS,  # hidden — test mode: in-process spawn
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    root_arg = args.root if args.root is not None else find_coordinator_root()
    if root_arg is None:
        err("agent-coherence-coordinator: not in a git repository")
        return 1
    root = Path(root_arg).resolve()

    # Daemonized worker mode: bind + serve, then block forever so the
    # daemon HTTP/sweep/idle threads stay alive.
    if args._daemonized:
        return _run_daemonized(root)

    # Unit 8 (Decision 1): release-all-grants + shutdown for backend
    # switch safety. Routed through the running coordinator so we
    # invalidate every M/E grant from the same process that owns the
    # SQLite registry (the operator's CLI invocation may not be that
    # process when the coordinator was detached on a prior session).
    if args.prepare_for_migration:
        return _run_prepare_for_migration(root, quiet=args.quiet)

    # Test mode: keep the legacy in-process behavior so test_claude_code_cli
    # can spawn + assert in the same Python process. Not for users.
    if args.no_detach:
        port = ensure_coordinator(root)
        if port == -1:
            err("agent-coherence-coordinator: coordinator could not be spawned")
            return 2
        if not args.quiet:
            print(f"port={port}", flush=True)
        return 0

    # Normal user path: probe for an existing coordinator first.
    pid_file = root / ".coherence" / "server.pid"
    existing_port = _read_port_from_file(pid_file)
    cfg = LifecycleConfig()
    if existing_port is not None and _tcp_probe(existing_port, cfg):
        if not args.quiet:
            print(f"port={existing_port}", flush=True)
        return 0

    # No live coordinator — fork a detached child to run it.
    return _spawn_detached(root, quiet=args.quiet)


def _run_prepare_for_migration(root: Path, *, quiet: bool) -> int:
    """Unit 8: backend-switch safety entry point.

    Connects to the running coordinator, posts /admin/prepare-for-migration
    with the operator opt-in header, then polls the pid file + TCP probe
    until the coordinator is genuinely gone. Returns 0 on clean
    shutdown, 0 also when no coordinator was running (idempotent — safe
    to script as a pre-switch step that may or may not find a live
    coordinator), 2 on any error reaching the coordinator.
    """
    import urllib.error as _urlerr

    from ccs.cli._coherence_client import (
        CoordinatorUnavailable,
        resolve_endpoint,
    )
    from ccs.cli._coherence_client import (
        post as _post,
    )

    pid_file = root / ".coherence" / "server.pid"
    if not pid_file.exists():
        if not quiet:
            print("prepare-for-migration: no coordinator running (idempotent no-op)", flush=True)
        return 0

    try:
        endpoint = resolve_endpoint(root)
    except CoordinatorUnavailable as exc:
        # The pid file existed but the coordinator isn't reachable — it
        # may have died uncleanly. Nothing to release in the registry
        # via HTTP; idempotent no-op.
        if not quiet:
            print(
                f"prepare-for-migration: coordinator not reachable ({exc}); "
                "treating as clean — nothing to release.",
                flush=True,
            )
        return 0

    try:
        # M-04 / finding #28: use the shared _coherence_client.post() with
        # extra_headers instead of the now-deleted _post_with_operator_header().
        # Uses CLI_HTTP_TIMEOUT_SEC (6.0s) instead of the old hardcoded 5s.
        resp = _post(endpoint, "/admin/prepare-for-migration", {},
                     extra_headers={"Coherence-Local-Operator": "true"})
    except _urlerr.HTTPError as exc:
        err(f"agent-coherence-coordinator: prepare-for-migration HTTP {exc.code}")
        return 2

    if not resp.get("ok"):
        err(f"agent-coherence-coordinator: prepare-for-migration returned {resp!r}")
        return 2

    released = int(resp.get("released", 0))
    errors = resp.get("errors") or []

    # Poll until the coordinator is no longer TCP-reachable. The server-
    # side schedules shutdown ~100ms after responding, then the in-flight
    # drain + serve_forever exit add a bit more — 5s budget is generous.
    cfg = LifecycleConfig()
    poll_deadline = time.monotonic() + 5.0
    while time.monotonic() < poll_deadline:
        port = _read_port_from_file(pid_file)
        if port is None or not _tcp_probe(port, cfg):
            break
        time.sleep(0.1)
    else:
        err(
            "agent-coherence-coordinator: prepare-for-migration: "
            "coordinator failed to shut down within 5s after release"
        )
        return 2

    if not quiet:
        print(
            f"prepare-for-migration: released {released} grant(s); "
            f"coordinator shutdown clean.",
            flush=True,
        )
        if errors:
            print(f"  errors: {errors}", flush=True)
    return 0


def _run_daemonized(root: Path) -> int:
    """Internal worker — called via ``--_daemonized``. Run the coordinator
    and block indefinitely. Exits on SIGTERM (sent by stop_coordinator)
    or natural idle-shutdown (lifecycle's idle loop also detaches via
    ``return`` in its loop, leaving the main thread to block here)."""
    port = ensure_coordinator(root)
    if port == -1:
        return 2

    # Block the main thread so the daemon HTTP/sweep/idle threads stay
    # alive. KP-7: use the public wait_for_shutdown API rather than
    # reaching into lifecycle._SPAWNED_REGISTRY + entry.shutdown_done.
    # The wait blocks until idle-shutdown completes, stop_coordinator
    # fires, or the shutdown sequence aborts.
    from ccs.adapters.claude_code.lifecycle import (
        wait_for_shutdown,
    )
    try:
        wait_for_shutdown(root)
    except KeyboardInterrupt:
        stop_coordinator(root)
    return 0


def _spawn_detached(root: Path, *, quiet: bool) -> int:
    """Spawn a detached child running the daemonized worker, then poll
    the port file until the child writes it (or timeout)."""
    pid_file = root / ".coherence" / "server.pid"

    # Use subprocess.Popen with start_new_session=True so the child runs
    # in its own process group and survives the parent's exit. stdio
    # redirected to /dev/null so the child doesn't keep the parent's
    # terminal busy.
    try:
        subprocess.Popen(
            [
                sys.executable, "-m", "ccs.cli.coherence_coordinator",
                "--_daemonized", "--root", str(root),
            ],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
            close_fds=True,
        )
    except OSError as exc:
        err(f"agent-coherence-coordinator: could not spawn detached worker: {exc}")
        return 2

    cfg = LifecycleConfig()
    for _ in range(_DETACH_PORT_WAIT_ATTEMPTS):
        port = _read_port_from_file(pid_file)
        if port is not None and _tcp_probe(port, cfg):
            if not quiet:
                print(f"port={port}", flush=True)
            return 0
        time.sleep(_DETACH_PORT_WAIT_INTERVAL_SEC)

    err(
        "agent-coherence-coordinator: detached worker did not become reachable "
        f"within {_DETACH_PORT_WAIT_ATTEMPTS * _DETACH_PORT_WAIT_INTERVAL_SEC:.0f}s"
    )
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
