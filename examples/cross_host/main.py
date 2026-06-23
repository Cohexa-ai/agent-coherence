#!/usr/bin/env python3
# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents
"""Slice-1 cross-host coherence demo (Gate A).

Two clients coordinate ONE coordinator over the network: a stale write is denied
by version-CAS *across the host boundary*, and the loser recovers via re-read +
retry — no silent lost update.

Topology (honest scope): a SINGLE centralized coordinator (SPOF, single-region);
NOT distributed/HA. Coordinator-version-only: the shared artifact is TRACKED on
the coordinator (so it is versioned) but NOT strict — the deny is the OCC
version-CAS; no strict read-enforcement is needed.

Run modes:
  - Local (default): with no CCS_REMOTE_HOST set, this spawns a loopback
    coordinator and runs both clients in one process — the mechanism smoke test,
    runnable anywhere. Proves the deny + recovery, NOT a real host boundary.
  - Cross-host: start a coordinator on host 1 bound to a private-range address,
    track the shared key, then on host 2 set CCS_REMOTE_COORDINATOR=1,
    CCS_REMOTE_HOST=<host1>, CCS_REMOTE_PORT=<port>, CCS_REMOTE_SECRET_FILE=<path>
    and run this script. See README.md. The genuine non-loopback bind/Host check
    runs only here (Linux netns/veth or two VMs).

Exit code: 0 iff the stale write was denied AND recovery succeeded (the honest
"broken-must-lose AND fixed-must-prevent" contract); 1 otherwise.
"""

from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path

from ccs.adapters.claude_code.lifecycle import (
    LifecycleConfig,
    ensure_coordinator,
    stop_coordinator,
)
from ccs.adapters.coherent_volume import CoherentVolume
from ccs.cli._coherence_client import (
    RemoteCoordinatorConfig,
    resolve_endpoint,
    resolve_remote_endpoint,
)
from ccs.cli._coherence_client import (
    post as _cc_post,
)
from ccs.core.exceptions import CoherenceError

SHARED_KEY = "shared.txt"


def _log(msg: str) -> None:
    print(msg, flush=True)


def _seed(root: Path) -> None:
    root.mkdir(parents=True, exist_ok=True)
    (root / SHARED_KEY).write_text("v0", encoding="utf-8")


def _run_demo(make_endpoint) -> bool:
    """A reads@vN; B commits@vN+1; A's stale write is denied; A recovers.

    ``make_endpoint`` returns a fresh CoordinatorEndpoint per client (each client
    is an independent writer). Returns True iff denied-then-recovered.
    """
    with tempfile.TemporaryDirectory() as tmp:
        root_a = Path(tmp) / "a"
        root_b = Path(tmp) / "b"
        _seed(root_a)
        _seed(root_b)
        vol_a = CoherentVolume(
            root_a, on_error="strict", on_stale_write="allow", remote_endpoint=make_endpoint()
        )
        vol_b = CoherentVolume(
            root_b, on_error="strict", on_stale_write="allow", remote_endpoint=make_endpoint()
        )

        _data_a, v_a = vol_a.read_with_version(SHARED_KEY)
        _log(f"  A read {SHARED_KEY} @ version {v_a}")

        _data_b, v_b = vol_b.read_with_version(SHARED_KEY)
        vol_b.write_cas_at(SHARED_KEY, v_b, b"from-b")
        _log(f"  B committed a new version (was {v_b})")

        try:
            vol_a.write_cas_at(SHARED_KEY, v_a, b"from-a")
            _log("  ✗ A's STALE write SUCCEEDED — coherence FAILED (silent lost update)")
            return False
        except CoherenceError as exc:
            _log(f"  ✓ A's stale write DENIED across the boundary: {type(exc).__name__}")

        _data_a2, v_a2 = vol_a.read_with_version(SHARED_KEY)
        if v_a2 <= v_a:
            _log("  ✗ recovery read did not advance the version")
            return False
        vol_a.write_cas_at(SHARED_KEY, v_a2, b"from-a-2")
        _log(f"  ✓ A recovered: re-read @ version {v_a2}, retried, committed")
        return True


def _track(endpoint) -> None:
    """Make the shared key TRACKED on the coordinator (so it is versioned)."""
    _cc_post(endpoint, "/policy/track", {"paths": [SHARED_KEY]})


def main() -> int:
    remote = RemoteCoordinatorConfig.from_env()
    if remote.enabled and remote.host:
        # Cross-host: connect to a coordinator started separately on host 1.
        if not (remote.port and remote.secret):
            _log("CCS_REMOTE_HOST set but CCS_REMOTE_PORT / CCS_REMOTE_SECRET_FILE missing")
            return 1
        _log(f"Cross-host demo against coordinator at {remote.host}:{remote.port}")

        def make_ep():
            return resolve_remote_endpoint(remote.host, remote.port, remote.secret)

        _track(make_ep())
        ok = _run_demo(make_ep)
    else:
        # Local smoke: spawn a loopback coordinator, run both clients here.
        os.environ.setdefault("CCS_REMOTE_COORDINATOR", "1")  # remote-mode clients on loopback
        _log("Local smoke (loopback, one process) — proves the mechanism, not a host boundary")
        with tempfile.TemporaryDirectory() as tmp:
            coord_root = Path(tmp) / "coord"
            coord_root.mkdir()
            ensure_coordinator(coord_root, config=LifecycleConfig(idle_shutdown_sec=0))
            try:
                ep = resolve_endpoint(coord_root)
                _track(ep)

                def make_ep():
                    return resolve_remote_endpoint("127.0.0.1", ep.port, ep.bearer)

                ok = _run_demo(make_ep)
            finally:
                stop_coordinator(coord_root)

    _log("RESULT: PASS" if ok else "RESULT: FAIL")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
