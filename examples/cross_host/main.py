#!/usr/bin/env python3
# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents
"""Cross-host coherence demo — slice 1 (artifact-coordination) + slice 2
(effect-ordering), with an optional negative-control (--baseline) mode.

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

CLI:
  - python examples/cross_host/main.py
        Runs the with-CCS demo only (slice 1 deny + recover, slice 2 fire/hold).
        Exit 0 iff the with-CCS contract holds.
  - python examples/cross_host/main.py --baseline
        Runs the negative control FIRST (silent lost update for slice 1, stale
        effect fire for slice 2 — the failure modes we claim CCS prevents),
        then the with-CCS demo. Exit 0 iff broken-must-lose AND fixed-must-prevent.
        This is the honest screen-share contract: the deny is measured against
        its absence, not asserted.

Exit code: 0 iff the honest contract holds (deny + recover with CCS — and, with
--baseline, ALSO silent loss + stale fire without CCS); 1 otherwise.
"""

from __future__ import annotations

import argparse
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
CONFIG_KEY = "config.json"


def _log(msg: str) -> None:
    print(msg, flush=True)


def _make_vols(tmp: str, make_endpoint, seed_a: bytes | None = None, seed_b: bytes | None = None):
    """Two CoherentVolumes on separate roots, both attached to the coordinator.

    Same setup for every scene; the baseline-vs-with-CCS contrast is in HOW the
    clients then use the volume API, not in the volume construction itself.
    """
    root_a = Path(tmp) / "a"
    root_b = Path(tmp) / "b"
    root_a.mkdir(parents=True, exist_ok=True)
    root_b.mkdir(parents=True, exist_ok=True)
    if seed_a is not None:
        (root_a / SHARED_KEY).write_bytes(seed_a)
    if seed_b is not None:
        (root_b / SHARED_KEY).write_bytes(seed_b)
    vol_a = CoherentVolume(root_a, on_error="strict", on_stale_write="allow", remote_endpoint=make_endpoint())
    vol_b = CoherentVolume(root_b, on_error="strict", on_stale_write="allow", remote_endpoint=make_endpoint())
    return vol_a, vol_b


def _run_demo(make_endpoint) -> bool:
    """Slice 1 (with CCS): A reads@vN; B commits@vN+1; A's stale write is denied;
    A recovers. Returns True iff denied-then-recovered.
    """
    with tempfile.TemporaryDirectory() as tmp:
        vol_a, vol_b = _make_vols(tmp, make_endpoint, seed_a=b"v0", seed_b=b"v0")

        _data_a, v_a = vol_a.read_with_version(SHARED_KEY)
        _log(f"  A read {SHARED_KEY} @ version {v_a} (decision-time version, tracked)")

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


def _run_demo_baseline(make_endpoint) -> bool:
    """Slice 1 (NEGATIVE CONTROL — no version-CAS discipline):
    A reads@vN but DOES NOT track its decision-time version; B commits@vN+1;
    A re-reads the current version right before writing and commits against it
    (the classic convention-only / un-coordinated lost-update bug pattern). B's
    bytes are silently lost.

    Returns True iff the lost update occurred as expected — i.e., the failure we
    claim CCS prevents is real and reproducible. This makes the deny in
    ``_run_demo`` meaningful (measured against its absence, not asserted).
    """
    with tempfile.TemporaryDirectory() as tmp:
        vol_a, vol_b = _make_vols(tmp, make_endpoint, seed_a=b"v0", seed_b=b"v0")

        # A reads, planning to use this as the decision-time version — but the
        # baseline agent has no version-CAS discipline and "forgets" v_a below.
        _data_a, v_a = vol_a.read_with_version(SHARED_KEY)
        _log(f"  A read {SHARED_KEY} @ version {v_a} (no decision-time discipline)")

        _data_b, v_b = vol_b.read_with_version(SHARED_KEY)
        vol_b.write_cas_at(SHARED_KEY, v_b, b"from-b")
        _log(f"  B committed 'from-b' @ v{v_b + 1}")

        # The baseline pattern: A re-reads the latest version right before
        # writing — there is no check that the artifact moved under it.
        _, v_a_latest = vol_a.read_with_version(SHARED_KEY)
        vol_a.write_cas_at(SHARED_KEY, v_a_latest, b"from-a")
        _log(f"  ✗ A's write SUCCEEDED against v{v_a_latest} — no deny (no decision-time check)")

        data_now, v_now = vol_a.read_with_version(SHARED_KEY)
        if data_now == b"from-b":
            _log(f"  ✗ unexpected: B's bytes survived at v{v_now} — baseline did not lose")
            return False
        _log(f"  ✓ baseline confirmed: canonical now has {data_now!r} @ v{v_now}; B's 'from-b' is LOST")
        return True


def _run_effect_gate(make_endpoint) -> bool:
    """Slice 2 (with CCS): an effect gated on config@vN FIRES when config is
    unchanged and is HELD when config advanced under it. Returns True iff both
    hold.

    Maps to north-star Epic — Effect-Ordering primitives (all shipped):
      EO-1 ``read_at_version``      — read the gating artifact's version
      EO-2 version-gated effect     — gate the effect on the decision-time version
      EO-3 Deny → HOLD              — stale gate holds the effect; no fire on stale input
    """
    with tempfile.TemporaryDirectory() as tmp:
        root_a = Path(tmp) / "a"
        root_b = Path(tmp) / "b"
        root_a.mkdir(parents=True, exist_ok=True)
        root_b.mkdir(parents=True, exist_ok=True)
        (root_a / CONFIG_KEY).write_text('{"v": 0}', encoding="utf-8")
        (root_b / CONFIG_KEY).write_text('{"v": 0}', encoding="utf-8")
        vol_a = CoherentVolume(root_a, on_error="strict", on_stale_write="allow", remote_endpoint=make_endpoint())
        vol_b = CoherentVolume(root_b, on_error="strict", on_stale_write="allow", remote_endpoint=make_endpoint())
        _c, v_decision = vol_a.read_with_version(CONFIG_KEY)
        _log(f"  EO-1 ‹read_at_version›: A read {CONFIG_KEY} @ v{v_decision}")

        def fires() -> bool:
            """The effect fires only if config is still at the decision version."""
            _cur, current = vol_a.read_with_version(CONFIG_KEY)
            return current == v_decision

        if not fires():
            _log("  ✗ effect-gate held even though config was unchanged")
            return False
        _log(f"  EO-2 ‹version-gated effect›: config unchanged @ v{v_decision} → effect would FIRE")

        _cb, v_b = vol_b.read_with_version(CONFIG_KEY)
        vol_b.write_cas_at(CONFIG_KEY, v_b, b'{"v": 1}')
        _log(f"  (B advanced config beyond v{v_decision})")
        if fires():
            _log("  ✗ effect-gate FIRED on a stale config — coherence FAILED")
            return False
        _log("  ✓ EO-3 ‹HOLD›: config advanced under A → effect HELD (not fired on stale input)")
        return True


def _run_effect_gate_baseline(make_endpoint) -> bool:
    """Slice 2 (NEGATIVE CONTROL — no effect-gate):
    A "fires" the effect against the latest config without gating on its
    decision-time version. The effect fires on stale config — a silent stale
    execution (the CI failure pattern where a build runs against a config that
    was edited while the runner was deciding).

    Returns True iff the stale fire occurred as expected — codifying the failure
    that EO-1..EO-3 prevent.
    """
    with tempfile.TemporaryDirectory() as tmp:
        root_a = Path(tmp) / "a"
        root_b = Path(tmp) / "b"
        root_a.mkdir(parents=True, exist_ok=True)
        root_b.mkdir(parents=True, exist_ok=True)
        (root_a / CONFIG_KEY).write_text('{"v": 0}', encoding="utf-8")
        (root_b / CONFIG_KEY).write_text('{"v": 0}', encoding="utf-8")
        vol_a = CoherentVolume(root_a, on_error="strict", on_stale_write="allow", remote_endpoint=make_endpoint())
        vol_b = CoherentVolume(root_b, on_error="strict", on_stale_write="allow", remote_endpoint=make_endpoint())

        _c, v_decision = vol_a.read_with_version(CONFIG_KEY)
        _log(f"  A read {CONFIG_KEY} @ v{v_decision} (no gate to enforce)")

        _cb, v_b = vol_b.read_with_version(CONFIG_KEY)
        vol_b.write_cas_at(CONFIG_KEY, v_b, b'{"v": 1}')
        _log(f"  B advanced config to v{v_b + 1}")

        # The baseline pattern: A fires the effect ungated. There is no
        # version-CAS check before the effect runs.
        _, v_a_when_firing = vol_a.read_with_version(CONFIG_KEY)
        if v_a_when_firing == v_decision:
            _log("  ✗ unexpected: config did not advance — baseline did not exercise stale fire")
            return False
        # "fired" — in a real CI step this would be the deploy/build invocation.
        _log(
            f"  ✗ effect FIRED on stale config (decided @ v{v_decision}, current is "
            f"v{v_a_when_firing}) — baseline confirmed"
        )
        return True


def _track(endpoint) -> None:
    """Track the demo keys on the coordinator (so they are versioned)."""
    _cc_post(endpoint, "/policy/track", {"paths": [SHARED_KEY, CONFIG_KEY]})


def _run_slices(make_endpoint, *, baseline: bool) -> bool:
    """Run the slices end-to-end.

    Default (baseline=False): with-CCS only — slice 1 deny+recover, slice 2 EO
    fire/HOLD. Exit 0 iff both with-CCS contracts hold.

    --baseline (baseline=True): negative control FIRST — slice 1 silent loss,
    slice 2 stale fire (the failures we claim CCS prevents); then the with-CCS
    pass. Exit 0 iff broken-must-lose AND fixed-must-prevent — the screen-share
    contract that makes the deny meaningful, not asserted.
    """
    if baseline:
        _log("=== Negative control — agents WITHOUT version-CAS discipline ===")
        _log("Slice 1 baseline — un-coordinated write (the silent lost update we prevent):")
        b1 = _run_demo_baseline(make_endpoint)
        _log("Slice 2 baseline — un-gated effect (the stale fire EO-1..EO-3 prevents):")
        b2 = _run_effect_gate_baseline(make_endpoint)
        if not (b1 and b2):
            _log("  ✗ baseline did not reproduce the failure — the negative control is broken")
            return False
        _log("")

    _log("=== With CCS — version-CAS spine + effect-ordering ===")
    _log("Slice 1 — artifact-coordination (stale write denied across the endpoint):")
    slice1 = _run_demo(make_endpoint)
    _log("Slice 2 — effect-ordering (EO-1..EO-3: gate effect on config@vN):")
    slice2 = _run_effect_gate(make_endpoint)
    return slice1 and slice2


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="examples/cross_host/main.py",
        description=("Cross-host coherence demo — slice 1 + slice 2, with optional negative-control mode."),
    )
    parser.add_argument(
        "--baseline",
        action="store_true",
        help=(
            "Run the negative control FIRST (silent lost update + stale fire), "
            "then the with-CCS demo. Exit 0 iff broken-must-lose AND "
            "fixed-must-prevent — the honest screen-share contract."
        ),
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    baseline = args.baseline

    remote = RemoteCoordinatorConfig.from_env()
    if remote.enabled and remote.host:
        # Cross-host: connect to a coordinator started separately on host 1.
        if not (remote.port and remote.secret):
            _log("CCS_REMOTE_HOST set but CCS_REMOTE_PORT / CCS_REMOTE_SECRET_FILE missing")
            return 1
        _log(f"Cross-host demo against coordinator at {remote.host}:{remote.port}")

        def make_ep():
            return resolve_remote_endpoint(remote.host, remote.port, remote.secret)

        try:
            _track(make_ep())
            ok = _run_slices(make_ep, baseline=baseline)
        except (OSError, CoherenceError) as exc:
            # Coordinator unreachable / auth failure -> a clean FAIL verdict,
            # never a raw traceback that reads as a crash rather than a deny.
            _log(f"Cross-host coordinator unreachable or rejected the client: {exc}")
            ok = False
    else:
        # Local smoke: spawn a loopback coordinator, run both clients here.
        # Guard against the silent-loopback footgun: CCS_REMOTE_HOST set but the
        # gate flag off means we are NOT testing a host boundary — say so loudly.
        if os.environ.get("CCS_REMOTE_HOST"):
            _log(
                "WARNING: CCS_REMOTE_HOST is set but CCS_REMOTE_COORDINATOR is off "
                "→ running LOCAL loopback smoke, not cross-host. Set "
                "CCS_REMOTE_COORDINATOR=1 to reach the remote coordinator."
            )
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

                ok = _run_slices(make_ep, baseline=baseline)
            finally:
                stop_coordinator(coord_root)

    _log("RESULT: PASS" if ok else "RESULT: FAIL")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
