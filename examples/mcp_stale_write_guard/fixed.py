# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Fixed cases: the same sequence, routed through the stale-write-guard-fs server.

GREEN drives the server's OWN tool contract (``ccs.mcp.server._do_*``: validate →
serialize → CoherentVolume → typed CallToolResult), so the demo proves the
server, not just the coordinator underneath it.

  - ``run_sequential(guarded=True)`` — the agent's stale write is DENIED
    (reason=stale_view); the agent reacquires and does NOT clobber → the peer's
    value survives EXACTLY. ``guarded=False`` is the NEGATIVE CONTROL: the same
    flow with the path left unguarded (the deny disabled) → the loss returns,
    proving the green depends on the deny.
  - ``run_concurrent()`` — two writers compare-and-set the same version; the
    loser gets a typed version_mismatch, re-reads, re-merges, and re-CASes → both
    lines land (the golden merge).

Sequenced, deterministic, offline: each run spawns a local coordinator subprocess
and tears it down in ``finally``.
"""

from __future__ import annotations

import os
import shutil
import sys
import tempfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))
# A spawned coordinator subprocess must import ``ccs`` too; propagate the src
# path so the demo also runs from a bare checkout (harmless when installed).
_pp = os.environ.get("PYTHONPATH", "")
if str(SRC_ROOT) not in _pp.split(os.pathsep):
    os.environ["PYTHONPATH"] = f"{SRC_ROOT}{os.pathsep}{_pp}" if _pp else str(SRC_ROOT)

from ccs.adapters.claude_code.lifecycle import LifecycleConfig, stop_coordinator
from ccs.adapters.coherent_volume import CoherentVolume
from ccs.mcp.server import _do_reacquire, _do_read, _do_write, _do_write_cas
from ccs.mcp.session import SessionConfig
from examples.mcp_stale_write_guard import (
    AGENT_LINE,
    BASE,
    GOLDEN,
    LINE_A,
    LINE_B,
    PEER_VALUE,
    REL,
)

# Snappy local spawn for a one-command demo; no idle shutdown mid-run.
_DEMO_CFG = LifecycleConfig(
    idle_shutdown_sec=0,
    sweep_interval_sec=0.1,
    port_file_retry_attempts=40,
    port_file_retry_interval_sec=0.05,
    connect_retry_attempts=20,
    connect_retry_interval_sec=0.05,
)


def _content(result) -> str:
    return result.structuredContent["content"]


def run_sequential(*, guarded: bool) -> dict[str, object]:
    """Stale-overwrite, denied + recovered through the server's tools.

    ``guarded`` toggles whether the artifact's glob is strict-enforced: True
    guards ``data/**`` (the deny fires); False guards an unrelated glob so the
    same flow runs with the deny DISABLED (the negative control)."""
    workspace = Path(tempfile.mkdtemp(prefix="swg_seq_"))
    target = workspace / REL
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(BASE)
    managed = ("data/**",) if guarded else ("other/**",)
    config = SessionConfig(root=workspace.resolve(), managed=managed)
    agent = CoherentVolume(workspace, managed=managed, on_error="strict", config=_DEMO_CFG)
    try:
        peer = CoherentVolume(workspace, managed=managed, on_error="strict", config=_DEMO_CFG)

        agent_buffer = _content(_do_read(agent, config, REL)) + AGENT_LINE  # derived from BASE
        _do_read(peer, config, REL)
        _do_write(peer, config, REL, PEER_VALUE)  # peer commits its line

        wrote = _do_write(agent, config, REL, agent_buffer)  # agent's STALE write
        denied = bool(wrote.isError)
        if denied:
            # Recover: reacquire shows the peer's value; the agent does NOT clobber.
            _do_reacquire(agent, config, REL)

        final = target.read_text()
        return {
            "guarded": guarded,
            "stale_write_denied": denied,
            "deny_reason": wrote.structuredContent.get("reason") if denied else None,
            "final": final,
            "preserved_peer_value": final == PEER_VALUE,
        }
    finally:
        stop_coordinator(workspace)
        shutil.rmtree(workspace, ignore_errors=True)


def run_concurrent() -> dict[str, object]:
    """Two writers CAS the same version; the loser re-reads, re-merges, re-CASes →
    both lines land (the golden merge), never a silent loss."""
    workspace = Path(tempfile.mkdtemp(prefix="swg_conc_"))
    target = workspace / REL
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(BASE)
    config = SessionConfig(root=workspace.resolve(), managed=("data/**",))
    vol_a = CoherentVolume(workspace, managed=("data/**",), on_error="strict", config=_DEMO_CFG)
    try:
        vol_b = CoherentVolume(workspace, managed=("data/**",), on_error="strict", config=_DEMO_CFG)

        read_a = _do_read(vol_a, config, REL)
        read_b = _do_read(vol_b, config, REL)
        va = read_a.structuredContent["version"]
        vb = read_b.structuredContent["version"]
        conflicts_a: dict[str, int] = {}
        conflicts_b: dict[str, int] = {}

        res_a = _do_write_cas(vol_a, config, conflicts_a, REL, va, _content(read_a) + LINE_A)
        res_b = _do_write_cas(vol_b, config, conflicts_b, REL, vb, _content(read_b) + LINE_B)
        b_first_conflicted = bool(res_b.isError)

        # B re-reads A's commit, re-merges its line, re-CASes.
        read_b2 = _do_read(vol_b, config, REL)
        vb2 = read_b2.structuredContent["version"]
        res_b2 = _do_write_cas(vol_b, config, conflicts_b, REL, vb2, _content(read_b2) + LINE_B)

        final = target.read_text()
        return {
            "a_committed": not bool(res_a.isError),
            "b_first_conflicted": b_first_conflicted,
            "b_merged_and_committed": not bool(res_b2.isError),
            "final": final,
            "merged_golden": final == GOLDEN,
        }
    finally:
        stop_coordinator(workspace)
        shutil.rmtree(workspace, ignore_errors=True)


def main() -> int:
    seq = run_sequential(guarded=True)
    conc = run_concurrent()
    print("FIXED (stale-write-guard-fs) — driven through the server's tools")
    print(f"  sequential: stale write denied={seq['stale_write_denied']}")
    print(f"  sequential: peer value preserved={seq['preserved_peer_value']}")
    print(f"  concurrent: golden merge={conc['merged_golden']}")
    ok = bool(seq["preserved_peer_value"]) and bool(conc["merged_golden"])
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
