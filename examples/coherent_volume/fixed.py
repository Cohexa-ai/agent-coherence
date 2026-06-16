# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Fixed case: the same stale-overwrite, denied and recovered via CoherentVolume.

Same sequence as ``broken.py`` — A reads, B reads, A commits, B writes from its
older read — but routed through the CoherentVolume *explicit* API. A's commit
invalidates B; B's stale write is then **denied** (the lost update is prevented),
and B recovers with ``reacquire()`` (re-mint identity + mandatory fresh read),
recomputes from the current value, and writes — so both updates survive.

Sequenced, not raced: deterministic by construction. The explicit
read/write/reacquire API is the supported primitive and the load-bearing proof
here; the optional ``install()`` open()-shim (see the README) is a convenience
layer over this same path, not a separate guarantee.

This spawns a local coordinator subprocess (no network, no API keys) and tears
it down in ``finally``.
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
from ccs.core.exceptions import CoherenceError

_START = 100
_A_ADDS = 10
_B_ADDS = 5
_REL = "data/budget.txt"
_SCENARIO = f"two agents update a shared total (start={_START}; A adds {_A_ADDS}, B adds {_B_ADDS})"

# Snappy local spawn for a one-command demo; no idle shutdown mid-run.
_DEMO_CFG = LifecycleConfig(
    idle_shutdown_sec=0,
    sweep_interval_sec=0.1,
    port_file_retry_attempts=40,
    port_file_retry_interval_sec=0.05,
    connect_retry_attempts=20,
    connect_retry_interval_sec=0.05,
)


def run_fixed() -> dict[str, object]:
    """Sequenced read→write through CoherentVolume; B's stale write is denied,
    then B recovers via reacquire() and no update is lost.

    Returns a structured trace mirroring ``run_broken`` for side-by-side asserts.
    """
    workspace = Path(tempfile.mkdtemp(prefix="coherent_volume_fixed_"))
    target = workspace / _REL
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(str(_START))

    # Agent A spawns the coordinator (strict on the managed glob); agent B is a
    # sibling instance that attaches to the same coordinator.
    vol_a = CoherentVolume(workspace, managed=("data/**",), config=_DEMO_CFG)
    try:
        vol_b = CoherentVolume(workspace, managed=("data/**",), config=_DEMO_CFG)

        a_view = int(vol_a.read(_REL).decode())  # A reads 100 (SHARED)
        b_view = int(vol_b.read(_REL).decode())  # B reads 100 (SHARED)

        vol_a.write(_REL, str(a_view + _A_ADDS).encode())  # A commits 110 -> B INVALID

        denied = False
        denial_reason = ""
        recovered = False
        try:
            # B attempts its stale write (computed from its pre-A-commit read).
            vol_b.write(_REL, str(b_view + _B_ADDS).encode())
        except CoherenceError as exc:
            # PREVENTION: the lost update is denied, fail-closed.
            denied = True
            denial_reason = str(exc)
            # RECOVERY: reacquire (re-mint + mandatory fresh read), recompute,
            # write from the *current* value — both updates now survive.
            fresh = int(vol_b.reacquire(_REL).decode())  # reads 110
            vol_b.write(_REL, str(fresh + _B_ADDS).encode())  # commits 115
            recovered = True

        final = int(target.read_text())
        expected = _START + _A_ADDS + _B_ADDS
        return {
            "scenario": _SCENARIO,
            "expected_total": expected,  # 115
            "b_write_denied": denied,
            "denial_reason": denial_reason,
            "b_recovered_via_reacquire": recovered,
            "final_total": final,  # 115 — no update lost
            "lost_update": final != expected,
        }
    finally:
        stop_coordinator(workspace)
        shutil.rmtree(workspace, ignore_errors=True)


def main() -> int:
    trace = run_fixed()
    print("FIXED (CoherentVolume) — the stale write is denied, then recovered")
    print(f"  scenario: {trace['scenario']}")
    print(f"  B's stale write denied: {trace['b_write_denied']}  (fail-closed)")
    print(f"  B recovered via reacquire(): {trace['b_recovered_via_reacquire']}")
    print(
        f"  LOST UPDATE: {trace['lost_update']}  "
        f"(final={trace['final_total']}, expected={trace['expected_total']})"
    )
    # Exit code reflects the invariant so an agent can use this as a gate.
    return 0 if not trace["lost_update"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
