# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Fixed case: the same stale write-back, denied and recovered via CoherentVolume.

Same sequence as ``broken.py`` — A reads the memory record, B appends a fact and
writes it back, A writes an edit computed from its older snapshot — but routed
through the CoherentVolume *explicit* API. B's write-back invalidates A's view;
A's stale write is then **denied** (`StaleView`, fail-closed), so the lost update
is prevented. A recovers with ``reacquire()`` (re-mint identity + a mandatory
fresh read), re-applies its edit *intent* (the summary refinement) on top of the
current record, and writes — so both B's fact and A's summary survive.

The record is derived from ``read()``/``reacquire()`` bytes, then written back —
this is exactly the supported "write from the bytes the API returned" shape.

Sequenced, not raced: deterministic by construction. This spawns a local
coordinator subprocess (no network, no API keys) and tears it down in ``finally``.
"""

from __future__ import annotations

import json
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

from ccs.adapters.claude_code.lifecycle import LifecycleConfig, stop_coordinator  # noqa: E402  (after sys.path setup)
from ccs.adapters.coherent_volume import CoherentVolume  # noqa: E402
from ccs.core.exceptions import CoherenceError  # noqa: E402

_V1 = {"summary": "Customer prefers email contact.", "facts": ["channel: email"]}
_B_FACT = "timezone: PST"
_A_SUMMARY = "Customer prefers email contact; flagged VIP."
_REL = "memory/customer.json"
_SCENARIO = "two agents edit one shared memory record (A refines the summary, B appends a fact)"

# Snappy local spawn for a one-command demo; no idle shutdown mid-run.
_DEMO_CFG = LifecycleConfig(
    idle_shutdown_sec=0,
    sweep_interval_sec=0.1,
    port_file_retry_attempts=40,
    port_file_retry_interval_sec=0.05,
    connect_retry_attempts=20,
    connect_retry_interval_sec=0.05,
)


def _dumps(record: dict) -> bytes:
    """Stable serialization so the demo is byte-deterministic."""
    return json.dumps(record, sort_keys=True).encode()


def run_fixed() -> dict[str, object]:
    """Sequenced read→write through CoherentVolume; A's stale write is denied,
    then A recovers via reacquire() and no update is lost.

    Returns a structured trace mirroring ``run_broken`` for side-by-side asserts.
    """
    workspace = Path(tempfile.mkdtemp(prefix="rag_stale_memory_fixed_"))
    target = workspace / _REL
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(_V1, sort_keys=True))

    # Agent A spawns the coordinator (strict on the managed glob); agent B is a
    # sibling instance that attaches to the same coordinator.
    vol_a = CoherentVolume(workspace, managed=("memory/**",), config=_DEMO_CFG)
    try:
        vol_b = CoherentVolume(workspace, managed=("memory/**",), config=_DEMO_CFG)

        a_view = json.loads(vol_a.read(_REL).decode())  # A reads v1 (SHARED)

        # Agent B appends a freshly-learned fact and writes it back -> A INVALID.
        b_view = json.loads(vol_b.read(_REL).decode())
        b_view["facts"].append(_B_FACT)
        vol_b.write(_REL, _dumps(b_view))  # v2 committed

        denied = False
        denial_reason = ""
        recovered = False
        try:
            # A attempts its stale write-back: refreshed summary on the OLD facts.
            a_view["summary"] = _A_SUMMARY
            vol_a.write(_REL, _dumps(a_view))
        except CoherenceError as exc:
            # PREVENTION: the lost update is denied, fail-closed.
            denied = True
            denial_reason = str(exc)
            # RECOVERY: reacquire (re-mint + mandatory fresh read), re-apply A's
            # edit INTENT (the summary refinement) on top of the current record —
            # so B's appended fact and A's summary both survive.
            fresh = json.loads(vol_a.reacquire(_REL).decode())  # reads v2
            fresh["summary"] = _A_SUMMARY
            vol_a.write(_REL, _dumps(fresh))
            recovered = True

        final = json.loads(target.read_text())
        b_fact_survived = _B_FACT in final["facts"]
        a_edit_survived = final["summary"] == _A_SUMMARY
        return {
            "scenario": _SCENARIO,
            "b_fact": _B_FACT,
            "a_summary": _A_SUMMARY,
            "a_write_denied": denied,
            "denial_reason": denial_reason,
            "a_recovered_via_reacquire": recovered,
            "final_facts": final["facts"],
            "final_summary": final["summary"],
            "b_fact_survived": b_fact_survived,  # True — nothing erased
            "a_edit_survived": a_edit_survived,  # True — A's summary landed
            "lost_update": a_edit_survived and not b_fact_survived,
        }
    finally:
        stop_coordinator(workspace)
        shutil.rmtree(workspace, ignore_errors=True)


def main() -> int:
    trace = run_fixed()
    print("FIXED (CoherentVolume) — the stale write-back is denied, then recovered")
    print(f"  scenario: {trace['scenario']}")
    print(f"  A's stale write denied: {trace['a_write_denied']}  (fail-closed)")
    print(f"  A recovered via reacquire(): {trace['a_recovered_via_reacquire']}")
    print(f"  final summary: {trace['final_summary']!r}")
    print(f"  final facts:   {trace['final_facts']}")
    print(
        f"  LOST UPDATE: {trace['lost_update']}  "
        f"(A's edit survived={trace['a_edit_survived']}, "
        f"B's fact survived={trace['b_fact_survived']})"
    )
    # Exit code reflects the invariant so an agent can use this as a gate.
    return 0 if not trace["lost_update"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
