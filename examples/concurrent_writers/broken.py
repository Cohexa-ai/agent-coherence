# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""Broken case: two CONCURRENT writers, one update silently lost.

Unlike ``examples/coherent_volume`` (a *sequential* stale-overwrite), this is a
true race: two threads read the same shared total at the same value, then both
write back a value computed from that read. With no coordination, the second
write to land silently overwrites the first — the classic concurrent lost
update. ``fixed.py`` runs the identical race through ``CoherentVolume.write_cas``,
where the loser is told it lost and re-applies, so both updates survive.

A read-barrier makes the *outcome class* deterministic — both threads read the
start value before either writes, so exactly one delta survives — while a
write-lock keeps the file from tearing. The lost update is therefore a logical
consequence of the shared stale read, not a corrupted file. Offline; no
coordinator (that is the point).
"""

from __future__ import annotations

import shutil
import sys
import tempfile
import threading
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

_START = 100
_A_ADDS = 10
_B_ADDS = 5
_SCENARIO = (
    f"two agents CONCURRENTLY update a shared total "
    f"(start={_START}; A adds {_A_ADDS}, B adds {_B_ADDS})"
)


def run_broken() -> dict[str, object]:
    """Two threads race a read→write with no coherence; one update is lost.

    Returns a structured trace so the runner and tests assert the lost update
    without parsing prose.
    """
    workspace = Path(tempfile.mkdtemp(prefix="concurrent_writers_broken_"))
    target = workspace / "tally.txt"
    try:
        target.write_text(str(_START))

        both_have_read = threading.Barrier(2)  # force both reads before either write
        write_lock = threading.Lock()  # keep the physical write from tearing

        def writer(delta: int) -> None:
            view = int(target.read_text().strip())  # both read the same start value...
            both_have_read.wait()  # ...before either writes
            with write_lock:
                # Blind write from the stale read — no check that the value moved.
                target.write_text(str(view + delta))

        threads = [
            threading.Thread(target=writer, args=(_A_ADDS,)),
            threading.Thread(target=writer, args=(_B_ADDS,)),
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        final = int(target.read_text().strip())
        expected = _START + _A_ADDS + _B_ADDS
        return {
            "scenario": _SCENARIO,
            "expected_total": expected,  # 115 if both updates survived
            "final_total": final,  # 110 or 105 — exactly one delta lost
            "lost_update": final != expected,
        }
    finally:
        shutil.rmtree(workspace, ignore_errors=True)


def main() -> int:
    trace = run_broken()
    print("BROKEN (no coherence) — two concurrent writers over a plain file")
    print(f"  scenario: {trace['scenario']}")
    print(
        f"  LOST UPDATE: {trace['lost_update']}  "
        f"(final={trace['final_total']}, expected={trace['expected_total']})"
    )
    # Exit code reflects the invariant so an agent can use this as a gate.
    return 0 if trace["lost_update"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
