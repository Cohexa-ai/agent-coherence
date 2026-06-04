# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""Broken case: a stale-overwrite lost update over plain files.

Two agents each update a shared total. Both read the same starting value; agent
A commits its update; then agent B commits an update it computed from the value
it read *before* A's commit. B's write silently overwrites A's — the classic
lost update (the OpenViktor cron shape: read, compute, write-back-from-the-old-
read). No coordinator is involved; that is the point. ``fixed.py`` shows the
same sequence denied and recovered through CoherentVolume.

Deterministic and offline — sequenced, not raced.
"""

from __future__ import annotations

import shutil
import sys
import tempfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

_START = 100
_A_ADDS = 10
_B_ADDS = 5
_SCENARIO = f"two agents update a shared total (start={_START}; A adds {_A_ADDS}, B adds {_B_ADDS})"


def run_broken() -> dict[str, object]:
    """Sequenced read→write with no coherence; B clobbers A.

    Returns a structured trace so the runner and tests can assert the lost update
    without parsing prose.
    """
    workspace = Path(tempfile.mkdtemp(prefix="coherent_volume_broken_"))
    target = workspace / "budget.txt"
    try:
        target.write_text(str(_START))

        a_view = int(target.read_text())  # agent A reads the total (100)
        b_view = int(target.read_text())  # agent B reads the same total (100)

        target.write_text(str(a_view + _A_ADDS))  # A commits 110
        # B commits a value computed from its PRE-A-commit read (100), not the
        # current 110 — its write silently overwrites A's update.
        target.write_text(str(b_view + _B_ADDS))  # B commits 105, clobbering A

        final = int(target.read_text())
        expected = _START + _A_ADDS + _B_ADDS
        return {
            "scenario": _SCENARIO,
            "expected_total": expected,  # 115 if both updates survived
            "a_wrote": a_view + _A_ADDS,
            "b_wrote": b_view + _B_ADDS,
            "final_total": final,  # 105 — A's +10 was lost
            "lost_update": final != expected,
        }
    finally:
        shutil.rmtree(workspace, ignore_errors=True)


def main() -> int:
    trace = run_broken()
    print("BROKEN (no coherence) — sequential stale-overwrite over plain files")
    print(f"  scenario: {trace['scenario']}")
    print(f"  A wrote {trace['a_wrote']}, then B wrote {trace['b_wrote']} (from its older read)")
    print(
        f"  LOST UPDATE: {trace['lost_update']}  "
        f"(final={trace['final_total']}, expected={trace['expected_total']})"
    )
    # Exit code reflects the invariant so an agent can use this as a gate.
    return 0 if trace["lost_update"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
