# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Side-by-side runner for the CoherentVolume stale-overwrite demo.

Runs the broken case (no coherence → agent B's stale write clobbers A's update)
and the fixed case (CoherentVolume denies B's stale write, B reacquires and
recovers → no update lost), and prints the divergence. Sequenced and offline —
no API keys, no cost; the fixed case spawns a local coordinator subprocess.

    python -m examples.coherent_volume.main
"""

from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from examples.coherent_volume import broken, fixed


def main() -> int:
    print("CoherentVolume — sequential stale-overwrite (lost-update) demo.")
    print("Same read→write sequence both times; only the coordination differs.\n")
    broken_trace = broken.run_broken()
    broken.main()
    print("")
    fixed_trace = fixed.run_fixed()
    fixed.main()

    print("\nTakeaway: without coherence, B's write from an older read silently")
    print("overwrites A (lost update). CoherentVolume denies that write fail-closed,")
    print("B reacquires the current value and rewrites — both updates survive.")

    # Exit non-zero if the demo's invariant did not hold (broken must lose, fixed
    # must prevent) so CI / a reader can trust the run rather than eyeball it.
    ok = bool(broken_trace["lost_update"]) and not bool(fixed_trace["lost_update"])
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())

# Comparing notes on multi-agent coherence?
# https://github.com/Cohexa-ai/agent-coherence/discussions
