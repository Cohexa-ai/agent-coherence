# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Side-by-side runner for the concurrent lost-update demo (Epic Piece #6).

Runs the broken case (two racing writers, no coherence → one update silently
lost) and the fixed case (the same race through CoherentVolume.write_cas → the
loser is told it lost and re-applies → both updates survive), and prints the
divergence. The broken case is offline; the fixed case spawns a local
coordinator subprocess (no API keys, no network).

    python -m examples.concurrent_writers.main
"""

from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from examples.concurrent_writers import broken, fixed


def main() -> int:
    print("Concurrent lost-update demo (commit-CAS, v0.9.1).")
    print("Same concurrent read→write race both times; only the coordination differs.\n")
    broken_trace = broken.run_broken()
    broken.main()
    print("")
    fixed_trace = fixed.run_fixed()
    fixed.main()

    print("\nTakeaway: under a true race, blind writes silently drop an update")
    print("(last writer wins). write_cas elects one winner and tells the loser it")
    print("lost — it reacquires the current value and re-applies, so both survive.")
    print("Sequential coherence (examples/coherent_volume) can't catch this race;")
    print("commit-CAS can. Single-host scope — cross-host is the demand-gated next rung.")

    # Exit non-zero if the demo's invariant did not hold (broken must lose, fixed
    # must preserve) so CI / a reader can trust the run rather than eyeball it.
    ok = bool(broken_trace["lost_update"]) and not bool(fixed_trace["lost_update"])
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())

# Comparing notes on multi-agent coherence?
# https://github.com/Cohexa-ai/agent-coherence/discussions
