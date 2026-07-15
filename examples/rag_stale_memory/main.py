# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Side-by-side runner for the RAG stale-memory write-back demo.

Runs the broken case (no coherence → agent A's stale write-back clobbers the
fact agent B appended) and the fixed case (CoherentVolume denies A's stale write,
A reacquires and recovers → no update lost), and prints the divergence. Sequenced
and offline — no API keys, no cost; the fixed case spawns a local coordinator
subprocess.

    python -m examples.rag_stale_memory.main
"""

from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from examples.rag_stale_memory import broken, fixed  # noqa: E402  (after sys.path setup)


def main() -> int:
    print("RAG stale-memory write-back — a moved memory record clobbered by a stale edit.")
    print("Same read→edit→write-back sequence both times; only the coordination differs.\n")
    broken_trace = broken.run_broken()
    broken.main()
    print("")
    fixed_trace = fixed.run_fixed()
    fixed.main()

    print("\nTakeaway: without coherence, A's edit from an older snapshot silently")
    print("erases the fact B appended (lost update). CoherentVolume denies that")
    print("write-back fail-closed; A reacquires the current record and re-applies its")
    print("edit — both B's fact and A's summary survive.")

    # Exit non-zero unless the invariant held both ways (broken must lose, fixed
    # must prevent) so CI / a reader can trust the run rather than eyeball it.
    ok = bool(broken_trace["lost_update"]) and not bool(fixed_trace["lost_update"])
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())

# Comparing notes on multi-agent coherence?
# https://github.com/Cohexa-ai/agent-coherence/discussions
