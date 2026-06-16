# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Side-by-side runner for the client-cache stale-read demo.

Runs the broken case (no coherence → agent B acts on a stale local cache) and
the fixed case (CoherenceAdapterCore invalidates the cache → B re-fetches), and
prints the divergence. Deterministic and offline — no API keys, no cost.

    python -m examples.conversations_stale_read.main
"""

from __future__ import annotations

from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from examples.conversations_stale_read import broken, fixed


def main() -> int:
    print("Conversations stale-read demo — the bug is the client cache, not the server.")
    print("(Q6 verified the OpenAI/Mistral Conversations APIs are read-after-write")
    print(" consistent; staleness lives in the agent's local cache.)\n")
    broken.main()
    print("")
    fixed.main()
    print("\nTakeaway: the server was consistent in both runs. Coherence is about")
    print("the readers' caches — surface the staleness before the agent acts on it.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

# Comparing notes on multi-agent coherence?
# https://github.com/hipvlady/agent-coherence/discussions
