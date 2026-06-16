# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Broken case: a client-side cache goes stale over a *consistent* shared store.

Framing (post-Q6, 2026-05-31). The Q6 probe found the OpenAI and Mistral
Conversations APIs read-after-write consistent across clients — they commit a
write before returning the ACK, so a separate client's read after that ACK sees
it. The stale-read bug therefore does **not** live in the server. It lives in
the *client*: agents cache conversation state locally to avoid re-fetching (and
re-paying for) the whole history, and that local cache goes stale the moment a
peer writes.

This module reproduces exactly that, deterministically and offline. The shared
store is a plain dict standing in for the consistent Conversations API; agent B
keeps a local cache and acts on it after agent A has written. No coordinator is
involved — that is the point. ``fixed.py`` shows the same trace with
``CoherenceAdapterCore`` invalidating B's cache.
"""

from __future__ import annotations

from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


def run_broken() -> dict[str, object]:
    """Two agents on one conversation; B acts from a stale local cache.

    Returns a structured trace so the runner and tests can assert the divergence
    without parsing prose.
    """
    # Consistent shared conversation store (stands in for the Conversations API,
    # which Q6 verified is read-after-write consistent).
    conversation = {"plan_version": 1}

    # Agent B reads the conversation once and caches it locally to save tokens.
    b_local_cache = dict(conversation)

    # Agent A appends a revision. The store is consistent — it now holds v2.
    conversation["plan_version"] = 2

    # Agent B acts on its next turn — but reads from its LOCAL CACHE, not the
    # store, so it decides on the stale v1 while the conversation already holds v2.
    b_decision_version = b_local_cache["plan_version"]

    return {
        "store_version": conversation["plan_version"],
        "b_decision_version": b_decision_version,
        "b_decision": f"execute plan v{b_decision_version}",
        "stale": b_decision_version != conversation["plan_version"],
    }


def main() -> int:
    trace = run_broken()
    print("BROKEN (no coherence) — client cache vs consistent store")
    print(f"  conversation store is at v{trace['store_version']}")
    print(f"  agent B decided: {trace['b_decision']}")
    print(f"  STALE: {trace['stale']}  (B acted on v{trace['b_decision_version']} "
          f"while the store held v{trace['store_version']})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
