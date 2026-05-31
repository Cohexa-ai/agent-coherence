# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""Fixed case: ``CoherenceAdapterCore`` invalidates the stale client cache.

Same scenario as ``broken.py`` (two agents, one shared conversation, B caches
locally), but B reads and A writes *through* the coordinator. When A commits its
revision, the in-process event bus invalidates B's cached entry, so B's next
read is a cache miss that fetches the fresh version — B never acts on stale
state.

Uses the real ``CoherenceAdapterCore`` (~30 LOC of glue, no LangGraph, no
OpenAI-adapter dependency) so the demo exercises the actual library. The
coherence machinery is server-consistency-independent: it works whether the
underlying store is the Conversations API, a Session cache, or any shared
artifact.
"""

from __future__ import annotations

from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from ccs.adapters.base import CoherenceAdapterCore
from ccs.core.states import MESIState


def run_fixed() -> dict[str, object]:
    """Two agents on one coherence-tracked artifact; B's cache is invalidated.

    Hardcoded ticks (t=1..3) force the ordering deterministically — no races.
    """
    core = CoherenceAdapterCore(strategy_name="lazy")
    core.register_agent("agent_a")
    core.register_agent("agent_b")
    artifact = core.register_artifact(name="conversation:plan", content="plan_version=1")

    # B reads at t=1 — caches the artifact via the coordinator.
    first = core.read(agent_name="agent_b", artifact_id=artifact.id, now_tick=1)

    # A writes the revision at t=2 — fires invalidation to peer B's cache.
    core.write(agent_name="agent_a", artifact_id=artifact.id, content="plan_version=2", now_tick=2)
    b_entry = core.runtime("agent_b").cache.get(artifact.id)
    b_invalidated = b_entry is not None and b_entry.state == MESIState.INVALID

    # B reads again at t=3 — cache miss → fetches the fresh version.
    second = core.read(agent_name="agent_b", artifact_id=artifact.id, now_tick=3)

    return {
        "b_first_read": first.content,
        "b_invalidated": b_invalidated,
        "b_second_read": second.content,
        "fresh": second.content == "plan_version=2",
    }


def main() -> int:
    trace = run_fixed()
    print("FIXED (CoherenceAdapterCore) — cache invalidated on peer write")
    print(f"  agent B first read:  {trace['b_first_read']}")
    print(f"  agent A wrote v2 → B's cache invalidated: {trace['b_invalidated']}")
    print(f"  agent B second read: {trace['b_second_read']}")
    print(f"  FRESH: {trace['fresh']}  (B re-fetched after the write, no stale action)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
