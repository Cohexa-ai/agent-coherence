"""3-agent code review example for CCSStore.

Demonstrates SHARED state across two reviewer agents and coherence invalidation
when the synthesizer writes the final review.

Graph: reviewer_a → reviewer_b → synthesizer

Key behaviours illustrated:
  1. reviewer_a reads the shared codebase (INVALID → SHARED, cache miss).
  2. reviewer_b reads the same codebase artifact (INVALID → SHARED, cache miss on first
     access — but the codebase is NOT modified between the two reviewer nodes, so
     reviewer_b's read is a miss only because it has never seen this artifact before;
     it transitions directly to SHARED without needing an exclusive fetch).
  3. synthesizer reads codebase (cache miss — first access by synthesizer) plus both
     reviews (both misses, since synthesizer has never read them before).
  4. synthesizer's write to 'final_review' invalidates no cached artifact because
     no other agent holds that key — it's a new artifact.

Token accounting:
  Cache hit:  tokens_consumed = 1  (content already in local cache)
  Cache miss: tokens_consumed = full content size

Run:
  python -m examples.code_review.main
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, TypedDict

from langgraph.config import get_store as lg_get_store
from langgraph.graph import END, START, StateGraph

from ccs.adapters.ccsstore import CCSStore, StoreMetricEvent

if TYPE_CHECKING:  # pragma: no cover - typing only
    from langgraph.graph.state import CompiledStateGraph

# ---------------------------------------------------------------------------
# Shared artifact content
# ---------------------------------------------------------------------------

CODEBASE = {
    "file": "coordinator/service.py",
    "summary": (
        "CoordinatorService manages the control plane: fetch, write, commit, invalidate. "
        "It maintains a per-artifact version map and per-agent MESI state registry. "
        "All state transitions are validated before application to prevent protocol violations."
    ),
    "lines": 342,
    "language": "Python",
}

SCOPE = "shared"
CODE_KEY = "codebase"
REVIEW_A_KEY = "review_a"
REVIEW_B_KEY = "review_b"
FINAL_KEY = "final_review"


# ---------------------------------------------------------------------------
# Graph state
# ---------------------------------------------------------------------------

class GraphState(TypedDict):
    log: list[str]


# ---------------------------------------------------------------------------
# Node implementations
# ---------------------------------------------------------------------------

def reviewer_a_node(state: GraphState) -> dict:
    """Reads codebase (miss), writes review_a."""
    store: CCSStore = lg_get_store()  # type: ignore[assignment]
    code = store.get(("reviewer_a", SCOPE), CODE_KEY)
    assert code is not None, "codebase missing"
    store.put(
        ("reviewer_a", SCOPE),
        REVIEW_A_KEY,
        {"reviewer": "reviewer_a", "verdict": "LGTM with minor nits", "score": 8},
    )
    return {"log": [*state["log"], "reviewer_a: read codebase → wrote review_a"]}


def reviewer_b_node(state: GraphState) -> dict:
    """Reads codebase (miss — first access by reviewer_b), writes review_b.

    reviewer_b never held the codebase before, so this is a cache miss even
    though reviewer_a already has it in SHARED state. The codebase artifact was
    NOT modified since reviewer_a's read, so no invalidation occurred.
    """
    store: CCSStore = lg_get_store()  # type: ignore[assignment]
    code = store.get(("reviewer_b", SCOPE), CODE_KEY)
    assert code is not None, "codebase missing"
    store.put(
        ("reviewer_b", SCOPE),
        REVIEW_B_KEY,
        {"reviewer": "reviewer_b", "verdict": "needs refactor in fetch path", "score": 6},
    )
    return {"log": [*state["log"], "reviewer_b: read codebase → wrote review_b"]}


def synthesizer_node(state: GraphState) -> dict:
    """Reads codebase + both reviews (all misses), writes final review."""
    store: CCSStore = lg_get_store()  # type: ignore[assignment]
    code = store.get(("synthesizer", SCOPE), CODE_KEY)
    review_a = store.get(("synthesizer", SCOPE), REVIEW_A_KEY)
    review_b = store.get(("synthesizer", SCOPE), REVIEW_B_KEY)
    assert all(x is not None for x in [code, review_a, review_b])
    store.put(
        ("synthesizer", SCOPE),
        FINAL_KEY,
        {
            "summary": "mixed: reviewer_a approves, reviewer_b requests refactor",
            "decision": "request-changes",
            "avg_score": 7,
        },
    )
    return {"log": [*state["log"], "synthesizer: read code+reviews → wrote final"]}


# ---------------------------------------------------------------------------
# Graph construction
# ---------------------------------------------------------------------------

def build_graph(store: CCSStore) -> "CompiledStateGraph":
    builder = StateGraph(GraphState)
    builder.add_node("reviewer_a", reviewer_a_node)
    builder.add_node("reviewer_b", reviewer_b_node)
    builder.add_node("synthesizer", synthesizer_node)
    builder.add_edge(START, "reviewer_a")
    builder.add_edge("reviewer_a", "reviewer_b")
    builder.add_edge("reviewer_b", "synthesizer")
    builder.add_edge("synthesizer", END)
    return builder.compile(store=store)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run() -> None:
    events: list[StoreMetricEvent] = []
    store = CCSStore(strategy="lazy", on_metric=events.append)
    # Pre-populate the codebase artifact before the review graph starts
    store.put(("setup", SCOPE), CODE_KEY, CODEBASE)
    # Remove the setup put event — it's infrastructure, not part of the review flow
    events[:] = [e for e in events if e.agent_name != "setup"]

    graph = build_graph(store)
    final_state = graph.invoke({"log": []})

    get_events = [e for e in events if e.operation == "get"]
    put_events = [e for e in events if e.operation == "put"]
    hits = [e for e in get_events if e.cache_hit]

    content_tokens = max(1, len(json.dumps(CODEBASE, separators=(",", ":"))) // 4)
    baseline_tokens = (len(put_events) + len(get_events)) * content_tokens
    ccs_tokens = sum(e.tokens_consumed for e in events if e.operation in ("put", "get"))
    reduction_pct = (baseline_tokens - ccs_tokens) / baseline_tokens * 100 if baseline_tokens else 0

    print()
    print("Example: 3-agent code review pipeline")
    print()
    print(f"  Content: ~{content_tokens} tokens per artifact")
    print()
    print(f"  Baseline (no cache): {baseline_tokens:>6} tokens")
    print(f"  CCSStore lazy:       {ccs_tokens:>6} tokens")
    print(f"  Token reduction:     {reduction_pct:.0f}%")
    print()
    print(f"  Cache hits:   {len(hits)} / {len(get_events)} reads")
    print()
    print("  Per-operation detail:")
    for e in events:
        hit_str = " [HIT]" if (e.operation == "get" and e.cache_hit) else ""
        print(f"    [{e.agent_name:12}] {e.operation:6}  {e.key:12}  {e.tokens_consumed:3} tokens{hit_str}")
    print()
    print("  Graph execution log:")
    for entry in final_state["log"]:
        print(f"    {entry}")


if __name__ == "__main__":
    run()
