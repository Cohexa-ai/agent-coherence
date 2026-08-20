"""4-agent research pipeline example for CCSStore.

Demonstrates independent per-artifact MESI state: a write to 'findings'
does NOT invalidate 'brief' held by other agents, because each artifact
key has its own MESI state per agent.

Graph: researcher → analyst → writer  (brief pre-populated by setup)

Key behaviours illustrated:
  1. researcher reads brief 3× (1 miss + 2 hits), then writes findings.
     Writing findings does NOT alter the MESI state of brief for any agent.
  2. analyst reads brief 3× — first read is a miss (analyst has never seen it),
     reads 2-3 are hits. Then reads findings 3× (same pattern). The brief's
     SHARED state is unaffected by the findings write in step 1.
  3. writer reads brief, findings, and analysis 2× each (1 miss + 1 hit per
     artifact). All three artifacts progress independently through INVALID →
     SHARED without interfering with one another.

Token accounting:
  Cache hit:  tokens_consumed = 1  (content already in local cache)
  Cache miss: tokens_consumed = full content size

Run:
  python -m examples.research_pipeline.main
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

BRIEF = {
    "topic": "Cache coherence protocols for multi-agent LLM systems",
    "scope": "Survey MESI, MSI, and MOESI protocols; evaluate token savings; measure hit rates.",
    "deadline": "2026-05-01",
    "owner": "research_lead",
}

SCOPE = "shared"
BRIEF_KEY = "brief"
FINDINGS_KEY = "findings"
ANALYSIS_KEY = "analysis"

CODE_READ_PASSES = 3   # multi-pass reads per agent to accumulate hits
WRITER_READ_PASSES = 2


# ---------------------------------------------------------------------------
# Graph state
# ---------------------------------------------------------------------------

class GraphState(TypedDict):
    log: list[str]


# ---------------------------------------------------------------------------
# Node implementations
# ---------------------------------------------------------------------------

def researcher_node(state: GraphState) -> dict:
    """Reads brief 3× (1 miss + 2 hits), writes findings."""
    store: CCSStore = lg_get_store()  # type: ignore[assignment]
    for _ in range(CODE_READ_PASSES):
        item = store.get(("researcher", SCOPE), BRIEF_KEY)
        assert item is not None, "brief missing"
    store.put(
        ("researcher", SCOPE),
        FINDINGS_KEY,
        {
            "summary": "MESI outperforms MSI by 40% on write-heavy workloads",
            "hit_rate": 0.75,
            "token_reduction": 0.69,
            "n_experiments": 12,
        },
    )
    return {"log": [*state["log"], "researcher: read brief×3 → wrote findings"]}


def analyst_node(state: GraphState) -> dict:
    """Reads brief 3× and findings 3× (each: 1 miss + 2 hits), writes analysis.

    researcher's write to 'findings' did NOT invalidate 'brief' — analyst
    reads brief fresh from INVALID (first access) but hits on passes 2-3.
    Both artifacts progress through SHARED state independently.
    """
    store: CCSStore = lg_get_store()  # type: ignore[assignment]
    for _ in range(CODE_READ_PASSES):
        item = store.get(("analyst", SCOPE), BRIEF_KEY)
        assert item is not None, "brief missing"
    for _ in range(CODE_READ_PASSES):
        item = store.get(("analyst", SCOPE), FINDINGS_KEY)
        assert item is not None, "findings missing"
    store.put(
        ("analyst", SCOPE),
        ANALYSIS_KEY,
        {
            "verdict": "MESI is preferred for multi-agent coherence",
            "key_insight": "Per-artifact SHARED state prevents cross-key invalidation cascade",
            "recommendation": "adopt_mesi",
        },
    )
    return {"log": [*state["log"], "analyst: read brief×3 + findings×3 → wrote analysis"]}


def writer_node(state: GraphState) -> dict:
    """Reads brief, findings, and analysis 2× each (1 miss + 1 hit per key), writes report."""
    store: CCSStore = lg_get_store()  # type: ignore[assignment]
    for _ in range(WRITER_READ_PASSES):
        item = store.get(("writer", SCOPE), BRIEF_KEY)
        assert item is not None, "brief missing"
    for _ in range(WRITER_READ_PASSES):
        item = store.get(("writer", SCOPE), FINDINGS_KEY)
        assert item is not None, "findings missing"
    for _ in range(WRITER_READ_PASSES):
        item = store.get(("writer", SCOPE), ANALYSIS_KEY)
        assert item is not None, "analysis missing"
    store.put(
        ("writer", SCOPE),
        "report",
        {
            "title": "Cache Coherence for Multi-Agent LLMs: Findings Report",
            "status": "final",
        },
    )
    return {"log": [*state["log"], "writer: read brief×2 + findings×2 + analysis×2 → wrote report"]}


# ---------------------------------------------------------------------------
# Graph construction
# ---------------------------------------------------------------------------

def build_graph(store: CCSStore) -> "CompiledStateGraph":
    builder = StateGraph(GraphState)
    builder.add_node("researcher", researcher_node)
    builder.add_node("analyst", analyst_node)
    builder.add_node("writer", writer_node)
    builder.add_edge(START, "researcher")
    builder.add_edge("researcher", "analyst")
    builder.add_edge("analyst", "writer")
    builder.add_edge("writer", END)
    return builder.compile(store=store)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run() -> None:
    events: list[StoreMetricEvent] = []
    store = CCSStore(strategy="lazy", on_metric=events.append)
    # Pre-populate the brief before the pipeline starts
    store.put(("setup", SCOPE), BRIEF_KEY, BRIEF)
    # Remove setup event — infrastructure, not part of pipeline
    events[:] = [e for e in events if e.agent_name != "setup"]

    graph = build_graph(store)
    final_state = graph.invoke({"log": []})

    get_events = [e for e in events if e.operation == "get"]
    put_events = [e for e in events if e.operation == "put"]
    hits = [e for e in get_events if e.cache_hit]

    content_tokens = max(1, len(json.dumps(BRIEF, separators=(",", ":"))) // 4)
    baseline_tokens = (len(put_events) + len(get_events)) * content_tokens
    ccs_tokens = sum(e.tokens_consumed for e in events if e.operation in ("put", "get"))
    reduction_pct = (baseline_tokens - ccs_tokens) / baseline_tokens * 100 if baseline_tokens else 0

    print()
    print("Example: 4-agent research pipeline")
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
