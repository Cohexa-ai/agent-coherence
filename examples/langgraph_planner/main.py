"""4-agent LangGraph integration demo for CCSStore.

Workload: Planner writes a shared plan once. Then Researcher, Executor, and
Reviewer each read the plan 4 times (simulating multiple passes over shared
context). Total: 1 write + 12 reads across 3 agents.

Cache behaviour with lazy strategy:
  Each downstream agent's first read is a cache miss (INVALID → SHARED).
  Reads 2-4 per agent are cache hits (SHARED state persists until next write).
  3 misses + 9 hits = 75% cache hit rate.

Token accounting:
  Cache miss: tokens_consumed = full content size (content fetched from coordinator)
  Cache hit:  tokens_consumed = 1 (no content transfer; already in local cache)

Run:
  python -m examples.langgraph_planner.main
"""
# ruff: noqa: F401

from __future__ import annotations

from typing import TypedDict

from langgraph.config import get_store as lg_get_store
from langgraph.graph import END, START, StateGraph

from ccs.adapters.ccsstore import CCSStore

# ---------------------------------------------------------------------------
# Shared artifact content  (~100 tokens to make the numbers meaningful)
# ---------------------------------------------------------------------------

PLAN_CONTENT = {
    "title": "Q2 Research Initiative",
    "objectives": [
        "Benchmark coherence protocol against baseline on 4-agent planning workload",
        "Verify MESI state transitions under concurrent write patterns",
        "Collect token consumption metrics across three strategy variants",
    ],
    "milestones": {
        "week_1": "Set up evaluation harness and baseline measurements",
        "week_2": "Run coherence protocol benchmarks and collect raw metrics",
        "week_3": "Statistical analysis and comparison report",
    },
    "owner": "planner",
    "status": "active",
}

PLAN_KEY = "plan"
PLAN_NAMESPACE_SCOPE = "shared"  # artifact scope — same for all agents

NUM_READS_PER_AGENT = 4  # reads 2-4 are cache hits
DOWNSTREAM_AGENTS = ["researcher", "executor", "reviewer"]


# ---------------------------------------------------------------------------
# Graph state
# ---------------------------------------------------------------------------

class GraphState(TypedDict):
    log: list[str]


# ---------------------------------------------------------------------------
# Node implementations
# ---------------------------------------------------------------------------

def planner_node(state: GraphState) -> dict:
    """Planner writes the shared plan artifact once."""
    store: CCSStore = lg_get_store()  # type: ignore[assignment]
    store.put(("planner", PLAN_NAMESPACE_SCOPE), PLAN_KEY, PLAN_CONTENT)
    return {"log": [*state["log"], "planner: wrote plan"]}


def _make_reader_node(agent_name: str):
    """Factory: node that reads the shared plan NUM_READS_PER_AGENT times."""

    def node(state: GraphState) -> dict:
        store: CCSStore = lg_get_store()  # type: ignore[assignment]
        for pass_num in range(1, NUM_READS_PER_AGENT + 1):
            item = store.get((agent_name, PLAN_NAMESPACE_SCOPE), PLAN_KEY)
            assert item is not None, f"{agent_name}: plan missing on pass {pass_num}"
        return {"log": [*state["log"], f"{agent_name}: read plan {NUM_READS_PER_AGENT}×"]}

    node.__name__ = f"{agent_name}_node"
    return node


# ---------------------------------------------------------------------------
# Graph construction
# ---------------------------------------------------------------------------

def build_graph(store: CCSStore) -> "CompiledStateGraph":
    builder = StateGraph(GraphState)

    builder.add_node("planner", planner_node)
    for name in DOWNSTREAM_AGENTS:
        builder.add_node(name, _make_reader_node(name))

    builder.add_edge(START, "planner")
    builder.add_edge("planner", "researcher")
    builder.add_edge("researcher", "executor")
    builder.add_edge("executor", "reviewer")
    builder.add_edge("reviewer", END)

    return builder.compile(store=store)


# ---------------------------------------------------------------------------
# CCSStore-free factory  (Tier 2 / ccs-diagnose Unit 2 substrate)
# ---------------------------------------------------------------------------
#
# The diagnose CLI is a passive observer of *any* LangGraph graph and must
# work without CCSStore being attached. The factory below produces a graph
# in the same family as ``build_graph`` (planner writes a shared plan, three
# downstream readers consume it) but uses native LangGraph state instead of
# the CCSStore-backed namespaced put/get.
#
# Workload sizing target (per Tier 2 plan, Unit 2 spike): the diagnose
# integration test needs ≥10 super-steps, ≥10 reads of a shared key, and
# ≥1 write to exercise the non-``insufficient`` verdict paths in the
# classifier (Unit 3). The structure below — planner once, then a 3-iteration
# loop over six reader nodes plus an audit + compile + finalizer — produces
# 14 super-steps with the plan key read in 13 of them. That comfortably
# clears the threshold without making the demo unrecognizable.
#
# This factory deliberately does not import ``CCSStore`` at call time so
# tests that load it can run without the coherence protocol being involved.
# ---------------------------------------------------------------------------


PLAN_CONTENT_NO_STORE: dict = {
    "title": "Q2 Research Initiative",
    "objectives": [
        "Diagnose-substrate workload reaching coverage threshold",
        "Multiple reader nodes referencing a shared 'plan' state key",
    ],
    "owner": "planner",
    "status": "active",
}

NO_STORE_DOWNSTREAM_AGENTS = (
    "researcher",
    "executor",
    "reviewer",
    "auditor",
    "compiler",
)
NO_STORE_LOOP_ITERATIONS = 2  # produces three passes through readers (initial + 2 loops)


class GraphStateNoStore(TypedDict):
    plan: dict
    log: list[str]
    iteration: int


def _planner_no_store(state: GraphStateNoStore) -> dict:
    """Single writer of the shared 'plan' state key."""
    return {
        "plan": PLAN_CONTENT_NO_STORE,
        "log": [*state["log"], "planner: wrote plan"],
    }


def _make_reader_no_store(agent_name: str):
    """Reader: consumes ``state['plan']`` and appends one log entry."""

    def node(state: GraphStateNoStore) -> dict:
        plan = state["plan"]
        # Read shape: assert the writer's contract is honoured.
        assert plan.get("owner") == "planner"
        return {
            "log": [
                *state["log"],
                f"{agent_name}: read plan ({len(plan.get('objectives', []))} objectives)",
            ]
        }

    node.__name__ = f"{agent_name}_no_store"
    return node


def _loop_controller_no_store(state: GraphStateNoStore) -> dict:
    return {
        "iteration": state["iteration"] + 1,
        "log": [*state["log"], "loop: tick"],
    }


def _should_loop_no_store(state: GraphStateNoStore) -> str:
    return "loop" if state["iteration"] < NO_STORE_LOOP_ITERATIONS else "finalizer"


def _finalizer_no_store(state: GraphStateNoStore) -> dict:
    return {
        "log": [*state["log"], f"finalizer: {len(state['log'])} entries"],
    }


def build_graph_no_store() -> "CompiledStateGraph":
    """Build a CCSStore-free graph reaching ≥10 super-steps and ≥10 reads.

    Used by the ``ccs-diagnose`` integration test as the canonical substrate;
    no MESI / coherence protocol involvement. The graph is intentionally
    simple — one writer, six readers, a small bounded loop, a finalizer.

    Returns:
        A compiled LangGraph ready to be invoked with
        ``{"plan": {}, "log": [], "iteration": 0}``.
    """
    builder = StateGraph(GraphStateNoStore)

    builder.add_node("planner", _planner_no_store)
    for name in NO_STORE_DOWNSTREAM_AGENTS:
        builder.add_node(name, _make_reader_no_store(name))
    builder.add_node("loop", _loop_controller_no_store)
    builder.add_node("finalizer", _finalizer_no_store)

    builder.add_edge(START, "planner")
    builder.add_edge("planner", "researcher")
    builder.add_edge("researcher", "executor")
    builder.add_edge("executor", "reviewer")
    builder.add_edge("reviewer", "auditor")
    builder.add_edge("auditor", "compiler")
    builder.add_edge("compiler", "loop")
    builder.add_conditional_edges(
        "loop",
        _should_loop_no_store,
        {"loop": "researcher", "finalizer": "finalizer"},
    )
    builder.add_edge("finalizer", END)

    return builder.compile()


def initial_state_no_store() -> GraphStateNoStore:
    """Return a fresh starting state for ``build_graph_no_store()``."""
    return {"plan": {}, "log": [], "iteration": 0}


# ---------------------------------------------------------------------------
# Main: run and print comparison table
# ---------------------------------------------------------------------------

def run() -> None:
    store = CCSStore(strategy="lazy", benchmark=True)
    graph = build_graph(store)

    final_state = graph.invoke({"log": []})

    print()
    print("Example: 4-agent planning pipeline")
    print()
    for entry in final_state["log"]:
        print(f"  {entry}")
    print()
    store.print_benchmark_summary()


if __name__ == "__main__":
    run()

# Comparing notes on multi-agent coherence?
# https://github.com/hipvlady/agent-coherence/discussions
