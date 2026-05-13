# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""Planner sub-agent (scripted stand-in).

Writes a task-spec artifact in two versions. v1 lists three callers of
``validateUser``. v2 adds the fourth caller the planner "discovered"
mid-flight. The point of the demo is what happens to the executor's view
of this artifact between the two writes.

The two ``planner_v*_node`` functions are nodes in the demo's
``StateGraph``. They access the shared ``CCSStore`` via
``langgraph.config.get_store`` inside the node body — the canonical
LangGraph idiom (see ``examples/langgraph_planner/main.py``).
"""

from __future__ import annotations

from typing import Any

from langgraph.config import get_store as lg_get_store

from ccs.adapters.ccsstore import CCSStore

PLANNER_AGENT = "planner"
SHARED_SCOPE = "shared"
TASK_KEY = "task"

# v1 task spec — three of the four callers in fixture_repo_ts.
TASK_SPEC_V1: dict[str, Any] = {
    "version": 1,
    "rename_from": "validateUser",
    "rename_to": "authenticateUser",
    "callers": [
        "src/middleware.ts",
        "src/handlers/login.ts",
        "src/handlers/refresh.ts",
    ],
}

# v2 task spec — the planner "discovers" the fourth caller mid-flight.
TASK_SPEC_V2: dict[str, Any] = {
    "version": 2,
    "rename_from": "validateUser",
    "rename_to": "authenticateUser",
    "callers": [
        "src/middleware.ts",
        "src/handlers/login.ts",
        "src/handlers/refresh.ts",
        "src/utils/session.ts",
    ],
}


def planner_v1_node(state: dict) -> dict:
    """Write the v1 task spec to the shared artifact."""
    store: CCSStore = lg_get_store()  # type: ignore[assignment]
    store.put((PLANNER_AGENT, SHARED_SCOPE), TASK_KEY, TASK_SPEC_V1)
    return {"log": [*state.get("log", []), "planner: wrote spec v1 (3 callers)"]}


def planner_v2_node(state: dict) -> dict:
    """Write the v2 task spec — planner re-plans mid-flight with the 4th caller."""
    store: CCSStore = lg_get_store()  # type: ignore[assignment]
    store.put((PLANNER_AGENT, SHARED_SCOPE), TASK_KEY, TASK_SPEC_V2)
    return {"log": [*state.get("log", []), "planner: wrote spec v2 (4 callers)"]}
