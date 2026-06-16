# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Executor sub-agent (scripted stand-in).

Reads the shared task-spec artifact at the start of its turn (caching it
in graph state — this is what makes the cache SHARED on the agent-coherence
side). Then performs the rename against the TypeScript fixture.

Two commit variants:

* ``executor_commit_node`` — the canonical executor: re-reads the spec from
  ``CCSStore`` at commit time. With write-side coherence, the planner's v2
  write has invalidated the executor's cache; the re-read fetches v2 and the
  rename includes all four callers. With invalidation suppressed (the
  no-invalidation demo variant), the re-read returns the still-SHARED v1
  from cache and the executor misses the fourth caller.
* ``executor_commit_node_no_refresh`` — the simulated agent-context-window
  variant (Unit 3). The executor commits using the v1 it captured at read
  time and does not consult the store again. This mirrors how real LLM
  sub-agents naturally behave — context windows ARE caches without
  invalidation by default. Without coherence, the executor never knows v2
  exists.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from langgraph.config import get_store as lg_get_store

from ccs.adapters.ccsstore import CCSStore

from .planner import PLANNER_AGENT, SHARED_SCOPE, TASK_KEY

EXECUTOR_AGENT = "executor"


def _apply_rename(spec: dict[str, Any], fixture_root: Path) -> list[str]:
    """Apply the rename to each caller listed in spec; return the touched files.

    Also renames the symbol's definition in src/auth.ts so the demo produces
    a real ``tsc`` failure when the spec is missing the fourth caller.
    """
    old = spec["rename_from"]
    new = spec["rename_to"]
    touched: list[str] = []

    # Rename the definition in auth.ts (the export and the function name).
    auth_path = fixture_root / "src" / "auth.ts"
    auth_text = auth_path.read_text(encoding="utf-8")
    auth_text = auth_text.replace(f"function {old}(", f"function {new}(")
    auth_path.write_text(auth_text, encoding="utf-8")
    touched.append("src/auth.ts")

    # Rename call sites in each listed caller. The bare ``str.replace`` is
    # safe here because the demo's fixture contains no compound identifiers
    # like ``validateUserRole`` or string literals embedding the symbol name.
    # Do not lift this function into a generic refactor tool without an
    # ast-aware rename; see plan Risks for the credibility note.
    for rel in spec["callers"]:
        caller_path = fixture_root / rel
        text = caller_path.read_text(encoding="utf-8")
        # Update both the import and the call site.
        text = text.replace(old, new)
        caller_path.write_text(text, encoding="utf-8")
        touched.append(rel)

    return touched


def executor_read_node(state: dict) -> dict:
    """Read the spec from the store. Caches the result in graph state.

    This is the read that produces the SHARED MESI state on the executor's
    side. The cached value carries forward to the commit node — that carry
    is what the simulated-cache (Unit 3) variant relies on to demonstrate
    stale-read failure without re-reading.
    """
    store: CCSStore = lg_get_store()  # type: ignore[assignment]
    item = store.get((EXECUTOR_AGENT, SHARED_SCOPE), TASK_KEY)
    spec = item.value if item is not None else None
    if spec is None:
        # Defensive: in the demo's canonical graph the planner runs before the
        # executor, so this branch is unreachable. Future graph reorders or
        # standalone unit tests of this node should still produce a coherent
        # log line rather than a KeyError on ``spec['version']``.
        return {
            "log": [*state.get("log", []), "executor: read returned no spec"],
            "cached_spec": None,
        }
    return {
        "log": [*state.get("log", []), f"executor: read spec v{spec['version']} (cached)"],
        "cached_spec": spec,
    }


def executor_commit_node(state: dict) -> dict:
    """Re-read the spec, then commit the rename against the fixture.

    With write-side coherence, the planner's v2 write between read and
    commit invalidates the cache; this re-read fetches v2. With invalidation
    suppressed, this re-read sees the still-SHARED v1 from local cache.
    """
    store: CCSStore = lg_get_store()  # type: ignore[assignment]
    item = store.get((EXECUTOR_AGENT, SHARED_SCOPE), TASK_KEY)
    spec = item.value if item is not None else None
    if spec is None:
        return {"log": [*state.get("log", []), "executor: no spec, no commit"]}

    fixture_root = Path(state["fixture_root"])
    touched = _apply_rename(spec, fixture_root)
    return {
        "log": [
            *state.get("log", []),
            f"executor: committed spec v{spec['version']} ({len(touched)} files renamed)",
        ],
        "committed_spec_version": spec["version"],
        "committed_files": touched,
    }


def executor_commit_node_no_refresh(state: dict) -> dict:
    """Commit using the cached spec from read time. Never re-reads.

    Simulates an LLM agent's context window: the executor "remembers" the
    spec it saw at the start of its turn and acts on that memory. Without
    a coherence protocol to update its memory, the executor never sees v2.
    """
    spec = state.get("cached_spec")
    if spec is None:
        return {"log": [*state.get("log", []), "executor: no cached spec, no commit"]}

    fixture_root = Path(state["fixture_root"])
    touched = _apply_rename(spec, fixture_root)
    return {
        "log": [
            *state.get("log", []),
            f"executor (context-cache): committed cached spec v{spec['version']} "
            f"({len(touched)} files renamed)",
        ],
        "committed_spec_version": spec["version"],
        "committed_files": touched,
    }
