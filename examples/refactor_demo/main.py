# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""Planner-executor refactor demo for write-side coherence.

Four-node ``StateGraph``:

    planner_v1 -> executor_read -> planner_v2 -> executor_commit

Three variants:

* ``with`` (default) — write-side coherence on. The planner's v2 write
  invalidates the executor's cached read; the executor's commit-time
  re-read fetches v2 and renames all four caller files. ``tsc`` passes.
* ``no-invalidation`` — same graph, same store, peer invalidation
  suppressed via ``strategies.disable_invalidation``. The executor's
  commit-time re-read returns the still-SHARED v1; the four-caller rename
  misses ``src/utils/session.ts``. ``tsc`` fails with TS2305.
* ``context-cache`` — simulated LLM context-window cache (Unit 3 of the
  plan). The executor commits from the spec it captured at read time and
  never re-consults the store. Without write-side coherence to refresh
  the cached value, the executor uses v1 and misses the fourth caller.

The fixture under ``examples/refactor_demo/fixture_repo_ts`` is copied to
a temp working directory before the graph runs, so the source tree is
never mutated. ``npx tsc --noEmit`` runs against the temp copy after the
graph completes.

Run
---
    python -m examples.refactor_demo.main                       # --variant=with
    python -m examples.refactor_demo.main --variant=no-invalidation
    python -m examples.refactor_demo.main --variant=context-cache
"""

from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any, TypedDict

from langgraph.graph import END, START, StateGraph

from ccs.adapters.ccsstore import CCSStore, StoreMetricEvent

from .executor import (
    executor_commit_node,
    executor_commit_node_no_refresh,
    executor_read_node,
)
from .planner import planner_v1_node, planner_v2_node
from .strategies import disable_invalidation


class GraphState(TypedDict, total=False):
    log: list[str]
    fixture_root: str
    cached_spec: dict[str, Any] | None
    committed_spec_version: int
    committed_files: list[str]

FIXTURE_REPO_TS = Path(__file__).parent / "fixture_repo_ts"


# ---------------------------------------------------------------------------
# Graph construction
# ---------------------------------------------------------------------------


def build_graph(store: CCSStore) -> Any:
    """Standard four-node graph with refresh-on-commit at the executor."""
    builder = StateGraph(GraphState)
    builder.add_node("planner_v1", planner_v1_node)
    builder.add_node("executor_read", executor_read_node)
    builder.add_node("planner_v2", planner_v2_node)
    builder.add_node("executor_commit", executor_commit_node)

    builder.add_edge(START, "planner_v1")
    builder.add_edge("planner_v1", "executor_read")
    builder.add_edge("executor_read", "planner_v2")
    builder.add_edge("planner_v2", "executor_commit")
    builder.add_edge("executor_commit", END)

    return builder.compile(store=store)


def build_graph_context_cache(store: CCSStore) -> Any:
    """Four-node graph where the executor commits from cached state, not from re-read.

    Simulates an LLM agent context window. Used by ``--variant=context-cache``.
    """
    builder = StateGraph(GraphState)
    builder.add_node("planner_v1", planner_v1_node)
    builder.add_node("executor_read", executor_read_node)
    builder.add_node("planner_v2", planner_v2_node)
    builder.add_node("executor_commit", executor_commit_node_no_refresh)

    builder.add_edge(START, "planner_v1")
    builder.add_edge("planner_v1", "executor_read")
    builder.add_edge("executor_read", "planner_v2")
    builder.add_edge("planner_v2", "executor_commit")
    builder.add_edge("executor_commit", END)

    return builder.compile(store=store)


# ---------------------------------------------------------------------------
# Inline event printing (Unit 3)
# ---------------------------------------------------------------------------


def format_event(event: StoreMetricEvent) -> None:
    """Print a single store event to stdout, one line per event.

    Format kept boring on purpose: terminal-readable, no color codes.
    The recording setup (Unit 4) decides on color and width.
    """
    marker = "HIT " if event.cache_hit else "MISS"
    print(
        f"[t+{event.tick:>3}] [{marker}] op={event.operation:<5} "
        f"agent={event.agent_name:<8} key={event.key}",
        flush=True,
    )


# ---------------------------------------------------------------------------
# Fixture handling
# ---------------------------------------------------------------------------


def setup_temp_fixture() -> Path:
    """Copy the TS fixture to a tmp working dir so the source tree is never mutated.

    Cleans up the tmp dir on any failure during setup, so we don't leak partial
    copies if shutil.copytree or the symlink call raises.
    """
    work_dir = Path(tempfile.mkdtemp(prefix="refactor_demo_"))
    dest = work_dir / "fixture_repo_ts"
    try:
        # Skip node_modules in the copy; symlink it after for tsc invocation.
        shutil.copytree(
            FIXTURE_REPO_TS,
            dest,
            ignore=shutil.ignore_patterns("node_modules", "dist", "*.tsbuildinfo"),
        )
        src_modules = FIXTURE_REPO_TS / "node_modules"
        if src_modules.exists():
            (dest / "node_modules").symlink_to(src_modules)
    except Exception:
        shutil.rmtree(work_dir, ignore_errors=True)
        raise
    return dest


def run_tsc(fixture_root: Path) -> tuple[int, str]:
    """Run ``npx tsc --noEmit`` against the fixture root. Returns (rc, output).

    Returns ``(127, message)`` when the Node toolchain is missing rather than
    letting ``FileNotFoundError`` propagate as a raw traceback — the demo's
    expected failure mode for an unprepared environment should be legible.
    """
    if shutil.which("npx") is None:
        return (
            127,
            "npx not found on PATH. The refactor demo needs Node.js >=18 and a "
            "one-time `npm install` inside examples/refactor_demo/fixture_repo_ts/.",
        )
    proc = subprocess.run(
        ["npx", "tsc", "--noEmit"],
        cwd=fixture_root,
        capture_output=True,
        text=True,
    )
    output = (proc.stdout + proc.stderr).strip()
    return proc.returncode, output


# ---------------------------------------------------------------------------
# Variant entry
# ---------------------------------------------------------------------------


def run(variant: str = "with") -> int:
    """Execute one demo variant end-to-end. Returns the tsc exit code (0 = OK)."""
    if variant not in ("with", "no-invalidation", "context-cache"):
        raise ValueError(f"unknown variant {variant!r}")

    fixture_root = setup_temp_fixture()
    try:
        store = CCSStore(strategy="lazy", on_metric=format_event)
        if variant == "no-invalidation":
            disable_invalidation(store)

        if variant == "context-cache":
            graph = build_graph_context_cache(store)
        else:
            graph = build_graph(store)

        initial_state = {"log": [], "fixture_root": str(fixture_root), "cached_spec": None}
        final_state = graph.invoke(initial_state)

        print()
        print(f"--- variant: {variant} ---")
        for line in final_state.get("log", []):
            print(line)
        committed_version = final_state.get("committed_spec_version")
        print(f"executor committed spec v{committed_version}")

        rc, output = run_tsc(fixture_root)
        if rc == 0:
            print("tsc: OK")
        else:
            print(f"tsc: FAIL (exit {rc})")
            print(output)

        return rc
    finally:
        shutil.rmtree(fixture_root.parent, ignore_errors=True)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="examples.refactor_demo.main",
        description=(
            "Refactor demo: two scripted sub-agents (planner + executor) collaborate on "
            "a shared task-spec artifact through CCSStore, then real tsc runs against a "
            "real TypeScript fixture. Three variants illustrate write-side coherence."
        ),
        epilog=(
            "EXIT CODES:\n"
            "  0   --variant=with: write-side coherence prevented the stale read; tsc passed.\n"
            "  >0  --variant=no-invalidation or --variant=context-cache: stale spec produced "
            "a real tsc failure (typically TS2305). This is the demo's INTENDED outcome and is "
            "not an error.\n"
            "  127 npx not on PATH. Install Node >=18 and run `npm install` inside "
            "examples/refactor_demo/fixture_repo_ts/.\n"
            "\n"
            "PREREQUISITES: Node >=18, npm. The fixture's node_modules is gitignored; run "
            "`npm install` once locally before invoking the demo or its tests."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--variant",
        choices=("with", "no-invalidation", "context-cache"),
        default="with",
        help=(
            "Demo variant. Default: with (write-side coherence on; tsc passes). "
            "no-invalidation: protocol-level proof — same store, event bus patched, "
            "stale v1 wins, tsc fails. context-cache: simulated LLM context-window cache, "
            "stale v1 wins, tsc fails."
        ),
    )
    args = parser.parse_args(argv)
    return run(variant=args.variant)


if __name__ == "__main__":
    sys.exit(main())
