# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Tests for the planner-executor refactor demo.

The demo lives under ``examples/refactor_demo/`` and is the protocol-level
proof asset for the write-side coherence positioning (see
``docs/plans/2026-05-12-001-feat-cocoindex-positioning-coding-agent-wedge-plan.md``).

These tests verify three orthogonal failure-prevention paths:

* with-coherence: planner's v2 write invalidates the executor's cache;
  the commit-time re-read fetches v2 and the rename includes all 4 callers.
* no-invalidation: same graph, but ``disable_invalidation`` patches the
  event bus' ``publish_invalidation`` to a no-op; the executor's commit-time
  re-read returns the still-SHARED v1 and misses the 4th caller.
* context-cache: simulated LLM context-window cache — the executor commits
  using the spec captured at read time and never re-reads.
"""

from __future__ import annotations

import shutil
from uuid import NAMESPACE_URL, uuid5

import pytest

# ``examples.refactor_demo`` is resolved via pyproject's
# ``[tool.pytest.ini_options] pythonpath = ["src", "."]``. No per-file
# sys.path manipulation is needed; do not reintroduce one here.

# Skip the whole module if langgraph isn't installed (e.g. bare [dev] install).
pytest.importorskip("langgraph.graph")

from ccs.adapters.ccsstore import CCSStore, StoreMetricEvent
from ccs.core.states import MESIState
from examples.refactor_demo.executor import EXECUTOR_AGENT
from examples.refactor_demo.main import (
    FIXTURE_REPO_TS,
    build_graph,
    build_graph_context_cache,
    run_tsc,
    setup_temp_fixture,
)
from examples.refactor_demo.planner import (
    PLANNER_AGENT,
    SHARED_SCOPE,
    TASK_KEY,
    TASK_SPEC_V2,
)
from examples.refactor_demo.strategies import disable_invalidation


def _has_tsc() -> bool:
    return shutil.which("npx") is not None and (FIXTURE_REPO_TS / "node_modules").exists()


# ---------------------------------------------------------------------------
# Variant: with-coherence (default)
# ---------------------------------------------------------------------------


def test_with_coherence_executor_commit_refetches_v2() -> None:
    """Executor's commit-time re-read returns v2 after planner's invalidating v2 write."""
    events: list[StoreMetricEvent] = []
    store = CCSStore(strategy="lazy", on_metric=events.append)
    fixture_root = setup_temp_fixture()
    try:
        graph = build_graph(store)
        final = graph.invoke({"log": [], "fixture_root": str(fixture_root), "cached_spec": None})

        assert final["committed_spec_version"] == 2
        # 3 v1 callers (middleware, login, refresh) + utils/session.ts (v2 addition) + auth.ts = 5
        assert len(final["committed_files"]) == 5
        # Identity check, not just count: the 4th caller must be in the rename set with-coherence.
        assert "src/utils/session.ts" in final["committed_files"]

        # The executor's commit-time get is a cache miss (cache was INVALID after planner v2).
        executor_gets = [e for e in events if e.operation == "get" and e.agent_name == EXECUTOR_AGENT]
        assert len(executor_gets) == 2, f"expected 2 executor gets, saw {len(executor_gets)}"
        assert executor_gets[0].cache_hit is False, "first read (executor_read_node) should be cache miss"
        assert executor_gets[1].cache_hit is False, "commit-time re-read should be cache miss after invalidation"
    finally:
        shutil.rmtree(fixture_root.parent, ignore_errors=True)


def test_with_coherence_executor_mesi_state_invalidates_between_reads() -> None:
    """Executor's MESI state ends SHARED; the cached content view is the refreshed v2."""
    store = CCSStore(strategy="lazy")
    fixture_root = setup_temp_fixture()
    try:
        graph = build_graph(store)
        artifact_id = uuid5(NAMESPACE_URL, f"ccs-artifact:{SHARED_SCOPE}:{TASK_KEY}")

        graph.invoke({"log": [], "fixture_root": str(fixture_root), "cached_spec": None})

        executor_entry = store.core.runtime(EXECUTOR_AGENT).cache.get(artifact_id)
        assert executor_entry is not None
        # Cache transitioned SHARED -> INVALID -> SHARED; final state is SHARED.
        # The exact ``local_version`` number is coordinator-internal; what matters
        # is what the executor's content view actually is.
        assert executor_entry.state == MESIState.SHARED

        # Authoritative content view: planner's most recent write was v2.
        item = store.get((EXECUTOR_AGENT, SHARED_SCOPE), TASK_KEY)
        assert item is not None
        assert item.value["version"] == 2
    finally:
        shutil.rmtree(fixture_root.parent, ignore_errors=True)


# ---------------------------------------------------------------------------
# Variant: no-invalidation (blog protocol-level proof)
# ---------------------------------------------------------------------------


def test_no_invalidation_executor_commits_stale_v1() -> None:
    """With invalidation suppressed, executor's commit-time re-read returns cached v1."""
    events: list[StoreMetricEvent] = []
    store = CCSStore(strategy="lazy", on_metric=events.append)
    disable_invalidation(store)
    fixture_root = setup_temp_fixture()
    try:
        graph = build_graph(store)
        final = graph.invoke({"log": [], "fixture_root": str(fixture_root), "cached_spec": None})

        # Executor commits v1 — the spec it cached at read time and that was never invalidated.
        assert final["committed_spec_version"] == 1
        # 3 callers from v1 + auth.ts = 4 files (utils/session.ts is missed).
        assert len(final["committed_files"]) == 4
        # Identity check: utils/session.ts must NOT be in the rename set when v1 wins.
        assert "src/utils/session.ts" not in final["committed_files"]

        # The commit-time re-read is a cache HIT because the bus was patched.
        executor_gets = [e for e in events if e.operation == "get" and e.agent_name == EXECUTOR_AGENT]
        assert len(executor_gets) == 2
        assert executor_gets[0].cache_hit is False, "first read is always a miss"
        assert executor_gets[1].cache_hit is True, "commit re-read should be a cache hit (no invalidation)"
    finally:
        shutil.rmtree(fixture_root.parent, ignore_errors=True)


def test_no_invalidation_keeps_executor_mesi_state_shared() -> None:
    """With invalidation suppressed, executor's cache stays SHARED across planner's v2 write."""
    store = CCSStore(strategy="lazy")
    disable_invalidation(store)
    fixture_root = setup_temp_fixture()
    try:
        graph = build_graph(store)
        artifact_id = uuid5(NAMESPACE_URL, f"ccs-artifact:{SHARED_SCOPE}:{TASK_KEY}")

        graph.invoke({"log": [], "fixture_root": str(fixture_root), "cached_spec": None})

        executor_entry = store.core.runtime(EXECUTOR_AGENT).cache.get(artifact_id)
        assert executor_entry is not None
        # State never flipped to INVALID because publish_invalidation is patched.
        # The application-level proof — that executor committed v1, missing the 4th
        # caller — is covered by test_no_invalidation_executor_commits_stale_v1.
        assert executor_entry.state == MESIState.SHARED, "without invalidation, peer stays SHARED"
    finally:
        shutil.rmtree(fixture_root.parent, ignore_errors=True)


# ---------------------------------------------------------------------------
# Variant: context-cache (Unit 3)
# ---------------------------------------------------------------------------


def test_context_cache_executor_commits_from_graph_state() -> None:
    """Context-cache variant: executor never re-reads; commits cached v1 from state."""
    events: list[StoreMetricEvent] = []
    store = CCSStore(strategy="lazy", on_metric=events.append)
    fixture_root = setup_temp_fixture()
    try:
        graph = build_graph_context_cache(store)
        final = graph.invoke({"log": [], "fixture_root": str(fixture_root), "cached_spec": None})

        assert final["committed_spec_version"] == 1
        assert len(final["committed_files"]) == 4
        # Identity check: utils/session.ts must NOT be in the rename set when v1 wins.
        assert "src/utils/session.ts" not in final["committed_files"]

        # Executor only does ONE get (executor_read_node). No commit-time re-read.
        executor_gets = [e for e in events if e.operation == "get" and e.agent_name == EXECUTOR_AGENT]
        assert len(executor_gets) == 1, "context-cache variant must not re-read at commit"
    finally:
        shutil.rmtree(fixture_root.parent, ignore_errors=True)


# ---------------------------------------------------------------------------
# Cross-variant: clean state between invocations
# ---------------------------------------------------------------------------


def test_fresh_store_per_invocation_has_no_state_bleed() -> None:
    """Running two variants back-to-back with separate stores must not bleed state."""
    fixture_root_1 = setup_temp_fixture()
    fixture_root_2 = setup_temp_fixture()
    try:
        store_1 = CCSStore(strategy="lazy")
        disable_invalidation(store_1)
        graph_1 = build_graph(store_1)
        final_1 = graph_1.invoke({"log": [], "fixture_root": str(fixture_root_1), "cached_spec": None})

        store_2 = CCSStore(strategy="lazy")  # default lazy; coherence on
        graph_2 = build_graph(store_2)
        final_2 = graph_2.invoke({"log": [], "fixture_root": str(fixture_root_2), "cached_spec": None})

        assert final_1["committed_spec_version"] == 1
        assert final_2["committed_spec_version"] == 2
    finally:
        shutil.rmtree(fixture_root_1.parent, ignore_errors=True)
        shutil.rmtree(fixture_root_2.parent, ignore_errors=True)


# ---------------------------------------------------------------------------
# Sanity: planner writes both versions to the artifact
# ---------------------------------------------------------------------------


def test_planner_v2_is_what_executor_with_coherence_observes() -> None:
    """Sanity: the with-coherence executor's committed spec matches TASK_SPEC_V2 exactly."""
    store = CCSStore(strategy="lazy")
    fixture_root = setup_temp_fixture()
    try:
        graph = build_graph(store)
        graph.invoke({"log": [], "fixture_root": str(fixture_root), "cached_spec": None})

        # The store's authoritative value for the artifact is v2.
        item = store.get((PLANNER_AGENT, SHARED_SCOPE), TASK_KEY)
        assert item is not None
        assert item.value == TASK_SPEC_V2
    finally:
        shutil.rmtree(fixture_root.parent, ignore_errors=True)


# ---------------------------------------------------------------------------
# End-to-end: actual tsc invocation
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not _has_tsc(), reason="requires Node toolchain (npx + node_modules)")
def test_with_coherence_tsc_passes() -> None:
    """End-to-end: with-coherence variant produces a clean tsc build."""
    fixture_root = setup_temp_fixture()
    try:
        store = CCSStore(strategy="lazy")
        graph = build_graph(store)
        graph.invoke({"log": [], "fixture_root": str(fixture_root), "cached_spec": None})

        rc, output = run_tsc(fixture_root)
        assert rc == 0, f"expected tsc to pass; output:\n{output}"
    finally:
        shutil.rmtree(fixture_root.parent, ignore_errors=True)


@pytest.mark.skipif(not _has_tsc(), reason="requires Node toolchain (npx + node_modules)")
def test_no_invalidation_tsc_fails_with_ts2305() -> None:
    """End-to-end: no-invalidation variant produces TS2305 on session.ts."""
    fixture_root = setup_temp_fixture()
    try:
        store = CCSStore(strategy="lazy")
        disable_invalidation(store)
        graph = build_graph(store)
        graph.invoke({"log": [], "fixture_root": str(fixture_root), "cached_spec": None})

        rc, output = run_tsc(fixture_root)
        assert rc != 0
        assert "TS2305" in output
        assert "validateUser" in output
        assert "utils/session.ts" in output
    finally:
        shutil.rmtree(fixture_root.parent, ignore_errors=True)


@pytest.mark.skipif(not _has_tsc(), reason="requires Node toolchain (npx + node_modules)")
def test_context_cache_tsc_fails_with_ts2305() -> None:
    """End-to-end: context-cache variant produces TS2305 on session.ts."""
    fixture_root = setup_temp_fixture()
    try:
        store = CCSStore(strategy="lazy")
        graph = build_graph_context_cache(store)
        graph.invoke({"log": [], "fixture_root": str(fixture_root), "cached_spec": None})

        rc, output = run_tsc(fixture_root)
        assert rc != 0
        assert "TS2305" in output
        assert "utils/session.ts" in output
    finally:
        shutil.rmtree(fixture_root.parent, ignore_errors=True)
