# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""Tests for ``ccs.diagnose.callback``.

Covers the Unit 2 plan's test scenarios:
* Happy path: 3-node synthetic graph; one event per super-step.
* Edge case: single-node single-super-step graph (``insufficient`` posture).
* Edge case: append-only ``messages`` artifact monotonic ID set.
* Edge case: ``messages`` mid-run trim → ID set shrinks.
* Error path: ``RemoteGraph`` attach refusal (graceful — no crash).
* Error path: subgraph (non-empty namespace with ``|``) → verdict signal.
* Error path: non-monotonic ``langgraph_step`` within same namespace.
* Error path: missing ``metadata['langgraph_node']`` → warning + DEFAULT_NODE_NAME.
* Integration: ``examples/langgraph_planner/build_graph_no_store()`` substrate.
* Determinism: identical input on a cold callback yields identical event tuples.
"""

from __future__ import annotations

import uuid
from typing import TypedDict

import pytest

pytest.importorskip("langgraph")
pytest.importorskip("langchain_core")

from langgraph.graph import END, START, StateGraph  # noqa: E402

from ccs.core.identity import artifact_uuid  # noqa: E402
from ccs.diagnose import CCS_DIAGNOSE_LOG_SCHEMA_VERSION  # noqa: E402
from ccs.diagnose.callback import (  # noqa: E402
    DEFAULT_NODE_NAME,
    DEFAULT_SCOPE,
    DiagnoseCallback,
    DiagnoseEvent,
    DiagnoseWarning,
    UnsupportedExecutionModelError,
)


# -----------------------------------------------------------------------
# Synthetic graph helpers
# -----------------------------------------------------------------------


class _ThreeNodeState(TypedDict):
    counter: int
    log: list[str]


def _build_three_node_graph():
    g = StateGraph(_ThreeNodeState)
    g.add_node("alpha", lambda s: {"counter": s["counter"] + 1, "log": [*s["log"], "a"]})
    g.add_node("beta", lambda s: {"counter": s["counter"] + 1, "log": [*s["log"], "b"]})
    g.add_node("gamma", lambda s: {"counter": s["counter"] + 1, "log": [*s["log"], "c"]})
    g.add_edge(START, "alpha")
    g.add_edge("alpha", "beta")
    g.add_edge("beta", "gamma")
    g.add_edge("gamma", END)
    return g.compile()


def _initial_three_node_state() -> _ThreeNodeState:
    return {"counter": 0, "log": []}


# -----------------------------------------------------------------------
# DiagnoseEvent shape and instance ID
# -----------------------------------------------------------------------


def test_diagnose_event_carries_validate_log_three_tuple():
    cb = DiagnoseCallback()
    graph = _build_three_node_graph()
    graph.invoke(_initial_three_node_state(), config={"callbacks": [cb]})

    events = cb.events
    assert events, "callback observed no events"
    for event in events:
        assert isinstance(event, DiagnoseEvent)
        assert event.sequence_number >= 1
        assert isinstance(event.instance_id, uuid.UUID)
        assert event.schema_version == CCS_DIAGNOSE_LOG_SCHEMA_VERSION


def test_sequence_numbers_are_dense_and_monotonic():
    cb = DiagnoseCallback()
    _build_three_node_graph().invoke(
        _initial_three_node_state(), config={"callbacks": [cb]}
    )
    seqs = [ev.sequence_number for ev in cb.events]
    assert seqs == list(range(1, len(seqs) + 1))


def test_instance_id_is_constant_per_callback_instance():
    cb = DiagnoseCallback()
    _build_three_node_graph().invoke(
        _initial_three_node_state(), config={"callbacks": [cb]}
    )
    iids = {ev.instance_id for ev in cb.events}
    assert iids == {cb.instance_id}


# -----------------------------------------------------------------------
# Happy path: 3-node synthetic graph
# -----------------------------------------------------------------------


def test_three_node_graph_yields_three_starts_and_three_ends():
    cb = DiagnoseCallback()
    _build_three_node_graph().invoke(
        _initial_three_node_state(), config={"callbacks": [cb]}
    )
    starts = [ev for ev in cb.events if ev.event_type == "node_start"]
    ends = [ev for ev in cb.events if ev.event_type == "node_end"]
    assert {ev.node for ev in starts} == {"alpha", "beta", "gamma"}
    assert {ev.node for ev in ends} == {"alpha", "beta", "gamma"}
    assert len(starts) == 3
    assert len(ends) == 3


def test_three_node_graph_super_step_attribution():
    cb = DiagnoseCallback()
    _build_three_node_graph().invoke(
        _initial_three_node_state(), config={"callbacks": [cb]}
    )
    by_node = {ev.node: ev.tick for ev in cb.events if ev.event_type == "node_start"}
    assert by_node == {"alpha": 1, "beta": 2, "gamma": 3}


def test_three_node_artifact_uuid_keys_match_state_keys():
    cb = DiagnoseCallback()
    _build_three_node_graph().invoke(
        _initial_three_node_state(), config={"callbacks": [cb]}
    )
    counter_uuid = artifact_uuid(DEFAULT_SCOPE, "counter")
    log_uuid = artifact_uuid(DEFAULT_SCOPE, "log")
    starts = [ev for ev in cb.events if ev.event_type == "node_start"]
    # Every node_start sees both keys in the merged state.
    for event in starts:
        assert counter_uuid in event.content_hashes
        assert log_uuid in event.content_hashes


def test_versions_default_to_content_hash_when_no_checkpointer():
    cb = DiagnoseCallback()
    _build_three_node_graph().invoke(
        _initial_three_node_state(), config={"callbacks": [cb]}
    )
    starts = [ev for ev in cb.events if ev.event_type == "node_start"]
    assert starts
    for event in starts:
        # Synthesized: version string == content hash for every key.
        assert event.artifact_versions == dict(event.content_hashes)


# -----------------------------------------------------------------------
# Determinism
# -----------------------------------------------------------------------


def _events_signature(events) -> tuple:
    """Comparable shape that ignores instance_id and run_id (per-run UUIDs)."""
    return tuple(
        (
            ev.sequence_number,
            ev.tick,
            ev.node,
            ev.event_type,
            tuple(sorted((str(k), v) for k, v in ev.artifact_versions.items())),
            tuple(sorted((str(k), v) for k, v in ev.content_hashes.items())),
            ev.namespace.split(":")[0],  # node prefix; UUID suffix varies per run
            ev.verdict_signal,
            ev.message,
        )
        for ev in events
    )


def test_events_are_deterministic_across_runs():
    cb1 = DiagnoseCallback()
    _build_three_node_graph().invoke(
        _initial_three_node_state(), config={"callbacks": [cb1]}
    )
    cb2 = DiagnoseCallback()
    _build_three_node_graph().invoke(
        _initial_three_node_state(), config={"callbacks": [cb2]}
    )
    assert _events_signature(cb1.events) == _events_signature(cb2.events)


# -----------------------------------------------------------------------
# Edge case: single node, single super-step
# -----------------------------------------------------------------------


def test_single_node_graph_emits_minimal_buffer():
    class S(TypedDict):
        v: int

    g = StateGraph(S)
    g.add_node("only", lambda s: {"v": s["v"] + 1})
    g.add_edge(START, "only")
    g.add_edge("only", END)
    cb = DiagnoseCallback()
    g.compile().invoke({"v": 0}, config={"callbacks": [cb]})

    node_events = [ev for ev in cb.events if ev.event_type in ("node_start", "node_end")]
    assert len(node_events) == 2
    assert all(ev.node == "only" for ev in node_events)


# -----------------------------------------------------------------------
# Edge case: append-only ``messages`` ID set monotonicity
# -----------------------------------------------------------------------


def test_append_only_messages_id_set_is_monotonic_via_content_hashes():
    """The callback exposes content hashes per super-step; classifier
    will compare append-only IDs in Unit 3. This test asserts the buffer
    structure supports that comparison.
    """

    class S(TypedDict):
        messages: list[dict]

    def n_a(s):
        return {"messages": [*s["messages"], {"id": "m1", "text": "hi"}]}

    def n_b(s):
        return {"messages": [*s["messages"], {"id": "m2", "text": "yo"}]}

    g = StateGraph(S)
    g.add_node("a", n_a)
    g.add_node("b", n_b)
    g.add_edge(START, "a")
    g.add_edge("a", "b")
    g.add_edge("b", END)
    cb = DiagnoseCallback()
    g.compile().invoke({"messages": []}, config={"callbacks": [cb]})

    starts = [ev for ev in cb.events if ev.event_type == "node_start"]
    msg_uuid = artifact_uuid(DEFAULT_SCOPE, "messages")
    # ID-set extraction is a Unit 3 concern; here we assert the hashes
    # differ across super-steps (proving content was different and the
    # append-only structure is observable via the buffer).
    a_start = next(ev for ev in starts if ev.node == "a")
    b_start = next(ev for ev in starts if ev.node == "b")
    assert a_start.content_hashes[msg_uuid] != b_start.content_hashes[msg_uuid]


def test_messages_trim_pattern_is_visible_via_hash_change():
    """When ``trim_messages`` shrinks the ID set, the messages content
    hash changes — Unit 3 will detect via ID-set comparison; here we
    just assert the buffer surfaces the change.
    """

    class S(TypedDict):
        messages: list[dict]

    def grow(s):
        return {"messages": [*s["messages"], {"id": "m1"}, {"id": "m2"}, {"id": "m3"}]}

    def trim(s):
        # Drop the first message (trim_messages-style shrink).
        return {"messages": s["messages"][1:]}

    def consume(s):
        return {"messages": s["messages"]}

    g = StateGraph(S)
    g.add_node("grow", grow)
    g.add_node("trim", trim)
    g.add_node("consume", consume)
    g.add_edge(START, "grow")
    g.add_edge("grow", "trim")
    g.add_edge("trim", "consume")
    g.add_edge("consume", END)
    cb = DiagnoseCallback()
    g.compile().invoke({"messages": []}, config={"callbacks": [cb]})

    starts = [ev for ev in cb.events if ev.event_type == "node_start"]
    msg_uuid = artifact_uuid(DEFAULT_SCOPE, "messages")
    consume_start = next(ev for ev in starts if ev.node == "consume")
    trim_start = next(ev for ev in starts if ev.node == "trim")
    # Hashes differ between trim's input (3 msgs) and consume's input (2 msgs).
    assert consume_start.content_hashes[msg_uuid] != trim_start.content_hashes[msg_uuid]


# -----------------------------------------------------------------------
# Error path: RemoteGraph
# -----------------------------------------------------------------------


class _FakeRemoteGraph:
    """Stand-in for ``langgraph.pregel.remote.RemoteGraph``.

    The real class requires API endpoint configuration to instantiate;
    we patch ``isinstance`` indirectly by registering this class as a
    virtual subclass via the import-shim path.
    """


def test_remote_graph_attach_does_not_crash_user_graph(monkeypatch):
    """The plan: refuse-or-downgrade, never crash. Default is downgrade."""
    from ccs.diagnose import callback as cb_mod

    monkeypatch.setattr(cb_mod, "_is_remote_graph", lambda graph: True)

    cb = DiagnoseCallback()
    cb.attach(object())  # pretend this is a RemoteGraph

    assert cb.has_verdict_signal("remote_graph_attached")
    # Assert the user's graph could still be invoked (the synthetic
    # graph below is unrelated; the point is no exception escaped).
    _build_three_node_graph().invoke(
        _initial_three_node_state(), config={"callbacks": [cb]}
    )


def test_remote_graph_with_raise_on_remote_raises_clean_error(monkeypatch):
    from ccs.diagnose import callback as cb_mod

    monkeypatch.setattr(cb_mod, "_is_remote_graph", lambda graph: True)

    cb = DiagnoseCallback(raise_on_remote=True)
    with pytest.raises(UnsupportedExecutionModelError):
        cb.attach(object())


# -----------------------------------------------------------------------
# Error path: subgraph (non-empty checkpoint_ns with '|')
# -----------------------------------------------------------------------


def test_subgraph_emits_verdict_signal_and_continues():
    class S(TypedDict):
        n: int

    inner = StateGraph(S)
    inner.add_node("ix", lambda s: {"n": s["n"] + 10})
    inner.add_edge(START, "ix")
    inner.add_edge("ix", END)
    inner_compiled = inner.compile()

    outer = StateGraph(S)
    outer.add_node("a", lambda s: {"n": s["n"] + 1})
    outer.add_node("sub", inner_compiled)
    outer.add_edge(START, "a")
    outer.add_edge("a", "sub")
    outer.add_edge("sub", END)

    cb = DiagnoseCallback()
    with pytest.warns(DiagnoseWarning):
        outer.compile().invoke({"n": 0}, config={"callbacks": [cb]})

    assert cb.has_verdict_signal("subgraph_observed")
    # Run still completed; node events still recorded for the outer nodes.
    outer_node_starts = [
        ev
        for ev in cb.events
        if ev.event_type == "node_start" and ev.node in {"a", "sub"}
    ]
    assert {ev.node for ev in outer_node_starts} == {"a", "sub"}


# -----------------------------------------------------------------------
# Error path: missing langgraph_node attribution
# -----------------------------------------------------------------------


def test_missing_langgraph_node_attributes_as_unknown_with_warning():
    cb = DiagnoseCallback()
    run_id = uuid.uuid4()
    # Simulate a malformed metadata payload that LangGraph would never
    # produce in normal operation but a third-party wrapper might.
    with pytest.warns(DiagnoseWarning):
        cb.on_chain_start(
            serialized={},
            inputs={"x": 1},
            run_id=run_id,
            tags=["graph:step:1"],
            metadata={"langgraph_step": 1, "langgraph_checkpoint_ns": "x:abc"},
        )

    starts = [ev for ev in cb.events if ev.event_type == "node_start"]
    assert len(starts) == 1
    assert starts[0].node == DEFAULT_NODE_NAME
    assert any(ev.event_type == "warning" for ev in cb.events)


def test_outer_langgraph_wrapper_event_is_silently_ignored():
    cb = DiagnoseCallback()
    run_id = uuid.uuid4()
    # The outer wrapper has no langgraph_node and no graph:step tag.
    cb.on_chain_start(
        serialized={},
        inputs={"x": 1},
        run_id=run_id,
        tags=[],
        metadata={},
    )
    assert cb.events == ()


# -----------------------------------------------------------------------
# Error path: non-monotonic langgraph_step within a single namespace
# -----------------------------------------------------------------------


def test_non_monotonic_step_within_namespace_emits_verdict_signal():
    cb = DiagnoseCallback()
    rid = uuid.uuid4()
    with pytest.warns(DiagnoseWarning):
        cb.on_chain_start(
            serialized={},
            inputs={"x": 1},
            run_id=rid,
            metadata={
                "langgraph_step": 5,
                "langgraph_node": "n",
                "langgraph_checkpoint_ns": "n:fixed",
            },
        )
        cb.on_chain_start(
            serialized={},
            inputs={"x": 1},
            run_id=rid,
            metadata={
                "langgraph_step": 4,  # monotonicity break
                "langgraph_node": "n",
                "langgraph_checkpoint_ns": "n:fixed",
            },
        )
    assert cb.has_verdict_signal("unsupported_execution_model")


def test_repeated_step_within_same_namespace_is_not_a_break():
    """LangGraph re-emits on_chain_start at the same (step, namespace) for
    nodes attached to a conditional edge — once for the edge resolver,
    once for the node body. This is benign and must not trip monotonicity.
    Regression for the substrate-integration finding (Day 3).
    """
    cb = DiagnoseCallback()
    rid = uuid.uuid4()
    cb.on_chain_start(
        serialized={},
        inputs={"x": 1},
        run_id=rid,
        metadata={
            "langgraph_step": 7,
            "langgraph_node": "loop",
            "langgraph_checkpoint_ns": "loop:fixed",
        },
    )
    cb.on_chain_start(
        serialized={},
        inputs={"x": 1},
        run_id=rid,
        metadata={
            "langgraph_step": 7,  # same step, same namespace — benign re-entry
            "langgraph_node": "loop",
            "langgraph_checkpoint_ns": "loop:fixed",
        },
    )
    assert not cb.has_verdict_signal("unsupported_execution_model")


# -----------------------------------------------------------------------
# Finalize and immutability
# -----------------------------------------------------------------------


def test_finalize_is_idempotent():
    cb = DiagnoseCallback()
    _build_three_node_graph().invoke(
        _initial_three_node_state(), config={"callbacks": [cb]}
    )
    first = cb.finalize()
    second = cb.finalize()
    assert first == second
    assert cb.is_finalized


def test_record_after_finalize_raises():
    cb = DiagnoseCallback()
    cb.finalize()
    with pytest.raises(RuntimeError):
        cb.on_chain_start(
            serialized={},
            inputs={"x": 1},
            run_id=uuid.uuid4(),
            metadata={
                "langgraph_step": 1,
                "langgraph_node": "n",
                "langgraph_checkpoint_ns": "n:abc",
            },
        )


# -----------------------------------------------------------------------
# Integration: examples/langgraph_planner/build_graph_no_store
# -----------------------------------------------------------------------


def test_integration_no_store_substrate_meets_coverage_threshold():
    """The Day 1 substrate gate: ≥10 super-steps, ≥10 reads, ≥1 write."""
    from examples.langgraph_planner.main import (
        build_graph_no_store,
        initial_state_no_store,
    )

    cb = DiagnoseCallback()
    graph = build_graph_no_store()
    graph.invoke(initial_state_no_store(), config={"callbacks": [cb]})

    starts = [ev for ev in cb.events if ev.event_type == "node_start"]
    distinct_steps = {ev.tick for ev in starts}
    distinct_nodes = {ev.node for ev in starts}

    assert len(distinct_steps) >= 10, f"super-steps: {sorted(distinct_steps)}"
    assert len(distinct_nodes) >= 3

    # ≥10 reads of the 'plan' state key (i.e. reader nodes saw a
    # populated plan in their merged-state input).
    plan_uuid = artifact_uuid(DEFAULT_SCOPE, "plan")
    reads_of_plan = sum(
        1
        for ev in starts
        if plan_uuid in ev.content_hashes
    )
    assert reads_of_plan >= 10

    # ≥1 write of 'plan' (planner's node_end output dict has 'plan').
    ends = [ev for ev in cb.events if ev.event_type == "node_end"]
    plan_writes = sum(
        1
        for ev in ends
        if plan_uuid in ev.content_hashes and ev.node == "planner"
    )
    assert plan_writes >= 1
