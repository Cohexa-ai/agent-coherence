# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""Tests for ``ccs.diagnose.checkpointer``.

Cross-validates that ``DiagnoseCheckpointer``-attached runs produce
``DiagnoseEvent`` records whose ``artifact_versions`` reflect LangGraph's
authoritative ``Checkpoint.channel_versions`` rather than synthetic
content-hash fallbacks.
"""

from __future__ import annotations

from typing import TypedDict

import pytest

pytest.importorskip("langgraph")
pytest.importorskip("langchain_core")

from langgraph.graph import END, START, StateGraph  # noqa: E402

from ccs.core.identity import artifact_uuid  # noqa: E402
from ccs.diagnose.callback import DEFAULT_SCOPE, DiagnoseCallback  # noqa: E402
from ccs.diagnose.checkpointer import DiagnoseCheckpointer  # noqa: E402


class _S(TypedDict):
    n: int
    plan: dict


def _two_node_graph(checkpointer):
    g = StateGraph(_S)
    g.add_node("write", lambda s: {"n": s["n"] + 1, "plan": {"v": 1}})
    g.add_node("read", lambda s: {"n": s["n"] + 1})
    g.add_edge(START, "write")
    g.add_edge("write", "read")
    g.add_edge("read", END)
    return g.compile(checkpointer=checkpointer)


def test_callback_works_without_checkpointer_attached():
    cb = DiagnoseCallback()
    graph = _two_node_graph(checkpointer=None)
    graph.invoke({"n": 0, "plan": {}}, config={"callbacks": [cb]})

    starts = [ev for ev in cb.events if ev.event_type == "node_start"]
    n_uuid = artifact_uuid(DEFAULT_SCOPE, "n")
    # No checkpointer overlay → versions equal content hashes.
    for ev in starts:
        assert ev.artifact_versions[n_uuid] == ev.content_hashes[n_uuid]


def test_checkpointer_overlay_replaces_synthetic_versions():
    cb = DiagnoseCallback()
    cp = DiagnoseCheckpointer(callback=cb)
    graph = _two_node_graph(checkpointer=cp)
    graph.invoke(
        {"n": 0, "plan": {}},
        config={
            "callbacks": [cb],
            "configurable": {"thread_id": "test-thread"},
        },
    )

    starts = [ev for ev in cb.events if ev.event_type == "node_start"]
    assert starts, "callback should have observed at least one node_start"
    n_uuid = artifact_uuid(DEFAULT_SCOPE, "n")

    # At least one event should have an authoritative LangGraph version
    # in the overlay rather than the content-hash fallback.
    overlaid = [
        ev
        for ev in starts
        if n_uuid in ev.artifact_versions
        and ev.artifact_versions[n_uuid] != ev.content_hashes[n_uuid]
    ]
    assert overlaid, (
        "Expected at least one event whose 'n' version came from "
        "Checkpoint.channel_versions, not the content-hash fallback. "
        "If this fails, LangGraph's channel-version shape changed; "
        "Unit 2 spike output documented forward via DiagnoseCheckpointer."
    )

    # Spot-check the version format is the LangGraph stamp shape:
    # padded int + '.' + suffix. A pure SHA-256 hex never contains '.'.
    sample_overlaid = overlaid[0].artifact_versions[n_uuid]
    assert "." in sample_overlaid


def test_checkpointer_with_no_callback_is_a_plain_memory_saver():
    cp = DiagnoseCheckpointer()
    assert cp.callback is None
    graph = _two_node_graph(checkpointer=cp)
    # Compile + invoke should succeed without diagnostic plumbing.
    graph.invoke(
        {"n": 0, "plan": {}},
        config={"configurable": {"thread_id": "no-callback"}},
    )


def test_attach_callback_after_construction_observes_subsequent_runs():
    cp = DiagnoseCheckpointer()
    cb = DiagnoseCallback()
    cp.attach_callback(cb)
    graph = _two_node_graph(checkpointer=cp)
    graph.invoke(
        {"n": 0, "plan": {}},
        config={
            "callbacks": [cb],
            "configurable": {"thread_id": "late-attach"},
        },
    )

    n_uuid = artifact_uuid(DEFAULT_SCOPE, "n")
    starts = [ev for ev in cb.events if ev.event_type == "node_start"]
    overlaid = [
        ev
        for ev in starts
        if n_uuid in ev.artifact_versions
        and ev.artifact_versions[n_uuid] != ev.content_hashes[n_uuid]
    ]
    assert overlaid


def test_checkpointer_failure_does_not_crash_user_graph(monkeypatch):
    """Forwarding errors must be swallowed: diagnose never crashes a graph."""
    cb = DiagnoseCallback()

    def boom(*args, **kwargs):
        raise RuntimeError("simulated overlay failure")

    monkeypatch.setattr(cb, "_ingest_channel_versions", boom)

    cp = DiagnoseCheckpointer(callback=cb)
    graph = _two_node_graph(checkpointer=cp)
    out = graph.invoke(
        {"n": 0, "plan": {}},
        config={
            "callbacks": [cb],
            "configurable": {"thread_id": "err-path"},
        },
    )
    assert out["n"] >= 2  # graph completed despite overlay errors
