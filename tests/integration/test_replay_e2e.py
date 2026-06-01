# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""End-to-end integration tests for the replay tool (Unit 6 of D v1).

Each test drives the FULL capture → load → run_predicates → emit → exit-code
pipeline. No layer is mocked; sim-engine-style fixtures produce real state_log
streams via the actual coordinator/agent runtime, and synthetic JSONL fixtures
cover scenarios the live coordinator forbids by construction (single-writer +
monotonic-version are invariants of the coordinator itself, so a "lost-write"
or "stale-read" breach has to be hand-written).

The fifth test is the **real-LangGraph completion gate** required by the v1
plan (Unit 6, P1-C resolution). It compiles a real ``langgraph.graph.StateGraph``
that exercises a write + multiple reads through ``CCSStore.record_to(path)``,
then replays the captured directory and asserts a clean run.

Tests skip cleanly when ``langgraph`` is not installed only for the real-fixture
test; the four synthetic e2e tests don't depend on langgraph.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from ccs.cli.coherence_replay import main as replay_main

# ---------------------------------------------------------------------------
# Helpers — minimal fixture builders (mirror the trace-format spec §3 + §4)
# ---------------------------------------------------------------------------


def _write_manifest(
    session_dir: Path,
    *,
    streams: list[str],
    instance_id: str | None = "instance-A",
    adapter_type: str = "e2e-fixture",
    start_tick: int = 0,
    end_tick: int = 10,
) -> None:
    session_dir.mkdir(parents=True, exist_ok=True)
    manifest = {
        "schema_version": 0,
        "schema_note": "e2e fixture",
        "adapter_type": adapter_type,
        "start_tick": start_tick,
        "end_tick": end_tick,
        "instance_id": instance_id,
        "streams": streams,
        "agents": {},
        "artifacts": {},
    }
    (session_dir / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")


def _state_log_entry(
    *,
    tick: int,
    sequence_number: int,
    agent_id: str = "agent-1",
    artifact_id: str = "art-1",
    from_state: str = "INVALID",
    to_state: str = "EXCLUSIVE",
    trigger: str = "write",
    version: int = 1,
    instance_id: str = "instance-A",
) -> dict[str, Any]:
    return {
        "tick": tick,
        "artifact_id": artifact_id,
        "agent_id": agent_id,
        "agent_name": agent_id,
        "from_state": from_state,
        "to_state": to_state,
        "trigger": trigger,
        "version": version,
        "content_hash": "abc",
        "sequence_number": sequence_number,
        "instance_id": instance_id,
        "schema_version": "ccs.state_log.v2",
    }


def _audit_entry(
    *,
    tick: int,
    sequence_number: int,
    agent_id: str = "reader",
    artifact_id: str = "art-1",
    version: int = 1,
    instance_id: str = "instance-A",
) -> dict[str, Any]:
    return {
        "tick": tick,
        "agent_id": agent_id,
        "agent_name": agent_id,
        "artifact_id": artifact_id,
        "version": version,
        "content_hash": "abc",
        "source": "fetch",
        "outcome": "content",
        "sequence_number": sequence_number,
        "instance_id": instance_id,
        "schema_version": "ccs.content_audit.v1",
    }


def _write_jsonl(session_dir: Path, name: str, entries: list[dict]) -> None:
    path = session_dir / f"{name}.jsonl"
    with path.open("w", encoding="utf-8") as fh:
        for entry in entries:
            fh.write(json.dumps(entry) + "\n")


# ---------------------------------------------------------------------------
# E2E 1: clean trace
# ---------------------------------------------------------------------------


def test_e2e_clean_trace(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Drive a single CCSStore agent through a clean write + read cycle.

    Single-writer + monotonic-version are honored by construction (the
    coordinator enforces them). Stale-read and lost-write predicates run
    against the captured streams and find no breaches.
    """
    pytest.importorskip("langgraph")
    from ccs.adapters.ccsstore import CCSStore
    session_dir = tmp_path / "clean-session"
    with CCSStore.record_to(session_dir, strategy="lazy") as store:
        store.put(("planner", "shared"), "plan", {"v": 1})
        result = store.get(("planner", "shared"), "plan")
        assert result is not None
        assert result.value == {"v": 1}

    # Capture artifacts exist + are well-formed.
    assert (session_dir / "manifest.json").exists()
    assert (session_dir / "state_log.jsonl").exists()
    manifest = json.loads((session_dir / "manifest.json").read_text())
    assert manifest["adapter_type"] == "langgraph-ccsstore"
    assert manifest["instance_id"] is not None

    rc = replay_main([str(session_dir)])
    out = capsys.readouterr().out
    assert rc == 0, f"clean trace should exit 0, got {rc}; stdout:\n{out}"
    assert "0 CONFIRMED" in out
    assert "0 AMBIGUOUS" in out


# ---------------------------------------------------------------------------
# E2E 2: breach trace (lost-write — coordinator-forbidden, so synthesized)
# ---------------------------------------------------------------------------


def test_e2e_breach_trace(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Synthesize a single-writer breach and verify CLI exits 1.

    The breach (two agents simultaneously in MODIFIED on the same artifact)
    is impossible to produce through real coordinator operations — the
    coordinator's ``check_single_writer`` invariant rejects it. We
    construct it on-disk to exercise the predicate engine end-to-end.
    """
    session_dir = tmp_path / "breach-session"
    _write_manifest(session_dir, streams=["state_log"])
    _write_jsonl(
        session_dir,
        "state_log",
        [
            _state_log_entry(
                tick=1, sequence_number=1, agent_id="agent-1",
                from_state="INVALID", to_state="EXCLUSIVE",
            ),
            _state_log_entry(
                tick=2, sequence_number=2, agent_id="agent-1",
                from_state="EXCLUSIVE", to_state="MODIFIED",
                trigger="commit", version=1,
            ),
            _state_log_entry(
                tick=3, sequence_number=3, agent_id="agent-2",
                from_state="INVALID", to_state="EXCLUSIVE",
            ),
            _state_log_entry(
                tick=4, sequence_number=4, agent_id="agent-2",
                from_state="EXCLUSIVE", to_state="MODIFIED",
                trigger="commit", version=2,
            ),
        ],
    )

    rc = replay_main([str(session_dir)])
    out = capsys.readouterr().out
    assert rc == 1, f"breach trace should exit 1, got {rc}; stdout:\n{out}"
    assert "[CONFIRMED]" in out
    assert "single-writer" in out


# ---------------------------------------------------------------------------
# E2E 3: AMBIGUOUS trace (same-tick read + commit)
# ---------------------------------------------------------------------------


def test_e2e_ambiguous_trace(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Same-tick read+commit collision → AMBIGUOUS stale-read.

    Default: suppressed from per-finding output, exits 0, summary shows
    "1 AMBIGUOUS (suppressed)". ``--include-ambiguous``: shown, still
    exits 0.
    """
    session_dir = tmp_path / "ambig-session"
    _write_manifest(session_dir, streams=["state_log", "content_audit_log"])
    state_entries = [
        # Seed v1 cleanly: writer reaches MODIFIED at tick 1.
        _state_log_entry(
            tick=1, sequence_number=1, agent_id="writer",
            from_state="INVALID", to_state="EXCLUSIVE",
        ),
        _state_log_entry(
            tick=1, sequence_number=2, agent_id="writer",
            from_state="EXCLUSIVE", to_state="MODIFIED",
            trigger="commit", version=1,
        ),
        # Writer re-acquires EXCLUSIVE at tick 4, then commits v2 at tick 5.
        _state_log_entry(
            tick=4, sequence_number=3, agent_id="writer",
            from_state="MODIFIED", to_state="EXCLUSIVE",
        ),
        _state_log_entry(
            tick=5, sequence_number=4, agent_id="writer",
            from_state="EXCLUSIVE", to_state="MODIFIED",
            trigger="commit", version=2,
        ),
    ]
    # Same-tick reader observation of stale version=1 at tick 5: AMBIGUOUS.
    audit_entries = [
        _audit_entry(tick=5, sequence_number=10, agent_id="reader-1", version=1),
    ]
    _write_jsonl(session_dir, "state_log", state_entries)
    _write_jsonl(session_dir, "content_audit_log", audit_entries)

    # Default — AMBIGUOUS suppressed.
    rc = replay_main([str(session_dir)])
    out = capsys.readouterr().out
    assert rc == 0
    assert "[AMBIGUOUS]" not in out
    assert "1 AMBIGUOUS (suppressed)" in out

    # --include-ambiguous — AMBIGUOUS shown, still exit 0.
    rc2 = replay_main([str(session_dir), "--include-ambiguous"])
    out2 = capsys.readouterr().out
    assert rc2 == 0
    assert "[AMBIGUOUS]" in out2
    assert "1 AMBIGUOUS (shown)" in out2


# ---------------------------------------------------------------------------
# E2E 4: multi-instance trace error
# ---------------------------------------------------------------------------


def test_e2e_multi_instance_trace(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Two ``instance_id`` values → CLI exits 3 with MULTI_INSTANCE_TRACE."""
    session_dir = tmp_path / "multi-instance"
    _write_manifest(session_dir, streams=["state_log"])
    _write_jsonl(
        session_dir,
        "state_log",
        [
            _state_log_entry(tick=1, sequence_number=1, instance_id="instance-A"),
            _state_log_entry(tick=2, sequence_number=2, instance_id="instance-B"),
        ],
    )

    rc = replay_main([str(session_dir)])
    captured = capsys.readouterr()
    assert rc == 3
    # Loader error message references the instance violation + D+1 roadmap.
    assert "instance" in captured.err.lower()
    assert "D+1" in captured.err
    assert "Traceback" not in captured.err


# ---------------------------------------------------------------------------
# E2E 5: REAL LANGGRAPH FIXTURE — Unit 6 completion gate
# ---------------------------------------------------------------------------


def test_e2e_real_langgraph_capture_and_replay(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """**Unit 6 completion gate** — capture a real LangGraph cycle and replay.

    Compiles a real ``langgraph.graph.StateGraph`` (planner writes; three
    reader nodes each read the same artifact multiple times) and routes
    everything through ``CCSStore.record_to(path)``. Then runs the CLI
    replay over the captured directory and asserts:

    1. The capture produced a well-formed ``manifest.json`` + non-empty
       ``state_log.jsonl``.
    2. Replay loads without trace errors.
    3. The clean cycle produces zero findings (the LangGraph state-graph
       paths exercise the coordinator's invariants normally — no breach
       should surface).

    Skips cleanly when ``langgraph`` is not installed.
    """
    pytest.importorskip("langgraph.graph")
    pytest.importorskip("langgraph.config")

    from typing import TypedDict

    from langgraph.config import get_store as lg_get_store
    from langgraph.graph import END, START, StateGraph

    from ccs.adapters.ccsstore import CCSStore

    class GraphState(TypedDict):
        log: list[str]

    artifact_scope = "shared"
    artifact_key = "plan"
    plan_content = {
        "title": "Replay e2e gate",
        "objectives": ["exercise CCSStore.record_to under real LangGraph"],
        "owner": "planner",
    }

    def planner_node(state: GraphState) -> dict:
        store: CCSStore = lg_get_store()  # type: ignore[assignment]
        store.put(("planner", artifact_scope), artifact_key, plan_content)
        return {"log": [*state["log"], "planner: wrote plan"]}

    def _make_reader(agent_name: str):
        def node(state: GraphState) -> dict:
            store: CCSStore = lg_get_store()  # type: ignore[assignment]
            # Multiple reads — first is a coordinator fetch (cache miss),
            # subsequent are cache hits. Exercises read-side state
            # transitions captured into the state_log stream.
            for _ in range(3):
                item = store.get((agent_name, artifact_scope), artifact_key)
                assert item is not None
                assert item.value == plan_content
            return {"log": [*state["log"], f"{agent_name}: read 3x"]}

        node.__name__ = f"{agent_name}_node"
        return node

    def build_graph(store: CCSStore):
        builder = StateGraph(GraphState)
        builder.add_node("planner", planner_node)
        for name in ("researcher", "executor", "reviewer"):
            builder.add_node(name, _make_reader(name))
        builder.add_edge(START, "planner")
        builder.add_edge("planner", "researcher")
        builder.add_edge("researcher", "executor")
        builder.add_edge("executor", "reviewer")
        builder.add_edge("reviewer", END)
        return builder.compile(store=store)

    session_dir = tmp_path / "real-langgraph"

    with CCSStore.record_to(session_dir, strategy="lazy") as store:
        graph = build_graph(store)
        final_state = graph.invoke({"log": []})

    # Sanity-check the captured graph actually ran end-to-end.
    assert "planner: wrote plan" in final_state["log"]
    assert any("read 3x" in entry for entry in final_state["log"])

    # Capture artifacts exist + are well-formed.
    manifest_path = session_dir / "manifest.json"
    state_log_path = session_dir / "state_log.jsonl"
    assert manifest_path.exists(), "real-LangGraph capture missing manifest.json"
    assert state_log_path.exists(), "real-LangGraph capture missing state_log.jsonl"
    manifest = json.loads(manifest_path.read_text())
    assert manifest["adapter_type"] == "langgraph-ccsstore"
    assert manifest["instance_id"] is not None
    state_log_lines = [
        line for line in state_log_path.read_text().splitlines() if line.strip()
    ]
    assert state_log_lines, "real-LangGraph capture produced empty state_log.jsonl"

    # Replay the captured directory — clean LangGraph cycle should produce
    # zero CONFIRMED findings. The reader fan-out (one writer + three
    # multi-read agents) exercises SHARED-state transitions captured into
    # the state_log stream.
    rc = replay_main([str(session_dir)])
    out = capsys.readouterr().out
    assert rc == 0, (
        f"real-LangGraph clean cycle should exit 0, got {rc}; stdout:\n{out}"
    )
    assert "0 CONFIRMED" in out
