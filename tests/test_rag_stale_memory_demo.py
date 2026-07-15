# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Offline tests for the RAG stale-memory write-back demo.

Sequenced, deterministic: the broken case proves an edit written back from an
older snapshot erases a peer's appended fact; the fixed case proves CoherentVolume
denies that stale write-back (fail-closed) and recovers via reacquire() with no
loss. The fixed case spawns a local coordinator subprocess (no network), so these
double as the end-to-end check for the demo.
"""

from __future__ import annotations

from examples.rag_stale_memory.broken import run_broken
from examples.rag_stale_memory.fixed import run_fixed
from examples.rag_stale_memory.main import main as run_main

_B_FACT = "timezone: PST"
_A_SUMMARY = "Customer prefers email contact; flagged VIP."

# --- broken case (no coherence) --------------------------------------------


def test_broken_loses_the_update() -> None:
    trace = run_broken()
    assert trace["lost_update"] is True
    assert trace["a_edit_survived"] is True  # A's summary landed
    assert trace["b_fact_survived"] is False  # B's appended fact was erased
    assert _B_FACT not in trace["final_facts"]


def test_broken_is_deterministic_across_runs() -> None:
    # Sequenced, not raced — the lost update must reproduce every time.
    assert all(run_broken()["lost_update"] is True for _ in range(10))


# --- fixed case (CoherentVolume explicit API) ------------------------------


def test_fixed_denies_stale_write_then_recovers() -> None:
    trace = run_fixed()
    assert trace["a_write_denied"] is True  # the stale write was denied fail-closed
    assert trace["a_recovered_via_reacquire"] is True  # recovery via reacquire()
    assert trace["lost_update"] is False
    assert trace["b_fact_survived"] is True  # B's fact preserved
    assert trace["a_edit_survived"] is True  # A's summary re-applied
    assert _B_FACT in trace["final_facts"]
    assert trace["final_summary"] == _A_SUMMARY
    assert trace["denial_reason"]  # the coordinator's deny reason, surfaced verbatim


def test_fixed_is_deterministic_across_runs() -> None:
    # Sequenced by construction → always prevents (assert the correctness
    # property, never pids or timing).
    assert all(run_fixed()["lost_update"] is False for _ in range(2))


# --- the divergence the demo exists to show --------------------------------


def test_broken_and_fixed_diverge_on_the_same_scenario() -> None:
    # Same read→edit→write-back sequence; only coherence differs.
    broken_trace = run_broken()
    fixed_trace = run_fixed()
    assert broken_trace["b_fact_survived"] is False  # lost update
    assert fixed_trace["b_fact_survived"] is True  # prevented + recovered
    assert broken_trace["scenario"] == fixed_trace["scenario"]


def test_runner_exits_zero_when_both_invariants_hold() -> None:
    # Exit 0 only if broken loses AND fixed prevents.
    assert run_main() == 0
