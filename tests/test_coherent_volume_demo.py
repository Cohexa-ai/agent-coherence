# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""Offline tests for the CoherentVolume stale-overwrite demo (Unit 4).

Sequenced, deterministic: the broken case proves a write from an older read
clobbers a peer's update; the fixed case proves CoherentVolume denies that stale
write (fail-closed) and recovers via reacquire() with no loss. The fixed case
spawns a local coordinator subprocess (no network), so these double as the
end-to-end check for the demo.
"""

from __future__ import annotations

from examples.coherent_volume.broken import run_broken
from examples.coherent_volume.fixed import run_fixed

# --- broken case (no coherence) --------------------------------------------


def test_broken_loses_the_update() -> None:
    trace = run_broken()
    assert trace["lost_update"] is True
    assert trace["final_total"] == 105  # B (from its older read of 100) clobbered A's 110
    assert trace["expected_total"] == 115  # both updates should have survived


def test_broken_is_deterministic_across_runs() -> None:
    # Sequenced, not raced — the lost update must reproduce every time.
    assert all(run_broken()["final_total"] == 105 for _ in range(10))


# --- fixed case (CoherentVolume explicit API) ------------------------------


def test_fixed_denies_stale_write_then_recovers() -> None:
    trace = run_fixed()
    assert trace["b_write_denied"] is True  # the stale write was denied fail-closed
    assert trace["b_recovered_via_reacquire"] is True  # recovery via reacquire()
    assert trace["lost_update"] is False
    assert trace["final_total"] == 115  # both updates survived
    assert trace["denial_reason"]  # the coordinator's deny reason, surfaced verbatim


def test_fixed_is_deterministic_across_runs() -> None:
    # Sequenced by construction → always prevents (assert the correctness
    # property, the final total — never pids or timing).
    assert all(run_fixed()["final_total"] == 115 for _ in range(2))


# --- the divergence the demo exists to show --------------------------------


def test_broken_and_fixed_diverge_on_the_same_scenario() -> None:
    # Same read→write sequence and starting value; only coherence differs.
    broken_trace = run_broken()
    fixed_trace = run_fixed()
    assert broken_trace["final_total"] == 105  # lost update
    assert fixed_trace["final_total"] == 115  # prevented + recovered
    assert broken_trace["scenario"] == fixed_trace["scenario"]
