# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""Offline tests for the concurrent lost-update demo (Epic Piece #6).

True-concurrency, not sequenced: the broken case proves two racing writers lose
an update with no coordination; the fixed case proves CoherentVolume.write_cas
(commit-CAS, v0.9.1) preserves every update under the same race — the loser is
told it lost and re-applies. The winner is non-deterministic; the asserted
property is the invariant (final total = start + the sum of deltas, no silent
loss), never timing or which writer won. The fixed case spawns a local
coordinator subprocess (no network).
"""

from __future__ import annotations

from examples.concurrent_writers.broken import run_broken
from examples.concurrent_writers.fixed import run_fixed

# --- broken case (no coherence) --------------------------------------------


def test_broken_loses_an_update_under_concurrency() -> None:
    trace = run_broken()
    assert trace["lost_update"] is True
    assert trace["expected_total"] == 115  # both updates should have survived
    assert trace["final_total"] in (105, 110)  # exactly one delta survived


def test_broken_always_loses_across_runs() -> None:
    # The read-barrier makes the outcome class deterministic: an update is lost
    # every run (which writer wins is timing; that one is lost is not).
    assert all(run_broken()["lost_update"] is True for _ in range(10))


# --- fixed case (CoherentVolume.write_cas / commit-CAS) --------------------


def test_fixed_preserves_both_updates_under_concurrency() -> None:
    trace = run_fixed()
    assert trace["lost_update"] is False
    assert trace["final_total"] == 115  # both updates survived the race
    assert trace["expected_total"] == 115
    # make_content runs at least once per writer; a winner-take-all run needs no
    # retry, so we assert the floor, not the conflict (timing-dependent).
    assert trace["attempts_a"] >= 1
    assert trace["attempts_b"] >= 1


def test_fixed_invariant_holds_across_runs() -> None:
    # The winner varies run to run; the invariant must not.
    assert all(run_fixed()["final_total"] == 115 for _ in range(3))


# --- the divergence the demo exists to show --------------------------------


def test_broken_and_fixed_diverge_on_the_same_race() -> None:
    # Same concurrent read→write race and starting value; only coherence differs.
    broken_trace = run_broken()
    fixed_trace = run_fixed()
    assert broken_trace["lost_update"] is True  # update lost
    assert fixed_trace["lost_update"] is False  # both survived
    assert broken_trace["scenario"] == fixed_trace["scenario"]
