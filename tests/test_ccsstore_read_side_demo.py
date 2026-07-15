# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Offline tests for the CCSStore read-side demo.

The demo's job is to make the read/write split legible: Act 1 proves the
read-side invalidation guarantee (a peer commit turns the next get into a
fresh miss serving the new version), Act 2 proves the documented boundary
(put() is not version-CAS — a stale write-back lands), and Act 3 proves the
write-side fix one call away (store.core.write_cas re-applies intent against
the fresh version, nothing lost). In-process, deterministic, no network.
"""

from __future__ import annotations

from examples.ccsstore_read_side.demo import (
    main,
    run_broken_writeback,
    run_fixed_writeback,
    run_readside,
)

# --- Act 1: read-side invalidation (the guarantee) --------------------------


def test_repeat_read_is_a_local_cache_hit() -> None:
    trace = run_readside()
    assert trace["first_was_miss"] is True
    assert trace["second_was_local_hit"] is True


def test_peer_commit_invalidates_and_next_get_serves_new_version() -> None:
    trace = run_readside()
    assert trace["post_commit_was_fresh_miss"] is True
    assert trace["saw_new_version"] is True
    assert trace["value_after_peer_commit"]["status"] == "approved-in-review"


# --- Act 2: the boundary (put is not version-CAS) ----------------------------


def test_stale_writeback_lands_and_erases_the_peer_update() -> None:
    trace = run_broken_writeback()
    assert trace["planner_update_lost"] is True
    assert trace["reviewer_edit_present"] is True
    assert "budget" not in trace["final_value"]


# --- Act 3: the fix (core.write_cas) -----------------------------------------


def test_write_cas_preserves_both_updates() -> None:
    trace = run_fixed_writeback()
    assert trace["planner_update_survived"] is True
    assert trace["reviewer_edit_present"] is True
    assert trace["final_value"]["budget"] == "approved"
    assert trace["final_value"]["owner"] == "reviewer"


# --- The demo doubles as a CI gate -------------------------------------------


def test_demo_main_exits_zero(capsys) -> None:
    assert main() == 0
    out = capsys.readouterr().out
    assert "ALL INVARIANTS HELD" in out
