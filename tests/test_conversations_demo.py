# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Offline tests for the client-cache stale-read demo (Unit 2, pivoted).

Deterministic, no network: the broken case proves a local cache goes stale over
a consistent store; the fixed case proves CoherenceAdapterCore invalidates that
cache so the reader re-fetches. Both run in the default ``pytest -q`` loop.
"""

from __future__ import annotations

from examples.conversations_stale_read.broken import run_broken
from examples.conversations_stale_read.fixed import run_fixed

# --- broken case (no coherence) --------------------------------------------


def test_broken_agent_acts_on_stale_cache():
    trace = run_broken()
    assert trace["stale"] is True
    assert trace["store_version"] == 2  # store is consistent at v2
    assert trace["b_decision_version"] == 1  # B acted on stale cached v1
    assert trace["b_decision"] == "execute plan v1"


def test_broken_is_deterministic_across_runs():
    # The pathology must reproduce every time, not "sometimes" (demo credibility).
    assert all(run_broken()["stale"] is True for _ in range(10))


# --- fixed case (CoherenceAdapterCore) -------------------------------------


def test_fixed_invalidates_cache_and_refetches_fresh():
    trace = run_fixed()
    assert trace["b_first_read"] == "plan_version=1"
    assert trace["b_invalidated"] is True  # peer write invalidated B's cache
    assert trace["b_second_read"] == "plan_version=2"
    assert trace["fresh"] is True


def test_fixed_is_deterministic_across_runs():
    assert all(run_fixed()["fresh"] is True for _ in range(10))


# --- the divergence the demo exists to show --------------------------------


def test_broken_and_fixed_diverge_on_the_same_scenario():
    # Same write-after-read scenario; only coherence differs.
    broken_trace = run_broken()
    fixed_trace = run_fixed()
    assert broken_trace["stale"] is True  # B acts on v1
    assert fixed_trace["fresh"] is True  # B acts on v2
