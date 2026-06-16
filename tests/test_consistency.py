# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Tests for simulation consistency monitor."""

from __future__ import annotations

from uuid import UUID

from ccs.core.states import MESIState
from ccs.simulation.consistency import ConsistencyMonitor
from ccs.strategies.access_count import AccessCountStrategy


def test_monitor_tracks_stale_reads_and_bound_violations() -> None:
    monitor = ConsistencyMonitor(strategy=AccessCountStrategy(max_accesses=2))
    agent = UUID(int=1)
    artifact = UUID(int=100)

    monitor.record_read(agent_id=agent, artifact_id=artifact, stale=True)
    monitor.record_read(agent_id=agent, artifact_id=artifact, stale=True)
    monitor.record_read(agent_id=agent, artifact_id=artifact, stale=True)

    assert monitor.stale_reads == 3
    assert monitor.max_stale_steps == 3
    assert monitor.staleness_bound_violations == 1


def test_monitor_resets_counter_on_fresh_read() -> None:
    monitor = ConsistencyMonitor(strategy=AccessCountStrategy(max_accesses=3))
    agent = UUID(int=2)
    artifact = UUID(int=101)

    monitor.record_read(agent_id=agent, artifact_id=artifact, stale=True)
    monitor.record_read(agent_id=agent, artifact_id=artifact, stale=False)
    monitor.record_read(agent_id=agent, artifact_id=artifact, stale=True)

    assert monitor.max_stale_steps == 1


def test_monitor_captures_swmr_and_monotonic_violations() -> None:
    monitor = ConsistencyMonitor(strategy=AccessCountStrategy(max_accesses=3))
    monitor.validate_single_writer(
        {
            UUID(int=1): MESIState.MODIFIED,
            UUID(int=2): MESIState.EXCLUSIVE,
        }
    )
    monitor.validate_monotonic(previous_version=5, current_version=4)

    assert monitor.swmr_violations == 1
    assert monitor.monotonic_version_violations == 1
