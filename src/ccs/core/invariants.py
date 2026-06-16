# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Invariant helpers for coherence runtime checks."""

from __future__ import annotations

from typing import Mapping

from .exceptions import InvariantViolationError
from .states import MESIState


def check_single_writer(state_by_agent: Mapping[object, MESIState]) -> None:
    """Ensure at most one agent is in MODIFIED or EXCLUSIVE for an artifact."""
    owners = [
        agent_id
        for agent_id, state in state_by_agent.items()
        if state in {MESIState.MODIFIED, MESIState.EXCLUSIVE}
    ]
    if len(owners) > 1:
        raise InvariantViolationError(
            f"single_writer_violated owners={owners}"
        )


def check_monotonic_version(previous_version: int, current_version: int) -> None:
    """Ensure artifact version never decreases."""
    if current_version < previous_version:
        raise InvariantViolationError(
            f"version_regressed previous={previous_version} current={current_version}"
        )


def check_bounded_staleness(steps_on_stale: int, max_steps: int) -> None:
    """Ensure stale-use steps stay within configured bound."""
    if steps_on_stale > max_steps:
        raise InvariantViolationError(
            f"staleness_bound_exceeded steps={steps_on_stale} max_steps={max_steps}"
        )
