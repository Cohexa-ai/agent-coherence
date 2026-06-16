# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Runtime consistency monitor for coherence simulations."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Mapping
from uuid import UUID

from ccs.core.exceptions import InvariantViolationError
from ccs.core.invariants import check_monotonic_version, check_single_writer
from ccs.core.states import MESIState
from ccs.strategies.base import SyncStrategy

from .bounds import enforce_strategy_staleness_bound


@dataclass
class ConsistencyMonitor:
    """Tracks invariant violations and stale-step behavior during a run."""

    strategy: SyncStrategy
    stale_reads: int = 0
    max_stale_steps: int = 0
    staleness_bound_violations: int = 0
    swmr_violations: int = 0
    monotonic_version_violations: int = 0
    _stale_steps: dict[tuple[UUID, UUID], int] = field(default_factory=dict)

    def validate_single_writer(self, state_by_agent: Mapping[UUID, MESIState]) -> None:
        """Validate SWMR and capture violations instead of raising."""
        try:
            check_single_writer(state_by_agent)
        except InvariantViolationError:
            self.swmr_violations += 1

    def validate_monotonic(self, previous_version: int, current_version: int) -> None:
        """Validate monotonic versioning and capture violations instead of raising."""
        try:
            check_monotonic_version(previous_version, current_version)
        except InvariantViolationError:
            self.monotonic_version_violations += 1

    def record_read(self, *, agent_id: UUID, artifact_id: UUID, stale: bool) -> None:
        """Track stale-step counters for one read action."""
        key = (agent_id, artifact_id)
        if not stale:
            self._stale_steps[key] = 0
            return

        self.stale_reads += 1
        steps = self._stale_steps.get(key, 0) + 1
        self._stale_steps[key] = steps
        self.max_stale_steps = max(self.max_stale_steps, steps)

        try:
            enforce_strategy_staleness_bound(self.strategy, steps_on_stale=steps)
        except InvariantViolationError:
            self.staleness_bound_violations += 1

    def reset_stale_steps(self, *, agent_id: UUID, artifact_id: UUID) -> None:
        """Reset stale-step counter after refresh/invalidation."""
        self._stale_steps[(agent_id, artifact_id)] = 0
