# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Simulation metrics payloads for coherence strategy evaluation."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any


@dataclass(frozen=True)
class SimulationMetrics:
    """Single-run coherence metrics used for comparisons and reports."""

    scenario: str
    strategy: str
    seed: int
    duration_ticks: int
    agent_count: int
    artifact_count: int
    total_actions: int
    read_actions: int
    write_actions: int
    fetch_actions: int
    cache_hits: int
    cache_misses: int
    stale_reads: int
    max_stale_steps: int
    staleness_bound_violations: int
    swmr_violations: int
    monotonic_version_violations: int
    invalidations_issued: int
    invalidations_delivered: int
    updates_issued: int
    updates_delivered: int
    message_overhead: int
    tokens_fetch: int
    tokens_broadcast: int
    tokens_invalidation: int
    context_injections: int
    transient_state_timeouts: int = 0
    stable_grant_reclamations: int = 0
    source_refetches: int = 0
    wasted_refetches: int = 0

    @property
    def synchronization_tokens(self) -> int:
        """Total synchronization token volume."""
        return self.tokens_fetch + self.tokens_broadcast + self.tokens_invalidation

    @property
    def cache_hit_rate(self) -> float:
        """Read-hit ratio across all read actions."""
        if self.read_actions == 0:
            return 0.0
        return self.cache_hits / float(self.read_actions)

    @property
    def invalidation_efficiency(self) -> float:
        """Delivery ratio for issued invalidation signals."""
        if self.invalidations_issued == 0:
            return 1.0
        return self.invalidations_delivered / float(self.invalidations_issued)

    @property
    def sync_broadcast_ratio(self) -> float:
        """Synchronization broadcast ratio: tokens_broadcast / total_sync_tokens.

        This is distinct from the paper's CTX-RR term (Coherence Reduction Ratio).
        The field was renamed from ``ctx_rr`` to avoid abbreviation collision.
        """
        total = self.synchronization_tokens
        if total == 0:
            return 0.0
        return self.tokens_broadcast / float(total)

    def to_dict(self) -> dict[str, Any]:
        """Return JSON-safe dictionary including derived fields."""
        payload = asdict(self)
        payload["synchronization_tokens"] = self.synchronization_tokens
        payload["cache_hit_rate"] = self.cache_hit_rate
        payload["invalidation_efficiency"] = self.invalidation_efficiency
        payload["sync_broadcast_ratio"] = self.sync_broadcast_ratio
        return payload


@dataclass(frozen=True)
class StrategyComparisonReport:
    """Structured JSON report contract for dashboard/reporting consumers."""

    scenario: str
    runs_per_strategy: int
    seed_start: int
    strategies: list[str]
    runs: list[SimulationMetrics]
    aggregated: list[dict[str, Any]]

    def to_dict(self) -> dict[str, Any]:
        """Return JSON-safe report payload."""
        return {
            "scenario": self.scenario,
            "runs_per_strategy": self.runs_per_strategy,
            "seed_start": self.seed_start,
            "strategies": list(self.strategies),
            "runs": [m.to_dict() for m in self.runs],
            "aggregated": list(self.aggregated),
        }
