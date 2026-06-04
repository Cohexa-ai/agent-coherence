# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""Aggregation utilities for multi-run coherence strategy comparisons."""

from __future__ import annotations

import statistics
from dataclasses import asdict, dataclass
from typing import Any, Iterable, Sequence

from .metrics import SimulationMetrics


def _mean(values: Sequence[float]) -> float:
    return statistics.mean(values) if values else 0.0


def _std(values: Sequence[float]) -> float:
    if len(values) <= 1:
        return 0.0
    return statistics.pstdev(values)


@dataclass(frozen=True)
class AggregatedMetrics:
    """Aggregated per-strategy metrics across repeated runs."""

    strategy: str
    runs: int
    synchronization_tokens_mean: float
    synchronization_tokens_std: float
    fetch_tokens_mean: float
    fetch_tokens_std: float
    broadcast_tokens_mean: float
    broadcast_tokens_std: float
    invalidation_tokens_mean: float
    invalidation_tokens_std: float
    cache_hit_rate_mean: float
    cache_hit_rate_std: float
    stale_reads_mean: float
    stale_reads_std: float
    max_stale_steps_mean: float
    max_stale_steps_std: float
    staleness_bound_violations_mean: float
    staleness_bound_violations_std: float
    sync_broadcast_ratio_mean: float
    sync_broadcast_ratio_std: float
    invalidation_efficiency_mean: float
    invalidation_efficiency_std: float
    message_overhead_mean: float
    message_overhead_std: float
    source_refetches_mean: float
    source_refetches_std: float
    wasted_refetches_mean: float
    wasted_refetches_std: float

    def to_dict(self) -> dict[str, Any]:
        """Return JSON-safe dictionary payload."""
        return asdict(self)


def aggregate_strategy_runs(strategy: str, runs: Sequence[SimulationMetrics]) -> AggregatedMetrics:
    """Aggregate one strategy across repeated simulation runs."""
    return AggregatedMetrics(
        strategy=strategy,
        runs=len(runs),
        synchronization_tokens_mean=_mean([float(m.synchronization_tokens) for m in runs]),
        synchronization_tokens_std=_std([float(m.synchronization_tokens) for m in runs]),
        fetch_tokens_mean=_mean([float(m.tokens_fetch) for m in runs]),
        fetch_tokens_std=_std([float(m.tokens_fetch) for m in runs]),
        broadcast_tokens_mean=_mean([float(m.tokens_broadcast) for m in runs]),
        broadcast_tokens_std=_std([float(m.tokens_broadcast) for m in runs]),
        invalidation_tokens_mean=_mean([float(m.tokens_invalidation) for m in runs]),
        invalidation_tokens_std=_std([float(m.tokens_invalidation) for m in runs]),
        cache_hit_rate_mean=_mean([m.cache_hit_rate for m in runs]),
        cache_hit_rate_std=_std([m.cache_hit_rate for m in runs]),
        stale_reads_mean=_mean([float(m.stale_reads) for m in runs]),
        stale_reads_std=_std([float(m.stale_reads) for m in runs]),
        max_stale_steps_mean=_mean([float(m.max_stale_steps) for m in runs]),
        max_stale_steps_std=_std([float(m.max_stale_steps) for m in runs]),
        staleness_bound_violations_mean=_mean([float(m.staleness_bound_violations) for m in runs]),
        staleness_bound_violations_std=_std([float(m.staleness_bound_violations) for m in runs]),
        sync_broadcast_ratio_mean=_mean([m.sync_broadcast_ratio for m in runs]),
        sync_broadcast_ratio_std=_std([m.sync_broadcast_ratio for m in runs]),
        invalidation_efficiency_mean=_mean([m.invalidation_efficiency for m in runs]),
        invalidation_efficiency_std=_std([m.invalidation_efficiency for m in runs]),
        message_overhead_mean=_mean([float(m.message_overhead) for m in runs]),
        message_overhead_std=_std([float(m.message_overhead) for m in runs]),
        source_refetches_mean=_mean([float(m.source_refetches) for m in runs]),
        source_refetches_std=_std([float(m.source_refetches) for m in runs]),
        wasted_refetches_mean=_mean([float(m.wasted_refetches) for m in runs]),
        wasted_refetches_std=_std([float(m.wasted_refetches) for m in runs]),
    )


def aggregate_comparison_runs(
    metrics_by_strategy: dict[str, Sequence[SimulationMetrics]],
) -> list[AggregatedMetrics]:
    """Aggregate all strategies preserving input order."""
    return [
        aggregate_strategy_runs(strategy, runs)
        for strategy, runs in metrics_by_strategy.items()
    ]


def flatten_metrics(metrics_by_strategy: dict[str, Iterable[SimulationMetrics]]) -> list[SimulationMetrics]:
    """Flatten grouped strategy metrics into one ordered list."""
    flattened: list[SimulationMetrics] = []
    for runs in metrics_by_strategy.values():
        flattened.extend(list(runs))
    return flattened
