# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Unit tests for multi-run coherence aggregation helpers."""

from __future__ import annotations

from ccs.simulation.aggregation import aggregate_comparison_runs, aggregate_strategy_runs, flatten_metrics
from ccs.simulation.metrics import SimulationMetrics


def _metric(
    strategy: str,
    *,
    fetch: int,
    broadcast: int,
    stale_reads: int,
    source_refetches: int = 0,
    wasted_refetches: int = 0,
) -> SimulationMetrics:
    return SimulationMetrics(
        scenario="tiny",
        strategy=strategy,
        seed=1,
        duration_ticks=10,
        agent_count=2,
        artifact_count=1,
        total_actions=10,
        read_actions=8,
        write_actions=2,
        fetch_actions=3,
        cache_hits=5,
        cache_misses=3,
        stale_reads=stale_reads,
        max_stale_steps=stale_reads,
        staleness_bound_violations=0,
        swmr_violations=0,
        monotonic_version_violations=0,
        invalidations_issued=2,
        invalidations_delivered=2,
        updates_issued=0,
        updates_delivered=0,
        message_overhead=2,
        tokens_fetch=fetch,
        tokens_broadcast=broadcast,
        tokens_invalidation=24,
        context_injections=3,
        transient_state_timeouts=0,
        source_refetches=source_refetches,
        wasted_refetches=wasted_refetches,
    )


def test_aggregate_strategy_runs() -> None:
    runs = [
        _metric("lazy", fetch=100, broadcast=0, stale_reads=1),
        _metric("lazy", fetch=300, broadcast=0, stale_reads=3),
    ]
    agg = aggregate_strategy_runs("lazy", runs)

    assert agg.strategy == "lazy"
    assert agg.runs == 2
    assert agg.fetch_tokens_mean == 200.0
    assert agg.broadcast_tokens_mean == 0.0
    assert agg.stale_reads_mean == 2.0


def test_aggregate_reports_source_and_wasted_refetch_means() -> None:
    runs = [
        _metric("lazy", fetch=100, broadcast=0, stale_reads=1, source_refetches=4, wasted_refetches=1),
        _metric("lazy", fetch=300, broadcast=0, stale_reads=3, source_refetches=8, wasted_refetches=3),
    ]
    agg = aggregate_strategy_runs("lazy", runs)

    assert agg.source_refetches_mean == 6.0
    assert agg.wasted_refetches_mean == 2.0
    # pstdev over [4, 8] is 2.0; over [1, 3] is 1.0.
    assert agg.source_refetches_std == 2.0
    assert agg.wasted_refetches_std == 1.0
    # New fields are part of the JSON-safe payload.
    payload = agg.to_dict()
    assert payload["source_refetches_mean"] == 6.0
    assert payload["wasted_refetches_mean"] == 2.0


def test_aggregate_comparison_and_flatten() -> None:
    grouped = {
        "eager": [_metric("eager", fetch=50, broadcast=200, stale_reads=0)],
        "lazy": [
            _metric("lazy", fetch=120, broadcast=0, stale_reads=1),
            _metric("lazy", fetch=180, broadcast=0, stale_reads=2),
        ],
    }
    flattened = flatten_metrics(grouped)
    assert len(flattened) == 3

    aggregated = aggregate_comparison_runs(grouped)
    assert [a.strategy for a in aggregated] == ["eager", "lazy"]
    assert aggregated[0].broadcast_tokens_mean == 200.0
    assert aggregated[1].fetch_tokens_mean == 150.0
