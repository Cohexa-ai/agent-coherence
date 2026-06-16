# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Tests for report rendering and dashboard payload generation."""

from __future__ import annotations

from ccs.output.report import build_dashboard_payload, render_html_report
from ccs.simulation.metrics import SimulationMetrics, StrategyComparisonReport


def _metrics() -> list[SimulationMetrics]:
    return [
        SimulationMetrics(
            scenario="demo",
            strategy="lazy",
            seed=1,
            duration_ticks=10,
            agent_count=2,
            artifact_count=1,
            total_actions=20,
            read_actions=14,
            write_actions=6,
            fetch_actions=8,
            cache_hits=6,
            cache_misses=8,
            stale_reads=2,
            max_stale_steps=1,
            staleness_bound_violations=0,
            swmr_violations=0,
            monotonic_version_violations=0,
            invalidations_issued=4,
            invalidations_delivered=4,
            updates_issued=0,
            updates_delivered=0,
            message_overhead=4,
            tokens_fetch=3200,
            tokens_broadcast=0,
            tokens_invalidation=48,
            context_injections=8,
        )
    ]


def test_build_dashboard_payload_has_schema_version() -> None:
    report = StrategyComparisonReport(
        scenario="demo",
        runs_per_strategy=1,
        seed_start=1,
        strategies=["lazy"],
        runs=_metrics(),
        aggregated=[
            {
                "strategy": "lazy",
                "synchronization_tokens_mean": 3248.0,
                "fetch_tokens_mean": 3200.0,
                "broadcast_tokens_mean": 0.0,
                "invalidation_tokens_mean": 48.0,
                "cache_hit_rate_mean": 0.4,
                "stale_reads_mean": 2.0,
                "max_stale_steps_mean": 1.0,
                "sync_broadcast_ratio_mean": 0.0,
            }
        ],
    )

    payload = build_dashboard_payload(report)
    assert payload["schema_version"] == "ccs.report.v1"
    assert payload["report"]["scenario"] == "demo"


def test_render_html_report_contains_summary_and_json() -> None:
    report = StrategyComparisonReport(
        scenario="demo",
        runs_per_strategy=1,
        seed_start=1,
        strategies=["lazy"],
        runs=_metrics(),
        aggregated=[
            {
                "strategy": "lazy",
                "synchronization_tokens_mean": 3248.0,
                "fetch_tokens_mean": 3200.0,
                "broadcast_tokens_mean": 0.0,
                "invalidation_tokens_mean": 48.0,
                "cache_hit_rate_mean": 0.4,
                "stale_reads_mean": 2.0,
                "max_stale_steps_mean": 1.0,
                "sync_broadcast_ratio_mean": 0.0,
            }
        ],
    )

    html = render_html_report(report)
    assert "CCS Strategy Comparison" in html
    assert "demo" in html
    assert "lazy" in html
    assert "ccs.report.v1" in html
