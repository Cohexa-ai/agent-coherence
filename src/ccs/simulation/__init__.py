# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Simulation and benchmarking helpers."""

from .aggregation import AggregatedMetrics, aggregate_comparison_runs, aggregate_strategy_runs
from .engine import SimulationEngine, run_strategy_comparison, run_strategy_range
from .metrics import SimulationMetrics, StrategyComparisonReport

__all__ = [
    "SimulationEngine",
    "SimulationMetrics",
    "StrategyComparisonReport",
    "AggregatedMetrics",
    "run_strategy_range",
    "run_strategy_comparison",
    "aggregate_strategy_runs",
    "aggregate_comparison_runs",
]
