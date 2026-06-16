# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""LangSmith run metadata exporter for CCSStore operations."""

from __future__ import annotations

import logging

from ccs.adapters.events import StoreMetricEvent, TelemetryExporter

logger = logging.getLogger(__name__)


class LangSmithExporter(TelemetryExporter):
    """Attaches CCSStore operation metadata to the active LangSmith run.

    Requires the ``langsmith`` package (``pip install "agent-coherence[langsmith]"``).
    If no active LangSmith run is found, events are silently discarded.
    Accumulators update on every event regardless of run availability.

    Usage::

        store = CCSStore(strategy="lazy", telemetry="langsmith")
        # or with an explicit project:
        from ccs.adapters.telemetry.langsmith import LangSmithExporter
        store = CCSStore(strategy="lazy", telemetry=LangSmithExporter())
    """

    def __init__(self) -> None:
        self._hits: int = 0
        self._misses: int = 0
        self._tokens_saved: int = 0
        self._baseline: int = 0

    def on_event(self, event: StoreMetricEvent) -> None:
        # Update accumulators before checking run availability.
        self._baseline += event.tokens_consumed + event.tokens_saved_estimate
        self._tokens_saved += event.tokens_saved_estimate
        if event.operation == "get":
            if event.cache_hit:
                self._hits += 1
            else:
                self._misses += 1

        try:
            from langsmith import run_helpers  # noqa: PLC0415
        except ImportError:
            return

        run = run_helpers.get_current_run_tree()
        if run is None:
            return

        total = self._hits + self._misses
        token_reduction_pct = (
            round(self._tokens_saved / self._baseline * 100, 1)
            if self._baseline > 0
            else 0.0
        )
        cache_hit_rate = round(self._hits / total, 3) if total > 0 else 0.0

        run.add_metadata(
            {
                "ccs.operation": event.operation,
                "ccs.agent_name": event.agent_name,
                "ccs.tokens_consumed": event.tokens_consumed,
                "ccs.cache_hit": event.cache_hit,
                "ccs.tick": event.tick,
                "ccs.token_reduction_pct": token_reduction_pct,
                "ccs.tokens_saved_estimate": self._tokens_saved,
                "ccs.cache_hit_rate": cache_hit_rate,
            }
        )
