# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Shared event types and telemetry protocol for CCSStore adapters.

Kept in a leaf module so ccsstore.py, telemetry/__init__.py, and concrete
exporter implementations (otel.py, langsmith.py) can all import from here
without creating circular dependencies.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass

CCS_METRIC_SCHEMA_VERSION = "ccs.metric.v1"


@dataclass
class StoreMetricEvent:
    """Emitted by CCSStore for each artifact operation when on_metric is set.

    sequence_number, instance_id, schema_version are None when the event was
    not emitted by a sequenced CCSStore producer (e.g. direct construction in
    tests or third-party code). validate_log will raise ValueError on None values
    since the fields are required for gap detection.
    """

    operation: str
    namespace: tuple[str, ...]
    key: str
    agent_name: str
    tokens_consumed: int
    cache_hit: bool
    tick: int
    tokens_saved_estimate: int = 0
    sequence_number: int | None = None
    instance_id: str | None = None
    schema_version: str | None = None


class TelemetryExporter(ABC):
    """Contract for CCSStore telemetry backends."""

    @abstractmethod
    def on_event(self, event: StoreMetricEvent) -> None:
        """Called after each CCSStore operation."""


class NoOpTelemetryExporter(TelemetryExporter):
    """Default exporter — discards all events at zero cost."""

    def on_event(self, event: StoreMetricEvent) -> None:
        pass
