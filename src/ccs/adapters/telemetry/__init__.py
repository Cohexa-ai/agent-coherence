# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Telemetry exporter protocol and dispatcher for CCSStore.

Re-exports TelemetryExporter, NoOpTelemetryExporter, and StoreMetricEvent from
ccs.adapters.events so callers only need to import from this package.
"""

from __future__ import annotations

# Public re-exports — concrete implementations live in events.py (leaf node)
from ccs.adapters.events import NoOpTelemetryExporter, StoreMetricEvent, TelemetryExporter

__all__ = ["TelemetryExporter", "NoOpTelemetryExporter", "StoreMetricEvent", "build_telemetry"]


def build_telemetry(spec: str | TelemetryExporter | None) -> TelemetryExporter:
    """Instantiate the right TelemetryExporter from a string shorthand or explicit instance.

    Args:
        spec: None, "opentelemetry", "langsmith", or a TelemetryExporter instance.

    Returns:
        A TelemetryExporter ready for use by CCSStore.

    Raises:
        ImportError: If the required optional package is not installed.
        TypeError: If spec is not a recognised type.
    """
    if spec is None:
        return NoOpTelemetryExporter()

    if isinstance(spec, TelemetryExporter):
        return spec

    if isinstance(spec, str):
        if spec == "opentelemetry":
            try:
                from ccs.adapters.telemetry.otel import OtelExporter  # noqa: PLC0415
            except ImportError as exc:
                raise ImportError(
                    "opentelemetry-api is required for telemetry='opentelemetry'. "
                    'Install it with: pip install "agent-coherence[otel]"'
                ) from exc
            return OtelExporter()

        if spec == "langsmith":
            try:
                from ccs.adapters.telemetry.langsmith import LangSmithExporter  # noqa: PLC0415
            except ImportError as exc:
                raise ImportError(
                    "langsmith is required for telemetry='langsmith'. "
                    'Install it with: pip install "agent-coherence[langsmith]"'
                ) from exc
            return LangSmithExporter()

        raise TypeError(
            f"Unknown telemetry shorthand {spec!r}. "
            "Use 'opentelemetry', 'langsmith', a TelemetryExporter instance, or None."
        )

    raise TypeError(
        f"telemetry must be a string shorthand, a TelemetryExporter instance, or None; "
        f"got {type(spec).__name__!r}"
    )
