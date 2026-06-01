"""Tests for the telemetry dispatcher and base types."""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from ccs.adapters.telemetry import (
    NoOpTelemetryExporter,
    TelemetryExporter,
    build_telemetry,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _fake_event():
    """Return a minimal StoreMetricEvent-like object for testing."""
    event = MagicMock()
    event.operation = "get"
    event.agent_name = "test_agent"
    event.tokens_consumed = 1
    event.cache_hit = True
    event.tick = 1
    return event


# ---------------------------------------------------------------------------
# build_telemetry — None
# ---------------------------------------------------------------------------

def test_build_telemetry_none_returns_noop():
    result = build_telemetry(None)
    assert isinstance(result, NoOpTelemetryExporter)


def test_build_telemetry_none_is_noop_exporter():
    result = build_telemetry(None)
    assert isinstance(result, TelemetryExporter)


# ---------------------------------------------------------------------------
# build_telemetry — explicit instance passthrough
# ---------------------------------------------------------------------------

def test_build_telemetry_exporter_instance_returned_as_is():
    exporter = NoOpTelemetryExporter()
    result = build_telemetry(exporter)
    assert result is exporter


def test_build_telemetry_custom_exporter_passthrough():
    class MyExporter(TelemetryExporter):
        def on_event(self, event):
            pass

    exporter = MyExporter()
    assert build_telemetry(exporter) is exporter


# ---------------------------------------------------------------------------
# build_telemetry — string shorthands (happy paths require packages installed)
# ---------------------------------------------------------------------------

def test_build_telemetry_opentelemetry_string():
    pytest.importorskip("opentelemetry")
    result = build_telemetry("opentelemetry")
    from ccs.adapters.telemetry.otel import OtelExporter
    assert isinstance(result, OtelExporter)


def test_build_telemetry_langsmith_string():
    pytest.importorskip("langsmith")
    result = build_telemetry("langsmith")
    from ccs.adapters.telemetry.langsmith import LangSmithExporter
    assert isinstance(result, LangSmithExporter)


# ---------------------------------------------------------------------------
# build_telemetry — error paths
# ---------------------------------------------------------------------------

def test_build_telemetry_unknown_string_raises_type_error():
    with pytest.raises(TypeError, match="Unknown telemetry shorthand"):
        build_telemetry("prometheus")


def test_build_telemetry_int_raises_type_error():
    with pytest.raises(TypeError, match="telemetry must be"):
        build_telemetry(42)  # type: ignore[arg-type]


def test_build_telemetry_list_raises_type_error():
    with pytest.raises(TypeError):
        build_telemetry([])  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# NoOpTelemetryExporter
# ---------------------------------------------------------------------------

def test_noop_on_event_no_exception():
    exporter = NoOpTelemetryExporter()
    exporter.on_event(_fake_event())  # must not raise


def test_noop_on_event_no_side_effect():
    exporter = NoOpTelemetryExporter()
    # Call multiple times — still no state change
    for _ in range(5):
        exporter.on_event(_fake_event())
