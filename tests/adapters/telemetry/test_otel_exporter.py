"""Tests for OtelExporter."""
from __future__ import annotations

import pytest

# Guard on `opentelemetry.sdk`, not the bare `opentelemetry` api: a `[otel]`
# install before this fix carried only `opentelemetry-api`, so importing the
# api succeeded while the `opentelemetry.sdk.*` imports below raised at
# collection time. Skipping on the sdk submodule degrades gracefully on any
# env without `opentelemetry-sdk` (e.g. a bare `[dev]` install).
pytest.importorskip("opentelemetry.sdk")

from langgraph.store.base import GetOp, PutOp
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader

from ccs.adapters.ccsstore import CCSStore
from ccs.adapters.events import StoreMetricEvent
from ccs.adapters.telemetry.otel import OtelExporter

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _provider_and_reader() -> tuple[MeterProvider, InMemoryMetricReader]:
    reader = InMemoryMetricReader()
    provider = MeterProvider(metric_readers=[reader])
    return provider, reader


def _fake_event(operation: str = "put", cache_hit: bool = False, tokens: int = 50) -> StoreMetricEvent:
    return StoreMetricEvent(
        operation=operation,
        namespace=("agent", "shared"),
        key="plan",
        agent_name="planner",
        tokens_consumed=tokens,
        cache_hit=cache_hit,
        tick=1,
    )


def _collect_metrics(reader: InMemoryMetricReader) -> dict[str, list]:
    """Return {metric_name: [data_point, ...]} from the reader."""
    metrics: dict[str, list] = {}
    data = reader.get_metrics_data()
    for resource_metric in data.resource_metrics:
        for scope_metric in resource_metric.scope_metrics:
            for metric in scope_metric.metrics:
                metrics[metric.name] = list(metric.data.data_points)
    return metrics


# ---------------------------------------------------------------------------
# Basic forwarding
# ---------------------------------------------------------------------------

def test_otel_put_records_operations_counter() -> None:
    provider, reader = _provider_and_reader()
    exporter = OtelExporter(meter_provider=provider)
    exporter.on_event(_fake_event(operation="put", tokens=100))

    metrics = _collect_metrics(reader)
    assert "ccs.store.operations" in metrics
    points = metrics["ccs.store.operations"]
    assert len(points) == 1
    assert points[0].value == 1
    assert points[0].attributes["ccs.operation"] == "put"
    assert points[0].attributes["ccs.cache_hit"] is False


def test_otel_put_records_tokens_counter() -> None:
    provider, reader = _provider_and_reader()
    exporter = OtelExporter(meter_provider=provider)
    exporter.on_event(_fake_event(operation="put", tokens=100))

    metrics = _collect_metrics(reader)
    assert "ccs.store.tokens_consumed" in metrics
    points = metrics["ccs.store.tokens_consumed"]
    assert points[0].value == 100


def test_otel_cache_hit_get_records_one_token() -> None:
    provider, reader = _provider_and_reader()
    exporter = OtelExporter(meter_provider=provider)
    exporter.on_event(_fake_event(operation="get", cache_hit=True, tokens=1))

    metrics = _collect_metrics(reader)
    token_points = metrics["ccs.store.tokens_consumed"]
    assert token_points[0].value == 1
    assert token_points[0].attributes["ccs.cache_hit"] is True


def test_otel_cache_miss_get_records_full_tokens() -> None:
    provider, reader = _provider_and_reader()
    exporter = OtelExporter(meter_provider=provider)
    exporter.on_event(_fake_event(operation="get", cache_hit=False, tokens=80))

    metrics = _collect_metrics(reader)
    token_points = metrics["ccs.store.tokens_consumed"]
    assert token_points[0].value == 80
    assert token_points[0].attributes["ccs.cache_hit"] is False


def test_otel_multiple_events_accumulate() -> None:
    provider, reader = _provider_and_reader()
    exporter = OtelExporter(meter_provider=provider)
    for _ in range(3):
        exporter.on_event(_fake_event(operation="get", tokens=50))

    metrics = _collect_metrics(reader)
    # Counter accumulates — all 3 share the same attributes so they roll into one point
    token_points = metrics["ccs.store.tokens_consumed"]
    assert sum(p.value for p in token_points) == 150


# ---------------------------------------------------------------------------
# No-op path (no SDK configured)
# ---------------------------------------------------------------------------

def test_otel_no_sdk_no_exception() -> None:
    # OtelExporter with the default global provider (no SDK in test env) must not raise
    exporter = OtelExporter()
    exporter.on_event(_fake_event())  # must complete without exception


# ---------------------------------------------------------------------------
# Integration: CCSStore + OtelExporter
# ---------------------------------------------------------------------------

def test_ccsstore_otel_records_put_and_get() -> None:
    provider, reader = _provider_and_reader()
    exporter = OtelExporter(meter_provider=provider)
    store = CCSStore(strategy="lazy", telemetry=exporter)

    store.batch([PutOp(namespace=("planner", "shared"), key="plan", value={"v": 1})])
    store.batch([GetOp(namespace=("planner", "shared"), key="plan")])

    metrics = _collect_metrics(reader)
    ops = metrics["ccs.store.operations"]
    operations = {p.attributes["ccs.operation"] for p in ops}
    assert "put" in operations
    assert "get" in operations


# ---------------------------------------------------------------------------
# tokens_saved_estimate propagation (R1 + Unit 3)
# ---------------------------------------------------------------------------

def test_store_metric_event_default_tokens_saved_is_zero() -> None:
    event = StoreMetricEvent(
        operation="get",
        namespace=("agent", "shared"),
        key="plan",
        agent_name="planner",
        tokens_consumed=50,
        cache_hit=False,
        tick=1,
    )
    assert event.tokens_saved_estimate == 0


def test_store_metric_event_tokens_saved_roundtrips() -> None:
    event = StoreMetricEvent(
        operation="get",
        namespace=("agent", "shared"),
        key="plan",
        agent_name="planner",
        tokens_consumed=1,
        cache_hit=True,
        tick=1,
        tokens_saved_estimate=99,
    )
    assert event.tokens_saved_estimate == 99


def test_otel_tokens_saved_counter_on_cache_hit() -> None:
    provider, reader = _provider_and_reader()
    exporter = OtelExporter(meter_provider=provider)
    store = CCSStore(strategy="lazy", telemetry=exporter)

    large_value = {"content": "x" * 200}
    store.batch([PutOp(namespace=("planner", "shared"), key="plan", value=large_value)])
    store.batch([GetOp(namespace=("planner", "shared"), key="plan")])  # cache hit

    metrics = _collect_metrics(reader)
    saved_points = metrics.get("ccs.store.tokens_saved_estimate", [])
    total_saved = sum(p.value for p in saved_points)
    assert total_saved > 0


def test_otel_tokens_saved_counter_zero_on_cache_miss() -> None:
    provider, reader = _provider_and_reader()
    exporter = OtelExporter(meter_provider=provider)
    store = CCSStore(strategy="lazy", telemetry=exporter)

    store.batch([PutOp(namespace=("planner", "shared"), key="plan", value={"v": 1})])
    store.batch([GetOp(namespace=("reviewer", "shared"), key="plan")])  # cache miss

    metrics = _collect_metrics(reader)
    saved_points = metrics.get("ccs.store.tokens_saved_estimate", [])
    total_saved = sum(p.value for p in saved_points)
    assert total_saved == 0


def test_otel_cache_hit_miss_counters_on_get() -> None:
    provider, reader = _provider_and_reader()
    exporter = OtelExporter(meter_provider=provider)
    store = CCSStore(strategy="lazy", telemetry=exporter)

    store.batch([PutOp(namespace=("planner", "shared"), key="plan", value={"v": 1})])
    store.batch([GetOp(namespace=("planner", "shared"), key="plan")])   # hit
    store.batch([GetOp(namespace=("reviewer", "shared"), key="plan")])  # miss

    metrics = _collect_metrics(reader)
    hits = sum(p.value for p in metrics.get("ccs.store.cache_hits", []))
    misses = sum(p.value for p in metrics.get("ccs.store.cache_misses", []))
    assert hits == 1
    assert misses == 1


def test_otel_put_does_not_increment_hit_or_miss_counters() -> None:
    provider, reader = _provider_and_reader()
    exporter = OtelExporter(meter_provider=provider)
    # Only put events — no gets
    exporter.on_event(_fake_event(operation="put", cache_hit=False, tokens=50))
    exporter.on_event(_fake_event(operation="put", cache_hit=False, tokens=50))

    metrics = _collect_metrics(reader)
    hits = sum(p.value for p in metrics.get("ccs.store.cache_hits", []))
    misses = sum(p.value for p in metrics.get("ccs.store.cache_misses", []))
    assert hits == 0
    assert misses == 0


def test_otel_degraded_mode_gauge_zero_initially() -> None:
    provider, reader = _provider_and_reader()
    exporter = OtelExporter(meter_provider=provider)
    exporter.on_event(_fake_event(operation="get", cache_hit=False))

    metrics = _collect_metrics(reader)
    gauge_points = metrics.get("ccs.store.degraded_mode", [])
    assert all(p.value == 0 for p in gauge_points)


def test_otel_degraded_mode_gauge_one_after_degraded_event() -> None:
    provider, reader = _provider_and_reader()
    exporter = OtelExporter(meter_provider=provider)
    exporter.on_event(_fake_event(operation="degraded", cache_hit=False))

    metrics = _collect_metrics(reader)
    gauge_points = metrics.get("ccs.store.degraded_mode", [])
    assert any(p.value == 1 for p in gauge_points)


def test_otel_degradation_events_counter_increments() -> None:
    provider, reader = _provider_and_reader()
    exporter = OtelExporter(meter_provider=provider)
    exporter.on_event(_fake_event(operation="degraded", cache_hit=False))
    exporter.on_event(_fake_event(operation="degraded", cache_hit=False))

    metrics = _collect_metrics(reader)
    degrade_points = metrics.get("ccs.store.degradation_events", [])
    assert sum(p.value for p in degrade_points) == 2
