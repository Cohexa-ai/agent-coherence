# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""OpenTelemetry metrics exporter for CCSStore operations."""

from __future__ import annotations

from opentelemetry import metrics
from opentelemetry.metrics import Counter, Meter, ObservableGauge

from ccs.adapters.events import StoreMetricEvent, TelemetryExporter

# Package version for meter instrumentation scope
_INSTRUMENTATION_VERSION = "0.2.0"


class OtelExporter(TelemetryExporter):
    """Forwards CCSStore StoreMetricEvents to OpenTelemetry Counter instruments.

    Uses the globally configured MeterProvider unless an explicit one is injected.
    If no SDK is configured, the OTel no-op path discards all records at zero cost.

    Usage::

        # Zero-config (uses global OTel provider if configured)
        store = CCSStore(strategy="lazy", telemetry="opentelemetry")

        # Explicit provider (useful for testing)
        from ccs.adapters.telemetry.otel import OtelExporter
        store = CCSStore(strategy="lazy", telemetry=OtelExporter(meter_provider=my_provider))
    """

    def __init__(self, meter_provider: metrics.MeterProvider | None = None) -> None:
        provider = meter_provider or metrics.get_meter_provider()
        self._meter: Meter = provider.get_meter(
            "ccs.adapters.ccsstore",
            version=_INSTRUMENTATION_VERSION,
        )
        # Instruments are singletons per meter — create once, not per event.
        self._ops_counter: Counter = self._meter.create_counter(
            name="ccs.store.operations",
            unit="{operation}",
            description="Count of CCSStore operations",
        )
        self._tokens_counter: Counter = self._meter.create_counter(
            name="ccs.store.tokens_consumed",
            unit="{token}",
            description="Tokens consumed by CCSStore operations",
        )
        self._tokens_saved_counter: Counter = self._meter.create_counter(
            name="ccs.store.tokens_saved_estimate",
            unit="{token}",
            description="Estimated tokens saved by cache hits",
        )
        self._hits_counter: Counter = self._meter.create_counter(
            name="ccs.store.cache_hits",
            unit="{operation}",
            description="Cache hits (get operations only)",
        )
        self._misses_counter: Counter = self._meter.create_counter(
            name="ccs.store.cache_misses",
            unit="{operation}",
            description="Cache misses (get operations only)",
        )
        self._degradation_events_counter: Counter = self._meter.create_counter(
            name="ccs.store.degradation_events",
            unit="{operation}",
            description="Count of coherence errors that degraded to fallback",
        )
        # Internal state for the degraded_mode observable gauge.
        # Written from on_event; read by the OTel collection thread.
        # Python's GIL makes bool read/write safe without a lock on CPython.
        self._degraded: bool = False
        self._degraded_mode_gauge: ObservableGauge = self._meter.create_observable_gauge(
            name="ccs.store.degraded_mode",
            callbacks=[self._degraded_mode_callback],
            unit="{bool}",
            description="1 if any degradation has occurred on this exporter instance, else 0",
        )

    def _degraded_mode_callback(self, options: object) -> list[object]:
        from opentelemetry.metrics import Observation  # noqa: PLC0415

        return [Observation(int(self._degraded))]

    def on_event(self, event: StoreMetricEvent) -> None:
        attributes = {
            "ccs.operation": event.operation,
            "ccs.agent_name": event.agent_name,
            "ccs.cache_hit": event.cache_hit,
        }
        self._ops_counter.add(1, attributes)
        self._tokens_counter.add(event.tokens_consumed, attributes)
        self._tokens_saved_counter.add(event.tokens_saved_estimate, attributes)

        if event.operation == "get":
            self._hits_counter.add(1 if event.cache_hit else 0, attributes)
            self._misses_counter.add(0 if event.cache_hit else 1, attributes)

        if event.operation == "degraded":
            self._degraded = True
            self._degradation_events_counter.add(1, attributes)
