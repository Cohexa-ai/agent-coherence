# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""Tests for CCSStore LangGraph BaseStore adapter."""

from __future__ import annotations

import json
import warnings
from typing import Any
from unittest.mock import patch
from uuid import NAMESPACE_URL, uuid5

import pytest

pytest.importorskip("langgraph.store.base")

from langgraph.store.base import GetOp, PutOp, SearchOp

from ccs.adapters.ccsstore import CCSStore, StoreMetricEvent
from ccs.core.states import MESIState

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _store(strategy: str = "lazy", **kwargs: Any) -> CCSStore:
    return CCSStore(strategy=strategy, **kwargs)


def _put(store: CCSStore, namespace: tuple[str, ...], key: str, value: dict) -> None:
    store.batch([PutOp(namespace=namespace, key=key, value=value)])


def _get(store: CCSStore, namespace: tuple[str, ...], key: str):
    return store.batch([GetOp(namespace=namespace, key=key)])[0]


def _delete(store: CCSStore, namespace: tuple[str, ...], key: str) -> None:
    store.batch([PutOp(namespace=namespace, key=key, value=None)])


# ---------------------------------------------------------------------------
# Get
# ---------------------------------------------------------------------------

def test_get_after_put_same_agent_is_cache_hit() -> None:
    events: list[StoreMetricEvent] = []
    store = _store(on_metric=events.append)
    _put(store, ("planner", "shared"), "plan", {"v": 1})
    result = _get(store, ("planner", "shared"), "plan")

    assert result is not None
    assert result.value == {"v": 1}
    get_event = next(e for e in events if e.operation == "get")
    assert get_event.cache_hit is True


def test_get_after_peer_put_is_cache_miss() -> None:
    events: list[StoreMetricEvent] = []
    store = _store(on_metric=events.append)
    _put(store, ("planner", "shared"), "plan", {"v": 1})
    # First get by reviewer: MESI miss → fetch
    result = _get(store, ("reviewer", "shared"), "plan")

    assert result is not None
    assert result.value == {"v": 1}
    get_event = next(e for e in events if e.operation == "get")
    assert get_event.cache_hit is False


def test_get_unknown_key_returns_none() -> None:
    store = _store()
    result = _get(store, ("planner", "shared"), "nonexistent")
    assert result is None


def test_get_unknown_key_does_not_emit_metric() -> None:
    events: list[StoreMetricEvent] = []
    store = _store(on_metric=events.append)
    _get(store, ("planner", "shared"), "nonexistent")
    assert not events


def test_get_short_namespace_raises_value_error() -> None:
    store = _store()
    with pytest.raises(ValueError):
        store.batch([GetOp(namespace=("planner",), key="plan")])


def test_get_lazy_registers_agent_on_first_access() -> None:
    store = _store()
    _put(store, ("planner", "shared"), "plan", {"x": 1})
    # reviewer has never been registered; get should trigger lazy registration
    result = _get(store, ("reviewer", "shared"), "plan")
    assert result is not None
    assert "reviewer" in store._known_agents


def test_get_two_agents_both_reach_shared_state() -> None:
    store = _store()
    _put(store, ("planner", "shared"), "plan", {"x": 1})
    _get(store, ("planner", "shared"), "plan")
    _get(store, ("reviewer", "shared"), "plan")

    artifact_id = uuid5(NAMESPACE_URL, "ccs-artifact:shared:plan")
    # Check the coordinator registry (ground truth), not the local cache.
    # The local cache may lag behind — the registry is always consistent.
    planner_id = store.core.agent_id_for("planner")
    reviewer_id = store.core.agent_id_for("reviewer")
    assert store.core.registry.get_agent_state(artifact_id, planner_id) == MESIState.SHARED
    assert store.core.registry.get_agent_state(artifact_id, reviewer_id) == MESIState.SHARED


def test_get_after_peer_write_is_invalidated_then_refreshed() -> None:
    store = _store()
    _put(store, ("planner", "shared"), "plan", {"v": 1})
    # reviewer reads → SHARED
    _get(store, ("reviewer", "shared"), "plan")
    # planner writes new version → reviewer invalidated
    _put(store, ("planner", "shared"), "plan", {"v": 2})

    artifact_id = uuid5(NAMESPACE_URL, "ccs-artifact:shared:plan")
    reviewer_entry = store.core.runtime("reviewer").cache.get(artifact_id)
    assert reviewer_entry is not None
    assert reviewer_entry.state == MESIState.INVALID

    result = _get(store, ("reviewer", "shared"), "plan")
    assert result is not None
    assert result.value == {"v": 2}


# ---------------------------------------------------------------------------
# Put
# ---------------------------------------------------------------------------

def test_put_creates_artifact_on_first_write() -> None:
    store = _store()
    _put(store, ("planner", "shared"), "plan", {"v": 1})
    result = _get(store, ("planner", "shared"), "plan")
    assert result is not None
    assert result.value == {"v": 1}


def test_put_second_write_increments_version() -> None:
    store = _store()
    _put(store, ("planner", "shared"), "plan", {"v": 1})
    _put(store, ("planner", "shared"), "plan", {"v": 2})
    result = _get(store, ("planner", "shared"), "plan")
    assert result is not None
    assert result.value == {"v": 2}


def test_put_short_namespace_raises_value_error() -> None:
    store = _store()
    with pytest.raises(ValueError):
        store.batch([PutOp(namespace=("planner",), key="plan", value={"x": 1})])


def test_put_ttl_non_none_emits_user_warning() -> None:
    store = _store()
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        store.batch([PutOp(namespace=("planner", "shared"), key="plan", value={"v": 1}, ttl=60.0)])
    assert any(issubclass(warning.category, UserWarning) for warning in w)


def test_put_index_param_accepted_without_error() -> None:
    store = _store()
    store.batch([PutOp(namespace=("planner", "shared"), key="plan", value={"v": 1}, index=["v"])])


def test_put_triggers_invalidation_to_peers() -> None:
    store = _store()
    _put(store, ("planner", "shared"), "plan", {"v": 1})
    _get(store, ("reviewer", "shared"), "plan")

    artifact_id = uuid5(NAMESPACE_URL, "ccs-artifact:shared:plan")
    reviewer_before = store.core.runtime("reviewer").cache.get(artifact_id)
    assert reviewer_before is not None and reviewer_before.state == MESIState.SHARED

    _put(store, ("planner", "shared"), "plan", {"v": 2})

    reviewer_after = store.core.runtime("reviewer").cache.get(artifact_id)
    assert reviewer_after is not None and reviewer_after.state == MESIState.INVALID


def test_put_emits_metric_event() -> None:
    events: list[StoreMetricEvent] = []
    store = _store(on_metric=events.append)
    _put(store, ("planner", "shared"), "plan", {"v": 1})
    put_events = [e for e in events if e.operation == "put"]
    assert len(put_events) == 1
    assert put_events[0].agent_name == "planner"
    assert put_events[0].cache_hit is False


# ---------------------------------------------------------------------------
# Delete
# ---------------------------------------------------------------------------

def test_delete_makes_subsequent_get_return_none() -> None:
    store = _store()
    _put(store, ("planner", "shared"), "plan", {"v": 1})
    _delete(store, ("planner", "shared"), "plan")
    assert _get(store, ("planner", "shared"), "plan") is None


def test_delete_absent_key_is_silent_no_op() -> None:
    store = _store()
    _delete(store, ("planner", "shared"), "nonexistent")  # must not raise


def test_delete_then_put_same_key_re_registers_successfully() -> None:
    store = _store()
    _put(store, ("planner", "shared"), "plan", {"v": 1})
    _delete(store, ("planner", "shared"), "plan")
    _put(store, ("planner", "shared"), "plan", {"v": 2})
    result = _get(store, ("planner", "shared"), "plan")
    assert result is not None
    assert result.value == {"v": 2}


def test_delete_sends_invalidation_to_peers_no_coherence_error() -> None:
    store = _store()
    _put(store, ("planner", "shared"), "plan", {"v": 1})
    _get(store, ("reviewer", "shared"), "plan")  # reviewer caches it

    # delete must propagate to reviewer without raising CoherenceError
    _delete(store, ("planner", "shared"), "plan")

    # reviewer get returns None (artifact gone)
    assert _get(store, ("reviewer", "shared"), "plan") is None


def test_delete_does_not_emit_metric_event() -> None:
    events: list[StoreMetricEvent] = []
    store = _store(on_metric=events.append)
    _put(store, ("planner", "shared"), "plan", {"v": 1})
    events.clear()
    _delete(store, ("planner", "shared"), "plan")
    assert not events


# ---------------------------------------------------------------------------
# Search
# ---------------------------------------------------------------------------

def test_search_by_agent_prefix_scopes_correctly() -> None:
    store = _store()
    _put(store, ("planner", "shared"), "plan", {"status": "active"})
    _put(store, ("reviewer", "shared"), "notes", {"status": "draft"})

    results = store.search(("planner",))
    keys = [r.key for r in results]
    assert "plan" in keys
    assert "notes" not in keys


def test_search_filter_primitive_eq() -> None:
    store = _store()
    _put(store, ("planner", "shared"), "a", {"status": "active"})
    _put(store, ("planner", "shared"), "b", {"status": "draft"})

    results = store.search(("planner",), filter={"status": "active"})
    assert len(results) == 1
    assert results[0].key == "a"


def test_search_filter_explicit_ne() -> None:
    store = _store()
    _put(store, ("planner", "shared"), "a", {"status": "active"})
    _put(store, ("planner", "shared"), "b", {"status": "draft"})

    results = store.search(("planner",), filter={"status": {"$ne": "draft"}})
    assert len(results) == 1
    assert results[0].key == "a"


def test_search_filter_explicit_eq_no_match_excluded() -> None:
    store = _store()
    _put(store, ("planner", "shared"), "a", {"status": "active"})
    _put(store, ("planner", "shared"), "b", {"status": "draft"})
    results = store.search(("planner",), filter={"status": {"$eq": "active"}})
    assert len(results) == 1
    assert results[0].key == "a"


def test_search_filter_unsupported_operator_raises() -> None:
    store = _store()
    _put(store, ("planner", "shared"), "a", {"score": 5})
    with pytest.raises(NotImplementedError):
        store.search(("planner",), filter={"score": {"$gt": 3}})


def test_search_query_param_emits_warning_returns_results() -> None:
    store = _store()
    _put(store, ("planner", "shared"), "a", {"x": 1})
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        results = store.search(("planner",), query="something")
    assert any(issubclass(warning.category, UserWarning) for warning in w)
    assert len(results) == 1  # still returns results, just unranked


def test_search_does_not_change_mesi_state() -> None:
    store = _store()
    _put(store, ("planner", "shared"), "plan", {"v": 1})
    artifact_id = uuid5(NAMESPACE_URL, "ccs-artifact:shared:plan")

    # planner holds MODIFIED after put
    planner_entry_before = store.core.runtime("planner").cache.get(artifact_id)
    state_before = planner_entry_before.state if planner_entry_before else None

    store.search(("planner",))

    planner_entry_after = store.core.runtime("planner").cache.get(artifact_id)
    state_after = planner_entry_after.state if planner_entry_after else None
    assert state_before == state_after


def test_search_before_any_writes_returns_empty() -> None:
    store = _store()
    assert store.search(("planner",)) == []


def test_search_emits_one_metric_per_result() -> None:
    events: list[StoreMetricEvent] = []
    store = _store(on_metric=events.append)
    _put(store, ("planner", "shared"), "a", {"x": 1})
    _put(store, ("planner", "shared"), "b", {"x": 2})
    events.clear()
    store.search(("planner",))
    search_events = [e for e in events if e.operation == "search.hit"]
    assert len(search_events) == 2


# ---------------------------------------------------------------------------
# List namespaces
# ---------------------------------------------------------------------------

def test_list_namespaces_no_writes_returns_empty() -> None:
    store = _store()
    assert store.list_namespaces(prefix=()) == []


def test_list_namespaces_after_two_puts_shows_both() -> None:
    store = _store()
    _put(store, ("planner", "shared"), "plan", {"v": 1})
    _put(store, ("reviewer", "notes"), "review", {"v": 1})
    namespaces = store.list_namespaces(prefix=())
    assert ("planner", "shared") in namespaces
    assert ("reviewer", "notes") in namespaces


def test_list_namespaces_prefix_condition_filters() -> None:
    store = _store()
    _put(store, ("planner", "shared"), "plan", {"v": 1})
    _put(store, ("reviewer", "notes"), "review", {"v": 1})
    results = store.list_namespaces(prefix=("planner",))
    assert ("planner", "shared") in results
    assert ("reviewer", "notes") not in results


def test_list_namespaces_suffix_condition_filters() -> None:
    store = _store()
    _put(store, ("planner", "shared"), "plan", {"v": 1})
    _put(store, ("reviewer", "shared"), "notes", {"v": 1})
    _put(store, ("executor", "private"), "task", {"v": 1})
    results = store.list_namespaces(suffix=("shared",))
    assert ("planner", "shared") in results
    assert ("reviewer", "shared") in results
    assert ("executor", "private") not in results


# ---------------------------------------------------------------------------
# Metrics
# ---------------------------------------------------------------------------

def test_on_metric_none_default_no_error() -> None:
    store = _store()  # on_metric=None
    _put(store, ("planner", "shared"), "plan", {"v": 1})
    _get(store, ("planner", "shared"), "plan")  # no error


def test_metric_get_cache_hit_true() -> None:
    events: list[StoreMetricEvent] = []
    store = _store(on_metric=events.append)
    _put(store, ("planner", "shared"), "plan", {"v": 1})
    events.clear()
    _get(store, ("planner", "shared"), "plan")
    assert events[0].cache_hit is True
    assert events[0].operation == "get"


def test_metric_get_cache_miss_false() -> None:
    events: list[StoreMetricEvent] = []
    store = _store(on_metric=events.append)
    _put(store, ("planner", "shared"), "plan", {"v": 1})
    events.clear()
    _get(store, ("reviewer", "shared"), "plan")
    assert events[0].cache_hit is False


def test_metric_tokens_consumed_estimation_put() -> None:
    events: list[StoreMetricEvent] = []
    store = _store(on_metric=events.append)
    value = {"content": "x" * 400}
    _put(store, ("planner", "shared"), "plan", value)
    put_event = next(e for e in events if e.operation == "put")
    expected = max(1, len(json.dumps(value, sort_keys=True, separators=(",", ":"))) // 4)
    assert put_event.tokens_consumed == expected


def test_metric_tokens_consumed_cache_miss_is_full_size() -> None:
    events: list[StoreMetricEvent] = []
    store = _store(on_metric=events.append)
    value = {"content": "x" * 400}
    _put(store, ("planner", "shared"), "plan", value)
    events.clear()
    _get(store, ("reviewer", "shared"), "plan")  # cache miss
    get_event = events[0]
    expected = max(1, len(json.dumps(value, sort_keys=True, separators=(",", ":"))) // 4)
    assert get_event.tokens_consumed == expected


def test_metric_tokens_consumed_cache_hit_is_one() -> None:
    events: list[StoreMetricEvent] = []
    store = _store(on_metric=events.append)
    _put(store, ("planner", "shared"), "plan", {"content": "x" * 400})
    events.clear()
    _get(store, ("planner", "shared"), "plan")  # cache hit (same agent that wrote)
    get_event = events[0]
    assert get_event.tokens_consumed == 1


def test_metric_custom_size_override() -> None:
    events: list[StoreMetricEvent] = []
    store = _store(on_metric=events.append)
    _put(store, ("planner", "shared"), "plan", {"data": "x", "__ccs_size_tokens__": 42})
    put_event = next(e for e in events if e.operation == "put")
    assert put_event.tokens_consumed == 42


# ---------------------------------------------------------------------------
# Tick and threading
# ---------------------------------------------------------------------------

def test_tick_starts_at_zero_increments_per_batch() -> None:
    store = _store()
    assert store._tick == 0
    store.batch([PutOp(namespace=("planner", "shared"), key="a", value={"x": 1})])
    assert store._tick == 1
    store.batch([PutOp(namespace=("planner", "shared"), key="b", value={"x": 2})])
    assert store._tick == 2


def test_tick_appears_in_metric_event() -> None:
    events: list[StoreMetricEvent] = []
    store = _store(on_metric=events.append)
    store.batch([PutOp(namespace=("planner", "shared"), key="a", value={"x": 1})])
    assert events[0].tick == 1


def test_abatch_returns_same_as_batch() -> None:
    import asyncio

    store = _store()
    ops = [PutOp(namespace=("planner", "shared"), key="a", value={"x": 1})]
    batch_result = store.batch(ops)

    store2 = _store()
    abatch_result = asyncio.run(store2.abatch(ops))
    # Both return [None] for a single PutOp
    assert batch_result == abatch_result


# ---------------------------------------------------------------------------
# Full batch dispatch
# ---------------------------------------------------------------------------

def test_batch_mixed_ops_returns_results_in_order() -> None:
    store = _store()
    _put(store, ("planner", "shared"), "a", {"v": 1})
    ops = [
        GetOp(namespace=("planner", "shared"), key="a"),
        PutOp(namespace=("planner", "shared"), key="b", value={"v": 2}),
        SearchOp(namespace_prefix=("planner",)),
    ]
    results = store.batch(ops)
    assert len(results) == 3
    assert results[0] is not None and results[0].value == {"v": 1}  # Item from Get
    assert results[1] is None  # Put returns None
    assert isinstance(results[2], list)  # Search returns list


def test_batch_value_error_on_short_namespace_propagates() -> None:
    store = _store()
    with pytest.raises(ValueError):
        store.batch([GetOp(namespace=("only_one",), key="k")])


def test_list_namespaces_max_depth_truncates_and_deduplicates() -> None:
    store = _store()
    _put(store, ("planner", "shared", "plans"), "a", {"v": 1})
    _put(store, ("planner", "shared", "plans"), "b", {"v": 2})
    _put(store, ("planner", "private"), "c", {"v": 3})
    # max_depth=2 → unique tuples of length 2
    results = store.list_namespaces(prefix=(), max_depth=2)
    assert ("planner", "shared") in results
    assert ("planner", "private") in results
    # original 3-element namespace should NOT appear
    assert ("planner", "shared", "plans") not in results


def test_delete_short_namespace_raises_value_error() -> None:
    store = _store()
    with pytest.raises(ValueError):
        store.batch([PutOp(namespace=("planner",), key="plan", value=None)])


def test_re_registration_after_delete_clears_deleted_ids() -> None:
    store = _store()
    _put(store, ("planner", "shared"), "plan", {"v": 1})
    _delete(store, ("planner", "shared"), "plan")

    from uuid import NAMESPACE_URL, uuid5
    artifact_id = uuid5(NAMESPACE_URL, "ccs-artifact:shared:plan")
    assert artifact_id in store._deleted_ids

    _put(store, ("planner", "shared"), "plan", {"v": 2})
    assert artifact_id not in store._deleted_ids


def test_metric_events_ordered_by_operation_sequence() -> None:
    events: list[StoreMetricEvent] = []
    store = _store(on_metric=events.append)
    ops = [
        PutOp(namespace=("planner", "shared"), key="a", value={"x": 1}),
        GetOp(namespace=("planner", "shared"), key="a"),
    ]
    store.batch(ops)
    assert events[0].operation == "put"
    assert events[1].operation == "get"


# ---------------------------------------------------------------------------
# Telemetry parameter
# ---------------------------------------------------------------------------

def test_ccsstore_default_has_noop_telemetry() -> None:
    from ccs.adapters.telemetry import NoOpTelemetryExporter
    store = _store()
    assert isinstance(store._telemetry, NoOpTelemetryExporter)


def test_ccsstore_telemetry_none_has_noop() -> None:
    from ccs.adapters.telemetry import NoOpTelemetryExporter
    store = _store(telemetry=None)
    assert isinstance(store._telemetry, NoOpTelemetryExporter)


def test_ccsstore_telemetry_exporter_receives_put_event() -> None:
    from ccs.adapters.telemetry import TelemetryExporter

    class CapturingExporter(TelemetryExporter):
        def __init__(self):
            self.events: list[StoreMetricEvent] = []
        def on_event(self, event: StoreMetricEvent) -> None:
            self.events.append(event)

    exporter = CapturingExporter()
    store = _store(telemetry=exporter)
    _put(store, ("planner", "shared"), "plan", {"v": 1})
    assert len(exporter.events) == 1
    assert exporter.events[0].operation == "put"


def test_ccsstore_telemetry_exporter_receives_get_event() -> None:
    from ccs.adapters.telemetry import TelemetryExporter

    class CapturingExporter(TelemetryExporter):
        def __init__(self):
            self.events: list[StoreMetricEvent] = []
        def on_event(self, event: StoreMetricEvent) -> None:
            self.events.append(event)

    exporter = CapturingExporter()
    store = _store(telemetry=exporter)
    _put(store, ("planner", "shared"), "plan", {"v": 1})
    exporter.events.clear()
    _get(store, ("planner", "shared"), "plan")
    assert len(exporter.events) == 1
    assert exporter.events[0].operation == "get"


def test_ccsstore_on_metric_and_telemetry_both_called() -> None:
    from ccs.adapters.telemetry import TelemetryExporter

    on_metric_events: list[StoreMetricEvent] = []

    class CapturingExporter(TelemetryExporter):
        def __init__(self):
            self.events: list[StoreMetricEvent] = []
        def on_event(self, event: StoreMetricEvent) -> None:
            self.events.append(event)

    exporter = CapturingExporter()
    store = _store(on_metric=on_metric_events.append, telemetry=exporter)
    _put(store, ("planner", "shared"), "plan", {"v": 1})
    assert len(on_metric_events) == 1
    assert len(exporter.events) == 1
    # Both receive the same event object
    assert on_metric_events[0] is exporter.events[0]


def test_ccsstore_telemetry_and_on_metric_none_no_error() -> None:
    store = _store(telemetry=None, on_metric=None)
    _put(store, ("planner", "shared"), "plan", {"v": 1})
    result = _get(store, ("planner", "shared"), "plan")
    assert result is not None


# ---------------------------------------------------------------------------
# Graceful degradation (on_error parameter)
# ---------------------------------------------------------------------------

def test_on_error_invalid_value_raises() -> None:
    with pytest.raises(ValueError, match="on_error"):
        CCSStore(on_error="bad")


def test_on_error_strict_is_default() -> None:
    store = _store()
    assert store._on_error == "strict"


def test_on_error_strict_reraises_coherence_error_on_get() -> None:
    from unittest.mock import patch

    from ccs.core.exceptions import CoherenceError

    store = _store(on_error="strict")
    _put(store, ("planner", "shared"), "plan", {"v": 1})
    with patch.object(store.core, "read", side_effect=CoherenceError("simulated")):
        with pytest.raises(CoherenceError):
            _get(store, ("reviewer", "shared"), "plan")  # different agent → cache miss → core.read


def test_on_error_strict_reraises_coherence_error_on_put() -> None:
    from unittest.mock import patch

    from ccs.core.exceptions import CoherenceError

    store = _store(on_error="strict")
    with patch.object(store.core, "write", side_effect=CoherenceError("simulated")):
        with pytest.raises(CoherenceError):
            _put(store, ("planner", "shared"), "plan", {"v": 1})


def test_on_error_degrade_put_emits_degraded_event() -> None:
    from unittest.mock import patch

    from ccs.core.exceptions import CoherenceError

    events: list[StoreMetricEvent] = []
    store = _store(on_error="degrade", on_metric=events.append)
    with patch.object(store.core, "write", side_effect=CoherenceError("simulated")):
        _put(store, ("planner", "shared"), "plan", {"v": 1})
    assert events[0].operation == "degraded"


def test_on_error_degrade_get_emits_degraded_event() -> None:
    from unittest.mock import patch

    from ccs.core.exceptions import CoherenceError

    events: list[StoreMetricEvent] = []
    store = _store(on_error="degrade", on_metric=events.append)
    _put(store, ("planner", "shared"), "plan", {"v": 1})
    events.clear()
    with patch.object(store.core, "read", side_effect=CoherenceError("simulated")):
        _get(store, ("reviewer", "shared"), "plan")  # reviewer cache miss → core.read → degrade
    assert events[0].operation == "degraded"


def test_on_error_degrade_get_returns_fallback_value() -> None:
    from unittest.mock import patch

    from ccs.core.exceptions import CoherenceError

    store = _store(on_error="degrade")
    # Degraded put — value lands in _fallback_store
    with patch.object(store.core, "write", side_effect=CoherenceError("simulated")):
        _put(store, ("planner", "shared"), "plan", {"v": 42})
    # Degraded get from same scope — retrieves from _fallback_store
    with patch.object(store.core, "read", side_effect=CoherenceError("simulated")):
        result = _get(store, ("reviewer", "shared"), "plan")
    assert result is not None
    assert result.value == {"v": 42}


def test_on_error_degrade_does_not_raise_on_coherence_error() -> None:
    from unittest.mock import patch

    from ccs.core.exceptions import CoherenceError

    store = _store(on_error="degrade")
    _put(store, ("planner", "shared"), "plan", {"v": 1})
    # Neither put nor get should raise when on_error="degrade"
    with patch.object(store.core, "write", side_effect=CoherenceError("simulated")):
        _put(store, ("planner", "shared"), "plan", {"v": 2})  # no raise
    with patch.object(store.core, "read", side_effect=CoherenceError("simulated")):
        _get(store, ("reviewer", "shared"), "plan")  # no raise


# ---------------------------------------------------------------------------
# CoherenceDegradedWarning + degradation visibility (R8 additions)
# ---------------------------------------------------------------------------

def test_is_degraded_false_before_any_error() -> None:
    store = _store(on_error="degrade")
    assert store.is_degraded is False


def test_is_degraded_true_after_degraded_get() -> None:
    import warnings

    from ccs.core.exceptions import CoherenceError

    store = _store(on_error="degrade")
    _put(store, ("planner", "shared"), "plan", {"v": 1})
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        with patch.object(store.core, "read", side_effect=CoherenceError("simulated")):
            _get(store, ("reviewer", "shared"), "plan")
    assert store.is_degraded is True


def test_is_degraded_true_after_degraded_put() -> None:
    import warnings

    from ccs.core.exceptions import CoherenceError

    store = _store(on_error="degrade")
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        with patch.object(store.core, "write", side_effect=CoherenceError("simulated")):
            _put(store, ("planner", "shared"), "plan", {"v": 1})
    assert store.is_degraded is True


def test_degradation_count_increments_per_error() -> None:
    import warnings

    from ccs.core.exceptions import CoherenceError

    store = _store(on_error="degrade")
    _put(store, ("planner", "shared"), "plan", {"v": 1})
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        with patch.object(store.core, "read", side_effect=CoherenceError("simulated")):
            _get(store, ("reviewer", "shared"), "plan")
            _get(store, ("reviewer", "shared"), "plan")
    assert store.degradation_count == 2


def test_degraded_warning_emitted_on_first_degradation() -> None:
    from ccs.adapters.ccsstore import CoherenceDegradedWarning
    from ccs.core.exceptions import CoherenceError

    store = _store(on_error="degrade")
    _put(store, ("planner", "shared"), "plan", {"v": 1})
    with pytest.warns(CoherenceDegradedWarning):
        with patch.object(store.core, "read", side_effect=CoherenceError("simulated")):
            _get(store, ("reviewer", "shared"), "plan")


def test_degraded_warning_not_emitted_on_second_degradation() -> None:
    import warnings

    from ccs.adapters.ccsstore import CoherenceDegradedWarning
    from ccs.core.exceptions import CoherenceError

    store = _store(on_error="degrade")
    _put(store, ("planner", "shared"), "plan", {"v": 1})
    # First degradation fires the warning
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        with patch.object(store.core, "read", side_effect=CoherenceError("simulated")):
            _get(store, ("reviewer", "shared"), "plan")
    # Second degradation must NOT fire CoherenceDegradedWarning
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        with patch.object(store.core, "read", side_effect=CoherenceError("simulated")):
            _get(store, ("reviewer", "shared"), "plan")
    degraded_warnings = [x for x in w if issubclass(x.category, CoherenceDegradedWarning)]
    assert len(degraded_warnings) == 0


def test_coherence_degraded_warning_importable_from_adapters() -> None:
    from ccs.adapters import CoherenceDegradedWarning
    assert issubclass(CoherenceDegradedWarning, UserWarning)


# ---------------------------------------------------------------------------
# Unit 1: schema version constants and StoreMetricEvent new fields
# ---------------------------------------------------------------------------

def test_schema_version_constants_importable() -> None:
    from ccs.adapters.events import CCS_METRIC_SCHEMA_VERSION
    from ccs.coordinator.registry import CCS_STATE_LOG_SCHEMA_VERSION
    assert CCS_STATE_LOG_SCHEMA_VERSION == "ccs.state_log.v2"
    assert CCS_METRIC_SCHEMA_VERSION == "ccs.metric.v1"


def test_store_metric_event_new_fields_default_to_none() -> None:
    event = StoreMetricEvent(
        operation="get",
        namespace=("a", "b"),
        key="k",
        agent_name="a",
        tokens_consumed=1,
        cache_hit=False,
        tick=1,
    )
    assert event.sequence_number is None
    assert event.instance_id is None
    assert event.schema_version is None


def test_store_metric_event_accepts_new_fields_as_kwargs() -> None:
    event = StoreMetricEvent(
        operation="get",
        namespace=("a", "b"),
        key="k",
        agent_name="a",
        tokens_consumed=1,
        cache_hit=False,
        tick=1,
        sequence_number=3,
        instance_id="abc-123",
        schema_version="ccs.metric.v1",
    )
    assert event.sequence_number == 3
    assert event.instance_id == "abc-123"
    assert event.schema_version == "ccs.metric.v1"


# ---------------------------------------------------------------------------
# Unit 3: CCSStore instance_id, metric sequence counter, _emit_metric helper
# ---------------------------------------------------------------------------

def test_get_emits_sequence_number_1_then_2() -> None:
    events: list[StoreMetricEvent] = []
    store = _store(on_metric=events.append)
    _put(store, ("planner", "shared"), "plan", {"v": 1})
    events.clear()
    _get(store, ("planner", "shared"), "plan")
    _get(store, ("planner", "shared"), "plan")
    get_events = [e for e in events if e.operation == "get"]
    assert get_events[0].sequence_number == 2  # put was seq=1
    assert get_events[1].sequence_number == 3


def test_put_increments_metric_seq() -> None:
    events: list[StoreMetricEvent] = []
    store = _store(on_metric=events.append)
    _put(store, ("planner", "shared"), "a", {"v": 1})
    _put(store, ("planner", "shared"), "b", {"v": 2})
    put_events = [e for e in events if e.operation == "put"]
    assert put_events[0].sequence_number == 1
    assert put_events[1].sequence_number == 2


def test_instance_id_on_metric_events_matches_store_instance() -> None:
    events: list[StoreMetricEvent] = []
    store = _store(on_metric=events.append)
    _put(store, ("planner", "shared"), "plan", {"v": 1})
    assert events[0].instance_id == store._instance_id


def test_schema_version_on_metric_events() -> None:
    events: list[StoreMetricEvent] = []
    store = _store(on_metric=events.append)
    _put(store, ("planner", "shared"), "plan", {"v": 1})
    assert events[0].schema_version == "ccs.metric.v1"


def test_two_ccsstore_instances_have_distinct_instance_ids() -> None:
    store_a = _store()
    store_b = _store()
    assert store_a._instance_id != store_b._instance_id


def test_search_hit_increments_metric_seq_per_result() -> None:
    events: list[StoreMetricEvent] = []
    store = _store(on_metric=events.append)
    _put(store, ("planner", "shared"), "a", {"v": 1})
    _put(store, ("planner", "shared"), "b", {"v": 2})
    events.clear()
    store.batch([SearchOp(namespace_prefix=("planner",), filter=None, limit=10, offset=0)])
    search_events = [e for e in events if e.operation == "search.hit"]
    assert len(search_events) == 2
    seqs = [e.sequence_number for e in search_events]
    assert seqs[1] == seqs[0] + 1


def test_out_of_sequence_event_produces_gap_in_validate_log(tmp_path) -> None:
    """An event with sequence_number=0 triggers Gap(expected=1, found=0)."""
    import json

    from ccs.validation import Gap, validate_log
    bad_line = {
        "sequence_number": 0,
        "instance_id": "some-id",
        "schema_version": "ccs.metric.v1",
        "operation": "get",
    }
    log_file = tmp_path / "test.jsonl"
    log_file.write_text(json.dumps(bad_line) + "\n")
    gaps, _ = validate_log(log_file, stream="metrics")
    assert gaps == [Gap(stream="metrics", expected=1, found=0, at_index=0)]


def test_none_sequence_number_raises_value_error(tmp_path) -> None:
    """StoreMetricEvent with default None sequence_number raises ValueError in validate_log."""
    import json

    from ccs.validation import validate_log
    bad_line = {
        "sequence_number": None,
        "instance_id": "some-id",
        "schema_version": "ccs.metric.v1",
    }
    log_file = tmp_path / "test.jsonl"
    log_file.write_text(json.dumps(bad_line) + "\n")
    with pytest.raises(ValueError, match="sequence_number"):
        validate_log(log_file)


# ---------------------------------------------------------------------------
# Crash recovery: CCSStore heartbeat / recover (Unit 3)
# ---------------------------------------------------------------------------


def test_ccsstore_batch_piggyback_heartbeat() -> None:
    from ccs.coordinator.service import CrashRecoveryConfig

    store = CCSStore(
        strategy="lazy",
        crash_recovery=CrashRecoveryConfig(enabled=True, heartbeat_timeout_ticks=10, max_hold_ticks=1000),
    )
    _put(store, ("planner", "shared"), "plan", {"v": 1})
    _get(store, ("planner", "shared"), "plan")

    agent_id = store.core.agent_id_for("planner")
    assert store.core.registry.last_heartbeat_tick(agent_id) is not None


def test_ccsstore_explicit_heartbeat() -> None:
    from ccs.coordinator.service import CrashRecoveryConfig

    store = CCSStore(
        strategy="lazy",
        crash_recovery=CrashRecoveryConfig(enabled=True, heartbeat_timeout_ticks=10, max_hold_ticks=1000),
    )
    _put(store, ("planner", "shared"), "plan", {"v": 1})

    store.heartbeat(agent_name="planner", now_tick=42)

    agent_id = store.core.agent_id_for("planner")
    assert store.core.registry.last_heartbeat_tick(agent_id) == 42


def test_ccsstore_heartbeat_requires_now_tick() -> None:
    from ccs.coordinator.service import CrashRecoveryConfig

    store = CCSStore(
        strategy="lazy",
        crash_recovery=CrashRecoveryConfig(enabled=True, heartbeat_timeout_ticks=10, max_hold_ticks=1000),
    )
    _put(store, ("planner", "shared"), "plan", {"v": 1})

    with pytest.raises(TypeError):
        store.heartbeat(agent_name="planner")  # type: ignore[call-arg]


def test_ccsstore_recover_invalidates_and_heartbeats() -> None:
    from ccs.coordinator.service import CrashRecoveryConfig

    store = CCSStore(
        strategy="lazy",
        crash_recovery=CrashRecoveryConfig(enabled=True, heartbeat_timeout_ticks=10, max_hold_ticks=1000),
    )
    _put(store, ("planner", "shared"), "plan", {"v": 1})
    _get(store, ("planner", "shared"), "plan")

    store.recover(agent_name="planner", now_tick=200)

    agent_id = store.core.agent_id_for("planner")
    runtime = store.core.runtime("planner")
    for entry in runtime.cache.entries().values():
        assert entry.state == MESIState.INVALID
    assert store.core.registry.last_heartbeat_tick(agent_id) == 200


def test_ccsstore_constructor_passthrough_failfast() -> None:
    from ccs.coordinator.service import CrashRecoveryConfig

    with pytest.raises(ValueError, match="composition violation"):
        CCSStore(
            strategy="lease",
            lease_ttl_ticks=300,
            crash_recovery=CrashRecoveryConfig(enabled=True, max_hold_ticks=300),
        )


def test_ccsstore_recover_checkpoint_restore_flow() -> None:
    from ccs.coordinator.service import CrashRecoveryConfig

    store = CCSStore(
        strategy="lazy",
        crash_recovery=CrashRecoveryConfig(enabled=True, heartbeat_timeout_ticks=10, max_hold_ticks=1000),
    )
    _put(store, ("planner", "shared"), "plan", {"v": 1})
    agent_id = store.core.agent_id_for("planner")

    # Simulate stale grant reclamation
    store.core.coordinator.enforce_stable_grant_timeouts(
        current_tick=50, heartbeat_timeout_ticks=10, max_hold_ticks=1000,
    )

    # Recover after restart
    store.recover(agent_name="planner", now_tick=51)

    # Cache is cleared — next put should succeed (re-acquires grant)
    entry = store.core.runtime("planner").cache.entries()
    for e in entry.values():
        assert e.state == MESIState.INVALID

    # Fresh heartbeat recorded
    assert store.core.registry.last_heartbeat_tick(agent_id) == 51


def test_ccsstore_flag_off_heartbeat_noop() -> None:
    store = _store()
    _put(store, ("planner", "shared"), "plan", {"v": 1})

    store.heartbeat(agent_name="planner", now_tick=42)

    agent_id = store.core.agent_id_for("planner")
    assert store.core.registry.last_heartbeat_tick(agent_id) is None


# ---- v0.8.3 C-flip Unit 2 — R2 internal-bare-construction hygiene ------------
#
# See docs/plans/2026-05-28-001-feat-c-flip-crash-recovery-default-on-plan.md
# Unit 2. The most-common adapter construction path (bare CCSStore() with no
# crash_recovery= argument) traverses CoherenceAdapterCore.__init__ which used
# to bare-construct CrashRecoveryConfig() — that emitted the v0.8.3
# DeprecationWarning from inside library code, indistinguishable to users
# from their own code. Test that the helper fix removed this.


def test_bare_ccsstore_emits_no_deprecation_warning() -> None:
    """R2: CCSStore() with no crash_recovery= must not surface the v0.8.3 warning."""
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        store = CCSStore(strategy="lazy")
    deprecation_warnings = [
        w for w in caught if issubclass(w.category, DeprecationWarning)
    ]
    assert deprecation_warnings == [], (
        f"bare CCSStore construction must not emit DeprecationWarning, got: "
        f"{[str(w.message) for w in deprecation_warnings]}"
    )
    # And the v0.8.x default behavior is preserved.
    assert store.core._crash_recovery.enabled is False
