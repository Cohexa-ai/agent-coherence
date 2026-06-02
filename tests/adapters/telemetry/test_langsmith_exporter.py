"""Tests for LangSmithExporter."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip("langsmith")

from langgraph.store.base import PutOp

from ccs.adapters.ccsstore import CCSStore
from ccs.adapters.events import StoreMetricEvent
from ccs.adapters.telemetry.langsmith import LangSmithExporter

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fake_event(operation: str = "get", cache_hit: bool = True, tokens: int = 1, tick: int = 2) -> StoreMetricEvent:
    return StoreMetricEvent(
        operation=operation,
        namespace=("agent", "shared"),
        key="plan",
        agent_name="researcher",
        tokens_consumed=tokens,
        cache_hit=cache_hit,
        tick=tick,
    )


# ---------------------------------------------------------------------------
# Happy path — active run
# ---------------------------------------------------------------------------

def test_langsmith_calls_add_metadata_when_run_active() -> None:
    exporter = LangSmithExporter()
    mock_run = MagicMock()

    with patch("langsmith.run_helpers.get_current_run_tree", return_value=mock_run):
        exporter.on_event(_fake_event())

    mock_run.add_metadata.assert_called_once()
    metadata = mock_run.add_metadata.call_args[0][0]
    assert "ccs.operation" in metadata
    assert metadata["ccs.operation"] == "get"


def test_langsmith_metadata_contains_all_fields() -> None:
    exporter = LangSmithExporter()
    mock_run = MagicMock()
    event = _fake_event(operation="put", cache_hit=False, tokens=80, tick=5)

    with patch("langsmith.run_helpers.get_current_run_tree", return_value=mock_run):
        exporter.on_event(event)

    metadata = mock_run.add_metadata.call_args[0][0]
    assert metadata["ccs.operation"] == "put"
    assert metadata["ccs.agent_name"] == "researcher"
    assert metadata["ccs.tokens_consumed"] == 80
    assert metadata["ccs.cache_hit"] is False
    assert metadata["ccs.tick"] == 5


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------

def test_langsmith_no_active_run_does_not_call_add_metadata() -> None:
    exporter = LangSmithExporter()

    with patch("langsmith.run_helpers.get_current_run_tree", return_value=None):
        exporter.on_event(_fake_event())  # must not raise

    # No add_metadata call possible since we have no mock_run to assert on —
    # the test passes if no exception is raised.


def test_langsmith_no_active_run_no_exception() -> None:
    exporter = LangSmithExporter()
    with patch("langsmith.run_helpers.get_current_run_tree", return_value=None):
        # Multiple calls — still no exception
        for _ in range(3):
            exporter.on_event(_fake_event())


# ---------------------------------------------------------------------------
# Integration: CCSStore + LangSmithExporter
# ---------------------------------------------------------------------------

def test_ccsstore_langsmith_receives_metadata_after_put() -> None:
    mock_run = MagicMock()
    exporter = LangSmithExporter()
    store = CCSStore(strategy="lazy", telemetry=exporter)

    with patch("langsmith.run_helpers.get_current_run_tree", return_value=mock_run):
        store.batch([PutOp(namespace=("planner", "shared"), key="plan", value={"v": 1})])

    mock_run.add_metadata.assert_called_once()
    metadata = mock_run.add_metadata.call_args[0][0]
    assert metadata["ccs.operation"] == "put"


# ---------------------------------------------------------------------------
# Stateful aggregation — headline keys (Unit 4)
# ---------------------------------------------------------------------------

def test_langsmith_headline_keys_on_cache_hit() -> None:
    exporter = LangSmithExporter()
    mock_run = MagicMock()
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
    with patch("langsmith.run_helpers.get_current_run_tree", return_value=mock_run):
        exporter.on_event(event)

    metadata = mock_run.add_metadata.call_args[0][0]
    assert metadata["ccs.cache_hit_rate"] == 1.0
    assert metadata["ccs.token_reduction_pct"] > 0
    assert metadata["ccs.tokens_saved_estimate"] > 0


def test_langsmith_headline_keys_on_cache_miss() -> None:
    exporter = LangSmithExporter()
    mock_run = MagicMock()
    event = StoreMetricEvent(
        operation="get",
        namespace=("agent", "shared"),
        key="plan",
        agent_name="planner",
        tokens_consumed=50,
        cache_hit=False,
        tick=1,
    )
    with patch("langsmith.run_helpers.get_current_run_tree", return_value=mock_run):
        exporter.on_event(event)

    metadata = mock_run.add_metadata.call_args[0][0]
    assert metadata["ccs.cache_hit_rate"] == 0.0
    assert metadata["ccs.tokens_saved_estimate"] == 0


def test_langsmith_cache_hit_rate_mixed() -> None:
    exporter = LangSmithExporter()
    mock_run = MagicMock()
    hit_event = StoreMetricEvent(
        operation="get", namespace=("a", "s"), key="k", agent_name="a",
        tokens_consumed=1, cache_hit=True, tick=1, tokens_saved_estimate=50,
    )
    miss_event = StoreMetricEvent(
        operation="get", namespace=("a", "s"), key="k", agent_name="b",
        tokens_consumed=50, cache_hit=False, tick=2,
    )
    with patch("langsmith.run_helpers.get_current_run_tree", return_value=mock_run):
        exporter.on_event(hit_event)
        exporter.on_event(miss_event)

    metadata = mock_run.add_metadata.call_args[0][0]
    assert metadata["ccs.cache_hit_rate"] == 0.5


def test_langsmith_zero_baseline_no_division_error() -> None:
    exporter = LangSmithExporter()
    mock_run = MagicMock()
    # Artificial edge: tokens_consumed=0 and tokens_saved_estimate=0 keeps baseline at 0.
    event = StoreMetricEvent(
        operation="get", namespace=("a", "s"), key="k", agent_name="a",
        tokens_consumed=0, cache_hit=False, tick=1,
    )
    with patch("langsmith.run_helpers.get_current_run_tree", return_value=mock_run):
        exporter.on_event(event)

    metadata = mock_run.add_metadata.call_args[0][0]
    assert metadata["ccs.token_reduction_pct"] == 0.0


def test_langsmith_accumulators_update_when_run_is_none() -> None:
    exporter = LangSmithExporter()
    event = StoreMetricEvent(
        operation="get", namespace=("a", "s"), key="k", agent_name="a",
        tokens_consumed=50, cache_hit=True, tick=1, tokens_saved_estimate=49,
    )
    with patch("langsmith.run_helpers.get_current_run_tree", return_value=None):
        exporter.on_event(event)  # must not raise

    assert exporter._hits == 1
    assert exporter._tokens_saved == 49
