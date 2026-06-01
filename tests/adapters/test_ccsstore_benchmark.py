"""Tests for CCSStore inline benchmark mode (R9)."""
from __future__ import annotations

from unittest.mock import patch

import pytest

pytest.importorskip("langgraph.store.base")

from langgraph.store.base import GetOp, PutOp

from ccs.adapters.ccsstore import CCSStore
from ccs.adapters.events import StoreMetricEvent
from ccs.core.exceptions import CoherenceError

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _store(**kwargs) -> CCSStore:
    return CCSStore(strategy="lazy", **kwargs)


def _put(store: CCSStore, namespace: tuple[str, ...], key: str, value: dict) -> None:
    store.batch([PutOp(namespace=namespace, key=key, value=value)])


def _get(store: CCSStore, namespace: tuple[str, ...], key: str):
    return store.batch([GetOp(namespace=namespace, key=key)])[0]


_EXPECTED_KEYS = {"baseline_tokens", "ccs_tokens", "tokens_saved", "token_reduction_pct",
                  "cache_hit_rate", "n_operations"}


# ---------------------------------------------------------------------------
# Happy paths
# ---------------------------------------------------------------------------

def test_benchmark_summary_has_all_keys() -> None:
    store = _store(benchmark=True)
    _put(store, ("planner", "shared"), "plan", {"v": 1})
    _get(store, ("planner", "shared"), "plan")
    summary = store.benchmark_summary()
    assert set(summary.keys()) == _EXPECTED_KEYS


def test_benchmark_summary_all_misses_zero_reduction() -> None:
    store = _store(benchmark=True)
    # Two separate agents; each read is a cache miss (different agent, no prior write)
    _put(store, ("planner", "shared"), "plan", {"v": 1})
    _get(store, ("reviewer", "shared"), "plan")  # reviewer: first read → miss

    summary = store.benchmark_summary()
    assert summary["token_reduction_pct"] == 0.0
    assert summary["tokens_saved"] == 0


def test_benchmark_summary_hit_produces_savings() -> None:
    # Use a value large enough that _estimate_tokens returns > 1 (so saved = estimate - 1 > 0)
    large_value = {"content": "x" * 200}
    store = _store(benchmark=True)
    _put(store, ("planner", "shared"), "plan", large_value)
    _get(store, ("planner", "shared"), "plan")  # same agent → EXCLUSIVE → cache hit

    summary = store.benchmark_summary()
    assert summary["tokens_saved"] > 0
    assert summary["token_reduction_pct"] > 0.0
    assert summary["cache_hit_rate"] == 1.0


def test_benchmark_summary_cache_hit_rate_all_hits() -> None:
    large_value = {"content": "x" * 200}
    store = _store(benchmark=True)
    _put(store, ("planner", "shared"), "plan", large_value)
    _get(store, ("planner", "shared"), "plan")
    _get(store, ("planner", "shared"), "plan")

    summary = store.benchmark_summary()
    assert summary["cache_hit_rate"] == 1.0
    assert summary["n_operations"] == 2


def test_benchmark_summary_cache_hit_rate_all_misses() -> None:
    store = _store(benchmark=True)
    _put(store, ("planner", "shared"), "plan", {"v": 1})
    _put(store, ("planner", "shared"), "note", {"v": 2})
    # reviewer reads both — first read of each is a miss
    _get(store, ("reviewer", "shared"), "plan")
    _get(store, ("reviewer", "shared"), "note")

    summary = store.benchmark_summary()
    assert summary["cache_hit_rate"] == 0.0
    assert summary["n_operations"] == 2


def test_benchmark_summary_mixed_ops() -> None:
    store = _store(benchmark=True)
    _put(store, ("planner", "shared"), "plan", {"v": 1})
    _get(store, ("planner", "shared"), "plan")   # hit
    _get(store, ("reviewer", "shared"), "plan")  # miss

    summary = store.benchmark_summary()
    assert summary["n_operations"] == 2
    assert summary["cache_hit_rate"] == 0.5


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------

def test_benchmark_summary_no_ops_returns_zeros() -> None:
    store = _store(benchmark=True)
    summary = store.benchmark_summary()
    assert summary["n_operations"] == 0
    assert summary["cache_hit_rate"] == 0.0
    assert summary["token_reduction_pct"] == 0.0
    assert summary["tokens_saved"] == 0


def test_benchmark_mode_false_is_default() -> None:
    store = _store()
    assert store._bm_baseline is None


def test_benchmark_mode_invalid_type_raises() -> None:
    with pytest.raises(TypeError, match="benchmark"):
        CCSStore(strategy="lazy", benchmark="yes")  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Error paths
# ---------------------------------------------------------------------------

def test_benchmark_summary_without_benchmark_mode_raises() -> None:
    store = _store(benchmark=False)
    with pytest.raises(RuntimeError, match="benchmark=True"):
        store.benchmark_summary()


def test_print_benchmark_summary_without_benchmark_mode_raises() -> None:
    store = _store(benchmark=False)
    with pytest.raises(RuntimeError, match="benchmark=True"):
        store.print_benchmark_summary()


# ---------------------------------------------------------------------------
# Integration
# ---------------------------------------------------------------------------

def test_benchmark_independent_of_on_metric() -> None:
    events: list[StoreMetricEvent] = []
    store = _store(benchmark=True, on_metric=events.append)
    _put(store, ("planner", "shared"), "plan", {"v": 1})
    _get(store, ("planner", "shared"), "plan")

    # Both paths accumulate independently
    assert len(events) == 2
    summary = store.benchmark_summary()
    assert summary["n_operations"] == 1  # only gets counted in benchmark


def test_benchmark_accumulates_degraded_gets() -> None:
    store = _store(benchmark=True, on_error="degrade")
    _put(store, ("planner", "shared"), "plan", {"v": 1})

    import warnings
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        with patch.object(store.core, "read", side_effect=CoherenceError("simulated")):
            _get(store, ("reviewer", "shared"), "plan")  # reviewer: degraded get

    summary = store.benchmark_summary()
    assert summary["n_operations"] == 1


def test_print_benchmark_summary_prints_to_stdout(capsys) -> None:
    store = _store(benchmark=True)
    _put(store, ("planner", "shared"), "plan", {"v": 1})
    _get(store, ("planner", "shared"), "plan")

    store.print_benchmark_summary()
    out = capsys.readouterr().out
    assert "CCSStore Benchmark Summary" in out
    assert "Token reduction" in out
    assert "Cache hit rate" in out
