# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""Tests for tools/cost_drift_check.py — the cost-lane drift gate.

Mirrors the token drift-check tests (``tests/test_benchmark_stage1.py`` § Unit 3):
``tools/`` is not an importable package, so the module is loaded by file path,
and each case writes tiny latest/expected JSON files into ``tmp_path``.
``check_drift`` returns ``(passed, messages)`` here, so the cell-set-mismatch
cases also assert the offending cell names surface in ``messages``.
"""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[1]


def _import_cost_drift_check():
    spec = importlib.util.spec_from_file_location(
        "cost_drift_check", _REPO_ROOT / "tools" / "cost_drift_check.py"
    )
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _write_json(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data))


def _cell(
    name: str,
    savings_ratio: float,
    refetches_avoided: float,
    wasted_refetches: float = 0.0,
) -> dict:
    return {
        "cell": name,
        "savings_ratio": savings_ratio,
        "refetches_avoided": refetches_avoided,
        "wasted_refetches": wasted_refetches,
    }


def _latest(*cells: dict) -> dict:
    """Latest sweep payload: cells live under ``rows`` (keyed by ``cell``)."""
    return {"sweep": "cost", "provenance": "temporal-sim (third lane)", "rows": list(cells)}


def _expected(*cells: dict) -> dict:
    """Committed baseline: cells live under ``cells`` (keyed by ``cell``)."""
    return {"grid": {}, "cells": list(cells)}


@pytest.fixture()
def cost_drift_check():
    return _import_cost_drift_check()


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


def test_cost_drift_check_passes_when_latest_equals_expected(cost_drift_check, tmp_path):
    latest = tmp_path / "cost_sweep.json"
    expected = tmp_path / "expected_cost.json"
    _write_json(latest, _latest(_cell("r0.0_s0.0", 0.7952, 87.0)))
    _write_json(expected, _expected(_cell("r0.0_s0.0", 0.7952, 87.0)))

    passed, _messages = cost_drift_check.check_drift(latest, expected)
    assert passed is True


def test_cost_drift_check_main_exits_zero_on_match(cost_drift_check, tmp_path, monkeypatch):
    latest = tmp_path / "cost_sweep.json"
    expected = tmp_path / "expected_cost.json"
    _write_json(latest, _latest(_cell("r0.0_s0.0", 0.7952, 87.0)))
    _write_json(expected, _expected(_cell("r0.0_s0.0", 0.7952, 87.0)))
    monkeypatch.setattr(cost_drift_check, "_LATEST_PATH", latest)
    monkeypatch.setattr(cost_drift_check, "_EXPECTED_PATH", expected)

    with pytest.raises(SystemExit) as exc:
        cost_drift_check.main([])
    assert exc.value.code == 0


def test_cost_drift_check_main_exits_one_on_drift(cost_drift_check, tmp_path, monkeypatch):
    latest = tmp_path / "cost_sweep.json"
    expected = tmp_path / "expected_cost.json"
    _write_json(latest, _latest(_cell("r0.0_s0.0", 0.9000, 87.0)))
    _write_json(expected, _expected(_cell("r0.0_s0.0", 0.7952, 87.0)))
    monkeypatch.setattr(cost_drift_check, "_LATEST_PATH", latest)
    monkeypatch.setattr(cost_drift_check, "_EXPECTED_PATH", expected)

    with pytest.raises(SystemExit) as exc:
        cost_drift_check.main([])
    assert exc.value.code == 1


# ---------------------------------------------------------------------------
# Threshold boundary (strict >)
# ---------------------------------------------------------------------------


def test_cost_drift_savings_delta_exactly_at_threshold_passes(cost_drift_check, tmp_path):
    # delta == _SAVINGS_THRESHOLD should pass (strict >). baseline 0.0 keeps the
    # boundary delta binary-exact (abs((0.0 + 0.02) - 0.0) == 0.02 exactly).
    latest = tmp_path / "cost_sweep.json"
    expected = tmp_path / "expected_cost.json"
    baseline = 0.0
    at_threshold = baseline + cost_drift_check._SAVINGS_THRESHOLD
    _write_json(latest, _latest(_cell("r0.5_s0.0", at_threshold, 20.6)))
    _write_json(expected, _expected(_cell("r0.5_s0.0", baseline, 20.6)))

    passed, _messages = cost_drift_check.check_drift(latest, expected)
    assert passed is True


def test_cost_drift_savings_delta_just_over_threshold_fails(cost_drift_check, tmp_path):
    latest = tmp_path / "cost_sweep.json"
    expected = tmp_path / "expected_cost.json"
    baseline = 0.0
    over_threshold = baseline + cost_drift_check._SAVINGS_THRESHOLD + 0.01
    _write_json(latest, _latest(_cell("r0.5_s0.0", over_threshold, 20.6)))
    _write_json(expected, _expected(_cell("r0.5_s0.0", baseline, 20.6)))

    passed, _messages = cost_drift_check.check_drift(latest, expected)
    assert passed is False


def test_cost_drift_avoided_delta_exactly_at_threshold_passes(cost_drift_check, tmp_path):
    latest = tmp_path / "cost_sweep.json"
    expected = tmp_path / "expected_cost.json"
    baseline = 20.0
    at_threshold = baseline + cost_drift_check._AVOIDED_THRESHOLD
    _write_json(latest, _latest(_cell("r0.5_s0.0", 0.1833, at_threshold)))
    _write_json(expected, _expected(_cell("r0.5_s0.0", 0.1833, baseline)))

    passed, _messages = cost_drift_check.check_drift(latest, expected)
    assert passed is True


def test_cost_drift_avoided_delta_just_over_threshold_fails(cost_drift_check, tmp_path):
    latest = tmp_path / "cost_sweep.json"
    expected = tmp_path / "expected_cost.json"
    baseline = 20.0
    over_threshold = baseline + cost_drift_check._AVOIDED_THRESHOLD + 0.5
    _write_json(latest, _latest(_cell("r0.5_s0.0", 0.1833, over_threshold)))
    _write_json(expected, _expected(_cell("r0.5_s0.0", 0.1833, baseline)))

    passed, _messages = cost_drift_check.check_drift(latest, expected)
    assert passed is False


# ---------------------------------------------------------------------------
# Error paths: missing files + cell-set mismatch
# ---------------------------------------------------------------------------


def test_cost_drift_check_fails_on_missing_latest(cost_drift_check, tmp_path):
    expected = tmp_path / "expected_cost.json"
    _write_json(expected, _expected(_cell("r0.0_s0.0", 0.7952, 87.0)))
    latest = tmp_path / "nonexistent.json"

    passed, messages = cost_drift_check.check_drift(latest, expected)
    assert passed is False
    assert any("not found" in line for line in messages)


def test_cost_drift_check_fails_on_missing_expected(cost_drift_check, tmp_path):
    latest = tmp_path / "cost_sweep.json"
    _write_json(latest, _latest(_cell("r0.0_s0.0", 0.7952, 87.0)))
    expected = tmp_path / "nonexistent.json"

    passed, messages = cost_drift_check.check_drift(latest, expected)
    assert passed is False
    assert any("not found" in line for line in messages)


def test_cost_drift_check_fails_on_cell_set_mismatch(cost_drift_check, tmp_path):
    # One cell MISSING from latest, one UNEXPECTED in latest — both names reported.
    latest = tmp_path / "cost_sweep.json"
    expected = tmp_path / "expected_cost.json"
    _write_json(
        latest,
        _latest(
            _cell("r0.0_s0.0", 0.7952, 87.0),
            _cell("r9.9_s9.9", 0.1, 1.0),  # unexpected
        ),
    )
    _write_json(
        expected,
        _expected(
            _cell("r0.0_s0.0", 0.7952, 87.0),
            _cell("r1.0_s0.0", 0.0, 0.0),  # missing from latest
        ),
    )

    passed, messages = cost_drift_check.check_drift(latest, expected)
    assert passed is False
    blob = "\n".join(messages)
    assert "MISSING" in blob and "r1.0_s0.0" in blob
    assert "UNEXPECTED" in blob and "r9.9_s9.9" in blob


# ---------------------------------------------------------------------------
# Pre-registration doc structure
# ---------------------------------------------------------------------------


def test_cost_preregistration_has_three_verdicts_and_distinguishers():
    doc = (_REPO_ROOT / "benchmarks" / "cost_preregistration.md").read_text()

    # The three falsifiability verdicts.
    assert "PASS" in doc
    assert "NULL" in doc
    assert "INCONCLUSIVE" in doc

    # The four INCONCLUSIVE distinguishers to triage before declaring NULL.
    for distinguisher in (
        "gating-dominates-everywhere",
        "grid-too-coarse",
        "degenerate-sensitivity-model",
        "baseline-mis-modeled",
    ):
        assert distinguisher in doc


# ---------------------------------------------------------------------------
# Answer-sensitivity axis is gated (wasted_refetches) + stream routing
# ---------------------------------------------------------------------------


def test_cost_drift_wasted_delta_just_over_threshold_fails(cost_drift_check, tmp_path):
    """savings/avoided match; only wasted_refetches drifts past its threshold.

    Guards the answer-sensitivity axis -- savings_ratio is sensitivity-invariant
    by design, so without gating wasted_refetches a sensitivity-axis regression
    would pass CI silently.
    """
    over = cost_drift_check._WASTED_THRESHOLD + 0.01
    latest = tmp_path / "cost_sweep.json"
    expected = tmp_path / "expected_cost.json"
    _write_json(latest, _latest(_cell("r0.5_s0.0", 0.5, 20.0, wasted_refetches=over)))
    _write_json(expected, _expected(_cell("r0.5_s0.0", 0.5, 20.0, wasted_refetches=0.0)))

    passed, messages = cost_drift_check.check_drift(latest, expected)
    assert passed is False
    assert any("wasted_refetches" in m for m in messages)


def test_cost_drift_main_routes_report_to_stdout_on_failure(
    cost_drift_check, tmp_path, monkeypatch, capsys
):
    """On failure the drift table goes to stdout (so a CI job capturing stdout
    sees it), and stderr carries the terminal failure signal."""
    latest = tmp_path / "cost_sweep.json"
    expected = tmp_path / "expected_cost.json"
    _write_json(latest, _latest(_cell("r0.0_s0.0", 0.9000, 87.0)))  # savings drifts
    _write_json(expected, _expected(_cell("r0.0_s0.0", 0.7952, 87.0)))
    monkeypatch.setattr(cost_drift_check, "_LATEST_PATH", latest)
    monkeypatch.setattr(cost_drift_check, "_EXPECTED_PATH", expected)

    with pytest.raises(SystemExit) as exc:
        cost_drift_check.main([])
    assert exc.value.code == 1
    captured = capsys.readouterr()
    assert "savings_ratio" in captured.out
    assert "FAILED" in captured.err


def test_cost_drift_check_flags_override_paths(cost_drift_check, tmp_path):
    """--latest / --expected route to the given files (no monkeypatching needed)."""
    latest = tmp_path / "sweep.json"
    expected = tmp_path / "baseline.json"
    _write_json(latest, _latest(_cell("r0.0_s0.0", 0.5, 10.0)))
    _write_json(expected, _expected(_cell("r0.0_s0.0", 0.5, 10.0)))

    with pytest.raises(SystemExit) as exc:
        cost_drift_check.main(["--latest", str(latest), "--expected", str(expected)])
    assert exc.value.code == 0
