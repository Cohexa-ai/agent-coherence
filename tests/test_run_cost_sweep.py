# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""Tests for tools/run_cost_sweep.py — the change-rate × answer-sensitivity sweep.

``tools/`` is not an importable package, so the module is loaded by file path
(mirrors ``tests/test_benchmark_stage1.py``). Most assertions drive the
``run_cost_sweep`` callable directly with a tiny grid; the sensitivity-endpoint
edge case reads the engine aggregates directly, which is cleaner than threading
those figures through the sweep payload.
"""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[1]
_SRC_ROOT = _REPO_ROOT / "src"
if str(_SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(_SRC_ROOT))

from ccs.simulation.engine import run_strategy_comparison  # noqa: E402
from ccs.simulation.scenarios import load_scenario  # noqa: E402

_ROW_KEYS = {
    "cell",
    "rate",
    "sensitivity",
    "blind_fetches",
    "gated_fetches",
    "always_fetches",
    "refetches_avoided",
    "wasted_refetches",
    "savings_ratio",
}
_PAYLOAD_KEYS = {"sweep", "provenance", "runs_per_point", "rates", "sensitivities", "rows"}


def _load_cost_sweep_module():
    """Import tools/run_cost_sweep.py by file path (tools/ is not a package)."""
    spec = importlib.util.spec_from_file_location(
        "run_cost_sweep", _REPO_ROOT / "tools" / "run_cost_sweep.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _row_by_cell(payload: dict, cell: str) -> dict:
    return next(row for row in payload["rows"] if row["cell"] == cell)


def test_run_cost_sweep_happy_returns_full_grid() -> None:
    module = _load_cost_sweep_module()
    payload = module.run_cost_sweep([0.0, 1.0], [0.0, 1.0], runs_per_point=2)

    assert _PAYLOAD_KEYS <= set(payload)
    assert payload["sweep"] == "cost"
    assert payload["provenance"], "every figure must be provenance-labeled"
    assert payload["runs_per_point"] == 2
    assert len(payload["rows"]) == 4

    for row in payload["rows"]:
        assert _ROW_KEYS == set(row)
        expected_cell = f"r{row['rate']}_s{row['sensitivity']}"
        assert row["cell"] == expected_cell


def test_run_cost_sweep_rate_endpoints_are_monotonic() -> None:
    """rate=0.0 → gating barely re-fetches (avoids ≈ all always-re-fetches);
    rate=1.0 → gating ≈ always (avoids little). Assert avoided(0.0) > avoided(1.0).
    """
    module = _load_cost_sweep_module()
    payload = module.run_cost_sweep([0.0, 1.0], [0.5], runs_per_point=4)

    low = _row_by_cell(payload, "r0.0_s0.5")
    high = _row_by_cell(payload, "r1.0_s0.5")

    # At rate 0.0 the source never changes: gating's re-fetches avoided ≈ the
    # full always-re-fetch count, and gated stays near the blind floor.
    assert low["refetches_avoided"] == pytest.approx(low["always_fetches"], rel=0.25)
    assert low["gated_fetches"] == pytest.approx(low["blind_fetches"], abs=low["blind_fetches"])

    # At rate 1.0 gating re-fetches on essentially every churned read ⇒ ≈ always.
    assert high["refetches_avoided"] == pytest.approx(0.0, abs=2.0)

    # Monotonic direction: more avoided when the source is calm than when it churns.
    assert low["refetches_avoided"] > high["refetches_avoided"]


def _gated_scenario(rate: float, sensitivity: float) -> dict:
    scenario = load_scenario(
        str(_REPO_ROOT / "benchmarks" / "scenarios" / "planning_canonical.yaml")
    )
    for artifact in scenario["artifacts"]:
        if bool(artifact.get("mutable", True)):
            artifact["volatility"] = rate
    scenario["source_mutation"] = {"enabled": True, "answer_sensitivity": sensitivity}
    scenario["simulation"]["seed"] = 20260318
    scenario["scenario"]["name"] = f"cost-sens-r{rate}-s{sensitivity}"
    return scenario


def _lazy_aggregate(rate: float, sensitivity: float) -> dict:
    report = run_strategy_comparison(
        _gated_scenario(rate, sensitivity),
        strategies=["lazy"],
        runs=4,
        seed_start=20260318,
    )
    return {item["strategy"]: item for item in report.aggregated}["lazy"]


def test_sensitivity_endpoints_wasted_fraction() -> None:
    """sensitivity=0.0 ⇒ all churn answer-irrelevant (wasted == source_refetches);
    sensitivity=1.0 ⇒ none wasted (wasted == 0). Read engine aggregates directly.
    """
    # Use a churning rate so there are source re-fetches to classify.
    irrelevant = _lazy_aggregate(0.5, 0.0)
    assert irrelevant["source_refetches_mean"] > 0
    assert irrelevant["wasted_refetches_mean"] == pytest.approx(
        irrelevant["source_refetches_mean"]
    )

    relevant = _lazy_aggregate(0.5, 1.0)
    assert relevant["source_refetches_mean"] > 0
    assert relevant["wasted_refetches_mean"] == 0.0


def test_run_cost_sweep_cli_output_roundtrips(tmp_path: Path) -> None:
    """Writing via the --output mechanism produces JSON that round-trips."""
    module = _load_cost_sweep_module()
    # Shrink the module-level grid so the CLI path runs fast.
    module.RATES = [0.0, 1.0]
    module.SENSITIVITIES = [0.0, 1.0]
    module.RUNS_PER_POINT = 2

    # ``--output`` is joined onto the module's REPO_ROOT, so point REPO_ROOT at
    # tmp_path and pass a repo-relative output name for this run.
    module.REPO_ROOT = tmp_path
    rc = module.main(["--output", "cost_sweep.json"])
    assert rc == 0

    written = tmp_path / "cost_sweep.json"
    assert written.exists()
    payload = json.loads(written.read_text(encoding="utf-8"))

    assert _PAYLOAD_KEYS <= set(payload)
    assert payload["sweep"] == "cost"
    assert payload["provenance"]
    assert len(payload["rows"]) == 4
    for row in payload["rows"]:
        assert _ROW_KEYS == set(row)
