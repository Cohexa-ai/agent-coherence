# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""Cost sweep: change-rate × answer-sensitivity → provenance-labeled savings map.

Third benchmark lane (temporal simulation). For every (change-rate, answer-
sensitivity) cell this sweeps THREE re-fetch configs and emits how many
re-fetches gating avoids versus an always-re-fetch baseline, plus how many of
gating's re-fetches were wasted (answer-irrelevant churn):

  - blind   : never re-fetches (the cost floor).
  - gated    : ``lazy`` under ``conditional_injection`` — re-fetches only on the
               invalidations a this-tick source mutation triggered.
  - always   : ``lazy`` under ``context_semantics.model = always_read`` — the
               re-fetch-on-every-read ceiling.

Each cell needs TWO runs because the always-re-fetch config is a different
``context_semantics.model`` (a scenario-level switch), not a strategy.

EVERY emitted figure is provenance-labeled: the payload carries a top-level
``provenance`` field and no bare percentage is written without it.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from ccs.simulation.engine import run_strategy_comparison  # noqa: E402
from ccs.simulation.metrics import StrategyComparisonReport  # noqa: E402
from ccs.simulation.scenarios import load_scenario  # noqa: E402

# Two-axis grid: change-rate (per-artifact volatility) × answer-sensitivity
# (fraction of source mutations that are answer-relevant). Small defaults so the
# driver runs fast; override via the callable for tests.
RATES = [0.0, 0.25, 0.5, 1.0]
SENSITIVITIES = [0.0, 0.5, 1.0]
RUNS_PER_POINT = 10

BASE_SCENARIO = REPO_ROOT / "benchmarks" / "scenarios" / "planning_canonical.yaml"
SEED_START = 20260318
PROVENANCE = "temporal-sim (third lane)"


def _make_scenario(rate: float, sensitivity: float) -> dict:
    """Gated config: source churn ON, ``conditional_injection`` (the default).

    Every mutable artifact's ``volatility`` is set to ``rate`` so the change-rate
    axis is a single dial; ``answer_sensitivity`` sets the relevant fraction.
    """
    scenario = load_scenario(str(BASE_SCENARIO))
    for artifact in scenario["artifacts"]:
        if bool(artifact.get("mutable", True)):
            artifact["volatility"] = rate
    scenario["source_mutation"] = {"enabled": True, "answer_sensitivity": sensitivity}
    scenario["simulation"]["seed"] = SEED_START
    scenario["scenario"]["name"] = f"cost-sweep-r{rate}-s{sensitivity}"
    return scenario


def _make_always_read_variant(rate: float, sensitivity: float) -> dict:
    """Always-re-fetch ceiling: same churn, but force a re-fetch on every read."""
    scenario = _make_scenario(rate, sensitivity)
    scenario["context_semantics"]["model"] = "always_read"
    scenario["scenario"]["name"] = f"cost-sweep-r{rate}-s{sensitivity}-always"
    return scenario


def _mean_fetch_actions(report: StrategyComparisonReport, strategy: str) -> float:
    """Average per-run ``fetch_actions`` for one strategy.

    ``fetch_actions`` lives only on per-run dicts (``report.runs``); it has no
    ``*_mean`` in ``report.aggregated``, so we average the per-run values here.
    ``report.runs`` is flattened across all strategies, hence the filter.
    """
    values = [m.to_dict()["fetch_actions"] for m in report.runs if m.strategy == strategy]
    if not values:
        return 0.0
    return sum(values) / len(values)


def _aggregated_by_strategy(report: StrategyComparisonReport) -> dict[str, dict]:
    return {item["strategy"]: item for item in report.aggregated}


def _build_cell_row(rate: float, sensitivity: float, runs_per_point: int) -> dict:
    """Run both configs for one cell and compute the savings-regime row."""
    gated_report = run_strategy_comparison(
        _make_scenario(rate, sensitivity),
        strategies=["blind", "lazy"],
        runs=runs_per_point,
        seed_start=SEED_START,
    )
    always_report = run_strategy_comparison(
        _make_always_read_variant(rate, sensitivity),
        strategies=["lazy"],
        runs=runs_per_point,
        seed_start=SEED_START,
    )

    blind_fetches = _mean_fetch_actions(gated_report, "blind")
    gated_fetches = _mean_fetch_actions(gated_report, "lazy")
    always_fetches = _mean_fetch_actions(always_report, "lazy")

    refetches_avoided = always_fetches - gated_fetches
    wasted_refetches = float(_aggregated_by_strategy(gated_report)["lazy"]["wasted_refetches_mean"])
    savings_ratio = 0.0 if always_fetches == 0 else 1.0 - gated_fetches / always_fetches

    return {
        "cell": f"r{rate}_s{sensitivity}",
        "rate": rate,
        "sensitivity": sensitivity,
        "blind_fetches": blind_fetches,
        "gated_fetches": gated_fetches,
        "always_fetches": always_fetches,
        "refetches_avoided": refetches_avoided,
        "wasted_refetches": wasted_refetches,
        "savings_ratio": round(savings_ratio, 4),
    }


def run_cost_sweep(
    rates: list[float],
    sensitivities: list[float],
    runs_per_point: int,
) -> dict:
    """Sweep change-rate × answer-sensitivity and return the savings-map payload.

    Callable without argparse so tests can drive a tiny grid. The returned dict
    is the exact JSON payload the CLI writes.
    """
    rows = [
        _build_cell_row(rate, sensitivity, runs_per_point)
        for rate in rates
        for sensitivity in sensitivities
    ]
    return {
        "sweep": "cost",
        "provenance": PROVENANCE,
        "runs_per_point": runs_per_point,
        "rates": list(rates),
        "sensitivities": list(sensitivities),
        "rows": rows,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Cost sweep (change-rate × answer-sensitivity) savings-regime map."
    )
    parser.add_argument("--output", default="benchmarks/results/cost_sweep.json")
    args = parser.parse_args(argv)

    payload = run_cost_sweep(RATES, SENSITIVITIES, RUNS_PER_POINT)
    for row in payload["rows"]:
        # Provenance-labeled: the savings_ratio percentage is always reported
        # alongside the payload's top-level ``provenance`` field.
        print(
            f"  {row['cell']:>12}: blind={row['blind_fetches']:>8.1f} "
            f"gated={row['gated_fetches']:>8.1f} always={row['always_fetches']:>8.1f} "
            f"avoided={row['refetches_avoided']:>8.1f} wasted={row['wasted_refetches']:>8.1f} "
            f"savings={row['savings_ratio']:.1%}"
        )

    output_path = REPO_ROOT / args.output
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"Wrote {output_path}  [provenance: {payload['provenance']}]")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
