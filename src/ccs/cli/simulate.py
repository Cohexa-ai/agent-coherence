# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""CLI entrypoint for single-strategy simulation execution."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Sequence

from ccs.output.report import write_html_report, write_json_report
from ccs.simulation.aggregation import aggregate_strategy_runs
from ccs.simulation.engine import SimulationEngine
from ccs.simulation.metrics import StrategyComparisonReport
from ccs.simulation.scenarios import load_scenario


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run one CCS simulation strategy.")
    parser.add_argument("--scenario", required=True, help="Path to scenario YAML file.")
    parser.add_argument(
        "--strategy",
        default="lazy",
        choices=["eager", "lazy", "lease", "access_count"],
        help="Strategy to execute.",
    )
    parser.add_argument("--seed", type=int, default=None, help="Override simulation seed.")
    parser.add_argument("--output-json", default=None, help="Optional JSON output path.")
    parser.add_argument("--output-html", default=None, help="Optional HTML report output path.")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    scenario = load_scenario(args.scenario)
    metrics = SimulationEngine(
        scenario,
        strategy_name=args.strategy,
        seed=args.seed,
    ).run()
    payload = metrics.to_dict()

    if args.output_json:
        Path(args.output_json).parent.mkdir(parents=True, exist_ok=True)
        Path(args.output_json).write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    else:
        print(json.dumps(payload, indent=2, sort_keys=True))

    if args.output_html:
        aggregated = [aggregate_strategy_runs(metrics.strategy, [metrics]).to_dict()]
        report = StrategyComparisonReport(
            scenario=metrics.scenario,
            runs_per_strategy=1,
            seed_start=metrics.seed,
            strategies=[metrics.strategy],
            runs=[metrics],
            aggregated=aggregated,
        )
        write_html_report(report, args.output_html)
        if args.output_json:
            # Keep HTML and dashboard payload aligned for downstream tools.
            write_json_report(report, Path(args.output_json).with_suffix(".dashboard.json"))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
