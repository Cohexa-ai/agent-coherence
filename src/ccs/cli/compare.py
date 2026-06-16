# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""CLI entrypoint for multi-strategy comparison runs."""

from __future__ import annotations

import argparse
import json
from typing import Sequence

from ccs.output.report import build_dashboard_payload, write_html_report, write_json_report
from ccs.simulation.engine import run_strategy_comparison
from ccs.simulation.scenarios import load_scenario


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run CCS strategy comparison.")
    parser.add_argument("--scenario", required=True, help="Path to scenario YAML file.")
    parser.add_argument(
        "--strategies",
        default="eager,lazy,lease,access_count",
        help="Comma-separated strategy names.",
    )
    parser.add_argument("--runs", type=int, default=3, help="Runs per strategy.")
    parser.add_argument("--seed-start", type=int, default=0, help="First seed in range.")
    parser.add_argument("--output-json", default=None, help="Optional dashboard JSON output path.")
    parser.add_argument("--output-html", default=None, help="Optional HTML report output path.")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    strategies = [value.strip() for value in args.strategies.split(",") if value.strip()]
    scenario = load_scenario(args.scenario)
    report = run_strategy_comparison(
        scenario,
        strategies=strategies,
        runs=args.runs,
        seed_start=args.seed_start,
    )
    payload = build_dashboard_payload(report)

    if args.output_json:
        write_json_report(report, args.output_json)
    else:
        print(json.dumps(payload, indent=2, sort_keys=True))

    if args.output_html:
        write_html_report(report, args.output_html)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
