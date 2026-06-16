# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Verify reproduced benchmark values against committed Step5 SUMMARY baseline."""

from __future__ import annotations

import argparse
from pathlib import Path
import re
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from ccs.simulation.engine import run_strategy_comparison
from ccs.simulation.scenarios import load_scenario


def _parse_summary_rows(summary_path: Path) -> dict[str, float]:
    rows: dict[str, float] = {}
    for line in summary_path.read_text(encoding="utf-8").splitlines():
        if not line.startswith("|"):
            continue
        if line.startswith("|---") or "lazy_savings_vs_eager" in line:
            continue
        cells = [cell.strip() for cell in line.split("|")[1:-1]]
        if len(cells) != 7:
            continue
        scenario_name = cells[0]
        try:
            rows[scenario_name] = float(cells[6])
        except ValueError:
            continue
    return rows


def _scenario_slug(scenario_name: str) -> str:
    return re.sub(r"-", "_", scenario_name)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Verify step5 baseline matches fresh simulation output.")
    parser.add_argument("--baseline", required=True, help="Path to baseline SUMMARY.md")
    parser.add_argument("--tolerance", type=float, default=0.005, help="Allowed absolute delta for savings metric")
    args = parser.parse_args(argv)

    baseline_path = Path(args.baseline)
    if not baseline_path.exists():
        print(f"Baseline file not found: {baseline_path}", file=sys.stderr)
        return 1

    baseline_rows = _parse_summary_rows(baseline_path)
    if not baseline_rows:
        print("No data rows found in baseline summary.", file=sys.stderr)
        return 1

    scenarios_dir = REPO_ROOT / "benchmarks" / "scenarios"
    runs = 10
    seed_start_base = 2026030400
    strategies = ["eager", "lazy"]

    mismatches: list[str] = []
    checked = 0

    for scenario_name, expected_savings in baseline_rows.items():
        scenario_file = scenarios_dir / f"{_scenario_slug(scenario_name)}.yaml"
        if not scenario_file.exists():
            print(f"skip: no scenario file for baseline row '{scenario_name}'")
            continue

        scenario = load_scenario(str(scenario_file))
        seed_start = int(scenario["simulation"].get("seed", seed_start_base))
        report = run_strategy_comparison(
            scenario,
            strategies=strategies,
            runs=runs,
            seed_start=seed_start,
        )
        aggregated = {item["strategy"]: item for item in report.aggregated}
        eager_sync = float(aggregated["eager"]["synchronization_tokens_mean"])
        lazy_sync = float(aggregated["lazy"]["synchronization_tokens_mean"])
        actual_savings = 0.0 if eager_sync <= 0 else max(0.0, 1.0 - (lazy_sync / eager_sync))

        delta = abs(actual_savings - expected_savings)
        checked += 1
        print(
            f"  {scenario_name:28s} expected={expected_savings:.4f} "
            f"actual={actual_savings:.4f} delta={delta:.4f}"
        )
        if delta > args.tolerance:
            mismatches.append(
                f"{scenario_name}: expected {expected_savings:.4f}, actual {actual_savings:.4f}, delta {delta:.4f}"
            )

    if mismatches:
        print("\nBaseline verification FAILED", file=sys.stderr)
        for mismatch in mismatches:
            print(f"  - {mismatch}", file=sys.stderr)
        return 1

    print(f"\nBaseline verification passed ({checked} scenarios within ±{args.tolerance:.4f}).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
