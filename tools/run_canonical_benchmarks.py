# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Canonical benchmark runner for paper Table 1 reproduction checks."""

from __future__ import annotations

from datetime import date
import hashlib
import json
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from ccs.output.report import write_html_report, write_json_report
from ccs.simulation.engine import run_strategy_comparison
from ccs.simulation.scenarios import load_scenario

CANONICAL_SCENARIOS = [
    ("planning_canonical.yaml", "A: Planning", 0.05, 0.950, 0.03),
    ("analysis_canonical.yaml", "B: Analysis", 0.10, 0.923, 0.03),
    ("dev_canonical.yaml", "C: Development", 0.25, 0.883, 0.04),
    ("churn_canonical.yaml", "D: High Churn", 0.50, 0.842, 0.05),
]

STRATEGIES = ["broadcast", "lazy"]
RUNS = 10
BROADCAST_BASELINE = 4 * 40 * 3 * 4096


def main() -> int:
    scenario_root = REPO_ROOT / "benchmarks" / "scenarios"
    output_root = REPO_ROOT / "benchmarks" / "results" / "canonical"
    output_root.mkdir(parents=True, exist_ok=True)

    summary_rows = [
        "| Scenario | V | T_broadcast | T_lazy | Savings | Target | Status |",
        "|---|---|---:|---:|---:|---:|---|",
    ]
    all_pass = True

    for file_name, label, volatility, target, tolerance in CANONICAL_SCENARIOS:
        scenario_path = scenario_root / file_name
        scenario = load_scenario(str(scenario_path))
        seed_start = int(scenario["simulation"]["seed"])
        report = run_strategy_comparison(
            scenario,
            strategies=STRATEGIES,
            runs=RUNS,
            seed_start=seed_start,
        )

        stem = scenario_path.stem
        write_json_report(report, output_root / f"{stem}.json")
        write_html_report(report, output_root / f"{stem}.html")

        aggregated = {item["strategy"]: item for item in report.aggregated}
        broadcast_sync = float(aggregated["broadcast"]["synchronization_tokens_mean"])
        lazy_sync = float(aggregated["lazy"]["synchronization_tokens_mean"])
        savings = max(0.0, 1.0 - lazy_sync / broadcast_sync) if broadcast_sync > 0 else 0.0

        baseline_ok = abs(broadcast_sync - BROADCAST_BASELINE) / BROADCAST_BASELINE < 0.05
        savings_ok = abs(savings - target) <= tolerance
        status = "PASS" if baseline_ok and savings_ok else "FAIL"
        if status == "FAIL":
            all_pass = False

        summary_rows.append(
            f"| {label} | {volatility:.2f} | {broadcast_sync:,.0f} | {lazy_sync:,.0f} "
            f"| {savings:.1%} | {target:.1%} ± {tolerance:.0%} | {status} |"
        )
        print(
            f"  {label}: T_bcast={broadcast_sync:,.0f} T_lazy={lazy_sync:,.0f} "
            f"savings={savings:.1%} target={target:.1%} [{status}]"
        )

    summary_contents = "\n".join(
        [
            "# Canonical Benchmark Summary — Paper §8 Table 1 Reproduction",
            "",
            f"- generated_on: {date.today()}",
            f"- baseline: T_broadcast = n*S*m*|d| = {BROADCAST_BASELINE:,} tokens",
            f"- runs_per_strategy: {RUNS}",
            f"- strategies: {', '.join(STRATEGIES)}",
            "",
            *summary_rows,
            "",
            "See `manifest.json` for reproducibility metadata.",
        ]
    ) + "\n"

    (output_root / "SUMMARY.md").write_text(summary_contents, encoding="utf-8")
    manifest = {
        "generated_on": str(date.today()),
        "broadcast_baseline_tokens": BROADCAST_BASELINE,
        "runs_per_strategy": RUNS,
        "strategies": STRATEGIES,
        "all_pass": all_pass,
        "baseline_checksums": {
            "SUMMARY.md": f"sha256:{hashlib.sha256(summary_contents.encode('utf-8')).hexdigest()}"
        },
    }
    (output_root / "manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True),
        encoding="utf-8",
    )

    if not all_pass:
        print("\n[FAIL] One or more scenarios did not match Table 1 targets.")
        return 1
    print("\n[PASS] All canonical scenarios match paper Table 1 within tolerance.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
