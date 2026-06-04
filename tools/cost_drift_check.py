# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""CI drift check: compare benchmarks/results/cost_sweep.json against benchmarks/expected_cost.json.

Mirrors the token drift-check contract (tools/benchmark_drift_check.py) for the
cost lane. Exits 0 if every cell's ``savings_ratio`` and ``refetches_avoided``
are within their committed-baseline tolerances. Exits 1 on any drift past a
threshold, a missing file, or a cell-set mismatch.

Two gating metrics, two tolerances (both strict ``>`` — a delta exactly at the
threshold passes):
  - ``savings_ratio``     : absolute tolerance ``_SAVINGS_THRESHOLD`` (the ratio
    is bounded [0, 1] and already rounded to 4 dp, so an absolute band is the
    natural unit — 0.02 ≈ 2 percentage points of savings).
  - ``refetches_avoided`` : absolute tolerance ``_AVOIDED_THRESHOLD`` on the
    mean fetch-action count (run-to-run averaging jitter is the only expected
    source of drift for a seeded sweep, so a small absolute band suffices).

Usage:
    python tools/cost_drift_check.py
    make cost-benchmark-check
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).parent.parent
_LATEST_PATH = _REPO_ROOT / "benchmarks" / "results" / "cost_sweep.json"
_EXPECTED_PATH = _REPO_ROOT / "benchmarks" / "expected_cost.json"

# Strict >; a delta exactly at the threshold passes (mirrors benchmark_drift_check).
_SAVINGS_THRESHOLD = 0.02  # absolute, on the [0, 1] savings_ratio
_AVOIDED_THRESHOLD = 2.0  # absolute, on the mean refetches_avoided count


def check_drift(latest_path: Path, expected_path: Path) -> tuple[bool, list[str]]:
    """Compare latest against expected; return ``(passed, messages)``.

    ``messages`` is the full human-readable report (table rows + any errors), in
    print order. ``main`` prints it and maps ``passed`` to the exit code.
    """
    if not expected_path.exists():
        return False, [
            f"ERROR: {expected_path} not found — "
            "run `make cost-benchmark` to establish a baseline, then commit the file.",
        ]

    if not latest_path.exists():
        return False, [
            f"ERROR: {latest_path} not found — run `make cost-benchmark` first.",
        ]

    latest_data = json.loads(latest_path.read_text())
    expected_data = json.loads(expected_path.read_text())

    # Latest is the sweep payload (rows under "rows"); expected is the committed
    # baseline (cells under "cells"). Both key on "cell".
    latest_by_cell = {row["cell"]: row for row in latest_data.get("rows", [])}
    expected_by_cell = {cell["cell"]: cell for cell in expected_data.get("cells", [])}

    errors: list[str] = []

    # Cells in expected but missing from latest
    for cell in expected_by_cell:
        if cell not in latest_by_cell:
            errors.append(f"  MISSING from latest: '{cell}' (present in expected)")

    # Cells in latest but not in expected
    for cell in latest_by_cell:
        if cell not in expected_by_cell:
            errors.append(f"  UNEXPECTED in latest: '{cell}' (not in expected)")

    if errors:
        messages = ["Cost cell set mismatch:"]
        messages.extend(errors)
        messages.append(
            "\nUpdate benchmarks/expected_cost.json by running `make cost-benchmark` "
            "and committing the result."
        )
        return False, messages

    # Drift check for matched cells.
    col = 14
    header = (
        f"  {'Cell':<{col}} {'metric':<18} {'Expected':>10}  {'Actual':>10}  {'Delta':>9}"
    )
    messages = [header, "-" * (col + 55)]
    drifted: list[str] = []

    for cell, expected_cell in expected_by_cell.items():
        latest_cell = latest_by_cell[cell]
        for metric, threshold in (
            ("savings_ratio", _SAVINGS_THRESHOLD),
            ("refetches_avoided", _AVOIDED_THRESHOLD),
        ):
            exp = float(expected_cell[metric])
            act = float(latest_cell[metric])
            delta = abs(act - exp)
            is_drift = delta > threshold
            flag = " ← DRIFT" if is_drift else ""
            messages.append(
                f"  {cell:<{col}} {metric:<18} {exp:>10.4f}  {act:>10.4f}  {delta:>9.4f}{flag}"
            )
            if is_drift:
                drifted.append(f"{cell}/{metric} (Δ={delta:.4f} > {threshold})")

    if drifted:
        messages.append("")
        messages.append(
            f"Cost benchmark regression check FAILED: {len(drifted)} metric(s) drifted: "
            + ", ".join(drifted)
        )
        messages.append(
            "Run `make cost-benchmark` and commit the updated benchmarks/expected_cost.json "
            "if the change is intentional."
        )
        return False, messages

    messages.append("")
    messages.append("Cost benchmark regression check passed.")
    return True, messages


def main() -> None:
    passed, messages = check_drift(_LATEST_PATH, _EXPECTED_PATH)
    stream = sys.stdout if passed else sys.stderr
    for line in messages:
        print(line, file=stream)
    sys.exit(0 if passed else 1)


if __name__ == "__main__":
    main()
