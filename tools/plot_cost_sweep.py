# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Plot the temporal-cost savings-regime curve from a cost-sweep payload.

Reads a ``cost_sweep`` JSON (see ``tools/run_cost_sweep.py``) and renders
``savings_ratio`` vs change-rate as an SVG. ``savings_ratio`` is
answer-sensitivity-invariant by design, so the curve is drawn from the
``sensitivity == 0.0`` rows; the pre-registered PASS thresholds (``X`` min
savings, ``R`` realistic-change-rate ceiling) are overlaid so the figure shows
the verdict, not just the numbers. Provenance is printed in the title.

Reproduce:
    python tools/plot_cost_sweep.py \
        --input benchmarks/results/cost_sweep_published.json \
        --output benchmarks/results/cost_sweep_savings_curve.svg
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")  # headless: no display needed
import matplotlib.pyplot as plt  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parents[1]
X_THRESHOLD = 0.30  # pre-registered min savings (savings_ratio >= X across r <= R)
R_CEILING = 0.30  # pre-registered realistic-change-rate ceiling


def _resolve(p: str) -> Path:
    path = Path(p)
    return path if path.is_absolute() else REPO_ROOT / path


def plot(input_path: Path, output_path: Path) -> None:
    payload = json.loads(input_path.read_text(encoding="utf-8"))
    rows = sorted(
        (r for r in payload["rows"] if r["sensitivity"] == 0.0),
        key=lambda r: r["rate"],
    )
    rates = [r["rate"] for r in rows]
    savings = [r["savings_ratio"] * 100 for r in rows]

    fig, ax = plt.subplots(figsize=(8, 5))
    ax.plot(rates, savings, marker="o", color="#2563eb", label="gating savings")
    ax.axhline(
        X_THRESHOLD * 100, color="#dc2626", linestyle="--", linewidth=1,
        label=f"PASS bar X = {X_THRESHOLD:.0%}",
    )
    ax.axvline(
        R_CEILING, color="#16a34a", linestyle=":", linewidth=1,
        label=f"realistic ceiling R = {R_CEILING}",
    )
    # Shade the in-band PASS region (r <= R, savings >= X).
    ax.axvspan(0, R_CEILING, color="#16a34a", alpha=0.06)

    ax.set_xlabel("source change-rate  r  (per-artifact volatility)")
    ax.set_ylabel("re-fetch savings vs always-read  (%)")
    ax.set_title(
        f"Temporal-cost savings regime  ·  n={payload['runs_per_point']}/point\n"
        f"[provenance: {payload['provenance']}]",
        fontsize=10,
    )
    ax.set_ylim(0, 100)
    ax.set_xlim(0, max(rates))
    ax.grid(True, alpha=0.3)
    ax.legend(loc="upper right", fontsize=9)
    fig.tight_layout()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, format="svg")
    print(f"Wrote {output_path}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Plot the cost-sweep savings curve.")
    parser.add_argument("--input", default="benchmarks/results/cost_sweep_published.json")
    parser.add_argument("--output", default="benchmarks/results/cost_sweep_savings_curve.svg")
    args = parser.parse_args(argv)
    plot(_resolve(args.input), _resolve(args.output))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
