# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""HTML/JSON report generation for simulation strategy comparisons."""

from __future__ import annotations

import json
from html import escape
from pathlib import Path
from typing import Any

from ccs.simulation.metrics import StrategyComparisonReport

_TEMPLATE_PATH = Path(__file__).with_name("templates") / "comparison_report.html"


def build_dashboard_payload(report: StrategyComparisonReport) -> dict[str, Any]:
    """Build versioned JSON payload contract for dashboard consumers."""
    return {
        "schema_version": "ccs.report.v1",
        "report": report.to_dict(),
    }


def render_html_report(report: StrategyComparisonReport) -> str:
    """Render standalone HTML report from comparison payload."""
    template = _TEMPLATE_PATH.read_text(encoding="utf-8")
    dashboard_payload = build_dashboard_payload(report)
    summary_table = _render_summary_table(report)
    report_json = escape(json.dumps(dashboard_payload, indent=2, sort_keys=True))
    scenario = escape(report.scenario)
    strategies = ", ".join(escape(name) for name in report.strategies)

    return (
        template.replace("{{SCENARIO}}", scenario)
        .replace("{{RUNS_PER_STRATEGY}}", str(report.runs_per_strategy))
        .replace("{{SEED_START}}", str(report.seed_start))
        .replace("{{STRATEGIES}}", strategies)
        .replace("{{SUMMARY_TABLE}}", summary_table)
        .replace("{{REPORT_JSON}}", report_json)
    )


def write_html_report(report: StrategyComparisonReport, destination: str | Path) -> Path:
    """Write HTML report to destination path."""
    output_path = Path(destination)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_html_report(report), encoding="utf-8")
    return output_path


def write_json_report(report: StrategyComparisonReport, destination: str | Path) -> Path:
    """Write dashboard JSON payload to destination path."""
    output_path = Path(destination)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    payload = build_dashboard_payload(report)
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return output_path


def _render_summary_table(report: StrategyComparisonReport) -> str:
    rows: list[str] = []
    for item in report.aggregated:
        rows.append(
            "".join(
                [
                    "<tr>",
                    f"<td>{escape(str(item.get('strategy', '')))}</td>",
                    f"<td>{_fmt(item.get('synchronization_tokens_mean'))}</td>",
                    f"<td>{_fmt(item.get('fetch_tokens_mean'))}</td>",
                    f"<td>{_fmt(item.get('broadcast_tokens_mean'))}</td>",
                    f"<td>{_fmt(item.get('invalidation_tokens_mean'))}</td>",
                    f"<td>{_fmt(item.get('cache_hit_rate_mean'))}</td>",
                    f"<td>{_fmt(item.get('stale_reads_mean'))}</td>",
                    f"<td>{_fmt(item.get('max_stale_steps_mean'))}</td>",
                    f"<td>{_fmt(item.get('sync_broadcast_ratio_mean'))}</td>",
                    "</tr>",
                ]
            )
        )
    if not rows:
        rows = ["<tr><td colspan='9'>No data</td></tr>"]

    header = (
        "<thead><tr>"
        "<th>Strategy</th>"
        "<th>Sync Tokens Mean</th>"
        "<th>Fetch Tokens Mean</th>"
        "<th>Broadcast Tokens Mean</th>"
        "<th>Invalidation Tokens Mean</th>"
        "<th>Cache Hit Rate Mean</th>"
        "<th>Stale Reads Mean</th>"
        "<th>Max Stale Steps Mean</th>"
        "<th>Sync/Broadcast Ratio Mean</th>"
        "</tr></thead>"
    )
    body = "<tbody>" + "".join(rows) + "</tbody>"
    return "<table>" + header + body + "</table>"


def _fmt(value: Any) -> str:
    if value is None:
        return "-"
    if isinstance(value, float):
        return f"{value:.4f}"
    return escape(str(value))
