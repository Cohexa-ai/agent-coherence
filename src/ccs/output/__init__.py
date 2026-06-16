# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Reporting and dashboard output helpers."""

from .report import build_dashboard_payload, render_html_report, write_html_report, write_json_report

__all__ = [
    "build_dashboard_payload",
    "render_html_report",
    "write_html_report",
    "write_json_report",
]
