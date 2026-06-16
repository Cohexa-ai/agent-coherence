# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""CLI tests for simulate and compare commands."""

from __future__ import annotations

import json
from pathlib import Path

import yaml

from ccs.cli.compare import main as compare_main
from ccs.cli.simulate import main as simulate_main


def _write_scenario(path: Path) -> None:
    payload = {
        "simulation": {"duration_ticks": 12, "num_agents": 3, "seed": 8, "action_probability": 0.7},
        "network": {"latency_ticks": 0, "message_loss_rate": 0.0},
        "scenario": {
            "name": "cli-smoke",
            "workload": "custom",
            "action_probability": 0.7,
            "write_probability": 0.25,
            "revocation_tick": None,
        },
        "artifacts": [
            {"id": "plan.md", "size_tokens": 300, "volatility": 0.1, "initial_version": 1, "mutable": True},
            {"id": "spec.json", "size_tokens": 500, "volatility": 0.1, "initial_version": 1, "mutable": True},
        ],
        "strategies": {
            "eager": {},
            "lazy": {"check_interval_ticks": 2},
            "lease": {"default_ttl_ticks": 5},
            "access_count": {"max_accesses": 4},
            "exec_count": {"max_operations": 4},
        },
        "transient": {"timeout_ticks": 5},
        "context_semantics": {"model": "pointer"},
    }
    path.write_text(yaml.safe_dump(payload), encoding="utf-8")


def test_simulate_cli_writes_json_and_html_outputs(tmp_path: Path) -> None:
    scenario = tmp_path / "scenario.yaml"
    out_json = tmp_path / "single.json"
    out_html = tmp_path / "single.html"
    _write_scenario(scenario)

    rc = simulate_main(
        [
            "--scenario",
            str(scenario),
            "--strategy",
            "lazy",
            "--seed",
            "42",
            "--output-json",
            str(out_json),
            "--output-html",
            str(out_html),
        ]
    )

    assert rc == 0
    assert out_json.exists()
    assert out_html.exists()
    data = json.loads(out_json.read_text(encoding="utf-8"))
    assert data["strategy"] == "lazy"
    assert "synchronization_tokens" in data
    assert (tmp_path / "single.dashboard.json").exists()


def test_compare_cli_writes_dashboard_json_and_html(tmp_path: Path) -> None:
    scenario = tmp_path / "scenario.yaml"
    out_json = tmp_path / "comparison.json"
    out_html = tmp_path / "comparison.html"
    _write_scenario(scenario)

    rc = compare_main(
        [
            "--scenario",
            str(scenario),
            "--strategies",
            "eager,lazy",
            "--runs",
            "2",
            "--seed-start",
            "50",
            "--output-json",
            str(out_json),
            "--output-html",
            str(out_html),
        ]
    )

    assert rc == 0
    assert out_json.exists()
    assert out_html.exists()
    payload = json.loads(out_json.read_text(encoding="utf-8"))
    assert payload["schema_version"] == "ccs.report.v1"
    assert payload["report"]["scenario"] == "cli-smoke"
    assert payload["report"]["strategies"] == ["eager", "lazy"]
