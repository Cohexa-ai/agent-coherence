# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""Tests for scenario loading and schema validation."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from ccs.core.exceptions import ScenarioValidationError
from ccs.simulation.scenarios import load_scenario


def _write_yaml(path: Path, payload: dict) -> None:
    path.write_text(yaml.safe_dump(payload), encoding="utf-8")


def test_load_valid_scenario_with_legacy_aliases(tmp_path: Path) -> None:
    scenario_path = tmp_path / "scenario.yaml"
    payload = {
        "simulation": {
            "duration_ticks": 10,
            "agents": 2,
            "actions_per_tick": 2,
            "latency_ticks": 1,
        },
        "scenario": {
            "name": "valid",
            "workload_name": "read_heavy",
            "write_probability": 0.2,
        },
        "artifacts": [
            {"id": "plan.md", "size_tokens": 4096, "volatility": 0.3},
        ],
        "strategies": {
            "lazy": {"check_interval_ticks": 2},
            "lease": {"default_ttl_ticks": 10},
            "exec_count": {"max_operations": 3},
        },
    }
    _write_yaml(scenario_path, payload)

    config = load_scenario(str(scenario_path))

    assert config["simulation"]["num_agents"] == 2
    assert config["scenario"]["workload"] == "read_heavy"
    assert config["strategies"]["access_count"]["max_accesses"] == 3
    assert config["strategies"]["exec_count"]["max_operations"] == 3
    assert config["context_semantics"]["model"] == "conditional_injection"


def test_load_scenario_rejects_missing_action_rate(tmp_path: Path) -> None:
    scenario_path = tmp_path / "invalid.yaml"
    payload = {
        "simulation": {"duration_ticks": 10, "num_agents": 2},
        "network": {"latency_ticks": 1, "message_loss_rate": 0.0},
        "scenario": {"name": "invalid", "workload": "custom", "write_probability": 0.1},
        "artifacts": [{"id": "plan.md", "size_tokens": 1024}],
        "strategies": {},
        "transient": {"timeout_ticks": 5},
        "context_semantics": {"model": "pointer"},
    }
    _write_yaml(scenario_path, payload)

    with pytest.raises(ScenarioValidationError):
        load_scenario(str(scenario_path))


def test_load_scenario_rejects_invalid_context_model(tmp_path: Path) -> None:
    scenario_path = tmp_path / "invalid-context.yaml"
    payload = {
        "simulation": {"duration_ticks": 10, "num_agents": 2, "seed": 1},
        "network": {"latency_ticks": 1, "message_loss_rate": 0.0},
        "scenario": {
            "name": "invalid",
            "workload": "custom",
            "action_probability": 0.5,
            "write_probability": 0.2,
        },
        "artifacts": [{"id": "plan.md", "size_tokens": 1024}],
        "strategies": {},
        "transient": {"timeout_ticks": 5},
        "context_semantics": {"model": "unknown"},
    }
    _write_yaml(scenario_path, payload)

    with pytest.raises(ScenarioValidationError):
        load_scenario(str(scenario_path))


def test_load_scenario_rejects_revocation_tick_out_of_range(tmp_path: Path) -> None:
    scenario_path = tmp_path / "invalid-revocation-tick.yaml"
    payload = {
        "simulation": {"duration_ticks": 10, "num_agents": 2, "seed": 1},
        "network": {"latency_ticks": 1, "message_loss_rate": 0.0},
        "scenario": {
            "name": "invalid",
            "workload": "custom",
            "action_probability": 0.5,
            "write_probability": 0.2,
            "revocation_tick": 10,
        },
        "artifacts": [{"id": "plan.md", "size_tokens": 1024}],
        "strategies": {},
        "transient": {"timeout_ticks": 5},
        "context_semantics": {"model": "pointer"},
    }
    _write_yaml(scenario_path, payload)

    with pytest.raises(ScenarioValidationError):
        load_scenario(str(scenario_path))



def _minimal_scenario_payload() -> dict:
    return {
        "simulation": {"duration_ticks": 10, "num_agents": 2, "seed": 1},
        "network": {"latency_ticks": 1, "message_loss_rate": 0.0},
        "scenario": {
            "name": "valid",
            "workload": "custom",
            "action_probability": 0.5,
            "write_probability": 0.2,
        },
        "artifacts": [{"id": "plan.md", "size_tokens": 1024}],
        "strategies": {},
        "transient": {"timeout_ticks": 5},
        "context_semantics": {"model": "pointer"},
    }


def test_load_scenario_without_crash_recovery_block_uses_defaults(tmp_path: Path) -> None:
    scenario_path = tmp_path / "no-cr.yaml"
    payload = _minimal_scenario_payload()
    _write_yaml(scenario_path, payload)

    config = load_scenario(str(scenario_path))
    # Block is normalized to an empty mapping; engine reads defaults from CrashRecoveryConfig.
    assert config["crash_recovery"] == {}


def test_load_scenario_with_crash_recovery_block_parses(tmp_path: Path) -> None:
    scenario_path = tmp_path / "with-cr.yaml"
    payload = _minimal_scenario_payload()
    payload["crash_recovery"] = {
        "enabled": True,
        "heartbeat_timeout_ticks": 5,
        "max_hold_ticks": 50,
    }
    _write_yaml(scenario_path, payload)

    config = load_scenario(str(scenario_path))
    assert config["crash_recovery"]["enabled"] is True
    assert config["crash_recovery"]["heartbeat_timeout_ticks"] == 5
    assert config["crash_recovery"]["max_hold_ticks"] == 50


def test_load_scenario_rejects_zero_heartbeat_timeout(tmp_path: Path) -> None:
    scenario_path = tmp_path / "bad-cr-hb.yaml"
    payload = _minimal_scenario_payload()
    payload["crash_recovery"] = {"heartbeat_timeout_ticks": 0}
    _write_yaml(scenario_path, payload)

    with pytest.raises(ScenarioValidationError, match="heartbeat_timeout_ticks"):
        load_scenario(str(scenario_path))


def test_load_scenario_rejects_non_bool_enabled(tmp_path: Path) -> None:
    scenario_path = tmp_path / "bad-cr-enabled.yaml"
    payload = _minimal_scenario_payload()
    payload["crash_recovery"] = {"enabled": "yes"}
    _write_yaml(scenario_path, payload)

    with pytest.raises(ScenarioValidationError, match="enabled"):
        load_scenario(str(scenario_path))


def test_load_scenario_rejects_negative_max_hold(tmp_path: Path) -> None:
    scenario_path = tmp_path / "bad-cr-mh.yaml"
    payload = _minimal_scenario_payload()
    payload["crash_recovery"] = {"max_hold_ticks": -1}
    _write_yaml(scenario_path, payload)

    with pytest.raises(ScenarioValidationError, match="max_hold_ticks"):
        load_scenario(str(scenario_path))
