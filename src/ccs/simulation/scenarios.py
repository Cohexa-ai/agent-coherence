# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Scenario loading and schema validation for coherence simulations."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from ccs.core.exceptions import ScenarioValidationError

_SUPPORTED_CONTEXT_MODELS = {"always_read", "conditional_injection", "pointer"}
_SUPPORTED_WORKLOADS = {"read_heavy", "write_heavy", "parallel_editing", "large_artifact_reasoning", "custom"}


def _require_mapping(data: dict[str, Any], path: Path, field: str) -> dict[str, Any]:
    value = data.get(field)
    if not isinstance(value, dict):
        raise ScenarioValidationError(str(path), f"'{field}' must be a mapping")
    return value


def _require_int(
    value: Any,
    *,
    path: Path,
    field: str,
    min_value: int | None = None,
    allow_none: bool = False,
) -> int | None:
    if value is None and allow_none:
        return None
    if not isinstance(value, int):
        raise ScenarioValidationError(str(path), f"'{field}' must be an integer")
    if min_value is not None and value < min_value:
        raise ScenarioValidationError(str(path), f"'{field}' must be >= {min_value}")
    return value


def _require_float(
    value: Any,
    *,
    path: Path,
    field: str,
    min_value: float | None = None,
    max_value: float | None = None,
    allow_none: bool = False,
) -> float | None:
    if value is None and allow_none:
        return None
    if not isinstance(value, (int, float)):
        raise ScenarioValidationError(str(path), f"'{field}' must be numeric")
    result = float(value)
    if min_value is not None and result < min_value:
        raise ScenarioValidationError(str(path), f"'{field}' must be >= {min_value}")
    if max_value is not None and result > max_value:
        raise ScenarioValidationError(str(path), f"'{field}' must be <= {max_value}")
    return result


def _require_bool(value: Any, *, path: Path, field: str, allow_none: bool = False) -> bool | None:
    if value is None and allow_none:
        return None
    if not isinstance(value, bool):
        raise ScenarioValidationError(str(path), f"'{field}' must be boolean")
    return value


def _normalize_legacy_keys(data: dict[str, Any]) -> dict[str, Any]:
    simulation = data.setdefault("simulation", {})
    network = data.setdefault("network", {})
    scenario = data.setdefault("scenario", {})
    strategies = data.setdefault("strategies", {})
    transient = data.setdefault("transient", {})
    context_semantics = data.setdefault("context_semantics", {})
    data.setdefault("crash_recovery", {})
    data.setdefault("source_mutation", {})

    if "num_agents" not in simulation and "agents" in simulation:
        simulation["num_agents"] = simulation["agents"]
    simulation.setdefault("seed", 42)

    if "latency_ticks" not in network and "latency_ticks" in simulation:
        network["latency_ticks"] = simulation["latency_ticks"]
    if "message_loss_rate" not in network and "message_loss_rate" in simulation:
        network["message_loss_rate"] = simulation["message_loss_rate"]
    network.setdefault("latency_ticks", 1)
    network.setdefault("message_loss_rate", 0.0)

    # Legacy action knobs.
    if "agent_velocity" not in scenario and "actions_per_tick" in simulation:
        scenario["agent_velocity"] = simulation["actions_per_tick"]
    if "action_probability" not in scenario and "action_probability" in simulation:
        scenario["action_probability"] = simulation["action_probability"]

    # Legacy scenario keys from earlier drafts.
    if "workload" not in scenario and "workload_name" in scenario:
        scenario["workload"] = scenario["workload_name"]

    # Strategy alias conversion.
    if "access_count" not in strategies and "exec_count" in strategies:
        strategies["access_count"] = strategies["exec_count"]
    if isinstance(strategies.get("access_count"), dict):
        access = strategies["access_count"]
        if "max_accesses" not in access and "max_operations" in access:
            access["max_accesses"] = access["max_operations"]

    # Context semantics alias conversion.
    if "model" not in context_semantics:
        if "access_model" in scenario:
            context_semantics["model"] = scenario["access_model"]
        else:
            context_semantics["model"] = "conditional_injection"

    transient.setdefault("timeout_ticks", 5)
    scenario.setdefault("workload", "custom")
    scenario.setdefault("write_probability", 0.3)
    scenario.setdefault("revocation_tick", None)

    return data


def _validate_simulation(simulation: dict[str, Any], path: Path) -> None:
    _require_int(simulation.get("duration_ticks"), path=path, field="simulation.duration_ticks", min_value=1)
    _require_int(simulation.get("num_agents"), path=path, field="simulation.num_agents", min_value=1)
    _require_int(simulation.get("seed"), path=path, field="simulation.seed")


def _validate_network(network: dict[str, Any], path: Path) -> None:
    _require_int(network.get("latency_ticks"), path=path, field="network.latency_ticks", min_value=0)
    _require_float(
        network.get("message_loss_rate"),
        path=path,
        field="network.message_loss_rate",
        min_value=0.0,
        max_value=0.999999,
    )


def _validate_scenario(scenario: dict[str, Any], simulation: dict[str, Any], path: Path) -> None:
    name = scenario.get("name")
    if not isinstance(name, str) or not name.strip():
        raise ScenarioValidationError(str(path), "'scenario.name' must be a non-empty string")

    workload = scenario.get("workload")
    if workload not in _SUPPORTED_WORKLOADS:
        raise ScenarioValidationError(
            str(path),
            f"'scenario.workload' must be one of: {', '.join(sorted(_SUPPORTED_WORKLOADS))}",
        )

    action_probability = _require_float(
        scenario.get("action_probability"),
        path=path,
        field="scenario.action_probability",
        min_value=0.0,
        max_value=1.0,
        allow_none=True,
    )
    agent_velocity = _require_int(
        scenario.get("agent_velocity"),
        path=path,
        field="scenario.agent_velocity",
        min_value=1,
        allow_none=True,
    )
    if action_probability is None and agent_velocity is None:
        raise ScenarioValidationError(
            str(path),
            "scenario must define either action_probability or agent_velocity",
        )

    _require_float(
        scenario.get("write_probability"),
        path=path,
        field="scenario.write_probability",
        min_value=0.0,
        max_value=1.0,
    )
    revocation_tick = _require_int(
        scenario.get("revocation_tick"),
        path=path,
        field="scenario.revocation_tick",
        min_value=0,
        allow_none=True,
    )
    if revocation_tick is not None and revocation_tick >= int(simulation["duration_ticks"]):
        raise ScenarioValidationError(
            str(path),
            "'scenario.revocation_tick' must be < simulation.duration_ticks",
        )


def _validate_artifacts(artifacts: Any, path: Path) -> None:
    if not isinstance(artifacts, list) or not artifacts:
        raise ScenarioValidationError(str(path), "'artifacts' must be a non-empty list")
    for idx, artifact in enumerate(artifacts):
        field_prefix = f"artifacts[{idx}]"
        if not isinstance(artifact, dict):
            raise ScenarioValidationError(str(path), f"'{field_prefix}' must be a mapping")
        artifact_id = artifact.get("id")
        if not isinstance(artifact_id, str) or not artifact_id.strip():
            raise ScenarioValidationError(str(path), f"'{field_prefix}.id' must be a non-empty string")
        _require_int(
            artifact.get("size_tokens"),
            path=path,
            field=f"{field_prefix}.size_tokens",
            min_value=1,
        )
        _require_float(
            artifact.get("volatility", 0.0),
            path=path,
            field=f"{field_prefix}.volatility",
            min_value=0.0,
            max_value=1.0,
        )
        _require_int(
            artifact.get("initial_version", 1),
            path=path,
            field=f"{field_prefix}.initial_version",
            min_value=1,
        )
        _require_bool(
            artifact.get("mutable", True),
            path=path,
            field=f"{field_prefix}.mutable",
        )
        depends_on = artifact.get("depends_on", [])
        if not isinstance(depends_on, list) or not all(isinstance(x, str) for x in depends_on):
            raise ScenarioValidationError(str(path), f"'{field_prefix}.depends_on' must be a list of strings")


def _validate_strategies(strategies: dict[str, Any], path: Path) -> None:
    for key in ("eager", "lazy", "lease", "access_count"):
        strategies.setdefault(key, {})
        if not isinstance(strategies[key], dict):
            raise ScenarioValidationError(str(path), f"'strategies.{key}' must be a mapping")

    _require_int(
        strategies["lazy"].get("check_interval_ticks", 10),
        path=path,
        field="strategies.lazy.check_interval_ticks",
        min_value=1,
    )
    _require_int(
        strategies["lease"].get("default_ttl_ticks", 300),
        path=path,
        field="strategies.lease.default_ttl_ticks",
        min_value=1,
    )
    _require_int(
        strategies["access_count"].get("max_accesses", 100),
        path=path,
        field="strategies.access_count.max_accesses",
        min_value=1,
    )


def _validate_transient(transient: dict[str, Any], path: Path) -> None:
    _require_int(
        transient.get("timeout_ticks"),
        path=path,
        field="transient.timeout_ticks",
        min_value=1,
    )


def _validate_crash_recovery(crash_recovery: dict[str, Any], path: Path) -> None:
    """Validate optional ``crash_recovery`` block; absent or empty ⇒ defaults."""
    if "enabled" in crash_recovery:
        _require_bool(crash_recovery["enabled"], path=path, field="crash_recovery.enabled")
    if "heartbeat_timeout_ticks" in crash_recovery:
        _require_int(
            crash_recovery["heartbeat_timeout_ticks"],
            path=path,
            field="crash_recovery.heartbeat_timeout_ticks",
            min_value=1,
        )
    if "max_hold_ticks" in crash_recovery:
        _require_int(
            crash_recovery["max_hold_ticks"],
            path=path,
            field="crash_recovery.max_hold_ticks",
            min_value=1,
        )


def _validate_source_mutation(source_mutation: dict[str, Any], path: Path) -> None:
    """Validate optional ``source_mutation`` block; absent or empty ⇒ defaults.

    ``enabled`` (bool, default False) gates the per-tick source-mutation step.
    ``answer_sensitivity`` (float in [0, 1], default 1.0) is the probability a
    given source mutation is tagged answer-relevant. Mirrors how
    ``crash_recovery`` is validated and the ``artifacts[].volatility`` range.
    """
    if "enabled" in source_mutation:
        _require_bool(source_mutation["enabled"], path=path, field="source_mutation.enabled")
    if "answer_sensitivity" in source_mutation:
        _require_float(
            source_mutation["answer_sensitivity"],
            path=path,
            field="source_mutation.answer_sensitivity",
            min_value=0.0,
            max_value=1.0,
        )


_SUPPORTED_FAILURE_ACTIONS = {"kill", "busy", "restore"}


def _validate_failure_events(failure_events: Any, path: Path) -> None:
    """Validate optional ``failure_events`` top-level list.

    Each entry: ``{tick, action, agent, [until_tick]}``.
      - ``action`` ∈ {kill, busy, restore}
      - ``tick`` integer >= 0
      - ``agent`` non-empty string (cross-checked against agent set at engine init)
      - ``until_tick`` required for ``busy`` and must be > ``tick``;
        forbidden for ``kill`` and ``restore``.
    """
    if failure_events is None:
        return
    if not isinstance(failure_events, list):
        raise ScenarioValidationError(str(path), "'failure_events' must be a list")
    for idx, event in enumerate(failure_events):
        prefix = f"failure_events[{idx}]"
        if not isinstance(event, dict):
            raise ScenarioValidationError(str(path), f"'{prefix}' must be a mapping")
        _require_int(event.get("tick"), path=path, field=f"{prefix}.tick", min_value=0)
        action = event.get("action")
        if action not in _SUPPORTED_FAILURE_ACTIONS:
            raise ScenarioValidationError(
                str(path),
                f"'{prefix}.action' must be one of: {', '.join(sorted(_SUPPORTED_FAILURE_ACTIONS))}",
            )
        agent = event.get("agent")
        if not isinstance(agent, str) or not agent.strip():
            raise ScenarioValidationError(str(path), f"'{prefix}.agent' must be a non-empty string")
        until_tick = event.get("until_tick")
        if action == "busy":
            if until_tick is None:
                raise ScenarioValidationError(
                    str(path), f"'{prefix}.until_tick' is required when action='busy'"
                )
            _require_int(until_tick, path=path, field=f"{prefix}.until_tick", min_value=0)
            if int(until_tick) <= int(event["tick"]):
                raise ScenarioValidationError(
                    str(path), f"'{prefix}.until_tick' must be > {prefix}.tick"
                )
        else:
            if until_tick is not None:
                raise ScenarioValidationError(
                    str(path),
                    f"'{prefix}.until_tick' is only valid when action='busy'",
                )


def _validate_context_semantics(context_semantics: dict[str, Any], path: Path) -> None:
    model = context_semantics.get("model")
    if model not in _SUPPORTED_CONTEXT_MODELS:
        raise ScenarioValidationError(
            str(path),
            f"'context_semantics.model' must be one of: {', '.join(sorted(_SUPPORTED_CONTEXT_MODELS))}",
        )


def _populate_runtime_aliases(data: dict[str, Any]) -> dict[str, Any]:
    """Backfill compatibility aliases for future adapters."""
    simulation = data["simulation"]
    scenario = data["scenario"]
    strategies = data["strategies"]

    simulation["agents"] = simulation["num_agents"]
    if "agent_velocity" in scenario and scenario["agent_velocity"] is not None:
        simulation["actions_per_tick"] = scenario["agent_velocity"]
    if "action_probability" in scenario and scenario["action_probability"] is not None:
        simulation["action_probability"] = scenario["action_probability"]

    strategies["exec_count"] = strategies["access_count"]
    if "max_accesses" in strategies["access_count"]:
        strategies["exec_count"]["max_operations"] = strategies["access_count"]["max_accesses"]

    return data


def validate_scenario(data: dict[str, Any], scenario_path: str | Path) -> dict[str, Any]:
    """Validate and normalize a loaded scenario payload."""
    path = Path(scenario_path)
    if not isinstance(data, dict):
        raise ScenarioValidationError(str(path), "scenario YAML root must be a mapping")

    normalized = _normalize_legacy_keys(data)
    simulation = _require_mapping(normalized, path, "simulation")
    network = _require_mapping(normalized, path, "network")
    scenario = _require_mapping(normalized, path, "scenario")
    strategies = _require_mapping(normalized, path, "strategies")
    transient = _require_mapping(normalized, path, "transient")
    context_semantics = _require_mapping(normalized, path, "context_semantics")
    crash_recovery = _require_mapping(normalized, path, "crash_recovery")
    source_mutation = _require_mapping(normalized, path, "source_mutation")

    _validate_simulation(simulation, path)
    _validate_network(network, path)
    _validate_scenario(scenario, simulation, path)
    _validate_artifacts(normalized.get("artifacts"), path)
    _validate_strategies(strategies, path)
    _validate_transient(transient, path)
    _validate_context_semantics(context_semantics, path)
    _validate_crash_recovery(crash_recovery, path)
    _validate_source_mutation(source_mutation, path)
    _validate_failure_events(normalized.get("failure_events"), path)

    return _populate_runtime_aliases(normalized)


def load_scenario(scenario_path: str) -> dict[str, Any]:
    """Load, validate, and normalize a scenario YAML file."""
    path = Path(scenario_path)
    with path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    return validate_scenario(data, path)

