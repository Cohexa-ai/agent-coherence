# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""Integration tests for simulation engine and comparison runner."""

from __future__ import annotations

from ccs.simulation.engine import SimulationEngine, run_strategy_comparison, run_strategy_range


def _scenario() -> dict:
    return {
        "simulation": {
            "duration_ticks": 20,
            "num_agents": 4,
            "seed": 11,
            "action_probability": 0.6,
            "actions_per_tick": 1,
        },
        "network": {
            "latency_ticks": 0,
            "message_loss_rate": 0.0,
        },
        "scenario": {
            "name": "engine-smoke",
            "workload": "read_heavy",
            "action_probability": 0.6,
            "write_probability": 0.15,
            "agent_velocity": None,
            "revocation_tick": None,
        },
        "artifacts": [
            {"id": "plan.md", "size_tokens": 400, "volatility": 0.2, "initial_version": 1, "mutable": True},
            {"id": "facts.json", "size_tokens": 800, "volatility": 0.1, "initial_version": 1, "mutable": True},
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


def _scenario_with_model(model: str) -> dict:
    scenario = _scenario()
    scenario["context_semantics"]["model"] = model
    return scenario


def test_engine_run_returns_coherence_metrics() -> None:
    metrics = SimulationEngine(_scenario(), strategy_name="lazy", seed=5).run()

    assert metrics.strategy == "lazy"
    assert metrics.duration_ticks == 20
    assert metrics.agent_count == 4
    assert metrics.artifact_count == 2
    assert metrics.total_actions == metrics.read_actions + metrics.write_actions
    assert metrics.synchronization_tokens == (
        metrics.tokens_fetch + metrics.tokens_broadcast + metrics.tokens_invalidation
    )
    assert "sync_broadcast_ratio" in metrics.to_dict()


def test_run_strategy_range_uses_seed_window() -> None:
    runs = run_strategy_range(_scenario(), strategy_name="access_count", runs=3, seed_start=100)
    assert len(runs) == 3
    assert [m.seed for m in runs] == [100, 101, 102]


def test_lazy_avoids_broadcast_tokens_compared_to_eager() -> None:
    lazy = SimulationEngine(_scenario(), strategy_name="lazy", seed=9).run()
    eager = SimulationEngine(_scenario(), strategy_name="eager", seed=9).run()

    assert lazy.tokens_broadcast == 0
    assert eager.tokens_broadcast >= 0
    assert eager.message_overhead >= lazy.message_overhead


def test_strategy_comparison_returns_dashboard_contract() -> None:
    report = run_strategy_comparison(
        _scenario(),
        strategies=["eager", "lazy"],
        runs=2,
        seed_start=40,
    )
    payload = report.to_dict()

    assert payload["scenario"] == "engine-smoke"
    assert payload["runs_per_strategy"] == 2
    assert payload["strategies"] == ["eager", "lazy"]
    assert len(payload["runs"]) == 4
    assert len(payload["aggregated"]) == 2


def test_same_seed_is_deterministic_for_engine_outputs() -> None:
    first = SimulationEngine(_scenario(), strategy_name="lazy", seed=123).run().to_dict()
    second = SimulationEngine(_scenario(), strategy_name="lazy", seed=123).run().to_dict()

    assert first == second


def test_engine_uses_agent_runtime_map_instead_of_legacy_cache_map() -> None:
    engine = SimulationEngine(_scenario(), strategy_name="lazy", seed=5)

    assert hasattr(engine, "_runtime_by_agent")
    assert not hasattr(engine, "_cache_by_agent")


def test_engine_reports_transient_timeouts_when_messages_are_delayed() -> None:
    scenario = _scenario()
    scenario["network"]["latency_ticks"] = 8
    scenario["simulation"]["duration_ticks"] = 12
    scenario["scenario"]["write_probability"] = 1.0
    scenario["scenario"]["action_probability"] = 1.0
    scenario["transient"]["timeout_ticks"] = 1

    metrics = SimulationEngine(scenario, strategy_name="lazy", seed=17).run()
    assert metrics.transient_state_timeouts >= 1


def test_always_read_forces_more_fetches_than_conditional() -> None:
    always = SimulationEngine(_scenario_with_model("always_read"), strategy_name="lazy", seed=22).run()
    conditional = SimulationEngine(_scenario_with_model("conditional_injection"), strategy_name="lazy", seed=22).run()

    assert always.fetch_actions >= conditional.fetch_actions
    assert always.tokens_fetch >= conditional.tokens_fetch


def test_pointer_model_reduces_eager_broadcast_token_cost() -> None:
    pointer = SimulationEngine(_scenario_with_model("pointer"), strategy_name="eager", seed=31).run()
    conditional = SimulationEngine(_scenario_with_model("conditional_injection"), strategy_name="eager", seed=31).run()

    assert pointer.tokens_broadcast <= conditional.tokens_broadcast


# ---- Unit 3: crash_recovery config plumbing ---------------------------------

import pytest  # noqa: E402

from ccs.coordinator.service import CrashRecoveryConfig  # noqa: E402


def test_engine_uses_default_crash_recovery_config_when_block_absent() -> None:
    engine = SimulationEngine(_scenario(), strategy_name="lazy", seed=5)
    assert engine._crash_recovery == CrashRecoveryConfig()
    assert engine._crash_recovery.enabled is False


def test_engine_reads_crash_recovery_block_from_scenario() -> None:
    scenario = _scenario()
    scenario["crash_recovery"] = {
        "enabled": False,
        "heartbeat_timeout_ticks": 7,
        "max_hold_ticks": 77,
    }
    engine = SimulationEngine(scenario, strategy_name="lazy", seed=5)
    assert engine._crash_recovery.heartbeat_timeout_ticks == 7
    assert engine._crash_recovery.max_hold_ticks == 77


def test_engine_fail_fast_when_max_hold_at_lease_ttl_boundary() -> None:
    scenario = _scenario()
    scenario["strategies"]["lease"] = {"default_ttl_ticks": 300}
    scenario["crash_recovery"] = {"enabled": True, "max_hold_ticks": 300}
    with pytest.raises(ValueError, match="max_hold_ticks"):
        SimulationEngine(scenario, strategy_name="lease", seed=5)


def test_engine_fail_fast_accepts_max_hold_above_ttl() -> None:
    scenario = _scenario()
    scenario["strategies"]["lease"] = {"default_ttl_ticks": 200}
    scenario["crash_recovery"] = {"enabled": True, "max_hold_ticks": 300}
    # No raise.
    SimulationEngine(scenario, strategy_name="lease", seed=5)


def test_engine_flag_off_state_log_is_byte_identical_with_or_without_block() -> None:
    """R5: the flag-off path must produce a state-log identical to the v0.5 baseline.

    We compare two runs of the same scenario+strategy+seed:
      - Run A: no ``crash_recovery`` block (legacy v0.5 shape).
      - Run B: explicit ``crash_recovery: { enabled: false, ... }`` block with
        knobs the sweep would otherwise read.

    Today this is preventive (Unit 4 will add the sweep call site). The
    contract: when ``enabled=False`` the engine MUST never invoke the sweep
    or emit heartbeats, so the state-log is identical to a run with the
    block omitted entirely.
    """

    def _run_and_capture(crash_recovery_block: dict | None) -> list[dict]:
        scenario = _scenario()
        if crash_recovery_block is not None:
            scenario["crash_recovery"] = crash_recovery_block
        engine = SimulationEngine(scenario, strategy_name="lazy", seed=42)
        # Wire a state_log onto the registry post-construction. Construction
        # already populated _instance_id, so it's safe to attach the callback.
        log: list[dict] = []
        engine._registry._state_log = log.append
        engine.run()
        # Normalize per-run uuid4 fields (instance_id, artifact_id) to
        # deterministic placeholders. artifact_id is fresh on every Artifact
        # construction; first-seen order maps to a stable placeholder.
        artifact_id_map: dict[str, str] = {}
        for entry in log:
            entry["instance_id"] = "<inst>"
            aid = entry.get("artifact_id")
            if aid is not None:
                aid_str = str(aid)
                placeholder = artifact_id_map.setdefault(
                    aid_str, f"<artifact-{len(artifact_id_map)}>"
                )
                entry["artifact_id"] = placeholder
        return log

    log_without_block = _run_and_capture(None)
    log_with_disabled_block = _run_and_capture(
        {"enabled": False, "heartbeat_timeout_ticks": 5, "max_hold_ticks": 50}
    )

    assert log_without_block == log_with_disabled_block
    assert len(log_without_block) > 0  # sanity: we actually captured something
