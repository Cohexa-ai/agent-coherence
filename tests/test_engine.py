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
    """v0.9.0: omitting the YAML block inherits the flipped library default
    (enabled=True with retuned 120/900), and engine construction surfaces no
    DeprecationWarning (the v0.8.3 warning is removed).
    """
    import warnings as _w
    with _w.catch_warnings(record=True) as caught:
        _w.simplefilter("always")
        engine = SimulationEngine(_scenario(), strategy_name="lazy", seed=5)
    deprecation_warnings = [
        w for w in caught if issubclass(w.category, DeprecationWarning)
    ]
    assert deprecation_warnings == [], (
        f"engine construction must not emit DeprecationWarning, got: "
        f"{[str(w.message) for w in deprecation_warnings]}"
    )
    # The engine inherits the flipped default; compare against explicit-True.
    assert engine._crash_recovery == CrashRecoveryConfig(enabled=True)
    assert engine._crash_recovery.enabled is True
    assert engine._crash_recovery.heartbeat_timeout_ticks == 120
    assert engine._crash_recovery.max_hold_ticks == 900


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


def _capture_normalized_state_log(
    crash_recovery_block: dict | None, *, seed: int = 42
) -> list[dict]:
    """Run ``_scenario()`` under the given crash_recovery block and return the
    state-log with per-run uuid fields (instance_id, artifact_id) normalized to
    deterministic placeholders. Shared by the R5 byte-identity tests below."""
    scenario = _scenario()
    if crash_recovery_block is not None:
        scenario["crash_recovery"] = crash_recovery_block
    engine = SimulationEngine(scenario, strategy_name="lazy", seed=seed)
    log: list[dict] = []
    engine._registry._state_log = log.append
    engine.run()
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


def test_engine_flag_off_state_log_is_byte_identical_across_explicit_false_variants() -> None:
    """R5 (off-path): two explicit ``{enabled: false}`` runs that differ only in
    the (ignored) sweep knobs produce byte-identical state-logs.

    Post-v0.9.0 the omitted-block default is ENABLED, so the off-path
    byte-identity contract is asserted between two EXPLICIT-false variants:
    when ``enabled=False`` the engine MUST never invoke the sweep or emit
    heartbeats, so the knobs are inert and the logs are identical.
    """
    log_a = _capture_normalized_state_log({"enabled": False})
    log_b = _capture_normalized_state_log(
        {"enabled": False, "heartbeat_timeout_ticks": 5, "max_hold_ticks": 50}
    )
    assert log_a == log_b
    assert len(log_a) > 0  # sanity: we actually captured something


def test_engine_flag_on_state_log_is_byte_identical_with_yaml_absent_or_explicit_true() -> None:
    """R5 (on-path — the inverted contract): an OMITTED crash_recovery block now
    behaves identically to an explicit ``{enabled: true}`` block. The engine
    inherits the flipped default, so both runs emit the same heartbeats and the
    state-logs are byte-identical. This is the v0.9.0 R5 preservation proof.
    """
    log_absent = _capture_normalized_state_log(None)
    log_explicit_true = _capture_normalized_state_log({"enabled": True})
    assert log_absent == log_explicit_true
    assert len(log_absent) > 0


def test_engine_flag_on_diverges_from_explicit_false_via_reclaim() -> None:
    """R5 (divergence is real, not trivial — ADV-07): an omitted crash_recovery
    block (enabled-by-default) DIVERGES from explicit ``{enabled: false}``, and
    the divergence is attributable to a ``reclaim_*`` event in the enabled-side
    log — proving the sweep mechanism actually ran, not merely that heartbeat
    entries appeared.

    Fixture preconditions (ADV-07): the workload drives > heartbeat_timeout_ticks
    (=120, the v0.9.0 default) so a killed grant-holder's heartbeat gaps past the
    timeout and the sweep reclaims it. No agent is pre-held past max_hold_ticks
    (=900), so the reclaim is heartbeat-attributable, not max-hold.
    """

    def _run(crash_recovery_block: dict | None) -> list[dict]:
        scenario = _failure_scenario(num_agents=2, duration=150)
        scenario["failure_events"] = [
            {"tick": 5, "action": "kill", "agent": "agent_0"},
        ]
        if crash_recovery_block is not None:
            scenario["crash_recovery"] = crash_recovery_block
        engine = SimulationEngine(scenario, strategy_name="lazy", seed=1)
        log: list[dict] = []
        engine._registry._state_log = log.append
        # Pre-grant EXCLUSIVE to the soon-killed agent so the sweep has a stable
        # grant to reclaim once its heartbeat gaps past the timeout.
        artifact_id = engine._artifact_ids[0]
        agent_a = engine._agent_id_by_name["agent_0"]
        engine._coordinator.fetch(
            FetchRequest(
                artifact_id=artifact_id,
                requesting_agent_id=agent_a,
                requested_at_tick=0,
            )
        )
        engine.run()
        return log

    log_enabled = _run(None)  # omitted block -> enabled-by-default
    log_disabled = _run({"enabled": False})  # explicit opt-out

    assert log_enabled != log_disabled, (
        "enabled-by-default must diverge from explicit-false"
    )
    enabled_reclaims = [
        e for e in log_enabled if str(e.get("trigger", "")).startswith("reclaim_")
    ]
    assert len(enabled_reclaims) >= 1, (
        "divergence must be attributable to a reclaim_* event in the enabled log "
        "(fixture must drive > heartbeat_timeout_ticks with a killed grant-holder)"
    )
    disabled_reclaims = [
        e for e in log_disabled if str(e.get("trigger", "")).startswith("reclaim_")
    ]
    assert disabled_reclaims == []


# ---- Unit 4: failure-event injection + sweep wiring -------------------------

from ccs.core.states import MESIState  # noqa: E402
from ccs.core.types import FetchRequest  # noqa: E402


def _failure_scenario(*, num_agents: int = 2, duration: int = 50) -> dict:
    """Minimal scenario for failure-event integration tests.

    Single small artifact, deterministic action knobs, no network latency.
    Action knobs are very low — tests drive state via direct registry/coordinator
    calls when they need precise control over MESI lifecycle.
    """
    return {
        "simulation": {
            "duration_ticks": duration,
            "num_agents": num_agents,
            "seed": 7,
            "action_probability": 0.0,
            "actions_per_tick": 1,
        },
        "network": {"latency_ticks": 0, "message_loss_rate": 0.0},
        "scenario": {
            "name": "failure-injection",
            "workload": "read_heavy",
            "action_probability": 0.0,
            "write_probability": 0.0,
            "agent_velocity": None,
            "revocation_tick": None,
        },
        "artifacts": [
            {"id": "plan.md", "size_tokens": 100, "volatility": 0.0, "initial_version": 1, "mutable": True},
        ],
        "strategies": {
            "eager": {},
            "lazy": {"check_interval_ticks": 100},
            "lease": {"default_ttl_ticks": 5},
            "access_count": {"max_accesses": 4},
            "exec_count": {"max_operations": 4},
        },
        "transient": {"timeout_ticks": 100},
        "context_semantics": {"model": "conditional_injection"},
    }


def _capture_state_log(engine: SimulationEngine) -> list[dict]:
    log: list[dict] = []
    engine._registry._state_log = log.append
    return log


def test_failure_event_kill_triggers_heartbeat_reclaim() -> None:
    """R1 / OOM-kill shape: A acquires E, is killed, sweep reclaims, B can write.

    A is granted EXCLUSIVE on artifact_0 at tick 0. A `kill` event fires at
    tick 5. After heartbeat_timeout_ticks elapse with no heartbeat, the
    sweep reclaims A's grant via ``reclaim_heartbeat``. Agent B's subsequent
    write succeeds.
    """
    scenario = _failure_scenario(num_agents=2, duration=30)
    scenario["crash_recovery"] = {
        "enabled": True,
        "heartbeat_timeout_ticks": 5,
        "max_hold_ticks": 1000,
    }
    scenario["failure_events"] = [
        {"tick": 5, "action": "kill", "agent": "agent_0"},
    ]
    engine = SimulationEngine(scenario, strategy_name="lazy", seed=1)
    log = _capture_state_log(engine)

    artifact_id = engine._artifact_ids[0]
    agent_a = engine._agent_id_by_name["agent_0"]
    agent_b = engine._agent_id_by_name["agent_1"]

    # Pre-grant EXCLUSIVE to A at tick 0.
    engine._coordinator.fetch(
        FetchRequest(artifact_id=artifact_id, requesting_agent_id=agent_a, requested_at_tick=0)
    )
    assert engine._registry.get_agent_state(artifact_id, agent_a) == MESIState.EXCLUSIVE

    engine.run()

    assert engine._stable_grant_reclamations >= 1
    reclaim_entries = [e for e in log if e.get("trigger") == "reclaim_heartbeat"]
    assert len(reclaim_entries) >= 1
    # B can now write because A's grant was reclaimed.
    engine._coordinator.write(agent_id=agent_b, artifact_id=artifact_id, issued_at_tick=29)
    assert engine._registry.get_agent_state(artifact_id, agent_b) == MESIState.EXCLUSIVE


def test_failure_event_alive_agent_max_hold_reclaim() -> None:
    """R2 / max-hold-alive shape: live agent never commits; max_hold reclaims.

    A acquires MODIFIED at tick 10, then continues to be heartbeated by the
    engine (it stays in ``_alive_agents``). After ``max_hold_ticks`` ticks
    pass with no commit, the sweep reclaims via ``reclaim_max_hold``.
    """
    scenario = _failure_scenario(num_agents=1, duration=60)
    scenario["crash_recovery"] = {
        "enabled": True,
        "heartbeat_timeout_ticks": 1000,  # heartbeat trigger never fires
        "max_hold_ticks": 30,
    }
    engine = SimulationEngine(scenario, strategy_name="lazy", seed=1)
    log = _capture_state_log(engine)

    artifact_id = engine._artifact_ids[0]
    agent_a = engine._agent_id_by_name["agent_0"]

    engine._coordinator.fetch(
        FetchRequest(artifact_id=artifact_id, requesting_agent_id=agent_a, requested_at_tick=0)
    )
    engine._coordinator.commit(
        agent_id=agent_a, artifact_id=artifact_id, content="v2", issued_at_tick=10
    )
    assert engine._registry.get_agent_state(artifact_id, agent_a) == MESIState.MODIFIED

    engine.run()

    reclaim_entries = [e for e in log if e.get("trigger") == "reclaim_max_hold"]
    assert len(reclaim_entries) >= 1
    assert engine._stable_grant_reclamations >= 1


def test_busy_agent_during_sync_compute_is_falsely_reclaimed() -> None:
    """Documents the false-reclaim-under-sync-compute shape (Carryover risk #3).

    Agent A acquires EXCLUSIVE; at tick 5, a ``busy`` event fires with
    ``until_tick = 5 + heartbeat_timeout_ticks + 1``. Inside the busy window
    A does NOT heartbeat and does NOT act. The sweep is expected to reclaim
    A via ``reclaim_heartbeat`` because the engine cannot distinguish
    "blocked on long sync compute" from "crashed". This test passes by
    DEMONSTRATING the failure mode, not by avoiding it.
    """
    heartbeat_timeout = 4
    busy_start = 5
    busy_until = busy_start + heartbeat_timeout + 2
    scenario = _failure_scenario(num_agents=1, duration=busy_until + 5)
    scenario["crash_recovery"] = {
        "enabled": True,
        "heartbeat_timeout_ticks": heartbeat_timeout,
        "max_hold_ticks": 1000,
    }
    scenario["failure_events"] = [
        {"tick": busy_start, "action": "busy", "agent": "agent_0", "until_tick": busy_until},
    ]
    engine = SimulationEngine(scenario, strategy_name="lazy", seed=1)
    log = _capture_state_log(engine)

    artifact_id = engine._artifact_ids[0]
    agent_a = engine._agent_id_by_name["agent_0"]

    engine._coordinator.fetch(
        FetchRequest(artifact_id=artifact_id, requesting_agent_id=agent_a, requested_at_tick=0)
    )

    engine.run()

    reclaim_entries = [e for e in log if e.get("trigger") == "reclaim_heartbeat"]
    assert len(reclaim_entries) >= 1, (
        "expected the sync-compute busy window to trigger a false-positive heartbeat reclaim"
    )
    assert engine._stable_grant_reclamations >= 1


def test_killed_agent_does_not_act() -> None:
    """A killed agent must never reach _perform_read or _perform_write."""
    scenario = _failure_scenario(num_agents=2, duration=30)
    # Force every alive agent to act every tick.
    scenario["scenario"]["action_probability"] = 1.0
    scenario["scenario"]["write_probability"] = 0.5
    scenario["failure_events"] = [
        {"tick": 5, "action": "kill", "agent": "agent_0"},
    ]

    engine = SimulationEngine(scenario, strategy_name="lazy", seed=3)
    agent_a = engine._agent_id_by_name["agent_0"]
    agent_b = engine._agent_id_by_name["agent_1"]

    seen_agents: list = []
    original = engine._execute_single_action

    def _spy(*, agent_id, now_tick):
        if now_tick >= 5:
            seen_agents.append((now_tick, agent_id))
        return original(agent_id=agent_id, now_tick=now_tick)

    engine._execute_single_action = _spy  # type: ignore[assignment]
    engine.run()

    # After tick 5 (kill takes effect at start of tick 5), A must never act.
    post_kill_a = [t for t, aid in seen_agents if aid == agent_a]
    post_kill_b = [t for t, aid in seen_agents if aid == agent_b]
    assert post_kill_a == [], f"killed agent acted at ticks {post_kill_a}"
    assert post_kill_b, "control: live agent should still be acting"


def test_metrics_include_stable_grant_reclamations_field() -> None:
    """R5: existing baseline scenarios get the new field defaulted to 0."""
    metrics = SimulationEngine(_scenario(), strategy_name="lazy", seed=5).run()
    payload = metrics.to_dict()
    assert "stable_grant_reclamations" in payload
    assert payload["stable_grant_reclamations"] == 0


def test_baseline_state_log_byte_identical_with_disabled_flag_after_unit4() -> None:
    """R5: with crash_recovery disabled, state-log is identical to v0.5 baseline.

    Identical to the Unit 3 byte-identity test but exercises the Unit 4
    code paths now that the sweep call site exists. The flag-off guard at
    the call site MUST keep the heartbeat / sweep entries out of the log.
    """

    # Post-v0.9.0 the omitted-block default is enabled, so both arms are
    # explicit-false to assert the off-path byte-identity (the Unit 4 sweep
    # call site must stay inert under enabled=False). Reuses the shared
    # _capture_normalized_state_log helper rather than a local duplicate.
    log_explicit_false = _capture_normalized_state_log({"enabled": False})
    log_with_disabled_block = _capture_normalized_state_log(
        {"enabled": False, "heartbeat_timeout_ticks": 5, "max_hold_ticks": 50}
    )
    assert log_explicit_false == log_with_disabled_block
    assert len(log_explicit_false) > 0


def test_failure_event_unknown_agent_name_raises() -> None:
    scenario = _failure_scenario(num_agents=1, duration=10)
    scenario["failure_events"] = [
        {"tick": 1, "action": "kill", "agent": "agent_999"},
    ]
    with pytest.raises(ValueError, match="unknown agent name"):
        SimulationEngine(scenario, strategy_name="lazy", seed=1)


def test_busy_window_auto_expires_without_restore_event() -> None:
    """`busy` with `until_tick` flips back to alive automatically."""
    scenario = _failure_scenario(num_agents=1, duration=20)
    scenario["scenario"]["action_probability"] = 1.0
    scenario["failure_events"] = [
        {"tick": 2, "action": "busy", "agent": "agent_0", "until_tick": 6},
    ]

    engine = SimulationEngine(scenario, strategy_name="lazy", seed=1)
    agent_a = engine._agent_id_by_name["agent_0"]

    seen: list[int] = []
    original = engine._execute_single_action

    def _spy(*, agent_id, now_tick):
        if agent_id == agent_a:
            seen.append(now_tick)
        return original(agent_id=agent_id, now_tick=now_tick)

    engine._execute_single_action = _spy  # type: ignore[assignment]
    engine.run()

    # During [2, 6) A is busy; from tick 6 onward A is alive again.
    assert all(t < 2 or t >= 6 for t in seen), seen
    assert any(t >= 6 for t in seen), "agent should resume acting after busy window"


def test_busy_then_kill_does_not_resurrect_at_busy_until_tick() -> None:
    """Review ADV-01: busy at tick T0 with until_tick=U, then kill at T1 < U.

    The busy auto-expiry at U must NOT silently resurrect the killed agent.
    Without the fix, agent_0 would re-enter `_alive_agents` at tick U.
    """
    busy_until = 12
    scenario = _failure_scenario(num_agents=1, duration=busy_until + 5)
    scenario["failure_events"] = [
        {"tick": 2, "action": "busy", "agent": "agent_0", "until_tick": busy_until},
        {"tick": 5, "action": "kill", "agent": "agent_0"},
    ]

    engine = SimulationEngine(scenario, strategy_name="lazy", seed=1)
    agent_a = engine._agent_id_by_name["agent_0"]
    engine.run()

    # After the run completes, the killed agent must remain killed and never
    # be in _alive_agents — the busy auto-expiry at tick=busy_until must not
    # have resurrected it.
    assert agent_a in engine._killed_agents
    assert agent_a not in engine._alive_agents
    assert agent_a not in engine._busy_agents
    assert agent_a not in engine._busy_until


def test_kill_then_restore_brings_agent_back_alive() -> None:
    """A `restore` event after `kill` must clear the killed state."""
    scenario = _failure_scenario(num_agents=1, duration=20)
    scenario["failure_events"] = [
        {"tick": 3, "action": "kill", "agent": "agent_0"},
        {"tick": 8, "action": "restore", "agent": "agent_0"},
    ]

    engine = SimulationEngine(scenario, strategy_name="lazy", seed=1)
    agent_a = engine._agent_id_by_name["agent_0"]
    engine.run()

    assert agent_a not in engine._killed_agents
    assert agent_a in engine._alive_agents


def test_busy_on_killed_agent_raises() -> None:
    """Review ADV-01: scheduling busy on a killed agent must fail loudly.

    Strict semantics — silent no-op would mask operator misconfiguration.
    """
    scenario = _failure_scenario(num_agents=1, duration=20)
    scenario["failure_events"] = [
        {"tick": 3, "action": "kill", "agent": "agent_0"},
        {"tick": 6, "action": "busy", "agent": "agent_0", "until_tick": 12},
    ]

    engine = SimulationEngine(scenario, strategy_name="lazy", seed=1)
    with pytest.raises(ValueError, match="busy.*killed agent"):
        engine.run()


def test_kill_then_busy_then_restore_round_trip() -> None:
    """kill → restore → busy is the supported way to reuse a killed agent."""
    scenario = _failure_scenario(num_agents=1, duration=20)
    scenario["failure_events"] = [
        {"tick": 3, "action": "kill", "agent": "agent_0"},
        {"tick": 6, "action": "restore", "agent": "agent_0"},
        {"tick": 9, "action": "busy", "agent": "agent_0", "until_tick": 14},
    ]

    engine = SimulationEngine(scenario, strategy_name="lazy", seed=1)
    agent_a = engine._agent_id_by_name["agent_0"]
    engine.run()

    # Final state: alive again after the busy window auto-expires.
    assert agent_a in engine._alive_agents
    assert agent_a not in engine._killed_agents
    assert agent_a not in engine._busy_agents


# Review fix T-05: restore-after-kill is asserted to clear killed state but
# was never directly verified to re-enable heartbeat emission. Test below
# confirms that after restore, the engine emits heartbeats for the agent.


def test_restore_after_kill_resumes_heartbeat_emission() -> None:
    """T-05: a `restore` event must re-enable heartbeat emission for the agent.

    Before restore, _emit_heartbeats_for_alive_agents skips the killed agent
    (it's not in _alive_agents). After restore, the agent is in _alive_agents
    again and the next tick's emission updates last_heartbeat_tick.
    """
    scenario = _failure_scenario(num_agents=1, duration=20)
    scenario["crash_recovery"] = {
        "enabled": True,
        "heartbeat_timeout_ticks": 50,  # generous so no reclaim disturbs the test
        "max_hold_ticks": 1000,
    }
    scenario["failure_events"] = [
        {"tick": 3, "action": "kill", "agent": "agent_0"},
        {"tick": 12, "action": "restore", "agent": "agent_0"},
    ]

    engine = SimulationEngine(scenario, strategy_name="lazy", seed=1)
    agent_a = engine._agent_id_by_name["agent_0"]
    engine.run()

    # last_heartbeat_tick must reflect a tick AFTER the restore (12), proving
    # that emission resumed. Run completes at duration_ticks=20, so the last
    # heartbeat should be at tick 19 (last tick before clock.advance to 20).
    last_hb = engine._registry.last_heartbeat_tick(agent_a)
    assert last_hb is not None
    assert last_hb >= 12, f"heartbeat should have resumed after restore, got {last_hb}"
    assert last_hb <= 19, f"heartbeat must not exceed final tick, got {last_hb}"
