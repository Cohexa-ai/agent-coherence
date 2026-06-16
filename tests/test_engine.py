# Copyright (c) 2026 agent-coherence contributors.
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

from uuid import UUID  # noqa: E402

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


# ---- Unit 2: source-mutation step (the change-rate dial) --------------------


def _mutation_scenario(
    *,
    enabled: bool,
    volatility: float = 1.0,
    answer_sensitivity: float = 1.0,
    num_agents: int = 2,
    duration: int = 10,
    mutable: bool = True,
) -> dict:
    """Scenario whose single artifact's mutation behavior is fully controlled.

    Action knobs are zero so agents take no actions of their own; the only
    version churn comes from the source-mutation step. The `source_mutation`
    block is included only when a non-default shape is requested by the caller.
    """
    scenario = {
        "simulation": {
            "duration_ticks": duration,
            "num_agents": num_agents,
            "seed": 7,
            "action_probability": 0.0,
            "actions_per_tick": 1,
        },
        "network": {"latency_ticks": 0, "message_loss_rate": 0.0},
        "scenario": {
            "name": "source-mutation",
            "workload": "read_heavy",
            "action_probability": 0.0,
            "write_probability": 0.0,
            "agent_velocity": None,
            "revocation_tick": None,
        },
        "artifacts": [
            {
                "id": "plan.md",
                "size_tokens": 100,
                "volatility": volatility,
                "initial_version": 1,
                "mutable": mutable,
            },
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
        "source_mutation": {"enabled": enabled, "answer_sensitivity": answer_sensitivity},
    }
    return scenario


def test_source_mutation_disabled_is_default_when_block_absent() -> None:
    """No ``source_mutation`` block ⇒ engine treats it as enabled=False."""
    engine = SimulationEngine(_scenario(), strategy_name="lazy", seed=5)
    assert engine._source_mutation_enabled is False


def test_source_mutation_explicit_disabled_flag() -> None:
    scenario = _mutation_scenario(enabled=False, volatility=1.0)
    engine = SimulationEngine(scenario, strategy_name="lazy", seed=5)
    assert engine._source_mutation_enabled is False


def test_source_mutation_full_volatility_advances_version_every_tick() -> None:
    """enabled + volatility=1.0 ⇒ canonical version advances on every tick."""
    duration = 8
    scenario = _mutation_scenario(enabled=True, volatility=1.0, duration=duration)
    engine = SimulationEngine(scenario, strategy_name="lazy", seed=5)
    artifact_id = engine._artifact_ids[0]
    start_version = engine._registry.get_artifact(artifact_id).version

    engine.run()

    end_version = engine._registry.get_artifact(artifact_id).version
    # One mutation per tick, no agent writes (action knobs are zero).
    assert end_version == start_version + duration


def test_source_mutation_invalidates_holders_to_invalid() -> None:
    """A mutation must flip a non-INVALID holder to INVALID in registry + cache."""
    scenario = _mutation_scenario(enabled=True, volatility=1.0, num_agents=2, duration=5)
    engine = SimulationEngine(scenario, strategy_name="lazy", seed=5)
    artifact_id = engine._artifact_ids[0]
    holder = engine._agent_ids[0]

    # Seed a SHARED holder directly: registry state + a live cache entry.
    engine._registry.set_agent_state(
        artifact_id, holder, MESIState.SHARED, trigger="seed", tick=0
    )
    runtime = engine._runtime_by_agent[holder]
    from ccs.core.types import ArtifactCacheEntry  # noqa: PLC0415

    runtime.cache.put(
        artifact_id,
        ArtifactCacheEntry(
            artifact_id=artifact_id,
            state=MESIState.SHARED,
            local_version=1,
            acquired_at_tick=0,
        ),
    )

    engine._apply_source_mutations_for_tick(0)

    assert engine._registry.get_agent_state(artifact_id, holder) == MESIState.INVALID
    cache_entry = runtime.cache.get(artifact_id)
    assert cache_entry is not None
    assert cache_entry.state == MESIState.INVALID


def test_source_mutation_state_log_uses_source_mutation_trigger() -> None:
    """The invalidation state-log entry must be labelled ``source_mutation``."""
    scenario = _mutation_scenario(enabled=True, volatility=1.0, num_agents=2, duration=5)
    engine = SimulationEngine(scenario, strategy_name="lazy", seed=5)
    artifact_id = engine._artifact_ids[0]
    holder = engine._agent_ids[0]
    engine._registry.set_agent_state(
        artifact_id, holder, MESIState.SHARED, trigger="seed", tick=0
    )

    log = _capture_state_log(engine)
    engine._apply_source_mutations_for_tick(1)

    triggers = [entry["trigger"] for entry in log]
    assert "source_mutation" in triggers
    assert "invalidate" not in triggers


def test_source_mutation_zero_volatility_never_mutates() -> None:
    """volatility=0.0 ⇒ no mutations even with enabled=True."""
    duration = 10
    scenario = _mutation_scenario(enabled=True, volatility=0.0, duration=duration)
    engine = SimulationEngine(scenario, strategy_name="lazy", seed=5)
    artifact_id = engine._artifact_ids[0]
    start_version = engine._registry.get_artifact(artifact_id).version

    engine.run()

    assert engine._registry.get_artifact(artifact_id).version == start_version


def test_source_mutation_immutable_artifact_never_mutates() -> None:
    """A mutable=False artifact is never source-mutated regardless of volatility."""
    scenario = _mutation_scenario(
        enabled=True, volatility=1.0, duration=6, mutable=False
    )
    engine = SimulationEngine(scenario, strategy_name="lazy", seed=5)
    artifact_id = engine._artifact_ids[0]
    start_version = engine._registry.get_artifact(artifact_id).version

    engine.run()

    assert engine._registry.get_artifact(artifact_id).version == start_version


def test_source_mutation_full_sensitivity_tags_all_relevant() -> None:
    """answer_sensitivity=1.0 ⇒ every mutation is tagged relevant=True."""
    scenario = _refetch_scenario(volatility=1.0, answer_sensitivity=1.0)
    engine = SimulationEngine(scenario, strategy_name="lazy", seed=5)

    seen_relevances: list[bool] = []
    original = engine._apply_source_mutations_for_tick

    def _spy(now: int) -> None:
        original(now)
        seen_relevances.extend(engine._pending_source_mutations.values())

    engine._apply_source_mutations_for_tick = _spy  # type: ignore[assignment]
    engine.run()

    assert seen_relevances, "expected at least one mutation"
    assert all(seen_relevances)


def test_source_mutation_zero_sensitivity_tags_all_irrelevant() -> None:
    """answer_sensitivity=0.0 ⇒ every mutation is tagged relevant=False."""
    scenario = _refetch_scenario(volatility=1.0, answer_sensitivity=0.0)
    engine = SimulationEngine(scenario, strategy_name="lazy", seed=5)

    seen_relevances: list[bool] = []
    original = engine._apply_source_mutations_for_tick

    def _spy(now: int) -> None:
        original(now)
        seen_relevances.extend(engine._pending_source_mutations.values())

    engine._apply_source_mutations_for_tick = _spy  # type: ignore[assignment]
    engine.run()

    assert seen_relevances, "expected at least one mutation"
    assert not any(seen_relevances)


def test_source_refetch_attribution_survives_unread_ticks() -> None:
    """A holder invalidated on an earlier tick is still credited when it finally
    re-reads (action_probability < 1).

    The per-(artifact, holder) marker persists across ticks rather than resetting,
    so EVERY re-fetch of a source-invalidated entry is counted exactly once --
    including the ones that lag the invalidating tick (the regime a per-tick reset
    would mis-count). With no agent writes, INVALID can only come from the source,
    so ``source_refetches`` must equal the number of reads that found the entry
    INVALID.
    """
    scenario = _refetch_scenario(volatility=0.5, answer_sensitivity=0.5, duration=60)
    # Read sparsely so entries stay INVALID across several mutation ticks before a
    # holder re-reads.
    scenario["scenario"]["action_probability"] = 0.4
    scenario["simulation"]["action_probability"] = 0.4
    engine = SimulationEngine(scenario, strategy_name="lazy", seed=7)

    invalid_entry_reads = 0
    original = engine._perform_read

    def _spy(*, agent_id: UUID, artifact_id: UUID, now_tick: int) -> None:
        nonlocal invalid_entry_reads
        entry = engine._runtime_by_agent[agent_id].cache.get(artifact_id)
        if entry is not None and entry.state == MESIState.INVALID:
            invalid_entry_reads += 1
        original(agent_id=agent_id, artifact_id=artifact_id, now_tick=now_tick)

    engine._perform_read = _spy  # type: ignore[assignment]
    metrics = engine.run()

    assert invalid_entry_reads > 0
    assert metrics.source_refetches == invalid_entry_reads
    assert metrics.wasted_refetches <= metrics.source_refetches


def test_always_read_does_not_overattribute_source_refetches() -> None:
    """Under context_model=always_read every read forces a fetch, but only the
    fetches of source-INVALIDATED entries are credited to the source -- forced
    re-reads of still-valid entries (and initial fills) are not. So
    source_refetches stays strictly below the total fetch count. (lease-expiry
    refreshes are excluded by the same entry.state == INVALID gate.)
    """
    scenario = _refetch_scenario(volatility=0.5, answer_sensitivity=0.5, duration=40)
    scenario["context_semantics"] = {"model": "always_read"}
    metrics = SimulationEngine(scenario, strategy_name="lazy", seed=5).run()

    assert metrics.fetch_actions > 0
    assert 0 < metrics.source_refetches < metrics.fetch_actions


def test_source_mutation_disabled_run_is_byte_identical_to_no_block() -> None:
    """Flag-off guarantee (TEST-FIRST): an ``enabled=False`` run produces a
    state-log byte-identical to the same scenario with no ``source_mutation``
    block at all.

    Mirrors ``test_engine_flag_off_state_log_is_byte_identical_with_or_without_block``:
    when the feature is disabled the engine MUST never bump a version or emit a
    ``source_mutation`` state entry, so the two logs are indistinguishable.
    """

    def _run_and_capture(source_mutation_block: dict | None) -> list[dict]:
        scenario = _scenario()
        if source_mutation_block is not None:
            scenario["source_mutation"] = source_mutation_block
        engine = SimulationEngine(scenario, strategy_name="lazy", seed=42)
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

    log_without_block = _run_and_capture(None)
    log_with_disabled_block = _run_and_capture(
        {"enabled": False, "answer_sensitivity": 1.0}
    )

    assert log_without_block == log_with_disabled_block
    assert len(log_without_block) > 0


def test_source_mutation_enabled_same_seed_runs_are_byte_identical() -> None:
    """Determinism: two ``enabled=True`` runs at the same seed must produce
    identical ``to_dict()`` payloads, and the dedicated mutation RNG must not
    have perturbed the action-selection stream non-deterministically.
    """
    scenario = _mutation_scenario(
        enabled=True, volatility=0.5, answer_sensitivity=0.5, num_agents=4, duration=20
    )
    # Give agents some action probability so we also prove the mutation RNG
    # does not bleed into the action-selection stream across the two runs.
    scenario["scenario"]["action_probability"] = 0.6
    scenario["simulation"]["action_probability"] = 0.6

    first = SimulationEngine(scenario, strategy_name="lazy", seed=123).run().to_dict()
    second = SimulationEngine(scenario, strategy_name="lazy", seed=123).run().to_dict()

    assert first == second


def test_source_mutation_does_not_shift_action_stream_when_off() -> None:
    """A disabled source_mutation block must leave metrics byte-identical to a
    run with no block — proving the separate RNG seeding never touches the
    action-selection path when the feature is off.
    """
    scenario_off = _mutation_scenario(enabled=False, volatility=1.0, num_agents=4, duration=20)
    scenario_off["scenario"]["action_probability"] = 0.6
    scenario_off["simulation"]["action_probability"] = 0.6

    scenario_none = _mutation_scenario(enabled=False, volatility=1.0, num_agents=4, duration=20)
    scenario_none["scenario"]["action_probability"] = 0.6
    scenario_none["simulation"]["action_probability"] = 0.6
    del scenario_none["source_mutation"]

    with_block = SimulationEngine(scenario_off, strategy_name="lazy", seed=77).run().to_dict()
    without_block = SimulationEngine(scenario_none, strategy_name="lazy", seed=77).run().to_dict()

    assert with_block == without_block


# ---- Unit 3: source-triggered & wasted re-fetch cost metrics ----------------


def _refetch_scenario(
    *,
    volatility: float = 0.5,
    answer_sensitivity: float = 0.5,
    num_agents: int = 4,
    duration: int = 40,
) -> dict:
    """Source-mutation scenario with read-only agents that DO act.

    Mutations invalidate holders; on their next read those holders re-fetch,
    which is what Unit 3 attributes. Agents read every tick (action_probability
    1.0, write_probability 0.0) so re-fetches actually occur. ``lazy`` is the
    realistic strategy — it re-fetches exactly on INVALID, the state a source
    mutation leaves a holder in.
    """
    scenario = _mutation_scenario(
        enabled=True,
        volatility=volatility,
        answer_sensitivity=answer_sensitivity,
        num_agents=num_agents,
        duration=duration,
    )
    scenario["scenario"]["action_probability"] = 1.0
    scenario["simulation"]["action_probability"] = 1.0
    scenario["scenario"]["write_probability"] = 0.0
    return scenario


def test_source_refetches_and_wasted_are_counted_for_lazy() -> None:
    """Happy path: lazy + mid volatility/sensitivity ⇒ some source re-fetches,
    a strict-positive wasted subset bounded by the total.
    """
    metrics = SimulationEngine(
        _refetch_scenario(volatility=0.5, answer_sensitivity=0.5),
        strategy_name="lazy",
        seed=5,
    ).run()

    assert metrics.source_refetches > 0
    assert 0 < metrics.wasted_refetches <= metrics.source_refetches


def test_full_sensitivity_yields_zero_wasted_refetches() -> None:
    """answer_sensitivity=1.0 ⇒ every mutation is relevant ⇒ nothing wasted."""
    metrics = SimulationEngine(
        _refetch_scenario(volatility=1.0, answer_sensitivity=1.0),
        strategy_name="lazy",
        seed=5,
    ).run()

    assert metrics.source_refetches > 0
    assert metrics.wasted_refetches == 0


def test_zero_sensitivity_makes_every_refetch_wasted() -> None:
    """answer_sensitivity=0.0 ⇒ every mutation is irrelevant ⇒ all re-fetches wasted."""
    metrics = SimulationEngine(
        _refetch_scenario(volatility=1.0, answer_sensitivity=0.0),
        strategy_name="lazy",
        seed=5,
    ).run()

    assert metrics.source_refetches > 0
    assert metrics.wasted_refetches == metrics.source_refetches


def test_blind_strategy_never_attributes_source_refetches() -> None:
    """blind never re-fetches on INVALID ⇒ source_refetches == 0 even at max
    volatility. The cost floor pays zero source-triggered re-fetch cost.
    """
    metrics = SimulationEngine(
        _refetch_scenario(volatility=1.0, answer_sensitivity=0.5),
        strategy_name="blind",
        seed=5,
    ).run()

    assert metrics.source_refetches == 0
    assert metrics.wasted_refetches == 0


def test_source_refetch_metrics_appear_in_to_dict() -> None:
    """Both new fields are part of the JSON-safe payload."""
    metrics = SimulationEngine(
        _refetch_scenario(volatility=0.5, answer_sensitivity=0.5),
        strategy_name="lazy",
        seed=5,
    ).run()
    payload = metrics.to_dict()

    assert "source_refetches" in payload
    assert "wasted_refetches" in payload
    assert payload["source_refetches"] == metrics.source_refetches
    assert payload["wasted_refetches"] == metrics.wasted_refetches


def test_source_refetch_metrics_are_deterministic_for_same_seed() -> None:
    """Same seed ⇒ identical source/wasted re-fetch counts across two runs."""
    scenario = _refetch_scenario(volatility=0.5, answer_sensitivity=0.5)
    first = SimulationEngine(scenario, strategy_name="lazy", seed=123).run()
    second = SimulationEngine(scenario, strategy_name="lazy", seed=123).run()

    assert first.source_refetches == second.source_refetches
    assert first.wasted_refetches == second.wasted_refetches


def test_source_refetch_metrics_default_to_zero_when_mutation_disabled() -> None:
    """No source mutations ⇒ no attribution: both counters stay 0."""
    metrics = SimulationEngine(_scenario(), strategy_name="lazy", seed=5).run()

    assert metrics.source_refetches == 0
    assert metrics.wasted_refetches == 0
