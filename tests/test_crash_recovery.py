# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""Tests for crash-recovery: stable-grant sweep + reclamation-aware commit error."""

from __future__ import annotations

import json
from pathlib import Path
from uuid import uuid4

import pytest

from ccs.coordinator.registry import ArtifactRegistry
from ccs.coordinator.service import CoordinatorService
from ccs.core.exceptions import CoherenceError
from ccs.core.invariants import check_monotonic_version, check_single_writer
from ccs.core.states import MESIState, TransientState
from ccs.core.types import FetchRequest
from ccs.validation import validate_log


def _service() -> CoordinatorService:
    return CoordinatorService(ArtifactRegistry())


# ---------------------------------------------------------------------------
# Sweep happy paths
# ---------------------------------------------------------------------------

def test_heartbeat_stale_exclusive_is_reclaimed() -> None:
    """R1: agent A holds EXCLUSIVE; heartbeat older than threshold; sweep reclaims."""
    entries: list[dict] = []
    registry = ArtifactRegistry(state_log=entries.append, instance_id="inst-1")
    svc = CoordinatorService(registry)
    artifact = svc.register_artifact(name="plan.md", content="v1")
    agent_a = uuid4()

    svc.fetch(FetchRequest(artifact_id=artifact.id, requesting_agent_id=agent_a, requested_at_tick=1))
    assert registry.get_agent_state(artifact.id, agent_a) == MESIState.EXCLUSIVE
    svc.record_heartbeat(agent_id=agent_a, now_tick=1)

    n = svc.enforce_stable_grant_timeouts(
        current_tick=20, heartbeat_timeout_ticks=10, max_hold_ticks=1000
    )

    assert n == 1
    assert registry.get_agent_state(artifact.id, agent_a) == MESIState.INVALID
    last_entry = entries[-1]
    assert last_entry["trigger"] == "reclaim_heartbeat"
    assert last_entry["content_hash"] is None
    assert last_entry["to_state"] == "INVALID"


def test_max_hold_modified_is_reclaimed_with_fresh_heartbeat() -> None:
    """R2: MODIFIED, fresh heartbeat, granted_at older than max_hold_ticks → reclaim_max_hold."""
    entries: list[dict] = []
    registry = ArtifactRegistry(state_log=entries.append, instance_id="inst-1")
    svc = CoordinatorService(registry)
    artifact = svc.register_artifact(name="plan.md", content="v1")
    agent_a = uuid4()

    svc.fetch(FetchRequest(artifact_id=artifact.id, requesting_agent_id=agent_a, requested_at_tick=0))
    # Drive A into MODIFIED via commit at tick 10 — sets granted_at_tick=10 (E→M preserves
    # original grant, which is tick 0; commit changes the state to MODIFIED but stays in M∪E,
    # so granted_at remains 0 from the initial fetch).
    svc.commit(agent_id=agent_a, artifact_id=artifact.id, content="v2", issued_at_tick=10)
    assert registry.get_agent_state(artifact.id, agent_a) == MESIState.MODIFIED

    granted_at = registry.granted_at_tick(agent_a, artifact.id)
    assert granted_at is not None
    version_before = registry.get_artifact(artifact.id).version

    # Fresh heartbeat — heartbeat trigger must NOT fire.
    svc.record_heartbeat(agent_id=agent_a, now_tick=granted_at + 500)

    n = svc.enforce_stable_grant_timeouts(
        current_tick=granted_at + 500,
        heartbeat_timeout_ticks=100,
        max_hold_ticks=400,
    )

    assert n == 1
    assert registry.get_agent_state(artifact.id, agent_a) == MESIState.INVALID
    assert registry.get_artifact(artifact.id).version == version_before
    last_entry = entries[-1]
    assert last_entry["trigger"] == "reclaim_max_hold"


def test_trigger_ordering_heartbeat_wins_over_max_hold() -> None:
    """A grant violating BOTH conditions reports reclaim_heartbeat (first match)."""
    entries: list[dict] = []
    registry = ArtifactRegistry(state_log=entries.append, instance_id="inst-1")
    svc = CoordinatorService(registry)
    artifact = svc.register_artifact(name="plan.md", content="v1")
    agent_a = uuid4()

    svc.fetch(FetchRequest(artifact_id=artifact.id, requesting_agent_id=agent_a, requested_at_tick=0))
    svc.record_heartbeat(agent_id=agent_a, now_tick=0)

    n = svc.enforce_stable_grant_timeouts(
        current_tick=10_000, heartbeat_timeout_ticks=10, max_hold_ticks=100
    )

    assert n == 1
    assert entries[-1]["trigger"] == "reclaim_heartbeat"


# ---------------------------------------------------------------------------
# Sweep edge cases
# ---------------------------------------------------------------------------

def test_agent_in_transient_eia_is_not_reclaimed() -> None:
    svc = _service()
    artifact = svc.register_artifact(name="plan.md", content="v1")
    agent_a = uuid4()

    svc.fetch(FetchRequest(artifact_id=artifact.id, requesting_agent_id=agent_a, requested_at_tick=0))
    # Push A into transient EIA without changing its EXCLUSIVE state.
    svc.registry.set_agent_transient(
        artifact.id, agent_a, TransientState.EIA, entered_tick=1
    )

    n = svc.enforce_stable_grant_timeouts(
        current_tick=1000, heartbeat_timeout_ticks=10, max_hold_ticks=100
    )

    assert n == 0
    assert svc.registry.get_agent_state(artifact.id, agent_a) == MESIState.EXCLUSIVE


def test_shared_holder_is_untouched() -> None:
    svc = _service()
    artifact = svc.register_artifact(name="plan.md", content="v1")
    a, b = uuid4(), uuid4()
    svc.fetch(FetchRequest(artifact_id=artifact.id, requesting_agent_id=a, requested_at_tick=0))
    svc.fetch(FetchRequest(artifact_id=artifact.id, requesting_agent_id=b, requested_at_tick=1))
    # Both now SHARED.
    assert svc.registry.get_agent_state(artifact.id, a) == MESIState.SHARED
    assert svc.registry.get_agent_state(artifact.id, b) == MESIState.SHARED

    n = svc.enforce_stable_grant_timeouts(
        current_tick=10_000, heartbeat_timeout_ticks=10, max_hold_ticks=100
    )

    assert n == 0
    assert svc.registry.get_agent_state(artifact.id, a) == MESIState.SHARED
    assert svc.registry.get_agent_state(artifact.id, b) == MESIState.SHARED


def test_grant_within_both_timeouts_is_no_op() -> None:
    entries: list[dict] = []
    registry = ArtifactRegistry(state_log=entries.append, instance_id="inst-1")
    svc = CoordinatorService(registry)
    artifact = svc.register_artifact(name="plan.md", content="v1")
    agent_a = uuid4()

    svc.fetch(FetchRequest(artifact_id=artifact.id, requesting_agent_id=agent_a, requested_at_tick=5))
    svc.record_heartbeat(agent_id=agent_a, now_tick=5)
    seq_before = registry._seq

    n = svc.enforce_stable_grant_timeouts(
        current_tick=10, heartbeat_timeout_ticks=10, max_hold_ticks=100
    )

    assert n == 0
    assert registry._seq == seq_before  # No log entries emitted.
    assert registry.get_agent_state(artifact.id, agent_a) == MESIState.EXCLUSIVE


# ---------------------------------------------------------------------------
# Safety (R3)
# ---------------------------------------------------------------------------

def test_peer_can_acquire_exclusive_after_reclamation() -> None:
    """R3: after reclamation of A, peer B's write() succeeds; single-writer holds."""
    svc = _service()
    artifact = svc.register_artifact(name="plan.md", content="v1")
    a, b = uuid4(), uuid4()
    svc.fetch(FetchRequest(artifact_id=artifact.id, requesting_agent_id=a, requested_at_tick=0))
    svc.record_heartbeat(agent_id=a, now_tick=0)

    svc.enforce_stable_grant_timeouts(
        current_tick=100, heartbeat_timeout_ticks=10, max_hold_ticks=1000
    )
    assert svc.registry.get_agent_state(artifact.id, a) == MESIState.INVALID

    svc.write(agent_id=b, artifact_id=artifact.id, issued_at_tick=101)
    assert svc.registry.get_agent_state(artifact.id, b) == MESIState.EXCLUSIVE
    # Single-writer holds throughout.
    check_single_writer(svc.registry.get_state_map(artifact.id))


def test_monotonic_version_unchanged_across_sweep() -> None:
    """R3: version unchanged for reclaimed grants (reclamation is state-only)."""
    svc = _service()
    artifact = svc.register_artifact(name="plan.md", content="v1")
    a = uuid4()
    svc.fetch(FetchRequest(artifact_id=artifact.id, requesting_agent_id=a, requested_at_tick=0))

    v_before = svc.registry.get_artifact(artifact.id).version
    svc.enforce_stable_grant_timeouts(
        current_tick=100, heartbeat_timeout_ticks=10, max_hold_ticks=1000
    )
    v_after = svc.registry.get_artifact(artifact.id).version
    assert v_before == v_after
    check_monotonic_version(v_before, v_after)


# ---------------------------------------------------------------------------
# Sweep ordering (R4)
# ---------------------------------------------------------------------------

def test_transient_sweep_first_then_stable_skips_invalid() -> None:
    """R4: transient sweep reclaims first; stable sweep sees INVALID and skips. One log entry."""
    entries: list[dict] = []
    registry = ArtifactRegistry(state_log=entries.append, instance_id="inst-1")
    svc = CoordinatorService(registry)
    artifact = svc.register_artifact(name="plan.md", content="v1")
    a = uuid4()

    svc.fetch(FetchRequest(artifact_id=artifact.id, requesting_agent_id=a, requested_at_tick=0))
    # Push A into transient EIA at tick 1 while keeping EXCLUSIVE state.
    registry.set_agent_transient(artifact.id, a, TransientState.EIA, entered_tick=1)
    # No heartbeat — A would also fail the heartbeat trigger.

    entries_at_setup = len(entries)

    transient_n = svc.enforce_transient_timeouts(current_tick=1000, timeout_ticks=10)
    assert transient_n == 1
    # State is now INVALID, transient cleared.
    assert registry.get_agent_state(artifact.id, a) == MESIState.INVALID
    assert registry.get_agent_transient(artifact.id, a) is None
    # Exactly one new entry from the transient sweep, with trigger="timeout".
    assert len(entries) == entries_at_setup + 1
    assert entries[-1]["trigger"] == "timeout"

    # Stable sweep now sees A in INVALID — should skip entirely.
    stable_n = svc.enforce_stable_grant_timeouts(
        current_tick=1000, heartbeat_timeout_ticks=10, max_hold_ticks=100
    )
    assert stable_n == 0
    assert len(entries) == entries_at_setup + 1  # No new entry.


# ---------------------------------------------------------------------------
# Observability (R6)
# ---------------------------------------------------------------------------

def test_state_log_validates_clean_after_reclamation(tmp_path: Path) -> None:
    """R6: validate_log reports no gaps; sequence numbers contiguous."""
    log_path = tmp_path / "state_log.jsonl"
    fh = log_path.open("w", encoding="utf-8")

    def emit(entry: dict) -> None:
        fh.write(json.dumps(entry) + "\n")

    registry = ArtifactRegistry(state_log=emit, instance_id="inst-r6")
    svc = CoordinatorService(registry)
    artifact = svc.register_artifact(name="plan.md", content="v1")
    a, b = uuid4(), uuid4()
    svc.fetch(FetchRequest(artifact_id=artifact.id, requesting_agent_id=a, requested_at_tick=0))
    svc.record_heartbeat(agent_id=a, now_tick=0)
    svc.enforce_stable_grant_timeouts(
        current_tick=100, heartbeat_timeout_ticks=10, max_hold_ticks=1000
    )
    svc.write(agent_id=b, artifact_id=artifact.id, issued_at_tick=101)
    fh.close()

    gaps, mismatches = validate_log(log_path, schema_version="ccs.state_log.v2")
    assert gaps == []
    assert mismatches == []


# ---------------------------------------------------------------------------
# Reclamation-aware commit error (R8)
# ---------------------------------------------------------------------------

def test_commit_after_heartbeat_reclamation_includes_diagnostic_message() -> None:
    """R8 (jingchang0623): late commit after reclaim_heartbeat carries trigger and tick."""
    svc = _service()
    artifact = svc.register_artifact(name="plan.md", content="v1")
    a = uuid4()
    svc.fetch(FetchRequest(artifact_id=artifact.id, requesting_agent_id=a, requested_at_tick=0))
    svc.record_heartbeat(agent_id=a, now_tick=0)

    svc.enforce_stable_grant_timeouts(
        current_tick=100, heartbeat_timeout_ticks=10, max_hold_ticks=1000
    )

    with pytest.raises(CoherenceError) as exc_info:
        svc.commit(agent_id=a, artifact_id=artifact.id, content="v2", issued_at_tick=101)

    msg = str(exc_info.value)
    assert "reclaimed_by=reclaim_heartbeat" in msg
    assert "at_tick=100" in msg


def test_commit_after_max_hold_reclamation_then_shared_fetch_still_diagnostic() -> None:
    """R8 (jessieibarra): slot survives SHARED re-fetch.

    A holds MODIFIED; sweep reclaims via reclaim_max_hold; A fetches and gets SHARED;
    A's stale commit STILL raises with the reclamation context.
    """
    svc = _service()
    artifact = svc.register_artifact(name="plan.md", content="v1")
    a, b = uuid4(), uuid4()

    # Drive A to MODIFIED.
    svc.fetch(FetchRequest(artifact_id=artifact.id, requesting_agent_id=a, requested_at_tick=0))
    svc.commit(agent_id=a, artifact_id=artifact.id, content="v2", issued_at_tick=1)
    assert svc.registry.get_agent_state(artifact.id, a) == MESIState.MODIFIED

    # Heartbeat fresh — only max-hold should fire.
    svc.record_heartbeat(agent_id=a, now_tick=500)
    svc.enforce_stable_grant_timeouts(
        current_tick=500, heartbeat_timeout_ticks=1000, max_hold_ticks=10
    )
    assert svc.registry.get_agent_state(artifact.id, a) == MESIState.INVALID
    assert svc.registry.get_last_reclamation(a, artifact.id) == ("reclaim_max_hold", 500)

    # B acquires + fetches so subsequent A-fetch grants SHARED, not EXCLUSIVE.
    svc.write(agent_id=b, artifact_id=artifact.id, issued_at_tick=501)
    svc.fetch(FetchRequest(artifact_id=artifact.id, requesting_agent_id=a, requested_at_tick=502))
    assert svc.registry.get_agent_state(artifact.id, a) == MESIState.SHARED

    # Slot survives SHARED transition.
    assert svc.registry.get_last_reclamation(a, artifact.id) == ("reclaim_max_hold", 500)

    with pytest.raises(CoherenceError) as exc_info:
        svc.commit(agent_id=a, artifact_id=artifact.id, content="v3", issued_at_tick=503)
    msg = str(exc_info.value)
    assert "reclaimed_by=reclaim_max_hold" in msg
    assert "at_tick=500" in msg


def test_commit_on_unrelated_invalid_uses_original_message_format() -> None:
    """R5: agent never held a grant; rejection message has no reclaimed_by= field."""
    svc = _service()
    artifact = svc.register_artifact(name="plan.md", content="v1")
    a = uuid4()
    # A is implicitly INVALID (never registered any state) — commit should reject without diagnostic.

    with pytest.raises(CoherenceError) as exc_info:
        svc.commit(agent_id=a, artifact_id=artifact.id, content="v2")

    msg = str(exc_info.value)
    assert "commit_not_allowed" in msg
    assert "reclaimed_by=" not in msg
    assert "at_tick=" not in msg


# ---------------------------------------------------------------------------
# Re-reclamation
# ---------------------------------------------------------------------------

def test_re_reclamation_after_reacquire_reflects_only_most_recent() -> None:
    """A reclaimed → re-acquires EXCLUSIVE (slot cleared) → reclaimed again with new trigger."""
    svc = _service()
    artifact = svc.register_artifact(name="plan.md", content="v1")
    a = uuid4()

    # First reclamation: heartbeat-stale.
    svc.fetch(FetchRequest(artifact_id=artifact.id, requesting_agent_id=a, requested_at_tick=0))
    svc.record_heartbeat(agent_id=a, now_tick=0)
    svc.enforce_stable_grant_timeouts(
        current_tick=100, heartbeat_timeout_ticks=10, max_hold_ticks=10_000
    )
    assert svc.registry.get_last_reclamation(a, artifact.id) == ("reclaim_heartbeat", 100)

    # A re-acquires EXCLUSIVE — slot must be cleared on M∪E acquire.
    svc.write(agent_id=a, artifact_id=artifact.id, issued_at_tick=200)
    assert svc.registry.get_agent_state(artifact.id, a) == MESIState.EXCLUSIVE
    assert svc.registry.get_last_reclamation(a, artifact.id) is None

    # Second reclamation: max-hold (heartbeat fresh this time).
    svc.record_heartbeat(agent_id=a, now_tick=10_000)
    svc.enforce_stable_grant_timeouts(
        current_tick=10_000, heartbeat_timeout_ticks=10_000, max_hold_ticks=100
    )
    assert svc.registry.get_last_reclamation(a, artifact.id) == ("reclaim_max_hold", 10_000)

    # Late commit reflects only the most recent reclamation.
    with pytest.raises(CoherenceError) as exc_info:
        svc.commit(agent_id=a, artifact_id=artifact.id, content="v2", issued_at_tick=10_001)
    msg = str(exc_info.value)
    assert "reclaimed_by=reclaim_max_hold" in msg
    assert "at_tick=10000" in msg
    assert "reclaim_heartbeat" not in msg


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def test_invalid_heartbeat_timeout_raises() -> None:
    svc = _service()
    with pytest.raises(ValueError):
        svc.enforce_stable_grant_timeouts(
            current_tick=10, heartbeat_timeout_ticks=0, max_hold_ticks=100
        )


def test_invalid_max_hold_raises() -> None:
    svc = _service()
    with pytest.raises(ValueError):
        svc.enforce_stable_grant_timeouts(
            current_tick=10, heartbeat_timeout_ticks=10, max_hold_ticks=0
        )


# ---------------------------------------------------------------------------
# Unit 5: combined validation-scenario driver test (R9)
# ---------------------------------------------------------------------------


def test_combined_validation_scenario_exercises_all_three_reclaim_shapes(
    tmp_path: Path,
) -> None:
    """R9: one scenario exercises OOM-kill, checkpoint-restore-then-commit,
    and live-but-stuck max-hold shapes in a single repeatable run.

    Setup:
      - agent_0 holds EXCLUSIVE on artifact_0; killed at tick 5 (YAML);
        expected: reclaim_heartbeat. agent_3 then writes artifact_0.
      - agent_1 holds MODIFIED on artifact_1; killed at tick 10 (YAML),
        restored at tick 30 (YAML); driver simulates the stale post-restore
        commit() and asserts CoherenceError carries reclaimed_by/at_tick.
      - agent_2 holds EXCLUSIVE on artifact_2; stays alive (heartbeats every
        tick); never commits. With max_hold_ticks=400, sweep reclaims via
        reclaim_max_hold around tick 400.

    YAML drives kill/restore deterministically; driver seeds the M∪E grants
    (engine has no per-agent grant config) and issues post-run probes.
    """
    from ccs.core.types import FetchRequest as _FR
    from ccs.simulation.engine import SimulationEngine
    from ccs.simulation.scenarios import load_scenario

    scenario = load_scenario("benchmarks/scenarios/crash_recovery_validation.yaml")
    engine = SimulationEngine(scenario, strategy_name="lazy")

    # Wire a state-log capture into the engine's registry so we can audit
    # reclamation entries and run validate_log on a JSONL dump afterwards.
    log_entries: list[dict] = []
    log_path = tmp_path / "state_log.jsonl"
    log_fh = log_path.open("w", encoding="utf-8")

    def _emit(entry: dict) -> None:
        log_entries.append(entry)
        log_fh.write(json.dumps(entry) + "\n")

    engine._registry._state_log = _emit  # noqa: SLF001 — test-only capture hook
    # Reset _seq so log entries start at 1 even though no log was attached
    # at registry construction; without this the first entry observed by the
    # capture lambda begins at the post-construction _seq value, not 1.
    engine._registry._seq = 0  # noqa: SLF001

    artifact_0 = engine._artifact_ids[0]
    artifact_1 = engine._artifact_ids[1]
    artifact_2 = engine._artifact_ids[2]
    agent_0 = engine._agent_id_by_name["agent_0"]
    agent_1 = engine._agent_id_by_name["agent_1"]
    agent_2 = engine._agent_id_by_name["agent_2"]
    agent_3 = engine._agent_id_by_name["agent_3"]

    # Seed initial M∪E grants at tick 0 — drive directly through the coordinator.
    engine._coordinator.fetch(  # agent_0 → EXCLUSIVE on artifact_0
        _FR(artifact_id=artifact_0, requesting_agent_id=agent_0, requested_at_tick=0)
    )
    engine._coordinator.fetch(  # agent_1 → EXCLUSIVE on artifact_1
        _FR(artifact_id=artifact_1, requesting_agent_id=agent_1, requested_at_tick=0)
    )
    engine._coordinator.commit(  # promote agent_1 to MODIFIED on artifact_1
        agent_id=agent_1, artifact_id=artifact_1, content="v2", issued_at_tick=0
    )
    engine._coordinator.fetch(  # agent_2 → EXCLUSIVE on artifact_2
        _FR(artifact_id=artifact_2, requesting_agent_id=agent_2, requested_at_tick=0)
    )

    assert engine._registry.get_agent_state(artifact_0, agent_0) == MESIState.EXCLUSIVE
    assert engine._registry.get_agent_state(artifact_1, agent_1) == MESIState.MODIFIED
    assert engine._registry.get_agent_state(artifact_2, agent_2) == MESIState.EXCLUSIVE

    # Run the simulation. The engine's per-tick loop applies failure events,
    # heartbeats _alive_agents, and runs the stable-grant sweep each tick.
    metrics = engine.run()

    # ---- Reclamation count + per-trigger breakdown ----
    reclaim_entries = [
        e for e in log_entries
        if e.get("trigger") in {"reclaim_heartbeat", "reclaim_max_hold"}
    ]
    heartbeat_entries = [e for e in reclaim_entries if e["trigger"] == "reclaim_heartbeat"]
    max_hold_entries = [e for e in reclaim_entries if e["trigger"] == "reclaim_max_hold"]

    assert metrics.stable_grant_reclamations >= 3, (
        f"expected ≥3 reclamations (one per agent_0/agent_1/agent_2); "
        f"got {metrics.stable_grant_reclamations}"
    )
    assert len(heartbeat_entries) >= 2, (
        f"expected ≥2 reclaim_heartbeat entries (agent_0 + agent_1); "
        f"got {len(heartbeat_entries)}"
    )
    assert len(max_hold_entries) >= 1, (
        f"expected ≥1 reclaim_max_hold entry (agent_2 live-but-stuck); "
        f"got {len(max_hold_entries)}"
    )

    # ---- Per-agent state checks ----
    assert engine._registry.get_agent_state(artifact_0, agent_0) == MESIState.INVALID
    assert engine._registry.get_agent_state(artifact_1, agent_1) == MESIState.INVALID
    assert engine._registry.get_agent_state(artifact_2, agent_2) == MESIState.INVALID

    # ---- Post-reclamation peer write (jingchang0623 unblocks) ----
    engine._coordinator.write(
        agent_id=agent_3, artifact_id=artifact_0, issued_at_tick=599
    )
    assert engine._registry.get_agent_state(artifact_0, agent_3) == MESIState.EXCLUSIVE
    check_single_writer(engine._registry.get_state_map(artifact_0))

    # ---- jessieibarra path: agent_1's late commit raises with diagnostic ----
    reclaim_for_a1 = engine._registry.get_last_reclamation(agent_1, artifact_1)
    assert reclaim_for_a1 is not None
    a1_trigger, a1_tick = reclaim_for_a1
    assert a1_trigger == "reclaim_heartbeat"

    with pytest.raises(CoherenceError) as exc_info:
        engine._coordinator.commit(
            agent_id=agent_1,
            artifact_id=artifact_1,
            content="late-restored-content",
            issued_at_tick=599,
        )
    msg = str(exc_info.value)
    assert "reclaimed_by=reclaim_heartbeat" in msg
    assert f"at_tick={a1_tick}" in msg

    # ---- Single-writer + monotonic version invariants on every artifact ----
    for artifact_id in engine._artifact_ids:
        check_single_writer(engine._registry.get_state_map(artifact_id))
        # Reclamation does not bump version; commit by agent_1 (tick 0) bumped
        # artifact_1 from v1 → v2, and agent_3's post-run write left
        # artifact_0 at v1 (write request without commit). Just spot-check
        # version is positive and the helper accepts the trivial pair.
        version = engine._registry.get_artifact(artifact_id).version
        assert version >= 1
        check_monotonic_version(version, version)

    # ---- State-log validation: no gaps, no schema mismatches (R6) ----
    log_fh.close()
    gaps, mismatches = validate_log(log_path, schema_version="ccs.state_log.v2")
    assert gaps == []
    assert mismatches == []
