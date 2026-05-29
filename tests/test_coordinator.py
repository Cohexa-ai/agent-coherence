# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""Contract tests for coordinator registry/service operations."""

from __future__ import annotations

from uuid import uuid4

import pytest

from ccs.coordinator.registry import ArtifactRegistry
from ccs.coordinator.service import CoordinatorService
from ccs.core.exceptions import CoherenceError
from ccs.core.states import MESIState, TransientState
from ccs.core.types import FetchRequest


def _service() -> CoordinatorService:
    return CoordinatorService(ArtifactRegistry())


def test_fetch_first_holder_gets_exclusive() -> None:
    svc = _service()
    artifact = svc.register_artifact(name="plan.md", content="v1")
    agent_a = uuid4()

    resp = svc.fetch(FetchRequest(artifact_id=artifact.id, requesting_agent_id=agent_a, requested_at_tick=1))

    assert resp.state_grant == MESIState.EXCLUSIVE
    assert resp.version == 1
    assert svc.registry.get_agent_state(artifact.id, agent_a) == MESIState.EXCLUSIVE


def test_second_fetch_downgrades_existing_owner_to_shared() -> None:
    svc = _service()
    artifact = svc.register_artifact(name="plan.md", content="v1")
    agent_a = uuid4()
    agent_b = uuid4()

    svc.fetch(FetchRequest(artifact_id=artifact.id, requesting_agent_id=agent_a, requested_at_tick=1))
    resp_b = svc.fetch(FetchRequest(artifact_id=artifact.id, requesting_agent_id=agent_b, requested_at_tick=2))

    assert resp_b.state_grant == MESIState.SHARED
    assert svc.registry.get_agent_state(artifact.id, agent_a) == MESIState.SHARED
    assert svc.registry.get_agent_state(artifact.id, agent_b) == MESIState.SHARED


def test_write_invalidates_peers_and_grants_exclusive() -> None:
    svc = _service()
    artifact = svc.register_artifact(name="plan.md", content="v1")
    agent_a = uuid4()
    agent_b = uuid4()
    svc.fetch(FetchRequest(artifact_id=artifact.id, requesting_agent_id=agent_a, requested_at_tick=1))
    svc.fetch(FetchRequest(artifact_id=artifact.id, requesting_agent_id=agent_b, requested_at_tick=2))

    signals = svc.write(agent_id=agent_a, artifact_id=artifact.id)

    assert len(signals) == 1
    assert signals[0].issuer_agent_id == agent_a
    assert svc.registry.get_agent_state(artifact.id, agent_a) == MESIState.EXCLUSIVE
    assert svc.registry.get_agent_state(artifact.id, agent_b) == MESIState.INVALID


def test_commit_requires_owner_state() -> None:
    svc = _service()
    artifact = svc.register_artifact(name="plan.md", content="v1")
    non_owner = uuid4()
    with pytest.raises(CoherenceError):
        svc.commit(agent_id=non_owner, artifact_id=artifact.id, content="v2")


def test_commit_increments_version_monotonically() -> None:
    svc = _service()
    owner = uuid4()
    peer = uuid4()
    artifact = svc.register_artifact(name="plan.md", content="v1")
    svc.fetch(FetchRequest(artifact_id=artifact.id, requesting_agent_id=owner, requested_at_tick=1))
    svc.fetch(FetchRequest(artifact_id=artifact.id, requesting_agent_id=peer, requested_at_tick=2))
    svc.write(agent_id=owner, artifact_id=artifact.id)

    v2, _ = svc.commit(agent_id=owner, artifact_id=artifact.id, content="v2")
    svc.write(agent_id=owner, artifact_id=artifact.id)
    v3, _ = svc.commit(agent_id=owner, artifact_id=artifact.id, content="v3")

    assert v2.version == 2
    assert v3.version == 3
    assert svc.registry.get_agent_state(artifact.id, owner) == MESIState.MODIFIED
    assert svc.registry.get_agent_state(artifact.id, peer) == MESIState.INVALID


def test_invalidate_marks_agent_invalid() -> None:
    svc = _service()
    artifact = svc.register_artifact(name="plan.md", content="v1")
    agent = uuid4()
    svc.fetch(FetchRequest(artifact_id=artifact.id, requesting_agent_id=agent, requested_at_tick=1))
    signal = svc.invalidate(
        agent_id=agent,
        artifact_id=artifact.id,
        new_version=2,
        issuer_agent_id=uuid4(),
        issued_at_tick=10,
    )

    assert signal.new_version == 2
    assert svc.registry.get_agent_state(artifact.id, agent) == MESIState.INVALID


def test_upgrade_flow_grants_exclusive_and_invalidates_peer() -> None:
    svc = _service()
    artifact = svc.register_artifact(name="plan.md", content="v1")
    owner = uuid4()
    peer = uuid4()
    svc.fetch(FetchRequest(artifact_id=artifact.id, requesting_agent_id=owner, requested_at_tick=1))
    svc.fetch(FetchRequest(artifact_id=artifact.id, requesting_agent_id=peer, requested_at_tick=2))

    signals = svc.upgrade(agent_id=owner, artifact_id=artifact.id, issued_at_tick=7)

    assert len(signals) == 1
    assert signals[0].issued_at_tick == 7
    assert svc.registry.get_agent_state(artifact.id, owner) == MESIState.EXCLUSIVE
    assert svc.registry.get_agent_state(artifact.id, peer) == MESIState.INVALID


def test_fetch_raises_coherence_error_when_content_missing() -> None:
    svc = _service()
    artifact = svc.register_artifact(name="plan.md", content="v1")
    svc.registry.set_artifact_and_content(artifact.id, artifact, None)  # type: ignore[arg-type]

    with pytest.raises(CoherenceError):
        svc.fetch(
            FetchRequest(
                artifact_id=artifact.id,
                requesting_agent_id=uuid4(),
                requested_at_tick=1,
            )
        )


def test_write_and_commit_propagate_issued_tick_in_signals() -> None:
    svc = _service()
    artifact = svc.register_artifact(name="plan.md", content="v1")
    owner = uuid4()
    peer = uuid4()
    svc.fetch(FetchRequest(artifact_id=artifact.id, requesting_agent_id=owner, requested_at_tick=1))
    svc.fetch(FetchRequest(artifact_id=artifact.id, requesting_agent_id=peer, requested_at_tick=2))

    write_signals = svc.write(agent_id=owner, artifact_id=artifact.id, issued_at_tick=11)
    assert write_signals
    assert all(signal.issued_at_tick == 11 for signal in write_signals)

    updated, commit_signals = svc.commit(
        agent_id=owner,
        artifact_id=artifact.id,
        content="v2",
        issued_at_tick=13,
    )
    assert updated.version == 2
    assert all(signal.issued_at_tick == 13 for signal in commit_signals)


def test_delete_returns_signals_for_non_invalid_holders_and_removes_artifact() -> None:
    svc = _service()
    artifact = svc.register_artifact(name="plan.md", content="v1")
    agent_a = uuid4()
    agent_b = uuid4()
    agent_c = uuid4()
    svc.fetch(FetchRequest(artifact_id=artifact.id, requesting_agent_id=agent_a, requested_at_tick=1))
    svc.fetch(FetchRequest(artifact_id=artifact.id, requesting_agent_id=agent_b, requested_at_tick=2))
    # agent_c has INVALID state (never fetched)

    signals = svc.delete(agent_id=agent_a, artifact_id=artifact.id, issued_at_tick=5)

    assert len(signals) == 2
    assert all(s.artifact_id == artifact.id for s in signals)
    assert all(s.issued_at_tick == 5 for s in signals)
    assert not svc.registry.has_artifact(artifact.id)


def test_delete_absent_artifact_returns_empty_list() -> None:
    svc = _service()
    absent_id = uuid4()

    signals = svc.delete(agent_id=uuid4(), artifact_id=absent_id)

    assert signals == []


def test_delete_all_invalid_holders_removes_artifact_and_returns_empty() -> None:
    svc = _service()
    artifact = svc.register_artifact(name="plan.md", content="v1")
    agent_a = uuid4()
    # Register agent state as INVALID explicitly
    svc.registry.set_agent_state(artifact.id, agent_a, MESIState.INVALID)

    signals = svc.delete(agent_id=uuid4(), artifact_id=artifact.id)

    assert signals == []
    assert not svc.registry.has_artifact(artifact.id)


def test_invalidate_returns_none_after_artifact_deleted() -> None:
    svc = _service()
    artifact = svc.register_artifact(name="plan.md", content="v1")
    agent_a = uuid4()
    svc.fetch(FetchRequest(artifact_id=artifact.id, requesting_agent_id=agent_a, requested_at_tick=1))
    svc.delete(agent_id=agent_a, artifact_id=artifact.id)

    result = svc.invalidate(
        agent_id=agent_a,
        artifact_id=artifact.id,
        new_version=1,
        issuer_agent_id=uuid4(),
        issued_at_tick=2,
    )

    assert result is None


def test_remove_artifact_unknown_id_is_silent() -> None:
    registry = ArtifactRegistry()
    registry.remove_artifact(uuid4())  # must not raise


def test_peer_transient_lifecycle_set_on_write_and_cleared_on_invalidate_ack() -> None:
    svc = _service()
    artifact = svc.register_artifact(name="plan.md", content="v1")
    owner = uuid4()
    peer = uuid4()
    svc.fetch(FetchRequest(artifact_id=artifact.id, requesting_agent_id=owner, requested_at_tick=1))
    svc.fetch(FetchRequest(artifact_id=artifact.id, requesting_agent_id=peer, requested_at_tick=2))

    svc.write(agent_id=owner, artifact_id=artifact.id, issued_at_tick=5)
    assert svc.registry.get_agent_transient(artifact.id, peer) == TransientState.SIA
    assert svc.registry.get_transient_tick(artifact.id, peer) == 5

    svc.invalidate(
        agent_id=peer,
        artifact_id=artifact.id,
        new_version=2,
        issuer_agent_id=owner,
        issued_at_tick=6,
    )
    assert svc.registry.get_agent_transient(artifact.id, peer) is None


# --- Crash-recovery Unit 1: heartbeat + grant-slot bookkeeping ---


def test_record_heartbeat_returns_last_seen_tick() -> None:
    svc = _service()
    agent = uuid4()

    svc.record_heartbeat(agent_id=agent, now_tick=5)

    assert svc.registry.last_heartbeat_tick(agent) == 5


def test_record_heartbeat_returns_max_of_seen_ticks() -> None:
    """R12 monotonicity: out-of-order delivery must not regress the heartbeat."""
    svc = _service()
    agent = uuid4()

    svc.record_heartbeat(agent_id=agent, now_tick=10)
    svc.record_heartbeat(agent_id=agent, now_tick=8)

    assert svc.registry.last_heartbeat_tick(agent) == 10


def test_record_heartbeat_rejects_negative_tick() -> None:
    svc = _service()
    with pytest.raises(ValueError):
        svc.record_heartbeat(agent_id=uuid4(), now_tick=-1)


def test_last_heartbeat_tick_returns_none_when_unknown() -> None:
    svc = _service()
    assert svc.registry.last_heartbeat_tick(uuid4()) is None


def test_record_heartbeat_does_not_emit_state_log_entry() -> None:
    """R5 byte-identity: heartbeat must NOT emit log entries or bump _seq."""
    entries: list[dict] = []
    registry = ArtifactRegistry(state_log=entries.append, instance_id="inst-1")
    svc = CoordinatorService(registry)
    seq_before = registry._seq

    svc.record_heartbeat(agent_id=uuid4(), now_tick=42)

    assert entries == []
    assert registry._seq == seq_before


def test_invalid_to_exclusive_populates_granted_at_tick() -> None:
    svc = _service()
    agent = uuid4()
    artifact = svc.register_artifact(name="plan.md", content="v1")

    svc.registry.set_agent_state(
        artifact.id, agent, MESIState.EXCLUSIVE, trigger="test", tick=7
    )

    assert svc.registry.granted_at_tick(agent, artifact.id) == 7


def test_exclusive_to_modified_preserves_granted_at_tick() -> None:
    svc = _service()
    agent = uuid4()
    artifact = svc.register_artifact(name="plan.md", content="v1")

    svc.registry.set_agent_state(
        artifact.id, agent, MESIState.EXCLUSIVE, trigger="test", tick=3
    )
    svc.registry.set_agent_state(
        artifact.id, agent, MESIState.MODIFIED, trigger="test", tick=9
    )

    # E→M stays within M∪E — the original grant tick must be preserved (R8 diagnostic context).
    assert svc.registry.granted_at_tick(agent, artifact.id) == 3


def test_modified_to_invalid_clears_granted_at_tick() -> None:
    svc = _service()
    agent = uuid4()
    artifact = svc.register_artifact(name="plan.md", content="v1")

    svc.registry.set_agent_state(
        artifact.id, agent, MESIState.MODIFIED, trigger="test", tick=3
    )
    svc.registry.set_agent_state(
        artifact.id, agent, MESIState.INVALID, trigger="test", tick=9
    )

    assert svc.registry.granted_at_tick(agent, artifact.id) is None


def test_reclamation_slot_survives_shared_acquire_then_clears_on_exclusive() -> None:
    """jessieibarra path: SHARED re-fetch by a reclaimed agent must NOT clear the slot.
    Slot clears ONLY on M∪E re-acquire."""
    svc = _service()
    agent = uuid4()
    artifact = svc.register_artifact(name="plan.md", content="v1")

    # Seed a reclamation slot via the helper (simulates Unit 2's sweep result).
    svc.registry.record_last_reclamation(agent, artifact.id, "reclaim_max_hold", 200)
    assert svc.registry.get_last_reclamation(agent, artifact.id) == ("reclaim_max_hold", 200)

    # INVALID → SHARED: slot must SURVIVE.
    svc.registry.set_agent_state(
        artifact.id, agent, MESIState.SHARED, trigger="fetch", tick=210
    )
    assert svc.registry.get_last_reclamation(agent, artifact.id) == ("reclaim_max_hold", 200)

    # SHARED → EXCLUSIVE (M∪E acquire, prev not in M∪E): slot now clears.
    svc.registry.set_agent_state(
        artifact.id, agent, MESIState.EXCLUSIVE, trigger="write", tick=220
    )
    assert svc.registry.get_last_reclamation(agent, artifact.id) is None


def test_reclamation_slot_untouched_by_set_agent_transient() -> None:
    """Slot-clear bookkeeping fires only on stable transitions through set_agent_state."""
    svc = _service()
    agent = uuid4()
    artifact = svc.register_artifact(name="plan.md", content="v1")
    svc.registry.record_last_reclamation(agent, artifact.id, "reclaim_heartbeat", 100)

    svc.registry.set_agent_transient(
        artifact.id, agent, TransientState.IED, entered_tick=105
    )
    assert svc.registry.get_last_reclamation(agent, artifact.id) == ("reclaim_heartbeat", 100)

    svc.registry.clear_agent_transient(artifact.id, agent)
    assert svc.registry.get_last_reclamation(agent, artifact.id) == ("reclaim_heartbeat", 100)


def test_set_agent_state_bookkeeping_emits_no_extra_log_entries() -> None:
    """R5 byte-identity: dict mutations on set_agent_state must NOT add state-log entries."""
    entries: list[dict] = []
    registry = ArtifactRegistry(state_log=entries.append, instance_id="inst-1")
    svc = CoordinatorService(registry)
    artifact = svc.register_artifact(name="plan.md", content="v1")
    agent = uuid4()
    entries_before = len(entries)
    seq_before = registry._seq

    registry.set_agent_state(artifact.id, agent, MESIState.EXCLUSIVE, trigger="t", tick=1)
    registry.set_agent_state(artifact.id, agent, MESIState.MODIFIED, trigger="t", tick=2)
    registry.set_agent_state(artifact.id, agent, MESIState.INVALID, trigger="t", tick=3)

    # Exactly 3 log entries total; bookkeeping itself produced none.
    assert len(entries) - entries_before == 3
    assert registry._seq - seq_before == 3


# ---- Unit 3: CrashRecoveryConfig + composition fail-fast --------------------

from ccs.coordinator.service import (  # noqa: E402
    CrashRecoveryConfig,
    validate_crash_recovery_config,
)
from ccs.strategies.lazy import LazyStrategy  # noqa: E402
from ccs.strategies.lease import LeaseStrategy  # noqa: E402


def test_crash_recovery_config_defaults_are_safe() -> None:
    """v0.8.3: bare CrashRecoveryConfig() construction is an intentional API
    surface and asserts both the v0.8.x default-disabled behavior AND the
    presence of the deprecation warning. The pytest.warns wrapper makes the
    warning part of the test contract, not collateral CI noise.
    """
    # Reset the module-level emit-once flag so this test sees the warning
    # regardless of test ordering. (The TestCrashRecoveryDeprecationWarning
    # class below also resets it; this standalone test predates that class.)
    from ccs.coordinator import service as _service_mod
    _service_mod._BARE_CONSTRUCTION_WARNED = False
    with pytest.warns(DeprecationWarning, match="enabled=True"):
        cfg = CrashRecoveryConfig()
    assert cfg.enabled is False
    assert cfg.heartbeat_timeout_ticks == 10
    assert cfg.max_hold_ticks == 1000


def testvalidate_crash_recovery_config_disabled_always_accepts() -> None:
    # Even an obviously bad max_hold_ticks vs ttl is fine when disabled.
    cfg = CrashRecoveryConfig(enabled=False, max_hold_ticks=10)
    validate_crash_recovery_config(cfg, LeaseStrategy(ttl_ticks=300))


def testvalidate_crash_recovery_config_rejects_equal_ttl() -> None:
    cfg = CrashRecoveryConfig(enabled=True, max_hold_ticks=300)
    with pytest.raises(ValueError, match="max_hold_ticks=300"):
        validate_crash_recovery_config(cfg, LeaseStrategy(ttl_ticks=300))


def testvalidate_crash_recovery_config_rejects_below_ttl() -> None:
    cfg = CrashRecoveryConfig(enabled=True, max_hold_ticks=100)
    with pytest.raises(ValueError):
        validate_crash_recovery_config(cfg, LeaseStrategy(ttl_ticks=300))


def testvalidate_crash_recovery_config_accepts_above_ttl() -> None:
    cfg = CrashRecoveryConfig(enabled=True, max_hold_ticks=300)
    validate_crash_recovery_config(cfg, LeaseStrategy(ttl_ticks=200))


def testvalidate_crash_recovery_config_skips_non_lease_strategy() -> None:
    """Strategies without ttl_ticks (lazy/eager/etc.) silent-accept (R11 skip rule)."""
    cfg = CrashRecoveryConfig(enabled=True, max_hold_ticks=300)
    # LazyStrategy exposes no ttl_ticks attribute.
    validate_crash_recovery_config(cfg, LazyStrategy())


# Review fix ADV-03: ttl_ticks=0 was silently routed to the warning path
# (with misleading "non-integer" text) and skipped R11 entirely. Now it's
# treated like any other int ttl and validated against max_hold_ticks.


class _ZeroTTLStrategy:
    """Test fixture: strategy that exposes ttl_ticks=0 (degenerate case)."""

    ttl_ticks = 0


class _StringTTLStrategy:
    """Test fixture: strategy with a non-integer ttl_ticks attribute."""

    ttl_ticks = "300"


def testvalidate_crash_recovery_config_zero_ttl_passes_when_max_hold_positive() -> None:
    """ADV-03: ttl_ticks=0 with max_hold_ticks=1 passes R11 (1 > 0). No warning."""
    import warnings as _w  # local import keeps top-of-file unchanged
    cfg = CrashRecoveryConfig(enabled=True, max_hold_ticks=1)
    with _w.catch_warnings():
        _w.simplefilter("error")  # Any warning fails the test.
        validate_crash_recovery_config(cfg, _ZeroTTLStrategy())


def testvalidate_crash_recovery_config_zero_ttl_rejects_zero_max_hold() -> None:
    """ADV-03: ttl_ticks=0 with max_hold_ticks=0 IS a R11 violation (0 not > 0)."""
    # max_hold_ticks=0 is normally rejected by the int-validator at sweep time,
    # but the composition rule must still flag this combination at construction.
    cfg = CrashRecoveryConfig(enabled=True, max_hold_ticks=0)
    with pytest.raises(ValueError, match="max_hold_ticks=0"):
        validate_crash_recovery_config(cfg, _ZeroTTLStrategy())


def testvalidate_crash_recovery_config_non_integer_ttl_warns() -> None:
    """ADV-03: only genuinely non-integer ttl_ticks (e.g., string) triggers the warn path."""
    import warnings as _w
    cfg = CrashRecoveryConfig(enabled=True, max_hold_ticks=300)
    with _w.catch_warnings(record=True) as caught:
        _w.simplefilter("always")
        validate_crash_recovery_config(cfg, _StringTTLStrategy())
    runtime_warnings = [w for w in caught if issubclass(w.category, RuntimeWarning)]
    assert len(runtime_warnings) == 1
    assert "non-integer" in str(runtime_warnings[0].message)
    assert "'300'" in str(runtime_warnings[0].message) or '"300"' in str(runtime_warnings[0].message)


# Review fix COR-01 / REL-01: bookkeeping must run before the log emit so a
# state_log raise leaves state_by_agent and granted_at_tick_by_agent
# consistent. Without this fix, max-hold reclamation silently misses live
# agents whose granted_at_tick slot was never written.

from ccs.core.types import Artifact  # noqa: E402


def test_set_agent_state_failed_log_emit_keeps_bookkeeping_consistent() -> None:
    """COR-01: log emit raise must NOT leave state_by_agent inconsistent with bookkeeping.

    With the fix, bookkeeping runs BEFORE the log emit. So if the log raises:
    - state_by_agent shows the new state (pre-existing semantics — not changed)
    - granted_at_tick_by_agent has the M∪E entry (NEW — was missing before fix)
    - The next sweep can correctly evaluate max-hold for the agent.
    """
    seen = [0]

    def flaky_log(entry):
        seen[0] += 1
        # Fail on every call — first M∪E acquire's log emit raises.
        raise RuntimeError("simulated log emit failure")

    registry = ArtifactRegistry(
        state_log=flaky_log, agent_names=None, instance_id="test-instance"
    )
    artifact = Artifact(name="x", version=1)
    registry.register_artifact(artifact, content="v1")
    agent_id = uuid4()

    with pytest.raises(RuntimeError, match="simulated log emit failure"):
        registry.set_agent_state(
            artifact.id, agent_id, MESIState.EXCLUSIVE, trigger="write", tick=42
        )

    # State mutation survived (pre-existing behavior, unchanged):
    assert registry.get_agent_state(artifact.id, agent_id) == MESIState.EXCLUSIVE
    # Bookkeeping is now CONSISTENT with that state — granted_at_tick is recorded:
    assert registry.granted_at_tick(agent_id, artifact.id) == 42


def test_set_agent_state_failed_log_emit_consistent_for_me_exit_too() -> None:
    """COR-01 mirror case: log raise on M∪E exit still clears the slot."""
    fail_on_call = 3
    seen = [0]

    def flaky_log(entry):
        seen[0] += 1
        if seen[0] >= fail_on_call:
            raise RuntimeError("simulated log emit failure")

    registry = ArtifactRegistry(
        state_log=flaky_log, agent_names=None, instance_id="test-instance"
    )
    artifact = Artifact(name="x", version=1)
    registry.register_artifact(artifact, content="v1")
    agent_id = uuid4()

    # Calls 1 & 2 succeed (acquire EXCLUSIVE, internal step). Call 3 fails.
    registry.set_agent_state(
        artifact.id, agent_id, MESIState.EXCLUSIVE, trigger="write", tick=10
    )
    registry.set_agent_state(
        artifact.id, agent_id, MESIState.MODIFIED, trigger="commit", tick=11
    )
    # granted_at_tick survives the M↔E transition (preserved at acquire).
    assert registry.granted_at_tick(agent_id, artifact.id) == 10

    with pytest.raises(RuntimeError, match="simulated log emit failure"):
        registry.set_agent_state(
            artifact.id, agent_id, MESIState.INVALID, trigger="reclaim", tick=20
        )
    # Bookkeeping cleanup ran even though log emit raised.
    assert registry.get_agent_state(artifact.id, agent_id) == MESIState.INVALID
    assert registry.granted_at_tick(agent_id, artifact.id) is None


# ---- v0.8.3 C-flip deprecation cycle ----------------------------------------
#
# See docs/plans/2026-05-28-001-feat-c-flip-crash-recovery-default-on-plan.md
# Unit 1. The bare CrashRecoveryConfig() construction must emit a one-shot
# DeprecationWarning naming the v0.9.0 default flip. Tests below assert both
# the emission behavior and the silence paths.


import warnings as _warnings  # noqa: E402

from ccs.coordinator import service as _service_module  # noqa: E402


@pytest.fixture
def reset_bare_construction_flag():
    """Reset the module-level emit-once flag between tests.

    Without this, the first test to construct CrashRecoveryConfig() would
    set _BARE_CONSTRUCTION_WARNED=True for the entire pytest session and
    later tests in this class would see zero warnings (false negatives).
    """
    _service_module._BARE_CONSTRUCTION_WARNED = False
    yield
    _service_module._BARE_CONSTRUCTION_WARNED = False


class TestCrashRecoveryDeprecationWarning:
    """Unit 1 — bare CrashRecoveryConfig() emits one-shot DeprecationWarning."""

    def test_bare_construction_emits_deprecation_warning(
        self, reset_bare_construction_flag
    ) -> None:
        """Happy path: bare construction in fresh process emits one warning."""
        with _warnings.catch_warnings(record=True) as caught:
            _warnings.simplefilter("always")
            CrashRecoveryConfig()
        deprecation_warnings = [
            w for w in caught if issubclass(w.category, DeprecationWarning)
        ]
        assert len(deprecation_warnings) == 1

    def test_warning_message_names_both_silence_paths(
        self, reset_bare_construction_flag
    ) -> None:
        """Warning prose must give users BOTH the opt-in and opt-out recipes."""
        with _warnings.catch_warnings(record=True) as caught:
            _warnings.simplefilter("always")
            CrashRecoveryConfig()
        # Length guard before indexing — without it, a regression that drops
        # the warning emission entirely surfaces as IndexError instead of an
        # informative AssertionError, masking the actual failure mode.
        assert len(caught) >= 1, (
            "bare CrashRecoveryConfig() must emit a warning; got 0 warnings"
        )
        msg = str(caught[0].message)
        assert "enabled=True" in msg, "warning must name the recommended opt-in"
        assert "enabled=False" in msg, "warning must name the explicit opt-out"
        assert "v0.9.0" in msg, "warning must name the target release"

    def test_bare_construction_preserves_v0_8_x_default(
        self, reset_bare_construction_flag
    ) -> None:
        """After bare construction, enabled is False (v0.8.x behavior preserved)."""
        with _warnings.catch_warnings():
            _warnings.simplefilter("ignore", DeprecationWarning)
            cfg = CrashRecoveryConfig()
        assert cfg.enabled is False
        assert cfg.heartbeat_timeout_ticks == 10
        assert cfg.max_hold_ticks == 1000

    def test_emit_once_dedupes_consecutive_bare_constructions(
        self, reset_bare_construction_flag
    ) -> None:
        """Module-level flag means two bare constructions emit exactly ONE warning."""
        with _warnings.catch_warnings(record=True) as caught:
            _warnings.simplefilter("always")
            CrashRecoveryConfig()
            CrashRecoveryConfig()
            CrashRecoveryConfig()
        deprecation_warnings = [
            w for w in caught if issubclass(w.category, DeprecationWarning)
        ]
        assert len(deprecation_warnings) == 1, (
            f"expected 1 warning across three bare constructions, "
            f"got {len(deprecation_warnings)}"
        )

    def test_explicit_false_emits_no_warning(
        self, reset_bare_construction_flag
    ) -> None:
        """User passes enabled=False explicitly → no warning, enabled stays False."""
        with _warnings.catch_warnings(record=True) as caught:
            _warnings.simplefilter("always")
            cfg = CrashRecoveryConfig(enabled=False)
        deprecation_warnings = [
            w for w in caught if issubclass(w.category, DeprecationWarning)
        ]
        assert deprecation_warnings == []
        assert cfg.enabled is False

    def test_explicit_true_emits_no_warning(
        self, reset_bare_construction_flag
    ) -> None:
        """User passes enabled=True explicitly → no warning, enabled stays True."""
        with _warnings.catch_warnings(record=True) as caught:
            _warnings.simplefilter("always")
            cfg = CrashRecoveryConfig(enabled=True)
        deprecation_warnings = [
            w for w in caught if issubclass(w.category, DeprecationWarning)
        ]
        assert deprecation_warnings == []
        assert cfg.enabled is True

    def test_composition_validation_unchanged_for_explicit_true(
        self, reset_bare_construction_flag
    ) -> None:
        """R11 composition rule still works on explicit-True configs after Unit 1."""
        cfg = CrashRecoveryConfig(
            enabled=True, heartbeat_timeout_ticks=10, max_hold_ticks=1000
        )
        # Should not raise — 1000 > 300.
        validate_crash_recovery_config(cfg, LeaseStrategy(ttl_ticks=300))

    def test_dataclass_remains_frozen(
        self, reset_bare_construction_flag
    ) -> None:
        """frozen=True invariant — sentinel mechanism must not break immutability."""
        with _warnings.catch_warnings():
            _warnings.simplefilter("ignore", DeprecationWarning)
            cfg = CrashRecoveryConfig()
        # Direct assignment must still raise FrozenInstanceError.
        with pytest.raises(Exception) as exc_info:
            cfg.enabled = True  # type: ignore[misc]
        assert "frozen" in str(exc_info.value).lower() or "FrozenInstanceError" in type(
            exc_info.value
        ).__name__

    # ---- RM-9 Layer 2: logging channel (ce:review AN-1) ---------------------

    def test_bare_construction_logs_warning_on_service_logger(
        self, reset_bare_construction_flag, caplog
    ) -> None:
        """RM-9 Layer 2: bare construction also logs a WARNING on
        ``ccs.coordinator.service`` so the migration signal survives CPython's
        default ``DeprecationWarning`` filter (which hides it for non-__main__
        importers — i.e. virtually every SDK consumer).
        """
        import logging

        with caplog.at_level(logging.WARNING, logger="ccs.coordinator.service"):
            with _warnings.catch_warnings():
                _warnings.simplefilter("ignore", DeprecationWarning)
                CrashRecoveryConfig()
        records = [
            r
            for r in caplog.records
            if r.name == "ccs.coordinator.service" and r.levelno == logging.WARNING
        ]
        assert len(records) == 1, "bare construction must log exactly one WARNING"
        message = records[0].getMessage()
        assert "enabled=True" in message, "log must name the recommended opt-in"
        assert "enabled=False" in message, "log must name the explicit opt-out"
        assert "v0.9.0" in message, "log must name the target release"

    def test_logger_channel_honors_emit_once(
        self, reset_bare_construction_flag, caplog
    ) -> None:
        """The logging channel shares the emit-once gate with warnings.warn:
        three bare constructions log exactly one WARNING."""
        import logging

        with caplog.at_level(logging.WARNING, logger="ccs.coordinator.service"):
            with _warnings.catch_warnings():
                _warnings.simplefilter("ignore", DeprecationWarning)
                CrashRecoveryConfig()
                CrashRecoveryConfig()
                CrashRecoveryConfig()
        records = [
            r
            for r in caplog.records
            if r.name == "ccs.coordinator.service" and r.levelno == logging.WARNING
        ]
        assert len(records) == 1


# ---- ce:review ADV-01 / ADV-03 — skipped-normalization robustness -----------
#
# The sentinel marking an unspecified ``enabled`` is falsy, so any path that
# skips __post_init__ normalization (importlib.reload identity mismatch, or a
# subclass __post_init__ that omits super()) still reads as disabled rather
# than truthy-enabled. Removed in v0.9.0 with the rest of the sentinel
# mechanism.

from ccs.coordinator.service import _DefaultEnabledSentinel  # noqa: E402


class TestSentinelNormalizationRobustness:
    """ADV-01/ADV-03 — skipped normalization must read as disabled, not enabled."""

    def test_sentinel_is_falsy(self) -> None:
        """The unset-enabled sentinel evaluates falsy."""
        assert bool(_service_module._DEFAULT_ENABLED_SENTINEL) is False
        assert not _service_module._DEFAULT_ENABLED_SENTINEL

    def test_subclass_post_init_override_without_super_reads_disabled(self) -> None:
        """ADV-03: a frozen subclass overriding __post_init__ without calling
        super() skips normalization — enabled stays the sentinel, which must be
        falsy so ``if config.enabled:`` does not misfire the sweep.
        """
        from dataclasses import dataclass as _dataclass

        @_dataclass(frozen=True)
        class _NoSuperConfig(CrashRecoveryConfig):
            def __post_init__(self) -> None:  # deliberately omits super()
                pass

        cfg = _NoSuperConfig()
        # enabled is the un-normalized sentinel object, not a real bool ...
        assert cfg.enabled is _service_module._DEFAULT_ENABLED_SENTINEL
        # ... but it reads as disabled, which is what production checks rely on.
        assert bool(cfg.enabled) is False
        assert not cfg.enabled

    def test_reload_identity_mismatch_reads_disabled(self) -> None:
        """ADV-01: simulate importlib.reload rebinding the module sentinel. An
        instance carrying a *different* (pre-reload) sentinel fails the ``is``
        check, skips normalization, and must still read as disabled.
        """
        stale_sentinel = _DefaultEnabledSentinel()  # distinct identity
        with _warnings.catch_warnings(record=True) as caught:
            _warnings.simplefilter("always")
            cfg = CrashRecoveryConfig(enabled=stale_sentinel)  # type: ignore[arg-type]
        # Identity check fails → normalization skipped → enabled keeps the stale
        # sentinel, and no warning fires (the bare path keys on the *current*
        # module sentinel).
        assert cfg.enabled is stale_sentinel
        assert bool(cfg.enabled) is False
        assert [w for w in caught if issubclass(w.category, DeprecationWarning)] == []


# ---- ce:review ADV-02 — concurrent emit-once --------------------------------


class TestConcurrentEmitOnce:
    """ADV-02 — the lock keeps emit-once intact under thread contention."""

    @pytest.mark.filterwarnings("ignore::DeprecationWarning")
    def test_concurrent_bare_construction_emits_one_log(
        self, reset_bare_construction_flag
    ) -> None:
        """Eight threads constructing bare configs simultaneously produce exactly
        one WARNING log. Asserts via the logging channel (thread-safe by design)
        rather than warnings.catch_warnings, which mutates global filter state and
        is itself not thread-safe. The expected DeprecationWarning is filtered at
        the mark (thread-safe) since this test cannot wrap workers in
        catch_warnings; the log assertion is the real contract.
        """
        import logging
        import threading

        captured: list[logging.LogRecord] = []
        capture_lock = threading.Lock()

        class _CountingHandler(logging.Handler):
            def emit(self, record: logging.LogRecord) -> None:
                with capture_lock:
                    captured.append(record)

        svc_logger = logging.getLogger("ccs.coordinator.service")
        handler = _CountingHandler()
        handler.setLevel(logging.WARNING)
        svc_logger.addHandler(handler)
        previous_level = svc_logger.level
        svc_logger.setLevel(logging.WARNING)

        thread_count = 8
        barrier = threading.Barrier(thread_count)

        def worker() -> None:
            barrier.wait()  # release all threads together to maximize contention
            try:
                CrashRecoveryConfig()
            except DeprecationWarning:
                # Under -W error the single emitting thread raises *after* the
                # log record is emitted (logging precedes warnings.warn).
                pass

        try:
            threads = [threading.Thread(target=worker) for _ in range(thread_count)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()
        finally:
            svc_logger.removeHandler(handler)
            svc_logger.setLevel(previous_level)

        warning_records = [r for r in captured if r.levelno == logging.WARNING]
        assert len(warning_records) == 1, (
            f"expected exactly one WARNING across {thread_count} concurrent bare "
            f"constructions, got {len(warning_records)}"
        )
