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
