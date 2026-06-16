# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Tests for transient timeout fail-safe behavior."""

from __future__ import annotations

from uuid import uuid4

from ccs.coordinator.registry import ArtifactRegistry
from ccs.coordinator.service import CoordinatorService
from ccs.core.states import MESIState, TransientState
from ccs.core.types import FetchRequest


def _service() -> CoordinatorService:
    return CoordinatorService(ArtifactRegistry())


def test_transient_timeout_forces_invalid_and_clears_transient() -> None:
    svc = _service()
    artifact = svc.register_artifact(name="plan.md", content="v1")
    owner = uuid4()
    peer = uuid4()
    svc.fetch(FetchRequest(artifact_id=artifact.id, requesting_agent_id=owner, requested_at_tick=1))
    svc.fetch(FetchRequest(artifact_id=artifact.id, requesting_agent_id=peer, requested_at_tick=2))

    svc.write(agent_id=owner, artifact_id=artifact.id, issued_at_tick=3)
    assert svc.registry.get_agent_transient(artifact.id, peer) == TransientState.SIA

    expired = svc.enforce_transient_timeouts(current_tick=6, timeout_ticks=2)

    assert expired == 1
    assert svc.registry.get_agent_transient(artifact.id, peer) is None
    assert svc.registry.get_agent_state(artifact.id, peer) == MESIState.INVALID


def test_transient_timeout_does_not_expire_early() -> None:
    svc = _service()
    artifact = svc.register_artifact(name="plan.md", content="v1")
    owner = uuid4()
    peer = uuid4()
    svc.fetch(FetchRequest(artifact_id=artifact.id, requesting_agent_id=owner, requested_at_tick=1))
    svc.fetch(FetchRequest(artifact_id=artifact.id, requesting_agent_id=peer, requested_at_tick=2))
    svc.write(agent_id=owner, artifact_id=artifact.id, issued_at_tick=3)

    expired = svc.enforce_transient_timeouts(current_tick=4, timeout_ticks=2)

    assert expired == 0
    assert svc.registry.get_agent_transient(artifact.id, peer) == TransientState.SIA
