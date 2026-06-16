# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Unit tests for core artifact dataclasses."""

from __future__ import annotations

from uuid import uuid4

from ccs.core.states import MESIState, TransientState
from ccs.core.types import Artifact, ArtifactCacheEntry, FetchRequest, FetchResponse, InvalidationSignal


def test_artifact_defaults() -> None:
    artifact = Artifact(name="plan.md", version=1)
    assert artifact.name == "plan.md"
    assert artifact.version == 1
    assert artifact.depends_on == ()


def test_cache_entry_fields() -> None:
    artifact_id = uuid4()
    entry = ArtifactCacheEntry(
        artifact_id=artifact_id,
        state=MESIState.SHARED,
        local_version=2,
        transient_state=TransientState.ISG,
        transient_entered_tick=5,
    )
    assert entry.artifact_id == artifact_id
    assert entry.transient_state == TransientState.ISG
    assert entry.local_version == 2


def test_signals_and_fetch_dtos() -> None:
    artifact_id = uuid4()
    agent_id = uuid4()
    signal = InvalidationSignal(
        artifact_id=artifact_id,
        new_version=3,
        issued_at_tick=10,
        issuer_agent_id=agent_id,
    )
    req = FetchRequest(
        artifact_id=artifact_id,
        requesting_agent_id=agent_id,
        requested_at_tick=11,
    )
    resp = FetchResponse(
        artifact_id=artifact_id,
        version=3,
        content="payload",
        state_grant=MESIState.SHARED,
    )

    assert signal.new_version == 3
    assert req.requested_at_tick == 11
    assert resp.state_grant == MESIState.SHARED

