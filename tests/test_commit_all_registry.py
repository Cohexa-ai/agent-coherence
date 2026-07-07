# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents
"""Registry-level tests for the SB-18 atomic multi-artifact publish (commit_all).

All-or-nothing: either every member of the write-set advances or none do, and a
partial batch is never observable. These exercise the in-memory
``ArtifactRegistry.commit_all`` directly.
"""
from __future__ import annotations

from uuid import uuid4

import pytest

from ccs.coordinator.registry import ArtifactRegistry
from ccs.core.states import MESIState
from ccs.core.types import (
    Artifact,
    CasCorruption,
    CommitAllEntry,
    MultiCommitConflict,
    MultiCommitResult,
)


def _reg_with(*names_versions):
    reg = ArtifactRegistry()
    arts = []
    for name, ver in names_versions:
        a = Artifact(id=uuid4(), name=name, version=ver, content_hash="h")
        reg.register_artifact(a, content=f"{name}-v{ver}")
        arts.append(a)
    return reg, arts


def test_commit_all_win_bumps_every_member():
    reg, (a, b) = _reg_with(("a.md", 1), ("b.md", 1))
    writer = uuid4()
    reg.set_agent_state(a.id, writer, MESIState.SHARED, tick=1)
    reg.set_agent_state(b.id, writer, MESIState.SHARED, tick=1)
    result = reg.commit_all(
        writer,
        {
            a.id: CommitAllEntry(expected_version=1, content_hash="ha2", content="a-v2"),
            b.id: CommitAllEntry(expected_version=1, content_hash="hb2", content="b-v2"),
        },
        tick=2,
    )
    assert isinstance(result, MultiCommitResult)
    assert result.versions == {a.id: 2, b.id: 2}
    assert reg.get_artifact(a.id).version == 2
    assert reg.get_artifact(b.id).version == 2


def test_commit_all_one_drifted_holds_whole_batch_zero_mutation():
    reg, (a, b) = _reg_with(("a.md", 1), ("b.md", 1))
    writer = uuid4()
    reg.set_agent_state(a.id, writer, MESIState.SHARED, tick=1)
    reg.set_agent_state(b.id, writer, MESIState.SHARED, tick=1)
    # a peer advances b under the writer's feet -> the writer's expected_version=1 is stale.
    peer = uuid4()
    reg.set_agent_state(b.id, peer, MESIState.SHARED, tick=1)
    reg.commit_cas(b.id, peer, expected_version=1, content_hash="hb2", content="b-v2", tick=1)
    assert reg.get_artifact(b.id).version == 2

    result = reg.commit_all(
        writer,
        {
            a.id: CommitAllEntry(expected_version=1, content_hash="ha2", content="a-v2"),
            b.id: CommitAllEntry(expected_version=1, content_hash="hb-x", content="b-x"),
        },
        tick=3,
    )
    assert isinstance(result, MultiCommitConflict)
    assert set(result.per_artifact) == {b.id}
    assert result.per_artifact[b.id].reason == "version_mismatch"
    assert result.per_artifact[b.id].current_version == 2
    # ALL-OR-NOTHING: the passing member a was NOT mutated.
    assert reg.get_artifact(a.id).version == 1


def test_commit_all_other_holder_holds_whole_batch():
    reg, (a, b) = _reg_with(("a.md", 1), ("b.md", 1))
    writer, peer = uuid4(), uuid4()
    reg.set_agent_state(a.id, writer, MESIState.SHARED, tick=1)
    reg.set_agent_state(b.id, writer, MESIState.SHARED, tick=1)
    reg.set_agent_state(b.id, peer, MESIState.EXCLUSIVE, tick=1)  # peer holds M/E on b
    result = reg.commit_all(
        writer,
        {
            a.id: CommitAllEntry(expected_version=1, content_hash="ha2"),
            b.id: CommitAllEntry(expected_version=1, content_hash="hb2"),
        },
        tick=2,
    )
    assert isinstance(result, MultiCommitConflict)
    assert result.per_artifact[b.id].reason == "other_holder"
    assert reg.get_artifact(a.id).version == 1  # all-or-nothing


def test_commit_all_corruption_aborts_batch():
    reg, (a,) = _reg_with(("a.md", 1))
    writer = uuid4()
    reg.set_agent_state(a.id, writer, MESIState.SHARED, tick=1)
    result = reg.commit_all(
        writer,
        {a.id: CommitAllEntry(expected_version=5, content_hash="h")},  # expected > current
        tick=2,
    )
    assert isinstance(result, CasCorruption)
    assert reg.get_artifact(a.id).version == 1


def test_commit_all_empty_write_set_raises():
    reg, _ = _reg_with(("a.md", 1))
    with pytest.raises(ValueError):
        reg.commit_all(uuid4(), {}, tick=1)


def test_commit_all_singleton_matches_commit_cas():
    reg, (a,) = _reg_with(("a.md", 1))
    writer = uuid4()
    reg.set_agent_state(a.id, writer, MESIState.SHARED, tick=1)
    result = reg.commit_all(
        writer,
        {a.id: CommitAllEntry(expected_version=1, content_hash="h2", content="v2")},
        tick=2,
    )
    assert isinstance(result, MultiCommitResult)
    assert result.versions == {a.id: 2}
    assert reg.get_artifact(a.id).version == 2


def test_commit_all_win_invalidates_peers_of_every_member():
    reg, (a, b) = _reg_with(("a.md", 1), ("b.md", 1))
    writer, peer = uuid4(), uuid4()
    reg.set_agent_state(a.id, writer, MESIState.SHARED, tick=1)
    reg.set_agent_state(b.id, writer, MESIState.SHARED, tick=1)
    reg.set_agent_state(a.id, peer, MESIState.SHARED, tick=1)  # peer caches a
    result = reg.commit_all(
        writer,
        {
            a.id: CommitAllEntry(expected_version=1, content_hash="ha2", content="a-v2"),
            b.id: CommitAllEntry(expected_version=1, content_hash="hb2", content="b-v2"),
        },
        tick=2,
    )
    assert isinstance(result, MultiCommitResult)
    assert peer in result.invalidated
    assert reg.get_agent_state(a.id, peer) == MESIState.INVALID
