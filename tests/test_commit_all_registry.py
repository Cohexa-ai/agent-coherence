# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents
"""Registry-level tests for the SB-18 atomic multi-artifact publish (commit_all).

All-or-nothing: either every member of the write-set advances or none do, and a
partial batch is never observable. Parametrized over BOTH registry backends
(in-memory + sqlite) so the two produce identical outcomes (backend parity).
"""
from __future__ import annotations

from uuid import uuid4

import pytest

from ccs.coordinator.registry import ArtifactRegistry
from ccs.coordinator.sqlite_registry import SqliteArtifactRegistry
from ccs.core.states import MESIState
from ccs.core.types import (
    Artifact,
    CasCorruption,
    CommitAllEntry,
    MultiCommitConflict,
    MultiCommitResult,
)


@pytest.fixture(params=["memory", "sqlite"])
def registry(request, tmp_path):
    if request.param == "memory":
        yield ArtifactRegistry()
    else:
        with SqliteArtifactRegistry(tmp_path / "state.db") as reg:
            yield reg


def _register(reg, name, version):
    a = Artifact(id=uuid4(), name=name, version=version, content_hash="h")
    reg.register_artifact(a, content=f"{name}-v{version}")
    return a


def test_commit_all_win_bumps_every_member(registry):
    a = _register(registry, "a.md", 1)
    b = _register(registry, "b.md", 1)
    writer = uuid4()
    registry.set_agent_state(a.id, writer, MESIState.SHARED, tick=1)
    registry.set_agent_state(b.id, writer, MESIState.SHARED, tick=1)
    result = registry.commit_all(
        writer,
        {
            a.id: CommitAllEntry(expected_version=1, content_hash="ha2", content="a-v2"),
            b.id: CommitAllEntry(expected_version=1, content_hash="hb2", content="b-v2"),
        },
        tick=2,
    )
    assert isinstance(result, MultiCommitResult)
    assert result.versions == {a.id: 2, b.id: 2}
    assert registry.get_artifact(a.id).version == 2
    assert registry.get_artifact(b.id).version == 2


def test_commit_all_one_drifted_holds_whole_batch_zero_mutation(registry):
    a = _register(registry, "a.md", 1)
    b = _register(registry, "b.md", 1)
    writer = uuid4()
    registry.set_agent_state(a.id, writer, MESIState.SHARED, tick=1)
    registry.set_agent_state(b.id, writer, MESIState.SHARED, tick=1)
    # a peer advances b under the writer's feet -> the writer's expected_version=1 is stale.
    peer = uuid4()
    registry.set_agent_state(b.id, peer, MESIState.SHARED, tick=1)
    registry.commit_cas(b.id, peer, expected_version=1, content_hash="hb2", content="b-v2", tick=1)
    assert registry.get_artifact(b.id).version == 2

    result = registry.commit_all(
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
    assert registry.get_artifact(a.id).version == 1


def test_commit_all_other_holder_holds_whole_batch(registry):
    a = _register(registry, "a.md", 1)
    b = _register(registry, "b.md", 1)
    writer, peer = uuid4(), uuid4()
    registry.set_agent_state(a.id, writer, MESIState.SHARED, tick=1)
    registry.set_agent_state(b.id, writer, MESIState.SHARED, tick=1)
    registry.set_agent_state(b.id, peer, MESIState.EXCLUSIVE, tick=1)  # peer holds M/E on b
    result = registry.commit_all(
        writer,
        {
            a.id: CommitAllEntry(expected_version=1, content_hash="ha2"),
            b.id: CommitAllEntry(expected_version=1, content_hash="hb2"),
        },
        tick=2,
    )
    assert isinstance(result, MultiCommitConflict)
    assert result.per_artifact[b.id].reason == "other_holder"
    assert registry.get_artifact(a.id).version == 1  # all-or-nothing


def test_commit_all_corruption_aborts_batch(registry):
    a = _register(registry, "a.md", 1)
    writer = uuid4()
    registry.set_agent_state(a.id, writer, MESIState.SHARED, tick=1)
    result = registry.commit_all(
        writer,
        {a.id: CommitAllEntry(expected_version=5, content_hash="h")},  # expected > current
        tick=2,
    )
    assert isinstance(result, CasCorruption)
    assert registry.get_artifact(a.id).version == 1


def test_commit_all_empty_write_set_raises(registry):
    with pytest.raises(ValueError):
        registry.commit_all(uuid4(), {}, tick=1)


def test_commit_all_singleton_matches_commit_cas(registry):
    a = _register(registry, "a.md", 1)
    writer = uuid4()
    registry.set_agent_state(a.id, writer, MESIState.SHARED, tick=1)
    result = registry.commit_all(
        writer,
        {a.id: CommitAllEntry(expected_version=1, content_hash="h2", content="v2")},
        tick=2,
    )
    assert isinstance(result, MultiCommitResult)
    assert result.versions == {a.id: 2}
    assert registry.get_artifact(a.id).version == 2


def test_commit_all_win_invalidates_peers_of_every_member(registry):
    a = _register(registry, "a.md", 1)
    b = _register(registry, "b.md", 1)
    writer, peer = uuid4(), uuid4()
    registry.set_agent_state(a.id, writer, MESIState.SHARED, tick=1)
    registry.set_agent_state(b.id, writer, MESIState.SHARED, tick=1)
    registry.set_agent_state(a.id, peer, MESIState.SHARED, tick=1)  # peer caches a
    result = registry.commit_all(
        writer,
        {
            a.id: CommitAllEntry(expected_version=1, content_hash="ha2", content="a-v2"),
            b.id: CommitAllEntry(expected_version=1, content_hash="hb2", content="b-v2"),
        },
        tick=2,
    )
    assert isinstance(result, MultiCommitResult)
    # per-artifact invalidation: the peer cached `a`, so it is invalidated for a.id
    assert peer in result.invalidated[a.id]
    assert registry.get_agent_state(a.id, peer) == MESIState.INVALID
