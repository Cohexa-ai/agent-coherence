# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Tests for agent local cache behavior."""

from __future__ import annotations

from uuid import uuid4

from ccs.agent.cache import ArtifactCache
from ccs.core.states import MESIState
from ccs.core.types import ArtifactCacheEntry


def test_cache_get_put_and_has_valid() -> None:
    cache = ArtifactCache()
    artifact_id = uuid4()
    assert cache.get(artifact_id) is None
    assert cache.has_valid(artifact_id) is False

    entry = ArtifactCacheEntry(
        artifact_id=artifact_id,
        state=MESIState.SHARED,
        local_version=3,
    )
    cache.put(artifact_id, entry)
    assert cache.get(artifact_id) == entry
    assert cache.has_valid(artifact_id) is True


def test_invalidate_existing_entry() -> None:
    cache = ArtifactCache()
    artifact_id = uuid4()
    cache.put(
        artifact_id,
        ArtifactCacheEntry(
            artifact_id=artifact_id,
            state=MESIState.EXCLUSIVE,
            local_version=7,
        ),
    )

    cache.invalidate(artifact_id, invalidated_version=5, issued_at_tick=10)
    invalid = cache.get(artifact_id)
    assert invalid is not None
    assert invalid.state == MESIState.INVALID
    assert invalid.local_version == 5
    assert cache.has_valid(artifact_id) is False


def test_invalidate_missing_entry_creates_placeholder() -> None:
    cache = ArtifactCache()
    artifact_id = uuid4()
    cache.invalidate(artifact_id, invalidated_version=2, issued_at_tick=4)

    invalid = cache.get(artifact_id)
    assert invalid is not None
    assert invalid.state == MESIState.INVALID
    assert invalid.local_version == 2
    assert invalid.acquired_at_tick == 4


# --- invalidate_all tests ---


def test_invalidate_all_transitions_all_entries_to_invalid() -> None:
    cache = ArtifactCache()
    ids = [uuid4() for _ in range(3)]
    states = [MESIState.MODIFIED, MESIState.EXCLUSIVE, MESIState.SHARED]
    for aid, state in zip(ids, states):
        cache.put(aid, ArtifactCacheEntry(artifact_id=aid, state=state, local_version=5))

    cache.invalidate_all()

    for aid in ids:
        entry = cache.get(aid)
        assert entry is not None
        assert entry.state == MESIState.INVALID
        assert entry.local_version == 5


def test_invalidate_all_with_version_clamps_each_entry() -> None:
    cache = ArtifactCache()
    ids = [uuid4() for _ in range(3)]
    for i, aid in enumerate(ids):
        cache.put(aid, ArtifactCacheEntry(artifact_id=aid, state=MESIState.EXCLUSIVE, local_version=i + 1))

    cache.invalidate_all(invalidated_version=2)

    assert cache.get(ids[0]).local_version == 1  # min(1, 2) = 1
    assert cache.get(ids[1]).local_version == 2  # min(2, 2) = 2
    assert cache.get(ids[2]).local_version == 2  # min(3, 2) = 2
    for aid in ids:
        assert cache.get(aid).state == MESIState.INVALID


def test_invalidate_all_empty_cache_is_noop() -> None:
    cache = ArtifactCache()
    cache.invalidate_all()
    assert cache.entries() == {}


def test_invalidate_all_already_invalid_entry_idempotent() -> None:
    cache = ArtifactCache()
    aid = uuid4()
    cache.put(aid, ArtifactCacheEntry(artifact_id=aid, state=MESIState.INVALID, local_version=3))

    cache.invalidate_all()

    entry = cache.get(aid)
    assert entry.state == MESIState.INVALID
    assert entry.local_version == 3


def test_invalidate_all_does_not_create_placeholders() -> None:
    cache = ArtifactCache()
    aid = uuid4()
    cache.put(aid, ArtifactCacheEntry(artifact_id=aid, state=MESIState.SHARED, local_version=1))

    cache.invalidate_all()

    unknown = uuid4()
    assert cache.get(unknown) is None
