# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""Tests for agent runtime protocol flows."""

from __future__ import annotations

from uuid import uuid4

from ccs.agent.runtime import AgentRuntime
from ccs.coordinator.registry import ArtifactRegistry
from ccs.coordinator.service import CoordinatorService
from ccs.core.states import MESIState
from ccs.core.types import InvalidationSignal
from ccs.strategies.lazy import LazyStrategy


def _runtime(coordinator: CoordinatorService | None = None) -> AgentRuntime:
    service = coordinator if coordinator is not None else CoordinatorService(ArtifactRegistry())
    return AgentRuntime(
        agent_id=uuid4(),
        coordinator=service,
        strategy=LazyStrategy(),
    )


def test_read_fetches_then_uses_local_cache() -> None:
    coordinator = CoordinatorService(ArtifactRegistry())
    artifact = coordinator.register_artifact(name="plan.md", content="v1")
    runtime = _runtime(coordinator)

    first = runtime.read(artifact.id, now_tick=1)
    second = runtime.read(artifact.id, now_tick=2)

    assert first.version == 1
    assert second.version == 1
    entry = runtime.cache.get(artifact.id)
    assert entry is not None
    assert entry.access_count == 1


def test_write_updates_version_and_marks_local_modified() -> None:
    coordinator = CoordinatorService(ArtifactRegistry())
    artifact = coordinator.register_artifact(name="plan.md", content="v1")
    runtime_a = _runtime(coordinator)
    runtime_b = _runtime(coordinator)
    runtime_a.read(artifact.id, now_tick=1)
    runtime_b.read(artifact.id, now_tick=1)

    updated, signals = runtime_a.write(artifact.id, content="v2", now_tick=2)

    assert updated.version == 2
    entry = runtime_a.cache.get(artifact.id)
    assert entry is not None
    assert entry.state == MESIState.MODIFIED
    assert runtime_a.content(artifact.id) == "v2"
    assert len(signals) >= 1


def test_handle_invalidation_marks_cache_invalid() -> None:
    coordinator = CoordinatorService(ArtifactRegistry())
    artifact = coordinator.register_artifact(name="plan.md", content="v1")
    runtime = _runtime(coordinator)
    runtime.read(artifact.id, now_tick=1)
    signal = InvalidationSignal(
        artifact_id=artifact.id,
        new_version=2,
        issued_at_tick=7,
        issuer_agent_id=uuid4(),
    )

    runtime.handle_invalidation(signal)

    entry = runtime.cache.get(artifact.id)
    assert entry is not None
    assert entry.state == MESIState.INVALID
    assert entry.local_version == 1


def test_handle_update_sets_shared_and_content() -> None:
    coordinator = CoordinatorService(ArtifactRegistry())
    artifact = coordinator.register_artifact(name="plan.md", content="v1")
    writer = uuid4()
    runtime = _runtime(coordinator)

    runtime.handle_update(
        artifact_id=artifact.id,
        version=3,
        content="v3",
        now_tick=8,
        writer_agent_id=writer,
    )

    entry = runtime.cache.get(artifact.id)
    assert entry is not None
    assert entry.state == MESIState.SHARED
    assert entry.local_version == 3
    assert runtime.content(artifact.id) == "v3"


def test_invalidate_all_cache_clears_entries_without_coordinator_call() -> None:
    coordinator = CoordinatorService(ArtifactRegistry())
    a1 = coordinator.register_artifact(name="a.md", content="v1")
    a2 = coordinator.register_artifact(name="b.md", content="v1")
    runtime = _runtime(coordinator)
    runtime.read(a1.id, now_tick=1)
    runtime.read(a2.id, now_tick=1)

    call_count = 0
    orig = coordinator.record_heartbeat

    def spy(**kwargs: object) -> None:
        nonlocal call_count
        call_count += 1
        orig(**kwargs)

    coordinator.record_heartbeat = spy  # type: ignore[assignment]

    runtime.invalidate_all_cache()

    assert runtime.cache.get(a1.id).state == MESIState.INVALID
    assert runtime.cache.get(a2.id).state == MESIState.INVALID
    assert call_count == 0
