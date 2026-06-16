# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Tests for in-memory event bus abstraction."""

from __future__ import annotations

from uuid import uuid4

from ccs.bus.event_bus import ArtifactUpdateEvent, InMemoryEventBus
from ccs.core.types import InvalidationSignal


def test_event_bus_delivers_invalidation_and_update_to_subscribed_agents() -> None:
    bus = InMemoryEventBus()
    agent_a = uuid4()
    agent_b = uuid4()
    artifact_id = uuid4()

    invalidations: list[InvalidationSignal] = []
    updates: list[ArtifactUpdateEvent] = []

    bus.subscribe(
        agent_id=agent_a,
        on_invalidation=invalidations.append,
        on_update=updates.append,
    )

    signal = InvalidationSignal(
        artifact_id=artifact_id,
        new_version=2,
        issued_at_tick=5,
        issuer_agent_id=agent_b,
    )
    delivered_invalidations = bus.publish_invalidation(signal, recipients=[agent_a, agent_b])
    assert delivered_invalidations == 1
    assert invalidations == [signal]

    event = ArtifactUpdateEvent(
        artifact_id=artifact_id,
        version=2,
        content="v2",
        issued_at_tick=5,
        issuer_agent_id=agent_b,
    )
    delivered_updates = bus.publish_update(event, recipients=[agent_a, agent_b])
    assert delivered_updates == 1
    assert updates == [event]


def test_event_bus_unsubscribe_stops_delivery() -> None:
    bus = InMemoryEventBus()
    agent_id = uuid4()
    issuer_id = uuid4()
    artifact_id = uuid4()
    received: list[InvalidationSignal] = []
    bus.subscribe(agent_id=agent_id, on_invalidation=received.append)

    bus.unsubscribe(agent_id=agent_id)
    delivered = bus.publish_invalidation(
        InvalidationSignal(
            artifact_id=artifact_id,
            new_version=2,
            issued_at_tick=1,
            issuer_agent_id=issuer_id,
        ),
        recipients=[agent_id],
    )
    assert delivered == 0
    assert received == []
