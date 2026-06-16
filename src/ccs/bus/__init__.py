# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Event channel abstractions for invalidation/update propagation."""

from .event_bus import ArtifactUpdateEvent, InMemoryEventBus

__all__ = ["InMemoryEventBus", "ArtifactUpdateEvent"]
