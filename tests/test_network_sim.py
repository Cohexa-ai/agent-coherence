# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Unit tests for network simulation transport."""

from __future__ import annotations

import random
from uuid import uuid4

from ccs.transport.network_sim import NetworkSimulator


def test_network_deliver_due_and_pending_count() -> None:
    rng = random.Random(42)
    network = NetworkSimulator(latency_ticks=3, message_loss_rate=0.0, rng=rng)
    dst = uuid4()
    msg = network.send("payload", None, dst, current_tick=5, message_type="invalidation")

    assert msg is not None
    assert network.pending_count == 1
    assert network.message_overhead == 1

    assert network.deliver_due(7) == []
    delivered = network.deliver_due(8)
    assert len(delivered) == 1
    assert delivered[0].destination == dst
    assert delivered[0].delivered is True
    assert network.pending_count == 0


def test_network_message_loss() -> None:
    rng = random.Random(1)
    network = NetworkSimulator(latency_ticks=1, message_loss_rate=1e-12, rng=rng)
    # With near-zero loss the first send should still succeed.
    delivered = network.send("payload", None, uuid4(), current_tick=0, message_type="fetch")
    assert delivered is not None

    always_drop = NetworkSimulator(latency_ticks=1, message_loss_rate=0.999999, rng=random.Random(0))
    # Extremely high loss should drop at least one message deterministically with this seed.
    dropped = always_drop.send("payload", None, uuid4(), current_tick=0, message_type="fetch")
    assert dropped is None
    assert always_drop.pending_count == 0
    assert always_drop.message_overhead == 1

