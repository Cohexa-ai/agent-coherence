# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents
"""The in-memory registry's concurrency contract: thread-safe, same as sqlite.

Companion to the plan's U2/U6: behavioral evidence here (concurrent writers
observe serialized effects), structural coverage (every public member holds
the lock) added by U6. The sqlite registry has carried this contract since it
existed; these tests are what retires the in-memory "single-threaded by
contract" era.
"""

from __future__ import annotations

import sys
import threading
from uuid import uuid4

from ccs.coordinator.registry import ArtifactRegistry
from ccs.core.states import MESIState
from ccs.core.types import Artifact

_WRITERS = 4
_CALLS_PER_WRITER = 400


def test_state_log_sequence_is_gapless_under_concurrent_writers() -> None:
    """N threads writing states produce sequence numbers 1..N*calls exactly.

    ``set_agent_state`` does ``self._seq += 1`` then emits; without the
    registry lock that read-modify-write interleaves and two writers reserve
    the same number (a duplicate) or roll back over each other's reservation
    (a gap). U2's mutant: revert the lock widening and this fails within a
    few hundred calls at a 1 us switch interval.
    """
    emitted: list[int] = []
    reg = ArtifactRegistry(
        state_log=lambda entry: emitted.append(entry["sequence_number"]),
        instance_id="lock-coverage-test",
    )
    artifact = Artifact(id=uuid4(), name="plan.md", version=1)
    reg.register_artifact(artifact, "v1")
    agents = [uuid4() for _ in range(_WRITERS)]

    start = threading.Barrier(_WRITERS, timeout=5)
    failures: list[BaseException] = []

    def _writes(agent_id) -> None:
        try:
            start.wait()
            for tick in range(_CALLS_PER_WRITER):
                reg.set_agent_state(
                    artifact.id, agent_id, MESIState.SHARED, trigger="fetch", tick=tick
                )
        except BaseException as exc:  # noqa: BLE001 — reported, not swallowed
            failures.append(exc)

    default_switch_interval = sys.getswitchinterval()
    sys.setswitchinterval(1e-6)
    try:
        threads = [threading.Thread(target=_writes, args=(a,), daemon=True) for a in agents]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)
            assert not t.is_alive(), "a writer never finished"
    finally:
        sys.setswitchinterval(default_switch_interval)

    assert not failures, f"a writer raised: {failures[0]!r}"
    total = _WRITERS * _CALLS_PER_WRITER
    assert sorted(emitted) == list(range(1, total + 1)), (
        f"sequence numbers are not gapless 1..{total}: "
        f"{len(emitted)} emitted, {len(set(emitted))} distinct, "
        f"min={min(emitted)}, max={max(emitted)}"
    )
