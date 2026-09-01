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
from pathlib import Path
from uuid import uuid4

import pytest

from ccs.coordinator.registry import ArtifactRegistry
from ccs.coordinator.sqlite_registry import SqliteArtifactRegistry
from ccs.core.states import MESIState
from ccs.core.types import Artifact, ConflictDetail

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


@pytest.fixture(params=["in_memory", "sqlite"])
def registry(request, tmp_path: Path):
    """Both registries: the arbitration contract is now identical (KD1)."""
    if request.param == "in_memory":
        yield ArtifactRegistry()
    else:
        with SqliteArtifactRegistry(tmp_path / "state.db") as reg:
            yield reg


_CAS_ROUNDS = 20
_CAS_WRITERS = 4


def test_concurrent_cas_writers_produce_one_winner_and_typed_conflicts(registry) -> None:
    """N optimistic writers racing one version: exactly one WIN per round.

    The U4 property: ``commit_cas``'s version check, holder check, fence check
    and apply are ONE serialized step. Split them (the pre-U4 shape: legs
    outside the hold, apply inside) and two writers can both pass the version
    check before either applies -- two WINs from one expected_version, a
    version that advances twice, and a lost update. Reverting U4 fails this
    within a few rounds at a 1 us switch interval.

    Losers must get a typed ``ConflictDetail`` back, never an exception, and
    a conflict must leave the version exactly where the winner put it.
    """
    artifact = Artifact(id=uuid4(), name="plan.md", version=1)
    registry.register_artifact(artifact, "v1")
    writers = [uuid4() for _ in range(_CAS_WRITERS)]

    default_switch_interval = sys.getswitchinterval()
    sys.setswitchinterval(1e-6)
    try:
        for round_no in range(_CAS_ROUNDS):
            expected = registry.get_artifact(artifact.id).version
            results: dict[int, object] = {}
            failures: list[BaseException] = []
            start = threading.Barrier(_CAS_WRITERS, timeout=5)

            def _commits(slot: int, agent_id) -> None:
                try:
                    start.wait()
                    results[slot] = registry.commit_cas(
                        artifact.id,
                        agent_id,
                        expected_version=expected,
                        content_hash=f"h{slot}",
                        content=f"round{round_no}-writer{slot}",
                        tick=round_no,
                    )
                except BaseException as exc:  # noqa: BLE001 — reported, not swallowed
                    failures.append(exc)

            threads = [
                threading.Thread(target=_commits, args=(slot, agent), daemon=True)
                for slot, agent in enumerate(writers)
            ]
            for t in threads:
                t.start()
            for t in threads:
                t.join(timeout=10)
                assert not t.is_alive(), f"round {round_no}: a writer never finished"

            assert not failures, f"round {round_no}: a writer raised: {failures[0]!r}"
            wins = [r for r in results.values() if isinstance(r, tuple)]
            conflicts = [r for r in results.values() if isinstance(r, ConflictDetail)]
            assert len(wins) == 1, (
                f"round {round_no}: {len(wins)} writers won the same "
                f"expected_version={expected} — the arbitration legs and the "
                f"apply are not one serialized step"
            )
            assert len(conflicts) == _CAS_WRITERS - 1
            assert all(c.reason == "version_mismatch" for c in conflicts)
            # The version moved exactly once: to the winner's next_version.
            (updated, _invalidated) = wins[0]
            assert updated.version == expected + 1
            assert registry.get_artifact(artifact.id).version == expected + 1
    finally:
        sys.setswitchinterval(default_switch_interval)
