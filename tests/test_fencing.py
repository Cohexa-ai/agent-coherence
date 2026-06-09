"""Read-generation fence (Piece #2) -- dual-registry parity harness (R4b).

A NET-NEW parametrized harness (none existed -- the closest precedent,
test_occ_commit_cas.py, tests each registry in separate functions) that runs the
SAME fence assertions against BOTH ArtifactRegistry (in-memory) and
SqliteArtifactRegistry. Asserts on version / owner_generation, never on content
bytes (sqlite keeps no content per KTD-13). The two registries share no base
class, so this is the guard against silent divergence -- including the
duplicated RECLAIM_TRIGGERS constant. Uses a fixed-stale-buffer arm (a reclaimed
holder's captured read_generation), not a refetch-safe read->+1->write arm.
"""

from __future__ import annotations

from pathlib import Path
from uuid import uuid4

import pytest

from ccs.coordinator.registry import ArtifactRegistry
from ccs.coordinator.sqlite_registry import SqliteArtifactRegistry
from ccs.core.exceptions import StaleReadGeneration
from ccs.core.states import MESIState
from ccs.core.types import Artifact, ConflictDetail


@pytest.fixture(params=["in_memory", "sqlite"])
def registry(request, tmp_path: Path):
    """Yield each registry implementation in turn so every test runs against
    both, identically."""
    if request.param == "in_memory":
        yield ArtifactRegistry()
    else:
        with SqliteArtifactRegistry(tmp_path / "state.db") as reg:
            yield reg


def _register(reg) -> Artifact:
    art = Artifact(id=uuid4(), name="plan.md", version=1, content_hash="h")
    reg.register_artifact(art, content="ignored")
    return art


def test_parity_owner_generation_bumps_on_reclaim_only(registry) -> None:
    reg = registry
    art = _register(reg)
    a, b = uuid4(), uuid4()
    assert reg.get_owner_generation(art.id) == 0
    reg.set_agent_state(art.id, a, MESIState.EXCLUSIVE, trigger="write", tick=1)
    reg.set_agent_state(art.id, a, MESIState.INVALID, trigger="reclaim_heartbeat", tick=10)
    assert reg.get_owner_generation(art.id) == 1
    # A peer-invalidation (non-reclaim trigger) does NOT bump.
    reg.set_agent_state(art.id, b, MESIState.EXCLUSIVE, trigger="write", tick=11)
    reg.set_agent_state(art.id, b, MESIState.INVALID, trigger="peer_invalidation", tick=12)
    assert reg.get_owner_generation(art.id) == 1


def test_parity_read_generation_captured_at_claim(registry) -> None:
    reg = registry
    art = _register(reg)
    a, b, c = uuid4(), uuid4(), uuid4()
    assert reg.get_read_generation(art.id, c) is None  # never claimed
    reg.set_agent_state(art.id, a, MESIState.EXCLUSIVE, trigger="write", tick=1)  # acquire
    assert reg.get_read_generation(art.id, a) == 0
    reg.set_agent_state(art.id, b, MESIState.SHARED, trigger="fetch", tick=2)  # fetch read
    assert reg.get_read_generation(art.id, b) == 0
    # A reclaim preserves a's captured value (it is NOT refreshed).
    reg.set_agent_state(art.id, a, MESIState.INVALID, trigger="reclaim_heartbeat", tick=10)
    assert reg.get_read_generation(art.id, a) == 0


def test_parity_commit_cas_fence_rejects_superseded_reader(registry) -> None:
    reg = registry
    art = _register(reg)
    a = uuid4()
    reg.set_agent_state(art.id, a, MESIState.EXCLUSIVE, trigger="write", tick=1)  # read_gen 0
    reg.set_agent_state(art.id, a, MESIState.INVALID, trigger="reclaim_heartbeat", tick=10)  # owner_gen 1
    # Fixed-stale-buffer arm: a's captured read_generation (0) < owner (1).
    res = reg.commit_cas(art.id, a, expected_version=1, content_hash="new")
    assert isinstance(res, ConflictDetail)
    assert res.reason == "stale_read_generation"
    assert reg.get_artifact(art.id).version == 1  # no phantom bump


def test_parity_pessimistic_fence_rejects_superseded_committer(registry) -> None:
    reg = registry
    art = _register(reg)
    a = uuid4()
    reg.set_agent_state(art.id, a, MESIState.EXCLUSIVE, trigger="write", tick=1)
    reg.set_agent_state(art.id, a, MESIState.INVALID, trigger="reclaim_heartbeat", tick=10)
    stale = Artifact(id=art.id, name="plan.md", version=2, content_hash="stale")
    with pytest.raises(StaleReadGeneration):
        reg.set_artifact_and_content(art.id, stale, "x", last_writer=a, fence_agent_id=a)
    assert reg.get_artifact(art.id).version == 1


def test_reclaim_trigger_constants_are_equal_across_registries() -> None:
    """The RECLAIM_TRIGGERS constant is duplicated (the registries share no base
    class); this pins the two copies equal so the bump can never diverge."""
    from ccs.coordinator.registry import RECLAIM_TRIGGERS as IN_MEMORY
    from ccs.coordinator.sqlite_registry import RECLAIM_TRIGGERS as SQLITE

    assert IN_MEMORY == SQLITE == frozenset({"reclaim_heartbeat", "reclaim_max_hold"})
