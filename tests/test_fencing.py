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
    # The OTHER reclaim trigger (max-hold) bumps too — both sweep triggers
    # behave identically on both registries.
    reg.set_agent_state(art.id, b, MESIState.EXCLUSIVE, trigger="write", tick=13)
    reg.set_agent_state(art.id, b, MESIState.INVALID, trigger="reclaim_max_hold", tick=20)
    assert reg.get_owner_generation(art.id) == 2


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
    """The RECLAIM_TRIGGERS / CLAIM_CAPTURE_TRIGGERS constants are duplicated
    (the registries share no base class); this pins the copies equal so the
    bump and the capture can never silently diverge. The capture pin also
    guards the service.fetch() trigger string: a rename there without updating
    the constants would silently disable read-generation capture on fetches."""
    from ccs.coordinator.registry import CLAIM_CAPTURE_TRIGGERS as MEM_CAPTURE
    from ccs.coordinator.registry import RECLAIM_TRIGGERS as IN_MEMORY
    from ccs.coordinator.sqlite_registry import CLAIM_CAPTURE_TRIGGERS as SQL_CAPTURE
    from ccs.coordinator.sqlite_registry import RECLAIM_TRIGGERS as SQLITE

    assert IN_MEMORY == SQLITE == frozenset(
        {"reclaim_heartbeat", "reclaim_max_hold", "timeout"}
    )
    assert MEM_CAPTURE == SQL_CAPTURE == frozenset({"fetch"})


def test_transient_timeout_eviction_bumps_and_fences(registry) -> None:
    """Review gated-fix regression: the transient-timeout fail-safe
    (enforce_transient_timeouts -> trigger="timeout") is a coordinator-side
    eviction too — it must bump owner_generation so a post-eviction zombie
    commit_cas is rejected by the fence (the version never moved, so
    version-CAS alone would have admitted it)."""
    reg = registry
    art = _register(reg)
    a = uuid4()
    reg.set_agent_state(art.id, a, MESIState.EXCLUSIVE, trigger="write", tick=1)  # claim g0
    # Transient-timeout fail-safe evicts the M/E holder.
    reg.set_agent_state(art.id, a, MESIState.INVALID, trigger="timeout", tick=10)
    assert reg.get_owner_generation(art.id) == 1
    # The zombie's commit_cas (version unchanged, no other holder, stale claim)
    # is rejected — previously this was admitted via equality on gen 0.
    res = reg.commit_cas(art.id, a, expected_version=1, content_hash="zombie")
    assert isinstance(res, ConflictDetail)
    assert res.reason == "stale_read_generation"
    assert reg.get_artifact(art.id).version == 1


def test_commit_fence_raise_clears_mwb_transient(registry) -> None:
    """Regression (review P1): a fence reject inside service.commit() must not
    leak the MWB transient it set moments earlier — a stuck MWB blocks the
    agent's next commit_cas (transient precondition) and makes the stable-grant
    sweep skip the pair until the transient timeout. Manufactures the exact
    race window: the grant is still EXCLUSIVE but a sweep superseded the claim
    (owner_generation advanced between commit()'s state check and the
    version persist)."""
    from ccs.coordinator.service import CoordinatorService
    from ccs.coordinator.sqlite_registry import SqliteArtifactRegistry as _Sqlite

    reg = registry
    art = _register(reg)
    service = CoordinatorService(reg)
    a = uuid4()
    # Pessimistic acquire WITHOUT a prior fetch (P0-fix path): captures gen 0.
    service.write(agent_id=a, artifact_id=art.id, issued_at_tick=1)
    # Manufacture the race: generation advances while the state stays E.
    if isinstance(reg, _Sqlite):
        reg._conn.execute(
            "UPDATE artifacts SET owner_generation = owner_generation + 1 WHERE id = ?",
            (art.id.hex,),
        )
    else:
        reg._records[art.id].owner_generation += 1

    with pytest.raises(StaleReadGeneration):
        service.commit(agent_id=a, artifact_id=art.id, content="late", issued_at_tick=2)

    # The MWB transient is cleared (not leaked) and no phantom bump landed.
    assert reg.get_agent_transient(art.id, a) is None
    assert reg.get_artifact(art.id).version == 1
