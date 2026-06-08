# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""Tests for the in-memory ArtifactRegistry OCC commit-CAS (plan Unit 2).

Covers the same 3-outcome matrix as tests/test_sqlite_registry.py plus a
sqlite/in-memory parity check, so the two registries (which share no base
class) are guaranteed to return identical outcomes for identical inputs.
"""

from __future__ import annotations

from pathlib import Path
from uuid import UUID, uuid4

import pytest

from ccs.coordinator.registry import ArtifactRegistry
from ccs.coordinator.sqlite_registry import SqliteArtifactRegistry
from ccs.core.states import MESIState
from ccs.core.types import Artifact, CasCorruption, ConflictDetail


def _make_artifact(version: int = 1, content_hash: str = "h-init") -> Artifact:
    return Artifact(id=uuid4(), name="plan.md", version=version, content_hash=content_hash)


def _seed(reg: ArtifactRegistry, *, version: int = 1) -> tuple[UUID, UUID]:
    """Register an artifact and put one agent in SHARED (OCC pre-commit state)."""
    art = _make_artifact(version=version)
    reg.register_artifact(art, content="v")
    agent = uuid4()
    reg.set_agent_state(art.id, agent, MESIState.SHARED, tick=1)
    return art.id, agent


# --------------------------------------------------------------------
# 3-outcome matrix (in-memory)
# --------------------------------------------------------------------


def test_commit_cas_happy_bumps_version_and_modifies() -> None:
    reg = ArtifactRegistry()
    artifact_id, agent = _seed(reg, version=1)
    result = reg.commit_cas(
        artifact_id, agent, expected_version=1, content_hash="h-new", tick=7
    )
    assert isinstance(result, tuple)
    updated, invalidated = result
    assert updated.version == 2
    assert updated.content_hash == "h-new"
    assert invalidated == []
    assert reg.get_agent_state(artifact_id, agent) == MESIState.MODIFIED
    assert reg.granted_at_tick(agent, artifact_id) == 7


def test_commit_cas_invalidates_non_invalid_peers() -> None:
    reg = ArtifactRegistry()
    artifact_id, agent = _seed(reg, version=1)
    peer_shared, peer_invalid = uuid4(), uuid4()
    reg.set_agent_state(artifact_id, peer_shared, MESIState.SHARED, tick=2)
    reg.set_agent_state(artifact_id, peer_invalid, MESIState.SHARED, tick=2)
    reg.set_agent_state(artifact_id, peer_invalid, MESIState.INVALID, tick=3)

    updated, invalidated = reg.commit_cas(
        artifact_id, agent, expected_version=1, content_hash="h-new", tick=9
    )
    assert updated.version == 2
    assert invalidated == [peer_shared]
    assert reg.get_agent_state(artifact_id, peer_shared) == MESIState.INVALID
    assert reg.get_agent_state(artifact_id, agent) == MESIState.MODIFIED


def test_commit_cas_version_mismatch_no_mutation() -> None:
    reg = ArtifactRegistry()
    artifact_id, _ = _seed(reg, version=5)
    loser = uuid4()
    reg.set_agent_state(artifact_id, loser, MESIState.SHARED, tick=2)
    result = reg.commit_cas(
        artifact_id, loser, expected_version=3, content_hash="h-stale", tick=10
    )
    assert result == ConflictDetail("version_mismatch", 5)
    art = reg.get_artifact(artifact_id)
    assert art.version == 5
    assert art.content_hash == "h-init"
    assert reg.get_agent_state(artifact_id, loser) == MESIState.SHARED


def test_commit_cas_other_holder_no_mutation() -> None:
    reg = ArtifactRegistry()
    artifact_id, occ_writer = _seed(reg, version=1)
    pessimistic = uuid4()
    reg.set_agent_state(artifact_id, pessimistic, MESIState.EXCLUSIVE, tick=2)
    result = reg.commit_cas(
        artifact_id, occ_writer, expected_version=1, content_hash="h-new", tick=11
    )
    assert result == ConflictDetail("other_holder", 1)
    assert reg.get_artifact(artifact_id).version == 1
    assert reg.get_agent_state(artifact_id, occ_writer) == MESIState.SHARED
    assert reg.get_agent_state(artifact_id, pessimistic) == MESIState.EXCLUSIVE


def test_commit_cas_expected_greater_returns_corruption() -> None:
    reg = ArtifactRegistry()
    artifact_id, agent = _seed(reg, version=2)
    result = reg.commit_cas(
        artifact_id, agent, expected_version=9, content_hash="x", tick=1
    )
    assert isinstance(result, CasCorruption)
    assert result.current_version == 2
    assert not isinstance(result, ConflictDetail)
    assert reg.get_artifact(artifact_id).version == 2


def test_commit_cas_version_check_precedes_holder_check() -> None:
    """A just-committed winner's MODIFIED must not mis-fire other_holder."""
    reg = ArtifactRegistry()
    artifact_id, winner = _seed(reg, version=1)
    loser = uuid4()
    reg.set_agent_state(artifact_id, loser, MESIState.SHARED, tick=2)
    reg.commit_cas(artifact_id, winner, expected_version=1, content_hash="w", tick=5)
    assert reg.get_agent_state(artifact_id, winner) == MESIState.MODIFIED
    result = reg.commit_cas(
        artifact_id, loser, expected_version=1, content_hash="l", tick=6
    )
    assert result == ConflictDetail("version_mismatch", 2)


def test_commit_cas_unknown_artifact_raises_keyerror() -> None:
    reg = ArtifactRegistry()
    with pytest.raises(KeyError):
        reg.commit_cas(uuid4(), uuid4(), expected_version=1, content_hash="x")


def test_commit_cas_committer_with_no_prior_state_wins() -> None:
    """An OCC writer in I (no prior state row) still wins a clean CAS → MODIFIED."""
    reg = ArtifactRegistry()
    art = _make_artifact(version=1)
    reg.register_artifact(art, content="v")
    fresh = uuid4()  # never had a state entry → effectively INVALID
    updated, invalidated = reg.commit_cas(
        art.id, fresh, expected_version=1, content_hash="h-new", tick=4
    )
    assert updated.version == 2
    assert invalidated == []
    assert reg.get_agent_state(art.id, fresh) == MESIState.MODIFIED
    assert reg.granted_at_tick(fresh, art.id) == 4


# --------------------------------------------------------------------
# state_log mutation-then-log rollback (in-memory atomicity oracle)
# --------------------------------------------------------------------


def test_commit_cas_state_log_raise_rolls_back_seq_and_state() -> None:
    """A state_log raise mid-CAS leaves version, _seq, and agent state
    untouched (mutation-then-log parity with set_agent_state)."""
    def hooky(entry: dict) -> None:
        if entry["to_state"] == "MODIFIED" and entry["trigger"] == "commit_cas":
            raise RuntimeError("simulated callback failure mid-CAS")

    reg = ArtifactRegistry(state_log=hooky, instance_id="inst-cas")
    art = _make_artifact(version=1)
    reg.register_artifact(art, content="v")
    agent, peer = uuid4(), uuid4()
    reg.set_agent_state(art.id, agent, MESIState.SHARED, tick=1)  # seq 1
    reg.set_agent_state(art.id, peer, MESIState.SHARED, tick=1)  # seq 2
    assert reg._seq == 2

    with pytest.raises(RuntimeError, match="simulated callback failure mid-CAS"):
        reg.commit_cas(art.id, agent, expected_version=1, content_hash="h-new", tick=5)

    # No mutation: version unchanged, both agents still SHARED.
    assert reg.get_artifact(art.id).version == 1
    assert reg.get_artifact(art.id).content_hash == "h-init"
    assert reg.get_agent_state(art.id, agent) == MESIState.SHARED
    assert reg.get_agent_state(art.id, peer) == MESIState.SHARED
    # _seq rolled back to 2 (peer emission undone + failed committer reservation).
    assert reg._seq == 2


# --------------------------------------------------------------------
# Parity: in-memory and sqlite return identical outcomes
# --------------------------------------------------------------------


def _build_pair(
    tmp_path: Path,
) -> tuple[ArtifactRegistry, SqliteArtifactRegistry]:
    return ArtifactRegistry(), SqliteArtifactRegistry(tmp_path / "parity.db")


def _normalize(result: object) -> object:
    """Reduce a CasResult to a comparable shape: WIN → ('win', version,
    n_invalidated); conflict/corruption compare by value directly."""
    if isinstance(result, tuple):
        updated, invalidated = result
        return ("win", updated.version, len(invalidated))
    return result


@pytest.mark.parametrize(
    "scenario",
    ["happy", "version_mismatch", "other_holder", "corruption", "with_peers"],
)
def test_commit_cas_parity_inmem_vs_sqlite(tmp_path: Path, scenario: str) -> None:
    """Both registries must return identical outcomes for identical inputs."""
    mem, sql = _build_pair(tmp_path)

    def run(reg: ArtifactRegistry | SqliteArtifactRegistry) -> tuple:
        art = _make_artifact(version=5, content_hash="h-init")
        reg.register_artifact(art, content="v")
        writer = uuid4()
        reg.set_agent_state(art.id, writer, MESIState.SHARED, tick=1)

        if scenario == "happy":
            res = reg.commit_cas(art.id, writer, expected_version=5, content_hash="h", tick=2)
        elif scenario == "version_mismatch":
            res = reg.commit_cas(art.id, writer, expected_version=3, content_hash="h", tick=2)
        elif scenario == "other_holder":
            reg.set_agent_state(art.id, uuid4(), MESIState.EXCLUSIVE, tick=2)
            res = reg.commit_cas(art.id, writer, expected_version=5, content_hash="h", tick=3)
        elif scenario == "corruption":
            res = reg.commit_cas(art.id, writer, expected_version=99, content_hash="h", tick=2)
        else:  # with_peers
            reg.set_agent_state(art.id, uuid4(), MESIState.SHARED, tick=2)
            reg.set_agent_state(art.id, uuid4(), MESIState.SHARED, tick=2)
            res = reg.commit_cas(art.id, writer, expected_version=5, content_hash="h", tick=3)

        # Tuple of (outcome, post-version, committer-state) — all three must
        # match across impls for true parity.
        return (
            _normalize(res),
            reg.get_artifact(art.id).version,
            reg.get_agent_state(art.id, writer),
        )

    try:
        mem_out = run(mem)
        sql_out = run(sql)
        assert mem_out == sql_out, f"{scenario}: in-memory {mem_out} != sqlite {sql_out}"
    finally:
        sql.close()
