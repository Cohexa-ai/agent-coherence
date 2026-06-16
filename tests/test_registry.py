# Copyright (c) 2026 agent-coherence contributors.
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


def test_commit_cas_happy_bumps_version_and_shares() -> None:
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
    # An OCC writer holds no grant — it ends SHARED (not MODIFIED), with no
    # granted_at_tick slot (SHARED is not an M∪E acquire).
    assert reg.get_agent_state(artifact_id, agent) == MESIState.SHARED
    assert reg.granted_at_tick(agent, artifact_id) is None


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
    assert reg.get_agent_state(artifact_id, agent) == MESIState.SHARED


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
    """The version branch fires before the holder branch, so a stale loser gets
    version_mismatch (not other_holder). The winner ends SHARED (an OCC writer
    holds no grant), so the discriminator here is purely the version check."""
    reg = ArtifactRegistry()
    artifact_id, winner = _seed(reg, version=1)
    loser = uuid4()
    reg.set_agent_state(artifact_id, loser, MESIState.SHARED, tick=2)
    reg.commit_cas(artifact_id, winner, expected_version=1, content_hash="w", tick=5)
    assert reg.get_agent_state(artifact_id, winner) == MESIState.SHARED
    result = reg.commit_cas(
        artifact_id, loser, expected_version=1, content_hash="l", tick=6
    )
    assert result == ConflictDetail("version_mismatch", 2)


def test_commit_cas_unknown_artifact_raises_keyerror() -> None:
    reg = ArtifactRegistry()
    with pytest.raises(KeyError):
        reg.commit_cas(uuid4(), uuid4(), expected_version=1, content_hash="x")


def test_commit_cas_committer_with_no_prior_state_wins() -> None:
    """An OCC writer in I (no prior state row) still wins a clean CAS → SHARED
    (an OCC writer holds no grant, so no granted_at_tick is set)."""
    reg = ArtifactRegistry()
    art = _make_artifact(version=1)
    reg.register_artifact(art, content="v")
    fresh = uuid4()  # never had a state entry → effectively INVALID
    updated, invalidated = reg.commit_cas(
        art.id, fresh, expected_version=1, content_hash="h-new", tick=4
    )
    assert updated.version == 2
    assert invalidated == []
    assert reg.get_agent_state(art.id, fresh) == MESIState.SHARED
    assert reg.granted_at_tick(fresh, art.id) is None


# --------------------------------------------------------------------
# state_log mutation-then-log rollback (in-memory atomicity oracle)
# --------------------------------------------------------------------


def test_commit_cas_state_log_raise_rolls_back_seq_and_state() -> None:
    """A state_log raise mid-CAS leaves version, _seq, and agent state
    untouched (mutation-then-log parity with set_agent_state)."""
    def hooky(entry: dict) -> None:
        # The committer now ends SHARED (OCC writer holds no grant); raise on its
        # emission. Peers emit INVALID, so SHARED uniquely targets the committer.
        if entry["to_state"] == "SHARED" and entry["trigger"] == "commit_cas":
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


def test_commit_cas_win_omitting_size_tokens_preserves_value_parity(tmp_path: Path) -> None:
    """FIX 2 parity: a WIN that omits ``size_tokens`` (the cross-process path
    always passes None) PRESERVES the persisted value rather than NULLing it —
    identically on BOTH registries. Without the fix the sqlite WIN wrote the raw
    None into the UPDATE, silently zeroing size_tokens on every cross-process
    OCC commit while the in-memory registry preserved it (a divergence)."""
    mem, sql = _build_pair(tmp_path)

    def run(reg: ArtifactRegistry | SqliteArtifactRegistry) -> tuple[int | None, int | None]:
        art = Artifact(
            id=uuid4(), name="plan.md", version=1, content_hash="h-init", size_tokens=42
        )
        reg.register_artifact(art, content="v")
        writer = uuid4()
        reg.set_agent_state(art.id, writer, MESIState.SHARED, tick=1)
        # WIN with size_tokens OMITTED (defaults to None).
        updated, _ = reg.commit_cas(
            art.id, writer, expected_version=1, content_hash="h-new", tick=2
        )
        # Returned Artifact AND the persisted row must both keep the prior 42.
        return updated.size_tokens, reg.get_artifact(art.id).size_tokens

    try:
        mem_out = run(mem)
        sql_out = run(sql)
        assert mem_out == (42, 42), f"in-memory did not preserve size_tokens: {mem_out}"
        assert sql_out == (42, 42), f"sqlite did not preserve size_tokens: {sql_out}"
        assert mem_out == sql_out
    finally:
        sql.close()


def test_commit_cas_win_with_explicit_size_tokens_overwrites_parity(tmp_path: Path) -> None:
    """Counterpart to the preserve test: a non-None ``size_tokens`` arg OVERWRITES
    the persisted value on both registries (the preserve rule fires only on None)."""
    mem, sql = _build_pair(tmp_path)

    def run(reg: ArtifactRegistry | SqliteArtifactRegistry) -> tuple[int | None, int | None]:
        art = Artifact(
            id=uuid4(), name="plan.md", version=1, content_hash="h-init", size_tokens=42
        )
        reg.register_artifact(art, content="v")
        writer = uuid4()
        reg.set_agent_state(art.id, writer, MESIState.SHARED, tick=1)
        updated, _ = reg.commit_cas(
            art.id, writer, expected_version=1, content_hash="h-new",
            size_tokens=99, tick=2,
        )
        return updated.size_tokens, reg.get_artifact(art.id).size_tokens

    try:
        assert run(mem) == (99, 99)
        assert run(sql) == (99, 99)
    finally:
        sql.close()


def test_commit_cas_inmem_win_updates_record_content_for_peer_fetch() -> None:
    """FIX 4: an in-memory commit_cas WIN with ``content`` advances
    ``record.content`` so a peer re-fetching via the service reads the winner's
    NEW body — not the stale pre-CAS seed. Without the fix the version +
    content_hash bumped but ``record.content`` kept the old bytes (silent content
    staleness)."""
    from ccs.coordinator.service import CoordinatorService
    from ccs.core.types import FetchRequest

    svc = CoordinatorService(ArtifactRegistry())
    artifact = svc.register_artifact(name="plan.md", content="seed-v1")
    winner = uuid4()
    peer = uuid4()
    # Two SHARED holders (a peer must exist so the later fetch grants SHARED, not
    # EXCLUSIVE, and so the winner has someone to be coherent with).
    svc.fetch(FetchRequest(artifact_id=artifact.id, requesting_agent_id=winner, requested_at_tick=1))
    svc.fetch(FetchRequest(artifact_id=artifact.id, requesting_agent_id=peer, requested_at_tick=2))

    updated, _ = svc.commit_cas(
        agent_id=winner,
        artifact_id=artifact.id,
        expected_version=artifact.version,
        content_hash="h-new",
        content="winner-v2",
        issued_at_tick=3,
    )
    assert updated.version == 2

    # Registry content advanced to the winner's body.
    assert svc.registry.get_content(artifact.id) == "winner-v2"
    # A peer re-fetch reads the NEW content at the new version (the peer was
    # invalidated by the win; the fetch re-grants it SHARED@v2 with fresh bytes).
    resp = svc.fetch(
        FetchRequest(artifact_id=artifact.id, requesting_agent_id=peer, requested_at_tick=4)
    )
    assert resp.version == 2
    assert resp.content == "winner-v2"


def test_commit_cas_inmem_win_with_content_updates_retained_version() -> None:
    """FIX 4 (retain_versions): the winning body is stored under the NEW version
    in version_history (not the stale pre-CAS body)."""
    reg = ArtifactRegistry(retain_versions=True)
    art = _make_artifact(version=1)
    reg.register_artifact(art, content="seed-v1")
    writer = uuid4()
    reg.set_agent_state(art.id, writer, MESIState.SHARED, tick=1)

    reg.commit_cas(
        art.id, writer, expected_version=1, content_hash="h-new",
        content="winner-v2", tick=2,
    )
    # The retained snapshot for version 2 is the winner's body.
    assert reg.get_content_at_version(art.id, 2) == "winner-v2"
    # The live content is also the winner's body.
    assert reg.get_content(art.id) == "winner-v2"


def test_commit_cas_inmem_win_without_content_leaves_body_unchanged() -> None:
    """FIX 4 default: when ``content`` is omitted (the cross-process / sqlite
    path), the in-memory body is left unchanged — only version + content_hash
    advance (prior content-coherence behaviour preserved)."""
    reg = ArtifactRegistry()
    art = _make_artifact(version=1)
    reg.register_artifact(art, content="seed-v1")
    writer = uuid4()
    reg.set_agent_state(art.id, writer, MESIState.SHARED, tick=1)

    updated, _ = reg.commit_cas(
        art.id, writer, expected_version=1, content_hash="h-new", tick=2
    )
    assert updated.version == 2
    assert updated.content_hash == "h-new"
    # Body untouched (no content threaded).
    assert reg.get_content(art.id) == "seed-v1"
