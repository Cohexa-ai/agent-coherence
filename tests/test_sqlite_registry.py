# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""Tests for SqliteArtifactRegistry (plan Unit 1).

Scope is the plugin's hot-path methods + plugin-only extensions
(resolve_or_register, artifacts_held_by_agent). Full ArtifactRegistry
parity test against tests/test_coordinator.py is NOT a v0.1 goal — that
test suite exercises content-fetch semantics that this storage layer
deliberately does not preserve (KTD-13 contract divergence).
"""

from __future__ import annotations

import threading
from pathlib import Path
from uuid import UUID, uuid4

import pytest

from ccs.coordinator.sqlite_registry import (
    CCS_STATE_LOG_SCHEMA_VERSION,
    SCHEMA_USER_VERSION,
    SchemaVersionError,
    SqliteArtifactRegistry,
)
from ccs.core.states import MESIState, TransientState
from ccs.core.types import Artifact, CasCorruption, ConflictDetail


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    return tmp_path / "state.db"


def _make_artifact(name: str = "plan.md", version: int = 1, content_hash: str = "h1") -> Artifact:
    return Artifact(id=uuid4(), name=name, version=version, content_hash=content_hash)


# --------------------------------------------------------------------
# Schema lifecycle
# --------------------------------------------------------------------


def test_fresh_init_creates_v1_schema(db_path: Path) -> None:
    with SqliteArtifactRegistry(db_path) as reg:
        # Tables exist
        rows = reg._conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        ).fetchall()
        names = {r[0] for r in rows}
        assert {"artifacts", "agent_states", "heartbeats", "registry_meta"}.issubset(names)
        # user_version set
        assert reg._conn.execute("PRAGMA user_version").fetchone()[0] == SCHEMA_USER_VERSION
        # registry_meta seeded
        meta = dict(reg._conn.execute("SELECT key, value FROM registry_meta").fetchall())
        assert "instance_id" in meta and "sequence_number" in meta
        assert meta["sequence_number"] == "0"


def test_rehydration_preserves_instance_id_and_seq(db_path: Path) -> None:
    # Write seed data with a state_log that emits a few entries.
    emitted: list[dict] = []
    instance_id_pinned = str(uuid4())
    with SqliteArtifactRegistry(
        db_path,
        state_log=emitted.append,
        instance_id=instance_id_pinned,
    ) as reg:
        art = _make_artifact()
        reg.register_artifact(art, content="ignored-per-KTD-13")
        agent = uuid4()
        reg.set_agent_state(art.id, agent, MESIState.SHARED, trigger="read", tick=100)
        reg.set_agent_state(art.id, agent, MESIState.EXCLUSIVE, trigger="write", tick=101)
        assert reg._seq == 2
        assert emitted[-1]["sequence_number"] == 2
        first_artifact_id = art.id

    # Re-open
    with SqliteArtifactRegistry(db_path) as reg2:
        assert reg2._instance_id == instance_id_pinned
        assert reg2._seq == 2  # persisted across restart
        assert reg2.has_artifact(first_artifact_id)
        assert reg2.get_agent_state(first_artifact_id, agent) == MESIState.EXCLUSIVE


def test_state_log_without_instance_id_raises(db_path: Path) -> None:
    with pytest.raises(ValueError, match="instance_id must be provided"):
        SqliteArtifactRegistry(db_path, state_log=lambda e: None)


def test_unexpected_schema_version_raises(db_path: Path) -> None:
    # Seed db with v1, then mutate user_version to a future value.
    with SqliteArtifactRegistry(db_path):
        pass
    import sqlite3
    conn = sqlite3.connect(str(db_path), isolation_level=None)
    conn.execute("PRAGMA user_version = 99")
    conn.close()

    with pytest.raises(SchemaVersionError, match="unexpected schema version 99"):
        SqliteArtifactRegistry(db_path)


# --------------------------------------------------------------------
# Artifact CRUD
# --------------------------------------------------------------------


def test_register_and_get_artifact(db_path: Path) -> None:
    with SqliteArtifactRegistry(db_path) as reg:
        art = _make_artifact(name="plan.md", version=1, content_hash="abc123")
        reg.register_artifact(art, content="ignored")
        assert reg.has_artifact(art.id)
        fetched = reg.get_artifact(art.id)
        assert fetched is not None
        assert fetched.name == "plan.md"
        assert fetched.version == 1
        assert fetched.content_hash == "abc123"


def test_get_content_returns_empty_for_known_none_for_unknown(db_path: Path) -> None:
    """KTD-13 contract divergence."""
    with SqliteArtifactRegistry(db_path) as reg:
        art = _make_artifact()
        reg.register_artifact(art, content="real-content-that-will-be-discarded")
        assert reg.get_content(art.id) == b""
        assert reg.get_content(uuid4()) is None


def test_artifact_ids_returns_all(db_path: Path) -> None:
    with SqliteArtifactRegistry(db_path) as reg:
        a1 = _make_artifact(name="plan.md")
        a2 = _make_artifact(name="spec.md")
        reg.register_artifact(a1, content="")
        reg.register_artifact(a2, content="")
        ids = reg.artifact_ids()
        assert set(ids) == {a1.id, a2.id}


def test_set_artifact_and_content_updates_metadata(db_path: Path) -> None:
    with SqliteArtifactRegistry(db_path) as reg:
        art = _make_artifact(version=1, content_hash="v1hash")
        reg.register_artifact(art, content="")
        new_art = Artifact(id=art.id, name=art.name, version=2, content_hash="v2hash")
        writer = uuid4()
        reg.set_artifact_and_content(art.id, new_art, content="ignored", last_writer=writer)
        fetched = reg.get_artifact(art.id)
        assert fetched.version == 2
        assert fetched.content_hash == "v2hash"


def test_remove_artifact_cascades_to_agent_states(db_path: Path) -> None:
    with SqliteArtifactRegistry(db_path) as reg:
        art = _make_artifact()
        reg.register_artifact(art, content="")
        agent = uuid4()
        reg.set_agent_state(art.id, agent, MESIState.SHARED, tick=1)
        reg.remove_artifact(art.id)
        assert not reg.has_artifact(art.id)
        # State for the removed artifact should be gone (FK cascade)
        assert reg.get_agent_state(art.id, agent) is None


# --------------------------------------------------------------------
# Agent state map
# --------------------------------------------------------------------


def test_set_agent_state_cycles(db_path: Path) -> None:
    with SqliteArtifactRegistry(db_path) as reg:
        art = _make_artifact()
        reg.register_artifact(art, content="")
        agent = uuid4()

        # INVALID -> SHARED
        reg.set_agent_state(art.id, agent, MESIState.SHARED, tick=10)
        assert reg.get_agent_state(art.id, agent) == MESIState.SHARED
        # SHARED -> EXCLUSIVE (M∪E acquire — sets granted_at_tick)
        reg.set_agent_state(art.id, agent, MESIState.EXCLUSIVE, tick=20)
        assert reg.granted_at_tick(agent, art.id) == 20
        # EXCLUSIVE -> MODIFIED (M↔E preserve grant tick)
        reg.set_agent_state(art.id, agent, MESIState.MODIFIED, tick=25)
        assert reg.granted_at_tick(agent, art.id) == 20  # preserved
        # MODIFIED -> INVALID (drops the slot)
        reg.set_agent_state(art.id, agent, MESIState.INVALID, tick=30)
        assert reg.granted_at_tick(agent, art.id) is None


def test_set_agent_state_on_unknown_artifact_raises(db_path: Path) -> None:
    with SqliteArtifactRegistry(db_path) as reg:
        with pytest.raises(KeyError):
            reg.set_agent_state(uuid4(), uuid4(), MESIState.SHARED, tick=1)


def test_get_state_map_returns_per_agent_view(db_path: Path) -> None:
    with SqliteArtifactRegistry(db_path) as reg:
        art = _make_artifact()
        reg.register_artifact(art, content="")
        a1, a2 = uuid4(), uuid4()
        reg.set_agent_state(art.id, a1, MESIState.SHARED, tick=1)
        reg.set_agent_state(art.id, a2, MESIState.EXCLUSIVE, tick=2)
        state_map = reg.get_state_map(art.id)
        assert state_map == {a1: MESIState.SHARED, a2: MESIState.EXCLUSIVE}


def test_valid_holders_excludes_invalid(db_path: Path) -> None:
    with SqliteArtifactRegistry(db_path) as reg:
        art = _make_artifact()
        reg.register_artifact(art, content="")
        a1, a2 = uuid4(), uuid4()
        reg.set_agent_state(art.id, a1, MESIState.SHARED, tick=1)
        reg.set_agent_state(art.id, a2, MESIState.SHARED, tick=2)
        reg.set_agent_state(art.id, a2, MESIState.INVALID, tick=3)
        holders = reg.valid_holders(art.id)
        assert holders == [a1]


# --------------------------------------------------------------------
# state_log mutation-then-log + sequence rollback
# --------------------------------------------------------------------


def test_state_log_emitted_on_each_state_change(db_path: Path) -> None:
    emitted: list[dict] = []
    with SqliteArtifactRegistry(
        db_path, state_log=emitted.append, instance_id="instance-1"
    ) as reg:
        art = _make_artifact()
        reg.register_artifact(art, content="")
        agent = uuid4()
        reg.set_agent_state(art.id, agent, MESIState.SHARED, trigger="t1", tick=10)
        reg.set_agent_state(art.id, agent, MESIState.EXCLUSIVE, trigger="t2", tick=11)
        assert len(emitted) == 2
        assert emitted[0]["sequence_number"] == 1
        assert emitted[0]["from_state"] == "INVALID"
        assert emitted[0]["to_state"] == "SHARED"
        assert emitted[0]["trigger"] == "t1"
        assert emitted[0]["schema_version"] == CCS_STATE_LOG_SCHEMA_VERSION
        assert emitted[0]["instance_id"] == "instance-1"
        assert emitted[1]["sequence_number"] == 2
        assert emitted[1]["from_state"] == "SHARED"
        assert emitted[1]["to_state"] == "EXCLUSIVE"


def test_state_log_raise_rolls_back_seq_and_state(db_path: Path) -> None:
    """Mutation-then-log invariant: callback raise → ROLLBACK + _seq decrement."""
    raise_on_seq = 2
    emitted: list[dict] = []

    def hooky(entry: dict) -> None:
        if entry["sequence_number"] == raise_on_seq:
            raise RuntimeError("simulated callback failure")
        emitted.append(entry)

    with SqliteArtifactRegistry(
        db_path, state_log=hooky, instance_id="instance-1"
    ) as reg:
        art = _make_artifact()
        reg.register_artifact(art, content="")
        agent = uuid4()
        # First transition emits seq=1 successfully.
        reg.set_agent_state(art.id, agent, MESIState.SHARED, tick=10)
        assert reg._seq == 1
        # Second transition would emit seq=2; callback raises.
        with pytest.raises(RuntimeError, match="simulated"):
            reg.set_agent_state(art.id, agent, MESIState.EXCLUSIVE, tick=11)
        # _seq must have rolled back to 1.
        assert reg._seq == 1
        # Agent state must remain SHARED (the rolled-back transition).
        assert reg.get_agent_state(art.id, agent) == MESIState.SHARED
        # registry_meta sequence_number persisted is still 1 (not 2).
        meta = dict(reg._conn.execute(
            "SELECT key, value FROM registry_meta WHERE key='sequence_number'"
        ).fetchall())
        assert meta["sequence_number"] == "1"


# --------------------------------------------------------------------
# Heartbeat monotonicity
# --------------------------------------------------------------------


def test_record_heartbeat_monotonic(db_path: Path) -> None:
    with SqliteArtifactRegistry(db_path) as reg:
        agent = uuid4()
        reg.record_heartbeat(agent, now_tick=100)
        reg.record_heartbeat(agent, now_tick=50)  # earlier; should be ignored
        reg.record_heartbeat(agent, now_tick=200)
        assert reg.last_heartbeat_tick(agent) == 200


# --------------------------------------------------------------------
# Plugin extensions
# --------------------------------------------------------------------


def test_resolve_or_register_first_observation_creates_v1(db_path: Path) -> None:
    with SqliteArtifactRegistry(db_path) as reg:
        artifact_id = reg.resolve_or_register("plan.md", content_hash="hash-v1")
        art = reg.get_artifact(artifact_id)
        assert art is not None
        assert art.name == "plan.md"
        assert art.version == 1
        assert art.content_hash == "hash-v1"


def test_resolve_or_register_idempotent(db_path: Path) -> None:
    with SqliteArtifactRegistry(db_path) as reg:
        id1 = reg.resolve_or_register("plan.md", content_hash="any-hash")
        id2 = reg.resolve_or_register("plan.md", content_hash="different-hash")
        assert id1 == id2
        # Content hash stays the original v1 — resolve_or_register does NOT update.
        assert reg.get_artifact(id1).content_hash == "any-hash"


def test_resolve_or_register_race_safe(db_path: Path) -> None:
    """N threads concurrently resolve the same fresh path → all return same id."""
    with SqliteArtifactRegistry(db_path) as reg:
        results: list[UUID] = []
        barrier = threading.Barrier(10)

        def call() -> None:
            barrier.wait()
            results.append(reg.resolve_or_register("plan.md", content_hash="h"))

        threads = [threading.Thread(target=call) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert len(results) == 10
        assert len(set(results)) == 1, f"expected single id, got {set(results)}"


def test_artifacts_held_by_agent_filters_by_state(db_path: Path) -> None:
    with SqliteArtifactRegistry(db_path) as reg:
        a_held = _make_artifact(name="plan.md")
        a_invalid = _make_artifact(name="other.md")
        reg.register_artifact(a_held, content="")
        reg.register_artifact(a_invalid, content="")
        agent = uuid4()
        reg.set_agent_state(a_held.id, agent, MESIState.EXCLUSIVE, tick=1)
        reg.set_agent_state(a_invalid.id, agent, MESIState.INVALID, tick=2)

        held = reg.artifacts_held_by_agent(
            agent, {MESIState.EXCLUSIVE, MESIState.MODIFIED}
        )
        assert held == [a_held.id]


def test_artifacts_held_by_agent_empty_states_returns_empty(db_path: Path) -> None:
    with SqliteArtifactRegistry(db_path) as reg:
        assert reg.artifacts_held_by_agent(uuid4(), set()) == []


def test_lookup_artifact_id_by_name_roundtrip(db_path: Path) -> None:
    with SqliteArtifactRegistry(db_path) as reg:
        artifact_id = reg.resolve_or_register("plan.md", content_hash="h")
        assert reg.lookup_artifact_id_by_name("plan.md") == artifact_id
        assert reg.lookup_artifact_id_by_name("does-not-exist.md") is None


# --------------------------------------------------------------------
# Transient state
# --------------------------------------------------------------------


def test_transient_state_set_get_clear(db_path: Path) -> None:
    with SqliteArtifactRegistry(db_path) as reg:
        art = _make_artifact()
        reg.register_artifact(art, content="")
        agent = uuid4()
        reg.set_agent_transient(art.id, agent, TransientState.ISG, entered_tick=5)
        assert reg.get_agent_transient(art.id, agent) == TransientState.ISG
        assert reg.get_transient_tick(art.id, agent) == 5
        reg.clear_agent_transient(art.id, agent)
        assert reg.get_agent_transient(art.id, agent) is None


# --------------------------------------------------------------------
# Cross-thread concurrency (ThreadingHTTPServer simulation)
# --------------------------------------------------------------------


def test_concurrent_state_changes_no_deadlock(db_path: Path) -> None:
    """10 threads racing on set_agent_state for distinct (agent, artifact)
    pairs should all complete without deadlock; final states are observable."""
    with SqliteArtifactRegistry(db_path) as reg:
        art = _make_artifact()
        reg.register_artifact(art, content="")
        agents = [uuid4() for _ in range(10)]

        def worker(agent_id: UUID, t: int) -> None:
            reg.set_agent_state(art.id, agent_id, MESIState.SHARED, tick=t)

        threads = [
            threading.Thread(target=worker, args=(a, i + 1))
            for i, a in enumerate(agents)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        state_map = reg.get_state_map(art.id)
        assert len(state_map) == 10
        assert all(s == MESIState.SHARED for s in state_map.values())


# ----------------------------------------------------------------------
# COR-04 — resolve_or_register re-fetch race produces informative error
# ----------------------------------------------------------------------


def test_cor04_resolve_or_register_post_rollback_delete_raises_runtime_error(
    tmp_path
):
    """COR-04: when a concurrent remove_artifact deletes the winning
    racer's row between our ROLLBACK and re-fetch, the function must
    raise an informative RuntimeError explaining the race rather than
    re-raising the original sqlite3.IntegrityError with no context.

    Wraps reg._conn in a proxy that forces INSERT to raise IntegrityError
    and the post-ROLLBACK SELECT to return None.
    """
    import sqlite3

    from ccs.coordinator.sqlite_registry import SqliteArtifactRegistry

    reg = SqliteArtifactRegistry(tmp_path / "state.db")
    try:
        real_conn = reg._conn
        call_state = {"insert_seen": False, "post_rollback_select_seen": False}

        class _ConnProxy:
            def execute(self, sql, *args):
                sql_lower = sql.strip().lower()
                if sql_lower.startswith("insert into artifacts"):
                    call_state["insert_seen"] = True
                    raise sqlite3.IntegrityError("simulated UNIQUE collision")
                if (
                    call_state["insert_seen"]
                    and not call_state["post_rollback_select_seen"]
                    and sql_lower.startswith("select id from artifacts where name")
                ):
                    call_state["post_rollback_select_seen"] = True

                    class _Empty:
                        def fetchone(self_inner):
                            return None

                    return _Empty()
                return real_conn.execute(sql, *args)

            def __getattr__(self, name):
                return getattr(real_conn, name)

        reg._conn = _ConnProxy()
        try:
            import pytest as _pytest
            with _pytest.raises(RuntimeError) as excinfo:
                reg.resolve_or_register("plan.md", content_hash="abc")
            assert "lost INSERT race" in str(excinfo.value)
            assert "plan.md" in str(excinfo.value)
            assert isinstance(excinfo.value.__cause__, sqlite3.IntegrityError)
        finally:
            reg._conn = real_conn
    finally:
        reg.close()


# --------------------------------------------------------------------
# OCC commit-CAS (plan Unit 2)
# --------------------------------------------------------------------


def _seed_artifact_with_shared_writer(
    reg: SqliteArtifactRegistry, *, version: int = 1
) -> tuple[UUID, UUID]:
    """Register an artifact at ``version`` and put one agent in SHARED (the OCC
    writer's pre-commit state). Returns ``(artifact_id, agent_id)``."""
    art = _make_artifact(version=version, content_hash="h-init")
    reg.register_artifact(art, content="")
    agent = uuid4()
    reg.set_agent_state(art.id, agent, MESIState.SHARED, tick=1)
    return art.id, agent


def test_commit_cas_happy_bumps_version_and_shares(db_path: Path) -> None:
    """expected == current, no other E/M holder → version N+1, agent SHARED."""
    with SqliteArtifactRegistry(db_path) as reg:
        artifact_id, agent = _seed_artifact_with_shared_writer(reg, version=1)
        result = reg.commit_cas(
            artifact_id, agent, expected_version=1, content_hash="h-new", tick=7
        )
        assert isinstance(result, tuple)
        updated, invalidated = result
        assert updated.version == 2
        assert updated.content_hash == "h-new"
        assert invalidated == []
        # An OCC writer holds no grant — it ends SHARED (not MODIFIED), and
        # SHARED is not an M∪E acquire so no granted_at_tick slot is set.
        assert reg.get_agent_state(artifact_id, agent) == MESIState.SHARED
        assert reg.granted_at_tick(agent, artifact_id) is None
        # last_writer recorded on the artifact row.
        assert reg.last_writer_for(artifact_id) == agent


def test_commit_cas_invalidates_non_invalid_peers(db_path: Path) -> None:
    """WIN invalidates every non-INVALID peer; returns their ids for signals."""
    with SqliteArtifactRegistry(db_path) as reg:
        artifact_id, agent = _seed_artifact_with_shared_writer(reg, version=1)
        peer_shared = uuid4()
        peer_invalid = uuid4()
        reg.set_agent_state(artifact_id, peer_shared, MESIState.SHARED, tick=2)
        reg.set_agent_state(artifact_id, peer_invalid, MESIState.SHARED, tick=2)
        reg.set_agent_state(artifact_id, peer_invalid, MESIState.INVALID, tick=3)

        updated, invalidated = reg.commit_cas(
            artifact_id, agent, expected_version=1, content_hash="h-new", tick=9
        )
        assert updated.version == 2
        # Only the still-valid peer is invalidated; the already-INVALID one is not.
        assert invalidated == [peer_shared]
        assert reg.get_agent_state(artifact_id, peer_shared) == MESIState.INVALID
        assert reg.get_agent_state(artifact_id, agent) == MESIState.SHARED


def test_commit_cas_version_mismatch_no_mutation(db_path: Path) -> None:
    """expected < current → version_mismatch ConflictDetail, NO mutation."""
    with SqliteArtifactRegistry(db_path) as reg:
        artifact_id, _ = _seed_artifact_with_shared_writer(reg, version=5)
        loser = uuid4()
        reg.set_agent_state(artifact_id, loser, MESIState.SHARED, tick=2)
        result = reg.commit_cas(
            artifact_id, loser, expected_version=3, content_hash="h-stale", tick=10
        )
        assert result == ConflictDetail("version_mismatch", 5)
        # No mutation: version unchanged, content_hash unchanged, state SHARED.
        art = reg.get_artifact(artifact_id)
        assert art.version == 5
        assert art.content_hash == "h-init"
        assert reg.get_agent_state(artifact_id, loser) == MESIState.SHARED


def test_commit_cas_other_holder_no_mutation(db_path: Path) -> None:
    """version matches but another agent holds E/M → other_holder, NO mutation."""
    with SqliteArtifactRegistry(db_path) as reg:
        artifact_id, occ_writer = _seed_artifact_with_shared_writer(reg, version=1)
        pessimistic = uuid4()
        reg.set_agent_state(artifact_id, pessimistic, MESIState.EXCLUSIVE, tick=2)
        result = reg.commit_cas(
            artifact_id, occ_writer, expected_version=1, content_hash="h-new", tick=11
        )
        assert result == ConflictDetail("other_holder", 1)
        assert reg.get_artifact(artifact_id).version == 1
        assert reg.get_agent_state(artifact_id, occ_writer) == MESIState.SHARED
        # The pessimistic holder is untouched.
        assert reg.get_agent_state(artifact_id, pessimistic) == MESIState.EXCLUSIVE


def test_commit_cas_modified_peer_also_trips_other_holder(db_path: Path) -> None:
    """MODIFIED (not just EXCLUSIVE) peer trips other_holder."""
    with SqliteArtifactRegistry(db_path) as reg:
        artifact_id, occ_writer = _seed_artifact_with_shared_writer(reg, version=1)
        holder = uuid4()
        reg.set_agent_state(artifact_id, holder, MESIState.EXCLUSIVE, tick=2)
        reg.set_agent_state(artifact_id, holder, MESIState.MODIFIED, tick=3)
        result = reg.commit_cas(
            artifact_id, occ_writer, expected_version=1, content_hash="x", tick=4
        )
        assert result == ConflictDetail("other_holder", 1)


def test_commit_cas_expected_greater_returns_corruption(db_path: Path) -> None:
    """expected > current → CasCorruption sentinel (service maps to CoherenceError)."""
    with SqliteArtifactRegistry(db_path) as reg:
        artifact_id, agent = _seed_artifact_with_shared_writer(reg, version=2)
        result = reg.commit_cas(
            artifact_id, agent, expected_version=9, content_hash="x", tick=1
        )
        assert isinstance(result, CasCorruption)
        assert result.current_version == 2
        # Corruption is distinct from a conflict.
        assert not isinstance(result, ConflictDetail)
        # No mutation.
        assert reg.get_artifact(artifact_id).version == 2


def test_commit_cas_version_check_precedes_holder_check(db_path: Path) -> None:
    """The version branch fires before the holder branch → a stale loser gets
    version_mismatch (not other_holder). The winner ends SHARED (an OCC writer
    holds no grant), so the version check is the sole discriminator here."""
    with SqliteArtifactRegistry(db_path) as reg:
        artifact_id, winner = _seed_artifact_with_shared_writer(reg, version=1)
        loser = uuid4()
        reg.set_agent_state(artifact_id, loser, MESIState.SHARED, tick=2)
        # Winner commits: version 1→2, winner now SHARED (no grant).
        reg.commit_cas(artifact_id, winner, expected_version=1, content_hash="w", tick=5)
        assert reg.get_agent_state(artifact_id, winner) == MESIState.SHARED
        # Loser retries at stale expected_version=1; version_mismatch must win
        # (not other_holder), proving branch order.
        result = reg.commit_cas(
            artifact_id, loser, expected_version=1, content_hash="l", tick=6
        )
        assert result == ConflictDetail("version_mismatch", 2)


def test_commit_cas_unknown_artifact_raises_keyerror(db_path: Path) -> None:
    with SqliteArtifactRegistry(db_path) as reg:
        with pytest.raises(KeyError):
            reg.commit_cas(uuid4(), uuid4(), expected_version=1, content_hash="x")


def test_commit_cas_emits_state_log_for_committer_and_peers(db_path: Path) -> None:
    """WIN emits one state_log entry per transition (peers→INVALID, committer→M),
    with monotonic sequence numbers continuing from prior emissions."""
    emitted: list[dict] = []
    with SqliteArtifactRegistry(
        db_path, state_log=emitted.append, instance_id="inst-cas"
    ) as reg:
        art = _make_artifact(version=1, content_hash="h-init")
        reg.register_artifact(art, content="")
        agent = uuid4()
        peer = uuid4()
        reg.set_agent_state(art.id, agent, MESIState.SHARED, tick=1)  # seq 1
        reg.set_agent_state(art.id, peer, MESIState.SHARED, tick=1)  # seq 2
        assert reg._seq == 2
        emitted.clear()

        reg.commit_cas(art.id, agent, expected_version=1, content_hash="h-new", tick=5)
        # One peer invalidation + one committer SHARED = 2 new entries.
        assert len(emitted) == 2
        transitions = {(e["from_state"], e["to_state"]) for e in emitted}
        assert ("SHARED", "INVALID") in transitions
        # The committer ends SHARED (OCC writer holds no grant): SHARED→SHARED.
        assert ("SHARED", "SHARED") in transitions
        seqs = sorted(e["sequence_number"] for e in emitted)
        assert seqs == [3, 4]
        assert reg._seq == 4
        # The committer entry carries the new content_hash; the invalidation does
        # not. (The committer is the SHARED→SHARED entry, not the peer→INVALID.)
        committer_entry = next(
            e for e in emitted if (e["from_state"], e["to_state"]) == ("SHARED", "SHARED")
        )
        assert committer_entry["content_hash"] == "h-new"
        assert committer_entry["version"] == 2
        inv_entry = next(e for e in emitted if e["to_state"] == "INVALID")
        assert inv_entry["content_hash"] is None
        # sequence_number persisted to registry_meta.
        meta = dict(reg._conn.execute(
            "SELECT key, value FROM registry_meta WHERE key='sequence_number'"
        ).fetchall())
        assert meta["sequence_number"] == "4"


def test_commit_cas_state_log_raise_rolls_back_seq_and_state(db_path: Path) -> None:
    """Atomicity oracle (mirrors test_state_log_raise_rolls_back_seq_and_state):
    a state_log raise mid-CAS → ROLLBACK leaves version + _seq + agent state
    untouched, and registry_meta.sequence_number unchanged."""
    # Raise on the committer's SHARED emission (the committer now ends SHARED —
    # an OCC writer holds no grant). The peer invalidation is emitted first
    # (to_state INVALID), the committer emission is the one that raises — forcing
    # a rollback after the UPDATE + peer mutation already ran inside the
    # transaction. SHARED uniquely targets the committer (the peer goes →INVALID).
    raise_on_to_state = "SHARED"
    emitted: list[dict] = []

    def hooky(entry: dict) -> None:
        if entry["to_state"] == raise_on_to_state and entry["trigger"] == "commit_cas":
            raise RuntimeError("simulated callback failure mid-CAS")
        emitted.append(entry)

    with SqliteArtifactRegistry(
        db_path, state_log=hooky, instance_id="inst-cas"
    ) as reg:
        art = _make_artifact(version=1, content_hash="h-init")
        reg.register_artifact(art, content="")
        agent = uuid4()
        peer = uuid4()
        reg.set_agent_state(art.id, agent, MESIState.SHARED, tick=1)  # seq 1
        reg.set_agent_state(art.id, peer, MESIState.SHARED, tick=1)  # seq 2
        assert reg._seq == 2

        with pytest.raises(RuntimeError, match="simulated callback failure mid-CAS"):
            reg.commit_cas(art.id, agent, expected_version=1, content_hash="h-new", tick=5)

        # Everything rolled back: version still 1, content_hash still h-init.
        art_now = reg.get_artifact(art.id)
        assert art_now.version == 1
        assert art_now.content_hash == "h-init"
        # Agent state preserved (committer still SHARED, peer still SHARED — the
        # peer's INVALID mutation was rolled back too).
        assert reg.get_agent_state(art.id, agent) == MESIState.SHARED
        assert reg.get_agent_state(art.id, peer) == MESIState.SHARED
        # _seq rolled back to 2 (both the peer emission AND the failed committer
        # reservation are undone).
        assert reg._seq == 2
        meta = dict(reg._conn.execute(
            "SELECT key, value FROM registry_meta WHERE key='sequence_number'"
        ).fetchall())
        assert meta["sequence_number"] == "2"


def test_commit_cas_concurrent_writers_exactly_one_wins(db_path: Path) -> None:
    """Two barrier-synced OCC writers (both SHARED, same expected_version) race
    on commit_cas → exactly one commits (version N+1, not N+2), the loser gets a
    ConflictDetail. No silent clobber (R5 atomicity via BEGIN IMMEDIATE)."""
    with SqliteArtifactRegistry(db_path) as reg:
        art = _make_artifact(version=1, content_hash="h-init")
        reg.register_artifact(art, content="")
        writers = [uuid4() for _ in range(8)]
        for w in writers:
            reg.set_agent_state(art.id, w, MESIState.SHARED, tick=1)

        results: dict[UUID, object] = {}
        barrier = threading.Barrier(len(writers))

        def attempt(writer_id: UUID) -> None:
            barrier.wait()
            results[writer_id] = reg.commit_cas(
                art.id, writer_id, expected_version=1,
                content_hash=f"h-{writer_id.hex[:6]}", tick=5,
            )

        threads = [threading.Thread(target=attempt, args=(w,)) for w in writers]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        wins = [r for r in results.values() if isinstance(r, tuple)]
        conflicts = [r for r in results.values() if isinstance(r, ConflictDetail)]
        assert len(wins) == 1, f"expected exactly one win, got {len(wins)}"
        assert len(conflicts) == len(writers) - 1
        # Final version advanced by exactly one — no lost update / no clobber.
        assert reg.get_artifact(art.id).version == 2
        # Every loser saw version_mismatch (two OCC writers are both SHARED, so
        # the loser is elected by the version check, never other_holder).
        assert all(c.reason == "version_mismatch" for c in conflicts)
        assert all(c.current_version == 2 for c in conflicts)


# ----------------------------------------------------------------------
# Read-generation fence (Piece #2) — Unit 2: additive schema, no migration
# ----------------------------------------------------------------------


def _build_pre_fence_db(db_path: Path, art_id: str, ag_id: str) -> None:
    """Create a v1 state.db with the schema as it existed BEFORE the
    read-generation fence (no owner_generation / read_generation columns, no
    coordinator_epoch), with one artifact + one M-grant, user_version = 1."""
    import sqlite3

    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            "CREATE TABLE artifacts (id TEXT PRIMARY KEY, name TEXT NOT NULL UNIQUE, "
            "version INTEGER NOT NULL, content_hash TEXT NOT NULL, size_tokens INTEGER, "
            "last_writer_id TEXT, updated_at REAL NOT NULL)"
        )
        conn.execute("CREATE INDEX idx_artifacts_name ON artifacts(name)")
        conn.execute(
            "CREATE TABLE agent_states (artifact_id TEXT NOT NULL, agent_id TEXT NOT NULL, "
            "state TEXT NOT NULL, transient_state TEXT, transient_tick INTEGER, "
            "granted_at_tick INTEGER, last_reclaim_trigger TEXT, last_reclaim_tick INTEGER, "
            "PRIMARY KEY (artifact_id, agent_id), "
            "FOREIGN KEY (artifact_id) REFERENCES artifacts(id) ON DELETE CASCADE)"
        )
        conn.execute(
            "CREATE TABLE heartbeats (agent_id TEXT PRIMARY KEY, last_tick INTEGER NOT NULL)"
        )
        conn.execute("CREATE TABLE registry_meta (key TEXT PRIMARY KEY, value TEXT)")
        conn.execute(
            "INSERT INTO registry_meta (key, value) VALUES (?, ?), (?, ?)",
            ("instance_id", "fixed-instance", "sequence_number", "7"),
        )
        conn.execute(
            "INSERT INTO artifacts (id, name, version, content_hash, updated_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (art_id, "plan.md", 3, "deadbeef", 1.0),
        )
        conn.execute(
            "INSERT INTO agent_states (artifact_id, agent_id, state) VALUES (?, ?, ?)",
            (art_id, ag_id, "M"),
        )
        conn.execute("PRAGMA user_version = 1")
        conn.execute("COMMIT")
    finally:
        conn.close()


def test_fresh_db_has_fence_schema(db_path: Path) -> None:
    """A freshly initialized db carries the fence columns + coordinator_epoch,
    and (option (a)) does NOT advance user_version beyond the existing v1."""
    with SqliteArtifactRegistry(db_path) as reg:
        art_cols = {r[1] for r in reg._conn.execute("PRAGMA table_info(artifacts)").fetchall()}
        state_cols = {
            r[1] for r in reg._conn.execute("PRAGMA table_info(agent_states)").fetchall()
        }
        assert "owner_generation" in art_cols
        assert "read_generation" in state_cols
        assert reg._coordinator_epoch
        assert reg._conn.execute("PRAGMA user_version").fetchone()[0] == 1


def test_pre_fence_db_upgrades_in_place_additively(db_path: Path) -> None:
    """A v1 db created before the fence gains the columns + coordinator_epoch
    additively on open, WITHOUT a user_version bump; existing rows backfill to
    owner_generation = 0 / read_generation = NULL, and prior meta is preserved.
    Re-open is idempotent and the epoch is stable."""
    art_id, ag_id = str(uuid4()), str(uuid4())
    _build_pre_fence_db(db_path, art_id, ag_id)

    with SqliteArtifactRegistry(db_path) as reg:
        art_cols = {r[1] for r in reg._conn.execute("PRAGMA table_info(artifacts)").fetchall()}
        state_cols = {
            r[1] for r in reg._conn.execute("PRAGMA table_info(agent_states)").fetchall()
        }
        assert "owner_generation" in art_cols
        assert "read_generation" in state_cols
        # Existing rows backfill: artifact -> 0, grant -> NULL (absent operand).
        assert (
            reg._conn.execute(
                "SELECT owner_generation FROM artifacts WHERE id = ?", (art_id,)
            ).fetchone()[0]
            == 0
        )
        assert (
            reg._conn.execute(
                "SELECT read_generation FROM agent_states "
                "WHERE artifact_id = ? AND agent_id = ?",
                (art_id, ag_id),
            ).fetchone()[0]
            is None
        )
        # Epoch seeded + loaded; user_version NOT bumped (option (a)).
        assert reg._coordinator_epoch
        assert reg._conn.execute("PRAGMA user_version").fetchone()[0] == 1
        # Prior meta preserved (no clobber of instance_id / sequence_number).
        assert reg._instance_id == "fixed-instance"
        assert reg._seq == 7
        epoch1 = reg._coordinator_epoch

    # Idempotent re-open: no error, columns intact, epoch stable.
    with SqliteArtifactRegistry(db_path) as reg2:
        assert reg2._coordinator_epoch == epoch1
        art_cols2 = {
            r[1] for r in reg2._conn.execute("PRAGMA table_info(artifacts)").fetchall()
        }
        assert "owner_generation" in art_cols2


def test_owner_generation_bumps_on_reclaim_only(db_path: Path) -> None:
    """A sweep reclamation (M/E -> INVALID via a reclaim trigger) bumps the
    artifact's owner_generation monotonically; a peer-invalidation or a clean
    downgrade does NOT bump (version-CAS covers those)."""
    with SqliteArtifactRegistry(db_path) as reg:
        art = Artifact(id=uuid4(), name="plan.md", version=1, content_hash="h")
        reg.register_artifact(art, content="ignored")
        a, b = uuid4(), uuid4()
        assert reg.get_owner_generation(art.id) == 0

        reg.set_agent_state(art.id, a, MESIState.EXCLUSIVE, trigger="write", tick=1)
        reg.set_agent_state(art.id, a, MESIState.INVALID, trigger="reclaim_heartbeat", tick=10)
        assert reg.get_owner_generation(art.id) == 1  # reclaim bumps

        reg.set_agent_state(art.id, b, MESIState.EXCLUSIVE, trigger="write", tick=11)
        reg.set_agent_state(art.id, b, MESIState.INVALID, trigger="reclaim_max_hold", tick=20)
        assert reg.get_owner_generation(art.id) == 2  # monotonic across reclaims

        # Peer-invalidation (non-reclaim trigger) does NOT bump.
        reg.set_agent_state(art.id, a, MESIState.EXCLUSIVE, trigger="write", tick=21)
        reg.set_agent_state(art.id, a, MESIState.INVALID, trigger="peer_invalidation", tick=22)
        assert reg.get_owner_generation(art.id) == 2

        # Clean downgrade E -> SHARED does NOT bump.
        reg.set_agent_state(art.id, b, MESIState.EXCLUSIVE, trigger="write", tick=23)
        reg.set_agent_state(art.id, b, MESIState.SHARED, trigger="downgrade", tick=24)
        assert reg.get_owner_generation(art.id) == 2
