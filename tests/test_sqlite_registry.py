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
from ccs.core.types import Artifact


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
