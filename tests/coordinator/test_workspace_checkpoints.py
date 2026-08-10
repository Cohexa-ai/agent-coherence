# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Registry v5 — workspace-checkpoint tables + the v3->v4 literal-stamp fix.

WV plan Unit 2 (docs/plans/2026-08-08-001-feat-workspace-versioning-e2e-plan.md).
Covers, per the unit's test scenarios:

- fresh v5 init lands the complete schema (both checkpoint tables + the name
  index) at ``user_version=5``;
- **the v3 -> 4 -> 5 walk regression** (the migration trap): ``_migrate_v3_to_v4``
  used to guard AND stamp with the ``SCHEMA_USER_VERSION`` constant, so bumping
  the constant to 5 would have stamped a v3-origin db ``user_version=5`` WITHOUT
  the v5 DDL (the chained v4->v5 step's loser-guard then sees >= 5 and no-ops) —
  the exact state the structural foreign-ledger probe refuses. The walk test
  fails if the ``_V4_USER_VERSION`` literal conversion is ever reverted;
- v4 -> v5 preserves existing data (artifacts, retained bodies, session pins);
- a SIGKILL-simulation mid-migration (DDL executed, raise before the in-txn
  stamp lands) leaves a BOOTABLE pre-state — the single-transaction proof: the
  rolled-back DDL does not survive, and a clean reopen re-migrates;
- the foreign-lineage probes still refuse (the new "v5 without
  ``workspace_checkpoints``" probe + the pre-existing Node-v3 marker);
- checkpoint CRUD round-trips on BOTH registries (create/read/update, restore
  status, member restore progress, pin state, pin refcounts), owner-absent
  fail-closed, and cross-arm parity of the observable state.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Iterator
from uuid import UUID, uuid4

import pytest

from ccs.coordinator.registry import ArtifactRegistry
from ccs.coordinator.registry_protocol import CheckpointMember, CheckpointRecord
from ccs.coordinator.sqlite_registry import (
    SCHEMA_USER_VERSION,
    CrossRuntimeSchemaError,
    SqliteArtifactRegistry,
)
from ccs.core.exceptions import CROSS_RUNTIME_SCHEMA_REASON
from ccs.core.types import Artifact


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    return tmp_path / "state.db"


# Both-arms fixture: every CRUD scenario runs against the in-memory AND the
# sqlite registry — the parametrization IS the parity harness for this surface.
@pytest.fixture(params=["in_memory", "sqlite"])
def registry(
    request: pytest.FixtureRequest, tmp_path: Path
) -> "Iterator[ArtifactRegistry | SqliteArtifactRegistry]":
    if request.param == "in_memory":
        yield ArtifactRegistry()
    else:
        reg = SqliteArtifactRegistry(tmp_path / "parity-arm.db")
        yield reg
        reg.close()


# ---------------------------------------------------------------------------
# Raw-db helpers (probes + shape reverts; plain sqlite3, never the registry)
# ---------------------------------------------------------------------------


def _user_version(db_path: Path) -> int:
    conn = sqlite3.connect(str(db_path))
    try:
        return conn.execute("PRAGMA user_version").fetchone()[0]
    finally:
        conn.close()


def _tables(db_path: Path) -> set[str]:
    conn = sqlite3.connect(str(db_path))
    try:
        return {
            r[0]
            for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
    finally:
        conn.close()


def _indexes(db_path: Path) -> set[str]:
    conn = sqlite3.connect(str(db_path))
    try:
        return {
            r[0]
            for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='index'"
            ).fetchall()
        }
    finally:
        conn.close()


def _revert_to_v4_shape(db_path: Path) -> None:
    """Mutate a current (v5) db on disk back to the v4 shape: workspace tables
    + name index ABSENT, stamped ``user_version=4`` (what a pre-WV build
    produced)."""
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("DROP TABLE IF EXISTS workspace_checkpoint_members")
        conn.execute("DROP TABLE IF EXISTS workspace_checkpoints")
        conn.execute("DROP INDEX IF EXISTS idx_workspace_checkpoints_name")
        conn.execute("PRAGMA user_version = 4")
        conn.commit()
    finally:
        conn.close()


def _revert_to_v3_shape(db_path: Path) -> None:
    """Mutate a current (v5) db on disk back to the pre-fix v3 shape:
    ``session_pins`` present, ``session_meta`` + both workspace tables ABSENT,
    stamped ``user_version=3`` (what an early SB-17 branch commit produced —
    the migration origin the v5 trap would have mis-stamped)."""
    _revert_to_v4_shape(db_path)
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("DROP TABLE IF EXISTS session_meta")
        conn.execute("DROP INDEX IF EXISTS idx_session_pins_artifact")
        conn.execute("PRAGMA user_version = 3")
        conn.commit()
    finally:
        conn.close()


def _forge_node_v3_db(db_path: Path) -> None:
    """Forge a sibling-Node-ledger v3 db: the shared v1 DDL + the Node ledger's
    v3 marker (``agent_states.deadline_tick``) + ``user_version=3``. Compact
    mirror of the fuller forge in ``tests/test_sqlite_registry.py``."""
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute(
            "CREATE TABLE artifacts (id TEXT PRIMARY KEY, name TEXT NOT NULL UNIQUE, "
            "version INTEGER NOT NULL, content_hash TEXT NOT NULL, size_tokens INTEGER, "
            "last_writer_id TEXT, updated_at REAL NOT NULL)"
        )
        conn.execute(
            "CREATE TABLE agent_states (artifact_id TEXT NOT NULL, agent_id TEXT NOT NULL, "
            "state TEXT NOT NULL, PRIMARY KEY (artifact_id, agent_id))"
        )
        conn.execute("CREATE TABLE registry_meta (key TEXT PRIMARY KEY, value TEXT)")
        conn.execute("ALTER TABLE agent_states ADD COLUMN deadline_tick INTEGER")
        conn.execute("PRAGMA user_version = 3")
        conn.commit()
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Manifest builders
# ---------------------------------------------------------------------------


def _checkpoint(
    checkpoint_id: str = "cp-1",
    name: str = "pre-refactor",
    owner: UUID | None = None,
    created_at: float = 100.0,
) -> CheckpointRecord:
    return CheckpointRecord(
        checkpoint_id=checkpoint_id,
        name=name,
        owner=owner if owner is not None else uuid4(),
        created_at=created_at,
        created_at_tick=7,
        window_min=10.0,
        window_max=10.5,
    )


def _members() -> list[CheckpointMember]:
    return [
        CheckpointMember(
            member_path="src/plan.md",
            artifact_id=uuid4(),
            native_token="v42",
            fingerprint="sha256:aa",
            captured_at=10.1,
            arbitration_tier="no-arbiter",
            restore_tier="restorable",
        ),
        CheckpointMember(
            member_path="s3://bucket/report.csv",
            artifact_id=None,
            native_token='etag-"abc"',
            fingerprint="sha256:bb",
            captured_at=10.2,
            dirty_during_window=True,
            arbitration_tier="native-cas",
            restore_tier="restorable-unpinned",
        ),
        CheckpointMember(
            member_path="tmp/scratch.txt",
            artifact_id=None,
            native_token=None,
            fingerprint=None,
            captured_at=10.3,
            absent=True,
        ),
    ]


# ---------------------------------------------------------------------------
# Fresh init + migrations
# ---------------------------------------------------------------------------


def test_fresh_v5_init_complete(db_path: Path) -> None:
    """A fresh db is created at v5 directly: both workspace-checkpoint tables +
    the name index present, ``user_version=5`` — no migration shim ever runs."""
    with SqliteArtifactRegistry(db_path) as reg:
        assert reg._conn.execute("PRAGMA user_version").fetchone()[0] == 5
    assert SCHEMA_USER_VERSION == 5
    tables = _tables(db_path)
    assert "workspace_checkpoints" in tables
    assert "workspace_checkpoint_members" in tables
    assert "idx_workspace_checkpoints_name" in _indexes(db_path)


def test_v3_to_v4_to_v5_walk_lands_v5_ddl_at_v5_stamp(db_path: Path) -> None:
    """THE TRAP REGRESSION (WV plan Unit 2 / review P1). A v3-origin db must
    walk 3 -> 4 -> 5 and end with the v5 DDL present AT the v5 stamp.

    Before the ``_V4_USER_VERSION`` literal conversion, ``_migrate_v3_to_v4``
    guarded and stamped with the ``SCHEMA_USER_VERSION`` constant: with the
    constant at 5, the v3->v4 step would stamp 5 directly, the chained v4->v5
    step's loser-guard would see >= 5 and no-op, and the db would carry
    ``user_version=5`` WITHOUT ``workspace_checkpoints`` — the exact state the
    structural probe refuses (so the NEXT open would brick with a
    cross-runtime error). This test fails if that conversion is reverted.
    """
    with SqliteArtifactRegistry(db_path, retain_versions=True) as reg:
        art = Artifact(id=uuid4(), name="plan.md", version=1, content_hash="h1")
        reg.register_artifact(art, "seed-body")
    _revert_to_v3_shape(db_path)
    assert _user_version(db_path) == 3
    assert "session_meta" not in _tables(db_path)
    assert "workspace_checkpoints" not in _tables(db_path)

    with SqliteArtifactRegistry(db_path, retain_versions=True) as reg:
        # The v5 stamp AND the v5 DDL, together — the trap produced the stamp
        # without the DDL.
        assert reg._conn.execute("PRAGMA user_version").fetchone()[0] == 5
        tables = {
            r[0]
            for r in reg._conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        assert "workspace_checkpoints" in tables
        assert "workspace_checkpoint_members" in tables
        # The intermediate v4 surface landed too (the chain did not skip a step).
        assert "session_meta" in tables
        # The new tables are USABLE, not just present.
        reg.create_checkpoint(_checkpoint(), _members())
        assert reg.get_checkpoint("cp-1") is not None
    # And the migrated db re-opens write-free at v5 (no bricking on reopen —
    # the trap's failure mode surfaced on the SECOND open).
    with SqliteArtifactRegistry(db_path, retain_versions=True) as reg:
        assert reg.get_checkpoint("cp-1") is not None


def test_v4_to_v5_preserves_existing_data(db_path: Path) -> None:
    """The v4->v5 step is additive-only: artifacts, retained bodies, session
    pins, and session owner-binding all survive, and the new tables work."""
    owner = uuid4()
    with SqliteArtifactRegistry(db_path, retain_versions=True) as reg:
        art = Artifact(id=uuid4(), name="plan.md", version=1, content_hash="h1")
        reg.register_artifact(art, "seed-body")
        cut = reg.capture_version_vector(
            [art.id], "tok-1", owner=owner, created_at_tick=3
        )
        assert cut == {art.id: 1}
    _revert_to_v4_shape(db_path)
    assert _user_version(db_path) == 4

    with SqliteArtifactRegistry(db_path, retain_versions=True) as reg:
        assert reg._conn.execute("PRAGMA user_version").fetchone()[0] == 5
        # Pre-migration data intact.
        got = reg.get_artifact(art.id)
        assert got is not None and got.version == 1 and got.content_hash == "h1"
        assert reg.get_content_at_version(art.id, 1) == "seed-body"
        assert reg.get_session_cut("tok-1") == {art.id: 1}
        assert reg.get_session_meta("tok-1") == (owner, 3)
        # New surface live.
        reg.create_checkpoint(_checkpoint(), _members())
        assert len(reg.get_checkpoint_members("cp-1")) == 3


def test_sigkill_mid_v4_to_v5_migration_leaves_bootable_pre_state(
    db_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Single-transaction proof: crash the migration AFTER its DDL executed but
    BEFORE the in-txn stamp lands. The rollback must discard the DDL (one
    ``BEGIN IMMEDIATE`` wraps DDL + stamp), leaving a bootable v4 db that a
    clean reopen re-migrates without 'table already exists'."""
    with SqliteArtifactRegistry(db_path, retain_versions=True) as reg:
        art = Artifact(id=uuid4(), name="plan.md", version=1, content_hash="h1")
        reg.register_artifact(art, "seed-body")
    _revert_to_v4_shape(db_path)

    real_connect = sqlite3.connect
    opened: list[sqlite3.Connection] = []

    class _CrashBeforeStamp:
        """Connection proxy that simulates a SIGKILL at the v5 stamp: the DDL
        statements before it have executed inside the open BEGIN IMMEDIATE."""

        def __init__(self, conn: sqlite3.Connection) -> None:
            object.__setattr__(self, "_conn", conn)

        def execute(self, sql: str, *args: object) -> object:
            if sql.strip().startswith("PRAGMA user_version = 5"):
                raise sqlite3.OperationalError(
                    "simulated SIGKILL before the v5 stamp"
                )
            return self._conn.execute(sql, *args)

        def __getattr__(self, name: str) -> object:
            return getattr(self._conn, name)

    def crashing_connect(*args: object, **kwargs: object) -> _CrashBeforeStamp:
        conn = real_connect(*args, **kwargs)  # type: ignore[arg-type]
        opened.append(conn)
        return _CrashBeforeStamp(conn)

    with monkeypatch.context() as mp:
        mp.setattr(
            "ccs.coordinator.sqlite_registry.sqlite3.connect", crashing_connect
        )
        with pytest.raises(sqlite3.OperationalError, match="simulated SIGKILL"):
            SqliteArtifactRegistry(db_path)
    for conn in opened:
        conn.close()

    # Pre-state bootable and UNCHANGED: still v4, and the workspace DDL that ran
    # before the crash did NOT survive (it was inside the rolled-back txn).
    assert _user_version(db_path) == 4
    assert "workspace_checkpoints" not in _tables(db_path)
    assert "workspace_checkpoint_members" not in _tables(db_path)

    # A clean reopen completes the migration (idempotent, no
    # "table already exists"), with the pre-migration data intact.
    with SqliteArtifactRegistry(db_path, retain_versions=True) as reg:
        assert reg._conn.execute("PRAGMA user_version").fetchone()[0] == 5
        got = reg.get_artifact(art.id)
        assert got is not None and got.content_hash == "h1"
        reg.create_checkpoint(_checkpoint(), [])
        assert reg.get_checkpoint("cp-1") is not None


# ---------------------------------------------------------------------------
# Foreign-lineage refusal (probes intact + the new v5 structural probe)
# ---------------------------------------------------------------------------


def test_v5_stamp_without_checkpoint_tables_is_refused(db_path: Path) -> None:
    """The new structural probe: ``user_version=5`` WITHOUT
    ``workspace_checkpoints`` is exactly the state the migration trap would have
    produced (and no ledger this build recognizes stamps it) — the open must
    fail CLOSED with the typed cross-runtime error and touch nothing."""
    with SqliteArtifactRegistry(db_path):
        pass
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("DROP TABLE IF EXISTS workspace_checkpoint_members")
        conn.execute("DROP TABLE IF EXISTS workspace_checkpoints")
        conn.commit()  # user_version stays 5
    finally:
        conn.close()

    with pytest.raises(CrossRuntimeSchemaError) as excinfo:
        SqliteArtifactRegistry(db_path)
    assert excinfo.value.reason == CROSS_RUNTIME_SCHEMA_REASON
    # Fail-closed means UNTOUCHED: stamp and shape exactly as forged.
    assert _user_version(db_path) == 5
    assert "workspace_checkpoints" not in _tables(db_path)

    # The read-only open path runs the same probes.
    with pytest.raises(CrossRuntimeSchemaError):
        SqliteArtifactRegistry(db_path, read_only=True)


def test_node_v3_db_still_refused(db_path: Path) -> None:
    """The pre-existing Node-ledger marker (``agent_states.deadline_tick`` at
    ``user_version>=3``) still refuses after the v5 bump — the probe chain was
    extended, not rearranged."""
    _forge_node_v3_db(db_path)
    with pytest.raises(CrossRuntimeSchemaError):
        SqliteArtifactRegistry(db_path)
    assert _user_version(db_path) == 3  # untouched


# ---------------------------------------------------------------------------
# CRUD round-trips — BOTH registries (the parametrized arms are the parity run)
# ---------------------------------------------------------------------------


def test_create_and_read_roundtrip(registry) -> None:
    checkpoint = _checkpoint()
    members = _members()
    registry.create_checkpoint(checkpoint, members)

    assert registry.get_checkpoint("cp-1") == checkpoint
    got = registry.get_checkpoint_members("cp-1")
    # Ordered by member_path; every fixed-width field round-trips by value
    # (bools stay bools, None stays None) — dataclass equality covers all.
    assert got == sorted(members, key=lambda m: m.member_path)


def test_unknown_checkpoint_reads_empty(registry) -> None:
    assert registry.get_checkpoint("nope") is None
    assert registry.get_checkpoint_members("nope") == []
    assert registry.list_checkpoints() == []


def test_list_checkpoints_ordered_by_created_at(registry) -> None:
    later = _checkpoint(checkpoint_id="cp-later", name="b", created_at=200.0)
    earlier = _checkpoint(checkpoint_id="cp-earlier", name="a", created_at=50.0)
    registry.create_checkpoint(later, [])
    registry.create_checkpoint(earlier, [])
    assert [c.checkpoint_id for c in registry.list_checkpoints()] == [
        "cp-earlier",
        "cp-later",
    ]


def test_duplicate_checkpoint_id_rejected(registry) -> None:
    registry.create_checkpoint(_checkpoint(), _members())
    with pytest.raises(ValueError):
        registry.create_checkpoint(_checkpoint(name="other-name"), [])
    # Original manifest intact.
    original = registry.get_checkpoint("cp-1")
    assert original is not None and original.name == "pre-refactor"
    assert len(registry.get_checkpoint_members("cp-1")) == 3


def test_duplicate_member_path_rejects_whole_manifest(registry) -> None:
    """Create is all-or-nothing: a duplicate member path rejects the WHOLE
    manifest — no header row, no partial member set."""
    member = _members()[0]
    with pytest.raises(ValueError):
        registry.create_checkpoint(_checkpoint(), [member, member])
    assert registry.get_checkpoint("cp-1") is None
    assert registry.get_checkpoint_members("cp-1") == []


def test_owner_absent_fail_closed(registry) -> None:
    """An ownerless manifest is never persisted (WV Unit 2 fail-closed rule:
    owner metadata rides the same transaction as the checkpoint row, so a
    missing owner must reject BEFORE anything lands)."""
    ownerless = CheckpointRecord(
        checkpoint_id="cp-1",
        name="pre-refactor",
        owner=None,  # type: ignore[arg-type]
        created_at=100.0,
        created_at_tick=7,
        window_min=10.0,
        window_max=10.5,
    )
    with pytest.raises(ValueError, match="owner"):
        registry.create_checkpoint(ownerless, _members())
    assert registry.get_checkpoint("cp-1") is None
    assert registry.get_checkpoint_members("cp-1") == []


def test_restore_status_update_roundtrip(registry) -> None:
    registry.create_checkpoint(_checkpoint(), [])
    registry.set_checkpoint_restore_status("cp-1", "in_progress", updated_at=42.0)
    got = registry.get_checkpoint("cp-1")
    assert got is not None
    assert got.restore_status == "in_progress"
    assert got.restore_updated_at == 42.0
    # The rest of the header is untouched.
    assert got.name == "pre-refactor" and got.pin_refcount == 0


def test_restore_status_unknown_checkpoint_raises(registry) -> None:
    with pytest.raises(KeyError):
        registry.set_checkpoint_restore_status("nope", "in_progress", updated_at=1.0)


def test_member_restore_progress_roundtrip(registry) -> None:
    registry.create_checkpoint(_checkpoint(), _members())
    registry.set_checkpoint_member_restore(
        "cp-1", "src/plan.md", restore_outcome="restored"
    )
    registry.set_checkpoint_member_restore(
        "cp-1",
        "tmp/scratch.txt",
        restore_outcome="deleted",
        deleted_at_restore=77.5,
    )
    by_path = {m.member_path: m for m in registry.get_checkpoint_members("cp-1")}
    assert by_path["src/plan.md"].restore_outcome == "restored"
    assert by_path["src/plan.md"].deleted_at_restore is None
    assert by_path["tmp/scratch.txt"].restore_outcome == "deleted"
    assert by_path["tmp/scratch.txt"].deleted_at_restore == 77.5
    # Untouched member unchanged.
    assert by_path["s3://bucket/report.csv"].restore_outcome is None


def test_member_restore_unknown_member_raises(registry) -> None:
    registry.create_checkpoint(_checkpoint(), _members())
    with pytest.raises(KeyError):
        registry.set_checkpoint_member_restore(
            "cp-1", "not-a-member", restore_outcome="restored"
        )
    with pytest.raises(KeyError):
        registry.set_checkpoint_member_restore(
            "cp-nope", "src/plan.md", restore_outcome="restored"
        )


def test_member_pin_update_preserves_tier_when_none(registry) -> None:
    registry.create_checkpoint(_checkpoint(), _members())
    registry.set_checkpoint_member_pin("cp-1", "src/plan.md", pin_state="pinned")
    by_path = {m.member_path: m for m in registry.get_checkpoint_members("cp-1")}
    assert by_path["src/plan.md"].pin_state == "pinned"
    assert by_path["src/plan.md"].restore_tier == "restorable"  # untouched


def test_member_pin_failure_downgrades_tier_loudly(registry) -> None:
    """The Unit-6 loud downgrade: a failed pin leg rewrites the member's tier to
    ``restorable-unpinned`` in the same step as the pin-state update."""
    registry.create_checkpoint(_checkpoint(), _members())
    registry.set_checkpoint_member_pin(
        "cp-1",
        "src/plan.md",
        pin_state="pin_failed",
        restore_tier="restorable-unpinned",
    )
    by_path = {m.member_path: m for m in registry.get_checkpoint_members("cp-1")}
    assert by_path["src/plan.md"].pin_state == "pin_failed"
    assert by_path["src/plan.md"].restore_tier == "restorable-unpinned"


def test_member_pin_unknown_member_raises(registry) -> None:
    registry.create_checkpoint(_checkpoint(), [])
    with pytest.raises(KeyError):
        registry.set_checkpoint_member_pin("cp-1", "nope", pin_state="pinned")


def test_pin_refcount_inc_dec_roundtrip(registry) -> None:
    registry.create_checkpoint(_checkpoint(), [])
    assert registry.adjust_checkpoint_pin_refcount("cp-1", +1) == 1
    assert registry.adjust_checkpoint_pin_refcount("cp-1", +2) == 3
    assert registry.adjust_checkpoint_pin_refcount("cp-1", -3) == 0
    got = registry.get_checkpoint("cp-1")
    assert got is not None and got.pin_refcount == 0


def test_pin_refcount_never_negative(registry) -> None:
    """A release without a matching pin is a bookkeeping bug — fail-closed, and
    the stored count is unchanged by the failed adjustment."""
    registry.create_checkpoint(_checkpoint(), [])
    registry.adjust_checkpoint_pin_refcount("cp-1", +1)
    with pytest.raises(ValueError):
        registry.adjust_checkpoint_pin_refcount("cp-1", -2)
    got = registry.get_checkpoint("cp-1")
    assert got is not None and got.pin_refcount == 1


def test_pin_refcount_unknown_checkpoint_raises(registry) -> None:
    with pytest.raises(KeyError):
        registry.adjust_checkpoint_pin_refcount("nope", +1)


# ---------------------------------------------------------------------------
# Restart-survival divergence + explicit cross-arm parity
# ---------------------------------------------------------------------------


def test_sqlite_checkpoint_survives_reopen(db_path: Path) -> None:
    """Manifests are restart-durable on the sqlite arm (the point of homing the
    manifest in registry v5): a full CRUD state survives close + reopen."""
    checkpoint = _checkpoint()
    with SqliteArtifactRegistry(db_path) as reg:
        reg.create_checkpoint(checkpoint, _members())
        reg.set_checkpoint_restore_status("cp-1", "completed", updated_at=9.0)
        reg.adjust_checkpoint_pin_refcount("cp-1", +2)
        reg.set_checkpoint_member_restore(
            "cp-1", "src/plan.md", restore_outcome="restored"
        )
    with SqliteArtifactRegistry(db_path) as reg:
        got = reg.get_checkpoint("cp-1")
        assert got is not None
        assert got.restore_status == "completed"
        assert got.pin_refcount == 2
        assert got.owner == checkpoint.owner
        by_path = {m.member_path: m for m in reg.get_checkpoint_members("cp-1")}
        assert by_path["src/plan.md"].restore_outcome == "restored"


def test_inmemory_checkpoints_are_process_scoped() -> None:
    """The asserted divergence (mirrors the session-pin rule): a fresh in-memory
    registry has no manifests — restart-survival is sqlite-only."""
    reg = ArtifactRegistry()
    reg.create_checkpoint(_checkpoint(), _members())
    assert reg.get_checkpoint("cp-1") is not None
    fresh = ArtifactRegistry()  # the "restart"
    assert fresh.get_checkpoint("cp-1") is None
    assert fresh.list_checkpoints() == []


def test_parity_same_sequence_same_observable_state(tmp_path: Path) -> None:
    """Run one CRUD sequence against BOTH arms and compare the observable end
    state member-by-member — the shared frozen dataclasses make cross-arm
    equality exact (identical records, identical ordering)."""
    owner = uuid4()
    members = _members()  # ONE member list — both arms store identical rows
    arms: "list[ArtifactRegistry | SqliteArtifactRegistry]" = [
        ArtifactRegistry(),
        SqliteArtifactRegistry(tmp_path / "parity.db"),
    ]
    try:
        for reg in arms:
            reg.create_checkpoint(_checkpoint(owner=owner), members)
            reg.create_checkpoint(
                _checkpoint(checkpoint_id="cp-2", name="later", owner=owner,
                            created_at=200.0),
                [],
            )
            reg.set_checkpoint_restore_status("cp-1", "in_progress", updated_at=5.0)
            reg.set_checkpoint_member_restore(
                "cp-1", "tmp/scratch.txt",
                restore_outcome="deleted", deleted_at_restore=6.0,
            )
            reg.set_checkpoint_member_pin(
                "cp-1", "src/plan.md",
                pin_state="pinned",
            )
            reg.adjust_checkpoint_pin_refcount("cp-1", +1)
        inmem, sqlite_arm = arms
        assert inmem.list_checkpoints() == sqlite_arm.list_checkpoints()
        assert inmem.get_checkpoint("cp-1") == sqlite_arm.get_checkpoint("cp-1")
        assert inmem.get_checkpoint_members("cp-1") == sqlite_arm.get_checkpoint_members("cp-1")
        assert inmem.get_checkpoint_members("cp-2") == sqlite_arm.get_checkpoint_members("cp-2")
    finally:
        arms[1].close()  # type: ignore[union-attr]
