# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""SQLite-WAL persistent artifact registry for cross-process coherence coordination.

This module is the persistence layer for the Claude Code coherence plugin
(per docs/plans/2026-05-13-001-feat-claude-code-coherence-plugin-v0.1-plan.md
Phase A Unit 1). It is a drop-in replacement for :class:`ArtifactRegistry`
that survives process restarts and is safe for multi-threaded access from
:class:`http.server.ThreadingHTTPServer` handler threads.

Contract divergence from in-memory ``ArtifactRegistry`` (per plan KTD-13):

  This registry does NOT persist artifact content. Only ``content_hash`` is
  stored. ``get_content(artifact_id)`` returns ``b""`` (empty bytes) for
  known artifacts and ``None`` for unknown. This is deliberate — the plugin's
  hot path (Unit 4 HTTP hook handlers) never calls
  ``CoordinatorService.fetch``; it uses ``resolve_or_register`` / ``write`` /
  ``commit`` / ``invalidate`` directly. Avoiding content storage shrinks the
  disclosure surface if ``.coherence/state.db`` is accidentally committed to
  git (KTD-13 defense-in-depth).

  The duck-typing parity test scoped for v0.1 covers the methods the plugin
  actually exercises; ``tests/test_coordinator.py`` patterns that exercise
  content-fetch semantics are NOT a v0.1 goal for this storage layer.

Schema (KTD-3, applied via ``PRAGMA user_version`` on init):

  PRAGMA user_version = 1;
  CREATE TABLE artifacts (
    id              TEXT PRIMARY KEY,        -- UUID hex
    name            TEXT NOT NULL UNIQUE,    -- parent-repo-relative path
    version         INTEGER NOT NULL,
    content_hash    TEXT NOT NULL,
    size_tokens     INTEGER,
    last_writer_id  TEXT,                    -- agent UUID
    updated_at      REAL NOT NULL            -- coordinator epoch seconds
  );
  CREATE INDEX idx_artifacts_name ON artifacts(name);
  CREATE TABLE agent_states (
    artifact_id     TEXT NOT NULL,
    agent_id        TEXT NOT NULL,
    state           TEXT NOT NULL,           -- MESIState enum value
    transient_state TEXT,
    transient_tick  INTEGER,
    granted_at_tick INTEGER,
    last_reclaim_trigger TEXT,
    last_reclaim_tick INTEGER,
    PRIMARY KEY (artifact_id, agent_id)
  );
  CREATE TABLE heartbeats (
    agent_id   TEXT PRIMARY KEY,
    last_tick  INTEGER NOT NULL
  );
  CREATE TABLE registry_meta (
    key   TEXT PRIMARY KEY,
    value TEXT
  );

Thread safety: process-level RLock guards all mutating methods. SQLite
connection opened with ``check_same_thread=False`` so handler threads share
one connection (mutation-then-log invariant preserved by RLock + BEGIN
IMMEDIATE transactions).
"""

from __future__ import annotations

import sqlite3
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterable, Optional, TypeAlias
from uuid import UUID, uuid4

from ccs.core.states import MESIState, TransientState
from ccs.core.types import Artifact

CCS_STATE_LOG_SCHEMA_VERSION = "ccs.state_log.v2"
"""Reuses the same schema version as in-memory registry (state_log emissions
are interchangeable from a downstream consumer's perspective)."""

SCHEMA_USER_VERSION = 1
"""Single-version guard per simplified migration framework (plan KTD-3 update).
v0.1 raises on mismatch; v0.2 will add real migration dispatch when needed."""

ReclamationSlot: TypeAlias = tuple[str, int]
_M_OR_E_STATES: frozenset[MESIState] = frozenset({MESIState.MODIFIED, MESIState.EXCLUSIVE})


class SchemaVersionError(RuntimeError):
    """Raised when an existing ``state.db`` carries an unexpected user_version.

    v0.1 ships a single-version guard rather than a full migration framework
    (per plan KTD-3 simplification). When v0.2 adds schema columns for
    strict-mode retry counters, a real migration dispatch lands here and
    this exception graduates to the not-yet-migrated branch.
    """


@dataclass(frozen=True)
class _ArtifactRow:
    """Internal row decoded from the artifacts table."""

    artifact: Artifact
    last_writer_id: Optional[UUID]


class SqliteArtifactRegistry:
    """SQLite-WAL persistent registry — drop-in for :class:`ArtifactRegistry`.

    Preserves the public surface of ``ArtifactRegistry`` for methods the
    plugin actually exercises (per KTD-13 contract divergence note above).
    Adds two methods needed by Unit 4 HTTP handlers: ``resolve_or_register``
    (KTD-9 first-observation seeding) and ``artifacts_held_by_agent``
    (KTD-11 session-stop release iteration).
    """

    def __init__(
        self,
        db_path: str | Path,
        *,
        state_log: Callable[[dict[str, Any]], None] | None = None,
        agent_names: dict[UUID, str] | None = None,
        instance_id: str | None = None,
        retain_versions: bool = False,
    ) -> None:
        if state_log is not None and instance_id is None:
            raise ValueError(
                "instance_id must be provided when state_log is set; "
                "pass instance_id=str(uuid4()) or route through the plugin coordinator "
                "which manages instance_id persistence in registry_meta"
            )

        # P2 ce-review fix #5 (maintainability): retain_versions=True was
        # silently ignored, diverging from ArtifactRegistry's contract
        # where True enables version-history queries. Raising here makes
        # the divergence loud — callers expecting history get a clear
        # error rather than silent None returns from get_content_at_version().
        # retain_versions=False (the default) is the only supported value
        # in v0.1; full version history is a v0.2 audit-trail feature.
        if retain_versions:
            raise NotImplementedError(
                "SqliteArtifactRegistry does not yet support retain_versions=True. "
                "Version history is a v0.2 audit feature; v0.1 only stores the "
                "latest version per artifact. Use the in-memory ArtifactRegistry "
                "if you need version history, or wait for v0.2."
            )

        self._db_path = Path(db_path)
        self._db_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        self._lock = threading.RLock()

        # check_same_thread=False because ThreadingHTTPServer handler threads
        # all share one registry instance. Coupled with the RLock on every
        # mutating method, this preserves the mutation-then-log invariant.
        self._conn = sqlite3.connect(
            str(self._db_path),
            check_same_thread=False,
            isolation_level=None,  # autocommit; we manage transactions explicitly
        )
        # WAL for concurrent readers + bounded busy_timeout for write contention.
        # synchronous=NORMAL is durable enough for non-financial use.
        #
        # busy_timeout=1500ms per v0.1.1 KTD-K REVISED ordering rule (lowered
        # from 2000ms). SQLite's busy_timeout is per-LOCK-ACQUISITION retry
        # budget, NOT per-transaction. A `write` transaction issues two lock
        # acquisitions (BEGIN IMMEDIATE acquires RESERVED + COMMIT promotes to
        # EXCLUSIVE), each consuming up to busy_timeout. Under sustained
        # contention, the prior 2000ms could compose to 4000ms cumulative —
        # equal to or above the 4s handler watchdog ceiling, racing the
        # SQLITE_BUSY return against the FuturesTimeout. The corrected formula
        # `busy_timeout ≤ (HANDLER_DEADLINE_SEC − safety) / max_lock_acquisitions`
        # gives `(4s − 0.5s safety) / 2 = 1.75s`; round down to 1500ms with
        # additional safety margin against multi-statement transactions that
        # may carry >2 lock acquisitions. See v0.1.1 plan KTD-K + the prior
        # version of this constant; do NOT raise without re-deriving against
        # the worst-case-lock-acquisition count for the hot path.
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA busy_timeout=1500")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute("PRAGMA foreign_keys=ON")

        self._initialize_schema(instance_id)
        self._state_log = state_log
        self._agent_names = agent_names
        self._retain_versions = retain_versions
        # In-memory tracking only; not persisted across restarts.
        # version_history-by-tick is a v0.2 audit feature, not v0.1.

    # ------------------------------------------------------------------
    # Schema lifecycle
    # ------------------------------------------------------------------

    def _initialize_schema(self, instance_id: str | None) -> None:
        """Apply schema or verify existing user_version; seed registry_meta."""
        with self._lock:
            current = self._conn.execute("PRAGMA user_version").fetchone()[0]
            if current == 0:
                # Fresh database — apply v1 schema.
                self._apply_v1_schema(instance_id)
            elif current == SCHEMA_USER_VERSION:
                # Existing v1 database — rehydrate _instance_id and _seq.
                self._rehydrate_meta(instance_id)
            else:
                raise SchemaVersionError(
                    f"unexpected schema version {current}; v0.1 expects {SCHEMA_USER_VERSION}. "
                    f"Delete {self._db_path} and restart, or upgrade to a plugin version "
                    f"that supports this schema."
                )

    def _apply_v1_schema(self, instance_id: str | None) -> None:
        """Create tables, indexes, and seed registry_meta. Caller holds lock.

        P1 ce-review fix (correctness): schema init must be atomic against
        SIGKILL. Earlier version ran executescript() THEN a separate PRAGMA
        user_version — SIGKILL between left user_version=0 with all tables
        present → next startup hit "table already exists" → DB permanently
        unbootable until manual rm.

        executescript() commits each statement in autocommit, so embedding
        the PRAGMA in the script only NARROWS the window — doesn't close it.
        The truly atomic fix is an explicit BEGIN IMMEDIATE / COMMIT wrapping
        all DDL + meta seed + PRAGMA user_version (which IS transactional
        when issued inside an explicit transaction per SQLite docs).
        """
        c = self._conn
        seed_id = instance_id if instance_id is not None else str(uuid4())
        # Use individual execute() calls (NOT executescript) so the explicit
        # BEGIN IMMEDIATE transaction is honored uniformly across Python
        # versions — executescript() has version-dependent quirks around
        # auto-committing on entry.
        c.execute("BEGIN IMMEDIATE")
        try:
            c.execute(
                """
                CREATE TABLE artifacts (
                    id              TEXT PRIMARY KEY,
                    name            TEXT NOT NULL UNIQUE,
                    version         INTEGER NOT NULL,
                    content_hash    TEXT NOT NULL,
                    size_tokens     INTEGER,
                    last_writer_id  TEXT,
                    updated_at      REAL NOT NULL
                )
                """
            )
            c.execute("CREATE INDEX idx_artifacts_name ON artifacts(name)")
            c.execute(
                """
                CREATE TABLE agent_states (
                    artifact_id          TEXT NOT NULL,
                    agent_id             TEXT NOT NULL,
                    state                TEXT NOT NULL,
                    transient_state      TEXT,
                    transient_tick       INTEGER,
                    granted_at_tick      INTEGER,
                    last_reclaim_trigger TEXT,
                    last_reclaim_tick    INTEGER,
                    PRIMARY KEY (artifact_id, agent_id),
                    FOREIGN KEY (artifact_id) REFERENCES artifacts(id) ON DELETE CASCADE
                )
                """
            )
            c.execute(
                """
                CREATE TABLE heartbeats (
                    agent_id   TEXT PRIMARY KEY,
                    last_tick  INTEGER NOT NULL
                )
                """
            )
            c.execute(
                """
                CREATE TABLE registry_meta (
                    key   TEXT PRIMARY KEY,
                    value TEXT
                )
                """
            )
            # A1: preemption notices. When one agent invalidates another's
            # M∪E grant (via CoordinatorService.write), the victim gets a
            # pending notice that surfaces on their next pre-read / pre-edit
            # hook. PRIMARY KEY (agent_id, artifact_id) means a second
            # preemption on the same (victim, artifact) UPSERTs — the
            # latest preempter wins, which is the right UX.
            c.execute(
                """
                CREATE TABLE pending_notices (
                    agent_id              TEXT NOT NULL,
                    artifact_id           TEXT NOT NULL,
                    preempter_agent_id    TEXT NOT NULL,
                    preempted_at_unix_ts  REAL NOT NULL,
                    PRIMARY KEY (agent_id, artifact_id),
                    FOREIGN KEY (artifact_id) REFERENCES artifacts(id) ON DELETE CASCADE
                )
                """
            )
            c.execute(
                "INSERT INTO registry_meta (key, value) VALUES (?, ?), (?, ?)",
                ("instance_id", seed_id, "sequence_number", "0"),
            )
            # PRAGMA is transactional inside an explicit BEGIN; cannot take
            # parameter bindings, so SCHEMA_USER_VERSION (int constant) is
            # interpolated directly.
            c.execute(f"PRAGMA user_version = {SCHEMA_USER_VERSION}")
            c.execute("COMMIT")
        except BaseException:
            # BaseException catches KeyboardInterrupt mid-init too so the
            # partial state doesn't poison the next start.
            try:
                c.execute("ROLLBACK")
            except Exception:
                pass
            raise
        self._instance_id = seed_id
        self._seq = 0

    def _rehydrate_meta(self, instance_id_override: str | None) -> None:
        """Load _instance_id + _seq from registry_meta. Caller holds lock."""
        rows = dict(
            self._conn.execute(
                "SELECT key, value FROM registry_meta WHERE key IN ('instance_id', 'sequence_number')"
            ).fetchall()
        )
        if "instance_id" not in rows or "sequence_number" not in rows:
            raise SchemaVersionError(
                f"registry_meta is missing required keys at {self._db_path}; "
                f"the database may be corrupted. Delete and restart to rehydrate."
            )
        # Caller's explicit instance_id wins (rare; typically used in tests).
        self._instance_id = instance_id_override or rows["instance_id"]
        self._seq = int(rows["sequence_number"])
        # A1 forward-compat: ensure pending_notices exists even on dbs
        # initialized before this table was added. PRAGMA user_version stays
        # at 1; this is an additive change that doesn't warrant a migration.
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS pending_notices (
                agent_id              TEXT NOT NULL,
                artifact_id           TEXT NOT NULL,
                preempter_agent_id    TEXT NOT NULL,
                preempted_at_unix_ts  REAL NOT NULL,
                PRIMARY KEY (agent_id, artifact_id),
                FOREIGN KEY (artifact_id) REFERENCES artifacts(id) ON DELETE CASCADE
            )
            """
        )

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------

    def close(self) -> None:
        """Close the underlying SQLite connection. Idempotent."""
        with self._lock:
            try:
                self._conn.close()
            except sqlite3.ProgrammingError:
                pass

    def __enter__(self) -> "SqliteArtifactRegistry":
        return self

    def __exit__(self, *exc_info: Any) -> None:
        self.close()

    # ------------------------------------------------------------------
    # ArtifactRegistry surface — artifact CRUD
    # ------------------------------------------------------------------

    def register_artifact(self, artifact: Artifact, content: str) -> None:
        """Insert artifact record. Content is hashed by the caller and stored
        only as ``content_hash`` (KTD-13); the ``content`` parameter is
        accepted for signature compatibility but its bytes are discarded."""
        del content  # KTD-13: not persisted. caller must pre-compute content_hash on artifact.
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                self._conn.execute(
                    """
                    INSERT INTO artifacts (id, name, version, content_hash, size_tokens, last_writer_id, updated_at)
                    VALUES (?, ?, ?, ?, ?, NULL, ?)
                    """,
                    (
                        artifact.id.hex,
                        artifact.name,
                        artifact.version,
                        artifact.content_hash or "",
                        artifact.size_tokens,
                        time.time(),
                    ),
                )
                self._conn.execute("COMMIT")
            except BaseException:
                # P2 ce-review fix #14 (kieran-python): BaseException catches
                # KeyboardInterrupt/SystemExit mid-transaction so ROLLBACK fires
                # before propagation — otherwise the connection is left with an
                # uncommitted transaction that the next BEGIN IMMEDIATE sees.
                self._conn.execute("ROLLBACK")
                raise

    def has_artifact(self, artifact_id: UUID) -> bool:
        """Return whether an artifact exists in registry."""
        with self._lock:
            row = self._conn.execute(
                "SELECT 1 FROM artifacts WHERE id = ?", (artifact_id.hex,)
            ).fetchone()
        return row is not None

    def artifact_ids(self) -> list[UUID]:
        """Return all known artifact ids."""
        with self._lock:
            rows = self._conn.execute("SELECT id FROM artifacts").fetchall()
        return [UUID(hex=r[0]) for r in rows]

    def get_artifact(self, artifact_id: UUID) -> Optional[Artifact]:
        """Return artifact metadata if present."""
        row = self._fetch_artifact_row(artifact_id)
        return row.artifact if row else None

    def get_content(self, artifact_id: UUID) -> Optional[bytes]:
        """KTD-13 contract divergence: returns ``b""`` for known artifacts
        and ``None`` for unknown. SqliteArtifactRegistry does not persist
        content. The plugin hot path never calls fetch(), so this signature
        is preserved only for duck-typing safety."""
        return b"" if self.has_artifact(artifact_id) else None

    def set_artifact_and_content(
        self,
        artifact_id: UUID,
        artifact: Artifact,
        content: str,
        *,
        last_writer: Optional[UUID] = None,
    ) -> None:
        """Replace artifact metadata for an existing record. ``content`` is
        ignored per KTD-13 — content_hash on the artifact is the source of
        truth for staleness comparison."""
        del content  # KTD-13: not persisted
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                self._conn.execute(
                    """
                    UPDATE artifacts
                    SET name = ?, version = ?, content_hash = ?, size_tokens = ?,
                        last_writer_id = ?, updated_at = ?
                    WHERE id = ?
                    """,
                    (
                        artifact.name,
                        artifact.version,
                        artifact.content_hash or "",
                        artifact.size_tokens,
                        last_writer.hex if last_writer else None,
                        time.time(),
                        artifact_id.hex,
                    ),
                )
                self._conn.execute("COMMIT")
            except BaseException:
                # P2 ce-review fix #14 (kieran-python): BaseException catches
                # KeyboardInterrupt/SystemExit mid-transaction so ROLLBACK fires
                # before propagation — otherwise the connection is left with an
                # uncommitted transaction that the next BEGIN IMMEDIATE sees.
                self._conn.execute("ROLLBACK")
                raise

    def get_content_at_version(self, artifact_id: UUID, version: int) -> str | None:
        """v0.1 returns None — content history is not persisted (KTD-13)."""
        del version
        return None

    def remove_artifact(self, artifact_id: UUID) -> None:
        """Remove artifact and cascade-delete agent_states for it."""
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                self._conn.execute(
                    "DELETE FROM artifacts WHERE id = ?", (artifact_id.hex,)
                )
                # FK cascade handles agent_states cleanup
                self._conn.execute("COMMIT")
            except BaseException:
                # P2 ce-review fix #14 (kieran-python): BaseException catches
                # KeyboardInterrupt/SystemExit mid-transaction so ROLLBACK fires
                # before propagation — otherwise the connection is left with an
                # uncommitted transaction that the next BEGIN IMMEDIATE sees.
                self._conn.execute("ROLLBACK")
                raise

    # ------------------------------------------------------------------
    # ArtifactRegistry surface — agent state map
    # ------------------------------------------------------------------

    def get_state_map(self, artifact_id: UUID) -> dict[UUID, MESIState]:
        """Return copy of per-agent MESI states for an artifact."""
        with self._lock:
            rows = self._conn.execute(
                "SELECT agent_id, state FROM agent_states WHERE artifact_id = ?",
                (artifact_id.hex,),
            ).fetchall()
        return {UUID(hex=r[0]): MESIState[r[1]] for r in rows}

    def status_snapshot(
        self,
    ) -> tuple[
        dict[UUID, dict[str, Any]],
        dict[UUID, dict[UUID, MESIState]],
    ]:
        """PERF-1 single-query batch for /status. Returns:

        - ``artifact_by_id``: ``{artifact_id: {"name", "version"}}`` — every
          known artifact, one entry per row.
        - ``state_by_artifact``: ``{artifact_id: {agent_id: MESIState}}`` —
          the per-artifact state map for every artifact (empty inner dict
          for artifacts no agent has ever held).

        Two queries (artifacts + agent_states joined by artifact_id), held
        under one lock so the snapshot is consistent. Replaces the old
        per-artifact loop that issued ``2 * N`` separate SELECTs
        (``get_artifact`` + ``get_state_map`` for each id) — eliminates
        the N+1 hot path in ``_handle_status``.
        """
        artifact_by_id: dict[UUID, dict[str, Any]] = {}
        state_by_artifact: dict[UUID, dict[UUID, MESIState]] = {}
        with self._lock:
            for row in self._conn.execute(
                "SELECT id, name, version FROM artifacts"
            ).fetchall():
                aid = UUID(hex=row[0])
                artifact_by_id[aid] = {"name": row[1], "version": row[2]}
                state_by_artifact[aid] = {}
            for row in self._conn.execute(
                "SELECT artifact_id, agent_id, state FROM agent_states"
            ).fetchall():
                aid = UUID(hex=row[0])
                gid = UUID(hex=row[1])
                # Only artifacts present in artifact_by_id get state rows.
                # Defensive: skip orphans from race with concurrent delete.
                if aid not in state_by_artifact:
                    continue
                state_by_artifact[aid][gid] = MESIState[row[2]]
        return artifact_by_id, state_by_artifact

    def get_agent_state(self, artifact_id: UUID, agent_id: UUID) -> MESIState | None:
        """Return MESI state for one agent/artifact pair if present."""
        with self._lock:
            row = self._conn.execute(
                "SELECT state FROM agent_states WHERE artifact_id = ? AND agent_id = ?",
                (artifact_id.hex, agent_id.hex),
            ).fetchone()
        return MESIState[row[0]] if row else None

    def set_agent_state(
        self,
        artifact_id: UUID,
        agent_id: UUID,
        state: MESIState,
        *,
        trigger: str = "unknown",
        tick: int = 0,
        content_hash: str | None = None,
    ) -> None:
        """Set MESI state for one agent/artifact pair.

        Preserves the in-memory registry's mutation-then-log + sequence-rollback
        contract (registry.py:115-173). On state_log exception, the SQL is
        rolled back AND _seq is decremented so the next successful emission
        does not create a phantom gap.
        """
        with self._lock:
            # COR-02: initialised outside the try so the except handler can
            # check it even if BEGIN IMMEDIATE or any pre-_seq SQL raises.
            seq_incremented_in_iteration = False
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                # Fetch prior state for the from→to log entry + bookkeeping decisions.
                prior_row = self._conn.execute(
                    "SELECT state, granted_at_tick FROM agent_states WHERE artifact_id = ? AND agent_id = ?",
                    (artifact_id.hex, agent_id.hex),
                ).fetchone()
                from_state = MESIState[prior_row[0]] if prior_row else MESIState.INVALID
                prior_granted_at = prior_row[1] if prior_row else None

                new_in_me = state in _M_OR_E_STATES
                prev_in_me = from_state in _M_OR_E_STATES

                # Crash-recovery bookkeeping: granted_at_tick set on M∪E acquire only;
                # M↔E transitions preserve the original grant tick (continuous M∪E hold).
                # Slot clears on M∪E acquire ONLY (not on SHARED) — preserves the
                # checkpoint-restore diagnostic across SHARED re-fetches.
                if new_in_me and not prev_in_me:
                    granted_at_tick = tick
                    clear_reclaim = True
                elif new_in_me and prev_in_me:
                    granted_at_tick = prior_granted_at  # preserve
                    clear_reclaim = False
                elif prev_in_me:
                    granted_at_tick = None  # drop the slot on M∪E release
                    clear_reclaim = False
                else:
                    granted_at_tick = prior_granted_at
                    clear_reclaim = False

                # Fetch artifact version for the log entry.
                version_row = self._conn.execute(
                    "SELECT version FROM artifacts WHERE id = ?", (artifact_id.hex,)
                ).fetchone()
                if version_row is None:
                    raise KeyError(f"artifact {artifact_id} not in registry")
                version = version_row[0]

                # Upsert agent state.
                if prior_row is None:
                    self._conn.execute(
                        """
                        INSERT INTO agent_states (artifact_id, agent_id, state, granted_at_tick,
                                                  last_reclaim_trigger, last_reclaim_tick)
                        VALUES (?, ?, ?, ?, NULL, NULL)
                        """,
                        (artifact_id.hex, agent_id.hex, state.name, granted_at_tick),
                    )
                else:
                    if clear_reclaim:
                        self._conn.execute(
                            """
                            UPDATE agent_states
                            SET state = ?, granted_at_tick = ?, last_reclaim_trigger = NULL,
                                last_reclaim_tick = NULL
                            WHERE artifact_id = ? AND agent_id = ?
                            """,
                            (state.name, granted_at_tick, artifact_id.hex, agent_id.hex),
                        )
                    else:
                        self._conn.execute(
                            """
                            UPDATE agent_states
                            SET state = ?, granted_at_tick = ?
                            WHERE artifact_id = ? AND agent_id = ?
                            """,
                            (state.name, granted_at_tick, artifact_id.hex, agent_id.hex),
                        )

                # Mutation-then-log: emit state_log BEFORE commit. If the callback
                # raises, ROLLBACK undoes the agent_states change AND we decrement
                # _seq so gap detection stays consistent.
                #
                # COR-02: track _seq mutation in this iteration so the outer
                # BaseException handler can also roll it back. Without this, a
                # COMMIT failure leaves in-memory _seq ahead of persisted
                # registry_meta.sequence_number — subsequent successful emissions
                # produce a phantom gap. The inner Exception-during-state_log
                # path already decrements; the outer ROLLBACK path now does too.
                if self._state_log is not None:
                    self._seq += 1
                    seq_incremented_in_iteration = True
                    entry = {
                        "tick": tick,
                        "artifact_id": str(artifact_id),
                        "agent_id": str(agent_id),
                        "agent_name": self._agent_names.get(agent_id) if self._agent_names is not None else None,
                        "from_state": from_state.name,
                        "to_state": state.name,
                        "trigger": trigger,
                        "version": version,
                        "content_hash": content_hash,
                        "sequence_number": self._seq,
                        "instance_id": self._instance_id,
                        "schema_version": CCS_STATE_LOG_SCHEMA_VERSION,
                    }
                    try:
                        self._state_log(entry)
                    except Exception:
                        self._seq -= 1
                        seq_incremented_in_iteration = False
                        raise
                    # Persist new _seq value so cross-restart consumers see continuity.
                    self._conn.execute(
                        "UPDATE registry_meta SET value = ? WHERE key = 'sequence_number'",
                        (str(self._seq),),
                    )

                self._conn.execute("COMMIT")
                # COMMIT succeeded — _seq is durably persisted; no rollback needed.
                seq_incremented_in_iteration = False
            except BaseException:
                # P2 ce-review fix #14 (kieran-python): BaseException catches
                # KeyboardInterrupt/SystemExit mid-transaction so ROLLBACK fires
                # before propagation — otherwise the connection is left with an
                # uncommitted transaction that the next BEGIN IMMEDIATE sees.
                self._conn.execute("ROLLBACK")
                # COR-02: if _seq was bumped in this iteration but COMMIT (or any
                # later step) failed, decrement to match the rolled-back DB state.
                if seq_incremented_in_iteration:
                    self._seq -= 1
                raise

    # ------------------------------------------------------------------
    # ArtifactRegistry surface — transient state
    # ------------------------------------------------------------------

    def get_agent_transient(self, artifact_id: UUID, agent_id: UUID) -> TransientState | None:
        """Return transient state for one agent/artifact pair if present."""
        with self._lock:
            row = self._conn.execute(
                "SELECT transient_state FROM agent_states WHERE artifact_id = ? AND agent_id = ?",
                (artifact_id.hex, agent_id.hex),
            ).fetchone()
        return TransientState[row[0]] if row and row[0] else None

    def set_agent_transient(
        self,
        artifact_id: UUID,
        agent_id: UUID,
        transient_state: TransientState,
        *,
        entered_tick: int,
    ) -> None:
        """Set transient state and entry tick for one agent/artifact pair."""
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                # Ensure row exists; the registry surface allows transient state
                # before stable state is set.
                self._conn.execute(
                    """
                    INSERT INTO agent_states (artifact_id, agent_id, state, transient_state, transient_tick)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(artifact_id, agent_id) DO UPDATE SET
                        transient_state = excluded.transient_state,
                        transient_tick = excluded.transient_tick
                    """,
                    (artifact_id.hex, agent_id.hex, MESIState.INVALID.name,
                     transient_state.name, entered_tick),
                )
                self._conn.execute("COMMIT")
            except BaseException:
                # P2 ce-review fix #14 (kieran-python): BaseException catches
                # KeyboardInterrupt/SystemExit mid-transaction so ROLLBACK fires
                # before propagation — otherwise the connection is left with an
                # uncommitted transaction that the next BEGIN IMMEDIATE sees.
                self._conn.execute("ROLLBACK")
                raise

    def clear_agent_transient(self, artifact_id: UUID, agent_id: UUID) -> None:
        """Clear transient state and timestamp for one agent/artifact pair."""
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                self._conn.execute(
                    """
                    UPDATE agent_states
                    SET transient_state = NULL, transient_tick = NULL
                    WHERE artifact_id = ? AND agent_id = ?
                    """,
                    (artifact_id.hex, agent_id.hex),
                )
                self._conn.execute("COMMIT")
            except BaseException:
                # P2 ce-review fix #14 (kieran-python): BaseException catches
                # KeyboardInterrupt/SystemExit mid-transaction so ROLLBACK fires
                # before propagation — otherwise the connection is left with an
                # uncommitted transaction that the next BEGIN IMMEDIATE sees.
                self._conn.execute("ROLLBACK")
                raise

    def get_transient_map(self, artifact_id: UUID) -> dict[UUID, TransientState]:
        """Return copy of per-agent transient states for an artifact."""
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT agent_id, transient_state FROM agent_states
                WHERE artifact_id = ? AND transient_state IS NOT NULL
                """,
                (artifact_id.hex,),
            ).fetchall()
        return {UUID(hex=r[0]): TransientState[r[1]] for r in rows}

    def get_transient_tick(self, artifact_id: UUID, agent_id: UUID) -> int | None:
        """Return tick when agent entered transient state if present."""
        with self._lock:
            row = self._conn.execute(
                "SELECT transient_tick FROM agent_states WHERE artifact_id = ? AND agent_id = ?",
                (artifact_id.hex, agent_id.hex),
            ).fetchone()
        return row[0] if row else None

    # ------------------------------------------------------------------
    # ArtifactRegistry surface — heartbeat + crash-recovery bookkeeping
    # ------------------------------------------------------------------

    def record_heartbeat(self, agent_id: UUID, now_tick: int) -> None:
        """Record an agent's heartbeat tick using max(prev, incoming) (R12 monotonicity)."""
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                self._conn.execute(
                    """
                    INSERT INTO heartbeats (agent_id, last_tick) VALUES (?, ?)
                    ON CONFLICT(agent_id) DO UPDATE SET
                        last_tick = MAX(heartbeats.last_tick, excluded.last_tick)
                    """,
                    (agent_id.hex, now_tick),
                )
                self._conn.execute("COMMIT")
            except BaseException:
                # P2 ce-review fix #14 (kieran-python): BaseException catches
                # KeyboardInterrupt/SystemExit mid-transaction so ROLLBACK fires
                # before propagation — otherwise the connection is left with an
                # uncommitted transaction that the next BEGIN IMMEDIATE sees.
                self._conn.execute("ROLLBACK")
                raise

    def last_heartbeat_tick(self, agent_id: UUID) -> int | None:
        """Return the last recorded heartbeat tick for an agent, if any."""
        with self._lock:
            row = self._conn.execute(
                "SELECT last_tick FROM heartbeats WHERE agent_id = ?", (agent_id.hex,)
            ).fetchone()
        return row[0] if row else None

    def record_last_reclamation(
        self, agent_id: UUID, artifact_id: UUID, trigger: str, tick: int
    ) -> None:
        """Record the most recent reclamation slot for an (agent, artifact) pair."""
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                self._conn.execute(
                    """
                    UPDATE agent_states
                    SET last_reclaim_trigger = ?, last_reclaim_tick = ?
                    WHERE artifact_id = ? AND agent_id = ?
                    """,
                    (trigger, tick, artifact_id.hex, agent_id.hex),
                )
                self._conn.execute("COMMIT")
            except BaseException:
                # P2 ce-review fix #14 (kieran-python): BaseException catches
                # KeyboardInterrupt/SystemExit mid-transaction so ROLLBACK fires
                # before propagation — otherwise the connection is left with an
                # uncommitted transaction that the next BEGIN IMMEDIATE sees.
                self._conn.execute("ROLLBACK")
                raise

    def get_last_reclamation(
        self, agent_id: UUID, artifact_id: UUID
    ) -> ReclamationSlot | None:
        """Return the most recent reclamation slot for an (agent, artifact) pair, if any."""
        with self._lock:
            row = self._conn.execute(
                """
                SELECT last_reclaim_trigger, last_reclaim_tick FROM agent_states
                WHERE artifact_id = ? AND agent_id = ?
                """,
                (artifact_id.hex, agent_id.hex),
            ).fetchone()
        if row is None or row[0] is None:
            return None
        return (row[0], row[1])

    def granted_at_tick(self, agent_id: UUID, artifact_id: UUID) -> int | None:
        """Return the tick at which agent acquired its current M/E grant on artifact, if any."""
        with self._lock:
            row = self._conn.execute(
                """
                SELECT granted_at_tick FROM agent_states
                WHERE artifact_id = ? AND agent_id = ?
                """,
                (artifact_id.hex, agent_id.hex),
            ).fetchone()
        return row[0] if row else None

    def valid_holders(self, artifact_id: UUID) -> list[UUID]:
        """Return agents that currently hold non-invalid entries."""
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT agent_id FROM agent_states
                WHERE artifact_id = ? AND state != ?
                """,
                (artifact_id.hex, MESIState.INVALID.name),
            ).fetchall()
        return [UUID(hex=r[0]) for r in rows]

    # ------------------------------------------------------------------
    # Plugin-only extensions (KTD-9 + KTD-11)
    # ------------------------------------------------------------------

    def resolve_or_register(
        self,
        parent_rel_path: str,
        content_hash: str,
        *,
        initial_owner: Optional[UUID] = None,
    ) -> UUID:
        """KTD-9 first-observation seeding for the plugin's pre-read handler.

        Atomically: SELECT artifact by name → if found, return its id;
        otherwise INSERT a new artifact at version 1 with the given content_hash
        and return the new id. Concurrent first-Reads from two sessions on
        the same fresh path converge to one row (BEGIN IMMEDIATE + UNIQUE
        constraint on artifacts.name absorbs the race; the second caller's
        INSERT raises IntegrityError, which we catch and re-fetch).

        The ``initial_owner`` parameter is accepted for API symmetry with
        ``CoordinatorService.register_artifact`` but does NOT set a MESI grant
        here. Grant assignment is the caller's responsibility (the plugin's
        pre-read handler then calls ``set_agent_state(..., SHARED, ...)``).
        """
        del initial_owner  # parameter symmetry only; plugin handler sets state explicitly
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                row = self._conn.execute(
                    "SELECT id FROM artifacts WHERE name = ?", (parent_rel_path,)
                ).fetchone()
                if row is not None:
                    self._conn.execute("COMMIT")
                    return UUID(hex=row[0])
                # First observation — insert.
                new_id = uuid4()
                self._conn.execute(
                    """
                    INSERT INTO artifacts (id, name, version, content_hash, size_tokens,
                                           last_writer_id, updated_at)
                    VALUES (?, ?, ?, ?, NULL, NULL, ?)
                    """,
                    (new_id.hex, parent_rel_path, 1, content_hash, time.time()),
                )
                self._conn.execute("COMMIT")
                return new_id
            except sqlite3.IntegrityError as exc:
                # Lost the UNIQUE-on-name race; another caller inserted between
                # our SELECT and INSERT. ROLLBACK and re-fetch.
                #
                # COR-04: a concurrent remove_artifact between ROLLBACK and
                # re-fetch can leave the row absent. Old behaviour: re-raise
                # the original IntegrityError with no context — operator sees
                # a confusing "UNIQUE constraint failed" trace for what's
                # really a delete race. New behaviour: raise an explicit
                # informative RuntimeError so the operator knows exactly
                # what happened.
                self._conn.execute("ROLLBACK")
                row = self._conn.execute(
                    "SELECT id FROM artifacts WHERE name = ?", (parent_rel_path,)
                ).fetchone()
                if row is None:
                    raise RuntimeError(
                        f"resolve_or_register: lost INSERT race on {parent_rel_path!r} "
                        "but the winning row was deleted before re-fetch. This "
                        "indicates a concurrent remove_artifact running against the "
                        "same name — caller should retry or treat the artifact as "
                        f"absent. Original IntegrityError: {exc}"
                    ) from exc
                return UUID(hex=row[0])
            except BaseException:
                # P2 ce-review fix #14 (kieran-python): BaseException catches
                # KeyboardInterrupt/SystemExit mid-transaction so ROLLBACK fires
                # before propagation — otherwise the connection is left with an
                # uncommitted transaction that the next BEGIN IMMEDIATE sees.
                self._conn.execute("ROLLBACK")
                raise

    def artifacts_held_by_agent(
        self, agent_id: UUID, states: Iterable[MESIState]
    ) -> list[UUID]:
        """KTD-11 session-stop release iteration: return artifacts where the
        given agent holds any of the listed MESI states. Used by Unit 4's
        ``/hooks/session-stop`` handler to enumerate uncommitted grants for
        release via :class:`CoordinatorService.invalidate`."""
        state_names = [s.name for s in states]
        if not state_names:
            return []
        placeholders = ",".join("?" * len(state_names))
        with self._lock:
            rows = self._conn.execute(
                f"""
                SELECT artifact_id FROM agent_states
                WHERE agent_id = ? AND state IN ({placeholders})
                """,
                (agent_id.hex, *state_names),
            ).fetchall()
        return [UUID(hex=r[0]) for r in rows]

    def lookup_artifact_id_by_name(self, parent_rel_path: str) -> UUID | None:
        """Read-only path lookup; used by status endpoint and tests."""
        with self._lock:
            row = self._conn.execute(
                "SELECT id FROM artifacts WHERE name = ?", (parent_rel_path,)
            ).fetchone()
        return UUID(hex=row[0]) if row else None

    def artifact_names_under_prefix(self, prefix: str) -> list[str]:
        """Return tracked-artifact paths registered under a directory prefix.

        Used by ``/hooks/pre-grep`` (v0.1.1 KTD-N) to find tracked artifacts
        a Grep operation would scan. Prefix matching uses SQL ``LIKE`` with
        ``escape`` to defang any ``%``/``_`` wildcards in the operator-
        supplied search root. Empty/``.``/``./`` prefix returns all
        registered artifacts (Grep over workspace root).
        """
        # Normalize prefix. Treat empty / "." / "./" as "all artifacts".
        if prefix in ("", ".", "./"):
            with self._lock:
                rows = self._conn.execute("SELECT name FROM artifacts").fetchall()
            return [r[0] for r in rows]
        # Strip trailing slash; ensure we don't accidentally claim
        # "docs/specs-internal/" as a child of "docs/specs/".
        normalized = prefix.rstrip("/") + "/"
        # Escape SQL LIKE wildcards.
        escaped = normalized.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
        pattern = escaped + "%"
        exact_match = prefix.rstrip("/")
        # REL-08: combine the LIKE-prefix query and the exact-match query
        # into a single UNION under one lock acquisition. The prior
        # two-step pattern released the lock between queries — a
        # concurrent delete or rename could remove an artifact between
        # them, producing a torn result set (LIKE row gone but exact-
        # match row appears, or vice versa). Single query closes the gap.
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT name FROM artifacts WHERE name LIKE ? ESCAPE '\\'
                UNION
                SELECT name FROM artifacts WHERE name = ?
                """,
                (pattern, exact_match),
            ).fetchall()
        return [r[0] for r in rows]

    # ------------------------------------------------------------------
    # A1 — Preemption notices (silent-grant-revocation surfacing)
    # ------------------------------------------------------------------

    def record_preemption_notice(
        self,
        *,
        victim_agent_id: UUID,
        artifact_id: UUID,
        preempter_agent_id: UUID,
        preempted_at_unix_ts: float,
    ) -> None:
        """Record that ``victim_agent_id`` had its M∪E grant on ``artifact_id``
        invalidated by ``preempter_agent_id``. The next hook handler that
        sees ``victim_agent_id`` should pop and surface the notice.

        UPSERT semantics (F5 hardening): the row stays at the notice with
        the LATEST wall-clock timestamp, not the latest commit order. If
        Y commits at ts=100 then Z commits at ts=50 (out-of-order due to
        scheduling jitter), Y's notice wins because Y's preemption is the
        more recent fact about the world. The WHERE clause makes the
        update conditional on excluded.preempted_at_unix_ts being strictly
        greater than the existing row's.
        """
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                self._conn.execute(
                    """
                    INSERT INTO pending_notices
                        (agent_id, artifact_id, preempter_agent_id, preempted_at_unix_ts)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(agent_id, artifact_id) DO UPDATE SET
                        preempter_agent_id = excluded.preempter_agent_id,
                        preempted_at_unix_ts = excluded.preempted_at_unix_ts
                    WHERE excluded.preempted_at_unix_ts > pending_notices.preempted_at_unix_ts
                    """,
                    (
                        victim_agent_id.hex,
                        artifact_id.hex,
                        preempter_agent_id.hex,
                        preempted_at_unix_ts,
                    ),
                )
                self._conn.execute("COMMIT")
            except BaseException:
                # P2 ce-review fix #14 (kieran-python): BaseException catches
                # KeyboardInterrupt/SystemExit mid-transaction so ROLLBACK fires
                # before propagation — otherwise the connection is left with an
                # uncommitted transaction that the next BEGIN IMMEDIATE sees.
                self._conn.execute("ROLLBACK")
                raise

    def pop_pending_notices(
        self, agent_id: UUID
    ) -> list[tuple[UUID, UUID, float]]:
        """Atomically SELECT and DELETE all pending notices for ``agent_id``.
        Returns list of ``(artifact_id, preempter_agent_id, preempted_at_unix_ts)``.
        Empty list if no pending notices."""
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                rows = self._conn.execute(
                    """
                    SELECT artifact_id, preempter_agent_id, preempted_at_unix_ts
                    FROM pending_notices WHERE agent_id = ?
                    """,
                    (agent_id.hex,),
                ).fetchall()
                if rows:
                    self._conn.execute(
                        "DELETE FROM pending_notices WHERE agent_id = ?",
                        (agent_id.hex,),
                    )
                self._conn.execute("COMMIT")
            except BaseException:
                # P2 ce-review fix #14 (kieran-python): BaseException catches
                # KeyboardInterrupt/SystemExit mid-transaction so ROLLBACK fires
                # before propagation — otherwise the connection is left with an
                # uncommitted transaction that the next BEGIN IMMEDIATE sees.
                self._conn.execute("ROLLBACK")
                raise
        return [
            (UUID(hex=r[0]), UUID(hex=r[1]), float(r[2]))
            for r in rows
        ]

    def get_artifact_updated_at(self, artifact_id: UUID) -> Optional[float]:
        """Return the wall-clock unix timestamp of the artifact's last
        update, or None if the artifact is unknown.

        P2 ce-review fix #16 (maintainability + kieran-python): the plugin
        adapter previously reached into ``_conn`` and ``_lock`` directly
        to read this column — a layer violation that would break on any
        connection-pool refactor. This public accessor replaces that."""
        with self._lock:
            row = self._conn.execute(
                "SELECT updated_at FROM artifacts WHERE id = ?",
                (artifact_id.hex,),
            ).fetchone()
        return float(row[0]) if row else None

    def peek_preemption_notice(
        self, agent_id: UUID, artifact_id: UUID
    ) -> Optional[tuple[UUID, float]]:
        """Non-destructive lookup: is there a pending notice for this
        (agent, artifact) pair? Returns (preempter_agent_id, ts) or None.
        Kept for telemetry / status surface use. The post-edit failure path
        uses :meth:`pop_preemption_notice` instead (F4 single-consumer)."""
        with self._lock:
            row = self._conn.execute(
                """
                SELECT preempter_agent_id, preempted_at_unix_ts
                FROM pending_notices WHERE agent_id = ? AND artifact_id = ?
                """,
                (agent_id.hex, artifact_id.hex),
            ).fetchone()
        if row is None:
            return None
        return UUID(hex=row[0]), float(row[1])

    def pop_preemption_notice(
        self, agent_id: UUID, artifact_id: UUID
    ) -> Optional[tuple[UUID, float]]:
        """F4 hardening: atomically SELECT + DELETE the single notice for
        this (agent, artifact) pair. Returns (preempter_agent_id, ts) or
        None. Used by the post-edit failure path so the notice is consumed
        at the point it surfaces in the error reason, preventing the next
        pre-event from re-emitting the same preemption prose."""
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                row = self._conn.execute(
                    """
                    SELECT preempter_agent_id, preempted_at_unix_ts
                    FROM pending_notices WHERE agent_id = ? AND artifact_id = ?
                    """,
                    (agent_id.hex, artifact_id.hex),
                ).fetchone()
                if row is not None:
                    self._conn.execute(
                        "DELETE FROM pending_notices WHERE agent_id = ? AND artifact_id = ?",
                        (agent_id.hex, artifact_id.hex),
                    )
                self._conn.execute("COMMIT")
            except BaseException:
                # P2 ce-review fix #14 (kieran-python): BaseException catches
                # KeyboardInterrupt/SystemExit mid-transaction so ROLLBACK fires
                # before propagation — otherwise the connection is left with an
                # uncommitted transaction that the next BEGIN IMMEDIATE sees.
                self._conn.execute("ROLLBACK")
                raise
        if row is None:
            return None
        return UUID(hex=row[0]), float(row[1])

    def evict_stale_notices(
        self, *, max_age_sec: float, now_unix: Optional[float] = None
    ) -> int:
        """F2 hardening: bound storage by deleting notices older than
        ``max_age_sec``. Returns rows deleted. Called periodically (e.g.
        on session register) so orphan notices for sessions that never
        return — e.g. dead Claude Code processes — don't accumulate.

        ``now_unix`` is parameterized so tests can pin the clock without
        monkey-patching ``time.time``; defaults to ``time.time()``.
        """
        if now_unix is None:
            # P3 ce-review fix #37: use module-level `time` import (was a
            # deferred `import time as _time` that shadowed the top-level
            # import and confused readers).
            now_unix = time.time()
        cutoff = now_unix - max_age_sec
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                cursor = self._conn.execute(
                    "DELETE FROM pending_notices WHERE preempted_at_unix_ts < ?",
                    (cutoff,),
                )
                deleted = cursor.rowcount
                self._conn.execute("COMMIT")
            except BaseException:
                # P2 ce-review fix #14 (kieran-python): BaseException catches
                # KeyboardInterrupt/SystemExit mid-transaction so ROLLBACK fires
                # before propagation — otherwise the connection is left with an
                # uncommitted transaction that the next BEGIN IMMEDIATE sees.
                self._conn.execute("ROLLBACK")
                raise
        return deleted

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _fetch_artifact_row(self, artifact_id: UUID) -> Optional[_ArtifactRow]:
        with self._lock:
            row = self._conn.execute(
                """
                SELECT name, version, content_hash, size_tokens, last_writer_id
                FROM artifacts WHERE id = ?
                """,
                (artifact_id.hex,),
            ).fetchone()
        if row is None:
            return None
        artifact = Artifact(
            id=artifact_id,
            name=row[0],
            version=row[1],
            content_hash=row[2] or None,
            size_tokens=row[3],
        )
        last_writer_id = UUID(hex=row[4]) if row[4] else None
        return _ArtifactRow(artifact=artifact, last_writer_id=last_writer_id)
