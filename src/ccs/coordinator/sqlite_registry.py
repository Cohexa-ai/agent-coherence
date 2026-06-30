# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""SQLite-WAL persistent artifact registry for cross-process coherence coordination.

This module is the persistence layer for the Claude Code coherence plugin
(per docs/plans/2026-05-13-001-feat-claude-code-coherence-plugin-v0.1-plan.md
Phase A Unit 1). It is a drop-in replacement for :class:`ArtifactRegistry`
that survives process restarts and is safe for multi-threaded access from
:class:`http.server.ThreadingHTTPServer` handler threads.

Contract divergence from in-memory ``ArtifactRegistry`` (per plan KTD-13):

  By default this registry does NOT persist artifact content. Only
  ``content_hash`` is stored. ``get_content(artifact_id)`` returns ``b""``
  (empty bytes) for known artifacts and ``None`` for unknown. This is
  deliberate — the plugin's hot path (Unit 4 HTTP hook handlers) never calls
  ``CoordinatorService.fetch``; it uses ``resolve_or_register`` / ``write`` /
  ``commit`` / ``invalidate`` directly. Avoiding content storage shrinks the
  disclosure surface if ``.coherence/state.db`` is accidentally committed to
  git (KTD-13 defense-in-depth).

  EXCEPTION — durable version retention (plan item N v1, Unit 3): when
  constructed with ``retain_versions=True`` (in-process embedders only; the
  HTTP coordinator-server constructor leaves it off), the BODY of each captured
  version is stored durably in the ``artifact_versions`` table (KTD-13 reversed
  for retained rows only). This is opt-in and reachable through
  ``get_content_at_version``, not ``get_content``. The file is held at mode 0600
  (race-free at creation) precisely because this puts content on disk; see the
  ``_DB_FILE_MODE`` note and ``docs/security.md``.

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

import logging
import os
import sqlite3
import stat
import threading
import time
import warnings
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Iterable, Iterator, NoReturn, Optional
from uuid import UUID, uuid4

from ccs.core.exceptions import (
    CROSS_RUNTIME_SCHEMA_REASON,
    STALE_READ_GENERATION_REASON,
    STORE_SIGNAL_BUSY,
    STORE_SIGNAL_UNREADABLE,
    STORE_SIGNAL_WAL_RECOVERY,
    UNKNOWN_ARTIFACT_REASON,
    StaleReadGeneration,
    WatchdogAbandoned,
)
from ccs.core.states import MESIState, TransientState
from ccs.core.types import (
    Artifact,
    CasCorruption,
    ConflictDetail,
    VersionedReadRejection,
)

# Contract return types (ReclamationSlot / CasResult / CaptureResult) live in the
# Protocol module (Phase 1 dedup); re-exported here so existing
# `from .sqlite_registry import CasResult` importers keep working.
from .registry_protocol import CaptureResult, CasResult, ReclamationSlot
from .retention import RetentionPolicy, collectible_versions

logger = logging.getLogger(__name__)

CCS_STATE_LOG_SCHEMA_VERSION = "ccs.state_log.v2"
"""Reuses the same schema version as in-memory registry (state_log emissions
are interchangeable from a downstream consumer's perspective)."""

SCHEMA_USER_VERSION = 4
"""Schema version stamped via ``PRAGMA user_version`` on init.

**v1 -> v2** (plan item N v1) added the durable ``artifact_versions`` table and
idempotently subsumed the fence-column + ``pending_notices`` no-version-bump
shims, so ``user_version=2`` GUARANTEES the complete v2 schema from every wild
v1 variant.

**v2 -> v3** (SB-17 / TX-1, Unit 2 / R1) adds the durable ``session_pins``
table (one row per ``(session_token, artifact_id)`` pinned version) so a
snapshot session's cut survives a coordinator restart (R6, sqlite-only). The
migration is idempotent (``CREATE TABLE IF NOT EXISTS`` + an in-txn version
stamp) and additive — it touches NO existing table, so the v2->v3 step is
purely a new-surface add.

**v3 -> v4** (SB-17 / TX-1, durable owner-binding) adds the ``session_meta``
table (the durable session owner + creation tick, so a survived session is
owner-validated post-restart — R13 holds across a restart) and the
``idx_session_pins_artifact`` index (the GC-exemption lookup probes
``session_pins`` by the non-leftmost-PK ``artifact_id`` on every commit). Both
are additive; the v3->v4 step is required because earlier commits of this branch
already stamped ``user_version=3`` with ``session_pins`` but WITHOUT
``session_meta`` — a bare in-place add would leave such a db unable to find
``session_meta`` (it opens via the write-free rehydrate path). The dedicated
``_migrate_v3_to_v4`` creates both for any v3 db; a Node-v3 db is rejected by
``_reject_foreign_ledger_db`` before reaching it.

**CROSS-RUNTIME LEDGER DIVERGENCE (security).** The sibling Node coordinator
(agent-coherence-plugin) shares the SAME ``state.db`` path but keeps its OWN
ledger: its v3 is ``ALTER TABLE agent_states ADD COLUMN deadline_tick`` — a
DIFFERENT schema than this repo's v3. A Node coordinator opening a Python-v3 db
(or vice-versa) must DETECT and REJECT rather than silently misread; the
``schema_runtime`` lineage stamp + the structural ``session_pins``-presence
probe in ``_reject_foreign_ledger_db`` enforce that fail-closed posture.

After migration the normal open path performs **no writes** (no shim ALTERs,
no IF-NOT-EXISTS), which is the prerequisite that makes the read-only open mode
implementable. See ``_migrate_v1_to_v2`` / ``_migrate_v2_to_v3`` and
``docs/solutions/runtime-errors/sqlite-schema-init-non-atomic-leaves-unbootable-db-2026-05-18.md``."""

_V2_USER_VERSION = 2
"""The intermediate v2 stamp the v1->v2 migration writes (before the chained
v2->v3 step). Pinned as a literal because the v1->v2 migration must NOT jump
straight to ``SCHEMA_USER_VERSION`` — it lands the v2 schema, then
``_migrate_v2_to_v3`` adds ``session_pins`` and stamps v3, then
``_migrate_v3_to_v4`` adds ``session_meta``. A v1 db steps 1 -> 2 -> 3 -> 4."""

_V3_USER_VERSION = 3
"""The intermediate v3 stamp ``_migrate_v2_to_v3`` writes (session_pins only),
before the chained ``_migrate_v3_to_v4`` step adds ``session_meta`` + the
``session_pins`` artifact index and advances to ``SCHEMA_USER_VERSION``. A
literal because session_pins-at-v3 is a distinct on-disk state earlier commits
of this branch already produced."""

_DB_FILE_MODE = 0o600
"""state.db (and its -wal/-shm sidecars) must be owner-read/write only.

Durable retention puts artifact *content* on disk (v1->v2), so the disclosure
posture of state.db now matches the audit log's: 0600, race-free at creation.
Pre-created via ``os.open(..., 0o600)`` BEFORE ``sqlite3.connect`` so the file
never exists at a broader umask mode; the migration re-applies it (and warns
once) to an operator-broadened pre-existing db. Mirrors
``adapters/claude_code/audit_log.py:_REQUIRED_MODE``."""

_M_OR_E_STATES: frozenset[MESIState] = frozenset({MESIState.MODIFIED, MESIState.EXCLUSIVE})

# Durable version-retention table (plan item N v1, Unit 3). One row per
# (artifact, version) retained snapshot. The ``content`` column is declared with
# NO type name on purpose: SQLite then gives it BLOB affinity (affinity NONE),
# which stores each value with its own storage class — a Python ``str`` binds as
# TEXT and a ``bytes`` binds as BLOB, and both round-trip back to the SAME Python
# type (``str``/``bytes``). A typed column (e.g. ``TEXT``/``BLOB``) would coerce
# one of them; affinity NONE is what makes the str-vs-bytes round-trip hold.
# ``captured_at`` is wall-clock ``time.time()`` (matches ``artifacts.updated_at``
# + the in-memory ``version_captured_at`` parallel dict). FK ON DELETE CASCADE
# (foreign_keys=ON) drops history when the artifact is removed.
_ARTIFACT_VERSIONS_DDL = """
CREATE TABLE artifact_versions (
    artifact_id TEXT NOT NULL,
    version     INTEGER NOT NULL,
    content,
    captured_at REAL NOT NULL,
    PRIMARY KEY (artifact_id, version),
    FOREIGN KEY (artifact_id) REFERENCES artifacts(id) ON DELETE CASCADE
)
"""

# Durable snapshot-session pin table (SB-17 / TX-1, Unit 2 / R1, R4). One row
# per (session_token, artifact_id) pinned version — the persisted mirror of the
# in-memory ``_session_pins`` dict. A live session's pinned version is exempt
# from the inline retention GC (the exemptions seam) until ``release_session``
# deletes its rows. Restart-survival is the whole point of the durable mirror
# (R6, sqlite-only): a coordinator restart re-reads the live pins so the cut is
# still held. NO foreign key to ``artifacts`` ON DELETE CASCADE here on
# PURPOSE: a pin must OUTLIVE a transient absence of its artifact only as far as
# the artifact actually exists — but the version-vector capture already
# rejected unknown ids before inserting, and the read-serve / liveness fail-
# closed path (Units 3/5) is what handles an artifact deleted out from under a
# live pin (typed SessionInvalidated, never wrong bytes). Keeping the pin table
# independent of the artifacts FK avoids a silent cascade-delete masking that
# fail-closed signal.
_SESSION_PINS_DDL = """
CREATE TABLE session_pins (
    session_token TEXT NOT NULL,
    artifact_id   TEXT NOT NULL,
    version       INTEGER NOT NULL,
    PRIMARY KEY (session_token, artifact_id)
)
"""

# The GC exemption read (``_live_pins_for_artifact_sql``, run on EVERY commit's
# retention pass) filters ``session_pins`` by ``artifact_id`` — the NON-leftmost
# column of the (session_token, artifact_id) PK, so without this index every
# commit does a full scan of session_pins (finding F6). The index makes the
# exemption lookup an index probe.
_SESSION_PINS_ARTIFACT_INDEX_DDL = (
    "CREATE INDEX idx_session_pins_artifact ON session_pins(artifact_id)"
)

# Durable session OWNER + creation tick (SB-17 / TX-1, R13/R6/R14). The pins in
# ``session_pins`` are durable and survive a coordinator restart, but the
# service-layer owner-binding + lease are in-memory and a restart wipes them —
# which would let ANY token-holder read a surviving cut post-restart (an
# owner-isolation bypass, finding F3). Persisting the owner alongside the pins
# lets post-restart validation fall back to the durable owner (R13 preserved
# across restart) while still serving the legitimate owner (R6 restart-survival
# preserved). ``created_at_tick`` survives too so the absolute-age ceiling (R14)
# can bound a durable session even after a restart wiped its in-memory state.
_SESSION_META_DDL = """
CREATE TABLE session_meta (
    session_token   TEXT PRIMARY KEY,
    owner           TEXT NOT NULL,
    created_at_tick INTEGER NOT NULL
)
"""

# Coordinator-side eviction triggers (the stable-grant sweep's two reclaim
# triggers + the transient-timeout fail-safe) — an M/E -> INVALID carrying one
# of these bumps the artifact's owner_generation (read-generation fence): the
# claim was revoked without a version move, which version-CAS cannot see.
# Duplicated from registry.py (the two registries share no base class); the
# dual-registry parity test pins them equal.
RECLAIM_TRIGGERS: frozenset[str] = frozenset(
    {"reclaim_heartbeat", "reclaim_max_hold", "timeout"}
)

# Genuine-content-read triggers for read-generation capture (mirrors
# registry.py; pinned equal by the parity test). Service.fetch() emits "fetch".
CLAIM_CAPTURE_TRIGGERS: frozenset[str] = frozenset({"fetch"})

# registry_meta keys persisting the retention policy on writer open (plan item
# N v1, Unit 3). ``retention_enabled`` is the explicit marker set even in the
# unbounded mode (retain_versions=True + policy=None → enabled with NULL axes):
# the artifact_versions table exists on EVERY v2 db, so table-presence alone
# cannot tell retention-on-unbounded from retention-never-enabled — a read-only
# resolver reads these keys to derive ``retention_off`` and the T-expiry axis.
_META_RETENTION_ENABLED = "retention_enabled"
_META_RETENTION_MAX_VERSIONS = "retention_max_versions"
_META_RETENTION_MAX_AGE_SECONDS = "retention_max_age_seconds"

# Cross-runtime lineage stamp (registry_meta). The sibling Node coordinator
# shares the SAME ``<workspace>/.coherence/state.db`` path but keeps its OWN
# migration ledger (its v2/v3 mean different schemas than this repo's v2), so
# ``PRAGMA user_version`` alone cannot say WHOSE ledger a file belongs to.
# This side stamps ``schema_runtime=python`` at fresh-create and at the v1->v2
# migration (inside those existing transactions — the v2 steady-state open
# stays write-free); the Node side mirrors with ``node`` (alignment issue).
# A present-and-foreign stamp is the STRONGEST cross-runtime marker — checked
# before any structural probing. Absence is NOT foreign: every db stamped
# before this marker shipped lacks the key.
_META_SCHEMA_RUNTIME = "schema_runtime"
_SCHEMA_RUNTIME_STAMP = "python"



class SchemaVersionError(RuntimeError):
    """Raised when an existing ``state.db`` carries an unexpected user_version.

    The message deliberately does **not** recommend deleting the database:
    as of v2 the store holds durable retained content + read-generation fence
    state, so a delete is destructive (it drops retained history AND the fence
    epoch). The forward path is upgrading the embedder binary to one that
    understands the schema, never ``rm state.db``.
    """


class CrossRuntimeSchemaError(SchemaVersionError):
    """The ``state.db`` was written under the sibling Node coordinator's ledger.

    The Node coordinator (agent-coherence-plugin) shares the same db path but
    assigns DIFFERENT meanings to ``user_version`` 2 and 3 (its v2 adds no
    schema objects; its v3 adds ``agent_states.deadline_tick``), so this
    Python coordinator fails CLOSED at open — it will neither read nor migrate
    a foreign-ledger db (migrating would corrupt the Node side's live state;
    reading would misinterpret its schema). Detection is in
    :meth:`SqliteArtifactRegistry._reject_foreign_ledger_db`.

    Subclasses :class:`SchemaVersionError` so every existing catch-site (the
    replay resolver's ``except SchemaVersionError`` mapping, CLI exit-code
    plumbing) handles it without change. ``reason`` is always
    :data:`ccs.core.exceptions.CROSS_RUNTIME_SCHEMA_REASON`; consumers match
    ``exc.reason == CONSTANT``, never a substring of the message (the
    typed-signal-not-substring house rule). Same anti-delete wording rule as
    the parent: the message must never advise removing the db — it holds the
    sibling runtime's live coordination state.
    """

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.reason = CROSS_RUNTIME_SCHEMA_REASON


class ReadOnlyRegistryError(RuntimeError):
    """Base for read-only-open failures (the resolver / SB-17 inherit this).

    A read-only :class:`SqliteArtifactRegistry` (``mode=ro`` URI) never creates,
    migrates, or mutates the store. The subclasses below give the CLI a typed,
    prose-free way to distinguish *why* a read-only open or call failed; they
    are exceptions (not the ``ConflictDetail`` typed-return discipline) because
    an open that cannot proceed has no value to return.
    """


class MissingDatabaseError(ReadOnlyRegistryError):
    """A read-only open was asked for a path that does not exist.

    A read-write open would *create* the file; read-only must NOT — a resolver
    pointed at the wrong path should fail loudly, never materialize a fresh
    empty ``.coherence/state.db``.
    """


class ReadOnlyMutationError(ReadOnlyRegistryError):
    """A mutating method was called on a read-only registry."""


class StoreNeedsRecoveryError(ReadOnlyRegistryError):
    """The read-only connection could not read the store (recovery/busy/other).

    After an embedder crash SQLite leaves a hot WAL whose recovery requires a
    *write* lock; a ``mode=ro`` connection raises ``SQLITE_READONLY_RECOVERY``.
    This is distinct from missing / corrupt — the remedy is to re-open
    the store once with the embedder (read-write) so it checkpoints the WAL,
    then retry the read-only resolve. The forensic population over-represents
    crashed writers, so this is the resolver's most on-brand failure mode.

    ``reason`` is the machine-readable classification signal (one of
    ``ccs.core.exceptions.STORE_OPEN_SIGNALS``: ``wal_recovery`` / ``busy`` /
    ``unreadable``), set at the SINGLE classification point
    (:func:`classify_sqlite_operational_signal`). Consumers (the replay
    resolver) branch on ``exc.reason == CONSTANT`` — never on the human
    message — per the typed-signal house rule.
    """

    def __init__(self, message: str, *, reason: str = STORE_SIGNAL_WAL_RECOVERY) -> None:
        super().__init__(message)
        self.reason = reason


def classify_sqlite_operational_signal(exc: sqlite3.OperationalError) -> str:
    """Classify an ``OperationalError`` from a read-only connection into one of
    the ``STORE_OPEN_SIGNALS`` (``busy`` / ``wal_recovery`` / ``unreadable``).

    The ONE place sqlite's rendered error text is substring-matched: Python's
    ``sqlite3`` exposes no stable error code on every path/version, so the
    classification is unavoidably text-based and therefore FRAGILE against
    sqlite message rewording — which is exactly why it must not be duplicated
    (the typed-signal-not-substring house rule,
    ``docs/solutions/best-practices/typed-signal-not-substring-...``). Every
    downstream consumer matches the returned constant with ``==``, never the
    message. Order matters: a locked store is checked FIRST because a busy
    signal has a different remedy (retry) than a recovery one (re-open with
    the embedder).
    """
    text = str(exc).lower()
    if "busy" in text or "locked" in text:
        return STORE_SIGNAL_BUSY
    if "readonly" in text or "recovery" in text or "wal" in text:
        return STORE_SIGNAL_WAL_RECOVERY
    return STORE_SIGNAL_UNREADABLE


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
        retention_policy: RetentionPolicy | None = None,
        read_only: bool = False,
    ) -> None:
        if state_log is not None and instance_id is None:
            raise ValueError(
                "instance_id must be provided when state_log is set; "
                "pass instance_id=str(uuid4()) or route through the plugin coordinator "
                "which manages instance_id persistence in registry_meta"
            )
        if read_only and state_log is not None:
            raise ValueError(
                "read_only=True is incompatible with state_log: a read-only "
                "registry never mutates, so it emits no state_log entries"
            )

        self._db_path = Path(db_path)
        self._lock = threading.RLock()
        self._read_only = read_only
        # Retention is active iff retain_versions=True. ``retention_policy=None``
        # with retain_versions=True == today's UNBOUNDED semantics (no GC) — the
        # back-compat contract for the v0.5 audit auto-wiring. A policy is an
        # explicit opt-in to BOUNDED retention. ``_retain_versions`` stays the
        # private name the recorder test pins. (Mirrors ArtifactRegistry.)
        self._retain_versions = retain_versions
        self._retention_policy = retention_policy
        self._state_log = state_log
        self._agent_names = agent_names

        if read_only:
            self._open_read_only(instance_id)
            return

        self._db_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        # Race-free 0600: pre-create state.db at 0o600 via os.open BEFORE
        # sqlite3.connect, so the file never exists at a broader umask mode in
        # the window between creation and an explicit chmod (the audit_log
        # pattern). os.open is a no-op-ish touch when the file already exists
        # (O_CREAT without O_EXCL); the migration re-applies the mode to a
        # pre-existing, possibly operator-broadened db and warns once.
        self._precreate_file_0600(self._db_path)
        # The -wal/-shm sidecars are pre-created at 0600 BEFORE the connect
        # too: sidecar-creation timing inside sqlite is platform-dependent
        # (some kernels materialize -shm at connection open, not at the
        # journal_mode=WAL switch), so touching them only after the connect
        # would leave a window in which sqlite materializes them under the
        # process umask. With both pre-touches ahead of the connect, no
        # sidecar is ever created by sqlite itself; the tighten-after pass
        # below stays as belt-and-suspenders.
        self._precreate_file_0600(self._wal_path())
        self._precreate_file_0600(self._shm_path())

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
        # Tighten the sidecars again post-WAL-enable: if SQLite created them at
        # the journal_mode switch above (before our pre-touch could win the
        # race on some platforms), this chmod brings them to 0600.
        self._chmod_if_exists(self._wal_path())
        self._chmod_if_exists(self._shm_path())

        self._initialize_schema(instance_id)
        # Persist the retention policy on writer open (incl. the explicit
        # unbounded marker) so a read-only resolver can derive T-expiry + the
        # retention_off reason from the store, not a process-local object.
        self._persist_retention_policy()
        # Policy is immutable per handle (see retention_meta) — cache once.
        self._retention_meta_cache = self._load_retention_meta()

    # ------------------------------------------------------------------
    # File-mode helpers (race-free 0600 — audit_log.py pattern)
    # ------------------------------------------------------------------

    def _wal_path(self) -> Path:
        return self._db_path.with_name(self._db_path.name + "-wal")

    def _shm_path(self) -> Path:
        return self._db_path.with_name(self._db_path.name + "-shm")

    @staticmethod
    def _precreate_file_0600(path: Path) -> None:
        """Create ``path`` at mode 0o600 if absent; leave an existing file as-is.

        ``os.open(O_CREAT|O_WRONLY, 0o600)`` applies the mode atomically at
        creation (subject to umask) so the file never exists at a broader mode
        in a creation->chmod window. We do NOT pass O_EXCL: a benign double-call
        (e.g. the file already there from a prior run) must not raise. The mode
        argument is ignored by the kernel when the file already exists — an
        operator-broadened existing db is re-tightened by the migration's chmod,
        not here.
        """
        try:
            fd = os.open(str(path), os.O_CREAT | os.O_WRONLY, _DB_FILE_MODE)
        except OSError:
            # Parent missing / permission — let sqlite3.connect surface the real
            # error rather than masking it here.
            return
        os.close(fd)

    @staticmethod
    def _chmod_if_exists(path: Path) -> None:
        """Best-effort tighten ``path`` to 0o600 if it exists."""
        try:
            os.chmod(str(path), _DB_FILE_MODE)
        except OSError:
            pass

    # ------------------------------------------------------------------
    # Schema lifecycle
    # ------------------------------------------------------------------

    def _initialize_schema(self, instance_id: str | None) -> None:
        """Apply schema, migrate, or verify user_version; seed registry_meta.

        The migration DISPATCH (the repo's first real one):

        - ``user_version == 0`` (brand-new file) → ``_apply_v2_schema``: the
          COMPLETE current schema (v2 tables + ``session_pins`` + ``session_meta``
          + the artifact index) in one atomic transaction (no shim ever runs on a
          fresh db).
        - ``user_version == 1`` (any wild v1 variant) → ``_migrate_v1_to_v2`` then
          ``_migrate_v2_to_v3`` then ``_migrate_v3_to_v4``: idempotently subsumes
          BOTH additive shims (fence columns + epoch seed, and ``pending_notices``),
          adds ``artifact_versions`` (v2), ``session_pins`` (v3), then
          ``session_meta`` + the artifact index (v4). The wild v1 population is a
          2x2 matrix ({±fence columns} x {±pending_notices}) because both shims
          shipped with no version bump; ``IF NOT EXISTS`` guards make each piece
          apply only when missing.
        - ``user_version == 2`` → ``_migrate_v2_to_v3`` then ``_migrate_v3_to_v4``:
          adds ``session_pins`` (v3), then ``session_meta`` + the index (v4).
        - ``user_version == 3`` → ``_migrate_v3_to_v4``: an existing v3 db
          (``session_pins`` present, ``session_meta`` ABSENT — earlier commits of
          this branch stamped it) gains ``session_meta`` + the index. Without this
          branch such a db would open write-free and crash on the first session op.
        - ``user_version == 4`` (SCHEMA_USER_VERSION) → ``_rehydrate_meta``: the
          WRITE-FREE open path (no ALTER, no IF-NOT-EXISTS) — the prerequisite for
          read-only mode.
        - anything else → :class:`SchemaVersionError` (no destructive advice).

        A :class:`CrossRuntimeSchemaError` pre-empts ALL of the above when the
        db carries sibling-Node-ledger markers (``_reject_foreign_ledger_db``,
        called BEFORE any migration or rehydrate write).
        """
        with self._lock:
            current = self._conn.execute("PRAGMA user_version").fetchone()[0]
            # Cross-runtime fail-closed guard: read-only probes, BEFORE the
            # dispatch can migrate (v1/v2 branch) or write anything — migrating a
            # Node-ledger db would corrupt the sibling coordinator's live state.
            self._reject_foreign_ledger_db(current)
            if current == 0:
                self._apply_v2_schema(instance_id)
            elif current == 1:
                # v1 → v2 → v3 → v4: each step is its own atomic BEGIN IMMEDIATE
                # and rehydrates meta; the chain lands the full current schema.
                self._migrate_v1_to_v2(instance_id)
                self._migrate_v2_to_v3(instance_id)
                self._migrate_v3_to_v4(instance_id)
            elif current == 2:
                # 2 → 3 → 4: add session_pins, then session_meta + the index.
                self._migrate_v2_to_v3(instance_id)
                self._migrate_v3_to_v4(instance_id)
            elif current == _V3_USER_VERSION:
                # An existing v3 db (session_pins, NO session_meta — earlier
                # commits of this branch stamped it) → add session_meta + index.
                # WITHOUT this branch such a db would open write-free and crash on
                # the first session op with 'no such table: session_meta'.
                self._migrate_v3_to_v4(instance_id)
            elif current == SCHEMA_USER_VERSION:
                # Existing v4 database — rehydrate; NO writes on this path (the
                # prerequisite for read-only open mode).
                self._rehydrate_meta(instance_id)
            else:
                raise SchemaVersionError(
                    f"unexpected schema version {current}; this build expects "
                    f"{SCHEMA_USER_VERSION}. The database at {self._db_path} was "
                    f"written by a newer coordinator build — either a newer "
                    f"Python coordinator, or a sibling-runtime coordinator whose "
                    f"ledger this build does not recognize. Upgrade this embedder "
                    f"to a build that understands schema v{current} rather than "
                    f"removing the store — as of v2 it holds durable retained "
                    f"content and read-generation fence state that a delete would "
                    f"destroy."
                )

    # ------------------------------------------------------------------
    # Cross-runtime fail-closed guard (sibling Node coordinator hazard)
    # ------------------------------------------------------------------

    @contextmanager
    def abort_guard(self, abort: "threading.Event | None" = None) -> Iterator[None]:
        """Acquire the registry write lock, then fail closed if the handler
        watchdog already timed out (finding A6).

        The dominant reason a mutation handler exceeds its 4s budget is that it
        is blocked here — on the process-level ``RLock`` that serializes every
        registry mutation — while a peer handler holds it. By the time this
        handler wins the lock the watchdog may have already fired, returned
        ``degraded: true`` to the client, and SET ``abort``. Checking ``abort``
        the instant we win the lock, and holding the lock across the caller's
        whole mutation (the RLock is reentrant, so the inner registry calls
        re-enter freely), makes the late "phantom grant" abort before it lands.

        ``abort=None`` — every non-watchdog caller (CoherentVolume, CCSStore,
        the CLI) — is a plain lock acquire with no behavioural change.

        Residual (documented, finding A6): a ``BEGIN IMMEDIATE`` that starts
        AFTER this check and then blocks on CROSS-PROCESS SQLite write
        contention is not covered. That window is narrow — the coordinator is
        single-process, so the RLock already serializes in-process writers — and
        remains observed by ``watchdog_late_completion_total``.
        """
        with self._lock:
            if abort is not None and abort.is_set():
                raise WatchdogAbandoned(
                    "handler watchdog timed out while this mutation was blocked "
                    "on the registry write lock; aborting before it lands (A6)."
                )
            yield

    def _has_table(self, name: str) -> bool:
        """Read-only probe: does a table exist? (sqlite_master, no writes)."""
        row = self._conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
            (name,),
        ).fetchone()
        return row is not None

    def _has_column(self, table: str, column: str) -> bool:
        """Read-only probe: does ``table`` carry ``column``? PRAGMA table_info
        takes no bindings (callers pass internal literals only) and returns
        zero rows for a missing table — absent table reads as absent column."""
        return any(
            row[1] == column
            for row in self._conn.execute(f"PRAGMA table_info({table})").fetchall()
        )

    def _read_schema_runtime_stamp(self) -> str | None:
        """Return ``registry_meta.schema_runtime``, or None when the table or
        key is absent (a fresh file, or any db stamped before the marker
        shipped — absence is NOT foreign)."""
        if not self._has_table("registry_meta"):
            return None
        row = self._conn.execute(
            "SELECT value FROM registry_meta WHERE key = ?",
            (_META_SCHEMA_RUNTIME,),
        ).fetchone()
        return row[0] if row is not None else None

    def _reject_foreign_ledger_db(self, current: int) -> None:
        """Fail closed when the db carries sibling-Node-coordinator markers.

        Called by BOTH open paths (writer ``_initialize_schema`` and
        ``_open_read_only``) right after reading ``user_version`` and BEFORE
        any migration or rehydrate write; every probe is a plain read
        (SELECT / PRAGMA table_info), so the guard itself can never dirty a
        foreign db.

        WHY structural markers and not the version number: the Node
        coordinator shares this db path but its ledger's v2 adds NO schema
        objects (pending_notices validation) and its v3 ALTERs
        ``agent_states ADD COLUMN deadline_tick``, while THIS repo's v2 adds
        ``artifact_versions`` and its v3 adds ``session_pins`` — the same
        user_version means different schemas depending on the writer. THE LEDGER
        DIVERGENCE AT v3 IS THE SB-17 SECURITY CONCERN: a Node coordinator
        opening this build's Python-v3 db (or this build opening a Node-v3 db)
        must DETECT/REJECT, never silently misread. Detection order (strongest
        marker first):

        1. ``registry_meta.schema_runtime`` present and foreign — the explicit
           lineage stamp, checked regardless of version (this side stamps
           ``python``; the Node side mirrors with ``node``). This alone catches
           the cross-runtime v3 collision in both directions when the stamp is
           present (every Python db since the marker shipped carries it).
        2. ``user_version >= 3`` with ``agent_states.deadline_tick`` — the
           Node ledger's v3 ALTER. ``>= 3``, not ``== 3``: a future Node v4+
           still carries the column (columns are never dropped). A genuine
           Python-v3 db NEVER carries ``deadline_tick`` (this repo's v3 adds
           ``session_pins``, not that column), so this never false-positives on
           our own v3. Literal ``3`` (the Node ledger's version), deliberately
           not SCHEMA_USER_VERSION.
        3. ``user_version == 2`` WITHOUT ``artifact_versions`` — a Python-v2
           db ALWAYS has the table (the v1->v2 migration creates it atomically
           with the version stamp); a Node-v2 db never does. Literal ``2``:
           the check pins user_version-2 semantics even after the Python v3 bump.
        4. ``user_version == 3`` WITHOUT ``session_pins`` — a Python-v3 db
           ALWAYS has the table (the v2->v3 migration / fresh apply creates it
           atomically with the v3 stamp); a Node-v3 db (whose v3 is the
           ``deadline_tick`` ALTER) never does. This is the structural fallback
           for a Node-v3 db whose ``deadline_tick`` probe somehow missed (and
           defense-in-depth alongside the ``schema_runtime`` stamp). Literal
           ``3`` — pins Python-v3 semantics.

        ``user_version == 1`` is deliberately NOT blocked: the Node ledger's
        v1 is a byte-for-byte mirror of this repo's v1 schema, so the two are
        indistinguishable by design — the normal v1->v2 migration proceeds
        (documented residual risk).
        """
        runtime = self._read_schema_runtime_stamp()
        if runtime is not None and runtime != _SCHEMA_RUNTIME_STAMP:
            self._raise_cross_runtime(
                f"is stamped registry_meta.schema_runtime={runtime!r} "
                f"(this build stamps {_SCHEMA_RUNTIME_STAMP!r})"
            )
        if current >= 3 and self._has_column("agent_states", "deadline_tick"):
            self._raise_cross_runtime(
                f"is user_version={current} and carries the "
                f"agent_states.deadline_tick column (the Node ledger's v3 "
                f"watchdog marker; no Python schema has it)"
            )
        if current == 2 and not self._has_table("artifact_versions"):
            self._raise_cross_runtime(
                "is user_version=2 without the artifact_versions table (a "
                "Python-v2 store always has it; the Node ledger's v2 adds no "
                "schema objects)"
            )
        if current == 3 and not self._has_table("session_pins"):
            self._raise_cross_runtime(
                "is user_version=3 without the session_pins table (a "
                "Python-v3 store always has it; the Node ledger's v3 is the "
                "agent_states.deadline_tick ALTER, not session_pins)"
            )

    def _raise_cross_runtime(self, detail: str) -> NoReturn:
        """Compose the single :class:`CrossRuntimeSchemaError` message shape.

        Wording rules: name the sibling Node coordinator as the likely writer,
        state the fail-closed posture, and point at the Node CLI's
        ``--prepare-for-migration`` as the supported backend-switch path. Per
        the :class:`SchemaVersionError` rule, NEVER advise deleting the db —
        it holds the sibling runtime's live coordination state (and possibly
        retained content), which a delete destroys.
        """
        raise CrossRuntimeSchemaError(
            f"the database at {self._db_path} {detail}. The likely writer is "
            f"the sibling Node coordinator (agent-coherence-coordinator), "
            f"which shares this path but keeps its own migration ledger — the "
            f"two ledgers assign different meanings to the same user_version "
            f"numbers. This Python coordinator will not read or migrate a "
            f"foreign-ledger db. To switch the store to this backend, run "
            f"`agent-coherence-coordinator --prepare-for-migration` (the "
            f"supported backend-switch path, which preserves the live "
            f"coordination state the file holds)."
        )

    def _apply_v2_schema(self, instance_id: str | None) -> None:
        """Create the COMPLETE current schema (v2 tables + the v3 ``session_pins``
        table) + seed registry_meta, stamping ``user_version=SCHEMA_USER_VERSION``.
        Caller holds lock. (The method keeps its historical ``_apply_v2_schema``
        name; a fresh db is always built at the latest schema directly so no
        migration shim ever runs against it.)

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

        A fresh db is created at v2 directly: the fence columns are inline in the
        ``artifacts``/``agent_states`` DDL, ``coordinator_epoch`` is seeded, and
        ``pending_notices`` + ``artifact_versions`` are created here — so NO shim
        (``_ensure_fence_columns`` / the ``pending_notices`` IF-NOT-EXISTS) ever
        runs against a fresh db, and the open path that follows is write-free.
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
                    id               TEXT PRIMARY KEY,
                    name             TEXT NOT NULL UNIQUE,
                    version          INTEGER NOT NULL,
                    owner_generation INTEGER NOT NULL DEFAULT 0,
                    content_hash     TEXT NOT NULL,
                    size_tokens      INTEGER,
                    last_writer_id   TEXT,
                    updated_at       REAL NOT NULL
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
                    read_generation      INTEGER,
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
            c.execute(_ARTIFACT_VERSIONS_DDL)
            # session_pins (v3, SB-17 Unit 2): a fresh db is created at the full
            # current schema directly — no v2->v3 shim ever runs against it.
            c.execute(_SESSION_PINS_DDL)
            c.execute(_SESSION_PINS_ARTIFACT_INDEX_DDL)
            c.execute(_SESSION_META_DDL)
            seed_epoch = uuid4().hex
            # schema_runtime: the cross-runtime lineage stamp, seeded inside
            # THIS creation transaction (no extra txn) so the sibling Node
            # coordinator's mirror check can fail closed on an explicit marker
            # instead of structural probing.
            c.execute(
                "INSERT INTO registry_meta (key, value) "
                "VALUES (?, ?), (?, ?), (?, ?), (?, ?)",
                (
                    "instance_id", seed_id,
                    "sequence_number", "0",
                    "coordinator_epoch", seed_epoch,
                    _META_SCHEMA_RUNTIME, _SCHEMA_RUNTIME_STAMP,
                ),
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
        self._coordinator_epoch = seed_epoch

    def _rehydrate_meta(self, instance_id_override: str | None) -> None:
        """Load _instance_id + _seq + epoch from registry_meta. Caller holds lock.

        WRITE-FREE: a v2 db carries the complete schema (the migration subsumed
        every shim), so this open path issues NO DDL/DML — exactly what lets the
        same code be reused read-only. Earlier versions ran the
        ``pending_notices`` IF-NOT-EXISTS and the fence-column ALTERs here on
        EVERY open; those moved into ``_migrate_v1_to_v2`` and run once.
        """
        rows = dict(
            self._conn.execute(
                "SELECT key, value FROM registry_meta WHERE key IN ('instance_id', 'sequence_number')"
            ).fetchall()
        )
        if "instance_id" not in rows or "sequence_number" not in rows:
            raise SchemaVersionError(
                f"registry_meta is missing required keys at {self._db_path}; "
                f"the database may be corrupted (a v2 store always carries these). "
                f"Restore from a backup rather than deleting — the store may hold "
                f"durable retained content."
            )
        # Caller's explicit instance_id wins (rare; typically used in tests).
        self._instance_id = instance_id_override or rows["instance_id"]
        self._seq = int(rows["sequence_number"])
        epoch_row = self._conn.execute(
            "SELECT value FROM registry_meta WHERE key = 'coordinator_epoch'"
        ).fetchone()
        if epoch_row is None:
            raise SchemaVersionError(
                f"coordinator_epoch missing from registry_meta at {self._db_path}; "
                f"the database may be corrupted (a v2 store always seeds the epoch). "
                f"Restore from a backup rather than deleting — the store may hold "
                f"durable retained content and fence state."
            )
        self._coordinator_epoch = epoch_row[0]

    def _migrate_v1_to_v2(self, instance_id: str | None) -> None:
        """Migrate any wild v1 db to v2 in ONE atomic transaction. Caller holds lock.

        Correctness-required (not housekeeping): v1 dbs in the wild form a 2x2
        matrix because BOTH the fence columns + ``coordinator_epoch`` seed AND
        the ``pending_notices`` table shipped as additive shims with no version
        bump. This migration idempotently subsumes BOTH shims and adds the new
        ``artifact_versions`` table, so after it ``user_version=2`` GUARANTEES
        the complete schema from EVERY v1 variant. Each piece is guarded
        (``PRAGMA table_info`` for columns, ``IF NOT EXISTS`` for tables,
        ``INSERT OR IGNORE`` for the epoch) so it applies only when missing.

        Atomicity discipline (learnings skeleton —
        sqlite-schema-init-non-atomic-leaves-unbootable-db): ONE explicit
        ``BEGIN IMMEDIATE`` wrapping all DDL + seed + the ``PRAGMA user_version``
        stamp (which IS transactional inside an explicit txn); individual
        ``execute()`` calls (NEVER ``executescript`` — its autocommit silently
        commits the pending BEGIN IMMEDIATE and varies by Python version);
        ``except BaseException`` ROLLBACK so a SIGKILL/Ctrl-C mid-migration
        leaves the v1 db intact (the next open re-migrates cleanly, never hits
        "table already exists"). Concurrent-loser path: two processes can BOTH
        read ``user_version == 1`` in the dispatch (a bare read, outside any
        txn) and both land here; they serialize on the ``BEGIN IMMEDIATE``
        write lock. The loser re-reads ``user_version`` INSIDE its txn, sees
        the winner already stamped v2, and no-ops (COMMIT immediately, zero
        DDL) before rehydrating — it does NOT re-run the migration body, so a
        future non-idempotent v3 step cannot be double-applied by the race.
        """
        c = self._conn
        c.execute("BEGIN IMMEDIATE")
        try:
            # Concurrent-loser guard: re-check the stamp now that the write
            # lock is held. The dispatch decision was made from a pre-lock
            # read; if a racing winner already advanced PAST v1 (to v2 or the
            # current v3), the v1->v2 body has nothing left to do — COMMIT the
            # empty txn and rehydrate. ``>= _V2_USER_VERSION`` (not ``==``)
            # because the winner may have already chained on to v3. The caller's
            # chained ``_migrate_v2_to_v3`` then runs its OWN loser-guarded txn,
            # which no-ops if v3 is already stamped.
            if c.execute("PRAGMA user_version").fetchone()[0] >= _V2_USER_VERSION:
                c.execute("COMMIT")
                self._rehydrate_meta(instance_id)
                return
            art_cols = {
                row[1] for row in c.execute("PRAGMA table_info(artifacts)").fetchall()
            }
            state_cols = {
                row[1] for row in c.execute("PRAGMA table_info(agent_states)").fetchall()
            }
            # --- Subsume the fence-column shim (was _ensure_fence_columns) ---
            if "owner_generation" not in art_cols:
                c.execute(
                    "ALTER TABLE artifacts ADD COLUMN owner_generation "
                    "INTEGER NOT NULL DEFAULT 0"
                )
            if "read_generation" not in state_cols:
                # Nullable: a pre-fence grant captured no generation. NULL is the
                # absent operand the commit guard ADMITS (a writer that never
                # established a fence claim -- version-CAS arbitrates it); only a
                # present-and-superseded read_generation is rejected.
                c.execute("ALTER TABLE agent_states ADD COLUMN read_generation INTEGER")
            c.execute(
                "INSERT OR IGNORE INTO registry_meta (key, value) VALUES (?, ?)",
                ("coordinator_epoch", uuid4().hex),
            )
            # Cross-runtime lineage stamp, atomic with the v1->v2 stamp in
            # THIS transaction (no extra txn). A genuine v1 db never carries
            # the key (the marker postdates v1), but OR IGNORE keeps the step
            # idempotent alongside the other half-migrated-db guards. The
            # foreign-stamp case was already rejected at dispatch
            # (_reject_foreign_ledger_db runs before the migration).
            c.execute(
                "INSERT OR IGNORE INTO registry_meta (key, value) VALUES (?, ?)",
                (_META_SCHEMA_RUNTIME, _SCHEMA_RUNTIME_STAMP),
            )
            # --- Subsume the pending_notices shim (was the IF-NOT-EXISTS) ---
            c.execute(
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
            # --- The new v2 surface: durable retention table ---
            # IF NOT EXISTS guards the half-migrated-db case (table created by a
            # prior crashed migration whose user_version stamp never committed).
            c.execute(_ARTIFACT_VERSIONS_DDL.replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS", 1))
            # In-txn stamp: PRAGMA user_version is transactional inside an explicit
            # BEGIN; commits atomically with the DDL above. Cannot take bindings,
            # so the int constant is interpolated. Stamps the INTERMEDIATE v2 (not
            # the current SCHEMA_USER_VERSION) — the chained _migrate_v2_to_v3 adds
            # session_pins and advances to v3.
            c.execute(f"PRAGMA user_version = {_V2_USER_VERSION}")
            c.execute("COMMIT")
        except BaseException:
            try:
                c.execute("ROLLBACK")
            except Exception:
                pass
            raise
        # Load meta from the now-migrated db (write-free).
        self._rehydrate_meta(instance_id)
        # A v1 db predates durable content storage; v2 may now persist content,
        # so re-apply 0600 to a possibly operator-broadened file and warn ONCE
        # that the content-storage posture changed (audit_log drift pattern).
        self._tighten_existing_db_mode_and_warn()

    def _migrate_v2_to_v3(self, instance_id: str | None) -> None:
        """Migrate a v2 db to the intermediate v3 in ONE atomic transaction
        (SB-17 / TX-1, Unit 2). Caller holds lock. Chained to ``_migrate_v3_to_v4``
        by the open dispatcher (a v2 db steps 2 -> 3 -> 4).

        Additive-only: adds the ``session_pins`` table and stamps
        ``user_version=_V3_USER_VERSION`` — it touches NO existing table, so there
        is no shim to subsume and no content/mode posture change. Same atomicity
        discipline as ``_migrate_v1_to_v2`` (the
        sqlite-schema-init-non-atomic-leaves-unbootable-db learning): ONE explicit
        ``BEGIN IMMEDIATE`` wrapping the DDL + the ``PRAGMA user_version`` stamp,
        individual ``execute()`` calls (never ``executescript``), and ``except
        BaseException`` ROLLBACK so a SIGKILL mid-migration leaves the v2 db intact
        for a clean re-migrate. ``session_meta`` + the artifact index belong to
        the v3->v4 step (they were added after earlier commits already stamped v3).

        Concurrent-loser path: two processes can both read ``user_version == 2``
        (a bare pre-lock read) and both land here; they serialize on the
        ``BEGIN IMMEDIATE`` write lock. The loser re-reads ``user_version`` INSIDE
        its txn, sees the winner already at >= v3, and no-ops.
        """
        c = self._conn
        c.execute("BEGIN IMMEDIATE")
        try:
            if c.execute("PRAGMA user_version").fetchone()[0] >= _V3_USER_VERSION:
                # A racing winner already advanced to >= v3 — nothing to do here
                # (the chained v3->v4 step runs next regardless).
                c.execute("COMMIT")
                self._rehydrate_meta(instance_id)
                return
            # IF NOT EXISTS guards the half-migrated-db case (table created by a
            # prior crashed migration whose user_version stamp never committed).
            c.execute(
                _SESSION_PINS_DDL.replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS", 1)
            )
            c.execute(f"PRAGMA user_version = {_V3_USER_VERSION}")
            c.execute("COMMIT")
        except BaseException:
            try:
                c.execute("ROLLBACK")
            except Exception:
                pass
            raise
        # Load meta from the now-migrated db (write-free).
        self._rehydrate_meta(instance_id)

    def _migrate_v3_to_v4(self, instance_id: str | None) -> None:
        """Migrate a v3 db (``session_pins`` present, ``session_meta`` absent) to
        v4 in ONE atomic transaction (SB-17 / TX-1, durable owner-binding). Caller
        holds lock.

        Why a dedicated step rather than folding it into the v2->v3 add: earlier
        commits of THIS branch already shipped ``user_version=3`` with
        ``session_pins`` but no ``session_meta``. Such a db opens via the
        write-free rehydrate path (``current == SCHEMA_USER_VERSION`` once that was
        4) and would NEVER gain ``session_meta`` — so every ``begin_session`` /
        liveness sweep would raise ``no such table: session_meta``. Routing
        ``current == 3`` through this step creates the table (and the
        ``session_pins`` artifact index) for any v3 db, fresh-this-build or
        earlier-branch.

        Additive-only (``session_meta`` + ``idx_session_pins_artifact``), same
        atomicity discipline as the other migrations. Idempotent via ``IF NOT
        EXISTS`` (a fresh-this-build db already created both in ``_apply_v2_schema``
        and never routes here; this step only fires for an existing v3 db).
        """
        c = self._conn
        c.execute("BEGIN IMMEDIATE")
        try:
            if c.execute("PRAGMA user_version").fetchone()[0] >= SCHEMA_USER_VERSION:
                # A racing winner already advanced to v4 — nothing to do.
                c.execute("COMMIT")
                self._rehydrate_meta(instance_id)
                return
            c.execute(
                _SESSION_META_DDL.replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS", 1)
            )
            c.execute(
                _SESSION_PINS_ARTIFACT_INDEX_DDL.replace(
                    "CREATE INDEX", "CREATE INDEX IF NOT EXISTS", 1
                )
            )
            c.execute(f"PRAGMA user_version = {SCHEMA_USER_VERSION}")
            c.execute("COMMIT")
        except BaseException:
            try:
                c.execute("ROLLBACK")
            except Exception:
                pass
            raise
        # Load meta from the now-migrated db (write-free).
        self._rehydrate_meta(instance_id)

    def _tighten_existing_db_mode_and_warn(self) -> None:
        """Re-apply 0600 to a pre-existing db whose mode an operator may have
        broadened, emitting a one-time stderr warning that the content-storage
        posture changed at v1->v2. Mirrors audit_log's mode-drift warning."""
        try:
            actual = stat.S_IMODE(self._db_path.stat().st_mode)
        except OSError:
            return
        if actual != _DB_FILE_MODE:
            logger.warning(
                "state.db at %s had mode %o (expected %o). The v1->v2 migration "
                "tightened it to %o: as of schema v2 this store persists durable "
                "retained artifact content, so it is now as sensitive as the "
                "content itself. Copies of state.db do NOT inherit 0600 and carry "
                "the same content — treat them accordingly.",
                self._db_path, actual, _DB_FILE_MODE, _DB_FILE_MODE,
            )
            self._chmod_if_exists(self._db_path)
        self._chmod_if_exists(self._wal_path())
        self._chmod_if_exists(self._shm_path())

    def _open_read_only(self, instance_id: str | None) -> None:
        """Open the store read-only: never creates, migrates, ALTERs, or mutates.

        Used by the replay resolver (Unit 6) and SB-17 later. The
        ``file:...?mode=ro`` URI requires ``uri=True`` to be passed EXPLICITLY —
        without it sqlite3 treats the whole string as a literal filename and can
        silently fall back to read-write (and create the file). A missing path is
        a typed :class:`MissingDatabaseError` (a read-write open would create it;
        read-only must not materialize a fresh empty store). A v1 (un-migrated)
        db is a typed :class:`SchemaVersionError` with NO migration attempted. A
        post-crash hot WAL that needs replay raises
        :class:`StoreNeedsRecoveryError` (a read-only connection cannot run WAL
        recovery — SQLITE_READONLY_RECOVERY).
        """
        if not self._db_path.exists():
            raise MissingDatabaseError(
                f"read-only open: no database at {self._db_path}. A read-only "
                f"registry never creates the file (that would materialize a fresh "
                f"empty store); point it at an existing coordinator state.db."
            )
        # mode=ro + uri=True (explicit) — see docstring on the silent-rw hazard.
        uri = f"file:{self._db_path}?mode=ro"
        self._conn = sqlite3.connect(
            uri,
            uri=True,
            check_same_thread=False,
            isolation_level=None,
        )
        # Everything past the connect is validation that can raise a TYPED
        # error; without the close-on-raise the construction failure would leak
        # the open handle (the caller never gets a registry to .close()). The
        # BaseException breadth mirrors the transaction-rollback idiom: a
        # Ctrl-C mid-open must not orphan the connection either.
        try:
            # foreign_keys is a no-op for a read-only conn (no writes); set for
            # parity. Do NOT touch journal_mode (that is a write) — opening ro
            # against a WAL db is fine for reads. busy_timeout helps a reader
            # wait out a concurrent writer's lock.
            self._conn.execute("PRAGMA busy_timeout=1500")
            try:
                current = self._conn.execute("PRAGMA user_version").fetchone()[0]
                # Cross-runtime fail-closed guard (same classification as the
                # writer path): the probes are plain reads, so a busy/hot-WAL
                # store surfaces the SAME typed signal as the version read.
                self._reject_foreign_ledger_db(current)
            except sqlite3.OperationalError as exc:
                raise self._classify_readonly_operational_error(exc) from exc
            if current > SCHEMA_USER_VERSION:
                # No Node marker matched above, so this is the future-Python
                # posture — but a sibling-runtime ledger is also a possible
                # writer, and the embedder cannot migrate DOWN, so the v1
                # remedy below would be wrong advice here.
                raise SchemaVersionError(
                    f"read-only open: database at {self._db_path} is schema "
                    f"v{current}, this build serves v{SCHEMA_USER_VERSION}. Either "
                    f"a newer Python coordinator or a sibling-runtime coordinator "
                    f"whose ledger this build does not recognize may have written "
                    f"it. Read-only mode performs NO migration — use a build that "
                    f"understands schema v{current}, then retry."
                )
            if current != SCHEMA_USER_VERSION:
                raise SchemaVersionError(
                    f"read-only open: database at {self._db_path} is schema "
                    f"v{current}, this build serves v{SCHEMA_USER_VERSION}. Read-only "
                    f"mode performs NO migration — re-open the store once with the "
                    f"embedder (read-write) to migrate it, then retry."
                )
            try:
                self._rehydrate_meta(instance_id)
            except sqlite3.OperationalError as exc:
                raise self._classify_readonly_operational_error(exc) from exc
            # Policy is immutable per handle (see retention_meta) — cache once.
            self._retention_meta_cache = self._load_retention_meta()
        except BaseException:
            self._conn.close()
            raise

    def _classify_readonly_operational_error(
        self, exc: sqlite3.OperationalError
    ) -> ReadOnlyRegistryError:
        """Map an OperationalError on a read-only connection to a typed error.

        Classification routes through the single
        :func:`classify_sqlite_operational_signal` seam; the signal rides on
        ``StoreNeedsRecoveryError.reason`` so consumers (the replay resolver)
        branch on the attribute, never re-parse the message. All three signals
        stay ONE exception type — a locked store and a hot WAL are both
        "cannot read right now" from the registry's perspective; the reason
        carries the operator remedy.
        """
        signal = classify_sqlite_operational_signal(exc)
        if signal == STORE_SIGNAL_BUSY:
            return StoreNeedsRecoveryError(
                f"the store at {self._db_path} is locked by a concurrent writer "
                f"(SQLITE_BUSY) and the read-only open timed out waiting for the "
                f"lock. Retry shortly. Underlying: {exc}",
                reason=signal,
            )
        if signal == STORE_SIGNAL_WAL_RECOVERY:
            return StoreNeedsRecoveryError(
                f"the store at {self._db_path} has an unclean write-ahead log that "
                f"a read-only connection cannot replay (SQLITE_READONLY_RECOVERY). "
                f"Re-open it once with the embedder (read-write) so it checkpoints "
                f"the WAL, then retry the read-only resolve. Underlying: {exc}",
                reason=signal,
            )
        return StoreNeedsRecoveryError(
            f"the read-only store at {self._db_path} could not be read "
            f"({exc}); it may need recovery (re-open once with the embedder) or "
            f"be corrupt.",
            reason=signal,
        )

    def _persist_retention_policy(self) -> None:
        """Persist the constructor retention policy to registry_meta (writer open).

        Stores ``retention_enabled`` (the explicit marker — set even in unbounded
        mode) plus the two axes (NULL when an axis is disabled or unbounded). A
        read-only resolver derives ``retention_off`` from the absence/0 of the
        marker and the T-expiry axis from the persisted ``max_age_seconds`` — so
        retention semantics are STORE-derived, not process-local. No open-time GC
        pass (dropped per plan): each writer's inline GC trusts its CONSTRUCTOR
        policy; the v1 one-writer-per-store assumption means the last writer
        open's persisted policy is what readers see.

        Write-free steady-state: the three keys are READ first and the UPSERT is
        skipped when the persisted values already match — a same-policy reopen
        issues no write at all (the property the write-free v2 open path
        documents, and what keeps a reopen from dirtying the WAL).
        """
        if self._read_only:
            return
        policy = self._retention_policy
        desired: dict[str, str | None] = {
            _META_RETENTION_ENABLED: "1" if self._retain_versions else "0",
            _META_RETENTION_MAX_VERSIONS: (
                str(policy.max_versions)
                if policy is not None and policy.max_versions is not None
                else None
            ),
            _META_RETENTION_MAX_AGE_SECONDS: (
                str(policy.max_age_seconds)
                if policy is not None and policy.max_age_seconds is not None
                else None
            ),
        }
        with self._lock:
            persisted = dict(
                self._conn.execute(
                    "SELECT key, value FROM registry_meta WHERE key IN (?, ?, ?)",
                    tuple(desired),
                ).fetchall()
            )
            # All three keys present AND equal (incl. NULL axes) ⇒ no write.
            # len() distinguishes an absent key from a present-NULL value.
            if len(persisted) == len(desired) and all(
                persisted[key] == value for key, value in desired.items()
            ):
                return
            # About to OVERWRITE a different persisted policy — warn first
            # (the operator-visible half of the single-writer-embedder
            # assumption; see _warn_on_policy_mismatch for the scope rules).
            self._warn_on_policy_mismatch(persisted, desired)
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                for key, value in desired.items():
                    self._conn.execute(
                        """
                        INSERT INTO registry_meta (key, value) VALUES (?, ?)
                        ON CONFLICT(key) DO UPDATE SET value = excluded.value
                        """,
                        (key, value),
                    )
                self._conn.execute("COMMIT")
            except BaseException:
                # Guarded ROLLBACK (the schema-init idiom): a COMMIT that failed
                # mid-promote may have already auto-rolled-back, in which case
                # the explicit ROLLBACK raises "no transaction is active" and
                # would mask the original error.
                try:
                    self._conn.execute("ROLLBACK")
                except (sqlite3.Error, OSError):
                    pass
                raise

    def _warn_on_policy_mismatch(
        self, persisted: dict[str, str | None], desired: dict[str, str | None]
    ) -> None:
        """Emit ONE RuntimeWarning when this writer's constructor retention
        policy is about to overwrite a DIFFERENT previously persisted one.

        WHY: the durable store's contract is one writer-embedder per store
        (see :meth:`retention_meta`); each writer's inline GC trusts its
        CONSTRUCTOR policy, so two embedders alternating with different
        policies flip-flop the persisted policy AND the GC behaviour readers
        derive from it — silently, unless surfaced here BEFORE the overwrite.

        Scope: both sides retention-ENABLED with differing K/T axes. A first
        persist (no keys yet) is setup, not a conflict; a matching reopen was
        already short-circuited by the caller; the enable/disable toggles stay
        silent (deliberate acts with their own documented semantics — see
        ``test_disable_retention_preserves_existing_rows``); read-only mode
        never reaches the caller at all.
        """
        if not self._retain_versions:
            return
        if persisted.get(_META_RETENTION_ENABLED) != "1":
            return  # nothing previously persisted with retention enabled
        # .get() folds absent-key and present-NULL to None — both mean "axis
        # unbounded/disabled", which is exactly the comparison we want here.
        p_k = persisted.get(_META_RETENTION_MAX_VERSIONS)
        p_t = persisted.get(_META_RETENTION_MAX_AGE_SECONDS)
        c_k = desired[_META_RETENTION_MAX_VERSIONS]
        c_t = desired[_META_RETENTION_MAX_AGE_SECONDS]
        if (p_k, p_t) == (c_k, c_t):
            return
        # stacklevel=4: warn() -> this helper -> _persist_retention_policy ->
        # __init__ -> the embedder's constructor call site (the actionable
        # frame: that is where the divergent policy is passed).
        warnings.warn(
            f"retention policy mismatch at {self._db_path}: the store persists "
            f"max_versions={p_k} / max_age_seconds={p_t}, but this writer was "
            f"constructed with max_versions={c_k} / max_age_seconds={c_t}; "
            f"overwriting with the constructor policy. The durable store "
            f"assumes a SINGLE writer-embedder per store — alternating "
            f"embedders with different policies flip-flop the persisted "
            f"policy and its GC behaviour.",
            RuntimeWarning,
            stacklevel=4,
        )

    def _load_retention_meta(self) -> tuple[bool, RetentionPolicy | None]:
        """Read ``(retention_enabled, persisted_policy_or_None)`` from
        registry_meta — the construction-time loader behind the
        :meth:`retention_meta` cache. One SELECT under the lock."""
        with self._lock:
            rows = dict(
                self._conn.execute(
                    "SELECT key, value FROM registry_meta WHERE key IN (?, ?, ?)",
                    (
                        _META_RETENTION_ENABLED,
                        _META_RETENTION_MAX_VERSIONS,
                        _META_RETENTION_MAX_AGE_SECONDS,
                    ),
                ).fetchall()
            )
        enabled = rows.get(_META_RETENTION_ENABLED) == "1"
        if not enabled:
            return False, None
        max_v = rows.get(_META_RETENTION_MAX_VERSIONS)
        max_a = rows.get(_META_RETENTION_MAX_AGE_SECONDS)
        if max_v is None and max_a is None:
            return True, None  # unbounded (NULL axes)
        return True, RetentionPolicy(
            max_versions=int(max_v) if max_v is not None else None,
            max_age_seconds=float(max_a) if max_a is not None else None,
        )

    def retention_meta(self) -> tuple[bool, RetentionPolicy | None]:
        """Return ``(retention_enabled, persisted_policy_or_None)``. Used by
        Unit 4's ``read_at_version`` (store-derived ``retention_off`` +
        T-expiry) on both the writer and the read-only resolver.
        ``retention_enabled=False`` ⇒ retention was never turned on for this
        store; ``True`` with a ``None`` policy ⇒ unbounded (NULL axes).

        Served from a construction-time cache, not a per-call SELECT: the
        policy is immutable per handle (a writer persists its constructor
        policy exactly once at open; nothing rewrites the keys while the handle
        lives), readers are short-lived, and the documented single-writer
        assumption means a peer writer changing the persisted policy mid-handle
        is outside the contract — so caching at construction is correct and
        keeps ``read_at_version`` loops off the DB for this lookup."""
        return self._retention_meta_cache

    def _guard_writable(self) -> None:
        """Raise if a mutator was called on a read-only registry."""
        if self._read_only:
            raise ReadOnlyMutationError(
                f"this SqliteArtifactRegistry was opened read-only "
                f"(mode=ro) against {self._db_path}; mutating methods are "
                f"rejected. Open it read-write (the embedder) to mutate."
            )

    def _capture_version_sql(
        self, artifact_id: UUID, version: int, content: bytes | str
    ) -> None:
        """Snapshot ``content`` under ``version`` + run inline K/T GC, INSIDE the
        caller's already-open ``BEGIN IMMEDIATE`` (crash-atomic with the commit /
        version-bump it captures — a rollback leaves neither phantom history nor
        a version move). Caller holds the lock and an open transaction; this adds
        NO new lock acquisition (busy_timeout derivation untouched).

        Mirrors the in-memory ``ArtifactRegistry._capture_version``: store body +
        wall-clock ``captured_at``, then (only when a bounded policy is set) drop
        the versions :func:`collectible_versions` marks — the current version is
        always exempt; unbounded mode (policy=None) skips GC entirely, preserving
        today's semantics. Decisions route through the persisted authoritative
        rows (this txn), never an in-memory mirror.

        ``content`` is stored against the affinity-NONE ``content`` column so a
        ``str`` round-trips TEXT and ``bytes`` round-trips BLOB by value. The
        single ``time.time()`` read is both the stamp and the GC reference.
        """
        c = self._conn
        captured_at = time.time()
        # REPLACE (not INSERT): re-capturing the same version (e.g. a retried
        # commit at the same version on the in-memory parity path) overwrites
        # rather than raising on the (artifact_id, version) PK.
        c.execute(
            "INSERT OR REPLACE INTO artifact_versions "
            "(artifact_id, version, content, captured_at) VALUES (?, ?, ?, ?)",
            (artifact_id.hex, version, content, captured_at),
        )
        if self._retention_policy is None:
            return  # unbounded mode (retain_versions=True, no policy): no GC.
        # Read the authoritative retained set for this artifact (this txn) and
        # compute the drop set with the SAME pure seam the in-memory path uses.
        rows = c.execute(
            "SELECT version, captured_at FROM artifact_versions WHERE artifact_id = ?",
            (artifact_id.hex,),
        ).fetchall()
        timestamps = {int(v): float(ts) for v, ts in rows}
        # Exemptions seam (Unit 2 — the first GC producer to populate it): every
        # version pinned by a LIVE snapshot session for this artifact is held
        # back from collection (R4). Read from session_pins in THIS same txn so
        # the exemption set is consistent with the capture (a concurrent
        # capture_version_vector / release_session serializes on the same
        # connection lock + BEGIN IMMEDIATE).
        for dropped in collectible_versions(
            timestamps,
            current_version=version,
            policy=self._retention_policy,
            now=captured_at,
            exemptions=self._live_pins_for_artifact_sql(artifact_id),
        ):
            c.execute(
                "DELETE FROM artifact_versions WHERE artifact_id = ? AND version = ?",
                (artifact_id.hex, dropped),
            )

    def _live_pins_for_artifact_sql(self, artifact_id: UUID) -> set[int]:
        """Return the versions pinned by LIVE snapshot sessions for ``artifact_id``
        (the GC exemption set, Unit 2 / R4) from the persisted ``session_pins``
        table. Mirrors the in-memory ``_live_pins_for_artifact`` and
        ``Snapshot.tla``'s ``PinnedVersions(art)``.

        MUST be called inside the caller's already-open ``BEGIN IMMEDIATE`` (it
        issues a bare ``SELECT`` on ``self._conn`` with no transaction of its
        own), so the exemption read is consistent with the capture/GC it feeds —
        a peer ``capture_version_vector`` / ``release_session`` serializes on the
        same connection lock and cannot half-apply between this read and the
        GC delete."""
        rows = self._conn.execute(
            "SELECT version FROM session_pins WHERE artifact_id = ?",
            (artifact_id.hex,),
        ).fetchall()
        return {int(r[0]) for r in rows}

    @property
    def coordinator_epoch(self) -> str:
        """The store's coordinator epoch (persisted in registry_meta).

        Read from ``registry_meta.coordinator_epoch`` at open (writer OR
        read-only), so a resolver opening the durable store read-only sees the
        same epoch the writer minted. Unit 4's ``read_at_version`` stamps this on
        every response/rejection and compares an optional ``expected_epoch``
        against it. Survives restart (unlike the in-memory registry's
        per-construction epoch). Epoch reset = delete-and-recreate the db, which
        also drops ``artifact_versions``."""
        return self._coordinator_epoch

    @property
    def instance_id(self) -> str:
        """The store's persisted instance identity (``registry_meta.instance_id``).

        Public read-only accessor mirroring :attr:`ArtifactRegistry.instance_id`
        so identity consumers (the replay resolver's ``--instance-id``
        cross-check, trace-manifest comparisons) never reach into the private
        field. Seeded at first create, stable across reopens."""
        return self._instance_id

    def get_owner_generation(self, artifact_id: UUID) -> int:
        """Return the artifact's ownership epoch (read-generation fence)."""
        with self._lock:
            row = self._conn.execute(
                "SELECT owner_generation FROM artifacts WHERE id = ?", (artifact_id.hex,)
            ).fetchone()
            if row is None:
                raise KeyError(f"artifact {artifact_id} not in registry")
            return row[0]

    def get_read_generation(self, artifact_id: UUID, agent_id: UUID) -> int | None:
        """Return the generation an agent captured at its last claim, or None if
        it never established a fence claim (a plain OCC writer that version-CAS,
        not the fence, arbitrates)."""
        with self._lock:
            row = self._conn.execute(
                "SELECT read_generation FROM agent_states "
                "WHERE artifact_id = ? AND agent_id = ?",
                (artifact_id.hex, agent_id.hex),
            ).fetchone()
            return row[0] if row is not None else None

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
        """Insert artifact record. ``content_hash`` (KTD-13) is always stored;
        the ``content`` BODY is stored in ``artifact_versions`` ONLY when
        retention is active (``retain_versions=True``) — otherwise discarded.

        Threat-model note: with retention on, ``register_artifact`` puts the v1
        body of a merely-OBSERVED artifact on disk (per plan R2 + docs/security)."""
        self._guard_writable()
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
                # Durable capture INSIDE this txn (crash-atomic with the insert).
                if self._retain_versions:
                    self._capture_version_sql(artifact.id, artifact.version, content)
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
        fence_agent_id: Optional[UUID] = None,
    ) -> None:
        """Replace artifact metadata for an existing record. ``content`` is
        ignored per KTD-13 — content_hash on the artifact is the source of
        truth for staleness comparison.

        Read-generation fence (pessimistic ``commit()`` path): when
        ``fence_agent_id`` is given, reject -- atomically with the version
        persist in this BEGIN IMMEDIATE -- if that committer's captured
        read_generation was superseded by a sweep reclamation. A ``None``
        fence_agent_id (source-churn) is unguarded.

        With retention active, the NEW ``content`` body is captured under the
        NEW version in ``artifact_versions``, atomically inside this txn (a fence
        reject raises before the capture, so no phantom history). Otherwise the
        body is discarded (KTD-13)."""
        self._guard_writable()
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                if fence_agent_id is not None:
                    rg_row = self._conn.execute(
                        "SELECT read_generation FROM agent_states "
                        "WHERE artifact_id = ? AND agent_id = ?",
                        (artifact_id.hex, fence_agent_id.hex),
                    ).fetchone()
                    if rg_row is not None and rg_row[0] is not None:
                        og_row = self._conn.execute(
                            "SELECT owner_generation FROM artifacts WHERE id = ?",
                            (artifact_id.hex,),
                        ).fetchone()
                        if og_row is None:
                            raise KeyError(f"artifact {artifact_id} not in registry")
                        owner_gen = og_row[0]
                        if rg_row[0] < owner_gen:
                            # Raise inside the try; the except below ROLLBACKs
                            # and re-raises (no double rollback).
                            raise StaleReadGeneration(
                                f"{STALE_READ_GENERATION_REASON} agent={fence_agent_id} "
                                f"artifact={artifact_id} read_gen={rg_row[0]} "
                                f"owner_gen={owner_gen}"
                            )
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
                # Durable capture of the NEW body under the NEW version, INSIDE
                # this txn (the fence reject above raises before we reach here).
                if self._retain_versions:
                    self._capture_version_sql(artifact_id, artifact.version, content)
                self._conn.execute("COMMIT")
            except BaseException:
                # P2 ce-review fix #14 (kieran-python): BaseException catches
                # KeyboardInterrupt/SystemExit mid-transaction so ROLLBACK fires
                # before propagation — otherwise the connection is left with an
                # uncommitted transaction that the next BEGIN IMMEDIATE sees.
                self._conn.execute("ROLLBACK")
                raise

    def get_content_at_version(
        self, artifact_id: UUID, version: int
    ) -> str | bytes | None:
        """Return the retained body for ``(artifact_id, version)``, or ``None``.

        Durable as of v2 (plan item N v1, Unit 3): reads the ``artifact_versions``
        table and returns the value with its original Python type — ``str`` for a
        TEXT row, ``bytes`` for a BLOB row (the affinity-NONE column round-trips
        by value). ``None`` when retention was never on, the row was K-evicted /
        T-expired, or it was never captured (e.g. ``commit_cas(content=None)``).
        Unit 4 builds the typed-reason surface (``not_retained`` vs ``retention_off``)
        above this raw getter.
        """
        with self._lock:
            row = self._conn.execute(
                "SELECT content FROM artifact_versions "
                "WHERE artifact_id = ? AND version = ?",
                (artifact_id.hex, version),
            ).fetchone()
        return row[0] if row is not None else None

    def get_version_record(
        self, artifact_id: UUID, version: int
    ) -> tuple[str | bytes, float] | None:
        """Return ``(content, captured_at)`` for a retained version, or ``None``.

        The single accessor Unit 4's ``read_at_version`` needs: the body AND its
        wall-clock ``captured_at`` (for ``VersionedContent.captured_at`` and the
        T-expiry check) in ONE call. Mirrors
        :meth:`ArtifactRegistry.get_version_record` so the two registries share
        one duck-type.

        Single-scope (R5 atomicity): the body and timestamp come from ONE row of
        ONE ``SELECT`` under ONE lock, so a racing writer's ``BEGIN IMMEDIATE``
        commit cannot interleave between reading the content and reading its
        stamp — they are consistent by construction (no separate-statement
        window two getters would have). Returns the body with its original Python
        type (TEXT→``str``, BLOB→``bytes``; affinity-NONE column). ``None`` when
        retention was never on, the row was K-evicted / T-expired, or it was
        never captured (e.g. ``commit_cas(content=None)``)."""
        with self._lock:
            row = self._conn.execute(
                "SELECT content, captured_at FROM artifact_versions "
                "WHERE artifact_id = ? AND version = ?",
                (artifact_id.hex, version),
            ).fetchone()
        if row is None:
            return None
        return row[0], float(row[1])

    def remove_artifact(self, artifact_id: UUID) -> None:
        """Remove artifact and cascade-delete agent_states + retained history.

        ``artifact_versions`` carries ``FOREIGN KEY ... ON DELETE CASCADE`` and
        ``PRAGMA foreign_keys=ON`` is set on the connection, so the retained
        history rows drop with the artifact (deleted ≡ never-existed)."""
        self._guard_writable()
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
    # Snapshot consistent-cut capture + pin store (SB-17 / TX-1, Unit 2)
    # ------------------------------------------------------------------

    def capture_version_vector(
        self,
        read_set: Iterable[UUID],
        session_token: str,
        *,
        owner: UUID | None = None,
        created_at_tick: int | None = None,
    ) -> CaptureResult:
        """Atomically pin a consistent multi-artifact CUT (SB-17 / TX-1, Unit 2 /
        R1). Written fresh from the contract — parity with
        :meth:`ArtifactRegistry.capture_version_vector` (the two registries share
        no base class), divergent ONLY on restart-survival (sqlite pins are
        durable; in-memory pins are process-scoped — the parity harness asserts
        the divergence).

        ONE ``BEGIN IMMEDIATE`` under one lock acquisition does the entire
        multi-artifact version read + row-count validation + pin insert — the
        ``status_snapshot`` consistent-multi-artifact-read-under-one-lock pattern
        + ``commit_cas``'s ``BEGIN IMMEDIATE``. A peer ``commit_cas`` is
        serialized entirely before or after the whole capture, never partially
        visible within the cut (no read skew).

        Non-mutating on the coherence plane: it mints NO MESI grant and captures
        NO ``read_generation`` (it never touches ``agent_states`` / the fence
        path) — a reader is not an owner. It writes ONLY the ``session_pins``
        rows (the durable mirror of the cut).

        Unknown-id validation (security, F7): the captured row-count must equal
        ``len(read_set)``. Any missing id → a typed
        :class:`~ccs.core.types.VersionedReadRejection` (``unknown_artifact``)
        and the txn COMMITs having inserted NO pins (no partial cut, no
        existence-probe oracle — the rejection is decided before any pin write).

        Args:
            read_set: The artifact ids to pin into the cut. Empty → empty cut.
            session_token: The server-minted session identity the pins key under.

        Returns:
            The pinned cut ``{artifact_id: version}`` on success, else a
            :class:`VersionedReadRejection` (``unknown_artifact``) — no pins
            inserted.
        """
        self._guard_writable()
        ids = list(read_set)
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                cut: dict[UUID, int] = {}
                missing: list[UUID] = []
                for artifact_id in ids:
                    row = self._conn.execute(
                        "SELECT version FROM artifacts WHERE id = ?",
                        (artifact_id.hex,),
                    ).fetchone()
                    if row is None:
                        missing.append(artifact_id)
                        continue
                    cut[artifact_id] = row[0]
                # F7: row-count == len(read_set) BEFORE any pin insert. A missing
                # id rejects the WHOLE capture — COMMIT the (pin-free) txn and
                # return the typed rejection. Nothing was written, so the COMMIT
                # just releases the lock.
                if missing:
                    self._conn.execute("COMMIT")
                    return VersionedReadRejection(
                        reason=UNKNOWN_ARTIFACT_REASON,
                        artifact_id=sorted(missing, key=lambda a: a.int)[0],
                        requested_version=0,
                        current_version=None,
                        coordinator_epoch=self._coordinator_epoch,
                    )
                # Insert the pins atomically with the version read (same txn) so
                # the exemptions seam sees the full cut the instant it is live.
                # INSERT OR REPLACE: re-binding a token (should not happen — the
                # service mints a fresh token per session) overwrites rather than
                # raising on the (session_token, artifact_id) PK.
                for artifact_id, version in cut.items():
                    self._conn.execute(
                        "INSERT OR REPLACE INTO session_pins "
                        "(session_token, artifact_id, version) VALUES (?, ?, ?)",
                        (session_token, artifact_id.hex, version),
                    )
                # Durable owner-binding (R13/R6/R14): persist the owner + creation
                # tick atomically with the pins so a post-restart read can fall
                # back to the durable owner (foreign caller rejected) and the
                # absolute-age ceiling can bound a survived session. Written even
                # for an empty cut (zero pins) so the owner-binding still survives.
                # Skipped only when the caller did not supply an owner (direct
                # registry-level test captures): those create no durable session.
                if owner is not None:
                    self._conn.execute(
                        "INSERT OR REPLACE INTO session_meta "
                        "(session_token, owner, created_at_tick) VALUES (?, ?, ?)",
                        (session_token, owner.hex, int(created_at_tick or 0)),
                    )
                self._conn.execute("COMMIT")
                return cut
            except BaseException:
                self._conn.execute("ROLLBACK")
                raise

    def release_session(self, session_token: str) -> None:
        """Drop a session's pins AND its durable owner-binding so its pinned
        versions become collectible again (Unit 2 / R4). Idempotent — releasing
        an unknown/already-released token deletes zero rows (no raise), mirroring
        the in-memory ``dict.pop``. Drops ``session_pins`` and ``session_meta`` in
        ONE txn so a reap can never leave a durable owner without its pins (or
        vice versa)."""
        self._guard_writable()
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                self._conn.execute(
                    "DELETE FROM session_pins WHERE session_token = ?",
                    (session_token,),
                )
                self._conn.execute(
                    "DELETE FROM session_meta WHERE session_token = ?",
                    (session_token,),
                )
                self._conn.execute("COMMIT")
            except BaseException:
                self._conn.execute("ROLLBACK")
                raise

    def get_session_meta(self, session_token: str) -> tuple[UUID, int] | None:
        """Return the durable ``(owner, created_at_tick)`` for ``session_token``,
        or ``None`` if there is none (SB-17 / TX-1, R13/R6/R14). Survives a
        coordinator restart (unlike the service-layer in-memory owner-binding), so
        post-restart owner validation can fall back to this: the legitimate owner
        is still served (R6) and a leaked-token foreign caller is still rejected
        (R13). A token that pinned an empty cut still has a meta row (the owner was
        recorded regardless of pin count)."""
        with self._lock:
            row = self._conn.execute(
                "SELECT owner, created_at_tick FROM session_meta "
                "WHERE session_token = ?",
                (session_token,),
            ).fetchone()
        if row is None:
            return None
        return (UUID(hex=row[0]), int(row[1]))

    def all_session_meta(self) -> dict[str, tuple[UUID, int]]:
        """Return ``{session_token: (owner, created_at_tick)}`` for every durable
        session (SB-17 / TX-1, R6/R14). The session-liveness sweep enumerates this
        UNION'd with its in-memory token set so a durable session that survived a
        restart (in-memory state wiped) is still bounded by the absolute-age
        ceiling and its orphaned pins are eventually reaped. Bounded by
        ``max_sessions`` and read on the infrequent sweep only."""
        with self._lock:
            rows = self._conn.execute(
                "SELECT session_token, owner, created_at_tick FROM session_meta"
            ).fetchall()
        return {r[0]: (UUID(hex=r[1]), int(r[2])) for r in rows}

    def session_count(self) -> int:
        """Return the number of LIVE sessions (SB-17 / TX-1, R14). Every session —
        in-memory OR a durable-only restart survivor — has exactly one
        ``session_meta`` row (written at ``begin_session``, dropped at
        reap/release), so this is the authoritative total the ``max_sessions`` cap
        must bound. Counting only the service-layer in-memory map would undercount
        durable survivors post-restart, letting the pinned-cut count transiently
        exceed the GC-starvation bound."""
        with self._lock:
            return int(
                self._conn.execute("SELECT COUNT(*) FROM session_meta").fetchone()[0]
            )

    def get_session_cut(self, session_token: str) -> dict[UUID, int] | None:
        """Return the pinned cut ``{artifact_id: version}`` for ``session_token``,
        or ``None`` if the token has no live pin rows (SB-17 / TX-1, Unit 3 / R2).

        The single accessor ``session_read`` needs to (a) tell a known token from
        an unknown/released one (``None`` ⇒ ``session_not_found``) and (b) read
        the per-artifact pinned version for the serve. Reads the durable
        ``session_pins`` table under one lock, so a coordinator RESTART correctly
        re-serves a non-empty sqlite session (the durable mirror is the whole
        point of R6 restart-survival), where an in-memory session would read
        ``None`` post-restart — the asserted parity divergence.

        Degenerate empty-read-set caveat (documented divergence): an empty cut
        inserts ZERO ``session_pins`` rows, so sqlite cannot durably distinguish
        an empty LIVE session from an unknown token — both read ``None`` here. The
        in-memory mirror keeps an empty ``{}`` entry and returns it. This diverges
        ONLY for a session that pinned nothing, which has no servable
        ``session_read`` either way (no artifact is in its cut), so the
        downstream rejection is benign on both arms (``session_not_found`` on
        sqlite vs ``artifact_not_in_cut`` in-memory). The realistic non-empty
        read-set is identical on both registries."""
        with self._lock:
            rows = self._conn.execute(
                "SELECT artifact_id, version FROM session_pins "
                "WHERE session_token = ?",
                (session_token,),
            ).fetchall()
        if not rows:
            return None
        return {UUID(hex=r[0]): int(r[1]) for r in rows}

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
        self._guard_writable()
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

                # Read-generation fence: on a sweep reclamation (M/E -> INVALID
                # via a reclaim trigger) bump the artifact's ownership epoch,
                # atomically with the state transition in this BEGIN IMMEDIATE,
                # so a commit by the reclaimed holder fails the generation check.
                if prev_in_me and not new_in_me and trigger in RECLAIM_TRIGGERS:
                    self._conn.execute(
                        "UPDATE artifacts SET owner_generation = owner_generation + 1 "
                        "WHERE id = ?",
                        (artifact_id.hex,),
                    )

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

                # Read-generation fence: capture the current ownership epoch into
                # the agent's read_generation when it establishes/refreshes a
                # write-claim (E/M acquire -- P0 fix, incl. acquire-without-read
                # -- or a fetch read), atomic with the grant in this BEGIN
                # IMMEDIATE. The agent_states row exists after the upsert above.
                # INVALID guard on the fetch leg: a future cache-miss-INVALID
                # fetch must not mint a fresh claim for an unfenced zombie.
                if (new_in_me and not prev_in_me) or (
                    trigger in CLAIM_CAPTURE_TRIGGERS and state != MESIState.INVALID
                ):
                    self._conn.execute(
                        "UPDATE agent_states SET read_generation = "
                        "(SELECT owner_generation FROM artifacts WHERE id = ?) "
                        "WHERE artifact_id = ? AND agent_id = ?",
                        (artifact_id.hex, artifact_id.hex, agent_id.hex),
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
        self._guard_writable()
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
        self._guard_writable()
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
        self._guard_writable()
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
        self._guard_writable()
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
        self._guard_writable()
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

    def commit_cas(
        self,
        artifact_id: UUID,
        agent_id: UUID,
        *,
        expected_version: int,
        content_hash: str,
        size_tokens: int | None = None,
        content: bytes | str | None = None,
        tick: int = 0,
        trigger: str = "commit_cas",
    ) -> CasResult:
        """Atomic optimistic-concurrency compare-and-swap commit (plan Unit 2,
        R1/R2/R4/R5/R8). Models :meth:`resolve_or_register`: one
        ``BEGIN IMMEDIATE`` under one lock acquisition does the entire
        version-check → discriminate → conditional-mutate sequence, so two
        cross-process OCC writers cannot both win the same version.

        Three-outcome discrimination (the version check PRECEDES the holder
        check so a just-committed winner's MODIFIED state never mis-fires
        ``other_holder`` for the loser):

        - ``expected_version > current`` → :class:`CasCorruption` (impossible
          from an honest single coordinator; service raises ``CoherenceError``).
          No mutation.
        - ``expected_version < current`` → ``ConflictDetail("version_mismatch")``.
          No mutation. (Two OCC writers — both SHARED — are arbitrated here.)
        - version matches **and** another agent holds M/E (a *pessimistic*
          peer; the OCC writer itself is S/I and does not count) →
          ``ConflictDetail("other_holder")``. No mutation.
        - else (version matches, no other M/E holder) → **WIN**: bump version
          to ``current + 1``, write ``content_hash`` + ``last_writer_id``,
          transition the committer S/I → SHARED, invalidate every non-INVALID
          peer to INVALID. Returns ``(updated_artifact, invalidated_agent_ids)``.

        The committer ends **SHARED, not MODIFIED**: an OCC writer is optimistic
        and never acquired EXCLUSIVE, so it holds no grant — SHARED is the honest
        end-state, and it keeps the same agent's subsequent commit_cas repeatable
        (a sticky MODIFIED would trip the service's D4 "M/E callers use commit()"
        precondition). ``size_tokens=None`` PRESERVES the persisted value (the
        cross-process path always passes None), matching the in-memory registry.

        The committer's bookkeeping and the per-peer invalidation are INLINED as
        raw SQL — :meth:`set_agent_state` opens its own ``BEGIN IMMEDIATE`` and
        cannot be called nested. This reproduces ``set_agent_state``'s contract
        for the participants it touches: SHARED is not in M∪E, so the committer's
        ``granted_at_tick`` is left untouched (preserved; no acquire) and its
        reclaim slot is NOT cleared, while a peer leaving M∪E drops its
        ``granted_at_tick`` slot; plus the mutation-then-log + ``_seq`` rollback
        invariant and the ``state_log`` emit per transition (KTD-13: compare on
        version, never content bytes).

        ``content`` is the winning body. KTD-13 still governs ``get_content``
        (the ``artifacts`` table persists no body; ``get_content`` returns
        ``b""``) and the content-hash remains the staleness source of truth —
        but with retention active the WIN captures ``content`` under
        ``next_version`` in ``artifact_versions``, atomically in this txn.
        ``content=None`` SKIPS the capture (mirror of the in-memory fix: the old
        path would have stored the stale OLD body under the NEW version — a
        latent history-poisoning bug; now an unsupplied body means no snapshot,
        and a later read of ``next_version`` misses). An embedder following the
        KTD-13 cross-process discipline (``content=None`` on every OCC commit)
        therefore gets NO durable capture — retention is inert for that workload
        by design.
        """
        self._guard_writable()
        with self._lock:
            # Track every _seq increment in this call so the outer
            # BaseException handler can roll them ALL back if COMMIT (or any
            # later step) fails — mirrors set_agent_state's COR-02 contract,
            # generalized to the multiple emissions one CAS can produce.
            seq_incremented_count = 0
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                version_row = self._conn.execute(
                    "SELECT version, size_tokens FROM artifacts WHERE id = ?",
                    (artifact_id.hex,),
                ).fetchone()
                if version_row is None:
                    raise KeyError(f"artifact {artifact_id} not in registry")
                current = version_row[0]
                current_size_tokens = version_row[1]

                # 3-outcome discrimination. Each non-win branch COMMITs the
                # (read-only) transaction and returns a typed result; nothing
                # was mutated, so the COMMIT just releases the lock cleanly.
                if expected_version > current:
                    self._conn.execute("COMMIT")
                    return CasCorruption(current_version=current)
                if expected_version < current:
                    self._conn.execute("COMMIT")
                    return ConflictDetail("version_mismatch", current)
                # Version matches. Holder check is the OCC-vs-pessimistic guard;
                # exclude the committer itself (it is S/I, but be defensive).
                other_holder = self._conn.execute(
                    """
                    SELECT 1 FROM agent_states
                    WHERE artifact_id = ? AND agent_id != ? AND state IN (?, ?)
                    LIMIT 1
                    """,
                    (
                        artifact_id.hex,
                        agent_id.hex,
                        MESIState.MODIFIED.name,
                        MESIState.EXCLUSIVE.name,
                    ),
                ).fetchone()
                if other_holder is not None:
                    self._conn.execute("COMMIT")
                    return ConflictDetail("other_holder", current)

                # Read-generation fence: reject a committer whose CAPTURED
                # read-claim was superseded by a sweep reclamation (a reclaimed
                # M/E holder kept its stale read_generation -- version unchanged,
                # no other M/E holder, so version-CAS cannot catch it). An ABSENT
                # read_generation (missing row or NULL) means a plain OCC writer
                # that never established a fence claim -- version-CAS protects it,
                # so admit. Strict->; equality admits. Server-side, no signature
                # change.
                rg_row = self._conn.execute(
                    "SELECT read_generation FROM agent_states "
                    "WHERE artifact_id = ? AND agent_id = ?",
                    (artifact_id.hex, agent_id.hex),
                ).fetchone()
                if rg_row is not None and rg_row[0] is not None:
                    og_row = self._conn.execute(
                        "SELECT owner_generation FROM artifacts WHERE id = ?",
                        (artifact_id.hex,),
                    ).fetchone()
                    if og_row is None:
                        raise KeyError(f"artifact {artifact_id} not in registry")
                    if rg_row[0] < og_row[0]:
                        self._conn.execute("COMMIT")
                        return ConflictDetail("stale_read_generation", current)

                # ---- WIN: mutate atomically ----
                next_version = current + 1
                # Preserve the persisted size_tokens when the caller passes None
                # (the cross-process / coordinator-server path always does) —
                # matches the in-memory registry, where a None arg keeps the
                # prior value rather than NULLing it. Without this, every
                # cross-process OCC commit would silently zero size_tokens.
                resolved_size_tokens = (
                    current_size_tokens if size_tokens is None else size_tokens
                )
                self._conn.execute(
                    """
                    UPDATE artifacts
                    SET version = ?, content_hash = ?, size_tokens = ?,
                        last_writer_id = ?, updated_at = ?
                    WHERE id = ?
                    """,
                    (
                        next_version,
                        content_hash,
                        resolved_size_tokens,
                        agent_id.hex,
                        time.time(),
                        artifact_id.hex,
                    ),
                )

                # Invalidate every non-INVALID peer (inlined; cannot call
                # set_agent_state). Read the peers first so we can both emit a
                # per-peer state_log entry and return the id list for the
                # service's InvalidationSignal construction.
                peer_rows = self._conn.execute(
                    """
                    SELECT agent_id, state, granted_at_tick FROM agent_states
                    WHERE artifact_id = ? AND agent_id != ? AND state != ?
                    """,
                    (artifact_id.hex, agent_id.hex, MESIState.INVALID.name),
                ).fetchall()
                invalidated: list[UUID] = []
                for peer_hex, peer_state_name, peer_granted in peer_rows:
                    peer_from = MESIState[peer_state_name]
                    # A peer leaving M∪E drops its granted_at_tick slot
                    # (set_agent_state's "prev_in_me and not new_in_me" branch).
                    peer_in_me = peer_from in _M_OR_E_STATES
                    new_granted = None if peer_in_me else peer_granted
                    self._conn.execute(
                        """
                        UPDATE agent_states
                        SET state = ?, granted_at_tick = ?
                        WHERE artifact_id = ? AND agent_id = ?
                        """,
                        (
                            MESIState.INVALID.name,
                            new_granted,
                            artifact_id.hex,
                            peer_hex,
                        ),
                    )
                    seq_incremented_count += self._emit_state_log(
                        artifact_id=artifact_id,
                        agent_id=UUID(hex=peer_hex),
                        from_state=peer_from,
                        to_state=MESIState.INVALID,
                        trigger=trigger,
                        tick=tick,
                        version=next_version,
                        content_hash=None,
                    )
                    invalidated.append(UUID(hex=peer_hex))

                # Transition the committer S/I → SHARED. An OCC writer holds NO
                # grant (it is optimistic — it never acquired EXCLUSIVE), so the
                # honest end-state is SHARED, NOT MODIFIED. A sticky MODIFIED
                # grant would make the SAME agent's next commit_cas/write_cas hit
                # the service D4 precondition (which rejects M/E callers) and hard-
                # fail; ending SHARED keeps OCC writes repeatable. SHARED is not
                # in M∪E, so this is NOT an acquire: do NOT set granted_at_tick to
                # ``tick`` and do NOT clear the reclaim slot (mirror
                # set_agent_state's non-M/E branch — preserve prior granted_at,
                # leave reclaim columns untouched). A fresh S/I committer holds no
                # grant slot, so the preserved value is None either way.
                committer_row = self._conn.execute(
                    "SELECT state, granted_at_tick FROM agent_states "
                    "WHERE artifact_id = ? AND agent_id = ?",
                    (artifact_id.hex, agent_id.hex),
                ).fetchone()
                committer_from = (
                    MESIState[committer_row[0]] if committer_row else MESIState.INVALID
                )
                if committer_row is None:
                    self._conn.execute(
                        """
                        INSERT INTO agent_states (artifact_id, agent_id, state, granted_at_tick,
                                                  last_reclaim_trigger, last_reclaim_tick)
                        VALUES (?, ?, ?, NULL, NULL, NULL)
                        """,
                        (artifact_id.hex, agent_id.hex, MESIState.SHARED.name),
                    )
                else:
                    # Preserve granted_at_tick (None for an S/I committer); the
                    # reclaim columns are NOT touched — exactly set_agent_state's
                    # "neither new nor prev in M∪E" path.
                    prior_granted_at = committer_row[1]
                    self._conn.execute(
                        """
                        UPDATE agent_states
                        SET state = ?, granted_at_tick = ?
                        WHERE artifact_id = ? AND agent_id = ?
                        """,
                        (MESIState.SHARED.name, prior_granted_at, artifact_id.hex, agent_id.hex),
                    )
                seq_incremented_count += self._emit_state_log(
                    artifact_id=artifact_id,
                    agent_id=agent_id,
                    from_state=committer_from,
                    to_state=MESIState.SHARED,
                    trigger=trigger,
                    tick=tick,
                    version=next_version,
                    content_hash=content_hash,
                )

                # Durable capture of the WINNING body under next_version, INSIDE
                # this txn (crash-atomic with the version bump). content=None
                # SKIPS capture (the in-memory parity fix): an unsupplied body
                # leaves next_version with no snapshot rather than poisoning it
                # with the stale OLD body. The non-OSError escape window (binding
                # a large bytes/str into the INSERT) is covered by the outer
                # BaseException ROLLBACK below.
                if self._retain_versions and content is not None:
                    self._capture_version_sql(artifact_id, next_version, content)

                self._conn.execute("COMMIT")
                # COMMIT succeeded — _seq durably persisted; suppress rollback.
                seq_incremented_count = 0
            except BaseException:
                # BaseException (not Exception) so KeyboardInterrupt/SystemExit
                # mid-transaction still ROLLBACK before propagating — the same
                # idiom every mutating method here uses.
                self._conn.execute("ROLLBACK")
                # Roll the in-memory _seq back to match the rolled-back DB so
                # the next successful emission does not leave a phantom gap.
                if seq_incremented_count:
                    self._seq -= seq_incremented_count
                raise

            updated = Artifact(
                id=artifact_id,
                name=self._artifact_name(artifact_id),
                version=next_version,
                content_hash=content_hash,
                size_tokens=resolved_size_tokens,
            )
            return updated, invalidated

    def _emit_state_log(
        self,
        *,
        artifact_id: UUID,
        agent_id: UUID,
        from_state: MESIState,
        to_state: MESIState,
        trigger: str,
        tick: int,
        version: int,
        content_hash: str | None,
    ) -> int:
        """Emit one ``state_log`` entry for a transition that already happened
        inside the CALLER's open ``BEGIN IMMEDIATE``. Returns 1 if ``_seq`` was
        bumped (so the caller can roll it back on a later failure), 0 otherwise.

        Reproduces the mutation-then-log + ``_seq`` rollback invariant from
        :meth:`set_agent_state` for the inlined CAS region: ``_seq`` is reserved
        on success, and if the callback raises we decrement and re-raise so the
        caller's ``ROLLBACK`` leaves no phantom gap. Caller holds the lock and
        an open transaction.
        """
        if self._state_log is None:
            return 0
        self._seq += 1
        entry = {
            "tick": tick,
            "artifact_id": str(artifact_id),
            "agent_id": str(agent_id),
            "agent_name": self._agent_names.get(agent_id) if self._agent_names is not None else None,
            "from_state": from_state.name,
            "to_state": to_state.name,
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
            raise
        self._conn.execute(
            "UPDATE registry_meta SET value = ? WHERE key = 'sequence_number'",
            (str(self._seq),),
        )
        return 1

    def _artifact_name(self, artifact_id: UUID) -> str:
        """Read the artifact's name inside the caller's open transaction."""
        row = self._conn.execute(
            "SELECT name FROM artifacts WHERE id = ?", (artifact_id.hex,)
        ).fetchone()
        if row is None:  # pragma: no cover - guarded by caller's earlier SELECT
            raise KeyError(f"artifact {artifact_id} not in registry")
        return row[0]

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
        self._guard_writable()
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
        self._guard_writable()
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
        self._guard_writable()
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
        self._guard_writable()
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

    def last_writer_for(self, artifact_id: UUID) -> Optional[UUID]:
        """COR-09: return the agent UUID that most recently committed to
        ``artifact_id``, or None if the artifact has no committed writer
        (first-observation-only, no successful post-edit yet).

        Reads ``artifacts.last_writer_id`` directly so the answer reflects
        actual commit history rather than current in-memory state. Closes
        the COR-09 gap where the state-map fallback could attribute the
        write to the very session receiving the stale-read warning.
        """
        with self._lock:
            row = self._conn.execute(
                "SELECT last_writer_id FROM artifacts WHERE id = ?",
                (artifact_id.hex,),
            ).fetchone()
        if row is None or row[0] is None:
            return None
        return UUID(hex=row[0])

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


if TYPE_CHECKING:
    # Static conformance assertion (Phase 1, zero runtime change): a type checker
    # rejects this if ``SqliteArtifactRegistry`` ever drifts from the
    # ``SqliteExtended`` contract ``coordinator_server.py`` depends on. Structural
    # (no runtime inheritance); no import cycle (registry_protocol imports only
    # domain types).
    from .registry_protocol import SqliteExtended

    def _conforms(r: SqliteArtifactRegistry) -> SqliteExtended:
        return r
