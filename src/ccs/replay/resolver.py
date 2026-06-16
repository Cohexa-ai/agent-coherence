# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Read-at-version resolver — the wired ``read_at_version`` consumer (Unit 6 / R5b).

This module is the bridge between the replay CLI and the durable retention
surface Units 3-4 shipped: given a ``.coherence/state.db`` path, an artifact
selector (workspace path OR raw UUID), and a version, it opens the store
**read-only** (never migrating, never materializing a fresh db), constructs a
:class:`~ccs.coordinator.service.CoordinatorService` over it, and calls
``read_at_version`` — the SAME surface SB-17 will consume later. It is the
restart-survival proof: a process can exit and a separate resolver process can
recover the exact bytes a prior writer committed at version k.

Layer note: ``replay`` is the ``interface`` layer; ``coordinator`` is
``application``. interface→application is a LEGAL downward import
(``src/ccs/hardening/architecture.py``). The coordinator must NOT import
replay — this dependency only goes one way.

Typed outcomes (no raw stack traces ever reach a script):

  ``resolve_version`` returns a :class:`ResolverResult` — one of:

  - :class:`~ccs.core.types.VersionedContent` (the WIN, retained body + metadata)
  - :class:`~ccs.core.types.VersionedReadRejection` (a service-level rejection
    carrying one of the six wire-stable reasons in
    :data:`ccs.core.exceptions.READ_AT_VERSION_REASONS`)

  …or RAISES a :class:`ResolverError` for an open/lookup failure that has no
  servable value:

  - :class:`ResolverMissingDatabaseError` — path does not exist (and the
    registry is NEVER constructed — see the pre-existence check below)
  - :class:`ResolverSchemaVersionError` — wrong ``user_version`` (a v1 db; the
    resolver performs NO migration)
  - :class:`ResolverNeedsRecoveryError` — post-crash hot WAL a read-only
    connection cannot replay
  - :class:`ResolverCorruptDatabaseError` — the file is not a SQLite database
  - :class:`ResolverBusyError` — the store is locked (SQLITE_BUSY)
  - :class:`ResolverUnknownArtifactPathError` — a workspace-path selector that
    matches no ``artifacts.name`` row (NOT a raw lookup stack trace)
  - :class:`ResolverInstanceMismatchError` — an ``--instance-id`` cross-check
    that disagrees with the store's persisted ``instance_id``

Import discipline (the optional-extra learning,
``docs/solutions/test-failures/pytest-collect-error-missing-optional-extra``):
the coordinator import lives INSIDE :func:`resolve_version`, not at module top —
nothing the replay CLI transitively loads eagerly imports anything beyond the
``[dev]`` surface. (``coordinator`` is not itself an optional extra, but keeping
the import surface lazy honors the discipline and keeps ``import
ccs.cli.coherence_replay`` cheap.)
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, TypeAlias
from uuid import UUID

from ccs.core.exceptions import STORE_SIGNAL_BUSY
from ccs.core.types import VersionedContent, VersionedReadRejection
from ccs.replay.errors import ReplayConfigurationError, ReplayTraceError

if TYPE_CHECKING:  # pragma: no cover — typing only; the runtime import is lazy
    from ccs.coordinator.sqlite_registry import SqliteArtifactRegistry

# The union ``resolve_version`` RETURNS (a typed result, never an exception).
# Errors that have no value to return are RAISED as ResolverError subclasses.
ResolverResult: TypeAlias = VersionedContent | VersionedReadRejection


# ---------------------------------------------------------------------------
# Resolver error taxonomy
# ---------------------------------------------------------------------------
#
# These are RAISED (not returned) because an open/lookup that cannot proceed has
# no servable value — mirroring the read-only registry's own exception choice
# (``ReadOnlyRegistryError`` family). Each maps to a DISTINCT CLI exit code so a
# script can branch on the failure class without parsing prose; the CLI renders
# the exact ``reason`` slug from ``.reason`` below, never a substring match.
#
# All inherit ``ReplayTraceError`` (the "fix the data / the store" category)
# EXCEPT ``ResolverInstanceMismatchError`` and the missing-path case, which are
# closer to caller misconfiguration. We keep the hierarchy aligned with the
# existing replay split so a caller can still ``except ReplayError`` to catch
# everything, but the CLI matches the concrete classes for its exit-code map.


class ResolverError(ReplayTraceError):
    """Base for read-at-version resolver open/lookup failures (Unit 6 / R5b).

    A subclass of :class:`~ccs.replay.errors.ReplayTraceError` so a caller that
    already does ``except ReplayError`` (or ``ReplayTraceError``) keeps catching
    resolver failures, but the CLI matches the concrete subclasses below to map
    each to its own exit code. Every subclass carries a stable ``reason`` slug
    (class attribute) the JSON envelope emits so scripts never parse the human
    message.
    """

    #: Stable, prose-free slug the CLI emits in the JSON envelope's ``reason``.
    reason: str = "resolver_error"


class ResolverMissingDatabaseError(ResolverError):
    """The ``--db`` path does not exist; the resolver constructed NO registry.

    Belt-and-suspenders against materializing a fresh empty
    ``.coherence/state.db``: :func:`resolve_version` checks ``path.exists()``
    BEFORE touching ``SqliteArtifactRegistry`` at all, so this is raised without
    any side effect (read-only mode would also refuse to create, but the
    pre-check guarantees zero filesystem touch and a clean message)."""

    reason = "db_missing"


class ResolverSchemaVersionError(ResolverError):
    """The store's ``user_version`` is not the version this build serves.

    A v1 (un-migrated) db lands here. Read-only mode performs NO migration —
    the remedy is to re-open the store once with the embedder (read-write).
    Distinct slug so the CLI never confuses "needs an upgrade" with "corrupt"."""

    reason = "schema_version_mismatch"


class ResolverNeedsRecoveryError(ResolverError):
    """The store has an unclean hot WAL a read-only connection cannot replay.

    After an embedder crash, SQLite forbids a ``mode=ro`` connection from
    running WAL recovery (SQLITE_READONLY_RECOVERY). The forensic population
    over-represents crashed writers, so this is the resolver's most on-brand
    failure: the remedy is to re-open once with the embedder (read-write) so it
    checkpoints the WAL, then retry the resolve."""

    reason = "needs_recovery"


class ResolverCorruptDatabaseError(ResolverError):
    """The ``--db`` path exists but is not a readable SQLite database.

    A non-sqlite file (truncated, wrong format, or random bytes) surfaces
    ``sqlite3.DatabaseError`` on first access; the resolver maps it to this
    distinct typed error so the CLI renders a clean ``db_corrupt`` reason rather
    than leaking a raw ``DatabaseError`` to a script."""

    reason = "db_corrupt"


class ResolverBusyError(ResolverError):
    """The store is locked by a concurrent writer (SQLITE_BUSY).

    A read-only resolve that times out waiting for a writer's lock surfaces
    here. Distinct from needs-recovery (a hot WAL) and corruption (bad bytes):
    the store is fine, just busy — retry shortly."""

    reason = "db_busy"


class ResolverUnknownArtifactPathError(ResolverError):
    """A workspace-path selector matched no ``artifacts.name`` row.

    ``artifacts.name`` is UNIQUE (the parent-repo-relative path), so a path
    selector resolves to at most one id; a miss is this typed error NAMING the
    failed path lookup — never a raw stack trace. (A raw-UUID selector that is
    simply unknown to the registry is NOT this error: it flows through to
    ``read_at_version`` and returns the ``unknown_artifact`` rejection, so a
    by-id miss and a by-path miss stay distinguishable.)"""

    reason = "unknown_artifact_path"


class ResolverInstanceMismatchError(ReplayConfigurationError):
    """The ``--instance-id`` cross-check disagreed with the store's identity.

    An identity guard available today (epoch-at-capture is the deferred recorder
    task): if the caller passes the ``instance_id`` from a trace manifest, the
    resolver verifies it equals the store's persisted ``instance_id`` before
    serving any bytes — catching a resolve pointed at the WRONG store. This is
    caller/config misuse (a :class:`~ccs.replay.errors.ReplayConfigurationError`,
    not a store/trace defect), so it sits in that arm of the hierarchy."""

    reason = "instance_id_mismatch"


@dataclass(frozen=True)
class ResolverRequest:
    """Inputs for one read-at-version resolve (Unit 6).

    ``selector`` is either a raw UUID (hex, with or without dashes) or a
    workspace path (``artifacts.name``); :func:`resolve_version` discriminates by
    attempting a UUID parse first. ``expected_epoch`` is a MANUAL passthrough
    (the recorder does not yet derive it — that is the deferred epoch-provenance
    task); ``expected_instance_id`` is the optional identity cross-check.
    """

    db_path: Path
    selector: str
    version: int
    expected_epoch: str | None = None
    expected_instance_id: str | None = None


def _coerce_selector_to_uuid(selector: str) -> UUID | None:
    """Return a UUID if ``selector`` parses as one, else ``None`` (a path).

    Tries the strict ``UUID(...)`` parse (accepts dashed and bare 32-hex forms).
    A non-UUID string is treated as a workspace path for the ``artifacts.name``
    lookup — there is no ambiguity in practice because workspace paths contain
    ``/`` or ``.`` and never parse as a UUID.
    """
    try:
        return UUID(selector)
    except (ValueError, AttributeError):
        return None


def resolve_version(request: ResolverRequest) -> ResolverResult:
    """Resolve "bytes at version k" against a read-only coordinator store (R5b).

    Steps (each failure is a typed error or typed rejection — never a raw trace):

    1. **Pre-existence check.** If ``request.db_path`` does not exist, raise
       :class:`ResolverMissingDatabaseError` and NEVER construct the registry
       (no fresh ``.coherence/state.db`` is ever materialized).
    2. **Read-only open.** Construct ``SqliteArtifactRegistry(path,
       read_only=True)`` — the Unit 3 mode that never creates/migrates/ALTERs and
       rejects mutators. Map its typed open failures
       (``MissingDatabaseError`` / ``SchemaVersionError`` /
       ``StoreNeedsRecoveryError``) plus raw ``sqlite3.DatabaseError`` (non-sqlite
       file) and SQLITE_BUSY to the resolver error taxonomy.
    3. **Instance cross-check** (optional). If ``expected_instance_id`` is given
       and != the store's persisted ``instance_id``, raise
       :class:`ResolverInstanceMismatchError`.
    4. **Selector → id.** A raw-UUID selector is used directly (an unknown id
       flows to ``read_at_version`` → ``unknown_artifact`` rejection). A path
       selector is looked up via ``artifacts.name`` (UNIQUE); a miss raises
       :class:`ResolverUnknownArtifactPathError`.
    5. **read_at_version.** Construct ``CoordinatorService`` over the read-only
       registry and return its ``VersionedContent | VersionedReadRejection``
       (``version < 1`` raises ``ValueError`` from the service — caller misuse,
       surfaced by the CLI as a usage error, not a resolver reason).

    The coordinator/sqlite imports are LAZY (inside this function) per the
    no-eager-optional-import discipline.
    """
    # (1) Pre-existence check — BEFORE any registry construction so a wrong path
    # never materializes a fresh empty store (zero filesystem side effect).
    if not request.db_path.exists():
        raise ResolverMissingDatabaseError(
            f"read-at-version resolve: no coordinator store at "
            f"{request.db_path}. The resolver opens the store read-only and "
            f"never creates it — point --db at an existing .coherence/state.db."
        )

    # Lazy imports (optional-extra discipline): keep ``import
    # ccs.cli.coherence_replay`` from pulling the coordinator + sqlite surface
    # at module-load time.
    import sqlite3

    from ccs.coordinator.service import CoordinatorService
    from ccs.coordinator.sqlite_registry import (
        MissingDatabaseError,
        SchemaVersionError,
        SqliteArtifactRegistry,
        StoreNeedsRecoveryError,
    )

    # (2) Read-only open with typed-failure mapping.
    try:
        registry = SqliteArtifactRegistry(request.db_path, read_only=True)
    except MissingDatabaseError as exc:
        # Defense in depth: the pre-check above already covers a missing path,
        # but a TOCTOU delete between the check and the open lands here too.
        raise ResolverMissingDatabaseError(str(exc)) from exc
    except SchemaVersionError as exc:
        raise ResolverSchemaVersionError(str(exc)) from exc
    except StoreNeedsRecoveryError as exc:
        # Branch on the typed signal the registry classified at ITS single
        # classification point — never on str(exc) content.
        raise _resolver_error_for_store_signal(exc.reason, exc) from exc
    except sqlite3.DatabaseError as exc:
        # A non-sqlite / corrupt file surfaces sqlite3.DatabaseError on the first
        # access inside the open. ``OperationalError`` is a DatabaseError
        # subclass; the read-only open already maps the recovery/locked
        # OperationalError cases to StoreNeedsRecoveryError, so reaching here
        # means a genuine corrupt-format signal.
        raise ResolverCorruptDatabaseError(
            f"read-at-version resolve: the file at {request.db_path} is not a "
            f"readable SQLite database (it may be truncated, the wrong format, "
            f"or not a coordinator store). Underlying: {exc}"
        ) from exc

    try:
        # (3) Optional instance-id identity guard.
        if request.expected_instance_id is not None:
            actual = registry.instance_id
            if actual != request.expected_instance_id:
                raise ResolverInstanceMismatchError(
                    f"read-at-version resolve: --instance-id "
                    f"{request.expected_instance_id!r} does not match the store's "
                    f"persisted instance_id {actual!r} at {request.db_path}. The "
                    f"resolve is pointed at a different store than the trace "
                    f"manifest describes; check --db."
                )

        # (4) Selector → artifact id.
        artifact_id = _resolve_artifact_id(registry, request.selector)

        # (5) read_at_version via the service over the read-only registry. A
        # SQLITE_BUSY/locked store can surface OperationalError at QUERY time
        # (after a clean open) — distinct from the open-time mapping above — so
        # we map it to the resolver taxonomy here too rather than leaking a raw
        # OperationalError. Classification routes through the registry's ONE
        # classification seam (typed-signal house rule). A genuine query-time
        # corruption signal (DatabaseError that is not Operational) maps to the
        # corrupt bucket.
        service = CoordinatorService(registry)
        try:
            return service.read_at_version(
                artifact_id, request.version, expected_epoch=request.expected_epoch
            )
        except sqlite3.OperationalError as exc:
            from ccs.coordinator.sqlite_registry import (
                classify_sqlite_operational_signal,
            )

            raise _resolver_error_for_store_signal(
                classify_sqlite_operational_signal(exc), exc
            ) from exc
        except sqlite3.DatabaseError as exc:
            raise ResolverCorruptDatabaseError(
                f"read-at-version resolve: a query against {request.db_path} "
                f"failed — the store may be corrupt. Underlying: {exc}"
            ) from exc
    finally:
        registry.close()


def _resolve_artifact_id(registry: "SqliteArtifactRegistry", selector: str) -> UUID:
    """Map a selector (raw UUID or ``artifacts.name`` path) to an artifact id.

    A UUID selector is returned as-is (an id unknown to the registry flows to
    ``read_at_version`` and becomes ``unknown_artifact`` — keeping a by-id miss
    distinct from a by-path miss). A path selector is looked up via the UNIQUE
    ``artifacts.name`` column; a miss raises
    :class:`ResolverUnknownArtifactPathError` (the named lookup, not a trace).
    """
    as_uuid = _coerce_selector_to_uuid(selector)
    if as_uuid is not None:
        return as_uuid
    looked_up = registry.lookup_artifact_id_by_name(selector)
    if looked_up is None:
        raise ResolverUnknownArtifactPathError(
            f"read-at-version resolve: no artifact registered at workspace path "
            f"{selector!r} (artifacts.name lookup miss). Pass a path that the "
            f"coordinator has observed, or a raw artifact UUID."
        )
    return looked_up


def _resolver_error_for_store_signal(signal: str, exc: Exception) -> ResolverError:
    """Map a typed store-open signal (``StoreNeedsRecoveryError.reason`` /
    ``classify_sqlite_operational_signal``) to the resolver error taxonomy.

    No message parsing happens here — the signal was classified ONCE at the
    registry seam and is matched with ``==`` against the constants in
    ``ccs.core.exceptions`` (the typed-signal house rule). ``busy`` keeps its
    own class (retry shortly); ``wal_recovery`` and the ``unreadable``
    catch-all share :class:`ResolverNeedsRecoveryError` because they share the
    operator remedy (re-open once with the embedder) — preserving the
    wire-visible JSON reasons / exit codes exactly.
    """
    if signal == STORE_SIGNAL_BUSY:
        return ResolverBusyError(
            f"read-at-version resolve: the store is locked by a concurrent "
            f"writer (SQLITE_BUSY) and the read-only resolve timed out waiting "
            f"for the lock. Retry shortly. Underlying: {exc}"
        )
    return ResolverNeedsRecoveryError(str(exc))
