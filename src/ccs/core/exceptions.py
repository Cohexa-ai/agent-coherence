# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Domain exception hierarchy for coherence protocol operations."""

from __future__ import annotations

# Wire-stable reason string for the retry-eligible OCC precondition where a peer
# invalidated the caller in the window BETWEEN its fresh read and its CAS. Defined
# ONCE here so the coordinator server's response mapping
# (``_handle_post_edit_cas``) and the CoherentVolume client matcher
# (``_classify_cas_response``) reference the SAME literal and cannot drift — a
# reword on one side would otherwise silently break the other's retry routing.
OCC_CALLER_TRANSIENT_REASON = "caller_in_transient_state"

# Read-generation fence (Piece #2): the reason a commit is rejected because the
# committer's read_generation is older than the artifact's current
# owner_generation -- its captured claim was superseded by a sweep reclamation.
# Retry-eligible (reacquire + fresh read + re-commit). Shared by the
# ConflictDetail reason (OCC path) and the StaleReadGeneration exception
# (pessimistic path) so the two surfaces cannot drift.
STALE_READ_GENERATION_REASON = "stale_read_generation"

# ---------------------------------------------------------------------------
# read-at-version rejection vocabulary (plan item N v1, Unit 4 / R5)
# ---------------------------------------------------------------------------
#
# The EXACT string values below are the WIRE CONTRACT: ``read_at_version``
# returns a :class:`~ccs.core.types.VersionedReadRejection` whose ``reason`` is
# ONE of these, and every consumer (the Unit 6 replay resolver, SB-17 later)
# matches with ``reason == CONSTANT`` against :data:`READ_AT_VERSION_REASONS` —
# NEVER a substring of any human message (the typed-signal-not-substring house
# rule, ``docs/solutions/best-practices/typed-signal-not-substring-...``).
# Renaming a value here is a wire break; add, do not mutate.
#
# Six reasons, not seven: ``never_retained`` and ``beyond_horizon`` deliberately
# COLLAPSE into a single ``not_retained``. Once a retained history carries gaps
# (a T-expiry cleanup, or an off->on retention toggle), "never captured" and
# "captured then collected/expired" are UNDECIDABLE from persisted state, so a
# wire-stable constant must not encode a forensic distinction the store cannot
# honestly make (plan Key Decisions: "Rejection vocabulary (6 reasons ...)").
RETENTION_OFF_REASON = "retention_off"
"""Retention was never enabled for this store (``retention_meta()[0]`` False).

Distinct from ``not_retained``: the artifact_versions surface exists on every
v2 sqlite db, so table-presence cannot tell retention-on-unbounded from
retention-never-enabled — the persisted ``retention_enabled`` marker does."""

UNKNOWN_ARTIFACT_REASON = "unknown_artifact"
"""The artifact id is unknown to the registry (``get_artifact`` is None).

Deleted == never-existed: a post-delete read also lands here (delete cascades
the history rows), per the plan's deliberate collapse."""

NOT_RETAINED_REASON = "not_retained"
"""``1 <= version < current`` but no servable retained row exists — it was
never captured (e.g. ``commit_cas(content=None)``), K-evicted, or T-expired.
The single, deliberately-merged reason (see the module note above)."""

EPOCH_MISMATCH_REASON = "epoch_mismatch"
"""An ``expected_epoch`` was supplied and != the registry's
``coordinator_epoch`` — the store was reset (delete-and-recreate) since the
caller captured the epoch, so its retained history is from a different epoch."""

CURRENT_VERSION_REASON = "current_version"
"""``version == current``. The history surface serves HISTORY ONLY; current
content is read via the protocol fetch path (``artifacts`` stores hashes, not
bodies). A read-only consumer cannot obtain current bytes through any surface —
by design (plan Key Decisions: consequence of ``current_version``)."""

FUTURE_VERSION_REASON = "future_version"
"""``version > current``. Preserves the diagnostic ``commit_cas`` keeps via
:class:`~ccs.core.types.CasCorruption`: a requested version ABOVE the current
one suggests a second coordinator writing the same store."""

# The closed set every consumer matches against (``reason in
# READ_AT_VERSION_REASONS``). ``version < 1`` is NOT here — it is a ``ValueError``
# (caller misuse, house style), never a rejection reason.
READ_AT_VERSION_REASONS: frozenset[str] = frozenset(
    {
        RETENTION_OFF_REASON,
        UNKNOWN_ARTIFACT_REASON,
        NOT_RETAINED_REASON,
        EPOCH_MISMATCH_REASON,
        CURRENT_VERSION_REASON,
        FUTURE_VERSION_REASON,
    }
)

# ---------------------------------------------------------------------------
# read-only store-open classification signals (Unit 6 hardening)
# ---------------------------------------------------------------------------
#
# Machine-readable signals carried by ``StoreNeedsRecoveryError.reason``
# (``ccs.coordinator.sqlite_registry``). INTERNAL routing values, not the wire
# contract: the CLI's JSON ``reason`` slugs (``needs_recovery`` / ``db_busy`` /
# ``db_corrupt``) stay owned by the resolver error classes. sqlite renders its
# operational errors as prose, so SOME substring matching is unavoidable — it
# happens in exactly ONE place (``classify_sqlite_operational_signal``), and
# every consumer branches on ``exc.reason == CONSTANT`` from here, never on a
# substring of the human message (the typed-signal-not-substring house rule,
# ``docs/solutions/best-practices/typed-signal-not-substring-...``).
STORE_SIGNAL_WAL_RECOVERY = "wal_recovery"
"""A hot WAL a read-only connection cannot replay (SQLITE_READONLY_RECOVERY).
Remedy: re-open once with the embedder (read-write) to checkpoint the WAL."""

STORE_SIGNAL_BUSY = "busy"
"""The store is locked by a concurrent writer (SQLITE_BUSY); retry shortly."""

STORE_SIGNAL_UNREADABLE = "unreadable"
"""The catch-all: the read-only connection could not read the store for a
reason that is neither a recognized recovery state nor a lock. The operator
remedy matches ``wal_recovery`` (re-open with the embedder), so consumers may
fold this into their needs-recovery surface."""

STORE_OPEN_SIGNALS: frozenset[str] = frozenset(
    {STORE_SIGNAL_WAL_RECOVERY, STORE_SIGNAL_BUSY, STORE_SIGNAL_UNREADABLE}
)

# ---------------------------------------------------------------------------
# cross-runtime store-open guard (sibling Node coordinator hazard)
# ---------------------------------------------------------------------------
#
# The sibling Node coordinator (the agent-coherence-plugin repo) shares the
# SAME ``<workspace>/.coherence/state.db`` path but maintains its OWN migration
# ledger: its v2 adds no schema objects (pending_notices validation) and its v3
# is ``ALTER TABLE agent_states ADD COLUMN deadline_tick`` — so the two ledgers
# assign DIFFERENT meanings to ``PRAGMA user_version`` 2 and 3 on the same
# file. ``CrossRuntimeSchemaError`` (defined in
# ``ccs.coordinator.sqlite_registry`` because it subclasses the
# coordinator-layer ``SchemaVersionError`` to keep existing catch-sites
# compatible; core must not import upward) carries THIS wire-stable reason so
# every consumer — and the Node side's mirror check — matches
# ``exc.reason == CONSTANT``, never a substring of the human message (the
# typed-signal-not-substring house rule,
# ``docs/solutions/best-practices/typed-signal-not-substring-...``).
# Renaming the value is a wire break; add, do not mutate.
CROSS_RUNTIME_SCHEMA_REASON = "cross_runtime_schema"

# ---------------------------------------------------------------------------
# MCP-C deny vocabulary (stale-write-guard-fs, 2026-06-18 plan, Unit 1)
# ---------------------------------------------------------------------------
#
# Each fail-closed coherence terminal carries a typed ``.reason`` CONSTANT on its
# class (below). The MCP deny mapper (``ccs.mcp.deny``) classifies by exception
# TYPE and reads ``.reason`` — NEVER a substring of the message, which stays the
# byte-stable coordinator ``permissionDecisionReason`` prose (the model's retry
# loop depends on that stability, auto-memory
# ``project_cc_strict_mode_retry_hazard``; the typed-signal-not-substring house
# rule). Renaming a value is a wire break; add, do not mutate.
STALE_VIEW_REASON = "stale_view"
COMMIT_PREEMPTED_REASON = "commit_preempted"
VIEW_WEDGED_REASON = "view_wedged"
COMMIT_UNCONFIRMED_REASON = "commit_unconfirmed"
CAS_EXHAUSTED_REASON = "cas_exhausted"
INTERNAL_CONCURRENCY_REASON = "internal_concurrency_error"
# Type B — synthesized by the mapper (no adapter raise-site) when the volume is
# unattached / coordinator transport failed; the write is NOT version-committed.
COORDINATOR_UNAVAILABLE_REASON = "coordinator_unavailable"


class CoherenceError(Exception):
    """Base error for coherence domain failures."""


class StaleReadGeneration(CoherenceError):
    """The read-generation fence rejected a commit: the committer's
    read_generation is older than the artifact's current owner_generation --
    its captured ownership/read-claim was superseded by a sweep reclamation.

    Raised on the pessimistic ``commit()`` path; the OCC
    ``commit_cas`` path returns a :class:`ConflictDetail` with the same reason
    instead. Retry-eligible: ``reacquire()`` + fresh read + re-commit. Carries
    ``STALE_READ_GENERATION_REASON`` so the client classifier matches it
    exactly (never on the human message)."""


class OccCallerTransientError(CoherenceError):
    """Retry-eligible OCC precondition: the caller is mid-transient at CAS time.

    Raised by ``CoordinatorService.commit_cas`` when a peer invalidated the
    caller between its fresh read and its commit-CAS — the registry left an
    invalidation transient that ``commit_cas`` rejects as a precondition. This
    is a LOST RACE, not corruption: a fresh identity (via ``reacquire()``) has
    no transient, so the client may retry.

    A dedicated type so the wire reason (:data:`OCC_CALLER_TRANSIENT_REASON`,
    surfaced by the coordinator server) is decoupled from the human-readable
    message — a reword of the message can no longer break the client's
    substring-free retry classification. The M/E-rejection and
    artifact-not-found branches of ``commit_cas`` stay plain
    :class:`CoherenceError`; only the transient precondition is retry-eligible.
    """


class CoherenceDegradedWarning(UserWarning):
    """Emitted once per adapter instance when a coherence error degrades to fallback.

    Canonical home so every adapter (CCSStore, OpenAIAgentsAdapter, ...) emits and
    catches the *same* class — ``from ccs.adapters import CoherenceDegradedWarning``
    must match whatever any adapter raises.
    """


class CoherenceTopologyWarning(UserWarning):
    """Emitted when an adapter is used in a topology its coherence model can't fully cover.

    Example: an OpenAI Agents run that combines a server-side ``conversation_id`` with
    multi-agent handoffs, where the SDK disables ``input_filter`` / nested handoff
    history — so handoff-history coherence is unavailable. Surfaced once, never silent.
    """


class InvalidTransitionError(CoherenceError):
    """Raised when the MESI transition table rejects a state transition."""

    def __init__(self, from_state: str, to_state: str, trigger: str):
        super().__init__(f"invalid_transition from={from_state} to={to_state} trigger={trigger}")
        self.from_state = from_state
        self.to_state = to_state
        self.trigger = trigger


class InvariantViolationError(CoherenceError):
    """Raised when a runtime invariant check fails."""


class CasRetriesExhausted(CoherenceError):
    """Raised when an optimistic-concurrency commit-CAS retry loop is exhausted.

    The typed terminal failure for the OCC write path (plan Unit 5, R6 /
    R-OCC-6). When ``AgentRuntime.write_cas`` has retried ``commit_cas`` the
    strategy-allowed number of times and every attempt lost the race
    (``ConflictDetail``), the loop surfaces THIS rather than silently dropping
    the write. A ``CasRetriesExhausted`` therefore means *no mutation landed for
    this caller* — the cache is left at the latest observed (refreshed) version,
    never corrupted with an unconfirmed write.

    A subclass of :class:`CoherenceError` so the deny-always-raises consumers
    (CoherentVolume / CCSStore strict mode) already treat it as a hard failure.

    Carries :data:`CAS_EXHAUSTED_REASON` (MCP-C Unit 1) so the deny mapper
    classifies it by type; the wire reason ``cas_exhausted`` is deliberately
    distinct from the ``cas_retries_exhausted`` prose token in the message.
    """

    reason = CAS_EXHAUSTED_REASON

    def __init__(self, artifact_id: object, attempts: int, last_current_version: int) -> None:
        super().__init__(
            f"cas_retries_exhausted artifact={artifact_id} attempts={attempts} "
            f"last_current_version={last_current_version} "
            f"(no write landed — every commit_cas attempt lost the race)"
        )
        self.artifact_id = artifact_id
        self.attempts = attempts
        self.last_current_version = last_current_version


class StaleView(CoherenceError):
    """Pre-edit deny (MCP-C Unit 1): this instance's view is INVALID — a peer
    committed a newer version, so the coordinator denied the write BEFORE any
    disk mutation. Recoverable: ``reacquire()`` for fresh bytes, then write FROM
    them. Carries :data:`STALE_VIEW_REASON`; the message stays the verbatim
    coordinator ``permissionDecisionReason`` (matched by type, not substring)."""

    reason = STALE_VIEW_REASON


class CommitPreempted(CoherenceError):
    """Post-edit deny (MCP-C Unit 1): the EXCLUSIVE grant was preempted /
    sweep-reclaimed AFTER the atomic disk write but BEFORE the commit landed, so
    the bytes may already be on disk *un-versioned* (disk ahead of the
    coordinator version). NOT only a concurrent edge — a lone sequential writer's
    grant can age out mid-write (crash-recovery sweep, default-on). Recover by
    re-reading fresh bytes and reconciling the pending buffer (agent-driven; v1
    has no server reconcile primitive). Carries :data:`COMMIT_PREEMPTED_REASON`."""

    reason = COMMIT_PREEMPTED_REASON


class ViewWedged(CoherenceError):
    """OCC comparand wedged (MCP-C Unit 1): a ``write_cas`` comparand read stayed
    strict-denied across the bounded reacquire streak — the view never cleared to
    a usable state. Not retry-eligible in-loop: wait or escalate. Carries
    :data:`VIEW_WEDGED_REASON`."""

    reason = VIEW_WEDGED_REASON


class CommitUnconfirmed(CoherenceError):
    """OCC commit unconfirmed (MCP-C Unit 1): the coordinator transport failed
    mid-commit, a false-negative ack — NOT a confirmed loss. The write may or may
    not have landed; reconcile by re-reading, then retry only if absent. Carries
    :data:`COMMIT_UNCONFIRMED_REASON`."""

    reason = COMMIT_UNCONFIRMED_REASON


class InternalConcurrencyError(CoherenceError):
    """The single-op guard fired (MCP-C Unit 1): one CoherentVolume instance was
    used concurrently from another thread — a SERVER misuse bug (the MCP server
    serializes tool access), never an agent-recoverable deny. Mapped to
    :data:`INTERNAL_CONCURRENCY_REASON`, distinct from ``stale_view`` so it is
    never relayed to the model as a retryable view."""

    reason = INTERNAL_CONCURRENCY_REASON


class ScenarioValidationError(CoherenceError):
    """Raised when scenario configuration does not match schema expectations."""

    def __init__(self, path: str, message: str):
        super().__init__(f"scenario={path}: {message}")
        self.path = path


class WatchdogAbandoned(RuntimeError):
    """A handler's 4s watchdog fired, so its still-running work was told to abort
    before it could mutate the registry (finding A6).

    Raised by the registries' ``abort_guard`` when the per-request abort
    :class:`threading.Event` is already set at the moment the mutation wins the
    registry write lock — i.e. the handler timed out (and the client already got
    ``degraded: true``) while this work was blocked on that lock. Aborting there
    is what stops the late "phantom grant" / grant-revocation from landing.

    Deliberately NOT a :class:`CoherenceError`: it never reaches a client. The
    only caller that ever sets the abort Event is the handler watchdog, which
    has already responded; this exception surfaces solely inside the abandoned
    pool future, where ``_on_watchdog_future_done_after_timeout`` treats it as a
    clean no-op (no phantom state landed). Every non-watchdog caller
    (CoherentVolume, CCSStore, the CLI) passes ``abort=None`` and never sees it.
    """
