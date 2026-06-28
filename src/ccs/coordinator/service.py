# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Coordinator service implementing core artifact coherence operations."""

from __future__ import annotations

import hmac
import logging
import secrets
import string
import threading
import time
import warnings
from collections import OrderedDict
from dataclasses import dataclass
from typing import Callable, Iterable, Optional
from uuid import NAMESPACE_URL, UUID, uuid5

from ccs.coordinator.retention import collectible_versions
from ccs.core.exceptions import (
    CURRENT_VERSION_REASON,
    EPOCH_MISMATCH_REASON,
    FUTURE_VERSION_REASON,
    NOT_RETAINED_REASON,
    OCC_CALLER_TRANSIENT_REASON,
    RETENTION_OFF_REASON,
    SESSION_ARTIFACT_NOT_IN_CUT_REASON,
    SESSION_INVALIDATED_REASON,
    SESSION_NOT_FOUND_REASON,
    UNKNOWN_ARTIFACT_REASON,
    CoherenceError,
    OccCallerTransientError,
    StaleReadGeneration,
)
from ccs.core.hashing import compute_content_hash
from ccs.core.invariants import check_monotonic_version, check_single_writer
from ccs.core.states import MESIState, TransientState
from ccs.core.types import (
    Artifact,
    CasCorruption,
    ConflictDetail,
    DataPlaneDeferredRead,
    FetchRequest,
    FetchResponse,
    InvalidationSignal,
    SessionCommitRejection,
    SessionReadRejection,
    SnapshotSession,
    VersionedContent,
    VersionedReadRejection,
)

from .registry import ArtifactRegistry

logger = logging.getLogger(__name__)


# v0.9.0 transitional first-use warning — see
# docs/plans/2026-05-28-001-feat-c-flip-crash-recovery-default-on-plan.md.
# The v0.8.3 deprecation cycle (falsy sentinel + DeprecationWarning) is gone
# now that the default has flipped to ``enabled=True``. A single transitional
# ``RuntimeWarning`` fires once per process on the first ``CrashRecoveryConfig``
# construction so jump-upgraders (v0.8.2 -> v0.9.0) who skipped the v0.8.3
# cycle still get a migration heads-up. Removed entirely in v0.10.0.
#
# Test-isolation contract: ``_V090_FIRST_USE_WARNED`` is module-level mutable
# state that persists across instances and pytest test functions in the same
# process. Tests that assert on the warning MUST reset it to ``False`` before
# construction (via ``service._V090_FIRST_USE_WARNED = False`` or the
# ``reset_v090_first_use_flag`` fixture in ``tests/test_coordinator.py``).
#
# ``_V090_FIRST_USE_LOCK`` makes the check-then-set atomic so two threads
# constructing configs concurrently cannot both emit. The GIL made the naive
# check incidentally safe; free-threaded Python 3.13+ removes it (mirrors the
# v0.8.3 ADV-02 lock). The warning is emitted OUTSIDE the lock because warning
# filters can run arbitrary user code.
_V090_FIRST_USE_WARNED: bool = False
_V090_FIRST_USE_LOCK = threading.Lock()

_V090_FIRST_USE_MESSAGE = (
    "CrashRecoveryConfig default changed in v0.9.0: enabled=True is now the "
    "default (was False in v0.8.x), so crash recovery runs by default. Pass "
    "CrashRecoveryConfig(enabled=True) to keep the new behavior or "
    "CrashRecoveryConfig(enabled=False) to opt out. This notice fires once per "
    "process on the first construction regardless of the value passed; to "
    "suppress it, filter RuntimeWarning from the 'ccs.coordinator.service' "
    "logger. See CHANGELOG.md (section: [0.9.0]) at "
    "https://github.com/hipvlady/agent-coherence/blob/main/CHANGELOG.md "
    "for migration details."
)


@dataclass(frozen=True)
class CrashRecoveryConfig:
    """Configuration knobs for the stable-grant reclamation sweep.

    The sweep ships **enabled by default** as of v0.9.0 (R10 — the default
    flipped from ``False`` to ``True``, so bare ``CrashRecoveryConfig()`` now
    activates crash recovery). A one-shot transitional ``RuntimeWarning`` fires
    on the first construction per process to flag the change for jump-upgraders
    who skipped the v0.8.3 deprecation cycle. Pass
    ``CrashRecoveryConfig(enabled=False)`` to restore v0.8.x behavior.

    Attributes:
        enabled: Master flag. When ``True`` (v0.9.0 default), the sweep
            reclaims stale M∪E grants. Pass ``enabled=False`` to opt out.
        heartbeat_timeout_ticks: Sweep reclaims any M∪E grant whose holder
            has not heartbeated within this many ticks.
        max_hold_ticks: Sweep reclaims any M∪E grant held for at least this
            many ticks regardless of heartbeat. Must be ``>`` the longest
            inspectable strategy lease TTL when ``enabled=True`` (R11).
    """

    enabled: bool = True
    heartbeat_timeout_ticks: int = 120
    max_hold_ticks: int = 900

    def __post_init__(self) -> None:
        """Emit the one-shot v0.9.0 transitional first-use warning.

        With the v0.8.3 sentinel removed, bare ``CrashRecoveryConfig()`` and
        explicit ``CrashRecoveryConfig(enabled=True)`` are indistinguishable,
        so the warning fires once per process on the FIRST construction
        regardless of how ``enabled`` was supplied — catching jump-upgraders
        (v0.8.2 -> v0.9.0) who skipped the v0.8.3 cycle. Removed in v0.10.0.
        """
        global _V090_FIRST_USE_WARNED
        # Claim the one-shot emission atomically under the lock, then emit
        # OUTSIDE it: warning filters can run arbitrary user code, and we never
        # hold the lock across that (mirrors the v0.8.3 ADV-02 discipline).
        should_emit = False
        with _V090_FIRST_USE_LOCK:
            if not _V090_FIRST_USE_WARNED:
                _V090_FIRST_USE_WARNED = True
                should_emit = True
        if should_emit:
            # stacklevel=3: warn() -> __post_init__ -> __init__ -> caller.
            warnings.warn(_V090_FIRST_USE_MESSAGE, RuntimeWarning, stacklevel=3)


def validate_crash_recovery_config(
    crash_recovery: CrashRecoveryConfig,
    strategy: object,
) -> None:
    """Fail-fast composition check (R11).

    When the sweep is enabled, ``max_hold_ticks`` must exceed the strategy's
    inspectable lease TTL strictly. Equal is rejected because a sweep at the
    TTL boundary races the strategy's own refresh logic.

    Strategies without an introspectable ``ttl_ticks`` attribute (lazy,
    eager, access-count, broadcast) cannot be statically validated against
    the rule; we emit a ``RuntimeWarning`` so a custom strategy with a
    non-inspectable TTL is at least surfaced, but do not refuse startup.
    """
    if not crash_recovery.enabled:
        return

    ttl = getattr(strategy, "ttl_ticks", None)

    # Built-in non-lease strategies (lazy, eager, access_count, broadcast) and
    # any custom strategy without a ttl_ticks attribute cannot be statically
    # validated against R11. Silent-accept matches the spec's design choice
    # for the common case.
    if ttl is None:
        return

    # Integer ttl (including 0 and negatives): apply the rule. Review fix
    # ADV-02 / ADV-03: previously this branch required ttl > 0, which silently
    # dropped ttl=0 into the "non-integer" warning path with misleading text.
    # Now any int ttl is checked, and the warn-on-non-integer branch below
    # only fires for genuinely non-integer attributes (string, float, etc.).
    if isinstance(ttl, int):
        if crash_recovery.max_hold_ticks <= ttl:
            raise ValueError(
                f"crash_recovery composition violation: "
                f"max_hold_ticks={crash_recovery.max_hold_ticks} must be > "
                f"strategy.ttl_ticks={ttl} "
                f"(strategy={type(strategy).__name__}); "
                f"sweep at lease TTL boundary races strategy refresh."
            )
        return

    warnings.warn(
        f"crash_recovery: strategy {type(strategy).__name__} exposes a "
        f"non-integer ttl_ticks={ttl!r}; composition rule (R11) cannot be "
        f"statically verified.",
        RuntimeWarning,
        stacklevel=3,
    )


# Snapshot session token entropy (SB-17 / TX-1, Unit 2 / R13). 32 bytes →
# ~43 url-safe chars; unguessable, server-minted, never client-supplied. The
# timing-safe compare + per-call validation that consume it are Unit 7.
_SESSION_TOKEN_BYTES = 32


# Server-minted session-token SHAPE (SB-17 / TX-1, Unit 5 / R4). A
# ``secrets.token_urlsafe(32)`` is ALWAYS exactly 43 characters drawn from the
# URL-safe base64 alphabet ([A-Za-z0-9_-], no padding). The session-liveness
# fail-closed taxonomy uses this as the structural discriminator: a token of
# this shape that has NO live cut was, or still looks like, a real session
# (reaped / GC-raced / restart-wiped) → ``session_invalidated`` (fail closed,
# never live HEAD). A token NOT of this shape is genuinely never-opened /
# malformed → ``session_not_found``. The check is structural only (it cannot
# prove a token was ever minted — fail-closed is the safe default for an
# in-shape token), and it is NOT an authentication boundary (the unguessable
# entropy + the Unit-7 owner-binding are). It exists ONLY to make the dead-vs-
# never-opened reason split honest and survive an in-memory restart that wipes
# every service-layer map including the tombstone.
_SESSION_TOKEN_LEN = 43
_SESSION_TOKEN_ALPHABET = frozenset(string.ascii_letters + string.digits + "-_")

# Bounded reaped-session tombstone cap (SB-17 / TX-1, Unit 5 / R4). The
# session-liveness sweep records a reaped token here so a subsequent
# ``session_read`` / ``session_commit`` attributes it precisely to
# ``session_invalidated`` ("your session died") rather than relying on the shape
# predicate alone. Capped + oldest-evicted so a long-lived coordinator cannot
# accumulate unbounded tombstones (a reaper-amplification DoS). Eviction is
# benign: an evicted token still classifies ``session_invalidated`` via the
# shape predicate (every server-minted token is in-shape), so dropping the
# tombstone entry only loses the precise "definitely reaped" attribution, never
# the fail-closed guarantee.
_REAPED_TOMBSTONE_CAP = 4096


def looks_like_session_token(token: str) -> bool:
    """Structural predicate: does ``token`` have the server-minted SHAPE?

    ``True`` iff it is exactly :data:`_SESSION_TOKEN_LEN` characters, all in the
    URL-safe base64 alphabet — the shape every ``begin_session`` token has. Used
    ONLY by the Unit-5 fail-closed taxonomy to split a dead/restart-wiped session
    (in-shape, no cut → ``session_invalidated``) from a genuinely-never-opened or
    malformed token (out-of-shape → ``session_not_found``). NOT an auth check; a
    well-formed token that was never minted still classifies invalidated, which
    is the safe (fail-closed) default — it is never served live HEAD either way.
    """
    return (
        len(token) == _SESSION_TOKEN_LEN
        and all(ch in _SESSION_TOKEN_ALPHABET for ch in token)
    )


class CoordinatorService:
    """Control-plane service for artifact read/write/commit synchronization."""

    def __init__(self, registry: ArtifactRegistry):
        self.registry = registry
        # Snapshot session owner-binding (SB-17 / TX-1, Unit 2 / R13):
        # ``{session_token: owner_id}``. Populated at ``begin_session`` mint;
        # the per-CALL read/commit validation that READS it (a foreign owner →
        # SessionInvalidated, timing-safe compare) is Unit 7. Unit 5 owner-binds
        # ONLY the HEARTBEAT path (``record_session_heartbeat``). Service-scoped
        # (a restart drops the bindings — the durable pins survive on sqlite, but
        # re-validating a post-restart token is the Unit 5 liveness concern).
        self._session_owners: dict[str, UUID] = {}
        # Session heartbeat lease (SB-17 / TX-1, Unit 5 / R4):
        # ``{session_token: last_heartbeat_tick}``. Keyed by the server-minted
        # SESSION TOKEN, NOT the MESI ``agent_id`` — a snapshot session is NOT a
        # MESI agent and holds no grant, so the grant sweep (which walks M∪E
        # holders) can never see it. The session-liveness sweep is a NEW axis
        # over THIS map. Seeded at ``begin_session`` with the creation tick so a
        # never-yet-heartbeated session still carries a lease baseline (mirrors a
        # grant's ``granted_at_tick``), rather than being reaped on the first
        # sweep. Service-scoped: an in-memory restart drops the lease (and the
        # pins), so a post-restart token has no cut and fails closed.
        self._session_heartbeats: dict[str, int] = {}
        # Bounded reaped-session tombstone (Unit 5 / R4): recently-reaped tokens,
        # capped + oldest-evicted (FIFO). Lets ``session_read`` / ``session_commit``
        # attribute a reaped token precisely to ``session_invalidated``. Eviction
        # is benign — an evicted in-shape token still classifies invalidated via
        # ``looks_like_session_token`` (the shape predicate), so the fail-closed
        # guarantee never depends on the tombstone, only the precise attribution.
        self._reaped_tombstone: "OrderedDict[str, None]" = OrderedDict()

    def register_artifact(
        self,
        *,
        name: str,
        content: str,
        initial_owner: UUID | None = None,
        size_tokens: int | None = None,
        content_hash: str | None = None,
        depends_on: tuple[UUID, ...] = (),
    ) -> Artifact:
        """Register a new artifact and optionally assign an initial owner."""
        artifact = Artifact(
            name=name,
            version=1,
            content_hash=content_hash,
            size_tokens=size_tokens,
            depends_on=depends_on,
        )
        self.registry.register_artifact(artifact, content)
        if initial_owner is not None:
            self.registry.set_agent_state(artifact.id, initial_owner, MESIState.EXCLUSIVE, trigger="register", tick=0)
        return artifact

    def fetch(self, request: FetchRequest) -> FetchResponse:
        """Fetch canonical artifact payload and grant requester state."""
        artifact = self._require_artifact(request.artifact_id)
        content = self.registry.get_content(request.artifact_id)
        if content is None:
            raise CoherenceError(f"artifact_content_missing artifact={request.artifact_id}")

        state_map = self.registry.get_state_map(request.artifact_id)
        other_holders = [
            agent_id
            for agent_id, state in state_map.items()
            if agent_id != request.requesting_agent_id and state != MESIState.INVALID
        ]

        grant = MESIState.EXCLUSIVE if not other_holders else MESIState.SHARED
        self.registry.set_agent_transient(
            request.artifact_id,
            request.requesting_agent_id,
            TransientState.IED if grant == MESIState.EXCLUSIVE else TransientState.ISG,
            entered_tick=request.requested_at_tick,
        )
        if other_holders:
            # Multiple readers must stay coherent; downgrade any exclusive/modified holder.
            for agent_id in other_holders:
                self.registry.set_agent_state(
                    request.artifact_id, agent_id, MESIState.SHARED, trigger="fetch", tick=request.requested_at_tick
                )

        self.registry.set_agent_state(
            request.artifact_id, request.requesting_agent_id, grant, trigger="fetch", tick=request.requested_at_tick
        )
        self.registry.clear_agent_transient(request.artifact_id, request.requesting_agent_id)
        self._validate_single_writer(request.artifact_id)

        return FetchResponse(
            artifact_id=request.artifact_id,
            version=artifact.version,
            content=content,
            state_grant=grant,
        )

    def read_at_version(
        self,
        artifact_id: UUID,
        version: int,
        expected_epoch: str | None = None,
    ) -> VersionedContent | VersionedReadRejection:
        """Read a specific RETAINED version off-protocol (plan item N v1 / R5–R7).

        The first-class read-at-version surface: returns the body the registry
        committed at ``version`` as a :class:`~ccs.core.types.VersionedContent`,
        or a typed :class:`~ccs.core.types.VersionedReadRejection` carrying one of
        the six wire-stable reasons in :data:`ccs.core.exceptions.READ_AT_VERSION_REASONS`.
        It is a typed RETURN, never an exception (the ``ConflictDetail``
        discipline) — except ``version < 1``, which is caller misuse and raises
        ``ValueError`` (house style; not a wire reason).

        **Protocol non-interaction by construction (R6, R7):** this method calls
        NONE of ``set_agent_state`` / ``set_agent_transient`` / grant / transient
        / invalidation code. Read-generation capture lives ONLY inside
        ``set_agent_state`` (``registry.py`` ``CLAIM_CAPTURE_TRIGGERS``), so not
        calling it means the read CANNOT touch ``read_generation`` (R6 fence
        non-capture) or any MESI state / invalidation membership (R7). The whole
        point is verifiable by reading this body: it only ever READS the registry
        (``get_artifact`` / ``retention_meta`` / ``coordinator_epoch`` /
        ``get_version_record``) and constructs frozen return values.

        Discrimination order (first match wins), computed against ONE snapshot of
        the current version so a racing commit cannot mislabel a reason:

        1. ``version < 1`` → ``ValueError`` (caller misuse).
        2. artifact unknown → ``unknown_artifact`` (``current_version=None``).
        3. retention not enabled for the store → ``retention_off``.
        4. ``expected_epoch`` supplied and != the store epoch → ``epoch_mismatch``.
        5. ``version == current`` → ``current_version`` (history surface serves
           history only; current bytes are read via the protocol fetch path).
        6. ``version > current`` → ``future_version`` (hints at a 2nd coordinator).
        7. ``1 <= version < current`` → fetch the row. Present AND not T-expired →
           ``VersionedContent``; absent OR T-expired → ``not_retained``.

        Single-scope atomicity: ``current`` is read ONCE (the linearization
        point) and every reason is decided relative to that snapshot. History
        rows below ``current`` are immutable (a version is captured once and only
        ever DROPPED by GC, never rewritten), so the step-7 row fetch is safe
        against a commit that races in after the ``current`` read — that commit
        only captures the NEW (higher) version and never touches the requested
        ``version < current`` row. The worst a race yields is the old-current
        served as history (correct bytes) or ``current_version`` (the value that
        WAS current at the read point) — never wrong bytes or a mislabeled reason.

        T-expiry is LOGICAL at read (R-fix: a read is non-mutating, so an
        age-collectible row is reported ``not_retained`` but NOT physically
        deleted here — physical deletion piggybacks on the next capture). It
        reuses the one GC seam :func:`collectible_versions` against the persisted
        policy so the read-side expiry rule matches the write-side eviction rule.

        Args:
            artifact_id: The artifact to read.
            version: The 1-based version to read (``< 1`` raises ``ValueError``).
            expected_epoch: If given, the read rejects ``epoch_mismatch`` unless
                it equals the store's ``coordinator_epoch`` (the store was reset
                since the caller captured the epoch).

        Returns:
            :class:`VersionedContent` on a hit, else :class:`VersionedReadRejection`.

        Raises:
            ValueError: ``version < 1`` (caller misuse — not a wire reason).
        """
        if version < 1:
            raise ValueError(
                f"read_at_version: version must be >= 1 (got {version}); "
                f"versions are 1-based. A sub-1 version is caller misuse, not a "
                f"retained-history miss (which is the not_retained reason)."
            )

        epoch = self.registry.coordinator_epoch

        # (2) Unknown artifact — no current version exists for it. Read the
        # artifact ONCE; ``current`` from this same metadata object is the
        # linearization snapshot every reason below is decided against.
        artifact = self.registry.get_artifact(artifact_id)
        if artifact is None:
            return VersionedReadRejection(
                reason=UNKNOWN_ARTIFACT_REASON,
                artifact_id=artifact_id,
                requested_version=version,
                current_version=None,
                coordinator_epoch=epoch,
            )
        current = artifact.version

        # (3) Retention never enabled for this store (store-derived: persisted
        # meta on sqlite, live ctor state in-memory). The artifact_versions
        # surface exists on every v2 db, so this marker — not table presence —
        # distinguishes retention-off from a mere history gap.
        retention_enabled, policy = self.registry.retention_meta()
        if not retention_enabled:
            return self._reject(
                RETENTION_OFF_REASON, artifact_id, version, current, epoch
            )

        # (4) Epoch guard — a stale expected_epoch means the store was reset
        # (delete-and-recreate) since the caller captured it, so its retained
        # history is from a different incarnation.
        if expected_epoch is not None and expected_epoch != epoch:
            return self._reject(
                EPOCH_MISMATCH_REASON, artifact_id, version, current, epoch
            )

        # (5) Current version — history surface serves HISTORY ONLY. Current
        # content is read via the protocol fetch path (artifacts store hashes,
        # not bodies), never here, by design.
        if version == current:
            return self._reject(
                CURRENT_VERSION_REASON, artifact_id, version, current, epoch
            )

        # (6) Future version — above current suggests a second coordinator
        # writing the same store (the diagnostic commit_cas keeps via CasCorruption).
        if version > current:
            return self._reject(
                FUTURE_VERSION_REASON, artifact_id, version, current, epoch
            )

        # (7) 1 <= version < current: a genuine history request. Fetch body +
        # capture timestamp in ONE scoped accessor (single SELECT/sqlite, GIL-
        # atomic pair/in-memory). Absent ⇒ not_retained (never captured, K-
        # evicted, or already T-swept).
        record = self.registry.get_version_record(artifact_id, version)
        if record is None:
            return self._reject(
                NOT_RETAINED_REASON, artifact_id, version, current, epoch
            )
        content, captured_at = record

        # Logical T-expiry (R-fix: non-mutating read). Reuse the single GC seam
        # against the persisted policy: an age-collectible row reports
        # not_retained without being physically deleted (deletion piggybacks on
        # the next capture). ``current`` is exempt in collectible_versions, but
        # we already excluded version==current above, so this only ages the
        # requested historical row. ``policy is None`` ⇒ unbounded ⇒ no T axis.
        #
        # Unit 3 (DONE): the read-serve allowance — a live-session pin suppresses
        # this read-side logical T-expiry so a pinned-but-age-collectible row is
        # still SERVED — lives in ``session_read`` (the session-scoped path), NOT
        # here. The bare ``read_at_version`` deliberately keeps ages-out semantics
        # (no live session, no allowance); ``session_read`` passes the pinned
        # version as its OWN ``exemptions`` to this same seam. (The GC-HOLD
        # exemption — distinct — was wired by Unit 2 at the GC producers.)
        if policy is not None and version in collectible_versions(
            {version: captured_at},
            current_version=current,
            policy=policy,
            now=time.time(),
        ):
            return self._reject(
                NOT_RETAINED_REASON, artifact_id, version, current, epoch
            )

        return VersionedContent(
            artifact_id=artifact_id,
            version=version,
            content=content,
            captured_at=captured_at,
            coordinator_epoch=epoch,
        )

    @staticmethod
    def _reject(
        reason: str,
        artifact_id: UUID,
        requested_version: int,
        current_version: int | None,
        epoch: str,
    ) -> VersionedReadRejection:
        """Build a :class:`VersionedReadRejection` (no body material, by type)."""
        return VersionedReadRejection(
            reason=reason,
            artifact_id=artifact_id,
            requested_version=requested_version,
            current_version=current_version,
            coordinator_epoch=epoch,
        )

    def begin_session(
        self,
        *,
        read_set: Iterable[UUID],
        owner: UUID,
        created_at_tick: int = 0,
    ) -> SnapshotSession | VersionedReadRejection:
        """Open a consistent multi-artifact snapshot session (SB-17 / TX-1,
        Unit 2 / R1, R13). Pins a coherent CUT of ``read_set`` at one
        linearization point and returns an inspectable
        :class:`~ccs.core.types.SnapshotSession`.

        Orchestration (Unit 2 scope):

        1. Mint a server-minted ``session_token`` (``secrets.token_urlsafe`` —
           unguessable, never client-supplied; R9/R13) and bind it to ``owner``
           (the creating caller's MESI agent/process identity) in
           ``_session_owners``. (Unit 2 mints + owner-binds AT CREATION ONLY;
           per-call token validation, timing-safe compare, caps, the
           heartbeat-lease, and the absolute-age ceiling are LATER units 5/7.)
        2. Call the registry's atomic ``capture_version_vector`` — the cut is
           captured and pinned in one linearization point, non-mutating (no MESI
           grant, no ``read_generation``). An unknown id in ``read_set`` returns
           a typed :class:`VersionedReadRejection` (``unknown_artifact``) with NO
           pins inserted; the token binding is then dropped (no half-open session
           for a rejected cut).
        3. Read ``retain_versions`` from the store (the deployment-branch
           indicator) and return the :class:`SnapshotSession` with the
           INSPECTABLE cut (R11).

        Byte handling is explicitly NOT here (Unit 3): this captures the
        version-MAP only and records ``retain_versions``; the eager-vs-lazy serve
        is resolved at the serve layer. For ``content=None`` / cross-process the
        bytes live in the data plane, not the coordinator.

        Args:
            read_set: The artifact ids to pin into the consistent cut.
            owner: The creating caller's identity (the MESI agent/process label),
                bound to the minted token for R13 owner-binding.

        Returns:
            A :class:`SnapshotSession` on success, else a
            :class:`VersionedReadRejection` (``unknown_artifact``) — no session
            opened, no pins held.
        """
        session_token = secrets.token_urlsafe(_SESSION_TOKEN_BYTES)
        # Owner-bind at mint (R13). Recorded BEFORE the capture so a concurrent
        # later-unit validator never sees a pinned-but-unowned token; dropped
        # again if the capture rejects. NOTE for Unit 7 (the per-call validator):
        # the INVERSE window also exists — between this line and the capture
        # below, the token is owned but PINLESS. Unit 7 must treat an owned-but-
        # pinless token as not-yet-live (or read owner-binding + pins together),
        # never as a valid empty session.
        self._session_owners[session_token] = owner
        result = self.registry.capture_version_vector(read_set, session_token)
        if isinstance(result, VersionedReadRejection):
            # No cut pinned (unknown id) ⇒ no session. Drop the owner binding so
            # the rejected token cannot linger as a half-open session.
            self._session_owners.pop(session_token, None)
            return result
        # Seed the heartbeat lease at the creation tick (Unit 5 / R4): a session
        # that never heartbeats still carries a baseline so it is not reaped on
        # the very first sweep — the lease starts now, exactly like a grant's
        # ``granted_at_tick``. Recorded only on a SUCCESSFUL capture (a rejected
        # capture returned above, leaving no lease and no owner binding).
        self._session_heartbeats[session_token] = created_at_tick
        retain_versions, _policy = self.registry.retention_meta()
        return SnapshotSession(
            session_token=session_token,
            cut=result,
            coordinator_epoch=self.registry.coordinator_epoch,
            retain_versions=retain_versions,
        )

    def session_read(
        self,
        session_token: str,
        artifact_id: UUID,
    ) -> VersionedContent | DataPlaneDeferredRead | SessionReadRejection:
        """Serve an artifact's PINNED version from a live snapshot session — the
        non-mutating read from the consistent cut (SB-17 / TX-1, Unit 3 / R2).

        A NEW service path, NOT an extension of ``read_at_version`` (which is the
        bare history read with its own frozen 6-reason contract and its
        deliberate ``version == current`` REJECTION). The two surfaces have
        OPPOSITE rules for the current version — bare ``read_at_version`` rejects
        it; ``session_read`` SERVES it — and a different validation gate (a live
        pin, not raw retention state). Threading session-awareness through
        ``read_at_version`` would entangle those contracts and muddy the
        ``current_version`` rejection a pinned test pins; a separate path keeps
        each surface honest. ``begin_session`` is the coherence event that earns
        the pinned-version serve (including ``version == current``), so the
        allowance is scoped to a valid live session here.

        **Non-mutating (R2, the shipped invariant):** like ``read_at_version``,
        this calls NONE of ``set_agent_state`` / ``set_agent_transient`` / grant /
        invalidation code. It only READS the registry (``get_session_cut`` /
        ``get_artifact`` / ``retention_meta`` / ``get_version_record`` /
        ``coordinator_epoch``) and builds frozen returns, so it mints NO MESI
        grant and captures NO ``read_generation`` (read-gen capture lives ONLY in
        ``set_agent_state``). A reader is not an owner.

        Bytes source — the deployment-dependent rule resolved at the serve layer
        (KTD), keyed off the session's branch (``retain_versions``, recorded at
        ``begin_session``):

        - **LAZY (``retain_versions=True``)** — the coordinator HAS bodies in
          history. Serve the PINNED version's body from ``get_version_record``
          (the retained-history accessor): the current version's body is captured
          into history at commit, so it serves BOTH ``pinned == current`` AND
          ``pinned < current`` uniformly. The TRANSITION is automatic — once a
          peer commits past the pin, ``current`` advances but the pinned row
          persists in history, so the SAME ``get_version_record(pinned)`` keeps
          serving the pinned bytes (re-read ``current`` each call, never cache
          the branch). **Read-serve allowance (the Unit-3 obligation):** the
          pinned version is passed as its OWN ``exemptions`` to the T-expiry
          ``collectible_versions`` seam, so a pinned-but-age-collectible row is
          STILL SERVED (distinct from the GC-hold the Unit-2 exemptions seam
          already provides at the GC producers — this lifts the read-side LOGICAL
          T-expiry that ``read_at_version`` would apply). A genuinely absent body
          (``content=None`` committed even under retain=True, or a GC race)
          degrades to the data-plane-deferred result, never a crash or wrong
          bytes.
        - **EAGER (``retain_versions=False`` / ``content=None`` ICP)** — the
          coordinator holds NO body for the pinned version (bodies live in the
          CoherentVolume data plane). Return a typed
          :class:`~ccs.core.types.DataPlaneDeferredRead` carrying the pinned
          version + epoch (+ ``content_hash`` when known) — the honest "ask the
          data plane for the bytes" signal. The actual eager byte serve is
          **Unit 6 (CoherentVolume)**; this method never reads the data plane.

        Validation (Unit 3 scope): the token must have a live pin for
        ``artifact_id``. An unknown/released token → ``session_not_found``; a live
        token whose cut lacks ``artifact_id`` → ``artifact_not_in_cut`` — both
        typed :class:`~ccs.core.types.SessionReadRejection`, NEVER a live-HEAD
        fall-through. (Per-call OWNER-binding validation — a foreign owner reading
        another's cut — is Unit 7; the heartbeat-liveness ``session_invalidated``
        axis is Unit 5. Unit 3 does the token-has-pin check only.)

        Args:
            session_token: The server-minted session identity from
                ``begin_session``.
            artifact_id: The artifact to read at its pinned version.

        Returns:
            :class:`VersionedContent` (coordinator-held pinned bytes),
            :class:`DataPlaneDeferredRead` (bytes live in the data plane), or a
            :class:`SessionReadRejection` (no valid pin) — all typed RETURNS,
            never an exception.
        """
        epoch = self.registry.coordinator_epoch

        # Unit 7: per-call OWNER-binding validation goes HERE — read
        # ``_session_owners[session_token]`` and reject a foreign caller
        # (timing-safe compare) with the Unit-5 ``session_invalidated`` reason
        # BEFORE the pin lookup. Unit 3 does the token-has-pin check only.
        cut = self.registry.get_session_cut(session_token)
        if cut is None:
            # FAIL CLOSED (Unit 5 / R4): no live cut for this token — it was
            # reaped by the session-liveness sweep, GC-raced, wiped by an
            # in-memory restart, released, or never opened. NEVER a live-HEAD
            # fall-through. ``_classify_no_cut_reason`` splits the wire-stable
            # taxonomy: ``session_invalidated`` for a reaped / restart-wiped /
            # in-shape token ("re-establish your session"), ``session_not_found``
            # for a genuinely never-opened / malformed token. Both are typed
            # rejections; the split only sharpens the signal.
            return SessionReadRejection(
                reason=self._classify_no_cut_reason(session_token),
                artifact_id=artifact_id,
                coordinator_epoch=epoch,
            )
        if artifact_id not in cut:
            # A live session, but this artifact was not pinned. Reject — NEVER
            # serve live HEAD for an un-pinned artifact (out of scope: a session
            # wanting fresh data starts a new session).
            return SessionReadRejection(
                reason=SESSION_ARTIFACT_NOT_IN_CUT_REASON,
                artifact_id=artifact_id,
                coordinator_epoch=epoch,
            )
        pinned = cut[artifact_id]

        # Read ``current`` ONCE (the per-call linearization snapshot) so the
        # branch routing and the deferred-hash hint are decided against one view.
        # The artifact may have been deleted out from under a live pin (the
        # session_pins table deliberately has NO cascade FK); that fail-closed
        # path is Unit 5 (``SessionInvalidated``). Until then a missing artifact
        # under a live pin degrades to data-plane-deferred (never wrong bytes).
        artifact = self.registry.get_artifact(artifact_id)
        current = artifact.version if artifact is not None else None
        # The pinned version's hash is knowable only when it is STILL current
        # (the artifacts table holds the current hash only); a superseded pin's
        # hash is not separately retained on the coordinator.
        pinned_hash = (
            artifact.content_hash
            if artifact is not None and current == pinned
            else None
        )

        retain_versions, policy = self.registry.retention_meta()
        if not retain_versions:
            # EAGER branch: the coordinator never retained a body for the pinned
            # version — the canonical bytes live in the data plane. Honest typed
            # deferral (pinned coordinates only, NO bytes); the data-plane serve
            # is Unit 6.
            return DataPlaneDeferredRead(
                artifact_id=artifact_id,
                version=pinned,
                content_hash=pinned_hash,
                coordinator_epoch=epoch,
            )

        # LAZY branch: serve the pinned version's body from retained history
        # (the current version's body is captured into history at commit, so
        # this serves both pinned==current and pinned<current). A genuinely
        # absent body (content=None under retain=True, or a GC race) is NOT a
        # crash — degrade to the data-plane-deferred signal.
        record = self.registry.get_version_record(artifact_id, pinned)
        if record is None:
            return DataPlaneDeferredRead(
                artifact_id=artifact_id,
                version=pinned,
                content_hash=pinned_hash,
                coordinator_epoch=epoch,
            )
        content, captured_at = record

        # Read-serve allowance (the Unit-3 obligation): the pinned version is its
        # OWN exemption to the read-side LOGICAL T-expiry, so a pinned-but-age-
        # collectible row is STILL served. Because ``pinned`` is always in
        # ``exemptions``, ``collectible_versions`` can never mark it — the call is
        # kept (rather than skipped) to make the allowance explicit and to age
        # NOTHING else here. ``policy is None`` ⇒ unbounded ⇒ no T axis. This is
        # the read-serve counterpart to the Unit-2 GC-hold ``exemptions`` seam.
        if policy is not None and current is not None:
            _served_despite_age = pinned not in collectible_versions(
                {pinned: captured_at},
                current_version=current,
                policy=policy,
                now=time.time(),
                exemptions={pinned},
            )
            # Invariant by construction: a self-exempt version is never
            # collectible. Asserting documents intent without a runtime branch.
            assert _served_despite_age, (
                "pinned version unexpectedly collectible despite self-exemption "
                "(the read-serve allowance regressed)"
            )

        return VersionedContent(
            artifact_id=artifact_id,
            version=pinned,
            content=content,
            captured_at=captured_at,
            coordinator_epoch=epoch,
        )

    def session_commit(
        self,
        session_token: str,
        artifact_id: UUID,
        content: bytes | str,
        *,
        size_tokens: int | None = None,
        issued_at_tick: int = 0,
    ) -> tuple[Artifact, list[InvalidationSignal]] | ConflictDetail | SessionCommitRejection:
        """Validate one artifact's commit against its PINNED version via the
        shipped ``commit_cas`` — the single-artifact OCC commit from a snapshot
        session (SB-17 / TX-1, Unit 4 / R3).

        The commit is arbitrated against the cut's pinned base: ``expected_version``
        is ``cut[artifact_id]`` (the version captured at ``begin_session``), so a
        commit WINS only if no peer moved the artifact since the cut was pinned.
        This reuses the shipped ``commit_cas`` arbitration VERBATIM — no
        re-implemented OCC, single-shot (NEVER the auto-rederive ``write_cas``
        loop, whose split-comparand hazard a pinned base is precisely meant to
        avoid).

        **The admit-on-absent load-bearing path (R3, the reconciled fence).** The
        commit rides a SESSION-SCOPED committer identity derived deterministically
        from the ``session_token`` (``uuid5``), NOT the owner's MESI ``agent_id``.
        The reason is the read-generation fence: ``commit_cas`` ADMITS a committer
        with NO captured ``read_generation`` (version-CAS then arbitrates) and
        REJECTS one whose PRESENT ``read_generation`` was superseded by a sweep
        reclamation. The owner's MESI agent could be carrying such a superseded
        ``read_generation`` from unrelated prior MESI activity — committing under
        it would spuriously fail with ``stale_read_generation`` on a perfectly
        healthy session. The session-derived identity has never established a fence
        claim (no ``read_generation`` row), so admit-on-absent holds and the
        pinned-base version-CAS is the sole arbiter. It is deterministic (stable
        across a session's calls) and collision-free against real agent ids (a
        ``uuid5`` over a 32-byte server-minted token namespace).

        Validation (Unit 4 scope): the token must have a live pin for
        ``artifact_id``. An unknown/released token → ``session_not_found``; a live
        token whose cut lacks ``artifact_id`` → ``artifact_not_in_cut`` — both a
        typed :class:`~ccs.core.types.SessionCommitRejection`, NEVER a silent
        fall-through to a live-HEAD commit. (Per-call OWNER-binding validation — a
        foreign owner committing into another's cut — and the caps are Unit 7; the
        heartbeat-liveness ``session_invalidated`` axis is Unit 5. Unit 4 does the
        token-has-pin check only.)

        Outcome mapping (mirrors the shipped ``commit_cas`` orchestration exactly):

        - WIN → ``(updated_artifact, invalidation_signals)``: the artifact moved
          to ``pinned + 1`` and ``commit_cas`` ALREADY invalidated the peers
          atomically — this method emits NO additional invalidation signal.
        - :class:`ConflictDetail` (``version_mismatch`` / ``other_holder`` /
          ``stale_read_generation``) → RETURNED UNCHANGED (HELD, retry-eligible;
          nothing mutated, so no invalidation is emitted). Recover via a NEW
          session + re-read + re-commit.
        - corruption (``expected_version > current``) → ``commit_cas`` maps the
          registry's :class:`CasCorruption` sentinel to a RAISED ``CoherenceError``
          (non-retryable); ``session_commit`` lets it propagate.

        **"Exactly one validated commit" (R11) is naturally enforced — no explicit
        single-use machinery.** After a WIN the artifact advanced to ``pinned + 1``
        but the cut still pins ``pinned``; a SECOND ``session_commit`` at the same
        pin therefore version-mismatches (``expected_version < current``) and is
        HELD. The pin is not consumed or rewritten here (that would foreclose the
        SB-18 multi-commit shape, R11) — staleness does the enforcing.

        Args:
            session_token: The server-minted session identity from
                ``begin_session``.
            artifact_id: The pinned artifact to commit. Must be in the cut.
            content: The new body. ``content_hash`` is derived from it
                (``compute_content_hash``); the body is threaded to ``commit_cas``
                so the in-memory path advances ``record.content`` on a WIN (the
                cross-process / ``content=None`` path keeps no body — see
                ``commit_cas``).
            size_tokens: Optional token count to persist with the commit.
            issued_at_tick: Logical tick for the commit (threaded to ``commit_cas``).

        Returns:
            ``(updated_artifact, signals)`` on a WIN, a :class:`ConflictDetail` on a
            retry-eligible lost race, or a :class:`SessionCommitRejection` on a
            validation failure — all typed RETURNS. Corruption RAISES
            ``CoherenceError`` (via ``commit_cas``); a missing artifact under a
            live pin also raises there (the fail-closed ``SessionInvalidated`` for
            that race is Unit 5).
        """
        epoch = self.registry.coordinator_epoch

        # Unit 7: per-call OWNER-binding validation goes HERE — read
        # ``_session_owners[session_token]`` and reject a foreign caller
        # (timing-safe compare) BEFORE the pin lookup. Unit 4 does the
        # token-has-pin check only.
        cut = self.registry.get_session_cut(session_token)
        if cut is None:
            # FAIL CLOSED (Unit 5 / R4): no live cut for this token — reaped,
            # GC-raced, restart-wiped, released, or never opened. NEVER a silent
            # fall-through to a live-HEAD commit. Same wire-stable taxonomy as
            # ``session_read``: ``session_invalidated`` for a reaped / restart-
            # wiped / in-shape token, ``session_not_found`` for a never-opened /
            # malformed one.
            return SessionCommitRejection(
                reason=self._classify_no_cut_reason(session_token),
                artifact_id=artifact_id,
                coordinator_epoch=epoch,
            )
        if artifact_id not in cut:
            # A live session, but this artifact was not pinned. Reject — NEVER
            # commit live HEAD for an un-pinned artifact (a session commits only
            # against what it pinned).
            return SessionCommitRejection(
                reason=SESSION_ARTIFACT_NOT_IN_CUT_REASON,
                artifact_id=artifact_id,
                coordinator_epoch=epoch,
            )

        expected_version = cut[artifact_id]
        committer_id = self._session_committer_id(session_token)
        content_hash = compute_content_hash(content)

        # Reuse the shipped service ``commit_cas`` orchestration VERBATIM: it owns
        # the CasCorruption-sentinel -> raised CoherenceError mapping, returns a
        # ConflictDetail unchanged (no mutation, no invalidation), and builds the
        # InvalidationSignal list on a WIN. Single-shot — there is no retry loop.
        # ``committer_id`` is fence-claimless (admit-on-absent), so the pinned
        # ``expected_version`` is the sole arbiter.
        return self.commit_cas(
            agent_id=committer_id,
            artifact_id=artifact_id,
            expected_version=expected_version,
            content_hash=content_hash,
            issued_at_tick=issued_at_tick,
            size_tokens=size_tokens,
            content=content,
        )

    @staticmethod
    def _session_committer_id(session_token: str) -> UUID:
        """Derive the SESSION-SCOPED committer identity for ``session_commit``.

        A deterministic ``uuid5`` over the server-minted ``session_token`` (under
        the URL namespace). Stable across a session's commits and collision-free
        against real MESI agent ids; crucially it has NEVER established a
        read-generation fence claim, so ``commit_cas`` ADMITS it on absence and
        the pinned-base version-CAS arbitrates (the R3 load-bearing path). NOT the
        owner's MESI ``agent_id``, which could carry a superseded
        ``read_generation`` that would spuriously trip the fence.
        """
        return uuid5(NAMESPACE_URL, session_token)

    # ------------------------------------------------------------------
    # Session pin lifetime — heartbeat lease + liveness sweep (Unit 5 / R4)
    # ------------------------------------------------------------------

    def record_session_heartbeat(
        self, *, session_token: str, owner: UUID, now_tick: int
    ) -> bool:
        """Refresh a snapshot session's heartbeat LEASE (SB-17 / TX-1, Unit 5 /
        R4). Keyed by the server-minted SESSION TOKEN, owner-bound.

        A session is NOT a MESI agent — this does NOT key by ``agent_id`` and does
        NOT reuse :meth:`record_heartbeat` (the grant-holder heartbeat). It keeps
        the session's own lease alive so the session-liveness sweep
        (:meth:`enforce_session_liveness`) does not reap its pins while the owner
        is still working. Monotonic like the grant heartbeat: ``max(prev,
        incoming)``, so a stale/replayed lower tick never moves the lease back.

        **Owner-bound (R13, security).** The caller must be the session's OWNER —
        the identity bound at ``begin_session``. A FOREIGN caller must NOT be able
        to keep another agent's session alive (that would let an attacker pin
        versions against GC indefinitely under someone else's session). The owner
        check is TIMING-SAFE: it compares the bound owner and the supplied
        ``owner`` via :func:`hmac.compare_digest` over their stable 16-byte
        big-endian encoding, so a foreign caller learns nothing from response
        timing about how much of the id matched. A mismatch is rejected and the
        lease is NOT refreshed.

        Heartbeating an unknown / released / restart-wiped token is a typed
        NO-OP: it returns ``False`` (never a crash, never a resurrection — a dead
        session cannot be revived by a heartbeat; re-establish it via
        ``begin_session``). It also does NOT create a lease for a token with no
        owner binding, so a heartbeat cannot conjure a live lease for a
        cut-less token.

        Args:
            session_token: The server-minted token from ``begin_session``.
            owner: The caller's identity; must match the bound owner (timing-safe).
            now_tick: The heartbeat tick (``>= 0``). Applied as ``max(prev,
                incoming)``.

        Returns:
            ``True`` if the lease was refreshed (known token, owner matched),
            else ``False`` (unknown/released/wiped token, or a foreign caller —
            indistinguishable to the caller by design: a foreign caller is not
            told whether the token exists).
        """
        if now_tick < 0:
            raise ValueError("now_tick must be >= 0")
        bound_owner = self._session_owners.get(session_token)
        if bound_owner is None:
            # Unknown / released / restart-wiped token: no owner binding ⇒ no
            # lease to refresh. Typed no-op (never a crash, never a new lease).
            return False
        # Timing-safe owner-binding check (R13): compare over the stable 16-byte
        # big-endian UUID encoding. A foreign caller cannot keep another's
        # session alive AND cannot probe id-match progress via response timing.
        if not hmac.compare_digest(
            bound_owner.bytes, owner.bytes
        ):
            return False
        prev = self._session_heartbeats.get(session_token)
        if prev is None or now_tick > prev:
            self._session_heartbeats[session_token] = now_tick
        return True

    def enforce_session_liveness(
        self,
        *,
        current_tick: int,
        heartbeat_timeout_ticks: int,
    ) -> int:
        """Reap snapshot sessions whose heartbeat lease has gone stale — the NEW
        session-liveness sweep AXIS (SB-17 / TX-1, Unit 5 / R4).

        Distinct from :meth:`enforce_stable_grant_timeouts`: that sweep walks
        only M∪E GRANT-HOLDERS, and a snapshot session holds NO grant, so it is
        invisible to the grant sweep. This sweep ENUMERATES SESSIONS (the
        owner-bound token set) and reaps any whose lease is stale, reusing the
        SAME heartbeat-staleness predicate SHAPE as the grant sweep (``current -
        last_hb >= timeout``, with ``>=`` matching ADV-02) and the same
        ``CrashRecoveryConfig`` knob (``heartbeat_timeout_ticks``). It does NOT
        reuse ``max_hold_ticks`` — the absolute-age ceiling that a live heartbeat
        must not exempt is Unit 7 (a SEPARATE cap), not the liveness lease.

        Reaping a session: :meth:`registry.release_session` drops its pins (so its
        pinned versions become collectible again), then its owner binding and
        heartbeat lease are cleared and the token is recorded in the bounded
        reaped tombstone. After reaping, a ``session_read`` / ``session_commit``
        on that token fails closed with ``session_invalidated`` (the cut is gone).

        A slow-but-LIVE session — one heartbeated within ``heartbeat_timeout_ticks``
        — is NOT reaped. This is a PREDICATE on the lease, not a hard TTL: a
        long-running session that keeps heartbeating survives indefinitely
        (the absolute-age ceiling that overrides even a live heartbeat is the
        SEPARATE Unit-7 cap).

        Args:
            current_tick: The sweep's logical clock (monotonic ticks, as the
                grant sweep uses).
            heartbeat_timeout_ticks: Reap any session whose lease is at least
                this many ticks stale (or that somehow has no lease — defensive).

        Returns:
            The number of sessions reaped.
        """
        if heartbeat_timeout_ticks < 1:
            raise ValueError("heartbeat_timeout_ticks must be >= 1")

        # Snapshot the token set first — reaping mutates ``_session_owners`` /
        # ``_session_heartbeats`` under the loop, so iterate a stable copy.
        tokens = list(self._session_owners.keys())
        reaped = 0
        for session_token in tokens:
            last_hb = self._session_heartbeats.get(session_token)
            # Same staleness predicate SHAPE as the grant sweep (``>=`` per
            # ADV-02). A missing lease (should not happen — begin_session seeds
            # one) is treated as stale, defensively, so a leaseless session is
            # never immortal.
            stale = (
                last_hb is None
                or (current_tick - last_hb) >= heartbeat_timeout_ticks
            )
            if not stale:
                continue
            self._reap_session(session_token)
            reaped += 1
        return reaped

    def _reap_session(self, session_token: str) -> None:
        """Drop a session's pins + lease + owner binding and tombstone the token
        (Unit 5 / R4). The single reap path so the order is consistent: release
        the pins FIRST (the registry is the durable/authoritative pin store, so a
        crash mid-reap leaves no live-pin-without-lease leak on sqlite), then
        clear the service-layer lease + owner binding, then tombstone."""
        # Pins first: the registry release is idempotent (unknown token → no-op),
        # so a double-reap or a restart-wiped pin set is harmless.
        self.registry.release_session(session_token)
        self._session_heartbeats.pop(session_token, None)
        self._session_owners.pop(session_token, None)
        self._tombstone_token(session_token)

    def _tombstone_token(self, session_token: str) -> None:
        """Record a reaped token in the bounded FIFO tombstone (Unit 5 / R4).
        Capped at :data:`_REAPED_TOMBSTONE_CAP`; the oldest entry is evicted when
        full. Eviction is benign — an evicted in-shape token still classifies
        ``session_invalidated`` via :func:`looks_like_session_token`, so the
        fail-closed guarantee never depends on tombstone residency."""
        # Move-to-end keeps the most-recently-reaped tokens; popitem(last=False)
        # evicts the oldest (FIFO) when over the cap.
        self._reaped_tombstone[session_token] = None
        self._reaped_tombstone.move_to_end(session_token)
        while len(self._reaped_tombstone) > _REAPED_TOMBSTONE_CAP:
            self._reaped_tombstone.popitem(last=False)

    def _classify_no_cut_reason(self, session_token: str) -> str:
        """Classify a token that has NO live cut into the wire-stable fail-closed
        reason (Unit 5 / R4). FAIL CLOSED in every branch — this only sharpens
        the SIGNAL, never serves live HEAD.

        - In the reaped tombstone → ``session_invalidated`` (definitely reaped
          this process: "your session died, re-establish it").
        - Shaped like a server-minted token (``looks_like_session_token``) →
          ``session_invalidated``. This is the POST-RESTART-UNKNOWN safety case:
          an in-memory restart wiped ``_session_pins`` (and the tombstone), so a
          previously-valid token now has no cut — but it WAS a real session, so it
          must fail closed as invalidated, NEVER served live HEAD as if pinned. A
          well-formed-but-never-minted token also lands here (fail-closed is the
          safe default for an in-shape token; it is rejected either way).
        - Otherwise (out-of-shape / malformed) → ``session_not_found``: a
          genuinely never-opened token. Kept reachable additively so a clearly
          bogus token is still distinguishable.
        """
        if session_token in self._reaped_tombstone:
            return SESSION_INVALIDATED_REASON
        if looks_like_session_token(session_token):
            return SESSION_INVALIDATED_REASON
        return SESSION_NOT_FOUND_REASON

    def write(
        self,
        *,
        agent_id: UUID,
        artifact_id: UUID,
        issued_at_tick: int = 0,
        abort: threading.Event | None = None,
    ) -> list[InvalidationSignal]:
        """Request write ownership by invalidating peers and granting EXCLUSIVE.

        Wrapped in :meth:`registry.abort_guard` (finding A6): if the handler
        watchdog already timed out, the grant aborts before it lands rather than
        leaving a phantom EXCLUSIVE the agent never saw (and silently
        invalidating its peers)."""
        with self.registry.abort_guard(abort):
            return self._write_impl(
                agent_id=agent_id, artifact_id=artifact_id, issued_at_tick=issued_at_tick
            )

    def _write_impl(
        self,
        *,
        agent_id: UUID,
        artifact_id: UUID,
        issued_at_tick: int = 0,
    ) -> list[InvalidationSignal]:
        artifact = self._require_artifact(artifact_id)
        self.registry.set_agent_transient(
            artifact_id,
            agent_id,
            TransientState.IED,
            entered_tick=issued_at_tick,
        )
        signals: list[InvalidationSignal] = []
        for peer_id, state in self.registry.get_state_map(artifact_id).items():
            if peer_id == agent_id or state == MESIState.INVALID:
                continue
            transient = _invalidation_transient_for_state(state)
            if transient is not None:
                self.registry.set_agent_transient(
                    artifact_id,
                    peer_id,
                    transient,
                    entered_tick=issued_at_tick,
                )
            self.registry.set_agent_state(artifact_id, peer_id, MESIState.INVALID, trigger="write", tick=issued_at_tick)
            signals.append(
                InvalidationSignal(
                    artifact_id=artifact_id,
                    new_version=artifact.version,
                    issued_at_tick=issued_at_tick,
                    issuer_agent_id=agent_id,
                )
            )

        self.registry.set_agent_state(artifact_id, agent_id, MESIState.EXCLUSIVE, trigger="write", tick=issued_at_tick)
        self.registry.clear_agent_transient(artifact_id, agent_id)
        self._validate_single_writer(artifact_id)
        return signals

    def upgrade(
        self,
        *,
        agent_id: UUID,
        artifact_id: UUID,
        issued_at_tick: int = 0,
    ) -> list[InvalidationSignal]:
        """Upgrade a shared holder to exclusive owner (alias of write request)."""
        return self.write(agent_id=agent_id, artifact_id=artifact_id, issued_at_tick=issued_at_tick)

    def commit(
        self,
        *,
        agent_id: UUID,
        artifact_id: UUID,
        content: str,
        issued_at_tick: int = 0,
        content_hash: str | None = None,
        size_tokens: int | None = None,
        abort: threading.Event | None = None,
    ) -> tuple[Artifact, list[InvalidationSignal]]:
        """Commit modified content under the A6 abort guard (see _commit_impl)."""
        with self.registry.abort_guard(abort):
            return self._commit_impl(
                agent_id=agent_id,
                artifact_id=artifact_id,
                content=content,
                issued_at_tick=issued_at_tick,
                content_hash=content_hash,
                size_tokens=size_tokens,
            )

    def _commit_impl(
        self,
        *,
        agent_id: UUID,
        artifact_id: UUID,
        content: str,
        issued_at_tick: int = 0,
        content_hash: str | None = None,
        size_tokens: int | None = None,
    ) -> tuple[Artifact, list[InvalidationSignal]]:
        """Commit modified content, increment version, and invalidate peers.

        Raises:
            CoherenceError: the committer does not hold M/E (e.g. its grant was
                already reclaimed — the error names the reclaim trigger/tick).
            StaleReadGeneration: the read-generation fence fired — a sweep
                reclaimed this committer in the race window between the state
                check above and the version persist (``ccs.core.exceptions``).
                Retry-eligible: ``reacquire()`` / re-acquire, take a fresh read,
                and re-commit; there is no built-in retry loop on this path
                (unlike ``write_cas``).
        """
        artifact = self._require_artifact(artifact_id)
        agent_state = self.registry.get_agent_state(artifact_id, agent_id)
        if agent_state not in {MESIState.EXCLUSIVE, MESIState.MODIFIED}:
            reclamation = self.registry.get_last_reclamation(agent_id, artifact_id)
            if reclamation is not None:
                trigger, reclaimed_at_tick = reclamation
                raise CoherenceError(
                    f"commit_not_allowed agent={agent_id} artifact={artifact_id} "
                    f"state={agent_state} reclaimed_by={trigger} at_tick={reclaimed_at_tick}"
                )
            raise CoherenceError(
                f"commit_not_allowed agent={agent_id} artifact={artifact_id} state={agent_state}"
            )

        self.registry.set_agent_transient(
            artifact_id,
            agent_id,
            TransientState.MWB,
            entered_tick=issued_at_tick,
        )
        next_version = artifact.version + 1
        check_monotonic_version(artifact.version, next_version)
        updated = Artifact(
            id=artifact.id,
            name=artifact.name,
            version=next_version,
            content_hash=content_hash if content_hash is not None else artifact.content_hash,
            size_tokens=size_tokens if size_tokens is not None else artifact.size_tokens,
            depends_on=artifact.depends_on,
        )
        try:
            self.registry.set_artifact_and_content(
                artifact_id,
                updated,
                content,
                last_writer=agent_id,
                # Read-generation fence: reject atomically with the version bump
                # if a sweep reclaimed this committer in the race window between
                # the get_agent_state check above and here.
                fence_agent_id=agent_id,
            )
        except StaleReadGeneration:
            # The MWB transient set above must not outlive a fence reject:
            # a stuck MWB blocks this agent's next commit_cas (transient
            # precondition) and makes the stable-grant sweep skip the pair
            # until the transient timeout. The reclaim already dropped the
            # grant, so clearing the transient is the only cleanup needed.
            self.registry.clear_agent_transient(artifact_id, agent_id)
            raise

        signals: list[InvalidationSignal] = []
        for peer_id, state in self.registry.get_state_map(artifact_id).items():
            if peer_id == agent_id or state == MESIState.INVALID:
                continue
            transient = _invalidation_transient_for_state(state)
            if transient is not None:
                self.registry.set_agent_transient(
                    artifact_id,
                    peer_id,
                    transient,
                    entered_tick=issued_at_tick,
                )
            self.registry.set_agent_state(
                artifact_id, peer_id, MESIState.INVALID, trigger="commit", tick=issued_at_tick
            )
            signals.append(
                InvalidationSignal(
                    artifact_id=artifact_id,
                    new_version=next_version,
                    issued_at_tick=issued_at_tick,
                    issuer_agent_id=agent_id,
                )
            )
        commit_hash = content_hash if content_hash is not None else compute_content_hash(content)
        self.registry.set_agent_state(
            artifact_id, agent_id, MESIState.MODIFIED,
            trigger="commit", tick=issued_at_tick, content_hash=commit_hash,
        )
        self.registry.clear_agent_transient(artifact_id, agent_id)
        self._validate_single_writer(artifact_id)
        return updated, signals

    def commit_cas(
        self,
        *,
        agent_id: UUID,
        artifact_id: UUID,
        expected_version: int,
        content_hash: str,
        issued_at_tick: int = 0,
        size_tokens: int | None = None,
        content: bytes | str | None = None,
        abort: threading.Event | None = None,
    ) -> tuple[Artifact, list[InvalidationSignal]] | ConflictDetail:
        """Optimistic-concurrency commit under the A6 abort guard (see _commit_cas_impl)."""
        with self.registry.abort_guard(abort):
            return self._commit_cas_impl(
                agent_id=agent_id,
                artifact_id=artifact_id,
                expected_version=expected_version,
                content_hash=content_hash,
                issued_at_tick=issued_at_tick,
                size_tokens=size_tokens,
                content=content,
            )

    def _commit_cas_impl(
        self,
        *,
        agent_id: UUID,
        artifact_id: UUID,
        expected_version: int,
        content_hash: str,
        issued_at_tick: int = 0,
        size_tokens: int | None = None,
        content: bytes | str | None = None,
    ) -> tuple[Artifact, list[InvalidationSignal]] | ConflictDetail:
        """Optimistic-concurrency commit via an atomic version-checked CAS.

        The OCC counterpart to :meth:`commit` (plan Unit 3, R1–R4/R6). Unlike
        ``commit`` — which requires the caller to already hold EXCLUSIVE/MODIFIED
        from a pessimistic ``write()`` acquire — ``commit_cas`` lets a SHARED (or
        INVALID) caller commit *only if* ``expected_version`` still matches the
        registry's current version and no *pessimistic* peer holds M/E. The
        winner is elected by the registry's serialized ``BEGIN IMMEDIATE``, not a
        lock on the acquire, so two concurrent OCC writers cannot both land the
        same version.

        This method owns the **D4 precondition layer** the registry deliberately
        omits (the registry only does the version/holder CAS):

        - artifact must exist (``_require_artifact`` → ``CoherenceError``);
        - the caller must NOT be mid-transient (``get_agent_transient`` is
          ``None``, else ``CoherenceError``);
        - the caller's MESI state must be SHARED or INVALID — a MODIFIED/EXCLUSIVE
          holder is an *acquired* pessimistic writer and must use plain
          :meth:`commit` (rejected with a ``CoherenceError`` pointing there).

        Three-outcome discrimination of the registry result (plan R2):

        - :class:`CasCorruption` (``expected_version > current``) → raise
          ``CoherenceError`` — corruption / a second coordinator on the store,
          non-retryable.
        - :class:`ConflictDetail` (``version_mismatch`` / ``other_holder`` /
          ``stale_read_generation``) → returned UNCHANGED, with **no mutation
          and no invalidation signals** (it is a typed return, never an
          exception). ``stale_read_generation`` is the read-generation fence:
          the committer's captured claim was superseded by a sweep reclamation;
          retry-eligible via reacquire + fresh read.
        - WIN ``(updated_artifact, invalidated_ids)`` → the registry has already
          done the peer-invalidation + committer S/I→SHARED transition
          atomically (the OCC writer holds no grant, so it ends SHARED — which
          keeps a subsequent commit_cas by the same caller eligible past the D4
          precondition below); this method only builds the matching
          :class:`InvalidationSignal` list (mirroring ``commit``'s shape),
          re-validates single-writer, and returns ``(updated, signals)``.

        ``content`` is the winning body, threaded to the registry so the
        in-memory (library) path advances ``record.content`` on a WIN — a peer
        re-fetch then reads the winner's NEW content, not the stale pre-CAS body.
        The cross-process / sqlite path passes ``None`` (it stores no content).

        Returns:
            ``(updated_artifact, signals)`` on a winning commit, or a
            :class:`ConflictDetail` on a retry-eligible conflict.

        Raises:
            OccCallerTransientError: the caller is mid-transient (a peer
                invalidated it between read and CAS) — a retry-eligible subclass
                of ``CoherenceError`` carrying the stable wire reason
                :data:`~ccs.core.exceptions.OCC_CALLER_TRANSIENT_REASON`.
            CoherenceError: artifact missing, caller in M/E (use ``commit``), or
                the registry reported corruption (all non-retryable).
        """
        artifact = self._require_artifact(artifact_id)

        if self.registry.get_agent_transient(artifact_id, agent_id) is not None:
            # Retry-eligible: a peer invalidated the caller between its read and
            # this CAS, leaving an invalidation transient. Typed so the wire
            # reason stays stable independent of this human message (AC2).
            raise OccCallerTransientError(
                f"commit_cas_not_allowed agent={agent_id} artifact={artifact_id} "
                f"reason={OCC_CALLER_TRANSIENT_REASON}"
            )

        agent_state = self.registry.get_agent_state(artifact_id, agent_id)
        if agent_state in {MESIState.EXCLUSIVE, MESIState.MODIFIED}:
            raise CoherenceError(
                f"commit_cas_not_allowed agent={agent_id} artifact={artifact_id} "
                f"state={agent_state} reason=occ_is_shared_or_invalid_only "
                f"(use commit() for an EXCLUSIVE/MODIFIED holder)"
            )

        result = self.registry.commit_cas(
            artifact_id,
            agent_id,
            expected_version=expected_version,
            content_hash=content_hash,
            size_tokens=size_tokens,
            content=content,
            tick=issued_at_tick,
        )

        if isinstance(result, CasCorruption):
            raise CoherenceError(
                f"commit_cas_corruption agent={agent_id} artifact={artifact_id} "
                f"expected_version={expected_version} "
                f"current_version={result.current_version} "
                f"(expected > current — corruption or multi-coordinator violation)"
            )
        if isinstance(result, ConflictDetail):
            # Typed retry-eligible conflict: no mutation happened in the
            # registry, so emit no invalidation signals and surface it as-is.
            return result

        updated, invalidated_ids = result
        # Defense-in-depth: the CAS computed N+1 atomically; assert it did not
        # regress (NOT the concurrency guard — that was the version check).
        check_monotonic_version(artifact.version, updated.version)
        signals = [
            InvalidationSignal(
                artifact_id=artifact_id,
                new_version=updated.version,
                issued_at_tick=issued_at_tick,
                issuer_agent_id=agent_id,
            )
            for _ in invalidated_ids
        ]
        self._validate_single_writer(artifact_id)
        return updated, signals

    def invalidate(
        self,
        *,
        agent_id: UUID,
        artifact_id: UUID,
        new_version: int,
        issuer_agent_id: UUID,
        issued_at_tick: int,
        abort: threading.Event | None = None,
    ) -> InvalidationSignal | None:
        """Apply invalidation for one agent under the A6 abort guard.

        Wrapped in :meth:`registry.abort_guard` (finding A6): a late
        session-stop release whose handler already timed out aborts here rather
        than revoking a grant the registry has since handed to another session.
        """
        with self.registry.abort_guard(abort):
            return self._invalidate_impl(
                agent_id=agent_id,
                artifact_id=artifact_id,
                new_version=new_version,
                issuer_agent_id=issuer_agent_id,
                issued_at_tick=issued_at_tick,
            )

    def _invalidate_impl(
        self,
        *,
        agent_id: UUID,
        artifact_id: UUID,
        new_version: int,
        issuer_agent_id: UUID,
        issued_at_tick: int,
    ) -> InvalidationSignal | None:
        if not self.registry.has_artifact(artifact_id):
            return None
        self.registry.set_agent_state(
            artifact_id, agent_id, MESIState.INVALID, trigger="invalidate", tick=issued_at_tick
        )
        self.registry.clear_agent_transient(artifact_id, agent_id)
        return InvalidationSignal(
            artifact_id=artifact_id,
            new_version=new_version,
            issued_at_tick=issued_at_tick,
            issuer_agent_id=issuer_agent_id,
        )

    def delete(
        self,
        *,
        agent_id: UUID,
        artifact_id: UUID,
        issued_at_tick: int = 0,
    ) -> list[InvalidationSignal]:
        """Remove artifact and emit invalidation signals to all non-INVALID holders.

        Does not require the caller to hold EXCLUSIVE or MODIFIED state first.
        Returns [] when the artifact is absent (silent no-op for the caller).
        """
        if not self.registry.has_artifact(artifact_id):
            return []
        artifact = self._require_artifact(artifact_id)
        signals = [
            InvalidationSignal(
                artifact_id=artifact_id,
                new_version=artifact.version,
                issued_at_tick=issued_at_tick,
                issuer_agent_id=agent_id,
            )
            for holder_id, state in self.registry.get_state_map(artifact_id).items()
            if state != MESIState.INVALID
        ]
        self.registry.remove_artifact(artifact_id)
        return signals

    def record_heartbeat(self, *, agent_id: UUID, now_tick: int) -> None:
        """Record an agent's heartbeat tick (R12: max(prev, incoming))."""
        if now_tick < 0:
            raise ValueError("now_tick must be >= 0")
        self.registry.record_heartbeat(agent_id, now_tick)

    def enforce_transient_timeouts(self, *, current_tick: int, timeout_ticks: int) -> int:
        """Force expired transient entries to INVALID as fail-safe recovery."""
        if timeout_ticks < 1:
            raise ValueError("timeout_ticks must be >= 1")

        expired = 0
        for artifact_id in self.registry.artifact_ids():
            for agent_id, transient in self.registry.get_transient_map(artifact_id).items():
                entered = self.registry.get_transient_tick(artifact_id, agent_id)
                if entered is None:
                    continue
                if (current_tick - entered) < timeout_ticks:
                    continue

                # Conservative fail-safe: transient timeout always forces local invalidation.
                self.registry.set_agent_state(
                    artifact_id, agent_id, MESIState.INVALID, trigger="timeout", tick=current_tick
                )
                self.registry.clear_agent_transient(artifact_id, agent_id)
                expired += 1

        return expired

    def enforce_stable_grant_timeouts(
        self,
        *,
        current_tick: int,
        heartbeat_timeout_ticks: int,
        max_hold_ticks: int,
        on_reclaim: Optional[Callable[[UUID, UUID, str], None]] = None,
    ) -> int:
        """Reclaim stale stable-state (M∪E) grants whose holders are gone or over-held.

        Trigger order (first match wins):
          1. ``reclaim_heartbeat`` — agent's last heartbeat is older than
             ``heartbeat_timeout_ticks``, or the agent has never heartbeated.
          2. ``reclaim_max_hold`` — agent's grant is at least ``max_hold_ticks``
             old. Skipped if ``granted_at_tick`` is missing (defensive).

        Pairs with a non-empty transient slot are skipped so the transient sweep
        (which must run first) owns those entries — preserves R4 sweep ordering.

        ADV-004: ``on_reclaim`` is a per-reclamation callback the adapter
        uses to record a preemption notice for the victim agent — so when
        the victim's post-edit later arrives and fails CoherenceError, the
        F4 enrichment path can pop the notice and emit a "reclaimed by
        sweep" message rather than a generic error with no context.
        Library code remains preemption-notice-agnostic; the registry
        method is adapter-only (SqliteArtifactRegistry).

        Returns the number of grants reclaimed.
        """
        if heartbeat_timeout_ticks < 1:
            raise ValueError("heartbeat_timeout_ticks must be >= 1")
        if max_hold_ticks < 1:
            raise ValueError("max_hold_ticks must be >= 1")

        m_or_e = {MESIState.MODIFIED, MESIState.EXCLUSIVE}
        snapshot: list[tuple[UUID, UUID, MESIState]] = [
            (artifact_id, agent_id, mesi)
            for artifact_id in self.registry.artifact_ids()
            for agent_id, mesi in self.registry.get_state_map(artifact_id).items()
            if mesi in m_or_e
        ]

        reclaimed = 0
        for artifact_id, agent_id, _mesi in snapshot:
            # Live read — agents that entered transient since the snapshot are owned by
            # the transient sweep, not this one (R4).
            if self.registry.get_agent_transient(artifact_id, agent_id) is not None:
                continue

            last_hb = self.registry.last_heartbeat_tick(agent_id)
            # Heartbeat uses `>=` to match max-hold's `>=` (review fix ADV-02).
            # An effective timeout of exactly heartbeat_timeout_ticks means a
            # grant is reclaimed when (current_tick - last_hb) reaches the
            # threshold, not the tick after. Matches the 'at least this many
            # ticks since last heartbeat' framing in CrashRecoveryConfig docs.
            heartbeat_stale = last_hb is None or (current_tick - last_hb) >= heartbeat_timeout_ticks

            if heartbeat_stale:
                trigger = "reclaim_heartbeat"
            else:
                granted_at = self.registry.granted_at_tick(agent_id, artifact_id)
                if granted_at is None:
                    # M∪E holder without granted_at — should not exist; skip to avoid
                    # blocking the sweep but log so operators can investigate.
                    logger.warning(
                        "sweep: M/E holder has no granted_at slot; skipping max-hold check "
                        "agent=%s artifact=%s",
                        agent_id, artifact_id,
                    )
                    continue
                if (current_tick - granted_at) >= max_hold_ticks:
                    trigger = "reclaim_max_hold"
                else:
                    continue

            self.registry.set_agent_state(
                artifact_id,
                agent_id,
                MESIState.INVALID,
                trigger=trigger,
                tick=current_tick,
                content_hash=None,
            )
            self.registry.record_last_reclamation(agent_id, artifact_id, trigger, current_tick)
            self._validate_single_writer(artifact_id)
            if on_reclaim is not None:
                # Best-effort: a notice-recording failure must not stop the sweep
                # (the reclamation itself already landed in the registry).
                try:
                    on_reclaim(artifact_id, agent_id, trigger)
                except Exception:  # noqa: BLE001 — telemetry surface, best-effort
                    logger.exception(
                        "on_reclaim callback raised for agent=%s artifact=%s trigger=%s",
                        agent_id, artifact_id, trigger,
                    )
            reclaimed += 1

        return reclaimed

    def _validate_single_writer(self, artifact_id: UUID) -> None:
        check_single_writer(self.registry.get_state_map(artifact_id))

    def _require_artifact(self, artifact_id: UUID) -> Artifact:
        artifact = self.registry.get_artifact(artifact_id)
        if artifact is None:
            raise CoherenceError(f"artifact_not_found artifact={artifact_id}")
        return artifact


def _invalidation_transient_for_state(state: MESIState) -> TransientState | None:
    if state == MESIState.SHARED:
        return TransientState.SIA
    if state == MESIState.EXCLUSIVE:
        return TransientState.EIA
    if state == MESIState.MODIFIED:
        return TransientState.MSA
    return None
