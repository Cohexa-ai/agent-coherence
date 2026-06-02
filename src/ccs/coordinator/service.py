# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""Coordinator service implementing core artifact coherence operations."""

from __future__ import annotations

import logging
import threading
import warnings
from dataclasses import dataclass, field
from typing import Callable, Optional
from uuid import UUID

from ccs.core.exceptions import CoherenceError
from ccs.core.hashing import compute_content_hash
from ccs.core.invariants import check_monotonic_version, check_single_writer
from ccs.core.states import MESIState, TransientState
from ccs.core.types import Artifact, FetchRequest, FetchResponse, InvalidationSignal

from .registry import ArtifactRegistry

logger = logging.getLogger(__name__)


# v0.8.3 deprecation cycle — see docs/plans/2026-05-28-001-feat-c-flip-...
# A module-level sentinel distinguishes bare ``CrashRecoveryConfig()`` from
# explicit ``CrashRecoveryConfig(enabled=False)``. The sentinel, the
# emit-once flag, the lock, and the message constant are all removed in
# v0.9.0 when the default flips to ``enabled=True``.


class _DefaultEnabledSentinel:
    """Falsy marker for an unspecified ``enabled`` in ``CrashRecoveryConfig``.

    ``__post_init__`` uses identity (``is``) to detect bare construction and
    normalize ``enabled`` to ``False``. The sentinel is deliberately *falsy*
    so that on the rare paths where normalization is skipped, the value still
    reads as disabled — matching the v0.8.x default and keeping the crash
    recovery sweep from silently activating:

    * ``importlib.reload`` (gunicorn/uvicorn ``--reload``, Jupyter autoreload)
      re-executes this module in place, rebinding ``_DEFAULT_ENABLED_SENTINEL``
      to a fresh instance. An instance of the *pre-reload* class still carries
      the old sentinel as its field default, so the ``is`` check fails and
      normalization is skipped (ce:review ADV-01).
    * A subclass whose ``__post_init__`` overrides ours without calling
      ``super().__post_init__()`` never normalizes at all (ce:review ADV-03).

    In both cases ``bool(enabled)`` is ``False``, so production checks of the
    form ``if self._crash_recovery.enabled:`` stay correct. Removed in v0.9.0.
    """

    __slots__ = ()

    def __bool__(self) -> bool:
        return False

    def __repr__(self) -> str:
        return "<CrashRecoveryConfig.enabled unset>"


# Test-isolation contract: ``_BARE_CONSTRUCTION_WARNED`` is module-level
# mutable state that persists across pytest test functions in the same
# process. Tests that assert on warning emission MUST reset it to ``False``
# before the bare construction (either directly via
# ``service._BARE_CONSTRUCTION_WARNED = False`` or via the
# ``reset_bare_construction_flag`` pytest fixture in
# ``tests/test_coordinator.py``). Without the reset, test ordering
# determines whether the first test sees the warning and later tests do
# not — a silent-degrade failure mode under pytest-xdist or randomized
# ordering. The reset is paired with the flag's lifecycle by intent: this
# whole block is removed in v0.9.0 along with the test fixture.
#
# ``_BARE_CONSTRUCTION_LOCK`` makes the check-then-set atomic so two threads
# constructing bare configs concurrently cannot both emit. The GIL made the
# naive check incidentally safe; free-threaded Python 3.13+ removes it. This
# mirrors the ``threading.Lock`` pattern the plan's KD-4 specifies for the
# v0.9.0 first-use guard (ce:review ADV-02).
_DEFAULT_ENABLED_SENTINEL = _DefaultEnabledSentinel()
_BARE_CONSTRUCTION_WARNED: bool = False
_BARE_CONSTRUCTION_LOCK = threading.Lock()

# Emitted on BOTH the warnings channel (visible under ``-W`` / pytest) and the
# logging channel (visible under any ``logging.basicConfig()`` — the common
# case CPython's default ``DeprecationWarning`` filter hides for non-__main__
# importers). These are two of the three RM-9 belt-and-suspenders layers; the
# third is the v0.9.0 transitional first-use warning. Removed in v0.9.0.
_BARE_CONSTRUCTION_MESSAGE = (
    "CrashRecoveryConfig() default will flip to `enabled=True` in v0.9.0. "
    "Recommended migration: pass CrashRecoveryConfig(enabled=True) to opt in "
    "now and surface any false-reclaim issues under your workload. If you "
    "have a specific reason to keep crash recovery off, pass "
    "CrashRecoveryConfig(enabled=False) explicitly. See CHANGELOG.md "
    "(section: [0.8.3]) at "
    "https://github.com/hipvlady/agent-coherence/blob/main/CHANGELOG.md "
    "for migration details."
)


@dataclass(frozen=True)
class CrashRecoveryConfig:
    """Configuration knobs for the stable-grant reclamation sweep.

    The sweep ships disabled by default in v0.8.x (R10). The default
    will flip to ``enabled=True`` in v0.9.0; bare ``CrashRecoveryConfig()``
    in v0.8.3 emits a ``DeprecationWarning`` once per process to warn
    downstream consumers ahead of the flip.

    Attributes:
        enabled: Master flag. When ``False`` (v0.8.x default), the sweep
            is never invoked and no heartbeat is required.
        heartbeat_timeout_ticks: Sweep reclaims any M∪E grant whose holder
            has not heartbeated within this many ticks.
        max_hold_ticks: Sweep reclaims any M∪E grant held for at least this
            many ticks regardless of heartbeat. Must be ``>`` the longest
            inspectable strategy lease TTL when ``enabled=True`` (R11).
    """

    # field(default=_DEFAULT_ENABLED_SENTINEL) lets __post_init__ distinguish
    # bare construction from explicit False; the type-ignore is necessary
    # because the runtime sentinel is not a bool but the public type is.
    enabled: bool = field(default=_DEFAULT_ENABLED_SENTINEL)  # type: ignore[assignment]
    heartbeat_timeout_ticks: int = 10
    max_hold_ticks: int = 1000

    def __post_init__(self) -> None:
        """Detect bare construction; emit the one-shot deprecation signal.

        The dataclass is ``frozen=True``, so we normalize the sentinel-typed
        ``enabled`` field via ``object.__setattr__`` (direct assignment would
        raise ``FrozenInstanceError``).
        """
        global _BARE_CONSTRUCTION_WARNED
        if self.enabled is _DEFAULT_ENABLED_SENTINEL:
            # Claim the one-shot emission atomically under the lock, then emit
            # OUTSIDE it: warning filters and logging handlers can run arbitrary
            # user code, and we never hold the lock across that (ADV-02).
            should_emit = False
            with _BARE_CONSTRUCTION_LOCK:
                if not _BARE_CONSTRUCTION_WARNED:
                    _BARE_CONSTRUCTION_WARNED = True
                    should_emit = True
            if should_emit:
                # logging first, so the migration signal is recorded even when
                # ``-W error`` escalates the DeprecationWarning into a raise
                # (and so it survives CPython's default filter — RM-9 Layer 2).
                logger.warning(_BARE_CONSTRUCTION_MESSAGE)
                # stacklevel=3: warn() -> __post_init__ -> __init__ -> caller.
                warnings.warn(
                    _BARE_CONSTRUCTION_MESSAGE, DeprecationWarning, stacklevel=3
                )
            # frozen dataclass — direct assignment raises FrozenInstanceError.
            # object.__setattr__ is the documented escape for __post_init__
            # normalization on frozen dataclasses.
            object.__setattr__(self, "enabled", False)


def _default_disabled_config() -> CrashRecoveryConfig:
    """Internal helper: construct ``CrashRecoveryConfig(enabled=False)``
    without triggering the v0.8.3 bare-construction ``DeprecationWarning``.

    Used by library-internal code paths (``simulation.engine``,
    ``adapters.base``) that need the v0.8.x default-disabled config object
    but should not surface the deprecation warning to end users — the bare
    construction is the library's own, not the user's, so it would be a
    false alarm.

    Removed in v0.9.0 when the default flips to ``enabled=True`` and the
    sentinel mechanism is unwound.
    """
    return CrashRecoveryConfig(enabled=False)


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


class CoordinatorService:
    """Control-plane service for artifact read/write/commit synchronization."""

    def __init__(self, registry: ArtifactRegistry):
        self.registry = registry

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

    def write(
        self,
        *,
        agent_id: UUID,
        artifact_id: UUID,
        issued_at_tick: int = 0,
    ) -> list[InvalidationSignal]:
        """Request write ownership by invalidating peers and granting EXCLUSIVE."""
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
    ) -> tuple[Artifact, list[InvalidationSignal]]:
        """Commit modified content, increment version, and invalidate peers."""
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
        self.registry.set_artifact_and_content(
            artifact_id,
            updated,
            content,
            last_writer=agent_id,
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

    def invalidate(
        self,
        *,
        agent_id: UUID,
        artifact_id: UUID,
        new_version: int,
        issuer_agent_id: UUID,
        issued_at_tick: int,
    ) -> InvalidationSignal | None:
        """Apply invalidation for one agent and return corresponding signal object.

        Returns None when the artifact has already been deleted — callers applying
        a delete-tombstone invalidation must not crash on a missing artifact.
        """
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
