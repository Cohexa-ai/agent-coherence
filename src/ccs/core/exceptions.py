# Copyright (c) 2026 Arbiter contributors.
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


class CoherenceError(Exception):
    """Base error for coherence domain failures."""


class StaleReadGeneration(CoherenceError):
    """The read-generation fence rejected a commit: the committer's
    read_generation is older than the artifact's current owner_generation --
    its captured ownership/read-claim was superseded by a sweep reclamation. Raised on the pessimistic ``commit()`` path; the OCC
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
    """

    def __init__(self, artifact_id: object, attempts: int, last_current_version: int) -> None:
        super().__init__(
            f"cas_retries_exhausted artifact={artifact_id} attempts={attempts} "
            f"last_current_version={last_current_version} "
            f"(no write landed — every commit_cas attempt lost the race)"
        )
        self.artifact_id = artifact_id
        self.attempts = attempts
        self.last_current_version = last_current_version


class ScenarioValidationError(CoherenceError):
    """Raised when scenario configuration does not match schema expectations."""

    def __init__(self, path: str, message: str):
        super().__init__(f"scenario={path}: {message}")
        self.path = path
