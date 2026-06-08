# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""Domain exception hierarchy for coherence protocol operations."""

from __future__ import annotations


class CoherenceError(Exception):
    """Base error for coherence domain failures."""


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
