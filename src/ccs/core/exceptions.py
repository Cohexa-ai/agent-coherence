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


class InvalidTransitionError(CoherenceError):
    """Raised when the MESI transition table rejects a state transition."""

    def __init__(self, from_state: str, to_state: str, trigger: str):
        super().__init__(f"invalid_transition from={from_state} to={to_state} trigger={trigger}")
        self.from_state = from_state
        self.to_state = to_state
        self.trigger = trigger


class InvariantViolationError(CoherenceError):
    """Raised when a runtime invariant check fails."""


class ScenarioValidationError(CoherenceError):
    """Raised when scenario configuration does not match schema expectations."""

    def __init__(self, path: str, message: str):
        super().__init__(f"scenario={path}: {message}")
        self.path = path
