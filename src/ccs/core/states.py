# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""MESI stable/transient state definitions and transition utilities."""

from __future__ import annotations

from enum import Enum

from .exceptions import InvalidTransitionError


class MESIState(Enum):
    """Stable MESI states for artifact coherence."""

    MODIFIED = "M"
    EXCLUSIVE = "E"
    SHARED = "S"
    INVALID = "I"


class TransientState(Enum):
    """Transient states representing in-flight synchronization work."""

    ISG = "ISG"  # Invalid -> Shared, waiting fetch grant
    IED = "IED"  # Invalid -> Exclusive, waiting exclusive grant
    EIA = "EIA"  # Exclusive -> Invalid, waiting acknowledgment
    SIA = "SIA"  # Shared -> Invalid, waiting acknowledgment
    MWB = "MWB"  # Modified -> write-back in progress
    MSA = "MSA"  # Modified -> Shared, write-back then shared


VALID_TRANSITIONS: set[tuple[MESIState, MESIState]] = {
    (MESIState.INVALID, MESIState.SHARED),
    (MESIState.INVALID, MESIState.EXCLUSIVE),
    (MESIState.SHARED, MESIState.INVALID),
    (MESIState.SHARED, MESIState.EXCLUSIVE),
    (MESIState.EXCLUSIVE, MESIState.SHARED),
    (MESIState.EXCLUSIVE, MESIState.MODIFIED),
    (MESIState.EXCLUSIVE, MESIState.INVALID),
    (MESIState.MODIFIED, MESIState.INVALID),
    (MESIState.MODIFIED, MESIState.SHARED),
}


def is_valid_transition(current_state: MESIState, next_state: MESIState) -> bool:
    """Return whether a stable-state transition is allowed."""
    return (current_state, next_state) in VALID_TRANSITIONS


def transition_state(current_state: MESIState, next_state: MESIState) -> MESIState:
    """Validate and perform a stable MESI transition."""
    if not is_valid_transition(current_state, next_state):
        raise InvalidTransitionError(current_state.value, next_state.value, "transition_state")
    return next_state


def can_act_in_transient(
    transient_state: TransientState,
    strategy_name: str,
    is_write: bool,
    *,
    lease_valid: bool = True,
    accesses_remaining: bool = True,
) -> bool:
    """Return whether an action is permitted while the entry is transient."""
    if transient_state in {TransientState.ISG, TransientState.IED}:
        return False

    if transient_state in {TransientState.EIA, TransientState.SIA}:
        if strategy_name == "eager":
            return False
        if strategy_name == "lazy":
            return True
        if strategy_name == "lease":
            return lease_valid
        if strategy_name == "access_count":
            return accesses_remaining
        # Non-enumerated strategies -- ``broadcast`` and the benchmark-internal
        # ``blind`` cost floor -- intentionally do not act mid-acquisition. For
        # ``blind`` this is verified not to contaminate the cost-floor measurement:
        # it still performs exactly num_agents x num_artifacts fetches regardless.
        return False

    if transient_state in {TransientState.MWB, TransientState.MSA}:
        return not is_write

    return False

