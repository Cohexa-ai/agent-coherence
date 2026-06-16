# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Unit tests for MESI states and transition helpers."""

from __future__ import annotations

import pytest

from ccs.core.exceptions import InvalidTransitionError
from ccs.core.states import (
    MESIState,
    TransientState,
    can_act_in_transient,
    is_valid_transition,
    transition_state,
)


@pytest.mark.parametrize(
    ("from_state", "to_state"),
    [
        (MESIState.INVALID, MESIState.SHARED),
        (MESIState.INVALID, MESIState.EXCLUSIVE),
        (MESIState.SHARED, MESIState.INVALID),
        (MESIState.SHARED, MESIState.EXCLUSIVE),
        (MESIState.EXCLUSIVE, MESIState.SHARED),
        (MESIState.EXCLUSIVE, MESIState.MODIFIED),
        (MESIState.EXCLUSIVE, MESIState.INVALID),
        (MESIState.MODIFIED, MESIState.INVALID),
        (MESIState.MODIFIED, MESIState.SHARED),
    ],
)
def test_valid_transitions(from_state: MESIState, to_state: MESIState) -> None:
    assert is_valid_transition(from_state, to_state) is True


@pytest.mark.parametrize(
    ("from_state", "to_state"),
    [
        (MESIState.INVALID, MESIState.MODIFIED),
        (MESIState.SHARED, MESIState.MODIFIED),
        (MESIState.MODIFIED, MESIState.EXCLUSIVE),
        (MESIState.INVALID, MESIState.INVALID),
    ],
)
def test_invalid_transitions(from_state: MESIState, to_state: MESIState) -> None:
    assert is_valid_transition(from_state, to_state) is False


def test_transition_state_raises_on_invalid_transition() -> None:
    with pytest.raises(InvalidTransitionError):
        transition_state(MESIState.INVALID, MESIState.MODIFIED)


@pytest.mark.parametrize(
    ("strategy_name", "lease_valid", "accesses_remaining", "expected"),
    [
        ("eager", True, True, False),
        ("lazy", True, True, True),
        ("lease", False, True, False),
        ("lease", True, True, True),
        ("access_count", True, False, False),
        ("access_count", True, True, True),
    ],
)
def test_transient_invalidation_action_gate(
    strategy_name: str, lease_valid: bool, accesses_remaining: bool, expected: bool
) -> None:
    assert (
        can_act_in_transient(
            TransientState.EIA,
            strategy_name,
            is_write=False,
            lease_valid=lease_valid,
            accesses_remaining=accesses_remaining,
        )
        is expected
    )

