# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Unit tests for invariant checks."""

from __future__ import annotations

import pytest

from ccs.core.exceptions import InvariantViolationError
from ccs.core.invariants import check_bounded_staleness, check_monotonic_version, check_single_writer
from ccs.core.states import MESIState


def test_single_writer_passes_for_one_owner() -> None:
    check_single_writer({"a": MESIState.MODIFIED, "b": MESIState.INVALID, "c": MESIState.SHARED})


def test_single_writer_fails_for_two_owners() -> None:
    with pytest.raises(InvariantViolationError):
        check_single_writer({"a": MESIState.MODIFIED, "b": MESIState.EXCLUSIVE})


def test_monotonic_version_check() -> None:
    check_monotonic_version(3, 3)
    check_monotonic_version(3, 4)
    with pytest.raises(InvariantViolationError):
        check_monotonic_version(3, 2)


def test_bounded_staleness_check() -> None:
    check_bounded_staleness(2, 2)
    with pytest.raises(InvariantViolationError):
        check_bounded_staleness(3, 2)

