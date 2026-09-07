# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Session-handoff demo: the ``Session`` process primitive.

Each ``Session`` drives a real spawned OS process holding one ``CoherentVolume``
over a shared workspace; the first session created spawns the coordinator and
later ones attach. These tests pin the channel contract the demo's acts build
on: a coordinator deny crosses the process boundary as a typed
``SessionDenied``, raw file I/O bypasses the guard entirely (the negative
control), and the two sessions really are distinct processes.
"""

from __future__ import annotations

import os
import shutil
import tempfile
from pathlib import Path

import pytest

from examples.session_handoff import A_STATUS_1, A_STATUS_2, B_PICKUP, GUARDED, NOTES
from examples.session_handoff.sessions import Session, SessionDenied


@pytest.fixture
def workspace() -> Path:
    root = Path(tempfile.mkdtemp(prefix="session_handoff_test_"))
    (root / NOTES).parent.mkdir(parents=True, exist_ok=True)
    try:
        yield root
    finally:
        shutil.rmtree(root, ignore_errors=True)


def test_session_write_denial_surfaces_as_session_denied(workspace: Path) -> None:
    a = Session(workspace, "A", GUARDED)
    try:
        b = Session(workspace, "B", GUARDED)
        try:
            a.write(A_STATUS_1)
            assert b.read() == A_STATUS_1  # B now holds a SHARED view
            a.write(A_STATUS_2)  # A's second commit invalidates B

            with pytest.raises(SessionDenied) as denied:
                b.write(B_PICKUP)  # B writes from its stale view -> denied
            assert denied.value.exc_name == "StaleView"
            assert denied.value.message  # the coordinator's reason travels verbatim
            assert (workspace / NOTES).read_bytes() == A_STATUS_2  # nothing clobbered

            assert b.reacquire() == A_STATUS_2  # fresh mandatory read
            b.write(B_PICKUP)  # recovered write lands
            assert (workspace / NOTES).read_bytes() == B_PICKUP
        finally:
            b.close()
    finally:
        a.close()


def test_session_raw_io_bypasses_the_guard(workspace: Path) -> None:
    a = Session(workspace, "A", GUARDED)
    try:
        b = Session(workspace, "B", GUARDED)
        try:
            a.raw_write(A_STATUS_1)
            assert b.raw_read() == A_STATUS_1
            a.raw_write(A_STATUS_2)

            b.raw_write(B_PICKUP)  # no exception: the stale write silently wins

            assert (workspace / NOTES).read_bytes() == B_PICKUP
        finally:
            b.close()
    finally:
        a.close()


def test_two_sessions_are_distinct_processes(workspace: Path) -> None:
    with Session(workspace, "A", GUARDED) as a, Session(workspace, "B", GUARDED) as b:
        assert a.pid != b.pid
        assert os.getpid() not in {a.pid, b.pid}


# --- the three acts: RED, GREEN, CONTROL -------------------------------------------------


def test_red_loses_the_pickup_line() -> None:
    from examples.session_handoff.broken import run_broken

    result = run_broken()

    assert result["act"] == "red"
    assert result["raised"] is None  # plain file I/O: nothing complained
    assert result["final"] == A_STATUS_2  # B's pickup line is gone
    assert result["expected"] == A_STATUS_2 + B_PICKUP
    assert result["b_line_present"] is False
    assert result["lost"] is True
    assert result["trace"] and all(isinstance(line, str) for line in result["trace"])


def test_green_denies_then_both_lines_survive() -> None:
    from examples.session_handoff.fixed import run_guarded

    result = run_guarded()

    assert result["act"] == "green"
    assert result["denied"] is True
    assert result["denial_exc"] == "StaleView"
    assert result["denial_message"]  # the coordinator's reason travels verbatim
    assert result["recovered"] is True
    assert result["final"] == A_STATUS_2 + B_PICKUP  # EXACT bytes: both lines survive
    assert result["expected"] == A_STATUS_2 + B_PICKUP
    assert result["b_line_present"] is True
    assert result["lost"] is False
    assert result["trace"] and all(isinstance(line, str) for line in result["trace"])


def test_control_with_guard_off_loses_again() -> None:
    from examples.session_handoff.fixed import run_control

    result = run_control()

    assert result["act"] == "control"
    assert result["denied"] is False  # NOTES sits outside the strict glob: no deny
    assert result["denial_exc"] is None
    assert result["denial_message"] is None
    assert result["recovered"] is False
    assert result["final"] == A_STATUS_2  # the loss returns
    assert result["b_line_present"] is False
    assert result["lost"] is True
    assert result["trace"] and all(isinstance(line, str) for line in result["trace"])


# --- the fourth act: HANDOFF (A stopped / A still working) -------------------------------


def test_handoff_stopped_rewinds_and_a_learns_on_next_write() -> None:
    from examples.session_handoff.handoff import run_handoff_stopped

    result = run_handoff_stopped()

    assert result["act"] == "handoff_stopped"
    assert isinstance(result["checkpoint_id"], str) and result["checkpoint_id"]
    assert len(result["members"]) == 1
    assert result["members"][0]["member_path"] == NOTES
    assert result["members"][0]["restore_tier"] == "restorable-unpinned"
    assert result["members"][0]["pin_state"] == "held"
    assert result["outcome"] == "restored"
    assert result["attempts"] == 1
    assert result["detail"]
    assert result["disk_after_restore"] == A_STATUS_1  # rewound to the handoff point
    assert result["a_denied"] is True  # A learns on its NEXT write, not at restore time
    assert result["a_denial_exc"] == "StaleView"
    assert result["a_denial_message"]
    assert result["a_reacquired"] == A_STATUS_1
    assert result["trace"] and all(isinstance(line, str) for line in result["trace"])
    assert result["checkpoint_id"] not in "\n".join(result["trace"])  # the uuid never enters the trace


def test_handoff_racing_concludes_conflict_not_clobber() -> None:
    from ccs.adapters.workspace import MAX_RESTORE_LEG_REDRIVES
    from examples.session_handoff.handoff import RACING_ATTEMPTS, run_handoff_racing, still_working_bytes

    result = run_handoff_racing()

    # The leg budget admits the initial attempt plus every re-drive, then concludes.
    assert RACING_ATTEMPTS == MAX_RESTORE_LEG_REDRIVES + 1
    assert result["act"] == "handoff_racing"
    assert isinstance(result["checkpoint_id"], str) and result["checkpoint_id"]
    assert len(result["members"]) == 1 and result["members"][0]["member_path"] == NOTES
    assert result["outcome"] == "conflict"
    assert result["attempts"] == RACING_ATTEMPTS
    assert "re-drive budget exhausted" in result["detail"]
    # One racing edit per admitted attempt, so the last edit index equals the attempt count.
    assert still_working_bytes(RACING_ATTEMPTS) == f"status: still working, edit {RACING_ATTEMPTS}\n".encode()
    assert result["disk_after_restore"] == still_working_bytes(RACING_ATTEMPTS)
    assert result["restore_landed"] is False  # A's work survives; nothing was clobbered
    assert result["trace"] and all(isinstance(line, str) for line in result["trace"])
    assert result["checkpoint_id"] not in "\n".join(result["trace"])


# --- the runner: all five acts under one exit code -----------------------------------------


def test_session_handoff_demo_exits_zero() -> None:
    from examples.session_handoff.main import main

    assert main([]) == 0
