# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""GREEN and CONTROL: the same handoff, routed through ``CoherentVolume``.

GREEN (``run_guarded``): both sessions manage ``handoff/**``. B's pickup write
invalidates A's view, so A's stale status write is DENIED (``StaleView``); A
``reacquire``s, sees B's line, and writes its updated status on top of it.
Both lines survive.

CONTROL (``run_control``): the identical code path with the strict glob pointed
elsewhere (``other/**``), so ``handoff/notes.md`` sits outside it and the deny
is off. A's stale write lands and B's line is lost again — GREEN depends on
the deny, not on the re-read.

Sequenced, not raced: deterministic by construction. Each session is a real OS
process (see ``sessions.py``): the first spawns the coordinator, the second
attaches to it.
"""

from __future__ import annotations

import shutil

from examples.session_handoff import A_STATUS_1, A_STATUS_2, B_PICKUP, GUARDED, NOTES, UNGUARDED
from examples.session_handoff.broken import EXPECTED, new_workspace, render_notes, verdict_line
from examples.session_handoff.sessions import Session, SessionDenied


def run_guarded() -> dict[str, object]:
    """GREEN: strict on ``handoff/**`` — A's stale write is denied, then recovered."""
    return _run_handoff("green", GUARDED)


def run_control() -> dict[str, object]:
    """CONTROL: strict glob elsewhere — the deny is off, so the loss returns."""
    return _run_handoff("control", UNGUARDED)


def _run_handoff(act: str, managed: tuple[str, ...]) -> dict[str, object]:
    """A posts status, B picks up, A writes from its earlier view; A recovers iff denied.

    Returns a structured trace mirroring ``run_broken`` for side-by-side asserts.
    """
    workspace = new_workspace(act)
    trace: list[str] = []
    a = Session(workspace, "A", managed)  # first session: spawns the coordinator
    try:
        b = Session(workspace, "B", managed)  # attaches to A's coordinator
        try:
            _post_status_and_pick_up(a, b, trace)
            denial = _write_stale_status(a, trace)
            recovered = denial is not None and _recover(a, trace)
            final = (workspace / NOTES).read_bytes()
            trace.append(verdict_line(final))
        finally:
            b.close()
    finally:
        a.close()
        shutil.rmtree(workspace, ignore_errors=True)
    return {
        "act": act,
        "final": final,
        "expected": EXPECTED,
        "b_line_present": B_PICKUP in final,
        "lost": final != EXPECTED,
        "denied": denial is not None,
        "denial_exc": denial.exc_name if denial else None,
        "denial_message": denial.message if denial else None,
        "recovered": recovered,
        "trace": trace,
    }


def _post_status_and_pick_up(a: Session, b: Session, trace: list[str]) -> None:
    """A posts its status; B reads it and appends its pickup line (A's view goes stale)."""
    a.write(A_STATUS_1)
    trace.append(f"A write     {render_notes(A_STATUS_1)}")
    seen = b.read()
    trace.append(f"B read      -> {render_notes(seen)}")
    b.write(seen + B_PICKUP)
    trace.append(f"B write     {render_notes(seen + B_PICKUP)}   (A's view of the notes is now stale)")


def _write_stale_status(a: Session, trace: list[str]) -> SessionDenied | None:
    """A — still working, never re-read — writes its updated status from its earlier view."""
    try:
        a.write(A_STATUS_2)
    except SessionDenied as denied:
        # Only the exception NAME goes in the trace: the coordinator's reason text
        # carries a session-id hash and a timestamp, which would make the trace
        # differ run to run. The verbatim reason travels in ``denial_message``.
        trace.append(f"A write     {render_notes(A_STATUS_2)}   -> DENIED {denied.exc_name}")
        return denied
    trace.append(f"A write     {render_notes(A_STATUS_2)}   (landed; B's line overwritten, nothing raised)")
    return None


def _recover(a: Session, trace: list[str]) -> bool:
    """After the deny: a mandatory fresh read, then A's update rebuilt FROM those bytes."""
    fresh = a.reacquire()
    trace.append(f"A reacquire -> {render_notes(fresh)}")
    # Rebuild from the fresh bytes rather than from memory: swap A's own status
    # line, keep everything else (B's pickup line) exactly as it is on disk.
    updated = fresh.replace(A_STATUS_1, A_STATUS_2, 1)
    a.write(updated)
    trace.append(f"A write     {render_notes(updated)}")
    return True
