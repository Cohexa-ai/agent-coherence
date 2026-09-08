# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Read-only open of a coordinator ``state.db``, shared by the offline readers.

:mod:`ccs.diagnose.conflict_counters` and :mod:`ccs.diagnose.foreign_writes`
are two reports over the same store, so the low-level open is the same fact
twice, not the sanctioned per-backend duplication the two registries carry.
Written out twice it drifts silently: a busy timeout re-derived from the
registry's budget, or a tightening of the missing-file translation, would land
in one report and leave the other answering on different terms.

What deliberately stays with each caller is the per-table
missing-versus-empty logic. The conflict reader maps an absent table and an
empty one to the same zero and documents that as zero recorded conflicts; the
foreign-write reader must keep the two apart, because a coverage claim depends
on knowing whether the detector ever ran. Those are different contracts over
the same connection, and merging them would cost one of them its honesty.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

__all__ = ["open_readonly_state_db"]

BUSY_TIMEOUT_MS = 1500
"""Matches the registry's own budget-derived value."""


def open_readonly_state_db(path: Path) -> sqlite3.Connection:
    """Open the coordinator store at ``path`` read-only, ready to query.

    Raises ``FileNotFoundError`` when nothing is there — a report against a
    store that does not exist is a caller error, never evidence of a quiet
    month. Every other failure to open propagates, so a broken read cannot be
    mistaken for an empty one. The caller owns the returned connection and
    must close it.
    """
    # mode=ro + uri=True: without the explicit uri flag sqlite3 treats the
    # string as a literal filename and can silently fall back to read-write.
    # No pre-check stat: connect directly and translate the failure, so a
    # missing file cannot slip through a check-to-open race window.
    try:
        conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    except sqlite3.OperationalError as exc:
        if not path.exists():
            raise FileNotFoundError(f"no coordinator database at {path}") from exc
        raise
    try:
        # A transient writer lock must wait, never masquerade as an empty
        # result — whatever "empty" means to the report being built.
        conn.execute(f"PRAGMA busy_timeout={BUSY_TIMEOUT_MS}")
    except BaseException:
        # The caller never received the handle, so it cannot close it.
        conn.close()
        raise
    return conn
