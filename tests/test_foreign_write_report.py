# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""U3 — the offline foreign-write report and its three distinguishable states.

The sibling conflict-counter reader deliberately maps an absent table and an
empty one to the same value, and documents that as "zero recorded conflicts".
This reader cannot: a coverage claim is gated on the detector having actually
observed a span, so reading a store where it never ran as a clean zero is the
one failure the instrument exists to prevent. Hence three states, and a
discriminator that keys on the observation ROW rather than on the table — both
tables are created on every writer open, including one whose sweep thread was
never started.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from uuid import uuid4

import pytest

from ccs.coordinator.sqlite_registry import SqliteArtifactRegistry
from ccs.diagnose.foreign_writes import (
    COUNTS,
    INSTRUMENTED_ZERO,
    NOT_INSTRUMENTED,
    read_foreign_write_report,
)

HASH_A = "a" * 64
HASH_B = "b" * 64


def _drop_detection_tables(db: Path) -> None:
    conn = sqlite3.connect(db)
    conn.execute("DROP TABLE foreign_write_counters")
    conn.execute("DROP TABLE foreign_write_observations")
    conn.commit()
    conn.close()


# ---------------------------------------------------------------------------
# The three states (R8, Success Criterion 2)
# ---------------------------------------------------------------------------


def test_a_store_predating_the_instrument_reads_as_not_instrumented(tmp_path: Path) -> None:
    db = tmp_path / "state.db"
    SqliteArtifactRegistry(db).close()
    _drop_detection_tables(db)
    report = read_foreign_write_report(db)
    assert report.state == NOT_INSTRUMENTED
    assert report.instrumented is False
    assert report.totals == {}
    assert report.runs == ()


def test_tables_present_but_never_ticked_reads_as_not_instrumented(tmp_path: Path) -> None:
    """The load-bearing case, and the one the sibling pattern cannot express.
    A coordinator started with the sweep disabled still opens the store as a
    writer, so both tables exist and are empty. That is not a zero."""
    db = tmp_path / "state.db"
    SqliteArtifactRegistry(db).close()
    report = read_foreign_write_report(db)
    assert report.state == NOT_INSTRUMENTED
    assert report.instrumented is False


def test_ticked_with_nothing_found_reads_as_a_real_zero(tmp_path: Path) -> None:
    db = tmp_path / "state.db"
    reg = SqliteArtifactRegistry(db)
    reg.record_detection_tick(100.0)
    reg.record_detection_tick(105.0)
    reg.close()

    report = read_foreign_write_report(db)
    assert report.state == INSTRUMENTED_ZERO
    assert report.instrumented is True
    assert report.totals == {}
    assert len(report.runs) == 1
    assert report.runs[0].tick_count == 2


def test_detections_read_back_per_artifact_and_outcome(tmp_path: Path) -> None:
    db = tmp_path / "state.db"
    art = uuid4()
    reg = SqliteArtifactRegistry(db)
    reg.record_detection_tick(100.0)
    reg.record_foreign_write(art, "foreign", HASH_A)
    reg.record_foreign_write(art, "lag_suppressed", HASH_B)
    reg.close()

    report = read_foreign_write_report(db)
    assert report.state == COUNTS
    assert report.totals == {art.hex: {"foreign": 1, "lag_suppressed": 1}}


# ---------------------------------------------------------------------------
# Coverage over a span (R12)
# ---------------------------------------------------------------------------


def test_a_span_inside_one_run_is_covered(tmp_path: Path) -> None:
    db = tmp_path / "state.db"
    reg = SqliteArtifactRegistry(db)
    for tick in (100.0, 110.0, 120.0):
        reg.record_detection_tick(tick)
    reg.close()
    assert read_foreign_write_report(db).covers(105.0, 115.0) is True


def test_a_span_reaching_past_the_last_tick_is_not_covered(tmp_path: Path) -> None:
    db = tmp_path / "state.db"
    reg = SqliteArtifactRegistry(db)
    reg.record_detection_tick(100.0)
    reg.record_detection_tick(120.0)
    reg.close()
    assert read_foreign_write_report(db).covers(110.0, 130.0) is False


def test_a_gap_between_two_runs_is_not_covered(tmp_path: Path) -> None:
    """The case a cumulative tick count cannot see: the coordinator was down
    for the middle of the session, yet the total looks healthy."""
    db = tmp_path / "state.db"
    first = SqliteArtifactRegistry(db)
    first.record_detection_tick(100.0)
    first.record_detection_tick(150.0)
    first.close()
    second = SqliteArtifactRegistry(db)
    second.record_detection_tick(400.0)
    second.record_detection_tick(450.0)
    second.close()

    report = read_foreign_write_report(db)
    assert report.covers(110.0, 140.0) is True
    assert report.covers(410.0, 440.0) is True
    assert report.covers(140.0, 410.0) is False


def test_an_uninstrumented_store_covers_nothing(tmp_path: Path) -> None:
    db = tmp_path / "state.db"
    SqliteArtifactRegistry(db).close()
    assert read_foreign_write_report(db).covers(0.0, 1.0) is False


def test_a_backwards_span_is_a_caller_error(tmp_path: Path) -> None:
    db = tmp_path / "state.db"
    reg = SqliteArtifactRegistry(db)
    reg.record_detection_tick(100.0)
    reg.close()
    with pytest.raises(ValueError):
        read_foreign_write_report(db).covers(200.0, 100.0)


# ---------------------------------------------------------------------------
# Failure modes never read as zero
# ---------------------------------------------------------------------------


def test_a_missing_file_raises_rather_than_reading_as_zero(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        read_foreign_write_report(tmp_path / "nope.db")


def test_a_corrupt_store_raises_rather_than_reading_as_zero(tmp_path: Path) -> None:
    """A broken read must never masquerade as a quiet month."""
    db = tmp_path / "state.db"
    db.write_bytes(b"this is not a sqlite database" * 64)
    with pytest.raises(sqlite3.DatabaseError):
        read_foreign_write_report(db)


def test_the_reader_does_not_modify_the_store(tmp_path: Path) -> None:
    """Raw read-only URI open, like the conflict-counter sibling: pointing the
    report at a store copied off a machine must not touch it. Asserted on the
    bytes rather than on sidecar files, which belong to the writer that made
    them — the WAL sidecar outlives the writer's close and says nothing about
    this reader."""
    db = tmp_path / "state.db"
    reg = SqliteArtifactRegistry(db)
    reg.record_detection_tick(100.0)
    reg.close()
    before_bytes = db.read_bytes()
    before_mtime = db.stat().st_mtime_ns

    report = read_foreign_write_report(db)

    assert report.state == INSTRUMENTED_ZERO
    assert db.read_bytes() == before_bytes
    assert db.stat().st_mtime_ns == before_mtime
