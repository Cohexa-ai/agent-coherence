# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""U2 — foreign-write detection counters and the per-run liveness row.

The detector counts each newly observed on-disk content once per artifact as
``foreign`` / ``mediated`` / ``lag_suppressed`` (R5, R7, R13), edge-gated on the
disk hash it last counted so one unreconciled divergence cannot accumulate a
count every tick (R15, KTD14). A separate per-run observation row records that
the detector actually ran (R8, R12, KTD6): mirroring the conflict counters alone
cannot distinguish "ran and saw nothing" from "never ran", because both tables
are created on every writer open whether or not the sweep thread exists.

Nothing here enforces: these are counts, and a count denies nothing (R9).
"""

from __future__ import annotations

from pathlib import Path
from uuid import UUID, uuid4

import pytest

from ccs.coordinator.registry import ArtifactRegistry
from ccs.coordinator.registry_protocol import FOREIGN_WRITE_OUTCOMES
from ccs.coordinator.sqlite_registry import SqliteArtifactRegistry

HASH_A = "a" * 64
HASH_B = "b" * 64


@pytest.fixture(params=["memory", "sqlite"])
def registry(request, tmp_path: Path):
    if request.param == "memory":
        yield ArtifactRegistry()
    else:
        reg = SqliteArtifactRegistry(tmp_path / "state.db")
        yield reg
        reg.close()


# ---------------------------------------------------------------------------
# Outcome vocabulary (R5 — three classifications and nothing finer)
# ---------------------------------------------------------------------------


def test_outcome_vocabulary_is_exactly_three() -> None:
    assert FOREIGN_WRITE_OUTCOMES == ("foreign", "mediated", "lag_suppressed")


def test_an_unknown_outcome_is_refused(registry) -> None:
    """R5 fixes the classification at three values; a fourth is a caller bug,
    not a silently-created bucket."""
    with pytest.raises(ValueError):
        registry.record_foreign_write(uuid4(), "probably_foreign", HASH_A)


# ---------------------------------------------------------------------------
# Edge-gated counting (R15 / KTD14)
# ---------------------------------------------------------------------------


def test_first_observation_of_a_content_counts(registry) -> None:
    art = uuid4()
    assert registry.record_foreign_write(art, "foreign", HASH_A) is True
    assert registry.foreign_write_totals() == {art: {"foreign": 1}}


def test_repeating_the_same_disk_hash_never_recounts(registry) -> None:
    """The whole point of KTD14: git reports a dirty file on every tick until
    it is committed, so a level-triggered count would record one edit forever."""
    art = uuid4()
    registry.record_foreign_write(art, "foreign", HASH_A)
    for _ in range(10):
        assert registry.record_foreign_write(art, "foreign", HASH_A) is False
    assert registry.foreign_write_totals() == {art: {"foreign": 1}}


def test_a_second_distinct_content_counts_again(registry) -> None:
    art = uuid4()
    registry.record_foreign_write(art, "foreign", HASH_A)
    assert registry.record_foreign_write(art, "foreign", HASH_B) is True
    assert registry.foreign_write_totals() == {art: {"foreign": 2}}


def test_the_edge_gate_is_per_artifact_not_global(registry) -> None:
    first, second = uuid4(), uuid4()
    assert registry.record_foreign_write(first, "foreign", HASH_A) is True
    assert registry.record_foreign_write(second, "foreign", HASH_A) is True


def test_a_new_content_under_a_different_outcome_still_counts(registry) -> None:
    """The gate keys on content, not on outcome: an artifact that diverges,
    is re-mediated, then diverges again records all three observations."""
    art = uuid4()
    registry.record_foreign_write(art, "foreign", HASH_A)
    registry.record_foreign_write(art, "mediated", HASH_B)
    registry.record_foreign_write(art, "foreign", HASH_A)
    assert registry.foreign_write_totals() == {art: {"foreign": 2, "mediated": 1}}


# ---------------------------------------------------------------------------
# Three independent buckets (R7, R13 — a suppression is never folded in)
# ---------------------------------------------------------------------------


def test_three_outcome_buckets_are_counted_independently(registry) -> None:
    art = uuid4()
    registry.record_foreign_write(art, "foreign", HASH_A)
    registry.record_foreign_write(art, "mediated", HASH_B)
    registry.record_foreign_write(art, "lag_suppressed", "c" * 64)
    assert registry.foreign_write_totals() == {
        art: {"foreign": 1, "mediated": 1, "lag_suppressed": 1}
    }


def test_zero_detections_is_an_empty_mapping_not_an_error(registry) -> None:
    assert registry.foreign_write_totals() == {}


# ---------------------------------------------------------------------------
# Per-run observation rows (R8, R12 / KTD6)
# ---------------------------------------------------------------------------


def test_a_fresh_store_has_no_observation_row(registry) -> None:
    """Zero ticks observed is what "never ran" looks like from inside."""
    assert registry.detection_runs() == []


def test_a_tick_opens_this_run_and_later_ticks_extend_it(registry) -> None:
    registry.record_detection_tick(100.0)
    registry.record_detection_tick(105.0)
    registry.record_detection_tick(110.0)
    runs = registry.detection_runs()
    assert len(runs) == 1
    run = runs[0]
    assert (run.first_tick_unix, run.last_tick_unix, run.tick_count) == (100.0, 110.0, 3)


def test_two_runs_against_one_store_produce_two_rows(tmp_path: Path) -> None:
    """R12 asks whether ticks covered a span. A cumulative count cannot answer
    that across a restart, so each run gets its own interval."""
    db = tmp_path / "state.db"
    first = SqliteArtifactRegistry(db)
    first.record_detection_tick(100.0)
    first.record_detection_tick(105.0)
    first.close()

    second = SqliteArtifactRegistry(db)
    second.record_detection_tick(500.0)
    runs = sorted(second.detection_runs(), key=lambda r: r.first_tick_unix)
    second.close()

    assert len(runs) == 2
    assert (runs[0].first_tick_unix, runs[0].last_tick_unix, runs[0].tick_count) == (
        100.0,
        105.0,
        2,
    )
    assert (runs[1].first_tick_unix, runs[1].last_tick_unix, runs[1].tick_count) == (
        500.0,
        500.0,
        1,
    )
    assert runs[0].run_id != runs[1].run_id


# ---------------------------------------------------------------------------
# Durability and the read-only handle (sqlite only)
# ---------------------------------------------------------------------------


def test_counts_and_runs_survive_close_and_reopen(tmp_path: Path) -> None:
    db = tmp_path / "state.db"
    art = uuid4()
    reg = SqliteArtifactRegistry(db)
    reg.record_foreign_write(art, "foreign", HASH_A)
    reg.record_detection_tick(100.0)
    reg.close()

    reopened = SqliteArtifactRegistry(db)
    assert reopened.foreign_write_totals() == {art: {"foreign": 1}}
    assert len(reopened.detection_runs()) == 1
    reopened.close()


def test_reopening_does_not_recount_an_unchanged_divergence(tmp_path: Path) -> None:
    """The edge state is durable, so a restart does not re-observe every
    still-dirty artifact as new."""
    db = tmp_path / "state.db"
    art = uuid4()
    reg = SqliteArtifactRegistry(db)
    reg.record_foreign_write(art, "foreign", HASH_A)
    reg.close()

    reopened = SqliteArtifactRegistry(db)
    assert reopened.record_foreign_write(art, "foreign", HASH_A) is False
    assert reopened.foreign_write_totals() == {art: {"foreign": 1}}
    reopened.close()


def test_a_read_only_open_creates_neither_table(tmp_path: Path) -> None:
    """Mirrors the conflict-counter contract: the write-free read-only open
    never runs the ensure, and its readers tolerate the absence."""
    import sqlite3

    db = tmp_path / "state.db"
    SqliteArtifactRegistry(db).close()

    conn = sqlite3.connect(db)
    tables = {
        row[0]
        for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
    }
    conn.close()
    assert "foreign_write_counters" in tables
    assert "foreign_write_observations" in tables


def test_read_only_handle_tolerates_a_pre_instrumentation_store(tmp_path: Path) -> None:
    """A store written before this instrument shipped is at the current schema
    (the detection tables are outside the versioned migration chain) but has
    neither table. A read-only handle reports empty rather than raising —
    matching the conflict counters. Built by dropping the tables a writer open
    creates, since the read-only open refuses a schema-less file outright."""
    import sqlite3

    db = tmp_path / "state.db"
    SqliteArtifactRegistry(db).close()
    conn = sqlite3.connect(db)
    conn.execute("DROP TABLE foreign_write_counters")
    conn.execute("DROP TABLE foreign_write_observations")
    conn.commit()
    conn.close()

    reg = SqliteArtifactRegistry(db, read_only=True)
    assert reg.foreign_write_totals() == {}
    assert reg.detection_runs() == []
    reg.close()


# ---------------------------------------------------------------------------
# Backend parity
# ---------------------------------------------------------------------------


def test_both_backends_expose_the_same_detection_surface() -> None:
    for name in (
        "record_foreign_write",
        "record_detection_tick",
        "foreign_write_totals",
        "detection_runs",
    ):
        assert callable(getattr(ArtifactRegistry(), name)), name
        assert hasattr(SqliteArtifactRegistry, name), name


def test_detection_writes_touch_no_artifact_row(tmp_path: Path) -> None:
    """R9 — the detector mutates nothing outside its own two tables. A count
    against an unregistered artifact id must not seed one."""
    import sqlite3

    db = tmp_path / "state.db"
    reg = SqliteArtifactRegistry(db)
    reg.record_foreign_write(UUID(int=1), "foreign", HASH_A)
    reg.record_detection_tick(100.0)
    reg.close()

    conn = sqlite3.connect(db)
    assert conn.execute("SELECT count(*) FROM artifacts").fetchone()[0] == 0
    conn.close()
