# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""Tests for the replay trace loader (Unit 3 of D v1).

Covers the consumer-side contract documented in
``docs/proposals/replay_trace_format.md`` §§1-5 and the loader
requirements in
``docs/plans/2026-05-26-001-feat-langgraph-cycle-replay-tooling-v1-plan.md``
Unit 3:

- ``load`` happy paths (both streams, state_log only)
- ``merged()`` order: intra-tick state_log before content_audit_log
- Lazy ``MultiInstanceTraceError`` from the iterator
- Lazy ``TraceCorruptionError`` on duplicate ``(instance_id, seq)``
- Eager ``ManifestMissingOrUnreadableError`` on missing / malformed
  manifest
- ``streams_present`` reconciliation when manifest declares a stream
  that is missing on disk (the "capture bug" signal for Unit 5)
- Round-trip integration with ``CCSStore.record_to`` (Unit 2)
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from ccs.replay import (
    LoadedTrace,
    ManifestMissingOrUnreadableError,
    MultiInstanceTraceError,
    TraceCorruptionError,
    load,
)
from replay_fixtures import (
    audit_entry as _audit_entry,
    state_log_entry as _state_log_entry,
    write_jsonl as _write_jsonl,
    write_manifest as _write_manifest,
)


# ---------------------------------------------------------------------------
# Happy paths
# ---------------------------------------------------------------------------


class TestLoadHappyPath:
    """Both streams declared and present — the dominant case."""

    def test_loads_both_streams_and_merges_in_order(self, tmp_path: Path) -> None:
        session = tmp_path / "session"
        _write_manifest(session, streams=["state_log", "content_audit_log"])
        _write_jsonl(
            session / "state_log.jsonl",
            [
                _state_log_entry(tick=1, sequence_number=1),
                _state_log_entry(tick=3, sequence_number=2, trigger="commit"),
            ],
        )
        _write_jsonl(
            session / "content_audit_log.jsonl",
            [
                _audit_entry(tick=2, sequence_number=1),
                _audit_entry(tick=4, sequence_number=2),
            ],
        )

        loaded = load(session)
        assert isinstance(loaded, LoadedTrace)
        assert loaded.streams_present == {"state_log", "content_audit_log"}

        merged = list(loaded.merged())
        ticks_and_kinds = [(k, e["tick"]) for k, e in merged]
        assert ticks_and_kinds == [
            ("state_log", 1),
            ("content_audit_log", 2),
            ("state_log", 3),
            ("content_audit_log", 4),
        ]

    def test_streams_present_omits_undeclared_streams_on_disk(
        self, tmp_path: Path
    ) -> None:
        """A stream file that exists on disk but is NOT declared in the
        manifest is treated as absent (defense against partial captures
        or partners copying only-some-files — per spec §1)."""
        session = tmp_path / "session"
        _write_manifest(session, streams=["state_log"])
        _write_jsonl(
            session / "state_log.jsonl",
            [_state_log_entry(tick=1, sequence_number=1)],
        )
        # Plant an undeclared stream file on disk.
        _write_jsonl(
            session / "content_audit_log.jsonl",
            [_audit_entry(tick=2, sequence_number=1)],
        )

        loaded = load(session)
        assert loaded.streams_present == {"state_log"}
        merged = list(loaded.merged())
        assert [k for k, _ in merged] == ["state_log"]


class TestStateLogOnly:
    """Compliance-partner shape: state_log opt-in, audit opted-out."""

    def test_loads_state_log_only_session(self, tmp_path: Path) -> None:
        session = tmp_path / "session"
        _write_manifest(session, streams=["state_log"])
        _write_jsonl(
            session / "state_log.jsonl",
            [
                _state_log_entry(tick=1, sequence_number=1),
                _state_log_entry(tick=2, sequence_number=2, trigger="commit"),
            ],
        )

        loaded = load(session)
        assert loaded.streams_present == {"state_log"}
        merged = list(loaded.merged())
        assert len(merged) == 2
        assert all(kind == "state_log" for kind, _ in merged)


# ---------------------------------------------------------------------------
# Merge-rule edge: intra-tick ordering
# ---------------------------------------------------------------------------


class TestIntraTickOrdering:
    """state_log MUST sort before content_audit_log at the same tick.

    The merge order tiebreaker is structural (per spec §5.1); predicates
    decide AMBIGUOUS vs CONFIRMED by comparing tick fields directly, so
    the AMBIGUOUS carve-out remains correct.
    """

    def test_state_log_before_audit_at_same_tick(self, tmp_path: Path) -> None:
        session = tmp_path / "session"
        _write_manifest(session, streams=["state_log", "content_audit_log"])
        _write_jsonl(
            session / "state_log.jsonl",
            [_state_log_entry(tick=5, sequence_number=1, trigger="commit")],
        )
        _write_jsonl(
            session / "content_audit_log.jsonl",
            [_audit_entry(tick=5, sequence_number=1)],
        )

        loaded = load(session)
        merged = list(loaded.merged())
        kinds = [k for k, _ in merged]
        assert kinds == ["state_log", "content_audit_log"]


# ---------------------------------------------------------------------------
# Lazy multi-instance detection
# ---------------------------------------------------------------------------


class TestMultiInstanceDetection:
    """Two distinct ``instance_id`` values in one stream → lazy raise."""

    def test_raises_with_d_plus_one_roadmap_pointer(self, tmp_path: Path) -> None:
        session = tmp_path / "session"
        _write_manifest(session, streams=["state_log"])
        _write_jsonl(
            session / "state_log.jsonl",
            [
                _state_log_entry(tick=1, sequence_number=1, instance_id="A"),
                _state_log_entry(tick=2, sequence_number=2, instance_id="B"),
            ],
        )

        loaded = load(session)
        # load() itself does NOT raise.
        assert loaded.streams_present == {"state_log"}

        with pytest.raises(MultiInstanceTraceError) as exc_info:
            list(loaded.merged())
        assert "per-instance-replay is roadmapped for D+1" in str(exc_info.value)

    def test_partial_walk_succeeds_until_boundary(self, tmp_path: Path) -> None:
        session = tmp_path / "session"
        _write_manifest(session, streams=["state_log"])
        _write_jsonl(
            session / "state_log.jsonl",
            [
                _state_log_entry(tick=1, sequence_number=1, instance_id="A"),
                _state_log_entry(tick=2, sequence_number=2, instance_id="A"),
                _state_log_entry(tick=3, sequence_number=3, instance_id="B"),
            ],
        )

        loaded = load(session)
        it = loaded.merged()
        first = next(it)
        second = next(it)
        assert first[1]["instance_id"] == "A"
        assert second[1]["instance_id"] == "A"
        with pytest.raises(MultiInstanceTraceError):
            next(it)


# ---------------------------------------------------------------------------
# Lazy duplicate-seq detection
# ---------------------------------------------------------------------------


class TestMalformedJsonDetection:
    """A malformed JSON line mid-stream raises TraceCorruptionError lazily."""

    def test_malformed_json_in_stream_raises_corruption_error(
        self, tmp_path: Path,
    ) -> None:
        session = tmp_path / "session"
        _write_manifest(session, streams=["state_log"])
        state_log_path = session / "state_log.jsonl"
        # First line valid JSON; second line garbage. Walker must reject
        # the file with a TraceCorruptionError that names the path AND
        # the offending line number so partners can find the bad row.
        state_log_path.write_text(
            json.dumps(_state_log_entry(tick=1, sequence_number=1))
            + "\n{not json at all}\n",
            encoding="utf-8",
        )

        loaded = load(session)
        with pytest.raises(TraceCorruptionError) as exc_info:
            list(loaded.merged())
        message = str(exc_info.value)
        assert str(state_log_path) in message
        # Garbage line is line 2.
        assert ":2" in message


class TestDuplicateSequenceDetection:
    """Defense in depth against partner-written callbacks that don't fsync."""

    def test_raises_with_seq_path_and_line_number(self, tmp_path: Path) -> None:
        session = tmp_path / "session"
        _write_manifest(session, streams=["state_log"])
        state_log_path = session / "state_log.jsonl"
        _write_jsonl(
            state_log_path,
            [
                _state_log_entry(tick=1, sequence_number=7),
                _state_log_entry(tick=2, sequence_number=8),
                _state_log_entry(tick=3, sequence_number=7),  # duplicate
            ],
        )

        loaded = load(session)
        with pytest.raises(TraceCorruptionError) as exc_info:
            list(loaded.merged())
        message = str(exc_info.value)
        assert "sequence_number=7" in message
        assert str(state_log_path) in message
        # The third entry is on line 3 in the JSONL file.
        assert ":3" in message


# ---------------------------------------------------------------------------
# Eager manifest errors
# ---------------------------------------------------------------------------


class TestManifestErrors:
    """Manifest problems surface eagerly with clear messages."""

    def test_nonexistent_session_dir_raises(self, tmp_path: Path) -> None:
        with pytest.raises(ManifestMissingOrUnreadableError) as exc_info:
            load(tmp_path / "does-not-exist")
        # Must NOT be a bare FileNotFoundError traceback.
        assert "manifest.json not found" in str(exc_info.value)

    def test_existing_dir_without_manifest_raises(self, tmp_path: Path) -> None:
        empty_dir = tmp_path / "empty"
        empty_dir.mkdir()
        with pytest.raises(ManifestMissingOrUnreadableError):
            load(empty_dir)

    def test_malformed_manifest_raises_clear_error(self, tmp_path: Path) -> None:
        session = tmp_path / "session"
        session.mkdir()
        (session / "manifest.json").write_text("{not valid json", encoding="utf-8")
        with pytest.raises(ManifestMissingOrUnreadableError) as exc_info:
            load(session)
        assert "not valid JSON" in str(exc_info.value)


# ---------------------------------------------------------------------------
# Capture-bug signal for Unit 5
# ---------------------------------------------------------------------------


class TestStreamPresenceReconciliation:
    """Manifest declares a stream but the file is missing on disk.

    Loader does NOT raise — this is the signal Unit 5's CLI surfaces
    as exit code 2 (vs. user opt-out which is exit 0).
    """

    def test_declared_but_missing_stream_omitted_from_streams_present(
        self, tmp_path: Path
    ) -> None:
        session = tmp_path / "session"
        _write_manifest(session, streams=["state_log", "content_audit_log"])
        _write_jsonl(
            session / "state_log.jsonl",
            [_state_log_entry(tick=1, sequence_number=1)],
        )
        # NOTE: content_audit_log.jsonl is NOT written.

        loaded = load(session)
        assert loaded.streams_present == {"state_log"}
        # Manifest still records the declaration — that's how Unit 5
        # distinguishes "capture bug" from "user opt-out".
        assert loaded.manifest["streams"] == ["state_log", "content_audit_log"]
        merged = list(loaded.merged())
        assert [k for k, _ in merged] == ["state_log"]


# ---------------------------------------------------------------------------
# Round-trip integration with the recorder (Unit 2)
# ---------------------------------------------------------------------------


class TestRecorderRoundTrip:
    """Capture from CCSStore.record_to, load with load(), assert entries
    match one-to-one with the emitted events."""

    def setup_method(self) -> None:
        pytest.importorskip("langgraph.store.base")

    def test_round_trip_matches_emitted_events(self, tmp_path: Path) -> None:
        from langgraph.store.base import GetOp, PutOp

        from ccs.adapters.ccsstore import CCSStore

        emitted_state: list[dict] = []
        emitted_audit: list[dict] = []
        session = tmp_path / "session"

        with CCSStore.record_to(
            session,
            state_log=emitted_state.append,
            content_audit_log=emitted_audit.append,
        ) as store:
            for i in range(5):
                store.batch([
                    PutOp(
                        namespace=("planner", "shared"),
                        key=f"k{i}",
                        value={"v": i},
                    ),
                ])
            store.batch([GetOp(namespace=("planner", "shared"), key="k0")])

        loaded = load(session)
        assert loaded.streams_present == {"state_log", "content_audit_log"}

        merged = list(loaded.merged())
        on_disk_state = [e for k, e in merged if k == "state_log"]
        on_disk_audit = [e for k, e in merged if k == "content_audit_log"]

        # The composed caller callbacks saw exactly what the file writers
        # wrote — that's the round-trip contract.
        assert len(on_disk_state) == len(emitted_state)
        assert len(on_disk_audit) == len(emitted_audit)
        # Entries match field-by-field (JSON round-trip preserves equality
        # because the recorder uses json.dumps with default settings).
        for emitted, loaded_entry in zip(emitted_state, on_disk_state):
            assert emitted == loaded_entry
        for emitted, loaded_entry in zip(emitted_audit, on_disk_audit):
            assert emitted == loaded_entry

    def test_round_trip_yields_state_log_first_at_same_tick(
        self, tmp_path: Path
    ) -> None:
        """A single batched put produces both a state_log commit and
        a content_audit entry at the same tick — merged() must yield
        state_log first."""
        from langgraph.store.base import PutOp

        from ccs.adapters.ccsstore import CCSStore

        session = tmp_path / "session"
        with CCSStore.record_to(session) as store:
            store.batch([PutOp(namespace=("ns", "shared"), key="k", value={"v": 1})])

        loaded = load(session)
        merged = list(loaded.merged())

        # Group by tick; for any tick with both kinds, state_log must
        # appear before content_audit_log.
        seen_audit_at_tick: dict[int, bool] = {}
        for kind, entry in merged:
            tick = entry["tick"]
            if kind == "content_audit_log":
                seen_audit_at_tick[tick] = True
            elif kind == "state_log":
                assert not seen_audit_at_tick.get(tick, False), (
                    f"state_log entry at tick {tick} appeared after a "
                    "content_audit_log entry at the same tick"
                )
