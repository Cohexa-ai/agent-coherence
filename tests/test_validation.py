# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Tests for ccs.validation.validate_log."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from ccs.validation import Gap, SchemaMismatch, validate_log

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _write_jsonl(tmp_path: Path, entries: list[dict], filename: str = "test.jsonl") -> Path:
    p = tmp_path / filename
    p.write_text("\n".join(json.dumps(e) for e in entries) + "\n", encoding="utf-8")
    return p


def _entry(seq: int, instance_id: str = "inst-a", schema_version: str = "ccs.state_log.v1") -> dict:
    return {"sequence_number": seq, "instance_id": instance_id, "schema_version": schema_version}


# ---------------------------------------------------------------------------
# Happy paths
# ---------------------------------------------------------------------------

def test_clean_log_returns_empty_lists(tmp_path: Path) -> None:
    path = _write_jsonl(tmp_path, [_entry(1), _entry(2), _entry(3)])
    gaps, mismatches = validate_log(path)
    assert gaps == []
    assert mismatches == []


def test_empty_file_returns_empty_lists(tmp_path: Path) -> None:
    path = tmp_path / "empty.jsonl"
    path.write_text("", encoding="utf-8")
    gaps, mismatches = validate_log(path)
    assert gaps == []
    assert mismatches == []


def test_single_entry_clean(tmp_path: Path) -> None:
    path = _write_jsonl(tmp_path, [_entry(1)])
    gaps, mismatches = validate_log(path)
    assert gaps == []
    assert mismatches == []


def test_stream_label_is_passed_through(tmp_path: Path) -> None:
    path = _write_jsonl(tmp_path, [_entry(1), _entry(3)])
    gaps, _ = validate_log(path, stream="metrics")
    assert gaps[0].stream == "metrics"


def test_schema_version_none_skips_version_checking(tmp_path: Path) -> None:
    entries = [_entry(1, schema_version="ccs.state_log.v999")]
    path = _write_jsonl(tmp_path, entries)
    gaps, mismatches = validate_log(path, schema_version=None)
    assert gaps == []
    assert mismatches == []


def test_schema_version_match_produces_no_mismatch(tmp_path: Path) -> None:
    path = _write_jsonl(tmp_path, [_entry(1), _entry(2)])
    gaps, mismatches = validate_log(path, schema_version="ccs.state_log.v1")
    assert gaps == []
    assert mismatches == []


# ---------------------------------------------------------------------------
# Gap detection
# ---------------------------------------------------------------------------

def test_gap_at_line_2(tmp_path: Path) -> None:
    path = _write_jsonl(tmp_path, [_entry(1), _entry(3)])
    gaps, _ = validate_log(path, stream="state_log")
    assert gaps == [Gap(stream="state_log", expected=2, found=3, at_index=1)]


def test_two_gaps_both_reported(tmp_path: Path) -> None:
    path = _write_jsonl(tmp_path, [_entry(1), _entry(3), _entry(5)])
    gaps, _ = validate_log(path)
    assert len(gaps) == 2
    assert gaps[0] == Gap(stream="state_log", expected=2, found=3, at_index=1)
    assert gaps[1] == Gap(stream="state_log", expected=4, found=5, at_index=2)


# ---------------------------------------------------------------------------
# Session boundary (multi-session)
# ---------------------------------------------------------------------------

def test_multi_session_no_false_positive_gap(tmp_path: Path) -> None:
    entries = [
        _entry(1, instance_id="inst-a"),
        _entry(2, instance_id="inst-a"),
        _entry(1, instance_id="inst-b"),  # new session — not a gap
        _entry(2, instance_id="inst-b"),
    ]
    path = _write_jsonl(tmp_path, entries)
    gaps, _ = validate_log(path)
    assert gaps == []


def test_multi_session_gap_within_second_session(tmp_path: Path) -> None:
    entries = [
        _entry(1, instance_id="inst-a"),
        _entry(1, instance_id="inst-b"),
        _entry(3, instance_id="inst-b"),  # gap: expected 2, found 3
    ]
    path = _write_jsonl(tmp_path, entries)
    gaps, _ = validate_log(path)
    assert gaps == [Gap(stream="state_log", expected=2, found=3, at_index=2)]


# ---------------------------------------------------------------------------
# Schema version checking
# ---------------------------------------------------------------------------

def test_schema_mismatch_reported(tmp_path: Path) -> None:
    entries = [_entry(1, schema_version="ccs.state_log.v2")]
    path = _write_jsonl(tmp_path, entries)
    _, mismatches = validate_log(path, schema_version="ccs.state_log.v1")
    assert mismatches == [
        SchemaMismatch(
            stream="state_log",
            found_version="ccs.state_log.v2",
            expected_version="ccs.state_log.v1",
            at_index=0,
        )
    ]


def test_gap_and_schema_mismatch_both_reported(tmp_path: Path) -> None:
    entries = [
        _entry(1, schema_version="ccs.state_log.v1"),
        _entry(3, schema_version="ccs.state_log.v2"),  # gap + version mismatch
    ]
    path = _write_jsonl(tmp_path, entries)
    gaps, mismatches = validate_log(path, schema_version="ccs.state_log.v1")
    assert len(gaps) == 1
    assert len(mismatches) == 1
    assert gaps[0].at_index == 1
    assert mismatches[0].at_index == 1


# ---------------------------------------------------------------------------
# Error paths
# ---------------------------------------------------------------------------

def test_missing_sequence_number_raises_valueerror(tmp_path: Path) -> None:
    entries = [{"instance_id": "inst-a"}]
    path = _write_jsonl(tmp_path, entries)
    with pytest.raises(ValueError, match="sequence_number"):
        validate_log(path)


def test_missing_instance_id_raises_valueerror(tmp_path: Path) -> None:
    entries = [{"sequence_number": 1}]
    path = _write_jsonl(tmp_path, entries)
    with pytest.raises(ValueError, match="instance_id"):
        validate_log(path)


def test_missing_schema_version_raises_when_param_provided(tmp_path: Path) -> None:
    entries = [{"sequence_number": 1, "instance_id": "inst-a"}]
    path = _write_jsonl(tmp_path, entries)
    with pytest.raises(ValueError, match="schema_version"):
        validate_log(path, schema_version="ccs.state_log.v1")


def test_missing_schema_version_ok_when_param_not_provided(tmp_path: Path) -> None:
    entries = [{"sequence_number": 1, "instance_id": "inst-a"}]
    path = _write_jsonl(tmp_path, entries)
    gaps, mismatches = validate_log(path)
    assert gaps == []
    assert mismatches == []


def test_valueerror_reports_correct_line_index(tmp_path: Path) -> None:
    entries = [_entry(1), {"instance_id": "inst-a"}]  # line 1 missing sequence_number
    path = _write_jsonl(tmp_path, entries)
    with pytest.raises(ValueError, match="Entry 1"):
        validate_log(path)


def test_malformed_json_raises_json_decode_error(tmp_path: Path) -> None:
    path = tmp_path / "bad.jsonl"
    path.write_text('{"sequence_number": 1, "instance_id": "x"}\nnot-json\n', encoding="utf-8")
    with pytest.raises(json.JSONDecodeError):
        validate_log(path)
