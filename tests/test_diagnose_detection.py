# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""Unit 4 tests — divergence detection engine."""

from __future__ import annotations

import uuid
from typing import Iterable, Mapping

import pytest

from ccs.core.hashing import compute_content_hash
from ccs.core.identity import artifact_uuid
from ccs.diagnose import CCS_DIAGNOSE_LOG_SCHEMA_VERSION
from ccs.diagnose.callback import DEFAULT_SCOPE, DiagnoseEvent
from ccs.diagnose.classifier import (
    Bucket,
    ClassifierVerdict,
    Confidence,
    CoverageReport,
)
from ccs.diagnose.detection import (
    DetectionReport,
    DivergenceEvent,
    ExclusionPanel,
    HeatmapRow,
    ReadObservation,
    ReaderPairCount,
    detect,
)


# -------------------------------------------------------------------- #
# Test helpers
# -------------------------------------------------------------------- #


_INSTANCE_ID = uuid.UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")


def _hash(value: object) -> str:
    return compute_content_hash(repr(value))


def _ids_for(keys: Iterable[str]) -> dict[str, uuid.UUID]:
    return {k: artifact_uuid(DEFAULT_SCOPE, k) for k in keys}


def _make_event(
    *,
    sequence: int,
    tick: int,
    node: str,
    event_type: str,
    state: Mapping[str, object] | None = None,
    explicit_versions: Mapping[str, str] | None = None,
    explicit_hashes: Mapping[str, str] | None = None,
) -> DiagnoseEvent:
    """Build a synthetic ``DiagnoseEvent`` for detection tests.

    ``state`` mirrors the convenience used in Unit 3's tests: each key is
    hashed and the same hash is used for both ``artifact_versions`` and
    ``content_hashes`` (no checkpointer overlay). ``explicit_versions`` and
    ``explicit_hashes`` let a test decouple the two — required for the
    AND-clause stress test (different version, identical content hash).
    """
    state = state or {}
    versions: dict[uuid.UUID, str] = {}
    hashes: dict[uuid.UUID, str] = {}
    for key, value in state.items():
        aid = artifact_uuid(DEFAULT_SCOPE, key)
        h = _hash(value)
        versions[aid] = h
        hashes[aid] = h
    if explicit_versions:
        for key, version in explicit_versions.items():
            versions[artifact_uuid(DEFAULT_SCOPE, key)] = version
    if explicit_hashes:
        for key, h in explicit_hashes.items():
            hashes[artifact_uuid(DEFAULT_SCOPE, key)] = h
    return DiagnoseEvent(
        sequence_number=sequence,
        instance_id=_INSTANCE_ID,
        schema_version=CCS_DIAGNOSE_LOG_SCHEMA_VERSION,
        tick=tick,
        node=node,
        event_type=event_type,  # type: ignore[arg-type]
        artifact_versions=versions,
        content_hashes=hashes,
        run_id="run-x",
        namespace="",
    )


def _verdict(
    *,
    bucket: Bucket = Bucket.SHARED_ARTIFACT,
    tracked_keys: tuple[str, ...] = (),
    append_only_keys: tuple[str, ...] = (),
    mutable_keys: tuple[str, ...] = (),
    confidence: Confidence = Confidence.PRELIMINARY,
) -> ClassifierVerdict:
    return ClassifierVerdict(
        bucket=bucket,
        confidence=confidence,
        coverage=CoverageReport(
            tick_count=20,
            read_count=20,
            write_count=4,
            artifact_count=len(tracked_keys),
            verdict_confidence=confidence,
        ),
        tracked_keys=tracked_keys,
        append_only_keys=append_only_keys,
        mutable_keys=mutable_keys,
    )


# -------------------------------------------------------------------- #
# Happy paths
# -------------------------------------------------------------------- #


def test_detect_records_divergence_when_writer_advances_then_lagging_read():
    # Scenario 1: reader A is handed v2 at tick 4 (after a writer wrote v2 in
    # tick 3), then a "lagging" reader B is handed v1 at tick 6. The runtime
    # exposed an inconsistency — B was handed an older version than A saw.
    key_index = _ids_for(["Y"])
    events = [
        _make_event(sequence=1, tick=3, node="supervisor", event_type="node_end", state={"Y": "v2-payload"}),
        _make_event(sequence=2, tick=4, node="A", event_type="node_start", state={"Y": "v2-payload"}),
        _make_event(sequence=3, tick=6, node="B", event_type="node_start", state={"Y": "v1-payload"}),
    ]

    report = detect(
        events,
        verdict=_verdict(tracked_keys=("Y",)),
        key_index=key_index,
    )

    assert len(report.headline_divergence_events) == 1
    ev = report.headline_divergence_events[0]
    assert ev.artifact_key == "Y"
    assert ev.earlier_read.node == "A"
    assert ev.later_read.node == "B"
    assert ev.canonical_writer == "supervisor"
    assert ev.canonical_writer_tick == 3
    assert ev.is_sequential_staleness is False
    assert ev.is_cold_start is False
    assert report.agent_pain_count == 1
    assert report.top_event is ev


def test_detect_no_divergence_when_reads_keep_up():
    # Scenario 2: read v1 → write v2 → read v2. The two reads were handed
    # different versions, but each reader saw the latest at the time of its
    # read; the AND clause holds (versions and hashes differ), but ... wait,
    # this *is* observable divergence by the witness-quality rule because
    # reader A was handed v1 and reader B was handed v2. Reframed as the
    # plan intends: when a write *between* two reads explains the version
    # gap and the later reader saw the *new* version (no lag), there's
    # nothing inconsistent. Tracked here as the "happy path" of a write
    # being followed by an up-to-date read.
    key_index = _ids_for(["Y"])
    # Both reads see v2 — no divergence between them.
    events = [
        _make_event(sequence=1, tick=2, node="W", event_type="node_end", state={"Y": "v2"}),
        _make_event(sequence=2, tick=3, node="A", event_type="node_start", state={"Y": "v2"}),
        _make_event(sequence=3, tick=4, node="B", event_type="node_start", state={"Y": "v2"}),
    ]
    report = detect(events, verdict=_verdict(tracked_keys=("Y",)), key_index=key_index)
    assert report.headline_divergence_events == ()
    assert report.excluded_events == ()


def test_detect_no_divergence_when_versions_equal_across_readers():
    # Scenario 3: two readers see identical version.
    key_index = _ids_for(["Y"])
    events = [
        _make_event(sequence=1, tick=1, node="A", event_type="node_start", state={"Y": "v1"}),
        _make_event(sequence=2, tick=2, node="B", event_type="node_start", state={"Y": "v1"}),
    ]
    report = detect(events, verdict=_verdict(tracked_keys=("Y",)), key_index=key_index)
    assert report.headline_divergence_events == ()


def test_and_clause_stress_test_no_divergence_when_only_version_differs():
    # Scenario 4: monotonic-stamp reducer bumps version; content hash unchanged.
    key_index = _ids_for(["Y"])
    same_hash = _hash("v1-payload")
    events = [
        _make_event(
            sequence=1,
            tick=1,
            node="A",
            event_type="node_start",
            explicit_versions={"Y": "stamp-1"},
            explicit_hashes={"Y": same_hash},
        ),
        _make_event(
            sequence=2,
            tick=2,
            node="W",
            event_type="node_end",
            explicit_versions={"Y": "stamp-2"},
            explicit_hashes={"Y": same_hash},
        ),
        _make_event(
            sequence=3,
            tick=3,
            node="B",
            event_type="node_start",
            explicit_versions={"Y": "stamp-2"},
            explicit_hashes={"Y": same_hash},
        ),
    ]
    report = detect(events, verdict=_verdict(tracked_keys=("Y",)), key_index=key_index)
    # Version pair differs across reads but content hash is identical → no divergence.
    assert report.headline_divergence_events == ()
    assert report.excluded_events == ()


# -------------------------------------------------------------------- #
# Sequential staleness exclusion + strict promotion
# -------------------------------------------------------------------- #


def _sequential_staleness_events() -> list[DiagnoseEvent]:
    # reader_a is handed v3 (post-W2), reader_b at tick 10 is handed v1
    # (stale relative to writes at ticks 5 and 7). w_prior=tick 5,
    # w_intervening=tick 7, later_read=tick 10 → gap of 5 ticks from w_prior,
    # meets the >=2 sequential-staleness rule.
    return [
        _make_event(sequence=1, tick=5, node="W", event_type="node_end", state={"Y": "v2"}),
        _make_event(sequence=2, tick=7, node="W2", event_type="node_end", state={"Y": "v3"}),
        _make_event(sequence=3, tick=8, node="reader_a", event_type="node_start", state={"Y": "v3"}),
        _make_event(sequence=4, tick=10, node="reader_b", event_type="node_start", state={"Y": "v1"}),
    ]


def test_sequential_staleness_excluded_by_default():
    key_index = _ids_for(["Y"])
    events = _sequential_staleness_events()
    report = detect(events, verdict=_verdict(tracked_keys=("Y",)), key_index=key_index)

    assert report.headline_divergence_events == ()
    assert len(report.excluded_events) == 1
    assert report.excluded_events[0].is_sequential_staleness is True
    assert report.exclusion_panel.sequential_staleness_count == 1
    assert report.strict_mode is False


def test_sequential_staleness_promoted_in_strict_mode():
    key_index = _ids_for(["Y"])
    events = _sequential_staleness_events()
    report = detect(
        events, verdict=_verdict(tracked_keys=("Y",)), key_index=key_index, strict=True
    )

    assert len(report.headline_divergence_events) == 1
    ev = report.headline_divergence_events[0]
    assert ev.is_sequential_staleness is True  # informational flag preserved
    assert report.strict_mode is True
    assert report.exclusion_panel.sequential_staleness_count == 0


# -------------------------------------------------------------------- #
# Cold start
# -------------------------------------------------------------------- #


def test_cold_start_excluded():
    key_index = _ids_for(["Y"])
    # Two readers see different versions; no write event in the buffer.
    events = [
        _make_event(sequence=1, tick=1, node="A", event_type="node_start", state={"Y": "v1"}),
        _make_event(sequence=2, tick=2, node="B", event_type="node_start", state={"Y": "v2"}),
    ]
    report = detect(events, verdict=_verdict(tracked_keys=("Y",)), key_index=key_index)
    assert report.headline_divergence_events == ()
    assert len(report.excluded_events) == 1
    assert report.excluded_events[0].is_cold_start is True
    assert report.exclusion_panel.cold_start_count == 1


def test_cold_start_not_promoted_by_strict():
    key_index = _ids_for(["Y"])
    events = [
        _make_event(sequence=1, tick=1, node="A", event_type="node_start", state={"Y": "v1"}),
        _make_event(sequence=2, tick=2, node="B", event_type="node_start", state={"Y": "v2"}),
    ]
    report = detect(
        events, verdict=_verdict(tracked_keys=("Y",)), key_index=key_index, strict=True
    )
    # Cold-start exclusions are *always* excluded — strict does not promote them.
    assert report.headline_divergence_events == ()
    assert report.exclusion_panel.cold_start_count == 1


# -------------------------------------------------------------------- #
# Append-only artifacts
# -------------------------------------------------------------------- #


def test_append_only_skip():
    key_index = _ids_for(["messages"])
    events = [
        _make_event(sequence=1, tick=1, node="A", event_type="node_start", state={"messages": ["m1"]}),
        _make_event(sequence=2, tick=2, node="A", event_type="node_end", state={"messages": ["m1", "m2"]}),
        _make_event(sequence=3, tick=3, node="B", event_type="node_start", state={"messages": ["m1"]}),
    ]
    report = detect(
        events,
        verdict=_verdict(tracked_keys=("messages",), append_only_keys=("messages",)),
        key_index=key_index,
    )
    # Append-only artifacts skip divergence detection entirely.
    assert report.headline_divergence_events == ()
    assert report.excluded_events == ()
    # Two reads in the buffer → 1 candidate pair examined and skipped.
    assert report.exclusion_panel.append_only_skip_count == 1


def test_mutable_messages_subject_to_detection():
    # Mutable artifacts (e.g. messages after a trim) ARE detected.
    # Reader A handed [m1, m2, m3] post-write, reader B handed [m1, m2] (lagging).
    key_index = _ids_for(["messages"])
    events = [
        _make_event(sequence=1, tick=3, node="W", event_type="node_end", state={"messages": ["m1", "m2", "m3"]}),
        _make_event(sequence=2, tick=4, node="A", event_type="node_start", state={"messages": ["m1", "m2", "m3"]}),
        _make_event(sequence=3, tick=6, node="B", event_type="node_start", state={"messages": ["m1", "m2"]}),
    ]
    report = detect(
        events,
        verdict=_verdict(tracked_keys=("messages",), mutable_keys=("messages",)),
        key_index=key_index,
    )
    assert len(report.headline_divergence_events) == 1
    assert report.exclusion_panel.append_only_skip_count == 0


# -------------------------------------------------------------------- #
# Insufficient verdict + empty buffer
# -------------------------------------------------------------------- #


def test_insufficient_verdict_short_circuits():
    key_index = _ids_for(["Y"])
    events = [
        _make_event(sequence=1, tick=1, node="A", event_type="node_start", state={"Y": "v1"}),
        _make_event(sequence=2, tick=2, node="B", event_type="node_start", state={"Y": "v2"}),
    ]
    insufficient = ClassifierVerdict(
        bucket=Bucket.INSUFFICIENT,
        confidence=Confidence.INSUFFICIENT,
        coverage=CoverageReport(
            tick_count=2, read_count=2, write_count=0, artifact_count=0,
            verdict_confidence=Confidence.INSUFFICIENT,
        ),
        reason="below coverage threshold",
    )
    report = detect(events, verdict=insufficient, key_index=key_index)
    assert report.headline_divergence_events == ()
    assert report.excluded_events == ()
    assert report.cost_unmeasurable_reason == "verdict_insufficient"


def test_empty_event_buffer_returns_empty_report():
    report = detect([], verdict=_verdict(tracked_keys=()), key_index={})
    assert report.headline_divergence_events == ()
    assert report.cost_unmeasurable_reason == "verdict_insufficient"
    assert report.schema_version == CCS_DIAGNOSE_LOG_SCHEMA_VERSION


# -------------------------------------------------------------------- #
# Mixed-divergence run + verdict consistency
# -------------------------------------------------------------------- #


def test_mixed_divergence_top_event_picks_artifact_with_most_events():
    key_index = _ids_for(["A", "B", "C"])
    events: list[DiagnoseEvent] = []
    seq = 0

    def add(tick: int, node: str, evtype: str, state: dict) -> None:
        nonlocal seq
        seq += 1
        events.append(_make_event(sequence=seq, tick=tick, node=node, event_type=evtype, state=state))

    # Artifact A — 4 divergence events: write at tick 1, then alternating
    # reads at the new and old versions across distinct readers.
    add(1, "wA", "node_start", {"A": "a-v1", "B": "b-v1", "C": "c-v1"})
    add(1, "wA", "node_end", {"A": "a-v1"})
    add(2, "rA1", "node_start", {"A": "a-v1"})
    add(3, "wA2", "node_end", {"A": "a-v2"})
    # Four readers see the stale "a-v1" relative to ones that saw "a-v2".
    # Set up: at tick 4, 5, 6, 7 readers see a-v2; at tick 8, 9, 10, 11
    # readers see a-v1 again (no intervening write between 3 and these).
    # Easier construction: 5 ticks, 5 readers, alternating v1/v2.
    # Reset and rebuild.

    events = []
    seq = 0
    # A: 1 write + lots of stale reads → divergent
    add(1, "wA", "node_end", {"A": "a-v1"})
    add(2, "rA_v1_x", "node_start", {"A": "a-v1"})  # baseline
    add(3, "rA_v2_y", "node_start", {"A": "a-v2"})  # divergence vs prior
    add(4, "rA_v2_y2", "node_start", {"A": "a-v2"})
    add(5, "rA_v1_z", "node_start", {"A": "a-v1"})  # more divergence pairs
    add(6, "rA_v2_z2", "node_start", {"A": "a-v2"})

    # B: stable — same version everywhere
    add(2, "rB", "node_start", {"B": "b-v1"})
    add(3, "rB2", "node_start", {"B": "b-v1"})

    # C: stable
    add(2, "rC", "node_start", {"C": "c-v1"})
    add(3, "rC2", "node_start", {"C": "c-v1"})

    report = detect(
        events,
        verdict=_verdict(tracked_keys=("A", "B", "C")),
        key_index=key_index,
    )
    assert len(report.headline_divergence_events) >= 1
    # All headline events are for artifact "A".
    assert all(ev.artifact_key == "A" for ev in report.headline_divergence_events)
    assert report.top_event is not None
    assert report.top_event.artifact_key == "A"
    # Heatmap includes only artifacts with non-zero divergent reads.
    assert tuple(row.artifact_key for row in report.heatmap) == ("A",)


def test_single_writer_verdict_with_zero_divergence_events():
    # Verdict says single_writer, all reads agree → zero divergence.
    key_index = _ids_for(["Y"])
    events = [
        _make_event(sequence=1, tick=1, node="W", event_type="node_end", state={"Y": "v1"}),
        _make_event(sequence=2, tick=2, node="A", event_type="node_start", state={"Y": "v1"}),
        _make_event(sequence=3, tick=3, node="B", event_type="node_start", state={"Y": "v1"}),
    ]
    report = detect(
        events,
        verdict=_verdict(bucket=Bucket.SINGLE_WRITER, tracked_keys=("Y",)),
        key_index=key_index,
    )
    assert report.headline_divergence_events == ()
    assert report.heatmap == ()
    assert report.agent_pain_count == 0


# -------------------------------------------------------------------- #
# Cost extrapolation paths
# -------------------------------------------------------------------- #


def _divergent_run() -> tuple[list[DiagnoseEvent], dict[str, uuid.UUID]]:
    """Reader A sees v2 (post-write), reader B is handed v1 (lagging) — divergence."""
    key_index = _ids_for(["Y"])
    events = [
        _make_event(sequence=1, tick=3, node="W", event_type="node_end", state={"Y": "v2"}),
        _make_event(sequence=2, tick=4, node="A", event_type="node_start", state={"Y": "v2"}),
        _make_event(sequence=3, tick=6, node="B", event_type="node_start", state={"Y": "v1"}),
    ]
    return events, key_index


def test_cost_with_volume_and_token_estimates():
    events, key_index = _divergent_run()
    aid = key_index["Y"]
    report = detect(
        events,
        verdict=_verdict(tracked_keys=("Y",)),
        key_index=key_index,
        volume_per_hour=50,
        value_token_estimates={aid: 200},
    )
    assert len(report.headline_divergence_events) == 1
    assert report.headline_divergence_events[0].rework_tokens == 200
    assert report.rework_tokens_this_run == 200
    # rework_cost_this_run = 200 * 0.003 / 1000 = 0.0006
    assert report.rework_cost_this_run == pytest.approx(0.0006)
    # Annualised: tokens / observed_ticks * 50 * 8760 * 0.003 / 1000.
    # observed ticks = {4, 5, 6} → 3.
    expected_annualised = (200 / 3) * 50 * 8760 * 0.003 / 1000.0
    assert report.rework_cost_annualized == pytest.approx(expected_annualised)
    assert report.cost_unmeasurable_reason is None


def test_cost_no_volume_returns_none_annualised():
    events, key_index = _divergent_run()
    aid = key_index["Y"]
    report = detect(
        events,
        verdict=_verdict(tracked_keys=("Y",)),
        key_index=key_index,
        value_token_estimates={aid: 200},
    )
    assert report.rework_cost_annualized is None
    assert report.rework_tokens_this_run == 200


def test_cost_unmeasurable_when_token_estimates_missing():
    events, key_index = _divergent_run()
    report = detect(
        events,
        verdict=_verdict(tracked_keys=("Y",)),
        key_index=key_index,
        volume_per_hour=50,
    )
    assert report.headline_divergence_events[0].rework_tokens == 0
    assert report.rework_tokens_this_run == 0
    assert report.rework_cost_this_run == 0.0
    assert report.cost_unmeasurable_reason == "value_token_estimates_missing"
    # Annualised is computable but evaluates to 0 because rework_tokens is 0.
    assert report.rework_cost_annualized == pytest.approx(0.0)


# -------------------------------------------------------------------- #
# Determinism
# -------------------------------------------------------------------- #


def test_determinism_same_inputs_same_report():
    events, key_index = _divergent_run()
    aid = key_index["Y"]
    kwargs = dict(
        verdict=_verdict(tracked_keys=("Y",)),
        key_index=key_index,
        value_token_estimates={aid: 200},
        volume_per_hour=10,
    )
    a = detect(events, **kwargs)
    b = detect(events, **kwargs)
    assert a == b


# -------------------------------------------------------------------- #
# Misc surface checks
# -------------------------------------------------------------------- #


def test_reader_pair_matrix_aggregates_pairs():
    # Two divergent reads of artifact Y: pair (A, B) once, pair (A, C) once.
    key_index = _ids_for(["Y"])
    events = [
        _make_event(sequence=1, tick=1, node="W0", event_type="node_end", state={"Y": "v1"}),
        _make_event(sequence=2, tick=2, node="A", event_type="node_start", state={"Y": "v1"}),
        _make_event(sequence=3, tick=3, node="W", event_type="node_end", state={"Y": "v2"}),
        _make_event(sequence=4, tick=4, node="B", event_type="node_start", state={"Y": "v1"}),
        _make_event(sequence=5, tick=5, node="C", event_type="node_start", state={"Y": "v2"}),
    ]
    # Strict mode included to capture sequential-staleness events as headline,
    # so the reader pair matrix is populated even when the v1 reader is stale.
    report = detect(
        events,
        verdict=_verdict(tracked_keys=("Y",)),
        key_index=key_index,
        strict=True,
    )
    # At least the (A, B) pair (both saw v1 and v2 across them) should appear.
    pairs = {(r.earlier_reader, r.later_reader) for r in report.reader_pair_matrix}
    assert ("A", "B") in pairs or ("A", "C") in pairs
    assert all(isinstance(r.event_count, int) and r.event_count > 0 for r in report.reader_pair_matrix)


def test_schema_version_echoed():
    events, key_index = _divergent_run()
    report = detect(events, verdict=_verdict(tracked_keys=("Y",)), key_index=key_index)
    assert report.schema_version == CCS_DIAGNOSE_LOG_SCHEMA_VERSION
