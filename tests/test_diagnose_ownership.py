# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Unit 5 tests — ownership-map helper for the HTML renderer."""

from __future__ import annotations

from diagnose_helpers import (
    ids_for as _ids_for,
)
from diagnose_helpers import (
    make_event as _make_event,
)

from ccs.diagnose.classifier import (
    Bucket,
    ClassifierVerdict,
    Confidence,
    CoverageReport,
)
from ccs.diagnose.ownership import OwnershipRow, compute_ownership_map


def _verdict(
    *,
    tracked_keys: tuple[str, ...],
    append_only_keys: tuple[str, ...] = (),
    bucket: Bucket = Bucket.SHARED_ARTIFACT,
) -> ClassifierVerdict:
    return ClassifierVerdict(
        bucket=bucket,
        confidence=Confidence.PRELIMINARY,
        coverage=CoverageReport(
            tick_count=20,
            read_count=20,
            write_count=4,
            artifact_count=len(tracked_keys),
            verdict_confidence=Confidence.PRELIMINARY,
        ),
        tracked_keys=tracked_keys,
        append_only_keys=append_only_keys,
    )


# -------------------------------------------------------------------- #
# Compute_ownership_map
# -------------------------------------------------------------------- #


def test_empty_inputs_returns_empty_tuple():
    rows = compute_ownership_map([], _verdict(tracked_keys=()), {})
    assert rows == ()


def test_no_tracked_keys_returns_empty_tuple():
    events = [
        _make_event(sequence=1, tick=0, node="a", event_type="node_end", state={"plan": "v1"})
    ]
    rows = compute_ownership_map(
        events, _verdict(tracked_keys=()), _ids_for(["plan"])
    )
    assert rows == ()


def test_writer_count_aggregation_per_artifact():
    events = [
        _make_event(sequence=1, tick=0, node="planner", event_type="node_end", state={"plan": "v1"}),
        _make_event(sequence=2, tick=1, node="planner", event_type="node_end", state={"plan": "v2"}),
        _make_event(sequence=3, tick=2, node="planner", event_type="node_end", state={"plan": "v3"}),
    ]
    rows = compute_ownership_map(
        events,
        _verdict(tracked_keys=("plan",)),
        _ids_for(["plan"]),
    )
    assert len(rows) == 1
    row = rows[0]
    assert row.artifact_key == "plan"
    assert row.writers == (("planner", 3),)


def test_reader_count_aggregation_per_artifact():
    events = [
        _make_event(
            sequence=1,
            tick=0,
            node="researcher",
            event_type="node_start",
            state={"plan": "v1"},
        ),
        _make_event(
            sequence=2,
            tick=1,
            node="researcher",
            event_type="node_start",
            state={"plan": "v1"},
        ),
        _make_event(
            sequence=3,
            tick=2,
            node="executor",
            event_type="node_start",
            state={"plan": "v1"},
        ),
    ]
    rows = compute_ownership_map(
        events,
        _verdict(tracked_keys=("plan",)),
        _ids_for(["plan"]),
    )
    assert rows[0].readers == (("researcher", 2), ("executor", 1))


def test_multi_writer_rows_sorted_first():
    events = [
        # Single-writer artifact with many reads
        _make_event(sequence=1, tick=0, node="a", event_type="node_end", state={"single": "v1"}),
        _make_event(sequence=2, tick=1, node="r1", event_type="node_start", state={"single": "v1"}),
        _make_event(sequence=3, tick=2, node="r1", event_type="node_start", state={"single": "v1"}),
        _make_event(sequence=4, tick=3, node="r1", event_type="node_start", state={"single": "v1"}),
        # Multi-writer artifact with fewer reads
        _make_event(sequence=5, tick=0, node="a", event_type="node_end", state={"shared": "v1"}),
        _make_event(sequence=6, tick=1, node="b", event_type="node_end", state={"shared": "v2"}),
        _make_event(sequence=7, tick=2, node="r1", event_type="node_start", state={"shared": "v2"}),
    ]
    rows = compute_ownership_map(
        events,
        _verdict(tracked_keys=("single", "shared")),
        _ids_for(["single", "shared"]),
    )
    # multi-writer first, despite single having more reads
    assert rows[0].artifact_key == "shared"
    assert rows[1].artifact_key == "single"


def test_multi_writer_rows_secondary_sort_by_total_reads_desc():
    events = []
    seq = 1
    # Two multi-writer artifacts: alpha has 1 read, bravo has 5 reads
    for tick, node in enumerate(("a", "b"), start=0):
        events.append(_make_event(
            sequence=seq, tick=tick, node=node, event_type="node_end",
            state={"alpha": f"v{tick}"},
        ))
        seq += 1
    for tick, node in enumerate(("a", "b"), start=10):
        events.append(_make_event(
            sequence=seq, tick=tick, node=node, event_type="node_end",
            state={"bravo": f"v{tick}"},
        ))
        seq += 1
    # alpha: 1 reader
    events.append(_make_event(
        sequence=seq, tick=20, node="r1", event_type="node_start",
        state={"alpha": "v0"},
    )); seq += 1
    # bravo: 5 readers
    for i in range(5):
        events.append(_make_event(
            sequence=seq, tick=20 + i, node=f"r{i}", event_type="node_start",
            state={"bravo": "v10"},
        ))
        seq += 1
    rows = compute_ownership_map(
        events,
        _verdict(tracked_keys=("alpha", "bravo")),
        _ids_for(["alpha", "bravo"]),
    )
    # bravo has more reads, both multi-writer
    assert [r.artifact_key for r in rows] == ["bravo", "alpha"]


def test_within_single_writer_group_sorted_by_reads_desc():
    events = []
    seq = 1
    # Artifact "low" with 1 read; "high" with 3 reads. Both single-writer.
    events.append(_make_event(
        sequence=seq, tick=0, node="w", event_type="node_end", state={"low": "v1"},
    )); seq += 1
    events.append(_make_event(
        sequence=seq, tick=0, node="w", event_type="node_end", state={"high": "v1"},
    )); seq += 1
    events.append(_make_event(
        sequence=seq, tick=1, node="r", event_type="node_start", state={"low": "v1"},
    )); seq += 1
    for _ in range(3):
        events.append(_make_event(
            sequence=seq, tick=2, node="r", event_type="node_start", state={"high": "v1"},
        ))
        seq += 1
    rows = compute_ownership_map(
        events,
        _verdict(tracked_keys=("low", "high")),
        _ids_for(["low", "high"]),
    )
    assert [r.artifact_key for r in rows] == ["high", "low"]


def test_append_only_flag_mirrors_verdict():
    events = [
        _make_event(sequence=1, tick=0, node="w", event_type="node_end", state={"messages": "v1"}),
        _make_event(sequence=2, tick=1, node="w", event_type="node_end", state={"plan": "v1"}),
    ]
    rows = compute_ownership_map(
        events,
        _verdict(tracked_keys=("messages", "plan"), append_only_keys=("messages",)),
        _ids_for(["messages", "plan"]),
    )
    by_key = {r.artifact_key: r for r in rows}
    assert by_key["messages"].append_only is True
    assert by_key["plan"].append_only is False


def test_version_range_single_distinct_version():
    events = [
        _make_event(
            sequence=1,
            tick=0,
            node="w",
            event_type="node_end",
            state={"plan": "x"},
            explicit_versions={"plan": "1"},
        ),
        _make_event(
            sequence=2,
            tick=1,
            node="r",
            event_type="node_start",
            state={"plan": "x"},
            explicit_versions={"plan": "1"},
        ),
    ]
    rows = compute_ownership_map(
        events,
        _verdict(tracked_keys=("plan",)),
        _ids_for(["plan"]),
    )
    assert rows[0].version_range == "v1"


def test_version_range_distinct_first_and_last():
    events = [
        _make_event(
            sequence=1, tick=0, node="w", event_type="node_end",
            state={"plan": "a"}, explicit_versions={"plan": "1"},
        ),
        _make_event(
            sequence=2, tick=1, node="w", event_type="node_end",
            state={"plan": "b"}, explicit_versions={"plan": "2"},
        ),
        _make_event(
            sequence=3, tick=2, node="w", event_type="node_end",
            state={"plan": "c"}, explicit_versions={"plan": "3"},
        ),
    ]
    rows = compute_ownership_map(
        events,
        _verdict(tracked_keys=("plan",)),
        _ids_for(["plan"]),
    )
    assert rows[0].version_range == "v1 -> v3"


def test_version_range_long_hex_truncated():
    long_hash = "a1b2c3d4e5f6789012345678"
    other = "deadbeefcafefoo"  # not hex; keeps verbatim
    events = [
        _make_event(
            sequence=1, tick=0, node="w", event_type="node_end",
            state={"plan": "x"}, explicit_versions={"plan": long_hash},
        ),
        _make_event(
            sequence=2, tick=1, node="w", event_type="node_end",
            state={"plan": "y"}, explicit_versions={"plan": other},
        ),
    ]
    rows = compute_ownership_map(
        events,
        _verdict(tracked_keys=("plan",)),
        _ids_for(["plan"]),
    )
    # First version is hex; truncated to 8. Second is non-hex; verbatim.
    assert rows[0].version_range == f"a1b2c3d4 -> {other}"


def test_unobserved_tracked_key_omitted_when_not_in_index():
    events = [
        _make_event(sequence=1, tick=0, node="w", event_type="node_end", state={"plan": "v1"}),
    ]
    # tracked_keys includes "ghost" but no index entry
    rows = compute_ownership_map(
        events,
        _verdict(tracked_keys=("plan", "ghost")),
        _ids_for(["plan"]),
    )
    assert {r.artifact_key for r in rows} == {"plan"}


def test_empty_node_string_writer_dropped():
    events = [
        _make_event(sequence=1, tick=0, node="", event_type="node_end", state={"plan": "v1"}),
        _make_event(sequence=2, tick=1, node="planner", event_type="node_end", state={"plan": "v2"}),
    ]
    rows = compute_ownership_map(
        events,
        _verdict(tracked_keys=("plan",)),
        _ids_for(["plan"]),
    )
    # The empty-string node should be dropped; only `planner` survives.
    assert rows[0].writers == (("planner", 1),)


def test_writer_tie_break_by_name_asc():
    events = [
        _make_event(sequence=1, tick=0, node="zeta", event_type="node_end", state={"plan": "v1"}),
        _make_event(sequence=2, tick=1, node="alpha", event_type="node_end", state={"plan": "v2"}),
    ]
    rows = compute_ownership_map(
        events,
        _verdict(tracked_keys=("plan",)),
        _ids_for(["plan"]),
    )
    # both at count 1, alpha should come before zeta
    assert rows[0].writers == (("alpha", 1), ("zeta", 1))


def test_deterministic_output():
    events = [
        _make_event(sequence=1, tick=0, node="a", event_type="node_end", state={"x": "v1", "y": "v1"}),
        _make_event(sequence=2, tick=1, node="b", event_type="node_end", state={"x": "v2"}),
        _make_event(sequence=3, tick=2, node="r", event_type="node_start", state={"x": "v2"}),
    ]
    verdict = _verdict(tracked_keys=("x", "y"))
    idx = _ids_for(["x", "y"])
    a = compute_ownership_map(events, verdict, idx)
    b = compute_ownership_map(events, verdict, idx)
    assert a == b


def test_returns_ownership_row_instances():
    events = [
        _make_event(sequence=1, tick=0, node="a", event_type="node_end", state={"plan": "v1"}),
    ]
    rows = compute_ownership_map(
        events, _verdict(tracked_keys=("plan",)), _ids_for(["plan"])
    )
    assert all(isinstance(row, OwnershipRow) for row in rows)
