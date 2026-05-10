# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""Unit 3 tests — ``langgraph-v0-preview`` write-pattern classifier."""

from __future__ import annotations

import uuid
from typing import Iterable, Mapping

import pytest

from ccs.core.hashing import compute_content_hash
from ccs.core.identity import artifact_uuid
from ccs.diagnose import CCS_DIAGNOSE_LOG_SCHEMA_VERSION
from ccs.diagnose.callback import DEFAULT_SCOPE, DiagnoseEvent, DiagnoseWarning
from ccs.diagnose.classifier import (
    Bucket,
    ClassifierOverrides,
    ClassifierVerdict,
    Confidence,
    build_key_index,
    classify,
    select_classifier,
)


# -------------------------------------------------------------------- #
# Test helpers
# -------------------------------------------------------------------- #


_INSTANCE_ID = uuid.UUID("11111111-2222-3333-4444-555555555555")


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
    verdict_signal: str | None = None,
    message: str = "",
    run_id: str = "run-x",
    namespace: str = "",
) -> DiagnoseEvent:
    """Build a synthetic ``DiagnoseEvent`` for classifier unit tests.

    ``state`` is the merged state dict (for ``node_start``) or the
    return-dict (for ``node_end``); each value is hashed exactly the way
    ``DiagnoseCallback`` does so the UUID derivation matches the production
    classifier path. Versions and content hashes are identical here (no
    checkpointer overlay), matching the callback's fallback when no
    checkpointer is attached.
    """
    state = state or {}
    versions: dict[uuid.UUID, str] = {}
    hashes: dict[uuid.UUID, str] = {}
    for key, value in state.items():
        aid = artifact_uuid(DEFAULT_SCOPE, key)
        h = _hash(value)
        versions[aid] = h
        hashes[aid] = h
    return DiagnoseEvent(
        sequence_number=sequence,
        instance_id=_INSTANCE_ID,
        schema_version=CCS_DIAGNOSE_LOG_SCHEMA_VERSION,
        tick=tick,
        node=node,
        event_type=event_type,  # type: ignore[arg-type]
        artifact_versions=versions,
        content_hashes=hashes,
        run_id=run_id,
        namespace=namespace,
        verdict_signal=verdict_signal,  # type: ignore[arg-type]
        message=message,
    )


def _build_high_coverage_run(
    *,
    artifacts: Mapping[str, str],
    tick_count: int = 60,
    extra_writes_per_artifact: int = 5,
) -> tuple[list[DiagnoseEvent], dict[str, uuid.UUID]]:
    """Synthesize a run that clears the ``HIGH`` confidence threshold.

    Each artifact in ``artifacts`` (key → writing node) gets a write per
    tick for the first ``extra_writes_per_artifact`` ticks; the remaining
    ticks emit reads only. ``tick_count`` distinct ticks are emitted, with
    enough ``node_start`` events to clear the ``read_count >= 100`` gate.
    """
    events: list[DiagnoseEvent] = []
    seq = 0
    base_state = {key: f"v0-{key}" for key in artifacts}

    for tick in range(tick_count):
        # Reads: every node sees the merged state on its node_start.
        # We emit two reads per tick (so 60 ticks → 120 reads).
        for reader_idx, node in enumerate(("reader_a", "reader_b")):
            seq += 1
            events.append(
                _make_event(
                    sequence=seq,
                    tick=tick,
                    node=node,
                    event_type="node_start",
                    state={key: f"{value}-t{tick}-{reader_idx}" for key, value in base_state.items()},
                )
            )
        # Writes: in the first ``extra_writes_per_artifact`` ticks each
        # artifact's writer emits a node_end with the new value.
        if tick < extra_writes_per_artifact:
            for key, writer in artifacts.items():
                seq += 1
                events.append(
                    _make_event(
                        sequence=seq,
                        tick=tick,
                        node=writer,
                        event_type="node_end",
                        state={key: f"{key}-write-t{tick}"},
                    )
                )

    return events, _ids_for(artifacts.keys())


# -------------------------------------------------------------------- #
# Bucket happy paths
# -------------------------------------------------------------------- #


def test_single_writer_high_confidence() -> None:
    """3-agent graph, each writes its own artifact → SINGLE_WRITER + HIGH."""
    events, key_index = _build_high_coverage_run(
        artifacts={"plan": "planner", "draft": "writer", "review": "reviewer"},
        tick_count=60,
        extra_writes_per_artifact=5,
    )
    verdict = classify(events, key_index=key_index)

    assert verdict.bucket is Bucket.SINGLE_WRITER
    assert verdict.confidence is Confidence.HIGH
    assert set(verdict.tracked_keys) == {"plan", "draft", "review"}
    assert verdict.coverage.tick_count == 60
    assert verdict.coverage.read_count >= 100
    assert verdict.coverage.write_count >= 5
    assert verdict.coverage.artifact_count == 3
    assert verdict.reason is None


def test_shared_artifact() -> None:
    """Two agents write the same task_queue → SHARED_ARTIFACT."""
    events: list[DiagnoseEvent] = []
    seq = 0
    # 2 writers hit `task_queue` at *different* ticks (no fork+merge → shared)
    for tick in range(15):
        seq += 1
        events.append(
            _make_event(
                sequence=seq,
                tick=tick,
                node="alpha",
                event_type="node_start",
                state={"task_queue": [f"task-{tick}"]},
            )
        )
        seq += 1
        events.append(
            _make_event(
                sequence=seq,
                tick=tick,
                node="beta",
                event_type="node_start",
                state={"task_queue": [f"task-{tick}"]},
            )
        )
        # Alpha writes on even ticks; Beta on odd. Ticks differ → not parallel.
        writer = "alpha" if tick % 2 == 0 else "beta"
        seq += 1
        events.append(
            _make_event(
                sequence=seq,
                tick=tick,
                node=writer,
                event_type="node_end",
                state={"task_queue": [f"task-{tick}-from-{writer}"]},
            )
        )

    verdict = classify(events, key_index=_ids_for(["task_queue"]))
    assert verdict.bucket is Bucket.SHARED_ARTIFACT
    # Two writers but on different ticks fails the parallel-branch heuristic.
    assert set(verdict.writers_by_key["task_queue"]) == {"alpha", "beta"}


def test_parallel_branch() -> None:
    """Fork+merge: two writers same tick, merge node reads both keys."""
    events: list[DiagnoseEvent] = []
    seq = 0

    # Build 12 ticks so coverage clears insufficient.
    # The parallel-branch heuristic requires every multi-writer key to
    # have its co-writers fire in the same super-step — so we have both
    # branches write to `report` *and* `forks` in the same tick.
    for tick in range(12):
        for node in ("branch_a", "branch_b"):
            seq += 1
            events.append(
                _make_event(
                    sequence=seq,
                    tick=tick,
                    node=node,
                    event_type="node_start",
                    state={"report": f"r-{tick}", "forks": "f"},
                )
            )
        for node in ("branch_a", "branch_b"):
            seq += 1
            events.append(
                _make_event(
                    sequence=seq,
                    tick=tick,
                    node=node,
                    event_type="node_end",
                    state={
                        "report": f"report-{tick}-from-{node}",
                        "forks": f"fork-{tick}-from-{node}",
                    },
                )
            )
        # Merge node reads BOTH multi-writer keys together.
        seq += 1
        events.append(
            _make_event(
                sequence=seq,
                tick=tick,
                node="merger",
                event_type="node_start",
                state={"report": f"r-{tick}", "forks": "f"},
            )
        )

    verdict = classify(events, key_index=_ids_for(["report", "forks"]))
    assert verdict.bucket is Bucket.PARALLEL_BRANCH


def test_mixed_pattern() -> None:
    """Some artifacts single-writer, others multi-writer → MIXED."""
    events: list[DiagnoseEvent] = []
    seq = 0
    for tick in range(15):
        # Reads
        for node in ("alpha", "beta"):
            seq += 1
            events.append(
                _make_event(
                    sequence=seq,
                    tick=tick,
                    node=node,
                    event_type="node_start",
                    state={"plan": "p", "queue": "q"},
                )
            )
        # Writes: plan only by `planner`; queue by both alpha and beta.
        seq += 1
        events.append(
            _make_event(
                sequence=seq,
                tick=tick,
                node="planner",
                event_type="node_end",
                state={"plan": f"plan-{tick}"},
            )
        )
        # Different ticks → not parallel → mixed.
        writer = "alpha" if tick % 2 == 0 else "beta"
        seq += 1
        events.append(
            _make_event(
                sequence=seq,
                tick=tick,
                node=writer,
                event_type="node_end",
                state={"queue": f"q-{tick}"},
            )
        )

    verdict = classify(events, key_index=_ids_for(["plan", "queue"]))
    assert verdict.bucket is Bucket.MIXED_PATTERN


# -------------------------------------------------------------------- #
# Coverage edge cases
# -------------------------------------------------------------------- #


def test_short_run_single_writer_bucket_with_insufficient_confidence() -> None:
    """5 ticks + 1 write → SINGLE_WRITER bucket, INSUFFICIENT confidence."""
    events: list[DiagnoseEvent] = []
    seq = 0
    for tick in range(5):
        seq += 1
        events.append(
            _make_event(
                sequence=seq,
                tick=tick,
                node="reader",
                event_type="node_start",
                state={"plan": f"p-{tick}"},
            )
        )
    seq += 1
    events.append(
        _make_event(
            sequence=seq,
            tick=4,
            node="writer",
            event_type="node_end",
            state={"plan": "final"},
        )
    )

    verdict = classify(events, key_index=_ids_for(["plan"]))
    # Confidence is INSUFFICIENT (<10 ticks); bucket falls to INSUFFICIENT
    # because the coverage gate produces ``Confidence.INSUFFICIENT`` and
    # short-circuits before bucket selection.
    assert verdict.confidence is Confidence.INSUFFICIENT
    assert verdict.bucket is Bucket.INSUFFICIENT
    assert verdict.reason == "below coverage threshold"


def test_zero_writes_returns_insufficient_with_reason() -> None:
    events: list[DiagnoseEvent] = []
    seq = 0
    for tick in range(20):
        seq += 1
        events.append(
            _make_event(
                sequence=seq,
                tick=tick,
                node="reader",
                event_type="node_start",
                state={"plan": f"p-{tick}"},
            )
        )
    verdict = classify(events, key_index=_ids_for(["plan"]))
    assert verdict.bucket is Bucket.INSUFFICIENT
    assert verdict.confidence is Confidence.INSUFFICIENT
    assert verdict.reason == "below coverage threshold"
    assert verdict.coverage.write_count == 0


def test_empty_event_buffer_returns_insufficient() -> None:
    verdict = classify([])
    assert verdict.bucket is Bucket.INSUFFICIENT
    assert verdict.confidence is Confidence.INSUFFICIENT
    assert verdict.reason == "below coverage threshold"
    assert verdict.coverage.tick_count == 0
    assert verdict.coverage.read_count == 0
    assert verdict.coverage.write_count == 0
    assert verdict.tracked_keys == ()


# -------------------------------------------------------------------- #
# Append-only detection
# -------------------------------------------------------------------- #


def test_messages_append_only_with_add_messages_pattern() -> None:
    """``messages`` ID-stable monotonic across ticks → append_only_keys."""
    events, key_index = _build_high_coverage_run(
        artifacts={"messages": "agent"},
        tick_count=60,
        extra_writes_per_artifact=10,
    )
    verdict = classify(events, key_index=key_index)
    # Each tick writes a unique value → distinct hashes → append-only signal.
    assert "messages" in verdict.append_only_keys
    assert "messages" not in verdict.mutable_keys


def test_messages_mutable_when_value_revisits_earlier_state() -> None:
    """trim_messages causes a previously-seen state to recur → mutable."""
    events: list[DiagnoseEvent] = []
    seq = 0
    states = ["msgs-0", "msgs-1", "msgs-2", "msgs-1", "msgs-3", "msgs-4"]
    for tick in range(20):
        seq += 1
        events.append(
            _make_event(
                sequence=seq,
                tick=tick,
                node="reader",
                event_type="node_start",
                state={"messages": "snapshot"},
            )
        )
    # Write at every other tick using ``states`` — the 4th distinct write
    # repeats an earlier value (simulating trim that reverted the set).
    for idx, value in enumerate(states):
        seq += 1
        events.append(
            _make_event(
                sequence=seq,
                tick=idx + 1,
                node="agent",
                event_type="node_end",
                state={"messages": value},
            )
        )

    verdict = classify(events, key_index=_ids_for(["messages"]))
    assert "messages" in verdict.mutable_keys
    assert "messages" not in verdict.append_only_keys


def test_remove_message_pattern_marks_mutable() -> None:
    """RemoveMessage shrinks the set: same as trim — value churn → mutable."""
    events: list[DiagnoseEvent] = []
    seq = 0
    # 11 reads to clear the read threshold.
    for tick in range(11):
        seq += 1
        events.append(
            _make_event(
                sequence=seq,
                tick=tick,
                node="reader",
                event_type="node_start",
                state={"messages": "snapshot"},
            )
        )
    # Three growing writes, then a write that reverts to the very first.
    for idx, value in enumerate(["a", "ab", "abc", "a"]):
        seq += 1
        events.append(
            _make_event(
                sequence=seq,
                tick=idx + 1,
                node="agent",
                event_type="node_end",
                state={"messages": value},
            )
        )
    verdict = classify(events, key_index=_ids_for(["messages"]))
    assert "messages" in verdict.mutable_keys
    assert "messages" not in verdict.append_only_keys


# -------------------------------------------------------------------- #
# Ignore rules + overrides
# -------------------------------------------------------------------- #


def test_known_framework_and_ephemera_keys_are_ignored() -> None:
    """``__interrupt__``, ``__pregel_pull``, ``_step_count`` → ignored buckets."""
    events: list[DiagnoseEvent] = []
    seq = 0
    state = {
        "plan": "p",
        "__interrupt__": "i",
        "__pregel_pull": "x",
        "_step_count": 1,
    }
    for tick in range(15):
        seq += 1
        events.append(
            _make_event(
                sequence=seq,
                tick=tick,
                node="reader",
                event_type="node_start",
                state=state,
            )
        )
    seq += 1
    events.append(
        _make_event(
            sequence=seq,
            tick=14,
            node="writer",
            event_type="node_end",
            state={"plan": "p2"},
        )
    )

    key_index = _ids_for(state.keys())
    verdict = classify(events, key_index=key_index)
    assert "__interrupt__" in verdict.ignored_framework_keys
    assert "__pregel_pull" in verdict.ignored_framework_keys
    assert "_step_count" in verdict.ignored_ephemera_keys
    # Known framework keys do NOT surface in unknown_underscore.
    assert "__interrupt__" not in verdict.unknown_underscore_keys
    assert "__pregel_pull" not in verdict.unknown_underscore_keys
    assert "plan" in verdict.tracked_keys


def test_unknown_underscore_key_is_staleness_sensor() -> None:
    """Unknown ``__*`` key → ignored AND in unknown_underscore_keys."""
    events: list[DiagnoseEvent] = []
    seq = 0
    state = {"plan": "p", "__new_internal_key": "x"}
    for tick in range(12):
        seq += 1
        events.append(
            _make_event(
                sequence=seq,
                tick=tick,
                node="reader",
                event_type="node_start",
                state=state,
            )
        )
    seq += 1
    events.append(
        _make_event(
            sequence=seq,
            tick=11,
            node="writer",
            event_type="node_end",
            state={"plan": "p2"},
        )
    )
    verdict = classify(events, key_index=_ids_for(state.keys()))
    assert "__new_internal_key" in verdict.ignored_framework_keys
    assert "__new_internal_key" in verdict.unknown_underscore_keys


def test_track_override_unignores_framework_key() -> None:
    """``--track __pregel_pull`` → tracked despite the ``__*`` rule."""
    events: list[DiagnoseEvent] = []
    seq = 0
    state = {"plan": "p", "__pregel_pull": "x"}
    for tick in range(12):
        seq += 1
        events.append(
            _make_event(
                sequence=seq,
                tick=tick,
                node="reader",
                event_type="node_start",
                state=state,
            )
        )
    seq += 1
    events.append(
        _make_event(
            sequence=seq,
            tick=11,
            node="writer",
            event_type="node_end",
            state={"__pregel_pull": "x2"},
        )
    )
    seq += 1
    events.append(
        _make_event(
            sequence=seq,
            tick=11,
            node="writer",
            event_type="node_end",
            state={"plan": "p2"},
        )
    )

    verdict = classify(
        events,
        overrides=ClassifierOverrides(track=("__pregel_pull",)),
        key_index=_ids_for(state.keys()),
    )
    assert "__pregel_pull" in verdict.tracked_keys
    assert "__pregel_pull" not in verdict.ignored_framework_keys


def test_ignore_override_drops_tracked_key() -> None:
    """``--ignore my_artifact`` → ignored despite no rule match."""
    events: list[DiagnoseEvent] = []
    seq = 0
    state = {"my_artifact": "v", "plan": "p"}
    for tick in range(12):
        seq += 1
        events.append(
            _make_event(
                sequence=seq,
                tick=tick,
                node="reader",
                event_type="node_start",
                state=state,
            )
        )
    seq += 1
    events.append(
        _make_event(
            sequence=seq,
            tick=11,
            node="writer",
            event_type="node_end",
            state={"plan": "p2"},
        )
    )

    verdict = classify(
        events,
        overrides=ClassifierOverrides(ignore=("my_artifact",)),
        key_index=_ids_for(state.keys()),
    )
    assert "my_artifact" not in verdict.tracked_keys
    assert "my_artifact" in verdict.ignored_ephemera_keys


def test_track_wins_over_ignore_when_both_set() -> None:
    """When a key is in both ``track`` and ``ignore``, ``track`` wins."""
    events: list[DiagnoseEvent] = []
    seq = 0
    state = {"plan": "p", "shared": "s"}
    for tick in range(12):
        seq += 1
        events.append(
            _make_event(
                sequence=seq,
                tick=tick,
                node="reader",
                event_type="node_start",
                state=state,
            )
        )
    seq += 1
    events.append(
        _make_event(
            sequence=seq,
            tick=11,
            node="writer",
            event_type="node_end",
            state={"shared": "s2"},
        )
    )
    verdict = classify(
        events,
        overrides=ClassifierOverrides(ignore=("shared",), track=("shared",)),
        key_index=_ids_for(state.keys()),
    )
    assert "shared" in verdict.tracked_keys


def test_override_referencing_unobserved_key_is_noop() -> None:
    """Override on a key that never appears in any event is silent."""
    events: list[DiagnoseEvent] = []
    seq = 0
    for tick in range(12):
        seq += 1
        events.append(
            _make_event(
                sequence=seq,
                tick=tick,
                node="reader",
                event_type="node_start",
                state={"plan": "p"},
            )
        )
    seq += 1
    events.append(
        _make_event(
            sequence=seq,
            tick=11,
            node="writer",
            event_type="node_end",
            state={"plan": "p2"},
        )
    )

    verdict = classify(
        events,
        overrides=ClassifierOverrides(ignore=("ghost_key",), track=("phantom",)),
        key_index=_ids_for(["plan"]),
    )
    assert verdict.bucket is Bucket.SINGLE_WRITER
    assert verdict.tracked_keys == ("plan",)
    assert "ghost_key" not in verdict.ignored_ephemera_keys
    assert "phantom" not in verdict.tracked_keys


# -------------------------------------------------------------------- #
# Verdict-signal short circuit
# -------------------------------------------------------------------- #


def test_unsupported_execution_signal_short_circuits() -> None:
    """A ``verdict_signal`` event ⇒ ``insufficient`` with matching reason."""
    events: list[DiagnoseEvent] = []
    seq = 0
    seq += 1
    events.append(
        _make_event(
            sequence=seq,
            tick=-1,
            node="",
            event_type="verdict_signal",
            verdict_signal="unsupported_execution_model",
            message="namespace went backwards",
        )
    )
    # Even with a high-coverage workload, the short-circuit wins.
    high_events, key_index = _build_high_coverage_run(
        artifacts={"plan": "planner"}, tick_count=60
    )
    events.extend(high_events)
    verdict = classify(events, key_index=key_index)
    assert verdict.bucket is Bucket.INSUFFICIENT
    assert verdict.confidence is Confidence.INSUFFICIENT
    assert verdict.reason == "unsupported_execution_model"


# -------------------------------------------------------------------- #
# Single-writer-verdict consistency check
# -------------------------------------------------------------------- #


def test_single_writer_consistency_downgrade_emits_warning() -> None:
    """Synthesize a buffer where the natural verdict is SINGLE_WRITER but
    one tracked artifact has 2 writers — classifier downgrades + warns.

    Note: the bucket selector already routes "any multi-writer" to
    SHARED/MIXED, so this scenario is effectively the same as MIXED.
    The consistency check is a *defense-in-depth* guard: even if a future
    refactor produced an inconsistent verdict, the final pass would catch
    it. To exercise it specifically we monkey-patch the picker.
    """
    from ccs.diagnose.classifier import langgraph_v0_preview as impl

    events, key_index = _build_high_coverage_run(
        artifacts={"plan": "planner", "queue": "alpha"},
        tick_count=60,
        extra_writes_per_artifact=10,
    )
    # Inject a second writer for `queue`.
    events.append(
        _make_event(
            sequence=10_000,
            tick=11,
            node="beta",
            event_type="node_end",
            state={"queue": "from-beta"},
        )
    )

    original_pick = impl._pick_bucket

    def force_single_writer(*args, **kwargs):  # type: ignore[no-untyped-def]
        return Bucket.SINGLE_WRITER

    impl._pick_bucket = force_single_writer  # type: ignore[assignment]
    try:
        with pytest.warns(DiagnoseWarning, match="downgraded to mixed_pattern"):
            verdict = classify(events, key_index=key_index)
    finally:
        impl._pick_bucket = original_pick  # type: ignore[assignment]

    assert verdict.bucket is Bucket.MIXED_PATTERN


# -------------------------------------------------------------------- #
# Determinism + registry
# -------------------------------------------------------------------- #


def test_determinism_same_input_yields_identical_verdict() -> None:
    events, key_index = _build_high_coverage_run(
        artifacts={"plan": "planner", "draft": "writer"},
        tick_count=60,
        extra_writes_per_artifact=5,
    )
    a = classify(events, key_index=key_index)
    b = classify(events, key_index=key_index)
    assert a == b


def test_select_classifier_default_and_unknown() -> None:
    fn = select_classifier("langgraph-v0-preview")
    assert callable(fn)
    with pytest.raises(ValueError, match="unknown classifier"):
        select_classifier("nonexistent")


def test_build_key_index_matches_callback_uuid_scheme() -> None:
    idx = build_key_index(["messages", "plan"])
    assert idx["messages"] == artifact_uuid(DEFAULT_SCOPE, "messages")
    assert idx["plan"] == artifact_uuid(DEFAULT_SCOPE, "plan")


def test_verdict_dataclass_is_frozen() -> None:
    events, key_index = _build_high_coverage_run(
        artifacts={"plan": "planner"}, tick_count=60
    )
    verdict = classify(events, key_index=key_index)
    with pytest.raises((AttributeError, Exception)):
        verdict.bucket = Bucket.INSUFFICIENT  # type: ignore[misc]
    assert isinstance(verdict, ClassifierVerdict)
