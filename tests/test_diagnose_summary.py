# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""Unit 6 tests — terminal summary string-builder.

Coverage:

* All 3 variant snapshots (divergence, zero-events, insufficient).
* Conditional cost-line rendering and ``--strict`` marker.
* Truncation rules for node names (30) and artifact keys (40).
* Unicode handling — non-ASCII keys / emoji nodes.
* ``line_width`` parameter accepted; default produces ≤80-char lines for
  the typical inputs (truncation thresholds keep us under 80).
* Determinism — same inputs yield identical strings on every call.
* No ANSI escape sequences anywhere in the output.
* Last line of every variant ends with the supplied ``html_path``.
"""

from __future__ import annotations

import uuid
from pathlib import Path

import pytest

from ccs.diagnose import CCS_DIAGNOSE_LOG_SCHEMA_VERSION
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
    ReadObservation,
)
from ccs.diagnose.summary import terminal_summary


# -------------------------------------------------------------------- #
# Builder helpers — keep tests terse; no production side effects
# -------------------------------------------------------------------- #


HTML_PATH = Path("/tmp/diagnose_report.html")


def _aid(seed: int = 0) -> uuid.UUID:
    return uuid.UUID(int=seed)


def _coverage(
    *,
    confidence: Confidence = Confidence.HIGH,
    artifact_count: int = 3,
    tick_count: int = 30,
    read_count: int = 60,
    write_count: int = 12,
) -> CoverageReport:
    return CoverageReport(
        tick_count=tick_count,
        read_count=read_count,
        write_count=write_count,
        artifact_count=artifact_count,
        verdict_confidence=confidence,
    )


def _verdict(
    *,
    bucket: Bucket = Bucket.SHARED_ARTIFACT,
    confidence: Confidence = Confidence.HIGH,
    tracked_keys: tuple[str, ...] = ("plan", "notes", "task_queue"),
    writers_by_key: dict[str, tuple[str, ...]] | None = None,
    reason: str | None = None,
    coverage: CoverageReport | None = None,
) -> ClassifierVerdict:
    return ClassifierVerdict(
        bucket=bucket,
        confidence=confidence,
        coverage=coverage
        or _coverage(confidence=confidence, artifact_count=len(tracked_keys)),
        tracked_keys=tracked_keys,
        reason=reason,
        writers_by_key=writers_by_key or {},
    )


def _read(
    *,
    node: str,
    tick: int,
    version: str = "1",
    content_hash: str = "h",
) -> ReadObservation:
    return ReadObservation(
        node=node, tick=tick, version=version, content_hash=content_hash
    )


def _event(
    *,
    artifact_key: str = "plan",
    artifact_id: uuid.UUID | None = None,
    earlier_node: str = "researcher",
    later_node: str = "executor",
    earlier_version: str = "1",
    later_version: str = "1",
    later_tick: int = 5,
    rework_tokens: int = 1500,
    is_sequential_staleness: bool = False,
    is_cold_start: bool = False,
) -> DivergenceEvent:
    return DivergenceEvent(
        artifact_key=artifact_key,
        artifact_id=artifact_id or _aid(1),
        earlier_read=_read(
            node=earlier_node, tick=1, version=earlier_version, content_hash="ha"
        ),
        later_read=_read(
            node=later_node, tick=later_tick, version=later_version,
            content_hash="hb",
        ),
        canonical_writer="planner",
        canonical_writer_tick=2,
        rework_tokens=rework_tokens,
        is_sequential_staleness=is_sequential_staleness,
        is_cold_start=is_cold_start,
    )


def _report(
    *,
    headline_events: tuple[DivergenceEvent, ...] = (),
    top_event: DivergenceEvent | None = None,
    agent_pain_count: int = 0,
    rework_tokens_this_run: int = 0,
    rework_cost_this_run: float = 0.0,
    rework_cost_annualized: float | None = None,
    cost_unmeasurable_reason: str | None = None,
    strict_mode: bool = False,
    sequential_staleness_count: int = 0,
) -> DetectionReport:
    return DetectionReport(
        headline_divergence_events=headline_events,
        excluded_events=(),
        heatmap=(),
        reader_pair_matrix=(),
        top_event=top_event,
        exclusion_panel=ExclusionPanel(
            sequential_staleness_count=sequential_staleness_count,
            cold_start_count=0,
            append_only_skip_count=0,
        ),
        agent_pain_count=agent_pain_count,
        rework_tokens_this_run=rework_tokens_this_run,
        rework_cost_this_run=rework_cost_this_run,
        rework_cost_annualized=rework_cost_annualized,
        cost_unmeasurable_reason=cost_unmeasurable_reason,
        strict_mode=strict_mode,
        schema_version=CCS_DIAGNOSE_LOG_SCHEMA_VERSION,
    )


# -------------------------------------------------------------------- #
# Variant 1 — divergence ≥ 1
# -------------------------------------------------------------------- #


def test_divergence_variant_with_cost_line_renders_5_lines() -> None:
    """Cost line is included when annualized > 0 and tokens > 0."""
    event = _event(rework_tokens=1500)
    report = _report(
        headline_events=(event,),
        top_event=event,
        agent_pain_count=2,
        rework_tokens_this_run=1500,
        rework_cost_this_run=0.0045,
        rework_cost_annualized=420.0,
    )
    verdict = _verdict(bucket=Bucket.SHARED_ARTIFACT, confidence=Confidence.HIGH)

    out = terminal_summary(verdict=verdict, report=report, html_path=HTML_PATH)
    lines = out.splitlines()

    assert len(lines) == 5
    assert lines[0] == "Your write pattern: shared_artifact · high"
    assert lines[1] == (
        "1 divergence events on 1 artifact(s) · "
        "2 sub-agents acted on out-of-date state"
    )
    assert lines[2].startswith("Top event: plan · v1→v1 · ")
    assert "researcher ↔ executor @ tick 5" in lines[2]
    assert lines[3].startswith("Rework floor: ~$420/yr ")
    assert "1500 tokens" in lines[3]
    assert "this run: $0" in lines[3]
    assert lines[4] == f"→ open {HTML_PATH} for full forensic"


def test_divergence_variant_without_cost_when_unmeasurable() -> None:
    """Cost line omitted when ``rework_cost_annualized is None``."""
    event = _event(rework_tokens=0)
    report = _report(
        headline_events=(event,),
        top_event=event,
        agent_pain_count=1,
        rework_tokens_this_run=0,
        rework_cost_this_run=0.0,
        rework_cost_annualized=None,
        cost_unmeasurable_reason="value_token_estimates_missing",
    )
    verdict = _verdict(bucket=Bucket.SHARED_ARTIFACT, confidence=Confidence.HIGH)

    out = terminal_summary(verdict=verdict, report=report, html_path=HTML_PATH)
    lines = out.splitlines()

    assert len(lines) == 4
    assert not any(line.startswith("Rework floor") for line in lines)
    assert lines[-1] == f"→ open {HTML_PATH} for full forensic"


def test_divergence_variant_omits_cost_when_zero_tokens() -> None:
    """Cost line omitted when tokens == 0 even if annualized cost set."""
    event = _event(rework_tokens=0)
    report = _report(
        headline_events=(event,),
        top_event=event,
        agent_pain_count=1,
        rework_tokens_this_run=0,
        rework_cost_this_run=0.0,
        rework_cost_annualized=12.5,  # nonsense but not None
    )
    verdict = _verdict(bucket=Bucket.SHARED_ARTIFACT, confidence=Confidence.HIGH)

    out = terminal_summary(verdict=verdict, report=report, html_path=HTML_PATH)
    lines = out.splitlines()

    assert len(lines) == 4
    assert not any(line.startswith("Rework floor") for line in lines)


def test_divergence_variant_strict_marker_appended() -> None:
    """Strict mode + sequential-staleness > 0 → ``· strict`` suffix."""
    event = _event()
    report = _report(
        headline_events=(event,),
        top_event=event,
        agent_pain_count=1,
        rework_tokens_this_run=500,
        rework_cost_this_run=0.0015,
        rework_cost_annualized=100.0,
        strict_mode=True,
        sequential_staleness_count=4,
    )
    verdict = _verdict(bucket=Bucket.MIXED_PATTERN, confidence=Confidence.HIGH)

    out = terminal_summary(verdict=verdict, report=report, html_path=HTML_PATH)
    lines = out.splitlines()

    counts_line = lines[1]
    assert counts_line.endswith("· strict"), counts_line


def test_divergence_variant_strict_without_seq_staleness_no_marker() -> None:
    """Strict mode alone (no sequential-staleness) does NOT add the marker."""
    event = _event()
    report = _report(
        headline_events=(event,),
        top_event=event,
        agent_pain_count=1,
        strict_mode=True,
        sequential_staleness_count=0,
    )
    verdict = _verdict(bucket=Bucket.MIXED_PATTERN, confidence=Confidence.HIGH)

    out = terminal_summary(verdict=verdict, report=report, html_path=HTML_PATH)
    counts_line = out.splitlines()[1]
    assert not counts_line.endswith("· strict")


def test_divergence_variant_multi_artifact_count() -> None:
    """Counts line reports distinct artifact count, not event count."""
    e1 = _event(artifact_key="plan", artifact_id=_aid(1))
    e2 = _event(artifact_key="plan", artifact_id=_aid(1), later_tick=7)
    e3 = _event(artifact_key="notes", artifact_id=_aid(2), later_tick=9)
    report = _report(
        headline_events=(e1, e2, e3),
        top_event=e1,
        agent_pain_count=2,
    )
    verdict = _verdict(bucket=Bucket.SHARED_ARTIFACT, confidence=Confidence.HIGH)

    out = terminal_summary(verdict=verdict, report=report, html_path=HTML_PATH)
    counts_line = out.splitlines()[1]
    assert counts_line.startswith("3 divergence events on 2 artifact(s) · ")


# -------------------------------------------------------------------- #
# Variant 2 — zero events, HIGH/PRELIMINARY confidence
# -------------------------------------------------------------------- #


def test_zero_events_variant_high_confidence() -> None:
    verdict = _verdict(
        bucket=Bucket.SINGLE_WRITER,
        confidence=Confidence.HIGH,
        tracked_keys=("plan", "notes", "task_queue"),
        writers_by_key={
            "plan": ("planner",),
            "notes": ("researcher",),
            "task_queue": ("supervisor", "planner"),
        },
        coverage=_coverage(confidence=Confidence.HIGH, artifact_count=3),
    )
    report = _report()

    out = terminal_summary(verdict=verdict, report=report, html_path=HTML_PATH)
    lines = out.splitlines()

    assert len(lines) == 4
    assert lines[0] == "Your write pattern: single_writer per artifact · high"
    assert lines[1] == (
        "3 tracked artifact(s) · 2 single-writer · 1 multi-writer · "
        "0 divergence events observed"
    )
    assert lines[2].startswith("Forward-looking: ")
    assert lines[3] == f"→ open {HTML_PATH} for full report"


def test_zero_events_variant_preliminary_confidence() -> None:
    verdict = _verdict(
        bucket=Bucket.PARALLEL_BRANCH,
        confidence=Confidence.PRELIMINARY,
        tracked_keys=("a", "b"),
        writers_by_key={"a": ("n1",), "b": ("n2",)},
        coverage=_coverage(confidence=Confidence.PRELIMINARY, artifact_count=2),
    )
    report = _report()

    out = terminal_summary(verdict=verdict, report=report, html_path=HTML_PATH)
    lines = out.splitlines()

    assert len(lines) == 4
    assert lines[0] == "Your write pattern: parallel_branch · preliminary"
    assert lines[3] == f"→ open {HTML_PATH} for full report"


# -------------------------------------------------------------------- #
# Variant 3 — insufficient coverage
# -------------------------------------------------------------------- #


def test_insufficient_variant_default_reason() -> None:
    coverage = CoverageReport(
        tick_count=2,
        read_count=4,
        write_count=1,
        artifact_count=1,
        verdict_confidence=Confidence.INSUFFICIENT,
    )
    verdict = ClassifierVerdict(
        bucket=Bucket.INSUFFICIENT,
        confidence=Confidence.INSUFFICIENT,
        coverage=coverage,
        reason="below coverage threshold",
    )
    report = _report()

    out = terminal_summary(verdict=verdict, report=report, html_path=HTML_PATH)
    lines = out.splitlines()

    assert len(lines) == 4
    assert lines[0] == "Your write pattern: below coverage threshold"
    assert lines[1] == "Observed: 2 ticks · 4 reads · 1 writes · 1 artifacts"
    assert lines[2].startswith("Re-run on a longer workload")
    assert lines[3] == f"→ open {HTML_PATH}"
    assert "full forensic" not in lines[3]


def test_insufficient_variant_unsupported_execution_model() -> None:
    """Verbatim ``verdict.reason`` rendering for unusual reasons."""
    coverage = CoverageReport(
        tick_count=0,
        read_count=0,
        write_count=0,
        artifact_count=0,
        verdict_confidence=Confidence.INSUFFICIENT,
    )
    verdict = ClassifierVerdict(
        bucket=Bucket.INSUFFICIENT,
        confidence=Confidence.INSUFFICIENT,
        coverage=coverage,
        reason="unsupported_execution_model",
    )
    report = _report()

    out = terminal_summary(verdict=verdict, report=report, html_path=HTML_PATH)
    assert out.splitlines()[0] == "Your write pattern: unsupported_execution_model"


def test_insufficient_variant_falls_back_when_reason_missing() -> None:
    coverage = CoverageReport(
        tick_count=0,
        read_count=0,
        write_count=0,
        artifact_count=0,
        verdict_confidence=Confidence.INSUFFICIENT,
    )
    verdict = ClassifierVerdict(
        bucket=Bucket.INSUFFICIENT,
        confidence=Confidence.INSUFFICIENT,
        coverage=coverage,
        reason=None,
    )
    out = terminal_summary(
        verdict=verdict, report=_report(), html_path=HTML_PATH
    )
    assert out.splitlines()[0] == "Your write pattern: insufficient coverage"


# -------------------------------------------------------------------- #
# Truncation
# -------------------------------------------------------------------- #


def test_long_node_name_truncated_to_30_chars_plus_ellipsis() -> None:
    long_node = "a" * 50  # 50 chars
    event = _event(later_node=long_node)
    report = _report(headline_events=(event,), top_event=event)
    verdict = _verdict(bucket=Bucket.SHARED_ARTIFACT, confidence=Confidence.HIGH)

    out = terminal_summary(verdict=verdict, report=report, html_path=HTML_PATH)
    top_line = out.splitlines()[2]
    # 29 visible chars + ellipsis = 30 displayed code points.
    assert ("a" * 29 + "…") in top_line
    assert ("a" * 30) not in top_line


def test_long_artifact_key_truncated_to_40_chars_plus_ellipsis() -> None:
    long_key = "x" * 60
    event = _event(artifact_key=long_key)
    report = _report(headline_events=(event,), top_event=event)
    verdict = _verdict(bucket=Bucket.SHARED_ARTIFACT, confidence=Confidence.HIGH)

    out = terminal_summary(verdict=verdict, report=report, html_path=HTML_PATH)
    top_line = out.splitlines()[2]
    assert ("x" * 39 + "…") in top_line
    assert ("x" * 40) not in top_line


def test_node_name_at_minimum_threshold_not_truncated() -> None:
    """12 chars → not truncated (at-or-below min visible threshold)."""
    name_12 = "abcdefghijkl"
    assert len(name_12) == 12
    event = _event(later_node=name_12)
    report = _report(headline_events=(event,), top_event=event)
    verdict = _verdict(bucket=Bucket.SHARED_ARTIFACT, confidence=Confidence.HIGH)

    out = terminal_summary(verdict=verdict, report=report, html_path=HTML_PATH)
    top_line = out.splitlines()[2]
    assert name_12 in top_line
    assert "…" not in top_line


def test_node_name_just_above_minimum_not_truncated() -> None:
    """13 chars → not truncated (above min, well below max=30)."""
    name_13 = "abcdefghijklm"
    assert len(name_13) == 13
    event = _event(later_node=name_13)
    report = _report(headline_events=(event,), top_event=event)
    verdict = _verdict(bucket=Bucket.SHARED_ARTIFACT, confidence=Confidence.HIGH)

    out = terminal_summary(verdict=verdict, report=report, html_path=HTML_PATH)
    top_line = out.splitlines()[2]
    assert name_13 in top_line
    assert "…" not in top_line


# -------------------------------------------------------------------- #
# Unicode
# -------------------------------------------------------------------- #


def test_non_ascii_artifact_key_renders_correctly() -> None:
    event = _event(artifact_key="メッセージ")
    report = _report(headline_events=(event,), top_event=event)
    verdict = _verdict(bucket=Bucket.SHARED_ARTIFACT, confidence=Confidence.HIGH)

    out = terminal_summary(verdict=verdict, report=report, html_path=HTML_PATH)
    assert "メッセージ" in out


def test_emoji_node_name_renders_correctly() -> None:
    """Emoji counted as code points, not bytes; truncation respects this."""
    name_with_emoji = "agent-🤖-foo"  # 11 code points
    event = _event(later_node=name_with_emoji)
    report = _report(headline_events=(event,), top_event=event)
    verdict = _verdict(bucket=Bucket.SHARED_ARTIFACT, confidence=Confidence.HIGH)

    out = terminal_summary(verdict=verdict, report=report, html_path=HTML_PATH)
    assert name_with_emoji in out
    assert "…" not in out  # Below truncation threshold.


# -------------------------------------------------------------------- #
# Line width / layout
# -------------------------------------------------------------------- #


def test_default_line_width_typical_input_fits_in_80_cols() -> None:
    """Typical-shaped input fits in 80 cols (excluding cost + pointer lines).

    Two lines are exempt from the 80-col target:

    * Pointer line — ``html_path`` is user-supplied and can be arbitrarily
      long.
    * Cost / rework-floor line — the copy includes a fixed
      ``— see "What This Report Does NOT Measure"`` tail (~41 chars after
      the dollar/token figures) which is a deliberate copy decision; the
      total exceeds 80 chars even on minimal inputs. v0 accepts the
      overflow rather than degrade the message.
    """
    event = _event(
        artifact_key="task_queue",
        earlier_node="researcher",
        later_node="executor",
        later_tick=42,
    )
    report = _report(
        headline_events=(event,),
        top_event=event,
        agent_pain_count=2,
        rework_tokens_this_run=1500,
        rework_cost_this_run=0.0045,
        rework_cost_annualized=420.0,
    )
    verdict = _verdict(bucket=Bucket.SHARED_ARTIFACT, confidence=Confidence.HIGH)

    out = terminal_summary(verdict=verdict, report=report, html_path=HTML_PATH)

    for line in out.splitlines():
        if line.startswith("Rework floor:"):
            continue
        if line.startswith("→ open "):
            continue
        assert len(line) <= 80, f"Line exceeds 80 chars: {line!r} ({len(line)})"


def test_line_width_120_does_not_change_truncation_thresholds() -> None:
    """v0 does not auto-expand thresholds when the terminal is wider."""
    long_node = "a" * 50
    event = _event(later_node=long_node)
    report = _report(headline_events=(event,), top_event=event)
    verdict = _verdict(bucket=Bucket.SHARED_ARTIFACT, confidence=Confidence.HIGH)

    out_default = terminal_summary(
        verdict=verdict, report=report, html_path=HTML_PATH, line_width=80
    )
    out_wide = terminal_summary(
        verdict=verdict, report=report, html_path=HTML_PATH, line_width=120
    )
    assert out_default == out_wide


# -------------------------------------------------------------------- #
# Determinism + universal invariants
# -------------------------------------------------------------------- #


def test_terminal_summary_is_deterministic() -> None:
    event = _event()
    report = _report(
        headline_events=(event,),
        top_event=event,
        agent_pain_count=1,
        rework_tokens_this_run=500,
        rework_cost_this_run=0.0015,
        rework_cost_annualized=100.0,
    )
    verdict = _verdict(bucket=Bucket.SHARED_ARTIFACT, confidence=Confidence.HIGH)

    a = terminal_summary(verdict=verdict, report=report, html_path=HTML_PATH)
    b = terminal_summary(verdict=verdict, report=report, html_path=HTML_PATH)
    assert a == b


@pytest.mark.parametrize(
    "verdict_factory, report_factory",
    [
        # Variant 1: divergence with cost
        (
            lambda: _verdict(bucket=Bucket.SHARED_ARTIFACT, confidence=Confidence.HIGH),
            lambda: _report(
                headline_events=(_event(),),
                top_event=_event(),
                agent_pain_count=1,
                rework_tokens_this_run=500,
                rework_cost_this_run=0.0015,
                rework_cost_annualized=100.0,
            ),
        ),
        # Variant 1: divergence without cost
        (
            lambda: _verdict(bucket=Bucket.SHARED_ARTIFACT, confidence=Confidence.HIGH),
            lambda: _report(
                headline_events=(_event(),),
                top_event=_event(),
                agent_pain_count=1,
            ),
        ),
        # Variant 2: zero events
        (
            lambda: _verdict(
                bucket=Bucket.SINGLE_WRITER,
                confidence=Confidence.HIGH,
                writers_by_key={"plan": ("p",)},
            ),
            lambda: _report(),
        ),
        # Variant 3: insufficient
        (
            lambda: ClassifierVerdict(
                bucket=Bucket.INSUFFICIENT,
                confidence=Confidence.INSUFFICIENT,
                coverage=CoverageReport(
                    tick_count=1,
                    read_count=2,
                    write_count=0,
                    artifact_count=0,
                    verdict_confidence=Confidence.INSUFFICIENT,
                ),
                reason="below coverage threshold",
            ),
            lambda: _report(),
        ),
    ],
)
def test_every_variant_produces_at_most_8_lines(
    verdict_factory, report_factory
) -> None:
    out = terminal_summary(
        verdict=verdict_factory(),
        report=report_factory(),
        html_path=HTML_PATH,
    )
    assert 4 <= len(out.splitlines()) <= 8


@pytest.mark.parametrize(
    "verdict_factory, report_factory",
    [
        (
            lambda: _verdict(bucket=Bucket.SHARED_ARTIFACT, confidence=Confidence.HIGH),
            lambda: _report(
                headline_events=(_event(),),
                top_event=_event(),
                agent_pain_count=1,
            ),
        ),
        (
            lambda: _verdict(
                bucket=Bucket.SINGLE_WRITER,
                confidence=Confidence.HIGH,
                writers_by_key={"plan": ("p",)},
            ),
            lambda: _report(),
        ),
        (
            lambda: ClassifierVerdict(
                bucket=Bucket.INSUFFICIENT,
                confidence=Confidence.INSUFFICIENT,
                coverage=CoverageReport(
                    tick_count=1,
                    read_count=2,
                    write_count=0,
                    artifact_count=0,
                    verdict_confidence=Confidence.INSUFFICIENT,
                ),
                reason="below coverage threshold",
            ),
            lambda: _report(),
        ),
    ],
)
def test_last_line_contains_html_path(verdict_factory, report_factory) -> None:
    """The last line is the call-to-action and contains the HTML path.

    Each variant has different trailing text (``for full forensic`` /
    ``for full report`` / nothing for insufficient), so we check
    membership rather than ``endswith``.
    """
    path = Path("/some/dir/diagnose_report.html")
    out = terminal_summary(
        verdict=verdict_factory(),
        report=report_factory(),
        html_path=path,
    )
    last_line = out.splitlines()[-1]
    assert str(path) in last_line
    assert last_line.startswith("→ open ")


def test_no_ansi_escape_sequences_in_output() -> None:
    """v0 ships zero ANSI color — predictable in CI logs and pipes."""
    samples: list[str] = []

    # Variant 1
    event = _event()
    samples.append(
        terminal_summary(
            verdict=_verdict(
                bucket=Bucket.SHARED_ARTIFACT, confidence=Confidence.HIGH
            ),
            report=_report(
                headline_events=(event,),
                top_event=event,
                agent_pain_count=1,
                rework_tokens_this_run=500,
                rework_cost_this_run=0.0015,
                rework_cost_annualized=100.0,
                strict_mode=True,
                sequential_staleness_count=2,
            ),
            html_path=HTML_PATH,
        )
    )

    # Variant 2
    samples.append(
        terminal_summary(
            verdict=_verdict(
                bucket=Bucket.SINGLE_WRITER,
                confidence=Confidence.HIGH,
                writers_by_key={"plan": ("p",)},
            ),
            report=_report(),
            html_path=HTML_PATH,
        )
    )

    # Variant 3
    samples.append(
        terminal_summary(
            verdict=ClassifierVerdict(
                bucket=Bucket.INSUFFICIENT,
                confidence=Confidence.INSUFFICIENT,
                coverage=CoverageReport(
                    tick_count=1,
                    read_count=2,
                    write_count=0,
                    artifact_count=0,
                    verdict_confidence=Confidence.INSUFFICIENT,
                ),
                reason="below coverage threshold",
            ),
            report=_report(),
            html_path=HTML_PATH,
        )
    )

    for out in samples:
        assert "\x1b[" not in out, f"ANSI escape in output: {out!r}"
