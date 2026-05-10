# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""Unit 5 tests — HTML renderer (Jinja2 + autoescape).

Coverage:

* All 5 verdict-bucket snapshots (single_writer, shared_artifact,
  parallel_branch, mixed_pattern, insufficient).
* All 4 CTA variants (cold_lead, warm_lead, forward_looking, insufficient).
* Headline secondary KPI selection (cost / auditability / auto / fallback).
* XSS payloads parsed via stdlib html.parser — no <script>, no onerror,
  no <b> in the rendered DOM.
* Self-contained output — no external <link>, <img>, <script src>.
* Bandit B701 sanity: env.autoescape is enabled for ``.html`` files.
* File-size sanity, mixed-divergence layout, determinism.
"""

from __future__ import annotations

import re
import uuid
from html.parser import HTMLParser
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
    HeatmapRow,
    ReadObservation,
    ReaderPairCount,
)
from ccs.diagnose.ownership import OwnershipRow
from ccs.diagnose.render import (
    DEFAULT_BOOK_A_CALL_URL,
    RenderOptions,
    build_environment,
    render_html,
    render_to_string,
)


# -------------------------------------------------------------------- #
# Helpers
# -------------------------------------------------------------------- #


def _aid(seed: int = 0) -> uuid.UUID:
    """Stable UUID for per-test artifact identity."""
    return uuid.UUID(int=seed)


def _coverage(
    *,
    confidence: Confidence = Confidence.PRELIMINARY,
    artifact_count: int = 3,
    tick_count: int = 30,
    read_count: int = 60,
    write_count: int = 6,
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
    bucket: Bucket = Bucket.SINGLE_WRITER,
    confidence: Confidence = Confidence.HIGH,
    tracked_keys: tuple[str, ...] = ("plan", "notes", "task_queue"),
    append_only_keys: tuple[str, ...] = (),
    mutable_keys: tuple[str, ...] = (),
    ignored_framework_keys: tuple[str, ...] = ("__interrupt__",),
    ignored_ephemera_keys: tuple[str, ...] = ("errors",),
    unknown_underscore_keys: tuple[str, ...] = (),
    reason: str | None = None,
) -> ClassifierVerdict:
    return ClassifierVerdict(
        bucket=bucket,
        confidence=confidence,
        coverage=_coverage(
            confidence=confidence, artifact_count=len(tracked_keys)
        ),
        tracked_keys=tracked_keys,
        ignored_framework_keys=ignored_framework_keys,
        ignored_ephemera_keys=ignored_ephemera_keys,
        append_only_keys=append_only_keys,
        mutable_keys=mutable_keys,
        unknown_underscore_keys=unknown_underscore_keys,
        reason=reason,
    )


def _empty_report(
    *,
    strict_mode: bool = False,
    cost_unmeasurable_reason: str | None = None,
) -> DetectionReport:
    return DetectionReport(
        headline_divergence_events=(),
        excluded_events=(),
        heatmap=(),
        reader_pair_matrix=(),
        top_event=None,
        exclusion_panel=ExclusionPanel(0, 0, 0),
        agent_pain_count=0,
        rework_tokens_this_run=0,
        rework_cost_this_run=0.0,
        rework_cost_annualized=None,
        cost_unmeasurable_reason=cost_unmeasurable_reason,
        strict_mode=strict_mode,
        schema_version=CCS_DIAGNOSE_LOG_SCHEMA_VERSION,
    )


def _make_divergence_event(
    *,
    artifact_key: str = "plan",
    artifact_id: uuid.UUID | None = None,
    earlier_node: str = "researcher",
    earlier_tick: int = 1,
    later_node: str = "executor",
    later_tick: int = 3,
    rework_tokens: int = 1500,
    canonical_writer: str | None = "planner",
    canonical_writer_tick: int | None = 2,
    is_sequential_staleness: bool = False,
    is_cold_start: bool = False,
) -> DivergenceEvent:
    return DivergenceEvent(
        artifact_key=artifact_key,
        artifact_id=artifact_id or _aid(1),
        earlier_read=ReadObservation(
            node=earlier_node, tick=earlier_tick,
            version="v1", content_hash="h1",
        ),
        later_read=ReadObservation(
            node=later_node, tick=later_tick,
            version="v1", content_hash="h1",
        ),
        canonical_writer=canonical_writer,
        canonical_writer_tick=canonical_writer_tick,
        rework_tokens=rework_tokens,
        is_sequential_staleness=is_sequential_staleness,
        is_cold_start=is_cold_start,
    )


def _ownership_row(
    *,
    artifact_key: str = "plan",
    artifact_id: uuid.UUID | None = None,
    writers: tuple[tuple[str, int], ...] = (("planner", 3),),
    readers: tuple[tuple[str, int], ...] = (("researcher", 5), ("executor", 4)),
    version_range: str = "v1 -> v3",
    append_only: bool = False,
) -> OwnershipRow:
    return OwnershipRow(
        artifact_key=artifact_key,
        artifact_id=artifact_id or _aid(1),
        writers=writers,
        readers=readers,
        version_range=version_range,
        append_only=append_only,
    )


# -------------------------------------------------------------------- #
# DOM-walking helpers (stdlib only)
# -------------------------------------------------------------------- #


class _DOMVisitor(HTMLParser):
    """Collect tags + attributes for assertion-style inspection."""

    def __init__(self) -> None:
        super().__init__()
        self.tags: list[tuple[str, dict[str, str]]] = []
        self.text_chunks: list[str] = []

    def handle_starttag(self, tag, attrs):
        self.tags.append((tag, dict(attrs)))

    def handle_startendtag(self, tag, attrs):
        self.tags.append((tag, dict(attrs)))

    def handle_data(self, data):
        self.text_chunks.append(data)

    def has_tag(self, name: str) -> bool:
        return any(tag == name for tag, _ in self.tags)

    def has_attr(self, attr_name: str) -> bool:
        for _, attrs in self.tags:
            if attr_name in attrs:
                return True
        return False

    def has_attr_with_value_substring(self, attr_name: str, needle: str) -> bool:
        for _, attrs in self.tags:
            value = attrs.get(attr_name, "")
            if needle in value:
                return True
        return False


def _parse(html: str) -> _DOMVisitor:
    visitor = _DOMVisitor()
    visitor.feed(html)
    return visitor


# -------------------------------------------------------------------- #
# 1-5: Verdict bucket snapshots
# -------------------------------------------------------------------- #


def test_snapshot_single_writer_high_confidence_zero_divergence():
    verdict = _verdict(
        bucket=Bucket.SINGLE_WRITER,
        confidence=Confidence.HIGH,
        tracked_keys=("plan", "notes", "task_queue"),
    )
    report = _empty_report()
    ownership = (
        _ownership_row(artifact_key="plan", writers=(("planner", 3),)),
        _ownership_row(
            artifact_key="notes",
            artifact_id=_aid(2),
            writers=(("researcher", 2),),
            readers=(("researcher", 1),),
        ),
    )

    html = render_to_string(verdict=verdict, report=report, ownership=ownership)

    # Section 2 is Ownership Map (no events).
    assert "Artifact Ownership Map" in html
    # Sections 3 & 4 omitted.
    assert "Per-Artifact Heatmap" not in html
    assert "Reader-Pair Matrix" not in html
    # CTA = forward-looking
    assert "If any of these are on your roadmap" in html
    # Verdict label exposed.
    assert "single_writer per artifact" in html
    assert "high" in html


def test_snapshot_shared_artifact_preliminary_with_divergence():
    aid = _aid(7)
    events = tuple(
        _make_divergence_event(
            artifact_key="plan",
            artifact_id=aid,
            earlier_tick=i,
            later_tick=i + 1,
        )
        for i in range(9)
    )
    heatmap = (
        HeatmapRow(artifact_key="plan", artifact_id=aid, divergent_reads=9, total_reads=20),
        HeatmapRow(artifact_key="notes", artifact_id=_aid(8), divergent_reads=4, total_reads=10),
        HeatmapRow(artifact_key="meta", artifact_id=_aid(9), divergent_reads=2, total_reads=5),
    )
    pair_matrix = (
        ReaderPairCount(earlier_reader="researcher", later_reader="executor", event_count=9),
        ReaderPairCount(earlier_reader="planner", later_reader="critic", event_count=3),
    )
    report = DetectionReport(
        headline_divergence_events=events,
        excluded_events=(),
        heatmap=heatmap,
        reader_pair_matrix=pair_matrix,
        top_event=events[0],
        exclusion_panel=ExclusionPanel(0, 0, 0),
        agent_pain_count=2,
        rework_tokens_this_run=13500,
        rework_cost_this_run=0.04,
        rework_cost_annualized=None,
        cost_unmeasurable_reason="value_token_estimates_missing",
        strict_mode=False,
        schema_version=CCS_DIAGNOSE_LOG_SCHEMA_VERSION,
    )
    verdict = _verdict(bucket=Bucket.SHARED_ARTIFACT, confidence=Confidence.PRELIMINARY)

    html = render_to_string(verdict=verdict, report=report, ownership=())

    assert "The Event That Matters Most" in html
    assert "Per-Artifact Heatmap" in html
    assert "Reader-Pair Matrix" in html
    # CTA = cold-lead default
    assert "30-min walk-through" in html
    # Reader pair counts present.
    assert "researcher" in html and "executor" in html


def test_snapshot_parallel_branch():
    aid = _aid(3)
    event = _make_divergence_event(artifact_id=aid)
    heatmap = (HeatmapRow(artifact_key="plan", artifact_id=aid, divergent_reads=1, total_reads=4),)
    pairs = (ReaderPairCount("researcher", "executor", 1),)
    report = DetectionReport(
        headline_divergence_events=(event,),
        excluded_events=(),
        heatmap=heatmap,
        reader_pair_matrix=pairs,
        top_event=event,
        exclusion_panel=ExclusionPanel(0, 0, 0),
        agent_pain_count=1,
        rework_tokens_this_run=1500,
        rework_cost_this_run=0.005,
        rework_cost_annualized=None,
        cost_unmeasurable_reason=None,
        strict_mode=False,
        schema_version=CCS_DIAGNOSE_LOG_SCHEMA_VERSION,
    )
    verdict = _verdict(bucket=Bucket.PARALLEL_BRANCH, confidence=Confidence.PRELIMINARY)

    html = render_to_string(verdict=verdict, report=report, ownership=())

    assert "parallel_branch" in html
    assert "The Event That Matters Most" in html
    assert "30-min walk-through" in html


def test_snapshot_mixed_pattern_with_ownership_appendix():
    aid_a = _aid(11)
    aid_b = _aid(12)
    aid_c = _aid(13)
    events = tuple(
        _make_divergence_event(artifact_id=aid_a, earlier_tick=i, later_tick=i + 1)
        for i in range(4)
    )
    heatmap = (
        HeatmapRow(artifact_key="A", artifact_id=aid_a, divergent_reads=4, total_reads=10),
    )
    pairs = (ReaderPairCount("r1", "r2", 4),)
    report = DetectionReport(
        headline_divergence_events=events,
        excluded_events=(),
        heatmap=heatmap,
        reader_pair_matrix=pairs,
        top_event=events[0],
        exclusion_panel=ExclusionPanel(0, 0, 0),
        agent_pain_count=2,
        rework_tokens_this_run=6000,
        rework_cost_this_run=0.02,
        rework_cost_annualized=None,
        cost_unmeasurable_reason=None,
        strict_mode=False,
        schema_version=CCS_DIAGNOSE_LOG_SCHEMA_VERSION,
    )
    verdict = _verdict(
        bucket=Bucket.MIXED_PATTERN,
        confidence=Confidence.PRELIMINARY,
        tracked_keys=("A", "B", "C"),
    )
    ownership = (
        _ownership_row(artifact_key="A", artifact_id=aid_a, writers=(("p", 2), ("q", 2))),
        _ownership_row(artifact_key="B", artifact_id=aid_b, writers=(("p", 1),)),
        _ownership_row(artifact_key="C", artifact_id=aid_c, writers=(("q", 1),)),
    )

    html = render_to_string(verdict=verdict, report=report, ownership=ownership)

    # Heatmap shows row A only.
    assert "Per-Artifact Heatmap" in html
    # Ownership Map collapsible appendix present (3 artifacts, 1 in heatmap).
    assert "<details>" in html
    # All three artifact keys reachable in template content.
    assert ">A<" in html and ">B<" in html and ">C<" in html
    # CTA = cold-lead default
    assert "30-min walk-through" in html


def test_snapshot_insufficient_renders_reason_no_heatmap():
    verdict = _verdict(
        bucket=Bucket.INSUFFICIENT,
        confidence=Confidence.INSUFFICIENT,
        tracked_keys=("plan",),
        reason="below coverage threshold",
    )
    report = _empty_report(cost_unmeasurable_reason="verdict_insufficient")

    html = render_to_string(verdict=verdict, report=report, ownership=())

    assert "below coverage threshold" in html
    assert "Per-Artifact Heatmap" not in html
    assert "Reader-Pair Matrix" not in html
    # CTA = insufficient variant
    assert "Re-run on a longer workload" in html


# -------------------------------------------------------------------- #
# 6-9: CTA variants
# -------------------------------------------------------------------- #


def test_cta_cold_lead_default_has_walkthrough_book_call_softask():
    aid = _aid(1)
    event = _make_divergence_event(artifact_id=aid)
    report = DetectionReport(
        headline_divergence_events=(event,),
        excluded_events=(),
        heatmap=(HeatmapRow("plan", aid, 1, 5),),
        reader_pair_matrix=(ReaderPairCount("a", "b", 1),),
        top_event=event,
        exclusion_panel=ExclusionPanel(0, 0, 0),
        agent_pain_count=1,
        rework_tokens_this_run=100,
        rework_cost_this_run=0.001,
        rework_cost_annualized=None,
        cost_unmeasurable_reason=None,
        strict_mode=False,
        schema_version=CCS_DIAGNOSE_LOG_SCHEMA_VERSION,
    )
    html = render_to_string(verdict=_verdict(), report=report, ownership=())
    assert "30-min walk-through" in html
    assert DEFAULT_BOOK_A_CALL_URL in html
    assert "yes/no" in html  # soft-ask phrase


def test_cta_warm_lead_no_walkthrough_no_softask_two_questions():
    aid = _aid(1)
    event = _make_divergence_event(artifact_id=aid, artifact_key="task_queue")
    report = DetectionReport(
        headline_divergence_events=(event,),
        excluded_events=(),
        heatmap=(HeatmapRow("task_queue", aid, 1, 5),),
        reader_pair_matrix=(ReaderPairCount("a", "b", 1),),
        top_event=event,
        exclusion_panel=ExclusionPanel(0, 0, 0),
        agent_pain_count=1,
        rework_tokens_this_run=100,
        rework_cost_this_run=0.001,
        rework_cost_annualized=12345.0,
        cost_unmeasurable_reason=None,
        strict_mode=False,
        schema_version=CCS_DIAGNOSE_LOG_SCHEMA_VERSION,
    )
    options = RenderOptions(warm_lead=True)
    html = render_to_string(
        verdict=_verdict(), report=report, ownership=(), options=options
    )
    assert "30-min walk-through" not in html
    assert "yes/no" not in html
    assert "two questions I'd love your read on" in html
    # Question 1 references top artifact key
    assert "task_queue" in html
    # Question 2 references the cost floor
    assert "$12,345/yr" in html


def test_cta_forward_looking_zero_events_with_upgrade_triggers():
    verdict = _verdict(bucket=Bucket.SINGLE_WRITER, confidence=Confidence.PRELIMINARY)
    report = _empty_report()
    html = render_to_string(verdict=verdict, report=report, ownership=())
    assert "If any of these are on your roadmap" in html
    assert "shared scratchpad" in html
    assert "vector store" in html
    assert DEFAULT_BOOK_A_CALL_URL in html
    assert "yes/no" in html


def test_cta_insufficient_no_softask():
    verdict = _verdict(
        bucket=Bucket.INSUFFICIENT,
        confidence=Confidence.INSUFFICIENT,
        reason="below coverage threshold",
    )
    report = _empty_report(cost_unmeasurable_reason="verdict_insufficient")
    html = render_to_string(verdict=verdict, report=report, ownership=())
    assert "Re-run on a longer workload" in html
    assert DEFAULT_BOOK_A_CALL_URL in html
    # Insufficient variant: NO soft-ask
    assert "yes/no" not in html
    assert "30-min walk-through" not in html


# -------------------------------------------------------------------- #
# 10-14: Headline secondary KPI
# -------------------------------------------------------------------- #


def test_headline_cost_with_annualized_value_shows_floor_label():
    aid = _aid(1)
    event = _make_divergence_event(artifact_id=aid)
    report = DetectionReport(
        headline_divergence_events=(event,),
        excluded_events=(),
        heatmap=(HeatmapRow("plan", aid, 1, 4),),
        reader_pair_matrix=(),
        top_event=event,
        exclusion_panel=ExclusionPanel(0, 0, 0),
        agent_pain_count=1,
        rework_tokens_this_run=2000,
        rework_cost_this_run=0.006,
        rework_cost_annualized=8765.43,
        cost_unmeasurable_reason=None,
        strict_mode=False,
        schema_version=CCS_DIAGNOSE_LOG_SCHEMA_VERSION,
    )
    options = RenderOptions(lead_pain_type="cost")
    html = render_to_string(
        verdict=_verdict(), report=report, ownership=(), options=options
    )
    assert "$8,765/yr" in html
    assert "floor" in html


def test_headline_cost_with_unmeasurable_reason_shows_fallback():
    report = _empty_report(cost_unmeasurable_reason="value_token_estimates_missing")
    options = RenderOptions(lead_pain_type="cost")
    html = render_to_string(
        verdict=_verdict(), report=report, ownership=(), options=options
    )
    assert "Rework cost: unmeasurable" in html
    assert "DiagnoseCheckpointer" in html


def test_headline_auditability_no_cost_line():
    aid = _aid(1)
    event = _make_divergence_event(artifact_id=aid)
    report = DetectionReport(
        headline_divergence_events=(event,),
        excluded_events=(),
        heatmap=(HeatmapRow("plan", aid, 1, 4),),
        reader_pair_matrix=(),
        top_event=event,
        exclusion_panel=ExclusionPanel(0, 0, 0),
        agent_pain_count=2,
        rework_tokens_this_run=2000,
        rework_cost_this_run=0.006,
        rework_cost_annualized=8765.43,
        cost_unmeasurable_reason=None,
        strict_mode=False,
        schema_version=CCS_DIAGNOSE_LOG_SCHEMA_VERSION,
    )
    options = RenderOptions(lead_pain_type="auditability")
    html = render_to_string(
        verdict=_verdict(), report=report, ownership=(), options=options
    )
    assert "$8,765/yr" not in html
    assert "1 divergence event" in html
    assert "agent_pain_count" in html


def test_headline_auto_routes_to_cost_when_annualized_present():
    aid = _aid(1)
    event = _make_divergence_event(artifact_id=aid)
    report = DetectionReport(
        headline_divergence_events=(event,),
        excluded_events=(),
        heatmap=(HeatmapRow("plan", aid, 1, 4),),
        reader_pair_matrix=(),
        top_event=event,
        exclusion_panel=ExclusionPanel(0, 0, 0),
        agent_pain_count=1,
        rework_tokens_this_run=500,
        rework_cost_this_run=0.0015,
        rework_cost_annualized=4321.0,
        cost_unmeasurable_reason=None,
        strict_mode=False,
        schema_version=CCS_DIAGNOSE_LOG_SCHEMA_VERSION,
    )
    html = render_to_string(verdict=_verdict(), report=report, ownership=())
    assert "$4,321/yr" in html


def test_headline_auto_routes_to_auditability_when_annualized_none():
    aid = _aid(1)
    event = _make_divergence_event(artifact_id=aid)
    report = DetectionReport(
        headline_divergence_events=(event,),
        excluded_events=(),
        heatmap=(HeatmapRow("plan", aid, 1, 4),),
        reader_pair_matrix=(),
        top_event=event,
        exclusion_panel=ExclusionPanel(0, 0, 0),
        agent_pain_count=1,
        rework_tokens_this_run=0,
        rework_cost_this_run=0.0,
        rework_cost_annualized=None,
        cost_unmeasurable_reason="value_token_estimates_missing",
        strict_mode=False,
        schema_version=CCS_DIAGNOSE_LOG_SCHEMA_VERSION,
    )
    html = render_to_string(verdict=_verdict(), report=report, ownership=())
    assert "1 divergence event" in html
    assert "/yr" not in html


# -------------------------------------------------------------------- #
# 15-18: XSS payload protection (parsed via stdlib html.parser)
# -------------------------------------------------------------------- #


def test_script_tag_in_state_key_does_not_create_script_element():
    payload_key = "<script>alert('pwn')</script>"
    verdict = _verdict(
        tracked_keys=(payload_key, "plan"),
        ignored_framework_keys=(),
        ignored_ephemera_keys=(),
    )
    report = _empty_report()
    html = render_to_string(verdict=verdict, report=report, ownership=())
    visitor = _parse(html)
    # Inline <script> tags must NEVER appear (we ship no inline JS).
    # Any rendered "<script>" from the malicious key is escaped.
    assert not visitor.has_tag("script")


def test_attribute_injection_payload_does_not_create_img_element():
    payload_key = '"><img src=x onerror=alert("pwn")>'
    verdict = _verdict(
        tracked_keys=(payload_key, "plan"),
        ignored_framework_keys=(),
        ignored_ephemera_keys=(),
    )
    report = _empty_report()
    html = render_to_string(verdict=verdict, report=report, ownership=())
    visitor = _parse(html)
    # No <img> elements anywhere in the document.
    assert not visitor.has_tag("img")
    # No onerror attribute anywhere.
    assert not visitor.has_attr("onerror")


def test_html_in_node_name_does_not_create_b_element():
    payload_node = "<b>fake-bold-agent</b>"
    aid = _aid(1)
    event = DivergenceEvent(
        artifact_key="plan",
        artifact_id=aid,
        earlier_read=ReadObservation(node=payload_node, tick=1, version="v1", content_hash="h1"),
        later_read=ReadObservation(node="reader", tick=3, version="v2", content_hash="h2"),
        canonical_writer="writer",
        canonical_writer_tick=2,
        rework_tokens=100,
        is_sequential_staleness=False,
        is_cold_start=False,
    )
    report = DetectionReport(
        headline_divergence_events=(event,),
        excluded_events=(),
        heatmap=(HeatmapRow("plan", aid, 1, 4),),
        reader_pair_matrix=(ReaderPairCount(payload_node, "reader", 1),),
        top_event=event,
        exclusion_panel=ExclusionPanel(0, 0, 0),
        agent_pain_count=1,
        rework_tokens_this_run=100,
        rework_cost_this_run=0.001,
        rework_cost_annualized=None,
        cost_unmeasurable_reason=None,
        strict_mode=False,
        schema_version=CCS_DIAGNOSE_LOG_SCHEMA_VERSION,
    )
    html = render_to_string(verdict=_verdict(), report=report, ownership=())
    visitor = _parse(html)
    # Our template uses <strong>, never <b>. Any <b> in DOM = XSS leak.
    assert not visitor.has_tag("b")
    # The literal string still present in escaped form.
    assert "fake-bold-agent" in html


def test_version_range_with_html_chars_escapes_correctly():
    row = OwnershipRow(
        artifact_key="plan",
        artifact_id=_aid(1),
        writers=(("planner", 3),),
        readers=(("reader", 4),),
        version_range="v1 < v2 > v3",
        append_only=False,
    )
    html = render_to_string(
        verdict=_verdict(bucket=Bucket.SINGLE_WRITER),
        report=_empty_report(),
        ownership=(row,),
    )
    # Angle brackets in user data must escape (autoescape's job).
    assert "&lt;" in html or "&amp;lt;" in html
    # Should NOT introduce unintended tags from this content.
    visitor = _parse(html)
    # v3 won't appear as a tag — sanity check.
    assert not visitor.has_tag("v2")


# -------------------------------------------------------------------- #
# 19: Bandit B701 sanity — autoescape configured
# -------------------------------------------------------------------- #


def test_jinja2_environment_has_autoescape_for_html_files():
    env = build_environment()
    assert env.autoescape is not None
    # Must escape HTML-ish files; non-HTML files (e.g. ".txt") must NOT escape.
    assert env.autoescape("anything.html") is True


# -------------------------------------------------------------------- #
# 20: Self-contained output
# -------------------------------------------------------------------- #


def test_output_has_no_external_resources():
    # Render a representative full-content report.
    aid = _aid(1)
    event = _make_divergence_event(artifact_id=aid)
    report = DetectionReport(
        headline_divergence_events=(event,),
        excluded_events=(),
        heatmap=(HeatmapRow("plan", aid, 1, 4),),
        reader_pair_matrix=(ReaderPairCount("a", "b", 1),),
        top_event=event,
        exclusion_panel=ExclusionPanel(1, 1, 1),
        agent_pain_count=1,
        rework_tokens_this_run=100,
        rework_cost_this_run=0.001,
        rework_cost_annualized=None,
        cost_unmeasurable_reason=None,
        strict_mode=False,
        schema_version=CCS_DIAGNOSE_LOG_SCHEMA_VERSION,
    )
    html = render_to_string(
        verdict=_verdict(unknown_underscore_keys=("__new_internal__",)),
        report=report,
        ownership=(),
    )
    visitor = _parse(html)
    # No <link> elements (would reference external stylesheets).
    assert not visitor.has_tag("link")
    # No <img> at all.
    assert not visitor.has_tag("img")
    # No <script> at all.
    assert not visitor.has_tag("script")
    # No external font/stylesheet URL anywhere in attributes.
    assert not visitor.has_attr_with_value_substring("href", "https://fonts.")
    assert not visitor.has_attr_with_value_substring("src", "http")


# -------------------------------------------------------------------- #
# 21: File-size sanity
# -------------------------------------------------------------------- #


def test_large_render_under_500kb(tmp_path: Path):
    tracked = tuple(f"artifact_{i:03d}" for i in range(100))
    verdict = _verdict(
        bucket=Bucket.SHARED_ARTIFACT,
        confidence=Confidence.PRELIMINARY,
        tracked_keys=tracked,
    )
    aid_first = _aid(0)
    events = tuple(
        _make_divergence_event(
            artifact_key=tracked[i],
            artifact_id=uuid.UUID(int=i),
            earlier_tick=i,
            later_tick=i + 1,
        )
        for i in range(10)
    )
    heatmap = tuple(
        HeatmapRow(
            artifact_key=tracked[i],
            artifact_id=uuid.UUID(int=i),
            divergent_reads=10 - i,
            total_reads=20,
        )
        for i in range(10)
    )
    pairs = tuple(
        ReaderPairCount(f"reader_{i}", f"writer_{i}", 5 - (i // 2))
        for i in range(5)
    )
    report = DetectionReport(
        headline_divergence_events=events,
        excluded_events=(),
        heatmap=heatmap,
        reader_pair_matrix=pairs,
        top_event=events[0],
        exclusion_panel=ExclusionPanel(2, 1, 0),
        agent_pain_count=8,
        rework_tokens_this_run=15000,
        rework_cost_this_run=0.045,
        rework_cost_annualized=12345.67,
        cost_unmeasurable_reason=None,
        strict_mode=False,
        schema_version=CCS_DIAGNOSE_LOG_SCHEMA_VERSION,
    )
    ownership = tuple(
        OwnershipRow(
            artifact_key=tracked[i],
            artifact_id=uuid.UUID(int=i),
            writers=(("writer_a", 2), ("writer_b", 1)) if i % 3 == 0 else (("writer_a", 3),),
            readers=(("reader_1", 5), ("reader_2", 2)),
            version_range="v1 -> v3",
            append_only=False,
        )
        for i in range(100)
    )
    output = tmp_path / "report.html"
    render_html(
        verdict=verdict, report=report, ownership=ownership, output_path=output
    )
    size = output.stat().st_size
    assert size < 500 * 1024, f"output too big: {size} bytes"
    # Sanity: not pathologically tiny either.
    assert size > 5 * 1024
    assert aid_first is not None  # silence unused-name warning


# -------------------------------------------------------------------- #
# 22: Mixed-divergence layout
# -------------------------------------------------------------------- #


def test_mixed_divergence_renders_collapsible_appendix():
    aid_a = _aid(1)
    aid_b = _aid(2)
    aid_c = _aid(3)
    event = _make_divergence_event(artifact_id=aid_a, artifact_key="A")
    heatmap = (HeatmapRow("A", aid_a, 1, 5),)
    pairs = (ReaderPairCount("a", "b", 1),)
    report = DetectionReport(
        headline_divergence_events=(event,),
        excluded_events=(),
        heatmap=heatmap,
        reader_pair_matrix=pairs,
        top_event=event,
        exclusion_panel=ExclusionPanel(0, 0, 0),
        agent_pain_count=1,
        rework_tokens_this_run=100,
        rework_cost_this_run=0.001,
        rework_cost_annualized=None,
        cost_unmeasurable_reason=None,
        strict_mode=False,
        schema_version=CCS_DIAGNOSE_LOG_SCHEMA_VERSION,
    )
    ownership = (
        _ownership_row(artifact_key="A", artifact_id=aid_a, writers=(("p", 1), ("q", 1))),
        _ownership_row(artifact_key="B", artifact_id=aid_b, writers=(("p", 1),)),
        _ownership_row(artifact_key="C", artifact_id=aid_c, writers=(("q", 1),)),
    )
    html = render_to_string(verdict=_verdict(), report=report, ownership=ownership)
    visitor = _parse(html)
    # <details> appears once (the appendix).
    assert visitor.has_tag("details")
    # All artifacts visible somewhere (in the appendix at minimum).
    for key in ("A", "B", "C"):
        assert f">{key}<" in html


# -------------------------------------------------------------------- #
# 23: Determinism
# -------------------------------------------------------------------- #


def test_render_is_deterministic():
    verdict = _verdict()
    report = _empty_report()
    ownership = (_ownership_row(),)
    options = RenderOptions(lead_pain_type="auto")

    a = render_to_string(
        verdict=verdict, report=report, ownership=ownership, options=options
    )
    b = render_to_string(
        verdict=verdict, report=report, ownership=ownership, options=options
    )
    assert a == b
    # No timestamp / no UUID-of-now leakage: assert no 4-digit-year date string.
    assert not re.search(r"20\d{2}-\d{2}-\d{2}T\d{2}", a)


# -------------------------------------------------------------------- #
# Extra: render_html writes file
# -------------------------------------------------------------------- #


def test_render_html_writes_file_to_disk(tmp_path: Path):
    output = tmp_path / "subdir" / "report.html"
    render_html(
        verdict=_verdict(),
        report=_empty_report(),
        ownership=(_ownership_row(),),
        output_path=output,
    )
    assert output.exists()
    content = output.read_text(encoding="utf-8")
    assert "<!doctype html>" in content
    assert "ccs-diagnose" in content


def test_unknown_underscore_keys_callout_present():
    verdict = _verdict(unknown_underscore_keys=("__pregel_unknown_v9__",))
    html = render_to_string(verdict=verdict, report=_empty_report(), ownership=())
    assert "Staleness sensor" in html
    assert "__pregel_unknown_v9__" in html


def test_strict_mode_footnote_only_when_relevant():
    # Strict mode True: footnote suppressed even with seq-staleness count.
    aid = _aid(1)
    event = _make_divergence_event(artifact_id=aid)
    report = DetectionReport(
        headline_divergence_events=(event,),
        excluded_events=(),
        heatmap=(HeatmapRow("plan", aid, 1, 4),),
        reader_pair_matrix=(),
        top_event=event,
        exclusion_panel=ExclusionPanel(2, 0, 0),
        agent_pain_count=1,
        rework_tokens_this_run=100,
        rework_cost_this_run=0.001,
        rework_cost_annualized=None,
        cost_unmeasurable_reason=None,
        strict_mode=True,
        schema_version=CCS_DIAGNOSE_LOG_SCHEMA_VERSION,
    )
    html_strict = render_to_string(verdict=_verdict(), report=report, ownership=())
    assert "Re-run with <code>--strict</code>" not in html_strict

    # Now strict mode False with seq-staleness > 0 → footnote present.
    report_loose = DetectionReport(
        headline_divergence_events=(event,),
        excluded_events=(),
        heatmap=(HeatmapRow("plan", aid, 1, 4),),
        reader_pair_matrix=(),
        top_event=event,
        exclusion_panel=ExclusionPanel(2, 0, 0),
        agent_pain_count=1,
        rework_tokens_this_run=100,
        rework_cost_this_run=0.001,
        rework_cost_annualized=None,
        cost_unmeasurable_reason=None,
        strict_mode=False,
        schema_version=CCS_DIAGNOSE_LOG_SCHEMA_VERSION,
    )
    html_loose = render_to_string(verdict=_verdict(), report=report_loose, ownership=())
    assert "--strict" in html_loose


def test_schema_version_in_footer():
    html = render_to_string(
        verdict=_verdict(),
        report=_empty_report(),
        ownership=(),
    )
    assert CCS_DIAGNOSE_LOG_SCHEMA_VERSION in html


def test_heatmap_omits_rows_with_zero_divergent_reads():
    aid = _aid(1)
    event = _make_divergence_event(artifact_id=aid, artifact_key="A")
    # Heatmap row with zero divergent_reads should be hidden.
    heatmap = (
        HeatmapRow("A", aid, 1, 4),
        HeatmapRow("B", _aid(2), 0, 7),  # zero — must be filtered
    )
    report = DetectionReport(
        headline_divergence_events=(event,),
        excluded_events=(),
        heatmap=heatmap,
        reader_pair_matrix=(),
        top_event=event,
        exclusion_panel=ExclusionPanel(0, 0, 0),
        agent_pain_count=1,
        rework_tokens_this_run=100,
        rework_cost_this_run=0.001,
        rework_cost_annualized=None,
        cost_unmeasurable_reason=None,
        strict_mode=False,
        schema_version=CCS_DIAGNOSE_LOG_SCHEMA_VERSION,
    )
    html = render_to_string(verdict=_verdict(), report=report, ownership=())
    # Pull the heatmap section's body (best-effort substring slice).
    heatmap_section_idx = html.find("Per-Artifact Heatmap")
    next_section_idx = html.find("Reader-Pair Matrix", heatmap_section_idx)
    if next_section_idx == -1:
        next_section_idx = html.find("Tracked Artifacts", heatmap_section_idx)
    heatmap_section = html[heatmap_section_idx:next_section_idx]
    assert "A" in heatmap_section
    assert ">B<" not in heatmap_section  # filtered out


# -------------------------------------------------------------------- #
# RenderOptions URL / email scheme allowlist (XSS prevention)
# -------------------------------------------------------------------- #


def test_render_options_rejects_javascript_book_url() -> None:
    """``javascript:`` href must be rejected at construction time."""
    with pytest.raises(ValueError, match="book_a_call_url"):
        RenderOptions(book_a_call_url="javascript:alert(1)")


def test_render_options_rejects_data_book_url() -> None:
    with pytest.raises(ValueError, match="book_a_call_url"):
        RenderOptions(book_a_call_url="data:text/html,<script>alert(1)</script>")


def test_render_options_rejects_vbscript_book_url() -> None:
    with pytest.raises(ValueError, match="book_a_call_url"):
        RenderOptions(book_a_call_url="vbscript:msgbox(1)")


def test_render_options_rejects_javascript_contact_email() -> None:
    """``javascript:`` payload in contact_email must be rejected."""
    with pytest.raises(ValueError, match="contact_email"):
        RenderOptions(contact_email="javascript:alert(1)")


def test_render_options_rejects_data_contact_email() -> None:
    with pytest.raises(ValueError, match="contact_email"):
        RenderOptions(contact_email="data:text/html,<script>alert(1)</script>")


def test_render_options_rejects_email_without_at_symbol() -> None:
    with pytest.raises(ValueError, match="contact_email"):
        RenderOptions(contact_email="not-an-email")


def test_render_options_rejects_email_with_whitespace() -> None:
    with pytest.raises(ValueError, match="contact_email"):
        RenderOptions(contact_email="user @example.com")


def test_render_options_accepts_https_book_url() -> None:
    """https:// is the canonical happy path."""
    opts = RenderOptions(book_a_call_url="https://cal.com/team/diagnose")
    assert opts.book_a_call_url == "https://cal.com/team/diagnose"


def test_render_options_accepts_http_book_url() -> None:
    opts = RenderOptions(book_a_call_url="http://localhost:8080/book")
    assert opts.book_a_call_url == "http://localhost:8080/book"


def test_render_options_accepts_normal_email() -> None:
    opts = RenderOptions(contact_email="alice@example.com")
    assert opts.contact_email == "alice@example.com"


def test_render_options_default_construction_succeeds() -> None:
    """The defaults must satisfy the validators (no regression on the happy path)."""
    opts = RenderOptions()
    assert opts.book_a_call_url.startswith("https://")
    assert "@" in opts.contact_email
