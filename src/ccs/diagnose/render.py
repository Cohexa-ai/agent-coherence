# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""Single-file HTML renderer for ``ccs-diagnose`` (Unit 5).

Produces a self-contained HTML report from a
:class:`ccs.diagnose.classifier.ClassifierVerdict`,
:class:`ccs.diagnose.detection.DetectionReport`, and the per-artifact
ownership map (:func:`ccs.diagnose.ownership.compute_ownership_map`).

Trust posture
=============

* **Jinja2 + ``select_autoescape(['html'])``** is mandatory. Bandit B701
  enforces autoescape; this module passes B701. State-key names, agent
  node names, and (in some scenarios) artifact-related strings reach the
  HTML body and attribute contexts. Production state keys can contain
  ``<script>``, attribute-injection payloads, anything. The PySpector
  CVE-2026-33140 (April 2026) is the canonical recent precedent; v0
  trusts only Jinja2 autoescape, not manual ``html.escape`` shortcuts.
* **No JavaScript dependencies.** Native HTML5 ``<details>`` for the
  collapsible Ownership Map appendix; no inline ``<script>``, no
  ``<script src=>``.
* **No external assets.** No ``<link>``, no ``<img src=...>`` referencing
  external URLs, no external font links — system fonts only. The HTML
  is shareable as-is via DM/Slack and opens offline.

Topology exposure
=================

State-key names render verbatim (with autoescape applied). Production
keys often encode internal service names or proprietary workflow shape;
users sharing the HTML accept this. Post-v0 ``--redact-keys`` will hash
key names with a per-run salt; the option will be added back to
``RenderOptions`` when that lands.

Determinism
===========

For identical ``(verdict, report, ownership, options)`` the renderer
returns identical bytes. No timestamps, no random IDs, no UUID-of-now in
the output.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, TypedDict

from jinja2 import Environment, FileSystemLoader, select_autoescape

from ccs.diagnose.classifier import Bucket, ClassifierVerdict, Confidence
from ccs.diagnose.detection import DetectionReport
from ccs.diagnose.ownership import OwnershipRow

from ._labels import BUCKET_DISPLAY as _BUCKET_DISPLAY
from ._labels import CONFIDENCE_LABEL as _CONFIDENCE_LABEL

__all__ = [
    "RenderOptions",
    "render_html",
    "render_to_string",
    "build_environment",
    "DEFAULT_BOOK_A_CALL_URL",
    "DEFAULT_CONTACT_EMAIL",
]


DEFAULT_BOOK_A_CALL_URL: str = os.environ.get(
    "CCS_DIAGNOSE_BOOK_A_CALL_URL",
    "https://cal.com/agent-coherence",
)
"""Production calendar link rendered in the report CTA when no override supplied.

Resolves at import time from ``CCS_DIAGNOSE_BOOK_A_CALL_URL`` if set,
else falls back to the hardcoded default. The same scheme allowlist
that ``RenderOptions.__post_init__`` applies to caller-supplied values
also applies to env-var overrides — an instance built with this
default will validate as usual, so a malicious env value
(``javascript:alert(1)``) is rejected the moment ``RenderOptions()``
is constructed.

Override per-invocation via ``RenderOptions(book_a_call_url=...)`` or the
``--book-a-call-url`` CLI flag.
"""

DEFAULT_CONTACT_EMAIL: str = os.environ.get(
    "CCS_DIAGNOSE_CONTACT_EMAIL",
    "vlad@agent-coherence.dev",
)
"""Placeholder reply-to address. Override via ``RenderOptions`` or by
setting ``CCS_DIAGNOSE_CONTACT_EMAIL`` before import. Subject to the
same scheme/format allowlist as caller-supplied values."""


_TEMPLATES_DIR = Path(__file__).with_name("templates")
_TEMPLATE_NAME = "diagnose_report.html"


_EMAIL_PATTERN = re.compile(r"^[^@\s<>\"'\\]+@[^@\s<>\"'\\]+$")


@dataclass(frozen=True)
class RenderOptions:
    """Caller-supplied knobs for the HTML renderer.

    ``lead_pain_type`` selects which secondary KPI rides the headline:

    * ``"cost"`` — annualized $/yr (with ``"floor"`` label).
    * ``"auditability"`` — divergence-event count + agent_pain_count.
    * ``"auto"`` — pick ``cost`` if ``report.rework_cost_annualized`` is
      a real number, else ``auditability``.

    ``warm_lead`` switches the CTA from cold-lead default to a 2-question
    seed for an upcoming call (no soft-ask, no book-a-call link).

    ``book_a_call_url`` and ``contact_email`` are validated in
    ``__post_init__`` to reject ``javascript:`` / ``data:`` / ``vbscript:``
    URI schemes. Jinja2 autoescape sanitizes HTML but does not gate URL
    schemes — an attacker-controlled CLI flag like
    ``--book-a-call-url 'javascript:alert(1)'`` would otherwise render a
    live click-to-execute href in the report.
    """

    lead_pain_type: Literal["cost", "auditability", "auto"] = "auto"
    warm_lead: bool = False
    book_a_call_url: str = DEFAULT_BOOK_A_CALL_URL
    contact_email: str = DEFAULT_CONTACT_EMAIL

    def __post_init__(self) -> None:
        _validate_book_a_call_url(self.book_a_call_url)
        _validate_contact_email(self.contact_email)


def _validate_book_a_call_url(url: str) -> None:
    """Reject any URL whose scheme isn't ``http`` or ``https``.

    The CTA renders ``<a href="{{ book_a_call_url }}">`` so a
    ``javascript:`` (or ``data:``, ``vbscript:``) scheme would produce a
    live XSS sink that Jinja2 autoescape does NOT block.
    """
    if not isinstance(url, str):
        raise ValueError(
            f"book_a_call_url must be a string; got {type(url).__name__}"
        )
    lowered = url.strip().lower()
    if not (lowered.startswith("http://") or lowered.startswith("https://")):
        raise ValueError(
            "book_a_call_url must start with http:// or https:// "
            f"(rejected: {url!r})"
        )


def _validate_contact_email(email: str) -> None:
    """Reject anything that doesn't look like a plain email address.

    The CTA renders ``<a href="mailto:{{ contact_email }}">``. Embedding a
    URL scheme inside the user-supplied value (``javascript:alert(1)``)
    would land in the ``mailto:`` href verbatim — most clients tolerate
    the prefix and the underlying scheme is still clickable. Reject
    schemes explicitly and require an ``@`` to keep the value
    well-formed.
    """
    if not isinstance(email, str):
        raise ValueError(
            f"contact_email must be a string; got {type(email).__name__}"
        )
    stripped = email.strip()
    lowered = stripped.lower()
    forbidden_prefixes = ("javascript:", "data:", "vbscript:", "file:")
    if any(lowered.startswith(prefix) for prefix in forbidden_prefixes):
        raise ValueError(
            "contact_email must not embed a URL scheme "
            f"(rejected: {email!r})"
        )
    if not _EMAIL_PATTERN.match(stripped):
        raise ValueError(
            "contact_email must look like a plain email address "
            f"(rejected: {email!r})"
        )


def render_html(
    *,
    verdict: ClassifierVerdict,
    report: DetectionReport,
    ownership: tuple[OwnershipRow, ...],
    output_path: Path,
    options: RenderOptions | None = None,
) -> None:
    """Render the report to ``output_path`` (creates parent dirs).

    Writes UTF-8 text. Existing files at ``output_path`` are overwritten.
    """
    html = render_to_string(
        verdict=verdict, report=report, ownership=ownership, options=options
    )
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(html, encoding="utf-8")


def render_to_string(
    *,
    verdict: ClassifierVerdict,
    report: DetectionReport,
    ownership: tuple[OwnershipRow, ...],
    options: RenderOptions | None = None,
) -> str:
    """Render the report and return it as a UTF-8 string."""
    options = options or RenderOptions()
    env = build_environment()
    template = env.get_template(_TEMPLATE_NAME)
    context = _build_context(
        verdict=verdict, report=report, ownership=ownership, options=options
    )
    return template.render(**context)


def build_environment() -> Environment:
    """Return a Jinja2 environment configured for HTML autoescape.

    Exposed as a public helper so tests can inspect ``env.autoescape``
    (Bandit B701 sanity check).
    """
    return Environment(
        loader=FileSystemLoader(str(_TEMPLATES_DIR)),
        autoescape=select_autoescape(["html"]),
        keep_trailing_newline=True,
    )


# -------------------------------------------------------------------- #
# Context builder
# -------------------------------------------------------------------- #


class _ReportContext(TypedDict):
    """Type signature for the Jinja2 template context.

    Lists every key the template consumes, so a missing or renamed
    field is a static-analysis failure instead of a Jinja2
    ``UndefinedError`` at render time. ``total=True`` (the default) —
    every field is required.
    """

    # Headline + verdict
    headline_label: str
    headline_subtitle: str
    verdict_reason: str
    is_insufficient: bool
    secondary_kpi_kind: str
    # Cost KPI
    rework_cost_annualized: float | None
    rework_cost_annualized_str: str | None
    rework_tokens_this_run: int | None
    cost_unmeasurable_reason: str | None
    cost_unmeasurable_message: str
    # Auditability KPI
    agent_pain_count: int
    headline_event_count: int
    # Section toggles
    has_events: bool
    show_heatmap: bool
    show_reader_pairs: bool
    show_excluded: bool
    show_ownership_appendix: bool
    # Section 2 data
    top_event: Any
    top_event_writes: Any
    ownership: tuple[OwnershipRow, ...]
    # Section 3 data
    heatmap_rows: tuple[_HeatmapDisplayRow, ...]
    # Section 4 data
    reader_pairs: Any
    # Section 5 data
    exclusion_panel: Any
    strict_mode: bool
    # Section 6 data
    tracked_keys: Any
    ignored_framework_keys: Any
    ignored_ephemera_keys: Any
    append_only_keys: Any
    mutable_keys: Any
    unknown_underscore_keys: Any
    # Section 7 data
    coverage: Any
    coverage_thresholds: Any
    confidence_label: str
    # Sections 8 & 9 — static copy
    copy_does_not_measure: str
    copy_cannot_tell_you: str
    # Section 10 — CTA
    cta_variant: str
    book_a_call_url: str
    contact_email: str
    warm_lead_questions: Any
    upgrade_triggers: Any
    soft_ask_message: str
    # Footer
    schema_version: str


def _build_context(
    *,
    verdict: ClassifierVerdict,
    report: DetectionReport,
    ownership: tuple[OwnershipRow, ...],
    options: RenderOptions,
) -> _ReportContext:
    """Compose the Jinja2 template context.

    All copy strings the template renders flow through here — the template
    itself is structural HTML + small ``{% if %}`` switches over typed
    flags. Putting copy decisions in Python keeps them out of the template
    where autoescape applies (so a typo in copy can't accidentally inject
    HTML).

    The body is a merge of four theme-grouped helpers (verdict, cost,
    section toggles, copy) that each return their own slice as a plain
    dict — easier to test and reason about in isolation.
    """
    toggles = _build_section_toggles(report=report, ownership=ownership)
    return _ReportContext(  # type: ignore[typeddict-item]
        **_build_verdict_fields(verdict=verdict, report=report, options=options),
        **_build_cost_fields(report=report),
        **toggles,
        **_build_section_data(verdict=verdict, report=report, ownership=ownership),
        **_build_copy_fields(verdict=verdict, report=report, options=options),
    )


def _build_verdict_fields(
    *,
    verdict: ClassifierVerdict,
    report: DetectionReport,
    options: RenderOptions,
) -> dict[str, Any]:
    """Headline label, subtitle, verdict reason, secondary-KPI selector."""
    headline = _build_headline(verdict)
    secondary_kpi_kind = _pick_secondary_kpi(
        lead_pain_type=options.lead_pain_type, report=report
    )
    return {
        "headline_label": headline.label,
        "headline_subtitle": headline.subtitle,
        "verdict_reason": verdict.reason or "",
        "is_insufficient": verdict.bucket is Bucket.INSUFFICIENT,
        "secondary_kpi_kind": secondary_kpi_kind,
    }


def _build_cost_fields(*, report: DetectionReport) -> dict[str, Any]:
    """Cost / auditability KPI fields and the unmeasurable-fallback copy."""
    return {
        "rework_cost_annualized": report.rework_cost_annualized,
        "rework_cost_annualized_str": _format_currency_per_year(
            report.rework_cost_annualized
        ),
        "rework_tokens_this_run": report.rework_tokens_this_run,
        "cost_unmeasurable_reason": report.cost_unmeasurable_reason,
        "cost_unmeasurable_message": _COPY_COST_UNMEASURABLE.get(
            report.cost_unmeasurable_reason or "", ""
        ),
        "agent_pain_count": report.agent_pain_count,
        "headline_event_count": len(report.headline_divergence_events),
    }


def _build_section_toggles(
    *,
    report: DetectionReport,
    ownership: tuple[OwnershipRow, ...],
) -> dict[str, bool]:
    """Boolean flags the template uses to gate optional sections."""
    has_events = len(report.headline_divergence_events) >= 1
    show_heatmap = any(row.divergent_reads > 0 for row in report.heatmap)
    show_reader_pairs = len(report.reader_pair_matrix) >= 1
    show_excluded = (
        report.exclusion_panel.sequential_staleness_count
        + report.exclusion_panel.cold_start_count
        + report.exclusion_panel.append_only_skip_count
    ) > 0
    # Mixed-divergence layout: at least one event present, but more
    # tracked artifacts than divergent ones — surface the full Ownership
    # Map as a collapsible appendix to Section 3.
    show_ownership_appendix = (
        has_events
        and show_heatmap
        and len(ownership) > sum(1 for _ in report.heatmap)
    )
    return {
        "has_events": has_events,
        "show_heatmap": show_heatmap,
        "show_reader_pairs": show_reader_pairs,
        "show_excluded": show_excluded,
        "show_ownership_appendix": show_ownership_appendix,
    }


@dataclass(frozen=True)
class _HeatmapDisplayRow:
    """A heatmap row enriched with its writer count for display.

    The detection-layer :class:`~ccs.diagnose.detection.HeatmapRow` ranks
    purely by ``divergent_reads``, which over-surfaces single-writer artifacts
    whose high ``share`` is expected pipeline ordering (readers handed the
    pre-write value). For the report we re-rank genuine *multi-writer*
    artifacts — the coordination signal — first.
    """

    artifact_key: str
    divergent_reads: int
    total_reads: int
    writer_count: int

    @property
    def is_multi_writer(self) -> bool:
        return self.writer_count >= 2


def _build_heatmap_display_rows(
    *,
    report: DetectionReport,
    ownership: tuple[OwnershipRow, ...],
) -> tuple[_HeatmapDisplayRow, ...]:
    """Heatmap rows for the report: joined with writer counts and re-ranked so
    genuine multi-writer artifacts sort above single-writer pipeline ordering.

    Mirrors the Ownership Map's multi-writer-first sort
    (:func:`ccs.diagnose.ownership._row_sort_key`). Detection's ``HeatmapRow``
    order — which feeds :func:`_pick_top_event` — is left unchanged; this is a
    presentation-only re-rank. Artifacts absent from ``ownership`` (writer
    count unknown) fall back to the existing divergent-reads order.
    """
    writers_by_id = {row.artifact_id: len(row.writers) for row in ownership}
    rows = (
        _HeatmapDisplayRow(
            artifact_key=row.artifact_key,
            divergent_reads=row.divergent_reads,
            total_reads=row.total_reads,
            writer_count=writers_by_id.get(row.artifact_id, 0),
        )
        for row in report.heatmap
        if row.divergent_reads > 0
    )
    # Stable sort: multi-writer first, then by divergent reads, then key. The
    # multi-writer threshold lives only in ``_HeatmapDisplayRow.is_multi_writer``.
    return tuple(
        sorted(
            rows,
            key=lambda r: (
                0 if r.is_multi_writer else 1,
                -r.divergent_reads,
                r.artifact_key,
            ),
        )
    )


def _build_section_data(
    *,
    verdict: ClassifierVerdict,
    report: DetectionReport,
    ownership: tuple[OwnershipRow, ...],
) -> dict[str, Any]:
    """Per-section data structures (sections 2-7)."""
    return {
        "top_event": report.top_event,
        "top_event_writes": _top_event_writes(report),
        "ownership": ownership,
        "heatmap_rows": _build_heatmap_display_rows(
            report=report, ownership=ownership
        ),
        "reader_pairs": report.reader_pair_matrix,
        "exclusion_panel": report.exclusion_panel,
        "strict_mode": report.strict_mode,
        "tracked_keys": verdict.tracked_keys,
        "ignored_framework_keys": verdict.ignored_framework_keys,
        "ignored_ephemera_keys": verdict.ignored_ephemera_keys,
        "append_only_keys": verdict.append_only_keys,
        "mutable_keys": verdict.mutable_keys,
        "unknown_underscore_keys": verdict.unknown_underscore_keys,
        "coverage": verdict.coverage,
        "coverage_thresholds": _COVERAGE_THRESHOLDS,
        "confidence_label": _CONFIDENCE_LABEL.get(
            verdict.confidence, str(verdict.confidence.value)
        ),
    }


def _build_copy_fields(
    *,
    verdict: ClassifierVerdict,
    report: DetectionReport,
    options: RenderOptions,
) -> dict[str, Any]:
    """Static copy blocks and the dynamic CTA section (section 10 + 8/9)."""
    cta_variant = _pick_cta_variant(
        verdict=verdict, report=report, options=options
    )
    return {
        "copy_does_not_measure": _COPY.does_not_measure,
        "copy_cannot_tell_you": _COPY.cannot_tell_you,
        "cta_variant": cta_variant,
        "book_a_call_url": options.book_a_call_url,
        "contact_email": options.contact_email,
        "warm_lead_questions": _build_warm_lead_questions(report),
        "upgrade_triggers": _COPY_UPGRADE_TRIGGERS,
        "soft_ask_message": _COPY_SOFT_ASK,
        "schema_version": report.schema_version,
    }


# -------------------------------------------------------------------- #
# Headline + KPI selection
# -------------------------------------------------------------------- #


@dataclass(frozen=True)
class _Headline:
    label: str
    subtitle: str


def _build_headline(verdict: ClassifierVerdict) -> _Headline:
    bucket_display = _BUCKET_DISPLAY.get(verdict.bucket, verdict.bucket.value)
    confidence_display = _CONFIDENCE_LABEL.get(
        verdict.confidence, verdict.confidence.value
    )
    label = f"Your write pattern: {bucket_display}"
    subtitle = f"confidence: {confidence_display}"
    return _Headline(label=label, subtitle=subtitle)


# Mirrors classifier internals; quoted in Section 7 so users can audit
# why the report was flagged at its given confidence.
_COVERAGE_THRESHOLDS: dict[str, int] = {
    "tick_count": 50,
    "read_count": 100,
    "write_count": 5,
}


def _pick_secondary_kpi(
    *,
    lead_pain_type: Literal["cost", "auditability", "auto"],
    report: DetectionReport,
) -> Literal["cost", "auditability"]:
    if lead_pain_type == "cost":
        return "cost"
    if lead_pain_type == "auditability":
        return "auditability"
    # auto
    if report.rework_cost_annualized is not None:
        return "cost"
    return "auditability"


def _format_currency_per_year(value: float | None) -> str:
    if value is None:
        return ""
    return f"${value:,.0f}/yr"


# -------------------------------------------------------------------- #
# CTA variant selection
# -------------------------------------------------------------------- #


def _pick_cta_variant(
    *,
    verdict: ClassifierVerdict,
    report: DetectionReport,
    options: RenderOptions,
) -> str:
    """Return one of: ``cold_lead``, ``warm_lead``, ``forward_looking``,
    ``insufficient``.
    """
    if verdict.bucket is Bucket.INSUFFICIENT:
        return "insufficient"
    if options.warm_lead:
        return "warm_lead"
    if len(report.headline_divergence_events) >= 1:
        return "cold_lead"
    if verdict.confidence is not Confidence.INSUFFICIENT:
        return "forward_looking"
    return "insufficient"


def _build_warm_lead_questions(report: DetectionReport) -> tuple[str, str]:
    """Two seed questions for warm-conversation variant.

    Includes graceful fallbacks per the plan's spec:
    * top divergent ``artifact_key`` -> ``"primary"``
    * annualized $ floor -> ``agent_pain_count`` line.
    """
    if report.top_event is not None:
        artifact_label = report.top_event.artifact_key
    else:
        artifact_label = "primary"

    if report.rework_cost_annualized is not None:
        impact_label = (
            f"the {_format_currency_per_year(report.rework_cost_annualized)} floor"
        )
    else:
        impact_label = (
            f"the agent_pain_count of {report.agent_pain_count} affected nodes"
        )

    q1 = (
        f"Does the {artifact_label} divergence pattern look real to you, or is "
        "your revision-loop driver something else?"
    )
    q2 = (
        f"{impact_label} vs. your overall coherence cost — does the share "
        "match what you've observed?"
    )
    return (q1, q2)


# -------------------------------------------------------------------- #
# Section 2 helper — write-event timeline for the top divergence event.
# -------------------------------------------------------------------- #


def _top_event_writes(report: DetectionReport) -> tuple[dict[str, object], ...]:
    """Return a small per-write list around the top event.

    The forensic mini-timeline shows the canonical writer (the most
    recent observed writer at ``later_read.tick``) when present. With
    only the canonical writer in scope we still expose it as a list to
    keep template loops uniform.
    """
    top = report.top_event
    if top is None or top.canonical_writer is None:
        return ()
    return (
        {
            "node": top.canonical_writer,
            "tick": top.canonical_writer_tick,
            "label": "most recent observed writer at later_read.tick",
        },
    )


# -------------------------------------------------------------------- #
# Static copy
# -------------------------------------------------------------------- #


@dataclass(frozen=True)
class _Copy:
    does_not_measure: tuple[str, ...]
    cannot_tell_you: tuple[str, ...]


_COPY = _Copy(
    does_not_measure=(
        "Broadcast rebroadcasting cost: when many agents subscribe to an "
        "artifact, every update fans out to all of them. v0-preview measures "
        "the floor — the tokens the divergent reader missed — but not the "
        "tokens spent on subscribers who already had the value.",
        "Redundant fetches: when an agent re-fetches an artifact it already "
        "holds locally because the runtime has no per-key read attribution. "
        "v0-preview cannot count these from the LangGraph callback alone.",
        "Wall-clock latency cost: a divergent read may force a node to retry "
        "or re-plan. v0-preview reports tokens-as-floor, not seconds-of-stall.",
    ),
    cannot_tell_you=(
        "v0-preview is a witness-quality report. The runtime can prove it "
        "*handed* node Z a stale copy of an artifact (the read view in the "
        "merged state). It cannot prove that node Z *read* the value — "
        "LangGraph exposes no per-key read-time interception point.",
        "Field names like earlier_read, later_read, and canonical_writer are "
        "observations, not attributions. canonical_writer is the most recent "
        "observed writer at later_read.tick — not 'the writer who was right'.",
        "Attribution upgrade path: install CCSStore as the LangGraph "
        "BaseStore. CCSStore observes the per-key fetch and write call sites "
        "and lifts witness-quality reads into provable attribution. Same "
        "diagnose surface, no callback rewiring.",
    ),
)


_COPY_COST_UNMEASURABLE: dict[str, str] = {
    "value_token_estimates_missing": (
        "Rework cost cannot be measured in v0-preview without a "
        "DiagnoseCheckpointer attached. Install ccs-diagnose with the "
        "checkpointer extra to populate value_token_estimates and re-run."
    ),
    "verdict_insufficient": (
        "Rework cost is not computed when the verdict is insufficient — "
        "the run was too short to classify writes."
    ),
}


_COPY_UPGRADE_TRIGGERS: tuple[str, ...] = (
    "adding a sub-agent that writes to a previously read-only artifact",
    "introducing a shared scratchpad / notes file across agents",
    "parallel sub-agents running concurrently against shared state",
    "upgrading from default state to a vector store + cache",
)


_COPY_SOFT_ASK: str = (
    "If a 30-min call is too much, replying with a one-line "
    "yes/no on whether the divergence count above matches your gut "
    "is also high-signal. v0-preview is calibrating against real graphs."
)
