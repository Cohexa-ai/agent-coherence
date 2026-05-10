# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""Terminal summary text-builder for ``ccs-diagnose`` (Unit 6).

Pure string builder: takes a :class:`ClassifierVerdict` (Unit 3) and a
:class:`DetectionReport` (Unit 4) and returns a 4–5 line ASCII-only block
suitable for printing to the terminal at the end of an invocation. The
caller (Unit 7's CLI) handles ``print()``; this module performs no I/O.

Three output variants
=====================

The variant is selected from ``(verdict, report)`` shape:

1. **Divergence ≥ 1** — at least one headline divergence event AND the
   classifier emitted a non-INSUFFICIENT bucket.
2. **Zero events** — no headline divergence events, ``HIGH`` or
   ``PRELIMINARY`` confidence, and a non-INSUFFICIENT bucket.
3. **Insufficient coverage** — classifier bucket is ``INSUFFICIENT``.

Output constraints
==================

* 4–5 lines (the spec allows up to 8; v0 uses 4–5).
* No ANSI color in v0 — predictable in CI logs, pipes, redirects, and
  email/Slack copy-paste. A future revision may add color behind a
  ``--color`` flag.
* Default line width: 80 chars; the ``line_width`` parameter is reserved
  for future word-wrap layouts. v0 does not auto-expand truncation
  thresholds when the terminal is wider; truncation is fixed at
  30 chars (node names) and 40 chars (artifact keys).
* The last line **always** ends with the ``html_path``. That is the call
  to action — the user clicks the path to read the full forensic.

Edge cases (documented short-circuits)
======================================

* **Top event with zero rework_tokens (cost unmeasurable).** In variant 1,
  if ``report.rework_cost_annualized is None`` OR
  ``report.rework_tokens_this_run == 0``, the rework-floor line is
  omitted entirely (we never render ``$0/yr``).
* **``--strict`` marker.** In variant 1, if ``report.strict_mode`` is
  ``True`` AND ``report.exclusion_panel.sequential_staleness_count > 0``,
  the counts line is suffixed with ``· strict`` so the reader sees the
  strict-mode signal without a dedicated line.

Determinism
===========

Pure function; deterministic on inputs. No timestamps, no environment
reads, no random ordering. Same inputs → identical output string.
"""

from __future__ import annotations

from pathlib import Path

from ccs.diagnose.classifier import Bucket, ClassifierVerdict, Confidence
from ccs.diagnose.detection import DetectionReport, DivergenceEvent

__all__ = ["terminal_summary"]


# -------------------------------------------------------------------- #
# Tunables (kept module-level so tests can reference them)
# -------------------------------------------------------------------- #


_NODE_NAME_MAX_CHARS: int = 30
_ARTIFACT_KEY_MAX_CHARS: int = 40
_TRUNCATE_MIN_VISIBLE: int = 12
_ELLIPSIS: str = "…"  # Unicode horizontal ellipsis.


_BUCKET_DISPLAY: dict[Bucket, str] = {
    Bucket.SINGLE_WRITER: "single_writer per artifact",
    Bucket.SHARED_ARTIFACT: "shared_artifact",
    Bucket.PARALLEL_BRANCH: "parallel_branch",
    Bucket.MIXED_PATTERN: "mixed pattern",
    Bucket.INSUFFICIENT: "insufficient coverage",
}

_CONFIDENCE_DISPLAY: dict[Confidence, str] = {
    Confidence.HIGH: "high",
    Confidence.PRELIMINARY: "preliminary",
    Confidence.INSUFFICIENT: "insufficient",
}


# -------------------------------------------------------------------- #
# Public API
# -------------------------------------------------------------------- #


def terminal_summary(
    *,
    verdict: ClassifierVerdict,
    report: DetectionReport,
    html_path: Path,
    line_width: int = 80,
) -> str:
    """Render the 4–5 line terminal summary block for ``ccs-diagnose``.

    Pure function — returns a newline-joined string with no trailing
    newline. The caller (Unit 7) handles ``print()``.

    Variant selection:

    * ``verdict.bucket == INSUFFICIENT`` → insufficient-coverage variant.
    * ``len(report.headline_divergence_events) >= 1`` AND non-INSUFFICIENT
      bucket → divergence variant.
    * Otherwise (zero events, non-INSUFFICIENT bucket) → zero-events
      variant.

    The ``line_width`` parameter is reserved for callers that pass
    ``shutil.get_terminal_size().columns``. v0 does not implement
    word-wrap; truncation thresholds are fixed.
    """
    if verdict.bucket is Bucket.INSUFFICIENT:
        return _render_insufficient(verdict=verdict, report=report, html_path=html_path)

    if report.headline_divergence_events:
        return _render_divergence(
            verdict=verdict, report=report, html_path=html_path
        )

    return _render_zero_events(verdict=verdict, report=report, html_path=html_path)


# -------------------------------------------------------------------- #
# Variant renderers
# -------------------------------------------------------------------- #


def _render_divergence(
    *,
    verdict: ClassifierVerdict,
    report: DetectionReport,
    html_path: Path,
) -> str:
    """Variant 1: ≥ 1 headline divergence event."""
    lines: list[str] = []

    # Line 1 — headline.
    lines.append(_headline_line(verdict))

    # Line 2 — counts (with optional ``· strict`` marker).
    artifact_count = len({ev.artifact_id for ev in report.headline_divergence_events})
    counts_line = (
        f"{len(report.headline_divergence_events)} divergence events on "
        f"{artifact_count} artifact(s) · "
        f"{report.agent_pain_count} sub-agents acted on out-of-date state"
    )
    if (
        report.strict_mode
        and report.exclusion_panel.sequential_staleness_count > 0
    ):
        counts_line += " · strict"
    lines.append(counts_line)

    # Line 3 — top-event one-liner.
    top = report.top_event
    if top is not None:
        lines.append(_top_event_line(top))

    # Line 4 (conditional) — rework cost floor.
    if (
        report.rework_cost_annualized is not None
        and report.rework_tokens_this_run > 0
        and report.rework_cost_annualized > 0
    ):
        annualized = _format_dollars(report.rework_cost_annualized)
        this_run = _format_dollars(report.rework_cost_this_run)
        lines.append(
            f"Rework floor: ~${annualized}/yr "
            f"({report.rework_tokens_this_run} tokens · this run: ${this_run}) "
            f'— see "What This Report Does NOT Measure"'
        )

    # Last line — pointer.
    lines.append(f"→ open {html_path} for full forensic")
    return "\n".join(lines)


def _render_zero_events(
    *,
    verdict: ClassifierVerdict,
    report: DetectionReport,
    html_path: Path,
) -> str:
    """Variant 2: zero headline events, HIGH/PRELIMINARY confidence."""
    tracked = verdict.coverage.artifact_count
    writers_by_key = verdict.writers_by_key or {}
    multi_writer = sum(1 for writers in writers_by_key.values() if len(writers) > 1)
    single_writer = sum(1 for writers in writers_by_key.values() if len(writers) == 1)

    lines = [
        _headline_line(verdict),
        (
            f"{tracked} tracked artifact(s) · "
            f"{single_writer} single-writer · "
            f"{multi_writer} multi-writer · "
            f"0 divergence events observed"
        ),
        (
            'Forward-looking: see "When a single-writer report is worth a '
            'conversation" in the report.'
        ),
        f"→ open {html_path} for full report",
    ]
    return "\n".join(lines)


def _render_insufficient(
    *,
    verdict: ClassifierVerdict,
    report: DetectionReport,
    html_path: Path,
) -> str:
    """Variant 3: classifier bucket is INSUFFICIENT."""
    reason = verdict.reason or "insufficient coverage"
    cov = verdict.coverage
    lines = [
        f"Your write pattern: {reason}",
        (
            f"Observed: {cov.tick_count} ticks · "
            f"{cov.read_count} reads · "
            f"{cov.write_count} writes · "
            f"{cov.artifact_count} artifacts"
        ),
        (
            "Re-run on a longer workload to classify, or talk to me about "
            "what shape would tell us something."
        ),
        f"→ open {html_path}",
    ]
    return "\n".join(lines)


# -------------------------------------------------------------------- #
# Line builders
# -------------------------------------------------------------------- #


def _headline_line(verdict: ClassifierVerdict) -> str:
    bucket = _BUCKET_DISPLAY.get(verdict.bucket, verdict.bucket.value)
    confidence = _CONFIDENCE_DISPLAY.get(verdict.confidence, verdict.confidence.value)
    return f"Your write pattern: {bucket} · {confidence}"


def _top_event_line(event: DivergenceEvent) -> str:
    artifact = _truncate(event.artifact_key, _ARTIFACT_KEY_MAX_CHARS)
    earlier_node = _truncate(event.earlier_read.node, _NODE_NAME_MAX_CHARS)
    later_node = _truncate(event.later_read.node, _NODE_NAME_MAX_CHARS)
    return (
        f"Top event: {artifact} · "
        f"v{event.earlier_read.version}→v{event.later_read.version} · "
        f"{earlier_node} ↔ {later_node} @ tick {event.later_read.tick}"
    )


# -------------------------------------------------------------------- #
# Small helpers
# -------------------------------------------------------------------- #


def _truncate(value: str, max_chars: int) -> str:
    """Truncate ``value`` to ``max_chars`` characters with a Unicode ellipsis.

    Counts Unicode code points (``len(str)``), not bytes — emoji and
    non-ASCII characters count as one each. Strings at or below
    ``_TRUNCATE_MIN_VISIBLE`` are returned unmodified even if the caller
    passes a smaller ``max_chars`` (defensive: keeps the output legible
    when the threshold table changes).
    """
    if len(value) <= _TRUNCATE_MIN_VISIBLE:
        return value
    if len(value) <= max_chars:
        return value
    # Reserve one slot for the ellipsis so total visible width is
    # ``max_chars`` characters.
    keep = max(_TRUNCATE_MIN_VISIBLE, max_chars - 1)
    return value[:keep] + _ELLIPSIS


def _format_dollars(amount: float) -> str:
    """Render USD with thousands separators and no fractional part.

    Costs are floors (per the detection-report contract). Showing cents
    would imply precision the model does not have.
    """
    return f"{int(round(amount)):,}"
