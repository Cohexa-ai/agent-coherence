# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""Human + JSON formatters for the replay CLI (Unit 5 of D v1).

Pure formatting layer over :class:`ccs.replay.predicates.Finding` and
:class:`ccs.replay.predicates.SummaryFinding`. Decoupled from
:mod:`ccs.cli.coherence_replay` so the same emitters can back the
audit-report template (spec §8.1) without re-implementing the schema.

The JSON formatter emits the schema documented in
``docs/proposals/replay_trace_format.md`` §7.1 (per-finding) and §7.2
(summary) verbatim — that contract is consumed by downstream audit
reports and the Survivor-#3 Visualizer, so field shape and ordering
are load-bearing, not stylistic.

AMBIGUOUS suppression contract (resolved in plan Open Questions P1-B):

- Default behavior suppresses per-finding AMBIGUOUS output from BOTH
  human and JSON formatters — same-tick intra-collision noise drowns
  CONFIRMED breaches in trace-heavy pipelines.
- ``--include-ambiguous`` surfaces them at the per-finding level only;
  the summary block ALWAYS includes the AMBIGUOUS count + threshold +
  callout independent of the flag.
- The callout appears when ``ambiguous_count > ambiguous_threshold``
  AND must name BOTH remedies (``--include-ambiguous`` + D+1 global
  sequence_number capture) so partners triaging a noisy trace know
  exactly what to try next.
"""

from __future__ import annotations

import json
import sys
from typing import Any, Iterable, TextIO

from ccs.replay.predicates import Finding, SummaryFinding

__all__ = [
    "emit_human",
    "emit_json",
]


# Stable iteration order across both formatters so per-invariant
# breakdowns don't depend on dict insertion order from a particular
# predicate selection.
_INVARIANT_ORDER: tuple[str, ...] = (
    "single-writer",
    "monotonic-version",
    "stale-read",
    "lost-write",
)


# ---------------------------------------------------------------------------
# Human formatter
# ---------------------------------------------------------------------------


def emit_human(
    findings: Iterable[Finding],
    summary: Iterable[SummaryFinding],
    *,
    include_ambiguous: bool = False,
    ambiguous_threshold: int = 10,
    quiet: bool = False,
    writer: TextIO | None = None,
) -> None:
    """Write a human-readable report to ``writer`` (default stdout).

    ``quiet`` suppresses ALL non-breach output: when there are no
    CONFIRMED findings AND no capture-bug SKIPs (opted_out=False),
    nothing is written. When breaches OR capture-bug skips exist,
    only the breach/skip blocks render (no summary header), so cron
    can scrape the file for content as the failure signal.
    """
    writer = writer or sys.stdout
    findings_list = list(findings)
    summary_list = list(summary)
    counts = _count_findings(findings_list, summary_list)

    cron_silent = quiet and counts["CONFIRMED"] == 0 and not _has_capture_bug(summary_list)
    if cron_silent:
        return

    for finding in findings_list:
        if finding.severity == "AMBIGUOUS" and not include_ambiguous:
            continue
        _write_finding_block(writer, finding)

    if quiet:
        # Breaches/capture-bug-skips present but summary suppressed.
        return

    _write_summary_section(
        writer,
        counts=counts,
        summary_list=summary_list,
        include_ambiguous=include_ambiguous,
        ambiguous_threshold=ambiguous_threshold,
    )


def _write_finding_block(writer: TextIO, finding: Finding) -> None:
    prefix = "[CONFIRMED]" if finding.severity == "CONFIRMED" else "[AMBIGUOUS]"
    start, end = finding.tick_range
    ctx = finding.context or {}
    details = finding.details or {}
    stream = ctx.get("stream", "?")
    seq = ctx.get("sequence_number", "?")
    writer.write(f"{prefix} {finding.invariant}\n")
    writer.write(f"  Tick range: {start} -> {end}\n")
    writer.write(f"  Agents:     {', '.join(finding.agents) if finding.agents else '(none)'}\n")
    writer.write(f"  Artifacts:  {', '.join(finding.artifacts) if finding.artifacts else '(none)'}\n")
    writer.write(f"  Expected:   {details.get('expected', '')}\n")
    writer.write(f"  Observed:   {details.get('observed', '')}\n")
    writer.write(f"  Trace:      {stream}.jsonl seq={seq}\n")
    writer.write("\n")


def _write_summary_section(
    writer: TextIO,
    *,
    counts: dict[str, int],
    summary_list: list[SummaryFinding],
    include_ambiguous: bool,
    ambiguous_threshold: int,
) -> None:
    ambig_word = "shown" if include_ambiguous else "suppressed"
    writer.write(
        f"Summary: {counts['CONFIRMED']} CONFIRMED, "
        f"{counts['AMBIGUOUS']} AMBIGUOUS ({ambig_word}), "
        f"{counts['SKIPPED']} SKIPPED\n"
    )
    writer.write("  By invariant:\n")
    by_inv = counts["by_invariant"]
    width = max(len(name) for name in _INVARIANT_ORDER)
    for name in _INVARIANT_ORDER:
        row = by_inv.get(name, {"CONFIRMED": 0, "AMBIGUOUS": 0, "SKIPPED": 0})
        writer.write(
            f"    {name:<{width}} : "
            f"{row['CONFIRMED']} CONFIRMED, "
            f"{row['AMBIGUOUS']} AMBIGUOUS, "
            f"{row['SKIPPED']} SKIPPED\n"
        )
    if summary_list:
        writer.write("  Skipped invariants:\n")
        for s in summary_list:
            writer.write(f"    {s.invariant}: {s.reason}\n")
    if counts["AMBIGUOUS"] > ambiguous_threshold:
        _write_ambiguous_callout(writer, counts["AMBIGUOUS"], ambiguous_threshold)


def _write_ambiguous_callout(writer: TextIO, count: int, threshold: int) -> None:
    writer.write(
        f"\nNOTE: AMBIGUOUS findings ({count}) exceed threshold ({threshold}).\n"
        f"      Intra-tick collisions are common in this pipeline. Either:\n"
        f"        - Rerun with --include-ambiguous to inspect each candidate.\n"
        f"        - Wait for D+1's global sequence_number capture to eliminate the ambiguity.\n"
    )


# ---------------------------------------------------------------------------
# JSON formatter
# ---------------------------------------------------------------------------


def emit_json(
    findings: Iterable[Finding],
    summary: Iterable[SummaryFinding],
    *,
    include_ambiguous: bool = False,
    ambiguous_threshold: int = 10,
    manifest: dict[str, Any] | None = None,
    streams_present: Iterable[str] | None = None,
    writer: TextIO | None = None,
) -> None:
    """Newline-delimited JSON: per-finding lines + one summary object.

    Schema matches ``docs/proposals/replay_trace_format.md`` §7.1
    (per-finding) and §7.2 (summary) — downstream consumers (audit
    report template, future Visualizer) depend on field shape.
    """
    writer = writer or sys.stdout
    findings_list = list(findings)
    summary_list = list(summary)

    for finding in findings_list:
        if finding.severity == "AMBIGUOUS" and not include_ambiguous:
            continue
        writer.write(json.dumps(_finding_to_obj(finding), sort_keys=False))
        writer.write("\n")

    summary_obj = _build_summary_obj(
        findings=findings_list,
        summary_list=summary_list,
        ambiguous_threshold=ambiguous_threshold,
        manifest=manifest or {},
        streams_present=list(streams_present or []),
    )
    writer.write(json.dumps(summary_obj, sort_keys=False))
    writer.write("\n")


def _finding_to_obj(finding: Finding) -> dict[str, Any]:
    return {
        "kind": "finding",
        "severity": finding.severity,
        "invariant": finding.invariant,
        "agents": list(finding.agents),
        "artifacts": list(finding.artifacts),
        "tick_range": {
            "start": finding.tick_range[0],
            "end": finding.tick_range[1],
        },
        "context": dict(finding.context or {}),
        "details": dict(finding.details or {}),
    }


def _build_summary_obj(
    *,
    findings: list[Finding],
    summary_list: list[SummaryFinding],
    ambiguous_threshold: int,
    manifest: dict[str, Any],
    streams_present: list[str],
) -> dict[str, Any]:
    counts = _count_findings(findings, summary_list)
    ambiguous_count = counts["AMBIGUOUS"]
    callout: str | None = None
    if ambiguous_count > ambiguous_threshold:
        callout = (
            f"AMBIGUOUS findings ({ambiguous_count}) exceed threshold "
            f"({ambiguous_threshold}); intra-tick collisions are common "
            f"in this pipeline. Rerun with --include-ambiguous to inspect, "
            f"or capture global sequence_number (D+1)."
        )
    return {
        "kind": "summary",
        "counts": {
            "CONFIRMED": counts["CONFIRMED"],
            "AMBIGUOUS": counts["AMBIGUOUS"],
            "SKIPPED": counts["SKIPPED"],
        },
        "counts_by_invariant": {
            name: counts["by_invariant"].get(
                name, {"CONFIRMED": 0, "AMBIGUOUS": 0, "SKIPPED": 0}
            )
            for name in _INVARIANT_ORDER
        },
        "skipped_reasons": [
            {
                "invariant": s.invariant,
                "reason": s.reason,
                "stream_required": s.stream_required,
                "opted_out": s.opted_out,
            }
            for s in summary_list
        ],
        "ambiguous_threshold": ambiguous_threshold,
        "ambiguous_callout": callout,
        "trace_metadata": {
            "adapter_type": manifest.get("adapter_type"),
            "start_tick": manifest.get("start_tick"),
            "end_tick": manifest.get("end_tick"),
            "instance_id": manifest.get("instance_id"),
            "streams_present": sorted(streams_present),
        },
    }


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _count_findings(
    findings: list[Finding],
    summary: list[SummaryFinding],
) -> dict[str, Any]:
    """Aggregate counts shared by human + JSON paths."""
    by_inv: dict[str, dict[str, int]] = {
        name: {"CONFIRMED": 0, "AMBIGUOUS": 0, "SKIPPED": 0}
        for name in _INVARIANT_ORDER
    }
    confirmed = 0
    ambiguous = 0
    for f in findings:
        bucket = by_inv.setdefault(
            f.invariant, {"CONFIRMED": 0, "AMBIGUOUS": 0, "SKIPPED": 0}
        )
        if f.severity == "CONFIRMED":
            confirmed += 1
            bucket["CONFIRMED"] += 1
        elif f.severity == "AMBIGUOUS":
            ambiguous += 1
            bucket["AMBIGUOUS"] += 1
    for s in summary:
        bucket = by_inv.setdefault(
            s.invariant, {"CONFIRMED": 0, "AMBIGUOUS": 0, "SKIPPED": 0}
        )
        bucket["SKIPPED"] += 1
    return {
        "CONFIRMED": confirmed,
        "AMBIGUOUS": ambiguous,
        "SKIPPED": len(summary),
        "by_invariant": by_inv,
    }


def _has_capture_bug(summary_list: list[SummaryFinding]) -> bool:
    """True iff any SKIPPED entry is a capture bug (opted_out=False)."""
    return any(s.opted_out is False for s in summary_list)
