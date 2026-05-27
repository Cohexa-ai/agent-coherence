# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""``agent-coherence-replay`` — invariant replay CLI (Unit 5 of D v1).

Walks a captured session directory (manifest.json + per-stream JSONL)
through the predicate engine and emits human-readable or JSON findings.

Exit-code mapping resolved in plan Open Question P1-A (mirrors
``docs/proposals/replay_trace_format.md`` §7.3):

- ``0`` — clean OR all SKIPPED entries are compliance opt-outs
  (``opted_out=True``) declared in ``manifest.streams``.
- ``1`` — ≥1 CONFIRMED breach.
- ``2`` — ≥1 SKIPPED with ``opted_out=False`` (manifest declared the
  stream but the file is missing — capture-side bug; surfaces as a
  distinct exit code so CI catches it instead of treating it as clean).
- ``3`` — trace error (``MultiInstanceTraceError``,
  ``TraceCorruptionError``, ``ManifestMissingOrUnreadableError``). The
  exception's message is printed to stderr verbatim; tracebacks are
  caught so partners get the actionable next-step pointer instead of
  Python internals.
- ``4`` — internal error (any other uncaught exception). Decouples
  CLI bugs from CONFIRMED breach (exit 1) so agents triage cleanly.
  The exception type + message land on stderr; Python tracebacks are
  swallowed.

Pipe-close handling: ``BrokenPipeError`` (e.g., from ``| head -5``)
exits ``0`` — consumer-closed-pipe is not a failure mode and should
not poison exit-code-driven CI pipelines.

AMBIGUOUS suppression resolved in plan Open Question P1-B: per-finding
output suppresses AMBIGUOUS by default; ``--include-ambiguous`` opts
in. Summary ALWAYS counts AMBIGUOUS, and the callout fires when count
exceeds ``--ambiguous-threshold`` (default 10), naming both remedies.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Sequence

from ccs.replay import (
    ReplayTraceError,
    load,
    run_predicates,
)
from ccs.replay.formatters import emit_human, emit_json
from ccs.replay.predicates import Finding, SummaryFinding

_VALID_INVARIANTS: tuple[str, ...] = (
    "single-writer",
    "monotonic-version",
    "stale-read",
    "lost-write",
)

# Trace-format spec maps every ``ReplayTraceError`` subclass
# (ManifestMissingOrUnreadableError, MultiInstanceTraceError,
# TraceCorruptionError, and any future trace-defect subclass) to exit
# code 3. Catching the base class instead of an explicit tuple means a
# new trace-error subclass auto-routes correctly without touching this
# handler — the previous tuple-based shape needed to be edited each
# time a new error class was added.
_TRACE_ERRORS: tuple[type[ReplayTraceError], ...] = (ReplayTraceError,)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="agent-coherence-replay",
        description=(
            "Replay a captured coordinator session and report invariant "
            "breaches. Consumes the trace format documented in "
            "docs/proposals/replay_trace_format.md."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Exit codes:\n"
            "  0  Clean OR all SKIPPED reasons opted out via manifest streams=\n"
            "     (also: BrokenPipeError — consumer closed the pipe early)\n"
            "  1  >=1 CONFIRMED invariant breach\n"
            "  2  >=1 SKIPPED for a stream declared but absent on disk "
            "(capture bug)\n"
            "  3  Trace error (manifest missing, MULTI_INSTANCE_TRACE, "
            "TRACE_CORRUPTION_DUPLICATE_SEQ)\n"
            "  4  Internal error (uncaught exception; CLI bug — file an issue)\n"
        ),
    )
    parser.add_argument(
        "session_dir",
        type=Path,
        help=(
            "Path to a captured session directory containing manifest.json "
            "plus per-stream .jsonl files."
        ),
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help=(
            "Emit newline-delimited JSON (per-finding lines + one summary "
            "object) matching the spec's §7 schema."
        ),
    )
    parser.add_argument(
        "--invariant",
        action="append",
        choices=list(_VALID_INVARIANTS),
        default=None,
        help=(
            "Restrict the run to the named predicate(s). Repeatable. "
            "Omitting the flag runs all four."
        ),
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help=(
            "Suppress non-breach output. Cron-friendly: a clean trace "
            "(no CONFIRMED + no opted_out=False SKIPPED) emits nothing."
        ),
    )
    parser.add_argument(
        "--include-ambiguous",
        action="store_true",
        help=(
            "Include per-finding details for AMBIGUOUS classifications "
            "(suppressed from default output; same-tick intra-collisions)."
        ),
    )
    parser.add_argument(
        "--ambiguous-threshold",
        type=int,
        default=10,
        help=(
            "Threshold for the AMBIGUOUS summary callout (default: 10). "
            "When the count exceeds the threshold, the summary block "
            "names both remedies (--include-ambiguous + D+1 global-seq)."
        ),
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Entry point — returns the exit code; never calls ``sys.exit``.

    Three guards in order of specificity:

    - ``_TRACE_ERRORS + OSError`` (exit 3): the documented corrupted-
      trace family + general read-time IO errors. The full guard wraps
      load + walk + emit because the loader's iterator is lazy:
      MultiInstance / TraceCorruption raise mid-walk, not at ``load()``
      time. Catching only ``load()`` would leak a traceback when the
      corruption hides past the manifest.
    - ``BrokenPipeError`` (exit 0): consumer closed the pipe (e.g.
      ``| head -5``). Not a failure; exit cleanly so CI scripts that
      compose us with pagers / line-limiters don't accumulate false
      positives. The ``__main__`` wrapper also guards a residual
      BrokenPipeError during interpreter shutdown when Python tries to
      flush stdout to the already-torn-down pipe.
    - ``Exception`` (exit 4): anything else uncaught. Surfaces the
      exception type + message on stderr; the traceback is swallowed
      so partners get an actionable signal instead of Python internals.
      Distinct from exit 1 (CONFIRMED breach) so agents triage cleanly.
    """
    args = build_parser().parse_args(argv)
    try:
        return _run(args)
    except BrokenPipeError:
        # Specific catch FIRST — BrokenPipeError inherits from OSError,
        # so the broader except below would otherwise swallow it into
        # exit 3. Consumer closed the pipe (e.g. ``| head``). Return
        # cleanly; do NOT call sys.stderr.close() — it breaks pytest
        # capture. The __main__ wrapper handles the residual
        # shutdown-time BrokenPipeError via os._exit.
        return 0
    except (ReplayTraceError, OSError) as exc:
        print(f"agent-coherence-replay: {exc}", file=sys.stderr)
        return 3
    except Exception as exc:  # noqa: BLE001 — intentional translate-and-return
        print(
            f"agent-coherence-replay: internal error: "
            f"{type(exc).__name__}: {exc}",
            file=sys.stderr,
        )
        return 4


def _run(args: argparse.Namespace) -> int:
    """Load, run predicates, emit, and return the exit code.

    Split from ``main()`` so the trace-error guard stays a single
    try/except over the full pipeline without bloating ``main()`` past
    the style-guide line ceiling.
    """
    if not args.session_dir.exists():
        print(
            f"agent-coherence-replay: session directory not found: "
            f"{args.session_dir}",
            file=sys.stderr,
        )
        return 3
    loaded = load(args.session_dir)
    findings, summary = run_predicates(loaded, invariants=args.invariant)
    if args.json:
        emit_json(
            findings,
            summary,
            include_ambiguous=args.include_ambiguous,
            ambiguous_threshold=args.ambiguous_threshold,
            quiet=args.quiet,
            manifest=loaded.manifest,
            streams_present=loaded.streams_present,
        )
    else:
        emit_human(
            findings,
            summary,
            include_ambiguous=args.include_ambiguous,
            ambiguous_threshold=args.ambiguous_threshold,
            quiet=args.quiet,
        )
    return _exit_code(findings, summary)


def _exit_code(
    findings: list[Finding],
    summary: list[SummaryFinding],
) -> int:
    """Apply the resolved exit-code mapping from plan P1-A."""
    has_confirmed = any(f.severity == "CONFIRMED" for f in findings)
    if has_confirmed:
        return 1
    has_unannounced_skip = any(s.opted_out is False for s in summary)
    if has_unannounced_skip:
        return 2
    return 0


if __name__ == "__main__":
    # Belt-and-suspenders for the entry point: a BrokenPipeError can
    # still surface during interpreter shutdown if Python tries to flush
    # stdout to a torn-down pipe after ``main()`` returns. Catching it
    # here and exiting via ``os._exit`` skips the second flush attempt
    # that would otherwise print "Exception ignored in: ..." to stderr.
    import os

    try:
        raise SystemExit(main())
    except BrokenPipeError:
        os._exit(0)
