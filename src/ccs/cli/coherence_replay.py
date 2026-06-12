# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""``agent-coherence-replay`` — invariant replay CLI (Unit 5 of D v1).

Two modes share one console script:

1. **Default — invariant replay** (``agent-coherence-replay <session_dir>``):
   walks a captured session directory (manifest.json + per-stream JSONL)
   through the predicate engine and emits human-readable or JSON findings.
2. **``resolve`` subcommand** (Unit 6 of item N v1 / R5b): answers "bytes at
   version k" against a (restarted) coordinator ``.coherence/state.db`` by
   opening it READ-ONLY and calling ``read_at_version`` — the restart-survival
   proof. Additive; the bare positional invocation above is byte-for-byte
   unchanged (see :func:`main` for how the modes are dispatched without the
   subparser shadowing a plain ``<session_dir>`` argument).

Exit-code mapping — the default mode owns ``0–4`` (resolved in plan Open
Question P1-A; mirrors ``docs/proposals/replay_trace_format.md`` §7.3); the
``resolve`` mode DELIBERATELY EXTENDS the space with ``5+`` so a script can tell
a resolve outcome from a replay outcome by exit code alone:

- ``0`` — clean OR all SKIPPED entries are compliance opt-outs
  (``opted_out=True``) declared in ``manifest.streams``; ALSO a resolve WIN
  (retained bytes found at the requested version).
- ``1`` — ≥1 CONFIRMED breach.
- ``2`` — ≥1 SKIPPED with ``opted_out=False`` (manifest declared the
  stream but the file is missing — capture-side bug; surfaces as a
  distinct exit code so CI catches it instead of treating it as clean).
- ``3`` — trace error (``MultiInstanceTraceError``, ``TraceCorruptionError``,
  ``ManifestMissingOrUnreadableError``). The exception's message is printed to
  stderr verbatim; tracebacks are caught so partners get the actionable
  next-step pointer instead of Python internals.
- ``4`` — internal error (any other uncaught exception). Decouples CLI bugs
  from CONFIRMED breach (exit 1) so agents triage cleanly. The exception type +
  message land on stderr; Python tracebacks are swallowed.

``resolve``-mode exit codes (5+) — one per read-at-version rejection reason and
one per resolver open/lookup error, so a script branches on the code (and the
JSON ``reason`` slug) without parsing prose:

- ``5``  — ``current_version`` rejection (history surface serves history only;
  current bytes are read via the protocol fetch path, never here).
- ``6``  — ``not_retained`` rejection (the version is in range but no servable
  row exists: never captured, K-evicted, or T-expired — deliberately one
  reason).
- ``7``  — ``unknown_artifact`` rejection (the artifact id is unknown to the
  registry; deleted ≡ never-existed).
- ``8``  — ``retention_off`` rejection (retention was never enabled for this
  store).
- ``9``  — ``epoch_mismatch`` rejection (``--expected-epoch`` != the store
  epoch; the store was reset since the caller captured it).
- ``10`` — ``future_version`` rejection (version > current — hints at a second
  coordinator writing the same store).
- ``11`` — resolver CONFIG error: the ``--db`` path is missing (no store
  materialized), OR an ``--instance-id`` cross-check disagreed with the store's
  persisted identity. (Both are caller/config misuse.)
- ``12`` — resolver STORE error: the store needs recovery (hot WAL a read-only
  conn cannot replay), is locked (SQLITE_BUSY), is corrupt (non-sqlite), or is a
  v1/wrong-schema db (read-only mode performs no migration). The JSON ``reason``
  slug distinguishes ``needs_recovery`` / ``db_busy`` / ``db_corrupt`` /
  ``schema_version_mismatch`` so scripts stay precise within this class.
- ``13`` — resolver LOOKUP error: a workspace-path selector matched no
  ``artifacts.name`` row (a by-path miss; distinct from the by-id
  ``unknown_artifact`` rejection above so the two never blur).

A ``ValueError`` from the service (``--version < 1``, caller misuse) is NOT a
resolver reason; ``resolve`` mode surfaces it as exit ``4`` (internal/usage)
with the message on stderr.

Pipe-close handling: ``BrokenPipeError`` (e.g., from ``| head -5``) exits ``0``
in both modes — consumer-closed-pipe is not a failure mode and should not poison
exit-code-driven CI pipelines.

AMBIGUOUS suppression resolved in plan Open Question P1-B: per-finding output
suppresses AMBIGUOUS by default; ``--include-ambiguous`` opts in. Summary ALWAYS
counts AMBIGUOUS, and the callout fires when count exceeds
``--ambiguous-threshold`` (default 10), naming both remedies.

JSON error envelope (Gated #15 resolution): when ``--json`` is active and a
trace error fires (exit 3), a final NDJSON object is written to stdout before
the human prose hits stderr::

    {"kind": "error", "exit_code": 3, "exception": "<ClassName>",
     "message": "..."}

Keeps stdout self-contained for ``--json`` consumers; the stderr line remains
for human log tailing. The pre-flight session-directory check raises
``SessionDirectoryNotFoundError`` (a ``ReplayTraceError`` subclass) so the
envelope logic stays centralized in the outer catch.

Content-safe-by-default (``resolve`` mode security decision, plan Unit 6
Approach): the DEFAULT output (human + ``--json``) carries METADATA ONLY —
``version``, ``coordinator_epoch``, ``captured_at``, ``content_hash``
(sha-256 over the bytes), ``content_length`` — plus a machine-readable
``reason`` on rejection. Retained BYTES reach stdout/a file ONLY via an explicit
``--include-content`` (base64 for BLOB/bytes with a ``content_encoding`` field;
str as-is) or ``--output-file`` (raw bytes, file created at ``0o600``). This is
the first surface piping retained bytes outside the 0600-protected store —
terminals, CI logs, and shell history must not capture secrets by default.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import sys
from pathlib import Path
from typing import Sequence

from ccs.replay import (
    ReplayTraceError,
    SessionDirectoryNotFoundError,
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
#
# NOTE: the Unit 6 ``ResolverError`` family also subclasses
# ``ReplayTraceError``, but ``resolve`` mode is dispatched BEFORE this guard
# (its own try/except in ``_run_resolve_guarded``), so resolver errors never
# fall through to the exit-3 default handler. The default replay path can never
# raise a ResolverError (it never imports the resolver).
_TRACE_ERRORS: tuple[type[ReplayTraceError], ...] = (ReplayTraceError,)

# resolve-mode exit codes (5+) — keyed by the wire-stable read_at_version reason
# so the mapping is impossible to drift from the constants in core.exceptions.
# Imported lazily inside the resolve path to honor the no-eager-optional-import
# discipline (this dict is built there, not at module import).
_RESOLVE_REJECTION_EXIT_CODE_NOTE = (
    "current_version=5 not_retained=6 unknown_artifact=7 retention_off=8 "
    "epoch_mismatch=9 future_version=10"
)


def build_parser() -> argparse.ArgumentParser:
    """Build the DEFAULT-mode (invariant replay) parser.

    Byte-compatibility contract: this parser is byte-for-byte the pre-Unit-6
    parser — a plain ``agent-coherence-replay <session_dir>`` parses exactly as
    before. The ``resolve`` subcommand is a SEPARATE parser
    (:func:`build_resolve_parser`); ``main`` dispatches to it only when ``argv``
    begins with the literal ``resolve`` token, so adding the new mode never
    changes how a bare positional is parsed (no subparser shadows it).
    """
    parser = argparse.ArgumentParser(
        prog="agent-coherence-replay",
        description=(
            "Replay a captured coordinator session and report invariant "
            "breaches. Consumes the trace format documented in "
            "docs/proposals/replay_trace_format.md. For read-at-version "
            "resolution against a (restarted) coordinator store, use the "
            "'resolve' subcommand: agent-coherence-replay resolve --help."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Exit codes (default replay mode):\n"
            "  0  Clean OR all SKIPPED reasons opted out via manifest streams=\n"
            "     (also: BrokenPipeError — consumer closed the pipe early)\n"
            "  1  >=1 CONFIRMED invariant breach\n"
            "  2  >=1 SKIPPED for a stream declared but absent on disk "
            "(capture bug)\n"
            "  3  Trace error (manifest missing, MULTI_INSTANCE_TRACE, "
            "TRACE_CORRUPTION_DUPLICATE_SEQ)\n"
            "  4  Internal error (uncaught exception; CLI bug — file an issue)\n"
            "\n"
            "Read-at-version resolution: 'agent-coherence-replay resolve "
            "--db <state.db> --artifact <path|uuid> --version <n>'.\n"
            "  resolve exit codes EXTEND the space (5+): "
            f"{_RESOLVE_REJECTION_EXIT_CODE_NOTE}; 11 config error (missing db / "
            "instance-id mismatch); 12 store error (needs_recovery / db_busy / "
            "db_corrupt / schema_version_mismatch); 13 unknown_artifact_path. "
            "See 'resolve --help'.\n"
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


def build_resolve_parser() -> argparse.ArgumentParser:
    """Build the ``resolve`` subcommand parser (read-at-version; Unit 6 / R5b).

    A standalone parser (prog ``agent-coherence-replay resolve``) so the default
    parser stays byte-identical. Content-safe by default: ``--include-content``
    and ``--output-file`` are the only ways retained bytes leave the process.
    """
    parser = argparse.ArgumentParser(
        prog="agent-coherence-replay resolve",
        description=(
            "Resolve 'bytes at version k' against a (restarted) coordinator "
            "store. Opens .coherence/state.db READ-ONLY (never migrates, never "
            "creates) and calls read_at_version. Output is content-safe by "
            "default (metadata only); retained bytes leave the process ONLY via "
            "--include-content or --output-file. Note: a rejection can become "
            "servable history the moment a peer commits a newer version, and "
            "current bytes are never served here (read those via the protocol "
            "fetch path)."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Exit codes (resolve mode):\n"
            "  0   WIN — retained bytes resolved at the requested version\n"
            "  5   current_version (use the protocol fetch path for current)\n"
            "  6   not_retained (never captured / K-evicted / T-expired)\n"
            "  7   unknown_artifact (id unknown; deleted == never-existed)\n"
            "  8   retention_off (retention never enabled for this store)\n"
            "  9   epoch_mismatch (--expected-epoch != store epoch)\n"
            "  10  future_version (version > current; hints at a 2nd coordinator)\n"
            "  11  config error: missing --db, or --instance-id mismatch\n"
            "  12  store error: needs_recovery / db_busy / db_corrupt / "
            "schema_version_mismatch\n"
            "  13  unknown_artifact_path (--artifact path matched no row)\n"
            "  4   usage error (e.g. --version < 1) or internal error\n"
        ),
    )
    parser.add_argument(
        "--db",
        type=Path,
        required=True,
        help=(
            "Path to the coordinator .coherence/state.db to resolve against. "
            "Opened READ-ONLY; a missing path fails (exit 11) and NEVER creates "
            "a fresh store."
        ),
    )
    parser.add_argument(
        "--artifact",
        required=True,
        help=(
            "Artifact selector: a workspace path (artifacts.name, UNIQUE) or a "
            "raw artifact UUID. A path that matches no row fails (exit 13); an "
            "unknown UUID returns the unknown_artifact rejection (exit 7)."
        ),
    )
    parser.add_argument(
        "--version",
        type=int,
        required=True,
        help=(
            "The 1-based version to resolve. Must be >= 1 (sub-1 is a usage "
            "error, exit 4). version == current is rejected (exit 5); the "
            "history surface serves history only."
        ),
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help=(
            "Emit a single JSON object (metadata-only by default) instead of "
            "human-readable text. Carries the wire-stable 'reason' on rejection "
            "so scripts never parse prose."
        ),
    )
    parser.add_argument(
        "--include-content",
        action="store_true",
        help=(
            "Include the retained body in the output (bytes are base64-encoded "
            "with a content_encoding field; str is emitted as-is). OFF by "
            "default: this is the first surface piping retained bytes outside "
            "the 0600 store — keep secrets out of terminals/CI logs/history."
        ),
    )
    parser.add_argument(
        "--output-file",
        type=Path,
        default=None,
        help=(
            "Write the RAW retained bytes to this path (created at 0o600). The "
            "metadata still goes to stdout. Use this instead of "
            "--include-content when piping binary content."
        ),
    )
    parser.add_argument(
        "--expected-epoch",
        default=None,
        help=(
            "If given, the resolve rejects epoch_mismatch (exit 9) unless this "
            "equals the store's coordinator_epoch. MANUAL flag — epoch-at-"
            "capture in the recorder is a separate deferred task."
        ),
    )
    parser.add_argument(
        "--instance-id",
        default=None,
        help=(
            "Optional identity cross-check: verify this equals the store's "
            "persisted instance_id (from a trace manifest) before serving any "
            "bytes. A mismatch fails (exit 11) — the resolve is pointed at the "
            "wrong store."
        ),
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Entry point — returns the exit code; never calls ``sys.exit``.

    Mode dispatch (the byte-compat hinge): if the FIRST positional token in
    ``argv`` is the literal ``resolve``, route to the resolve subcommand;
    otherwise run the default invariant-replay path with the unchanged parser.
    Peeking at ``argv`` (rather than wiring ``add_subparsers`` onto the default
    parser) is what keeps a plain ``agent-coherence-replay <session_dir>``
    parsing identical to the pre-Unit-6 CLI — a subparser would otherwise
    consume ``<session_dir>`` as an (invalid) subcommand name.

    Default-mode guards in order of specificity:

    - ``_TRACE_ERRORS + OSError`` (exit 3): the documented corrupted-trace
      family + general read-time IO errors. The full guard wraps load + walk +
      emit because the loader's iterator is lazy: MultiInstance / TraceCorruption
      raise mid-walk, not at ``load()`` time. Catching only ``load()`` would leak
      a traceback when the corruption hides past the manifest.
    - ``BrokenPipeError`` (exit 0): consumer closed the pipe (e.g. ``| head -5``).
      Not a failure; exit cleanly so CI scripts that compose us with pagers /
      line-limiters don't accumulate false positives. The ``__main__`` wrapper
      also guards a residual BrokenPipeError during interpreter shutdown.
    - ``Exception`` (exit 4): anything else uncaught. Surfaces the exception type
      + message on stderr; the traceback is swallowed so partners get an
      actionable signal instead of Python internals. Distinct from exit 1
      (CONFIRMED breach) so agents triage cleanly.
    """
    argv_list = list(sys.argv[1:] if argv is None else argv)
    if argv_list and argv_list[0] == "resolve":
        return _run_resolve_guarded(argv_list[1:])

    args = build_parser().parse_args(argv_list)
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
        # JSON consumers need a self-contained stdout — emit the error
        # envelope BEFORE the human prose so an agent capturing stdout
        # can parse a single NDJSON stream end-to-end. Use write+flush
        # rather than print so a downstream BrokenPipeError surfaces
        # cleanly (Gated #1 handles BrokenPipeError end-to-end).
        if args.json:
            sys.stdout.write(json.dumps({
                "kind": "error",
                "exit_code": 3,
                "exception": type(exc).__name__,
                "message": str(exc),
            }) + "\n")
            sys.stdout.flush()
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
        # Raise rather than print+return so the outer trace-error catch
        # in main() handles both the stderr prose and the --json error
        # envelope in one place (Gated #15).
        raise SessionDirectoryNotFoundError(
            f"session directory not found: {args.session_dir}"
        )
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


# ---------------------------------------------------------------------------
# resolve subcommand (Unit 6 / R5b) — read-at-version against a read-only store
# ---------------------------------------------------------------------------


def _run_resolve_guarded(resolve_argv: Sequence[str]) -> int:
    """Parse + dispatch the resolve subcommand under the same guard discipline.

    ``BrokenPipeError`` → exit 0 (pipe closed). Resolver errors and rejections
    are handled INSIDE ``_run_resolve`` (each mapped to its own 5+ exit code);
    anything else uncaught → exit 4 with the type/message on stderr (no
    traceback), matching the default mode's internal-error contract. A
    ``ValueError`` (``--version < 1``) is caller misuse → exit 4 as a usage
    error.
    """
    args = build_resolve_parser().parse_args(list(resolve_argv))
    try:
        return _run_resolve(args)
    except BrokenPipeError:
        return 0
    except ValueError as exc:
        # --version < 1 reaches here from the service; a usage error, not a
        # resolver reason. Surface on stderr (+ JSON envelope) and exit 4.
        _emit_resolve_error_envelope(args, reason="usage_error", exit_code=4, exc=exc)
        return 4
    except Exception as exc:  # noqa: BLE001 — intentional translate-and-return
        print(
            f"agent-coherence-replay resolve: internal error: "
            f"{type(exc).__name__}: {exc}",
            file=sys.stderr,
        )
        return 4


def _run_resolve(args: argparse.Namespace) -> int:
    """Resolve one version and render the content-safe output. Returns exit code.

    Lazy imports (optional-extra discipline): the resolver + reason constants are
    imported HERE, not at module top, so ``import ccs.cli.coherence_replay`` does
    not pull the coordinator surface.
    """
    from ccs.core.exceptions import (
        CURRENT_VERSION_REASON,
        EPOCH_MISMATCH_REASON,
        FUTURE_VERSION_REASON,
        NOT_RETAINED_REASON,
        RETENTION_OFF_REASON,
        UNKNOWN_ARTIFACT_REASON,
    )
    from ccs.core.types import VersionedContent, VersionedReadRejection
    from ccs.replay.resolver import (
        ResolverError,
        ResolverInstanceMismatchError,
        ResolverMissingDatabaseError,
        ResolverRequest,
        ResolverUnknownArtifactPathError,
        resolve_version,
    )

    # Per-rejection exit code, keyed on the wire-stable reason so it cannot drift.
    rejection_exit_codes = {
        CURRENT_VERSION_REASON: 5,
        NOT_RETAINED_REASON: 6,
        UNKNOWN_ARTIFACT_REASON: 7,
        RETENTION_OFF_REASON: 8,
        EPOCH_MISMATCH_REASON: 9,
        FUTURE_VERSION_REASON: 10,
    }

    request = ResolverRequest(
        db_path=args.db,
        selector=args.artifact,
        version=args.version,
        expected_epoch=args.expected_epoch,
        expected_instance_id=args.instance_id,
    )

    try:
        outcome = resolve_version(request)
    except (ResolverMissingDatabaseError, ResolverInstanceMismatchError) as exc:
        # Config/caller misuse: missing db (no store) or identity mismatch.
        _emit_resolve_error_envelope(args, reason=exc.reason, exit_code=11, exc=exc)
        return 11
    except ResolverUnknownArtifactPathError as exc:
        # A by-PATH miss — distinct from the by-id unknown_artifact rejection.
        _emit_resolve_error_envelope(args, reason=exc.reason, exit_code=13, exc=exc)
        return 13
    except ResolverError as exc:
        # All remaining store-level open failures (needs_recovery / db_busy /
        # db_corrupt / schema_version_mismatch). The slug distinguishes them.
        _emit_resolve_error_envelope(args, reason=exc.reason, exit_code=12, exc=exc)
        return 12

    if isinstance(outcome, VersionedReadRejection):
        exit_code = rejection_exit_codes[outcome.reason]
        _emit_resolve_rejection(args, outcome)
        return exit_code

    assert isinstance(outcome, VersionedContent)
    _emit_resolve_content(args, outcome)
    return 0


def _content_metadata(content: str | bytes) -> tuple[str, int]:
    """Return ``(sha256_hex, length)`` over the body — the content-safe fields.

    The hash is computed over the raw bytes (utf-8 for a ``str``) so a consumer
    can verify integrity / dedup WITHOUT the bytes themselves reaching stdout.
    ``length`` is the byte length (utf-8-encoded for str), matching the hash
    input so the two describe the same payload.
    """
    raw = content.encode("utf-8") if isinstance(content, str) else content
    return hashlib.sha256(raw).hexdigest(), len(raw)


def _write_output_file(path: Path, content: str | bytes) -> None:
    """Write RAW bytes to ``path`` at mode 0o600 (race-free at creation).

    Pre-creates the file via ``os.open(O_CREAT, 0o600)`` BEFORE writing so the
    bytes never land in a file that briefly existed world-readable (the
    audit_log / state.db 0600-at-creation pattern). A ``str`` body is written as
    utf-8; ``bytes`` verbatim.
    """
    import os

    raw = content.encode("utf-8") if isinstance(content, str) else content
    fd = os.open(str(path), os.O_CREAT | os.O_WRONLY | os.O_TRUNC, 0o600)
    try:
        with os.fdopen(fd, "wb") as fh:
            fh.write(raw)
    except BaseException:
        # If fdopen failed before taking ownership, close the raw fd.
        try:
            os.close(fd)
        except OSError:
            pass
        raise
    # Tighten in case an operator-broadened pre-existing file kept a wider mode
    # (O_CREAT does not chmod an existing file).
    try:
        os.chmod(str(path), 0o600)
    except OSError:
        pass


def _emit_resolve_content(args: argparse.Namespace, outcome) -> None:
    """Render a WIN — metadata always; bytes only via --include-content/--output-file."""
    content = outcome.content
    content_hash, content_length = _content_metadata(content)
    is_bytes = isinstance(content, bytes)

    if args.output_file is not None:
        _write_output_file(args.output_file, content)

    payload: dict[str, object] = {
        "kind": "resolved",
        "exit_code": 0,
        "artifact_id": str(outcome.artifact_id),
        "version": outcome.version,
        "coordinator_epoch": outcome.coordinator_epoch,
        "captured_at": outcome.captured_at,
        "content_hash": content_hash,
        "content_length": content_length,
    }
    if args.output_file is not None:
        payload["output_file"] = str(args.output_file)

    if args.include_content:
        # str as-is; bytes base64 with a content_encoding field so a JSON
        # consumer can round-trip the exact bytes. Mirrored in human mode.
        if is_bytes:
            payload["content_encoding"] = "base64"
            payload["content"] = base64.b64encode(content).decode("ascii")
        else:
            payload["content_encoding"] = "utf-8"
            payload["content"] = content

    if args.json:
        _emit_json_line(payload)
        return

    # Human: metadata block; the body (when opted-in) printed last so a reader
    # sees the metadata even if the body is large/binary.
    lines = [
        "resolved retained version",
        f"  artifact_id:       {outcome.artifact_id}",
        f"  version:           {outcome.version}",
        f"  coordinator_epoch: {outcome.coordinator_epoch}",
        f"  captured_at:       {outcome.captured_at}",
        f"  content_hash:      sha256:{content_hash}",
        f"  content_length:    {content_length}",
    ]
    if args.output_file is not None:
        lines.append(f"  output_file:       {args.output_file} (0600)")
    if args.include_content:
        encoding = "base64" if is_bytes else "utf-8"
        lines.append(f"  content_encoding:  {encoding}")
        body = (
            base64.b64encode(content).decode("ascii") if is_bytes else content
        )
        lines.append(f"  content:           {body}")
    _write_human_block("\n".join(lines))


def _emit_resolve_rejection(args: argparse.Namespace, rejection) -> None:
    """Render a typed rejection — reason slug + version metadata, NO body."""
    payload = {
        "kind": "rejected",
        "exit_code": None,  # filled by the caller's exit-code map; informational
        "reason": rejection.reason,
        "artifact_id": str(rejection.artifact_id),
        "requested_version": rejection.requested_version,
        "current_version": rejection.current_version,
        "coordinator_epoch": rejection.coordinator_epoch,
    }
    if args.json:
        # Drop the placeholder exit_code (the process exit code is authoritative;
        # keeping a null here would mislead a scripted consumer).
        payload.pop("exit_code")
        _emit_json_line(payload)
        return
    lines = [
        f"read-at-version rejected: {rejection.reason}",
        f"  artifact_id:       {rejection.artifact_id}",
        f"  requested_version: {rejection.requested_version}",
        f"  current_version:   {rejection.current_version}",
        f"  coordinator_epoch: {rejection.coordinator_epoch}",
    ]
    _write_human_block("\n".join(lines))


def _emit_resolve_error_envelope(
    args: argparse.Namespace, *, reason: str, exit_code: int, exc: Exception
) -> None:
    """Emit a resolver open/lookup error: JSON envelope (if --json) + stderr prose.

    Keeps stdout self-contained for ``--json`` consumers (one JSON object) and
    always writes the human message to stderr for log tailing — the same
    split-stream discipline the default mode's exit-3 envelope uses.
    """
    if getattr(args, "json", False):
        _emit_json_line(
            {
                "kind": "error",
                "exit_code": exit_code,
                "reason": reason,
                "exception": type(exc).__name__,
                "message": str(exc),
            }
        )
    print(f"agent-coherence-replay resolve: {exc}", file=sys.stderr)


def _emit_json_line(payload: dict) -> None:
    """Write one JSON object + newline to stdout with an explicit flush.

    write+flush (not ``print``) so a downstream ``BrokenPipeError`` surfaces
    cleanly to the guard rather than being buffered past the close.
    """
    sys.stdout.write(json.dumps(payload) + "\n")
    sys.stdout.flush()


def _write_human_block(text: str) -> None:
    """Write a human-readable block + newline to stdout with an explicit flush."""
    sys.stdout.write(text + "\n")
    sys.stdout.flush()


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
