# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""Calibration corpus JSONL append for ``ccs-diagnose`` v0-preview (Unit 9).

v0 ships **no** live endpoint. Calibration data is appended to a local
JSONL file the user can share manually if they choose to contribute to
v1 promotion. The file format is validated by
:func:`ccs.validation.validate_log` at the same 3-tuple shape used by
other CCS event streams (``sequence_number``, ``instance_id``,
``schema_version``) — this is the Spike 0 falsification check from the
plan: if ``validate_log`` cannot ingest these entries, the JSONL identity
claim is broken.

NOT collected, stored, logged, or transmitted
=============================================

agent names, artifact names, content, hashes, tool calls, prompts, run
timing, IP addresses.

The append is gated by consent (see
:mod:`ccs.diagnose.telemetry.ConsentState`). When consent is denied (or
an env-var kill switch is active and Unit 8's resolver returned a
denied state), :func:`append_calibration_entry` returns a result with
``written=False`` and a machine-readable ``reason`` — it never raises and
never silently no-ops.

Hard guarantees
===============

* **No import-time side effects.** ``import ccs.diagnose.calibration``
  starts no threads, opens no sockets, reads no files, creates no
  directories. The audit-hook test from Unit 8 (extended in Unit 9)
  enforces this.
* **No new dependencies.** Stdlib only.
* **No network code.** Submission lives in a future unit; v0 only writes
  the local JSONL file. The user shares it manually if they choose.
* **Atomic append.** Each line is one ``os.write`` of a JSON object plus
  trailing ``\\n``, opened with ``O_APPEND``.
"""

from __future__ import annotations

import json
import os
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ccs.diagnose import CCS_DIAGNOSE_LOG_SCHEMA_VERSION
from ccs.diagnose.classifier import ClassifierVerdict
from ccs.diagnose.detection import DetectionReport
from ccs.diagnose.telemetry import ConsentState, payload_for

__all__ = [
    "DEFAULT_CALIBRATION_PATH_RELATIVE",
    "CalibrationWriteResult",
    "calibration_path",
    "append_calibration_entry",
]


DEFAULT_CALIBRATION_PATH_RELATIVE: Path = Path("ccs-diagnose") / "calibration.jsonl"
"""Relative path under ``$XDG_DATA_HOME`` (or ``~/.local/share``) for the JSONL file.

Note this is a **data** path, not config — calibration is accumulated
output, not user preference. Mirrors the XDG Base Directory Specification
distinction (``XDG_CONFIG_HOME`` vs ``XDG_DATA_HOME``).
"""


@dataclass(frozen=True)
class CalibrationWriteResult:
    """Outcome of one :func:`append_calibration_entry` call.

    ``reason`` is a short machine-readable token so callers (the CLI) can
    print actionable messages without parsing free-form text. Known
    values: ``"ok"``, ``"consent_not_granted"``, and any string starting
    with ``"io_error"`` for filesystem failures.
    """

    written: bool
    path: Path | None
    reason: str


def calibration_path() -> Path:
    """Resolve the calibration JSONL path.

    Honors ``$XDG_DATA_HOME`` when set and non-empty; otherwise falls
    back to ``~/.local/share/ccs-diagnose/calibration.jsonl`` (the XDG
    default). Returns the path; does **not** create directories — that
    happens lazily on first write inside :func:`append_calibration_entry`.
    """
    xdg = os.environ.get("XDG_DATA_HOME", "").strip()
    if xdg:
        base = Path(xdg)
    else:
        base = Path.home() / ".local" / "share"
    return base / DEFAULT_CALIBRATION_PATH_RELATIVE


def _build_entry(
    *,
    verdict: ClassifierVerdict,
    report: DetectionReport,
    consent: ConsentState,
) -> dict[str, Any]:
    """Build one JSONL entry dict.

    The 3-tuple required by :func:`ccs.validation.validate_log` is emitted
    at the top level; payload fields from :func:`payload_for` are merged
    flat. ``payload_for``'s own ``schema_version`` is stripped before the
    merge so the output has exactly one ``schema_version`` field — keeping
    the entry both auditable and ``validate_log``-compatible.
    """
    del report  # report fields are not surfaced in the payload
    payload = payload_for(verdict, consent=consent)
    payload_no_schema = {k: v for k, v in payload.items() if k != "schema_version"}
    return {
        # ``validate_log`` 3-tuple (top level, required).
        "sequence_number": 1,
        "instance_id": str(uuid.uuid4()),
        "schema_version": CCS_DIAGNOSE_LOG_SCHEMA_VERSION,
        # Flatten the payload so a reader running ``jq '.verdict_bucket'``
        # sees the field directly without descending into a nested
        # ``payload`` object. This is a deliberate v0 ergonomics choice.
        **payload_no_schema,
    }


def append_calibration_entry(
    *,
    verdict: ClassifierVerdict,
    report: DetectionReport,
    consent: ConsentState,
    path: Path | None = None,
) -> CalibrationWriteResult:
    """Append one calibration entry to the JSONL file.

    Each CLI invocation produces one append, hence one line, with
    ``sequence_number == 1`` and a fresh ``instance_id``. The file grows
    monotonically across invocations; ``validate_log`` resets its
    per-stream counter at every ``instance_id`` boundary so the file
    remains gap-clean.

    Parameters
    ----------
    verdict / report:
        Classifier output for the current run. Forwarded to
        :func:`ccs.diagnose.telemetry.payload_for` to produce the
        anonymized payload merged into the entry.
    consent:
        Already-resolved consent state from the CLI pipeline. The append
        is gated on ``consent.granted``; a denied state short-circuits
        with ``reason="consent_not_granted"`` and no file I/O.
    path:
        Override for the JSONL location. Defaults to
        :func:`calibration_path`.

    Returns
    -------
    :class:`CalibrationWriteResult`
        Describes what happened so the CLI can print an informational
        message. Never raises on filesystem errors — they surface as
        ``reason="io_error: <message>"``.
    """
    if not consent.granted:
        return CalibrationWriteResult(
            written=False,
            path=path,
            reason="consent_not_granted",
        )

    target = path if path is not None else calibration_path()
    parent = target.parent

    # Create the parent directory with restrictive mode if missing. We do
    # NOT aggressively re-chmod existing directories — users may have
    # intentionally relaxed permissions, and the calibration file's own
    # 0o600 mode is the load-bearing protection.
    try:
        parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    except OSError as exc:
        return CalibrationWriteResult(
            written=False,
            path=target,
            reason=f"io_error: {exc}",
        )

    entry = _build_entry(verdict=verdict, report=report, consent=consent)
    line = (
        json.dumps(entry, sort_keys=True, separators=(",", ":"), default=str) + "\n"
    )

    # Open with O_CREAT | O_APPEND, mode 0o600. POSIX guarantees atomic
    # append for writes <= PIPE_BUF; a single JSON entry on one filesystem
    # write should fit (the diagnose payload is ~1KB worst-case).
    try:
        fd = os.open(
            os.fspath(target),
            os.O_CREAT | os.O_APPEND | os.O_WRONLY,
            0o600,
        )
    except OSError as exc:
        return CalibrationWriteResult(
            written=False,
            path=target,
            reason=f"io_error: {exc}",
        )

    try:
        try:
            os.write(fd, line.encode("utf-8"))
        finally:
            os.close(fd)
    except OSError as exc:
        return CalibrationWriteResult(
            written=False,
            path=target,
            reason=f"io_error: {exc}",
        )

    return CalibrationWriteResult(written=True, path=target, reason="ok")
