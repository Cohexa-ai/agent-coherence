# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Log validation helpers for CCS materialized JSONL event streams.

Gap-detection model
-------------------
Three values together make a gap-detectable stream:

* ``sequence_number`` — a per-stream monotonic int starting at 1, incrementing
  by 1 for each emitted entry.  CCS guarantees no gaps in its own emission; a
  gap means at least one entry was dropped *after* emission (e.g. partial write,
  truncation, or line deletion).
* ``instance_id`` — a UUID4 string fixed for one ``CCSStore`` session.  When
  ``instance_id`` changes, ``validate_log`` resets its counter.  A
  ``sequence_number`` returning to 1 at a session boundary is not a gap.
* ``schema_version`` — a constant string (e.g. ``"ccs.state_log.v1"``) that
  names the emitting surface.  Pass the expected value to ``validate_log`` to
  catch schema drift.

"Missing" vs "unset" vs "skipped":  all three map to the same wire
representation only when the emitter conflates them.  CCS never emits entries
with missing fields; consumers should treat ``ValueError`` from ``validate_log``
as "producer bug or pre-upgrade line," not "clean log."

Upgrade-boundary limitation:
  JSONL files that span a CCS upgrade boundary (pre-upgrade entries lack
  ``sequence_number`` / ``instance_id``) will raise ``ValueError`` on the
  first old-format entry. Split the log at the upgrade boundary before calling
  ``validate_log``.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, NamedTuple

from ccs.adapters.events import CCS_METRIC_SCHEMA_VERSION
from ccs.coordinator.registry import CCS_STATE_LOG_SCHEMA_VERSION

__all__ = [
    "Gap",
    "SchemaMismatch",
    "validate_log",
    "CCS_STATE_LOG_SCHEMA_VERSION",
    "CCS_METRIC_SCHEMA_VERSION",
]


class Gap(NamedTuple):
    """A detected sequence gap in a CCS event stream."""

    stream: str    # caller-supplied label, e.g. "state_log" or "metrics"
    expected: int  # last_seen + 1
    found: int     # actual sequence_number in this entry
    at_index: int  # 0-based line number in the JSONL file


class SchemaMismatch(NamedTuple):
    """A schema version that does not match the expected value."""

    stream: str             # caller-supplied label
    found_version: str      # actual schema_version in this entry
    expected_version: str   # the schema_version the caller expected
    at_index: int           # 0-based line number in the JSONL file


def validate_log(
    path: str | Path,
    *,
    stream: str = "state_log",
    schema_version: str | None = None,
) -> tuple[list[Gap], list[SchemaMismatch]]:
    """Return ``([], [])`` on a clean log; non-empty lists identify every problem.

    Parameters
    ----------
    path:
        Path to a JSONL file written by CCS (state log or metrics stream).
    stream:
        A caller-supplied label written into returned ``Gap`` and
        ``SchemaMismatch`` objects. Passing ``"metrics"`` on a state log file
        produces no error — the parameter is a label, not a filter.
    schema_version:
        When provided, each entry's ``schema_version`` field must match this
        value; mismatches are appended to the returned ``SchemaMismatch`` list.
        When ``None`` (the default), schema version checking is skipped.

    Raises
    ------
    ValueError
        If any line is missing ``sequence_number``, ``instance_id``, or
        (when *schema_version* is not ``None``) ``schema_version``. The scan
        aborts at the first missing field; any gaps found before that line are
        discarded.
    json.JSONDecodeError
        If any line is not valid JSON. Propagated to the caller; no partial
        result is returned.
    """
    gaps: list[Gap] = []
    mismatches: list[SchemaMismatch] = []
    last_seen: int = 0
    last_instance_id: str | None = None
    entry_index: int = 0  # 0-based count of non-empty entries; used for at_index

    with Path(path).open(encoding="utf-8") as fh:
        for raw_line in fh:
            raw_line = raw_line.strip()
            if not raw_line:
                continue
            entry: dict[str, Any] = json.loads(raw_line)

            for required_key in ("sequence_number", "instance_id"):
                if required_key not in entry:
                    raise ValueError(f"Entry {entry_index}: missing field '{required_key}'")

            if schema_version is not None and "schema_version" not in entry:
                raise ValueError(f"Entry {entry_index}: missing field 'schema_version'")

            current_instance_id: str = entry["instance_id"]
            if current_instance_id != last_instance_id:
                last_seen = 0
                last_instance_id = current_instance_id

            seq = entry["sequence_number"]
            if not isinstance(seq, int):
                raise ValueError(
                    f"Entry {entry_index}: 'sequence_number' must be int, got {type(seq).__name__!r}"
                )
            if seq != last_seen + 1:
                gaps.append(Gap(stream=stream, expected=last_seen + 1, found=seq, at_index=entry_index))
            last_seen = seq

            if schema_version is not None and entry["schema_version"] != schema_version:
                mismatches.append(
                    SchemaMismatch(
                        stream=stream,
                        found_version=entry["schema_version"],
                        expected_version=schema_version,
                        at_index=entry_index,
                    )
                )

            entry_index += 1

    return gaps, mismatches
