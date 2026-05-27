# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""Trace loader — merge engine + corruption detection (Unit 3 of D v1).

Consumer side of the replay-trace contract documented in
``docs/proposals/replay_trace_format.md``. Reads a captured session
directory and exposes:

- :class:`LoadedTrace` — parsed manifest + ``streams_present`` set + a
  ``merged()`` iterator yielding entries in the documented merge order.
- :func:`load` — opens the manifest, validates stream presence, and
  returns a :class:`LoadedTrace`.

Three error classes carry actionable next-step pointers (per §5.4 of
the trace-format spec):

- :class:`ManifestMissingOrUnreadableError` — raised eagerly by
  :func:`load` when ``manifest.json`` cannot be parsed.
- :class:`MultiInstanceTraceError` — raised lazily from
  :meth:`LoadedTrace.merged` when two ``instance_id`` values are
  observed in the same stream.
- :class:`TraceCorruptionError` — raised lazily when two entries in a
  stream share ``(instance_id, sequence_number)``.

The merge is a streaming heap-merge over open file iterators (see
``_iter_merged``). The full trace is never materialized in memory —
memory cost is O(open files), not O(events). This is the load-bearing
invariant for partner traces with hundreds of thousands of entries.
"""

from __future__ import annotations

import heapq
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterator

__all__ = [
    "LoadedTrace",
    "ManifestMissingOrUnreadableError",
    "MultiInstanceTraceError",
    "TraceCorruptionError",
    "load",
]

_STATE_LOG = "state_log"
_CONTENT_AUDIT_LOG = "content_audit_log"
_STRICT_DENY_AUDIT = "strict_deny_audit"

# Merge-rule priority per docs/proposals/replay_trace_format.md §5.1:
# at equal tick, state_log sorts before content_audit_log because
# intra-tick state transitions logically precede the reads they enable.
# Predicates compare `tick` fields directly (not merge order) when
# deciding AMBIGUOUS vs CONFIRMED, so this tiebreaker is purely for
# replay-walk determinism.
_STREAM_PRIORITY: dict[str, int] = {
    _STATE_LOG: 0,
    _CONTENT_AUDIT_LOG: 1,
    _STRICT_DENY_AUDIT: 2,
}

_KNOWN_STREAMS: frozenset[str] = frozenset(_STREAM_PRIORITY)


# ---------------------------------------------------------------------------
# Error classes
# ---------------------------------------------------------------------------


class ManifestMissingOrUnreadableError(RuntimeError):
    """``manifest.json`` does not exist or fails JSON parse.

    Raised eagerly by :func:`load` — partial walks are not possible
    without a manifest. Message points at the offending path so partners
    can re-capture or fix the directory.
    """


class MultiInstanceTraceError(RuntimeError):
    """Two distinct ``instance_id`` values observed in the same stream.

    v1 supports single-coordinator-instance traces only. Raised lazily
    from the iterator so partial walks succeed up to the boundary —
    Unit 5's CLI maps this to exit code 3 and surfaces the D+1
    roadmap pointer to the operator.
    """


class TraceCorruptionError(RuntimeError):
    """Two entries in a stream share ``(instance_id, sequence_number)``.

    Defense-in-depth against partner-written callbacks (CrewAI /
    AutoGen direct wiring) that don't honor fsync per line — when
    ``ArtifactRegistry._seq`` rolls back on a write failure but the
    failed entry was already on disk, the next successful entry
    reuses the rolled-back seq and collides. Caught here so partner
    traces fail loudly rather than silently mis-classifying.

    Raised lazily from the iterator with the duplicate seq value, the
    offending file path, and the line number of the second occurrence.
    """


# ---------------------------------------------------------------------------
# LoadedTrace dataclass
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class LoadedTrace:
    """Parsed manifest + open-stream surface for a captured session.

    ``streams_present`` reconciles ``manifest.streams`` against what is
    actually on disk: a stream declared in the manifest but missing
    from disk is NOT in this set. Unit 5's CLI uses the divergence
    between declared-and-present vs declared-but-missing to distinguish
    user opt-out (exit 0) from capture-bug (exit 2). Loader does not
    raise on this divergence; reporting is the CLI's job.
    """

    session_dir: Path
    manifest: dict[str, Any]
    streams_present: set[str]
    # Internal: the on-disk paths for streams that are present. Keyed by
    # stream name so ``merged()`` can build per-stream file iterators.
    _stream_paths: dict[str, Path] = field(default_factory=dict, repr=False)

    def merged(self) -> Iterator[tuple[str, dict[str, Any]]]:
        """Streaming heap-merge across the present streams.

        Yields ``(stream_kind, entry)`` tuples in
        ``(tick asc, stream_kind_priority asc, sequence_number asc)``
        order per the trace-format spec §5.1.

        Validation (multi-instance, duplicate-seq) is performed lazily
        as entries flow through — partial walks succeed up to the
        first offending entry, matching the spec's lazy-raise contract.
        """
        return _iter_merged(self._stream_paths)


# ---------------------------------------------------------------------------
# Loader entry point
# ---------------------------------------------------------------------------


def load(session_dir: Path | str) -> LoadedTrace:
    """Read ``manifest.json`` and prepare the merged-stream iterator.

    Reconciles the manifest's ``streams`` declaration against on-disk
    presence; the returned ``streams_present`` is the intersection.
    """
    session_dir = Path(session_dir)
    manifest = _read_manifest(session_dir)
    declared = _declared_streams(manifest)
    present_paths = {
        name: path
        for name, path in (
            (name, session_dir / f"{name}.jsonl") for name in declared
        )
        if path.exists()
    }
    return LoadedTrace(
        session_dir=session_dir,
        manifest=manifest,
        streams_present=set(present_paths),
        _stream_paths=present_paths,
    )


# ---------------------------------------------------------------------------
# Manifest reading
# ---------------------------------------------------------------------------


def _read_manifest(session_dir: Path) -> dict[str, Any]:
    manifest_path = session_dir / "manifest.json"
    if not manifest_path.exists():
        raise ManifestMissingOrUnreadableError(
            f"manifest.json not found at {manifest_path!s}; "
            f"re-capture with CCSStore.record_to(path) or "
            f"record_callbacks(path, accept_unverified=True) — see "
            f"docs/proposals/replay_trace_format.md §1"
        )
    try:
        raw = manifest_path.read_text(encoding="utf-8")
    except OSError as exc:
        raise ManifestMissingOrUnreadableError(
            f"manifest.json at {manifest_path!s} is unreadable: {exc}"
        ) from exc
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ManifestMissingOrUnreadableError(
            f"manifest.json at {manifest_path!s} is not valid JSON "
            f"(line {exc.lineno}, col {exc.colno}): {exc.msg}"
        ) from exc
    if not isinstance(parsed, dict):
        raise ManifestMissingOrUnreadableError(
            f"manifest.json at {manifest_path!s} must be a JSON object, "
            f"got {type(parsed).__name__}"
        )
    return parsed


def _declared_streams(manifest: dict[str, Any]) -> list[str]:
    raw = manifest.get("streams", [])
    if not isinstance(raw, list):
        raise ManifestMissingOrUnreadableError(
            "manifest.streams must be a list; got "
            f"{type(raw).__name__}"
        )
    # Preserve declaration order from the manifest after filtering to
    # known stream kinds — strict_deny_audit is reserved in v1 but a
    # future declaration should not crash the loader.
    return [name for name in raw if name in _KNOWN_STREAMS]


# ---------------------------------------------------------------------------
# Streaming heap-merge
# ---------------------------------------------------------------------------


def _iter_stream_file(path: Path, stream_name: str) -> Iterator[tuple[
    int, int, int, str, int, dict[str, Any], Path, int,
]]:
    """Yield merge-key + entry tuples for one open JSONL file.

    The key shape is ``(tick, stream_priority, sequence_number,
    stream_name, counter)`` — heapq.merge picks the smallest by
    lexicographic comparison. ``counter`` is a per-stream monotonic
    integer that guarantees total order without ever comparing the
    ``dict`` entry at position 5, making the merge TypeError-safe even
    if a future stream accidentally reuses a priority value.
    """
    priority = _STREAM_PRIORITY[stream_name]
    with path.open("r", encoding="utf-8") as fh:
        for counter, (line_number, line) in enumerate(
            enumerate(fh, start=1), start=0
        ):
            line = line.strip()
            if not line:
                continue
            try:
                entry = json.loads(line)
            except json.JSONDecodeError as exc:
                raise TraceCorruptionError(
                    f"stream {stream_name!r} contains malformed JSON at "
                    f"{path!s}:{line_number}: {exc.msg}"
                ) from exc
            tick = entry.get("tick")
            seq = entry.get("sequence_number")
            if not isinstance(tick, int) or not isinstance(seq, int):
                raise TraceCorruptionError(
                    f"stream {stream_name!r} entry at {path!s}:{line_number} "
                    f"is missing required int field 'tick' or "
                    f"'sequence_number'"
                )
            yield (tick, priority, seq, stream_name, counter, entry, path, line_number)


def _iter_merged(
    stream_paths: dict[str, Path],
) -> Iterator[tuple[str, dict[str, Any]]]:
    """Heap-merge open-file iterators and apply lazy validations.

    STREAMING CONTRACT: this function NEVER materializes the merged
    stream into a list. ``heapq.merge`` consumes the per-file
    generators on demand; per-stream bookkeeping is O(1) state. Open
    file handles are closed when the per-stream generators exhaust
    (via the ``with`` block in :func:`_iter_stream_file`) — partial
    walks may leak fds until garbage collection, acceptable for v1
    since the CLI consumes the iterator to completion or aborts the
    process on error.
    """
    per_stream_iters = [
        _iter_stream_file(path, name)
        for name, path in sorted(stream_paths.items())
    ]
    # Per-stream validation state. Lives outside the generator loop so
    # each stream's invariants are tracked independently — a multi-
    # instance jump on state_log does not poison content_audit_log's
    # tracker, matching the spec's per-stream framing.
    #
    # ``seen_seq`` is a dict keyed by ``(stream, instance_id, seq)`` →
    # ``(path, line)`` of first occurrence so the duplicate error can
    # name both lines. Grows O(N) with the number of entries — every
    # unique (stream, instance_id, seq) triple is retained to detect
    # late-appearing duplicates anywhere in the trace walk.
    seen_instance_id: dict[str, str] = {}
    seen_seq: dict[tuple[str, str, int], tuple[Path, int]] = {}

    try:
        for tick, _priority, seq, stream_name, _counter, entry, path, line_no in heapq.merge(
            *per_stream_iters
        ):
            _check_instance_id(stream_name, entry, seen_instance_id)
            _check_duplicate_seq(stream_name, entry, seq, path, line_no, seen_seq)
            yield stream_name, entry
    finally:
        for it in per_stream_iters:
            it.close()


def _check_instance_id(
    stream_name: str,
    entry: dict[str, Any],
    seen: dict[str, str],
) -> None:
    instance_id = entry.get("instance_id")
    if instance_id is None:
        return
    previous = seen.get(stream_name)
    if previous is None:
        seen[stream_name] = instance_id
        return
    if instance_id != previous:
        raise MultiInstanceTraceError(
            "per-instance-replay is roadmapped for D+1; split the trace "
            "by instance_id or re-capture from a single coordinator "
            f"session (stream={stream_name!r} observed "
            f"instance_id={previous!r} then {instance_id!r})"
        )


def _check_duplicate_seq(
    stream_name: str,
    entry: dict[str, Any],
    seq: int,
    path: Path,
    line_no: int,
    seen: dict[tuple[str, str, int], tuple[Path, int]],
) -> None:
    # (stream, instance_id) is the uniqueness scope per the trace-format
    # spec §5.4 — sequence_number is a per-stream counter that may legally
    # reset across instance_id changes (though those raise MultiInstance
    # first; defense in depth).
    instance_id = entry.get("instance_id") or ""
    key = (stream_name, instance_id, seq)
    prior = seen.get(key)
    if prior is not None:
        first_path, first_line = prior
        raise TraceCorruptionError(
            f"stream {stream_name!r} duplicate sequence_number={seq} "
            f"detected at {path!s}:{line_no} (first occurrence at "
            f"{first_path!s}:{first_line}); partner-written callbacks "
            "must honor fsync-per-line — see "
            "docs/proposals/replay_trace_format.md §5.4"
        )
    seen[key] = (path, line_no)
