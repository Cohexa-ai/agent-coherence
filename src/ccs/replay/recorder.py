# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""Capture-side implementation for invariant replay traces (D v1).

Implements the producer contract defined in
``docs/proposals/replay_trace_format.md``:

- ``manifest.json`` written at ``__enter__`` (header) and re-written
  atomically at ``__exit__`` (finalized fields).
- One JSONL file per declared stream (``state_log``,
  ``content_audit_log``). Each line: ``json.dumps + "\\n"``, flushed,
  then ``os.fsync(fd)`` so the coordinator's ``_seq`` rollback
  (registry.py: ``_seq -= 1`` on callback exception) is durable.
- Caller-supplied callbacks compose with the file-writing callbacks;
  never overridden. The caller callback fires FIRST so its exception
  short-circuits the file write (matches existing rollback semantics).
- Capture-time ``instance_id``-change detection emits a stderr warning
  naming the ``MULTI_INSTANCE_TRACE`` D+1 roadmap item.

The ``streams=`` opt-out (e.g. ``streams={'state_log'}``) controls
which JSONL files are opened on disk. The wrapped audit callback
ALWAYS exists so ``CCSStore.__init__`` keeps ``retain_versions=True``
(line 88 of ``ccsstore.py``) — the per-stream opt-out only suppresses
the file write, not the callback wiring.
"""

from __future__ import annotations

import json
import logging
import os
import sys
import tempfile
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from types import TracebackType
from typing import Any, Callable, Iterator

from ccs.replay.errors import ReplayConfigurationError

logger = logging.getLogger(__name__)

__all__ = [
    "UnverifiedAdapterCaptureError",
    "RecordingSession",
    "record_callbacks",
    "DEFAULT_STREAMS",
    "SCHEMA_VERSION",
    "SCHEMA_NOTE",
]

SCHEMA_VERSION = 0
"""Replay-trace manifest schema version. v1 pins this to ``0`` to signal
deliberate instability; promotes to ``1`` after the 30-day partner
retro per the v1 plan §Sequencing Step 6."""

SCHEMA_NOTE = (
    "experimental replay-contract format; pins to 1 after the Step 6 "
    "retro if it survives 30-day partner feedback"
)

DEFAULT_STREAMS: frozenset[str] = frozenset({"state_log", "content_audit_log"})

_STATE_LOG = "state_log"
_CONTENT_AUDIT_LOG = "content_audit_log"
_VALID_STREAMS: frozenset[str] = frozenset({_STATE_LOG, _CONTENT_AUDIT_LOG})

# Stderr text emitted on accept_unverified opt-in. Pinned by tests so we
# can change the body without breaking partner tooling that greps for
# the leading prefix.
_UNVERIFIED_WARNING_PREFIX = "CrewAI/AutoGen capture is wired but unverified"
_UNVERIFIED_WARNING = (
    "CrewAI/AutoGen capture is wired but unverified in v1; smoke tests "
    "land in D+1 paired with adapter-specific record_to wrappers; "
    "please file an issue if you hit problems."
)

# Stderr text emitted on first observed instance_id change. References the
# D+1 roadmap item by name so partners can grep for it.
_MULTI_INSTANCE_WARNING_PREFIX = "MULTI_INSTANCE_TRACE detected at capture time"
_MULTI_INSTANCE_WARNING = (
    "MULTI_INSTANCE_TRACE detected at capture time on stream {stream!r}: "
    "instance_id changed from {old!r} to {new!r}. v1 replay aborts on "
    "multi-instance traces; per-instance replay loops land in D+1. "
    "Stop and restart the capture against a single coordinator instance."
)


class UnverifiedAdapterCaptureError(ReplayConfigurationError):
    """Raised by :func:`record_callbacks` when ``accept_unverified=True``
    was not passed.

    The opt-in gate lives on the low-level helper, not on
    ``CCSStore.record_to``: CCSStore is verified in v1, so its
    classmethod sets the flag automatically. CrewAI / AutoGen users
    invoke the helper directly and MUST acknowledge the unverified
    status by passing the flag — surfacing the v1 scope boundary at
    the call site, not buried in README copy.

    Inherits from :class:`ccs.replay.ReplayConfigurationError` (API
    misuse) rather than :class:`ccs.replay.ReplayTraceError` — the
    trace itself never got written.
    """


# ---------------------------------------------------------------------------
# Stream writers
# ---------------------------------------------------------------------------


@dataclass
class _StreamWriter:
    """One open JSONL stream + its bookkeeping.

    Owns its file descriptor (not a Python file object) because
    ``os.fsync`` needs the raw fd and we want to keep the write +
    flush + fsync sequence on the syscall layer.
    """

    name: str
    path: Path | None  # None when stream is opt-ed out (no-op writer)
    fd: int | None = None
    last_instance_id: str | None = None
    first_tick: int | None = None
    last_tick: int | None = None
    instance_id_warned: bool = False

    @property
    def is_active(self) -> bool:
        return self.fd is not None

    def write(self, entry: dict[str, Any]) -> None:
        """Append one JSONL line + flush + fsync.

        When ``self.path`` is ``None`` (opt-ed out), do bookkeeping only.
        Bookkeeping still tracks ticks + instance_id so the manifest
        finalizer + multi-instance warning fire for caller-supplied
        callbacks that consume the wrapped no-op writer.
        """
        self._update_tracking(entry)
        if self.fd is None:
            return
        payload = (json.dumps(entry, ensure_ascii=False) + "\n").encode("utf-8")
        os.write(self.fd, payload)
        os.fsync(self.fd)

    def _update_tracking(self, entry: dict[str, Any]) -> None:
        tick = entry.get("tick")
        if isinstance(tick, int):
            if self.first_tick is None or tick < self.first_tick:
                self.first_tick = tick
            if self.last_tick is None or tick > self.last_tick:
                self.last_tick = tick
        instance_id = entry.get("instance_id")
        if instance_id is None:
            return
        if self.last_instance_id is None:
            self.last_instance_id = instance_id
            return
        if instance_id != self.last_instance_id and not self.instance_id_warned:
            self.instance_id_warned = True
            sys.stderr.write(
                _MULTI_INSTANCE_WARNING.format(
                    stream=self.name,
                    old=self.last_instance_id,
                    new=instance_id,
                ) + "\n"
            )
            # Update so a third id triggers a second warning (rare but valid).
            self.last_instance_id = instance_id

    def close(self) -> None:
        if self.fd is not None:
            os.close(self.fd)
            self.fd = None


def _open_stream(session_dir: Path, name: str, *, active: bool) -> _StreamWriter:
    """Open one stream writer. ``active=False`` returns a no-op writer
    (still consumes events for tracking, but writes nothing to disk)."""
    if not active:
        return _StreamWriter(name=name, path=None)
    path = session_dir / f"{name}.jsonl"
    # O_APPEND keeps each write atomic at the kernel layer for payloads
    # below PIPE_BUF (4096 bytes). Mode 0o600 — traces may contain
    # artifact UUIDs and content hashes; default-deny on read access.
    fd = os.open(path, os.O_APPEND | os.O_CREAT | os.O_WRONLY, 0o600)
    return _StreamWriter(name=name, path=path, fd=fd)


# ---------------------------------------------------------------------------
# Manifest
# ---------------------------------------------------------------------------


def _atomic_write_manifest(session_dir: Path, manifest: dict[str, Any]) -> None:
    """Write ``manifest.json`` via tempfile + ``os.replace`` so a crash
    mid-rewrite leaves the previous manifest intact."""
    target = session_dir / "manifest.json"
    # Same-dir tempfile so os.replace is atomic (rename across filesystems
    # would not be).
    fd, tmp_path = tempfile.mkstemp(
        prefix="manifest.", suffix=".json.tmp", dir=str(session_dir)
    )
    try:
        payload = json.dumps(manifest, indent=2, sort_keys=False).encode("utf-8")
        os.write(fd, payload)
        os.fsync(fd)
    finally:
        os.close(fd)
    try:
        os.replace(tmp_path, target)
    except OSError:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


def _header_manifest(streams: list[str], adapter_type: str) -> dict[str, Any]:
    """Manifest header fields written on ``__enter__``.

    ``end_tick``, ``instance_id``, ``agents``, ``artifacts`` are filled
    on ``__exit__`` (no MESI activity has fired yet at enter time).
    """
    return {
        "schema_version": SCHEMA_VERSION,
        "schema_note": SCHEMA_NOTE,
        "adapter_type": adapter_type,
        "start_tick": 0,
        "end_tick": 0,
        "instance_id": None,
        "streams": streams,
        "agents": {},
        "artifacts": {},
    }


# ---------------------------------------------------------------------------
# RecordingSession — context manager
# ---------------------------------------------------------------------------


@dataclass
class RecordingSession:
    """Manage the on-disk capture session for one ``record_callbacks``
    or ``CCSStore.record_to`` invocation.

    The session owns its directory, the manifest lifecycle, and the
    per-stream writers. Composition with caller-supplied callbacks
    happens in ``record_callbacks`` (the helper builds wrapped
    callbacks before constructing the session).

    The session does NOT touch a coordinator directly. To populate
    ``agents`` and ``artifacts`` in the finalized manifest, the caller
    passes a ``drain_registries`` callable returning ``(agents_map,
    artifacts_map)``. The helper-level entry point supplies a no-op
    drain (empty dicts); ``CCSStore.record_to`` supplies a closure
    over its internal core.
    """

    session_dir: Path
    streams: list[str]
    adapter_type: str
    drain_registries: Callable[[], tuple[dict[str, str], dict[str, str]]] = field(
        default=lambda: ({}, {})
    )
    _writers: dict[str, _StreamWriter] = field(default_factory=dict, init=False)

    def __enter__(self) -> "RecordingSession":
        self.session_dir.mkdir(parents=True, exist_ok=True)
        # streams parameter declares what the manifest advertises; we
        # always open the audit no-op writer too so caller-supplied
        # audit callbacks still see the bookkeeping (instance_id tracking,
        # composition).
        for stream_name in _VALID_STREAMS:
            active = stream_name in self.streams
            self._writers[stream_name] = _open_stream(
                self.session_dir, stream_name, active=active
            )
        # __exit__ is NOT called when __enter__ raises (Python context-
        # manager protocol), so any failure between opening fds above and
        # the manifest write below would leak the open fds. Guard the
        # manifest write and close any already-opened writers before
        # re-raising (Gated #4).
        try:
            _atomic_write_manifest(
                self.session_dir,
                _header_manifest(self.streams, self.adapter_type),
            )
        except Exception:
            for writer in self._writers.values():
                try:
                    writer.close()
                except OSError:
                    pass  # best-effort cleanup; original exception is what matters
            self._writers.clear()
            raise
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        try:
            self._finalize_manifest()
        finally:
            for writer in self._writers.values():
                writer.close()

    def writer(self, stream_name: str) -> _StreamWriter:
        return self._writers[stream_name]

    def _finalize_manifest(self) -> None:
        agents_map, artifacts_map = self.drain_registries()
        start_tick = self._min_first_tick()
        end_tick = self._max_last_tick()
        instance_id = self._observed_instance_id()
        manifest = {
            "schema_version": SCHEMA_VERSION,
            "schema_note": SCHEMA_NOTE,
            "adapter_type": self.adapter_type,
            "start_tick": start_tick,
            "end_tick": end_tick,
            "instance_id": instance_id,
            "streams": self.streams,
            "agents": agents_map,
            "artifacts": artifacts_map,
        }
        _atomic_write_manifest(self.session_dir, manifest)

    def _min_first_tick(self) -> int:
        ticks = [w.first_tick for w in self._writers.values() if w.first_tick is not None]
        return min(ticks) if ticks else 0

    def _max_last_tick(self) -> int:
        ticks = [w.last_tick for w in self._writers.values() if w.last_tick is not None]
        return max(ticks) if ticks else 0

    def _observed_instance_id(self) -> str | None:
        # First non-None across active writers wins. All writers see
        # entries from the same coordinator instance in the
        # well-behaved case; multi-instance is already warned.
        for writer in self._writers.values():
            if writer.last_instance_id is not None:
                return writer.last_instance_id
        return None


# ---------------------------------------------------------------------------
# Callback composition
# ---------------------------------------------------------------------------


def _compose(
    caller_cb: Callable[[dict[str, Any]], None] | None,
    writer: _StreamWriter,
) -> Callable[[dict[str, Any]], None]:
    """Build a callback that fires the caller's callback first, then
    writes to the stream.

    The caller-first order matches the rollback contract: if the
    caller's callback raises, the file write is skipped AND the
    coordinator's ``_seq -= 1`` rollback still fires from registry.py.
    A file-write failure (disk full) likewise propagates and triggers
    the rollback; fsync-per-line ensures the failed entry is not on
    disk for the next attempt.
    """
    def _wrapped(entry: dict[str, Any]) -> None:
        if caller_cb is not None:
            caller_cb(entry)
        writer.write(entry)

    return _wrapped


# ---------------------------------------------------------------------------
# Public helper
# ---------------------------------------------------------------------------


def _normalize_streams(streams: set[str] | frozenset[str] | None) -> list[str]:
    """Validate the caller-supplied stream set and freeze its iteration
    order to the declared spec (state_log first, audit second)."""
    if streams is None:
        chosen = DEFAULT_STREAMS
    else:
        chosen = frozenset(streams)
        unknown = chosen - _VALID_STREAMS
        if unknown:
            raise ValueError(
                f"Unknown streams in record_callbacks(streams={chosen!r}); "
                f"valid set is {sorted(_VALID_STREAMS)!r}"
            )
        if _STATE_LOG not in chosen:
            # state_log carries 3 of 4 invariants; an opt-out would
            # leave a useless trace. Fail fast at capture time.
            raise ValueError(
                "record_callbacks requires 'state_log' in streams= "
                "(it carries SINGLE_WRITER, MONOTONIC_VERSION, and "
                "LOST_WRITE invariants in v1)"
            )
    return [s for s in (_STATE_LOG, _CONTENT_AUDIT_LOG) if s in chosen]


@contextmanager
def record_callbacks(
    path: str | Path,
    *,
    streams: set[str] | frozenset[str] | None = None,
    accept_unverified: bool = False,
    state_log: Callable[[dict[str, Any]], None] | None = None,
    content_audit_log: Callable[[dict[str, Any]], None] | None = None,
    adapter_type: str = "coherence-adapter-core",
    drain_registries: Callable[
        [], tuple[dict[str, str], dict[str, str]]
    ] | None = None,
    _verified_caller: bool = False,
) -> Iterator[tuple[
    Callable[[dict[str, Any]], None],
    Callable[[dict[str, Any]], None],
]]:
    """Low-level non-LangGraph recorder helper.

    Yields a ``(state_log_cb, content_audit_log_cb)`` tuple inside a
    context manager. Callers wire these into
    ``CoherenceAdapterCore(state_log=..., content_audit_log=...)``.

    The audit callback ALWAYS exists (even when
    ``streams={'state_log'}``) so downstream
    ``retain_versions=content_audit_log is not None`` checks stay
    truthy. The opt-out only suppresses the file write.

    Args:
        path: Session directory. Created if missing.
        streams: Subset of ``{"state_log", "content_audit_log"}`` that
            should land on disk. Default: both. ``state_log`` cannot
            be opted out.
        accept_unverified: Required opt-in for direct callers. CCSStore
            sets this automatically via its ``record_to`` wrapper;
            CrewAI / AutoGen users must pass it explicitly.
        state_log: Caller-supplied state-log callback. Composed BEFORE
            the file writer (caller exception short-circuits write +
            triggers ``_seq`` rollback).
        content_audit_log: Same as ``state_log`` for the audit stream.
        adapter_type: Recorded in the manifest. Default
            ``coherence-adapter-core`` for direct helper use;
            ``CCSStore.record_to`` overrides to ``langgraph-ccsstore``.
        drain_registries: Closure invoked on ``__exit__`` to populate
            the manifest's ``agents`` / ``artifacts`` maps from the
            coordinator's registries. Direct callers leave this
            ``None`` (empty maps).

    Raises:
        UnverifiedAdapterCaptureError: ``accept_unverified`` not set.
        ValueError: ``streams=`` contains unknown values or omits
            ``state_log``.
    """
    if not accept_unverified:
        raise UnverifiedAdapterCaptureError(
            "record_callbacks requires accept_unverified=True. "
            "v1 only verifies CCSStore captures end-to-end; CrewAI / "
            "AutoGen capture is wired but unverified — pass "
            "accept_unverified=True to acknowledge."
        )

    # The stderr warning is only emitted when an unverified caller used
    # the opt-in flag. Verified adapters (CCSStore) pass
    # _verified_caller=True to suppress it — the flag is a private
    # signal from the wrapper, not a public knob.
    if not _verified_caller:
        sys.stderr.write(_UNVERIFIED_WARNING + "\n")

    session_streams = _normalize_streams(streams)
    drain = drain_registries if drain_registries is not None else (lambda: ({}, {}))
    session = RecordingSession(
        session_dir=Path(path),
        streams=session_streams,
        adapter_type=adapter_type,
        drain_registries=drain,
    )
    with session:
        state_cb = _compose(state_log, session.writer(_STATE_LOG))
        audit_cb = _compose(content_audit_log, session.writer(_CONTENT_AUDIT_LOG))
        yield state_cb, audit_cb
