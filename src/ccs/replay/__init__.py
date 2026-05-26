# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""Replay capture surface for invariant replay (D v1).

This package exposes the producer side of the replay trace contract
documented in ``docs/proposals/replay_trace_format.md``. Consumers
(loader, predicates, CLI) land in sibling units; this module owns the
on-disk write path only.

Public API:

- :class:`UnverifiedAdapterCaptureError` — raised by
  :func:`record_callbacks` when called without
  ``accept_unverified=True``. Verified adapters (currently only
  ``CCSStore``) set the flag automatically.
- :func:`record_callbacks` — low-level non-LangGraph helper that
  yields a ``(state_log_cb, content_audit_log_cb)`` tuple ready to be
  passed into ``CoherenceAdapterCore``. Compose with caller-provided
  callbacks; never overrides them.

``CCSStore.record_to`` is the LangGraph-shaped thin wrapper over
:func:`record_callbacks` and lives on the adapter class itself.
"""

from ccs.replay.loader import (
    LoadedTrace,
    ManifestMissingOrUnreadableError,
    MultiInstanceTraceError,
    TraceCorruptionError,
    load,
)
from ccs.replay.predicates import (
    CORE_PREDICATES,
    Finding,
    LostWritePredicate,
    MonotonicVersionPredicate,
    Predicate,
    SingleWriterPredicate,
    StaleReadPredicate,
    SummaryFinding,
    run_predicates,
)
from ccs.replay.formatters import (
    emit_human,
    emit_json,
)
from ccs.replay.recorder import (
    UnverifiedAdapterCaptureError,
    record_callbacks,
)

__all__ = [
    "CORE_PREDICATES",
    "Finding",
    "LoadedTrace",
    "LostWritePredicate",
    "ManifestMissingOrUnreadableError",
    "MonotonicVersionPredicate",
    "MultiInstanceTraceError",
    "Predicate",
    "SingleWriterPredicate",
    "StaleReadPredicate",
    "SummaryFinding",
    "TraceCorruptionError",
    "UnverifiedAdapterCaptureError",
    "emit_human",
    "emit_json",
    "load",
    "record_callbacks",
    "run_predicates",
]
