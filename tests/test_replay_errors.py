# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""Tests for the ``ccs.replay`` exception hierarchy (Gated #11).

Verifies the two-tier semantic split:

- :class:`ReplayConfigurationError` — API misuse (caller fault)
- :class:`ReplayTraceError` — trace structural defect (data fault;
  CLI exit code 3)

Both inherit from :class:`ReplayError` so partner callers can write a
single ``except ReplayError`` if they want everything, or narrow to
one of the two semantic categories. The CLI relies on the
``ReplayTraceError`` umbrella so future trace-defect subclasses
auto-route to exit code 3 without touching the handler.
"""

from __future__ import annotations

import pytest

from ccs.replay import (
    ManifestMissingOrUnreadableError,
    MultiInstanceTraceError,
    ReplayConfigurationError,
    ReplayError,
    ReplayTraceError,
    TraceCorruptionError,
    UnverifiedAdapterCaptureError,
)


def test_multi_instance_trace_error_is_replay_trace_error() -> None:
    assert issubclass(MultiInstanceTraceError, ReplayTraceError)
    assert issubclass(MultiInstanceTraceError, ReplayError)


def test_trace_corruption_error_is_replay_trace_error() -> None:
    assert issubclass(TraceCorruptionError, ReplayTraceError)
    assert issubclass(TraceCorruptionError, ReplayError)


def test_manifest_missing_error_is_replay_trace_error() -> None:
    assert issubclass(ManifestMissingOrUnreadableError, ReplayTraceError)
    assert issubclass(ManifestMissingOrUnreadableError, ReplayError)


def test_unverified_adapter_error_is_replay_configuration_error() -> None:
    assert issubclass(UnverifiedAdapterCaptureError, ReplayConfigurationError)
    assert issubclass(UnverifiedAdapterCaptureError, ReplayError)
    # Explicitly NOT a trace error — accept_unverified is a call-site
    # mistake, not a defective trace on disk.
    assert not issubclass(UnverifiedAdapterCaptureError, ReplayTraceError)


def test_configuration_and_trace_are_disjoint() -> None:
    """The two semantic categories share only ``ReplayError`` —
    callers can rely on the disjointness when picking which one to
    catch.
    """
    assert not issubclass(ReplayConfigurationError, ReplayTraceError)
    assert not issubclass(ReplayTraceError, ReplayConfigurationError)


def test_except_replay_trace_error_catches_all_trace_errors() -> None:
    """Single ``except`` clause replaces the old 3-tuple workaround
    in ``ccs.cli.coherence_replay``. Catches every trace-defect
    subclass via the umbrella base, so a future subclass routes to
    exit 3 without touching the CLI handler.
    """
    errors_to_catch: tuple[ReplayTraceError, ...] = (
        MultiInstanceTraceError("test"),
        TraceCorruptionError("test"),
        ManifestMissingOrUnreadableError("test"),
    )
    for err in errors_to_catch:
        try:
            raise err
        except ReplayTraceError:
            pass  # expected
        else:
            pytest.fail(
                f"{type(err).__name__} not caught by ReplayTraceError"
            )


def test_except_replay_error_catches_both_categories() -> None:
    """``ReplayError`` is the single top-level umbrella — useful for
    partners who want to catch any replay-module failure with one
    clause.
    """
    errors_to_catch: tuple[ReplayError, ...] = (
        MultiInstanceTraceError("test"),
        TraceCorruptionError("test"),
        ManifestMissingOrUnreadableError("test"),
        UnverifiedAdapterCaptureError("test"),
    )
    for err in errors_to_catch:
        try:
            raise err
        except ReplayError:
            pass  # expected
        else:
            pytest.fail(f"{type(err).__name__} not caught by ReplayError")


def test_replay_error_is_distinct_from_runtime_error() -> None:
    """Previously every replay exception inherited ``RuntimeError`` —
    catching ``RuntimeError`` in partner code would have caught a
    grab-bag including replay errors. Post-fix, partners should catch
    ``ReplayError`` (or a subclass) explicitly.
    """
    # ReplayError inherits directly from Exception, not RuntimeError.
    assert not issubclass(ReplayError, RuntimeError)
    # And the concrete subclasses no longer carry the RuntimeError tag
    # either — the inheritance was reparented.
    assert not issubclass(UnverifiedAdapterCaptureError, RuntimeError)
    assert not issubclass(MultiInstanceTraceError, RuntimeError)
    assert not issubclass(TraceCorruptionError, RuntimeError)
    assert not issubclass(ManifestMissingOrUnreadableError, RuntimeError)
