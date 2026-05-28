# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""Base exception classes for ``ccs.replay``.

Lives in its own module (rather than ``__init__.py``) so concrete
subclasses in ``recorder`` / ``loader`` can import the base classes
without forming an import cycle through the package ``__init__``.

The hierarchy is a two-tier semantic split so callers can write a
single ``except`` clause matching the failure category:

::

    ReplayError                       (base — catches everything)
    ├── ReplayConfigurationError      (API misuse — fix the call)
    │   └── UnverifiedAdapterCaptureError
    └── ReplayTraceError              (trace defect — fix the data; CLI exit 3)
        ├── ManifestMissingOrUnreadableError
        ├── MultiInstanceTraceError
        └── TraceCorruptionError

Future trace-defect subclasses inherit ``ReplayTraceError`` and
auto-route to CLI exit code 3 without touching the handler.
"""

from __future__ import annotations


class ReplayError(Exception):
    """Base class for all errors raised by the ``ccs.replay`` module."""


class ReplayConfigurationError(ReplayError):
    """Configuration / API misuse — caller passed wrong arguments or used
    the wrong entry point. Examples: ``UnverifiedAdapterCaptureError``
    (caller forgot ``accept_unverified=True``). Not a trace defect.
    """


class ReplayTraceError(ReplayError):
    """Errors raised when reading or interpreting a captured trace.
    The trace is structurally invalid (multi-instance, duplicate seq,
    missing manifest, or unreadable manifest). Maps to CLI exit code 3.
    """
