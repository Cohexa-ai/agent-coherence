# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""Telemetry payload constructor and consent flow surface.

v0 (Unit 7 stub):

* :func:`payload_for` returns a minimal anonymized payload dict for a
  ``(verdict, report)`` pair.
* :func:`payload_for_from_json` reconstructs the same shape from a JSON
  report file (used by ``--show-payload``) without rehydrating the full
  dataclass tree — JSON is dict-shaped already.
* No consent prompt, no env-var honors, no submission code, no UUID4
  installation token. Unit 8 fills these in.

Unit 8 will add:

* TTY-aware consent flow (skip when stdout is not a TTY).
* ``DO_NOT_TRACK`` / ``DISABLE_TELEMETRY`` / ``CCS_DIAGNOSE_NO_TELEMETRY``
  environment-variable honors.
* Atomic ``consent.json`` with ``policy_version`` + UUID4
  ``installation_token``.
* ``--reset-token`` regeneration of the token in ``consent.json``.

The plan deliberately defers HTTPS POST + hashcash + retry/timeout to the
post-promotion ops plan. v0 calibration ships through Unit 9's local JSONL
only — there is no network code in this module today.

What this surface collects (Unit 8 will populate ``installation_token``):

* stack name + version (e.g. ``"LangGraph 0.6.x"`` — Unit 8 will detect
  the actual version)
* ``classifier_version`` (= :data:`CCS_DIAGNOSE_LOG_SCHEMA_VERSION`)
* verdict bucket + confidence
* coverage shape (counts only, no values)
* timestamp
* installation_token (``None`` in v0; Unit 8 populates from
  ``consent.json``)

What this surface deliberately does NOT collect: agent names, artifact
names, content, hashes, tool calls, prompts, run timing, IP addresses.
"""

from __future__ import annotations

import platform
from datetime import datetime, timezone
from typing import Any, Mapping

from ccs import __version__ as agent_coherence_version
from ccs.diagnose import CCS_DIAGNOSE_LOG_SCHEMA_VERSION
from ccs.diagnose.classifier import ClassifierVerdict
from ccs.diagnose.detection import DetectionReport

__all__ = ["payload_for", "payload_for_from_json"]


_STACK_NAME: str = "LangGraph"
"""Stack label included in every payload.

Unit 8 will detect the actual installed LangGraph version via
``importlib.metadata.version('langgraph')``.
"""


def payload_for(verdict: ClassifierVerdict, report: DetectionReport) -> dict[str, Any]:
    """Return the minimal anonymized telemetry payload for a verdict + report.

    Pure function aside from the timestamp (``datetime.now(UTC)``).

    No agent names, artifact names, content, or hashes are included. The
    submission-side network code lives in Unit 8 — Unit 7 only needs the
    payload shape stable so the CLI's flag plumbing has something to print
    when ``--dry-run`` is supplied.
    """
    return {
        "schema_version": CCS_DIAGNOSE_LOG_SCHEMA_VERSION,
        "stack": _STACK_NAME,
        "agent_coherence_version": agent_coherence_version,
        "python_version": platform.python_version(),
        "classifier_version": CCS_DIAGNOSE_LOG_SCHEMA_VERSION,
        "verdict_bucket": verdict.bucket.value,
        "verdict_confidence": verdict.confidence.value,
        "coverage": {
            "tick_count": verdict.coverage.tick_count,
            "read_count": verdict.coverage.read_count,
            "write_count": verdict.coverage.write_count,
            "artifact_count": verdict.coverage.artifact_count,
        },
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        # Unit 8 populates this field from ``consent.json`` after the
        # consent prompt completes. v0 always emits ``None``.
        "installation_token": None,
    }


def payload_for_from_json(loaded: Mapping[str, Any]) -> dict[str, Any]:
    """Reconstruct a payload from a loaded JSON report file.

    Used by ``ccs-diagnose --show-payload PATH`` so users can preview what
    would be submitted (when Unit 8 lands the network surface) without
    re-running the pipeline.

    The report file shape is the JSON written by Unit 7's
    ``--output-json``: ``{"schema_version": ..., "verdict": {...},
    "report": {...}}``. We accept dict-shaped input so we don't need to
    rehydrate the dataclass tree just to extract the bucket / confidence
    / coverage fields.
    """
    verdict_dict = loaded.get("verdict", {})
    coverage_dict = verdict_dict.get("coverage", {})
    schema_version = loaded.get("schema_version", CCS_DIAGNOSE_LOG_SCHEMA_VERSION)
    return {
        "schema_version": schema_version,
        "stack": _STACK_NAME,
        "agent_coherence_version": agent_coherence_version,
        "python_version": platform.python_version(),
        "classifier_version": schema_version,
        "verdict_bucket": verdict_dict.get("bucket"),
        "verdict_confidence": verdict_dict.get("confidence"),
        "coverage": {
            "tick_count": coverage_dict.get("tick_count", 0),
            "read_count": coverage_dict.get("read_count", 0),
            "write_count": coverage_dict.get("write_count", 0),
            "artifact_count": coverage_dict.get("artifact_count", 0),
        },
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "installation_token": None,
    }
