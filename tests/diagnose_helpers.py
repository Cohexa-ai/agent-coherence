# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Shared helpers for ``ccs.diagnose`` test suites.

Three test files (classifier, detection, ownership) historically
maintained near-identical ``_make_event`` builders. The duplication made
schema changes (e.g. adding ``run_id`` / ``namespace``) a 3-edit chore
and tempted small drift. This module centralises the builder and a
couple of identity-helper convenience wrappers.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterable, Mapping

from ccs.core.hashing import compute_content_hash
from ccs.core.identity import artifact_uuid
from ccs.diagnose import CCS_DIAGNOSE_LOG_SCHEMA_VERSION
from ccs.diagnose.callback import DEFAULT_SCOPE, DiagnoseEvent

__all__ = [
    "INSTANCE_ID",
    "hash_value",
    "ids_for",
    "make_event",
]


INSTANCE_ID: uuid.UUID = uuid.UUID("11111111-2222-3333-4444-555555555555")
"""Stable instance UUID used by all synthetic events.

Pinning a constant keeps tests deterministic and makes diff inspection
straightforward when an event-shape regression surfaces.
"""


def hash_value(value: object) -> str:
    """Hash ``value`` exactly the way ``DiagnoseCallback`` does at runtime."""
    return compute_content_hash(repr(value))


def ids_for(keys: Iterable[str]) -> dict[str, uuid.UUID]:
    """Return a name → UUID index for ``build_key_index``-style call sites."""
    return {k: artifact_uuid(DEFAULT_SCOPE, k) for k in keys}


def make_event(
    *,
    sequence: int,
    tick: int,
    node: str,
    event_type: str,
    state: Mapping[str, object] | None = None,
    explicit_versions: Mapping[str, str] | None = None,
    explicit_hashes: Mapping[str, str] | None = None,
    verdict_signal: str | None = None,
    message: str = "",
    run_id: str = "run-x",
    namespace: str = "",
) -> DiagnoseEvent:
    """Build a synthetic :class:`DiagnoseEvent` for diagnose unit tests.

    ``state`` mirrors the convenience used across classifier/detection/
    ownership tests: each key/value is hashed and the same hash populates
    both ``artifact_versions`` and ``content_hashes`` (matching the
    callback's no-checkpointer fallback). ``explicit_versions`` /
    ``explicit_hashes`` decouple the two for tests that must distinguish
    "different version, identical content" cases.
    """
    state = state or {}
    versions: dict[uuid.UUID, str] = {}
    hashes: dict[uuid.UUID, str] = {}
    for key, value in state.items():
        aid = artifact_uuid(DEFAULT_SCOPE, key)
        h = hash_value(value)
        versions[aid] = h
        hashes[aid] = h
    if explicit_versions:
        for key, version in explicit_versions.items():
            versions[artifact_uuid(DEFAULT_SCOPE, key)] = version
    if explicit_hashes:
        for key, h in explicit_hashes.items():
            hashes[artifact_uuid(DEFAULT_SCOPE, key)] = h
    return DiagnoseEvent(
        sequence_number=sequence,
        instance_id=INSTANCE_ID,
        schema_version=CCS_DIAGNOSE_LOG_SCHEMA_VERSION,
        tick=tick,
        node=node,
        event_type=event_type,  # type: ignore[arg-type]
        artifact_versions=versions,
        content_hashes=hashes,
        run_id=run_id,
        namespace=namespace,
        verdict_signal=verdict_signal,  # type: ignore[arg-type]
        message=message,
    )
