# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""Artifact identity helper — deterministic UUIDv5 over a structured name.

Shared between CCSStore (coherence-protocol artifact registration) and the
ccs-diagnose CLI (passive observation), so identity is content-free and
collision-stable across the two surfaces. Originally inline at
src/ccs/adapters/ccsstore.py:526; extracted here as part of Tier 2 Unit 1
of the ccs-diagnose plan.

Convention:
    artifact_uuid(scope, key) -> uuid5(NAMESPACE_URL, "ccs-artifact:{scope}:{key}")

``scope`` is the already-joined string form of the artifact's scope tuple
(e.g. ``"graph:thread"``); callers that hold a tuple should join with ``:``
first. The helper does NOT escape ``:`` inside scope or key — collisions
between ``(scope="a:b", key="c")`` and ``(scope="a", key="b:c")`` are
possible. Callers must pick unambiguous scope/key shapes or accept the
collision risk.
"""

from __future__ import annotations

import uuid

__all__ = ["artifact_uuid"]


def artifact_uuid(scope: str, key: str) -> uuid.UUID:
    """Return a deterministic UUIDv5 for the given (scope, key) pair.

    The UUID is derived from ``f"ccs-artifact:{scope}:{key}"`` under
    ``uuid.NAMESPACE_URL``. Same inputs always yield the same UUID across
    processes and machines.

    Args:
        scope: Already-joined scope string (e.g. ``"graph:thread"``). Empty
            string is permitted; the helper does not validate the shape.
        key: Top-level state-key name or artifact identifier.

    Returns:
        A UUIDv5 deterministic in (scope, key).
    """
    return uuid.uuid5(uuid.NAMESPACE_URL, f"ccs-artifact:{scope}:{key}")
