# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""The ``swg_status`` payload: an honest, 3-state view of coherence enforcement.

The load-bearing honesty bug this avoids (SC5): conflating ``off`` (coordinator
reachable, no strict patterns) with ``unknown`` (coordinator unreachable). A
caller that reads ``off`` may assume it is safe to write unguarded; ``unknown``
must NOT collapse to that. The shipped ``strict_mode_active()`` returns ``False``
for both, so this composes the raw ``/status`` instead.

``per_path`` enforcement is the CLIENT's belief — a tracked artifact that matches
THIS server's managed globs — not a cross-checked coordinator fact. A sibling
volume with different managed globs is indistinguishable
(``heterogeneous_scope_detectable=false``); v1 makes that gap loud, not
detectable.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from ccs.adapters.claude_code.policy import _matches_any

if TYPE_CHECKING:
    from ccs.adapters.coherent_volume import CoherentVolume
    from ccs.mcp.session import SessionConfig


def build_status(volume: CoherentVolume, config: SessionConfig) -> dict:
    """Compose the ``swg_status`` payload from the coordinator ``/status`` + the
    volume's local view + this server's managed scope."""
    status_doc = volume.coordinator_status()  # None if unattached / unreachable
    return {
        "coordinator": _coordinator_state(volume, status_doc),
        "is_attached": volume.is_attached,
        "is_degraded": volume.is_degraded,
        "session_id": volume.session_id,
        "managed": list(config.managed),
        "per_path": _per_path(config, status_doc),
        "single_host_only": True,
        # v1 cannot tell a guarded workspace from a heterogeneous multi-host or
        # differently-scoped one. The gap is loud here, not programmatically
        # detectable (per-glob detection → v1.1).
        "heterogeneous_scope_detectable": False,
    }


def _coordinator_state(volume: CoherentVolume, status_doc: dict | None) -> str:
    """``on`` (reachable + strict patterns) / ``off`` (reachable + none) /
    ``unknown`` (unattached or unreachable) — ``unknown`` is NEVER reported as
    ``off``."""
    if not volume.is_attached or status_doc is None:
        return "unknown"
    summary = status_doc.get("policy_summary")
    count = summary.get("strict_mode_pattern_count") if isinstance(summary, dict) else None
    if not isinstance(count, int) or isinstance(count, bool):
        return "unknown"
    return "on" if count > 0 else "off"


def _per_path(config: SessionConfig, status_doc: dict | None) -> dict:
    """Per tracked artifact: its version and whether it is ``enforced`` (matches
    this server's managed globs) or merely ``not_registered`` for strict
    enforcement by this server."""
    per_path: dict[str, dict] = {}
    if not isinstance(status_doc, dict):
        return per_path
    for artifact in status_doc.get("tracked_artifacts", []):
        if not isinstance(artifact, dict):
            continue
        path = artifact.get("path")
        if not isinstance(path, str):
            continue
        enforced = _matches_any(path, config.managed)
        per_path[path] = {
            "version": artifact.get("version"),
            "status": "enforced" if enforced else "not_registered",
        }
    return per_path
