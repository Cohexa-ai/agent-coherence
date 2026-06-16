# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Artifact Ownership Map computation for ``ccs-diagnose`` (Unit 5 helper).

Unit 4's :class:`ccs.diagnose.detection.DetectionReport` exposes a
per-artifact heatmap (``divergent_reads`` / ``total_reads``) but does NOT
attribute writers and readers by node name. The renderer's *Artifact
Ownership Map* panel needs that data:

    artifact          | writers (agent, count)        | readers (agent, count) | versions   | append_only
    state.task_queue  | supervisor (3 writes)         | researcher (24 reads)…| v1 -> v3   | False

This module is the single source of truth for that computation. Kept
separate from :mod:`ccs.diagnose.render` so the helper has its own test
surface (Unit 5 coverage requirement) and so Unit 7's CLI can compute it
once and pass it to both the HTML renderer and the future terminal
summary (Unit 6).

Witness-quality framing
=======================

Just like the rest of the diagnose surface: writes are observed (the
runtime received a node-end with the artifact in its return dict) and
reads are observed (the runtime *handed* a node a state containing the
artifact). Whether a node read or wrote the value is unobservable.

Sort order (matches the plan's render rule)
============================================

* Multi-writer rows first (loud signal — the ``shared_artifact`` pattern
  is visible at the artifact level even when no individual divergence
  event was caught).
* Within each group: by total read count desc, ties broken by
  ``artifact_key`` lexicographically for determinism.
"""

from __future__ import annotations

import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass

from ccs.diagnose.callback import DiagnoseEvent
from ccs.diagnose.classifier import ClassifierVerdict

__all__ = ["OwnershipRow", "compute_ownership_map"]


@dataclass(frozen=True)
class OwnershipRow:
    """Per-artifact ownership view used by the HTML renderer.

    Attributes:
        artifact_key: Top-level state-key name (verbatim — autoescape on
            render is the XSS defence).
        artifact_id: UUIDv5 identity from
            :func:`ccs.core.identity.artifact_uuid`.
        writers: Tuple of ``(agent_name, write_count)`` sorted by count
            desc, ties broken by name. ``agent_name`` is the literal
            ``DiagnoseEvent.node`` string; empty-string nodes are dropped.
        readers: Same shape as ``writers`` but for ``node_start`` events
            (read views).
        version_range: Compact string of the form ``"v1"`` (single
            version) or ``"v1 -> v3"`` (multiple distinct versions
            observed). Versions are taken verbatim from
            ``DiagnoseEvent.artifact_versions`` — no ordering assumption
            is made about their content.
        append_only: ``True`` if the artifact name is in
            ``verdict.append_only_keys``. A pure mirror of the
            classifier's decision.
    """

    artifact_key: str
    artifact_id: uuid.UUID
    writers: tuple[tuple[str, int], ...]
    readers: tuple[tuple[str, int], ...]
    version_range: str
    append_only: bool


def compute_ownership_map(
    events: Sequence[DiagnoseEvent],
    verdict: ClassifierVerdict,
    key_index: Mapping[str, uuid.UUID],
) -> tuple[OwnershipRow, ...]:
    """Build the per-artifact ownership map for the renderer.

    Pure function; deterministic on its inputs. No I/O.

    Only artifacts in ``verdict.tracked_keys`` and present in
    ``key_index`` are surfaced. Artifacts the classifier ignored as
    framework / ephemera are intentionally omitted.

    Returns rows sorted as documented at module level.
    """
    tracked = tuple(verdict.tracked_keys)
    if not tracked:
        return ()

    inverse: dict[uuid.UUID, str] = {}
    for name in tracked:
        aid = key_index.get(name)
        if aid is not None:
            inverse[aid] = name

    if not inverse:
        return ()

    append_only_set = set(verdict.append_only_keys)

    writers_per_artifact: dict[uuid.UUID, dict[str, int]] = {
        aid: {} for aid in inverse
    }
    readers_per_artifact: dict[uuid.UUID, dict[str, int]] = {
        aid: {} for aid in inverse
    }
    versions_per_artifact: dict[uuid.UUID, list[str]] = {
        aid: [] for aid in inverse
    }

    for ev in events:
        if ev.event_type not in ("node_start", "node_end"):
            continue
        if not ev.artifact_versions:
            continue
        agent = ev.node
        for aid, version in ev.artifact_versions.items():
            if aid not in inverse:
                continue
            if ev.event_type == "node_end":
                if agent:
                    writers_per_artifact[aid][agent] = (
                        writers_per_artifact[aid].get(agent, 0) + 1
                    )
            else:  # node_start
                if agent:
                    readers_per_artifact[aid][agent] = (
                        readers_per_artifact[aid].get(agent, 0) + 1
                    )
            versions_per_artifact[aid].append(str(version))

    rows: list[OwnershipRow] = []
    for aid, key in inverse.items():
        writers = _sort_count_pairs(writers_per_artifact[aid])
        readers = _sort_count_pairs(readers_per_artifact[aid])
        version_range = _summarise_versions(versions_per_artifact[aid])
        rows.append(
            OwnershipRow(
                artifact_key=key,
                artifact_id=aid,
                writers=writers,
                readers=readers,
                version_range=version_range,
                append_only=key in append_only_set,
            )
        )

    rows.sort(key=_row_sort_key)
    return tuple(rows)


# -------------------------------------------------------------------- #
# Internals
# -------------------------------------------------------------------- #


def _sort_count_pairs(counts: Mapping[str, int]) -> tuple[tuple[str, int], ...]:
    """Sort ``(name, count)`` pairs by count desc, name asc for ties."""
    return tuple(sorted(counts.items(), key=lambda item: (-item[1], item[0])))


def _summarise_versions(versions: Sequence[str]) -> str:
    """Return ``"v?"``, ``"v1"``, or ``"v1 -> v3"``-style summary.

    Versions are taken in observation order. The first is rendered as
    the "from" version; the last *distinct* one as the "to" version.
    Single-distinct-version observations render plain. No observations
    render as ``"v?"`` (artifact in the index but never seen on a node
    boundary — defensive, should be filtered out earlier in practice).
    """
    if not versions:
        return "v?"

    first = versions[0]
    last_distinct = first
    for version in versions[1:]:
        if version != last_distinct:
            last_distinct = version

    if last_distinct == first:
        return _format_version(first)
    return f"{_format_version(first)} -> {_format_version(last_distinct)}"


def _format_version(version: str) -> str:
    """Render a version string compactly.

    Pure-numeric versions are prefixed with ``v`` for readability;
    everything else is rendered verbatim (autoescape applies on the
    render side). Long content-hash versions are truncated to the first
    8 hex chars so the string fits in a small table cell — this is a
    display-only summarisation; the underlying ``DivergenceEvent`` carries
    the full version when forensic detail is needed.
    """
    if version.isdigit():
        return f"v{version}"
    if _looks_like_hex(version) and len(version) > 8:
        return version[:8]
    return version


def _looks_like_hex(value: str) -> bool:
    if not value:
        return False
    try:
        int(value, 16)
    except ValueError:
        return False
    return True


def _row_sort_key(row: OwnershipRow) -> tuple[int, int, str]:
    """Sort: multi-writer rows first, then by total reads desc, then key asc."""
    multi_writer_priority = 0 if len(row.writers) >= 2 else 1
    total_reads = sum(count for _, count in row.readers)
    return (multi_writer_priority, -total_reads, row.artifact_key)
