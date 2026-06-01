# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""Deterministic text/JSON diff helpers for artifact synchronization."""

from __future__ import annotations

import difflib
import json
from dataclasses import dataclass
from typing import Any, Mapping


@dataclass(frozen=True)
class DiffStats:
    """Simple delta accounting for observability and debugging."""

    added_lines: int
    removed_lines: int
    unchanged_lines: int

    @property
    def changed_lines(self) -> int:
        """Return conservative line-change approximation."""
        return max(self.added_lines, self.removed_lines)


def compute_text_delta(old_text: str, new_text: str) -> list[str]:
    """Return ndiff-style line delta from old to new text."""
    old_lines = old_text.splitlines(keepends=True)
    new_lines = new_text.splitlines(keepends=True)
    return list(difflib.ndiff(old_lines, new_lines))


def apply_text_delta(delta: list[str]) -> str:
    """Reconstruct target text from ndiff-style delta."""
    return "".join(difflib.restore(delta, 2))


def compute_json_delta(
    old_payload: Mapping[str, Any] | list[Any],
    new_payload: Mapping[str, Any] | list[Any],
    *,
    indent: int = 2,
) -> list[str]:
    """Return deterministic text delta for JSON payload changes."""
    old_text = _canonical_json(old_payload, indent=indent)
    new_text = _canonical_json(new_payload, indent=indent)
    return compute_text_delta(old_text, new_text)


def apply_json_delta(delta: list[str]) -> Any:
    """Reconstruct target JSON payload from delta."""
    text = apply_text_delta(delta)
    return json.loads(text)


def compute_delta_stats(delta: list[str]) -> DiffStats:
    """Return simple counts of added/removed/unchanged lines in a delta."""
    added = 0
    removed = 0
    unchanged = 0
    for line in delta:
        if line.startswith("+ "):
            added += 1
        elif line.startswith("- "):
            removed += 1
        elif line.startswith("  "):
            unchanged += 1
    return DiffStats(added_lines=added, removed_lines=removed, unchanged_lines=unchanged)


def estimate_delta_size(delta: list[str]) -> int:
    """Return serialized character size for transport-cost estimation."""
    return sum(len(line) for line in delta)


def _canonical_json(payload: Mapping[str, Any] | list[Any], *, indent: int) -> str:
    text = json.dumps(payload, sort_keys=True, indent=indent)
    if not text.endswith("\n"):
        text += "\n"
    return text
