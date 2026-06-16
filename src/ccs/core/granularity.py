# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Artifact granularity levels and v0.1 support boundaries."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class GranularityLevel(Enum):
    """Artifact granularity tiers for current and planned CCS versions."""

    COARSE = "coarse"
    MEDIUM = "medium"
    FINE = "fine"


@dataclass(frozen=True)
class GranularitySpec:
    """Specification row for one granularity level."""

    level: GranularityLevel
    min_tokens: int
    max_tokens: int
    description: str
    v01_supported: bool


GRANULARITY_SPECS: dict[GranularityLevel, GranularitySpec] = {
    GranularityLevel.COARSE: GranularitySpec(
        level=GranularityLevel.COARSE,
        min_tokens=2048,
        max_tokens=8192,
        description="Full document granularity; atomic cache line in CCS v0.1.",
        v01_supported=True,
    ),
    GranularityLevel.MEDIUM: GranularitySpec(
        level=GranularityLevel.MEDIUM,
        min_tokens=256,
        max_tokens=2048,
        description="Section/function granularity; planned for sub-artifact v0.2.",
        v01_supported=False,
    ),
    GranularityLevel.FINE: GranularitySpec(
        level=GranularityLevel.FINE,
        min_tokens=10,
        max_tokens=100,
        description="Field/line granularity; planned for hierarchical artifact graphs.",
        v01_supported=False,
    ),
}

CANONICAL_ARTIFACT_TOKENS: int = 4096
DEFAULT_GRANULARITY: GranularityLevel = GranularityLevel.COARSE
