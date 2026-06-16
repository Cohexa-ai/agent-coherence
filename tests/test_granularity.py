# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Tests for artifact granularity constants and v0.1 support flags."""

from __future__ import annotations

from ccs.core.granularity import (
    CANONICAL_ARTIFACT_TOKENS,
    DEFAULT_GRANULARITY,
    GRANULARITY_SPECS,
    GranularityLevel,
)


def test_v01_only_supports_coarse() -> None:
    supported = [spec for spec in GRANULARITY_SPECS.values() if spec.v01_supported]
    assert len(supported) == 1
    assert supported[0].level == GranularityLevel.COARSE


def test_canonical_tokens_in_coarse_range() -> None:
    coarse = GRANULARITY_SPECS[GranularityLevel.COARSE]
    assert coarse.min_tokens <= CANONICAL_ARTIFACT_TOKENS <= coarse.max_tokens


def test_default_is_coarse() -> None:
    assert DEFAULT_GRANULARITY == GranularityLevel.COARSE
