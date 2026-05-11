# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""Shared display labels for ``Bucket`` and ``Confidence`` enums.

Keeping these dicts in one place prevents the HTML renderer and the
terminal summary from drifting apart on the same surface (e.g. one
showing "mixed pattern" and the other "mixed_pattern").
"""

from __future__ import annotations

from .classifier import Bucket, Confidence

__all__ = ["BUCKET_DISPLAY", "CONFIDENCE_LABEL"]


BUCKET_DISPLAY: dict[Bucket, str] = {
    Bucket.SINGLE_WRITER: "single_writer per artifact",
    Bucket.SHARED_ARTIFACT: "shared_artifact",
    Bucket.PARALLEL_BRANCH: "parallel_branch",
    Bucket.MIXED_PATTERN: "mixed pattern",
    Bucket.INSUFFICIENT: "insufficient coverage",
}

CONFIDENCE_LABEL: dict[Confidence, str] = {
    Confidence.HIGH: "high",
    Confidence.PRELIMINARY: "preliminary",
    Confidence.INSUFFICIENT: "insufficient",
}
