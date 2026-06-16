# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Tests for optional artifact diff engine utilities."""

from __future__ import annotations

from ccs.artifacts.diff_engine import (
    apply_json_delta,
    apply_text_delta,
    compute_delta_stats,
    compute_json_delta,
    compute_text_delta,
    estimate_delta_size,
)


def test_text_delta_roundtrip_reconstructs_new_content() -> None:
    old = "line-a\nline-b\nline-c\n"
    new = "line-a\nline-b2\nline-c\nline-d\n"

    delta = compute_text_delta(old, new)
    reconstructed = apply_text_delta(delta)

    assert reconstructed == new


def test_json_delta_roundtrip_reconstructs_payload() -> None:
    old_payload = {"version": 1, "items": [{"id": "a", "score": 1}]}
    new_payload = {"version": 2, "items": [{"id": "a", "score": 2}, {"id": "b", "score": 1}]}

    delta = compute_json_delta(old_payload, new_payload)
    reconstructed = apply_json_delta(delta)

    assert reconstructed == new_payload


def test_delta_stats_and_size_are_reported() -> None:
    delta = compute_text_delta("a\nb\n", "a\nc\n")
    stats = compute_delta_stats(delta)
    size = estimate_delta_size(delta)

    assert stats.added_lines >= 1
    assert stats.removed_lines >= 1
    assert stats.changed_lines >= 1
    assert size > 0


def test_no_change_yields_only_unchanged_lines() -> None:
    delta = compute_text_delta("x\ny\n", "x\ny\n")
    stats = compute_delta_stats(delta)

    assert stats.added_lines == 0
    assert stats.removed_lines == 0
    assert stats.unchanged_lines == 2
