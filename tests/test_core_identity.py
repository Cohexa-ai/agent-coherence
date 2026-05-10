# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""Tests for ccs.core.identity.artifact_uuid.

Covers Unit 1 of the ccs-diagnose Tier 2 plan: shared-infrastructure helper
extracted from src/ccs/adapters/ccsstore.py:526 so the CLI and CCSStore
register artifacts under the same identity scheme.
"""

from __future__ import annotations

import uuid

from ccs.core.identity import artifact_uuid


def test_artifact_uuid_deterministic_for_same_scope_and_key() -> None:
    """Same (scope, key) yields the same UUIDv5 across calls."""
    first = artifact_uuid("langgraph", "messages")
    second = artifact_uuid("langgraph", "messages")
    assert first == second
    assert isinstance(first, uuid.UUID)
    assert first.version == 5


def test_artifact_uuid_different_for_different_keys() -> None:
    """Different keys under the same scope yield different UUIDs."""
    first = artifact_uuid("langgraph", "messages")
    second = artifact_uuid("langgraph", "scratchpad")
    assert first != second


def test_artifact_uuid_different_for_different_scopes() -> None:
    """Same key under different scopes yields different UUIDs."""
    first = artifact_uuid("langgraph", "messages")
    second = artifact_uuid("crewai", "messages")
    assert first != second


def test_artifact_uuid_empty_scope_permitted() -> None:
    """Empty scope is permitted; produces a deterministic UUID distinct from non-empty scopes."""
    first = artifact_uuid("", "messages")
    second = artifact_uuid("", "messages")
    assert first == second
    assert first != artifact_uuid("langgraph", "messages")


def test_artifact_uuid_unicode_key_deterministic() -> None:
    """Unicode keys produce deterministic UUIDs (no encoding-related drift)."""
    first = artifact_uuid("langgraph", "メッセージ")
    second = artifact_uuid("langgraph", "メッセージ")
    assert first == second


def test_artifact_uuid_colon_in_scope_or_key_does_not_escape() -> None:
    """Documents v0 behavior: helper does NOT escape ``:`` between scope and key.

    Without escaping, ``artifact_uuid("a:b", "c")`` and ``artifact_uuid("a", "b:c")``
    collapse to the same content string ``ccs-artifact:a:b:c`` and produce the same
    UUID. Callers must pick unambiguous scope/key shapes or accept the collision risk.
    If a future revision adds escaping, this assertion flips and the docstring is
    updated.
    """
    assert artifact_uuid("a:b", "c") == artifact_uuid("a", "b:c")


def test_artifact_uuid_matches_pre_refactor_ccsstore_format() -> None:
    """Regression: helper produces the same UUID as the pre-refactor inline pattern.

    Before extraction, ``src/ccs/adapters/ccsstore.py:526`` computed:
        uuid.uuid5(uuid.NAMESPACE_URL, f"ccs-artifact:{scope_str}:{key}")

    Any artifact registered under the inline scheme must continue to produce the
    same identity after the helper extraction so existing CCSStore consumers see
    no identity drift.
    """
    scope_str = "graph:thread"
    key = "messages"
    expected = uuid.uuid5(uuid.NAMESPACE_URL, f"ccs-artifact:{scope_str}:{key}")
    assert artifact_uuid(scope_str, key) == expected
