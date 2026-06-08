# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""Cross-implementation warn-mode wire-shape corpus tests (plan Unit 7a).

Parametrized over (fixture, backend). Each row runs the fixture's request
against the named backend in an isolated tmp workspace and asserts the
normalized response matches the fixture's ``expected`` block.

Marked ``protocol_corpus`` — opt-in via ``pytest -m protocol_corpus``. Skipped
in default runs (see ``pyproject.toml`` ``addopts``)."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from tests.protocol_corpus.harness import (
    BACKEND_NODE,
    Fixture,
    load_fixtures,
    normalize_response,
    resolve_node_dist_path,
    run_scenario,
)

pytestmark = pytest.mark.protocol_corpus


def _all_warn_mode_fixtures() -> list[Fixture]:
    """Loaded once at collection so parametrize ids stay stable."""
    return load_fixtures("warn_mode")


def _parametrize_rows() -> list[tuple[Fixture, str]]:
    """Cartesian product of (fixture, backend) restricted to each fixture's
    declared backends list."""
    rows: list[tuple[Fixture, str]] = []
    for fixture in _all_warn_mode_fixtures():
        for backend in fixture.backends:
            rows.append((fixture, backend))
    return rows


def _row_id(row: tuple[Fixture, str]) -> str:
    fixture, backend = row
    return f"{fixture.name}[{backend}]"


_ROWS = _parametrize_rows()
_NODE_DIST_PATH = resolve_node_dist_path()


@pytest.mark.parametrize("row", _ROWS, ids=[_row_id(r) for r in _ROWS] if _ROWS else None)
def test_warn_mode_fixture_response_matches_expected(
    row: tuple[Fixture, str],
    tmp_path: Path,
) -> None:
    fixture, backend = row
    if backend == BACKEND_NODE and _NODE_DIST_PATH is None:
        pytest.xfail(
            "Node coordinator dist not resolvable. Build the plugin checkout "
            "(npm ci && npm run build) or set AGENT_COHERENCE_PLUGIN_DIST_PATH."
        )

    actual_status, actual_body = run_scenario(
        fixture=fixture,
        backend_id=backend,
        workspace=tmp_path,
        node_dist_path=_NODE_DIST_PATH,
    )

    # Pre-normalize the EXPECTED body too — fixtures can be authored either
    # with the post-normalization sentinels already in place ("<TS>", "<UUID>")
    # or with realistic-looking placeholders. Round-trip both through the same
    # normalizer so author choice doesn't affect equality.
    expected_status = fixture.expected["status"]
    expected_body = normalize_response(
        fixture.expected["body"],
        ignore_keys=fixture.ignore_keys,
        optional_keys=fixture.optional_keys,
    )

    assert actual_status == expected_status, (
        f"{fixture.name}[{backend}]: status mismatch — "
        f"expected {expected_status}, got {actual_status}\n"
        f"body={actual_body!r}"
    )
    assert actual_body == expected_body, (
        f"{fixture.name}[{backend}]: body mismatch\n"
        f"expected={expected_body!r}\nactual=  {actual_body!r}"
    )


def test_collection_loaded_fixtures() -> None:
    """Self-test: at least one fixture is present so parametrize doesn't
    silently no-op. Catches the failure mode where the fixtures directory
    is empty or path-resolution is wrong."""
    fixtures = _all_warn_mode_fixtures()
    assert len(fixtures) >= 8, (
        f"Expected ≥8 warn-mode fixtures, found {len(fixtures)}. "
        f"Add coverage in tests/protocol_corpus/fixtures/warn_mode/."
    )


def test_normalizer_self_test_detects_real_divergence() -> None:
    """Harness self-test (per Unit 7a verification): deliberately-different
    bodies must NOT compare equal after normalization. Catches the failure
    mode where over-aggressive normalization rules false-pass real drift."""
    body_a: dict[str, Any] = {
        "schema_version": 1,
        "coordinator_uptime_seconds": 123.4,
        "instance_id": "11111111-1111-4111-8111-111111111111",
        "tracked_count": 5,
    }
    body_b: dict[str, Any] = {
        "schema_version": 1,
        "coordinator_uptime_seconds": 999.9,         # would be ignored
        "instance_id": "22222222-2222-4222-8222-222222222222",  # would be ignored
        "tracked_count": 7,                            # real divergence
    }
    norm_a = normalize_response(body_a)
    norm_b = normalize_response(body_b)
    assert norm_a != norm_b, (
        "Normalizer should NOT mask real differences in non-time/UUID fields. "
        f"norm_a={norm_a!r}, norm_b={norm_b!r}"
    )


def test_normalizer_handles_nested_uuid_and_timestamp() -> None:
    """Self-test: nested dicts and lists get walked, string-position UUID/ISO
    timestamps scrubbed."""
    body: dict[str, Any] = {
        "sessions": [
            {
                "agent_id": "11111111-1111-4111-8111-111111111111",
                "started_at": 1700000000.0,
                "last_request_at": "2026-05-23T12:34:56Z",
                "message": "agent 22222222-2222-4222-8222-222222222222 acquired at 2026-05-23T10:00:00Z",
            }
        ],
        "coordinator_uptime_seconds": 42,
    }
    out = normalize_response(body)
    session = out["sessions"][0]
    assert session["agent_id"] == "<UUID>"
    assert session["started_at"] == "<TS>"
    assert session["last_request_at"] == "<TS>"
    # In-string substitution: both the embedded UUID and timestamp scrubbed.
    assert "<UUID>" in session["message"]
    assert "<TS>" in session["message"]
    assert out["coordinator_uptime_seconds"] == "<UPTIME>"
