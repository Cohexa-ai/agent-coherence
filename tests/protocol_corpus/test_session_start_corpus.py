# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Cross-implementation SB-10 session-start / deferred re-grounding corpus.

Byte wire-parity for the ``/hooks/session-start`` endpoint (SB-10 U2/U6) and
the deferred-injection allow envelopes (SB-10 U4/U8) across the Python and
Node coordinators. Fixtures live in ``fixtures/session_start/`` and follow
the warn/strict corpus schema — ``preflight_requests`` drives the multi-step
"A reads → B commits → A session-start → A admits" setups.

The SB-10 prose templates (``SESSION_START_*`` in ``hook_payloads``) carry NO
timestamps by design, so the payloads byte-match without touching the
harness's fixed normalization key lists. The stale-read warning prose that
the deferred block appends onto is also byte-mirrored; only its in-string
ISO timestamps go through the existing ``_ISO_TS_RE`` scrub.

Marked ``protocol_corpus`` — opt-in via ``pytest -m protocol_corpus``. Skipped
in default runs (see ``pyproject.toml`` ``addopts``)."""

from __future__ import annotations

from pathlib import Path

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


def _all_session_start_fixtures() -> list[Fixture]:
    """Loaded once at collection so parametrize ids stay stable."""
    return load_fixtures("session_start")


def _parametrize_rows() -> list[tuple[Fixture, str]]:
    rows: list[tuple[Fixture, str]] = []
    for fixture in _all_session_start_fixtures():
        for backend in fixture.backends:
            rows.append((fixture, backend))
    return rows


def _row_id(row: tuple[Fixture, str]) -> str:
    fixture, backend = row
    return f"{fixture.name}[{backend}]"


_ROWS = _parametrize_rows()
_NODE_DIST_PATH = resolve_node_dist_path()


@pytest.mark.parametrize("row", _ROWS, ids=[_row_id(r) for r in _ROWS] if _ROWS else None)
def test_session_start_fixture_response_matches_expected(
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


def test_collection_loaded_session_start_fixtures() -> None:
    """Self-test: the six SB-10 fixtures are present so parametrize doesn't
    silently no-op. Catches the failure mode where the fixtures directory
    is empty or path-resolution is wrong."""
    fixtures = _all_session_start_fixtures()
    assert len(fixtures) >= 6, (
        f"Expected ≥6 session-start fixtures, found {len(fixtures)}. "
        f"Add coverage in tests/protocol_corpus/fixtures/session_start/."
    )
