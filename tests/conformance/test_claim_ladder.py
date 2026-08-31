# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""U7 meta-test — the claim ladder is machine-checked (R-6, KTD-10; AE4).

Resolves every rung's proving tests by ACTUAL pytest collection (a renamed or
deleted test fails here with the rung named), refuses unconditional skips,
and drift-guards the rung pins against the README in both directions. The
node-id resolution lives here, in the repo tree, so a foreign consumer
importing the packaged registry never inherits repo-local test references.
"""

from __future__ import annotations

import re
import subprocess
import sys
from functools import lru_cache
from pathlib import Path

import pytest

from ccs.testing.claim_ladder import CLAIM_LADDER

_REPO_ROOT = Path(__file__).resolve().parents[2]
_README = _REPO_ROOT / "README.md"


def _all_proving_tests() -> list[tuple[str, str]]:
    return [(rung.rung, node_id) for rung in CLAIM_LADDER for node_id in rung.proving_tests]


@lru_cache(maxsize=1)
def _collected_node_ids() -> tuple[str, ...]:
    """One real collection pass over every file the ladder names."""
    files = sorted({node_id.split("::", 1)[0] for _, node_id in _all_proving_tests()})
    proc = subprocess.run(
        [sys.executable, "-m", "pytest", "--collect-only", "-q", "-p", "no:cacheprovider", *files],
        capture_output=True,
        text=True,
        cwd=_REPO_ROOT,
        timeout=300,
    )
    assert proc.returncode == 0, (
        f"collection over the ladder's files failed:\n{proc.stdout}\n{proc.stderr}"
    )
    return tuple(line.strip() for line in proc.stdout.splitlines() if "::" in line)


@pytest.mark.parametrize(("rung", "node_id"), _all_proving_tests())
def test_every_proving_test_resolves_at_collection(rung: str, node_id: str) -> None:
    """AE4: a claim whose proving test vanished fails CI with the rung named."""
    collected = _collected_node_ids()
    resolved = any(
        line == node_id or line.startswith(node_id + "[") for line in collected
    )
    assert resolved, (
        f"rung {rung!r} names {node_id!r}, which did not resolve at collection "
        "time — the claim has lost its proving test"
    )


@pytest.mark.parametrize(("rung", "node_id"), _all_proving_tests())
def test_no_proving_test_is_unconditionally_skipped(rung: str, node_id: str) -> None:
    """A proving test behind an unconditional skip proves nothing."""
    file_part, test_name = node_id.split("::", 1)
    source = (_REPO_ROOT / file_part).read_text()
    bare_name = test_name.rsplit("::", 1)[-1]
    match = re.search(
        rf"((?:^\s*@.*\n)*)^\s*def {re.escape(bare_name)}\(", source, re.MULTILINE
    )
    assert match is not None, f"rung {rung!r}: {bare_name!r} not found in {file_part}"
    decorators = match.group(1)
    assert "pytest.mark.skip(" not in decorators and "pytest.mark.skip\n" not in decorators, (
        f"rung {rung!r}: {node_id!r} is unconditionally skipped (skipif with a "
        "stated platform reason is the only allowed guard)"
    )
    assert "pytestmark = pytest.mark.skip\n" not in source


def _claim_section() -> str:
    text = _README.read_text()
    start = text.index("## What it guarantees")
    end = text.index("**Scope, honestly:**", start)
    return text[start:end]


def test_every_rung_pin_appears_in_the_readme() -> None:
    """Direction A: the registry claims nothing the README does not say."""
    readme = _README.read_text()
    for rung in CLAIM_LADDER:
        for pin in rung.readme_pins:
            assert pin in readme, (
                f"rung {rung.rung!r} pins {pin!r}, which the README no longer "
                "contains — retire the rung or restore the claim"
            )


def test_every_readme_claim_row_has_a_rung() -> None:
    """Direction B: the README claims nothing the registry does not back."""
    row_leads = re.findall(r"^\| \*\*(.+?)\*\*", _claim_section(), re.MULTILINE)
    assert row_leads, "the README claim table was not found — update the drift guard"
    all_pins = {pin for rung in CLAIM_LADDER for pin in rung.readme_pins}
    for lead in row_leads:
        assert any(lead.startswith(pin) or pin.startswith(lead) for pin in all_pins), (
            f"README claims {lead!r} but no rung backs it — add the rung (with "
            "proving tests) or remove the claim"
        )


def test_no_cross_host_rung_exists_to_claim() -> None:
    """The ladder's ceiling is the single-coordinator boundary — a cross-host
    rung may only ever arrive together with the tests that earn it."""
    for rung in CLAIM_LADDER:
        assert "cross-host" not in rung.rung, f"unexpected cross-host rung {rung.rung!r}"
        assert "cross-host" not in rung.guarantee.lower()


def test_rung_slugs_are_unique_and_populated() -> None:
    slugs = [rung.rung for rung in CLAIM_LADDER]
    assert len(slugs) == len(set(slugs))
    for rung in CLAIM_LADDER:
        assert rung.guarantee and rung.readme_pins and rung.proving_tests
