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

import ast
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


def _decorator_dotted_name(node: ast.expr) -> str:
    """Dotted name of a decorator/mark expression, unwrapping a call to its func."""
    if isinstance(node, ast.Call):
        node = node.func
    parts: list[str] = []
    while isinstance(node, ast.Attribute):
        parts.append(node.attr)
        node = node.value
    if isinstance(node, ast.Name):
        parts.append(node.id)
    return ".".join(reversed(parts))


def _pytestmark_values(tree: ast.Module) -> list[ast.expr]:
    """Mark expressions from module-level ``pytestmark = ...`` (bare or list)."""
    values: list[ast.expr] = []
    for stmt in tree.body:
        if isinstance(stmt, ast.Assign) and any(
            isinstance(target, ast.Name) and target.id == "pytestmark"
            for target in stmt.targets
        ):
            value = stmt.value
            values.extend(value.elts if isinstance(value, (ast.List, ast.Tuple)) else [value])
    return values


def _assert_not_unconditionally_skipped(
    source: str, bare_name: str, rung: str, file_label: str
) -> None:
    """AST-level skip guard — immune to formatting (multi-line decorators,
    ``pytestmark`` assignments) that evades line-oriented string checks."""
    tree = ast.parse(source)
    functions = [
        node
        for node in ast.walk(tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name == bare_name
    ]
    assert functions, f"rung {rung!r}: {bare_name!r} not found in {file_label}"
    marks = [mark for function in functions for mark in function.decorator_list]
    marks.extend(_pytestmark_values(tree))
    # endswith keeps ``mark.skipif`` (the platform-gate exception) allowed while
    # catching every ``pytest.mark.skip`` spelling — bare, called, or aliased.
    skips = [mark for mark in marks if _decorator_dotted_name(mark).endswith("mark.skip")]
    assert not skips, (
        f"rung {rung!r}: {bare_name!r} in {file_label} is unconditionally skipped "
        "(skipif with a stated platform reason is the only allowed guard)"
    )


@pytest.mark.parametrize(("rung", "node_id"), _all_proving_tests())
def test_no_proving_test_is_unconditionally_skipped(rung: str, node_id: str) -> None:
    """A proving test behind an unconditional skip proves nothing."""
    file_part, test_name = node_id.split("::", 1)
    source = (_REPO_ROOT / file_part).read_text()
    bare_name = test_name.rsplit("::", 1)[-1]
    _assert_not_unconditionally_skipped(source, bare_name, rung, file_part)


def test_skip_guard_catches_multiline_skip_decorator() -> None:
    """Teeth: a multi-line ``@pytest.mark.skip(...)`` cannot evade the guard."""
    source = (
        "import pytest\n\n"
        "@pytest.mark.skip(\n"
        '    reason="x"\n'
        ")\n"
        "def test_foo(): ...\n"
    )
    with pytest.raises(AssertionError, match="unconditionally skipped"):
        _assert_not_unconditionally_skipped(source, "test_foo", "rung-x", "<inline>")


def test_skip_guard_catches_module_level_pytestmark_skip() -> None:
    """Teeth: ``pytestmark = pytest.mark.skip(...)`` cannot evade the guard."""
    source = (
        "import pytest\n\n"
        'pytestmark = pytest.mark.skip(reason="x")\n\n'
        "def test_foo(): ...\n"
    )
    with pytest.raises(AssertionError, match="unconditionally skipped"):
        _assert_not_unconditionally_skipped(source, "test_foo", "rung-x", "<inline>")


def test_skip_guard_allows_skipif_platform_gate() -> None:
    """Positive control: skipif-with-reason is the sanctioned platform gate."""
    source = (
        "import pytest\n\n"
        '@pytest.mark.skipif(True, reason="platform")\n'
        "def test_foo(): ...\n"
    )
    _assert_not_unconditionally_skipped(source, "test_foo", "rung-x", "<inline>")


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
        assert lead in all_pins, (
            f"README claims {lead!r} but no rung backs it — add the rung (with "
            "proving tests) or remove the claim"
        )


def test_readme_row_guard_rejects_extending_lead() -> None:
    """Teeth: a lead that merely extends a real pin (broader claim, same prefix)
    must not satisfy the exact-membership check."""
    all_pins = {pin for rung in CLAIM_LADDER for pin in rung.readme_pins}
    assert "Stale-read overwrite across hosts" not in all_pins


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
