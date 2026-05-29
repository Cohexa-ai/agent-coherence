# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""Tests for architecture boundary and cycle checks."""

from __future__ import annotations

import ast
from pathlib import Path

from ccs.hardening.architecture import find_boundary_violations, find_cycles, run_architecture_checks


def test_project_architecture_has_no_boundary_or_cycle_violations() -> None:
    src_root = Path(__file__).resolve().parents[1] / "src"
    report = run_architecture_checks(src_root)

    assert report.boundary_violations == []
    assert report.cycles == []


def test_cycle_detection_finds_strongly_connected_component() -> None:
    graph = {
        "ccs.core.a": {"ccs.core.b"},
        "ccs.core.b": {"ccs.core.c"},
        "ccs.core.c": {"ccs.core.a"},
        "ccs.core.d": set(),
    }
    cycles = find_cycles(graph)
    assert cycles == [["ccs.core.a", "ccs.core.b", "ccs.core.c"]]


def test_boundary_violation_detection_reports_forbidden_edge() -> None:
    graph = {
        "ccs.core.types": {"ccs.simulation.engine"},
        "ccs.simulation.engine": set(),
    }
    violations = find_boundary_violations(graph)
    assert len(violations) == 1
    assert "ccs.core.types" in violations[0]
    assert "ccs.simulation.engine" in violations[0]


# ---- v0.8.3 C-flip Unit 3 grep-gate -----------------------------------------
#
# See docs/plans/2026-05-28-001-feat-c-flip-crash-recovery-default-on-plan.md
# Unit 3. During the v0.8.3 → v0.9.0 deprecation window, library-internal
# bare ``CrashRecoveryConfig()`` calls would surface the DeprecationWarning
# from inside library code (per R2). This regression gate codifies the
# guarantee: no bare construction sites in src/ outside the helper itself.
#
# This entire test (and its helper) is REMOVED in v0.9.0 when the deprecation
# warning is also removed and bare ``CrashRecoveryConfig()`` becomes safe again.


def _find_bare_crash_recovery_config_sites(src_root: Path) -> list[str]:
    """Return repo-relative paths + line numbers of bare CrashRecoveryConfig()
    call sites in src/, EXCLUDING the helper definition in service.py.

    A "bare" call is ``CrashRecoveryConfig()`` with zero positional and zero
    keyword arguments. Explicit constructions like ``CrashRecoveryConfig(
    enabled=False)`` or ``CrashRecoveryConfig(enabled=True, ...)`` are
    permitted because they bypass the sentinel detection.
    """
    findings: list[str] = []
    for py_path in src_root.rglob("*.py"):
        # Skip the helper definition itself — _default_disabled_config builds
        # CrashRecoveryConfig(enabled=False) which has kwargs, so it is not
        # bare; nothing to exclude here, but kept for clarity.
        try:
            tree = ast.parse(py_path.read_text(encoding="utf-8"))
        except SyntaxError:
            continue
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            func = node.func
            # Match `CrashRecoveryConfig(...)` (Name) and
            # `module.CrashRecoveryConfig(...)` (Attribute).
            name = None
            if isinstance(func, ast.Name):
                name = func.id
            elif isinstance(func, ast.Attribute):
                name = func.attr
            if name != "CrashRecoveryConfig":
                continue
            if node.args or node.keywords:
                continue  # explicit construction — bypasses the sentinel
            rel = py_path.relative_to(src_root.parent)
            findings.append(f"{rel}:{node.lineno}")
    return sorted(findings)


def test_no_bare_crash_recovery_config_construction_in_src() -> None:
    """v0.8.3 R2 regression gate: zero bare CrashRecoveryConfig() in src/.

    Library-internal code must use _default_disabled_config() or pass
    enabled= explicitly. This test prevents a future patch from silently
    re-introducing a bare construction site that would surface the
    v0.8.3 DeprecationWarning to users from inside library code.

    REMOVED in v0.9.0 when the deprecation warning is also removed.
    """
    repo_root = Path(__file__).resolve().parents[1]
    src_root = repo_root / "src"
    sites = _find_bare_crash_recovery_config_sites(src_root)
    assert sites == [], (
        "Bare CrashRecoveryConfig() construction sites found in src/. "
        "During the v0.8.3 deprecation window, these surface a "
        "DeprecationWarning to users from library-internal code. "
        "Use _default_disabled_config() or pass enabled= explicitly.\n"
        f"Sites: {sites}"
    )
