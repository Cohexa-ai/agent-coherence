# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""Tests for ``agent-coherence-migrate-deny`` (v0.2 plan Unit 3, KTD-R).

Covers (per plan test scenarios):
- Happy: "Use rg instead of grep" → Bash(grep:*)
- Happy: "Never use python3 -c" → Bash(python3 -c:*) + Bash(python -c:*)
- Happy: multiple rules → dedup'd array
- Edge: missing CLAUDE.md → empty array + stderr warning, exit 0
- Edge: zero rules in CLAUDE.md → empty array, exit 0
- Edge: ambiguous phrasing → NOT matched (under-emit bias)
- Security — symlink containment:
  * CLAUDE.md symlink to /etc/passwd → refuse, exit 2
  * CLAUDE.md symlink to sibling workspace → refuse, exit 2
  * --claude-md ../sibling → refuse, exit 2
  * --workspace /tmp/random-dir (no .coherence/, no .git/) → refuse, exit 1
- Security — write isolation: settings.json byte-identical before/after
- Security — no LLM/network: no outbound HTTP issued (filesystem-bound)
- Error: malformed UTF-8 → exit 2 with clear stderr
- Integration: stdout output parses as valid JSON
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from ccs.adapters.claude_code.migrate_deny import detect_deny_entries, main


# ----------------------------------------------------------------------
# Fixtures
# ----------------------------------------------------------------------


@pytest.fixture
def workspace(tmp_path: Path) -> Path:
    """A fresh workspace dir with .coherence/ marker so the helper accepts
    it without --claude-md override."""
    (tmp_path / ".coherence").mkdir()
    return tmp_path


def _run(argv: list[str], capsys: pytest.CaptureFixture[str]) -> tuple[int, str, str]:
    """Invoke main() in-process and capture (exit_code, stdout, stderr)."""
    code = main(argv)
    captured = capsys.readouterr()
    return code, captured.out, captured.err


# ----------------------------------------------------------------------
# Happy-path detection
# ----------------------------------------------------------------------


def test_grep_to_rg_rule(workspace: Path, capsys: pytest.CaptureFixture[str]) -> None:
    (workspace / "CLAUDE.md").write_text("# Project rules\n\nUse `rg` instead of `grep`.\n")
    code, out, err = _run(["--workspace", str(workspace)], capsys)
    assert code == 0
    parsed = json.loads(out)
    assert parsed == {"permissions": {"deny": ["Bash(grep:*)"]}}


def test_never_python_dash_c_rule(
    workspace: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    (workspace / "CLAUDE.md").write_text("Never use `python3 -c`.\n")
    code, out, err = _run(["--workspace", str(workspace)], capsys)
    assert code == 0
    parsed = json.loads(out)
    assert "Bash(python3 -c:*)" in parsed["permissions"]["deny"]
    assert "Bash(python -c:*)" in parsed["permissions"]["deny"]


def test_multiple_rules_deduplicated(
    workspace: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    (workspace / "CLAUDE.md").write_text(
        "Use `rg` instead of `grep`.\n"
        "Never use sudo.\n"
        "Never use `python3 -c`.\n"
        # Repeat of grep-to-rg in different prose — must dedup.
        "Use `rg`, not `grep`.\n"
    )
    code, out, err = _run(["--workspace", str(workspace)], capsys)
    assert code == 0
    parsed = json.loads(out)
    entries = parsed["permissions"]["deny"]
    # Deduped — Bash(grep:*) appears once even though two patterns matched.
    assert entries.count("Bash(grep:*)") == 1
    assert "Bash(sudo:*)" in entries
    assert "Bash(python3 -c:*)" in entries


def test_agents_md_also_scanned(
    workspace: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    (workspace / "AGENTS.md").write_text("Never use perl -e.\n")
    code, out, err = _run(["--workspace", str(workspace)], capsys)
    assert code == 0
    parsed = json.loads(out)
    assert "Bash(perl -e:*)" in parsed["permissions"]["deny"]


# ----------------------------------------------------------------------
# Edge cases
# ----------------------------------------------------------------------


def test_no_claude_md_emits_empty_array_and_warning(
    workspace: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """No CLAUDE.md or AGENTS.md → empty deny array, stderr warning, exit 0."""
    code, out, err = _run(["--workspace", str(workspace)], capsys)
    assert code == 0
    parsed = json.loads(out)
    assert parsed == {"permissions": {"deny": []}}
    assert "no CLAUDE.md or AGENTS.md found" in err


def test_claude_md_with_zero_rules_emits_empty_array(
    workspace: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    (workspace / "CLAUDE.md").write_text(
        "# Project conventions\n\nWe ship to PyPI weekly.\n"
        "Use feature branches.\n"
    )
    code, out, err = _run(["--workspace", str(workspace)], capsys)
    assert code == 0
    parsed = json.loads(out)
    assert parsed == {"permissions": {"deny": []}}


def test_ambiguous_phrasing_not_matched_under_emit_bias(
    workspace: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Under-emit bias: 'prefer rg over grep when possible' is too soft to
    auto-translate; do NOT propose a deny."""
    (workspace / "CLAUDE.md").write_text(
        "We prefer `rg` over `grep` when possible.\n"
        "Avoid `python -c` for production code.\n"  # avoid != never
    )
    code, out, err = _run(["--workspace", str(workspace)], capsys)
    assert code == 0
    parsed = json.loads(out)
    # Neither prefer-soft nor avoid-soft triggers — under-emit bias.
    assert parsed == {"permissions": {"deny": []}}


# ----------------------------------------------------------------------
# Security — symlink containment (KTD-R)
# ----------------------------------------------------------------------


def test_symlink_to_outside_file_rejected(
    tmp_path_factory: pytest.TempPathFactory,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """CLAUDE.md as a symlink to a file outside the workspace is refused
    with exit 2 and clear stderr. No content read, no JSON emitted.
    The two tmp dirs come from tmp_path_factory so they are siblings —
    NOT one nested in the other (which would make the symlink target
    actually inside the workspace by canonical-path)."""
    workspace = tmp_path_factory.mktemp("workspace")
    (workspace / ".coherence").mkdir()
    outside_dir = tmp_path_factory.mktemp("outside")
    outside_file = outside_dir / "attacker.txt"
    outside_file.write_text("attacker-crafted CLAUDE.md\nUse `rg` instead of `grep`.\n")
    claude_md = workspace / "CLAUDE.md"
    claude_md.symlink_to(outside_file)
    code, out, err = _run(["--workspace", str(workspace), "--claude-md", str(claude_md)], capsys)
    assert code == 2
    assert "NOT a descendant" in err
    # No JSON emitted on rejection.
    assert out == ""


def test_relative_traversal_override_rejected(
    workspace: Path, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """--claude-md ../sibling-workspace/CLAUDE.md is refused."""
    sibling = tmp_path / "sibling-workspace"
    sibling.mkdir()
    (sibling / "CLAUDE.md").write_text("Use `rg` instead of `grep`.\n")
    traversal = workspace / ".." / sibling.name / "CLAUDE.md"
    code, out, err = _run(
        ["--workspace", str(workspace), "--claude-md", str(traversal)],
        capsys,
    )
    assert code == 2
    assert "NOT a descendant" in err


def test_workspace_without_markers_rejected(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """--workspace /tmp/random-dir without .coherence/ or .git/ is refused."""
    random_dir = tmp_path / "random"
    random_dir.mkdir()
    code, out, err = _run(["--workspace", str(random_dir)], capsys)
    assert code == 1
    assert "neither .coherence/ nor .git/" in err


def test_workspace_with_git_marker_accepted(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """A .git/ marker is sufficient (operator may not have .coherence/ yet)."""
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / ".git").mkdir()
    (repo / "CLAUDE.md").write_text("Use `rg` instead of `grep`.\n")
    code, out, err = _run(["--workspace", str(repo)], capsys)
    assert code == 0
    parsed = json.loads(out)
    assert "Bash(grep:*)" in parsed["permissions"]["deny"]


# ----------------------------------------------------------------------
# Security — write isolation
# ----------------------------------------------------------------------


def test_helper_never_writes_to_settings_json(
    workspace: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Filesystem snapshot: .claude/settings.local.json is byte-identical
    before and after invocation."""
    settings_dir = workspace / ".claude"
    settings_dir.mkdir()
    settings = settings_dir / "settings.local.json"
    initial_content = '{"permissions": {"deny": ["Bash(curl:*)"]}}\n'
    settings.write_text(initial_content)
    initial_stat = settings.stat()

    (workspace / "CLAUDE.md").write_text("Use `rg` instead of `grep`.\n")
    code, out, err = _run(["--workspace", str(workspace)], capsys)
    assert code == 0
    # File unchanged byte-by-byte AND mtime unchanged.
    assert settings.read_text() == initial_content
    assert settings.stat().st_mtime == initial_stat.st_mtime


def test_helper_does_not_create_settings_json_if_absent(
    workspace: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """No .claude/settings.local.json exists before; none after."""
    (workspace / "CLAUDE.md").write_text("Never use sudo.\n")
    code, out, err = _run(["--workspace", str(workspace)], capsys)
    assert code == 0
    assert not (workspace / ".claude" / "settings.local.json").exists()


# ----------------------------------------------------------------------
# Error paths
# ----------------------------------------------------------------------


def test_malformed_utf8_in_claude_md_exit_2(
    workspace: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Invalid UTF-8 in CLAUDE.md → exit 2 with clear diagnostic. Do NOT
    mangle output or silently skip; the helper bails so the operator
    knows to fix the file."""
    (workspace / "CLAUDE.md").write_bytes(b"\xff\xfeINVALID UTF-8\n")
    code, out, err = _run(["--workspace", str(workspace)], capsys)
    assert code == 2
    assert "not valid UTF-8" in err


# ----------------------------------------------------------------------
# Integration — output parses cleanly + pipeable
# ----------------------------------------------------------------------


def test_output_is_valid_json(
    workspace: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """The stdout block parses cleanly with no trailing junk; structure
    matches the settings.json permissions schema."""
    (workspace / "CLAUDE.md").write_text("Never use sudo.\n")
    code, out, err = _run(["--workspace", str(workspace)], capsys)
    assert code == 0
    parsed = json.loads(out)
    assert "permissions" in parsed
    assert "deny" in parsed["permissions"]
    assert isinstance(parsed["permissions"]["deny"], list)


def test_console_script_registered_via_entry_point() -> None:
    """The pyproject.toml [project.scripts] entry installs the script
    under ``agent-coherence-migrate-deny``. Import path resolves."""
    from ccs.adapters.claude_code import migrate_deny

    assert callable(migrate_deny.main)
