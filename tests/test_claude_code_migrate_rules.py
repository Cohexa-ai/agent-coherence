# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""Tests for the Unit 9 ``agent-coherence-migrate-rules`` CLI (R19).

The detector is heuristic — these tests pin the load-bearing positive
detections (grep→rg, sudo, python -c, perl -e, ruby -e, cat/sed/awk for
files) and the false-positive negative cases (prose that mentions a
tool but doesn't restrict it must NOT propose a deny entry)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from ccs.cli import coherence_migrate_rules
from ccs.cli.coherence_migrate_rules import detect_rules


# ----------------------------------------------------------------------
# Fixtures
# ----------------------------------------------------------------------


@pytest.fixture
def workspace(tmp_path: Path) -> Path:
    """A workspace with a .git marker so find_coordinator_root resolves."""
    (tmp_path / ".git").mkdir()
    return tmp_path


def _write_claude_md(workspace: Path, body: str) -> None:
    (workspace / "CLAUDE.md").write_text(body, encoding="utf-8")


# ----------------------------------------------------------------------
# Detection — positive cases (each rule type)
# ----------------------------------------------------------------------


def test_detect_grep_to_rg_canonical_prose(workspace: Path) -> None:
    _write_claude_md(workspace, "## Style\n\n- Use rg instead of grep for all searches.\n")
    report = detect_rules(workspace)
    assert any(r.name == "grep_to_rg" for r in report.detected)
    assert "Bash(grep:*)" in report.proposed_entries


def test_detect_no_sudo_canonical_prose(workspace: Path) -> None:
    _write_claude_md(workspace, "Operator commandments:\n- Never use sudo.\n")
    report = detect_rules(workspace)
    assert any(r.name == "no_sudo" for r in report.detected)
    assert "Bash(sudo *)" in report.proposed_entries


def test_detect_no_python_dash_c(workspace: Path) -> None:
    _write_claude_md(workspace, "Don't use python -c for scripting; write a .py file.")
    report = detect_rules(workspace)
    assert any(r.name == "no_python_dash_c" for r in report.detected)
    assert "Bash(python -c *)" in report.proposed_entries
    assert "Bash(python3 -c *)" in report.proposed_entries


def test_detect_no_perl_dash_e(workspace: Path) -> None:
    _write_claude_md(workspace, "Avoid perl -e one-liners — they are unreviewable.")
    report = detect_rules(workspace)
    assert any(r.name == "no_perl_dash_e" for r in report.detected)
    assert "Bash(perl -e *)" in report.proposed_entries


def test_detect_no_ruby_dash_e(workspace: Path) -> None:
    _write_claude_md(workspace, "Don't use ruby -e in this codebase.")
    report = detect_rules(workspace)
    assert any(r.name == "no_ruby_dash_e" for r in report.detected)
    assert "Bash(ruby -e *)" in report.proposed_entries


def test_detect_cat_for_files(workspace: Path) -> None:
    _write_claude_md(workspace, "Use the Read tool, not cat, for reading files.")
    report = detect_rules(workspace)
    assert any(r.name == "cat_for_files" for r in report.detected)
    assert "Bash(cat:*)" in report.proposed_entries


def test_detect_sed_for_files(workspace: Path) -> None:
    _write_claude_md(workspace, "Use the Edit tool instead of sed for editing files.")
    report = detect_rules(workspace)
    assert any(r.name == "sed_for_files" for r in report.detected)


def test_detect_awk_for_files(workspace: Path) -> None:
    """T-04 / finding #23: awk_for_files positive-detection test (was missing)."""
    _write_claude_md(workspace, "Don't use awk to read or edit files.")
    report = detect_rules(workspace)
    assert any(r.name == "awk_for_files" for r in report.detected)
    assert "Bash(awk:*)" in report.proposed_entries


# ----------------------------------------------------------------------
# Detection — negative cases (false-positive resistance)
# ----------------------------------------------------------------------


def test_no_detection_when_claude_md_is_neutral_prose(workspace: Path) -> None:
    """CLAUDE.md that mentions tools without restricting them must not
    propose denies. A naive substring search would hit on every project."""
    _write_claude_md(
        workspace,
        "## Setup\n\n"
        "Run `npm test` after editing TypeScript files. "
        "Logs are appended to ./logs/build.log. "
        "Use grep on the logs to find errors during local debugging.\n",
    )
    report = detect_rules(workspace)
    assert report.detected == []


def test_no_detection_for_descriptive_python_mention(workspace: Path) -> None:
    """A CLAUDE.md that says "we run `python -c 'import x'` in CI" without
    a restriction phrase must NOT flag python -c — restriction phrases are
    the trigger ("don't", "no", "avoid", "never")."""
    _write_claude_md(
        workspace,
        "CI runs python -c 'import this' as a smoke check.",
    )
    report = detect_rules(workspace)
    assert not any(r.name == "no_python_dash_c" for r in report.detected)


def test_no_claude_md_at_all_returns_empty_report(workspace: Path) -> None:
    """Idempotent over missing CLAUDE.md — the helper is safe to run in
    a fresh project."""
    report = detect_rules(workspace)
    assert report.detected == []


# ----------------------------------------------------------------------
# Deduplication: entries already in settings*.json
# ----------------------------------------------------------------------


def test_already_present_entries_are_not_re_proposed(workspace: Path) -> None:
    """If permissions.deny already lists Bash(sudo *), don't propose it
    again — but still detect the rule (so the operator sees the match)."""
    _write_claude_md(workspace, "Never use sudo.\n")
    claude_dir = workspace / ".claude"
    claude_dir.mkdir(parents=True)
    (claude_dir / "settings.local.json").write_text(json.dumps({
        "permissions": {"deny": ["Bash(sudo *)"]},
    }))
    report = detect_rules(workspace)
    assert any(r.name == "no_sudo" for r in report.detected)
    assert "Bash(sudo *)" not in report.proposed_entries


# ----------------------------------------------------------------------
# CLI integration
# ----------------------------------------------------------------------


def test_cli_no_claude_md_exits_0_with_message(
    workspace: Path, capsys: pytest.CaptureFixture[str],
) -> None:
    rc = coherence_migrate_rules.main(["--root", str(workspace)])
    captured = capsys.readouterr()
    assert rc == 0
    assert "no CLAUDE.md" in captured.out


def test_cli_no_rules_detected_exits_0_with_message(
    workspace: Path, capsys: pytest.CaptureFixture[str],
) -> None:
    _write_claude_md(workspace, "## Setup\n\nRun the tests.\n")
    rc = coherence_migrate_rules.main(["--root", str(workspace)])
    captured = capsys.readouterr()
    assert rc == 0
    assert "no tool-class rule patterns detected" in captured.out


def test_cli_prints_proposed_settings_fragment_by_default(
    workspace: Path, capsys: pytest.CaptureFixture[str],
) -> None:
    _write_claude_md(workspace, "Never use sudo. Use rg instead of grep.")
    rc = coherence_migrate_rules.main(["--root", str(workspace)])
    captured = capsys.readouterr()
    assert rc == 0
    # The rendered fragment is JSON; parse it to confirm shape.
    json_start = captured.out.index("{")
    json_end = captured.out.rindex("}") + 1
    fragment = json.loads(captured.out[json_start:json_end])
    deny = fragment["permissions"]["deny"]
    assert "Bash(sudo *)" in deny
    assert "Bash(grep:*)" in deny
    # Default mode is flag-only — verify settings.local.json was NOT written.
    assert not (workspace / ".claude" / "settings.local.json").exists()


def test_cli_apply_with_yes_writes_settings_local(
    workspace: Path, capsys: pytest.CaptureFixture[str],
) -> None:
    _write_claude_md(workspace, "Never use sudo. Use rg instead of grep.")
    rc = coherence_migrate_rules.main([
        "--root", str(workspace), "--apply", "--yes",
    ])
    captured = capsys.readouterr()
    assert rc == 0
    settings_path = workspace / ".claude" / "settings.local.json"
    assert settings_path.is_file()
    data = json.loads(settings_path.read_text())
    deny = data["permissions"]["deny"]
    assert "Bash(sudo *)" in deny
    assert "Bash(grep:*)" in deny


def test_cli_apply_merges_with_existing_deny_list(
    workspace: Path, capsys: pytest.CaptureFixture[str],
) -> None:
    """--apply must MERGE, not overwrite — operator-curated deny entries
    persist alongside the new proposals."""
    _write_claude_md(workspace, "Never use sudo.")
    claude_dir = workspace / ".claude"
    claude_dir.mkdir(parents=True)
    (claude_dir / "settings.local.json").write_text(json.dumps({
        "permissions": {"deny": ["Bash(curl evil.example.com:*)"]},
        "allow": ["Bash(rg *)"],  # unrelated key — must survive
    }))
    rc = coherence_migrate_rules.main([
        "--root", str(workspace), "--apply", "--yes",
    ])
    captured = capsys.readouterr()
    assert rc == 0
    data = json.loads((claude_dir / "settings.local.json").read_text())
    deny = data["permissions"]["deny"]
    assert "Bash(curl evil.example.com:*)" in deny  # preserved
    assert "Bash(sudo *)" in deny  # newly appended
    assert data["allow"] == ["Bash(rg *)"]  # unrelated key preserved


def test_cli_apply_refuses_to_overwrite_malformed_settings(
    workspace: Path, capsys: pytest.CaptureFixture[str],
) -> None:
    """If settings.local.json is malformed JSON or has the wrong shape,
    --apply MUST refuse and exit 2 — operator-curated state takes priority
    over the helper's heuristics."""
    _write_claude_md(workspace, "Never use sudo.")
    claude_dir = workspace / ".claude"
    claude_dir.mkdir(parents=True)
    (claude_dir / "settings.local.json").write_text("{ not valid json")
    rc = coherence_migrate_rules.main([
        "--root", str(workspace), "--apply", "--yes",
    ])
    assert rc == 2


def test_cli_no_root_exits_1(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Outside a git repo with no --root flag, exit 1."""
    monkeypatch.chdir(tmp_path)
    rc = coherence_migrate_rules.main([])
    captured = capsys.readouterr()
    assert rc == 1
    assert "not in a git repository" in captured.err
