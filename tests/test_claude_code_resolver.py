# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""Tests for find_coordinator_root (plan Unit 2).

Verifies that the resolver correctly identifies the parent repo root from:
- A regular git checkout
- A linked git worktree (the failure mode that motivated KTD-8/the resolver)
- Outside a git repo (graceful no-op)

The linked-worktree case is the load-bearing test — empirical results in
brainstorm §13.1 showed all naive signals resolve to the worktree, not the
parent. This test re-creates that scenario locally via ``git worktree add``.
"""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest

from ccs.adapters.claude_code.resolver import find_coordinator_root


def _run(*args: str, cwd: Path | None = None) -> subprocess.CompletedProcess:
    return subprocess.run(args, cwd=cwd, check=True, capture_output=True, text=True)


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    """A fresh git repo with one initial commit."""
    _run("git", "init", "-b", "main", "-q", cwd=tmp_path)
    _run("git", "config", "user.email", "t@t", cwd=tmp_path)
    _run("git", "config", "user.name", "t", cwd=tmp_path)
    (tmp_path / "README.md").write_text("hello\n")
    _run("git", "add", "README.md", cwd=tmp_path)
    _run("git", "commit", "-qm", "init", cwd=tmp_path)
    return tmp_path.resolve()


# --------------------------------------------------------------------
# Regular repo case
# --------------------------------------------------------------------


def test_regular_repo_returns_toplevel(repo: Path) -> None:
    """In a regular checkout, resolver returns the working tree root."""
    result = find_coordinator_root(repo)
    assert result == repo


def test_subdirectory_returns_parent_root(repo: Path) -> None:
    """Called from a subdirectory, resolver returns the repo root."""
    sub = repo / "src" / "deep" / "nested"
    sub.mkdir(parents=True)
    result = find_coordinator_root(sub)
    assert result == repo


# --------------------------------------------------------------------
# Linked-worktree case (KTD §8 motivating scenario)
# --------------------------------------------------------------------


def test_linked_worktree_returns_PARENT_root_not_worktree(repo: Path, tmp_path: Path) -> None:
    """The load-bearing test: from inside a linked worktree, the resolver
    must return the PARENT repo root, NOT the worktree path. This is the
    failure mode of every naive signal per brainstorm §13.1."""
    worktree = tmp_path / "wt-test"
    _run("git", "worktree", "add", "-b", "feat-test", str(worktree), cwd=repo)

    # Sanity: the worktree exists and is its own working tree
    naive_toplevel = subprocess.check_output(
        ["git", "-C", str(worktree), "rev-parse", "--show-toplevel"], text=True
    ).strip()
    assert Path(naive_toplevel).resolve() == worktree.resolve()  # naive lies

    # The resolver returns the PARENT, not the worktree
    result = find_coordinator_root(worktree)
    assert result == repo, (
        f"resolver returned worktree path {result} instead of parent repo {repo}; "
        "this is the failure mode the §8 resolver exists to prevent"
    )


def test_linked_worktree_subdirectory(repo: Path, tmp_path: Path) -> None:
    """From a subdirectory inside a worktree, still returns parent root."""
    worktree = tmp_path / "wt-nested"
    _run("git", "worktree", "add", "-b", "feat-nested", str(worktree), cwd=repo)
    sub = worktree / "deep" / "path"
    sub.mkdir(parents=True)
    result = find_coordinator_root(sub)
    assert result == repo


# --------------------------------------------------------------------
# Edge cases
# --------------------------------------------------------------------


def test_outside_git_repo_returns_none(tmp_path: Path) -> None:
    """A tmpdir that isn't a git repo at all — resolver returns None."""
    not_a_repo = tmp_path / "scratch"
    not_a_repo.mkdir()
    assert find_coordinator_root(not_a_repo) is None


def test_nonexistent_path_returns_none(tmp_path: Path) -> None:
    """A path that doesn't exist on disk — resolver returns None."""
    assert find_coordinator_root(tmp_path / "does-not-exist") is None


def test_default_start_uses_cwd(repo: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """When called with no argument, resolver uses os.getcwd()."""
    monkeypatch.chdir(repo)
    assert find_coordinator_root() == repo


def test_path_object_accepted(repo: Path) -> None:
    """Path objects are valid input (not just strings)."""
    assert find_coordinator_root(Path(repo)) == repo


def test_string_accepted(repo: Path) -> None:
    """Strings are valid input (not just Path objects)."""
    assert find_coordinator_root(str(repo)) == repo


def test_git_not_on_path_returns_none(repo: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """If ``git`` is not on PATH at all, resolver returns None gracefully."""
    monkeypatch.setenv("PATH", "/nonexistent-dir-only")
    assert find_coordinator_root(repo) is None
