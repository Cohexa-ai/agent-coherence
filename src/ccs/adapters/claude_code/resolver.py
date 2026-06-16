# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Resolve the parent repo root from any directory inside it, including
linked worktrees that Claude Code's ``--worktree`` flag creates.

This is the canonical primitive for plan §8. Empirical results (brainstorm
§13.1, verified 2026-05-13 against ``claude`` v2.1.131) show that every
naive "find the project root" signal (``$CLAUDE_PROJECT_DIR``, ``PWD``,
``stdin.cwd``, ``git rev-parse --show-toplevel``) resolves to the WORKTREE
path inside a linked-worktree session, not the parent repo. The plugin
needs the parent repo for the SQLite coordinator location, so naive
approaches put a separate coordinator inside every worktree — defeating
coordination across them.

The git-native fix: ``git rev-parse --git-common-dir`` returns the
absolute path to the parent's ``.git`` directory whenever the caller is
inside a linked worktree. Strip ``/.git`` to get the parent repo root.
For a regular (non-worktree) repo, ``--git-common-dir`` returns ``.git``
(relative), and ``--show-toplevel`` is the right answer.

This is a contract baked into git itself, stable across any future
Claude Code or Agent View internal changes (which is why ``--git-common-dir``
is preferred over walking up looking for ``.claude/worktrees/``).
"""

from __future__ import annotations

import logging
import os
import subprocess
from pathlib import Path

logger = logging.getLogger(__name__)


def find_coordinator_root(start: str | os.PathLike[str] | None = None) -> Path | None:
    """Return the parent repo root from any path inside it.

    Behavior:
    - Regular (non-worktree) git repo: returns the result of
      ``git rev-parse --show-toplevel`` (the working tree root).
    - Linked git worktree (e.g., Claude Code's ``.claude/worktrees/<name>/``):
      returns the parent repo root, derived from
      ``git rev-parse --git-common-dir``.
    - Not inside a git repo at all: returns ``None`` (caller no-ops, e.g.
      hook handlers degrade silently per plan §7 graceful-degradation).
    - ``git`` not on PATH: returns ``None`` with a logged WARNING.

    The function is pure — no caching, no side effects, no I/O beyond the
    git subprocess. Cheap enough (~5–15ms) to call per hook invocation;
    KTD-7 path normalization happens downstream once this returns.

    Args:
        start: Directory to resolve from. Defaults to current working
            directory if None. May be a string or any os.PathLike.

    Returns:
        Absolute Path to the parent repo root, or None if not in a git repo.
    """
    if start is None:
        start = os.getcwd()
    start_path = Path(start).resolve()
    if not start_path.exists():
        return None

    try:
        common = _git(start_path, "rev-parse", "--git-common-dir")
    except (FileNotFoundError, _GitInvocationError):
        return None
    if common is None:
        return None

    # If --git-common-dir returned an absolute path, we're in a linked
    # worktree. The parent repo root is dirname(common_dir).
    common_path = Path(common)
    if common_path.is_absolute():
        # common_path is .../parent-repo/.git — strip the .git to get parent root.
        parent_root = common_path.parent
        # Resolve symlinks (macOS /var → /private/var) for cross-comparison stability.
        return parent_root.resolve()

    # Regular git repo (or main worktree). Fall back to --show-toplevel.
    try:
        toplevel = _git(start_path, "rev-parse", "--show-toplevel")
    except _GitInvocationError:
        return None
    if toplevel is None:
        return None
    return Path(toplevel).resolve()


# ----------------------------------------------------------------------
# Internals
# ----------------------------------------------------------------------


class _GitInvocationError(RuntimeError):
    """Internal: a non-zero git exit that callers should map to None."""


def _git(cwd: Path, *args: str) -> str | None:
    """Run ``git -C <cwd> <args>`` and return stdout stripped, or None on
    a clean non-zero exit (e.g., "not in a git repo"). Raises
    :class:`_GitInvocationError` for unexpected failures (e.g., crash)
    so callers can distinguish "git said no" from "git couldn't run."

    FileNotFoundError (git not on PATH) propagates so the caller can
    log + return None at the boundary.
    """
    try:
        result = subprocess.run(
            ["git", "-C", str(cwd), *args],
            check=False,
            capture_output=True,
            text=True,
            timeout=5.0,
        )
    except subprocess.TimeoutExpired as exc:
        logger.warning("git timed out (%s); treating as not-in-repo", " ".join(args))
        raise _GitInvocationError("git timeout") from exc

    if result.returncode != 0:
        # Clean failure (e.g., "fatal: not a git repository"). Map to None.
        return None
    return result.stdout.strip() or None
