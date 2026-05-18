# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""Tracked-artifact policy — decide whether a given path is coordinated.

Per plan KTD-8: the coordinator watches a narrow set of paths by default
(``CLAUDE.md``, ``AGENTS.md``, anything under ``docs/{specs,plans,brainstorms}/``,
``plan.md|task.md|spec.md`` at any depth). Users can add patterns via
``.coherence/tracked.yaml`` and remove patterns via ``.coherence/ignored.yaml``.

Design constraints:

- **Loaded once at coordinator startup** (per scope-guardian review). Hot-reload
  on mtime was removed as speculative optimization. Pattern changes take effect
  on coordinator restart, which is cheap given idle-shutdown + lazy re-spawn.
- **Path-traversal guard** (per security-lens review): patterns containing
  ``..`` components or starting with ``/`` are rejected with WARNING and skipped.
  A single bad entry shouldn't disable the whole policy.
- **Cross-language friendly defaults**: defaults shouldn't false-positive in
  Node, Rust, Django, or other-language repos. Unit 8 1000-path benchmark covers.
- All policy decisions key on **parent-repo-relative** paths (KTD-7 normalization
  happens upstream in the hook handler).
"""

from __future__ import annotations

import fnmatch
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, Optional, Sequence

import yaml

logger = logging.getLogger(__name__)


DEFAULT_TRACKED_PATTERNS: tuple[str, ...] = (
    # Repo-root coordination files
    "CLAUDE.md",
    "AGENTS.md",
    # Spec/plan/brainstorm directories
    "docs/specs/**/*.md",
    "docs/plans/**/*.md",
    "docs/brainstorms/**/*.md",
    # Conventional coordination filenames at any depth
    "**/plan.md",
    "**/task.md",
    "**/spec.md",
)
"""Patterns the policy module ships with by default. Cross-language safe —
the 1000-path benchmark in Unit 8 verifies 0 false positives across Node,
Rust, Django, and other-ecosystem path samples."""


@dataclass
class TrackedArtifactPolicy:
    """Decide whether a parent-repo-relative path is coordinated.

    Construct via :meth:`load`, never directly — the loader applies the
    path-traversal guard and reads optional YAML overrides.
    """

    coordinator_root: Path
    tracked_patterns: tuple[str, ...] = DEFAULT_TRACKED_PATTERNS
    ignored_patterns: tuple[str, ...] = ()
    user_added_patterns: tuple[str, ...] = field(default_factory=tuple)
    rejected_patterns: tuple[tuple[str, str], ...] = field(default_factory=tuple)
    """Patterns rejected by the path-traversal guard, with reason. Surfaced
    by :meth:`rejected` for status/debug visibility."""

    @classmethod
    def load(cls, coordinator_root: Path | str) -> "TrackedArtifactPolicy":
        """Load policy: defaults + ``.coherence/tracked.yaml`` opt-in +
        ``.coherence/ignored.yaml`` opt-out. YAML files are optional.
        Patterns failing the path-traversal guard are rejected with WARNING."""
        root = Path(coordinator_root).resolve()
        rejected: list[tuple[str, str]] = []

        added = _load_yaml_patterns(root / ".coherence" / "tracked.yaml", rejected)
        ignored = _load_yaml_patterns(root / ".coherence" / "ignored.yaml", rejected)
        return cls(
            coordinator_root=root,
            tracked_patterns=DEFAULT_TRACKED_PATTERNS,
            ignored_patterns=tuple(ignored),
            user_added_patterns=tuple(added),
            rejected_patterns=tuple(rejected),
        )

    def is_tracked(self, parent_relative_path: str) -> bool:
        """Return True if the given parent-repo-relative path is coordinated.

        Algorithm: path is tracked if it matches any default OR user-added
        pattern, AND does not match any ignored pattern. Ignore wins ties.
        """
        normalized = _normalize_relative(parent_relative_path)
        if normalized is None:
            # Absolute path or .. traversal — never tracked.
            return False

        tracked = _matches_any(normalized, self.tracked_patterns) or _matches_any(
            normalized, self.user_added_patterns
        )
        if not tracked:
            return False
        if _matches_any(normalized, self.ignored_patterns):
            return False
        return True

    def rejected(self) -> Sequence[tuple[str, str]]:
        """Return (pattern, reason) pairs rejected by the path-traversal guard."""
        return self.rejected_patterns

    def summary(self) -> dict[str, object]:
        """Compact summary for the ``/status`` endpoint and CLI display."""
        return {
            "coordinator_root": str(self.coordinator_root),
            "default_pattern_count": len(self.tracked_patterns),
            "user_added_pattern_count": len(self.user_added_patterns),
            "ignored_pattern_count": len(self.ignored_patterns),
            "rejected_pattern_count": len(self.rejected_patterns),
        }


# ----------------------------------------------------------------------
# Internals
# ----------------------------------------------------------------------


def _normalize_relative(p: str) -> Optional[str]:
    """Return the path with leading ``./`` stripped, or None if the path
    is absolute or contains ``..`` components (defense-in-depth even
    though the hook handler also normalizes upstream)."""
    if not p:
        return None
    # P1 ce-review fix (kieran-python + correctness convergence): use
    # removeprefix, NOT lstrip. lstrip strips a SET of characters {".", "/"}
    # so ".env" → "env", ".gitignore" → "gitignore", silently making dotfile
    # patterns in tracked.yaml unmatchable. removeprefix strips the literal
    # "./" prefix only (Python 3.9+).
    cleaned = p.removeprefix("./")
    if not cleaned:
        # Pure "." or "./"; not a file path.
        return None
    if p.startswith("/"):
        return None
    if ".." in Path(cleaned).parts:
        return None
    return cleaned


def _matches_any(path: str, patterns: Iterable[str]) -> bool:
    """Glob-match path against a list of patterns. Uses ``fnmatch`` for
    ``*``/``?`` semantics; ``**`` is treated as zero-or-more path segments."""
    posix_path = path.replace("\\", "/")
    for pattern in patterns:
        if _glob_match(posix_path, pattern):
            return True
    return False


def _glob_match(path: str, pattern: str) -> bool:
    """Match a posix-style path against a glob pattern supporting ``**``."""
    # fnmatch handles ``*`` (any chars in segment) and ``?`` (single char).
    # For ``**`` (any number of path segments), convert to a regex-equivalent.
    if "**" not in pattern:
        return fnmatch.fnmatchcase(path, pattern)
    # Translate ``**`` → ``.*``, ``*`` → ``[^/]*``, ``?`` → ``.``, escape rest.
    import re

    parts: list[str] = []
    i = 0
    while i < len(pattern):
        c = pattern[i]
        if c == "*":
            if i + 1 < len(pattern) and pattern[i + 1] == "*":
                parts.append(".*")
                i += 2
                # Skip trailing slash after `**/`
                if i < len(pattern) and pattern[i] == "/":
                    i += 1
            else:
                parts.append("[^/]*")
                i += 1
        elif c == "?":
            parts.append("[^/]")
            i += 1
        else:
            parts.append(re.escape(c))
            i += 1
    regex = "^" + "".join(parts) + "$"
    return re.match(regex, path) is not None


def _load_yaml_patterns(
    yaml_path: Path, rejected: list[tuple[str, str]]
) -> list[str]:
    """Read a YAML file containing a list of pattern strings. Apply the
    path-traversal guard. Returns the surviving patterns; mutates the
    ``rejected`` list with (pattern, reason) for each rejection.

    Missing file → empty list (not an error).
    Malformed YAML → empty list + WARNING (do not crash hooks).
    Non-list top-level → empty list + WARNING.
    """
    if not yaml_path.is_file():
        return []

    try:
        raw = yaml.safe_load(yaml_path.read_text())
    except yaml.YAMLError as exc:
        logger.warning("malformed YAML at %s; falling back to defaults: %s", yaml_path, exc)
        return []
    except OSError as exc:
        logger.warning("could not read %s; falling back to defaults: %s", yaml_path, exc)
        return []

    if raw is None:
        return []
    if not isinstance(raw, list):
        logger.warning(
            "%s top-level must be a list of patterns; got %s. Ignoring.",
            yaml_path,
            type(raw).__name__,
        )
        return []

    surviving: list[str] = []
    for item in raw:
        if not isinstance(item, str):
            rejected.append((str(item), f"non-string pattern ({type(item).__name__})"))
            logger.warning("rejecting non-string pattern in %s: %r", yaml_path, item)
            continue
        reason = _validate_pattern(item)
        if reason is not None:
            rejected.append((item, reason))
            logger.warning("rejecting pattern in %s (%s): %r", yaml_path, reason, item)
            continue
        surviving.append(item)
    return surviving


def _validate_pattern(pattern: str) -> Optional[str]:
    """Path-traversal guard. Returns None if pattern is acceptable, else
    a short reason string."""
    if not pattern:
        return "empty pattern"
    if pattern.startswith("/"):
        return "absolute path"
    # Split on both unix and windows separators to be safe; we only support
    # unix-style patterns in v0.1 but reject windows-style traversal too.
    parts = pattern.replace("\\", "/").split("/")
    if ".." in parts:
        return "contains '..' (path traversal)"
    return None
