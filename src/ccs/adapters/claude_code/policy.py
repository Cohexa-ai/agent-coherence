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
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, Sequence

import yaml

logger = logging.getLogger(__name__)


STRICT_MODE_PATH_WARN_THRESHOLD: int = 50
"""v0.2 KTD-O footgun guard: if ``strict_mode_paths`` matches more than this
many tracked artifacts, the policy emits a one-shot WARNING via
:meth:`TrackedArtifactPolicy.warn_if_strict_threshold_exceeded`. The caller
(coordinator service after artifact registration / status snapshot) invokes
the check at runtime — the policy module itself cannot enumerate tracked
artifacts at load time because the artifact set lives in SQLite, not on disk
as concrete paths. Guards against an operator typing ``**`` in
``strict_mode.yaml`` and silently locking down the whole workspace."""


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


def _compile_glob_pattern(pattern: str) -> re.Pattern[str] | None:
    """PERF-2 / finding #16: pre-compile a ``**``-containing glob pattern into
    a re.Pattern at construction time. Returns None for patterns without ``**``
    (those use fnmatch at match-time with no string-build overhead)."""
    if "**" not in pattern:
        return None
    parts: list[str] = []
    i = 0
    while i < len(pattern):
        c = pattern[i]
        if c == "*":
            if i + 1 < len(pattern) and pattern[i + 1] == "*":
                parts.append(".*")
                i += 2
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
    return re.compile("^" + "".join(parts) + "$")


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
    strict_mode_paths: tuple[str, ...] = field(default_factory=tuple)
    """v0.2 KTD-O: glob patterns that opt selected tracked artifacts into
    strict mode. An artifact is in strict mode iff it is in the tracked set
    AND matches at least one strict_mode_paths glob. Empty tuple → no
    artifacts in strict mode (back-compatible with v0.1 / v0.1.1, where
    every tracked artifact is warn-mode by default). Loaded from
    ``.coherence/strict_mode.yaml`` (parallel to tracked.yaml /
    ignored.yaml); same path-traversal guard applies."""
    rejected_patterns: tuple[tuple[str, str], ...] = field(default_factory=tuple)
    """Patterns rejected by the path-traversal guard, with reason. Surfaced
    by :meth:`rejected` for status/debug visibility."""

    # PERF-2 / finding #16: pre-compiled regex cache for ``**`` patterns.
    # Built in __post_init__ so the cost is paid exactly once at load time.
    _compiled_patterns: dict[str, re.Pattern[str]] = field(
        default_factory=dict, repr=False, compare=False
    )
    # KTD-O one-shot threshold-warning guard. Reset on every reload() so a
    # post-track policy swap can re-emit the warning if the operator widened
    # the strict-mode glob in a hot-reload path.
    _strict_threshold_warning_emitted: bool = field(
        default=False, repr=False, compare=False
    )

    def __post_init__(self) -> None:
        """Pre-compile all ``**``-containing patterns across all pattern sets."""
        for patterns in (
            self.tracked_patterns,
            self.ignored_patterns,
            self.user_added_patterns,
            self.strict_mode_paths,
        ):
            for p in patterns:
                if p not in self._compiled_patterns:
                    compiled = _compile_glob_pattern(p)
                    if compiled is not None:
                        self._compiled_patterns[p] = compiled

    @classmethod
    def load(cls, coordinator_root: Path | str) -> "TrackedArtifactPolicy":
        """Load policy: defaults + ``.coherence/tracked.yaml`` opt-in +
        ``.coherence/ignored.yaml`` opt-out + ``.coherence/strict_mode.yaml``
        v0.2 opt-in (KTD-O). All YAML files are optional. Patterns failing
        the path-traversal guard are rejected with WARNING."""
        root = Path(coordinator_root).resolve()
        rejected: list[tuple[str, str]] = []

        added = _load_yaml_patterns(root / ".coherence" / "tracked.yaml", rejected)
        ignored = _load_yaml_patterns(root / ".coherence" / "ignored.yaml", rejected)
        strict = _load_yaml_patterns(root / ".coherence" / "strict_mode.yaml", rejected)
        return cls(
            coordinator_root=root,
            tracked_patterns=DEFAULT_TRACKED_PATTERNS,
            ignored_patterns=tuple(ignored),
            user_added_patterns=tuple(added),
            strict_mode_paths=tuple(strict),
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

        # PERF-2 / finding #16: pass pre-compiled cache so _glob_match skips
        # the string-build loop for ``**`` patterns.
        cache = self._compiled_patterns
        tracked = (
            _matches_any(normalized, self.tracked_patterns, cache)
            or _matches_any(normalized, self.user_added_patterns, cache)
        )
        if not tracked:
            return False
        if _matches_any(normalized, self.ignored_patterns, cache):
            return False
        return True

    def is_strict_mode(self, parent_relative_path: str) -> bool:
        """v0.2 KTD-O: return True iff the path is in strict mode.

        Strict-mode contract: a path is in strict mode iff it is in the
        tracked set AND matches at least one strict_mode_paths glob.
        Intersection semantics — never applies to untracked paths even when
        a strict_mode pattern would match them. Empty strict_mode_paths
        short-circuits to False (back-compat with v0.1.1).

        Hooks gate strict-mode behavior (``permissionDecision: "deny"``) on
        this method only; the v0.1.1 warn-mode allow path is preserved for
        every artifact that returns False here."""
        if not self.strict_mode_paths:
            # Fast path — no strict-mode opt-in; v0.1.1 behavior preserved.
            return False
        if not self.is_tracked(parent_relative_path):
            # Intersection: strict mode never applies to untracked paths.
            return False
        normalized = _normalize_relative(parent_relative_path)
        if normalized is None:
            return False
        return _matches_any(normalized, self.strict_mode_paths, self._compiled_patterns)

    def count_strict_mode_matches(self, candidate_paths: Iterable[str]) -> int:
        """Count how many of ``candidate_paths`` are in strict mode.

        Used by the coordinator service (after registry status snapshot) to
        feed :meth:`warn_if_strict_threshold_exceeded` without forcing the
        policy module to know about the registry."""
        return sum(1 for p in candidate_paths if self.is_strict_mode(p))

    def warn_if_strict_threshold_exceeded(
        self,
        candidate_paths: Iterable[str],
        threshold: int = STRICT_MODE_PATH_WARN_THRESHOLD,
    ) -> bool:
        """v0.2 KTD-O footgun guard. One-shot WARNING emitter.

        Returns True if a warning was emitted (count > threshold) on this
        invocation. Subsequent calls return False until policy reload —
        guards against log-spam under repeated /status calls. The caller is
        the coordinator service; it invokes this after enumerating tracked
        artifacts so the count is concrete rather than glob-derived."""
        if self._strict_threshold_warning_emitted:
            return False
        count = self.count_strict_mode_matches(candidate_paths)
        if count > threshold:
            logger.warning(
                "strict_mode_paths matches %d tracked artifacts (> threshold %d). "
                "Verify .coherence/strict_mode.yaml is not over-broad — e.g., "
                "a literal '**' pattern silently opts the entire workspace into "
                "strict mode (permissionDecision: 'deny' on stale reads).",
                count,
                threshold,
            )
            self._strict_threshold_warning_emitted = True
            return True
        return False

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
            "strict_mode_pattern_count": len(self.strict_mode_paths),
            "rejected_pattern_count": len(self.rejected_patterns),
        }


# ----------------------------------------------------------------------
# Internals
# ----------------------------------------------------------------------


def _normalize_relative(p: str) -> str | None:
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


def _matches_any(
    path: str,
    patterns: Iterable[str],
    compiled: dict[str, re.Pattern[str]] | None = None,
) -> bool:
    """Glob-match path against a list of patterns. Uses ``fnmatch`` for
    ``*``/``?`` semantics; ``**`` is treated as zero-or-more path segments.

    PERF-2 / finding #16: ``compiled`` is an optional pre-compiled pattern
    cache (keyed by pattern string). When provided, ``**`` patterns skip the
    string-build loop and use the cached re.Pattern directly."""
    posix_path = path.replace("\\", "/")
    for pattern in patterns:
        if _glob_match(posix_path, pattern, compiled):
            return True
    return False


def _glob_match(
    path: str,
    pattern: str,
    compiled: dict[str, re.Pattern[str]] | None = None,
) -> bool:
    """Match a posix-style path against a glob pattern supporting ``**``.

    PERF-2 / finding #16: when ``compiled`` is provided, ``**`` patterns use
    the pre-compiled re.Pattern directly, skipping the string-build loop."""
    # fnmatch handles ``*`` (any chars in segment) and ``?`` (single char).
    # For ``**`` (any number of path segments), convert to a regex-equivalent.
    if "**" not in pattern:
        return fnmatch.fnmatchcase(path, pattern)
    # Fast path: use the pre-compiled pattern if available.
    if compiled is not None and pattern in compiled:
        return compiled[pattern].match(path) is not None
    # Slow path (called without a cache, e.g. from tests): build on the fly.
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
    regex_str = "^" + "".join(parts) + "$"
    return re.match(regex_str, path) is not None


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


def _validate_pattern(pattern: str) -> str | None:
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
