# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""``agent-coherence-migrate-deny`` — propose ``permissions.deny`` JSON to
stdout from prose rules in CLAUDE.md / AGENTS.md (v0.2 plan Unit 3, KTD-R).

Distinct from v0.1.1's ``agent-coherence-migrate-rules``:

- **NEVER writes to settings.json.** Output to stdout only. The operator
  reviews and pastes into ``.claude/settings.local.json`` themselves.
- **Symlink-contained.** Resolves CLAUDE.md / AGENTS.md / workspace root
  to canonical paths via ``os.path.realpath`` and refuses with non-zero
  exit if any canonical path escapes the resolved workspace root. Blocks
  the symlink-out-of-workspace attack vector documented in KTD-R: a
  malicious or compromised upstream supplying ``CLAUDE.md -> /etc/passwd``
  (or sibling-workspace CLAUDE.md) could yield attacker-crafted
  ``permissions.deny`` entries that the operator pastes blindly.
- **NEVER invokes an LLM.** Deterministic pattern-match only.
- **NEVER scans .env or other private files.** Only the explicitly-named
  CLAUDE.md / AGENTS.md (or operator overrides via flags).

KTD-R locked invariants (security review, 2026-05-21):
- helper NEVER writes to settings.json (filesystem snapshot tested)
- helper NEVER asks Claude / an LLM
- helper NEVER reads files whose canonical path escapes the resolved
  workspace root, regardless of how they were referenced (symlink, hard
  link, relative ``..`` traversal in ``--claude-md`` override).
- helper NEVER makes outbound HTTP requests

Exit codes:
- 0: completed (proposals printed, or empty array, or no CLAUDE.md found)
- 1: workspace root could not be resolved (missing .coherence/ AND .git)
- 2: symlink-containment violation OR malformed UTF-8 in input file
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

# ----------------------------------------------------------------------
# Detection rules — deterministic, regex-only, under-emit bias per KTD-R
# ----------------------------------------------------------------------


@dataclass(frozen=True)
class _DenyRule:
    """One detection rule. Matches one or more regex against the input
    text and produces ``Bash(...)`` deny entries on hit."""

    name: str
    triggers: tuple[re.Pattern[str], ...]
    deny_entries: tuple[str, ...]


def _re(pattern: str) -> re.Pattern[str]:
    return re.compile(pattern, re.IGNORECASE | re.MULTILINE)


# Under-emit bias: only match canonical Claude Code restriction phrasings.
# Ambiguous wording ("prefer rg over grep when possible") deliberately
# NOT matched — a false positive here ends up in an operator-readable
# JSON block, but the operator should not have to filter noise. The
# Unit 3 plan test scenarios assert ambiguous phrasings are NOT matched.
_RULES: tuple[_DenyRule, ...] = (
    _DenyRule(
        name="grep_to_rg",
        triggers=(
            _re(r"use\s+`?rg`?\s+instead\s+of\s+`?grep`?"),
            _re(r"use\s+`?rg`?,\s*not\s+`?grep`?"),
            _re(r"use\s+`?rg`?\s+over\s+`?grep`?"),
        ),
        deny_entries=("Bash(grep:*)",),
    ),
    _DenyRule(
        name="never_python_dash_c",
        triggers=(
            _re(r"never\s+use\s+`?python3?\s*-c`?"),
            _re(r"`?python3?\s*-c`?\s+is\s+forbidden"),
            _re(r"don'?t\s+use\s+`?python3?\s*-c`?"),
        ),
        deny_entries=("Bash(python -c:*)", "Bash(python3 -c:*)"),
    ),
    _DenyRule(
        name="never_perl_dash_e",
        triggers=(
            _re(r"never\s+use\s+`?perl\s*-e`?"),
            _re(r"`?perl\s*-e`?\s+is\s+forbidden"),
            _re(r"don'?t\s+use\s+`?perl\s*-e`?"),
        ),
        deny_entries=("Bash(perl -e:*)",),
    ),
    _DenyRule(
        name="never_ruby_dash_e",
        triggers=(
            _re(r"never\s+use\s+`?ruby\s*-e`?"),
            _re(r"`?ruby\s*-e`?\s+is\s+forbidden"),
            _re(r"don'?t\s+use\s+`?ruby\s*-e`?"),
        ),
        deny_entries=("Bash(ruby -e:*)",),
    ),
    _DenyRule(
        name="never_node_dash_e",
        triggers=(
            _re(r"never\s+use\s+`?node\s*-e`?"),
            _re(r"`?node\s*-e`?\s+is\s+forbidden"),
            _re(r"don'?t\s+use\s+`?node\s*-e`?"),
        ),
        deny_entries=("Bash(node -e:*)",),
    ),
    _DenyRule(
        name="never_sudo",
        triggers=(
            _re(r"never\s+(use\s+)?sudo\b"),
            _re(r"sudo\s+is\s+forbidden"),
            _re(r"don'?t\s+(use\s+)?sudo\b"),
            _re(r"avoid\s+`?sudo`?\b"),
        ),
        deny_entries=("Bash(sudo:*)",),
    ),
)


# ----------------------------------------------------------------------
# Path resolution + symlink-containment guard (KTD-R)
# ----------------------------------------------------------------------


def _find_workspace_root(start: Path) -> Path | None:
    """Walk up from ``start`` looking for a dir containing ``.coherence/``
    or ``.git/`` (canonical workspace markers). Returns None if neither
    found before reaching the filesystem root.

    NOT the same as ``find_coordinator_root`` (which assumes git is the
    authority); the v0.2 helper accepts either marker so it works in
    coordinator-only checkouts AND git checkouts."""
    current = start.resolve()
    while True:
        if (current / ".coherence").is_dir() or (current / ".git").is_dir():
            return current
        if current == current.parent:
            return None
        current = current.parent


def _is_descendant_of(candidate: Path, root: Path) -> bool:
    """Canonical-path containment check. Both paths are resolved (symlinks
    followed) before comparison. KTD-R: this is the single point that
    decides whether a CLAUDE.md / AGENTS.md path is in-bounds."""
    try:
        candidate_real = candidate.resolve(strict=False)
        root_real = root.resolve(strict=False)
    except OSError:
        return False
    try:
        candidate_real.relative_to(root_real)
        return True
    except ValueError:
        return False


def _resolve_or_reject(
    label: str,
    candidate: Path,
    workspace_real: Path,
) -> Path | None:
    """Resolve ``candidate`` to canonical and verify it descends from
    ``workspace_real``. Returns the resolved Path on success, None on
    rejection (with diagnostic to stderr)."""
    try:
        resolved = candidate.resolve(strict=False)
    except OSError as exc:
        print(
            f"agent-coherence-migrate-deny: cannot resolve {label} "
            f"({candidate}): {exc}",
            file=sys.stderr,
        )
        return None
    if not _is_descendant_of(resolved, workspace_real):
        print(
            f"agent-coherence-migrate-deny: {label} canonical path "
            f"({resolved}) is NOT a descendant of resolved workspace root "
            f"({workspace_real}). Refusing — KTD-R symlink containment "
            f"blocks reading files outside the workspace boundary.",
            file=sys.stderr,
        )
        return None
    return resolved


# ----------------------------------------------------------------------
# Detection + emission
# ----------------------------------------------------------------------


@dataclass(frozen=True)
class _Detection:
    rule_name: str
    deny_entries: tuple[str, ...]
    source_file: str


def detect_deny_entries(text: str, source_label: str) -> list[_Detection]:
    """Apply all _RULES to ``text``. Returns list of detections.

    Public for testing — call with the resolved file contents."""
    out: list[_Detection] = []
    for rule in _RULES:
        if any(trig.search(text) for trig in rule.triggers):
            out.append(_Detection(
                rule_name=rule.name,
                deny_entries=rule.deny_entries,
                source_file=source_label,
            ))
    return out


def _emit_json(deny_entries: list[str]) -> str:
    """Render the paste-ready ``{"permissions": {"deny": [...]}}`` block."""
    return json.dumps(
        {"permissions": {"deny": deny_entries}},
        indent=2,
        ensure_ascii=False,
    )


# ----------------------------------------------------------------------
# CLI
# ----------------------------------------------------------------------


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="agent-coherence-migrate-deny",
        description=(
            "Propose permissions.deny JSON from prose rules in CLAUDE.md "
            "/ AGENTS.md. Output to stdout only — NEVER writes to "
            "settings.json. Symlink-contained: refuses to read files "
            "outside the resolved workspace root (KTD-R)."
        ),
    )
    p.add_argument(
        "--workspace",
        type=Path,
        default=None,
        help=(
            "Workspace root override (default: walk up from cwd looking "
            "for .coherence/ or .git/). Rejected if not a directory or "
            "missing both markers."
        ),
    )
    p.add_argument(
        "--claude-md",
        type=Path,
        default=None,
        help=(
            "Override the CLAUDE.md path (default: <workspace>/CLAUDE.md). "
            "Canonical path must descend from workspace root or REJECTED."
        ),
    )
    p.add_argument(
        "--agents-md",
        type=Path,
        default=None,
        help=(
            "Override the AGENTS.md path (default: <workspace>/AGENTS.md). "
            "Canonical path must descend from workspace root or REJECTED."
        ),
    )
    return p


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)

    # Step 1 — resolve workspace root (canonical).
    if args.workspace is not None:
        ws = args.workspace
        if not ws.is_dir():
            print(
                f"agent-coherence-migrate-deny: --workspace {ws} is not a "
                f"directory.",
                file=sys.stderr,
            )
            return 1
        ws_real = ws.resolve()
        if not (ws_real / ".coherence").is_dir() and not (ws_real / ".git").is_dir():
            print(
                f"agent-coherence-migrate-deny: --workspace {ws_real} has "
                f"neither .coherence/ nor .git/ — refusing (this guard "
                f"prevents scanning arbitrary directories like /tmp).",
                file=sys.stderr,
            )
            return 1
    else:
        found = _find_workspace_root(Path.cwd())
        if found is None:
            print(
                "agent-coherence-migrate-deny: cwd is not inside a workspace "
                "with .coherence/ or .git/. Pass --workspace explicitly.",
                file=sys.stderr,
            )
            return 1
        ws_real = found

    # Step 2 — resolve CLAUDE.md (default + canonical containment check).
    claude_candidate = args.claude_md if args.claude_md is not None else (ws_real / "CLAUDE.md")
    claude_resolved = _resolve_or_reject("CLAUDE.md", claude_candidate, ws_real)
    if claude_resolved is None and args.claude_md is not None:
        # Operator-supplied override that fails containment: hard reject.
        return 2
    # Default path that doesn't exist is NOT a containment failure — it
    # just means there's no CLAUDE.md to scan.
    if claude_resolved is None:
        claude_resolved = claude_candidate

    # Step 3 — resolve AGENTS.md (same shape).
    agents_candidate = args.agents_md if args.agents_md is not None else (ws_real / "AGENTS.md")
    agents_resolved = _resolve_or_reject("AGENTS.md", agents_candidate, ws_real)
    if agents_resolved is None and args.agents_md is not None:
        return 2
    if agents_resolved is None:
        agents_resolved = agents_candidate

    # Step 4 — read whatever exists. Missing files → empty contribution.
    detections: list[_Detection] = []
    for label, path in (("CLAUDE.md", claude_resolved), ("AGENTS.md", agents_resolved)):
        if not path.is_file():
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError as exc:
            print(
                f"agent-coherence-migrate-deny: {label} at {path} is not "
                f"valid UTF-8: {exc}. Refusing — repair encoding before "
                f"running the helper.",
                file=sys.stderr,
            )
            return 2
        except OSError as exc:
            print(
                f"agent-coherence-migrate-deny: cannot read {label} at "
                f"{path}: {exc}",
                file=sys.stderr,
            )
            continue
        detections.extend(detect_deny_entries(text, label))

    # Step 5 — deduplicate while preserving first-seen order. The operator
    # gets a deterministic block they can diff across runs.
    seen: set[str] = set()
    deny_entries: list[str] = []
    for det in detections:
        for entry in det.deny_entries:
            if entry not in seen:
                seen.add(entry)
                deny_entries.append(entry)

    if not detections:
        if not claude_resolved.is_file() and not agents_resolved.is_file():
            print(
                f"agent-coherence-migrate-deny: no CLAUDE.md or AGENTS.md "
                f"found in {ws_real}. Emitting empty deny block.",
                file=sys.stderr,
            )

    # Step 6 — emit the JSON block.
    print(_emit_json(deny_entries))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
