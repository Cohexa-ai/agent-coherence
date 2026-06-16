# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""``agent-coherence-track`` — add one or more paths to the tracked set.

Validates each path (relative, no traversal), then calls the coordinator's
POST /policy/track endpoint which both appends to ``tracked.yaml`` and
reloads the live policy. Idempotent.

Exit codes:
- 0: all paths accepted (or partially accepted with warnings)
- 1: not in a git repo / all paths rejected by validation
- 2: coordinator unreachable / HTTP error
"""

from __future__ import annotations

import argparse
import urllib.error
from pathlib import Path
from typing import Sequence

from ccs.adapters.claude_code.resolver import find_coordinator_root
from ccs.cli._coherence_client import (
    CoordinatorUnavailable,
    err,
    http_status_from_error,
    normalize_workspace_path,
    post,
    resolve_endpoint,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="agent-coherence-track",
        description="Add one or more paths to the coordinator's tracked set.",
    )
    parser.add_argument(
        "paths",
        nargs="+",
        help=(
            "One or more paths to track. Accepts workspace-relative paths "
            "(e.g. 'docs/plan.md') OR absolute paths inside the workspace "
            "root (auto-normalized to workspace-relative before send). "
            "Absolute paths outside the workspace are rejected. No '..' "
            "traversal."
        ),
    )
    parser.add_argument(
        "--root",
        type=Path,
        default=None,
        help="Override the coordinator root (default: walk up from cwd to git root).",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    root = args.root if args.root is not None else find_coordinator_root()
    if root is None:
        err("agent-coherence-track: not in a git repository")
        return 1

    # Local pre-validation + normalization so we can fail fast without a
    # network round-trip. normalize_workspace_path accepts both relative
    # paths (e.g. "docs/plan.md") and absolute paths inside the workspace
    # root (e.g. "/Users/x/repo/docs/plan.md") — the latter auto-strips to
    # workspace-relative before send. Absolute paths outside root are
    # rejected. This matches the operator UX expectation when the path is
    # passed verbatim from the /agent-coherence:track skill template.
    invalid: list[tuple[str, str]] = []
    valid: list[str] = []
    for p in args.paths:
        normalized, reason = normalize_workspace_path(p, Path(root))
        if reason is not None:
            invalid.append((p, reason))  # original input in error for clarity
        else:
            valid.append(normalized)  # normalized form to coordinator

    if not valid:
        for p, reason in invalid:
            err(f"agent-coherence-track: rejected {p!r}: {reason}")
        return 1

    try:
        endpoint = resolve_endpoint(Path(root))
        payload = post(endpoint, "/policy/track", {"paths": valid})
    except CoordinatorUnavailable as exc:
        err(f"agent-coherence-track: {exc}")
        return 2
    except urllib.error.HTTPError as exc:
        body = http_status_from_error(exc)
        msg = (body or {}).get("error", str(exc))
        err(f"agent-coherence-track: HTTP {exc.code}: {msg}")
        return 2

    added: list[str] = payload.get("added", [])
    rejected: list[dict] = payload.get("rejected", [])
    for p in added:
        # Success → stdout (machine-parseable by callers). Warn-on-stderr
        # if the path doesn't exist on disk yet (operationally fine, but
        # worth surfacing as diagnostic info).
        disk_path = Path(root) / p
        if not disk_path.exists():
            print(f"agent-coherence-track: tracked {p}", flush=True)
            err(f"agent-coherence-track: warning: {p} does not exist on disk yet")
        else:
            print(f"agent-coherence-track: tracked {p}", flush=True)
    for entry in rejected:
        err(
            f"agent-coherence-track: rejected {entry.get('path', '')}: "
            f"{entry.get('reason', '')}"
        )
    for p, reason in invalid:
        err(f"agent-coherence-track: rejected {p!r}: {reason}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
