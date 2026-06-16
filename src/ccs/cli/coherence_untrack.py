# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""``agent-coherence-untrack`` — append paths to the workspace ignored set.

Symmetric to :mod:`coherence_track`, but writes to ``ignored.yaml`` via
the coordinator's POST /policy/untrack endpoint. Does NOT delete existing
artifact rows from SQLite (preserves audit trail); future reads simply
suppress warnings because the path is excluded.

Exit codes:
- 0: all paths accepted
- 1: not in a git repo / all paths rejected by local validation
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
        prog="agent-coherence-untrack",
        description="Append one or more paths to the coordinator's ignored set.",
    )
    parser.add_argument(
        "paths",
        nargs="+",
        help=(
            "One or more paths to ignore. Accepts workspace-relative paths "
            "(e.g. 'docs/draft.md') OR absolute paths inside the workspace "
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
        err("agent-coherence-untrack: not in a git repository")
        return 1

    # See coherence_track.py for rationale: normalize_workspace_path lets
    # the operator pass absolute paths inside the workspace and the CLI
    # auto-strips to workspace-relative before send. Matches the
    # /agent-coherence:untrack skill template UX (which substitutes
    # $ARGUMENTS verbatim).
    invalid: list[tuple[str, str]] = []
    valid: list[str] = []
    for p in args.paths:
        normalized, reason = normalize_workspace_path(p, Path(root))
        if reason is not None:
            invalid.append((p, reason))
        else:
            valid.append(normalized)

    if not valid:
        for p, reason in invalid:
            err(f"agent-coherence-untrack: rejected {p!r}: {reason}")
        return 1

    try:
        endpoint = resolve_endpoint(Path(root))
        payload = post(endpoint, "/policy/untrack", {"paths": valid})
    except CoordinatorUnavailable as exc:
        err(f"agent-coherence-untrack: {exc}")
        return 2
    except urllib.error.HTTPError as exc:
        body = http_status_from_error(exc)
        msg = (body or {}).get("error", str(exc))
        err(f"agent-coherence-untrack: HTTP {exc.code}: {msg}")
        return 2

    removed: list[str] = payload.get("removed", [])
    for p in removed:
        # Success → stdout (machine-parseable by callers).
        print(f"agent-coherence-untrack: untracked {p}", flush=True)
    for p, reason in invalid:
        err(f"agent-coherence-untrack: rejected {p!r}: {reason}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
