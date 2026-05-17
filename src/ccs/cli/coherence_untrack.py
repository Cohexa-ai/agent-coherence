# Copyright (c) 2026 Arbiter contributors.
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
    http_status_from_error,
    post,
    resolve_endpoint,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="agent-coherence-untrack",
        description="Append one or more workspace-relative paths to the coordinator's ignored set.",
    )
    parser.add_argument(
        "paths",
        nargs="+",
        help="One or more workspace-relative paths to ignore (no leading '/' or '..').",
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
        print("agent-coherence-untrack: not in a git repository", flush=True)
        return 1

    invalid: list[tuple[str, str]] = []
    valid: list[str] = []
    for p in args.paths:
        reason = _validate_path(p)
        if reason is not None:
            invalid.append((p, reason))
        else:
            valid.append(p)

    if not valid:
        for p, reason in invalid:
            print(f"agent-coherence-untrack: rejected {p!r}: {reason}", flush=True)
        return 1

    try:
        endpoint = resolve_endpoint(Path(root))
        payload = post(endpoint, "/policy/untrack", {"paths": valid})
    except CoordinatorUnavailable as exc:
        print(f"agent-coherence-untrack: {exc}", flush=True)
        return 2
    except urllib.error.HTTPError as exc:
        body = http_status_from_error(exc)
        msg = (body or {}).get("error", str(exc))
        print(f"agent-coherence-untrack: HTTP {exc.code}: {msg}", flush=True)
        return 2

    removed: list[str] = payload.get("removed", [])
    for p in removed:
        print(f"agent-coherence-untrack: untracked {p}", flush=True)
    for p, reason in invalid:
        print(f"agent-coherence-untrack: rejected {p!r}: {reason}", flush=True)

    return 0


def _validate_path(p: str) -> str | None:
    if not p:
        return "empty"
    if p.startswith("/"):
        return "path must be relative (no leading '/')"
    if ".." in Path(p).parts:
        return "path must not contain '..' traversal"
    return None


if __name__ == "__main__":
    raise SystemExit(main())
