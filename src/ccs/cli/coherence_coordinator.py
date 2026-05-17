# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""``agent-coherence-coordinator`` — lazy-spawn the workspace coordinator.

Invoked by the plugin's ``bin/ensure-coordinator`` shim from the
``SessionStart`` hook. Cheap to re-invoke: if a coordinator is already
running for the workspace, returns immediately (the unified spawn-or-join
loop in :func:`ensure_coordinator` reads the existing port).

Exit codes:
- 0: coordinator is up and reachable (port printed to stdout)
- 1: not in a git repository (no coordinator root resolved)
- 2: coordinator failed to spawn (platform unsupported, FS read-only, etc.)
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import Sequence

from ccs.adapters.claude_code.lifecycle import ensure_coordinator
from ccs.adapters.claude_code.resolver import find_coordinator_root

logger = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="agent-coherence-coordinator",
        description="Lazy-spawn the Claude Code coherence coordinator for this workspace.",
    )
    parser.add_argument(
        "--root",
        type=Path,
        default=None,
        help="Override the coordinator root (default: walk up from cwd to git root).",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress the 'port=N' line on stdout (still exits 0 on success).",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    root = args.root if args.root is not None else find_coordinator_root()
    if root is None:
        print("agent-coherence-coordinator: not in a git repository", flush=True)
        return 1

    port = ensure_coordinator(Path(root))
    if port == -1:
        print(
            "agent-coherence-coordinator: coordinator could not be spawned "
            "(workspace not writable, or platform not supported)",
            flush=True,
        )
        return 2

    if not args.quiet:
        print(f"port={port}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
