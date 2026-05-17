# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""``agent-coherence-status`` — print tracked artifacts × sessions × MESI states.

Reads the coordinator's GET /status endpoint and renders a terminal-friendly
table. Backs the ``/agent-coherence status`` slash command.

Exit codes:
- 0: status fetched and printed (including "no coordinator running")
- 1: not in a git repo
- 2: coordinator running but returned an error
"""

from __future__ import annotations

import argparse
import sys
import urllib.error
from pathlib import Path
from typing import Any, Sequence

from ccs.adapters.claude_code.resolver import find_coordinator_root
from ccs.cli._coherence_client import (
    CoordinatorUnavailable,
    get,
    http_status_from_error,
    resolve_endpoint,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="agent-coherence-status",
        description="Show tracked artifacts and per-session MESI states for this workspace.",
    )
    parser.add_argument(
        "--root",
        type=Path,
        default=None,
        help="Override the coordinator root (default: walk up from cwd to git root).",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit the raw JSON response instead of the rendered table.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    root = args.root if args.root is not None else find_coordinator_root()
    if root is None:
        print("agent-coherence-status: not in a git repository", flush=True)
        return 1

    try:
        endpoint = resolve_endpoint(Path(root))
        payload = get(endpoint, "/status")
    except CoordinatorUnavailable as exc:
        print(f"agent-coherence-status: {exc}", flush=True)
        return 0  # graceful — no coordinator is a normal state
    except urllib.error.HTTPError as exc:
        body = http_status_from_error(exc)
        msg = (body or {}).get("error", str(exc))
        print(f"agent-coherence-status: HTTP {exc.code}: {msg}", flush=True)
        return 2

    if args.json:
        import json as _json
        print(_json.dumps(payload, indent=2), flush=True)
        return 0

    _render_table(payload)
    return 0


def _render_table(payload: dict[str, Any]) -> None:
    """Manual column alignment — stdlib only, no rich/tabulate."""
    tracked = payload.get("tracked_artifacts", [])
    sessions = payload.get("sessions", [])
    uptime = payload.get("coordinator_uptime_s", 0.0)
    pid = payload.get("coordinator_pid", 0)

    print(f"Coordinator: pid={pid} uptime={uptime:.0f}s")
    print()

    if not tracked:
        print("No tracked artifacts.")
    else:
        print("Tracked artifacts:")
        path_w = max(len("path"), max(len(a.get("path", "")) for a in tracked))
        print(f"  {'path':<{path_w}}  {'version':>7}")
        print(f"  {'-' * path_w}  {'-' * 7}")
        for a in tracked:
            print(f"  {a.get('path', ''):<{path_w}}  {a.get('version', 0):>7}")
    print()

    if not sessions:
        print("No active sessions.")
        return

    print("Sessions:")
    for s in sessions:
        sid = s.get("agent_id", "?")
        name = s.get("agent_name", "")
        per_artifact = s.get("states", {})
        print(f"  {sid[:8]}  {name}")
        if not per_artifact:
            print("    (no held grants)")
            continue
        path_w = max(len(p) for p in per_artifact)
        for path, state in sorted(per_artifact.items()):
            print(f"    {path:<{path_w}}  {state}")


if __name__ == "__main__":
    raise SystemExit(main())
