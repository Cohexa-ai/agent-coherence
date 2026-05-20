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
    err,
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
    # KTD-J (Unit 8): --detail mirrors the /status three-tier disclosure
    # model. Default 'full' so the local-operator CLI keeps surfacing pid
    # + absolute root + all counters; 'metrics' for dashboard scrapers
    # that want only the counter block; 'minimal' for a redacted view
    # safe to paste in bug reports.
    parser.add_argument(
        "--detail",
        choices=["minimal", "full", "metrics"],
        default="full",
        help=(
            "Disclosure tier (default: full). 'minimal' redacts coordinator_pid "
            "and absolute paths; 'metrics' returns counters only; 'full' is the "
            "operator view used by /agent-coherence status."
        ),
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    root = args.root if args.root is not None else find_coordinator_root()
    if root is None:
        err("agent-coherence-status: not in a git repository")
        return 1

    try:
        endpoint = resolve_endpoint(Path(root))
        # R12 (Unit 6) + KTD-J (Unit 8): the detail tier is selected via
        # --detail. Only 'full' needs the Coherence-Local-Operator opt-in
        # header; the lower tiers degrade by design if the header is
        # missing, but we always set it from this CLI since it's a
        # legitimate local operator.
        payload = get(
            endpoint,
            f"/status?detail={args.detail}",
            extra_headers={"Coherence-Local-Operator": "true"},
        )
    except CoordinatorUnavailable as exc:
        err(f"agent-coherence-status: {exc}")
        return 0  # graceful — no coordinator is a normal state
    except urllib.error.HTTPError as exc:
        body = http_status_from_error(exc)
        msg = (body or {}).get("error", str(exc))
        err(f"agent-coherence-status: HTTP {exc.code}: {msg}")
        return 2

    if args.json:
        import json as _json
        print(_json.dumps(payload, indent=2), flush=True)
        return 0

    if args.detail == "metrics":
        _render_metrics(payload)
    else:
        _render_table(payload)
    return 0


def _render_table(payload: dict[str, Any]) -> None:
    """Manual column alignment — stdlib only, no rich/tabulate."""
    tracked = payload.get("tracked_artifacts", [])
    sessions = payload.get("sessions", [])
    policy = payload.get("policy_summary", {})
    uptime = payload.get("coordinator_uptime_s", 0.0)
    pid = payload.get("coordinator_pid", 0)
    backend = payload.get("coordinator_backend", "python")
    version = payload.get("coordinator_version", "")

    header_bits: list[str] = []
    if pid:
        header_bits.append(f"pid={pid}")
    header_bits.append(f"uptime={uptime:.0f}s")
    header_bits.append(f"backend={backend}")
    if version:
        header_bits.append(f"version={version}")
    print("Coordinator: " + " ".join(header_bits))
    print()

    # Policy section first — distinguishes "what's eligible to be tracked"
    # (defaults + user-added patterns) from "what's been observed so far"
    # (tracked_artifacts, which requires at least one Read to seed).
    if policy:
        default_n = policy.get("default_pattern_count", 0)
        user_n = policy.get("user_added_pattern_count", 0)
        ignored_n = policy.get("ignored_pattern_count", 0)
        print(
            "Policy: "
            f"{default_n} default pattern(s), "
            f"{user_n} user-added, "
            f"{ignored_n} ignored"
        )
        print()

    if not tracked:
        # Disambiguate: empty registry vs empty policy. Policy may match
        # paths the registry hasn't observed yet (first Read seeds them).
        if policy and (policy.get("default_pattern_count", 0) + policy.get("user_added_pattern_count", 0)) > 0:
            print(
                "No artifacts observed yet (paths matching the policy will "
                "be registered on first Read)."
            )
        else:
            print("No tracked artifacts (policy is empty).")
    else:
        print("Observed artifacts:")
        path_w = max(len("path"), max(len(a.get("path", "")) for a in tracked))
        print(f"  {'path':<{path_w}}  {'version':>7}")
        print(f"  {'-' * path_w}  {'-' * 7}")
        for a in tracked:
            print(f"  {a.get('path', ''):<{path_w}}  {a.get('version', 0):>7}")
    print()

    if not sessions:
        print("No active sessions.")
    else:
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

    # KTD-J (Unit 8): counters section. Only printed when the payload
    # actually carries counter data — the minimal tier strips them.
    _render_counter_block(payload)


def _render_counter_block(payload: dict[str, Any]) -> None:
    """KTD-J counter block, printed after the artifacts/sessions section
    of the full-tier table. No-op if the payload doesn't carry counters
    (e.g., minimal tier responses)."""
    endpoint_counters = payload.get("endpoint_counters") or {}
    has_endpoint_counters = any(v for v in endpoint_counters.values())
    keys_present = [
        k for k in (
            "intra_task_acquire_release_total",
            "stale_warning_emitted_total",
            "stale_warning_reread_total",
            "watchdog_timeouts_total",
            "watchdog_queue_overflows_total",
            "handler_concurrency_overflows_total",
            "cold_start_duration_ms",
        ) if k in payload
    ]
    if not has_endpoint_counters and not keys_present:
        return

    print()
    print("Counters:")
    if endpoint_counters:
        # Stable order so operator-facing output is diff-friendly.
        for name in (
            "pre_read_total",
            "pre_edit_total",
            "post_edit_total",
            "session_stop_total",
            "pre_bash_total",
            "pre_grep_total",
            "policy_track_total",
            "policy_untrack_total",
            "status_total",
        ):
            value = endpoint_counters.get(name, 0)
            print(f"  {name:<40}  {value}")
    for name in keys_present:
        value = payload.get(name, 0)
        if isinstance(value, float):
            value_str = f"{value:.1f}"
        else:
            value_str = str(value)
        print(f"  {name:<40}  {value_str}")


def _render_metrics(payload: dict[str, Any]) -> None:
    """KTD-J `--detail metrics` rendering — counter block only, no
    artifact/session detail. Used by dashboard scrapers that want a
    consistent counter format without parsing JSON."""
    backend = payload.get("coordinator_backend", "python")
    version = payload.get("coordinator_version", "")
    print(f"Coordinator metrics: backend={backend} version={version}")
    _render_counter_block(payload)


if __name__ == "__main__":
    raise SystemExit(main())
