# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""``agent-coherence-status`` — print tracked artifacts × sessions × MESI states.

Reads the coordinator's GET /status endpoint and renders a terminal-friendly
table. Backs the ``/agent-coherence status`` slash command.

Exit codes:
- 0: status fetched and printed (including "no coordinator running")
- 1: not in a git repo
- 2: coordinator running but returned an error
- 3: --self-test exercised but the smoke scenario failed

KTD-J (Unit 8): ``--self-test`` runs an end-to-end smoke against a real
coordinator. Two synthetic sessions exercise the stale-read warning
path; the smoke fails if (a) the coordinator is unreachable, (b) the
stale-warning response shape is wrong, or (c) counters do not increment
in the expected pattern. README's post-install step points operators at
this command so silent install regressions are caught locally before
they reach a real agent session.
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
    # KTD-J (Unit 8): post-install smoke. Drives a two-session stale-read
    # scenario against a real coordinator; exits non-zero if the smoke
    # detects a regression. README's "After install:" step calls this.
    parser.add_argument(
        "--self-test",
        action="store_true",
        help=(
            "Run an end-to-end smoke against a live coordinator. "
            "Validates pre-read/pre-edit/post-edit chain, stale-warning "
            "emission, and counter increments. Exits 0 on success, 3 on "
            "failure with an actionable diagnostic on stderr."
        ),
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    root = args.root if args.root is not None else find_coordinator_root()
    if root is None:
        err("agent-coherence-status: not in a git repository")
        return 1

    if args.self_test:
        return _run_self_test(Path(root), json_mode=args.json)

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


def _run_self_test(root: Path, *, json_mode: bool = False) -> int:
    """KTD-J (Unit 8): end-to-end smoke against a live coordinator.

    Two synthetic sessions A and B drive the stale-read warning path:

    1. A pre-reads ``plan.md`` (tracked by default policy) — fresh, seeds v1.
    2. B pre-edits ``plan.md`` — acquires EXCLUSIVE.
    3. B post-edits — commits v2, releases EXCLUSIVE.
    4. A pre-reads ``plan.md`` again — stale warning fires.
    5. Verify ``status: stale`` + ``hookSpecificOutput.additionalContext``
       prose contains the path.
    6. Verify ``stale_warning_emitted_total`` incremented in ``/status``.

    Returns 0 on success, 3 on any verification failure. Diagnostics
    print to stderr so the README's post-install step ("run
    ``agent-coherence-status --self-test``") gives the operator an
    actionable error rather than a stack trace.
    """
    from ccs.cli._coherence_client import post as _post
    import uuid as _uuid

    try:
        endpoint = resolve_endpoint(root)
    except CoordinatorUnavailable as exc:
        err(
            f"agent-coherence-status --self-test: coordinator unreachable "
            f"({exc}). Spawn one first by running any hook (or "
            f"``agent-coherence-coordinator``)."
        )
        return 3

    # Deterministic-but-distinct UUIDs so re-runs against the same
    # workspace produce predictable agent-name surfaces.
    ns = _uuid.UUID("11111111-2222-4333-8444-555555555555")
    sid_a = str(_uuid.uuid5(ns, "self-test-A"))
    sid_b = str(_uuid.uuid5(ns, "self-test-B"))
    path = "plan.md"  # part of DEFAULT_TRACKED_PATTERNS

    def _step(name: str, body: dict[str, Any]) -> dict[str, Any] | None:
        try:
            return _post(endpoint, name, body)
        except urllib.error.HTTPError as exc:
            err(f"--self-test: {name} returned HTTP {exc.code}")
            return None

    # Step 1 — A's first read seeds the artifact.
    r1 = _step("/hooks/pre-read", {
        "session_id": sid_a, "path": path,
        "content_hash": "a" * 64,
    })
    if r1 is None or r1.get("status") != "fresh":
        err(f"--self-test: expected fresh on first pre-read, got {r1!r}")
        return 3

    # Step 2 — B pre-edits.
    r2 = _step("/hooks/pre-edit", {"session_id": sid_b, "path": path})
    if r2 is None or not r2.get("ok", True):
        err(f"--self-test: pre-edit failed: {r2!r}")
        return 3

    # Step 3 — B commits.
    r3 = _step("/hooks/post-edit", {
        "session_id": sid_b, "path": path,
        "content_hash": "b" * 64, "success": True,
    })
    if r3 is None or not r3.get("ok"):
        err(f"--self-test: post-edit failed: {r3!r}")
        return 3

    # Step 4 — A re-reads → expect stale.
    r4 = _step("/hooks/pre-read", {"session_id": sid_a, "path": path})
    if r4 is None:
        return 3
    if r4.get("status") != "stale":
        err(
            f"--self-test: expected stale warning on A's re-read after B's "
            f"commit, got {r4.get('status')!r} (full response: {r4!r}). "
            f"This usually means the hooks aren't wired or the coordinator "
            f"is running a stale build."
        )
        return 3
    out = r4.get("hookSpecificOutput") or {}
    ctx = out.get("additionalContext", "")
    if path not in ctx:
        err(
            f"--self-test: stale-warning prose did not mention {path}: "
            f"{ctx!r}"
        )
        return 3

    # Step 5 — counters reflect the activity.
    try:
        status = get(
            endpoint, "/status?detail=metrics",
            extra_headers={"Coherence-Local-Operator": "true"},
        )
    except urllib.error.HTTPError as exc:
        err(f"--self-test: /status returned HTTP {exc.code}")
        return 3
    if status.get("stale_warning_emitted_total", 0) < 1:
        err(
            "--self-test: stale_warning_emitted_total did not increment "
            "(coordinator KTD-J counters appear to be inert)."
        )
        return 3
    eps = status.get("endpoint_counters") or {}
    if eps.get("pre_read_total", 0) < 2 or eps.get("post_edit_total", 0) < 1:
        err(
            f"--self-test: endpoint counters did not reflect the four-step "
            f"scenario: {eps!r}"
        )
        return 3

    if json_mode:
        import json as _json
        steps = [
            "pre-read fresh",
            "pre-edit",
            "post-edit commit",
            f"pre-read STALE ({path})",
        ]
        print(_json.dumps({"self_test": "pass", "steps_observed": steps, "error": None}), flush=True)
    else:
        print("agent-coherence-status --self-test: OK", flush=True)
        print(
            f"  pre-read fresh → pre-edit → post-edit commit → pre-read STALE "
            f"({path}) — all four steps observed.",
            flush=True,
        )
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
