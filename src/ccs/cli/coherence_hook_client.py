# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""``agent-coherence-hook-client`` — command-type hook handler bridge.

Phase E.0 probe 2A surfaced that Claude Code v2.1.131 rejects hooks.json
at LOAD TIME if any URL contains ``${COHERENCE_PORT}`` — strict-URL
schema validation runs before env-var expansion. Workaround: HTTP-type
hooks are NOT viable; the plugin uses command-type hooks that invoke
this client, which resolves the port from ``.coherence/server.pid``
directly (no env-var dependency).

## Subcommands

Each subcommand maps to one coordinator endpoint and translates the
Claude Code stdin contract → the coordinator's payload contract:

| Subcommand     | CC hook         | Coordinator endpoint    |
|----------------|-----------------|-------------------------|
| pre-read       | PreToolUse:Read | POST /hooks/pre-read    |
| pre-edit       | PreToolUse:Edit | POST /hooks/pre-edit    |
| post-edit      | PostToolUse     | POST /hooks/post-edit   |
| session-stop   | Stop            | POST /hooks/session-stop|

## stdin contract

Claude Code sends a JSON object on stdin with at least:
- ``session_id`` (UUID string)
- ``tool_name`` (for tool-related hooks)
- ``tool_input`` (dict; for Read/Edit/Write this has ``file_path``)
- ``tool_response`` (for PostToolUse only)

We translate:
- ``file_path`` (absolute) → workspace-relative path via the resolver.
- ``tool_response.success`` (for post-edit; defaults True if missing).
- ``tool_response.content_hash`` if precomputed by an upstream wrapper;
  otherwise the client hashes the post-edit file content on the fly.

## stdout contract

Whatever the coordinator returns. The coordinator's responses already
match Claude Code's ``hookSpecificOutput`` shape for the relevant cases
(stale-read warning, edit-collision warning). For uninteresting cases
(fresh read, ok commit) the response is a small JSON status object that
Claude Code ignores.

Exit code is always 0 on success — even if the coordinator returns
``ok: False``, that's a logical outcome surfaced to the model via
``additionalContext``, not a hook failure.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import urllib.error
from pathlib import Path
from typing import Any, Optional, Sequence

from ccs.adapters.claude_code.resolver import find_coordinator_root
from ccs.cli._coherence_client import (
    CoordinatorEndpoint,
    CoordinatorUnavailable,
    post,
    resolve_endpoint,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="agent-coherence-hook-client",
        description=(
            "Claude Code command-type hook handler that bridges stdin "
            "hook payloads to the local coherence coordinator."
        ),
    )
    parser.add_argument(
        "subcommand",
        choices=["pre-read", "pre-edit", "post-edit", "session-stop"],
        help="Which coordinator endpoint to invoke. Maps 1:1 to the CC hook event.",
    )
    parser.add_argument(
        "--root",
        type=Path,
        default=None,
        help="Override the coordinator root (default: walk up from cwd).",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Entry point. Wraps the real dispatch in a top-level except so that no
    unexpected exception (refactor regression, payload-builder bug, anything
    not anticipated) can violate the always-exit-0 hook contract.

    P2 ce-review fix #8 (reliability): top-level broad except guarantees the
    hook contract — emit ``{}`` and exit 0 no matter what fails.
    P3 ce-review fix #27 (agent-native): isatty guard prevents indefinite
    block when a developer runs the hook-client manually for testing.
    P3 ce-review fix #24 (cli-readiness): empty stdin path now emits ``{}``
    for consistency with the malformed-stdin path (both produce parseable
    JSON for any upstream wrapper doing json.loads on stdout).
    """
    try:
        return _main_inner(argv)
    except SystemExit:
        # argparse / explicit sys.exit — propagate normally
        raise
    except BaseException:
        # CC must never see the hook block its tool call. Even on
        # KeyboardInterrupt or an unexpected programming error, emit the
        # no-op response and exit 0.
        _emit_empty()
        return 0


def _main_inner(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    # Developer footgun: if stdin is a TTY, the read below would block
    # indefinitely. Emit a brief usage hint to stderr and exit clean so
    # someone testing manually understands what's expected.
    if sys.stdin.isatty():
        print(
            "agent-coherence-hook-client: stdin is a terminal — "
            "this command expects a Claude Code hook JSON payload on stdin.",
            file=sys.stderr,
        )
        _emit_empty()
        return 0

    # Read CC's hook stdin payload.
    try:
        raw = sys.stdin.read()
    except OSError:
        _emit_empty()
        return 0  # no stdin — emit {} for output consistency
    if not raw.strip():
        _emit_empty()  # consistent with malformed-stdin path
        return 0

    try:
        cc_payload = json.loads(raw)
    except json.JSONDecodeError:
        # Don't crash CC; degrade silently.
        _emit_empty()
        return 0

    # Resolve coordinator root + endpoint.
    root = args.root if args.root is not None else find_coordinator_root()
    if root is None:
        _emit_empty()
        return 0
    root_path = Path(root).resolve()

    try:
        endpoint = resolve_endpoint(root_path)
    except CoordinatorUnavailable:
        _emit_empty()
        return 0

    # Dispatch. Broader except below catches any unexpected exception from
    # payload builders or _call (e.g., AttributeError from a malformed
    # cc_payload, refactor-introduced exception type) so hook never blocks.
    try:
        if args.subcommand == "pre-read":
            payload = _build_pre_read(cc_payload, root_path)
            response = _call(endpoint, "/hooks/pre-read", payload)
        elif args.subcommand == "pre-edit":
            payload = _build_pre_edit(cc_payload, root_path)
            response = _call(endpoint, "/hooks/pre-edit", payload)
        elif args.subcommand == "post-edit":
            payload = _build_post_edit(cc_payload, root_path)
            response = _call(endpoint, "/hooks/post-edit", payload)
        elif args.subcommand == "session-stop":
            payload = _build_session_stop(cc_payload)
            response = _call(endpoint, "/hooks/session-stop", payload)
        else:  # pragma: no cover — argparse already validates
            _emit_empty()
            return 0
    except CoordinatorUnavailable:
        _emit_empty()
        return 0
    except _SkipHook:
        _emit_empty()
        return 0
    except Exception:
        # P2 ce-review fix #8: any unexpected exception from a payload
        # builder, _call, or future refactor must NOT propagate — CC
        # requires the hook to exit clean. The top-level except in main()
        # also catches but that's BaseException-wide; this one preserves
        # KeyboardInterrupt propagation to the outer handler.
        _emit_empty()
        return 0

    if response is None:
        _emit_empty()
        return 0

    # Pass the coordinator's response straight through to CC. Coordinator
    # responses already match CC's hookSpecificOutput shape for the cases
    # that need to inject context (stale read, edit collision).
    print(json.dumps(response), flush=True)
    return 0


class _SkipHook(Exception):
    """Internal signal: this hook invocation has nothing meaningful to do
    (untracked file, missing required field, etc.). Emit empty response."""


def _emit_empty() -> None:
    """Standard 'no-op' hook response — empty JSON object, exit 0."""
    print("{}", flush=True)


def _call(
    endpoint: CoordinatorEndpoint, path: str, payload: dict[str, Any]
) -> Optional[dict[str, Any]]:
    try:
        return post(endpoint, path, payload)
    except urllib.error.HTTPError:
        # Coordinator rejected the request (validation error). Degrade
        # silently — the hook should NEVER block the user's tool call.
        return None


# ----------------------------------------------------------------------
# Payload translators
# ----------------------------------------------------------------------


def _build_pre_read(cc: dict[str, Any], root: Path) -> dict[str, Any]:
    session_id = _require_session_id(cc)
    file_path = _require_file_path(cc)
    rel = _to_workspace_relative(file_path, root)
    return {"session_id": session_id, "path": rel}


def _build_pre_edit(cc: dict[str, Any], root: Path) -> dict[str, Any]:
    session_id = _require_session_id(cc)
    file_path = _require_file_path(cc)
    rel = _to_workspace_relative(file_path, root)
    return {"session_id": session_id, "path": rel}


def _build_post_edit(cc: dict[str, Any], root: Path) -> dict[str, Any]:
    session_id = _require_session_id(cc)
    file_path = _require_file_path(cc)
    rel = _to_workspace_relative(file_path, root)
    tool_response = cc.get("tool_response") or {}
    # CC's tool_response shape varies; treat missing 'success' as True
    # (if CC fired PostToolUse at all, the tool didn't hard-fail).
    success = bool(tool_response.get("success", True))
    content_hash: Optional[str] = tool_response.get("content_hash")
    if success and content_hash is None:
        # Hash the post-write content from disk. The file path is
        # absolute per CC's stdin contract.
        content_hash = _hash_file(Path(file_path))
    body: dict[str, Any] = {
        "session_id": session_id,
        "path": rel,
        "success": success,
    }
    if content_hash is not None:
        body["content_hash"] = content_hash
    return body


def _build_session_stop(cc: dict[str, Any]) -> dict[str, Any]:
    session_id = _require_session_id(cc)
    return {"session_id": session_id}


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------


def _require_session_id(cc: dict[str, Any]) -> str:
    sid = cc.get("session_id")
    if not isinstance(sid, str) or not sid:
        raise _SkipHook("session_id missing")
    return sid


def _require_file_path(cc: dict[str, Any]) -> str:
    tool_input = cc.get("tool_input") or {}
    fp = tool_input.get("file_path")
    if not isinstance(fp, str) or not fp:
        raise _SkipHook("tool_input.file_path missing")
    return fp


def _to_workspace_relative(file_path: str, root: Path) -> str:
    """Convert CC's absolute file_path → workspace-relative path the
    coordinator expects. If the path is outside the workspace (shouldn't
    happen but defensive), skip the hook rather than send garbage."""
    try:
        rel = Path(file_path).resolve().relative_to(root)
    except ValueError:
        raise _SkipHook(f"path outside workspace root: {file_path}")
    return str(rel)


def _hash_file(path: Path) -> Optional[str]:
    """SHA-256 of the file at the given absolute path. Returns None on
    read failure (don't crash the hook)."""
    try:
        h = hashlib.sha256()
        with path.open("rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                h.update(chunk)
        return h.hexdigest()
    except OSError:
        return None


if __name__ == "__main__":
    raise SystemExit(main())
