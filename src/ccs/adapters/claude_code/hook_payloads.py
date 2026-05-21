# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""Typed JSON shapes for the coordinator's wire contract (KTD per Unit 4).

These TypedDicts define every request body and response shape the HTTP
coordinator accepts/emits. The cc_hook_stdin contract test in Unit 8
round-trips realistic Claude Code hook stdin payloads through this
contract; CI catches drift on every Claude Code minor version bump.

Per-invocation variation in warning templates (per Unit 4 pre-flight):
- Every additionalContext payload includes a timestamp + last-writer
  session id, so the exact prose differs on every invocation. When
  v0.2 strict mode swaps ``permissionDecision: "allow"`` → ``"deny"``,
  the model receives a varying reason each time it retries — sidesteps
  the empirical retry-loop hazard from brainstorm §13.5 where identical
  deny reasons caused the model to retry with a fresh ``tool_use_id``.

Constraint per origin §7.4 + KTD-12: the structured ``summary`` metadata
NEVER includes raw file content, content hashes themselves, diff text, or
content-derived data. Only path / version / session-id / timestamp.
"""

from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Literal, NotRequired, TypedDict


# ----------------------------------------------------------------------
# Request bodies (from hook handlers → coordinator)
# ----------------------------------------------------------------------


class PreReadRequest(TypedDict):
    session_id: str
    path: str  # parent-repo-relative; KTD-7 normalization happens client-side
    content_hash: NotRequired[str]


class PreEditRequest(TypedDict):
    session_id: str
    path: str


class PostEditRequest(TypedDict):
    session_id: str
    path: str
    content_hash: str
    success: bool


class SessionStopRequest(TypedDict):
    session_id: str


class PolicyTrackRequest(TypedDict):
    paths: list[str]


class PolicyUntrackRequest(TypedDict):
    paths: list[str]


# ----------------------------------------------------------------------
# Response bodies (coordinator → hook handler)
# ----------------------------------------------------------------------


class StaleSummary(TypedDict):
    """Structured stale-read metadata. NEVER includes content/hash bytes.

    Two timestamps serve different purposes:
    - `last_writer_at_unix_ts` is from the registry's `artifacts.updated_at`
      (the REAL commit wall-clock); semantically honest.
    - `warning_generated_at_unix_ts` is `now()` at handler time; provides
      structural per-invocation variation that survives the case where two
      reads of the same stale state somehow occur (defense-in-depth against
      the §13.5 retry-loop hazard once v0.2 strict mode flips allow→deny).
    """
    path: str
    current_version: int
    prior_version_seen_by_session: int | None
    last_writer_session_id: str
    last_writer_at_unix_ts: float
    warning_generated_at_unix_ts: float
    hash_differs: bool


class FreshResponse(TypedDict):
    status: Literal["fresh"]


class StaleResponse(TypedDict):
    """The complete wire shape for a stale-read response (AC-04 / finding #25).

    ``build_stale_response`` emits all three fields; tests access
    ``body['status']`` and ``body['summary']['path']`` directly.
    Document the full shape so typed consumers have an accurate contract.
    """
    hookSpecificOutput: "PreToolUseHookOutput"
    status: Literal["stale"]
    summary: StaleSummary


class PreToolUseHookOutput(TypedDict):
    hookEventName: Literal["PreToolUse"]
    permissionDecision: Literal["allow", "deny"]
    additionalContext: str
    permissionDecisionReason: NotRequired[str]


class OkResponse(TypedDict):
    ok: Literal[True]


class CollisionResponse(TypedDict):
    """Edit collision (another session holds EXCLUSIVE). v0.1 warn-only —
    permissionDecision stays "allow"; v0.2 strict mode flips this to "deny"."""
    hookSpecificOutput: PreToolUseHookOutput


class SessionStopResponse(TypedDict):
    ok: Literal[True]
    released_artifacts: list[str]  # parent-repo-relative paths


class PolicyTrackResponse(TypedDict):
    ok: Literal[True]
    added: list[str]
    rejected: list[dict]  # [{"path": "...", "reason": "..."}, ...]


class PolicyUntrackResponse(TypedDict):
    ok: Literal[True]
    removed: list[str]
    rejected: list[dict]  # [{"path": "...", "reason": "..."}, ...] — AC-06 / finding #27


class StatusResponse(TypedDict):
    tracked_artifacts: list[dict]  # [{"path": "...", "version": int, "last_writer": "..."}, ...]
    sessions: list[dict]  # [{"session_id": "...", "states": {path: state_name}}, ...]
    coordinator_uptime_s: float
    coordinator_pid: int


class ErrorResponse(TypedDict):
    error: str


# ----------------------------------------------------------------------
# Warning templates — per-invocation variation for v0.2 strict-mode safety
# ----------------------------------------------------------------------


def stale_read_warning(summary: StaleSummary) -> str:
    """Build the stale-read additionalContext message.

    Per-invocation variation: both `last_writer_at_unix_ts` (real commit tick)
    and `warning_generated_at_unix_ts` (handler-time `now()`) appear in the
    prose. The latter guarantees byte-different text on every invocation,
    structurally precluding the §13.5 retry-loop hazard when v0.2 strict
    mode flips allow → deny.

    F1 fix: distinguishes "first observation of this artifact" from
    "previously-seen-but-now-invalidated" cases with accurate prose.

    Constraint: no content bytes, no content hashes, no diff text.
    """
    last_writer_short = summary["last_writer_session_id"][:8]
    last_writer_ts = datetime.fromtimestamp(
        summary["last_writer_at_unix_ts"], tz=timezone.utc
    ).isoformat()
    generated_ts = datetime.fromtimestamp(
        summary["warning_generated_at_unix_ts"], tz=timezone.utc
    ).isoformat()
    prior = summary.get("prior_version_seen_by_session")
    if prior is not None:
        prior_clause = f"you previously saw v{prior}"
    else:
        prior_clause = (
            "this is the first time your session has observed this artifact "
            "(another session in this workspace registered it before you)"
        )
    if summary["hash_differs"]:
        divergence = (
            "Your worktree's current content also differs from the coordinator's "
            "last-recorded hash, which suggests in-flight local edits or a "
            "different branch checkout."
        )
    else:
        divergence = (
            "Your worktree's content matches the last-recorded hash; the divergence "
            "is purely about version-tracking metadata."
        )
    return (
        f"⚠ Stale read [warning emitted {generated_ts}]: {summary['path']} was "
        f"updated by session {last_writer_short} at {last_writer_ts}. "
        f"Current version is v{summary['current_version']}; {prior_clause}. "
        f"{divergence} "
        f"Consider re-reading {summary['path']} before acting on stale assumptions."
    )


def edit_collision_warning(
    holder_session_id: str,
    holder_acquired_at_unix_ts: float,
    path: str,
) -> str:
    """Build the edit-collision additionalContext message (KTD-1 +
    KTD-9 same-hash-blindness mitigation).

    Per-invocation variation: holder session id + acquired-at timestamp
    + the unique current time at message-build time all change between
    invocations. Future v0.2 strict mode can flip allow → deny safely.
    """
    holder_short = holder_session_id[:8]
    holder_ts = datetime.fromtimestamp(
        holder_acquired_at_unix_ts, tz=timezone.utc
    ).isoformat()
    detected_ts = datetime.now(tz=timezone.utc).isoformat()
    return (
        f"⚠ Concurrent edit detected at {detected_ts} (UTC): another session "
        f"({holder_short}) has been editing {path} since {holder_ts}. "
        f"Your edit will land in your own worktree, but only one session's "
        f"commit will be accepted by the coordinator. Consider waiting for the "
        f"other session to finish or coordinating which one should proceed."
    )


def build_stale_response(summary: StaleSummary) -> dict:
    """Top-level hookSpecificOutput for a stale-read PreToolUse response.
    Returns a plain dict (not StaleResponse TypedDict) so JSON serializer
    accepts it without runtime TypedDict gymnastics."""
    return {
        "hookSpecificOutput": {
            "hookEventName": "PreToolUse",
            "permissionDecision": "allow",  # v0.1 warn-only; v0.2 may flip
            "additionalContext": stale_read_warning(summary),
        },
        "status": "stale",
        "summary": summary,
    }


def build_collision_response(
    holder_session_id: str,
    holder_acquired_at_unix_ts: float,
    path: str,
) -> dict:
    """Top-level hookSpecificOutput for an edit-collision PreToolUse response."""
    return {
        "hookSpecificOutput": {
            "hookEventName": "PreToolUse",
            "permissionDecision": "allow",  # v0.1 warn-only
            "additionalContext": edit_collision_warning(
                holder_session_id, holder_acquired_at_unix_ts, path
            ),
        },
        "ok": True,
        "collision": True,
    }


def now_unix() -> float:
    """Single source of truth for the coordinator's notion of 'now', so
    tests can mock it cleanly later."""
    return time.time()
