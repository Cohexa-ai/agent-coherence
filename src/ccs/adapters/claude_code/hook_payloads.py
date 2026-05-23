# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""Typed JSON shapes for the coordinator's wire contract (KTD per Unit 4).

These TypedDicts define every request body and response shape the HTTP
coordinator accepts/emits. The cc_hook_stdin contract test in Unit 8
round-trips realistic Claude Code hook stdin payloads through this
contract; CI catches drift on every Claude Code minor version bump.

Per-invocation variation in WARN-mode templates (v0.1.1 design):
- Every additionalContext payload on the WARN-mode allow path includes a
  timestamp + last-writer session id, so the exact prose differs on every
  invocation. The original §13.5 rationale was that varied text sidesteps
  the model retry loop on identical denials. NOTE: the v0.2 Phase 0
  falsifiability experiment (see ``docs/probes/2026-05-19-ktd-e-falsifiability/REPORT.md``)
  inverted this finding for the DENY path — varied text actually WORSENS
  opus (5 retries vs 2 with static text; opus reads varied deny text as
  prompt-injection patterns and retries to disambiguate). v0.2 strict-mode
  deny therefore uses a STATIC reason template
  (``STRICT_MODE_DENY_REASON_TEMPLATE``) byte-stable across retries.

Constraint per origin §7.4 + KTD-12: the structured ``summary`` metadata
NEVER includes raw file content, content hashes themselves, diff text, or
content-derived data. Only path / version / session-id / timestamp.

v0.2 KTD-U structural invariant (security):
- ``TERMINAL_DENIAL_CLASSES`` enumerates denial classes that MUST NEVER be
  converted to ``permissionDecision: "allow"``. Every allow-emission path
  routes through ``emit_allow()`` which asserts membership; tests in
  ``tests/integration/test_strict_mode.py`` parametrize over the
  call-site list and a meta-test grep-counts call sites in this file +
  ``coordinator_server.py`` to force list extension on every new allow
  path. See plan Unit 2 (KTD-P, KTD-Q, KTD-U).
"""

from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any, Literal, NotRequired, TypedDict


# ----------------------------------------------------------------------
# v0.2 strict-mode helpers — KTD-P (static deny text), KTD-U (terminal
# denial invariant)
# ----------------------------------------------------------------------


TERMINAL_DENIAL_CLASSES: frozenset[str] = frozenset({
    "permissions_deny_strict_mode",
})
"""KTD-U security invariant: denial classes that MUST NEVER be converted to
``permissionDecision: "allow"``. Any code path emitting allow checks
membership via :func:`emit_allow`; passing a terminal class raises
AssertionError. Tests at ``tests/integration/test_strict_mode.py``
parametrize the call-site list and meta-test the count so a future
contributor adding a new allow path is forced to extend the parameter list
(and therefore consider the invariant) rather than satisfying the test
trivially. Adding a new terminal class extends the boundary; never remove
an entry without a security review."""


STRICT_MODE_DENY_REASON_TEMPLATE: str = (
    "Stale read denied: {path} was updated by session {last_writer_short} "
    "at {last_writer_ts_iso}. Re-read {path} via the Read tool before "
    "proceeding. This denial is structural (v0.2 strict mode); retrying "
    "the same operation will produce the same denial."
)
"""KTD-P static deny text. Byte-stable across retries of the same
(session, artifact) staleness event because every substitution is
deterministic per-artifact (path), per-preempter (last_writer_short), or
per-commit-tick (last_writer_ts_iso). The Phase 0 H1 falsification proved
varied deny text WORSENS opus behavior (5 retries vs 2 with static text);
this template guards against accidental re-introduction of per-invocation
fields. The format-string placeholder set is locked by
``test_strict_mode_deny_reason_template_is_static``."""


def emit_allow(
    *,
    source: str,
    additional_context: str | None = None,
    denial_class: str | None = None,
) -> dict[str, Any]:
    """Build the ``hookSpecificOutput`` envelope for a ``permissionDecision:
    "allow"`` response. ALL allow emissions route through this helper.

    KTD-U enforcement: if ``denial_class`` is in :data:`TERMINAL_DENIAL_CLASSES`,
    raises ``AssertionError`` with a diagnostic naming the source call site.
    A code path that knows it's converting a terminal-class denial back to
    allow cannot satisfy this check; that's the structural invariant.

    Args:
        source: short identifier of the call site (e.g. ``"pre_read_fresh_with_notice"``).
            Used by the KTD-U meta-test for parameter-list coverage and the
            AssertionError diagnostic.
        additional_context: optional ``additionalContext`` prose. Omitted when
            None — the model gets a quiet allow.
        denial_class: optional denial classification the caller is converting
            to allow. Almost always ``None`` for legitimate allow paths; the
            argument exists so tests can synthesize "this caller refuses to
            convert TERMINAL_DENIAL_CLASSES inputs to allow."
    """
    assert denial_class not in TERMINAL_DENIAL_CLASSES, (
        f"emit_allow(source={source!r}, denial_class={denial_class!r}): "
        f"refused to convert TERMINAL_DENIAL_CLASSES member to allow. "
        f"This is the KTD-U security invariant — strict-mode denials are "
        f"structurally terminal."
    )
    out: dict[str, Any] = {
        "hookEventName": "PreToolUse",
        "permissionDecision": "allow",
    }
    if additional_context is not None:
        out["additionalContext"] = additional_context
    return out


def emit_strict_deny(
    *,
    source: str,
    summary: "StaleSummary",
) -> dict[str, Any]:
    """Build the ``hookSpecificOutput`` envelope for a v0.2 strict-mode
    deny response.

    The ``permissionDecisionReason`` is rendered via
    :data:`STRICT_MODE_DENY_REASON_TEMPLATE` — static, byte-stable across
    retries per KTD-P. The ``source`` argument is preserved for telemetry
    (Unit 4 audit-log append) and parameter-list parity with :func:`emit_allow`.
    """
    last_writer_full = summary.get("last_writer_session_id") or "<unknown>"
    # Preserve placeholder values like "<unknown>" verbatim — slicing to 8
    # would lose the closing angle bracket and produce malformed prose
    # ("<unknown" instead of "<unknown>"). Real UUIDv4 session ids are 36
    # chars, so the 8-char short form is unambiguous when present.
    if last_writer_full.startswith("<") and last_writer_full.endswith(">"):
        last_writer_short = last_writer_full
    else:
        last_writer_short = last_writer_full[:8]
    last_writer_ts_iso = datetime.fromtimestamp(
        summary["last_writer_at_unix_ts"], tz=timezone.utc
    ).isoformat()
    reason = STRICT_MODE_DENY_REASON_TEMPLATE.format(
        path=summary["path"],
        last_writer_short=last_writer_short,
        last_writer_ts_iso=last_writer_ts_iso,
    )
    return {
        "hookEventName": "PreToolUse",
        "permissionDecision": "deny",
        "permissionDecisionReason": reason,
    }


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
    """Fresh-read response shape.

    AC-08: ``hookSpecificOutput`` is OPTIONAL and present when the
    session has pending preemption notices that the wrapper at
    ``_handle_pre_read.work_with_notice_surfacing`` attaches. Typed
    consumers should treat ``hookSpecificOutput`` as ``NotRequired``
    on every endpoint that returns a fresh envelope (pre-read,
    pre-bash, pre-grep).
    """
    status: Literal["fresh"]
    hookSpecificOutput: NotRequired["PreToolUseHookOutput"]
    # AC-05: also present on watchdog-timeout degraded responses.
    degraded: NotRequired[bool]


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
    # AC-02: canonical name follows KTD-J convention (full-word _seconds
    # suffix). ``coordinator_uptime_s`` is emitted alongside as a
    # deprecated alias for one release; consumers should migrate to the
    # canonical name. Removed in v0.2.
    coordinator_uptime_seconds: float
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
    """Top-level hookSpecificOutput for a v0.1.1 warn-mode stale-read
    PreToolUse response. Routes through :func:`emit_allow` to satisfy the
    KTD-U structural invariant.

    Strict-mode callers must call :func:`emit_strict_deny` directly +
    wrap the result; this builder is reserved for the warn-mode (allow)
    path that v0.1.1 ships and v0.2 preserves for non-strict-mode
    artifacts."""
    return {
        "hookSpecificOutput": emit_allow(
            source="stale_response_builder",
            additional_context=stale_read_warning(summary),
        ),
        "status": "stale",
        "summary": summary,
    }


def build_collision_response(
    holder_session_id: str,
    holder_acquired_at_unix_ts: float,
    path: str,
) -> dict:
    """Top-level hookSpecificOutput for an edit-collision PreToolUse response.

    Edit collisions are distinct from stale-reads — the editor wants
    EXCLUSIVE but another session holds it. v0.1.1 surfaces the collision
    via warn-mode allow + additionalContext; v0.2 preserves this shape
    (strict-mode flip applies to stale-reads, not contention)."""
    return {
        "hookSpecificOutput": emit_allow(
            source="collision_response_builder",
            additional_context=edit_collision_warning(
                holder_session_id, holder_acquired_at_unix_ts, path
            ),
        ),
        "ok": True,
        "collision": True,
    }


def now_unix() -> float:
    """Single source of truth for the coordinator's notion of 'now', so
    tests can mock it cleanly later."""
    return time.time()
