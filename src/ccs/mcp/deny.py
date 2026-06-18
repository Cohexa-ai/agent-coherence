# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""MCP-C deny-contract mapper (stale-write-guard-fs, Unit 1).

Translates every fail-closed coherence terminal into a *non-ignorable* MCP tool
result — ``CallToolResult(isError=True, structuredContent={...})`` — so a client
relays the deny to the model as a business-logic error it self-corrects on,
NEVER as a success. Degrade/deny are HTTP-200 bodies inside the coordinator, so a
mapping bug here silently reintroduces the lost update: this is the load-bearing
first deliverable, built and tested before any tool binding.

Two input types (plan §"Deny reason-flow"):

- **Type A** — typed adapter exceptions, matched by EXACT ``type(exc)`` and read
  via the ``.reason`` constant carried on the exception class. The exception
  *message* stays the verbatim coordinator ``permissionDecisionReason`` (it is
  surfaced as ``detail``; the model's retry loop depends on that byte-stability,
  auto-memory ``project_cc_strict_mode_retry_hazard``). An unrecognized
  ``CoherenceError`` (a CAS corruption raise, a path escape) fails closed as
  ``internal_error`` — the mapper never lets an exception become a success.
- **Type B** — synthesized here (NO adapter raise-site) when the volume is
  unattached / the coordinator transport failed → ``coordinator_unavailable``.

The ``reason`` token is NEVER substring-matched off the message (the
typed-signal-not-substring house rule).
"""

from __future__ import annotations

from dataclasses import dataclass

from mcp.types import CallToolResult, TextContent

from ccs.core.exceptions import (
    COORDINATOR_UNAVAILABLE_REASON,
    CasRetriesExhausted,
    CommitPreempted,
    CommitUnconfirmed,
    InternalConcurrencyError,
    StaleView,
    ViewWedged,
)


@dataclass(frozen=True)
class _Terminal:
    """The wire shape for one deny terminal: a typed ``reason``, a ``recover``
    verb the agent can act on, and whether a bare retry is safe."""

    reason: str
    recover: str
    retryable: bool


# Type A — matched by EXACT exception type (so a future subclass cannot silently
# inherit a mapping). ``reason`` mirrors the constant carried on the exception
# class itself; recover/retryable are this layer's contract.
_TERMINALS: dict[type, _Terminal] = {
    StaleView: _Terminal(StaleView.reason, "reacquire", True),
    CommitPreempted: _Terminal(CommitPreempted.reason, "reacquire_and_reconcile", False),
    ViewWedged: _Terminal(ViewWedged.reason, "wait_or_escalate", False),
    CommitUnconfirmed: _Terminal(CommitUnconfirmed.reason, "read_then_retry", False),
    CasRetriesExhausted: _Terminal(CasRetriesExhausted.reason, "stop", False),
    InternalConcurrencyError: _Terminal(InternalConcurrencyError.reason, "none", False),
}

# Fallback for any unrecognized exception (an unexpected ``CoherenceError`` such
# as a CAS corruption raise or a path escape): fail closed as a generic internal
# error — never a success, never a recoverable ``stale_view``.
_UNRECOGNIZED = _Terminal("internal_error", "none", False)


def _result(terminal: _Terminal, detail: str) -> CallToolResult:
    """Build the non-ignorable tool result. ``detail`` is the verbatim deny prose,
    carried in BOTH ``structuredContent`` (structured clients) and the text
    content (clients that surface only the text channel still see the deny)."""
    return CallToolResult(
        isError=True,
        content=[TextContent(type="text", text=detail)],
        structuredContent={
            "reason": terminal.reason,
            "recover": terminal.recover,
            "retryable": terminal.retryable,
            "detail": detail,
        },
    )


def deny_result(exc: BaseException) -> CallToolResult:
    """Map a coherence terminal (Type A) to a non-ignorable ``isError`` result,
    preserving the coordinator deny text verbatim in ``detail``."""
    terminal = _TERMINALS.get(type(exc), _UNRECOGNIZED)
    return _result(terminal, str(exc))


def coordinator_unavailable_result(detail: str) -> CallToolResult:
    """Type B (synthesized): no adapter raised, but the volume is unattached or
    the coordinator transport failed. Fail closed — the write is NOT
    version-committed."""
    return _result(
        _Terminal(COORDINATOR_UNAVAILABLE_REASON, "retry_later", False),
        detail,
    )
