"""Unit 1 — the deny-contract mapper.

Every fail-closed coherence terminal must become a *non-ignorable* tool result
(``CallToolResult(isError=True, structuredContent={reason,recover,retryable,detail})``)
with the coordinator's deny prose preserved **verbatim** in ``detail`` (the
model's retry loop relies on byte-stable deny text — auto-memory
``project_cc_strict_mode_retry_hazard``). The ``reason`` token is a typed
constant carried on the exception *type*, never substring-matched off the
message. Contract table: plan §"Deny vocabulary" (2026-06-18 plan, lines 101–111).

This is the load-bearing first deliverable: a mapping bug silently reintroduces
the lost update (degrade/deny are HTTP-200 bodies), so it is built and tested
before any tool binding.
"""

from __future__ import annotations

import pytest

from ccs.core.exceptions import (
    CasRetriesExhausted,
    CoherenceError,
    CommitPreempted,
    CommitUnconfirmed,
    InternalConcurrencyError,
    StaleView,
    ViewWedged,
)
from ccs.mcp.deny import coordinator_unavailable_result, deny_result

# (exc, reason, recover, retryable) — the exact plan contract (lines 103–109).
_TERMINALS = [
    (StaleView("stale prose"), "stale_view", "reacquire", True),
    (
        CommitPreempted("preempt prose"),
        "commit_preempted",
        "reacquire_and_reconcile",
        False,
    ),
    (ViewWedged("wedged prose"), "view_wedged", "wait_or_escalate", False),
    (
        CommitUnconfirmed("unconfirmed prose"),
        "commit_unconfirmed",
        "read_then_retry",
        False,
    ),
    (CasRetriesExhausted("data/x", 8, 3), "cas_exhausted", "stop", False),
    (
        InternalConcurrencyError("concurrent use detected"),
        "internal_concurrency_error",
        "none",
        False,
    ),
]


@pytest.mark.parametrize("exc, reason, recover, retryable", _TERMINALS)
def test_typed_terminal_maps_to_non_ignorable_iserror(exc, reason, recover, retryable):
    """Happy path: each typed terminal → isError + the exact {reason,recover,retryable}."""
    result = deny_result(exc)

    assert result.isError is True
    sc = result.structuredContent
    assert sc["reason"] == reason
    assert sc["recover"] == recover
    assert sc["retryable"] is retryable
    # The deny prose survives verbatim — regenerating it worsens model retries.
    assert sc["detail"] == str(exc)
    # The body is also relayed as text content (clients that surface only the
    # text channel still see the deny).
    assert result.content[0].text == str(exc)


def test_reason_token_is_separate_from_verbatim_detail():
    """The .reason constant lives on the type; the message stays the byte-stable
    coordinator prose. The two must not be conflated."""
    prose = "coherence coordinator denied the write (stale view); reacquire() and write from the fresh bytes"
    result = deny_result(StaleView(prose))

    assert result.structuredContent["detail"] == prose  # verbatim message
    assert result.structuredContent["reason"] == "stale_view"  # typed constant
    assert StaleView.reason == "stale_view"  # carried on the type, not parsed


def test_stale_view_and_commit_preempt_produce_distinct_reasons():
    """A pre-edit stale deny (recoverable by reacquire) and a post-edit preempt
    (disk may hold un-versioned bytes) MUST NOT collapse to one reason."""
    stale = deny_result(StaleView("same prose")).structuredContent
    preempt = deny_result(CommitPreempted("same prose")).structuredContent

    assert stale["reason"] == "stale_view"
    assert stale["recover"] == "reacquire"
    assert stale["retryable"] is True
    assert preempt["reason"] == "commit_preempted"
    assert preempt["recover"] == "reacquire_and_reconcile"
    assert preempt["retryable"] is False


def test_a5_concurrency_never_masquerades_as_stale_view():
    """The A5 single-op guard is a server bug, not a recoverable deny — it must
    never be relayed as a stale view the agent would retry."""
    result = deny_result(InternalConcurrencyError("single-threaded; concurrent use"))

    assert result.isError is True
    assert result.structuredContent["reason"] == "internal_concurrency_error"
    assert result.structuredContent["reason"] != "stale_view"
    assert result.structuredContent["retryable"] is False


def test_unrecognized_coherence_error_fails_closed_generic():
    """A plain/unexpected CoherenceError (e.g. a CAS corruption raise, a path
    escape) fails closed as a generic internal error — never a success, never a
    recoverable stale_view."""
    result = deny_result(CoherenceError("unexpected commit corruption"))

    assert result.isError is True
    assert result.structuredContent["reason"] == "internal_error"
    assert result.structuredContent["reason"] != "stale_view"
    assert result.structuredContent["retryable"] is False
    assert result.structuredContent["detail"] == "unexpected commit corruption"


def test_type_b_coordinator_unavailable_is_synthesized_and_fails_closed():
    """Type B: no adapter raise-site — when the volume is unattached/transport
    failed, deny.py synthesizes a fail-closed coordinator_unavailable."""
    result = coordinator_unavailable_result("coordinator unreachable at attach")

    assert result.isError is True
    sc = result.structuredContent
    assert sc["reason"] == "coordinator_unavailable"
    assert sc["recover"] == "retry_later"
    assert sc["retryable"] is False
    assert sc["detail"] == "coordinator unreachable at attach"


def test_cas_exhausted_reason_constant_lives_on_the_type():
    """CasRetriesExhausted is pre-existing; Unit 1 adds .reason so the mapper
    classifies it by type (reconciling its `cas_retries_exhausted` prose token
    with the `cas_exhausted` wire reason)."""
    assert CasRetriesExhausted.reason == "cas_exhausted"
    exc = CasRetriesExhausted("data/x", 8, 3)
    assert deny_result(exc).structuredContent["reason"] == "cas_exhausted"
    # the prose token (the message) is distinct from the wire reason
    assert "cas_retries_exhausted" in str(exc)
