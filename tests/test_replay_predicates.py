# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""Tests for the replay predicate engine (Unit 4 of D v1).

Covers the contract documented in
``docs/proposals/replay_trace_format.md`` §5.2-§5.3 and the predicate
requirements in
``docs/plans/2026-05-26-001-feat-langgraph-cycle-replay-tooling-v1-plan.md``
Unit 4:

- Single-writer happy + violation paths.
- Monotonic-version happy + regression paths.
- Stale-read CONFIRMED / AMBIGUOUS / clean / null-agent_id / earlier-tick.
- Lost-write happy + violation + the peer-invalidation regression
  (the LostWrite ADV-01 v1-blocker fix from document review).
- SKIPPED dispatch: opt-out (caller dropped a stream), capture bug
  (declared stream missing on disk), all-streams-missing degenerate.
- Integration with the sim engine via Unit 3's loader.
- ``invariants=`` restriction omits unselected predicates from BOTH
  findings AND skips.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from ccs.replay import (
    Finding,
    LoadedTrace,
    SummaryFinding,
    load,
    run_predicates,
)
from replay_fixtures import (
    audit_entry as _audit_line,
    state_log_entry as _state_log_line,
    write_jsonl,
    write_manifest as _write_manifest,
)


# ---------------------------------------------------------------------------
# Thin wrappers — call the hoisted helpers in tests/replay_fixtures.py
# ---------------------------------------------------------------------------


def _write_state_log(session_dir: Path, entries: list[dict]) -> None:
    write_jsonl(session_dir / "state_log.jsonl", entries)


def _write_audit_log(session_dir: Path, entries: list[dict]) -> None:
    write_jsonl(session_dir / "content_audit_log.jsonl", entries)


def _make_session(
    tmp_path: Path,
    *,
    streams: list[str],
    state_entries: list[dict] | None = None,
    audit_entries: list[dict] | None = None,
) -> LoadedTrace:
    session = tmp_path / "session"
    _write_manifest(session, streams=streams)
    if "state_log" in streams and state_entries is not None:
        _write_state_log(session, state_entries)
    if "content_audit_log" in streams and audit_entries is not None:
        _write_audit_log(session, audit_entries)
    return load(session)


# ---------------------------------------------------------------------------
# Single-writer predicate
# ---------------------------------------------------------------------------


class TestSingleWriter:
    """Single-writer: at most one M∪E owner per artifact at any tick."""

    def test_clean_trace_emits_no_findings(self, tmp_path: Path) -> None:
        # One writer at a time: agent-1 takes E, transitions to I, then
        # agent-2 takes E. Single-writer holds across the full transcript.
        entries = [
            _state_log_line(
                tick=1, sequence_number=1, agent_id="agent-1",
                from_state="INVALID", to_state="EXCLUSIVE",
            ),
            _state_log_line(
                tick=2, sequence_number=2, agent_id="agent-1",
                from_state="EXCLUSIVE", to_state="INVALID",
            ),
            _state_log_line(
                tick=3, sequence_number=3, agent_id="agent-2",
                from_state="INVALID", to_state="EXCLUSIVE",
            ),
        ]
        loaded = _make_session(
            tmp_path, streams=["state_log"], state_entries=entries,
        )
        findings, summary = run_predicates(loaded)
        assert findings == []
        # state_log-only manifest skips stale-read; assert ONLY stale-read
        # is in summary (not single-writer / monotonic-version / lost-write).
        assert {s.invariant for s in summary} == {"stale-read"}

    def test_double_owner_emits_confirmed(self, tmp_path: Path) -> None:
        # agent-1 takes M; agent-2 ALSO reaches M for the same artifact —
        # impossible under a correct coordinator but the trace stream
        # records it. The predicate must catch it at tick 2.
        entries = [
            _state_log_line(
                tick=1, sequence_number=1, agent_id="agent-1",
                from_state="INVALID", to_state="MODIFIED",
                trigger="commit", version=1,
            ),
            _state_log_line(
                tick=2, sequence_number=2, agent_id="agent-2",
                from_state="INVALID", to_state="MODIFIED",
                trigger="commit", version=2,
            ),
        ]
        loaded = _make_session(
            tmp_path, streams=["state_log"], state_entries=entries,
        )
        findings, _ = run_predicates(loaded, invariants=["single-writer"])
        assert len(findings) == 1
        f = findings[0]
        assert f.invariant == "single-writer"
        assert f.severity == "CONFIRMED"
        assert set(f.agents) == {"agent-1", "agent-2"}
        assert f.artifacts == ("art-1",)
        assert f.tick_range == (2, 2)


# ---------------------------------------------------------------------------
# Monotonic-version predicate
# ---------------------------------------------------------------------------


class TestMonotonicVersion:
    """Monotonic-version: committed versions strictly increase per artifact."""

    def test_clean_trace_emits_no_findings(self, tmp_path: Path) -> None:
        entries = [
            _state_log_line(
                tick=1, sequence_number=1,
                from_state="INVALID", to_state="MODIFIED",
                trigger="commit", version=1,
            ),
            _state_log_line(
                tick=2, sequence_number=2,
                from_state="MODIFIED", to_state="MODIFIED",
                trigger="commit", version=2,
            ),
            _state_log_line(
                tick=3, sequence_number=3,
                from_state="MODIFIED", to_state="MODIFIED",
                trigger="commit", version=3,
            ),
        ]
        loaded = _make_session(
            tmp_path, streams=["state_log"], state_entries=entries,
        )
        findings, _ = run_predicates(loaded, invariants=["monotonic-version"])
        assert findings == []

    def test_version_regression_emits_confirmed(
        self, tmp_path: Path,
    ) -> None:
        entries = [
            _state_log_line(
                tick=1, sequence_number=1,
                from_state="INVALID", to_state="MODIFIED",
                trigger="commit", version=5,
            ),
            _state_log_line(
                tick=2, sequence_number=2,
                from_state="MODIFIED", to_state="MODIFIED",
                trigger="commit", version=3,  # regression
            ),
        ]
        loaded = _make_session(
            tmp_path, streams=["state_log"], state_entries=entries,
        )
        findings, _ = run_predicates(loaded, invariants=["monotonic-version"])
        assert len(findings) == 1
        assert findings[0].invariant == "monotonic-version"
        assert findings[0].severity == "CONFIRMED"
        assert findings[0].artifacts == ("art-1",)
        assert findings[0].tick_range == (2, 2)


# ---------------------------------------------------------------------------
# Stale-read predicate
# ---------------------------------------------------------------------------


class TestStaleRead:
    """Stale-read with CONFIRMED / AMBIGUOUS carve-out (spec §5.2)."""

    def test_current_read_emits_no_findings(self, tmp_path: Path) -> None:
        state_entries = [
            _state_log_line(
                tick=1, sequence_number=1,
                from_state="INVALID", to_state="MODIFIED",
                trigger="commit", version=1,
            ),
        ]
        audit_entries = [
            _audit_line(tick=1, sequence_number=1, version=1),
        ]
        loaded = _make_session(
            tmp_path, streams=["state_log", "content_audit_log"],
            state_entries=state_entries, audit_entries=audit_entries,
        )
        findings, _ = run_predicates(loaded, invariants=["stale-read"])
        assert findings == []

    def test_later_read_of_older_version_confirmed(
        self, tmp_path: Path,
    ) -> None:
        # v2 committed at tick 5; read of v1 at tick 8. CONFIRMED.
        state_entries = [
            _state_log_line(
                tick=2, sequence_number=1, agent_id="writer",
                from_state="INVALID", to_state="MODIFIED",
                trigger="commit", version=1,
            ),
            _state_log_line(
                tick=5, sequence_number=2, agent_id="writer",
                from_state="MODIFIED", to_state="MODIFIED",
                trigger="commit", version=2,
            ),
        ]
        audit_entries = [
            _audit_line(
                tick=8, sequence_number=1, agent_id="reader", version=1,
            ),
        ]
        loaded = _make_session(
            tmp_path, streams=["state_log", "content_audit_log"],
            state_entries=state_entries, audit_entries=audit_entries,
        )
        findings, _ = run_predicates(loaded, invariants=["stale-read"])
        assert len(findings) == 1
        f = findings[0]
        assert f.severity == "CONFIRMED"
        assert f.kind == "stale-read"
        assert f.agents == ("reader",)
        assert f.tick_range == (5, 8)

    def test_same_tick_collision_ambiguous(self, tmp_path: Path) -> None:
        # v2 committed at tick 5; read of v1 ALSO at tick 5. Spec §5.2
        # requires AMBIGUOUS (intra-tick ordering undetermined).
        state_entries = [
            _state_log_line(
                tick=2, sequence_number=1,
                from_state="INVALID", to_state="MODIFIED",
                trigger="commit", version=1,
            ),
            _state_log_line(
                tick=5, sequence_number=2,
                from_state="MODIFIED", to_state="MODIFIED",
                trigger="commit", version=2,
            ),
        ]
        audit_entries = [
            _audit_line(
                tick=5, sequence_number=1, agent_id="reader", version=1,
            ),
        ]
        loaded = _make_session(
            tmp_path, streams=["state_log", "content_audit_log"],
            state_entries=state_entries, audit_entries=audit_entries,
        )
        findings, _ = run_predicates(loaded, invariants=["stale-read"])
        assert len(findings) == 1
        assert findings[0].severity == "AMBIGUOUS"
        assert findings[0].kind == "stale-read-ambiguous"
        assert findings[0].tick_range == (5, 5)

    def test_earlier_read_of_current_version_clean(
        self, tmp_path: Path,
    ) -> None:
        # Read at tick t of v_n; v_{n+1} committed at tick t+1. The read
        # was current at the time; later commit does not make it stale.
        state_entries = [
            _state_log_line(
                tick=2, sequence_number=1,
                from_state="INVALID", to_state="MODIFIED",
                trigger="commit", version=1,
            ),
            _state_log_line(
                tick=10, sequence_number=2,
                from_state="MODIFIED", to_state="MODIFIED",
                trigger="commit", version=2,
            ),
        ]
        audit_entries = [
            _audit_line(
                tick=5, sequence_number=1, agent_id="reader", version=1,
            ),
        ]
        loaded = _make_session(
            tmp_path, streams=["state_log", "content_audit_log"],
            state_entries=state_entries, audit_entries=audit_entries,
        )
        findings, _ = run_predicates(loaded, invariants=["stale-read"])
        assert findings == []

    def test_null_agent_id_skipped_silently(self, tmp_path: Path) -> None:
        state_entries = [
            _state_log_line(
                tick=2, sequence_number=1,
                from_state="INVALID", to_state="MODIFIED",
                trigger="commit", version=2,
            ),
        ]
        # CCSStore search-miss path emits agent_id=null with an older
        # version field. Predicate MUST NOT fire (spec §4.1).
        audit_entries = [
            _audit_line(
                tick=5, sequence_number=1, agent_id=None, version=1,
            ),
        ]
        loaded = _make_session(
            tmp_path, streams=["state_log", "content_audit_log"],
            state_entries=state_entries, audit_entries=audit_entries,
        )
        findings, _ = run_predicates(loaded, invariants=["stale-read"])
        assert findings == []

    def test_outcome_not_content_skipped_silently(self, tmp_path: Path) -> None:
        """Non-content outcomes (cache miss, error) early-return.

        ``_classify_stale_read`` is only reached for ``outcome="content"``
        entries; anything else (e.g. ``"empty"``, ``"error"``) is dropped
        before stale comparison even with a stale version field.
        """
        state_entries = [
            _state_log_line(
                tick=2, sequence_number=1,
                from_state="INVALID", to_state="MODIFIED",
                trigger="commit", version=2,
            ),
        ]
        audit_entries = [
            _audit_line(
                tick=5, sequence_number=1, agent_id="reader",
                version=1, outcome="empty",
            ),
        ]
        loaded = _make_session(
            tmp_path, streams=["state_log", "content_audit_log"],
            state_entries=state_entries, audit_entries=audit_entries,
        )
        findings, _ = run_predicates(loaded, invariants=["stale-read"])
        assert findings == []


# ---------------------------------------------------------------------------
# Lost-write predicate (includes the v1-blocker peer-invalidation test)
# ---------------------------------------------------------------------------


class TestLostWrite:
    """Lost-write: writer commits from non-owner state."""

    def test_clean_e_to_m_emits_no_findings(self, tmp_path: Path) -> None:
        entries = [
            _state_log_line(
                tick=1, sequence_number=1,
                from_state="INVALID", to_state="EXCLUSIVE",
                trigger="write",
            ),
            _state_log_line(
                tick=2, sequence_number=2,
                from_state="EXCLUSIVE", to_state="MODIFIED",
                trigger="commit", version=1,
            ),
        ]
        loaded = _make_session(
            tmp_path, streams=["state_log"], state_entries=entries,
        )
        findings, _ = run_predicates(loaded, invariants=["lost-write"])
        assert findings == []

    def test_s_to_m_commit_emits_confirmed(self, tmp_path: Path) -> None:
        # SHARED → MODIFIED via commit: the writer never held E. Lost write.
        entries = [
            _state_log_line(
                tick=1, sequence_number=1,
                from_state="SHARED", to_state="MODIFIED",
                trigger="commit", version=1,
            ),
        ]
        loaded = _make_session(
            tmp_path, streams=["state_log"], state_entries=entries,
        )
        findings, _ = run_predicates(loaded, invariants=["lost-write"])
        assert len(findings) == 1
        assert findings[0].invariant == "lost-write"
        assert findings[0].severity == "CONFIRMED"
        assert findings[0].details["observed"] == "from_state = SHARED"

    def test_peer_invalidations_not_counted_as_lost_writes(
        self, tmp_path: Path,
    ) -> None:
        """ADV-01 regression — the v1-blocker fix from document review.

        Scenario: writer commits with N=3 peers in S. coordinator/service.py
        emits N=3 state_log entries with trigger="commit",
        from_state="SHARED", to_state="INVALID" (the peer invalidations,
        line 277) plus ONE writer self-transition (E→M, line 289). The
        predicate MUST inspect only the writer commit (which is clean
        E→M) and IGNORE the three peer entries.

        Without the trigger="commit" + to_state ∈ {M, E} filter, the
        predicate would fire 3 false-positive LostWrite findings on
        EVERY clean trace with peers — torpedoing first-touch partner
        trust. Zero findings is the only acceptable outcome.
        """
        entries = [
            # Peers all S, writer holds E
            _state_log_line(
                tick=1, sequence_number=1, agent_id="writer",
                from_state="INVALID", to_state="EXCLUSIVE",
                trigger="write",
            ),
            # Peer A invalidation: S → I, trigger="commit"
            _state_log_line(
                tick=2, sequence_number=2, agent_id="peer-a",
                from_state="SHARED", to_state="INVALID",
                trigger="commit", version=1,
            ),
            # Peer B invalidation: S → I, trigger="commit"
            _state_log_line(
                tick=2, sequence_number=3, agent_id="peer-b",
                from_state="SHARED", to_state="INVALID",
                trigger="commit", version=1,
            ),
            # Peer C invalidation: S → I, trigger="commit"
            _state_log_line(
                tick=2, sequence_number=4, agent_id="peer-c",
                from_state="SHARED", to_state="INVALID",
                trigger="commit", version=1,
            ),
            # Writer self-transition: E → M, trigger="commit"
            _state_log_line(
                tick=2, sequence_number=5, agent_id="writer",
                from_state="EXCLUSIVE", to_state="MODIFIED",
                trigger="commit", version=1,
            ),
        ]
        loaded = _make_session(
            tmp_path, streams=["state_log"], state_entries=entries,
        )
        findings, _ = run_predicates(loaded, invariants=["lost-write"])
        assert findings == [], (
            "ADV-01 regression: peer-invalidation transitions must not be "
            "counted as lost writes. Got findings: " + repr(findings)
        )


# ---------------------------------------------------------------------------
# SKIPPED dispatch
# ---------------------------------------------------------------------------


class TestSkippedDispatch:
    """SKIPPED dispatch (spec §5.3) — opt-out vs capture bug."""

    def test_opt_out_when_audit_not_declared(self, tmp_path: Path) -> None:
        # Manifest declares only state_log. Stale-read needs both
        # streams → SKIPPED with opted_out=True. Other three run.
        # Clean E→M commit so the three running predicates emit nothing.
        entries = [
            _state_log_line(
                tick=1, sequence_number=1,
                from_state="INVALID", to_state="EXCLUSIVE",
                trigger="write",
            ),
            _state_log_line(
                tick=2, sequence_number=2,
                from_state="EXCLUSIVE", to_state="MODIFIED",
                trigger="commit", version=1,
            ),
        ]
        loaded = _make_session(
            tmp_path, streams=["state_log"], state_entries=entries,
        )
        findings, summary = run_predicates(loaded)
        assert findings == []
        skipped_invariants = {s.invariant for s in summary}
        assert skipped_invariants == {"stale-read"}
        stale_skip = next(s for s in summary if s.invariant == "stale-read")
        assert stale_skip.kind == "skipped"
        assert stale_skip.stream_required == "content_audit_log"
        assert stale_skip.opted_out is True

    def test_capture_bug_when_declared_but_missing_on_disk(
        self, tmp_path: Path,
    ) -> None:
        # Manifest declares BOTH streams; only state_log lands on disk.
        # streams_present excludes content_audit_log → SKIPPED with
        # opted_out=False (the capture-bug signal for Unit 5).
        session = tmp_path / "session"
        _write_manifest(session, streams=["state_log", "content_audit_log"])
        _write_state_log(
            session,
            [
                _state_log_line(
                    tick=1, sequence_number=1,
                    from_state="INVALID", to_state="MODIFIED",
                    trigger="commit", version=1,
                ),
            ],
        )
        # Deliberately DO NOT write content_audit_log.jsonl
        loaded = load(session)

        _, summary = run_predicates(loaded)
        stale_skip = next(s for s in summary if s.invariant == "stale-read")
        assert stale_skip.stream_required == "content_audit_log"
        assert stale_skip.opted_out is False

    def test_no_streams_skips_all_predicates(self, tmp_path: Path) -> None:
        # Empty streams_present → every predicate is skipped, no findings.
        session = tmp_path / "session"
        _write_manifest(session, streams=[])
        loaded = load(session)
        findings, summary = run_predicates(loaded)
        assert findings == []
        skipped_invariants = {s.invariant for s in summary}
        assert skipped_invariants == {
            "single-writer", "monotonic-version", "stale-read", "lost-write",
        }


# ---------------------------------------------------------------------------
# Invariant restriction
# ---------------------------------------------------------------------------


class TestInvariantRestriction:
    """``invariants=`` parameter restricts predicates entirely.

    Unselected predicates do NOT appear in findings AND do NOT appear
    in skips — they're never dispatched, never inspected.
    """

    def test_only_selected_predicate_runs(self, tmp_path: Path) -> None:
        # Trace would breach single-writer AND lost-write. With
        # invariants=["lost-write"], only lost-write findings appear.
        entries = [
            _state_log_line(
                tick=1, sequence_number=1, agent_id="agent-1",
                from_state="INVALID", to_state="MODIFIED",
                trigger="commit", version=1,
            ),
            # Double-owner (would trigger single-writer)
            _state_log_line(
                tick=2, sequence_number=2, agent_id="agent-2",
                from_state="INVALID", to_state="MODIFIED",
                trigger="commit", version=2,
            ),
            # Lost write from S
            _state_log_line(
                tick=3, sequence_number=3, agent_id="agent-3",
                from_state="SHARED", to_state="MODIFIED",
                trigger="commit", version=3,
            ),
        ]
        loaded = _make_session(
            tmp_path, streams=["state_log"], state_entries=entries,
        )
        findings, summary = run_predicates(loaded, invariants=["lost-write"])
        assert all(f.invariant == "lost-write" for f in findings)
        assert all(s.invariant == "lost-write" for s in summary)
        # Stale-read needs content_audit_log so it WOULD have been
        # skipped — but it was filtered out of the dispatch entirely.
        assert "stale-read" not in {s.invariant for s in summary}


# ---------------------------------------------------------------------------
# Integration with sim-engine via Unit 3 loader
# ---------------------------------------------------------------------------


class TestSimEngineIntegration:
    """End-to-end: hand-crafted multi-breach trace via the public loader."""

    def test_mixed_clean_and_breach_counts(self, tmp_path: Path) -> None:
        # Construct a trace with:
        # - 1 clean E→M commit (no findings)
        # - 1 monotonic-version regression (v3→v2)
        # - 1 lost-write (S→M)
        # - 1 stale-read CONFIRMED (v1 read at tick 10 of v2 committed
        #   at tick 5)
        state_entries = [
            _state_log_line(
                tick=1, sequence_number=1, agent_id="writer-a",
                artifact_id="art-1",
                from_state="INVALID", to_state="EXCLUSIVE",
                trigger="write",
            ),
            _state_log_line(
                tick=2, sequence_number=2, agent_id="writer-a",
                artifact_id="art-1",
                from_state="EXCLUSIVE", to_state="MODIFIED",
                trigger="commit", version=1,
            ),
            _state_log_line(
                tick=5, sequence_number=3, agent_id="writer-a",
                artifact_id="art-1",
                from_state="MODIFIED", to_state="MODIFIED",
                trigger="commit", version=3,
            ),
            # Monotonic-version regression: v3 → v2
            _state_log_line(
                tick=6, sequence_number=4, agent_id="writer-a",
                artifact_id="art-1",
                from_state="MODIFIED", to_state="MODIFIED",
                trigger="commit", version=2,
            ),
            # Lost-write on DIFFERENT artifact to avoid single-writer noise
            _state_log_line(
                tick=7, sequence_number=5, agent_id="writer-b",
                artifact_id="art-2",
                from_state="SHARED", to_state="MODIFIED",
                trigger="commit", version=1,
            ),
        ]
        audit_entries = [
            # Stale read at tick 10 of v1 (v3 was already committed at tick 5)
            _audit_line(
                tick=10, sequence_number=1, agent_id="reader",
                artifact_id="art-1", version=1,
            ),
        ]
        loaded = _make_session(
            tmp_path, streams=["state_log", "content_audit_log"],
            state_entries=state_entries, audit_entries=audit_entries,
        )
        findings, summary = run_predicates(loaded)

        by_invariant: dict[str, int] = {}
        for f in findings:
            by_invariant[f.invariant] = by_invariant.get(f.invariant, 0) + 1
        # Single-writer should NOT fire (only one writer holds M∪E per
        # artifact at a time).
        assert by_invariant.get("single-writer", 0) == 0
        assert by_invariant["monotonic-version"] == 1
        assert by_invariant["lost-write"] == 1
        assert by_invariant["stale-read"] == 1
        # No SKIPPED in summary — both streams present.
        assert summary == []


# ---------------------------------------------------------------------------
# Finding dataclass shape sanity (frozen, hashable)
# ---------------------------------------------------------------------------


def test_finding_is_frozen() -> None:
    f = Finding(
        kind="lost-write",
        severity="CONFIRMED",
        invariant="lost-write",
        agents=("a",),
        artifacts=("art-1",),
        tick_range=(1, 1),
    )
    with pytest.raises(Exception):
        f.severity = "AMBIGUOUS"  # type: ignore[misc]


def test_unknown_invariant_name_raises_value_error(tmp_path: Path) -> None:
    """Programmatic callers passing a typo'd predicate name fail loudly.

    The CLI guards via argparse ``choices=`` — but library callers don't,
    and a silent empty result is the worst possible failure mode.
    """
    loaded = _make_session(tmp_path, streams=["state_log"], state_entries=[])
    with pytest.raises(ValueError) as exc_info:
        run_predicates(loaded, invariants=["misspelled-name"])
    message = str(exc_info.value)
    assert "misspelled-name" in message
    assert "valid" in message


def test_summary_finding_is_frozen() -> None:
    s = SummaryFinding(
        kind="skipped",
        invariant="stale-read",
        reason="x",
        stream_required="content_audit_log",
        opted_out=True,
    )
    with pytest.raises(Exception):
        s.opted_out = False  # type: ignore[misc]
