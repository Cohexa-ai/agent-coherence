# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""Tests for ``agent-coherence-replay`` CLI (Unit 5 of D v1).

Covers every test scenario enumerated in plan §Unit 5:

- Exit codes 0/1/2/3 each have ≥1 explicit assertion.
- AMBIGUOUS suppression default behavior + ``--include-ambiguous``
  override.
- AMBIGUOUS callout threshold (exceed → callout present; below → absent).
- ``--invariant`` restricts both findings AND SKIPPED outputs.
- ``--quiet`` produces zero stdout on a clean trace.
- Trace errors (MULTI_INSTANCE, TRACE_CORRUPTION, missing manifest)
  all map to exit code 3 with NO Python traceback on stderr.
- ``--json`` schema conformance: structural validation against spec §7.

Fixtures mirror the on-disk trace format documented in
``docs/proposals/replay_trace_format.md`` (manifest.json + per-stream
.jsonl files). Helper builders kept short and explicit so each test
documents its own scenario.
"""

from __future__ import annotations

import io
import json
from pathlib import Path
from typing import Any

import pytest

from ccs.cli.coherence_replay import build_parser, main


# ---------------------------------------------------------------------------
# Fixture builders
# ---------------------------------------------------------------------------


def _write_manifest(
    session_dir: Path,
    *,
    streams: list[str],
    instance_id: str | None = "instance-A",
    adapter_type: str = "test-fixture",
    start_tick: int = 0,
    end_tick: int = 10,
) -> None:
    session_dir.mkdir(parents=True, exist_ok=True)
    manifest = {
        "schema_version": 0,
        "schema_note": "test fixture",
        "adapter_type": adapter_type,
        "start_tick": start_tick,
        "end_tick": end_tick,
        "instance_id": instance_id,
        "streams": streams,
        "agents": {},
        "artifacts": {},
    }
    (session_dir / "manifest.json").write_text(
        json.dumps(manifest), encoding="utf-8"
    )


def _state_log_entry(
    *,
    tick: int,
    sequence_number: int,
    agent_id: str = "agent-1",
    artifact_id: str = "art-1",
    from_state: str = "INVALID",
    to_state: str = "EXCLUSIVE",
    trigger: str = "write",
    version: int = 1,
    instance_id: str = "instance-A",
) -> dict[str, Any]:
    return {
        "tick": tick,
        "artifact_id": artifact_id,
        "agent_id": agent_id,
        "agent_name": agent_id,
        "from_state": from_state,
        "to_state": to_state,
        "trigger": trigger,
        "version": version,
        "content_hash": "abc",
        "sequence_number": sequence_number,
        "instance_id": instance_id,
        "schema_version": "ccs.state_log.v2",
    }


def _audit_entry(
    *,
    tick: int,
    sequence_number: int,
    agent_id: str | None = "agent-1",
    artifact_id: str = "art-1",
    version: int = 1,
    outcome: str = "content",
    instance_id: str = "instance-A",
) -> dict[str, Any]:
    return {
        "tick": tick,
        "agent_id": agent_id,
        "agent_name": agent_id,
        "artifact_id": artifact_id,
        "version": version,
        "content_hash": "abc",
        "source": "fetch",
        "outcome": outcome,
        "sequence_number": sequence_number,
        "instance_id": instance_id,
        "schema_version": "ccs.content_audit.v1",
    }


def _write_jsonl(session_dir: Path, name: str, entries: list[dict]) -> None:
    path = session_dir / f"{name}.jsonl"
    with path.open("w", encoding="utf-8") as fh:
        for entry in entries:
            fh.write(json.dumps(entry) + "\n")


def _clean_session(tmp_path: Path) -> Path:
    """Single agent acquires E, commits, releases. Zero invariant breaches."""
    session = tmp_path / "clean"
    _write_manifest(session, streams=["state_log", "content_audit_log"])
    _write_jsonl(session, "state_log", [
        _state_log_entry(
            tick=1, sequence_number=1,
            from_state="INVALID", to_state="EXCLUSIVE",
        ),
        _state_log_entry(
            tick=2, sequence_number=2,
            from_state="EXCLUSIVE", to_state="MODIFIED",
            trigger="commit", version=1,
        ),
        _state_log_entry(
            tick=3, sequence_number=3,
            from_state="MODIFIED", to_state="INVALID",
        ),
    ])
    _write_jsonl(session, "content_audit_log", [])
    return session


def _single_writer_breach_session(tmp_path: Path) -> Path:
    """Both agents hold MODIFIED simultaneously.

    Routed through EXCLUSIVE first so the commit's ``from_state``
    isn't INVALID — that would also trigger lost-write and the test
    couldn't assert "exactly one finding". Same routing for
    monotonic-version: versions 1 then 2 strictly increase.
    """
    session = tmp_path / "breach"
    _write_manifest(session, streams=["state_log"])
    _write_jsonl(session, "state_log", [
        _state_log_entry(
            tick=1, sequence_number=1, agent_id="agent-1",
            from_state="INVALID", to_state="EXCLUSIVE",
        ),
        _state_log_entry(
            tick=2, sequence_number=2, agent_id="agent-1",
            from_state="EXCLUSIVE", to_state="MODIFIED",
            trigger="commit", version=1,
        ),
        _state_log_entry(
            tick=3, sequence_number=3, agent_id="agent-2",
            from_state="INVALID", to_state="EXCLUSIVE",
        ),
        # agent-2 now reaches MODIFIED while agent-1 still holds MODIFIED.
        _state_log_entry(
            tick=4, sequence_number=4, agent_id="agent-2",
            from_state="EXCLUSIVE", to_state="MODIFIED",
            trigger="commit", version=2,
        ),
    ])
    return session


def _ambiguous_session(tmp_path: Path, *, count: int = 1) -> Path:
    """Same-tick read + commit produces ``count`` AMBIGUOUS stale-reads.

    Writer goes INVALID -> EXCLUSIVE -> MODIFIED so the commit's
    ``from_state == EXCLUSIVE`` doesn't trip lost-write. Stale-read
    needs a prior committed version baseline: tick 1 seeds v1, tick 5
    commits v2; same-tick readers at tick 5 observing v1 produce
    AMBIGUOUS findings.
    """
    session = tmp_path / f"ambig-{count}"
    _write_manifest(session, streams=["state_log", "content_audit_log"])
    state_entries = [
        # Seed v1 cleanly.
        _state_log_entry(
            tick=1, sequence_number=1, agent_id="writer",
            from_state="INVALID", to_state="EXCLUSIVE",
        ),
        _state_log_entry(
            tick=1, sequence_number=2, agent_id="writer",
            from_state="EXCLUSIVE", to_state="MODIFIED",
            trigger="commit", version=1,
        ),
        _state_log_entry(
            tick=4, sequence_number=3, agent_id="writer",
            from_state="MODIFIED", to_state="EXCLUSIVE",
        ),
        # Tick 5 commits v2 — the version-2 commit reader collisions race.
        _state_log_entry(
            tick=5, sequence_number=4, agent_id="writer",
            from_state="EXCLUSIVE", to_state="MODIFIED",
            trigger="commit", version=2,
        ),
    ]
    # Same-tick (tick=5) reader observations of stale version=1: AMBIGUOUS.
    audit_entries = [
        _audit_entry(
            tick=5, sequence_number=10 + i,
            agent_id=f"reader-{i}", version=1,
        )
        for i in range(count)
    ]
    _write_jsonl(session, "state_log", state_entries)
    _write_jsonl(session, "content_audit_log", audit_entries)
    return session


def _opted_out_session(tmp_path: Path) -> Path:
    """Manifest declares only ``state_log`` — stale-read SKIPPED with opted_out=True."""
    session = tmp_path / "opted-out"
    _write_manifest(session, streams=["state_log"])
    _write_jsonl(session, "state_log", [
        _state_log_entry(
            tick=1, sequence_number=1,
            from_state="INVALID", to_state="EXCLUSIVE",
        ),
    ])
    return session


def _capture_bug_session(tmp_path: Path) -> Path:
    """Manifest declares content_audit_log but the file is missing."""
    session = tmp_path / "capture-bug"
    _write_manifest(session, streams=["state_log", "content_audit_log"])
    _write_jsonl(session, "state_log", [
        _state_log_entry(
            tick=1, sequence_number=1,
            from_state="INVALID", to_state="EXCLUSIVE",
        ),
    ])
    # content_audit_log.jsonl deliberately NOT written.
    return session


def _multi_instance_session(tmp_path: Path) -> Path:
    session = tmp_path / "multi-instance"
    _write_manifest(session, streams=["state_log"])
    _write_jsonl(session, "state_log", [
        _state_log_entry(tick=1, sequence_number=1, instance_id="instance-A"),
        _state_log_entry(tick=2, sequence_number=2, instance_id="instance-B"),
    ])
    return session


def _trace_corruption_session(tmp_path: Path) -> Path:
    session = tmp_path / "corrupt"
    _write_manifest(session, streams=["state_log"])
    _write_jsonl(session, "state_log", [
        _state_log_entry(tick=1, sequence_number=5),
        _state_log_entry(tick=2, sequence_number=5),  # duplicate seq
    ])
    return session


# ---------------------------------------------------------------------------
# Parser smoke
# ---------------------------------------------------------------------------


def test_build_parser_help_string_mentions_replay_format() -> None:
    parser = build_parser()
    help_text = parser.format_help()
    assert "agent-coherence-replay" in help_text
    assert "replay_trace_format.md" in help_text


# ---------------------------------------------------------------------------
# Happy paths
# ---------------------------------------------------------------------------


class TestCleanTrace:
    """Exit 0: clean trace + stream opt-outs allowed."""

    def test_clean_human_exits_zero(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str],
    ) -> None:
        session = _clean_session(tmp_path)
        rc = main([str(session)])
        out = capsys.readouterr().out
        assert rc == 0
        assert "0 CONFIRMED" in out
        assert "0 AMBIGUOUS" in out

    def test_clean_json_exits_zero_with_summary_only(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str],
    ) -> None:
        session = _clean_session(tmp_path)
        rc = main([str(session), "--json"])
        out = capsys.readouterr().out
        lines = [line for line in out.strip().split("\n") if line]
        assert rc == 0
        # Zero per-finding lines; one summary line.
        assert len(lines) == 1
        summary = json.loads(lines[0])
        assert summary["kind"] == "summary"
        assert summary["counts"] == {"CONFIRMED": 0, "AMBIGUOUS": 0, "SKIPPED": 0}
        assert summary["ambiguous_threshold"] == 10
        assert summary["ambiguous_callout"] is None
        # Spec §7.2 — trace_metadata must echo manifest fields.
        meta = summary["trace_metadata"]
        assert meta["adapter_type"] == "test-fixture"
        assert meta["instance_id"] == "instance-A"
        assert "state_log" in meta["streams_present"]

    def test_quiet_clean_emits_nothing(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str],
    ) -> None:
        session = _clean_session(tmp_path)
        rc = main([str(session), "--quiet"])
        captured = capsys.readouterr()
        assert rc == 0
        assert captured.out == ""


# ---------------------------------------------------------------------------
# CONFIRMED breach (exit 1)
# ---------------------------------------------------------------------------


class TestConfirmedBreach:
    """Exit 1: at least one CONFIRMED finding."""

    def test_single_writer_breach_exits_one(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str],
    ) -> None:
        session = _single_writer_breach_session(tmp_path)
        rc = main([str(session)])
        out = capsys.readouterr().out
        assert rc == 1
        assert "[CONFIRMED]" in out
        assert "single-writer" in out
        # Two transitions land overlapping ownership: one when agent-2
        # takes EXCLUSIVE while agent-1 holds MODIFIED, another when
        # agent-2 reaches MODIFIED. Both are legitimate CONFIRMED.
        assert "CONFIRMED" in out

    def test_breach_json_emits_per_finding_line(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str],
    ) -> None:
        session = _single_writer_breach_session(tmp_path)
        rc = main([str(session), "--json"])
        out = capsys.readouterr().out
        lines = [line for line in out.strip().split("\n") if line]
        assert rc == 1
        # At least one finding + summary as last line.
        assert len(lines) >= 2
        finding = json.loads(lines[0])
        assert finding["kind"] == "finding"
        assert finding["severity"] == "CONFIRMED"
        assert finding["invariant"] == "single-writer"
        summary = json.loads(lines[-1])
        assert summary["kind"] == "summary"
        assert summary["counts"]["CONFIRMED"] >= 1


# ---------------------------------------------------------------------------
# AMBIGUOUS handling
# ---------------------------------------------------------------------------


class TestAmbiguousSuppression:
    """Default suppression + --include-ambiguous override."""

    def test_ambiguous_suppressed_by_default(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str],
    ) -> None:
        session = _ambiguous_session(tmp_path, count=1)
        rc = main([str(session)])
        out = capsys.readouterr().out
        assert rc == 0
        assert "[AMBIGUOUS]" not in out
        # Summary still counts the AMBIGUOUS finding.
        assert "1 AMBIGUOUS (suppressed)" in out

    def test_include_ambiguous_shows_per_finding(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str],
    ) -> None:
        session = _ambiguous_session(tmp_path, count=1)
        rc = main([str(session), "--include-ambiguous"])
        out = capsys.readouterr().out
        assert rc == 0
        assert "[AMBIGUOUS]" in out
        assert "1 AMBIGUOUS (shown)" in out

    def test_ambiguous_json_suppressed_in_per_finding(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str],
    ) -> None:
        session = _ambiguous_session(tmp_path, count=1)
        rc = main([str(session), "--json"])
        out = capsys.readouterr().out
        lines = [line for line in out.strip().split("\n") if line]
        assert rc == 0
        # Only summary; per-finding suppressed.
        assert len(lines) == 1
        summary = json.loads(lines[0])
        assert summary["counts"]["AMBIGUOUS"] == 1

    def test_ambiguous_json_with_flag_shows_finding(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str],
    ) -> None:
        session = _ambiguous_session(tmp_path, count=1)
        rc = main([str(session), "--json", "--include-ambiguous"])
        out = capsys.readouterr().out
        lines = [line for line in out.strip().split("\n") if line]
        assert rc == 0
        assert len(lines) == 2
        finding = json.loads(lines[0])
        assert finding["severity"] == "AMBIGUOUS"


class TestAmbiguousThreshold:
    """Callout fires when count exceeds threshold; absent when below."""

    def test_callout_fires_when_exceeding_default(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str],
    ) -> None:
        session = _ambiguous_session(tmp_path, count=11)
        rc = main([str(session)])
        out = capsys.readouterr().out
        assert rc == 0
        assert "exceed threshold" in out
        assert "--include-ambiguous" in out
        assert "global sequence_number" in out

    def test_callout_absent_when_below_custom_threshold(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str],
    ) -> None:
        session = _ambiguous_session(tmp_path, count=11)
        rc = main([str(session), "--ambiguous-threshold", "100"])
        out = capsys.readouterr().out
        assert rc == 0
        assert "exceed threshold" not in out

    def test_callout_in_json_summary(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str],
    ) -> None:
        session = _ambiguous_session(tmp_path, count=11)
        rc = main([str(session), "--json"])
        out = capsys.readouterr().out
        summary = json.loads(out.strip().split("\n")[-1])
        assert rc == 0
        assert summary["ambiguous_callout"] is not None
        assert "--include-ambiguous" in summary["ambiguous_callout"]


# ---------------------------------------------------------------------------
# --invariant restriction
# ---------------------------------------------------------------------------


class TestInvariantRestriction:
    """--invariant LIMITS both findings AND skip list."""

    def test_only_named_invariants_run(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str],
    ) -> None:
        session = _clean_session(tmp_path)
        rc = main([str(session), "--invariant", "lost-write", "--json"])
        out = capsys.readouterr().out
        summary = json.loads(out.strip().split("\n")[-1])
        # No findings (clean), no skips (stale-read predicate not selected
        # at all, so its missing-stream is NOT in summary).
        assert rc == 0
        assert summary["counts"]["SKIPPED"] == 0
        assert summary["skipped_reasons"] == []


# ---------------------------------------------------------------------------
# SKIPPED-only paths
# ---------------------------------------------------------------------------


class TestSkippedOptedOut:
    """Exit 0: stream opt-out declared in manifest."""

    def test_opted_out_skip_exits_zero(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str],
    ) -> None:
        session = _opted_out_session(tmp_path)
        rc = main([str(session)])
        out = capsys.readouterr().out
        assert rc == 0
        assert "SKIPPED" in out
        assert "opted out" in out

    def test_opted_out_skip_json_marks_opted_out_true(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str],
    ) -> None:
        session = _opted_out_session(tmp_path)
        rc = main([str(session), "--json"])
        summary = json.loads(capsys.readouterr().out.strip().split("\n")[-1])
        assert rc == 0
        skipped = summary["skipped_reasons"]
        assert len(skipped) == 1
        assert skipped[0]["invariant"] == "stale-read"
        assert skipped[0]["opted_out"] is True


class TestSkippedCaptureBug:
    """Exit 2: manifest declared the stream but file is missing."""

    def test_capture_bug_skip_exits_two(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str],
    ) -> None:
        session = _capture_bug_session(tmp_path)
        rc = main([str(session)])
        out = capsys.readouterr().out
        assert rc == 2
        # Reason must say "capture-side bug" so triage isn't ambiguous.
        assert "capture" in out.lower()

    def test_capture_bug_json_marks_opted_out_false(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str],
    ) -> None:
        session = _capture_bug_session(tmp_path)
        rc = main([str(session), "--json"])
        summary = json.loads(capsys.readouterr().out.strip().split("\n")[-1])
        assert rc == 2
        skipped = summary["skipped_reasons"]
        assert any(
            s["invariant"] == "stale-read" and s["opted_out"] is False
            for s in skipped
        )


# ---------------------------------------------------------------------------
# Trace errors (exit 3)
# ---------------------------------------------------------------------------


class TestTraceErrors:
    """Exit 3 for the three loader/iterator error classes."""

    def test_missing_manifest_exits_three(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str],
    ) -> None:
        session = tmp_path / "nonexistent"
        rc = main([str(session)])
        captured = capsys.readouterr()
        assert rc == 3
        # Pre-flight fires before the loader for a nonexistent directory.
        assert "session directory not found" in captured.err or str(session) in captured.err
        # No Python traceback leakage to stderr.
        assert "Traceback" not in captured.err

    def test_multi_instance_exits_three(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str],
    ) -> None:
        session = _multi_instance_session(tmp_path)
        rc = main([str(session)])
        captured = capsys.readouterr()
        assert rc == 3
        assert "instance" in captured.err.lower()
        # Loader error msg points at D+1 roadmap.
        assert "D+1" in captured.err
        assert "Traceback" not in captured.err

    def test_trace_corruption_exits_three(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str],
    ) -> None:
        session = _trace_corruption_session(tmp_path)
        rc = main([str(session)])
        captured = capsys.readouterr()
        assert rc == 3
        assert "duplicate" in captured.err.lower() or "sequence_number" in captured.err
        assert "Traceback" not in captured.err

    # ----- Gated #15: --json error envelope on exit 3 -----

    def test_multi_instance_json_emits_error_envelope_on_stdout(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str],
    ) -> None:
        session = _multi_instance_session(tmp_path)
        rc = main([str(session), "--json"])
        captured = capsys.readouterr()
        assert rc == 3
        # stdout has exactly one JSON line — the error envelope.
        stdout_lines = [line for line in captured.out.splitlines() if line.strip()]
        assert len(stdout_lines) == 1
        envelope = json.loads(stdout_lines[0])
        assert envelope["kind"] == "error"
        assert envelope["exit_code"] == 3
        assert envelope["exception"] == "MultiInstanceTraceError"
        assert isinstance(envelope["message"], str)
        # Carries the actionable D+1 roadmap pointer surfaced by the loader.
        assert "D+1" in envelope["message"]
        # stderr prose retained for human log tailers.
        assert "agent-coherence-replay" in captured.err
        assert "Traceback" not in captured.err

    def test_trace_corruption_json_emits_error_envelope_on_stdout(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str],
    ) -> None:
        session = _trace_corruption_session(tmp_path)
        rc = main([str(session), "--json"])
        captured = capsys.readouterr()
        assert rc == 3
        stdout_lines = [line for line in captured.out.splitlines() if line.strip()]
        assert len(stdout_lines) == 1
        envelope = json.loads(stdout_lines[0])
        assert envelope["kind"] == "error"
        assert envelope["exit_code"] == 3
        assert envelope["exception"] == "TraceCorruptionError"
        assert isinstance(envelope["message"], str)
        assert "agent-coherence-replay" in captured.err
        assert "Traceback" not in captured.err

    def test_missing_session_dir_json_emits_error_envelope_on_stdout(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str],
    ) -> None:
        session = tmp_path / "nonexistent"
        rc = main([str(session), "--json"])
        captured = capsys.readouterr()
        assert rc == 3
        stdout_lines = [line for line in captured.out.splitlines() if line.strip()]
        assert len(stdout_lines) == 1
        envelope = json.loads(stdout_lines[0])
        assert envelope["kind"] == "error"
        assert envelope["exit_code"] == 3
        # Route (a): pre-flight raises SessionDirectoryNotFoundError so
        # the envelope class name is consistent with the trace-error catch.
        assert envelope["exception"] == "SessionDirectoryNotFoundError"
        assert "session directory not found" in envelope["message"]
        assert "agent-coherence-replay" in captured.err
        assert "Traceback" not in captured.err

    def test_no_json_flag_keeps_stdout_empty_on_trace_error(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Regression guard: WITHOUT ``--json``, stdout stays empty on
        exit 3 — the envelope is a ``--json``-only addition; no behavior
        change for the default human path.
        """
        session = _multi_instance_session(tmp_path)
        rc = main([str(session)])  # no --json
        captured = capsys.readouterr()
        assert rc == 3
        assert captured.out.strip() == ""  # stdout still empty
        assert "agent-coherence-replay" in captured.err  # prose still on stderr

    def test_session_dir_not_found_is_replay_trace_error_subclass(self) -> None:
        """``SessionDirectoryNotFoundError`` must inherit
        ``ReplayTraceError`` so the outer catch and the JSON envelope
        both fire (Gated #15 route (a) invariant).
        """
        from ccs.replay import ReplayTraceError, SessionDirectoryNotFoundError
        assert issubclass(SessionDirectoryNotFoundError, ReplayTraceError)


# ---------------------------------------------------------------------------
# Exception envelope (Gated #1) — BrokenPipeError + uncaught Exception
# ---------------------------------------------------------------------------


class TestExceptionEnvelope:
    """BrokenPipeError -> exit 0; any other uncaught Exception -> exit 4.

    Decouples CLI bugs / pipe-close from the CONFIRMED breach signal
    (exit 1) so agent consumers can triage cleanly.
    """

    def test_broken_pipe_exits_zero(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        # A consumer that closes the pipe early (e.g. ``| head -1``)
        # surfaces as BrokenPipeError on the next write. Replay treats
        # it as benign — exit 0, not a failure.
        session = _clean_session(tmp_path)

        def raise_broken_pipe(*_args: Any, **_kwargs: Any) -> int:
            raise BrokenPipeError(32, "Broken pipe")

        monkeypatch.setattr("ccs.cli.coherence_replay._run", raise_broken_pipe)
        rc = main([str(session)])
        assert rc == 0

    def test_internal_error_exits_four_with_stderr_message(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        # An uncaught exception inside _run (loader bug, predicate
        # crash, etc.) maps to exit 4 with a typed stderr line and
        # no Python traceback.
        session = _clean_session(tmp_path)

        def raise_internal(*_args: Any, **_kwargs: Any) -> int:
            raise RuntimeError("simulated CLI internal error")

        monkeypatch.setattr("ccs.cli.coherence_replay._run", raise_internal)
        rc = main([str(session)])
        captured = capsys.readouterr()
        assert rc == 4
        assert "internal error" in captured.err
        assert "RuntimeError" in captured.err
        assert "simulated CLI internal error" in captured.err
        assert "Traceback" not in captured.err

    def test_internal_error_distinct_from_confirmed_breach(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        # Regression guard: exit 4 must not collide with exit 1
        # (CONFIRMED breach) so agents distinguish "tool crashed" from
        # "real coordination defect found."
        session = _clean_session(tmp_path)

        def raise_internal(*_args: Any, **_kwargs: Any) -> int:
            raise ValueError("predicate fold bug")

        monkeypatch.setattr("ccs.cli.coherence_replay._run", raise_internal)
        rc = main([str(session)])
        assert rc != 1
        assert rc == 4


# ---------------------------------------------------------------------------
# JSON schema conformance
# ---------------------------------------------------------------------------


class TestJsonSchemaConformance:
    """Structural validation of the §7 schema against a synthesized trace."""

    def test_summary_fields_present_and_well_typed(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str],
    ) -> None:
        session = _clean_session(tmp_path)
        rc = main([str(session), "--json"])
        out = capsys.readouterr().out
        summary = json.loads(out.strip().split("\n")[-1])
        assert rc == 0
        # §7.2 required fields:
        for field in (
            "kind", "counts", "counts_by_invariant", "skipped_reasons",
            "ambiguous_threshold", "ambiguous_callout", "trace_metadata",
        ):
            assert field in summary, f"missing field {field}"
        assert summary["kind"] == "summary"
        assert isinstance(summary["counts"], dict)
        assert isinstance(summary["counts_by_invariant"], dict)
        assert isinstance(summary["skipped_reasons"], list)
        assert isinstance(summary["ambiguous_threshold"], int)
        # ambiguous_callout is string OR null per spec
        assert summary["ambiguous_callout"] is None or isinstance(
            summary["ambiguous_callout"], str
        )
        assert isinstance(summary["trace_metadata"], dict)
        # counts_by_invariant must cover all four canonical names
        for name in (
            "single-writer", "monotonic-version", "stale-read", "lost-write",
        ):
            assert name in summary["counts_by_invariant"]
            row = summary["counts_by_invariant"][name]
            for sev in ("CONFIRMED", "AMBIGUOUS", "SKIPPED"):
                assert sev in row
                assert isinstance(row[sev], int)

    def test_per_finding_fields_present_and_well_typed(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str],
    ) -> None:
        session = _single_writer_breach_session(tmp_path)
        rc = main([str(session), "--json"])
        out = capsys.readouterr().out
        first_line = out.strip().split("\n")[0]
        finding = json.loads(first_line)
        assert rc == 1
        # §7.1 required fields:
        for field in (
            "kind", "severity", "invariant", "agents", "artifacts",
            "tick_range", "context", "details",
        ):
            assert field in finding, f"missing field {field}"
        assert finding["kind"] == "finding"
        assert finding["severity"] in {"CONFIRMED", "AMBIGUOUS"}
        assert isinstance(finding["agents"], list)
        assert isinstance(finding["artifacts"], list)
        assert isinstance(finding["tick_range"], dict)
        assert "start" in finding["tick_range"]
        assert "end" in finding["tick_range"]
        assert isinstance(finding["tick_range"]["start"], int)
        assert isinstance(finding["tick_range"]["end"], int)
        assert isinstance(finding["context"], dict)
        assert isinstance(finding["details"], dict)


# ---------------------------------------------------------------------------
# Quiet mode with breaches
# ---------------------------------------------------------------------------


def test_quiet_with_breach_emits_only_findings(
    tmp_path: Path, capsys: pytest.CaptureFixture[str],
) -> None:
    """--quiet + CONFIRMED breach: finding block present, summary absent."""
    session = _single_writer_breach_session(tmp_path)
    rc = main([str(session), "--quiet"])
    out = capsys.readouterr().out
    assert rc == 1
    assert "[CONFIRMED]" in out
    # Summary block suppressed under --quiet
    assert "Summary:" not in out


def test_quiet_with_capture_bug_emits_skip_lines(
    tmp_path: Path, capsys: pytest.CaptureFixture[str],
) -> None:
    """--quiet + capture-bug SKIP: still exits 2 (cron catches it)."""
    session = _capture_bug_session(tmp_path)
    rc = main([str(session), "--quiet"])
    captured = capsys.readouterr()
    # Exit code is the load-bearing signal here; stdout content is
    # secondary under --quiet. The important assertion is that the
    # capture-bug skip is NOT silenced into a clean exit 0.
    assert rc == 2
    assert "Traceback" not in captured.err


# ---------------------------------------------------------------------------
# --quiet --json combination
# ---------------------------------------------------------------------------


def test_quiet_json_clean_trace_emits_nothing(
    tmp_path: Path, capsys: pytest.CaptureFixture[str],
) -> None:
    """--quiet --json on a clean trace: no output (cron-friendly zero signal)."""
    session = _clean_session(tmp_path)
    rc = main([str(session), "--quiet", "--json"])
    captured = capsys.readouterr()
    assert rc == 0
    assert captured.out == "", "clean trace with --quiet --json must produce no output"


def test_quiet_json_breach_emits_findings_and_summary(
    tmp_path: Path, capsys: pytest.CaptureFixture[str],
) -> None:
    """--quiet --json with CONFIRMED breach: full JSON output still emitted."""
    session = _single_writer_breach_session(tmp_path)
    rc = main([str(session), "--quiet", "--json"])
    out = capsys.readouterr().out
    assert rc == 1
    lines = [l for l in out.strip().split("\n") if l]
    assert len(lines) >= 2, "expected at least one finding line + summary line"
    objects = [json.loads(l) for l in lines]
    kinds = {o.get("kind") for o in objects}
    assert "finding" in kinds
    assert "summary" in kinds
