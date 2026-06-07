# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""Tests for the replay capture surface (Unit 2 of D v1).

Covers the contract documented in
``docs/proposals/replay_trace_format.md`` from the producer side:

- ``record_callbacks`` helper opt-in gate + composition + tuple yield
- ``CCSStore.record_to`` thin wrapper
- Manifest header + finalize on enter/exit
- fsync-per-line discipline (call-count assertion, not subprocess kill)
- ``streams=`` opt-out preserving ``retain_versions=True`` on CCSStore
- Multi-instance ``instance_id`` stderr warning at capture time
- Rollback durability via fsync-before-failure
"""

from __future__ import annotations

import json
import os
import stat
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

from ccs.replay import (
    SessionDirectoryNotEmptyError,
    UnverifiedAdapterCaptureError,
    record_callbacks,
)
from ccs.replay.recorder import (
    DEFAULT_STREAMS,
    SCHEMA_NOTE,
    SCHEMA_VERSION,
)

# ---------------------------------------------------------------------------
# Test fixtures
# ---------------------------------------------------------------------------


def _state_log_entry(
    *,
    tick: int = 1,
    instance_id: str = "instance-A",
    sequence_number: int = 1,
) -> dict:
    """Synthetic state_log entry shaped per replay_trace_format §3."""
    return {
        "tick": tick,
        "artifact_id": "art-1",
        "agent_id": "agent-1",
        "agent_name": "researcher",
        "from_state": "INVALID",
        "to_state": "EXCLUSIVE",
        "trigger": "write",
        "version": 1,
        "content_hash": "abc",
        "sequence_number": sequence_number,
        "instance_id": instance_id,
        "schema_version": "ccs.state_log.v2",
    }


def _audit_entry(
    *,
    tick: int = 1,
    instance_id: str = "instance-A",
    sequence_number: int = 1,
) -> dict:
    """Synthetic content_audit_log entry shaped per replay_trace_format §4."""
    return {
        "tick": tick,
        "agent_id": "agent-1",
        "agent_name": "researcher",
        "artifact_id": "art-1",
        "version": 1,
        "content_hash": "abc",
        "source": "fetch",
        "outcome": "content",
        "sequence_number": sequence_number,
        "instance_id": instance_id,
        "schema_version": "ccs.content_audit.v1",
    }


def _read_jsonl(path: Path) -> list[dict]:
    with path.open("r") as fh:
        return [json.loads(line) for line in fh if line.strip()]


# ---------------------------------------------------------------------------
# record_callbacks — opt-in gate
# ---------------------------------------------------------------------------


class TestUnverifiedOptIn:
    """The opt-in gate lives on the helper, not the CCSStore wrapper."""

    def test_helper_without_opt_in_raises(self, tmp_path: Path) -> None:
        with pytest.raises(UnverifiedAdapterCaptureError):
            with record_callbacks(tmp_path / "session"):
                pass

    def test_helper_with_opt_in_succeeds(self, tmp_path: Path) -> None:
        with record_callbacks(
            tmp_path / "session", accept_unverified=True
        ) as (state_cb, audit_cb):
            assert callable(state_cb)
            assert callable(audit_cb)

    def test_helper_emits_unverified_stderr(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        with record_callbacks(tmp_path / "session", accept_unverified=True):
            pass
        captured = capsys.readouterr()
        assert "unverified" in captured.err
        assert "CrewAI/AutoGen" in captured.err


# ---------------------------------------------------------------------------
# Manifest header + finalize
# ---------------------------------------------------------------------------


class TestManifestLifecycle:
    """Manifest written on enter (header), rewritten atomically on exit."""

    def test_header_written_on_enter(self, tmp_path: Path) -> None:
        session_dir = tmp_path / "session"
        with record_callbacks(session_dir, accept_unverified=True):
            manifest = json.loads((session_dir / "manifest.json").read_text())
            assert manifest["schema_version"] == SCHEMA_VERSION
            assert manifest["schema_note"] == SCHEMA_NOTE
            assert manifest["adapter_type"] == "coherence-adapter-core"
            assert set(manifest["streams"]) == DEFAULT_STREAMS

    def test_finalized_fields_on_exit(self, tmp_path: Path) -> None:
        session_dir = tmp_path / "session"
        with record_callbacks(
            session_dir, accept_unverified=True
        ) as (state_cb, _audit_cb):
            state_cb(_state_log_entry(tick=5, instance_id="inst-X"))
            state_cb(_state_log_entry(tick=11, instance_id="inst-X", sequence_number=2))

        manifest = json.loads((session_dir / "manifest.json").read_text())
        assert manifest["start_tick"] == 5
        assert manifest["end_tick"] == 11
        assert manifest["instance_id"] == "inst-X"

    def test_no_events_yields_zero_ticks(self, tmp_path: Path) -> None:
        session_dir = tmp_path / "session"
        with record_callbacks(session_dir, accept_unverified=True):
            pass
        manifest = json.loads((session_dir / "manifest.json").read_text())
        assert manifest["start_tick"] == 0
        assert manifest["end_tick"] == 0
        assert manifest["instance_id"] is None
        assert manifest["agents"] == {}
        assert manifest["artifacts"] == {}


# ---------------------------------------------------------------------------
# Existing-path safety (Gated #2) — refuse-if-exists
# ---------------------------------------------------------------------------


class TestExistingPathSafety:
    """A pre-existing manifest.json in session_dir means a second capture
    would silently interleave entries from two coordinator instances.
    Refuse at __enter__ so the failure surfaces before any new entries
    land on disk."""

    def test_pre_existing_manifest_raises(self, tmp_path: Path) -> None:
        session_dir = tmp_path / "session"
        # First capture completes normally — leaves a manifest behind.
        with record_callbacks(session_dir, accept_unverified=True):
            pass
        assert (session_dir / "manifest.json").exists()

        # Second capture against the same path must refuse.
        with pytest.raises(SessionDirectoryNotEmptyError) as exc_info:
            with record_callbacks(session_dir, accept_unverified=True):
                pass
        msg = str(exc_info.value)
        assert "manifest.json" in msg
        assert str(session_dir) in msg
        assert "delete" in msg.lower() or "choose a different path" in msg.lower()

    def test_pre_existing_manifest_does_not_overwrite_state_log(
        self, tmp_path: Path,
    ) -> None:
        # Regression guard: the refuse-if-exists check fires BEFORE any
        # stream writers open, so the prior state_log.jsonl is untouched
        # when the second capture is rejected.
        session_dir = tmp_path / "session"
        with record_callbacks(
            session_dir, accept_unverified=True
        ) as (state_cb, _audit_cb):
            state_cb(_state_log_entry(tick=1, instance_id="inst-original"))
        original_state = (session_dir / "state_log.jsonl").read_bytes()

        with pytest.raises(SessionDirectoryNotEmptyError):
            with record_callbacks(session_dir, accept_unverified=True):
                pass

        # state_log.jsonl is byte-identical to what the first capture
        # wrote — no append from the rejected second attempt.
        assert (session_dir / "state_log.jsonl").read_bytes() == original_state

    def test_empty_directory_succeeds(self, tmp_path: Path) -> None:
        # Pre-existing empty directory is fine — only manifest.json
        # presence triggers the refusal.
        session_dir = tmp_path / "session"
        session_dir.mkdir()
        with record_callbacks(session_dir, accept_unverified=True):
            pass
        assert (session_dir / "manifest.json").exists()

    def test_pre_existing_jsonl_without_manifest_succeeds(
        self, tmp_path: Path,
    ) -> None:
        # A stray state_log.jsonl without a manifest is treated as
        # incidental clutter — the manifest is the authoritative
        # "session exists" marker. (The session's __enter__ opens the
        # JSONL in append mode, so a partner pre-staging garbage data
        # there would corrupt the trace, but that's a partner-side
        # documentation matter, not something the refuse-if-exists
        # gate is for.)
        session_dir = tmp_path / "session"
        session_dir.mkdir()
        (session_dir / "state_log.jsonl").write_text("preexisting garbage\n")
        with record_callbacks(session_dir, accept_unverified=True):
            pass
        assert (session_dir / "manifest.json").exists()


# ---------------------------------------------------------------------------
# __enter__ fd cleanup on manifest write failure (Gated #4)
# ---------------------------------------------------------------------------


class TestEnterFdCleanup:
    """If _atomic_write_manifest raises during __enter__, opened stream
    writers must be closed before re-raising. Python's context-manager
    protocol does NOT call __exit__ when __enter__ raises, so without
    explicit cleanup the file descriptors would leak.
    """

    def test_manifest_write_failure_closes_opened_writers(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        # Patch _atomic_write_manifest to raise on the __enter__ call
        # (after writers are opened). The fix must close every writer
        # in self._writers before re-raising.
        from ccs.replay import recorder as recorder_mod

        opened_writers: list[object] = []
        original_open_stream = recorder_mod._open_stream

        def tracking_open_stream(*args: Any, **kwargs: Any) -> object:
            writer = original_open_stream(*args, **kwargs)
            opened_writers.append(writer)
            return writer

        monkeypatch.setattr(recorder_mod, "_open_stream", tracking_open_stream)

        def raise_on_manifest(*_a: Any, **_kw: Any) -> None:
            raise OSError("simulated disk full on manifest tempfile")

        monkeypatch.setattr(
            recorder_mod, "_atomic_write_manifest", raise_on_manifest
        )

        session_dir = tmp_path / "session"
        try:
            with record_callbacks(session_dir, accept_unverified=True):
                pytest.fail("__enter__ should have raised before yielding")
        except OSError as exc:
            assert "simulated disk full" in str(exc)

        # Writers were opened (the fixture proves __enter__ reached the
        # _atomic_write_manifest call) AND every writer is now closed.
        assert len(opened_writers) > 0, "no writers were opened — fixture bug"
        for writer in opened_writers:
            # _StreamWriter.close() is idempotent; we assert closed-state
            # via the underlying file handle attribute (active writers
            # carry a non-None handle; closed ones are None).
            handle = getattr(writer, "_handle", None)
            assert handle is None or handle.closed, (
                f"writer {writer!r} left an open file descriptor after "
                f"__enter__ failure — fd leak"
            )


# ---------------------------------------------------------------------------
# fsync-per-line contract
# ---------------------------------------------------------------------------


class TestFsyncContract:
    """Each JSONL line write triggers exactly one fsync.

    Per the plan: durability proof via call-count, not subprocess kill.
    A failed callback raises before the next write, but the fsync that
    fired on prior lines means those entries are durably on disk.
    """

    def test_fsync_once_per_line(self, tmp_path: Path) -> None:
        session_dir = tmp_path / "session"
        line_count = 5
        with patch("ccs.replay.recorder.os.fsync", wraps=os.fsync) as mock_fsync:
            with record_callbacks(
                session_dir, accept_unverified=True
            ) as (state_cb, audit_cb):
                # Reset after enter (manifest open may have fsync'd via the
                # tempfile + replace path — count only stream-line syncs).
                mock_fsync.reset_mock()
                for i in range(line_count):
                    state_cb(_state_log_entry(tick=i + 1, sequence_number=i + 1))
                for i in range(line_count):
                    audit_cb(_audit_entry(tick=i + 1, sequence_number=i + 1))
                stream_call_count = mock_fsync.call_count

        # Each of the 10 line writes (5 state + 5 audit) triggered one fsync.
        # Additional fsync calls after this point come from manifest atomic
        # rewrite on __exit__ — not counted here.
        assert stream_call_count == 2 * line_count


# ---------------------------------------------------------------------------
# streams= opt-out semantics
# ---------------------------------------------------------------------------


class TestStreamsOptOut:
    """streams={'state_log'} suppresses the audit JSONL file but keeps
    the callback live for composition + bookkeeping."""

    def test_state_log_only_skips_audit_file(self, tmp_path: Path) -> None:
        session_dir = tmp_path / "session"
        with record_callbacks(
            session_dir, streams={"state_log"}, accept_unverified=True
        ) as (state_cb, audit_cb):
            state_cb(_state_log_entry())
            # Audit callback still callable; bookkeeping fires but no disk write.
            audit_cb(_audit_entry())

        assert (session_dir / "state_log.jsonl").exists()
        assert not (session_dir / "content_audit_log.jsonl").exists()

        manifest = json.loads((session_dir / "manifest.json").read_text())
        assert manifest["streams"] == ["state_log"]

    def test_audit_callback_is_no_op_when_opted_out(self, tmp_path: Path) -> None:
        """Wrapped audit callback exists (truthy) even when opted out —
        this preserves CCSStore's retain_versions=True semantics."""
        with record_callbacks(
            tmp_path / "session",
            streams={"state_log"},
            accept_unverified=True,
        ) as (_state_cb, audit_cb):
            assert audit_cb is not None
            assert callable(audit_cb)
            # Calling does not raise; bookkeeping only.
            audit_cb(_audit_entry())

    def test_state_log_opt_out_rejected(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError, match="state_log"):
            with record_callbacks(
                tmp_path / "session",
                streams={"content_audit_log"},
                accept_unverified=True,
            ):
                pass

    def test_unknown_stream_rejected(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError, match="Unknown streams"):
            with record_callbacks(
                tmp_path / "session",
                streams={"state_log", "transient_state_log"},
                accept_unverified=True,
            ):
                pass


# ---------------------------------------------------------------------------
# Callback composition
# ---------------------------------------------------------------------------


class TestCallbackComposition:
    """Caller callbacks compose with file writers — neither overrides."""

    def test_caller_state_log_fires_alongside_file_write(self, tmp_path: Path) -> None:
        captured: list[dict] = []
        session_dir = tmp_path / "session"
        with record_callbacks(
            session_dir,
            accept_unverified=True,
            state_log=captured.append,
        ) as (state_cb, _audit_cb):
            entry = _state_log_entry()
            state_cb(entry)

        assert captured == [_state_log_entry()]
        on_disk = _read_jsonl(session_dir / "state_log.jsonl")
        assert on_disk == [_state_log_entry()]

    def test_caller_audit_log_fires_alongside_file_write(self, tmp_path: Path) -> None:
        captured: list[dict] = []
        session_dir = tmp_path / "session"
        with record_callbacks(
            session_dir,
            accept_unverified=True,
            content_audit_log=captured.append,
        ) as (_state_cb, audit_cb):
            audit_cb(_audit_entry())

        assert len(captured) == 1
        on_disk = _read_jsonl(session_dir / "content_audit_log.jsonl")
        assert on_disk == captured


# ---------------------------------------------------------------------------
# Multi-instance warning
# ---------------------------------------------------------------------------


class TestMultiInstanceDetection:
    """A new instance_id mid-capture emits a stderr warning naming the
    MULTI_INSTANCE_TRACE D+1 roadmap item."""

    def test_instance_id_change_warns(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        session_dir = tmp_path / "session"
        with record_callbacks(
            session_dir, accept_unverified=True
        ) as (state_cb, _audit_cb):
            state_cb(_state_log_entry(instance_id="inst-A", sequence_number=1))
            state_cb(_state_log_entry(instance_id="inst-B", sequence_number=2))

        captured = capsys.readouterr()
        assert "MULTI_INSTANCE_TRACE" in captured.err

    def test_stable_instance_id_does_not_warn(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        session_dir = tmp_path / "session"
        with record_callbacks(
            session_dir, accept_unverified=True
        ) as (state_cb, _audit_cb):
            state_cb(_state_log_entry(instance_id="inst-A", sequence_number=1))
            state_cb(_state_log_entry(instance_id="inst-A", sequence_number=2))

        captured = capsys.readouterr()
        assert "MULTI_INSTANCE_TRACE" not in captured.err


# ---------------------------------------------------------------------------
# Rollback durability
# ---------------------------------------------------------------------------


class TestRollbackDurability:
    """A file-writer IOError propagates so the coordinator's _seq
    rollback fires. Prior successful writes are durable thanks to fsync."""

    def test_fsync_before_failure_keeps_prior_writes_durable(
        self, tmp_path: Path
    ) -> None:
        """Simulate: 1st line succeeds (fsync fires), 2nd line's
        os.write raises IOError; the helper propagates so the caller
        (coordinator) can roll back _seq. The 1st line is on disk."""
        session_dir = tmp_path / "session"

        # Track fsync — must fire on the durable first write.
        fsync_calls: list[int] = []
        original_fsync = os.fsync

        def tracking_fsync(fd: int) -> None:
            fsync_calls.append(fd)
            original_fsync(fd)

        original_write = os.write

        with record_callbacks(
            session_dir, accept_unverified=True
        ) as (state_cb, _audit_cb):
            with patch(
                "ccs.replay.recorder.os.fsync", side_effect=tracking_fsync
            ):
                # First line: real write + fsync.
                state_cb(_state_log_entry(sequence_number=1))
                fsync_after_first = len(fsync_calls)

                # Second line: os.write raises before fsync can fire.
                def flaky_write(fd: int, data: bytes) -> int:
                    raise OSError("disk full")

                with patch(
                    "ccs.replay.recorder.os.write", side_effect=flaky_write
                ):
                    with pytest.raises(OSError, match="disk full"):
                        state_cb(_state_log_entry(sequence_number=2))

                # No additional fsync between failed write and now —
                # fsync did NOT fire for the failed entry.
                assert len(fsync_calls) == fsync_after_first
                assert fsync_after_first >= 1

        # First write made it; second did not.
        on_disk = _read_jsonl(session_dir / "state_log.jsonl")
        assert len(on_disk) == 1
        assert on_disk[0]["sequence_number"] == 1
        # Suppress unused-name warning for original_write (kept available
        # if a future variant of this test wants to mix flaky/real writes).
        _ = original_write


# ---------------------------------------------------------------------------
# CCSStore.record_to thin wrapper
# ---------------------------------------------------------------------------


class TestCCSStoreRecordTo:
    """CCSStore.record_to is the verified LangGraph-shaped wrapper.

    It sets accept_unverified=True automatically and does NOT emit the
    unverified-adapter warning (CCSStore is verified in v1).
    """

    def setup_method(self) -> None:
        pytest.importorskip("langgraph.store.base")

    def test_yields_ccsstore_and_writes_manifest(self, tmp_path: Path) -> None:
        from ccs.adapters.ccsstore import CCSStore

        session_dir = tmp_path / "session"
        with CCSStore.record_to(session_dir) as store:
            from langgraph.store.base import PutOp

            store.batch([PutOp(namespace=("planner", "shared"), key="plan", value={"v": 1})])

        manifest = json.loads((session_dir / "manifest.json").read_text())
        assert manifest["adapter_type"] == "langgraph-ccsstore"
        assert set(manifest["streams"]) == DEFAULT_STREAMS
        assert manifest["instance_id"] is not None
        # The put registered an agent and an artifact — both must be
        # drained into the finalized manifest.
        assert manifest["agents"]
        assert manifest["artifacts"]
        assert (session_dir / "state_log.jsonl").exists()
        assert (session_dir / "content_audit_log.jsonl").exists()

    def test_does_not_emit_unverified_warning(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        from ccs.adapters.ccsstore import CCSStore

        with CCSStore.record_to(tmp_path / "session"):
            pass

        captured = capsys.readouterr()
        assert "unverified" not in captured.err
        assert "CrewAI/AutoGen" not in captured.err

    def test_retain_versions_preserved_when_audit_opted_out(self, tmp_path: Path) -> None:
        """streams={'state_log'} must not break retain_versions=True on
        the constructed CCSStore — the wrapped audit callback always
        exists even though it's a no-op writer."""
        from ccs.adapters.ccsstore import CCSStore

        session_dir = tmp_path / "session"
        with CCSStore.record_to(
            session_dir, streams={"state_log"}
        ) as store:
            assert store.core.registry._retain_versions is True

        assert not (session_dir / "content_audit_log.jsonl").exists()
        manifest = json.loads((session_dir / "manifest.json").read_text())
        assert manifest["streams"] == ["state_log"]

    def test_caller_state_log_composes(self, tmp_path: Path) -> None:
        """Caller-supplied state_log callback composes with the file
        writer — both fire, neither is overridden."""
        from langgraph.store.base import PutOp

        from ccs.adapters.ccsstore import CCSStore

        captured: list[dict] = []
        session_dir = tmp_path / "session"
        with CCSStore.record_to(
            session_dir, state_log=captured.append
        ) as store:
            store.batch([PutOp(namespace=("planner", "shared"), key="k", value={"v": 1})])

        assert captured, "caller state_log was not invoked"
        on_disk = _read_jsonl(session_dir / "state_log.jsonl")
        assert on_disk, "file writer did not produce on-disk entries"
        # Both saw the same entries (composition, not override).
        assert len(captured) == len(on_disk)


# ---------------------------------------------------------------------------
# Session-directory permissions
# ---------------------------------------------------------------------------


class TestSessionDirPermissions:
    """The session directory is created owner-only (0o700) to match the
    0o600 stream-file mode in _open_stream. A world-traversable trace
    directory would let other local users enumerate stream filenames and
    sizes even though the JSONL contents stay unreadable."""

    def test_session_dir_created_owner_only(self, tmp_path: Path) -> None:
        session_dir = tmp_path / "session"
        assert not session_dir.exists()
        with record_callbacks(session_dir, accept_unverified=True):
            pass
        dir_mode = stat.S_IMODE(session_dir.stat().st_mode)
        # 0o700 is requested at creation; umask can only narrow it (0o700
        # has no group/other bits to clear), so assert no group/other
        # access rather than an exact mode that could vary by platform.
        assert dir_mode & 0o077 == 0, f"session dir too permissive: {oct(dir_mode)}"

    def test_session_dir_and_stream_file_both_default_deny(
        self, tmp_path: Path
    ) -> None:
        """Regression guard for the dir/file mode pair: the directory
        (0o700) and the stream JSONL it contains (0o600) are both
        owner-only — neither leaks group/other access."""
        session_dir = tmp_path / "session"
        with record_callbacks(
            session_dir, accept_unverified=True
        ) as (state_cb, _audit_cb):
            state_cb(_state_log_entry())

        dir_mode = stat.S_IMODE(session_dir.stat().st_mode)
        file_mode = stat.S_IMODE((session_dir / "state_log.jsonl").stat().st_mode)
        assert dir_mode & 0o077 == 0, f"session dir too permissive: {oct(dir_mode)}"
        assert file_mode & 0o077 == 0, f"stream file too permissive: {oct(file_mode)}"
