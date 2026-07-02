# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Tests for the content-free snapshot-session audit log (SB-17 / TX-1,
Unit 8 / R10a).

Mirrors the KTD-V bounded-payload discipline of
``test_audit_log_payload_bounded_no_user_content`` (in
``tests/test_claude_code_strict_mode_telemetry.py``) for the SEPARATE
session-audit module: begin / commit / invalidate events carry only
session id (a hash of the token), read-set artifact ids, pinned versions,
and timestamps — NEVER content bytes, body hashes, or user prose.
"""

from __future__ import annotations

import hashlib
import json
import stat
from datetime import datetime
from pathlib import Path
from uuid import uuid4

from ccs.adapters.claude_code.session_audit_log import (
    _REQUIRED_MODE,
    _resolve_session_audit_log_path,
    append_session_begin,
    append_session_commit,
    append_session_invalidate,
)


def _read_records(coordinator_root: Path) -> list[dict]:
    path = _resolve_session_audit_log_path(coordinator_root)
    lines = path.read_text().strip().splitlines()
    return [json.loads(line) for line in lines]


# The forbidden field set — adding any of these would leak more surface than
# R10a scopes. ``content_hash`` is forbidden too: the audit log records NO
# hashes of bodies (the session-token HASH is recorded under the ``session``
# key, which is allowed and is not a body hash).
_FORBIDDEN = {
    "content",
    "content_hash",
    "body",
    "bytes",
    "command",
    "tool_input",
    "additionalContext",
    "user_message",
    "prose",
    "token",  # the RAW session token must never appear
    "session_token",
}


def test_session_begin_record_bounded_fields(tmp_path: Path) -> None:
    (tmp_path / ".coherence").mkdir(mode=0o700)
    a1, a2 = uuid4(), uuid4()
    token = "tok-begin-xyz"
    assert append_session_begin(tmp_path, session_token=token, cut={a1: 42, a2: 7})
    record = _read_records(tmp_path)[0]
    # EXACT bounded field set (like test_audit_log_payload_bounded_no_user_content).
    assert set(record.keys()) == {"ts", "event", "session", "cut"}
    assert record["event"] == "session_begin"
    # session is the SHA-256 hash of the token, never the raw token.
    assert record["session"] == hashlib.sha256(token.encode()).hexdigest()
    assert token not in json.dumps(record)
    # cut carries ids + versions only.
    assert record["cut"] == {str(a1): 42, str(a2): 7}
    datetime.fromisoformat(record["ts"])


def test_session_commit_record_bounded_fields(tmp_path: Path) -> None:
    (tmp_path / ".coherence").mkdir(mode=0o700)
    artifact = uuid4()
    token = "tok-commit-abc"
    assert append_session_commit(
        tmp_path,
        session_token=token,
        artifact_id=artifact,
        pinned_version=42,
        committed_version=43,
    )
    record = _read_records(tmp_path)[0]
    assert set(record.keys()) == {
        "ts", "event", "session", "artifact", "pinned_version", "committed_version",
    }
    assert record["event"] == "session_commit"
    assert record["artifact"] == str(artifact)
    assert record["pinned_version"] == 42
    assert record["committed_version"] == 43
    assert not (set(record.keys()) & _FORBIDDEN)


def test_session_invalidate_record_bounded_fields(tmp_path: Path) -> None:
    (tmp_path / ".coherence").mkdir(mode=0o700)
    token = "tok-inv-123"
    assert append_session_invalidate(
        tmp_path, session_token=token, reason="session_invalidated",
    )
    record = _read_records(tmp_path)[0]
    assert set(record.keys()) == {"ts", "event", "session", "reason"}
    assert record["event"] == "session_invalidate"
    # reason is a machine token, never prose.
    assert record["reason"] == "session_invalidated"
    assert not (set(record.keys()) & _FORBIDDEN)


def test_all_three_events_no_forbidden_fields_no_content(tmp_path: Path) -> None:
    """The exact-field-set guard like the KTD-V counterpart: across all three
    event kinds, no forbidden (content/prose/raw-token) field ever appears."""
    (tmp_path / ".coherence").mkdir(mode=0o700)
    artifact = uuid4()
    token = "tok-secret-never-logged"
    secret_prose = "the user's confidential plan body"
    append_session_begin(tmp_path, session_token=token, cut={artifact: 1})
    append_session_commit(
        tmp_path, session_token=token, artifact_id=artifact,
        pinned_version=1, committed_version=2,
    )
    append_session_invalidate(tmp_path, session_token=token, reason="session_invalidated")
    raw = _resolve_session_audit_log_path(tmp_path).read_text()
    # The raw token and any prose must never appear anywhere in the log bytes.
    assert token not in raw
    assert secret_prose not in raw
    for record in _read_records(tmp_path):
        assert not (set(record.keys()) & _FORBIDDEN), (
            f"forbidden field leaked: {set(record.keys()) & _FORBIDDEN}"
        )


def test_session_audit_file_mode_0600(tmp_path: Path) -> None:
    (tmp_path / ".coherence").mkdir(mode=0o700)
    append_session_begin(tmp_path, session_token="t", cut={uuid4(): 1})
    path = _resolve_session_audit_log_path(tmp_path)
    assert stat.S_IMODE(path.stat().st_mode) == _REQUIRED_MODE


def test_session_audit_required_mode_constant() -> None:
    """R10a locked: the required mode is 0o600 (owner-only)."""
    assert _REQUIRED_MODE == 0o600


def test_session_audit_o_nofollow_refuses_symlink(tmp_path: Path) -> None:
    """O_NOFOLLOW: a symlink planted at the audit path must NOT be followed —
    the append fails closed (returns False), never redirects elsewhere."""
    (tmp_path / ".coherence").mkdir(mode=0o700)
    audit_path = _resolve_session_audit_log_path(tmp_path)
    target = tmp_path / "elsewhere.log"
    target.write_text("")
    audit_path.symlink_to(target)
    ok = append_session_begin(tmp_path, session_token="t", cut={uuid4(): 1})
    assert ok is False
    # The symlink target must be untouched (nothing written through the link).
    assert target.read_text() == ""


def test_session_audit_append_failure_does_not_raise(tmp_path: Path) -> None:
    """No .coherence/ dir → os.open fails; the helper swallows the OSError
    (the coordinator transaction has already committed)."""
    ok = append_session_begin(tmp_path, session_token="t", cut={uuid4(): 1})
    assert ok is False
