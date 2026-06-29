# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Content-free snapshot-session audit log — appends session lifecycle
events (begin / commit / invalidate) as JSONL to
``<coordinator_root>/.coherence/session-audit.log`` (SB-17 / TX-1, Unit 8 /
R10a).

Why a SEPARATE module from :mod:`audit_log`:
- ``audit_log`` carries the LOCKED KTD-V invariant ("Denial events only …
  ``decision`` is always ``strict_deny``"), guarded by
  ``test_audit_log_payload_bounded_no_user_content``. Session lifecycle
  events are a DIFFERENT event family (begin / commit / invalidate, not a
  strict-deny), so folding them into ``audit_log`` would break that
  invariant. They go here, in their own file, with their own bounded
  schema — ``audit_log`` is untouched (two reviewers, plan Unit 8).
- Single-responsibility helper the session endpoints import.

Content-free invariants (R10a — mirrors KTD-V's bounded-payload
discipline):
- **No content bytes. No content HASHES of bodies. No user prose.** A
  session-audit record carries only session-lifecycle METADATA: a
  non-secret session id (a hash of the server-minted token, NOT the token
  itself — see ``_hash_session_token``), the read-set artifact ids, the
  pinned versions, the event kind, and an ISO-8601 timestamp. The field
  set is FIXED per event kind and pinned by
  ``test_session_audit_payload_bounded_no_user_content`` so no body
  material can creep in.
- **No raw session token.** The token is secret material (it authorizes
  ``session.read`` / ``session.commit``); the audit log records a SHA-256
  hash of it as the correlation handle, so an audit-log reader can join
  events for one session WITHOUT the log ever carrying credential
  material.
- **0o600 file mode + O_NOFOLLOW.** Created with an explicit mode flag and
  ``O_NOFOLLOW`` so a pre-planted symlink at the audit path cannot
  redirect the append elsewhere (matches ``audit_log``'s discipline). Mode
  drift on an existing file (operator chmod 0644) is logged as a warning;
  the append still proceeds because the data is content-free lifecycle
  metadata, not credential material.

Concurrent writes: ``os.O_APPEND`` advances the file offset atomically per
``write`` syscall, but the POSIX whole-write atomicity guarantee only holds
below ``PIPE_BUF`` (as little as 512 bytes on macOS), and a max-cardinality
cut record exceeds that — so concurrent ``/session/begin`` appenders could
interleave and tear a JSONL line (finding F9). A process-level
``_AUDIT_WRITE_LOCK`` serializes the open+write+close so each record lands
whole, regardless of size. The coordinator is single-process per host, so an
in-process lock covers the real concurrency (multiple HTTP handler threads);
cross-process sharing of one audit file is not a supported topology.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import stat
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal, Mapping
from uuid import UUID

logger = logging.getLogger(__name__)

_AUDIT_WRITE_LOCK = threading.Lock()
"""Serializes session-audit appends so a multi-KB record (a max-cardinality
cut) cannot tear under concurrent ``/session/begin`` writers (finding F9).
In-process scope — the coordinator is single-process per host."""


_SESSION_AUDIT_LOG_FILENAME = "session-audit.log"
_REQUIRED_MODE = 0o600
"""R10a locked: session-audit.log must be mode 0o600 (owner read/write
only). Pinned via ``test_session_audit_required_mode_constant``."""

SessionEvent = Literal["session_begin", "session_commit", "session_invalidate"]
"""The three lifecycle event kinds. ADDITIVE-only — a new kind is added,
never folded into an existing literal (wire-stability discipline)."""


def _resolve_session_audit_log_path(coordinator_root: Path) -> Path:
    """Compute ``<coordinator_root>/.coherence/session-audit.log``. The
    directory is expected to exist (coordinator startup creates it); this
    helper does NOT create it — a missing directory surfaces as a
    coordinator misconfiguration (logged, append skipped), not silent log
    loss elsewhere."""
    return coordinator_root / ".coherence" / _SESSION_AUDIT_LOG_FILENAME


def _hash_session_token(session_token: str) -> str:
    """Return a SHA-256 hex digest of the server-minted session token — the
    NON-SECRET correlation handle recorded in the audit log.

    The raw token authorizes ``session.read`` / ``session.commit``, so it is
    credential material and MUST NOT land in the log. A stable hash lets an
    audit reader correlate the begin / commit / invalidate events of one
    session without the log ever carrying the secret. (This hashes the
    SESSION TOKEN, never any artifact body — the no-body-hash invariant is
    about CONTENT hashes, which this is not.)"""
    return hashlib.sha256(session_token.encode("utf-8")).hexdigest()


def _check_mode_or_warn(path: Path) -> bool:
    """Return True iff the existing file mode matches ``_REQUIRED_MODE``. On
    drift, emit a ``logger.warning`` and return False — the caller still
    appends (content-free lifecycle metadata is not credential material;
    refusing would silently drop audit data after an operator chmod).
    Mirrors ``audit_log._check_mode_or_warn``."""
    try:
        actual_mode = stat.S_IMODE(path.stat().st_mode)
    except OSError:
        return True  # File doesn't exist yet — will be created with 0o600.
    if actual_mode != _REQUIRED_MODE:
        logger.warning(
            "session-audit.log at %s has mode %o (expected %o). Continuing "
            "append — content-free lifecycle metadata is not credential "
            "material — but run `chmod 0600 .coherence/session-audit.log` to "
            "restore.",
            path, actual_mode, _REQUIRED_MODE,
        )
        return False
    return True


def _append_record(coordinator_root: Path, record: dict) -> bool:
    """Append one JSONL ``record`` to ``.coherence/session-audit.log`` with
    the 0o600 + ``O_NOFOLLOW`` + ``O_APPEND`` discipline.

    Returns True if the append succeeded; False on OSError (disk full,
    symlink at the path → ``O_NOFOLLOW`` ELOOP, unreadable). Errors are
    LOGGED, not raised — an audit-log failure must NEVER corrupt the
    coordinator transaction (it has already committed by the time this is
    called)."""
    audit_path = _resolve_session_audit_log_path(coordinator_root)
    _check_mode_or_warn(audit_path)
    payload = (json.dumps(record, ensure_ascii=False) + "\n").encode("utf-8")
    # O_NOFOLLOW: refuse to follow a symlink planted at the audit path (a
    # same-UID adversary could otherwise redirect the append). Mode 0o600
    # applies only on file CREATION; an existing file keeps its mode (verified
    # above). The ``_AUDIT_WRITE_LOCK`` makes the whole open+write+close
    # atomic against concurrent in-process appenders so a record larger than
    # PIPE_BUF cannot tear a JSONL line (finding F9); ``os.write`` of a bounded
    # record to a regular file is a single physical write under the lock.
    flags = os.O_APPEND | os.O_CREAT | os.O_WRONLY | os.O_NOFOLLOW
    try:
        with _AUDIT_WRITE_LOCK:
            fd = os.open(audit_path, flags, _REQUIRED_MODE)
            try:
                os.write(fd, payload)
            finally:
                os.close(fd)
        return True
    except OSError as exc:
        logger.error(
            "session-audit.log append failed at %s: %s. Event NOT recorded "
            "(coordinator transaction already committed; the session call "
            "proceeded normally).",
            audit_path, exc,
        )
        return False


def _serialize_versions(versions: Mapping[UUID, int]) -> dict[str, int]:
    """Render a ``{artifact_id: version}`` map as a JSON-serializable
    ``{str(uuid): int}`` dict. Carries ONLY ids + versions — no bytes, no
    body hashes, no prose."""
    return {str(artifact_id): int(version) for artifact_id, version in versions.items()}


def append_session_begin(
    coordinator_root: Path,
    *,
    session_token: str,
    cut: Mapping[UUID, int],
) -> bool:
    """Append a ``session_begin`` event: a session opened, pinning ``cut``.

    R10a bounded payload — fields fixed at:
        ts          — ISO-8601 UTC timestamp
        event       — "session_begin"
        session     — SHA-256 hash of the session token (non-secret handle)
        cut         — {artifact_id: pinned_version} (read-set ids + versions)

    NO content bytes. NO body hashes. NO user prose. NO raw token."""
    record = {
        "ts": datetime.now(tz=timezone.utc).isoformat(),
        "event": "session_begin",
        "session": _hash_session_token(session_token),
        "cut": _serialize_versions(cut),
    }
    return _append_record(coordinator_root, record)


def append_session_commit(
    coordinator_root: Path,
    *,
    session_token: str,
    artifact_id: UUID,
    pinned_version: int,
    committed_version: int,
) -> bool:
    """Append a ``session_commit`` event: a single-artifact OCC commit WON
    against its pinned base.

    R10a bounded payload — fields fixed at:
        ts                 — ISO-8601 UTC timestamp
        event              — "session_commit"
        session            — SHA-256 hash of the session token
        artifact           — committed artifact id (str UUID)
        pinned_version     — the cut's pinned version (the OCC comparand)
        committed_version  — the new version after the WIN

    NO content bytes. NO body hashes. NO user prose. NO raw token."""
    record = {
        "ts": datetime.now(tz=timezone.utc).isoformat(),
        "event": "session_commit",
        "session": _hash_session_token(session_token),
        "artifact": str(artifact_id),
        "pinned_version": int(pinned_version),
        "committed_version": int(committed_version),
    }
    return _append_record(coordinator_root, record)


def append_session_invalidate(
    coordinator_root: Path,
    *,
    session_token: str,
    reason: str,
) -> bool:
    """Append a ``session_invalidate`` event: a session was found dead /
    fenced off — its read or commit failed closed (``SessionInvalidated`` or
    a ``session_invalidated`` / ``session_not_found`` typed rejection).

    R10a bounded payload — fields fixed at:
        ts        — ISO-8601 UTC timestamp
        event     — "session_invalidate"
        session   — SHA-256 hash of the session token
        reason    — the wire-stable fail-closed reason CONSTANT (never prose)

    ``reason`` is one of the bounded SESSION_READ / SESSION_COMMIT reason
    constants (a machine token like ``session_invalidated``), never a human
    message. NO content bytes. NO body hashes. NO user prose. NO raw
    token."""
    record = {
        "ts": datetime.now(tz=timezone.utc).isoformat(),
        "event": "session_invalidate",
        "session": _hash_session_token(session_token),
        "reason": reason,
    }
    return _append_record(coordinator_root, record)


__all__ = [
    "SessionEvent",
    "append_session_begin",
    "append_session_commit",
    "append_session_invalidate",
]
