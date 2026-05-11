# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""Telemetry payload constructor and consent flow surface (Unit 8).

This module is the *single-function source of truth* for the data
``ccs-diagnose`` would submit if (when) submission lands in a follow-up
unit. v0 ships **no** network code — no HTTPS POST, no hashcash, no
retry/timeout — only:

* an opt-in TTY-aware consent prompt persisted to
  ``$XDG_CONFIG_HOME/ccs-diagnose/consent.json`` (or
  ``~/.config/ccs-diagnose/consent.json``),
* environment-variable kill switches (``DO_NOT_TRACK``,
  ``DISABLE_TELEMETRY``, ``CCS_DIAGNOSE_NO_TELEMETRY``),
* :func:`payload_for` / :func:`payload_for_from_json` returning the
  payload dict that *would* be submitted, so it is auditable in
  isolation.

Hard guarantees
===============

* **No import-time side effects.** ``import ccs.diagnose.telemetry``
  starts no threads, opens no sockets, reads no files. The CI guard at
  ``tests/test_diagnose_no_import_side_effects.py`` enforces this with
  :func:`sys.addaudithook`.
* **Opt-in by default.** The first-run TTY prompt defaults to *no*. We
  re-prompt only when ``policy_version`` advances. Non-TTY / CI is a
  silent skip — no prompt, no calibration write.
* **Atomic consent write.** ``save_consent`` writes to a temp file in
  the same directory and uses :func:`os.replace` so partial writes are
  never observed. The file is created with mode ``0o600`` and the
  parent directory with mode ``0o700``.
* **Auditable payload.** Run
  ``python -c "from ccs.diagnose.telemetry import payload_for; ..."`` to
  print the literal dict that would be submitted. The schema is
  pinned by ``test_diagnose_telemetry.py`` (exact field set).

What this surface collects (only when consent is granted)
=========================================================

* ``stack`` — best-effort label like ``"LangGraph 0.6.3"`` resolved via
  :func:`importlib.metadata.version`.
* ``classifier_version`` — equal to
  :data:`ccs.diagnose.CCS_DIAGNOSE_LOG_SCHEMA_VERSION`.
* ``verdict_bucket`` / ``verdict_confidence`` — the classifier's labels.
* ``coverage`` — counts only (tick / read / write / artifact).
* ``timestamp_utc`` — ISO-8601 UTC.
* ``installation_token`` — UUID4 generated locally on consent. Stored in
  ``consent.json``; ``--reset-token`` regenerates.

What this surface deliberately does NOT collect
================================================

agent names, artifact names, content, hashes, tool calls, prompts,
run timing, IP addresses.

The :func:`payload_for` shape is asserted strictly by the tests — adding
a field here is a deliberate API change and must update the tests in
the same commit.
"""

from __future__ import annotations

import json
import os
import platform
import stat
import sys
import tempfile
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from collections.abc import Mapping
from typing import Any, TextIO

from ccs import __version__ as agent_coherence_version
from ccs.diagnose import CCS_DIAGNOSE_LOG_SCHEMA_VERSION
from ccs.diagnose.classifier import ClassifierVerdict
from ccs.diagnose.detection import DetectionReport

__all__ = [
    "ConsentState",
    "CURRENT_POLICY_VERSION",
    "CONSENT_FILE_NAME",
    "ENV_KILL_SWITCHES",
    "consent_path",
    "env_kill_switch_active",
    "is_interactive",
    "load_consent",
    "save_consent",
    "reset_token",
    "prompt_for_consent",
    "resolve_consent",
    "payload_for",
    "payload_for_from_json",
]


# -------------------------------------------------------------------- #
# Public constants
# -------------------------------------------------------------------- #

CURRENT_POLICY_VERSION: int = 1
"""Policy version emitted by v0. Bump on user-visible consent-copy changes.

When ``consent.json`` carries an older version, :func:`resolve_consent`
re-prompts (only on TTY); on non-TTY the older version is silently
treated as no consent.
"""

CONSENT_FILE_NAME: str = "consent.json"
"""Filename of the persisted consent state inside the config directory."""

ENV_KILL_SWITCHES: tuple[str, ...] = (
    "DO_NOT_TRACK",
    "DISABLE_TELEMETRY",
    "CCS_DIAGNOSE_NO_TELEMETRY",
)
"""Env vars that *force* no telemetry regardless of stored consent.

Cross-tool convention. Truthy values: ``1``, ``true``, ``yes`` (case
sensitive — most existing tools key on a non-empty value, we accept the
common explicit truthy spellings without case-folding the value).
Empty string and ``0`` are falsy.
"""

_TRUTHY_ENV_VALUES: frozenset[str] = frozenset({"1", "true", "yes"})


# -------------------------------------------------------------------- #
# ConsentState dataclass
# -------------------------------------------------------------------- #


@dataclass(frozen=True)
class ConsentState:
    """Persisted consent decision for this installation.

    ``installation_token`` is a UUID4 generated locally only when consent
    is granted. It is *never* generated automatically — only on a
    granted prompt response or :func:`reset_token`.
    """

    granted: bool
    policy_version: int
    installation_token: uuid.UUID | None


def _denied(policy_version: int = CURRENT_POLICY_VERSION) -> ConsentState:
    """Convenience: a denied / no-token consent state."""
    return ConsentState(
        granted=False, policy_version=policy_version, installation_token=None
    )


# -------------------------------------------------------------------- #
# Path resolution
# -------------------------------------------------------------------- #


def consent_path() -> Path:
    """Resolve the consent.json path.

    Honors ``$XDG_CONFIG_HOME`` when set and non-empty; otherwise falls
    back to ``~/.config/ccs-diagnose/consent.json`` (XDG default).
    """
    xdg = os.environ.get("XDG_CONFIG_HOME")
    if xdg:
        base = Path(xdg)
    else:
        base = Path.home() / ".config"
    return base / "ccs-diagnose" / CONSENT_FILE_NAME


# -------------------------------------------------------------------- #
# Environment / TTY detection
# -------------------------------------------------------------------- #


def env_kill_switch_active() -> str | None:
    """Return the *name* of the first active kill switch, or ``None``.

    Empty string and ``0`` are treated as falsy. Other non-empty values
    are treated as truthy to match cross-tool convention (``DO_NOT_TRACK``
    has historically been "any non-empty value", but we accept the common
    explicit truthy spellings to keep behavior predictable).
    """
    for name in ENV_KILL_SWITCHES:
        raw = os.environ.get(name)
        if raw is None:
            continue
        if raw == "" or raw == "0":
            continue
        if raw.lower() in _TRUTHY_ENV_VALUES:
            return name
        # Any other non-empty value: treat as truthy (DO_NOT_TRACK lineage).
        return name
    return None


def is_interactive() -> bool:
    """True iff stdin is a TTY AND ``CI`` is unset/empty.

    Both conditions matter: many CI runners attach a TTY but set
    ``CI=true``; conversely, redirected stdin (``ccs-diagnose < /dev/null``)
    must skip the prompt even on a developer laptop.
    """
    if os.environ.get("CI"):
        return False
    try:
        return bool(sys.stdin.isatty())
    except (AttributeError, ValueError):
        return False


# -------------------------------------------------------------------- #
# Consent file I/O
# -------------------------------------------------------------------- #


def load_consent() -> ConsentState | None:
    """Load consent.json from disk.

    Returns
    -------
    * ``None`` — when a kill switch is active. The caller should treat
      this as "no consent" without re-prompting.
    * ``ConsentState(granted=False, policy_version=0, ...)`` — when the
      file is missing or malformed. Distinguishing this from "denied at
      v1" lets the caller detect policy-version drift and re-prompt.
    * ``ConsentState`` parsed from the file otherwise.

    No exception leaves this function: malformed JSON, missing fields,
    bad UUID, OS errors are all mapped to "treat as no consent".
    """
    if env_kill_switch_active() is not None:
        return None

    path = consent_path()
    try:
        raw = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return _denied(policy_version=0)
    except OSError:
        return _denied(policy_version=0)

    try:
        loaded = json.loads(raw)
    except json.JSONDecodeError:
        return _denied(policy_version=0)

    if not isinstance(loaded, dict):
        return _denied(policy_version=0)

    granted = bool(loaded.get("granted", False))
    try:
        policy_version = int(loaded.get("policy_version", 0))
    except (TypeError, ValueError):
        policy_version = 0

    raw_token = loaded.get("installation_token")
    token: uuid.UUID | None
    if raw_token is None:
        token = None
    else:
        try:
            token = uuid.UUID(str(raw_token))
        except (ValueError, AttributeError, TypeError):
            # Token field present but unreadable -> treat as no consent
            # so the caller re-prompts and we don't emit a corrupted ID.
            return _denied(policy_version=0)

    # If granted is True but token is None, the file is internally
    # inconsistent: drop it back to "no consent" so re-prompt fires.
    if granted and token is None:
        return _denied(policy_version=0)

    return ConsentState(
        granted=granted,
        policy_version=policy_version,
        installation_token=token,
    )


def save_consent(state: ConsentState) -> None:
    """Atomically persist ``state`` to consent.json.

    Strategy: write to a temp file in the same directory, fchmod 0o600,
    then :func:`os.replace`. This keeps the read-modify-write race-free
    on POSIX systems (the rename is atomic; readers either see the old
    file or the new file, never a half-written one).

    Parent directory is created with mode ``0o700`` if missing. Existing
    parent directories are *not* aggressively re-chmoded so the user's
    intentional permission overrides are preserved.

    Raises any underlying ``OSError`` — most callers should catch and
    treat the failure as "no consent persisted".
    """
    target = consent_path()
    parent = target.parent
    if not parent.exists():
        # Create with restrictive mode in one step to avoid a window where
        # the directory exists with default umask permissions.
        prev_umask = os.umask(0o077)
        try:
            parent.mkdir(parents=True, mode=0o700, exist_ok=True)
        finally:
            os.umask(prev_umask)
        # Verify mode after creation (defensive).
        try:
            mode = stat.S_IMODE(parent.stat().st_mode)
            if mode & 0o077:
                os.chmod(parent, 0o700)
        except OSError:
            pass

    payload: dict[str, Any] = {
        "granted": bool(state.granted),
        "policy_version": int(state.policy_version),
        "installation_token": (
            str(state.installation_token)
            if state.installation_token is not None
            else None
        ),
    }
    serialized = json.dumps(payload, indent=2, sort_keys=True)

    # NamedTemporaryFile in the target dir + os.replace is the
    # cross-platform way to get an atomic rename.
    tmp_fd: int | None = None
    tmp_name: str | None = None
    try:
        tmp_fd, tmp_name = tempfile.mkstemp(
            prefix=".consent.", suffix=".tmp", dir=str(parent)
        )
        with os.fdopen(tmp_fd, "w", encoding="utf-8") as f:
            tmp_fd = None  # ownership transferred to the file object
            f.write(serialized)
            f.flush()
            os.fsync(f.fileno())
        os.chmod(tmp_name, 0o600)
        os.replace(tmp_name, target)
        tmp_name = None  # successfully renamed away
    finally:
        if tmp_fd is not None:
            try:
                os.close(tmp_fd)
            except OSError:
                pass
        if tmp_name is not None and os.path.exists(tmp_name):
            try:
                os.unlink(tmp_name)
            except OSError:
                pass


def reset_token() -> uuid.UUID:
    """Generate a fresh UUID4, persist a granted consent, return the token.

    Used by ``ccs-diagnose --reset-token``. Overwrites any existing
    consent.json. The returned token is the same one written to disk so
    the caller can display it to the user.
    """
    new_token = uuid.uuid4()
    save_consent(
        ConsentState(
            granted=True,
            policy_version=CURRENT_POLICY_VERSION,
            installation_token=new_token,
        )
    )
    return new_token


# -------------------------------------------------------------------- #
# Consent prompt
# -------------------------------------------------------------------- #


_PROMPT_TEXT = (
    "ccs-diagnose can contribute an anonymous data point to a public benchmark of\n"
    "multi-agent write patterns by stack.\n"
    "\n"
    "Collected:\n"
    "  - stack name + version (e.g., LangGraph 0.6.3)\n"
    "  - classifier verdict + confidence\n"
    "  - coverage shape (turns / reads / writes -- counts only, no values)\n"
    "  - timestamp\n"
    "  - installation_token (locally generated UUID4; links multiple runs from\n"
    "    this machine; reset any time with --reset-token)\n"
    "\n"
    "NOT collected, stored, logged, or transmitted:\n"
    "  - agent names, artifact names, content, hashes\n"
    "  - tool calls, prompts, run timing, any payload data\n"
    "  - IP addresses\n"
    "\n"
    "Contribute? [y / N / always-yes-for-this-machine] "
)


def _classify_response(raw: str) -> str:
    """Map a raw prompt response to ``"yes"``, ``"no"``, ``"always"``, or ``"unknown"``.

    Matching rules (case-insensitive):
    * empty input -> ``"no"`` (default-N on Enter)
    * starts with ``always`` -> ``"always"``
    * starts with ``y`` -> ``"yes"``
    * starts with ``n`` -> ``"no"``
    * otherwise -> ``"unknown"``
    """
    cleaned = raw.strip().lower()
    if cleaned == "":
        return "no"
    if cleaned.startswith("always"):
        return "always"
    if cleaned.startswith("y"):
        return "yes"
    if cleaned.startswith("n"):
        return "no"
    return "unknown"


def prompt_for_consent(
    stream_in: TextIO | None = None,
    stream_out: TextIO | None = None,
) -> ConsentState:
    """Print the consent copy and read one line of input.

    On unrecognized input, re-prompts exactly once then defaults to N.
    Returns a ``ConsentState`` with a fresh UUID4 when granted, otherwise
    ``granted=False`` with no token.

    ``stream_in`` / ``stream_out`` default to ``sys.stdin`` / ``sys.stdout``.
    Tests inject :class:`io.StringIO` instances.
    """
    sin = stream_in if stream_in is not None else sys.stdin
    sout = stream_out if stream_out is not None else sys.stdout

    sout.write(_PROMPT_TEXT)
    sout.flush()
    first = sin.readline()
    decision = _classify_response(first if first is not None else "")

    if decision == "unknown":
        sout.write("Please answer y, N, or always-yes-for-this-machine. ")
        sout.flush()
        second = sin.readline()
        decision = _classify_response(second if second is not None else "")
        if decision == "unknown":
            decision = "no"

    if decision in ("yes", "always"):
        # ``always`` is reserved for future revisions to skip re-prompts
        # on policy bumps; v0 treats it as "yes" with token.
        return ConsentState(
            granted=True,
            policy_version=CURRENT_POLICY_VERSION,
            installation_token=uuid.uuid4(),
        )
    return _denied()


# -------------------------------------------------------------------- #
# Top-level resolver
# -------------------------------------------------------------------- #


def resolve_consent() -> ConsentState:
    """Return the consent state the rest of the pipeline should use.

    Order of precedence:

    1. **Env-var kill switch** active -> denied, no prompt, no write.
    2. **Non-interactive context** (no TTY or ``CI`` set) -> existing
       state if usable, otherwise denied. Never prompts.
    3. **Existing consent at the current policy version** -> use as-is.
    4. **Existing consent at an older policy version** -> re-prompt
       (TTY only) and persist the new decision.
    5. **No / corrupt consent** -> prompt (TTY only) and persist.
    """
    if env_kill_switch_active() is not None:
        return _denied()

    existing = load_consent()
    interactive = is_interactive()

    if existing is not None and existing.policy_version == CURRENT_POLICY_VERSION:
        # Honor the stored decision exactly. ``granted=False`` here is a
        # legitimate persisted "no" -- do not re-prompt.
        return existing

    if not interactive:
        # Non-interactive: never prompt. Return whatever we have, or denied.
        if existing is not None and existing.granted and existing.policy_version > 0:
            # Policy advanced but token still valid; safest default is to
            # silently treat as no consent until the user gets a TTY.
            return _denied()
        return _denied()

    # Interactive prompt.
    new_state = prompt_for_consent()
    try:
        save_consent(new_state)
    except OSError:
        # Persisting failed (e.g., read-only home). Honor the in-memory
        # decision for this run; do not crash the pipeline.
        pass
    return new_state


# -------------------------------------------------------------------- #
# Stack detection
# -------------------------------------------------------------------- #


def _detect_stack() -> str:
    """Best-effort identification of the running stack.

    Returns ``"LangGraph <version>"`` when ``importlib.metadata`` knows
    about the langgraph package; otherwise ``"LangGraph (version-unknown)"``.
    Never raises.
    """
    try:
        from importlib.metadata import PackageNotFoundError, version
    except ImportError:  # pragma: no cover - 3.8+
        return "LangGraph (version-unknown)"
    try:
        return f"LangGraph {version('langgraph')}"
    except PackageNotFoundError:
        return "LangGraph (version-unknown)"
    except Exception:  # noqa: BLE001 - defensive against metadata quirks
        return "LangGraph (version-unknown)"


# -------------------------------------------------------------------- #
# Payload constructors
# -------------------------------------------------------------------- #


def payload_for(
    verdict: ClassifierVerdict,
    *,
    consent: ConsentState | None = None,
) -> dict[str, Any]:
    """Return the anonymized telemetry payload for a verdict.

    This function is the *single auditable surface* of what would be
    submitted. The schema is asserted strictly by
    ``test_diagnose_telemetry.py``.

    Pure aside from the timestamp (``datetime.now(UTC)``) and the
    one-shot importlib metadata lookup in :func:`_detect_stack`. The
    detection report is intentionally NOT a parameter — every field this
    payload surfaces lives on the verdict (bucket, confidence, coverage
    counts). Callers wanting to ship report-derived metrics in a future
    schema version should add a new payload constructor rather than
    expanding this signature.
    """
    state = consent if consent is not None else _denied()
    return {
        "schema_version": CCS_DIAGNOSE_LOG_SCHEMA_VERSION,
        "stack": _detect_stack(),
        "agent_coherence_version": agent_coherence_version,
        "python_version": platform.python_version(),
        "classifier_version": CCS_DIAGNOSE_LOG_SCHEMA_VERSION,
        "verdict_bucket": verdict.bucket.value,
        "verdict_confidence": verdict.confidence.value,
        "coverage": {
            "tick_count": verdict.coverage.tick_count,
            "read_count": verdict.coverage.read_count,
            "write_count": verdict.coverage.write_count,
            "artifact_count": verdict.coverage.artifact_count,
        },
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "installation_token": (
            str(state.installation_token)
            if state.granted and state.installation_token is not None
            else None
        ),
        "policy_version": state.policy_version,
    }


def payload_for_from_json(
    loaded: Mapping[str, Any],
    *,
    consent: ConsentState | None = None,
) -> dict[str, Any]:
    """Reconstruct a payload from a loaded JSON report file.

    Used by ``ccs-diagnose --show-payload PATH``. Accepts dict-shaped
    input so we don't need to rehydrate the full dataclass tree just to
    extract the bucket / confidence / coverage fields.

    Missing nested fields default to zeros / ``None`` so corrupted /
    truncated reports still produce a printable payload.
    """
    state = consent if consent is not None else _denied()
    verdict_dict = loaded.get("verdict", {}) if isinstance(loaded, Mapping) else {}
    if not isinstance(verdict_dict, Mapping):
        verdict_dict = {}
    coverage_dict = verdict_dict.get("coverage", {})
    if not isinstance(coverage_dict, Mapping):
        coverage_dict = {}
    schema_version = loaded.get("schema_version", CCS_DIAGNOSE_LOG_SCHEMA_VERSION)
    return {
        "schema_version": schema_version,
        "stack": _detect_stack(),
        "agent_coherence_version": agent_coherence_version,
        "python_version": platform.python_version(),
        "classifier_version": schema_version,
        "verdict_bucket": verdict_dict.get("bucket"),
        "verdict_confidence": verdict_dict.get("confidence"),
        "coverage": {
            "tick_count": coverage_dict.get("tick_count", 0),
            "read_count": coverage_dict.get("read_count", 0),
            "write_count": coverage_dict.get("write_count", 0),
            "artifact_count": coverage_dict.get("artifact_count", 0),
        },
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "installation_token": (
            str(state.installation_token)
            if state.granted and state.installation_token is not None
            else None
        ),
        "policy_version": state.policy_version,
    }
