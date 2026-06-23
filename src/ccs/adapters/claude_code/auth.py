# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Shared-secret authentication for the local HTTP coordinator (KTD-12).

Without this, any same-user process or browser tab can corrupt MESI state
via direct POST or DNS rebinding (browser pages can resolve their own
domain to 127.0.0.1 and bypass same-origin). Suppression of stale-read
warnings is the most direct attack — it would nullify the product's value
silently. ~50 lines of stdlib, no dependency cost.

VERIFIED 2026-05-14 (brainstorm §13.8, §13.9):
- HTTP hook handler accepts arbitrary Authorization headers (axios/1.13.6)
- Bearer secret is REDACTED from `--include-hook-events` debug streams

Threat model:
- Adversary 1: another process running as the same OS user (e.g., a malicious
  npm package, a compromised dev tool). Mitigated by hook.secret being mode
  0600 — only this user can read it.
- Adversary 2: browser tab visiting an attacker page that does DNS rebinding
  to resolve attacker.com → 127.0.0.1. Mitigated by Host-header check
  (browser sends Host: attacker.com, server rejects).
- NOT mitigated: malicious code running with the same UID that ALSO has
  filesystem read of `.coherence/hook.secret`. v0.1 accepts this — it's the
  same trust boundary as the user's shell history, SSH agent socket, etc.
"""

from __future__ import annotations

import hmac
import ipaddress
import logging
import os
import secrets
import time
from pathlib import Path

logger = logging.getLogger(__name__)


SECRET_FILENAME = "hook.secret"
"""The file name inside <coordinator-root>/.coherence/ that holds the
hex-encoded shared secret. Mode 0600 — owner-read-only."""

SECRET_BYTES = 32
"""32 bytes of random entropy → 64-char hex token in the Authorization header."""

ENSURE_SECRET_MAX_RETRIES = 5
"""R11 (Unit 6): bound on the empty-file recovery loop in :func:`ensure_secret`.
If we observe 'file exists but is empty' more than this many times in a row,
something pathological is happening (a racer that creates but never writes,
a misbehaving editor, disk-full mid-write); fail closed rather than risk
clobbering valid secrets via O_TRUNC."""

ENSURE_SECRET_RETRY_SLEEP_SEC = 0.020
"""R11 (Unit 6): brief sleep between empty-file recovery attempts so a
racer that has the file open but hasn't flushed its write yet gets a
chance to make progress before we re-poll."""

_BEARER_PREFIX = "Bearer "
_HOST_ALLOWLIST: frozenset[str] = frozenset({"localhost", "127.0.0.1"})

#: Truthy env values that opt into cross-host mode (mirrors the client flag).
_REMOTE_TRUTHY_ENV_VALUES: frozenset[str] = frozenset({"1", "true", "yes", "on"})

#: Explicit private-range networks a cross-host coordinator may bind to. We
#: range-check explicitly because ``ipaddress.is_private`` returns True for BOTH
#: ``0.0.0.0`` and loopback aliases (e.g. 127.0.0.2) — admitting either would
#: defeat the bind guard. Loopback, link-local (169.254/16) and CGNAT (100.64/10)
#: are deliberately excluded here (loopback is handled separately).
_PRIVATE_V4_NETS = (
    ipaddress.ip_network("10.0.0.0/8"),
    ipaddress.ip_network("172.16.0.0/12"),
    ipaddress.ip_network("192.168.0.0/16"),
)
_PRIVATE_V6_NET = ipaddress.ip_network("fc00::/7")


def _remote_flag_enabled() -> bool:
    """True when CCS_REMOTE_COORDINATOR opts into cross-host mode (default OFF)."""
    return (
        os.environ.get("CCS_REMOTE_COORDINATOR", "").strip().lower()
        in _REMOTE_TRUTHY_ENV_VALUES
    )


def is_loopback_host(host: str) -> bool:
    """True for the always-allowed loopback names (localhost / 127.0.0.1)."""
    return host in _HOST_ALLOWLIST


def _is_private_range(ip: ipaddress.IPv4Address | ipaddress.IPv6Address) -> bool:
    if ip.version == 4:
        return any(ip in net for net in _PRIVATE_V4_NETS)
    return ip in _PRIVATE_V6_NET


def validate_bind_host(host: str) -> None:
    """Validate a coordinator bind address (raise ``ValueError`` if disallowed).

    Loopback (localhost / 127.0.0.1) is always allowed. Any other address
    requires the cross-host opt-in (``CCS_REMOTE_COORDINATOR``) AND must be an
    explicit RFC-1918/4193 private-range IP. ``0.0.0.0`` / wildcard, loopback
    aliases (127.0.0.2), link-local (169.254/16), CGNAT (100.64/10) and public
    addresses are all rejected — binding to any of them would expose the
    coordinator beyond the intended private network.
    """
    if is_loopback_host(host):
        return
    if not _remote_flag_enabled():
        raise ValueError(
            f"refusing to bind the coordinator to {host!r} beyond loopback; set "
            "CCS_REMOTE_COORDINATOR to opt into cross-host mode"
        )
    try:
        ip = ipaddress.ip_address(host)
    except ValueError as exc:
        raise ValueError(f"bind host {host!r} is not a valid IP address") from exc
    if ip.is_unspecified:
        raise ValueError(f"refusing to bind the coordinator to the wildcard address {host!r}")
    if not _is_private_range(ip):
        raise ValueError(
            f"bind host {host!r} is not an RFC-1918/4193 private-range address "
            "(loopback aliases, link-local, CGNAT and public addresses are rejected)"
        )


def build_host_allowlist(bind_host: str) -> frozenset[str]:
    """The Host-header allowlist for a coordinator bound to ``bind_host``.

    Always includes loopback; for a validated non-loopback bind it also admits
    that exact host. Validates ``bind_host`` (raises on a disallowed bind), so a
    coordinator constructed with a bad bind fails loud at construction.
    """
    validate_bind_host(bind_host)
    if is_loopback_host(bind_host):
        return _HOST_ALLOWLIST
    return _HOST_ALLOWLIST | {bind_host}


class EnsureSecretError(RuntimeError):
    """R11 (Unit 6): ensure_secret could not converge — the file exists
    but stays empty across ENSURE_SECRET_MAX_RETRIES attempts. The
    coordinator startup path should treat this as fatal; the alternative
    (O_TRUNC re-write of a file another process may have just populated)
    risks clobbering a concurrent racer's valid secret and giving two
    spawn-side processes different secrets for the same workspace."""


def ensure_secret(coordinator_root: Path) -> str:
    """Generate-and-persist the shared secret if missing; otherwise load it.

    Idempotent: safe to call from every coordinator spawn. Returns the
    hex-encoded secret. Raises ``OSError`` if the ``.coherence`` directory
    cannot be created (graceful-degradation should happen at the
    lifecycle layer, not here). Raises :class:`EnsureSecretError` if a
    ``hook.secret`` file exists but stays empty across
    ``ENSURE_SECRET_MAX_RETRIES`` attempts — see R11 for the rationale
    on failing closed instead of falling back to O_TRUNC.

    R11 (Unit 6): the empty-file recovery branch is now a bounded
    O_EXCL retry loop instead of the prior O_TRUNC re-write. The
    O_TRUNC path could clobber a concurrent spawn's valid secret in
    the narrow window between O_EXCL-create and write — both processes
    would then walk away with DIFFERENT secrets for the same workspace,
    which silently breaks all peer hooks until one coordinator restarts.
    """
    coherence_dir = coordinator_root / ".coherence"
    coherence_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    secret_path = coherence_dir / SECRET_FILENAME

    for attempt in range(ENSURE_SECRET_MAX_RETRIES):
        # Fast path: file exists and is populated.
        # SEC-02 / finding #40: wrap read_text() in try/except so that a
        # same-UID concurrent unlink between is_file() and read_text()
        # (TOCTOU) causes a retry instead of a propagated FileNotFoundError
        # that would crash coordinator startup.
        if secret_path.is_file():
            try:
                token = secret_path.read_text().strip()
            except (FileNotFoundError, OSError):
                continue
            if token:
                return token

        # Try the atomic O_EXCL create. Either we win (write our secret)
        # or someone else owns the file (re-read on next iteration).
        new_token = secrets.token_hex(SECRET_BYTES)
        try:
            fd = os.open(
                str(secret_path),
                os.O_CREAT | os.O_EXCL | os.O_WRONLY,
                0o600,
            )
        except FileExistsError:
            # Another process owns the file. Brief sleep so they can
            # finish writing, then loop back to the fast-path re-read.
            if attempt + 1 < ENSURE_SECRET_MAX_RETRIES:
                time.sleep(ENSURE_SECRET_RETRY_SLEEP_SEC)
            continue
        with os.fdopen(fd, "w") as f:
            f.write(new_token + "\n")
        logger.info("generated shared secret at %s", secret_path)
        return new_token

    # All retries observed the file existing but staying empty. We do
    # NOT O_TRUNC over it — that would risk overwriting a concurrent
    # racer's just-written secret. Fail closed; coordinator startup
    # aborts with an actionable error.
    raise EnsureSecretError(
        f"hook.secret at {secret_path} exists but stayed empty across "
        f"{ENSURE_SECRET_MAX_RETRIES} attempts; refusing to O_TRUNC over a "
        f"file another process may be writing concurrently. Manually "
        f"remove {secret_path} if it is genuinely stale."
    )


def load_secret(coordinator_root: Path) -> str | None:
    """Load the secret if it exists. Returns None if the file is missing.
    Used by hook clients (CLI scripts, console-script entry points) that
    should NEVER create the secret — only the coordinator-spawn path does."""
    secret_path = coordinator_root / ".coherence" / SECRET_FILENAME
    if not secret_path.is_file():
        return None
    # SEC-02 / finding #40: same TOCTOU guard as ensure_secret — a concurrent
    # unlink between is_file() and read_text() returns None instead of raising.
    try:
        token = secret_path.read_text().strip()
    except (FileNotFoundError, OSError):
        return None
    return token or None


def verify_bearer(authorization_header: str | None, expected_secret: str) -> bool:
    """Constant-time comparison of an Authorization header against the
    expected secret. Returns True only when the header is present, well-
    formed (``Bearer <token>``), and the token matches exactly.

    Constant-time prevents timing oracles on the token bytes — relevant
    because the server's response time is observable from another process
    on the same machine."""
    if not authorization_header:
        return False
    if not authorization_header.startswith(_BEARER_PREFIX):
        return False
    presented = authorization_header[len(_BEARER_PREFIX):]
    return hmac.compare_digest(presented, expected_secret)


def verify_host(host_header: str | None, allowlist: frozenset[str] = _HOST_ALLOWLIST) -> bool:
    """Reject Host headers not in ``allowlist`` (default: localhost/127.0.0.1).

    Block DNS rebinding: an attacker page at attacker.com resolves
    attacker.com → 127.0.0.1, browser sends ``Host: attacker.com``, server
    must reject. The cross-host coordinator passes a wider allowlist
    ({loopback, validated bind_host}); every other host still 403s. Allows the
    allowlisted names with or without a port suffix.
    """
    if not host_header:
        return False
    # Strip port suffix if present (e.g., "127.0.0.1:54321")
    hostname = host_header.split(":", 1)[0]
    return hostname in allowlist
