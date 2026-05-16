# Copyright (c) 2026 Arbiter contributors.
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
import logging
import os
import secrets
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


SECRET_FILENAME = "hook.secret"
"""The file name inside <coordinator-root>/.coherence/ that holds the
hex-encoded shared secret. Mode 0600 — owner-read-only."""

SECRET_BYTES = 32
"""32 bytes of random entropy → 64-char hex token in the Authorization header."""

_BEARER_PREFIX = "Bearer "
_HOST_ALLOWLIST: frozenset[str] = frozenset({"localhost", "127.0.0.1"})


def ensure_secret(coordinator_root: Path) -> str:
    """Generate-and-persist the shared secret if missing; otherwise load it.

    Idempotent: safe to call from every coordinator spawn. Returns the
    hex-encoded secret. Raises OSError if the .coherence directory cannot
    be created or the file cannot be written (graceful-degradation should
    happen at the lifecycle layer, not here).
    """
    coherence_dir = coordinator_root / ".coherence"
    coherence_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    secret_path = coherence_dir / SECRET_FILENAME

    if secret_path.is_file():
        # Existing secret — load it. Don't rotate without explicit invalidation
        # (see project_plugin_release_sequence.md: v0.1 secret persists across
        # restarts; v0.2 may add rotation).
        token = secret_path.read_text().strip()
        if token:
            return token
        logger.warning("hook.secret exists but is empty; regenerating")

    # Generate fresh secret. write_text + chmod is the cleanest stdlib path;
    # we accept a brief mode-0644 window between create and chmod because
    # this only happens on first spawn ever in a workspace and the file is
    # inside a 0700 directory.
    token = secrets.token_hex(SECRET_BYTES)
    secret_path.write_text(token + "\n")
    os.chmod(secret_path, 0o600)
    logger.info("generated shared secret at %s", secret_path)
    return token


def load_secret(coordinator_root: Path) -> Optional[str]:
    """Load the secret if it exists. Returns None if the file is missing.
    Used by hook clients (CLI scripts, console-script entry points) that
    should NEVER create the secret — only the coordinator-spawn path does."""
    secret_path = coordinator_root / ".coherence" / SECRET_FILENAME
    if not secret_path.is_file():
        return None
    token = secret_path.read_text().strip()
    return token or None


def verify_bearer(authorization_header: Optional[str], expected_secret: str) -> bool:
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


def verify_host(host_header: Optional[str]) -> bool:
    """Reject Host headers that don't resolve to localhost/127.0.0.1.

    Block DNS rebinding: an attacker page at attacker.com resolves
    attacker.com → 127.0.0.1, browser sends ``Host: attacker.com``, server
    must reject. We allow ``localhost`` and ``127.0.0.1``, with or without
    a port suffix.
    """
    if not host_header:
        return False
    # Strip port suffix if present (e.g., "127.0.0.1:54321")
    hostname = host_header.split(":", 1)[0]
    return hostname in _HOST_ALLOWLIST
