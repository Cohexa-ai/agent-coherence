# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""URI→coordinator-key validation for the stale-write-guard-fs server.

Client tools pass an opaque path string; this module turns it into a SAFE
workspace-relative coordinator key BEFORE it reaches ``CoherentVolume``. It:

- reuses the coordinator's authoritative :func:`validate_path` (control chars,
  backslash, length, ``..``, leading ``/``) so the server and coordinator agree;
- reuses :func:`policy._normalize_relative` (``removeprefix`` — NEVER ``lstrip``;
  an ``lstrip`` slip silently untracks dotfiles like ``.env``);
- canonicalises repeated slashes via posix normalisation;
- rejects every ``.coherence/**`` path — the coordinator's SQLite state, PID/port
  file, hook secret, and policy YAMLs are an info-disclosure surface, not a user
  artifact (matched case-insensitively; the dir is always literally
  ``.coherence`` and a case-variant maps to it on a case-insensitive filesystem);
- rejects path/symlink escapes via realpath containment.

**TOCTOU residual (stated, v1):** a regular file swapped for an out-of-root
symlink AFTER validation but BEFORE the adapter's open is an accepted same-uid
residual — the adapter open is not ``O_NOFOLLOW`` in v1, and the trust model is
single-uid single-host. v1.1 wires ``O_NOFOLLOW`` into the adapter open.
"""

from __future__ import annotations

import os
from pathlib import Path

from ccs.adapters.claude_code.coordinator_server import validate_path
from ccs.adapters.claude_code.policy import _normalize_relative

_COHERENCE_DIR = ".coherence"


class UriValidationError(ValueError):
    """A client URI failed validation (traversal, absolute, control chars,
    ``.coherence`` state, or a path/symlink escape).

    A client-INPUT error, NOT a coherence deny — the tool layer maps it to a
    non-deny tool error, never to ``stale_view``.
    """


def validate_uri(uri: object, *, root: Path) -> str:
    """Validate a client URI into a workspace-relative coordinator key.

    Returns the normalised relative key on success; raises
    :class:`UriValidationError` on any unsafe input.
    """
    # 1. The coordinator's authoritative string checks (control chars, backslash,
    #    length, leading '/', '..'). Reused so server and coordinator never drift.
    reason = validate_path(uri)
    if reason is not None:
        raise UriValidationError(reason)
    assert isinstance(uri, str)  # validate_path guarantees a non-empty str here

    # 2. Normalise: strip a leading './' (removeprefix), reject absolute / '..'.
    normalized = _normalize_relative(uri)
    if normalized is None:
        raise UriValidationError("path is absolute, empty, or contains '..'")

    # 3. Canonicalise repeated/trailing slashes so the coordinator key is stable
    #    and matches what the adapter's `_to_relative` would track.
    key = Path(normalized).as_posix()

    # 4. Reject the coordinator's own state directory (info-disclosure surface).
    if _targets_coherence_state(key):
        raise UriValidationError("path targets coordinator state (.coherence/**) and is not a user artifact")

    # 5. Reject path/symlink escapes: realpath resolves intermediate + final-
    #    component symlinks; a target resolving outside root escapes the guard.
    _reject_path_escape(root, key)
    return key


def _targets_coherence_state(key: str) -> bool:
    parts = Path(key).parts
    return bool(parts) and parts[0].lower() == _COHERENCE_DIR


def _reject_path_escape(root: Path, key: str) -> None:
    root_real = os.path.realpath(root)
    target_real = os.path.realpath(Path(root) / key)
    if not _is_within(target_real, root_real):
        raise UriValidationError("path escapes the workspace root (traversal or symlink)")


def _is_within(candidate: str, root: str) -> bool:
    try:
        Path(candidate).relative_to(root)
        return True
    except ValueError:
        return False
