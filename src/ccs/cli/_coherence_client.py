# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Shared HTTP client helpers for the four agent-coherence-* console scripts.

The coordinator binds to 127.0.0.1 with shared-secret Bearer auth
(KTD-12). Each console script needs:

1. Resolve the workspace root.
2. Read the port from ``<root>/.coherence/server.pid``.
3. Read the bearer secret from ``<root>/.coherence/hook.secret``.
4. Make an authenticated request with a short timeout.

Failures degrade gracefully — these scripts run interactively and should
print a one-line human message + exit 1 rather than dump a stack trace.
"""

from __future__ import annotations

import json
import logging
import os
import sys
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ccs.adapters.claude_code.lifecycle import read_port_from_file as _read_port_from_file

logger = logging.getLogger(__name__)

#: HTTP timeout for CLI requests. The coordinator's per-request watchdog is
#: 4s; we add headroom for connection setup. CLI users are interactive so a
#: 6s ceiling is preferable to retrying.
CLI_HTTP_TIMEOUT_SEC = 6.0


def err(message: str) -> None:
    """Write a diagnostic / error line to stderr.

    P2 ce-review fix #15 (cli-readiness): error output must go to stderr so
    agents composing workflows can read machine-parseable data from stdout
    (e.g., ``port=$(agent-coherence-coordinator)``) without prose pollution.
    Success output stays on stdout via plain ``print()``.
    """
    print(message, file=sys.stderr, flush=True)


def validate_relative_path(p: str) -> str | None:
    """Client-side path pre-check before sending to the coordinator.

    Reject absolute paths, ``..`` traversal, and empty input. Returns
    None on valid, a reason string on invalid.

    M-02 layer-distinction note: this is the CLI-side check (light;
    designed for friendly operator error messages before the request
    is built). The server-side check is
    :func:`ccs.adapters.claude_code.coordinator_server.validate_path`
    — STRICTER (also rejects backslash-leading paths, control characters,
    paths longer than MAX_PATH_LEN, non-string types). The server-side
    check is the authoritative gate; this CLI check is for fast feedback
    without a coordinator round-trip. Do NOT remove either — they live
    at different layers of the trust boundary.

    P2 ce-review fix #6 (maintainability): consolidates the
    ``_validate_path`` helper that previously existed byte-for-byte in
    both coherence_track.py and coherence_untrack.py — divergence risk
    eliminated. Both scripts now import this single source of truth.

    Note: this is a PURE-STRING validator. Callers that want to accept
    absolute paths inside the workspace (operator-UX path, exposed via
    ``/agent-coherence:track`` skill template that passes ``$ARGUMENTS``
    verbatim) should call :func:`normalize_workspace_path` instead, which
    handles absolute-vs-relative + workspace-containment before delegating
    to this function."""
    if not p:
        return "empty"
    if p.startswith("/"):
        return "path must be relative (no leading '/')"
    if ".." in Path(p).parts:
        return "path must not contain '..' traversal"
    return None


def normalize_workspace_path(p: str, root: Path) -> tuple[str, str | None]:
    """Normalize a CLI path argument to workspace-relative form.

    Returns ``(normalized_path, None)`` if the path is valid; returns
    ``(original_path, reason_string)`` if invalid. Accepts both relative
    and absolute paths:

    - **Empty** → ``("", "empty")``
    - **Relative** (e.g., ``"docs/plan.md"``) → validated as-is via
      :func:`validate_relative_path`; returned unchanged on success.
    - **Absolute and inside workspace root** (e.g.,
      ``"/Users/x/repo/docs/plan.md"`` with ``root=/Users/x/repo``) →
      stripped to workspace-relative (``"docs/plan.md"``); re-validated
      for ``..`` traversal defense.
    - **Absolute and outside workspace root** (e.g., ``"/etc/passwd"``)
      → rejected with ``"path outside workspace root"``.

    This helper exists because the Claude Code plugin's
    ``/agent-coherence:track`` skill template substitutes ``$ARGUMENTS``
    verbatim — operators routinely type absolute paths (autocomplete from
    their shell or IDE). Pre-2026-05-26 the CLI rejected those outright;
    this helper normalizes them so the operator UX matches the skill UX.

    The normalized form is what gets written to ``tracked.yaml`` /
    ``ignored.yaml`` — absolute paths must NEVER leak into those files
    because they're per-machine / per-worktree and would break cross-host
    state sharing if the coordinator-backed state.db is ever migrated.

    M-02 trust-boundary note: the server-side validator
    (:func:`ccs.adapters.claude_code.coordinator_server.validate_path`)
    still independently rejects absolute paths in the coordinator
    request body. This client-side normalization happens BEFORE the
    request is built — by the time the request hits the wire, the path
    is workspace-relative. The server check remains the authoritative
    gate against malformed direct-HTTP calls that bypass this CLI.
    """
    if not p:
        return p, "empty"
    if Path(p).is_absolute():
        try:
            normalized = str(Path(p).resolve().relative_to(root.resolve()))
        except ValueError:
            return p, "path outside workspace root"
        # Re-validate the normalized form against the pure-string rules
        # (catches e.g. a resolved path that still contains '..' — defensive)
        reason = validate_relative_path(normalized)
        return (normalized, None) if reason is None else (p, reason)
    reason = validate_relative_path(p)
    return (p, None) if reason is None else (p, reason)


@dataclass(frozen=True)
class CoordinatorEndpoint:
    """Resolved (host, port, bearer_token) for a coordinator.

    ``host`` defaults to loopback so the local path is byte-unchanged; the
    cross-host demo (gated by :class:`RemoteCoordinatorConfig`) supplies a
    routable host via :func:`resolve_remote_endpoint`.
    """

    port: int
    bearer: str
    host: str = "127.0.0.1"

    @property
    def base_url(self) -> str:
        return f"http://{self.host}:{self.port}"


class CoordinatorUnavailable(Exception):
    """Coordinator is not running or its auth surface is missing.

    Carries a human-readable message the console script prints verbatim.
    """


def resolve_endpoint(coordinator_root: Path) -> CoordinatorEndpoint:
    """Read port + secret from ``<root>/.coherence/`` or raise
    :class:`CoordinatorUnavailable` with an operator-friendly message."""
    coherence_dir = coordinator_root / ".coherence"
    pid_file = coherence_dir / "server.pid"
    secret_file = coherence_dir / "hook.secret"

    port = _read_port_from_file(pid_file)
    if port is None:
        raise CoordinatorUnavailable(
            "no coordinator running for this workspace "
            f"(no port in {pid_file}); start one with `agent-coherence-coordinator`"
        )

    if not secret_file.is_file():
        raise CoordinatorUnavailable(
            "coordinator authentication unavailable "
            f"(missing {secret_file}); restart with `agent-coherence-coordinator`"
        )

    try:
        bearer = secret_file.read_text(encoding="utf-8").strip()
    except OSError as exc:
        raise CoordinatorUnavailable(
            f"could not read {secret_file}: {exc}"
        ) from exc

    if not bearer:
        raise CoordinatorUnavailable(
            f"{secret_file} is empty; restart with `agent-coherence-coordinator`"
        )

    return CoordinatorEndpoint(port=port, bearer=bearer)


def resolve_remote_endpoint(host: str, port: int, secret: str) -> CoordinatorEndpoint:
    """Build an endpoint for a REMOTE coordinator (cross-host demo).

    Unlike :func:`resolve_endpoint`, this reads nothing from the local
    ``.coherence/`` directory — host, port, and the bearer secret are supplied
    by the caller (typically via :meth:`RemoteCoordinatorConfig.from_env`).
    Gated by :class:`RemoteCoordinatorConfig`; never used on the loopback-only
    local path.
    """
    if not host:
        raise CoordinatorUnavailable("remote coordinator host is empty")
    if not secret:
        raise CoordinatorUnavailable("remote coordinator bearer secret is empty")
    return CoordinatorEndpoint(port=port, bearer=secret, host=host)


#: Truthy env values that enable cross-host remote mode (mirrors the
#: telemetry kill-switch parser). Everything else (incl. "" and "0") is OFF.
_REMOTE_TRUTHY_ENV_VALUES: frozenset[str] = frozenset({"1", "true", "yes", "on"})


@dataclass(frozen=True)
class RemoteCoordinatorConfig:
    """Default-OFF gate for cross-host remote-coordinator mode.

    Absent the ``CCS_REMOTE_COORDINATOR`` env flag, :meth:`from_env` returns a
    disabled config and every existing loopback-only behavior is byte-unchanged
    — the cross-host relaxation never reaches local users.

    Secret channel: the bearer is read from a FILE whose path is given by
    ``CCS_REMOTE_SECRET_FILE`` — never inline in an env var, which would leak in
    ``ps`` / ``docker inspect`` (the R7 security pass owns this). The file
    mirrors the local ``hook.secret`` (mode 0600, mounted into the remote
    container).
    """

    enabled: bool
    host: str | None = None
    port: int | None = None
    secret: str | None = None

    @classmethod
    def from_env(cls, env: dict[str, str] | None = None) -> RemoteCoordinatorConfig:
        """Parse the cross-host flag from the environment (default OFF)."""
        env = os.environ if env is None else env
        flag = env.get("CCS_REMOTE_COORDINATOR", "").strip().lower()
        if flag not in _REMOTE_TRUTHY_ENV_VALUES:
            return cls(enabled=False)
        host = (env.get("CCS_REMOTE_HOST") or "").strip() or None
        port_raw = (env.get("CCS_REMOTE_PORT") or "").strip()
        port = int(port_raw) if port_raw.isdigit() else None
        return cls(enabled=True, host=host, port=port, secret=cls._read_secret(env))

    @staticmethod
    def _read_secret(env: dict[str, str]) -> str | None:
        """Read the bearer from ``CCS_REMOTE_SECRET_FILE`` (not an inline env var).

        Hardened: refuses to follow a symlinked secret file (``O_NOFOLLOW`` — an
        attacker able to set the env var could otherwise repoint it at any
        readable file), and warns if the file is group/world-accessible (``0600``
        expected, like the local ``hook.secret``). Fails closed (returns ``None``)
        on any error.
        """
        secret_path = (env.get("CCS_REMOTE_SECRET_FILE") or "").strip()
        if not secret_path:
            return None
        try:
            fd = os.open(secret_path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
        except OSError:
            return None  # missing, or a symlink (ELOOP) — fail closed
        try:
            with os.fdopen(fd, encoding="utf-8") as handle:
                mode = os.fstat(handle.fileno()).st_mode
                if mode & 0o077:
                    logger.warning(
                        "CCS_REMOTE_SECRET_FILE %s is group/world-accessible (mode %o); "
                        "tighten it to 0600",
                        secret_path,
                        mode & 0o777,
                    )
                secret = handle.read().strip()
        except OSError:
            return None
        return secret or None


def get(
    endpoint: CoordinatorEndpoint,
    path: str,
    *,
    extra_headers: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Authenticated GET. Raises :class:`CoordinatorUnavailable` on network
    error; raises :class:`urllib.error.HTTPError` for non-2xx so the caller
    can format status codes explicitly.

    R12 (Unit 6): ``extra_headers`` lets local-operator CLIs (e.g.,
    ``agent-coherence-status``) add ``Coherence-Local-Operator: true``
    for the elevated ``/status?detail=full`` tier without hard-coding
    that header here."""
    headers: dict[str, str] = {
        "Authorization": f"Bearer {endpoint.bearer}",
        "Host": endpoint.host,
    }
    if extra_headers:
        headers.update(extra_headers)
    req = urllib.request.Request(
        url=f"{endpoint.base_url}{path}",
        method="GET",
        headers=headers,
    )
    return _execute(req)


def post(
    endpoint: CoordinatorEndpoint,
    path: str,
    body: dict[str, Any],
    *,
    extra_headers: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Authenticated POST with JSON body.

    M-04 / finding #28: ``extra_headers`` mirrors the pattern on ``get()``
    so callers can add e.g. ``Coherence-Local-Operator: true`` without
    reimplementing the urllib transport layer.
    """
    payload = json.dumps(body).encode("utf-8")
    headers: dict[str, str] = {
        "Authorization": f"Bearer {endpoint.bearer}",
        "Host": endpoint.host,
        "Content-Type": "application/json",
    }
    if extra_headers:
        headers.update(extra_headers)
    req = urllib.request.Request(
        url=f"{endpoint.base_url}{path}",
        data=payload,
        method="POST",
        headers=headers,
    )
    return _execute(req)


def _execute(req: urllib.request.Request) -> dict[str, Any]:
    try:
        with urllib.request.urlopen(req, timeout=CLI_HTTP_TIMEOUT_SEC) as resp:
            raw = resp.read()
    except urllib.error.HTTPError:
        # Caller handles status-code-specific paths.
        raise
    except urllib.error.URLError as exc:
        raise CoordinatorUnavailable(
            f"could not reach coordinator at {req.full_url}: {exc.reason}"
        ) from exc
    except (OSError, TimeoutError) as exc:
        raise CoordinatorUnavailable(
            f"network error talking to coordinator: {exc}"
        ) from exc

    if not raw:
        return {}
    try:
        return json.loads(raw.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise CoordinatorUnavailable(
            f"coordinator returned non-JSON response: {exc}"
        ) from exc


def http_status_from_error(exc: urllib.error.HTTPError) -> dict[str, Any] | None:
    """Best-effort JSON decode of an HTTPError body, for one-line user output."""
    try:
        raw = exc.read()
    except Exception:
        return None
    if not raw:
        return None
    try:
        return json.loads(raw.decode("utf-8"))
    except Exception:
        return None
