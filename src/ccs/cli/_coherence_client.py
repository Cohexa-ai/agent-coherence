# Copyright (c) 2026 Arbiter contributors.
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
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

from ccs.adapters.claude_code.lifecycle import _read_port_from_file

logger = logging.getLogger(__name__)

#: HTTP timeout for CLI requests. The coordinator's per-request watchdog is
#: 4s; we add headroom for connection setup. CLI users are interactive so a
#: 6s ceiling is preferable to retrying.
CLI_HTTP_TIMEOUT_SEC = 6.0


@dataclass(frozen=True)
class CoordinatorEndpoint:
    """Resolved (port, bearer_token) pair for the local coordinator."""

    port: int
    bearer: str

    @property
    def base_url(self) -> str:
        return f"http://127.0.0.1:{self.port}"


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


def get(endpoint: CoordinatorEndpoint, path: str) -> dict[str, Any]:
    """Authenticated GET. Raises :class:`CoordinatorUnavailable` on network
    error; raises :class:`urllib.error.HTTPError` for non-2xx so the caller
    can format status codes explicitly."""
    req = urllib.request.Request(
        url=f"{endpoint.base_url}{path}",
        method="GET",
        headers={
            "Authorization": f"Bearer {endpoint.bearer}",
            "Host": "127.0.0.1",
        },
    )
    return _execute(req)


def post(endpoint: CoordinatorEndpoint, path: str, body: dict[str, Any]) -> dict[str, Any]:
    """Authenticated POST with JSON body."""
    payload = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(
        url=f"{endpoint.base_url}{path}",
        data=payload,
        method="POST",
        headers={
            "Authorization": f"Bearer {endpoint.bearer}",
            "Host": "127.0.0.1",
            "Content-Type": "application/json",
        },
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


def http_status_from_error(exc: urllib.error.HTTPError) -> Optional[dict[str, Any]]:
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
