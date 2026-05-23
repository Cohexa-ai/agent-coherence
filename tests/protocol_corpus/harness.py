# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""Cross-implementation protocol corpus harness (plan Unit 7).

Loads JSON fixtures from ``fixtures/<mode>/*.json`` and routes each fixture's
``request`` to BOTH the Python in-thread coordinator AND the Node subprocess
coordinator, then asserts the responses are JSON-equivalent after normalization.

The harness is the security-relevant surface here: the normalization rules
("what we ignore in the diff") decide whether a real Python ↔ Node drift gets
caught or false-passes. Each rule has an inline comment explaining why the
field is ignored — review the rules in a focused PR per the Unit 7 verification
checklist.

Fixture schema (see ``fixtures/warn_mode/*.json``)::

    {
      "name": "<unique-fixture-name>",
      "description": "<optional human-readable>",
      "setup": {
        "tracked": ["<gitignore-glob>", ...],
        "files":   {"<path>": "<contents>", ...},
        "policy":  {<optional pre-test policy.yaml overrides>}
      },
      "request": {
        "method":  "POST" | "GET",        # default POST
        "path":    "/health" | ...,
        "body":    {<JSON body>}           # for POST
        "headers": {<optional overrides>}  # auth headers added automatically
      },
      "expected": {
        "status": <int>,
        "body":   {<normalized expected JSON>}
      },
      "backends": ["python", "node"]      # default both
    }

Spawn isolation: each fixture gets a fresh ``tmp_path`` workspace. The Python
coordinator runs in-thread (no subprocess overhead). The Node coordinator runs
as ``node <plugin-dist>/coordinator.js`` with ``AGENT_COHERENCE_WORKSPACE``
pointing at the tmp workspace. Both coordinators bind ephemeral ports.

Plugin coordinator discovery: ``AGENT_COHERENCE_PLUGIN_DIST_PATH`` env var
(absolute path to ``dist/coordinator.js``); fallback ``../agent-coherence-plugin/dist/coordinator.js``
relative to the library repo root; fallback ``~/projects/agent-coherence-plugin/dist/coordinator.js``.
If none resolve, Node backend scenarios xfail with a clear reason."""

from __future__ import annotations

import json
import os
import re
import socket
import subprocess
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator, Optional
from urllib import error as urlerror
from urllib import request as urlrequest


REPO_ROOT = Path(__file__).resolve().parents[2]
FIXTURES_ROOT = Path(__file__).resolve().parent / "fixtures"
HARNESS_TIMEOUT_SEC = 10.0
NODE_SPAWN_TIMEOUT_SEC = 8.0
NODE_SHUTDOWN_TIMEOUT_SEC = 5.0


# ----------------------------------------------------------------------
# Normalization rules
# ----------------------------------------------------------------------

# Field names whose VALUE is non-deterministic across coordinator instances
# and should be replaced with a sentinel before diffing. Comments explain the
# rationale for each — these are the "what we ignore" rules and any addition
# weakens the harness's catch surface, so changes go in a dedicated PR.
_TS_SENTINEL = "<TS>"
_UUID_SENTINEL = "<UUID>"
_PID_SENTINEL = "<PID>"
_UPTIME_SENTINEL = "<UPTIME>"
_PORT_SENTINEL = "<PORT>"
_HASH_SENTINEL = "<SHA256>"

# Top-level / nested key names whose value is normalized regardless of type.
# Listed explicitly so a new "started_at_ms" field doesn't silently slip
# through normalization just because it looks ISO-8601-ish.
_TIMESTAMP_KEYS: frozenset[str] = frozenset({
    # Wall-clock fields surfaced by /status default + metrics tiers.
    "ts",
    "started_at",
    "last_request_at",
    "last_401_warn_at",
    "started_at_ms",        # Node-side field; Python emits float seconds
    "last_completed_ms",
    "first_observation_ts",
    "last_seen_at",
})

_UPTIME_KEYS: frozenset[str] = frozenset({
    "coordinator_uptime_seconds",
    # AC-02 deprecated alias — Python emits both during the v0.1.x window so a
    # consumer migrating from the old name still gets a value. Removed in v0.2.
    "coordinator_uptime_s",
    "uptime_seconds",
    "uptime_ms",
    "process_uptime_seconds",
})

_PID_KEYS: frozenset[str] = frozenset({
    "pid",
    "coordinator_pid",
    "process_pid",
    "worker_pid",
})

_PORT_KEYS: frozenset[str] = frozenset({
    "port",
    "coordinator_port",
    "listen_port",
})

# Fields whose value is a UUID4 — we keep them present (the wire shape carries
# the field, both coordinators emit a value) but normalize the value itself.
_UUID_KEYS: frozenset[str] = frozenset({
    "instance_id",
    "agent",
    "agent_id",
    "session_id",
    "request_id",
})

# Content-hash fields. SHA-256 hex is deterministic for identical bytes, so we
# *don't* normalize these by default — drift in content_hash IS a real wire
# regression. Sentinel reserved for fixtures that need to ignore content_hash
# (e.g., where the hash depends on a timestamp embedded in the file).
_HASH_KEYS: frozenset[str] = frozenset()  # opt-in per fixture via "ignore_keys"

# UUIDv4 string regex — used to scrub UUID-shaped values that appear in string
# positions (error messages, log fragments) even when the key name isn't in
# _UUID_KEYS. Permissive: any 8-4-4-4-12 hex sequence.
_UUID_RE = re.compile(
    r"\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b"
)

# ISO-8601 timestamp regex — used to scrub timestamp-shaped values in string
# positions (error messages with "at 2026-05-23T...").
_ISO_TS_RE = re.compile(
    r"\b\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?\b"
)


def normalize_response(
    value: Any,
    *,
    ignore_keys: Optional[frozenset[str]] = None,
) -> Any:
    """Replace non-deterministic field values with stable sentinels.

    Recurses into dicts and lists. Scalars are returned as-is unless the
    enclosing key is in one of the normalization sets.

    ``ignore_keys`` — per-fixture opt-in set of additional key names whose
    values should be replaced with ``"<IGNORED>"``. Use sparingly; each entry
    is a hole in the catch surface."""
    ignore_keys = ignore_keys or frozenset()

    def _walk(v: Any, parent_key: Optional[str]) -> Any:
        if isinstance(v, dict):
            return {k: _walk_keyed(v[k], k) for k in sorted(v.keys())}
        if isinstance(v, list):
            return [_walk(item, None) for item in v]
        if isinstance(v, str):
            # String-position scrubbing: UUID and ISO-8601 substrings in
            # error messages / log fragments.
            scrubbed = _UUID_RE.sub(_UUID_SENTINEL, v)
            scrubbed = _ISO_TS_RE.sub(_TS_SENTINEL, scrubbed)
            return scrubbed
        return v

    def _walk_keyed(v: Any, key: str) -> Any:
        # Per-fixture ignore set wins over everything else.
        if key in ignore_keys:
            return "<IGNORED>"
        # Key-driven normalization fires regardless of value type so we don't
        # care whether the coordinator emits int or float for an uptime.
        if key in _TIMESTAMP_KEYS:
            return _TS_SENTINEL
        if key in _UPTIME_KEYS:
            return _UPTIME_SENTINEL
        if key in _PID_KEYS:
            return _PID_SENTINEL
        if key in _PORT_KEYS:
            return _PORT_SENTINEL
        if key in _UUID_KEYS:
            return _UUID_SENTINEL
        if key in _HASH_KEYS:
            return _HASH_SENTINEL
        return _walk(v, key)

    return _walk(value, None)


# ----------------------------------------------------------------------
# Backend identifiers
# ----------------------------------------------------------------------


BACKEND_PYTHON = "python"
BACKEND_NODE = "node"
ALL_BACKENDS = (BACKEND_PYTHON, BACKEND_NODE)


# ----------------------------------------------------------------------
# Fixture loader
# ----------------------------------------------------------------------


@dataclass(frozen=True)
class Fixture:
    """A single protocol-corpus scenario."""

    name: str
    description: str
    path: Path
    setup: dict[str, Any]
    request: dict[str, Any]
    expected: dict[str, Any]
    backends: tuple[str, ...]
    ignore_keys: frozenset[str]


def load_fixtures(mode: str = "warn_mode") -> list[Fixture]:
    """Load all JSON fixtures under ``fixtures/<mode>/`` in sorted order."""
    mode_root = FIXTURES_ROOT / mode
    if not mode_root.exists():
        return []
    fixtures: list[Fixture] = []
    for path in sorted(mode_root.glob("*.json")):
        with path.open() as fh:
            data = json.load(fh)
        backends_raw = data.get("backends") or list(ALL_BACKENDS)
        unknown = [b for b in backends_raw if b not in ALL_BACKENDS]
        if unknown:
            raise ValueError(
                f"{path.name}: unknown backends {unknown!r}; allowed={ALL_BACKENDS}"
            )
        fixtures.append(
            Fixture(
                name=data["name"],
                description=data.get("description", ""),
                path=path,
                setup=data.get("setup", {}),
                request=data["request"],
                expected=data["expected"],
                backends=tuple(backends_raw),
                ignore_keys=frozenset(data.get("ignore_keys", [])),
            )
        )
    return fixtures


# ----------------------------------------------------------------------
# Workspace setup
# ----------------------------------------------------------------------


def apply_setup(workspace: Path, setup: dict[str, Any]) -> None:
    """Materialize a fixture's ``setup`` block onto a fresh tmp workspace.

    - ``files``: writes named files relative to the workspace root.
    - ``tracked``: writes ``.coherence/tracked.yaml`` with one path per line.
    - ``policy``: NOT IMPLEMENTED yet — strict-mode-fixture-only; lands in
      Unit 7b alongside Unit 2's strict-mode wire shape additions."""
    coherence_dir = workspace / ".coherence"
    coherence_dir.mkdir(exist_ok=True, mode=0o700)

    files = setup.get("files") or {}
    for rel_path, contents in files.items():
        target = workspace / rel_path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(contents)

    tracked = setup.get("tracked") or []
    if tracked:
        # Match TrackedArtifactPolicy.load expectations — YAML list form. We
        # write the minimal valid shape that the policy loader accepts; if
        # the policy loader's API changes, this stays the only place to
        # adapt.
        lines = ["tracked:"]
        for pat in tracked:
            lines.append(f"  - {pat}")
        (coherence_dir / "tracked.yaml").write_text("\n".join(lines) + "\n")


# ----------------------------------------------------------------------
# Coordinator backends
# ----------------------------------------------------------------------


class CoordinatorBackend:
    """Abstract base: spawn, expose (port, secret), shutdown."""

    backend_id: str

    def start(self, workspace: Path) -> None:
        raise NotImplementedError

    def url(self, path: str) -> str:
        raise NotImplementedError

    def secret(self) -> str:
        raise NotImplementedError

    def shutdown(self) -> None:
        raise NotImplementedError


class PythonCoordinator(CoordinatorBackend):
    """In-process Python coordinator via ``CoordinatorHTTPServer.serve_in_thread``."""

    backend_id = BACKEND_PYTHON

    def __init__(self) -> None:
        self._server = None
        self._secret: Optional[str] = None
        self._port: Optional[int] = None

    def start(self, workspace: Path) -> None:
        # Defer the import so test discovery doesn't pay the cost.
        from ccs.adapters.claude_code.auth import load_secret
        from ccs.adapters.claude_code.coordinator_server import (
            CoordinatorHTTPServer,
        )

        server = CoordinatorHTTPServer(workspace, port=0, instance_id="protocol-corpus-py")
        server.serve_in_thread()
        # Tiny grace window — matches the existing
        # tests/test_claude_code_coordinator_server.py pattern.
        time.sleep(0.05)
        secret = load_secret(server.coordinator_root)
        assert secret is not None, "Python coordinator failed to write hook.secret"
        self._server = server
        self._secret = secret
        self._port = server.port

    def url(self, path: str) -> str:
        assert self._port is not None
        return f"http://127.0.0.1:{self._port}{path}"

    def secret(self) -> str:
        assert self._secret is not None
        return self._secret

    def shutdown(self) -> None:
        if self._server is not None:
            self._server.shutdown()


class NodeCoordinator(CoordinatorBackend):
    """Out-of-process Node coordinator spawned as ``node dist/coordinator.js``."""

    backend_id = BACKEND_NODE

    def __init__(self, dist_path: Path) -> None:
        if not dist_path.exists():
            raise FileNotFoundError(
                f"Node coordinator entry point not found: {dist_path}. "
                f"Build the plugin (cd <plugin-repo> && npm ci && npm run build) "
                f"or set AGENT_COHERENCE_PLUGIN_DIST_PATH to the absolute path."
            )
        self._dist_path = dist_path
        self._proc: Optional[subprocess.Popen[bytes]] = None
        self._port: Optional[int] = None
        self._secret: Optional[str] = None

    def start(self, workspace: Path) -> None:
        env = dict(os.environ)
        env["AGENT_COHERENCE_WORKSPACE"] = str(workspace)
        # Suppress harmless verbose logging if the operator wants quieter
        # test output (Node coordinator logs to stderr; harness captures
        # both streams and surfaces them only on failure).
        self._proc = subprocess.Popen(
            ["node", str(self._dist_path)],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        port, secret = _wait_for_coordinator(
            workspace,
            timeout_sec=NODE_SPAWN_TIMEOUT_SEC,
            proc=self._proc,
            backend_label="node",
        )
        self._port = port
        self._secret = secret

    def url(self, path: str) -> str:
        assert self._port is not None
        return f"http://127.0.0.1:{self._port}{path}"

    def secret(self) -> str:
        assert self._secret is not None
        return self._secret

    def shutdown(self) -> None:
        if self._proc is None:
            return
        # SIGTERM → graceful close per coordinator.ts's shutdown handler.
        # Fall back to SIGKILL after NODE_SHUTDOWN_TIMEOUT_SEC.
        self._proc.terminate()
        try:
            self._proc.wait(timeout=NODE_SHUTDOWN_TIMEOUT_SEC)
        except subprocess.TimeoutExpired:
            self._proc.kill()
            self._proc.wait(timeout=NODE_SHUTDOWN_TIMEOUT_SEC)


def _wait_for_coordinator(
    workspace: Path,
    *,
    timeout_sec: float,
    proc: subprocess.Popen[bytes],
    backend_label: str,
) -> tuple[int, str]:
    """Poll for ``.coherence/server.pid`` + ``hook.secret`` to materialize.

    Surfaces subprocess crash early (returns nonzero before the pid file lands)
    with the captured stderr — a silent timeout would otherwise hide the real
    diagnostic from a failed Node build."""
    pid_file = workspace / ".coherence" / "server.pid"
    secret_file = workspace / ".coherence" / "hook.secret"
    deadline = time.monotonic() + timeout_sec
    while time.monotonic() < deadline:
        rc = proc.poll()
        if rc is not None:
            stderr = proc.stderr.read().decode("utf-8", errors="replace") if proc.stderr else ""
            stdout = proc.stdout.read().decode("utf-8", errors="replace") if proc.stdout else ""
            raise RuntimeError(
                f"{backend_label} coordinator exited with rc={rc} before writing server.pid.\n"
                f"stdout:\n{stdout}\nstderr:\n{stderr}"
            )
        if pid_file.exists() and secret_file.exists():
            try:
                lines = pid_file.read_text().splitlines()
                if len(lines) >= 2:
                    port = int(lines[1])
                    secret = secret_file.read_text().strip()
                    if secret:
                        # Probe the actual socket once before returning so we
                        # don't race the listen() callback on slow CI.
                        if _socket_open("127.0.0.1", port):
                            return port, secret
            except (OSError, ValueError):
                pass  # Partial write — retry on next tick.
        time.sleep(0.05)
    raise TimeoutError(
        f"{backend_label} coordinator did not write server.pid within {timeout_sec}s; "
        f"workspace={workspace}"
    )


def _socket_open(host: str, port: int) -> bool:
    """Check whether the coordinator's listen socket is accepting yet."""
    try:
        with socket.create_connection((host, port), timeout=0.5):
            return True
    except OSError:
        return False


def resolve_node_dist_path() -> Optional[Path]:
    """Locate the Node coordinator entry point.

    Resolution order:
    1. ``AGENT_COHERENCE_PLUGIN_DIST_PATH`` env var (absolute path)
    2. ``../agent-coherence-plugin/dist/coordinator.js`` relative to library repo root
    3. ``~/projects/agent-coherence-plugin/dist/coordinator.js``

    Returns ``None`` if nothing resolves — the harness then xfails Node backend
    scenarios with a clear reason rather than hanging."""
    explicit = os.environ.get("AGENT_COHERENCE_PLUGIN_DIST_PATH")
    if explicit:
        p = Path(explicit).expanduser().resolve()
        if p.exists():
            return p
    sibling = (REPO_ROOT.parent / "agent-coherence-plugin" / "dist" / "coordinator.js").resolve()
    if sibling.exists():
        return sibling
    home_fallback = (Path.home() / "projects" / "agent-coherence-plugin" / "dist" / "coordinator.js").resolve()
    if home_fallback.exists():
        return home_fallback
    return None


# ----------------------------------------------------------------------
# Request execution
# ----------------------------------------------------------------------


def execute_request(
    backend: CoordinatorBackend,
    request: dict[str, Any],
) -> tuple[int, dict[str, Any]]:
    """Issue a fixture's request against ``backend``. Returns (status, body_dict).

    Adds the Authorization + Host headers automatically. Body is JSON-encoded
    for POST; ignored for GET."""
    method = request.get("method", "POST").upper()
    path = request["path"]
    body = request.get("body")

    headers = {
        "Authorization": f"Bearer {backend.secret()}",
        "Host": "127.0.0.1",
        "Content-Type": "application/json",
    }
    headers.update(request.get("headers", {}))

    data = json.dumps(body).encode("utf-8") if body is not None else b""
    req = urlrequest.Request(
        backend.url(path),
        data=data if method == "POST" else None,
        method=method,
        headers=headers,
    )
    try:
        with urlrequest.urlopen(req, timeout=HARNESS_TIMEOUT_SEC) as resp:
            raw = resp.read().decode("utf-8")
            parsed = json.loads(raw) if raw else {}
            return resp.status, parsed
    except urlerror.HTTPError as exc:
        raw = exc.read().decode("utf-8") if exc.fp else ""
        parsed = json.loads(raw) if raw else {}
        return exc.code, parsed


# ----------------------------------------------------------------------
# Top-level scenario runner
# ----------------------------------------------------------------------


@contextmanager
def coordinator_running(
    backend_id: str,
    workspace: Path,
    node_dist_path: Optional[Path] = None,
) -> Iterator[CoordinatorBackend]:
    """Context manager that spawns a coordinator + tears it down cleanly."""
    if backend_id == BACKEND_PYTHON:
        backend: CoordinatorBackend = PythonCoordinator()
    elif backend_id == BACKEND_NODE:
        if node_dist_path is None:
            raise RuntimeError(
                "Node backend requested but no plugin dist path resolved. "
                "Set AGENT_COHERENCE_PLUGIN_DIST_PATH or build the plugin checkout."
            )
        backend = NodeCoordinator(node_dist_path)
    else:
        raise ValueError(f"Unknown backend: {backend_id!r}")
    backend.start(workspace)
    try:
        yield backend
    finally:
        backend.shutdown()


def run_scenario(
    fixture: Fixture,
    backend_id: str,
    workspace: Path,
    node_dist_path: Optional[Path] = None,
) -> tuple[int, dict[str, Any]]:
    """End-to-end: setup workspace → spawn backend → POST → normalize → return.

    Returns the normalized (status, body) for assertion by the caller."""
    apply_setup(workspace, fixture.setup)
    with coordinator_running(backend_id, workspace, node_dist_path) as backend:
        status, body = execute_request(backend, fixture.request)
    return status, normalize_response(body, ignore_keys=fixture.ignore_keys)
