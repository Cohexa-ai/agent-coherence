# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""CoherentVolume — the data-plane coherent-workspace appliance (v1).

v1 prevents the **sequential stale-read→write lost update** for a single-host
agent fleet sharing files in a workspace — the OpenViktor cron shape (agent A
reads v1, agent B reads v1, A commits v2, B's stale write is denied → B
re-reads). It does **not** serialize concurrent racing writers, attach to a
coordinator it did not spawn, or detect an agent that re-reads fresh bytes then
writes a buffer computed from older bytes — those are explicit v1.1 / honest-
boundary cases (see ``docs/plans/2026-06-03-001-feat-data-plane-coherent-workspace-plan.md``
and ``docs/solutions/best-practices/coordinator-invalidation-not-mutex-honest-coherence-claims-2026-06-04.md``).

**Architecture.** CoherentVolume is a thin *out-of-process coordinator client*,
not a wrapper over the in-process :class:`~ccs.adapters.base.CoherenceAdapterCore`.
The teeth that make invalidation enforceable — the strict-mode ``INVALID``-deny —
live in the coordinator HTTP server (``ccs.adapters.claude_code``), so v1 reuses
that shipped path: it writes the policy YAML, spawns/attaches the local-HTTP
coordinator over SQLite-WAL, and routes reads/writes through the ``/hooks/*``
endpoints. Content stays on the real filesystem; the coordinator holds MESI
state + content-hash + version only.

This module (Unit 1) establishes the façade scaffolding: spawn-with-strict
enablement, per-instance identity (fork-safe), and the fail-closed degrade
contract. The read/write contract (Unit 2) and the ``install()`` shim (Unit 3)
build on it.
"""

from __future__ import annotations

import logging
import os
import threading
import uuid
import warnings
from pathlib import Path
from typing import Literal

import yaml

from ccs.adapters.claude_code.lifecycle import (
    LifecycleConfig,
    connect_or_spawn,
    read_port_from_file,
    tcp_probe,
)
from ccs.cli._coherence_client import (
    CoordinatorEndpoint,
    CoordinatorUnavailable,
    resolve_endpoint,
)
from ccs.cli._coherence_client import (
    get as _coordinator_get,
)
from ccs.core.exceptions import CoherenceDegradedWarning, CoherenceError

logger = logging.getLogger(__name__)

__all__ = ["CoherentVolume"]


class CoherentVolume:
    """Coherent shared workspace for a single-host agent fleet (v1).

    The appliance **spawns** the coordinator for ``root`` and enables strict
    mode on the ``managed`` globs (by writing ``.coherence/tracked.yaml`` and
    ``.coherence/strict_mode.yaml`` before spawn — the coordinator loads policy
    once at startup, so enablement must precede the spawn). Strict mode is what
    gives invalidation teeth: a write from an ``INVALID`` holder is denied, and
    the façade surfaces that deny *fail-closed* so the caller re-reads.

    ``on_error``:

    - ``"strict"`` (default): any coherence failure — a coordinator that is
      unavailable, or one already running that we cannot enable strict on —
      raises :class:`~ccs.core.exceptions.CoherenceError`. Fail-closed.
    - ``"degrade"``: the same conditions warn once
      (:class:`~ccs.core.exceptions.CoherenceDegradedWarning`) and the appliance
      operates best-effort (coherence may be off). Mirrors the other adapters.
    """

    def __init__(
        self,
        root: str | os.PathLike[str],
        *,
        managed: tuple[str, ...] = (),
        on_error: Literal["strict", "degrade"] = "strict",
        config: LifecycleConfig | None = None,
        bind_host: str = "127.0.0.1",
    ) -> None:
        if on_error not in ("strict", "degrade"):
            raise ValueError(f"on_error must be 'strict' or 'degrade', got {on_error!r}")

        self._root = Path(root).resolve()
        # Managed globs are coordinator-policy patterns (parent-repo-relative,
        # same grammar as tracked.yaml). They must be tracked AND strict for the
        # INVALID-deny to fire (is_strict_mode ⊂ is_tracked).
        self._managed: tuple[str, ...] = tuple(managed)
        self._on_error = on_error
        self._config = config
        self._bind_host = bind_host

        self._lock = threading.Lock()
        self._tick = 0
        self._degradation_count = 0
        self._endpoint: CoordinatorEndpoint | None = None
        # Per-path state used by the Unit 2 read/write contract; declared here so
        # the fork handler can reset it.
        self._path_map: dict[str, uuid.UUID] = {}
        self._last_committed_hash: dict[str, str] = {}

        self._mint_identity()
        # A forked child must not share the parent's identity (single-writer
        # would conflate them) or its cached endpoint/connection.
        os.register_at_fork(after_in_child=self._after_fork)

        self._attach_with_strict()

    # --- identity -----------------------------------------------------------

    def _mint_identity(self) -> None:
        # A v4 UUID string satisfies the coordinator's session_id regex
        # (``_SESSION_ID_RE``); the server derives a stable agent id from it via
        # ``session_to_agent_id``. Per-instance (not per-process): two volumes in
        # one process are distinct writers.
        self._session_id = str(uuid.uuid4())

    def _after_fork(self) -> None:
        # Runs in the child after fork. Re-mint identity, drop the inherited
        # endpoint (its connection/secret context belongs to the parent), and
        # clear per-path beliefs the child has not established itself.
        with self._lock:
            self._session_id = str(uuid.uuid4())
            self._endpoint = None
            self._tick = 0
            self._path_map.clear()
            self._last_committed_hash.clear()

    @property
    def session_id(self) -> str:
        """The per-instance coordinator session id (a v4 UUID string)."""
        return self._session_id

    @property
    def is_attached(self) -> bool:
        """True if a coordinator endpoint was resolved (strict-mode owner)."""
        return self._endpoint is not None

    # --- spawn-with-strict --------------------------------------------------

    def _attach_with_strict(self) -> None:
        coherence_dir = self._root / ".coherence"
        pid_file = coherence_dir / "server.pid"
        cfg = self._config or LifecycleConfig()

        # Detect a pre-existing (foreign) coordinator BEFORE writing our policy
        # YAML. Strict mode is load-once-at-startup: if a coordinator is already
        # running, our YAML will not take effect, so v1 cannot guarantee
        # enforcement on it. (Foreign-attach is a v1.1 surface.)
        pre_port = read_port_from_file(pid_file)
        foreign = pre_port is not None and tcp_probe(pre_port, cfg, bind_host=self._bind_host)
        if foreign:
            self._fail_closed_or_degrade(
                "a coordinator is already running for this workspace; CoherentVolume v1 "
                "must spawn the coordinator to enable strict-mode enforcement "
                "(foreign-attach is v1.1). Stop the existing coordinator or use a "
                "dedicated workspace root."
            )
            # In degrade mode, continue best-effort: attach to the existing
            # coordinator, but coherence enforcement may be off.
        else:
            self._write_policy_yaml()

        port = connect_or_spawn(self._root, config=cfg, bind_host=self._bind_host)
        if port == -1:
            self._fail_closed_or_degrade(
                "coordinator unavailable (could not spawn or attach for this workspace)"
            )
            self._endpoint = None
            return

        try:
            self._endpoint = resolve_endpoint(self._root)
        except CoordinatorUnavailable as exc:
            self._fail_closed_or_degrade(str(exc))
            self._endpoint = None

    def _write_policy_yaml(self) -> None:
        """Enable strict mode on the managed globs before the coordinator spawns.

        Writes the managed globs to both ``tracked.yaml`` and ``strict_mode.yaml``
        (a path must be tracked AND strict for the deny to fire). Idempotent —
        merges with any existing entries. No-op when ``managed`` is empty.
        """
        if not self._managed:
            return
        coherence_dir = self._root / ".coherence"
        coherence_dir.mkdir(parents=True, exist_ok=True)
        self._merge_yaml_list(coherence_dir / "tracked.yaml", self._managed)
        self._merge_yaml_list(coherence_dir / "strict_mode.yaml", self._managed)

    @staticmethod
    def _merge_yaml_list(path: Path, globs: tuple[str, ...]) -> None:
        existing: list[str] = []
        if path.is_file():
            try:
                loaded = yaml.safe_load(path.read_text(encoding="utf-8"))
            except (yaml.YAMLError, OSError):
                loaded = None
            if isinstance(loaded, list):
                existing = [x for x in loaded if isinstance(x, str)]
        merged = sorted(set(existing) | set(globs))
        if merged == sorted(existing):
            return  # nothing new — avoid a needless rewrite
        # Atomic write so a concurrent coordinator spawn never reads a partial file.
        tmp = path.with_name(path.name + ".tmp")
        tmp.write_text(yaml.safe_dump(merged, default_flow_style=False), encoding="utf-8")
        os.replace(tmp, path)

    # --- strict-mode verification (used by tests + callers) -----------------

    def strict_mode_active(self) -> bool:
        """True if the attached coordinator reports any strict-mode patterns.

        Queries the coordinator ``/status`` policy summary. A freshly spawned
        coordinator that loaded our ``strict_mode.yaml`` reports
        ``strict_mode_pattern_count > 0``. Returns False if unattached or if the
        status surface is unavailable.
        """
        if self._endpoint is None:
            return False
        count = self._policy_summary_value("strict_mode_pattern_count")
        return isinstance(count, int) and count > 0

    def _policy_summary_value(self, key: str) -> object | None:
        if self._endpoint is None:
            return None
        try:
            status = _coordinator_get(self._endpoint, "/status")
        except Exception:  # status is best-effort; any failure → unknown
            return None
        if not isinstance(status, dict):
            return None
        summary = status.get("policy_summary")
        if isinstance(summary, dict) and key in summary:
            return summary[key]
        return status.get(key)

    # --- degrade contract ---------------------------------------------------

    @property
    def is_degraded(self) -> bool:
        return self._degradation_count > 0

    @property
    def degradation_count(self) -> int:
        return self._degradation_count

    def _next_tick(self) -> int:
        with self._lock:
            self._tick += 1
            return self._tick

    def _fail_closed_or_degrade(self, message: str) -> None:
        """Strict → raise (fail-closed); degrade → warn once + count."""
        if self._on_error == "strict":
            raise CoherenceError(message)
        self._record_degraded(message)

    def _record_degraded(self, message: str) -> None:
        with self._lock:
            first = self._degradation_count == 0
            self._degradation_count += 1
        if first:
            warnings.warn(
                f"CoherentVolume degraded: {message}",
                CoherenceDegradedWarning,
                stacklevel=3,
            )
            logger.warning("CoherentVolume degraded under on_error='degrade': %s", message)
