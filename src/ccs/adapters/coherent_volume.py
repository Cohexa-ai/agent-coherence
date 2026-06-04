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

The façade scaffolding (Unit 1) is spawn-with-strict enablement, per-instance
identity (fork-safe), and the fail-closed degrade contract. The sequential
read/write/reacquire contract (Unit 2) builds on it: :meth:`CoherentVolume.read`
registers a SHARED view, :meth:`CoherentVolume.write` acquires EXCLUSIVE (or
fails closed on a stale-view deny), and :meth:`CoherentVolume.reacquire`
recovers from the sticky strict deny. The ``install()`` shim (Unit 3) is next.
"""

from __future__ import annotations

import builtins
import contextlib
import hashlib
import io
import logging
import os
import threading
import urllib.error
import uuid
import warnings
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Literal

import yaml

from ccs.adapters.claude_code.lifecycle import (
    LifecycleConfig,
    connect_or_spawn,
    read_port_from_file,
    tcp_probe,
)
from ccs.adapters.claude_code.policy import _matches_any
from ccs.cli._coherence_client import (
    CoordinatorEndpoint,
    CoordinatorUnavailable,
    resolve_endpoint,
)
from ccs.cli._coherence_client import (
    get as _coordinator_get,
)
from ccs.cli._coherence_client import (
    post as _coordinator_post,
)
from ccs.core.exceptions import CoherenceDegradedWarning, CoherenceError

logger = logging.getLogger(__name__)

__all__ = ["CoherentVolume", "coherent_workspace", "install", "uninstall"]


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

    **Fleet requirement (hard, v1).** Every instance coordinating a given
    workspace MUST declare the **same** ``managed`` globs. Strict enforcement is
    verified only coarsely (the coordinator's ``/status`` exposes a strict-pattern
    *count*, not the patterns), so a sibling whose globs **differ** from the
    spawner's passes construction yet its own paths are **not** strict — its stale
    writes then land **with no signal** (``is_degraded`` stays ``False``). A
    precise per-glob check needs coordinator support; until then a
    heterogeneous-globs fleet is unsupported (v1.1).
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
        self._degradation_count = 0
        self._endpoint: CoordinatorEndpoint | None = None
        # Set by the fork child-handler so the next read/write re-attaches (the
        # child cannot re-attach inside the fork handler — see _ensure_attached).
        self._needs_reattach = False
        # Per-path commit hashes for the Unit 2 write no-op-skip; declared here so
        # the fork handler and reacquire() can reset it.
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
            self._needs_reattach = True
            self._last_committed_hash.clear()

    def _ensure_attached(self) -> None:
        """Lazily re-attach after a fork dropped the endpoint.

        ``_after_fork`` re-mints identity and clears the endpoint but cannot
        re-attach in the fork handler (the coordinator-client context is the
        parent's). The child's first ``read``/``write`` re-attaches here,
        sibling-attaching to the coordinator under the child's fresh identity.
        A no-op outside the post-fork window.
        """
        if self._endpoint is None and self._needs_reattach:
            self._needs_reattach = False
            self._attach_with_strict()

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

        # Strict mode is load-once-at-startup, so only the process that SPAWNS
        # the coordinator can enable it. Write our policy YAML only when no
        # coordinator is running yet (we are about to spawn it). If one is
        # already running we must NOT mutate its policy files: our write cannot
        # take effect (load-once), and clobbering a foreign coordinator's policy
        # is a side effect on someone else's workspace. We verify enforcement
        # after attaching instead.
        pre_port = read_port_from_file(pid_file)
        pre_existing = pre_port is not None and tcp_probe(
            pre_port, cfg, bind_host=self._bind_host
        )
        if not pre_existing:
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
            return

        # Verify strict mode is actually enforceable for the managed paths. One
        # post-attach check covers three cases:
        #   * we spawned the coordinator     -> it loaded our YAML -> strict on
        #   * a sibling appliance spawned it  -> strict already on  -> the FLEET
        #     case (two volumes coordinating one workspace): attaching is correct
        #   * a foreign coordinator (e.g. a Claude Code session) -> strict OFF and
        #     we cannot enable it (load-once) -> fail closed.
        # NB: strict_mode_active() is a COARSE "any strict pattern present" check
        # (the /status summary exposes only a count, not the patterns). HARD v1
        # REQUIREMENT: every instance coordinating a workspace must declare the
        # SAME managed globs. A sibling whose globs differ from the spawner's
        # passes this check (count > 0 from the spawner's globs) yet its own paths
        # are NOT strict — its stale writes then land with NO signal (is_degraded
        # stays False). A precise per-glob check needs coordinator support; until
        # then a heterogeneous-globs fleet is unsupported (v1.1).
        if self._managed and not self.strict_mode_active():
            self._fail_closed_or_degrade(
                "attached to a coordinator that does not enforce strict mode for the "
                "managed paths. CoherentVolume v1 can enable strict mode only on a "
                "coordinator it (or a sibling appliance) spawned — not on a foreign / "
                "already-running coordinator (load-once policy). Stop the existing "
                "coordinator, use a dedicated workspace root, or wait for foreign-attach "
                "(v1.1). In degrade mode the volume operates best-effort with coherence "
                "enforcement off."
            )
            # Degrade mode fell through (strict raised above). Do NOT keep a live
            # endpoint to a coordinator that does not enforce our paths — that
            # would route reads/writes through a non-strict coordinator while
            # is_attached reported True. Mirror the other two degrade branches.
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
        # UUID-suffix the temp name (like _atomic_write) so a predictable ``.tmp``
        # path can't be pre-placed as a symlink by a local same-uid process (the
        # write_text would otherwise follow it).
        tmp = path.with_name(f"{path.name}.{uuid.uuid4().hex}.tmp")
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

    # --- read / write / reacquire contract (Unit 2) -------------------------

    def read(self, path: str | os.PathLike[str]) -> bytes:
        """Read a workspace file and register a SHARED view of it.

        Always returns the current on-disk bytes. The ``pre-read`` call records
        this instance as a SHARED reader@hash on the coordinator — that is what
        makes a *later* peer write invalidate this instance (the sequential
        stale-read→write guard). A stale-read response never makes ``read()``
        raise: returning current bytes is always safe.

        The strict deny is **sticky** (KTD-T): if this instance is already
        ``INVALID`` on a strict path, ``pre-read`` does NOT re-grant SHARED, so
        ``read()`` returns the fresh bytes yet the instance stays ``INVALID``
        and a subsequent :meth:`write` is still denied. Recovery is
        :meth:`reacquire`, not a bare ``read()``.

        Raises ``FileNotFoundError`` for a missing file (no phantom artifact is
        seeded). Under ``on_error="strict"`` a coordinator-infrastructure
        failure (unavailable coordinator, watchdog timeout) raises
        ``CoherenceError`` — a read whose coherence cannot be registered fails
        closed.
        """
        self._ensure_attached()
        abs_path, rel = self._to_relative(path)
        # Stat before registering so a missing file raises rather than seeding a
        # phantom artifact in the coordinator registry.
        if not abs_path.is_file():
            raise FileNotFoundError(f"no such file in workspace: {rel}")
        data = self._read_file_bytes(abs_path)  # empty file -> b"" -> sha256(b"")
        if self._endpoint is not None:
            resp = self._post(
                "/hooks/pre-read",
                {
                    "session_id": self._session_id,
                    "path": rel,
                    "content_hash": self._sha256_bytes(data),
                },
            )
            # A stale / strict-deny response is expected and changes nothing here
            # (read returns current bytes; INVALID stays sticky). Only a watchdog
            # timeout is an infra failure that fails closed.
            if isinstance(resp, dict) and resp.get("degraded"):
                self._fail_closed_or_degrade(
                    f"coordinator watchdog timeout during read of {rel}"
                )
        return data

    def write(self, path: str | os.PathLike[str], data: bytes | bytearray) -> None:
        """Write bytes to a workspace file under the single-writer guard.

        Prevents the **sequential** stale-read→write lost update: if a peer
        committed a newer version since this instance last read, this instance
        is ``INVALID`` and the coordinator denies the write (``pre-edit``). The
        deny is surfaced as ``CoherenceError`` carrying the coordinator's
        byte-stable reason VERBATIM. **A deny always raises, in both
        ``on_error`` modes** — the deny is enforcement working, not an
        infrastructure failure; recover via :meth:`reacquire` and write from the
        fresh bytes.

        Does NOT prevent concurrent racing writers (this is
        single-writer-by-invalidation, not a mutex) nor a caller that ignores
        :meth:`reacquire`'s fresh bytes. ``on_error`` governs only
        coherence-infrastructure failures (unavailable coordinator, watchdog
        timeout): strict raises, degrade warns once and writes best-effort.
        """
        if not isinstance(data, (bytes, bytearray)):
            raise TypeError("CoherentVolume.write expects bytes")
        data = bytes(data)
        self._ensure_attached()
        abs_path, rel = self._to_relative(path)

        if self._endpoint is None:
            # Unattached / degraded coordinator (strict already raised at
            # construction). Degrade mode: best-effort write, no enforcement.
            self._atomic_write(abs_path, data)
            self._last_committed_hash[rel] = self._sha256_bytes(data)
            return

        # pre-edit: acquire EXCLUSIVE, or be denied because we are INVALID.
        resp = self._post("/hooks/pre-edit", {"session_id": self._session_id, "path": rel})
        if resp is not None:
            self._check_grant(resp, rel, phase="pre-edit")

        new_hash = self._sha256_bytes(data)
        # No-op skip: bytes identical to our own last commit while we hold a fresh
        # grant means the file already has them — skip the os.replace (no
        # filesystem churn) but still finalize the grant via post-edit below.
        if self._last_committed_hash.get(rel) != new_hash:
            try:
                self._atomic_write(abs_path, data)
            except OSError:
                # The FS write failed AFTER pre-edit granted EXCLUSIVE. Release the
                # grant via the coordinator's tool-failure path so it is not
                # orphaned until the sweep, then re-raise the original error.
                # Best-effort release — a release failure must not mask the OSError.
                with contextlib.suppress(Exception):
                    _coordinator_post(
                        self._endpoint,
                        "/hooks/post-edit",
                        {"session_id": self._session_id, "path": rel, "success": False},
                    )
                raise

        post_resp = self._post(
            "/hooks/post-edit",
            {
                "session_id": self._session_id,
                "path": rel,
                "success": True,
                "content_hash": new_hash,
            },
        )
        if post_resp is not None:
            # ok:false here means the grant was preempted / sweep-reclaimed mid-
            # write (a concurrent-writer case v1 does not claim to serialize);
            # fail closed so the caller knows the commit did not register.
            self._check_grant(post_resp, rel, phase="post-edit")
        self._last_committed_hash[rel] = new_hash

    def reacquire(self, path: str | os.PathLike[str]) -> bytes:
        """Recover from a sticky strict deny and return the current bytes.

        The strict deny is sticky: once ``INVALID``, a bare :meth:`read` does
        NOT clear it (KTD-T). Recovery requires a fresh coordinator identity AND
        a fresh read under that identity, atomically: the new identity carries
        no ``INVALID`` state, and the mandatory read registers it as
        SHARED@current.

        **The forced read is non-optional.** Re-minting identity *without*
        reading would merely rename the stale-buffer hole — the next write would
        be granted while the caller still holds pre-reacquire bytes. The caller
        MUST write from the bytes this method returns (or a later :meth:`read`),
        never from a buffer computed before ``reacquire()``; v1 cannot catch a
        caller that ignores them (the one fundamental, OCC-proof boundary).

        Re-minting the identity resets this instance's view of *every* path, not
        just ``path`` — other tracked paths should be re-read before writing.
        """
        with self._lock:
            self._session_id = str(uuid.uuid4())  # fresh identity -> no INVALID
            self._last_committed_hash.clear()  # the new identity has committed nothing
        # MANDATORY fresh read under the new identity -> SHARED@current.
        return self.read(path)

    # --- coordinator I/O helpers --------------------------------------------

    def _to_relative(self, path: str | os.PathLike[str]) -> tuple[Path, str]:
        """Resolve ``path`` to ``(absolute, workspace-relative-posix)``. Raises
        ``CoherenceError`` if it escapes the workspace root."""
        abs_path = Path(path)
        if not abs_path.is_absolute():
            abs_path = self._root / abs_path
        abs_path = abs_path.resolve()
        try:
            rel = abs_path.relative_to(self._root)
        except ValueError as exc:
            raise CoherenceError(
                f"path is outside the CoherentVolume root {self._root}: {abs_path}"
            ) from exc
        return abs_path, rel.as_posix()

    @staticmethod
    def _sha256_bytes(data: bytes) -> str:
        return hashlib.sha256(data).hexdigest()

    @staticmethod
    def _read_file_bytes(abs_path: Path) -> bytes:
        """Read a file via raw ``os.read`` (NOT ``builtins.open``), so the
        volume's own I/O is never self-intercepted by the ``install()``
        open()-shim (which patches ``builtins.open`` / ``io.open`` only — a
        ``read_bytes`` here would recurse back into the shim)."""
        fd = os.open(abs_path, os.O_RDONLY)
        try:
            chunks: list[bytes] = []
            while True:
                chunk = os.read(fd, 65536)
                if not chunk:
                    break
                chunks.append(chunk)
            return b"".join(chunks)
        finally:
            os.close(fd)

    def _atomic_write(self, abs_path: Path, data: bytes) -> None:
        """Durable, atomic replace via raw ``os.write`` + ``os.replace`` (NOT
        ``builtins.open``), so the volume's own I/O is never self-intercepted by
        the ``install()`` open()-shim — an ``open(tmp, "wb")`` here would route
        the temp file (it matches the managed glob) back through the shim and
        recurse infinitely."""
        abs_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = abs_path.with_name(f"{abs_path.name}.{self._session_id}.tmp")
        try:
            fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
            try:
                view = memoryview(data)
                while view:
                    view = view[os.write(fd, view):]
                os.fsync(fd)
            finally:
                os.close(fd)
            os.replace(tmp, abs_path)
        except OSError:
            # Don't leave an orphan temp on a failed write; re-raise so write()
            # releases the grant and propagates the error.
            with contextlib.suppress(OSError):
                os.unlink(tmp)
            raise

    def _post(self, endpoint_path: str, payload: dict) -> dict | None:
        """POST to the coordinator. Transport errors and non-2xx HTTP responses
        route through ``on_error`` (strict raises, degrade warns + returns
        ``None``). Otherwise returns the parsed 200 body."""
        try:
            return _coordinator_post(self._endpoint, endpoint_path, payload)
        except (CoordinatorUnavailable, urllib.error.HTTPError) as exc:
            self._fail_closed_or_degrade(
                f"coordinator request to {endpoint_path} failed: {exc}"
            )
            return None  # reached only in degrade mode

    def _check_grant(self, resp: dict, rel: str, *, phase: str) -> None:
        """Map a coordinator pre-/post-edit response to the fail-closed contract.

        A deny (``ok: false`` — strict-deny or collision/preempt) ALWAYS raises
        ``CoherenceError`` with the reason verbatim, in both ``on_error`` modes:
        the deny is the enforcement signal. A watchdog-timeout degrade
        (``degraded: true``) is an infra failure and routes through ``on_error``.
        """
        if not isinstance(resp, dict):
            return
        if resp.get("ok") is False:
            raise CoherenceError(self._deny_reason(resp))
        if resp.get("degraded"):
            self._fail_closed_or_degrade(
                f"coordinator watchdog timeout on {phase} for {rel}"
            )

    @staticmethod
    def _deny_reason(resp: dict) -> str:
        """Extract the coordinator's deny reason VERBATIM (regenerating it
        worsens model retries — auto memory: project_cc_strict_mode_retry_hazard)."""
        hook_output = resp.get("hookSpecificOutput")
        if isinstance(hook_output, dict):
            reason = hook_output.get("permissionDecisionReason")
            if isinstance(reason, str) and reason:
                return reason
        reason = resp.get("reason")
        if isinstance(reason, str) and reason:
            return reason
        return (
            "coherence coordinator denied the write (stale view); "
            "reacquire() and write from the fresh bytes"
        )

    # --- degrade contract ---------------------------------------------------

    @property
    def is_degraded(self) -> bool:
        return self._degradation_count > 0

    @property
    def degradation_count(self) -> int:
        return self._degradation_count

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


# ----------------------------------------------------------------------
# install() — opt-in builtins.open / io.open shim (Unit 3, demo-grade)
# ----------------------------------------------------------------------
#
# Routes opens of *managed* paths through a process-singleton CoherentVolume so
# existing code gets coherence without changing its open()/pathlib calls. This
# is a DEMO-GRADE convenience layer; the explicit CoherentVolume read/write/
# reacquire API is the supported primitive. Coverage matrix (single-host, one
# process + its forks):
#
#   COORDINATED   builtins.open(p, "r"|"rb"|"w"|"wb") for a managed path, and
#                 pathlib Path.open / read_text / write_text / read_bytes /
#                 write_bytes — they call io.open, which we patch ALONGSIDE
#                 builtins.open (patching builtins.open alone does NOT catch
#                 pathlib; verified empirically).
#   NOT COVERED   os.open / os.write (raw fds), subprocess / shell redirection
#                 ("echo >> p"), mmap, C-level writes, and append / update /
#                 exclusive modes ("a", "r+", "x") — all delegate to the
#                 original open() unchanged.
#
# Recovery: a managed read() through the shim registers a SHARED view, so a peer
# commit makes the next shim'd write fail closed (raises out of close()). The
# deny is STICKY — a bare re-open for read does NOT clear it; recovery uses the
# explicit volume.reacquire() (the shim deliberately keeps open() semantics
# simple rather than auto-refetching, which would defeat the guard).


class _CommitOnCloseMixin:
    """In-memory write buffer that commits through the volume on ``close()``.

    A stale-view deny raises out of ``close()`` (fail-closed), so a
    ``with open(p, "w") as f: ...`` block surfaces the lost-update guard. The
    bytes never touch disk except via the volume's atomic write.
    """

    _volume: CoherentVolume
    _rel: str
    _encoding: str | None  # None => binary mode
    _committed: bool

    def __exit__(self, exc_type, exc_val, exc_tb):  # type: ignore[override]
        # If the ``with`` body raised, DISCARD the buffered write rather than
        # commit a partial/abandoned buffer. No coordinator grant was acquired
        # (the EXCLUSIVE acquire happens inside volume.write, only on a clean
        # commit in close()), so there is nothing to release — skipping the
        # commit is sufficient and leaks nothing.
        if exc_type is not None:
            self._committed = True
        return super().__exit__(exc_type, exc_val, exc_tb)  # type: ignore[misc]

    def close(self) -> None:  # type: ignore[override]
        if self._committed or self.closed:  # type: ignore[attr-defined]
            super().close()  # type: ignore[misc]
            return
        self._committed = True
        try:
            payload = self.getvalue()  # type: ignore[attr-defined]
            if self._encoding is not None:
                payload = payload.encode(self._encoding)
            self._volume.write(self._rel, payload)
        finally:
            super().close()  # type: ignore[misc]


class _CoherentBytesWriter(_CommitOnCloseMixin, io.BytesIO):
    def __init__(self, volume: CoherentVolume, rel: str) -> None:
        io.BytesIO.__init__(self)
        self._volume = volume
        self._rel = rel
        self._encoding = None
        self._committed = False


class _CoherentTextWriter(_CommitOnCloseMixin, io.StringIO):
    def __init__(self, volume: CoherentVolume, rel: str, encoding: str) -> None:
        io.StringIO.__init__(self)
        self._volume = volume
        self._rel = rel
        self._encoding = encoding
        self._committed = False


class _ShimState:
    """Process-global state for the installed shim (one workspace per process)."""

    def __init__(self, volume: CoherentVolume, original_open: Callable) -> None:
        self.volume = volume
        self.original_open = original_open


_shim_state: _ShimState | None = None


def _managed_rel(volume: CoherentVolume, file: object) -> str | None:
    """Return the workspace-relative posix path if ``file`` is a managed path
    under the volume root, else ``None`` (→ delegate to the original open()).

    Never raises: an fd int, a bytes path, an outside-root or unresolvable path
    all fall back to ``None`` so the original open() handles them unchanged.
    """
    if isinstance(file, int):  # raw fd — not a path
        return None
    try:
        raw = os.fspath(file)
    except TypeError:
        return None
    if not isinstance(raw, str):  # bytes path — demo-grade: delegate
        return None
    try:
        # Resolve like builtins.open (relative → against CWD), then require it be
        # under the volume root.
        rel = Path(raw).resolve().relative_to(volume._root).as_posix()
    except (ValueError, OSError):
        return None
    # Use the coordinator's own glob matcher so the shim's notion of "managed" is
    # exactly the coordinator's tracked set (handles ``**`` segment globs).
    return rel if _matches_any(rel, volume._managed) else None


def _make_open_wrapper(volume: CoherentVolume, original_open: Callable) -> Callable:
    def coherent_open(file, mode="r", *args, **kwargs):
        rel = _managed_rel(volume, file)
        if rel is None:
            return original_open(file, mode, *args, **kwargs)
        # Demo-grade: only plain read / truncating-write are mediated. Append,
        # update ("+"), and exclusive ("x") modes delegate unchanged.
        if "+" in mode or "a" in mode or "x" in mode:
            return original_open(file, mode, *args, **kwargs)
        if "w" in mode:
            if "b" in mode:
                return _CoherentBytesWriter(volume, rel)
            return _CoherentTextWriter(volume, rel, kwargs.get("encoding") or "utf-8")
        if "r" in mode:
            # Register the SHARED view (raises FileNotFoundError if missing, like
            # open()), then return a real handle over the same on-disk bytes.
            volume.read(rel)
            return original_open(file, mode, *args, **kwargs)
        return original_open(file, mode, *args, **kwargs)

    return coherent_open


def install(
    root: str | os.PathLike[str],
    *,
    managed: tuple[str, ...] = (),
    on_error: Literal["strict", "degrade"] = "strict",
    config: LifecycleConfig | None = None,
    bind_host: str = "127.0.0.1",
) -> CoherentVolume:
    """Patch ``builtins.open`` + ``io.open`` to route managed-path opens through
    a process-singleton :class:`CoherentVolume`. Idempotent — a second call
    returns the already-installed volume (one workspace per process in v1).
    Reverse with :func:`uninstall`, or use the :func:`coherent_workspace` context
    manager. Opt-in and demo-grade — see the coverage matrix above.
    """
    global _shim_state
    if _shim_state is not None:
        return _shim_state.volume
    volume = CoherentVolume(
        root, managed=managed, on_error=on_error, config=config, bind_host=bind_host
    )
    original_open = builtins.open  # is io.open (same object) at install time
    wrapper = _make_open_wrapper(volume, original_open)
    builtins.open = wrapper  # type: ignore[assignment]
    io.open = wrapper  # type: ignore[assignment]  # pathlib routes here, not builtins.open
    _shim_state = _ShimState(volume, original_open)
    return volume


def uninstall() -> None:
    """Restore the original ``builtins.open`` / ``io.open``. Idempotent."""
    global _shim_state
    if _shim_state is None:
        return
    builtins.open = _shim_state.original_open  # type: ignore[assignment]
    io.open = _shim_state.original_open  # type: ignore[assignment]
    _shim_state = None


@contextlib.contextmanager
def coherent_workspace(
    root: str | os.PathLike[str],
    *,
    managed: tuple[str, ...] = (),
    on_error: Literal["strict", "degrade"] = "strict",
    config: LifecycleConfig | None = None,
    bind_host: str = "127.0.0.1",
) -> Iterator[CoherentVolume]:
    """Context manager wrapping :func:`install`/:func:`uninstall`; yields the
    process-singleton :class:`CoherentVolume`. Guarantees the ``open()`` patch is
    reversed on exit even if the body raises.

    Reentrant-safe: if a shim is already installed (e.g. a nested
    ``coherent_workspace``), this yields the existing volume and does NOT
    uninstall on exit — the outer context owns the patch.
    """
    already_installed = _shim_state is not None
    volume = install(
        root, managed=managed, on_error=on_error, config=config, bind_host=bind_host
    )
    try:
        yield volume
    finally:
        if not already_installed:
            uninstall()
