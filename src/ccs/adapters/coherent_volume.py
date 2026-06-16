# Copyright (c) 2026 agent-coherence contributors.
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
from ccs.core.exceptions import (
    OCC_CALLER_TRANSIENT_REASON,
    STALE_READ_GENERATION_REASON,
    CasRetriesExhausted,
    CoherenceDegradedWarning,
    CoherenceError,
)

logger = logging.getLogger(__name__)

__all__ = ["CoherentVolume", "coherent_workspace", "install", "uninstall"]

# Plan Unit 6 (R6): client-side bound on the OCC re-mint→re-commit loop in
# :meth:`CoherentVolume.write_cas`. Mirrors ``SyncStrategy.max_cas_retries``
# (the in-process library knob) — the HTTP path runs its own bounded loop via
# ``_remint()`` + a fresh hash-checked read rather than the
# ``AgentRuntime``/``SyncStrategy`` loop (which governs only the in-process
# library path). Bounds TWO independent failure modes, both fail-closed:
#
# - Total commit (CAS POST) attempts is ``MAX_CAS_REACQUIRES + 1`` (initial +
#   retries); on exhaustion ``write_cas`` raises
#   :class:`~ccs.core.exceptions.CasRetriesExhausted` (a typed terminal, never
#   a silent drop). A stale-denied comparand read never POSTs, so it does NOT
#   consume this budget.
# - CONSECUTIVE stale-denied comparand reads are bounded at
#   ``MAX_CAS_REACQUIRES + 1``; on exhaustion ``write_cas`` raises
#   :class:`~ccs.core.exceptions.CoherenceError` (a view that never clears —
#   wedged coordinator / perpetually lagging disk — must not spin). A clean
#   read resets the streak.
MAX_CAS_REACQUIRES = 8


class CoherentVolume:
    """Coherent shared workspace for a single-host agent fleet (v1).

    .. warning::

       **An instance is NOT thread-safe — one operation at a time, one instance
       per thread (A5).** ``read``/``write``/``write_cas`` read ``_session_id``
       lock-free while ``reacquire``/``_after_fork`` re-mint it, so overlapping
       calls on a SINGLE instance from different threads could split an in-flight
       CAS across identities. This is misuse, and it is made LOUD: a public op
       that detects another op already in flight on the same instance raises
       :class:`~ccs.core.exceptions.CoherenceError` rather than corrupting
       silently. Each thread (and each forked child) must own its OWN instance —
       per-instance identity is exactly what makes distinct writers distinct. The
       guard is re-entrant for the same thread, so the internal
       ``write_cas`` → ``reacquire`` → ``read`` nesting is unaffected.

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
        # A5: single-instance concurrency guard, SEPARATE from self._lock (which
        # protects identity mutation in reacquire/_after_fork). A non-reentrant
        # threading.Lock here would deadlock with the reacquire-within-write_cas
        # path; instead we track the owning thread + a re-entry depth so the SAME
        # thread's nested internal calls (write_cas → reacquire → read) pass while
        # an OVERLAPPING call from a DIFFERENT thread raises. _guard_meta_lock is
        # held only for the microsecond check-then-set, never across an operation.
        self._guard_meta_lock = threading.Lock()
        self._guard_owner_ident: int | None = None
        self._guard_depth = 0
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

    # --- single-instance concurrency guard (A5) -----------------------------

    @contextlib.contextmanager
    def _single_op_guard(self) -> Iterator[None]:
        """Reject overlapping use of ONE instance across threads (A5).

        A :class:`CoherentVolume` instance is single-threaded by contract: one
        operation at a time. Concurrent ``read``/``write``/``write_cas`` on the
        same instance from different threads could split an in-flight CAS across
        identities (``reacquire``/``_after_fork`` re-mint ``_session_id`` while
        the lock-free op path reads it). This guard makes that misuse LOUD rather
        than silently corrupting: a second thread entering while another holds the
        guard raises ``CoherenceError``.

        Re-entrant for the SAME thread so internal nesting works (``write_cas``
        calls :meth:`reacquire`, which calls :meth:`read`): the owning thread
        bumps a depth counter instead of self-deadlocking. A separate
        non-reentrant ``threading.Lock`` would deadlock that path, and silently
        serializing instead of raising would hide the misuse + risk deadlock with
        ``self._lock`` — so we detect-and-raise, never block.
        """
        ident = threading.get_ident()
        with self._guard_meta_lock:
            if self._guard_owner_ident is not None and self._guard_owner_ident != ident:
                raise CoherenceError(
                    "CoherentVolume is single-threaded; concurrent use detected. "
                    "One operation at a time per instance — use one instance per "
                    "thread (an in-flight read/write/write_cas can re-mint identity "
                    "via reacquire, so overlapping ops on one instance could split a "
                    "CAS across identities)."
                )
            self._guard_owner_ident = ident
            self._guard_depth += 1
        try:
            yield
        finally:
            with self._guard_meta_lock:
                self._guard_depth -= 1
                if self._guard_depth <= 0:
                    self._guard_depth = 0
                    self._guard_owner_ident = None

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

        **Not thread-safe (A5).** Overlapping use of this instance from another
        thread raises ``CoherenceError`` (the single-op guard) — one instance per
        thread.
        """
        with self._single_op_guard():
            return self._read_impl(path)

    def _read_impl(self, path: str | os.PathLike[str]) -> bytes:
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

        **Not thread-safe (A5).** Overlapping use of this instance from another
        thread raises ``CoherenceError`` (the single-op guard) — one instance per
        thread.
        """
        with self._single_op_guard():
            self._write_impl(path, data)

    def _write_impl(self, path: str | os.PathLike[str], data: bytes | bytearray) -> None:
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

        # EXCLUSIVE grant is now held. ANY failure before the post-edit commit
        # must release it via the coordinator's tool-failure path (success:false),
        # not only an OSError from the atomic write: an unexpected error while
        # hashing the data or the on-disk file (e.g. MemoryError on a very large
        # file) would otherwise orphan the grant until the crash-recovery sweep.
        try:
            new_hash = self._sha256_bytes(data)
            # No-op skip: skip the os.replace (and its fsync) only when the file
            # ALREADY holds these bytes. _last_committed_hash is a cheap fast-path
            # gate (it skips the disk read on the common "bytes changed" write); the
            # CURRENT on-disk hash is the authority, because the cached belief alone
            # can be stale across a peer commit (see _disk_hash for the full why).
            already_on_disk = (
                self._last_committed_hash.get(rel) == new_hash
                and self._disk_hash(abs_path) == new_hash
            )
            if not already_on_disk:
                self._atomic_write(abs_path, data)
        except Exception:
            # Release the grant so it is not orphaned until the sweep, then
            # re-raise the original error. Best-effort release — a release failure
            # must not mask it.
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
        self._remint()  # fresh identity -> no INVALID; has committed nothing
        # MANDATORY fresh read under the new identity -> SHARED@current.
        return self.read(path)

    def _remint(self) -> None:
        """Re-mint coordinator identity (shed ``INVALID`` + any invalidation
        transient) WITHOUT a read — the OCC :meth:`write_cas` retry primitive.

        Distinct from :meth:`reacquire` (re-mint **and** a read): reacquire's
        read registers the fresh identity as ``SHARED``, after which the
        ``write_cas`` loop's next :meth:`_read_with_version` hits the
        coordinator's *fresh-SHARED* pre-read branch, which returns the version
        WITHOUT re-checking the disk bytes' hash. That pairs a possibly-stale
        on-disk read with a fresh version — and because the commit-CAS checks
        only the *version*, a ``make_content()`` over the stale bytes would WIN
        and silently drop a peer's update (an on-disk lost update; the
        protocol-level ``NoLostUpdate`` still holds — the version-bump count is
        right — but the persisted value is wrong). Re-minting WITHOUT a read
        keeps the loop's next ``_read_with_version`` a **None-state, HASH-CHECKED**
        read (warn on hash match, deny on mismatch), so its ``(bytes, version)``
        comparand pair is always validated.
        """
        with self._lock:
            self._session_id = str(uuid.uuid4())  # fresh identity -> no INVALID
            self._last_committed_hash.clear()  # the new identity has committed nothing

    def write_cas(
        self,
        path: str | os.PathLike[str],
        make_content: Callable[[bytes], bytes | bytearray],
    ) -> None:
        """Optimistically commit a write that BYPASSES the EXCLUSIVE acquire.

        The OCC counterpart to :meth:`write` (plan Unit 6). Unlike ``write`` —
        which takes EXCLUSIVE via ``pre-edit`` before committing — ``write_cas``
        **never acquires EXCLUSIVE**: it reads (→ SHARED), derives the bytes via
        ``make_content(current_bytes)``, and commits through the coordinator's
        ``/hooks/post-edit-cas`` version-checked CAS. The winner is elected by
        the coordinator's serialized commit, so two concurrent OCC writers
        cannot both land the same version — the loser is told ``version_mismatch``
        and retries. The pessimistic ``write()`` path is untouched.

        Conflict recovery: on a ``version_mismatch`` / ``other_holder`` conflict
        (or a stale-read deny) the loop re-mints identity (:meth:`_remint` —
        sheds ``INVALID``, but does NOT do reacquire's read; that read would route
        the next comparand read through the coordinator's unchecked *fresh-SHARED*
        branch and re-open the lost update), then on the next attempt does ONE
        hash-checked fresh read whose ``(bytes, version)`` pair is validated,
        re-derives the bytes from that view via ``make_content``, and retries —
        bounded by :data:`MAX_CAS_REACQUIRES`. On exhaustion it raises
        :class:`~ccs.core.exceptions.CasRetriesExhausted` (a typed terminal,
        NEVER a silent drop).

        **Contention bound.** The commit budget is :data:`MAX_CAS_REACQUIRES`
        (=8 → 9 CAS attempts); each lost race (a peer winning the version)
        costs one. Under very high single-host contention — more concurrent
        writers racing the SAME key than the budget — a writer can exhaust it
        and raise :class:`~ccs.core.exceptions.CasRetriesExhausted`. A
        stale-denied comparand read (the transient window where a peer's commit
        landed but its disk write hasn't) does NOT consume the commit budget;
        instead CONSECUTIVE denied reads are separately bounded (also at
        ``MAX_CAS_REACQUIRES + 1``) and raise ``CoherenceError`` if the view
        never clears. Both terminals are the honest fail-closed outcome,
        **never** a silent lost update: the invariant this method guarantees is
        *final == start + every applied delta, OR a typed raise* — a successful
        return always means the update landed.

        ``make_content`` is invoked once per attempt with the freshly-read
        current bytes and returns the bytes to commit — re-deriving the caller's
        intent against the latest state is what makes the retry an *update*
        rather than a stale overwrite. A caller that ignores its ``bytes``
        argument and returns a buffer computed from older bytes defeats the
        guard (the one fundamental OCC-proof boundary; same as :meth:`reacquire`).

        **A deny ALWAYS raises, in both ``on_error`` modes** — including the
        coordinator's fail-closed degrade body
        (``{ok: false, degraded: true, reason: "commit_unconfirmed"}``): a
        degraded CAS is unconfirmed, so it must read as failure (the client must
        never assume the write landed). The SAME fail-closed rule covers a
        mid-commit transport failure that ``degrade`` mode swallowed (``_post``
        returned ``None`` after a version was read): an unconfirmed CAS must
        never write its bytes to disk, so it raises in both modes rather than
        best-effort writing (unconfirmed bytes on disk would re-open the
        lost-update). ``on_error`` still governs whether an infra failure raises
        vs. warns *inside* ``_post`` and the genuinely-unattached
        (``_endpoint is None``) degrade branch above — but never lets an
        unconfirmed OCC commit silently land.

        **Not thread-safe (A5).** Overlapping use of this instance from another
        thread raises ``CoherenceError`` (the single-op guard). The internal
        re-mint + re-read retries run on the SAME thread under the guard already
        held by this call — one instance per thread.
        """
        with self._single_op_guard():
            self._write_cas_impl(path, make_content)

    def _write_cas_impl(
        self,
        path: str | os.PathLike[str],
        make_content: Callable[[bytes], bytes | bytearray],
    ) -> None:
        self._ensure_attached()
        _abs_path, rel = self._to_relative(path)

        if self._endpoint is None:
            # Unattached / degraded coordinator (strict already raised at
            # construction). Degrade mode: best-effort write, no enforcement —
            # there is no version to CAS against, so derive from current bytes.
            current = self._current_bytes_or_empty(_abs_path)
            data = bytes(make_content(current))
            self._atomic_write(_abs_path, data)
            self._last_committed_hash[rel] = self._sha256_bytes(data)
            return

        last_current_version = -1
        max_attempts = MAX_CAS_REACQUIRES + 1  # commit (CAS POST) budget
        cas_attempts = 0
        denied_streak = 0  # CONSECUTIVE stale-denied comparand reads
        while True:
            # Fresh read each attempt. _read_with_version returns the bytes, the
            # coordinator's authoritative version (the OCC comparand), and whether
            # the read was a strict-deny (stale_denied — this instance is INVALID,
            # or a re-minted identity whose disk read does not match the recorded
            # content). The comparand read ALWAYS runs under a None-state identity
            # (the first read's, or a _remint()ed one), so the coordinator
            # HASH-CHECKS it: warn (re-grant SHARED) on a hash match, deny on a
            # mismatch. That is exactly what makes (current_bytes,
            # expected_version) a VALIDATED pair — current_bytes is the content
            # the coordinator records at expected_version, so make_content()
            # derives the successor from the right state. (Re-minting WITHOUT a
            # read is the point: reacquire()'s read would leave the identity
            # SHARED and route the next comparand read through the coordinator's
            # fresh-SHARED branch, which returns the version WITHOUT a hash
            # check — see _remint() / KTD-LU.)
            current_bytes, expected_version, stale_denied = self._read_with_version(rel)
            if stale_denied:
                # Cannot CAS from this view: INVALID, or the disk lags a just-
                # committed version whose peer write has not landed yet
                # (hash_differs under strict). Re-mint identity (sheds INVALID +
                # the invalidation transient) and RE-READ — once the peer's disk
                # write lands, the hash-checked None-state read yields a validated
                # comparand pair. A denied read never POSTs a commit, so it does
                # NOT consume the CAS budget (max_attempts counts *commit
                # attempts*, keeping MAX_CAS_REACQUIRES' documented semantic);
                # instead the CONSECUTIVE-denied streak is bounded so a
                # never-clearing view (wedged coordinator, perpetually lagging
                # disk, a starving foreign writer) still fails closed instead of
                # spinning. A clean read resets the streak.
                last_current_version = max(last_current_version, expected_version)
                denied_streak += 1
                if denied_streak > MAX_CAS_REACQUIRES:
                    raise CoherenceError(
                        f"OCC comparand read of {rel} stayed strict-denied across "
                        f"{denied_streak} consecutive reads under re-minted "
                        "identities; cannot establish a clean (bytes, version) "
                        "view to CAS from — the on-disk content may be lagging "
                        "peer commits or the coordinator may be wedged. No write "
                        "landed (fail-closed)."
                    )
                self._remint()
                continue
            denied_streak = 0

            data = bytes(make_content(current_bytes))
            new_hash = self._sha256_bytes(data)

            # CAS FIRST, write to disk only on WIN. An OCC writer is S/I — it
            # holds no EXCLUSIVE grant, so unconfirmed bytes must NEVER touch
            # disk (a denied/degraded CAS landing on disk would be the very
            # lost-update this guards). Contrast the pessimistic write(), which
            # writes between pre-edit (EXCLUSIVE held) and post-edit.
            cas_attempts += 1
            resp = self._post(
                "/hooks/post-edit-cas",
                {
                    "session_id": self._session_id,
                    "path": rel,
                    "success": True,
                    "content_hash": new_hash,
                    "expected_version": expected_version,
                },
            )
            if resp is None:
                # Degrade mode swallowed a mid-operation transport/infra failure
                # in _post and returned None — the CAS was NOT confirmed. An OCC
                # writer holds NO grant (S/I), so unconfirmed bytes must NEVER
                # touch disk: writing them would re-open the very lost-update this
                # guards. Fail closed in BOTH on_error modes — identical to how
                # the {ok:false, degraded:true, commit_unconfirmed} degrade BODY
                # is handled below ("raise"). on_error governs only whether the
                # infra failure already warned (degrade) or raised (strict) inside
                # _post; either way an unconfirmed CAS must read as failure.
                raise CoherenceError(
                    f"OCC commit of {rel} could not be confirmed (coordinator "
                    "transport failed mid-commit); the write did not land. "
                    "reacquire() and retry from the fresh bytes."
                )

            outcome = self._classify_cas_response(resp)
            if outcome == "win":
                self._atomic_write(_abs_path, data)  # confirmed → persist
                self._last_committed_hash[rel] = new_hash
                return
            if outcome == "conflict":
                last_current_version = self._cas_current_version(resp, last_current_version)
                if cas_attempts >= max_attempts:
                    # Every allowed commit attempt lost the race — typed
                    # terminal, never a silent drop. (last_current_version is
                    # the latest version the loser observed.)
                    raise CasRetriesExhausted(
                        artifact_id=rel,
                        attempts=cas_attempts,
                        last_current_version=last_current_version,
                    )
                # Re-mint (NOT reacquire) before the next attempt so the next
                # comparand read is a hash-checked None-state read that sees the
                # winner's state — reacquire()'s read would route it through the
                # unchecked fresh-SHARED branch and could pair stale bytes with a
                # fresh version (the on-disk lost update; see _remint() / KTD-LU).
                self._remint()
                continue
            # outcome == "raise": a deny, corruption, or the fail-closed
            # commit_unconfirmed degrade body — ALWAYS raises in both modes.
            # Nothing was written to disk (the CAS did not win).
            raise CoherenceError(self._deny_reason(resp))

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

    @staticmethod
    def _disk_hash(abs_path: Path) -> str | None:
        """SHA-256 of the CURRENT on-disk bytes, or ``None`` if the file is
        absent/unreadable.

        The write no-op-skip uses this to confirm the file ALREADY holds the
        bytes being committed before skipping the ``os.replace``. A per-instance
        cached hash (``_last_committed_hash``) can be stale across a peer commit:
        on a tracked-but-non-strict glob the coordinator re-grants the write
        without a deny, so the cache still reads this instance's OWN last hash
        while disk holds the peer's bytes. The cache is therefore only a cheap
        fast-path gate; the on-disk hash is the authority for whether a write is
        truly a no-op. Reads via :meth:`_read_file_bytes` (raw ``os.read``) so it
        is never self-intercepted by the ``install()`` open()-shim."""
        try:
            return CoherentVolume._sha256_bytes(CoherentVolume._read_file_bytes(abs_path))
        except OSError:
            return None

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

    def _read_with_version(self, rel: str) -> tuple[bytes, int, bool]:
        """OCC helper: register a SHARED view and return
        ``(bytes, version, stale_denied)``.

        Mirrors :meth:`read` (same pre-read call + same fail-closed degrade
        handling) but also surfaces the coordinator's authoritative ``version``
        — the comparand ``write_cas`` passes as ``expected_version`` — and
        whether this instance is INVALID (a sticky strict-deny: the pre-read
        returned a ``stale`` response and did NOT re-grant SHARED, KTD-T).
        ``read`` itself stays ``bytes``-returning (a public contract); this is
        the internal version-aware variant ``write_cas`` drives.

        A fresh pre-read carries ``{"status": "fresh", "version": N}``; a
        warn-mode stale read and a strict-deny both carry ``status == "stale"``
        with the version under ``summary.current_version``. ``stale_denied`` is
        True on the strict-deny case — an INVALID writer cannot CAS until it
        :meth:`reacquire`s. If the version cannot be resolved (older coordinator
        / degraded status surface) ``0`` is used — a CAS against
        ``expected_version=0`` on a non-empty artifact loses cleanly
        (``version_mismatch``), so the fallback never causes a silent overwrite.
        """
        abs_path, _rel = self._to_relative(rel)
        if not abs_path.is_file():
            raise FileNotFoundError(f"no such file in workspace: {_rel}")
        data = self._read_file_bytes(abs_path)
        version = 0
        stale_denied = False
        if self._endpoint is not None:
            resp = self._post(
                "/hooks/pre-read",
                {
                    "session_id": self._session_id,
                    "path": _rel,
                    "content_hash": self._sha256_bytes(data),
                },
            )
            if isinstance(resp, dict):
                if resp.get("degraded"):
                    self._fail_closed_or_degrade(
                        f"coordinator watchdog timeout during read of {_rel}"
                    )
                version = self._pre_read_version(resp)
                # A strict-deny (INVALID, NOT re-granted — KTD-T) is the only
                # pre-read outcome that leaves this instance unable to CAS: it
                # stays INVALID AND keeps the invalidation transient the peer
                # commit set, which commit_cas rejects as a precondition. A
                # warn-mode stale read (permissionDecision == "allow") re-grants
                # SHARED, so it is NOT treated as denied here. The distinguisher
                # is permissionDecision == "deny" (set only by emit_strict_deny).
                hook_output = resp.get("hookSpecificOutput")
                if isinstance(hook_output, dict):
                    stale_denied = hook_output.get("permissionDecision") == "deny"
        return data, version, stale_denied

    @staticmethod
    def _pre_read_version(resp: dict) -> int:
        """Extract the coordinator version from a pre-read response (fresh or
        stale shape). Returns 0 when absent (older coordinator / degraded)."""
        v = resp.get("version")
        if isinstance(v, int) and not isinstance(v, bool):
            return v
        summary = resp.get("summary")
        if isinstance(summary, dict):
            cv = summary.get("current_version")
            if isinstance(cv, int) and not isinstance(cv, bool):
                return cv
        return 0

    def _current_bytes_or_empty(self, abs_path: Path) -> bytes:
        """Degrade-path read of the on-disk bytes (b\"\" if the file is absent),
        so ``make_content`` always gets a defined current view."""
        if not abs_path.is_file():
            return b""
        return self._read_file_bytes(abs_path)

    # Retry-eligible OCC conflict reasons matched EXACTLY against the wire
    # ``reason``: the typed ``ConflictDetail`` reasons plus
    # ``caller_in_transient_state`` (AC2 — a peer invalidated us mid-window).
    # The transient literal is the SHARED constant the coordinator server emits,
    # so a reword on either side can't drift the retry classification.
    _CAS_RETRY_REASONS: frozenset[str] = frozenset(
        {
            "version_mismatch",
            "other_holder",
            OCC_CALLER_TRANSIENT_REASON,
            # Read-generation fence: a reclaimed reader's OCC commit_cas returns
            # ConflictDetail("stale_read_generation"); retry via reacquire +
            # fresh read (the next fetch captures the current generation).
            STALE_READ_GENERATION_REASON,
        }
    )

    def _classify_cas_response(self, resp: dict) -> Literal["win", "conflict", "raise"]:
        """Map a ``/hooks/post-edit-cas`` 200 body to an OCC outcome.

        - ``ok: true``  → ``"win"`` (the CAS committed; version bumped).
        - ``ok: false`` with a retry-eligible reason → ``"conflict"`` (reacquire
          + retry). Matched EXACTLY against :attr:`_CAS_RETRY_REASONS`: the
          typed ``ConflictDetail`` reasons (``version_mismatch`` /
          ``other_holder`` / ``stale_read_generation`` — the read-generation
          fence: the caller's captured claim was superseded by a sweep
          reclamation; a reacquire + fresh read mints a current claim)
          AND ``caller_in_transient_state`` — when a peer
          invalidates this instance in the window BETWEEN its fresh read and its
          CAS, the coordinator leaves an invalidation transient that
          ``commit_cas`` rejects as a precondition; that is a lost race, not
          corruption, so reacquire + retry (a fresh identity has no transient).
          The transient reason is the shared
          :data:`~ccs.core.exceptions.OCC_CALLER_TRANSIENT_REASON` the server
          emits, so the match is exact (no brittle substring) and cannot drift.
        - ``ok: false`` otherwise — true corruption (``commit_cas_corruption``,
          ``expected > current``) OR the fail-closed ``{degraded: true,
          reason: "commit_unconfirmed"}`` body → ``"raise"``. The degrade body
          deliberately reads as failure so the client never mistakes an
          unconfirmed CAS for a landed write.
        """
        if resp.get("ok") is True:
            return "win"
        reason = resp.get("reason")
        if reason in self._CAS_RETRY_REASONS:
            return "conflict"
        return "raise"

    @staticmethod
    def _cas_current_version(resp: dict, fallback: int) -> int:
        cv = resp.get("current_version")
        if isinstance(cv, int) and not isinstance(cv, bool):
            return cv
        return fallback

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
