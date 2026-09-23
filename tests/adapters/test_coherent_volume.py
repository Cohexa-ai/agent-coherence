# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Unit 1 tests for CoherentVolume: spawn-with-strict, identity, fail-closed.

These exercise the façade scaffolding only (construction, strict-mode
enablement, per-instance + fork-safe identity, and the on_error contract).
The read/write contract (Unit 2) and the install() shim (Unit 3) are tested
separately.
"""

from __future__ import annotations

import builtins
import io
import logging
import os
import select
import signal
import subprocess
import threading
import time
import warnings
from pathlib import Path
from types import SimpleNamespace

import pytest

import ccs.adapters.coherent_volume as coherent_volume_module
from ccs.adapters.claude_code.coordinator_server import (
    read_subagent_id,
    session_to_agent_id,
)
from ccs.adapters.claude_code.lifecycle import (
    LifecycleConfig,
    ensure_coordinator,
    stop_coordinator,
)
from ccs.adapters.coherent_volume import (
    DENIED_READ_BACKOFF_BASE_SEC,
    DENIED_READ_BACKOFF_CAP_SEC,
    MAX_CAS_REACQUIRES,
    CoherentVolume,
    coherent_workspace,
    install,
    uninstall,
)
from ccs.cli._coherence_client import CoordinatorUnavailable
from ccs.core.exceptions import (
    CasRetriesExhausted,
    CasVersionConflict,
    CoherenceDegradedWarning,
    CoherenceError,
    StaleView,
    ViewWedged,
)


@pytest.fixture
def fast_cfg() -> LifecycleConfig:
    """Coordinator config tuned for fast tests (no idle shutdown)."""
    return LifecycleConfig(
        idle_shutdown_sec=0,
        sweep_interval_sec=0.1,
        notice_evict_max_age_sec=1.0,
        port_file_retry_attempts=20,
        port_file_retry_interval_sec=0.05,
        connect_retry_attempts=10,
        connect_retry_interval_sec=0.05,
    )


def test_construct_spawns_with_strict_enabled(tmp_path: Path, fast_cfg: LifecycleConfig) -> None:
    """Constructing with managed globs spawns a coordinator that actually
    reports strict mode (verified on the coordinator via /status, not just
    the façade's intent)."""
    vol = CoherentVolume(tmp_path, managed=("data/**",), config=fast_cfg)
    try:
        assert vol.is_attached
        assert vol.strict_mode_active() is True
        assert not vol.is_degraded
    finally:
        stop_coordinator(tmp_path)


def test_unmanaged_paths_get_no_strict(tmp_path: Path, fast_cfg: LifecycleConfig) -> None:
    """With no managed globs there is no strict-mode opt-in — documents why
    the managed set is what gives invalidation teeth."""
    vol = CoherentVolume(tmp_path, managed=(), config=fast_cfg)
    try:
        assert vol.is_attached
        assert vol.strict_mode_active() is False
    finally:
        stop_coordinator(tmp_path)


def test_fresh_workspace_coherence_dir_is_0700_without_tighten_warning(
    tmp_path: Path, fast_cfg: LifecycleConfig, caplog: pytest.LogCaptureFixture
) -> None:
    """The pre-spawn policy write creates ``.coherence/`` itself, ahead of the
    lifecycle. It must create it at the 0700 the lifecycle requires; otherwise
    every brand-new workspace spawns with a "tightened existing .coherence
    directory" warning that blames the operator for a directory the volume
    created a moment earlier. umask is pinned to 022 so a permissive default
    ``mkdir`` is observable regardless of the host's setting."""
    prior_umask = os.umask(0o022)
    try:
        caplog.set_level(logging.WARNING, logger="ccs.adapters.claude_code.lifecycle")
        vol = CoherentVolume(tmp_path, managed=("data/**",), config=fast_cfg)
        assert vol.is_attached
        assert ((tmp_path / ".coherence").stat().st_mode & 0o777) == 0o700
        tightened = [
            r.getMessage()
            for r in caplog.records
            if "tightened existing .coherence directory" in r.getMessage()
        ]
        assert tightened == []
    finally:
        os.umask(prior_umask)
        stop_coordinator(tmp_path)


def test_per_instance_identity_is_distinct(tmp_path: Path, fast_cfg: LifecycleConfig) -> None:
    """Two CoherentVolume instances are distinct writers (distinct session ids),
    even in the same process."""
    ws_a = tmp_path / "a"
    ws_b = tmp_path / "b"
    ws_a.mkdir()
    ws_b.mkdir()
    vol_a = CoherentVolume(ws_a, managed=("data/**",), config=fast_cfg)
    vol_b = CoherentVolume(ws_b, managed=("data/**",), config=fast_cfg)
    try:
        assert vol_a.session_id != vol_b.session_id
        # Each session id is a v4-shaped UUID string.
        assert len(vol_a.session_id) == 36 and vol_a.session_id.count("-") == 4
    finally:
        stop_coordinator(ws_a)
        stop_coordinator(ws_b)


def test_after_fork_remints_identity_and_drops_endpoint(
    tmp_path: Path, fast_cfg: LifecycleConfig
) -> None:
    """The fork child-handler re-mints identity and drops the cached endpoint
    (so a forked worker is not conflated with its parent as one writer)."""
    vol = CoherentVolume(tmp_path, managed=("data/**",), config=fast_cfg)
    try:
        parent_id = vol.session_id
        assert vol.is_attached
        vol._after_fork()  # simulate the child-side handler directly
        assert vol.session_id != parent_id
        assert vol._endpoint is None
    finally:
        stop_coordinator(tmp_path)


@pytest.mark.skipif(not hasattr(os, "fork"), reason="requires os.fork (POSIX)")
def test_real_fork_child_has_distinct_identity(
    tmp_path: Path, fast_cfg: LifecycleConfig
) -> None:
    """An actual os.fork() child re-mints identity via os.register_at_fork."""
    vol = CoherentVolume(tmp_path, managed=("data/**",), config=fast_cfg)
    parent_id = vol.session_id
    read_fd, write_fd = os.pipe()
    pid = os.fork()
    if pid == 0:  # child
        os.close(read_fd)
        try:
            os.write(write_fd, vol.session_id.encode("utf-8"))
        finally:
            os.close(write_fd)
            os._exit(0)
    # parent
    os.close(write_fd)
    try:
        child_id = os.read(read_fd, 64).decode("utf-8")
        os.close(read_fd)
        os.waitpid(pid, 0)
        assert child_id != parent_id
        assert len(child_id) == 36
    finally:
        stop_coordinator(tmp_path)


def test_foreign_coordinator_strict_raises(tmp_path: Path, fast_cfg: LifecycleConfig) -> None:
    """A coordinator already running (not spawned by the appliance) cannot have
    strict mode enabled on it (load-once policy); strict mode fails closed."""
    port = ensure_coordinator(tmp_path, config=fast_cfg)
    assert port > 0
    try:
        with pytest.raises(CoherenceError):
            CoherentVolume(tmp_path, managed=("data/**",), on_error="strict", config=fast_cfg)
    finally:
        stop_coordinator(tmp_path)


def test_foreign_coordinator_degrade_warns(tmp_path: Path, fast_cfg: LifecycleConfig) -> None:
    """Under on_error='degrade' the same foreign-coordinator condition warns
    once and operates best-effort rather than raising."""
    port = ensure_coordinator(tmp_path, config=fast_cfg)
    assert port > 0
    try:
        with pytest.warns(CoherenceDegradedWarning):
            vol = CoherentVolume(tmp_path, managed=("data/**",), on_error="degrade", config=fast_cfg)
        assert vol.is_degraded
    finally:
        stop_coordinator(tmp_path)


# ---------------------------------------------------------------------------
# Unit 2 — sequential enforce-on-INVALID read/write/reacquire contract.
#
# The teeth: a write from a holder that a peer commit invalidated is DENIED
# (fail-closed). These tests use a FIXED stale buffer — bytes computed from the
# view read BEFORE the peer commit, never re-read — i.e. the OpenViktor cron
# lost-update shape. A refetch-safe "re-read then write" arm would pass even if
# the deny were broken (it would silently re-fetch fresh bytes), so it proves
# nothing; only the fixed-stale-buffer shape actually exercises the deny. See
# docs/solutions/best-practices/
#   coordinator-invalidation-not-mutex-honest-coherence-claims-2026-06-04.md.
# ---------------------------------------------------------------------------


def _seed(tmp_path: Path, rel: str = "data/shared.txt", content: bytes = b"v1") -> Path:
    """Create a tracked file under the workspace; return its absolute path."""
    target = tmp_path / rel
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(content)
    return target


def _pair(tmp_path: Path, cfg: LifecycleConfig) -> tuple[CoherentVolume, CoherentVolume]:
    """Two volumes sharing one workspace + coordinator: A spawns it (writing the
    strict policy), B sibling-attaches to the strict coordinator A spawned."""
    vol_a = CoherentVolume(tmp_path, managed=("data/**",), config=cfg)
    vol_b = CoherentVolume(tmp_path, managed=("data/**",), config=cfg)
    return vol_a, vol_b


def _track_only(tmp_path: Path, glob: str = "data/**") -> None:
    """Mark a glob TRACKED but NOT strict before the coordinator spawns.

    Writes ``.coherence/tracked.yaml`` (so a peer commit invalidates a SHARED
    view) while deliberately leaving ``strict_mode.yaml`` absent (so the re-grant
    is warn-mode — never denied). ``managed=()`` volumes then attach to this
    coordinator without the strict-mode requirement that ``managed`` globs carry.
    Reuses the coordinator's own ``_merge_yaml_list`` writer so the fixture tracks
    the real tracked.yaml format instead of duplicating it.
    """
    coherence_dir = tmp_path / ".coherence"
    coherence_dir.mkdir(parents=True, exist_ok=True)
    CoherentVolume._merge_yaml_list(coherence_dir / "tracked.yaml", (glob,))


def _agent_id(vol: CoherentVolume) -> str:
    """The coordinator's grant-row key for the volume's CURRENT attempt: the
    session id folded with the per-attempt incarnation, derived the way the
    coordinator derives it from the request body."""
    return str(session_to_agent_id(vol.session_id, vol._incarnation))


def _held(vol: CoherentVolume, agent_id: str) -> dict[str, str]:
    """What the COORDINATOR says ``agent_id`` holds, ``{path: state}``, read from
    ``/status`` — which lists every non-INVALID row, so ``{}`` means the row holds
    nothing (released, or never taken)."""
    status = vol.coordinator_status()
    assert status is not None, "the coordinator must be reachable to read its grant rows"
    for session in status["sessions"]:
        if session["agent_id"] == agent_id:
            return dict(session["states"])
    return {}


def _end_turn(vol: CoherentVolume) -> None:
    """Release the volume's grants the way an agent's turn end does: a
    session-stop naming the CURRENT incarnation. A stop without it addresses the
    session's parent row, which holds nothing, and releases nothing."""
    from ccs.cli._coherence_client import post as _cpost

    _cpost(
        vol._endpoint,
        "/hooks/session-stop",
        {"session_id": vol.session_id, "agent_id": vol._incarnation},
    )
    assert _held(vol, _agent_id(vol)) == {}, "the turn-end stop released nothing"


def test_sibling_volume_attaches_to_strict_coordinator(
    tmp_path: Path, fast_cfg: LifecycleConfig
) -> None:
    """The fleet case: two volumes on one workspace both attach with strict
    enforced. The second must NOT trip the foreign-coordinator guard — a sibling
    appliance enabled strict, so attaching (rather than failing closed) is
    correct. A truly foreign coordinator without strict still fails closed
    (covered by test_foreign_coordinator_strict_raises)."""
    _seed(tmp_path)
    vol_a, vol_b = _pair(tmp_path, fast_cfg)
    try:
        assert vol_a.is_attached and vol_b.is_attached
        assert vol_a.strict_mode_active() and vol_b.strict_mode_active()
        assert vol_a.session_id != vol_b.session_id
        assert not vol_a.is_degraded and not vol_b.is_degraded
    finally:
        stop_coordinator(tmp_path)


def test_fixed_stale_buffer_write_is_denied(
    tmp_path: Path, fast_cfg: LifecycleConfig
) -> None:
    """THE TEETH. A reads v1, B reads v1, A commits v2 (B -> INVALID), then B
    writes a buffer it computed from v1 WITHOUT re-reading -> the coordinator
    denies the write and write() raises. The stale bytes never land."""
    target = _seed(tmp_path, content=b"v1")
    vol_a, vol_b = _pair(tmp_path, fast_cfg)
    try:
        assert vol_a.read("data/shared.txt") == b"v1"
        b_view = vol_b.read("data/shared.txt")
        assert b_view == b"v1"
        # B captures a write derived from its v1 view (the lost-update shape).
        b_stale_buffer = b_view + b"\nappended-by-B"

        vol_a.write("data/shared.txt", b"v2-from-A")  # B -> INVALID
        assert target.read_bytes() == b"v2-from-A"

        with pytest.raises(CoherenceError):
            vol_b.write("data/shared.txt", b_stale_buffer)  # DENIED

        # The deny actually protected the file — the stale write did not land.
        assert target.read_bytes() == b"v2-from-A"
    finally:
        stop_coordinator(tmp_path)


def test_strict_deny_is_sticky_bare_read_does_not_recover(
    tmp_path: Path, fast_cfg: LifecycleConfig
) -> None:
    """KTD-T: once INVALID, a bare read() returns fresh bytes but does NOT clear
    INVALID — a subsequent write is still denied, with byte-stable deny text
    across retries (a bare re-read is more robust than an auto-refetch would
    be; recovery requires reacquire())."""
    target = _seed(tmp_path, content=b"v1")
    vol_a, vol_b = _pair(tmp_path, fast_cfg)
    try:
        vol_a.read("data/shared.txt")
        vol_b.read("data/shared.txt")
        vol_a.write("data/shared.txt", b"v2-from-A")  # B -> INVALID

        # Bare re-read returns the current bytes ...
        assert vol_b.read("data/shared.txt") == b"v2-from-A"
        # ... but does NOT clear INVALID: the write is still denied.
        with pytest.raises(CoherenceError) as first:
            vol_b.write("data/shared.txt", b"v3-attempt-1")
        with pytest.raises(CoherenceError) as second:
            vol_b.write("data/shared.txt", b"v3-attempt-2")
        # Byte-stable deny reason across retries (KTD-P — the model's retry loop
        # relies on this; regenerating it worsens retries).
        assert str(first.value) == str(second.value)
        assert target.read_bytes() == b"v2-from-A"
    finally:
        stop_coordinator(tmp_path)


def test_reacquire_recovers_then_write_succeeds(
    tmp_path: Path, fast_cfg: LifecycleConfig
) -> None:
    """RECOVERY: reacquire() re-mints identity AND does a mandatory fresh read
    (atomically), clearing INVALID. A write from the returned fresh bytes then
    succeeds — no lost update.

    What sheds the sticky INVALID is a FRESH COORDINATOR ROW, and the row is keyed
    on the session id folded with the per-attempt incarnation — so the row key
    must change while the session id stays put. A reacquire that left the key
    unchanged would land the read on the INVALID row and never clear it."""
    target = _seed(tmp_path, content=b"v1")
    vol_a, vol_b = _pair(tmp_path, fast_cfg)
    try:
        vol_a.read("data/shared.txt")
        vol_b.read("data/shared.txt")
        vol_a.write("data/shared.txt", b"v2-from-A")  # B -> INVALID

        with pytest.raises(CoherenceError):
            vol_b.write("data/shared.txt", b"stale")  # denied

        old_session = vol_b.session_id
        old_row = _agent_id(vol_b)
        fresh = vol_b.reacquire("data/shared.txt")
        assert fresh == b"v2-from-A"  # mandatory fresh read returns current bytes
        assert _agent_id(vol_b) != old_row  # a fresh coordinator row ...
        assert vol_b.session_id == old_session  # ... under the same session id
        assert _held(vol_b, _agent_id(vol_b)) == {"data/shared.txt": "SHARED"}

        # Write rebased on the fresh bytes -> granted.
        vol_b.write("data/shared.txt", fresh + b"\nrebased-by-B")
        assert target.read_bytes() == b"v2-from-A\nrebased-by-B"
    finally:
        stop_coordinator(tmp_path)


def test_first_time_writer_is_not_denied(
    tmp_path: Path, fast_cfg: LifecycleConfig
) -> None:
    """Negative control / boundary: strict mode denies an INVALID (preempted)
    writer, NOT a first-time writer. A write to a path this instance never read
    is granted — the strict intent is 'must re-read after preemption', not
    'must read before any write'."""
    _seed(tmp_path)  # ensure data/ exists so the managed glob spawns a coordinator
    vol = CoherentVolume(tmp_path, managed=("data/**",), config=fast_cfg)
    try:
        vol.write("data/brand-new.txt", b"hello")  # never read -> granted
        assert (tmp_path / "data/brand-new.txt").read_bytes() == b"hello"
    finally:
        stop_coordinator(tmp_path)


def test_reacquire_then_ignoring_fresh_bytes_is_not_caught(
    tmp_path: Path, fast_cfg: LifecycleConfig
) -> None:
    """HONEST BOUNDARY (documented, not a bug): after reacquire() returns fresh
    bytes, a caller that IGNORES them and writes a buffer computed earlier is
    NOT caught — no layer (OCC included) catches 'wrote from a buffer older than
    the read'. v1's honest scope is 'write from the bytes read()/reacquire()
    returned'. This pins the ceiling so a future reader doesn't mistake it for a
    regression."""
    target = _seed(tmp_path, content=b"v1")
    vol_a, vol_b = _pair(tmp_path, fast_cfg)
    try:
        vol_a.read("data/shared.txt")
        b_v1_view = vol_b.read("data/shared.txt")
        vol_a.write("data/shared.txt", b"v2-from-A")  # B -> INVALID
        vol_b.reacquire("data/shared.txt")  # B current again — but ignores the result
        # B writes a buffer derived from the STALE v1 view -> NOT caught.
        vol_b.write("data/shared.txt", b_v1_view + b"\nignored-reacquire")
        assert target.read_bytes() == b"v1\nignored-reacquire"
    finally:
        stop_coordinator(tmp_path)


def test_read_missing_file_raises_filenotfound(
    tmp_path: Path, fast_cfg: LifecycleConfig
) -> None:
    """read() stats before registering: a missing file raises FileNotFoundError
    and seeds no phantom artifact in the coordinator."""
    _seed(tmp_path)  # ensure data/ exists so a coordinator spawns
    vol = CoherentVolume(tmp_path, managed=("data/**",), config=fast_cfg)
    try:
        with pytest.raises(FileNotFoundError):
            vol.read("data/missing.txt")
    finally:
        stop_coordinator(tmp_path)


def test_read_empty_file_then_write(tmp_path: Path, fast_cfg: LifecycleConfig) -> None:
    """An empty file reads as b'' (sha256(b'')), and a subsequent write from the
    same instance is granted (no spurious deny on the empty-hash seed)."""
    _seed(tmp_path, rel="data/empty.txt", content=b"")
    vol = CoherentVolume(tmp_path, managed=("data/**",), config=fast_cfg)
    try:
        assert vol.read("data/empty.txt") == b""
        vol.write("data/empty.txt", b"now-full")
        assert (tmp_path / "data/empty.txt").read_bytes() == b"now-full"
    finally:
        stop_coordinator(tmp_path)


def test_identical_rewrite_skips_filesystem_write(
    tmp_path: Path, fast_cfg: LifecycleConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    """No-op skip: rewriting the exact bytes this instance last committed, while
    holding a fresh grant, skips the os.replace (no filesystem churn) but still
    finalizes the coordinator grant (so the EXCLUSIVE grant is not leaked)."""
    import ccs.adapters.coherent_volume as cv_mod

    _seed(tmp_path, rel="data/x.txt", content=b"orig")
    vol = CoherentVolume(tmp_path, managed=("data/**",), config=fast_cfg)
    try:
        vol.write("data/x.txt", b"committed")  # establishes last_committed_hash
        assert (tmp_path / "data/x.txt").read_bytes() == b"committed"

        calls = {"n": 0}
        real_replace = cv_mod.os.replace

        def counting_replace(src: object, dst: object) -> None:
            calls["n"] += 1
            real_replace(src, dst)

        monkeypatch.setattr(cv_mod.os, "replace", counting_replace)
        vol.write("data/x.txt", b"committed")  # identical bytes -> no-op skip
        assert calls["n"] == 0  # os.replace NOT called the second time
        assert (tmp_path / "data/x.txt").read_bytes() == b"committed"
    finally:
        stop_coordinator(tmp_path)


def test_no_op_skip_gated_on_disk_not_stale_cache(
    tmp_path: Path, fast_cfg: LifecycleConfig
) -> None:
    """Regression (pre-existing since CoherentVolume v1): the write no-op-skip
    must check the CURRENT on-disk bytes, not a per-instance cached hash a peer
    commit left stale.

    On a tracked-but-NON-strict glob, pre-edit RE-GRANTS (no strict deny), so the
    skip's own check is the only thing between a stale belief and a silent skip.
    A writes C (cache := H(C)); peer B overwrites with D (A is invalidated but,
    non-strict, not denied); A writes C again. A's cache still reads
    H(C) == H(C), so a cache-TRUSTING skip leaves B's D on disk while post-edit
    commits H(C) — disk/coordinator divergence, and the next reader gets D under a
    coordinator hash of C. The skip must fire only when the file ACTUALLY holds
    the bytes, so A's rewrite lands C.
    """
    rel = "data/shared.txt"
    _track_only(tmp_path)  # tracked but non-strict, before any coordinator spawns
    target = _seed(tmp_path, rel=rel, content=b"v0")

    vol_a = CoherentVolume(tmp_path, managed=(), config=fast_cfg)
    vol_b = CoherentVolume(tmp_path, managed=(), config=fast_cfg)
    try:
        # Warn-mode setup sanity: both attached, and the coordinator is NOT strict
        # for the path (so the later re-grant is not denied — the bug's precondition).
        assert vol_a.is_attached and vol_b.is_attached
        assert not vol_a.strict_mode_active()

        c_bytes = b"content-from-A"
        d_bytes = b"content-from-B-overwrite"

        vol_a.read(rel)
        vol_a.write(rel, c_bytes)  # A commits C: cache := H(C), disk == C
        assert target.read_bytes() == c_bytes

        vol_b.read(rel)  # B SHARED@C
        vol_b.write(rel, d_bytes)  # B commits D: A -> INVALID, disk == D
        assert target.read_bytes() == d_bytes

        # A rewrites the SAME bytes it last committed. Non-strict -> pre-edit
        # re-grants; A's cache still reads H(C). A cache-trusting no-op skip would
        # leave B's D on disk (the bug); the disk-gated skip rewrites C.
        vol_a.write(rel, c_bytes)
        assert target.read_bytes() == c_bytes  # A's intent on disk, not B's stale D
    finally:
        stop_coordinator(tmp_path)


def test_no_op_skip_not_taken_when_disk_file_missing(
    tmp_path: Path, fast_cfg: LifecycleConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The no-op skip is gated on the on-disk hash, so a cache hit ALONE does not
    skip the write. If the file is gone at write time (``_disk_hash`` -> None,
    and None != new_hash), the write proceeds and recreates it. Pins the
    ``_disk_hash`` missing-file branch the divergence fix relies on."""
    import ccs.adapters.coherent_volume as cv_mod

    _seed(tmp_path, rel="data/x.txt", content=b"orig")
    vol = CoherentVolume(tmp_path, managed=("data/**",), config=fast_cfg)
    try:
        vol.write("data/x.txt", b"committed")  # cache := H("committed"), disk holds it
        (tmp_path / "data/x.txt").unlink()  # disk now diverges from the cached belief

        calls = {"n": 0}
        real_replace = cv_mod.os.replace

        def counting_replace(src: object, dst: object) -> None:
            calls["n"] += 1
            real_replace(src, dst)

        monkeypatch.setattr(cv_mod.os, "replace", counting_replace)
        vol.write("data/x.txt", b"committed")  # same bytes, but the file is GONE
        assert calls["n"] == 1  # skip NOT taken — the write recreated the file
        assert (tmp_path / "data/x.txt").read_bytes() == b"committed"
    finally:
        stop_coordinator(tmp_path)


def test_deny_raises_even_in_degrade_mode(
    tmp_path: Path, fast_cfg: LifecycleConfig
) -> None:
    """A coordinator deny is enforcement WORKING, not an infrastructure failure,
    so write() raises on deny in BOTH on_error modes. on_error governs only
    infra failures (unavailable coordinator, watchdog timeout) — not the deny."""
    target = _seed(tmp_path, content=b"v1")
    vol_a = CoherentVolume(tmp_path, managed=("data/**",), on_error="degrade", config=fast_cfg)
    vol_b = CoherentVolume(tmp_path, managed=("data/**",), on_error="degrade", config=fast_cfg)
    try:
        vol_a.read("data/shared.txt")
        vol_b.read("data/shared.txt")
        vol_a.write("data/shared.txt", b"v2-from-A")  # B -> INVALID
        with pytest.raises(CoherenceError):
            vol_b.write("data/shared.txt", b"stale")  # deny still raises in degrade mode
        assert not vol_b.is_degraded  # the deny did not register as infra degradation
        assert target.read_bytes() == b"v2-from-A"
    finally:
        stop_coordinator(tmp_path)


# ---------------------------------------------------------------------------
# Unit 6 — CoherentVolume.write_cas (OCC write path, bypasses the acquire).
#
# write_cas reads (→ SHARED) → derives bytes via make_content(current) →
# commits through /hooks/post-edit-cas. On a version conflict it reacquire()s
# (re-mint + fresh read) and retries, bounded by MAX_CAS_REACQUIRES; on
# exhaustion it raises CasRetriesExhausted. Deny — including the fail-closed
# {ok:false, degraded:true, commit_unconfirmed} body — ALWAYS raises.
# ---------------------------------------------------------------------------


def test_write_cas_first_writer_commits(tmp_path: Path, fast_cfg: LifecycleConfig) -> None:
    """A single OCC writer reads then write_cas-commits cleanly (version bumps
    on the coordinator; bytes land on disk)."""
    target = _seed(tmp_path, content=b"v1")
    vol = CoherentVolume(tmp_path, managed=("data/**",), config=fast_cfg)
    try:
        vol.write_cas("data/shared.txt", lambda cur: cur + b"\nappended")
        assert target.read_bytes() == b"v1\nappended"
    finally:
        stop_coordinator(tmp_path)


def test_write_cas_conflict_reacquires_and_converges(
    tmp_path: Path, fast_cfg: LifecycleConfig
) -> None:
    """THE OCC RECOVERY: A reads v1, B reads v1, A commits v2 (B → INVALID).
    A's turn then ends (session-stop releases A's grant — the realistic
    "the other agent finished" case). B.write_cas finds itself INVALID,
    reacquire()s (re-mint + fresh read of A's v2 bytes), re-derives via
    make_content against the fresh view, and commits → converges. No lost
    update: B's commit is an UPDATE rebased on A's v2, not a stale clobber.

    (A's grant must clear before B's OCC commit can land: an OCC writer is S/I
    and never invalidates a peer's MODIFIED grant, so a lingering pessimistic
    holder yields ``other_holder`` until the grant is released or times out —
    the OCC-vs-pessimistic coexistence bound. Here A releases via session-stop.)
    """
    target = _seed(tmp_path, content=b"v1")
    vol_a, vol_b = _pair(tmp_path, fast_cfg)
    try:
        vol_a.read("data/shared.txt")
        vol_b.read("data/shared.txt")  # B is SHARED@v1

        vol_a.write("data/shared.txt", b"v2-from-A")  # B → INVALID, version → 2
        assert target.read_bytes() == b"v2-from-A"
        # A's turn ends — release its grant so the OCC writer is not blocked by
        # other_holder against A's lingering MODIFIED.
        _end_turn(vol_a)

        seen: list[bytes] = []

        def make(current: bytes) -> bytes:
            # Records the bytes each attempt derives from — proves the retry
            # re-reads A's v2 (not B's stale v1 buffer).
            seen.append(current)
            return current + b"\nrebased-by-B"

        vol_b.write_cas("data/shared.txt", make)
        # Converged on top of A's bytes — the lost update did NOT happen.
        assert target.read_bytes() == b"v2-from-A\nrebased-by-B"
        # The winning attempt derived from A's v2 bytes (re-read via reacquire),
        # never from the original stale v1.
        assert b"v2-from-A" in seen[-1]
    finally:
        stop_coordinator(tmp_path)


def test_write_cas_exhaustion_raises_typed_terminal(
    tmp_path: Path, fast_cfg: LifecycleConfig
) -> None:
    """Bounded progress (R6): if every attempt loses the race, write_cas raises
    CasRetriesExhausted (a typed terminal) rather than silently dropping the
    write. Simulated by a peer that commits a fresh version on EVERY attempt, so
    B's expected_version is always stale by commit time."""
    import ccs.adapters.coherent_volume as cv_mod

    target = _seed(tmp_path, content=b"v1")
    vol_a, vol_b = _pair(tmp_path, fast_cfg)
    # Shrink the bound so the test is fast + deterministic.
    original_max = cv_mod.MAX_CAS_REACQUIRES
    cv_mod.MAX_CAS_REACQUIRES = 2
    try:
        vol_a.read("data/shared.txt")
        counter = {"n": 1}

        def make(current: bytes) -> bytes:
            # On every B attempt, A commits a NEW version first → B's read is
            # immediately stale → guaranteed version_mismatch each attempt.
            counter["n"] += 1
            vol_a.reacquire("data/shared.txt")
            vol_a.write("data/shared.txt", f"vA-{counter['n']}".encode())
            return current + b"\nB-attempt"

        with pytest.raises(CasRetriesExhausted) as exc:
            vol_b.write_cas("data/shared.txt", make)
        # The terminal records the artifact + that no write landed for B.
        assert exc.value.attempts == cv_mod.MAX_CAS_REACQUIRES + 1
        # B's stale buffer never clobbered A's latest.
        assert b"B-attempt" not in target.read_bytes()
    finally:
        cv_mod.MAX_CAS_REACQUIRES = original_max
        stop_coordinator(tmp_path)


def test_write_cas_deny_raises_in_both_on_error_modes(
    tmp_path: Path, fast_cfg: LifecycleConfig
) -> None:
    """A non-retry-eligible deny (e.g. corruption: expected_version > current)
    ALWAYS raises CoherenceError — in BOTH strict and degrade on_error modes —
    and never silently succeeds. write_cas sources expected_version from its own
    read, so we force corruption by stubbing _read_with_version to over-report
    the version (expected > current → the coordinator returns an error body)."""
    for mode in ("strict", "degrade"):
        target = _seed(tmp_path, content=b"v1")
        vol = CoherentVolume(
            tmp_path, managed=("data/**",), on_error=mode, config=fast_cfg
        )
        try:
            # Seed the artifact on the coordinator (v1 + SHARED) via a real read
            # so the CAS has a real version to compare against.
            assert vol.read("data/shared.txt") == b"v1"
            # Force expected_version far above current → corruption body
            # ({ok:false, reason:commit_cas_corruption...}) which must raise.
            # (bytes, version, stale_denied, generation, stale_status) — not
            # stale, so no reacquire.
            vol._read_with_version = lambda rel: (b"v1", 999, False, 0, False)  # type: ignore[assignment]
            with pytest.raises(CoherenceError):
                vol.write_cas("data/shared.txt", lambda cur: b"should-not-land")
            assert not vol.is_degraded, (
                "a deny is enforcement working, not infra degradation"
            )
            # The unconfirmed write never landed.
            assert target.read_bytes() == b"v1"
        finally:
            stop_coordinator(tmp_path)


def test_write_cas_degrade_body_raises_in_both_modes(
    tmp_path: Path, fast_cfg: LifecycleConfig
) -> None:
    """The fail-closed degrade body ({ok:false, degraded:true,
    reason:'commit_unconfirmed'}) must raise in BOTH on_error modes — a degraded
    CAS is unconfirmed, so the client must never assume the write landed.
    Simulated by stubbing the coordinator POST to return that body."""
    for mode in ("strict", "degrade"):
        target = _seed(tmp_path, content=b"v1")
        vol = CoherentVolume(
            tmp_path, managed=("data/**",), on_error=mode, config=fast_cfg
        )
        try:
            real_post = vol._post

            def fake_post(endpoint_path: str, payload: dict, _real=real_post):
                if endpoint_path == "/hooks/post-edit-cas":
                    return {"ok": False, "degraded": True, "reason": "commit_unconfirmed"}
                return _real(endpoint_path, payload)

            vol._post = fake_post  # type: ignore[assignment]
            with pytest.raises(CoherenceError):
                vol.write_cas("data/shared.txt", lambda cur: b"unconfirmed")
            # commit_unconfirmed is a hard failure, not infra degradation, so the
            # deny path does NOT bump the degradation counter.
            assert not vol.is_degraded
            assert target.read_bytes() == b"v1"
        finally:
            stop_coordinator(tmp_path)


def test_write_cas_make_content_sees_current_bytes(
    tmp_path: Path, fast_cfg: LifecycleConfig
) -> None:
    """make_content is invoked with the freshly-read current bytes so the
    caller re-derives intent against the latest state (the OCC update contract,
    same boundary as reacquire())."""
    _seed(tmp_path, content=b"hello")
    vol = CoherentVolume(tmp_path, managed=("data/**",), config=fast_cfg)
    try:
        captured: list[bytes] = []
        vol.write_cas("data/shared.txt", lambda cur: captured.append(cur) or (cur + b"!"))
        assert captured == [b"hello"]
        assert (tmp_path / "data/shared.txt").read_bytes() == b"hello!"
    finally:
        stop_coordinator(tmp_path)


def test_write_cas_degrade_none_response_fails_closed_no_disk_write(
    tmp_path: Path, fast_cfg: LifecycleConfig
) -> None:
    """FIX 1: in degrade mode, a mid-commit transport failure that ``_post``
    swallowed (returns None for /hooks/post-edit-cas AFTER a version was read)
    must FAIL CLOSED — raise CoherenceError and NOT write the unconfirmed bytes
    to disk. An OCC writer holds no grant, so unconfirmed bytes touching disk
    would re-open the lost update the feature prevents. Before the fix this path
    best-effort _atomic_write'd and returned success."""
    target = _seed(tmp_path, content=b"v1")
    vol = CoherentVolume(
        tmp_path, managed=("data/**",), on_error="degrade", config=fast_cfg
    )
    try:
        # Seed a real read so the CAS has a real version comparand (pre-read must
        # succeed; only the commit POST is forced to None).
        assert vol.read("data/shared.txt") == b"v1"
        real_post = vol._post

        def fake_post(endpoint_path: str, payload: dict, _real=real_post):
            # Simulate a degrade-swallowed transport failure on the OCC commit
            # only — every other call (pre-read) behaves normally.
            if endpoint_path == "/hooks/post-edit-cas":
                return None
            return _real(endpoint_path, payload)

        vol._post = fake_post  # type: ignore[assignment]
        with pytest.raises(CoherenceError):
            vol.write_cas("data/shared.txt", lambda cur: b"unconfirmed-bytes")
        # The unconfirmed bytes NEVER touched disk (the whole point of the fix).
        assert target.read_bytes() == b"v1"
    finally:
        stop_coordinator(tmp_path)


def test_write_cas_repeatable_for_same_volume(
    tmp_path: Path, fast_cfg: LifecycleConfig
) -> None:
    """FIX 3 (cross-process): the same volume can write_cas the same path TWICE
    back-to-back — both win (version bumps each time) with no D4 'use commit()'
    rejection, because a winning commit_cas ends the committer SHARED on the
    coordinator (an OCC writer holds no grant). Before the fix the first win left
    the agent MODIFIED and the second write_cas hard-failed the D4 precondition."""
    target = _seed(tmp_path, content=b"v1")
    vol = CoherentVolume(tmp_path, managed=("data/**",), config=fast_cfg)
    try:
        vol.write_cas("data/shared.txt", lambda cur: cur + b"\nfirst")
        assert target.read_bytes() == b"v1\nfirst"
        # Second OCC write by the SAME volume must also land (no D4 rejection).
        vol.write_cas("data/shared.txt", lambda cur: cur + b"\nsecond")
        assert target.read_bytes() == b"v1\nfirst\nsecond"
    finally:
        stop_coordinator(tmp_path)


def test_classify_cas_response_transient_is_conflict(
    tmp_path: Path, fast_cfg: LifecycleConfig
) -> None:
    """AC2 (unit): the stable wire reason 'caller_in_transient_state' classifies
    as a retry-eligible 'conflict' via an EXACT match (no brittle substring) so a
    reword of the coordinator's human message can't break retry routing."""
    _seed(tmp_path)
    vol = CoherentVolume(tmp_path, managed=("data/**",), config=fast_cfg)
    try:
        assert vol._classify_cas_response(
            {"ok": False, "reason": "caller_in_transient_state"}
        ) == "conflict"
        # The legacy human message ("commit_cas_not_allowed ... reason=...") is
        # no longer matched — only the exact stable reason routes to conflict.
        assert vol._classify_cas_response(
            {"ok": False, "reason": "commit_cas_not_allowed agent=x reason=caller_in_transient_state"}
        ) == "raise"
        # Sanity: the typed ConflictDetail reasons still classify as conflict.
        assert vol._classify_cas_response(
            {"ok": False, "reason": "version_mismatch", "current_version": 2}
        ) == "conflict"
    finally:
        stop_coordinator(tmp_path)


def test_write_cas_transient_reason_reacquires_and_converges(
    tmp_path: Path, fast_cfg: LifecycleConfig
) -> None:
    """AC2 (end-to-end client): a CAS that comes back with the stable transient
    reason 'caller_in_transient_state' is treated as a CONFLICT — write_cas
    reacquires (re-mint + fresh read) and retries to convergence, NOT raise.
    Stubbed so the FIRST OCC commit returns the transient body and the next
    passes through to the real coordinator (which wins)."""
    target = _seed(tmp_path, content=b"v1")
    vol = CoherentVolume(tmp_path, managed=("data/**",), config=fast_cfg)
    try:
        # A real read so the artifact + version exist for the eventual real CAS.
        assert vol.read("data/shared.txt") == b"v1"
        real_post = vol._post
        cas_calls = {"n": 0}

        def fake_post(endpoint_path: str, payload: dict, _real=real_post):
            if endpoint_path == "/hooks/post-edit-cas":
                cas_calls["n"] += 1
                if cas_calls["n"] == 1:
                    # First attempt: a peer invalidated us mid-window. Stable
                    # retry-eligible reason — the client must reacquire + retry.
                    return {
                        "ok": False,
                        "reason": "caller_in_transient_state",
                        "current_version": 1,
                    }
            return _real(endpoint_path, payload)

        vol._post = fake_post  # type: ignore[assignment]
        vol.write_cas("data/shared.txt", lambda cur: cur + b"\nrebased")
        # Converged (did NOT raise): the retry landed the rebased bytes.
        assert target.read_bytes() == b"v1\nrebased"
        assert cas_calls["n"] >= 2  # first transient-conflict, then a real win
        # A retry-eligible conflict is not infra degradation.
        assert not vol.is_degraded
    finally:
        stop_coordinator(tmp_path)


# ---------------------------------------------------------------------------
# A5 — single-instance concurrency guard. One instance is single-threaded by
# contract; overlapping use across threads raises (loud misuse) rather than
# splitting an in-flight CAS across re-minted identities. The guard is re-entrant
# for the same thread so the internal write_cas → reacquire → read nesting works.
# ---------------------------------------------------------------------------


def test_overlapping_use_from_another_thread_raises(
    tmp_path: Path, fast_cfg: LifecycleConfig
) -> None:
    """A5: while one thread holds an op in flight on an instance, a second
    thread calling a public op on the SAME instance raises CoherenceError —
    concurrent use is detected, not silently allowed."""
    _seed(tmp_path)
    vol = CoherentVolume(tmp_path, managed=("data/**",), config=fast_cfg)
    held = threading.Event()
    release = threading.Event()

    def holder() -> None:
        # Hold the guard the way an in-flight op does (same mechanism the public
        # ops use), then block so the main thread's op truly overlaps.
        with vol._single_op_guard():
            held.set()
            release.wait(timeout=5)

    t = threading.Thread(target=holder)
    t.start()
    try:
        assert held.wait(timeout=5), "holder thread never acquired the guard"
        # A different thread (this one) calling a public op while the guard is
        # held elsewhere must raise — overlapping single-instance use.
        with pytest.raises(CoherenceError, match="single-threaded"):
            vol.read("data/shared.txt")
        with pytest.raises(CoherenceError, match="single-threaded"):
            vol.write("data/shared.txt", b"nope")
        with pytest.raises(CoherenceError, match="single-threaded"):
            vol.write_cas("data/shared.txt", lambda cur: b"nope")
    finally:
        release.set()
        t.join(timeout=5)
        stop_coordinator(tmp_path)


def test_guard_released_after_op_allows_subsequent_ops(
    tmp_path: Path, fast_cfg: LifecycleConfig
) -> None:
    """A5: the guard is released in a finally, so normal SEQUENTIAL use is
    unaffected — back-to-back read/write/write_cas (and the internal
    reacquire-within-write_cas path) all succeed. Also asserts the guard owner is
    cleared after each op so the instance is reusable."""
    target = _seed(tmp_path, content=b"v1")
    vol_a, vol_b = _pair(tmp_path, fast_cfg)
    try:
        # Sequential reads + write on one instance: guard taken + released each
        # time, never tripping itself.
        assert vol_a.read("data/shared.txt") == b"v1"
        assert vol_b.read("data/shared.txt") == b"v1"
        vol_a.write("data/shared.txt", b"v2-from-A")  # B -> INVALID
        assert vol_a._guard_owner_ident is None  # released after the op
        # A's turn ends — release its MODIFIED grant so B's OCC commit is not
        # blocked by other_holder (the OCC-vs-pessimistic coexistence bound).
        _end_turn(vol_a)

        # The internal reacquire-within-write_cas path: B is INVALID, so
        # write_cas must reacquire() (which calls read()) on the SAME thread —
        # re-entering the guard, not deadlocking or tripping it — and converge.
        vol_b.write_cas("data/shared.txt", lambda cur: cur + b"\nrebased-by-B")
        assert target.read_bytes() == b"v2-from-A\nrebased-by-B"
        assert vol_b._guard_owner_ident is None  # released after write_cas too

        # And a plain direct reacquire() still works (its internal read()
        # re-enters the guard fresh on this thread).
        fresh = vol_b.reacquire("data/shared.txt")
        assert fresh == b"v2-from-A\nrebased-by-B"
        assert vol_b._guard_owner_ident is None  # released after reacquire too
    finally:
        stop_coordinator(tmp_path)


# ---------------------------------------------------------------------------
# T2 — write_cas recovery from a STICKY strict-deny (KTD-T), plus the
# bounded fail-closed terminal. Distinct from the conflict-classify convergence
# above: here B is INVALID at CAS time, so the stale-deny branch (NOT a
# version_mismatch conflict) drives the re-mint + retry.
# ---------------------------------------------------------------------------


def test_write_cas_recovers_from_sticky_strict_deny_and_converges(
    tmp_path: Path, fast_cfg: LifecycleConfig
) -> None:
    """T2: a peer commit leaves THIS volume INVALID (sticky strict-deny). The
    very first OCC read inside write_cas is a strict-deny (stale_denied), so
    write_cas must re-mint identity (clears INVALID + the invalidation transient)
    and commit the rebased bytes — converge, NOT raise. No lost update: B's
    commit is rebased on A's v2."""
    target = _seed(tmp_path, content=b"v1")
    vol_a, vol_b = _pair(tmp_path, fast_cfg)
    try:
        vol_a.read("data/shared.txt")
        vol_b.read("data/shared.txt")  # B SHARED@v1
        vol_a.write("data/shared.txt", b"v2-from-A")  # B -> INVALID (sticky deny)
        # A's turn ends — release its grant so B's OCC commit is not blocked by
        # other_holder against A's lingering MODIFIED.
        _end_turn(vol_a)

        # Confirm B really is in the sticky-deny state BEFORE write_cas: a bare
        # version-aware read reports stale_denied=True (INVALID, not re-granted).
        _bytes, _ver, stale_denied, _gen, _stale = vol_b._read_with_version(
            "data/shared.txt"
        )
        assert stale_denied is True, "precondition: B must be a sticky strict-deny"

        seen: list[bytes] = []

        def make(current: bytes) -> bytes:
            seen.append(current)
            return current + b"\nrebased-by-B"

        # write_cas drives the stale-deny branch → re-mint → fresh hash-checked
        # read → CAS.
        vol_b.write_cas("data/shared.txt", make)
        assert target.read_bytes() == b"v2-from-A\nrebased-by-B"
        # The winning attempt derived from A's v2 (re-read after re-mint), never
        # the stale v1 buffer.
        assert b"v2-from-A" in seen[-1]
        assert not vol_b.is_degraded  # a deny/recovery is enforcement, not infra
    finally:
        stop_coordinator(tmp_path)


def test_write_cas_fails_closed_with_typed_terminal_when_reads_stay_denied(
    tmp_path: Path, fast_cfg: LifecycleConfig
) -> None:
    """T2: a comparand read that NEVER clears (every read is a strict-deny) must
    fail closed with a TYPED terminal — never a silent drop and never an
    infinite spin. The loop re-mints identity (NOT reacquire(), whose read would
    route the next comparand read through the coordinator's unchecked
    fresh-SHARED branch — the on-disk lost-update hole) and re-reads, bounded by
    the CONSECUTIVE-denied-streak limit (a denied read never POSTs a commit, so
    it must not consume the CAS budget — that one counts commit attempts).
    Crucially make_content() runs ONLY after a read that is NOT denied, so a
    perpetually-denied artifact derives and commits NOTHING — there is no stale
    buffer to land. Simulated by stubbing _read_with_version to always report
    stale_denied."""
    _seed(tmp_path, content=b"v1")
    vol = CoherentVolume(tmp_path, managed=("data/**",), config=fast_cfg)
    try:
        assert vol.read("data/shared.txt") == b"v1"
        calls = {"n": 0}

        def always_denied(rel: str):
            # (bytes, version, stale_denied, generation, stale_status) — every
            # comparand read is a deny (a deny carries no confirmed generation
            # and is a stale-status read).
            calls["n"] += 1
            return (b"v1", 1, True, None, True)

        vol._read_with_version = always_denied  # type: ignore[assignment]

        made = {"called": False}

        def make(_cur: bytes) -> bytes:
            made["called"] = True
            return b"should-not-commit"

        # Typed terminal (the denied-streak bound), never a silent loss.
        with pytest.raises(CoherenceError, match="stayed strict-denied"):
            vol.write_cas("data/shared.txt", make)
        # Bounded, not an infinite spin: exactly MAX_CAS_REACQUIRES + 1
        # consecutive denied reads, then the typed raise.
        assert calls["n"] == MAX_CAS_REACQUIRES + 1
        # make_content NEVER ran: a denied read derives/commits no bytes, so a
        # stale buffer can never land (the on-disk lost update is impossible).
        assert made["called"] is False
    finally:
        stop_coordinator(tmp_path)


# ---------------------------------------------------------------------------
# Unit 3 — install() builtins.open / io.open shim (opt-in, demo-grade).
#
# Routes managed-path opens through a process-singleton volume. The coverage
# matrix is the contract: builtins.open + pathlib are coordinated; os.open and
# subprocess are NOT. The shim preserves the sequential guard (the lost update
# is denied through open() too, via fail-closed close()).
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _uninstall_shim_safety():
    """Safety net: never let an installed open()-shim leak across tests (a leaked
    builtins.open patch would corrupt every later test). No-op when not installed."""
    yield
    uninstall()


def test_shim_coordinates_open_round_trip(tmp_path: Path, fast_cfg: LifecycleConfig) -> None:
    """A plain open() read+write of a managed path is coordinated, and the patch
    is reversed on context exit."""
    target = _seed(tmp_path, content=b"v1")
    original_open = builtins.open
    try:
        with coherent_workspace(tmp_path, managed=("data/**",), config=fast_cfg) as vol:
            assert builtins.open is not original_open  # patched while installed
            with open(target) as f:  # read via the shim
                assert f.read() == "v1"
            with open(target, "w") as f:  # write via the shim
                f.write("v2")
            assert target.read_bytes() == b"v2"
            # Proof the write was coordinated (routed through volume.write):
            assert "data/shared.txt" in vol._last_committed_hash
        assert builtins.open is original_open  # restored on exit
    finally:
        stop_coordinator(tmp_path)


def test_shim_install_is_idempotent(tmp_path: Path, fast_cfg: LifecycleConfig) -> None:
    """A second install() returns the already-installed singleton volume (one
    workspace per process in v1)."""
    _seed(tmp_path)
    try:
        vol1 = install(tmp_path, managed=("data/**",), config=fast_cfg)
        vol2 = install(tmp_path, managed=("data/**",), config=fast_cfg)
        assert vol1 is vol2
    finally:
        uninstall()
        stop_coordinator(tmp_path)


def test_shim_covers_pathlib_write_text(tmp_path: Path, fast_cfg: LifecycleConfig) -> None:
    """pathlib Path.write_text IS coordinated — it calls io.open, which the shim
    patches alongside builtins.open (patching builtins.open alone would miss it)."""
    (tmp_path / "data").mkdir()
    try:
        with coherent_workspace(tmp_path, managed=("data/**",), config=fast_cfg) as vol:
            (tmp_path / "data/note.txt").write_text("hello")  # pathlib -> io.open -> shim
            assert (tmp_path / "data/note.txt").read_bytes() == b"hello"
            assert "data/note.txt" in vol._last_committed_hash  # coordinated, not bypassed
    finally:
        stop_coordinator(tmp_path)


def test_shim_does_not_cover_os_open_or_subprocess(
    tmp_path: Path, fast_cfg: LifecycleConfig
) -> None:
    """The documented boundary: os.open/os.write (raw fds) and subprocess/shell
    redirection bypass the shim — the bytes land but the volume never sees them."""
    target = _seed(tmp_path, rel="data/raw.txt", content=b"orig")
    try:
        with coherent_workspace(tmp_path, managed=("data/**",), config=fast_cfg) as vol:
            fd = os.open(str(target), os.O_WRONLY | os.O_TRUNC)
            try:
                os.write(fd, b"via-os")
            finally:
                os.close(fd)
            subprocess.run(
                ["sh", "-c", f"printf '+sub' >> {target}"], check=True
            )
            assert target.read_bytes() == b"via-os+sub"  # both writes landed on disk
            # ... but neither was coordinated (the documented NOT-COVERED boundary).
            assert "data/raw.txt" not in vol._last_committed_hash
    finally:
        stop_coordinator(tmp_path)


def test_shim_inert_without_install() -> None:
    """Without install(), builtins.open / io.open are the real builtins — importing
    the module has no side effect on open()."""
    assert builtins.open.__name__ == "open"  # not our 'coherent_open' wrapper
    assert builtins.open is io.open


def test_shim_lost_update_is_denied_through_open(
    tmp_path: Path, fast_cfg: LifecycleConfig
) -> None:
    """The teeth, through the shim: B reads v1 via open(); a peer (explicit API,
    sibling-attached) commits v2; B writes a v1-derived buffer via open() →
    close() raises fail-closed and the stale bytes never land."""
    target = _seed(tmp_path, content=b"v1")
    try:
        with coherent_workspace(tmp_path, managed=("data/**",), config=fast_cfg):
            # Peer A: explicit API on the same workspace (sibling-attaches to the
            # coordinator the shim singleton spawned).
            vol_a = CoherentVolume(tmp_path, managed=("data/**",), config=fast_cfg)
            with open(target) as f:  # B reads v1 via the shim -> SHARED@v1
                b_view = f.read()
            assert b_view == "v1"
            vol_a.write("data/shared.txt", b"v2-from-A")  # B -> INVALID
            # B writes a v1-derived buffer via open() (never re-read) -> deny on close.
            with pytest.raises(CoherenceError):
                with open(target, "w") as f:
                    f.write(b_view + "-edited-by-B")
            assert target.read_bytes() == b"v2-from-A"  # stale write did not land
    finally:
        stop_coordinator(tmp_path)


def test_shim_reattaches_after_fork(tmp_path: Path, fast_cfg: LifecycleConfig) -> None:
    """After a fork drops the endpoint, the next shim'd open lazily re-attaches
    under the child's fresh identity (simulated via a direct _after_fork call to
    avoid forking the coordinator's threads)."""
    _seed(tmp_path, content=b"v1")
    try:
        with coherent_workspace(tmp_path, managed=("data/**",), config=fast_cfg) as vol:
            assert vol.is_attached
            old_sid = vol.session_id
            vol._after_fork()  # simulate the child-side fork handler
            assert vol._endpoint is None and vol._needs_reattach
            with open(tmp_path / "data/shared.txt") as f:  # lazily re-attaches
                assert f.read() == "v1"
            assert vol.is_attached  # re-attached
            assert vol.session_id != old_sid  # fresh identity
    finally:
        stop_coordinator(tmp_path)


# ---------------------------------------------------------------------------
# Code-review regression tests (PR #91): fail-closed completeness, grant-leak
# safety, the no-op-skip grant finalization, fork/degrade edges, and the shim's
# exceptional-close discard.
# ---------------------------------------------------------------------------


def _raise_oserror(*_args: object, **_kwargs: object) -> None:
    raise OSError("simulated filesystem failure")


def test_fs_write_failure_releases_grant(
    tmp_path: Path, fast_cfg: LifecycleConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    """If the atomic FS write fails AFTER pre-edit granted EXCLUSIVE, the grant is
    released via a post-edit success:false (not orphaned until the sweep), and the
    original OSError propagates.

    Asserted on the COORDINATOR, not only on the request being sent: the release
    must name the incarnation that holds the grant, and one that does not
    addresses the session's parent row and releases nothing — an orphaned
    EXCLUSIVE that still looks like a release on the wire."""
    import ccs.adapters.coherent_volume as cv_mod

    _seed(tmp_path)
    vol = CoherentVolume(tmp_path, managed=("data/**",), config=fast_cfg)
    try:
        posts: list[tuple[str, dict]] = []
        real_post = cv_mod._coordinator_post

        def spy(endpoint: object, path: str, payload: dict) -> object:
            posts.append((path, dict(payload)))
            return real_post(endpoint, path, payload)

        monkeypatch.setattr(cv_mod, "_coordinator_post", spy)
        monkeypatch.setattr(vol, "_atomic_write", _raise_oserror)

        with pytest.raises(OSError):
            vol.write("data/shared.txt", b"never-lands")

        assert any(
            p == "/hooks/post-edit" and pay.get("success") is False for p, pay in posts
        ), "FS-write failure must release the grant via post-edit success=false"
        assert _held(vol, _agent_id(vol)) == {}, (
            "the failure release reached the coordinator but left the grant held"
        )
    finally:
        stop_coordinator(tmp_path)


def _raise_runtime(*_args: object, **_kwargs: object) -> str:
    raise RuntimeError("simulated non-OSError in the pre-write window")


def test_non_oserror_in_write_window_releases_grant(
    tmp_path: Path, fast_cfg: LifecycleConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A NON-OSError raised after pre-edit granted EXCLUSIVE but before the
    post-edit commit (here from _disk_hash, in the no-op-skip check) must still
    release the grant via post-edit success:false — not orphan it until the
    crash-recovery sweep. The original handler caught only OSError around
    _atomic_write, leaving the hashing / disk-read window unprotected."""
    import ccs.adapters.coherent_volume as cv_mod

    _seed(tmp_path, rel="data/x.txt", content=b"orig")
    vol = CoherentVolume(tmp_path, managed=("data/**",), config=fast_cfg)
    try:
        vol.write("data/x.txt", b"same")  # cache := H("same") so _disk_hash is reached next

        posts: list[tuple[str, dict]] = []
        real_post = cv_mod._coordinator_post

        def spy(endpoint: object, path: str, payload: dict) -> object:
            posts.append((path, dict(payload)))
            return real_post(endpoint, path, payload)

        monkeypatch.setattr(cv_mod, "_coordinator_post", spy)
        monkeypatch.setattr(vol, "_disk_hash", _raise_runtime)  # non-OSError in the window

        with pytest.raises(RuntimeError):
            vol.write("data/x.txt", b"same")  # cache hit -> _disk_hash -> RuntimeError

        assert any(
            p == "/hooks/post-edit" and pay.get("success") is False for p, pay in posts
        ), "a non-OSError in the post-grant window must release the grant"
    finally:
        stop_coordinator(tmp_path)


def test_no_op_skip_still_finalizes_grant(
    tmp_path: Path, fast_cfg: LifecycleConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The no-op skip (identical bytes) skips the os.replace but MUST still call
    post-edit to finalize the EXCLUSIVE grant — otherwise the grant leaks. The
    earlier os.replace-spy test cannot see this failure mode."""
    import ccs.adapters.coherent_volume as cv_mod

    _seed(tmp_path, rel="data/x.txt", content=b"orig")
    vol = CoherentVolume(tmp_path, managed=("data/**",), config=fast_cfg)
    try:
        vol.write("data/x.txt", b"same")  # establishes last_committed_hash

        posts: list[str] = []
        real_post = cv_mod._coordinator_post

        def spy(endpoint: object, path: str, payload: dict) -> object:
            posts.append(path)
            return real_post(endpoint, path, payload)

        monkeypatch.setattr(cv_mod, "_coordinator_post", spy)
        vol.write("data/x.txt", b"same")  # identical -> no-op skip
        assert "/hooks/post-edit" in posts, "no-op skip must still finalize the grant"
    finally:
        stop_coordinator(tmp_path)


def test_write_fails_closed_on_watchdog_degrade(
    tmp_path: Path, fast_cfg: LifecycleConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A watchdog-timeout degrade ({ok:true, degraded:true}) at pre-edit is an
    infra failure → in strict mode write() fails closed (raises), it does NOT
    proceed. Covers the degrade branch of _check_grant during an active write."""
    import ccs.adapters.coherent_volume as cv_mod

    _seed(tmp_path)
    vol = CoherentVolume(tmp_path, managed=("data/**",), config=fast_cfg)  # strict
    try:
        def fake_post(endpoint: object, path: str, payload: dict) -> dict:
            if path == "/hooks/pre-edit":
                return {"ok": True, "degraded": True}  # watchdog-timeout envelope
            return {"ok": True}

        monkeypatch.setattr(cv_mod, "_coordinator_post", fake_post)
        with pytest.raises(CoherenceError):
            vol.write("data/shared.txt", b"x")
    finally:
        stop_coordinator(tmp_path)


def test_shim_exceptional_close_discards_write(
    tmp_path: Path, fast_cfg: LifecycleConfig
) -> None:
    """A `with open(p,'w') as f: ...` block whose body raises DISCARDS the
    buffered write rather than committing a partial/abandoned buffer (and leaks no
    grant — the acquire only happens on a clean commit)."""
    target = _seed(tmp_path, content=b"v1")
    try:
        with coherent_workspace(tmp_path, managed=("data/**",), config=fast_cfg):
            with pytest.raises(RuntimeError):
                with open(target, "w") as f:
                    f.write("garbage-must-not-commit")
                    raise RuntimeError("boom")
            assert target.read_bytes() == b"v1"  # buffer discarded, file unchanged
    finally:
        stop_coordinator(tmp_path)


def test_degrade_foreign_coordinator_drops_endpoint(
    tmp_path: Path, fast_cfg: LifecycleConfig
) -> None:
    """Under on_error='degrade', attaching to a foreign non-strict coordinator
    must drop the endpoint (not stay attached to a coordinator that can't enforce
    the managed paths while is_attached reports True)."""
    port = ensure_coordinator(tmp_path, config=fast_cfg)  # foreign: no strict yaml
    assert port > 0
    try:
        with pytest.warns(CoherenceDegradedWarning):
            vol = CoherentVolume(
                tmp_path, managed=("data/**",), on_error="degrade", config=fast_cfg
            )
        assert vol.is_degraded
        assert not vol.is_attached  # endpoint dropped — no false sense of coordination
    finally:
        stop_coordinator(tmp_path)


def test_nested_coherent_workspace_keeps_outer_shim(
    tmp_path: Path, fast_cfg: LifecycleConfig
) -> None:
    """A nested coherent_workspace must NOT uninstall the outer shim on inner
    exit — the outer context owns the patch."""
    _seed(tmp_path)
    try:
        with coherent_workspace(tmp_path, managed=("data/**",), config=fast_cfg):
            outer_open = builtins.open
            assert outer_open.__name__ == "coherent_open"  # outer installed
            with coherent_workspace(tmp_path, managed=("data/**",), config=fast_cfg):
                pass  # inner exit must NOT uninstall
            assert builtins.open is outer_open  # outer shim still active
        assert builtins.open.__name__ == "open"  # outer exit restores
    finally:
        stop_coordinator(tmp_path)


def test_write_rejects_non_bytes(tmp_path: Path, fast_cfg: LifecycleConfig) -> None:
    """write() rejects non-bytes input with TypeError (the bytes|bytearray contract)."""
    _seed(tmp_path)
    vol = CoherentVolume(tmp_path, managed=("data/**",), config=fast_cfg)
    try:
        with pytest.raises(TypeError):
            vol.write("data/shared.txt", "a string, not bytes")  # type: ignore[arg-type]
    finally:
        stop_coordinator(tmp_path)


def test_write_accepts_bytearray(tmp_path: Path, fast_cfg: LifecycleConfig) -> None:
    """write() accepts bytearray (matches the bytes|bytearray annotation)."""
    target = _seed(tmp_path)
    vol = CoherentVolume(tmp_path, managed=("data/**",), config=fast_cfg)
    try:
        vol.write("data/shared.txt", bytearray(b"from-bytearray"))
        assert target.read_bytes() == b"from-bytearray"
    finally:
        stop_coordinator(tmp_path)


def test_read_outside_root_raises(tmp_path: Path, fast_cfg: LifecycleConfig) -> None:
    """A path that escapes the workspace root raises CoherenceError (not a silent
    coordinate-the-wrong-file)."""
    _seed(tmp_path)
    vol = CoherentVolume(tmp_path, managed=("data/**",), config=fast_cfg)
    try:
        with pytest.raises(CoherenceError):
            vol.read("/etc/hostname")  # absolute, outside the workspace root
    finally:
        stop_coordinator(tmp_path)


def test_stale_read_generation_is_cas_retry_eligible() -> None:
    """The read-generation fence reject reason is retry-eligible on the OCC
    cross-process path (reacquire + fresh read) -- classified 'conflict', not a
    terminal 'raise', and matched EXACTLY against the shared constant. Tested
    spawn-free: _classify_cas_response reads only the class attr."""
    from unittest.mock import MagicMock

    from ccs.core.exceptions import STALE_READ_GENERATION_REASON

    assert STALE_READ_GENERATION_REASON in CoherentVolume._CAS_RETRY_REASONS
    classify = CoherentVolume._classify_cas_response
    # spec'd mock: any future self-attribute the classifier grows raises
    # AttributeError here instead of silently returning a MagicMock value.
    stub = MagicMock(spec=CoherentVolume)
    stub._CAS_RETRY_REASONS = CoherentVolume._CAS_RETRY_REASONS
    assert classify(stub, {"ok": False, "reason": STALE_READ_GENERATION_REASON}) == "conflict"
    assert classify(stub, {"ok": True}) == "win"
    assert classify(stub, {"ok": False, "reason": "commit_cas_corruption"}) == "raise"


# ----------------------------------------------------------------------
# on_stale_read knob (PH-A read-surface instance): opt-in enforce on a
# foreign-edit / stale-view strict deny at read time.
# ----------------------------------------------------------------------


def _seed_file(tmp_path: Path, rel: str = "data/x.txt", content: bytes = b"v1") -> Path:
    target = tmp_path / rel
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(content)
    return target


def test_on_stale_read_invalid_value_rejected(tmp_path: Path) -> None:
    with pytest.raises(ValueError):
        CoherentVolume(tmp_path, managed=("data/**",), on_stale_read="bogus")


def test_on_stale_read_allow_is_default_and_swallows(
    tmp_path: Path, fast_cfg: LifecycleConfig
) -> None:
    """Default on_stale_read='allow' is back-compat: a foreign edit is detected
    coordinator-side but read() returns the current bytes, no raise."""
    target = _seed_file(tmp_path, content=b"v1")
    vol = CoherentVolume(tmp_path, managed=("data/**",), config=fast_cfg)
    try:
        assert vol.read("data/x.txt") == b"v1"  # SHARED@v1
        target.write_bytes(b"v2")               # FOREIGN edit (not via the volume)
        assert vol.read("data/x.txt") == b"v2"  # swallowed -> fresh bytes
    finally:
        stop_coordinator(tmp_path)


def test_on_stale_read_raise_surfaces_foreign_edit(
    tmp_path: Path, fast_cfg: LifecycleConfig
) -> None:
    """on_stale_read='raise': a SHARED holder whose tracked file was edited
    out-of-band gets StaleView on read, instead of silently receiving the
    foreign bytes."""
    target = _seed_file(tmp_path, content=b"v1")
    vol = CoherentVolume(
        tmp_path, managed=("data/**",), on_stale_read="raise", config=fast_cfg
    )
    try:
        assert vol.read("data/x.txt") == b"v1"
        target.write_bytes(b"v2")
        with pytest.raises(StaleView):
            vol.read("data/x.txt")
    finally:
        stop_coordinator(tmp_path)


def test_reacquire_recovers_under_on_stale_read_raise(
    tmp_path: Path, fast_cfg: LifecycleConfig
) -> None:
    """reacquire()'s recovery read bypasses on_stale_read='raise' so recovery is
    never blocked: it returns the current bytes without raising."""
    target = _seed_file(tmp_path, content=b"v1")
    vol = CoherentVolume(
        tmp_path, managed=("data/**",), on_stale_read="raise", config=fast_cfg
    )
    try:
        vol.read("data/x.txt")
        target.write_bytes(b"v2")
        with pytest.raises(StaleView):
            vol.read("data/x.txt")
        assert vol.reacquire("data/x.txt") == b"v2"  # recovery does not raise
    finally:
        stop_coordinator(tmp_path)


def test_on_stale_read_raise_does_not_fire_on_unmanaged_path(
    tmp_path: Path, fast_cfg: LifecycleConfig
) -> None:
    """on_stale_read='raise' only surfaces a STRICT-mode deny. A foreign edit to a
    path OUTSIDE the managed (strict) globs must NOT raise — the coordinator fires
    the deny only when is_strict_mode(path) is True."""
    target = _seed_file(tmp_path, rel="notes/x.txt", content=b"v1")
    vol = CoherentVolume(
        tmp_path, managed=("data/**",), on_stale_read="raise", config=fast_cfg
    )
    try:
        assert vol.read("notes/x.txt") == b"v1"  # not under managed -> not strict
        target.write_bytes(b"v2")                # foreign edit on a non-strict path
        assert vol.read("notes/x.txt") == b"v2"  # no raise: returns fresh bytes
    finally:
        stop_coordinator(tmp_path)


def test_on_stale_read_raise_independent_of_on_error_degrade(
    tmp_path: Path, fast_cfg: LifecycleConfig
) -> None:
    """on_stale_read and on_error govern independent branches: a foreign-edit deny
    still raises StaleView under on_error='degrade' (degrade governs infra
    failures, not the semantic stale-view deny)."""
    target = _seed_file(tmp_path, content=b"v1")
    vol = CoherentVolume(
        tmp_path, managed=("data/**",),
        on_error="degrade", on_stale_read="raise", config=fast_cfg,
    )
    try:
        assert vol.read("data/x.txt") == b"v1"
        target.write_bytes(b"v2")
        with pytest.raises(StaleView):
            vol.read("data/x.txt")
    finally:
        stop_coordinator(tmp_path)


# ----------------------------------------------------------------------
# SB-23 pre-write content-CAS (on_stale_write): deny a write that would
# clobber a foreign / out-of-band edit since the last read/write.
# ----------------------------------------------------------------------


def test_on_stale_write_invalid_value_rejected(tmp_path: Path) -> None:
    with pytest.raises(ValueError):
        CoherentVolume(tmp_path, managed=("data/**",), on_stale_write="bogus")


def test_write_denies_foreign_edit_by_default(
    tmp_path: Path, fast_cfg: LifecycleConfig
) -> None:
    """Default on_stale_write='raise': a write whose target was edited out-of-band
    since the last read is denied (StaleView) and does NOT clobber the foreign bytes."""
    target = _seed_file(tmp_path, content=b"v1")
    vol = CoherentVolume(tmp_path, managed=("data/**",), config=fast_cfg)
    try:
        assert vol.read("data/x.txt") == b"v1"   # seeds baseline = hash(v1)
        target.write_bytes(b"v2")                # FOREIGN edit (not via the volume)
        with pytest.raises(StaleView):
            vol.write("data/x.txt", b"v3")
        assert target.read_bytes() == b"v2"      # foreign edit NOT clobbered
    finally:
        stop_coordinator(tmp_path)


def test_write_guard_seeded_by_read_only(
    tmp_path: Path, fast_cfg: LifecycleConfig
) -> None:
    """The read seeds the baseline even with no prior write — a first-read-then-write
    still guards (confirms read-seeding, not write, drives the guard)."""
    target = _seed_file(tmp_path, content=b"v1")
    vol = CoherentVolume(tmp_path, managed=("data/**",), config=fast_cfg)
    try:
        vol.read("data/x.txt")                   # first access, never written
        target.write_bytes(b"foreign")
        with pytest.raises(StaleView):
            vol.write("data/x.txt", b"mine")
    finally:
        stop_coordinator(tmp_path)


def test_write_succeeds_when_disk_unchanged_and_advances_baseline(
    tmp_path: Path, fast_cfg: LifecycleConfig
) -> None:
    """No foreign edit: the write proceeds; the own write advances the baseline so a
    second consecutive write does not false-deny."""
    _seed_file(tmp_path, content=b"v1")
    vol = CoherentVolume(tmp_path, managed=("data/**",), config=fast_cfg)
    try:
        vol.read("data/x.txt")
        vol.write("data/x.txt", b"v2")           # disk matches baseline -> proceeds
        assert (tmp_path / "data/x.txt").read_bytes() == b"v2"
        vol.write("data/x.txt", b"v3")           # own write advanced baseline -> no false deny
        assert (tmp_path / "data/x.txt").read_bytes() == b"v3"
    finally:
        stop_coordinator(tmp_path)


def test_reacquire_after_sb23_deny_then_write_succeeds(
    tmp_path: Path, fast_cfg: LifecycleConfig
) -> None:
    """After a foreign-edit deny, reacquire() re-seeds the baseline so the rebuilt
    write succeeds (no false deny after recovery)."""
    target = _seed_file(tmp_path, content=b"v1")
    vol = CoherentVolume(tmp_path, managed=("data/**",), config=fast_cfg)
    try:
        vol.read("data/x.txt")
        target.write_bytes(b"v2")
        with pytest.raises(StaleView):
            vol.write("data/x.txt", b"clobber")
        assert vol.reacquire("data/x.txt") == b"v2"   # re-seeds baseline = hash(v2)
        vol.write("data/x.txt", b"v3-from-v2")        # rebuilt from fresh -> succeeds
        assert target.read_bytes() == b"v3-from-v2"
    finally:
        stop_coordinator(tmp_path)


def test_on_stale_write_allow_proceeds_and_clobbers(
    tmp_path: Path, fast_cfg: LifecycleConfig
) -> None:
    """on_stale_write='allow' restores pre-SB-23 behavior: the foreign edit is not
    raised; the write proceeds and clobbers."""
    target = _seed_file(tmp_path, content=b"v1")
    vol = CoherentVolume(
        tmp_path, managed=("data/**",), on_stale_write="allow", config=fast_cfg
    )
    try:
        vol.read("data/x.txt")
        target.write_bytes(b"v2")
        vol.write("data/x.txt", b"v3")           # no raise; clobbers v2
        assert target.read_bytes() == b"v3"
    finally:
        stop_coordinator(tmp_path)


def test_write_after_foreign_delete_proceeds(
    tmp_path: Path, fast_cfg: LifecycleConfig
) -> None:
    """Foreign delete: the re-hash returns None (absent file) -> no-baseline skip; the
    write proceeds as a normal create (R6)."""
    target = _seed_file(tmp_path, content=b"v1")
    vol = CoherentVolume(tmp_path, managed=("data/**",), config=fast_cfg)
    try:
        vol.read("data/x.txt")
        target.unlink()                          # FOREIGN delete
        vol.write("data/x.txt", b"recreated")    # None disk hash -> no deny
        assert target.read_bytes() == b"recreated"
    finally:
        stop_coordinator(tmp_path)


def test_post_edit_preempt_still_advances_observed_baseline(
    tmp_path: Path, fast_cfg: LifecycleConfig, monkeypatch
) -> None:
    """R5: if the atomic write lands but the post-edit POST then preempts, the
    observed baseline must STILL be advanced (the bytes are on disk) so the instance
    does not self-false-deny on a later write."""
    from ccs.core.exceptions import CommitPreempted

    _seed_file(tmp_path, content=b"v1")
    vol = CoherentVolume(tmp_path, managed=("data/**",), config=fast_cfg)
    try:
        vol.read("data/x.txt")
        orig_check = vol._check_grant

        def fake_check_grant(resp, rel, *, phase):
            if phase == "post-edit":
                raise CommitPreempted("simulated preempt")
            return orig_check(resp, rel, phase=phase)

        monkeypatch.setattr(vol, "_check_grant", fake_check_grant)
        with pytest.raises(CoherenceError):
            vol.write("data/x.txt", b"v2")
        # Bytes landed AND the observed baseline advanced to hash(v2).
        assert (tmp_path / "data/x.txt").read_bytes() == b"v2"
        assert vol._last_observed_hash["data/x.txt"] == vol._sha256_bytes(b"v2")
    finally:
        stop_coordinator(tmp_path)


def test_install_forwards_on_stale_write(
    tmp_path: Path, fast_cfg: LifecycleConfig
) -> None:
    """install()/coherent_workspace() forward on_stale_write to the volume."""
    _seed_file(tmp_path, content=b"v1")
    vol = install(tmp_path, managed=("data/**",), on_stale_write="allow", config=fast_cfg)
    try:
        assert vol._on_stale_write == "allow"
    finally:
        uninstall()
        stop_coordinator(tmp_path)


def test_write_unmanaged_path_in_managed_volume_proceeds(
    tmp_path: Path, fast_cfg: LifecycleConfig
) -> None:
    """SB-23 fires only for a path THIS volume manages. A volume managing other/**
    that reads+writes data/... (unmanaged here) does NOT deny a divergent disk —
    that path is not strict, so a coordinated change there is not necessarily a
    foreign edit (the bool(managed) gate would wrongly fire; the per-path match
    must not)."""
    target = _seed_file(tmp_path, rel="data/x.txt", content=b"v1")
    vol = CoherentVolume(tmp_path, managed=("other/**",), config=fast_cfg)
    try:
        vol.read("data/x.txt")                   # seeds a baseline, but data/** is unmanaged
        target.write_bytes(b"v2")                # divergent disk on an unmanaged path
        vol.write("data/x.txt", b"v3")           # SB-23 does NOT fire -> proceeds
        assert target.read_bytes() == b"v3"
    finally:
        stop_coordinator(tmp_path)


def test_remint_preserves_other_paths_baseline(
    tmp_path: Path, fast_cfg: LifecycleConfig
) -> None:
    """A re-mint (here via reacquire of a DIFFERENT path) must NOT clear the
    foreign-edit baseline of OTHER managed paths — observed-disk hashes are
    disk-scoped, not identity-scoped. Otherwise every write_cas conflict (which
    re-mints) would blind SB-23 on all other previously-read paths."""
    a = _seed_file(tmp_path, rel="data/a.txt", content=b"a1")
    _seed_file(tmp_path, rel="data/b.txt", content=b"b1")
    vol = CoherentVolume(tmp_path, managed=("data/**",), config=fast_cfg)
    try:
        vol.read("data/a.txt")               # seed A's baseline
        vol.read("data/b.txt")               # seed B's baseline
        vol.reacquire("data/b.txt")          # re-mints; A's baseline must SURVIVE
        a.write_bytes(b"foreign-a2")         # foreign edit on A
        with pytest.raises(StaleView):
            vol.write("data/a.txt", b"a3")   # A's baseline survived -> deny
    finally:
        stop_coordinator(tmp_path)


def test_write_denies_foreign_edit_independent_of_on_error_degrade(
    tmp_path: Path, fast_cfg: LifecycleConfig
) -> None:
    """SB-23 is adapter-local: the foreign-edit deny fires regardless of on_error
    (the guard does not need the coordinator). Write-side mirror of
    test_on_stale_read_raise_independent_of_on_error_degrade."""
    target = _seed_file(tmp_path, content=b"v1")
    vol = CoherentVolume(
        tmp_path, managed=("data/**",), on_error="degrade", config=fast_cfg
    )
    try:
        vol.read("data/x.txt")
        target.write_bytes(b"v2")
        with pytest.raises(StaleView):
            vol.write("data/x.txt", b"v3")
    finally:
        stop_coordinator(tmp_path)


def test_write_after_write_cas_win_no_false_deny(
    tmp_path: Path, fast_cfg: LifecycleConfig
) -> None:
    """A write_cas win advances the observed baseline, so a following plain write on
    the same path (no foreign edit) does NOT false-deny."""
    _seed_file(tmp_path, content=b"v1")
    vol = CoherentVolume(tmp_path, managed=("data/**",), config=fast_cfg)
    try:
        vol.write_cas("data/x.txt", lambda cur: cur + b"-cas")  # win -> advances baseline
        vol.write("data/x.txt", b"plain-after-cas")             # no foreign edit -> succeeds
        assert (tmp_path / "data/x.txt").read_bytes() == b"plain-after-cas"
    finally:
        stop_coordinator(tmp_path)


def test_write_cas_seeds_baseline_then_plain_write_denies_foreign_edit(
    tmp_path: Path, fast_cfg: LifecycleConfig
) -> None:
    """The OCC read path seeds the baseline: after a write_cas win, a foreign edit
    then a plain write is denied (confirms the OCC-read seeding feeds the guard)."""
    target = _seed_file(tmp_path, content=b"v1")
    vol = CoherentVolume(tmp_path, managed=("data/**",), config=fast_cfg)
    try:
        vol.write_cas("data/x.txt", lambda cur: b"v2")   # win; baseline=hash(v2), disk=v2
        target.write_bytes(b"foreign-v3")                # foreign edit
        with pytest.raises(StaleView):
            vol.write("data/x.txt", b"v4")
    finally:
        stop_coordinator(tmp_path)


def test_write_managed_path_without_prior_read_proceeds(
    tmp_path: Path, fast_cfg: LifecycleConfig
) -> None:
    """No baseline (a managed path never read through this volume) -> SB-23 skips;
    the write proceeds (the guard needs a prior observation to compare against)."""
    _seed_file(tmp_path, content=b"v1")
    vol = CoherentVolume(tmp_path, managed=("data/**",), config=fast_cfg)
    try:
        vol.write("data/x.txt", b"v2")   # never read -> no baseline -> proceeds
        assert (tmp_path / "data/x.txt").read_bytes() == b"v2"
    finally:
        stop_coordinator(tmp_path)


def test_stale_write_deny_reason_is_byte_stable(
    tmp_path: Path, fast_cfg: LifecycleConfig
) -> None:
    """The foreign-edit deny raises with the exact static _STALE_WRITE_DENY_REASON
    constant — no path/hash/timestamp interpolation, so a model's retry loop sees
    identical text every time (KTD-P). Regression pin against future interpolation."""
    from ccs.adapters.coherent_volume import _STALE_WRITE_DENY_REASON

    target = _seed_file(tmp_path, content=b"v1")
    vol = CoherentVolume(tmp_path, managed=("data/**",), config=fast_cfg)
    try:
        vol.read("data/x.txt")
        target.write_bytes(b"v2")
        with pytest.raises(StaleView) as exc:
            vol.write("data/x.txt", b"v3")
        assert str(exc.value) == _STALE_WRITE_DENY_REASON  # static, no interpolation
    finally:
        stop_coordinator(tmp_path)


def test_write_cas_on_foreign_edit_wedges_not_stale_view(
    tmp_path: Path, fast_cfg: LifecycleConfig
) -> None:
    """SB-23 scope boundary: write_cas is version-CAS, not content-CAS — it does NOT
    raise StaleView. But on a strict path it is NOT unguarded: the read-side hash
    deny makes write_cas's comparand read (_read_with_version) fail closed, so a
    pre-existing foreign edit WEDGES the CAS (ViewWedged after the reacquire budget)
    rather than silently clobbering it. Pins both: SB-23's content-CAS guards only
    the plain write() path, and write_cas still fails closed (no silent loss)."""
    from ccs.core.exceptions import ViewWedged

    target = _seed_file(tmp_path, content=b"v1")
    vol = CoherentVolume(tmp_path, managed=("data/**",), config=fast_cfg)
    try:
        vol.read("data/x.txt")
        target.write_bytes(b"foreign-v2")    # foreign edit (coordinator version unchanged)
        with pytest.raises(ViewWedged):      # fail-closed — NOT StaleView, NOT a clobber
            vol.write_cas("data/x.txt", lambda cur: cur + b"-cas")
        assert target.read_bytes() == b"foreign-v2"  # foreign edit intact (not clobbered)
    finally:
        stop_coordinator(tmp_path)


def test_write_cas_waits_between_denied_comparand_reads(
    tmp_path: Path, fast_cfg: LifecycleConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The denied comparand read polls a TRANSIENT that clears on ANOTHER writer's
    progress (the window between a peer's confirmed CAS and its disk write), so the
    poll must WAIT — yielding the CPU to that peer — not spin.

    Regression for the intermittent concurrent-writers demo red (2026-09-09): with
    no wait, the streak bound was denominated in local HTTP round trips, so it
    bought only ~40ms of wall clock and a peer descheduled inside that window
    (routine on a loaded CI runner) wedged the loser even though the view would
    have cleared moments later.

    Pins the SCHEDULE, not a scalar floor. An ``elapsed >= sum(schedule)`` assertion
    derives its own threshold from these same constants, so it moves with them:
    zeroing the base makes the bound ``>= 0`` and the fix can be reverted with the
    test still green (measured — the zeroed mutant passed). The recorded sequence
    plus the non-degeneracy assertions below fail on every reachable mutant: a
    removed wait, a zeroed or inverted constant, a schedule flattened to the cap,
    and an off-by-one in the exponent.
    """
    # Without this the rest is vacuous: a zeroed base makes every entry 0.0, so the
    # recorded sequence still matches and the elapsed floor still holds.
    assert DENIED_READ_BACKOFF_BASE_SEC > 0
    assert DENIED_READ_BACKOFF_CAP_SEC >= DENIED_READ_BACKOFF_BASE_SEC
    schedule = [
        min(DENIED_READ_BACKOFF_BASE_SEC * 2**i, DENIED_READ_BACKOFF_CAP_SEC)
        for i in range(MAX_CAS_REACQUIRES)  # one wait per denied read before the bound trips
    ]

    # Record what the loop asks to wait, and still wait it. ``time`` is used nowhere
    # else in the adapter, so shimming that module-level name leaves the real
    # time.sleep untouched for every other caller, the coordinator client included.
    waits: list[float] = []

    def recording_sleep(seconds: float) -> None:
        waits.append(seconds)
        time.sleep(seconds)

    monkeypatch.setattr(
        coherent_volume_module, "time", SimpleNamespace(sleep=recording_sleep)
    )

    target = _seed_file(tmp_path, content=b"v1")
    vol = CoherentVolume(tmp_path, managed=("data/**",), config=fast_cfg)
    try:
        vol.read("data/x.txt")
        target.write_bytes(b"foreign-v2")  # a denied view that never clears
        waits.clear()  # only the write_cas call's own waits are the subject
        started = time.perf_counter()
        with pytest.raises(ViewWedged):
            vol.write_cas("data/x.txt", lambda cur: cur + b"-cas")
        elapsed = time.perf_counter() - started
        observed = list(waits)
    finally:
        stop_coordinator(tmp_path)

    assert observed == schedule, (
        f"denied-read backoff schedule changed: waited {observed}, expected {schedule}"
    )
    # And the waits were real wall clock, not merely requested.
    assert elapsed >= sum(schedule), (
        f"write_cas wedged after {elapsed:.3f}s but its own backoff schedule is "
        f"{sum(schedule):.3f}s — the recorded waits did not actually elapse"
    )


# ---------------------------------------------------------------------------
# A stable session id across re-mints, and the release of an abandoned write
# grant.
#
# A re-mint sheds the sticky INVALID, the invalidation transient and the read
# generation by landing the next request on a FRESH coordinator row. The row key
# is the session id folded with a per-attempt incarnation that every request
# carries in the subagent field, so the session id stays put while the row moves.
# The abandoned incarnation is a different agent to the coordinator: a grant it
# still holds is foreign to this volume, and write() is the one path that leaves
# one (EXCLUSIVE, then MODIFIED) — so the re-mint that abandons it releases it.
# ---------------------------------------------------------------------------


def _count_stops(
    monkeypatch: pytest.MonkeyPatch, sent: list[tuple[str, dict, str]], vol: CoherentVolume
) -> None:
    """Record every coordinator request as ``(route, body, current incarnation at
    send time)`` and forward it unchanged."""
    real_post = coherent_volume_module._coordinator_post

    def spy(endpoint: object, path: str, payload: dict) -> object:
        sent.append((path, dict(payload), vol._incarnation))
        return real_post(endpoint, path, payload)

    monkeypatch.setattr(coherent_volume_module, "_coordinator_post", spy)


def test_every_attempt_lands_on_its_own_coordinator_row(
    tmp_path: Path, fast_cfg: LifecycleConfig
) -> None:
    """The incarnation rides in the coordinator's subagent field, and the
    coordinator reads a value outside that field's shape as "no subagent" —
    silently, with no 400. Every attempt would then share the session's ONE parent
    row, the INVALID a peer's commit leaves there would outlive every re-mint, and
    recovery would wedge. Pinned two ways: the coordinator's own reader returns
    each incarnation verbatim, and its grant rows show each attempt on a row of
    its own while the parent row is never touched."""
    rel = "data/shared.txt"
    _seed(tmp_path, content=b"v1")
    vol = CoherentVolume(tmp_path, managed=("data/**",), config=fast_cfg)
    try:
        parent_row = str(session_to_agent_id(vol.session_id))
        session = vol.session_id
        rows: list[str] = []
        for attempt in range(3):
            if attempt == 0:
                vol.read(rel)
            else:
                vol.reacquire(rel)
            assert read_subagent_id({"agent_id": vol._incarnation}) == vol._incarnation
            rows.append(_agent_id(vol))
            assert _held(vol, rows[-1]) == {rel: "SHARED"}
        assert len(set(rows)) == 3, "a re-mint must move the next request to a new row"
        assert parent_row not in rows
        assert _held(vol, parent_row) == {}
        assert vol.session_id == session
    finally:
        stop_coordinator(tmp_path)


def test_every_request_names_the_current_incarnation(
    tmp_path: Path, fast_cfg: LifecycleConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Every request the volume sends names its CURRENT incarnation; the one
    exception is the release of an abandoned write grant, which names the
    abandoned one. A request without it lands on the session's parent row: a read
    there registers a view the next attempt does not own, and a release there
    releases nothing."""
    rel, other = "data/shared.txt", "data/other.txt"
    _seed(tmp_path, content=b"v1")
    _seed(tmp_path, rel=other, content=b"o1")
    vol = CoherentVolume(tmp_path, managed=("data/**",), config=fast_cfg)
    try:
        sent: list[tuple[str, dict, str]] = []
        _count_stops(monkeypatch, sent, vol)

        vol.read(rel)
        vol.write_cas(rel, lambda cur: cur + b"+cas")
        vol.write(rel, b"v3")
        _data, version = vol.read_with_version(rel)
        vol.write_cas_at(rel, version, b"v4")  # re-mints: releases write()'s grant
        vol.reacquire(rel)
        _data, version = vol.read_with_version(rel)
        _data, other_version = vol.read_with_version(other)
        vol.atomic_publish([(rel, version, "v5"), (other, other_version, "o2")])

        routes = {route for route, _body, _current in sent}
        assert routes >= {
            "/hooks/pre-read",
            "/hooks/pre-edit",
            "/hooks/post-edit",
            "/hooks/post-edit-cas",
            "/hooks/session-stop",
            "/session/begin",
            "/session/commit_all",
        }, routes
        # write() ran once, so exactly one incarnation took a grant, and the one
        # release must name THAT incarnation -- not merely some other one.
        (write_incarnation,) = {
            body["agent_id"] for route, body, _c in sent if route == "/hooks/pre-edit"
        }
        stops = [body for route, body, _c in sent if route == "/hooks/session-stop"]
        assert [body["agent_id"] for body in stops] == [write_incarnation], (
            "the release did not name the incarnation write() left holding the grant"
        )
        for route, body, current in sent:
            assert body["session_id"] == vol.session_id, route
            if route != "/hooks/session-stop":
                assert body.get("agent_id") == current, route
    finally:
        stop_coordinator(tmp_path)


@pytest.mark.parametrize("lost", ["pre-edit answer", "post-edit"])
def test_write_cas_at_commits_over_a_grant_its_own_write_left_standing(
    tmp_path: Path, fast_cfg: LifecycleConfig, monkeypatch: pytest.MonkeyPatch, lost: str
) -> None:
    """A write() that took EXCLUSIVE and failed before committing leaves the grant
    standing at the UNCHANGED version. A CAS at that version from the same volume
    re-mints first, and the old incarnation is a different agent to the
    coordinator, so the CAS met its own grant as ``other_holder`` with expected ==
    current — and every retry re-minted into the same refusal (#196). Two ways to
    strand the grant: the acquire landed but its answer was lost, or the commit
    never reached the coordinator. The grant has to be recorded at the acquire,
    not at the commit, or both are missed."""
    rel = "data/shared.txt"
    target = _seed(tmp_path, content=b"v1")
    vol = CoherentVolume(tmp_path, managed=("data/**",), config=fast_cfg)
    try:
        _data, version = vol.read_with_version(rel)
        real_post = coherent_volume_module._coordinator_post

        def strand_the_grant(endpoint: object, path: str, payload: dict) -> object:
            if lost == "pre-edit answer" and path == "/hooks/pre-edit":
                real_post(endpoint, path, payload)  # the acquire lands ...
                raise CoordinatorUnavailable("simulated: the acquire's answer was lost")
            if lost == "post-edit" and path == "/hooks/post-edit" and payload.get("success"):
                raise CoordinatorUnavailable("simulated: the commit never arrived")
            return real_post(endpoint, path, payload)

        monkeypatch.setattr(coherent_volume_module, "_coordinator_post", strand_the_grant)
        # Same bytes as on disk, so the only thing left over is the grant.
        with pytest.raises(CoherenceError):
            vol.write(rel, b"v1")
        monkeypatch.setattr(coherent_volume_module, "_coordinator_post", real_post)
        stranded = _agent_id(vol)
        assert _held(vol, stranded) == {rel: "EXCLUSIVE"}, "precondition: the grant stands"

        vol.write_cas_at(rel, version, b"v2-cas")

        assert target.read_bytes() == b"v2-cas"
        assert _held(vol, stranded) == {}, "the abandoned incarnation still holds a grant"
    finally:
        stop_coordinator(tmp_path)


def test_write_cas_at_after_a_committed_write_on_the_same_volume_commits(
    tmp_path: Path, fast_cfg: LifecycleConfig
) -> None:
    """A successful write() leaves this volume's incarnation MODIFIED — the grant
    outlives the call — and a CAS at the NEW version from the same volume met it
    as ``other_holder`` with expected == current (2 == 2): the volume refused by
    its own finished write. The re-mint that abandons the incarnation releases
    the grant, and afterwards the abandoned row holds nothing."""
    rel = "data/shared.txt"
    target = _seed(tmp_path, content=b"v1")
    vol = CoherentVolume(tmp_path, managed=("data/**",), config=fast_cfg)
    try:
        vol.write(rel, b"v2-pessimistic")
        writer_row = _agent_id(vol)
        assert _held(vol, writer_row) == {rel: "MODIFIED"}, "precondition: the grant stands"
        _data, version = vol.read_with_version(rel)

        vol.write_cas_at(rel, version, b"v3-cas")

        assert target.read_bytes() == b"v3-cas"
        assert _held(vol, writer_row) == {}, "the abandoned incarnation still holds a grant"
    finally:
        stop_coordinator(tmp_path)


def test_write_cas_directly_after_a_committed_write_on_the_same_volume_commits(
    tmp_path: Path, fast_cfg: LifecycleConfig
) -> None:
    """write() then write_cas on the same file, with nothing in between. The
    loop's first attempt used to run under the incarnation write() left
    MODIFIED, so its comparand read was not the None-state, hash-checked read
    the loop relies on, and the commit was refused outright
    (``commit_cas_not_allowed ... occ_is_shared_or_invalid_only``) — write_cas_at
    worked after a write() and write_cas did not. The loop now rotates first
    when the current incarnation holds a write grant, and the grant is released."""
    rel = "data/shared.txt"
    target = _seed(tmp_path, content=b"v1")
    vol = CoherentVolume(tmp_path, managed=("data/**",), config=fast_cfg)
    try:
        vol.write(rel, b"v2-pessimistic")
        writer_row = _agent_id(vol)
        assert _held(vol, writer_row) == {rel: "MODIFIED"}, "precondition: the grant stands"

        vol.write_cas(rel, lambda cur: cur + b"+cas")

        assert target.read_bytes() == b"v2-pessimistic+cas"
        assert _held(vol, writer_row) == {}, "the abandoned incarnation still holds a grant"
    finally:
        stop_coordinator(tmp_path)


def test_uncontended_write_cas_without_a_prior_write_does_not_rotate(
    tmp_path: Path, fast_cfg: LifecycleConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The rotation write_cas makes after a write() must not become a rotation on
    every write_cas: an optimistic-only commit with no contention is one
    comparand read and one CAS under the volume's current incarnation, with no
    release. Counts the requests, so an unconditional rotate-first cannot pass."""
    rel = "data/shared.txt"
    target = _seed(tmp_path, content=b"v1")
    vol = CoherentVolume(tmp_path, managed=("data/**",), config=fast_cfg)
    try:
        vol.read(rel)
        incarnation_before = vol._incarnation
        sent: list[tuple[str, dict, str]] = []
        _count_stops(monkeypatch, sent, vol)

        vol.write_cas(rel, lambda cur: cur + b"+cas")

        assert [route for route, _b, _c in sent] == ["/hooks/pre-read", "/hooks/post-edit-cas"]
        # A rotation that sends no request is invisible to the route list, so
        # compare against the incarnation the volume held before the call.
        assert {body.get("agent_id") for _r, body, _c in sent} == {incarnation_before}, (
            "an uncontended write_cas moved to a new incarnation")
        assert target.read_bytes() == b"v1+cas"
    finally:
        stop_coordinator(tmp_path)


def test_a_denied_write_on_another_path_keeps_the_grant_record(
    tmp_path: Path, fast_cfg: LifecycleConfig
) -> None:
    """One incarnation can hold a write grant on one path and then be denied a
    write on another. The deny proves no grant was taken on the SECOND path only;
    if it dropped the incarnation's record, the grant on the first path would be
    forgotten, and the next optimistic commit there would run under the
    incarnation still holding it and be refused with no peer holding anything."""
    p, q = "data/p.txt", "data/q.txt"
    target = _seed(tmp_path, rel=p, content=b"p1")
    _seed(tmp_path, rel=q, content=b"q1")
    vol, peer = _pair(tmp_path, fast_cfg)
    try:
        vol.read(q)
        vol.write(p, b"p2")
        writer_row = _agent_id(vol)
        assert _held(vol, writer_row) == {p: "MODIFIED", q: "SHARED"}, "precondition"
        peer.read(q)
        peer.write(q, b"q2-peer")  # the volume's row goes INVALID on q
        _end_turn(peer)
        with pytest.raises(StaleView):
            vol.write(q, b"stale")  # denied, on the same incarnation that holds p

        vol.write_cas(p, lambda cur: cur + b"+cas")

        assert target.read_bytes() == b"p2+cas"
        assert _held(vol, writer_row) == {}, "the grant on the first path was never released"
    finally:
        stop_coordinator(tmp_path)


def test_a_degraded_release_answer_is_not_a_confirmed_release(
    tmp_path: Path, fast_cfg: LifecycleConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A watchdog-degraded session-stop answers ``ok: true`` whether or not the
    release ran. Treated as confirmed, it would drop the record while the grant
    still stands, and every later commit would be refused by the volume's own
    grant with nothing left to retry the release. It must stay recorded, and the
    next attempt must release it."""
    rel = "data/shared.txt"
    target = _seed(tmp_path, content=b"v1")
    vol = CoherentVolume(tmp_path, managed=("data/**",), on_error="degrade", config=fast_cfg)
    try:
        vol.write(rel, b"v2")
        writer_row = _agent_id(vol)
        _data, version = vol.read_with_version(rel)
        real_post = coherent_volume_module._coordinator_post
        degraded_left = [1]

        def degrade_one_release(endpoint: object, path: str, payload: dict) -> object:
            if path == "/hooks/session-stop" and degraded_left[0]:
                degraded_left[0] -= 1
                return {"ok": True, "degraded": True}  # the release did not run
            return real_post(endpoint, path, payload)

        monkeypatch.setattr(coherent_volume_module, "_coordinator_post", degrade_one_release)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            with pytest.raises(CasVersionConflict):
                vol.write_cas_at(rel, version, b"v3")
        assert _held(vol, writer_row) == {rel: "MODIFIED"}, "precondition: the release never ran"

        vol.write_cas_at(rel, version, b"v3")

        assert target.read_bytes() == b"v3"
        assert _held(vol, writer_row) == {}
    finally:
        stop_coordinator(tmp_path)


def test_a_failed_release_stops_the_pass_and_keeps_every_record(
    tmp_path: Path, fast_cfg: LifecycleConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    """When one release fails, the rest would meet the same coordinator, so the
    pass stops: one re-mint spends at most one failed request, and every
    incarnation it did not confirm stays recorded for the next re-mint."""
    p, q, r = "data/p.txt", "data/q.txt", "data/r.txt"
    for rel in (p, q, r):
        _seed(tmp_path, rel=rel, content=b"1")
    vol = CoherentVolume(tmp_path, managed=("data/**",), on_error="degrade", config=fast_cfg)
    try:
        real_post = coherent_volume_module._coordinator_post
        stops: list[str] = []
        failing = [True]

        def fail_releases(endpoint: object, path: str, payload: dict) -> object:
            if path == "/hooks/session-stop":
                stops.append(payload["agent_id"])
                if failing[0]:
                    return {"ok": True, "degraded": True}
            return real_post(endpoint, path, payload)

        monkeypatch.setattr(coherent_volume_module, "_coordinator_post", fail_releases)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            vol.write(p, b"2")
            first_row = _agent_id(vol)
            vol.reacquire(r)  # re-mint: its release of the first incarnation fails
            vol.write(q, b"2")
            second_row = _agent_id(vol)
            stops.clear()
            vol.reacquire(r)  # re-mint with two incarnations recorded

        def write_grants(row: str) -> dict[str, str]:
            # SHARED rows from the reacquire reads block nothing and are not released.
            return {k: v for k, v in _held(vol, row).items() if v in ("MODIFIED", "EXCLUSIVE")}

        assert len(stops) == 1, f"one re-mint sent {len(stops)} failing releases"
        assert write_grants(first_row) == {p: "MODIFIED"}
        assert write_grants(second_row) == {q: "MODIFIED"}

        failing[0] = False
        vol.reacquire(r)
        assert write_grants(first_row) == {} and write_grants(second_row) == {}, (
            "an incarnation the failed pass skipped was dropped from the record")
    finally:
        stop_coordinator(tmp_path)


def test_write_cas_after_write_and_reacquire_on_the_same_volume_commits(
    tmp_path: Path, fast_cfg: LifecycleConfig
) -> None:
    """The retry-loop form of the same self-refusal: after a write(), reacquire()
    moved the volume to a fresh identity but left the write's MODIFIED grant with
    the old one, so every write_cas attempt was refused as ``other_holder`` by the
    volume's own grant, each retry re-minted into the same refusal, and the loop
    exhausted its budget (``CasRetriesExhausted``) with no peer anywhere."""
    rel = "data/shared.txt"
    target = _seed(tmp_path, content=b"v1")
    vol = CoherentVolume(tmp_path, managed=("data/**",), config=fast_cfg)
    try:
        vol.write(rel, b"v2-pessimistic")
        assert vol.reacquire(rel) == b"v2-pessimistic"

        vol.write_cas(rel, lambda cur: cur + b"+cas")

        assert target.read_bytes() == b"v2-pessimistic+cas"
    finally:
        stop_coordinator(tmp_path)


def test_re_mint_spends_no_request_on_an_incarnation_without_a_write_grant(
    tmp_path: Path, fast_cfg: LifecycleConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An incarnation that only read, committed optimistically, or had its write
    DENIED holds at most SHARED/INVALID and blocks nothing; releasing it anyway
    would add a round trip to every re-mint — every optimistic retry — for
    nothing. The requests are COUNTED, so an unconditional release cannot pass.
    The second half keeps the zero honest: a write() that takes a grant costs
    exactly ONE release at the transition, however many re-mints follow it."""
    rel = "data/shared.txt"
    target = _seed(tmp_path, content=b"v1")
    vol, peer = _pair(tmp_path, fast_cfg)
    try:
        vol.read(rel)
        peer.read(rel)
        peer.write(rel, b"v2-peer")  # vol -> INVALID
        _end_turn(peer)
        sent: list[tuple[str, dict, str]] = []
        _count_stops(monkeypatch, sent, vol)

        def stops() -> int:
            return sum(1 for route, _b, _c in sent if route == "/hooks/session-stop")

        with pytest.raises(StaleView):
            vol.write(rel, b"stale")  # denied: no grant was taken
        assert _held(vol, _agent_id(vol)) == {}  # the denied incarnation is INVALID
        assert vol.reacquire(rel) == b"v2-peer"  # re-mint 1 abandons an INVALID row
        assert _held(vol, _agent_id(vol)) == {rel: "SHARED"}
        _data, version = vol.read_with_version(rel)
        vol.write_cas_at(rel, version, b"v3-cas")  # re-mint 2 abandons a SHARED row
        vol.reacquire(rel)  # re-mint 3
        rows = {body["agent_id"] for _r, body, _c in sent if "agent_id" in body}
        assert len(rows) == 4, f"expected four incarnations across three re-mints, saw {len(rows)}"
        assert stops() == 0, "a re-mint released an incarnation that held no write grant"

        vol.write(rel, b"v4-pessimistic")
        for _ in range(2):
            _data, version = vol.read_with_version(rel)
            vol.write_cas_at(rel, version, b"v5-cas")
        assert stops() == 1, "a write() grant costs exactly one release at the transition"
        assert target.read_bytes() == b"v5-cas"
    finally:
        stop_coordinator(tmp_path)


@pytest.mark.parametrize("on_error", ["strict", "degrade"])
def test_failed_release_is_kept_and_retried_at_the_next_re_mint(
    tmp_path: Path, fast_cfg: LifecycleConfig, monkeypatch: pytest.MonkeyPatch, on_error: str
) -> None:
    """A release that does not reach the coordinator must not be forgotten: the
    grant it was for still stands, and dropping the record would bring the
    self-refusal back with nothing left to retry it. It fails like any other
    coordinator request (strict raises, degrade warns and the CAS is refused as
    ``other_holder`` — a typed signal, not a silent drop), and the next re-mint
    releases it and commits.

    The failing call runs on a worker with a bounded join, so a release made while
    holding the volume's identity lock — which deadlocks as soon as degrade mode
    records the failure, because that takes the same lock — fails here by name
    instead of hanging the run."""
    rel = "data/shared.txt"
    target = _seed(tmp_path, content=b"v1")
    vol = CoherentVolume(tmp_path, managed=("data/**",), on_error=on_error, config=fast_cfg)
    try:
        vol.write(rel, b"v2-pessimistic")
        writer_row = _agent_id(vol)
        _data, version = vol.read_with_version(rel)
        real_post = coherent_volume_module._coordinator_post
        failures = {"left": 1}

        def release_fails_once(endpoint: object, path: str, payload: dict) -> object:
            if path == "/hooks/session-stop" and failures["left"]:
                failures["left"] -= 1
                raise CoordinatorUnavailable("simulated: the release did not arrive")
            return real_post(endpoint, path, payload)

        monkeypatch.setattr(coherent_volume_module, "_coordinator_post", release_fails_once)
        outcome: dict[str, BaseException | None] = {}

        def attempt() -> None:
            try:
                vol.write_cas_at(rel, version, b"v3-cas")
                outcome["raised"] = None
            except BaseException as exc:  # handed to the test thread below
                outcome["raised"] = exc

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            worker = threading.Thread(target=attempt, daemon=True)
            worker.start()
            worker.join(timeout=30)
        if worker.is_alive():
            pytest.fail(
                "write_cas_at never returned after its release failed: the release "
                "is waiting on the volume's identity lock, which it already holds"
            )
        raised = outcome["raised"]
        assert failures["left"] == 0, "the simulated failure never fired"
        if on_error == "strict":
            assert isinstance(raised, CoherenceError), raised
            assert not isinstance(raised, CasVersionConflict), raised
            assert "session-stop" in str(raised)
        else:
            assert isinstance(raised, CasVersionConflict), raised
            assert raised.reason == "other_holder"
            assert any(issubclass(w.category, CoherenceDegradedWarning) for w in caught)
        assert target.read_bytes() == b"v2-pessimistic"
        assert _held(vol, writer_row) == {rel: "MODIFIED"}

        vol.write_cas_at(rel, version, b"v3-cas")

        assert target.read_bytes() == b"v3-cas"
        assert _held(vol, writer_row) == {}
    finally:
        stop_coordinator(tmp_path)


def test_after_fork_forgets_the_parents_write_grants(
    tmp_path: Path, fast_cfg: LifecycleConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A forked child starts with a copy of the grants the parent's write()
    recorded, but they belong to the parent's identity, which is still live in the
    parent. The child must release nothing — not in the fork handler, not at its
    first re-mint — and the parent's row must keep its grant. (Simulated in
    process, as the other fork tests are: the handler runs on the same object.)"""
    rel = "data/shared.txt"
    _seed(tmp_path, content=b"v1")
    vol = CoherentVolume(tmp_path, managed=("data/**",), config=fast_cfg)
    try:
        vol.write(rel, b"v2-parent")
        parent_row = _agent_id(vol)
        assert _held(vol, parent_row) == {rel: "MODIFIED"}, "precondition: the grant stands"
        sent: list[tuple[str, dict, str]] = []
        _count_stops(monkeypatch, sent, vol)

        vol._after_fork()  # the child's fork handler
        vol._ensure_attached()  # the child's first operation re-attaches ...
        vol._remint()  # ... and re-mints

        assert [r for r, _b, _c in sent if r == "/hooks/session-stop"] == []
        assert _held(vol, parent_row) == {rel: "MODIFIED"}
    finally:
        stop_coordinator(tmp_path)


@pytest.mark.skipif(not hasattr(os, "fork"), reason="requires os.fork (POSIX)")
def test_real_fork_child_releases_nothing_and_the_parent_keeps_its_grant(
    tmp_path: Path, fast_cfg: LifecycleConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The same property across a real ``os.fork()``: the registered fork handler
    runs in the child with the parent's endpoint still in hand, so a release there
    would reach the coordinator and revoke the grant the parent's in-flight write
    holds. The child reports how many releases it sent; the parent then checks its
    own grant on the coordinator."""
    rel = "data/shared.txt"
    _seed(tmp_path, content=b"v1")
    vol = CoherentVolume(tmp_path, managed=("data/**",), config=fast_cfg)
    try:
        vol.write(rel, b"v2-parent")
        parent_row = _agent_id(vol)
        assert _held(vol, parent_row) == {rel: "MODIFIED"}, "precondition: the grant stands"
        sent: list[tuple[str, dict, str]] = []
        _count_stops(monkeypatch, sent, vol)
        read_fd, write_fd = os.pipe()
        pid = os.fork()
        if pid == 0:  # child: the fork handler has already run
            os.close(read_fd)
            try:
                vol._remint()  # the child's first re-mint
                stops = sum(1 for r, _b, _c in sent if r == "/hooks/session-stop")
                os.write(write_fd, f"{stops}|{len(vol._grant_incarnations)}".encode())
            except BaseException as exc:  # report, never hang the parent
                os.write(write_fd, f"child raised {exc!r}".encode())
            finally:
                os.close(write_fd)
                os._exit(0)
        os.close(write_fd)
        ready, _w, _x = select.select([read_fd], [], [], 30)
        if not ready:
            os.kill(pid, signal.SIGKILL)
            os.waitpid(pid, 0)
            pytest.fail("timed out waiting for the forked child's release count")
        report = os.read(read_fd, 256).decode("utf-8")
        os.close(read_fd)
        os.waitpid(pid, 0)

        assert report == "0|0", f"child: releases sent | grants still recorded = {report}"
        assert _held(vol, parent_row) == {rel: "MODIFIED"}
    finally:
        stop_coordinator(tmp_path)
