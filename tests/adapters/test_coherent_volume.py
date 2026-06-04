# Copyright (c) 2026 Arbiter contributors.
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
import os
import subprocess
from pathlib import Path

import pytest

from ccs.adapters.claude_code.lifecycle import (
    LifecycleConfig,
    ensure_coordinator,
    stop_coordinator,
)
from ccs.adapters.coherent_volume import (
    CoherentVolume,
    coherent_workspace,
    install,
    uninstall,
)
from ccs.core.exceptions import CoherenceDegradedWarning, CoherenceError


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
    succeeds — no lost update."""
    target = _seed(tmp_path, content=b"v1")
    vol_a, vol_b = _pair(tmp_path, fast_cfg)
    try:
        vol_a.read("data/shared.txt")
        vol_b.read("data/shared.txt")
        vol_a.write("data/shared.txt", b"v2-from-A")  # B -> INVALID

        with pytest.raises(CoherenceError):
            vol_b.write("data/shared.txt", b"stale")  # denied

        old_session = vol_b.session_id
        fresh = vol_b.reacquire("data/shared.txt")
        assert fresh == b"v2-from-A"  # mandatory fresh read returns current bytes
        assert vol_b.session_id != old_session  # identity re-minted

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
