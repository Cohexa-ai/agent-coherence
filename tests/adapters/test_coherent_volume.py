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
import threading
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
from ccs.core.exceptions import (
    CasRetriesExhausted,
    CoherenceDegradedWarning,
    CoherenceError,
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
    from ccs.cli._coherence_client import post as _cpost

    target = _seed(tmp_path, content=b"v1")
    vol_a, vol_b = _pair(tmp_path, fast_cfg)
    try:
        vol_a.read("data/shared.txt")
        vol_b.read("data/shared.txt")  # B is SHARED@v1

        vol_a.write("data/shared.txt", b"v2-from-A")  # B → INVALID, version → 2
        assert target.read_bytes() == b"v2-from-A"
        # A's turn ends — release its grant so the OCC writer is not blocked by
        # other_holder against A's lingering MODIFIED.
        _cpost(vol_a._endpoint, "/hooks/session-stop", {"session_id": vol_a.session_id})

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
            # (bytes, version, stale_denied) — not stale, so no reacquire.
            vol._read_with_version = lambda rel: (b"v1", 999, False)  # type: ignore[assignment]
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
    from ccs.cli._coherence_client import post as _cpost

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
        _cpost(vol_a._endpoint, "/hooks/session-stop", {"session_id": vol_a.session_id})

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
# coordinator-wedged guard. Distinct from the conflict-classify convergence
# above: here B is INVALID at CAS time, so the stale-deny branch (NOT a
# version_mismatch conflict) drives the reacquire.
# ---------------------------------------------------------------------------


def test_write_cas_recovers_from_sticky_strict_deny_and_converges(
    tmp_path: Path, fast_cfg: LifecycleConfig
) -> None:
    """T2: a peer commit leaves THIS volume INVALID (sticky strict-deny). The
    very first OCC read inside write_cas is a strict-deny (stale_denied), so
    write_cas must reacquire() (re-mint identity → clears INVALID + the
    invalidation transient) and commit the rebased bytes — converge, NOT raise.
    No lost update: B's commit is rebased on A's v2."""
    from ccs.cli._coherence_client import post as _cpost

    target = _seed(tmp_path, content=b"v1")
    vol_a, vol_b = _pair(tmp_path, fast_cfg)
    try:
        vol_a.read("data/shared.txt")
        vol_b.read("data/shared.txt")  # B SHARED@v1
        vol_a.write("data/shared.txt", b"v2-from-A")  # B -> INVALID (sticky deny)
        # A's turn ends — release its grant so B's OCC commit is not blocked by
        # other_holder against A's lingering MODIFIED.
        _cpost(vol_a._endpoint, "/hooks/session-stop", {"session_id": vol_a.session_id})

        # Confirm B really is in the sticky-deny state BEFORE write_cas: a bare
        # version-aware read reports stale_denied=True (INVALID, not re-granted).
        _bytes, _ver, stale_denied = vol_b._read_with_version("data/shared.txt")
        assert stale_denied is True, "precondition: B must be a sticky strict-deny"

        seen: list[bytes] = []

        def make(current: bytes) -> bytes:
            seen.append(current)
            return current + b"\nrebased-by-B"

        # write_cas drives the stale-deny branch → reacquire → fresh read → CAS.
        vol_b.write_cas("data/shared.txt", make)
        assert target.read_bytes() == b"v2-from-A\nrebased-by-B"
        # The winning attempt derived from A's v2 (re-read via reacquire), never
        # the stale v1 buffer.
        assert b"v2-from-A" in seen[-1]
        assert not vol_b.is_degraded  # a deny/recovery is enforcement, not infra
    finally:
        stop_coordinator(tmp_path)


def test_write_cas_raises_when_reacquire_cannot_clear_invalid(
    tmp_path: Path, fast_cfg: LifecycleConfig
) -> None:
    """T2: the coordinator-wedged guard. If a fresh-minted identity STILL reads
    as a strict-deny (impossible in normal operation — a brand-new identity has
    no prior INVALID), write_cas must fail closed with a clear CoherenceError and
    NOT spin forever. Simulated by stubbing _read_with_version to report
    stale_denied on both the fresh read AND the post-reacquire re-read."""
    _seed(tmp_path, content=b"v1")
    vol = CoherentVolume(tmp_path, managed=("data/**",), config=fast_cfg)
    try:
        assert vol.read("data/shared.txt") == b"v1"
        calls = {"n": 0}

        def always_denied(rel: str):
            # (bytes, version, stale_denied) — always denied → the fresh read is
            # denied AND the re-read after reacquire is denied (refused_again).
            calls["n"] += 1
            return (b"v1", 1, True)

        vol._read_with_version = always_denied  # type: ignore[assignment]

        made = {"called": False}

        def make(_cur: bytes) -> bytes:
            made["called"] = True
            return b"should-not-commit"

        with pytest.raises(CoherenceError, match="wedged"):
            vol.write_cas("data/shared.txt", make)
        # No spin: fresh read (1) + post-reacquire re-read (2) = exactly 2 calls,
        # then it raises before ever deriving/committing bytes.
        assert calls["n"] == 2
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
    original OSError propagates."""
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
