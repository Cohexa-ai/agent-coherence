# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents
"""CoherentVolume.atomic_publish — SB-18 all-or-nothing multi-artifact publish.

Real-coordinator two-writer tests (the ``_pair`` + fixed-stale-buffer shape from
test_coherent_volume.py). The load-bearing property: either every member of the
write-set lands or none does, and a torn intermediate is never on disk.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from ccs.adapters.claude_code.lifecycle import (
    LifecycleConfig,
    stop_coordinator,
)
from ccs.adapters.coherent_volume import CoherentVolume
from ccs.core.exceptions import (
    CasVersionConflict,
    CoherenceError,
    PublishMaterializationError,
    StaleView,
    ViewWedged,
)


@pytest.fixture
def fast_cfg() -> LifecycleConfig:
    return LifecycleConfig(
        idle_shutdown_sec=0,
        sweep_interval_sec=0.1,
        notice_evict_max_age_sec=1.0,
        port_file_retry_attempts=20,
        port_file_retry_interval_sec=0.05,
        connect_retry_attempts=10,
        connect_retry_interval_sec=0.05,
    )


def _seed(tmp_path: Path, rel: str, content: bytes) -> Path:
    target = tmp_path / rel
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(content)
    return target


def _pair(tmp_path: Path, cfg: LifecycleConfig) -> tuple[CoherentVolume, CoherentVolume]:
    vol_a = CoherentVolume(tmp_path, managed=("data/**",), config=cfg)
    vol_b = CoherentVolume(tmp_path, managed=("data/**",), config=cfg)
    return vol_a, vol_b


# ----------------------------------------------------------------------
# Happy path — every member lands atomically, versions returned
# ----------------------------------------------------------------------


def test_multi_publish_writes_every_member(tmp_path: Path, fast_cfg: LifecycleConfig) -> None:
    _seed(tmp_path, "data/a.txt", b"a-v1")
    _seed(tmp_path, "data/b.txt", b"b-v1")
    vol_a, _vol_b = _pair(tmp_path, fast_cfg)
    try:
        versions = vol_a.atomic_publish(
            [("data/a.txt", 1, b"a-v2"), ("data/b.txt", 1, b"b-v2")]
        )
        assert versions == {"data/a.txt": 2, "data/b.txt": 2}
        assert (tmp_path / "data/a.txt").read_bytes() == b"a-v2"
        assert (tmp_path / "data/b.txt").read_bytes() == b"b-v2"
    finally:
        stop_coordinator(tmp_path)


def test_single_publish_uses_standalone_and_returns_version(
    tmp_path: Path, fast_cfg: LifecycleConfig
) -> None:
    """A size-1 write-set takes the standalone CAS path (no session) and still
    returns the new version keyed by path."""
    _seed(tmp_path, "data/a.txt", b"a-v1")
    vol_a, _vol_b = _pair(tmp_path, fast_cfg)
    try:
        versions = vol_a.atomic_publish([("data/a.txt", 1, b"a-v2")])
        assert versions == {"data/a.txt": 2}
        assert (tmp_path / "data/a.txt").read_bytes() == b"a-v2"
    finally:
        stop_coordinator(tmp_path)


def test_str_and_bytes_content_both_publish(
    tmp_path: Path, fast_cfg: LifecycleConfig
) -> None:
    _seed(tmp_path, "data/a.txt", b"a-v1")
    _seed(tmp_path, "data/b.txt", b"b-v1")
    vol_a, _vol_b = _pair(tmp_path, fast_cfg)
    try:
        vol_a.atomic_publish([("data/a.txt", 1, "a-v2"), ("data/b.txt", 1, b"b-v2")])
        assert (tmp_path / "data/a.txt").read_bytes() == b"a-v2"
        assert (tmp_path / "data/b.txt").read_bytes() == b"b-v2"
    finally:
        stop_coordinator(tmp_path)


# ----------------------------------------------------------------------
# All-or-nothing — one moved member holds the WHOLE publish, zero mutation
# ----------------------------------------------------------------------


def test_one_moved_member_holds_whole_batch_zero_files_written(
    tmp_path: Path, fast_cfg: LifecycleConfig
) -> None:
    """A peer moves ONE member before the publish opens its session → the caller's
    view is stale at capture → the WHOLE publish is held and NEITHER file is
    written (the batch fixed-stale-buffer shape)."""
    _seed(tmp_path, "data/a.txt", b"a-v1")
    _seed(tmp_path, "data/b.txt", b"b-v1")
    vol_a, vol_b = _pair(tmp_path, fast_cfg)
    try:
        vol_b.write_cas_at("data/b.txt", 1, b"b-peer")  # b -> v2, a stays v1
        assert (tmp_path / "data/b.txt").read_bytes() == b"b-peer"

        with pytest.raises(CasVersionConflict):
            vol_a.atomic_publish(
                [("data/a.txt", 1, b"a-v2"), ("data/b.txt", 1, b"b-v2")]
            )
        # ALL-OR-NOTHING: the passing member a was NOT written; b holds the peer's.
        assert (tmp_path / "data/a.txt").read_bytes() == b"a-v1"
        assert (tmp_path / "data/b.txt").read_bytes() == b"b-peer"
    finally:
        stop_coordinator(tmp_path)


def test_peer_commit_inside_capture_commit_window_is_held(
    tmp_path: Path, fast_cfg: LifecycleConfig
) -> None:
    """The widened-window race: a peer commits a member AFTER the cut is pinned but
    BEFORE session/commit_all. The pinned comparand version-mismatches at the
    registry → HELD (StaleView), and NO file is written."""
    _seed(tmp_path, "data/a.txt", b"a-v1")
    _seed(tmp_path, "data/b.txt", b"b-v1")
    vol_a, vol_b = _pair(tmp_path, fast_cfg)
    try:
        real_post = vol_a._post

        def racing_post(endpoint_path: str, payload: dict):
            # Land a peer commit on b.txt in the window between begin (cut pinned)
            # and commit_all — the exact race the session path is exposed to.
            if endpoint_path == "/session/commit_all":
                vol_b.write_cas_at("data/b.txt", 1, b"b-peer-raced")
            return real_post(endpoint_path, payload)

        vol_a._post = racing_post
        with pytest.raises(StaleView):
            vol_a.atomic_publish(
                [("data/a.txt", 1, b"a-v2"), ("data/b.txt", 1, b"b-v2")]
            )
        # All-or-nothing: neither file written; b holds the peer's raced bytes.
        assert (tmp_path / "data/a.txt").read_bytes() == b"a-v1"
        assert (tmp_path / "data/b.txt").read_bytes() == b"b-peer-raced"
    finally:
        stop_coordinator(tmp_path)


def test_single_publish_stale_raises_cas_conflict(
    tmp_path: Path, fast_cfg: LifecycleConfig
) -> None:
    _seed(tmp_path, "data/a.txt", b"a-v1")
    vol_a, vol_b = _pair(tmp_path, fast_cfg)
    try:
        vol_b.write_cas_at("data/a.txt", 1, b"a-peer")  # a -> v2
        with pytest.raises(CasVersionConflict):
            vol_a.atomic_publish([("data/a.txt", 1, b"a-v2")])
        assert (tmp_path / "data/a.txt").read_bytes() == b"a-peer"  # not clobbered
    finally:
        stop_coordinator(tmp_path)


# ----------------------------------------------------------------------
# Recovery — reacquire + re-read fresh versions, then re-publish succeeds
# ----------------------------------------------------------------------


def test_publish_recovers_after_hold(tmp_path: Path, fast_cfg: LifecycleConfig) -> None:
    _seed(tmp_path, "data/a.txt", b"a-v1")
    _seed(tmp_path, "data/b.txt", b"b-v1")
    vol_a, vol_b = _pair(tmp_path, fast_cfg)
    try:
        vol_b.write_cas_at("data/b.txt", 1, b"b-peer")  # b -> v2
        with pytest.raises(CasVersionConflict):
            vol_a.atomic_publish(
                [("data/a.txt", 1, b"a-v2"), ("data/b.txt", 1, b"b-v2")]
            )
        # Recover: re-read the fresh versions, then re-publish against them.
        _a_bytes, a_ver = vol_a.read_with_version("data/a.txt")
        _b_bytes, b_ver = vol_a.read_with_version("data/b.txt")
        versions = vol_a.atomic_publish(
            [("data/a.txt", a_ver, b"a-final"), ("data/b.txt", b_ver, b"b-final")]
        )
        assert versions == {"data/a.txt": a_ver + 1, "data/b.txt": b_ver + 1}
        assert (tmp_path / "data/a.txt").read_bytes() == b"a-final"
        assert (tmp_path / "data/b.txt").read_bytes() == b"b-final"
    finally:
        stop_coordinator(tmp_path)


# ----------------------------------------------------------------------
# Input validation — fail loud before any coordinator I/O
# ----------------------------------------------------------------------


def test_empty_write_set_raises(tmp_path: Path, fast_cfg: LifecycleConfig) -> None:
    vol = CoherentVolume(tmp_path, managed=("data/**",), config=fast_cfg)
    try:
        with pytest.raises(ValueError):
            vol.atomic_publish([])
    finally:
        stop_coordinator(tmp_path)


def test_duplicate_path_raises(tmp_path: Path, fast_cfg: LifecycleConfig) -> None:
    vol = CoherentVolume(tmp_path, managed=("data/**",), config=fast_cfg)
    try:
        with pytest.raises(ValueError):
            vol.atomic_publish([("data/a.txt", 1, b"x"), ("data/a.txt", 1, b"y")])
    finally:
        stop_coordinator(tmp_path)


def test_same_bytes_member_is_still_written_and_bumped(
    tmp_path: Path, fast_cfg: LifecycleConfig
) -> None:
    """A member whose new bytes equal its current bytes is NOT skipped — it is
    still written and its version bumped, so disk never diverges from the
    coordinator's recorded hash."""
    _seed(tmp_path, "data/a.txt", b"a-v1")
    _seed(tmp_path, "data/b.txt", b"b-v1")
    vol_a, _vol_b = _pair(tmp_path, fast_cfg)
    try:
        versions = vol_a.atomic_publish(
            [("data/a.txt", 1, b"a-v2"), ("data/b.txt", 1, b"b-v1")]  # b unchanged
        )
        assert versions == {"data/a.txt": 2, "data/b.txt": 2}
        assert (tmp_path / "data/b.txt").read_bytes() == b"b-v1"
    finally:
        stop_coordinator(tmp_path)


def test_replace_failure_on_second_member_is_typed_and_names_landed(
    tmp_path: Path, fast_cfg: LifecycleConfig
) -> None:
    """A disk fault on the SECOND member's os.replace (after the coordinator has
    already committed the whole batch) raises a TYPED PublishMaterializationError
    naming exactly which members landed vs not — never a bare OSError implying
    nothing published — and the op guard is released."""
    _seed(tmp_path, "data/a.txt", b"a-v1")
    _seed(tmp_path, "data/b.txt", b"b-v1")
    vol_a, _vol_b = _pair(tmp_path, fast_cfg)
    try:
        real_replace = vol_a._replace_tmp
        calls = {"n": 0}

        def boom(tmp, abs_path):
            calls["n"] += 1
            if calls["n"] == 2:  # a lands, b's rename faults
                raise OSError("injected disk-full on the second rename")
            return real_replace(tmp, abs_path)

        vol_a._replace_tmp = boom
        with pytest.raises(PublishMaterializationError) as exc:
            vol_a.atomic_publish(
                [("data/a.txt", 1, b"a-v2"), ("data/b.txt", 1, b"b-v2")]
            )
        # The typed error names the torn set: a landed, b did not.
        assert exc.value.landed == ("data/a.txt",)
        assert exc.value.not_landed == ("data/b.txt",)
        # On-disk state matches: a has the new bytes, b still holds its old bytes.
        assert (tmp_path / "data/a.txt").read_bytes() == b"a-v2"
        assert (tmp_path / "data/b.txt").read_bytes() == b"b-v1"
        # Guard released despite the raise — a later op is not wedged.
        vol_a._replace_tmp = real_replace
        _bytes, version = vol_a.read_with_version("data/a.txt")
        assert isinstance(version, int)
    finally:
        stop_coordinator(tmp_path)


def test_staging_failure_leaves_disk_uniformly_old(
    tmp_path: Path, fast_cfg: LifecycleConfig
) -> None:
    """A disk fault while STAGING a later member (before any os.replace) leaves NO
    member replaced — disk is uniformly old (coordinator ahead of disk, not torn)
    — and raises PublishMaterializationError with landed empty."""
    _seed(tmp_path, "data/a.txt", b"a-v1")
    _seed(tmp_path, "data/b.txt", b"b-v1")
    vol_a, _vol_b = _pair(tmp_path, fast_cfg)
    try:
        real_stage = vol_a._stage_tmp
        calls = {"n": 0}

        def boom(abs_path, data):
            calls["n"] += 1
            if calls["n"] == 2:  # a stages fine, b's staging faults
                raise OSError("injected disk-full while staging the second member")
            return real_stage(abs_path, data)

        vol_a._stage_tmp = boom
        with pytest.raises(PublishMaterializationError) as exc:
            vol_a.atomic_publish(
                [("data/a.txt", 1, b"a-v2"), ("data/b.txt", 1, b"b-v2")]
            )
        assert exc.value.landed == ()
        assert set(exc.value.not_landed) == {"data/a.txt", "data/b.txt"}
        # NEITHER file replaced — both hold their old bytes.
        assert (tmp_path / "data/a.txt").read_bytes() == b"a-v1"
        assert (tmp_path / "data/b.txt").read_bytes() == b"b-v1"
        vol_a._stage_tmp = real_stage
        _bytes, version = vol_a.read_with_version("data/b.txt")
        assert isinstance(version, int)
    finally:
        stop_coordinator(tmp_path)


def test_multi_non_utf8_member_raises_before_io(
    tmp_path: Path, fast_cfg: LifecycleConfig
) -> None:
    _seed(tmp_path, "data/a.txt", b"a-v1")
    _seed(tmp_path, "data/b.txt", b"b-v1")
    vol = CoherentVolume(tmp_path, managed=("data/**",), config=fast_cfg)
    try:
        with pytest.raises(ValueError):
            vol.atomic_publish(
                [("data/a.txt", 1, b"ok"), ("data/b.txt", 1, b"\xff\xfe")]
            )
        # Failed before any coordinator I/O → nothing on disk changed.
        assert (tmp_path / "data/a.txt").read_bytes() == b"a-v1"
        assert (tmp_path / "data/b.txt").read_bytes() == b"b-v1"
    finally:
        stop_coordinator(tmp_path)


def test_corruption_reason_is_non_retryable_not_staleview(
    tmp_path: Path, fast_cfg: LifecycleConfig
) -> None:
    """A corruption reason from commit_all maps to a NON-retryable CoherenceError,
    NOT a retryable StaleView (mirrors the size-1 CAS path). Corruption is
    unreachable via the pinned cut, so the wire body is injected directly to
    exercise the client's error-typing dispatch (defense-in-depth)."""
    _seed(tmp_path, "data/a.txt", b"a-v1")
    _seed(tmp_path, "data/b.txt", b"b-v1")
    vol_a, _vol_b = _pair(tmp_path, fast_cfg)
    try:
        real_post = vol_a._post

        def synth(endpoint_path, payload):
            if endpoint_path == "/session/commit_all":
                return {"ok": False, "reason": "commit_all_corruption agent=x current_version=9"}
            return real_post(endpoint_path, payload)

        vol_a._post = synth
        with pytest.raises(CoherenceError) as exc:
            vol_a.atomic_publish(
                [("data/a.txt", 1, b"a-v2"), ("data/b.txt", 1, b"b-v2")]
            )
        # Non-retryable: the plain CoherenceError, never the retryable StaleView.
        assert not isinstance(exc.value, StaleView)
        # Nothing written on disk.
        assert (tmp_path / "data/a.txt").read_bytes() == b"a-v1"
        assert (tmp_path / "data/b.txt").read_bytes() == b"b-v1"
    finally:
        stop_coordinator(tmp_path)


# ----------------------------------------------------------------------
# Foreign-edit boundary (SB-18 × SB-23) — pinned, deliberately asymmetric
# ----------------------------------------------------------------------
#
# atomic_publish is VERSION-OCC against the coordinator, and only volume-mediated
# writes advance versions. The multi-member session path never re-reads disk
# between the caller's read and materialization, so an out-of-band edit in that
# window cannot version-mismatch and is NOT content-checked — the SB-23 predicate
# (_check_foreign_edit) does not run there, even though _read_with_version seeds
# its baseline. This is the DOCUMENTED boundary (the API is for volume-mediated
# writer sets only — see the docstring's foreign-edit paragraph and the guide's
# scope note), not an accident. These tests pin both publish surfaces so any
# change to the boundary is a deliberate, test-flipping decision; write()'s and
# write_cas's fail-closed contrast is pinned in test_coherent_volume.py
# (test_write_denies_foreign_edit_by_default,
# test_write_cas_on_foreign_edit_wedges_not_stale_view).


def test_multi_publish_overwrites_foreign_edit_documented_boundary(
    tmp_path: Path, fast_cfg: LifecycleConfig
) -> None:
    """An out-of-band disk edit between read_with_version and a MULTI-member
    publish advances no version, so the batch commits and overwrites it — the
    seeded SB-23 baseline is not consulted on this path. Flipping this behavior
    means flipping this test WITH the docstring/guide/README scope notes."""
    _seed(tmp_path, "data/a.txt", b"a-v1")
    _seed(tmp_path, "data/b.txt", b"b-v1")
    vol = CoherentVolume(tmp_path, managed=("data/**",), config=fast_cfg)
    try:
        _, version_a = vol.read_with_version("data/a.txt")
        _, version_b = vol.read_with_version("data/b.txt")  # seeds the baseline
        # Straight to disk: no volume, no version advance — a hand edit.
        (tmp_path / "data/b.txt").write_bytes(b"b-FOREIGN")

        versions = vol.atomic_publish(
            [("data/a.txt", version_a, b"a-v2"), ("data/b.txt", version_b, b"b-v2")]
        )

        assert versions == {
            "data/a.txt": version_a + 1,
            "data/b.txt": version_b + 1,
        }
        # The foreign bytes are gone — the documented cost of the version-OCC
        # boundary on the session path.
        assert (tmp_path / "data/b.txt").read_bytes() == b"b-v2"
    finally:
        stop_coordinator(tmp_path)


def test_single_publish_fails_closed_on_foreign_edit(
    tmp_path: Path, fast_cfg: LifecycleConfig
) -> None:
    """The SAME edit against a SINGLE-member publish fails closed: the standalone
    CAS path's comparand read is hash-checked under a re-minted identity, so the
    foreign bytes deny the read and the publish wedges instead of clobbering.
    Pins the intra-API asymmetry the boundary comment above documents."""
    _seed(tmp_path, "data/a.txt", b"a-v1")
    vol = CoherentVolume(tmp_path, managed=("data/**",), config=fast_cfg)
    try:
        _, version_a = vol.read_with_version("data/a.txt")
        (tmp_path / "data/a.txt").write_bytes(b"a-FOREIGN")

        with pytest.raises(ViewWedged):
            vol.atomic_publish([("data/a.txt", version_a, b"a-v2")])

        # Fail-closed: the foreign edit survives, nothing was published.
        assert (tmp_path / "data/a.txt").read_bytes() == b"a-FOREIGN"
    finally:
        stop_coordinator(tmp_path)
