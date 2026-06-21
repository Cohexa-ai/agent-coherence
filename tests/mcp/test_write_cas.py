"""Unit 5 — swg_write_cas (Option A): the concurrent single-host regime, honestly.

Option A is a SINGLE-SHOT version-checked CAS: commit IFF current == the agent's
expected_version, else a TYPED conflict (current_version returned) — never an
auto-merge, never a silent overwrite (the split-comparand lost update). The
per-session counter bounds a cooperating agent's retry loop.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from ccs.adapters.claude_code.lifecycle import LifecycleConfig, stop_coordinator
from ccs.adapters.coherent_volume import CoherentVolume
from ccs.core.exceptions import CasVersionConflict
from ccs.mcp.server import (
    _WRITE_CAS_DESC,
    MAX_CAS_CONFLICTS,
    _do_read,
    _do_write,
    _do_write_cas,
)
from ccs.mcp.session import SessionConfig


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


def _seed(tmp_path: Path, content: bytes = b"v1") -> Path:
    target = tmp_path / "data" / "shared.txt"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(content)
    return target


def _vol(tmp_path: Path, cfg: LifecycleConfig) -> CoherentVolume:
    return CoherentVolume(tmp_path, managed=("data/**",), on_error="strict", config=cfg)


def _config(tmp_path: Path) -> SessionConfig:
    return SessionConfig(root=tmp_path.resolve(), managed=("data/**",))


_PATH = "data/shared.txt"


# --- happy -------------------------------------------------------------------


def test_write_cas_commits_at_current_version(tmp_path: Path, fast_cfg: LifecycleConfig) -> None:
    target = _seed(tmp_path, b"v1")
    config = _config(tmp_path)
    vol = _vol(tmp_path, fast_cfg)
    try:
        version = _do_read(vol, config, _PATH).structuredContent["version"]
        result = _do_write_cas(vol, config, {}, _PATH, version, "v1-merged")
        assert result.isError is False
        assert target.read_bytes() == b"v1-merged"
    finally:
        stop_coordinator(tmp_path)


# --- stale expected_version → typed conflict, no write -----------------------


def test_write_cas_stale_version_is_typed_conflict(tmp_path: Path, fast_cfg: LifecycleConfig) -> None:
    target = _seed(tmp_path, b"v1")
    config = _config(tmp_path)
    vol_a = _vol(tmp_path, fast_cfg)
    vol_b = _vol(tmp_path, fast_cfg)
    try:
        version = _do_read(vol_a, config, _PATH).structuredContent["version"]
        # Peer B commits → current advances past A's read version.
        _do_read(vol_b, config, _PATH)
        assert _do_write(vol_b, config, _PATH, "v2-from-b").isError is False

        conflicts: dict[str, int] = {}
        result = _do_write_cas(vol_a, config, conflicts, _PATH, version, "stale-merge")
        assert result.isError is True
        sc = result.structuredContent
        assert sc["reason"] == "version_mismatch"
        assert sc["recover"] == "read_then_merge"
        assert sc["retryable"] is False
        assert sc["current_version"] > version  # the agent learns where to re-CAS
        assert target.read_bytes() == b"v2-from-b"  # A's stale CAS did NOT land
        assert conflicts[_PATH] == 1  # cooperating-agent counter ticked
    finally:
        stop_coordinator(tmp_path)


def test_write_cas_expected_zero_loses_cleanly(tmp_path: Path, fast_cfg: LifecycleConfig) -> None:
    target = _seed(tmp_path, b"v1")
    config = _config(tmp_path)
    vol = _vol(tmp_path, fast_cfg)
    try:
        version = _do_read(vol, config, _PATH).structuredContent["version"]
        assert version > 0
        result = _do_write_cas(vol, config, {}, _PATH, 0, "overwrite")
        assert result.isError is True
        assert result.structuredContent["reason"] == "version_mismatch"
        assert target.read_bytes() == b"v1"  # no silent overwrite
    finally:
        stop_coordinator(tmp_path)


# --- cooperating-agent counter exhaustion ------------------------------------


def test_write_cas_counter_exhausts_for_cooperating_agent(tmp_path: Path, fast_cfg: LifecycleConfig) -> None:
    _seed(tmp_path, b"v1")
    config = _config(tmp_path)
    vol_a = _vol(tmp_path, fast_cfg)
    vol_b = _vol(tmp_path, fast_cfg)
    try:
        stale = _do_read(vol_a, config, _PATH).structuredContent["version"]
        _do_read(vol_b, config, _PATH)
        _do_write(vol_b, config, _PATH, "advanced")  # current now != stale forever

        conflicts: dict[str, int] = {}
        # MAX_CAS_CONFLICTS conflicts return version_mismatch ...
        for _ in range(MAX_CAS_CONFLICTS):
            r = _do_write_cas(vol_a, config, conflicts, _PATH, stale, "x")
            assert r.structuredContent["reason"] == "version_mismatch"
        # ... the next one trips the cooperating-agent bound.
        exhausted = _do_write_cas(vol_a, config, conflicts, _PATH, stale, "x")
        assert exhausted.structuredContent["reason"] == "cas_exhausted"
        assert exhausted.structuredContent["retryable"] is False
    finally:
        stop_coordinator(tmp_path)


# --- no silent loss (the core property) --------------------------------------


def test_write_cas_two_writers_no_silent_loss(tmp_path: Path, fast_cfg: LifecycleConfig) -> None:
    """Two writers read the same version and both CAS at it: exactly one wins, the
    other is a typed version_mismatch, and the file holds the winner's content —
    never a silent lost update."""
    target = _seed(tmp_path, b"base")
    config = _config(tmp_path)
    vol_a = _vol(tmp_path, fast_cfg)
    vol_b = _vol(tmp_path, fast_cfg)
    try:
        va = _do_read(vol_a, config, _PATH).structuredContent["version"]
        vb = _do_read(vol_b, config, _PATH).structuredContent["version"]
        assert va == vb

        res_a = _do_write_cas(vol_a, config, {}, _PATH, va, "a-merged")
        res_b = _do_write_cas(vol_b, config, {}, _PATH, vb, "b-merged")

        wins = [r for r in (res_a, res_b) if not r.isError]
        conflicts = [r for r in (res_a, res_b) if r.isError]
        assert len(wins) == 1
        assert len(conflicts) == 1
        assert conflicts[0].structuredContent["reason"] == "version_mismatch"

        winner = b"a-merged" if not res_a.isError else b"b-merged"
        assert target.read_bytes() == winner
    finally:
        stop_coordinator(tmp_path)


# --- adapter primitive -------------------------------------------------------


def test_adapter_write_cas_at_raises_typed_conflict(tmp_path: Path, fast_cfg: LifecycleConfig) -> None:
    _seed(tmp_path, b"v1")
    vol_a = _vol(tmp_path, fast_cfg)
    vol_b = _vol(tmp_path, fast_cfg)
    try:
        _bytes, va = vol_a.read_with_version(_PATH)
        vol_b.read_with_version(_PATH)
        vol_b.write(_PATH, b"v2")  # advance current
        with pytest.raises(CasVersionConflict) as exc:
            vol_a.write_cas_at(_PATH, va, b"stale")
        assert exc.value.expected_version == va
        assert exc.value.current_version > va
    finally:
        stop_coordinator(tmp_path)


# --- honesty surface ---------------------------------------------------------


def test_write_cas_description_states_cooperating_caveat() -> None:
    assert "COOPERATING" in _WRITE_CAS_DESC
    assert "livelock-proof" in _WRITE_CAS_DESC
    assert "version_mismatch" in _WRITE_CAS_DESC
    assert "auto-merge" in _WRITE_CAS_DESC


# --- fail-closed + counter semantics -----------------------------------------


def test_write_cas_missing_file_is_file_not_found(tmp_path: Path, fast_cfg: LifecycleConfig) -> None:
    """A CAS on a non-existent file is a non-deny client error (file_not_found),
    not an escaped FileNotFoundError."""
    (tmp_path / "data").mkdir()
    config = _config(tmp_path)
    vol = _vol(tmp_path, fast_cfg)
    try:
        result = _do_write_cas(vol, config, {}, "data/nope.txt", 0, "x")
        assert result.isError is True
        assert result.structuredContent["reason"] == "file_not_found"
    finally:
        stop_coordinator(tmp_path)


def test_write_cas_counter_resets_after_a_win(tmp_path: Path, fast_cfg: LifecycleConfig) -> None:
    """A win mid-streak resets the per-path conflict counter — a subsequent
    conflict starts fresh at 1, not at the prior streak."""
    _seed(tmp_path, b"v1")
    config = _config(tmp_path)
    vol = _vol(tmp_path, fast_cfg)
    try:
        conflicts: dict[str, int] = {}
        # expected_version=0 always conflicts against a v>0 file.
        for _ in range(MAX_CAS_CONFLICTS - 1):
            assert _do_write_cas(vol, config, conflicts, _PATH, 0, "x").structuredContent["reason"] == "version_mismatch"
        assert conflicts[_PATH] == MAX_CAS_CONFLICTS - 1

        # A win resets the streak ...
        version = _do_read(vol, config, _PATH).structuredContent["version"]
        assert _do_write_cas(vol, config, conflicts, _PATH, version, "won").isError is False
        assert _PATH not in conflicts

        # ... so the next conflict starts at 1, not near the exhaustion bound.
        assert _do_write_cas(vol, config, conflicts, _PATH, 0, "x").structuredContent["reason"] == "version_mismatch"
        assert conflicts[_PATH] == 1
    finally:
        stop_coordinator(tmp_path)


# --- SB-23: foreign-edit-at-write via swg_write -------------------------------


def test_swg_write_denies_foreign_edit(tmp_path: Path, fast_cfg: LifecycleConfig) -> None:
    """SB-23 via swg_write: a plain write that would clobber a foreign / out-of-band
    edit (no peer commit — the disk changed OUTSIDE the coordinator) is the
    recoverable ``stale_view`` deny, not a silent overwrite."""
    target = _seed(tmp_path, b"v1")
    config = _config(tmp_path)
    vol = _vol(tmp_path, fast_cfg)
    try:
        _do_read(vol, config, _PATH)               # seeds the SB-23 baseline
        target.write_bytes(b"foreign-v2")          # out-of-band edit (not via the volume)
        denied = _do_write(vol, config, _PATH, "clobber")
        assert denied.isError is True
        sc = denied.structuredContent
        assert sc["reason"] == "stale_view"
        assert sc["recover"] == "reacquire"
        assert target.read_bytes() == b"foreign-v2"  # foreign edit NOT clobbered
    finally:
        stop_coordinator(tmp_path)
