"""Unit 4 — swg_read / swg_write / swg_reacquire against a real coordinator.

Tests drive the sync ``_do_*`` helpers (the tool logic) with real
``CoherentVolume`` instances + a real loopback coordinator, so the contract is
exercised end-to-end without a FastMCP client. The headline is the sequential
deny→reacquire→write loop; the rest is fail-closed + edge coverage + the honesty
surface (SC4).
"""

from __future__ import annotations

from pathlib import Path

import pytest

from ccs.adapters.claude_code.lifecycle import LifecycleConfig, stop_coordinator
from ccs.adapters.coherent_volume import CoherentVolume
from ccs.mcp.server import (
    _READ_DESC,
    _STATUS_DESC,
    _WRITE_DESC,
    INSTRUCTIONS,
    _do_reacquire,
    _do_read,
    _do_write,
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


def _seed(tmp_path: Path, rel: str = "data/shared.txt", content: bytes = b"v1") -> Path:
    target = tmp_path / rel
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(content)
    return target


def _vol(tmp_path: Path, cfg: LifecycleConfig) -> CoherentVolume:
    return CoherentVolume(tmp_path, managed=("data/**",), on_error="strict", config=cfg)


def _config(tmp_path: Path) -> SessionConfig:
    return SessionConfig(root=tmp_path.resolve(), managed=("data/**",))


# --- happy + the headline sequential loop ------------------------------------


def test_read_then_write_happy(tmp_path: Path, fast_cfg: LifecycleConfig) -> None:
    target = _seed(tmp_path, content=b"v1")
    config = _config(tmp_path)
    vol = _vol(tmp_path, fast_cfg)
    try:
        read = _do_read(vol, config, "data/shared.txt")
        assert read.isError is False
        assert read.structuredContent["content"] == "v1"
        assert isinstance(read.structuredContent["version"], int)

        wrote = _do_write(vol, config, "data/shared.txt", "v2")
        assert wrote.isError is False
        assert target.read_bytes() == b"v2"
    finally:
        stop_coordinator(tmp_path)


def test_sequential_deny_reacquire_write_loop(tmp_path: Path, fast_cfg: LifecycleConfig) -> None:
    """read v → peer commits v+1 → swg_write(stale_view) → swg_reacquire(v+1) →
    swg_write(from those bytes) ok. The lost update is denied, not silent."""
    target = _seed(tmp_path, content=b"v1")
    config = _config(tmp_path)
    vol_a = _vol(tmp_path, fast_cfg)
    vol_b = _vol(tmp_path, fast_cfg)
    try:
        assert _do_read(vol_a, config, "data/shared.txt").structuredContent["content"] == "v1"

        # Peer B commits v2 → A goes INVALID.
        _do_read(vol_b, config, "data/shared.txt")
        assert _do_write(vol_b, config, "data/shared.txt", "v2-from-b").isError is False

        # A's stale write is DENIED with the recoverable stale_view terminal.
        denied = _do_write(vol_a, config, "data/shared.txt", "v2-from-a")
        assert denied.isError is True
        assert denied.structuredContent["reason"] == "stale_view"
        assert denied.structuredContent["recover"] == "reacquire"
        assert denied.structuredContent["retryable"] is True
        assert target.read_bytes() == b"v2-from-b"  # A's stale write did NOT land

        # A reacquires → fresh bytes, then writes FROM them → ok.
        reacq = _do_reacquire(vol_a, config, "data/shared.txt")
        assert reacq.isError is False
        assert reacq.structuredContent["content"] == "v2-from-b"
        assert "version lineage" in reacq.structuredContent["note"]

        wrote = _do_write(vol_a, config, "data/shared.txt", "v3-from-a")
        assert wrote.isError is False
        assert target.read_bytes() == b"v3-from-a"
    finally:
        stop_coordinator(tmp_path)


# --- fail-closed -------------------------------------------------------------


def test_write_unattached_fails_closed_no_disk_write(tmp_path: Path, fast_cfg: LifecycleConfig) -> None:
    """A mid-session endpoint loss must fail closed BEFORE the adapter's
    best-effort unversioned write — coordinator_unavailable, disk untouched."""
    target = _seed(tmp_path, content=b"v1")
    config = _config(tmp_path)
    vol = _vol(tmp_path, fast_cfg)
    try:
        vol._endpoint = None  # simulate a post-construction endpoint loss
        result = _do_write(vol, config, "data/shared.txt", "should-not-land")
        assert result.isError is True
        assert result.structuredContent["reason"] == "coordinator_unavailable"
        assert target.read_bytes() == b"v1"  # NO disk write
    finally:
        stop_coordinator(tmp_path)


# --- edges -------------------------------------------------------------------


def test_read_invalid_path_is_non_deny_error(tmp_path: Path, fast_cfg: LifecycleConfig) -> None:
    config = _config(tmp_path)
    vol = _vol(tmp_path, fast_cfg)
    try:
        result = _do_read(vol, config, "../escape")
        assert result.isError is True
        assert result.structuredContent["reason"] == "invalid_path"
        assert result.structuredContent["reason"] != "stale_view"
    finally:
        stop_coordinator(tmp_path)


def test_read_missing_file_is_not_stale_view(tmp_path: Path, fast_cfg: LifecycleConfig) -> None:
    _seed(tmp_path, content=b"v1")  # creates data/ but not nope.txt
    config = _config(tmp_path)
    vol = _vol(tmp_path, fast_cfg)
    try:
        result = _do_read(vol, config, "data/nope.txt")
        assert result.isError is True
        assert result.structuredContent["reason"] == "file_not_found"
        assert result.structuredContent["reason"] != "stale_view"
    finally:
        stop_coordinator(tmp_path)


def test_read_empty_file_is_valid_view(tmp_path: Path, fast_cfg: LifecycleConfig) -> None:
    _seed(tmp_path, rel="data/empty.txt", content=b"")
    config = _config(tmp_path)
    vol = _vol(tmp_path, fast_cfg)
    try:
        result = _do_read(vol, config, "data/empty.txt")
        assert result.isError is False
        assert result.structuredContent["content"] == ""
    finally:
        stop_coordinator(tmp_path)


def test_read_binary_file_unsupported(tmp_path: Path, fast_cfg: LifecycleConfig) -> None:
    _seed(tmp_path, rel="data/bin.dat", content=b"\xff\xfe\x00\x01")
    config = _config(tmp_path)
    vol = _vol(tmp_path, fast_cfg)
    try:
        result = _do_read(vol, config, "data/bin.dat")
        assert result.isError is True
        assert result.structuredContent["reason"] == "binary_unsupported"
    finally:
        stop_coordinator(tmp_path)


# --- honesty surface (SC4) ---------------------------------------------------


def test_tool_descriptions_state_the_scope() -> None:
    for desc in (_READ_DESC, _WRITE_DESC, _STATUS_DESC):
        assert "SINGLE-HOST" in desc
        assert "auto-merge" in desc
    assert "heterogeneous_scope_detectable=false" in _STATUS_DESC


def test_instructions_state_forbidden_and_trust_boundary() -> None:
    text = INSTRUCTIONS.lower()
    assert "different hosts" in text
    assert "auto-merge" in text
    assert "trust boundary" in text
    assert "single-uid" in text
