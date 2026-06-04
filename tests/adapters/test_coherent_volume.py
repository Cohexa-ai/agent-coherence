# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""Unit 1 tests for CoherentVolume: spawn-with-strict, identity, fail-closed.

These exercise the façade scaffolding only (construction, strict-mode
enablement, per-instance + fork-safe identity, and the on_error contract).
The read/write contract (Unit 2) and the install() shim (Unit 3) are tested
separately.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from ccs.adapters.claude_code.lifecycle import (
    LifecycleConfig,
    ensure_coordinator,
    stop_coordinator,
)
from ccs.adapters.coherent_volume import CoherentVolume
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
