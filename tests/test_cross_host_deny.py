"""Unit 5 (R3): slice-1 cross-host deny + recover.

This loopback integration test proves the full chain end-to-end on a real socket
(runnable on any platform): two remote-endpoint clients (never-spawn) coordinate
ONE coordinator; a stale write is denied by version-CAS *across the endpoint* and
recovers via re-read + retry. The genuinely-non-loopback path (a real RFC-1918
bind exercising verify_host's new branch) is the netns/veth runner in
examples/cross_host/ (Linux-only); that Host-check logic is unit-covered in
tests/test_claude_code_bind_validation.py.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from ccs.adapters.claude_code.lifecycle import (
    LifecycleConfig,
    ensure_coordinator,
    stop_coordinator,
)
from ccs.adapters.coherent_volume import CoherentVolume
from ccs.cli._coherence_client import (
    post as _cc_post,
)
from ccs.cli._coherence_client import (
    resolve_endpoint,
    resolve_remote_endpoint,
)
from ccs.core.exceptions import CasVersionConflict


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


def test_slice1_cross_endpoint_deny_and_recover(
    tmp_path: Path, fast_cfg: LifecycleConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("CCS_REMOTE_COORDINATOR", "1")
    coord_root = tmp_path / "coord"
    coord_root.mkdir()
    root_a = tmp_path / "a"
    root_a.mkdir()
    root_b = tmp_path / "b"
    root_b.mkdir()

    ensure_coordinator(coord_root, config=fast_cfg)
    try:
        ep = resolve_endpoint(coord_root)
        # Coordinator-version-only: the artifact must be TRACKED on the coordinator
        # to be versioned (an untracked path is never versioned -> no CAS). Tracking
        # is separate from STRICT mode — it gives versioning; strict would add
        # read-deny enforcement (not needed for the version-CAS write deny).
        _cc_post(ep, "/policy/track", {"paths": ["shared.txt"]})

        def remote():
            return resolve_remote_endpoint("127.0.0.1", ep.port, ep.bearer)

        vol_a = CoherentVolume(
            root_a, on_error="strict", on_stale_write="allow", remote_endpoint=remote()
        )
        vol_b = CoherentVolume(
            root_b, on_error="strict", on_stale_write="allow", remote_endpoint=remote()
        )
        # Coordinator-version-only: per-root file copies; coordination is the
        # coordinator's version of the shared key.
        (root_a / "shared.txt").write_text("v0", encoding="utf-8")
        (root_b / "shared.txt").write_text("v0", encoding="utf-8")

        # A reads the shared key -> version v_a (registers SHARED on the coordinator).
        _data_a, v_a = vol_a.read_with_version("shared.txt")

        # B reads + commits -> the coordinator version advances past v_a.
        _data_b, v_b = vol_b.read_with_version("shared.txt")
        vol_b.write_cas_at("shared.txt", v_b, b"from-b")

        # A's write against its now-stale version is DENIED across the endpoint
        # (the typed version-CAS conflict, NOT just any CoherenceError — pin the
        # exact type and the carried versions so the deny can't pass on an
        # unrelated failure).
        with pytest.raises(CasVersionConflict) as deny:
            vol_a.write_cas_at("shared.txt", v_a, b"from-a")
        assert deny.value.expected_version == v_a
        assert deny.value.current_version > v_a

        # A recovers: re-read -> fresh version -> retry succeeds (no silent loss).
        _data_a2, v_a2 = vol_a.read_with_version("shared.txt")
        assert v_a2 > v_a
        vol_a.write_cas_at("shared.txt", v_a2, b"from-a-2")
        assert (root_a / "shared.txt").read_bytes() == b"from-a-2"
    finally:
        stop_coordinator(coord_root)


def test_slice2_effect_gate_across_hosts(
    tmp_path: Path, fast_cfg: LifecycleConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Slice-2 (R4): an effect gated on config@vN FIRES when config is unchanged
    and is HELD when config advanced under it — across the endpoint, on the
    shipped read_with_version primitive."""
    monkeypatch.setenv("CCS_REMOTE_COORDINATOR", "1")
    coord_root = tmp_path / "coord"
    coord_root.mkdir()
    root_a = tmp_path / "a"
    root_a.mkdir()
    root_b = tmp_path / "b"
    root_b.mkdir()

    ensure_coordinator(coord_root, config=fast_cfg)
    try:
        ep = resolve_endpoint(coord_root)
        _cc_post(ep, "/policy/track", {"paths": ["config.json"]})

        def remote():
            return resolve_remote_endpoint("127.0.0.1", ep.port, ep.bearer)

        vol_a = CoherentVolume(
            root_a, on_error="strict", on_stale_write="allow", remote_endpoint=remote()
        )
        vol_b = CoherentVolume(
            root_b, on_error="strict", on_stale_write="allow", remote_endpoint=remote()
        )
        (root_a / "config.json").write_text('{"v": 0}', encoding="utf-8")
        (root_b / "config.json").write_text('{"v": 0}', encoding="utf-8")

        # A reads config -> the version its effect decision depends on.
        _c, v_decision = vol_a.read_with_version("config.json")

        def effect_fires(decision_version: int) -> bool:
            """Gate the effect on config still being at the decision version."""
            _cur, current = vol_a.read_with_version("config.json")
            return current == decision_version

        # Unchanged config -> the gate FIRES.
        assert effect_fires(v_decision) is True

        # B advances config under A.
        _cb, v_b = vol_b.read_with_version("config.json")
        vol_b.write_cas_at("config.json", v_b, b'{"v": 1}')

        # Config moved -> the gate HOLDS the effect (not fired on stale input).
        assert effect_fires(v_decision) is False
    finally:
        stop_coordinator(coord_root)
