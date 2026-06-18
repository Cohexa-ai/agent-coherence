"""Unit 4 — swg_status is honest about the THREE states.

The load-bearing bug (SC5): never report ``unknown`` (coordinator unreachable)
as ``off`` (reachable, no strict patterns). A caller that reads ``off`` may write
unguarded; ``unknown`` must not collapse to that. The 3-state logic is tested as
a pure function over synthetic ``/status`` docs, plus one live ``on`` check.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from ccs.adapters.claude_code.lifecycle import LifecycleConfig, stop_coordinator
from ccs.mcp.session import SessionConfig
from ccs.mcp.status import _coordinator_state, _per_path, build_status


class _StubVolume:
    def __init__(self, attached: bool, degraded: bool = False, session: str = "sess-1") -> None:
        self.is_attached = attached
        self.is_degraded = degraded
        self.session_id = session


def _doc(count: int | None) -> dict:
    summary = {} if count is None else {"strict_mode_pattern_count": count}
    return {"policy_summary": summary}


def test_state_on_when_reachable_with_strict_patterns() -> None:
    assert _coordinator_state(_StubVolume(True), _doc(2)) == "on"


def test_state_off_when_reachable_with_no_strict_patterns() -> None:
    assert _coordinator_state(_StubVolume(True), _doc(0)) == "off"


def test_state_unknown_when_unreachable_never_off() -> None:
    """SC5: a None status doc (unreachable) is unknown, NOT off."""
    assert _coordinator_state(_StubVolume(True), None) == "unknown"


def test_state_unknown_when_unattached() -> None:
    assert _coordinator_state(_StubVolume(False), _doc(2)) == "unknown"


def test_state_unknown_when_count_missing() -> None:
    assert _coordinator_state(_StubVolume(True), _doc(None)) == "unknown"


def test_per_path_enforced_vs_not_registered() -> None:
    doc = {
        "tracked_artifacts": [
            {"path": "data/a.txt", "version": 3},
            {"path": "other/b.txt", "version": 1},
        ]
    }
    config = SessionConfig(root=Path("/x"), managed=("data/**",))
    per_path = _per_path(config, doc)
    assert per_path["data/a.txt"] == {"version": 3, "status": "enforced"}
    assert per_path["other/b.txt"] == {"version": 1, "status": "not_registered"}


def test_per_path_empty_when_no_status() -> None:
    assert _per_path(SessionConfig(root=Path("/x"), managed=("data/**",)), None) == {}


# --- live integration --------------------------------------------------------


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


def test_build_status_live_reports_on(tmp_path: Path, fast_cfg: LifecycleConfig) -> None:
    (tmp_path / "data").mkdir()
    (tmp_path / "data" / "shared.txt").write_bytes(b"v1")
    config = SessionConfig(root=tmp_path.resolve(), managed=("data/**",))
    # build_volume ignores the config's LifecycleConfig; construct directly for speed.
    from ccs.adapters.coherent_volume import CoherentVolume

    volume = CoherentVolume(tmp_path, managed=("data/**",), on_error="strict", config=fast_cfg)
    try:
        status = build_status(volume, config)
        assert status["coordinator"] == "on"
        assert status["is_attached"] is True
        assert status["is_degraded"] is False
        assert status["single_host_only"] is True
        assert status["heterogeneous_scope_detectable"] is False
        assert status["managed"] == ["data/**"]
    finally:
        stop_coordinator(tmp_path)
