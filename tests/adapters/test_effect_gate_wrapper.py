# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Tests for the effect-ordering gate wrapper (``gate()``).

EO-8 regression: an escaping effect is HELD (never fires) when a gated input's
version advanced between capture and fire, and fires when the input is
unchanged. Uses a REAL coordinator subprocess (the ``test_coherent_volume.py``
pattern) with a FIXED-STALE BUFFER: the peer commit is driven explicitly inside
``decide()`` (between capture and re-validate), never a sleep.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from ccs.adapters.claude_code.lifecycle import LifecycleConfig, stop_coordinator
from ccs.adapters.coherent_volume import CoherentVolume
from ccs.adapters.effect_gate import gate
from ccs.core.exceptions import StaleView

REL = "data/config.txt"


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


def _seed(tmp_path: Path) -> None:
    (tmp_path / "data").mkdir(parents=True, exist_ok=True)


def test_gate_exported_from_adapters_package() -> None:
    """``gate`` is reachable from the package (the lazy ``__getattr__`` export)."""
    from ccs.adapters import gate as pkg_gate

    assert pkg_gate is gate


def test_escaping_effect_fires_when_unchanged(
    tmp_path: Path, fast_cfg: LifecycleConfig
) -> None:
    """Negative control: an unchanged input fires the escaping effect once and
    returns its result, so the HELD test proves the deny, not a vacuous pass."""
    _seed(tmp_path)
    vol = CoherentVolume(tmp_path, managed=("data/**",), config=fast_cfg)
    try:
        vol.write(REL, b"config-v1")
        fired: list[str] = []

        def decide(data: bytes) -> str:
            assert data == b"config-v1"
            return "deploy"

        def effect(decision: str) -> str:
            fired.append(decision)
            return f"{decision}-fired"

        result = gate(vol, REL, decide=decide, effect=effect)
        assert result == "deploy-fired"
        assert fired == ["deploy"]
    finally:
        stop_coordinator(tmp_path)


def test_escaping_effect_held_when_input_advanced(
    tmp_path: Path, fast_cfg: LifecycleConfig
) -> None:
    """EO-8: ``gate()`` HOLDs (raises ``StaleView``, the effect NEVER fires) when
    a peer advances the gated input's version between capture and fire. The peer
    commit is a fixed-stale buffer driven inside ``decide()``, not a sleep."""
    _seed(tmp_path)
    vol_a = CoherentVolume(tmp_path, managed=("data/**",), config=fast_cfg)
    try:
        vol_a.write(REL, b"config-v1")
        vol_b = CoherentVolume(tmp_path, managed=("data/**",), config=fast_cfg)
        fired: list[str] = []

        def decide(data: bytes) -> str:
            # FIXED-STALE BUFFER: a PEER advances the input between capture and
            # the gate's re-validate -- deterministic, not a timing race.
            vol_b.write(REL, b"config-v2-peer")
            return "deploy"

        def effect(decision: str) -> str:  # pragma: no cover - must never run
            fired.append(decision)
            return "should-not-fire"

        with pytest.raises(StaleView) as exc_info:
            gate(vol_a, REL, decide=decide, effect=effect)

        assert fired == []  # the effect never fired on stale input
        assert exc_info.value.expected_version != exc_info.value.current_version
    finally:
        stop_coordinator(tmp_path)


def test_effect_required(tmp_path: Path, fast_cfg: LifecycleConfig) -> None:
    """Omitting the ``effect`` callable is a caller error (required kw-only)."""
    _seed(tmp_path)
    vol = CoherentVolume(tmp_path, managed=("data/**",), config=fast_cfg)
    try:
        vol.write(REL, b"config-v1")
        with pytest.raises(TypeError):
            gate(vol, REL, decide=lambda d: "x")  # type: ignore[call-arg]
    finally:
        stop_coordinator(tmp_path)


def test_vanished_input_holds(tmp_path: Path, fast_cfg: LifecycleConfig) -> None:
    """If the gated input vanishes between capture and re-validate, the gate
    HOLDs fail-closed (cannot prove unchanged) and never fires."""
    _seed(tmp_path)
    vol = CoherentVolume(tmp_path, managed=("data/**",), config=fast_cfg)
    try:
        vol.write(REL, b"config-v1")
        fired: list[str] = []

        def decide(data: bytes) -> str:
            (tmp_path / REL).unlink()  # vanish between capture and re-validate
            return "deploy"

        def effect(decision: str) -> str:  # pragma: no cover - must never run
            fired.append(decision)
            return "x"

        with pytest.raises(StaleView):
            gate(vol, REL, decide=decide, effect=effect)
        assert fired == []
    finally:
        stop_coordinator(tmp_path)
