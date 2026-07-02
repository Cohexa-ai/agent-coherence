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
from ccs.core.exceptions import CoherenceDegradedWarning, CoherenceError, StaleView

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
        # the peer's single write advanced the version by exactly one (monotonic),
        # so pin the specific drift, not merely that the two versions differ.
        assert exc_info.value.current_version == exc_info.value.expected_version + 1
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


def test_gate_holds_end_to_end_when_coordinator_degrades(
    tmp_path: Path, fast_cfg: LifecycleConfig
) -> None:
    """End-to-end degrade: a real coordinator taken down between capture and
    re-validate surfaces version 0 (degrade mode, fail-closed), so the gate
    HOLDs and the escaping effect never fires on an unconfirmed input."""
    _seed(tmp_path)
    vol = CoherentVolume(
        tmp_path, managed=("data/**",), on_error="degrade", config=fast_cfg
    )
    try:
        vol.write(REL, b"config-v1")
        fired: list[str] = []

        def decide(data: bytes) -> str:
            stop_coordinator(tmp_path)  # coordinator down before the re-validate read
            return "deploy"

        def effect(decision: str) -> str:  # pragma: no cover - must never run
            fired.append(decision)
            return "should-not-fire"

        with pytest.warns(CoherenceDegradedWarning), pytest.raises(StaleView):
            gate(vol, REL, decide=decide, effect=effect)
        assert fired == []
    finally:
        stop_coordinator(tmp_path)


def test_gate_propagates_infra_error_in_strict_mode(
    tmp_path: Path, fast_cfg: LifecycleConfig
) -> None:
    """Strict mode: an infra failure during the re-validate read raises a raw
    CoherenceError (NOT a StaleView HOLD), which propagates through gate()
    unmodified -- the effect still never fires."""
    _seed(tmp_path)
    vol = CoherentVolume(tmp_path, managed=("data/**",), config=fast_cfg)  # strict default
    try:
        vol.write(REL, b"config-v1")
        fired: list[str] = []

        def decide(data: bytes) -> str:
            stop_coordinator(tmp_path)
            return "deploy"

        def effect(decision: str) -> str:  # pragma: no cover - must never run
            fired.append(decision)
            return "should-not-fire"

        with pytest.raises(CoherenceError) as exc_info:
            gate(vol, REL, decide=decide, effect=effect)
        assert not isinstance(exc_info.value, StaleView)  # infra failure, not a HOLD
        assert fired == []
    finally:
        stop_coordinator(tmp_path)


def test_gate_accepts_pathlib_path(tmp_path: Path, fast_cfg: LifecycleConfig) -> None:
    """gate() accepts an os.PathLike input (pathlib.Path), not only str, and
    fires when unchanged -- exercising the PathLike type through the real
    read_with_version round-trip."""
    _seed(tmp_path)
    vol = CoherentVolume(tmp_path, managed=("data/**",), config=fast_cfg)
    try:
        vol.write(REL, b"config-v1")
        result = gate(vol, Path(REL), decide=lambda d: "go", effect=lambda x: f"{x}-fired")
        assert result == "go-fired"
    finally:
        stop_coordinator(tmp_path)


# --- Fast unit tests of gate()'s pure logic (a stub volume, no coordinator) ----


class _StubVolume:
    """A minimal CoherentVolume stand-in for gate()'s pure logic: successive
    read_with_version calls yield scripted (bytes, version) pairs, or raise."""

    def __init__(self, reads: list) -> None:
        self._reads = list(reads)
        self._i = 0

    def read_with_version(self, path: str) -> tuple[bytes, int]:
        item = self._reads[self._i]
        self._i += 1
        if isinstance(item, Exception):
            raise item
        return item


def test_gate_fires_on_stub_unchanged() -> None:
    fired: list[str] = []

    def effect(x: str) -> str:
        fired.append(x)
        return "ok"

    result = gate(
        _StubVolume([(b"cfg", 5), (b"cfg", 5)]),
        "p",
        decide=lambda d: "go",
        effect=effect,
    )
    assert result == "ok"
    assert fired == ["go"]


def test_gate_holds_on_stub_moved() -> None:
    fired: list[str] = []
    with pytest.raises(StaleView) as exc:
        gate(
            _StubVolume([(b"cfg", 5), (b"cfg", 6)]),
            "p",
            decide=lambda d: "go",
            effect=fired.append,
        )
    assert fired == []
    assert exc.value.expected_version == 5
    assert exc.value.current_version == 6


def test_gate_holds_on_unconfirmed_version() -> None:
    """Fail-closed: an unresolved version (0 -- a degraded / degrade-mode read)
    HOLDs rather than firing on input the coordinator never confirmed."""
    fired: list[str] = []
    with pytest.raises(StaleView) as exc:
        gate(
            _StubVolume([(b"cfg", 0), (b"cfg", 0)]),
            "p",
            decide=lambda d: "go",
            effect=fired.append,
        )
    assert fired == []
    assert exc.value.expected_version == 0


def test_gate_holds_on_vanish_carries_none_current() -> None:
    fired: list[str] = []
    with pytest.raises(StaleView) as exc:
        gate(
            _StubVolume([(b"cfg", 5), FileNotFoundError()]),
            "p",
            decide=lambda d: "go",
            effect=fired.append,
        )
    assert fired == []
    assert exc.value.current_version is None


def test_gate_holds_when_capture_read_degraded() -> None:
    """Degrade-mode capture: a capture read that could not be confirmed
    (version 0) HOLDs even though the re-read resolved to a real version."""
    fired: list[str] = []
    with pytest.raises(StaleView) as exc:
        gate(
            _StubVolume([(b"cfg", 0), (b"cfg", 5)]),
            "p",
            decide=lambda d: "go",
            effect=fired.append,
        )
    assert fired == []
    assert exc.value.expected_version == 0


def test_gate_holds_when_revalidate_read_degraded() -> None:
    """Degrade-mode re-read: a real capture (v5) whose re-read degrades to
    version 0 HOLDs -- the degrade-mode race the fail-closed guard covers.
    The ``unconfirmed`` term itself is pinned by the (0,0) case in
    test_gate_holds_on_unconfirmed_version (dropping the term fails that test);
    ``or`` vs ``and`` on the term is behaviorally equivalent across gate()'s
    reachable version domain, so no black-box test distinguishes them."""
    fired: list[str] = []
    with pytest.raises(StaleView) as exc:
        gate(
            _StubVolume([(b"cfg", 5), (b"cfg", 0)]),
            "p",
            decide=lambda d: "go",
            effect=fired.append,
        )
    assert fired == []
    assert exc.value.expected_version == 5
    assert exc.value.current_version == 0


def test_gate_requires_callable_effect() -> None:
    """The explicit non-callable ``effect`` guard (distinct from omitting it)."""
    with pytest.raises(TypeError):
        gate(_StubVolume([(b"x", 1)]), "p", decide=lambda d: "go", effect=5)  # type: ignore[arg-type]


def test_gate_requires_callable_decide() -> None:
    with pytest.raises(TypeError):
        gate(_StubVolume([(b"x", 1)]), "p", decide=None, effect=lambda x: x)  # type: ignore[arg-type]


def test_gate_hold_tolerates_non_pathlike_path() -> None:
    """The HOLD message builds via os.fspath() but falls back to str(), so a
    duck-typed volume with a non-PathLike path still raises StaleView rather than
    a masking TypeError (a real CoherentVolume rejects such a path earlier)."""
    with pytest.raises(StaleView):
        gate(
            _StubVolume([(b"cfg", 5), (b"cfg", 6)]),
            12345,  # type: ignore[arg-type]
            decide=lambda d: "go",
            effect=lambda x: x,
        )


def test_bare_stale_view_exposes_none_version_attrs() -> None:
    """A StaleView raised without drift (the coordinator deny sites) still
    exposes expected_version/current_version as None, so a generic
    ``except StaleView`` handler reads the same shape gate() raises."""
    exc = StaleView("peer committed a newer version")
    assert exc.expected_version is None
    assert exc.current_version is None
