# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Tests for the effect-ordering gate wrapper (``gate()``).

EO-8 regression: an escaping effect is HELD (never fires) when a gated input's
version advanced between capture and fire, and fires when the input is
unchanged. Generation-fence regression: the effect is also HELD when a sweep
reclaimed the grant the decision was read under — owner_generation advanced
while the version stayed put, the drift a version-only comparand cannot see.
Uses a REAL coordinator subprocess (the ``test_coherent_volume.py`` pattern)
with a FIXED-STALE BUFFER: the peer commit (or the sweep reclaim) is driven
explicitly inside ``decide()`` (between capture and re-validate), never a
sleep.
"""

from __future__ import annotations

import time
from pathlib import Path

import pytest

from ccs.adapters.claude_code import lifecycle
from ccs.adapters.claude_code.lifecycle import LifecycleConfig, stop_coordinator
from ccs.adapters.coherent_volume import CoherentVolume
from ccs.adapters.effect_gate import gate
from ccs.cli._coherence_client import post as _cc_post
from ccs.cli._coherence_client import resolve_endpoint, resolve_remote_endpoint
from ccs.core.exceptions import (
    HOLD_READ_DENIED,
    CoherenceDegradedWarning,
    CoherenceError,
    StaleView,
)

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


def test_escaping_effect_held_when_grant_reclaimed_strict_deny_leg(
    tmp_path: Path, fast_cfg: LifecycleConfig
) -> None:
    """Generation-fence regression, strict leg (the reclaim-zombie
    interleaving): a sweep reclaims THIS holder's stalled M grant between
    capture and fire. Nothing else writes the artifact, so the version
    comparand is UNCHANGED. A CoherentVolume installs its managed globs
    strict, so the zombie's re-validation read is the byte-stable strict deny
    — which deliberately carries NO generation. PRE-FIX, gate() extracted the
    unchanged version from the deny's summary, discarded ``stale_denied``,
    and FIRED the effect straight through the deny; POST-FIX the unconfirmed
    generation HOLDs it. The reclaim is a fixed-stale buffer driven
    explicitly inside ``decide()`` with synthetic far-future ticks, never a
    sleep."""
    _seed(tmp_path)
    vol = CoherentVolume(tmp_path, managed=("data/**",), config=fast_cfg)
    try:
        vol.write(REL, b"config-v1")  # M grant, heartbeat seeded at now
        coordinator = lifecycle._SPAWNED_REGISTRY[str(tmp_path.resolve())].coordinator
        fired: list[str] = []

        def decide(data: bytes) -> str:
            # FIXED-STALE BUFFER: the sweep reclaims this holder's grant between
            # capture and re-validate (heartbeat_timeout_ticks=1 against a
            # far-future tick forces reclaim_heartbeat; max_hold stays off).
            reclaimed = coordinator.service.enforce_stable_grant_timeouts(
                current_tick=int(time.time()) + 999_999,
                heartbeat_timeout_ticks=1,
                max_hold_ticks=999_999_999,
            )
            assert reclaimed == 1, "sweep should have reclaimed exactly one grant"
            return "deploy"

        def effect(decision: str) -> str:  # pragma: no cover - must never run
            fired.append(decision)
            return "should-not-fire"

        with pytest.raises(StaleView) as exc_info:
            gate(vol, REL, decide=decide, effect=effect)

        assert fired == []  # the zombie's effect never fired through the deny
        exc = exc_info.value
        # THE pre-fix hole: the version never moved (the deny's summary still
        # matched), so a version-only comparand admitted the fire…
        assert exc.current_version == exc.expected_version
        # …and the HOLD came from the generation leg: confirmed at capture,
        # deliberately unconfirmed on the strict deny. The cause names the DENY
        # specifically — on the strict path that is how a reclaim reaches the
        # client, and it is recoverable, so it must not hide in the residual
        # "no generation reported" bucket (which includes the one case a
        # reacquire cannot fix).
        assert exc.expected_generation is not None
        assert exc.current_generation is None
        assert exc.hold_cause == HOLD_READ_DENIED
        assert "denied at re-read" in str(exc)
    finally:
        stop_coordinator(tmp_path)


def test_escaping_effect_held_when_grant_reclaimed_version_unchanged(
    tmp_path: Path, fast_cfg: LifecycleConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Generation-fence regression, warn leg: against a coordinator that
    TRACKS the artifact without strict mode (the fleet / foreign-coordinator
    shape), the zombie's re-validation is a warn-stale re-grant carrying the
    SAME version and an ADVANCED owner_generation — the exact drift a
    version-only comparand cannot see. PRE-FIX gate() compared versions,
    found them equal, and FIRED; POST-FIX it HOLDs on the moved epoch, with
    the g→g+1 drift on the exception."""
    monkeypatch.setenv("CCS_REMOTE_COORDINATOR", "1")
    coord_root = tmp_path / "coord"
    coord_root.mkdir()
    root_w = tmp_path / "w"
    root_w.mkdir()
    lifecycle.ensure_coordinator(coord_root, config=fast_cfg)
    try:
        ep = resolve_endpoint(coord_root)
        # Tracked (versioned + stale detection) but NOT strict: the reclaimed
        # holder's re-read is a warn-stale re-grant, not a deny.
        _cc_post(ep, "/policy/track", {"paths": ["task.txt"]})
        vol = CoherentVolume(
            root_w,
            on_error="strict",
            on_stale_write="allow",
            remote_endpoint=resolve_remote_endpoint("127.0.0.1", ep.port, ep.bearer),
        )
        (root_w / "task.txt").write_bytes(b"task-v1")
        vol.write("task.txt", b"task-v1")  # M grant, heartbeat seeded at now
        coordinator = lifecycle._SPAWNED_REGISTRY[
            str(coord_root.resolve())
        ].coordinator
        fired: list[str] = []

        def decide(data: bytes) -> str:
            reclaimed = coordinator.service.enforce_stable_grant_timeouts(
                current_tick=int(time.time()) + 999_999,
                heartbeat_timeout_ticks=1,
                max_hold_ticks=999_999_999,
            )
            assert reclaimed == 1, "sweep should have reclaimed exactly one grant"
            return "deploy"

        def effect(decision: str) -> str:  # pragma: no cover - must never run
            fired.append(decision)
            return "should-not-fire"

        with pytest.raises(StaleView) as exc_info:
            gate(vol, "task.txt", decide=decide, effect=effect)

        assert fired == []  # the zombie's effect never fired
        exc = exc_info.value
        # THE distinguishing shape: the version never moved…
        assert exc.current_version == exc.expected_version
        # …but the ownership epoch advanced by exactly the one reclaim.
        assert exc.expected_generation is not None
        assert exc.current_generation == exc.expected_generation + 1
        assert "grant reclaimed" in str(exc)
    finally:
        stop_coordinator(coord_root)


def test_escaping_effect_held_when_warn_mode_bytes_are_stale(
    tmp_path: Path, fast_cfg: LifecycleConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Warn-mode value/comparand binding: the bytes handed to decide() and the
    comparand pair come from two instants (a disk read, then the coordinator
    round-trip). On a tracked-but-NOT-strict artifact a superseded reader is
    warn-allowed — re-granted with the CURRENT pair — so the pair would
    re-validate clean at the boundary while decide() consumed bytes the
    coordinator knows are superseded. The coordinator flags exactly that
    (``hash_differs``), and an unconfirmable view yields NO confirmed
    generation, so the gate HOLDs rather than firing a stale-derived effect."""
    monkeypatch.setenv("CCS_REMOTE_COORDINATOR", "1")
    coord_root = tmp_path / "coord"
    coord_root.mkdir()
    root_w = tmp_path / "w"
    root_w.mkdir()
    root_p = tmp_path / "p"
    root_p.mkdir()
    lifecycle.ensure_coordinator(coord_root, config=fast_cfg)
    try:
        ep = resolve_endpoint(coord_root)
        _cc_post(ep, "/policy/track", {"paths": ["task.txt"]})

        def vol(root: Path) -> CoherentVolume:
            return CoherentVolume(
                root,
                on_error="strict",
                on_stale_write="allow",
                remote_endpoint=resolve_remote_endpoint(
                    "127.0.0.1", ep.port, ep.bearer
                ),
            )

        worker, peer = vol(root_w), vol(root_p)
        (root_w / "task.txt").write_bytes(b"plan-A")
        (root_p / "task.txt").write_bytes(b"plan-A")
        worker.write("task.txt", b"plan-A")
        # The peer supersedes the content; the worker's own disk still holds
        # plan-A, so its next read hands decide() superseded bytes.
        peer.write("task.txt", b"plan-B")
        fired: list[str] = []

        def effect(decision: str) -> str:  # pragma: no cover - must never run
            fired.append(decision)
            return "should-not-fire"

        with pytest.raises(StaleView) as exc_info:
            gate(worker, "task.txt", decide=lambda d: d.decode(), effect=effect)

        assert fired == []  # never fired on a decision derived from plan-A
        assert exc_info.value.current_generation is None
        assert "no confirmed ownership generation" in str(exc_info.value)
    finally:
        stop_coordinator(coord_root)


def test_gate_fires_on_the_callers_own_recent_write(
    tmp_path: Path, fast_cfg: LifecycleConfig
) -> None:
    """The fail-closed ``hash_differs`` rule must not over-trigger on the most
    common path there is: a caller gating an artifact it just wrote itself. Its
    own committed bytes ARE the canonical content, so the generation stays
    confirmed and the effect fires. Repeated write -> gate cycles too, so a
    commit-to-disk lag cannot accumulate into a permanent HOLD."""
    _seed(tmp_path)
    vol = CoherentVolume(tmp_path, managed=("data/**",), config=fast_cfg)
    try:
        fired: list[str] = []
        for content in (b"config-v1", b"config-v2"):
            vol.write(REL, content)
            fired.clear()
            gate(vol, REL, decide=lambda d: d.decode(), effect=fired.append)
            assert fired == [content.decode()], (
                "gating a path the caller just wrote must FIRE, never HOLD"
            )
    finally:
        stop_coordinator(tmp_path)


def test_gate_fires_on_own_write_in_warn_mode(
    tmp_path: Path, fast_cfg: LifecycleConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Same guarantee on the tracked-but-NOT-strict topology, where a superseded
    read is a warn-mode re-grant rather than a deny — the arm where an
    over-eager unconfirmed-generation rule would silently wedge every gate."""
    monkeypatch.setenv("CCS_REMOTE_COORDINATOR", "1")
    coord_root = tmp_path / "coord"
    coord_root.mkdir()
    root_w = tmp_path / "w"
    root_w.mkdir()
    lifecycle.ensure_coordinator(coord_root, config=fast_cfg)
    try:
        ep = resolve_endpoint(coord_root)
        _cc_post(ep, "/policy/track", {"paths": ["task.txt"]})
        vol = CoherentVolume(
            root_w,
            on_error="strict",
            on_stale_write="allow",
            remote_endpoint=resolve_remote_endpoint("127.0.0.1", ep.port, ep.bearer),
        )
        (root_w / "task.txt").write_bytes(b"plan-A")
        vol.write("task.txt", b"plan-A")
        fired: list[str] = []
        gate(vol, "task.txt", decide=lambda d: d.decode(), effect=fired.append)
        assert fired == ["plan-A"]
    finally:
        stop_coordinator(coord_root)


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


def test_gate_does_not_absolve_a_foreign_edit_it_never_showed_the_caller(
    tmp_path: Path, fast_cfg: LifecycleConfig
) -> None:
    """Python-API twin of the MCP surface's paired-write regression.

    ``gate()`` makes TWO reads, and only the first is an observation: its bytes
    go to ``decide()``, so seeding the foreign-edit baseline there is correct.
    The boundary re-read is a VERIFICATION — it compares comparands and discards
    the bytes — so it must leave the baseline alone. An out-of-band edit landing
    between the two is therefore one the caller demonstrably never saw, and the
    caller's next write must still be denied.

    Without ``observe=False`` on the boundary read the baseline advances to the
    foreign bytes, the write-side guard sees them as already-observed, and the
    agent silently clobbers an edit it was never shown — so calling the safety
    check would make the NEXT write less safe than skipping it.

    Read the two HOLDs differently. The FIRST is only a precondition: the gate
    refuses, but through the coordinator's own hash-mismatch handling, which is
    independent of the flag under test — and *which* mechanism answers is
    timing-dependent (inside the self-commit-lag window the client's
    ``hash_differs`` rule reports an unconfirmed generation; outside it the
    coordinator strict-denies), so this deliberately does not pin a
    ``hold_cause`` there. The SECOND raise is the one that pins ``observe``:
    it is the SB-23 write-side guard, asserted on its own reason text so the
    test cannot pass on an unrelated deny.
    """
    _seed(tmp_path)
    vol = CoherentVolume(tmp_path, managed=("data/**",), config=fast_cfg)
    try:
        vol.write(REL, b"config-v1")
        fired: list[str] = []

        def decide(data: bytes) -> str:
            assert data == b"config-v1", "the caller observes v1, and only v1"
            # FIXED-STALE BUFFER: an out-of-band editor lands AFTER the caller's
            # observation and BEFORE the boundary re-read — deterministic, no sleep.
            (tmp_path / REL).write_bytes(b"foreign-v2")
            return "deploy"

        def effect(decision: str) -> str:  # pragma: no cover - must never run
            fired.append(decision)
            return "should-not-fire"

        with pytest.raises(StaleView):  # precondition, not the assertion under test
            gate(vol, REL, decide=decide, effect=effect)
        assert fired == []

        # THE POINT: the caller was shown config-v1 and never foreign-v2, so the
        # write-side foreign-edit guard must still fire. Pinned on the guard's own
        # reason text — a bare `raises(StaleView)` would also accept an unrelated
        # deny (a sticky-INVALID view, a strict pre-edit refusal) and pass while
        # proving nothing.
        with pytest.raises(StaleView) as denied:
            vol.write(REL, b"agent-write")
        assert "out-of-band edit" in str(denied.value), (
            "the denial must come from the SB-23 foreign-edit guard, not from "
            f"some other refusal: {denied.value}"
        )
        assert (tmp_path / REL).read_bytes() == b"foreign-v2", (
            "the foreign bytes must survive — the gate must not have laundered "
            "an edit it read only to compare and then discarded"
        )
    finally:
        stop_coordinator(tmp_path)

# --- Fast unit tests of gate()'s pure logic (a stub volume, no coordinator) ----


class _StubVolume:
    """A minimal CoherentVolume stand-in for gate()'s pure logic: successive
    read_with_version_generation calls yield scripted
    (bytes, version, owner_generation) triples, or raise. A confirmed but
    arbitrary generation (7) is the default in version-focused tests so the
    version semantics stay isolated from the generation leg."""

    def __init__(self, reads: list) -> None:
        self._reads = list(reads)
        self._i = 0

    def read_with_version_generation(
        self, path: str, *, observe: bool = True
    ) -> tuple[bytes, int, int | None]:
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
        _StubVolume([(b"cfg", 5, 7), (b"cfg", 5, 7)]),
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
            _StubVolume([(b"cfg", 5, 7), (b"cfg", 6, 7)]),
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
            _StubVolume([(b"cfg", 0, 7), (b"cfg", 0, 7)]),
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
            _StubVolume([(b"cfg", 5, 7), FileNotFoundError()]),
            "p",
            decide=lambda d: "go",
            effect=fired.append,
        )
    assert fired == []
    assert exc.value.current_version is None
    # _held() threads the capture generation through the vanish branch too, so
    # a handler reading the drift sees the same shape on every HOLD class.
    assert exc.value.expected_generation == 7
    assert exc.value.current_generation is None


def test_gate_holds_when_capture_read_degraded() -> None:
    """Degrade-mode capture: a capture read that could not be confirmed
    (version 0) HOLDs even though the re-read resolved to a real version."""
    fired: list[str] = []
    with pytest.raises(StaleView) as exc:
        gate(
            _StubVolume([(b"cfg", 0, 7), (b"cfg", 5, 7)]),
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
            _StubVolume([(b"cfg", 5, 7), (b"cfg", 0, 7)]),
            "p",
            decide=lambda d: "go",
            effect=fired.append,
        )
    assert fired == []
    assert exc.value.expected_version == 5
    assert exc.value.current_version == 0


def test_gate_holds_on_generation_moved_version_unchanged() -> None:
    """THE generation-fence case: the version comparand is unchanged (nothing
    re-wrote the artifact) but the ownership generation advanced (a sweep
    reclaimed the grant the decision was read under). A version-only check
    fires here; the gate must HOLD, with the drift on the exception."""
    fired: list[str] = []
    with pytest.raises(StaleView) as exc:
        gate(
            _StubVolume([(b"cfg", 5, 7), (b"cfg", 5, 8)]),
            "p",
            decide=lambda d: "go",
            effect=fired.append,
        )
    assert fired == []
    assert exc.value.expected_version == 5
    assert exc.value.current_version == 5  # the version NEVER moved
    assert exc.value.expected_generation == 7
    assert exc.value.current_generation == 8
    assert "grant reclaimed" in str(exc.value)


def test_gate_holds_on_unconfirmed_capture_generation() -> None:
    """Fail-closed: a capture read with no confirmed generation (an older
    coordinator that predates the fence, a strict deny, or a degraded read)
    HOLDs even though both versions match and the re-read carries one."""
    fired: list[str] = []
    with pytest.raises(StaleView) as exc:
        gate(
            _StubVolume([(b"cfg", 5, None), (b"cfg", 5, 7)]),
            "p",
            decide=lambda d: "go",
            effect=fired.append,
        )
    assert fired == []
    assert exc.value.expected_generation is None
    assert exc.value.current_generation == 7


def test_gate_holds_on_unconfirmed_revalidate_generation() -> None:
    """Fail-closed, the other leg: a confirmed capture whose re-read loses the
    generation confirmation (a deny or degrade mid-flight) HOLDs."""
    fired: list[str] = []
    with pytest.raises(StaleView) as exc:
        gate(
            _StubVolume([(b"cfg", 5, 7), (b"cfg", 5, None)]),
            "p",
            decide=lambda d: "go",
            effect=fired.append,
        )
    assert fired == []
    assert exc.value.expected_generation == 7
    assert exc.value.current_generation is None


def test_gate_holds_when_both_comparands_moved_version_detail_wins() -> None:
    """Both axes moved at once (a peer commit AND a reclaim). It HOLDs, carries
    both drifts, and the message reports the version move — the deliberate
    branch priority in _held(), which is otherwise unpinned."""
    fired: list[str] = []
    with pytest.raises(StaleView) as exc:
        gate(
            _StubVolume([(b"cfg", 5, 7), (b"cfg", 6, 8)]),
            "p",
            decide=lambda d: "go",
            effect=fired.append,
        )
    assert fired == []
    assert (exc.value.expected_version, exc.value.current_version) == (5, 6)
    assert (exc.value.expected_generation, exc.value.current_generation) == (7, 8)
    assert "moved to v6" in str(exc.value)


def test_gate_holds_when_generation_unconfirmed_on_both_reads() -> None:
    """The shape an older coordinator produces on EVERY read: matching versions
    and None generations on both sides. Two Nones are not "no movement" — the
    gate must HOLD loudly rather than silently reverting to the
    generation-blind check."""
    fired: list[str] = []
    with pytest.raises(StaleView) as exc:
        gate(
            _StubVolume([(b"cfg", 5, None), (b"cfg", 5, None)]),
            "p",
            decide=lambda d: "go",
            effect=fired.append,
        )
    assert fired == []
    assert exc.value.expected_generation is None
    assert exc.value.current_generation is None
    assert "no confirmed ownership generation" in str(exc.value)


def test_gate_requires_callable_effect() -> None:
    """The explicit non-callable ``effect`` guard (distinct from omitting it)."""
    with pytest.raises(TypeError):
        gate(_StubVolume([(b"x", 1, 7)]), "p", decide=lambda d: "go", effect=5)  # type: ignore[arg-type]


def test_gate_requires_callable_decide() -> None:
    with pytest.raises(TypeError):
        gate(_StubVolume([(b"x", 1, 7)]), "p", decide=None, effect=lambda x: x)  # type: ignore[arg-type]


def test_gate_hold_tolerates_non_pathlike_path() -> None:
    """The HOLD message builds via os.fspath() but falls back to str(), so a
    duck-typed volume with a non-PathLike path still raises StaleView rather than
    a masking TypeError (a real CoherentVolume rejects such a path earlier)."""
    with pytest.raises(StaleView):
        gate(
            _StubVolume([(b"cfg", 5, 7), (b"cfg", 6, 7)]),
            12345,  # type: ignore[arg-type]
            decide=lambda d: "go",
            effect=lambda x: x,
        )


def test_bare_stale_view_exposes_none_version_attrs() -> None:
    """A StaleView raised without drift (the coordinator deny sites) still
    exposes expected_version/current_version AND the generation pair as None,
    so a generic ``except StaleView`` handler reads the same shape gate()
    raises."""
    exc = StaleView("peer committed a newer version")
    assert exc.expected_version is None
    assert exc.current_version is None
    assert exc.expected_generation is None
    assert exc.current_generation is None


def test_hold_cause_is_typed_per_class() -> None:
    """Every HOLD class carries a distinct typed cause, so an agent branches on
    a value rather than substring-matching the human message — and can tell a
    HOLD reacquire() clears from one that it never will."""
    from ccs.core.exceptions import (
        HOLD_GENERATION_UNCONFIRMED,
        HOLD_GRANT_RECLAIMED,
        HOLD_INPUT_VANISHED,
        HOLD_VERSION_MOVED,
        HOLD_VERSION_UNCONFIRMED,
    )

    cases = [
        ([(b"c", 5, 7), (b"c", 6, 7)], HOLD_VERSION_MOVED),
        ([(b"c", 5, 7), (b"c", 5, 8)], HOLD_GRANT_RECLAIMED),
        ([(b"c", 5, 7), FileNotFoundError()], HOLD_INPUT_VANISHED),
        ([(b"c", 5, 7), (b"c", 0, 7)], HOLD_VERSION_UNCONFIRMED),
        ([(b"c", 5, 7), (b"c", 5, None)], HOLD_GENERATION_UNCONFIRMED),
    ]
    for reads, expected_cause in cases:
        with pytest.raises(StaleView) as exc:
            gate(_StubVolume(reads), "p", decide=lambda d: "go", effect=lambda x: x)
        assert exc.value.hold_cause == expected_cause, reads
    # A bare coordinator-raised StaleView carries no cause (uniform shape).
    assert StaleView("peer committed").hold_cause is None
