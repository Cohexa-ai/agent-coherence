# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""A denied read inside a peer's commit→disk window must not hand out a comparand.

A peer's ``write_cas_at`` confirms its CAS at the coordinator and only THEN
writes its bytes to disk. A read that lands between the two sees the OLD bytes
on disk while the coordinator already reports the NEW version, and under strict
mode the coordinator denies it with ``hash_differs``. Handing that pair back as
``(bytes, version)`` is a split comparand: the caller derives from the old
bytes, passes the new version to ``write_cas_at``, and by the time the CAS runs
the peer's disk write has landed, so the version check passes and the peer's
update is overwritten.

The window is forced here rather than raced: the peer's ``_atomic_write`` waits
on an event the test releases, so every test below reads INSIDE the window.
"""

from __future__ import annotations

import threading
from pathlib import Path

import pytest

from ccs.adapters.claude_code.lifecycle import LifecycleConfig, stop_coordinator
from ccs.adapters.coherent_volume import CoherentVolume
from ccs.adapters.effect_gate import gate
from ccs.core.exceptions import StaleView

_PATH = "data/counter.txt"
_WAIT_SEC = 10.0


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


def _seed(tmp_path: Path, content: bytes = b"0") -> Path:
    target = tmp_path / "data" / "counter.txt"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(content)
    return target


def _vol(tmp_path: Path, cfg: LifecycleConfig) -> CoherentVolume:
    return CoherentVolume(tmp_path, managed=("data/**",), on_error="strict", config=cfg)


class LaggingPeer:
    """A peer whose confirmed CAS is held OFF disk until the test releases it.

    ``write_cas_at`` calls ``_atomic_write`` only after the coordinator confirmed
    the CAS, so blocking there opens the commit→disk window on demand.
    """

    def __init__(
        self, vol: CoherentVolume, monkeypatch: pytest.MonkeyPatch, path: str = _PATH
    ) -> None:
        self._vol = vol
        self._path = path
        self._committed = threading.Event()
        self._release = threading.Event()
        self._errors: list[BaseException] = []
        self._thread: threading.Thread | None = None
        real_write = vol._atomic_write

        def lagging_write(abs_path: Path, data: bytes) -> None:
            self._committed.set()
            if not self._release.wait(_WAIT_SEC):
                raise AssertionError(
                    "timed out waiting for the test to release the peer's disk write"
                )
            real_write(abs_path, data)

        monkeypatch.setattr(vol, "_atomic_write", lagging_write)

    def commit(self, new_content: bytes) -> None:
        """Start the peer's CAS and return once it is confirmed but not on disk."""
        _data, version = self._vol.read_with_version(self._path)

        def run() -> None:
            try:
                self._vol.write_cas_at(self._path, version, new_content)
            except BaseException as exc:  # surfaced by finish()
                self._errors.append(exc)
                self._committed.set()

        self._thread = threading.Thread(target=run, name="lagging-peer")
        self._thread.start()
        assert self._committed.wait(_WAIT_SEC), (
            "timed out waiting for the peer's CAS to be confirmed at the coordinator"
        )
        assert not self._errors, f"peer CAS failed before its disk write: {self._errors!r}"

    def finish(self) -> None:
        """Let the peer's disk write land and wait for its write_cas_at to return."""
        self._release.set()
        assert self._thread is not None
        self._thread.join(_WAIT_SEC)
        assert not self._thread.is_alive(), (
            "timed out waiting for the peer's write_cas_at to return after release"
        )
        assert not self._errors, f"peer CAS failed: {self._errors!r}"


@pytest.fixture
def volumes(tmp_path: Path, fast_cfg: LifecycleConfig):
    target = _seed(tmp_path)
    peer = _vol(tmp_path, fast_cfg)
    reader = _vol(tmp_path, fast_cfg)
    try:
        yield target, peer, reader
    finally:
        stop_coordinator(tmp_path)


def _increment(data: bytes) -> bytes:
    return str(int(data) + 1).encode()


# Two ways a reader reaches the window: never read the key (no grant, so the
# coordinator hash-checks it), or read it before the peer's commit (SHARED, then
# INVALID when the peer commits). Both are denied with hash_differs.
_READER_STATES = pytest.mark.parametrize(
    "read_before_peer", [False, True], ids=["no-prior-grant", "invalidated"]
)


@_READER_STATES
def test_read_with_version_refuses_a_split_pair_in_the_commit_window(
    volumes, monkeypatch: pytest.MonkeyPatch, read_before_peer: bool
) -> None:
    _target, peer_vol, reader = volumes
    if read_before_peer:
        reader.read_with_version(_PATH)
    peer = LaggingPeer(peer_vol, monkeypatch)
    peer.commit(b"1")
    try:
        with pytest.raises(StaleView):
            reader.read_with_version(_PATH)
    finally:
        peer.finish()


@_READER_STATES
def test_read_modify_cas_through_the_window_loses_no_update(
    volumes, monkeypatch: pytest.MonkeyPatch, read_before_peer: bool
) -> None:
    """The whole read→derive→write_cas_at cycle, run the way a caller would:
    a refused read is retried once the peer has moved on. Before the refusal
    existed, the reader derived "1" from the old "0", CAS'd it at the peer's
    version, and won, so the file ended at "1" with the peer's increment gone."""
    target, peer_vol, reader = volumes
    if read_before_peer:
        reader.read_with_version(_PATH)
    peer = LaggingPeer(peer_vol, monkeypatch)
    peer.commit(b"1")
    try:
        pair = reader.read_with_version(_PATH)
    except StaleView:
        pair = None
    peer.finish()
    if pair is None:
        pair = reader.read_with_version(_PATH)
    data, version = pair
    reader.write_cas_at(_PATH, version, _increment(data))
    assert target.read_bytes() == b"2"


def test_read_with_version_still_answers_an_invalidated_reader_once_disk_landed(
    volumes, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The other side of the refusal: a sticky-INVALID reader whose bytes DO
    match the coordinator's version is still denied, and that pair is sound, so
    it is returned as before (the documented sticky-INVALID read)."""
    _target, peer_vol, reader = volumes
    reader.read_with_version(_PATH)
    peer = LaggingPeer(peer_vol, monkeypatch)
    peer.commit(b"1")
    peer.finish()
    # KTD-T keeps a strict deny sticky, so this probe leaves the reader INVALID.
    probe = reader._read_with_version(_PATH)
    assert probe.stale_denied is True, "precondition: the reader must still be INVALID"
    assert probe.content_differs is False, "precondition: its bytes must match the version"
    data, version = reader.read_with_version(_PATH)
    assert data == b"1"
    reader.write_cas_at(_PATH, version, _increment(data))
    assert (reader._root / _PATH).read_bytes() == b"2"


@_READER_STATES
def test_read_with_version_generation_refuses_a_split_pair_in_the_commit_window(
    volumes, monkeypatch: pytest.MonkeyPatch, read_before_peer: bool
) -> None:
    _target, peer_vol, reader = volumes
    if read_before_peer:
        reader.read_with_version_generation(_PATH)
    peer = LaggingPeer(peer_vol, monkeypatch)
    peer.commit(b"1")
    try:
        with pytest.raises(StaleView):
            reader.read_with_version_generation(_PATH)
        # The flags still describe the read that was refused.
        assert reader._last_read_denied is True
    finally:
        peer.finish()


def test_verification_read_reports_the_split_instead_of_raising(
    volumes, monkeypatch: pytest.MonkeyPatch
) -> None:
    """observe=False is the effect fence's re-validate read: it discards the
    bytes and classifies from the flags, so it must keep answering (the fence
    names the HOLD from ``_last_read_denied``) rather than raise untyped."""
    _target, peer_vol, reader = volumes
    peer = LaggingPeer(peer_vol, monkeypatch)
    peer.commit(b"1")
    try:
        _data, _version, generation = reader.read_with_version_generation(
            _PATH, observe=False
        )
        assert reader._last_read_denied is True
        assert generation is None
    finally:
        peer.finish()


def test_gate_does_not_decide_from_bytes_read_in_the_commit_window(
    volumes, monkeypatch: pytest.MonkeyPatch
) -> None:
    _target, peer_vol, reader = volumes
    peer = LaggingPeer(peer_vol, monkeypatch)
    peer.commit(b"1")
    decided: list[bytes] = []
    try:
        with pytest.raises(StaleView):
            gate(
                reader,
                _PATH,
                decide=lambda data: decided.append(data),
                effect=lambda _decision: pytest.fail("effect fired on a split read"),
            )
    finally:
        peer.finish()
    assert decided == []
