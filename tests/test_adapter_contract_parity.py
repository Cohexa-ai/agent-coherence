"""Cross-substrate adapter parity test (DX-3): same protocol, two adapters.

Documents the version-CAS contract in
``examples/cross_host/ADAPTER-CONTRACT.md`` and proves it as a passing test
rather than an assertion. The shipped file adapter (``CoherentVolume``) is
exercised live against a loopback coordinator; a minimal in-memory KV adapter
satisfying the same Protocol is exercised in-process. Both must reject A's
stale write with a typed ``CoherenceError`` and let A recover via re-read +
retry.

This is the demo's "DX-3 (Pluggable substrate adapters — same protocol, two
topologies)" claim made testable. A future adapter (HTTP MCP, SQL, blob store)
gets added here as one more parametrize case; the protocol surface and the
recover-by-reread invariant stay the same.
"""

from __future__ import annotations

import threading
from pathlib import Path
from typing import Protocol

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
from ccs.core.exceptions import CoherenceError

KEY = "shared.txt"


# ---------------------------------------------------------------------------
# The contract — also written in examples/cross_host/ADAPTER-CONTRACT.md as a
# Protocol, repeated here so the test file is self-contained for a reader.
# ---------------------------------------------------------------------------


class CoherenceAdapter(Protocol):
    """The version-CAS contract any substrate adapter satisfies."""

    def read_with_version(self, key: str) -> tuple[bytes, int]: ...
    def write_cas_at(self, key: str, expected_version: int, data: bytes) -> None: ...


# ---------------------------------------------------------------------------
# Reference KV adapter — NOT a shipped product, a Protocol-satisfying minimal
# implementation so the parity claim has a second adapter to test against. The
# point is the protocol surface, not the storage class.
# ---------------------------------------------------------------------------


class _MemoryKVAdapter:
    """Single in-memory KV implementing the CoherenceAdapter Protocol.

    Two clients share the SAME instance — coordination is the version held
    here, equivalent to the coordinator's role for ``CoherentVolume``.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._data: dict[str, bytes] = {}
        self._versions: dict[str, int] = {}

    def read_with_version(self, key: str) -> tuple[bytes, int]:
        with self._lock:
            return self._data.get(key, b""), self._versions.get(key, 0)

    def write_cas_at(self, key: str, expected_version: int, data: bytes) -> None:
        with self._lock:
            current = self._versions.get(key, 0)
            if current != expected_version:
                raise CoherenceError(f"stale write: expected v{expected_version}, current v{current}")
            self._data[key] = data
            self._versions[key] = current + 1


# ---------------------------------------------------------------------------
# Fixtures — each adapter pair represents "two clients sharing the same
# coordination authority for KEY," whatever the substrate.
# ---------------------------------------------------------------------------


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


@pytest.fixture
def file_adapter_pair(tmp_path: Path, fast_cfg: LifecycleConfig, monkeypatch: pytest.MonkeyPatch):
    """Pair of ``CoherentVolume`` clients against a real loopback coordinator —
    the file-on-volume substrate the cross-host demo exercises."""
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
        # Tracked but not strict — the protocol's version-CAS deny is the
        # whole mechanism the contract defines.
        _cc_post(ep, "/policy/track", {"paths": [KEY]})

        def _remote():
            return resolve_remote_endpoint("127.0.0.1", ep.port, ep.bearer)

        a = CoherentVolume(root_a, on_error="strict", on_stale_write="allow", remote_endpoint=_remote())
        b = CoherentVolume(root_b, on_error="strict", on_stale_write="allow", remote_endpoint=_remote())
        (root_a / KEY).write_text("v0", encoding="utf-8")
        (root_b / KEY).write_text("v0", encoding="utf-8")
        yield a, b
    finally:
        stop_coordinator(coord_root)


@pytest.fixture
def memory_kv_adapter_pair():
    """Pair of clients into the same in-memory KV — a Protocol-satisfying
    reference adapter, not a shipped product. Demonstrates the contract
    generalizes beyond the file substrate."""
    kv = _MemoryKVAdapter()
    yield kv, kv  # same instance: coordination is the in-memory version


# ---------------------------------------------------------------------------
# Parity test — the SAME scenario, parametrized over the two adapter pairs.
# ---------------------------------------------------------------------------


def _scenario_deny_then_recover(a: CoherenceAdapter, b: CoherenceAdapter) -> None:
    """The slice-1 scenario, written once against the contract surface.

    Both adapters must:
      1. Surface a decision-time version on read.
      2. Reject A's stale write with a ``CoherenceError`` (typed deny, not
         silent overwrite).
      3. Let A recover by re-reading the fresh version + retrying.
    """
    # A reads its decision-time version.
    _data_a, v_a = a.read_with_version(KEY)

    # B reads, then commits — the coordination authority advances past v_a.
    _data_b, v_b = b.read_with_version(KEY)
    b.write_cas_at(KEY, v_b, b"from-b")

    # A's stale write must be denied across the protocol.
    with pytest.raises(CoherenceError):
        a.write_cas_at(KEY, v_a, b"from-a")

    # A recovers — re-read returns a strictly newer version; retry succeeds.
    _data_a2, v_a2 = a.read_with_version(KEY)
    assert v_a2 > v_a, "recovery read must advance the version"
    a.write_cas_at(KEY, v_a2, b"from-a-2")

    # Final read confirms A's recovered write landed and the version advanced
    # past B's commit — no silent loss in either direction.
    _data_final, v_final = a.read_with_version(KEY)
    assert v_final > v_b, "final version must advance past B's commit"


def test_file_adapter_satisfies_contract(file_adapter_pair) -> None:
    """``CoherentVolume`` (file-on-volume substrate, networked coordinator)
    satisfies the CoherenceAdapter contract."""
    a, b = file_adapter_pair
    _scenario_deny_then_recover(a, b)


def test_memory_kv_adapter_satisfies_contract(memory_kv_adapter_pair) -> None:
    """An in-memory KV adapter (Protocol-satisfying reference, not shipped)
    satisfies the same contract — proving the surface generalizes beyond the
    file substrate."""
    a, b = memory_kv_adapter_pair
    _scenario_deny_then_recover(a, b)


def test_memory_kv_adapter_baseline_loses_silently_without_cas() -> None:
    """The negative control on the reference adapter: writing against the
    LATEST version (skipping the decision-time-version check) succeeds and
    silently loses the peer's commit. Codifies that the deny in the
    contract-satisfying test above is *measured*, not asserted — the SAME
    storage exhibits the SAME failure mode the contract prevents."""
    kv = _MemoryKVAdapter()

    # A reads its decision-time version but does not track it.
    _, v_a = kv.read_with_version(KEY)

    # B commits.
    _, v_b = kv.read_with_version(KEY)
    kv.write_cas_at(KEY, v_b, b"from-b")

    # A writes against the LATEST version — the baseline (no CAS discipline).
    _, v_a_latest = kv.read_with_version(KEY)
    assert v_a_latest > v_a, "precondition: B's commit advanced the version"
    kv.write_cas_at(KEY, v_a_latest, b"from-a")

    # B's bytes are gone — the silent lost update the contract prevents.
    data_now, _v_now = kv.read_with_version(KEY)
    assert data_now == b"from-a"
    assert data_now != b"from-b", "baseline should have lost B's bytes"
