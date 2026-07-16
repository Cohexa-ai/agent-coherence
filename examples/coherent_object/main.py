# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents
"""CoherentObject demo — a peer's commit invalidates the other agent's cached read
before it acts, over a shared object.

Two agents share ONE object. Agent A reads the object and begins a multi-step
edit; agent B commits a new version through the binding; the coordinator marks
A's cached view INVALID; **A's next binding-mediated act is DENIED before it
touches the object** — A reacquires fresh state and re-decides. The ``--baseline``
arm drops the binding: A acts on its stale cache with no signal (the failure the
binding catches).

    python -m examples.coherent_object.main             # the with-binding demo
    python -m examples.coherent_object.main --baseline  # negative control first

Offline, no credentials — it spawns a local coordinator subprocess and models the
object with a tiny in-memory stand-in so the demo runs anywhere. In production the
SAME binding points at a real S3 bucket (see docs/usage/byo-substrate.md and the
conformance kit's real_substrate arm). Exit code 0 iff the contract holds: with
the binding the stale act is DENIED before the object is touched and A recovers
via reacquire; with ``--baseline`` the un-coordinated agent acts on stale state.

Single-host, cooperative — two agents/processes against one coordinator. NEVER
two hosts against one distributed substrate (no Multi-Region Access Point, no
cross-region replica).
"""

from __future__ import annotations

import argparse
import hashlib
import sys
import tempfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from ccs.adapters.claude_code.lifecycle import (  # noqa: E402  (after sys.path setup)
    LifecycleConfig,
    stop_coordinator,
)
from ccs.adapters.substrate import (  # noqa: E402
    CasConflict,
    CasWriteResult,
    CasWritten,
    CoordinatedSubstrate,
    ReconcileDecision,
    ReconcileVerdict,
    SubstrateCoordinatorSession,
)
from ccs.core.exceptions import StaleView  # noqa: E402
from ccs.core.substrate import CapabilityDescriptor, Tier  # noqa: E402

REF = "shared/agent_scratchpad.json"
SEED = b'{"plan": "draft", "step": 1}'
PEER_EDIT = b'{"plan": "revised", "step": 2}'


def _fast_cfg() -> LifecycleConfig:
    return LifecycleConfig(
        idle_shutdown_sec=0,
        sweep_interval_sec=0.1,
        notice_evict_max_age_sec=1.0,
        port_file_retry_attempts=20,
        port_file_retry_interval_sec=0.05,
        connect_retry_attempts=10,
        connect_retry_interval_sec=0.05,
    )


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


class _Bucket:
    """The shared object state (bytes + an opaque ETag), shared by every agent's
    binding view — one underlying artifact, distinct agents."""

    def __init__(self) -> None:
        self._data: dict[str, tuple[bytes, str]] = {}
        self._counter = 0

    def set(self, ref: str, data: bytes) -> str:
        self._counter += 1
        etag = f'"etag-{self._counter}"'
        self._data[ref] = (data, etag)
        return etag

    def get(self, ref: str) -> tuple[bytes, str] | None:
        return self._data.get(ref)


class _InMemoryObject:
    """A tiny in-memory stand-in for a real S3 object, so the demo runs offline.
    Production swaps this for ``ccs.adapters.coherent_object.CoherentObject``
    pointed at a real bucket — the coordinator-mediated behaviour is identical."""

    #: The object body never reaches the coordinator (never-ship-a-store).
    SENDS_CONTENT_TO_COORDINATOR: bool = False

    def __init__(self, store: _Bucket) -> None:
        self._store = store
        self.cas_calls: list[str] = []
        self._descriptor = CapabilityDescriptor(
            tier=Tier.NATIVE_CAS,
            version_source="object ETag",
            least_privilege="in-memory demo stand-in",
            consistency_note="read-after-write per key (demo)",
        )

    @property
    def descriptor(self) -> CapabilityDescriptor:
        return self._descriptor

    def coordinator_commit_content(self) -> None:
        return None

    def read(self, artifact_ref: str) -> tuple[bytes, str]:
        entry = self._store.get(artifact_ref)
        if entry is None:
            raise KeyError(artifact_ref)
        return entry

    def cas_write(self, artifact_ref: str, *, expected_token: str, new_bytes: bytes) -> CasWriteResult:
        self.cas_calls.append(expected_token)
        entry = self._store.get(artifact_ref)
        if entry is None or entry[1] != expected_token:
            return CasConflict()
        return CasWritten(token=self._store.set(artifact_ref, bytes(new_bytes)))

    def reconcile_after_unknown(
        self, artifact_ref: str, *, expected_token: str, intended_hash: str
    ) -> ReconcileDecision:
        entry = self._store.get(artifact_ref)
        if entry is None:
            return ReconcileDecision(ReconcileVerdict.HOLD, None, None)
        observed_bytes, observed_token = entry
        if observed_token == expected_token:
            return ReconcileDecision(ReconcileVerdict.RE_DRIVE, observed_bytes, observed_token)
        if _sha256(observed_bytes) == intended_hash:
            return ReconcileDecision(ReconcileVerdict.CONVERGE, observed_bytes, observed_token)
        return ReconcileDecision(ReconcileVerdict.CONFLICT, observed_bytes, observed_token)


def _decide(scratchpad: bytes) -> str:
    """A trivial decision derived from the object the agent read."""
    return f"append_note(to={scratchpad.decode()})"


def run_with_binding(root: Path) -> dict:
    """A peer's commit invalidates A's cached read; A's next act is DENIED before
    the object is touched; A reacquires and commits on fresh state."""
    store = _Bucket()
    store.set(REF, SEED)
    session_a = SubstrateCoordinatorSession(root, managed=("**",), config=_fast_cfg())
    session_b = SubstrateCoordinatorSession(root, managed=("**",), config=_fast_cfg())
    trace: dict = {"denied_before_act": False, "substrate_untouched": False, "committed_on_fresh": False}
    try:
        fake_a = _InMemoryObject(store)
        agent_a = CoordinatedSubstrate(fake_a, session_a)
        agent_b = CoordinatedSubstrate(_InMemoryObject(store), session_b)

        a_bytes, a_tok = agent_a.read(REF)
        print(f"  A reads {REF} -> {a_bytes.decode()!r} and starts a multi-step edit")
        _b_bytes, b_tok = agent_b.read(REF)

        committed = agent_b.commit(REF, expected_token=b_tok, new_bytes=PEER_EDIT)
        print(f"  B commits a new version through the binding (coordinator v{committed.version}); A is now INVALID")

        try:
            agent_a.commit(REF, expected_token=a_tok, new_bytes=b'{"plan": "draft", "step": 1, "A": true}')
            print("  x A's stale edit was NOT denied — coherence FAILED")
        except StaleView:
            trace["denied_before_act"] = True
            trace["substrate_untouched"] = fake_a.cas_calls == []
            print("  ok A's cached view was invalidated -> edit DENIED before the object was touched")

        fresh_bytes, fresh_tok = agent_a.reacquire(REF)
        landed = agent_a.commit(REF, expected_token=fresh_tok, new_bytes=fresh_bytes + b" +A")
        trace["committed_on_fresh"] = True
        print(f"  ok A reacquired fresh state {fresh_bytes.decode()!r} and committed (coordinator v{landed.version})")
        return trace
    finally:
        stop_coordinator(root)


def run_baseline(root: Path) -> dict:
    """Negative control: no binding. A caches a read, a peer moves the object, and
    A acts on the stale cache with no signal."""
    del root  # the baseline needs no coordinator — a bare cache + the object's CAS
    store = _Bucket()
    store.set(REF, SEED)
    trace: dict = {"acted_on_stale": False}

    a_cached, _a_tok = store.get(REF)
    print(f"  A reads and caches {REF} -> {a_cached.decode()!r}, starts a multi-step edit")
    store.set(REF, PEER_EDIT)
    print("  B changes the object (step 1 -> step 2) while A is mid-decision")

    decision = _decide(a_cached)
    trace["acted_on_stale"] = True
    print(f"  x A acts on its STALE cache: {decision} (the object already moved; no signal)")
    print("    the object's bare put-object --if-match would reject A's WRITE, but only at")
    print("    write time — nothing told A its cached VIEW went stale before it acted")
    return trace


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="CoherentObject cross-agent demo (single-host).")
    parser.add_argument(
        "--baseline",
        action="store_true",
        help="run the no-binding negative control first (A acts on a stale cache)",
    )
    args = parser.parse_args(argv)

    print("CoherentObject demo — a peer's commit invalidates A's cached read before A acts.\n")

    baseline_ok = True
    if args.baseline:
        print("Baseline (no binding — a bare cache + the object's own CAS):")
        with tempfile.TemporaryDirectory() as tmp:
            baseline = run_baseline(Path(tmp))
        baseline_ok = bool(baseline["acted_on_stale"])
        print("")

    print("With the binding (coordinator-mediated):")
    with tempfile.TemporaryDirectory() as tmp:
        gated = run_with_binding(Path(tmp))

    print("\nTakeaway: the binding tells A its CACHED view went stale BEFORE it acts, in the")
    print("same typed vocabulary A's file and store artifacts use (uniformity). The bare")
    print("object CAS only rejects at write time. The read-generation fence is NOT demoed and")
    print("NOT claimed — v1 OCC writers ride admit-on-absent + version-CAS. See effect_gate")
    print("for the escaping-effect sibling of this state-write ordering.")

    gated_ok = (
        gated["denied_before_act"] and gated["substrate_untouched"] and gated["committed_on_fresh"]
    )
    return 0 if (gated_ok and baseline_ok) else 1


if __name__ == "__main__":
    raise SystemExit(main())

# Comparing notes on multi-agent coherence?
# https://github.com/Cohexa-ai/agent-coherence/discussions
