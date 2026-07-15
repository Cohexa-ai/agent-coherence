# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Cross-agent coherence over a substrate binding (Unit 5).

These drive the coordinator-mediated layer with a REAL coordinator subprocess
(spawned via the shipped lifecycle, torn down in ``finally``) and a FAKE
in-memory substrate, so the coordinator-mediated behaviour — pull invalidation,
the divergence taxonomy, the never-ship-a-store commit path — is exercised
without a real Postgres or S3.

The fake models the shared substrate state (one row / one object) in a store
shared by every agent's binding view, mirroring reality: distinct agents, one
underlying artifact. It can script an ``UNKNOWN`` write (landed or not) so the
reconciliation dispatch is reachable, and it reconciles by the same
token-identity logic both real bindings use.
"""

from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

import ccs.adapters.substrate as substrate_module
from ccs.adapters.claude_code.lifecycle import LifecycleConfig, stop_coordinator
from ccs.adapters.substrate import (
    CasConflict,
    CasUnknown,
    CasWriteResult,
    CasWritten,
    CoordinatedSubstrate,
    CoordinatorConflict,
    CoordinatorWin,
    ReconcileDecision,
    ReconcileVerdict,
    SubstrateCoordinatorSession,
)
from ccs.core.exceptions import (
    COMMIT_UNCONFIRMED_REASON,
    VERSION_MISMATCH_REASON,
    CasVersionConflict,
    CoherenceError,
    CommitUnconfirmed,
    StaleView,
    ViewWedged,
)
from ccs.core.substrate import CapabilityDescriptor, Tier

REF = "workspace/shared.bin"


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


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


# --- fake substrate ---------------------------------------------------------


class _FakeStore:
    """The shared substrate state (one row / one object), shared by all views."""

    def __init__(self) -> None:
        self._data: dict[str, tuple[bytes, str]] = {}
        self._counter = 0

    def _mint(self) -> str:
        self._counter += 1
        return f"tok-{self._counter}"

    def seed(self, ref: str, data: bytes) -> str:
        token = self._mint()
        self._data[ref] = (data, token)
        return token

    def get(self, ref: str) -> tuple[bytes, str] | None:
        return self._data.get(ref)

    def set(self, ref: str, data: bytes) -> str:
        return self.seed(ref, data)

    def delete(self, ref: str) -> None:
        self._data.pop(ref, None)


def _descriptor(arm: str) -> CapabilityDescriptor:
    return CapabilityDescriptor(
        tier=Tier.NATIVE_CAS,
        version_source="fake row-version" if arm == "row" else "fake object ETag",
        least_privilege="fake",
        consistency_note="fake single-primary",
    )


class _FakeSubstrate:
    """A per-agent binding view over the shared store, implementing the
    reconciling-substrate surface with realistic token-identity logic."""

    def __init__(self, store: _FakeStore, arm: str = "object") -> None:
        self._store = store
        self._arm = arm
        self._descriptor = _descriptor(arm)
        self.cas_calls: list[tuple[str, str, bytes]] = []
        # A QUEUE of scripted outcomes (not a single slot): a re-drive path issues
        # a second cas_write, so tests must script both legs independently.
        self._scripts: list[tuple[str, bool]] = []
        # Runs inside reconcile_after_unknown (before the verdict) — a seam to
        # inject a byte-identical peer's coordinator bump mid-commit.
        self.reconcile_hook = None

    @property
    def descriptor(self) -> CapabilityDescriptor:
        return self._descriptor

    def script_unknown(self, *, landed: bool) -> None:
        """Enqueue: the next scripted ``cas_write`` returns ``CasUnknown``;
        ``landed`` applies the write to the store."""
        self._scripts.append(("unknown", landed))

    def script_conflict(self) -> None:
        """Enqueue: the next scripted ``cas_write`` returns ``CasConflict`` (no
        write landed)."""
        self._scripts.append(("conflict", False))

    def script_ghost_conflict(self) -> None:
        """Enqueue: the next scripted ``cas_write`` applies the write (my bytes
        land under a fresh token — an in-flight ghost) THEN returns ``CasConflict``,
        so the re-drive sees a moved token carrying its own intended bytes."""
        self._scripts.append(("ghost", True))

    def read(self, artifact_ref: str) -> tuple[bytes, str]:
        entry = self._store.get(artifact_ref)
        if entry is None:
            raise KeyError(artifact_ref)
        return entry

    def cas_write(
        self, artifact_ref: str, *, expected_token: str, new_bytes: bytes
    ) -> CasWriteResult:
        self.cas_calls.append((artifact_ref, expected_token, bytes(new_bytes)))
        if self._scripts:
            kind, landed = self._scripts.pop(0)
            if kind == "unknown":
                if landed:
                    self._store.set(artifact_ref, bytes(new_bytes))
                return CasUnknown()
            if kind == "ghost":
                # The in-flight ghost landed my bytes under a fresh token, then the
                # (late) response is a conflict — the re-drive must detect the ghost.
                self._store.set(artifact_ref, bytes(new_bytes))
                return CasConflict()
            return CasConflict()
        entry = self._store.get(artifact_ref)
        if entry is None or entry[1] != expected_token:
            return CasConflict()
        return CasWritten(token=self._store.set(artifact_ref, bytes(new_bytes)))

    def reconcile_after_unknown(
        self, artifact_ref: str, *, expected_token: str, intended_hash: str
    ) -> ReconcileDecision:
        if self.reconcile_hook is not None:
            self.reconcile_hook()
        entry = self._store.get(artifact_ref)
        if entry is None:
            return ReconcileDecision(ReconcileVerdict.HOLD, None, None)
        observed_bytes, observed_token = entry
        if observed_token == expected_token:
            return ReconcileDecision(ReconcileVerdict.RE_DRIVE, observed_bytes, observed_token)
        if _sha256(observed_bytes) == intended_hash:
            return ReconcileDecision(ReconcileVerdict.CONVERGE, observed_bytes, observed_token)
        # The token moved and the bytes differ: object → CONFLICT, row → RE_DERIVE.
        verdict = ReconcileVerdict.CONFLICT if self._arm == "object" else ReconcileVerdict.RE_DERIVE
        return ReconcileDecision(verdict, observed_bytes, observed_token)


def _agent(
    store: _FakeStore, session: SubstrateCoordinatorSession, *, arm: str = "object"
) -> tuple[CoordinatedSubstrate, _FakeSubstrate]:
    fake = _FakeSubstrate(store, arm)
    return CoordinatedSubstrate(fake, session), fake


def _session(tmp_path: Path, fast_cfg: LifecycleConfig) -> SubstrateCoordinatorSession:
    return SubstrateCoordinatorSession(tmp_path, managed=("**",), config=fast_cfg)


# --- happy: pull invalidation before act (LOAD-BEARING) ---------------------


def test_peer_commit_denies_next_act_before_write(
    tmp_path: Path, fast_cfg: LifecycleConfig
) -> None:
    store = _FakeStore()
    store.seed(REF, b"v1")
    sa, sb = _session(tmp_path, fast_cfg), _session(tmp_path, fast_cfg)
    try:
        a, fake_a = _agent(store, sa)
        b, _fake_b = _agent(store, sb)
        _a_bytes, a_tok = a.read(REF)
        _b_bytes, b_tok = b.read(REF)

        b.commit(REF, expected_token=b_tok, new_bytes=b"v2")  # B wins; A invalidated

        # A's NEXT binding-mediated act is DENIED as the uniform typed conflict,
        # BEFORE the substrate is touched — the case a bare CAS never surfaces.
        with pytest.raises(StaleView):
            a.commit(REF, expected_token=a_tok, new_bytes=b"v2-from-A")
        assert fake_a.cas_calls == []  # deny-before-act: no substrate write attempted
    finally:
        stop_coordinator(tmp_path)


def test_read_time_deny_surfaces_stale_view(
    tmp_path: Path, fast_cfg: LifecycleConfig
) -> None:
    store = _FakeStore()
    store.seed(REF, b"v1")
    sa, sb = _session(tmp_path, fast_cfg), _session(tmp_path, fast_cfg)
    try:
        a, _fa = _agent(store, sa)
        b, _fb = _agent(store, sb)
        a.read(REF)
        _b_bytes, b_tok = b.read(REF)
        b.commit(REF, expected_token=b_tok, new_bytes=b"v2")

        # on_stale='allow' (default) returns bytes; 'raise' surfaces StaleView.
        assert a.read(REF) == store.get(REF)
        with pytest.raises(StaleView):
            a.read(REF, on_stale="raise")
    finally:
        stop_coordinator(tmp_path)


def test_identity_stable_across_read_and_commit_fresh_after_reacquire(
    tmp_path: Path, fast_cfg: LifecycleConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = _FakeStore()
    store.seed(REF, b"v1")
    seen: list[str] = []
    real_post = substrate_module._coordinator_post

    def spy(endpoint, path, payload):  # noqa: ANN001, ANN202
        if path in ("/hooks/pre-read", "/hooks/post-edit-cas"):
            seen.append(payload["session_id"])
        return real_post(endpoint, path, payload)

    monkeypatch.setattr(substrate_module, "_coordinator_post", spy)
    sa = _session(tmp_path, fast_cfg)
    try:
        a, _fa = _agent(store, sa)
        before = a.session_id
        _bytes, tok = a.read(REF)
        a.commit(REF, expected_token=tok, new_bytes=b"v2")
        # The pre-read AND the post-edit-cas resolve to the SAME identity.
        assert set(seen) == {before}

        a.reacquire(REF)
        assert a.session_id != before  # a fresh id is minted ONLY on reacquire
    finally:
        stop_coordinator(tmp_path)


# --- divergence 1: coordinator-leg UNKNOWN (late-land / degrade) -------------


def test_divergence1_coordinator_leg_unknown_no_re_drive(
    tmp_path: Path, fast_cfg: LifecycleConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = _FakeStore()
    store.seed(REF, b"v1")
    real_post = substrate_module._coordinator_post

    def failing_commit(endpoint, path, payload):  # noqa: ANN001, ANN202
        if path == "/hooks/post-edit-cas":
            raise substrate_module.CoordinatorUnavailable("simulated commit timeout")
        return real_post(endpoint, path, payload)

    monkeypatch.setattr(substrate_module, "_coordinator_post", failing_commit)
    sa = _session(tmp_path, fast_cfg)
    try:
        a, fake_a = _agent(store, sa)
        _bytes, tok = a.read(REF)
        # Substrate write LANDS (WIN), but the coordinator bump times out.
        with pytest.raises(CommitUnconfirmed):
            a.commit(REF, expected_token=tok, new_bytes=b"v2")
        assert len(fake_a.cas_calls) == 1  # NEVER blind re-drive after a landed write
        assert store.get(REF)[0] == b"v2"  # the substrate write is durable
    finally:
        stop_coordinator(tmp_path)


# --- divergence 2: substrate UNKNOWN, per arm -------------------------------


def test_divergence2_converge_drives_bump_and_invalidates_peer(
    tmp_path: Path, fast_cfg: LifecycleConfig
) -> None:
    """PG-arm / never-converge-wedge negative: a landed-unknown write CONVERGES
    and its coordinator bump STILL fires — the peer is invalidated (the bump is
    not stranded)."""
    store = _FakeStore()
    store.seed(REF, b"v1")
    sa, sb = _session(tmp_path, fast_cfg), _session(tmp_path, fast_cfg)
    try:
        a, fake_a = _agent(store, sa, arm="row")
        b, _fb = _agent(store, sb, arm="row")
        _ab, _atok = a.read(REF)
        _bb, b_tok = b.read(REF)

        fake_a.script_unknown(landed=True)  # A's write lands but the ack is lost
        result = a.commit(REF, expected_token=_atok, new_bytes=b"v2")

        assert result.converged is True
        assert result.summary == "converged"  # never "landed" — attribution disclaimed
        assert len(fake_a.cas_calls) == 1  # no re-drive of a landed write
        # The converge bump fired → the peer is invalidated (not stranded).
        with pytest.raises(StaleView):
            b.commit(REF, expected_token=b_tok, new_bytes=b"vB")
    finally:
        stop_coordinator(tmp_path)


def test_divergence2_re_drive_under_held_token(
    tmp_path: Path, fast_cfg: LifecycleConfig
) -> None:
    """S3-arm: an UNKNOWN write that did NOT land (token unmoved) is re-driven
    ONCE under the held token, then lands and bumps."""
    store = _FakeStore()
    store.seed(REF, b"v1")
    sa = _session(tmp_path, fast_cfg)
    try:
        a, fake_a = _agent(store, sa, arm="object")
        _ab, a_tok = a.read(REF)

        fake_a.script_unknown(landed=False)  # not landed → token unmoved → RE_DRIVE
        result = a.commit(REF, expected_token=a_tok, new_bytes=b"v2")

        assert result.converged is False  # a clean re-drive win, not a converge
        assert len(fake_a.cas_calls) == 2  # initial (unknown) + one re-drive
        assert store.get(REF)[0] == b"v2"
    finally:
        stop_coordinator(tmp_path)


def test_divergence2_converge_complete_on_byte_identical_peer(
    tmp_path: Path, fast_cfg: LifecycleConfig
) -> None:
    """S3-arm: a landed-unknown write whose bump loses to a byte-IDENTICAL peer
    completes as converged (coordinator already holds the intended hash) — NO
    re-drive, NO second bump."""
    store = _FakeStore()
    store.seed(REF, b"v1")
    sa, sb = _session(tmp_path, fast_cfg), _session(tmp_path, fast_cfg)
    try:
        a, fake_a = _agent(store, sa, arm="object")
        b, _fb = _agent(store, sb, arm="object")
        _ab, a_tok = a.read(REF)
        _bb, _b_tok = b.read(REF)  # B is SHARED@v1 so it can bump the coordinator

        intended = _sha256(b"v2")

        def peer_bumps_first() -> None:
            # A byte-identical peer carries b"v2" to the coordinator first.
            sb.commit_cas(REF, expected_version=1, content_hash=intended)

        fake_a.script_unknown(landed=True)  # A's substrate write landed (b"v2")
        fake_a.reconcile_hook = peer_bumps_first
        result = a.commit(REF, expected_token=a_tok, new_bytes=b"v2")

        assert result.converged is True
        assert len(fake_a.cas_calls) == 1  # no re-drive, no second substrate write
    finally:
        stop_coordinator(tmp_path)


def test_divergence2_conflict_on_different_bytes(
    tmp_path: Path, fast_cfg: LifecycleConfig
) -> None:
    """S3-arm: an UNKNOWN write where the token moved to DIFFERENT bytes is a
    real peer conflict — typed, never re-driven."""
    store = _FakeStore()
    store.seed(REF, b"v1")
    sa = _session(tmp_path, fast_cfg)
    try:
        a, fake_a = _agent(store, sa, arm="object")
        _ab, a_tok = a.read(REF)

        fake_a.script_unknown(landed=False)
        store.set(REF, b"foreign")  # a foreign writer moved the substrate

        with pytest.raises(CasVersionConflict):
            a.commit(REF, expected_token=a_tok, new_bytes=b"v2")
        assert len(fake_a.cas_calls) == 1  # never re-driven
    finally:
        stop_coordinator(tmp_path)


# --- never-ship-a-store on the wire -----------------------------------------


def test_commit_wire_payload_carries_hash_not_content(
    tmp_path: Path, fast_cfg: LifecycleConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = _FakeStore()
    store.seed(REF, b"v1")
    captured: list[dict] = []
    real_post = substrate_module._coordinator_post

    def spy(endpoint, path, payload):  # noqa: ANN001, ANN202
        if path == "/hooks/post-edit-cas":
            captured.append(dict(payload))
        return real_post(endpoint, path, payload)

    monkeypatch.setattr(substrate_module, "_coordinator_post", spy)
    sa = _session(tmp_path, fast_cfg)
    try:
        a, _fa = _agent(store, sa)
        _bytes, tok = a.read(REF)
        a.commit(REF, expected_token=tok, new_bytes=b"v2")

        assert captured, "the commit must POST /hooks/post-edit-cas"
        for payload in captured:
            assert payload["content_hash"] == _sha256(b"v2")
            assert "content" not in payload  # bytes are NEVER sent
    finally:
        stop_coordinator(tmp_path)


# --- forbidden: coordinator-bump-first --------------------------------------


def test_coordinator_bump_first_is_forbidden(
    tmp_path: Path, fast_cfg: LifecycleConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A write that FAILS the substrate CAS must never reach the coordinator —
    so a peer is never invalidated for a write that did not land."""
    store = _FakeStore()
    store.seed(REF, b"v1")
    posts: list[str] = []
    real_post = substrate_module._coordinator_post

    def spy(endpoint, path, payload):  # noqa: ANN001, ANN202
        posts.append(path)
        return real_post(endpoint, path, payload)

    monkeypatch.setattr(substrate_module, "_coordinator_post", spy)
    sa, sb = _session(tmp_path, fast_cfg), _session(tmp_path, fast_cfg)
    try:
        a, _fa = _agent(store, sa)
        b, fake_b = _agent(store, sb)
        _ab, a_tok = a.read(REF)
        _bb, b_tok = b.read(REF)

        fake_b.script_conflict()  # B's substrate CAS fails
        with pytest.raises(CasVersionConflict):
            b.commit(REF, expected_token=b_tok, new_bytes=b"vB")
        # The failed substrate CAS never drove a coordinator bump.
        assert "/hooks/post-edit-cas" not in posts

        # A was NOT invalidated → A commits cleanly (proof the bump never fired).
        result = a.commit(REF, expected_token=a_tok, new_bytes=b"vA")
        assert result.version >= 2
    finally:
        stop_coordinator(tmp_path)


# --- crash-between-legs → coordinator-behind → ViewWedged -------------------


def test_crash_between_legs_surfaces_view_wedged(
    tmp_path: Path, fast_cfg: LifecycleConfig
) -> None:
    """A substrate write that lands while the coordinator bump is skipped (a
    crash between the legs) leaves the coordinator behind; a peer's next binding
    read is a wedged view. Recovery is reacquire (the carve-out: a non-re-reading
    peer stays unprotected)."""
    store = _FakeStore()
    store.seed(REF, b"v1")
    sa, sb = _session(tmp_path, fast_cfg), _session(tmp_path, fast_cfg)
    try:
        a, _fa = _agent(store, sa)
        b, _fb = _agent(store, sb)
        a.read(REF)  # seeds the coordinator artifact @ v1 / hash(v1)

        # A's substrate write lands but the coordinator bump never fires (crash).
        store.set(REF, b"v2-crashed")

        with pytest.raises(ViewWedged):
            b.read(REF)
        # reacquire recovers the fresh bytes without raising.
        rec_bytes, _rec_tok = b.reacquire(REF)
        assert rec_bytes == b"v2-crashed"
    finally:
        stop_coordinator(tmp_path)


# --- cross-substrate uniformity ---------------------------------------------


@pytest.mark.parametrize("arm", ["row", "object"])
def test_uniform_typed_conflict_across_substrates(
    tmp_path: Path, fast_cfg: LifecycleConfig, arm: str
) -> None:
    """The SAME typed deny (StaleView) fires for a row-shaped fake AND an
    object-shaped fake — the cross-substrate uniformity co-headline."""
    ref = f"workspace/{arm}.bin"
    store = _FakeStore()
    store.seed(ref, b"v1")
    sa, sb = _session(tmp_path, fast_cfg), _session(tmp_path, fast_cfg)
    try:
        a, _fa = _agent(store, sa, arm=arm)
        b, _fb = _agent(store, sb, arm=arm)
        _ab, a_tok = a.read(ref)
        _bb, b_tok = b.read(ref)
        b.commit(ref, expected_token=b_tok, new_bytes=b"v2")
        with pytest.raises(StaleView):
            a.commit(ref, expected_token=a_tok, new_bytes=b"vA")
    finally:
        stop_coordinator(tmp_path)


# --- admit-on-absent (the fence is inert by design) -------------------------


def test_admit_on_absent_occ_writer_commits(
    tmp_path: Path, fast_cfg: LifecycleConfig
) -> None:
    """v1 captures no read_generation, so the OCC writer sits on the fence's
    admit-on-absent path: a clean commit LANDS (the substrate CAS arbitrates) —
    no fence rejection is claimed."""
    store = _FakeStore()
    store.seed(REF, b"v1")
    sa = _session(tmp_path, fast_cfg)
    try:
        a, _fa = _agent(store, sa)
        _bytes, tok = a.read(REF)
        result = a.commit(REF, expected_token=tok, new_bytes=b"v2")
        assert result.version >= 2  # admitted; no stale_read_generation rejection
        assert result.converged is False
    finally:
        stop_coordinator(tmp_path)


# --- no-op: byte-identical commit mints no phantom advance ------------------


def test_noop_commit_touches_neither_leg(
    tmp_path: Path, fast_cfg: LifecycleConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Committing the exact bytes last observed advances NOTHING — no substrate
    write, no coordinator bump — so a byte-identical rewrite never invalidates a
    peer (Open Q C)."""
    store = _FakeStore()
    store.seed(REF, b"v1")
    posts: list[str] = []
    real_post = substrate_module._coordinator_post

    def spy(endpoint, path, payload):  # noqa: ANN001, ANN202
        posts.append(path)
        return real_post(endpoint, path, payload)

    monkeypatch.setattr(substrate_module, "_coordinator_post", spy)
    sa = _session(tmp_path, fast_cfg)
    try:
        a, fake_a = _agent(store, sa)
        _bytes, tok = a.read(REF)
        result = a.commit(REF, expected_token=tok, new_bytes=b"v1")  # identical

        assert result.noop is True
        assert result.summary == "unchanged"
        assert fake_a.cas_calls == []  # no substrate write
        assert "/hooks/post-edit-cas" not in posts  # no coordinator bump
    finally:
        stop_coordinator(tmp_path)


# --- re-drive: retry outcomes (the honesty boundary) ------------------------


def test_re_drive_retry_conflict_is_typed_conflict(
    tmp_path: Path, fast_cfg: LifecycleConfig
) -> None:
    """RE_DRIVE whose retry loses to a peer (token still unmoved for me) surfaces
    the typed conflict after EXACTLY two substrate writes — never a third, never a
    blind win."""
    store = _FakeStore()
    store.seed(REF, b"v1")
    sa = _session(tmp_path, fast_cfg)
    try:
        a, fake_a = _agent(store, sa, arm="object")
        _ab, a_tok = a.read(REF)

        fake_a.script_unknown(landed=False)  # 1st: unknown, not landed → RE_DRIVE
        fake_a.script_conflict()  # 2nd (the re-drive): conflict → typed conflict
        with pytest.raises(CasVersionConflict):
            a.commit(REF, expected_token=a_tok, new_bytes=b"v2")
        assert len(fake_a.cas_calls) == 2
    finally:
        stop_coordinator(tmp_path)


def test_re_drive_retry_second_unknown_is_unconfirmed(
    tmp_path: Path, fast_cfg: LifecycleConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A SECOND unknown on the re-drive fails closed (CommitUnconfirmed) — it does
    NOT loop unbounded and NEVER bumps the coordinator."""
    store = _FakeStore()
    store.seed(REF, b"v1")
    posts: list[str] = []
    real_post = substrate_module._coordinator_post

    def spy(endpoint, path, payload):  # noqa: ANN001, ANN202
        posts.append(path)
        return real_post(endpoint, path, payload)

    monkeypatch.setattr(substrate_module, "_coordinator_post", spy)
    sa = _session(tmp_path, fast_cfg)
    try:
        a, fake_a = _agent(store, sa, arm="object")
        _ab, a_tok = a.read(REF)

        fake_a.script_unknown(landed=False)  # 1st: unknown → RE_DRIVE
        fake_a.script_unknown(landed=False)  # 2nd: unknown again → fail-closed
        with pytest.raises(CommitUnconfirmed):
            a.commit(REF, expected_token=a_tok, new_bytes=b"v2")
        assert len(fake_a.cas_calls) == 2  # no third attempt
        assert "/hooks/post-edit-cas" not in posts  # never bumped
    finally:
        stop_coordinator(tmp_path)


def test_re_drive_detects_own_ghost_and_converges(
    tmp_path: Path, fast_cfg: LifecycleConfig
) -> None:
    """RE_DRIVE whose retry conflicts because MY OWN in-flight ghost put landed
    converges (my bytes are present) and drives the bump — not a misleading peer
    conflict."""
    store = _FakeStore()
    store.seed(REF, b"v1")
    sa = _session(tmp_path, fast_cfg)
    try:
        a, fake_a = _agent(store, sa, arm="object")
        _ab, a_tok = a.read(REF)

        fake_a.script_unknown(landed=False)  # 1st: unknown, not landed → RE_DRIVE
        fake_a.script_ghost_conflict()  # 2nd: ghost lands my bytes, THEN conflicts
        result = a.commit(REF, expected_token=a_tok, new_bytes=b"v2")

        assert result.converged is True  # my ghost carried my bytes
        assert len(fake_a.cas_calls) == 2
        assert store.get(REF)[0] == b"v2"
    finally:
        stop_coordinator(tmp_path)


# --- HOLD: absent operand wedges the view, never bumps ----------------------


def test_hold_verdict_wedges_without_bump(
    tmp_path: Path, fast_cfg: LifecycleConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An unknown write whose operand is ABSENT at reconcile (a raced delete)
    HOLDs → ViewWedged, and NEVER fires the coordinator bump (no phantom advance
    on a deleted operand)."""
    store = _FakeStore()
    store.seed(REF, b"v1")
    posts: list[str] = []
    real_post = substrate_module._coordinator_post

    def spy(endpoint, path, payload):  # noqa: ANN001, ANN202
        posts.append(path)
        return real_post(endpoint, path, payload)

    monkeypatch.setattr(substrate_module, "_coordinator_post", spy)
    sa = _session(tmp_path, fast_cfg)
    try:
        a, fake_a = _agent(store, sa, arm="object")
        _ab, a_tok = a.read(REF)

        fake_a.script_unknown(landed=False)
        fake_a.reconcile_hook = lambda: store.delete(REF)  # operand vanishes
        with pytest.raises(ViewWedged):
            a.commit(REF, expected_token=a_tok, new_bytes=b"v2")
        assert len(fake_a.cas_calls) == 1
        assert "/hooks/post-edit-cas" not in posts  # HOLD never bumps
    finally:
        stop_coordinator(tmp_path)


# --- converged/clean bump losing to a peer → typed conflict -----------------


def test_converged_bump_conflict_on_different_peer_raises(
    tmp_path: Path, fast_cfg: LifecycleConfig
) -> None:
    """A converged write whose bump loses to a DIFFERENT-bytes peer is a real
    conflict (the coordinator hash does not match my intended) — NOT a false
    converge that would mask a lost update."""
    store = _FakeStore()
    store.seed(REF, b"v1")
    sa, sb = _session(tmp_path, fast_cfg), _session(tmp_path, fast_cfg)
    try:
        a, fake_a = _agent(store, sa, arm="object")
        b, _fb = _agent(store, sb, arm="object")
        _ab, a_tok = a.read(REF)
        _bb, _b_tok = b.read(REF)  # B is SHARED@v1 so it can bump the coordinator

        def peer_bumps_different() -> None:
            sb.commit_cas(REF, expected_version=1, content_hash=_sha256(b"peer-different"))

        fake_a.script_unknown(landed=True)  # A's write landed (b"v2") → CONVERGE
        fake_a.reconcile_hook = peer_bumps_different
        with pytest.raises(CasVersionConflict):
            a.commit(REF, expected_token=a_tok, new_bytes=b"v2")
        assert len(fake_a.cas_calls) == 1  # no re-drive of a landed write
    finally:
        stop_coordinator(tmp_path)


def test_clean_win_bump_conflict_raises(
    tmp_path: Path, fast_cfg: LifecycleConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A clean substrate win whose coordinator bump loses to a peer (a peer bumped
    between this agent's pre-read and its bump) surfaces the typed conflict."""
    store = _FakeStore()
    store.seed(REF, b"v1")
    sa, sb = _session(tmp_path, fast_cfg), _session(tmp_path, fast_cfg)
    injected = {"done": False}
    real_post = substrate_module._coordinator_post

    def spy(endpoint, path, payload):  # noqa: ANN001, ANN202
        # Just before A's bump, let a peer bump the coordinator once (v1 → v2), so
        # A's clean-win bump at expected_version=1 conflicts. The guard keeps the
        # peer's own post-edit-cas from re-triggering the injection.
        if (
            path == "/hooks/post-edit-cas"
            and payload.get("session_id") == sa.session_id
            and not injected["done"]
        ):
            injected["done"] = True
            sb.commit_cas(REF, expected_version=1, content_hash=_sha256(b"peer"))
        return real_post(endpoint, path, payload)

    monkeypatch.setattr(substrate_module, "_coordinator_post", spy)
    try:
        a, fake_a = _agent(store, sa, arm="object")
        b, _fb = _agent(store, sb, arm="object")
        _ab, a_tok = a.read(REF)
        _bb, _b_tok = b.read(REF)  # B SHARED@v1 so its injected bump lands

        with pytest.raises(CasVersionConflict):
            a.commit(REF, expected_token=a_tok, new_bytes=b"vA")
        assert len(fake_a.cas_calls) == 1  # a clean win, no reconcile/re-drive
    finally:
        stop_coordinator(tmp_path)


# --- never-ship-a-store, made load-bearing at composition -------------------


def test_binding_declaring_it_sends_content_is_refused() -> None:
    """A binding that declares SENDS_CONTENT_TO_COORDINATOR=True is refused at
    composition — the never-ship-a-store floor is enforced, not merely declared."""

    class _ContentLeakingBinding:
        SENDS_CONTENT_TO_COORDINATOR = True

    with pytest.raises(CoherenceError):
        CoordinatedSubstrate(_ContentLeakingBinding(), object())  # type: ignore[arg-type]


# --- coordinator commit classification (fail-closed) ------------------------


def test_classify_commit_ok_is_win() -> None:
    result = substrate_module._classify_commit({"ok": True, "version": 5}, expected_version=4)
    assert isinstance(result, CoordinatorWin) and result.version == 5


def test_classify_commit_degraded_body_is_unconfirmed() -> None:
    with pytest.raises(CommitUnconfirmed):
        substrate_module._classify_commit({"ok": False, "degraded": True}, expected_version=4)


def test_classify_commit_unconfirmed_reason_is_unconfirmed() -> None:
    with pytest.raises(CommitUnconfirmed):
        substrate_module._classify_commit(
            {"ok": False, "reason": COMMIT_UNCONFIRMED_REASON}, expected_version=4
        )


def test_classify_commit_retryable_reason_is_conflict() -> None:
    result = substrate_module._classify_commit(
        {"ok": False, "reason": VERSION_MISMATCH_REASON, "current_version": 7},
        expected_version=4,
    )
    assert isinstance(result, CoordinatorConflict) and result.current_version == 7


def test_classify_commit_unknown_reason_fails_closed() -> None:
    with pytest.raises(CoherenceError):
        substrate_module._classify_commit({"ok": False, "reason": "mystery"}, expected_version=4)
