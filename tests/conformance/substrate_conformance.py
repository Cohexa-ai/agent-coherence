# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""The BYO-substrate tier-honesty conformance kit (Unit 7).

A descriptor-parametrized kit that certifies a substrate binding's declared tier
is HONEST and, crucially, that the **adapter's** invalidation value — not the
bare substrate compare-and-set — is what passes. The scenarios are plain
functions taking a :class:`ConformanceBinding` (a seam that mints binding views
over ONE shared artifact) and a session factory (an agent identity on ONE
coordinator). The default arm plugs an in-memory fake substrate; the
``real_substrate``-gated arm plugs the real ``CoherentRow`` / ``CoherentObject``
bindings against a real Postgres / S3 (see ``test_tier_honesty.py``).

The two load-bearing native-CAS scenarios are deliberately distinct:

- :func:`assert_racing_writers_one_winner` — a non-coordinated (fixed-stale)
  writer moves the substrate; the loser gets the uniform typed retryable conflict
  from the **substrate CAS**. This is the guarantee a bare CAS already gives —
  ``(i)`` alone is the bare CAS.
- :func:`assert_invalidation_before_act` — a peer commit denies the other's
  binding-mediated act with a typed ``StaleView`` **before** its write reaches
  the substrate CAS. The deny being ``StaleView`` (the coordinator's pull
  invalidation), NOT the substrate's ``CasVersionConflict``, is the proof that the
  coordinator layer is load-bearing — ``(ii)`` is what a bare CAS never surfaces.

The kit REJECTS an overclaiming binding: the split-comparand scenario
(:func:`assert_rejects_split_comparand`) is the PR-#107 negative control that a
version-CAS / ``NoLostUpdate`` check does not catch, and it MUST fail a
deliberately-split binding.

Deferred with the fence work (do NOT write speculatively): (iii) fence-rejection
and (iv) clock-domain sweep-liveness. Those assertions join the kit only when the
Phase-0 spike un-defers the read-generation fence — v1 bindings ride
admit-on-absent + the version-CAS and claim no fence.
"""

from __future__ import annotations

import hashlib
import sqlite3
from typing import Protocol, runtime_checkable
from uuid import UUID, uuid4

import pytest

import ccs.adapters.substrate as substrate_module
from ccs.adapters.substrate import (
    CasConflict,
    CasUnknown,
    CasWriteResult,
    CasWritten,
    CoordinatedSubstrate,
    ReconcileDecision,
    ReconcileVerdict,
    ReconcilingSubstrate,
    SubstrateCoordinatorSession,
)
from ccs.core.exceptions import CasVersionConflict, StaleView
from ccs.core.substrate import (
    CapabilityDescriptor,
    Tier,
    retention_is_empty_for,
)

# The wording a non-native tier must never carry: enforcement / CAS / rollback /
# duplicate-effect claims. Matched case-insensitively, so the guarantee text must
# express its disclaimers without these trigger words.
_FORBIDDEN_ENFORCEMENT_WORDS = ("enforce", "cas", "rollback", "dedup")


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _fresh_ref(prefix: str = "conformance") -> str:
    """A unique artifact ref, so sequential scenarios on ONE coordinator never
    collide on shared per-artifact state."""
    return f"{prefix}/{uuid4().hex}.bin"


# ===========================================================================
# The in-memory fake substrate (default arm) — defined HERE, never imported
# from the tests package, so the kit is self-contained.
# ===========================================================================


def _fake_descriptor(arm: str) -> CapabilityDescriptor:
    return CapabilityDescriptor(
        tier=Tier.NATIVE_CAS,
        version_source="fake row-version" if arm == "row" else "fake object ETag",
        least_privilege="in-memory conformance fake",
        consistency_note="fake single-primary",
    )


class InMemoryStore:
    """The shared substrate state (one row / one object per ref), shared by every
    binding view — distinct agents, one underlying artifact."""

    def __init__(self) -> None:
        self._data: dict[str, tuple[bytes, str]] = {}
        self._counter = 0

    def set(self, ref: str, data: bytes) -> str:
        self._counter += 1
        token = f"tok-{self._counter}"
        self._data[ref] = (bytes(data), token)
        return token

    def get(self, ref: str) -> tuple[bytes, str] | None:
        return self._data.get(ref)


class ConformanceSubstrate:
    """A per-agent binding view over the shared store, implementing the
    reconciling-substrate surface with realistic token-identity logic.

    Supports two test seams: :meth:`script_unknown` (force an UNKNOWN write, so
    the abort-after-partial-visibility reconciliation is reachable) and
    :meth:`schedule_peer_write` (a peer write that lands right after this view's
    atomic read — used by the split-comparand control, which OVERRIDES ``read``).
    """

    #: The body never reaches the coordinator (never-ship-a-store).
    SENDS_CONTENT_TO_COORDINATOR: bool = False

    def __init__(self, store: InMemoryStore, arm: str = "object") -> None:
        self._store = store
        self._arm = arm
        self._descriptor = _fake_descriptor(arm)
        self.cas_calls: list[tuple[str, str, bytes]] = []
        self._script: tuple[str, bool] | None = None
        self._scheduled_peer: bytes | None = None

    @property
    def descriptor(self) -> CapabilityDescriptor:
        return self._descriptor

    def coordinator_commit_content(self) -> None:
        return None

    def script_unknown(self, *, landed: bool) -> None:
        """Next ``cas_write`` returns ``CasUnknown``; ``landed`` applies the write."""
        self._script = ("unknown", landed)

    def schedule_peer_write(self, data: bytes) -> None:
        """A peer write that lands immediately AFTER this view's atomic read — so
        this (conforming) view's token is the PRE-peer token and a later CAS
        under it conflicts. The split control overrides ``read`` to capture the
        POST-peer token instead."""
        self._scheduled_peer = bytes(data)

    def read(self, artifact_ref: str) -> tuple[bytes, str]:
        entry = self._store.get(artifact_ref)
        if entry is None:
            raise KeyError(artifact_ref)
        # Atomic pair captured BEFORE any scheduled peer write lands.
        pair = entry
        if self._scheduled_peer is not None:
            self._store.set(artifact_ref, self._scheduled_peer)
            self._scheduled_peer = None
        return pair

    def cas_write(self, artifact_ref: str, *, expected_token: str, new_bytes: bytes) -> CasWriteResult:
        self.cas_calls.append((artifact_ref, expected_token, bytes(new_bytes)))
        script, self._script = self._script, None
        if script is not None:
            _kind, landed = script
            if landed:
                self._store.set(artifact_ref, bytes(new_bytes))
            return CasUnknown()
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
        verdict = ReconcileVerdict.CONFLICT if self._arm == "object" else ReconcileVerdict.RE_DERIVE
        return ReconcileDecision(verdict, observed_bytes, observed_token)


class SplitComparandSubstrate(ConformanceSubstrate):
    """The PR-#107 lost-update bug: ``(bytes, token)`` from TWO reads.

    ``read`` returns the bytes from read A but the token from a LATER read B, with
    a peer write landing between the two sub-reads. The returned token vouches for
    bytes it never described, so a subsequent CAS under it PASSES and silently
    loses the peer's write — the exact bug a version-CAS / ``NoLostUpdate`` check
    does not catch. The kit MUST reject this binding.
    """

    def read(self, artifact_ref: str) -> tuple[bytes, str]:
        entry = self._store.get(artifact_ref)
        if entry is None:
            raise KeyError(artifact_ref)
        bytes_a = entry[0]  # read A: bytes
        if self._scheduled_peer is not None:  # a peer writes BETWEEN the two reads
            self._store.set(artifact_ref, self._scheduled_peer)
            self._scheduled_peer = None
        fresh = self._store.get(artifact_ref)
        token_b = fresh[1] if fresh is not None else entry[1]  # read B: token only
        return bytes_a, token_b  # SPLIT: stale bytes, fresh token


class LwwSubstrate:
    """A detect-only substrate model: NO atomic compare-and-set — every write is
    last-write-wins. Used by the forced detect-only arm to demonstrate a SILENT
    lost update (the tier catches a sequential stale-read→write but cannot prevent
    a concurrent race)."""

    def __init__(self, store: InMemoryStore) -> None:
        self._store = store
        self._descriptor = CapabilityDescriptor(
            tier=Tier.DETECT_ONLY,
            version_source=None,
            least_privilege="in-memory detect-only fake",
            consistency_note="last-write-wins; no atomic CAS",
        )

    @property
    def descriptor(self) -> CapabilityDescriptor:
        return self._descriptor

    def read(self, artifact_ref: str) -> tuple[bytes, str]:
        entry = self._store.get(artifact_ref)
        if entry is None:
            raise KeyError(artifact_ref)
        return entry

    def cas_write(self, artifact_ref: str, *, expected_token: str, new_bytes: bytes) -> CasWriteResult:
        # No atomic CAS: expected_token is ignored, the write always lands.
        del expected_token
        return CasWritten(token=self._store.set(artifact_ref, bytes(new_bytes)))


# ===========================================================================
# The binding seam — the ONE abstraction every arm (fake + real) plugs into.
# ===========================================================================


@runtime_checkable
class ConformanceBinding(Protocol):
    """What a conformance run needs from a binding: a descriptor, a way to seed
    and non-coordinated-write the shared artifact, and a factory for fresh binding
    views over it. The in-memory fake and the real PG/S3 bindings both satisfy it.
    """

    @property
    def descriptor(self) -> CapabilityDescriptor: ...

    def seed(self, ref: str, data: bytes) -> None: ...

    def foreign_write(self, ref: str, data: bytes) -> None: ...

    def make_view(self) -> ReconcilingSubstrate: ...


class InMemoryBinding:
    """A :class:`ConformanceBinding` backed by the in-memory fake substrate."""

    def __init__(self, arm: str = "object") -> None:
        self._store = InMemoryStore()
        self._arm = arm

    @property
    def descriptor(self) -> CapabilityDescriptor:
        return _fake_descriptor(self._arm)

    def seed(self, ref: str, data: bytes) -> None:
        self._store.set(ref, data)

    def foreign_write(self, ref: str, data: bytes) -> None:
        # A non-coordinated (fixed-stale) writer moves the substrate WITHOUT
        # telling the coordinator — the racing-writer the substrate CAS arbitrates.
        self._store.set(ref, data)

    def make_view(self) -> ConformanceSubstrate:
        return ConformanceSubstrate(self._store, self._arm)


# ===========================================================================
# Native-CAS scenarios — substrate-agnostic (fake AND real_substrate).
# ===========================================================================


def assert_native_cas_descriptor(descriptor: CapabilityDescriptor) -> None:
    """The declared tier is native-CAS with the honest timeout-asterisk wording."""
    assert descriptor.tier is Tier.NATIVE_CAS
    assert descriptor.version_source, "native-CAS must name its version source"
    text = descriptor.guarantee_text
    assert "enforces no-lost-update on the version-CAS axis only" in text
    assert "token-identity reconciliation" in text
    assert "single-host" in text


def assert_racing_writers_one_winner(
    binding: ConformanceBinding, make_session: "SessionFactory"
) -> None:
    """(i) The SUBSTRATE CAS arbitrates: a non-coordinated writer moves the
    substrate; the loser gets the uniform typed retryable conflict. This is the
    guarantee a bare CAS already gives (it is NOT the coordinator's add)."""
    ref = _fresh_ref()
    binding.seed(ref, b"v1")
    agent = CoordinatedSubstrate(binding.make_view(), make_session())
    _bytes, tok = agent.read(ref)

    # A concurrent (non-coordinated) writer moves the substrate — the coordinator
    # is NOT told, so the pre-read stays clean and the substrate CAS is the arbiter.
    binding.foreign_write(ref, b"v2-foreign")

    with pytest.raises(CasVersionConflict):
        agent.commit(ref, expected_token=tok, new_bytes=b"vA")
    # The foreign write is the one winner; the loser's bytes never landed.
    assert binding.make_view().read(ref)[0] == b"v2-foreign"


def assert_invalidation_before_act(
    binding: ConformanceBinding, make_session: "SessionFactory"
) -> None:
    """(ii) The COORDINATOR layer denies a peer-invalidated act with ``StaleView``
    BEFORE the substrate CAS. The deny being ``StaleView`` (not the substrate's
    ``CasVersionConflict``) is the proof the coordinator layer is load-bearing —
    the case a bare CAS never surfaces."""
    ref = _fresh_ref()
    binding.seed(ref, b"v1")
    view_a = binding.make_view()
    agent_a = CoordinatedSubstrate(view_a, make_session())
    agent_b = CoordinatedSubstrate(binding.make_view(), make_session())

    _ab, a_tok = agent_a.read(ref)
    _bb, b_tok = agent_b.read(ref)
    agent_b.commit(ref, expected_token=b_tok, new_bytes=b"v2")  # B wins; A invalidated

    with pytest.raises(StaleView):
        agent_a.commit(ref, expected_token=a_tok, new_bytes=b"vA")
    # Deny-before-act: the substrate write was never attempted (fake-observable).
    if hasattr(view_a, "cas_calls"):
        assert view_a.cas_calls == []


def assert_never_ship_a_store_wire(
    binding: ConformanceBinding, make_session: "SessionFactory"
) -> None:
    """(a) Binding behavior: the commit path threads ``content=None`` and the wire
    payload carries the fixed-width ``content_hash`` but NO content."""
    view = binding.make_view()
    assert getattr(view, "SENDS_CONTENT_TO_COORDINATOR", None) is False
    assert view.coordinator_commit_content() is None

    ref = _fresh_ref()
    binding.seed(ref, b"v1")
    captured: list[dict] = []
    real_post = substrate_module._coordinator_post

    def spy(endpoint, path, payload):  # noqa: ANN001, ANN202
        if path == "/hooks/post-edit-cas":
            captured.append(dict(payload))
        return real_post(endpoint, path, payload)

    substrate_module._coordinator_post = spy
    try:
        agent = CoordinatedSubstrate(view, make_session())
        _bytes, tok = agent.read(ref)
        agent.commit(ref, expected_token=tok, new_bytes=b"v2")
    finally:
        substrate_module._coordinator_post = real_post

    assert captured, "a commit must POST /hooks/post-edit-cas"
    for payload in captured:
        assert "content" not in payload, "bytes are NEVER sent to the coordinator"
        assert isinstance(payload.get("content_hash"), str)


def run_native_cas_conformance(
    binding: ConformanceBinding, make_session: "SessionFactory"
) -> None:
    """The substrate-agnostic native-CAS arm — run against the fake AND real
    substrates. (i) proves the bare CAS; (ii) proves the coordinator's
    invalidation-before-act; (a) pins never-ship-a-store on the wire."""
    assert_native_cas_descriptor(binding.descriptor)
    assert_racing_writers_one_winner(binding, make_session)
    assert_invalidation_before_act(binding, make_session)
    assert_never_ship_a_store_wire(binding, make_session)


# ===========================================================================
# Fake-driven scenarios — abort reconciliation + the negative controls.
# ===========================================================================


def assert_abort_after_partial_visibility(
    binding: ConformanceBinding, make_session: "SessionFactory"
) -> None:
    """A commit whose substrate write LANDED but whose ack was aborted mid-commit
    must reconcile by token-identity (CONVERGE), never assume nothing landed."""
    ref = _fresh_ref()
    binding.seed(ref, b"v1")
    view = binding.make_view()
    if not hasattr(view, "script_unknown"):
        pytest.skip("abort-after-partial-visibility needs a scriptable substrate")

    agent = CoordinatedSubstrate(view, make_session())
    _bytes, tok = agent.read(ref)
    view.script_unknown(landed=True)  # the write lands; the ack aborts mid-commit

    result = agent.commit(ref, expected_token=tok, new_bytes=b"v2")
    assert result.converged is True, "an aborted-but-landed write must reconcile, not re-drive"
    assert result.summary == "converged"  # never "landed" — attribution disclaimed
    assert binding.make_view().read(ref)[0] == b"v2"  # the landed write is adopted, not lost


def assert_rejects_split_comparand(store_arm: str = "object") -> None:
    """The PR-#107 negative control (MANDATORY): a CONFORMING view passes the
    read-pair-consistency check; a SPLIT view silently loses the peer's write.

    A conforming view captures ``(bytes, token)`` atomically, so a peer write that
    lands after the read leaves its CAS-comparand STALE → the later CAS conflicts →
    the peer's write survives. A split view captures the token from a later read,
    so its CAS PASSES on a token that never described its bytes → the peer's write
    is silently lost. This function ASSERTS "the peer's write survives"; it PASSES
    for the conforming view and RAISES ``AssertionError`` for the split view (the
    test wraps the split call in ``pytest.raises(AssertionError)``).
    """
    _run_split_control(ConformanceSubstrate, store_arm)


def assert_split_view_is_rejected(store_arm: str = "object") -> None:
    """Confirm the split view fails the control (helper for the MUST-FAIL test)."""
    with pytest.raises(AssertionError):
        _run_split_control(SplitComparandSubstrate, store_arm)


def _run_split_control(view_cls: type[ConformanceSubstrate], arm: str) -> None:
    ref = _fresh_ref()
    store = InMemoryStore()
    store.set(ref, b"base")
    view = view_cls(store, arm)
    view.schedule_peer_write(b"peer-change")

    _bytes, token = view.read(ref)
    result = view.cas_write(ref, expected_token=token, new_bytes=b"derived-from-stale")

    final = store.get(ref)[0]
    assert final == b"peer-change" and isinstance(result, CasConflict), (
        "a split comparand silently lost the peer's write (PR-#107): the CAS "
        "passed on a token that vouched for bytes it never described"
    )


def assert_detect_only_silent_lost_update() -> None:
    """The forced detect-only arm: force the interleave and prove the lost update
    ACTUALLY occurred and was SILENT (no raise) — "not prevented" demonstrated,
    not merely unobserved. Also pins the descriptor's honest (non-enforcement)
    tier text."""
    ref = _fresh_ref()
    store = InMemoryStore()
    store.set(ref, b"v1")
    sub = LwwSubstrate(store)

    _bytes, tok = sub.read(ref)  # two writers both read v1
    # Forced interleave: a peer write lands, then the racing writer writes under
    # the SAME (now stale) token. A detect-only substrate has no atomic CAS, so
    # BOTH land — the peer's write is silently lost.
    peer_result = sub.cas_write(ref, expected_token=tok, new_bytes=b"peer")
    racer_result = sub.cas_write(ref, expected_token=tok, new_bytes=b"racer")

    assert isinstance(peer_result, CasWritten) and isinstance(racer_result, CasWritten)
    assert store.get(ref)[0] == b"racer", "the racer silently overwrote the peer (a race not prevented)"
    # The descriptor's skip-with-reason: detection wording, never enforcement.
    text = sub.descriptor.guarantee_text.lower()
    assert "catches a sequential stale-read" in text
    assert "cannot prevent a concurrent race" in text
    for word in _FORBIDDEN_ENFORCEMENT_WORDS:
        assert word not in text, f"detect-only text must not contain {word!r}"


def assert_forward_only_honest() -> None:
    """The forward-only arm (descriptor-level, pre-network): effect-ordering-only
    text with NO enforcement/CAS/rollback/dedup wording, and the no-version_source
    validation pinned. No binding ships for this tier in v1."""
    descriptor = CapabilityDescriptor(
        tier=Tier.FORWARD_ONLY,
        version_source=None,
        least_privilege="an action-backend credential",
        consistency_note="the effect is forward-only",
    )
    text = descriptor.guarantee_text.lower()
    assert "effect ordering only" in text
    assert "freshness" in text
    assert "deny-before-act" in text
    for word in _FORBIDDEN_ENFORCEMENT_WORDS:
        assert word not in text, f"forward-only text must not contain {word!r}"
    # A forward-only descriptor declaring a version_source is rejected: an action
    # mints no token to compare.
    with pytest.raises(ValueError, match="version_source"):
        CapabilityDescriptor(tier=Tier.FORWARD_ONLY, version_source="etag")


# ===========================================================================
# never-ship-a-store — the coordinator RETENTION backstop (b).
# ===========================================================================


def _dump_retention_rows(db_path) -> list[dict]:
    """Dump the coordinator's ``artifact_versions`` table (data-in for the
    ``retention_is_empty_for`` predicate). Any row is a content-proportional
    shadow the floor forbids for a binding's artifacts."""
    con = sqlite3.connect(str(db_path))
    try:
        rows = con.execute("SELECT artifact_id, version FROM artifact_versions").fetchall()
    finally:
        con.close()
    return [{"artifact_id": UUID(hex=aid), "version": version} for aid, version in rows]


def assert_coordinator_retention_empty(tmp_path) -> None:
    """(b) Coordinator backstop, WITH ``retain_versions=True`` (else it false-greens
    exactly when it matters): the binding's hash-only registration leg leaves the
    ``artifact_versions`` table EMPTY, while a content-bearing register does NOT —
    proving the zero-rows result is a real property, not a retention-off vacuum.

    Note: the row-count probe cannot see a hypothetical NEW content-proportional
    store OUTSIDE ``artifact_versions`` — that is the core ``never_ship_a_store``
    predicate's job (Unit 1) plus the per-binding review checklist.
    """
    from ccs.coordinator.sqlite_registry import SqliteArtifactRegistry
    from ccs.core.types import Artifact

    db_path = tmp_path / "state.db"
    with SqliteArtifactRegistry(db_path, retain_versions=True) as reg:
        # The binding registers via the hash-only resolve_or_register path — never
        # register_artifact(artifact, content), which lands a v1 body under retention.
        binding_id = reg.resolve_or_register("byo/row", "a" * 64)
        rows = _dump_retention_rows(db_path)
        assert retention_is_empty_for(rows, [binding_id]), (
            "the binding's hash-only registration must leave artifact_versions empty"
        )

        # Teeth: a content-bearing register DOES capture a body row under the same
        # retention — so the zero-rows result above cannot be a vacuous pass.
        bearing = Artifact(id=uuid4(), name="content-bearing", version=1, content_hash="b" * 64)
        reg.register_artifact(bearing, content="a body")
        rows_after = _dump_retention_rows(db_path)
        assert not retention_is_empty_for(rows_after, [bearing.id]), (
            "retention must be ACTIVE — a content-bearing register must leave a row"
        )


# ===========================================================================
# Session harness — a factory that mints agent identities over ONE coordinator.
# ===========================================================================


@runtime_checkable
class SessionFactory(Protocol):
    """Mint a fresh :class:`SubstrateCoordinatorSession` (a new agent identity) on
    ONE coordinator root. Two calls are two agents sharing one artifact."""

    def __call__(self) -> SubstrateCoordinatorSession: ...


class CoordinatorHarness:
    """Owns the coordinator lifecycle for a conformance run: each call mints a new
    agent identity on ONE root; :meth:`close` tears the coordinator down."""

    def __init__(self, root, config) -> None:
        from pathlib import Path

        self._root = Path(root)
        self._config = config

    def __call__(self) -> SubstrateCoordinatorSession:
        return SubstrateCoordinatorSession(self._root, managed=("**",), config=self._config)

    def close(self) -> None:
        from ccs.adapters.claude_code.lifecycle import stop_coordinator

        stop_coordinator(self._root)


# ---------------------------------------------------------------------------
# real_substrate note (MinIO): the racing-writers + invalidation assertions ALSO
# run against real Postgres AND real S3 when the `real_substrate` marker is
# selected and credentials are present (CCS_TEST_PG_DSN / CCS_REAL_S3_BUCKET) —
# wired in test_tier_honesty.py. Moto/LocalStack are excluded (they serialize →
# concurrency false-greens). For a local S3-compatible fixture, PIN MinIO to its
# FINAL release tag/digest (upstream archived read-only 2026-04-25 — API drift is
# impossible, but so are CVE fixes; never expose it) — its conditional-write
# semantics are adequate (real 412s, atomic, wildcard If-None-Match) EXCEPT it
# documents no 409 ConditionalRequestConflict path, so the 409-retry branch is
# stub-tested locally and integration-verified against real AWS only. Do NOT stand
# up MinIO here — this note is the record; the env-gated tests do the rest.
# ---------------------------------------------------------------------------
