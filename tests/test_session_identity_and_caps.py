# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Unit 7 — session identity, isolation & resource caps (SB-17 / TX-1).

Plan: ``docs/plans/2026-06-26-002-feat-read-side-transaction-snapshot-plan.md``
Unit 7. Requirement trace: **R13** (owner-isolation: a SIBLING agent cannot read
or commit another session's cut, even with a leaked token — the per-call owner
check is TIMING-SAFE) and **R14** (resource caps bound the snapshot blast radius:
``max_sessions`` / ``max_read_set_cardinality`` / ``absolute_age_ticks``). Builds
on Units 2-5: ``begin_session`` / cut-capture (U2), ``session_read`` (U3),
``session_commit`` (U4), the heartbeat-lease + liveness sweep (U5).

Two axes under test:

- **Identity / isolation (R13).** ``session_read`` / ``session_commit`` take a
  REQUIRED keyword-only ``caller`` validated against the owner bound at
  ``begin_session`` via :func:`hmac.compare_digest` over the stable 16-byte
  ``UUID.bytes`` encoding (never ``==``). A FOREIGN caller fails closed by RAISE
  (:class:`SessionInvalidated`); the OWNER's own calls succeed; an owned-but-
  pinless token fails closed with the typed ``session_invalidated`` reason, never
  served.
- **Caps (R14).** ``begin_session`` rejects (typed RETURN, never an exception)
  when opening the session would exceed ``max_sessions`` concurrent sessions
  (``session_cap_exceeded``) or when ``read_set`` exceeds
  ``max_read_set_cardinality`` (``read_set_too_large``); the liveness sweep reaps
  a session past ``absolute_age_ticks`` EVEN WITH a fresh heartbeat (the hard age
  ceiling, threat-model #3). Reason matching is ALWAYS ``reason == CONSTANT``
  against the imported wire-stable constant — never a substring of a human
  message (the typed-signal-not-substring house rule).

Timing uses FIXED logical ticks (no wall-clock sleeps), mirroring
tests/test_session_lifetime.py. Caps are made REACHABLE in-test by constructing
the service with a SMALL :class:`SessionCapsConfig` rather than the
security-calibrated defaults.
"""

from __future__ import annotations

import hmac
import inspect
from pathlib import Path
from uuid import UUID, uuid4

import pytest

from ccs.coordinator.registry import ArtifactRegistry
from ccs.coordinator.retention import RetentionPolicy
from ccs.coordinator.service import (
    CoordinatorService,
    SessionCapsConfig,
)
from ccs.coordinator.sqlite_registry import SqliteArtifactRegistry
from ccs.core.exceptions import (
    SESSION_BEGIN_CAP_REASONS,
    SESSION_CAP_EXCEEDED_REASON,
    SESSION_INVALIDATED_REASON,
    SESSION_READ_SET_TOO_LARGE_REASON,
    SessionInvalidated,
)
from ccs.core.types import (
    Artifact,
    SessionCommitRejection,
    SessionReadRejection,
    SnapshotSession,
    VersionedContent,
    VersionedReadRejection,
)

# Timing constants — fixed logical ticks (no wall-clock sleeps).
_HB_TIMEOUT = 120


# ---------------------------------------------------------------------------
# Builders — mirror tests/test_session_read.py / test_session_commit.py.
# ---------------------------------------------------------------------------


def _register(reg, artifact_id: UUID, name: str, body: str, version: int = 1) -> None:
    art = Artifact(id=artifact_id, name=name, version=version, content_hash="h1")
    reg.register_artifact(art, content=body)


def _registries(tmp_path: Path):
    """An (in_memory, sqlite) LAZY pair — ``retain_versions=True`` so the
    served-body assertions are concrete on both arms."""
    pol = RetentionPolicy(max_versions=8)
    mem = ArtifactRegistry(retain_versions=True, retention_policy=pol)
    sql = SqliteArtifactRegistry(
        tmp_path / "snap_identity.db", retain_versions=True, retention_policy=pol
    )
    return mem, sql


def _small_caps() -> SessionCapsConfig:
    """A SMALL caps config so every R14 bound is reachable in a unit test (the
    security-calibrated defaults of 256 / 64 / 3600 are not)."""
    return SessionCapsConfig(
        max_sessions=3,
        max_read_set_cardinality=2,
        absolute_age_ticks=1000,
    )


# ===========================================================================
# A — Owner isolation (R13): a FOREIGN caller is rejected; the OWNER succeeds.
# ===========================================================================


class TestOwnerIsolation:
    @pytest.mark.parametrize("arm", ["in_memory", "sqlite"])
    def test_foreign_caller_read_raises_session_invalidated(
        self, tmp_path: Path, arm: str
    ) -> None:
        """A sibling agent that did not open the session CANNOT read its cut, even
        with the (leaked) token — the per-call owner check fails CLOSED by RAISE,
        never a live serve."""
        mem, sql = _registries(tmp_path)
        reg = mem if arm == "in_memory" else sql
        try:
            svc = CoordinatorService(reg)
            owner = uuid4()
            foreign = uuid4()
            a = uuid4()
            _register(reg, a, "plan.md", "PINNED-V1")
            session = svc.begin_session(read_set=[a], owner=owner)
            assert isinstance(session, SnapshotSession)
            tok = session.session_token

            with pytest.raises(SessionInvalidated) as exc:
                svc.session_read(tok, a, caller=foreign)
            assert exc.value.reason == SESSION_INVALIDATED_REASON

            # The OWNER's own read still serves the pinned bytes.
            r = svc.session_read(tok, a, caller=owner)
            assert isinstance(r, VersionedContent)
            assert r.content == "PINNED-V1"
        finally:
            sql.close()

    @pytest.mark.parametrize("arm", ["in_memory", "sqlite"])
    def test_foreign_caller_commit_raises_session_invalidated(
        self, tmp_path: Path, arm: str
    ) -> None:
        """A sibling cannot commit against another's cut — the foreign
        ``session_commit`` RAISES before any OCC arbitration, and the artifact is
        UNTOUCHED. The owner's own commit then WINS."""
        mem, sql = _registries(tmp_path)
        reg = mem if arm == "in_memory" else sql
        try:
            svc = CoordinatorService(reg)
            owner = uuid4()
            foreign = uuid4()
            a = uuid4()
            _register(reg, a, "plan.md", "V1")
            session = svc.begin_session(read_set=[a], owner=owner)
            tok = session.session_token

            with pytest.raises(SessionInvalidated) as exc:
                svc.session_commit(tok, a, "FOREIGN-WRITE", caller=foreign)
            assert exc.value.reason == SESSION_INVALIDATED_REASON
            # The foreign commit never landed: still at v1.
            assert reg.get_artifact(a).version == 1

            # The OWNER's own commit WINS.
            result = svc.session_commit(tok, a, "OWNER-WRITE", caller=owner)
            assert isinstance(result, tuple)
            updated, _signals = result
            assert updated.version == 2
        finally:
            sql.close()

    @pytest.mark.parametrize("arm", ["in_memory", "sqlite"])
    def test_sibling_with_own_session_cannot_cross_read(
        self, tmp_path: Path, arm: str
    ) -> None:
        """Two agents each hold their OWN session. Neither can read the other's
        cut: cross-agent access is OUT (R13), even though each is a legitimate
        owner of a DIFFERENT token."""
        mem, sql = _registries(tmp_path)
        reg = mem if arm == "in_memory" else sql
        try:
            svc = CoordinatorService(reg)
            owner_a = uuid4()
            owner_b = uuid4()
            a = uuid4()
            _register(reg, a, "plan.md", "BODY")
            sess_a = svc.begin_session(read_set=[a], owner=owner_a)
            sess_b = svc.begin_session(read_set=[a], owner=owner_b)

            # A reads its own cut fine, but cannot read B's token (and vice versa).
            assert isinstance(
                svc.session_read(sess_a.session_token, a, caller=owner_a),
                VersionedContent,
            )
            with pytest.raises(SessionInvalidated):
                svc.session_read(sess_b.session_token, a, caller=owner_a)
            with pytest.raises(SessionInvalidated):
                svc.session_read(sess_a.session_token, a, caller=owner_b)
        finally:
            sql.close()


# ===========================================================================
# B — Timing-safe owner compare (R13): a near-miss owner UUID is rejected; the
#     validator uses hmac.compare_digest, never ``==``.
# ===========================================================================


class TestTimingSafeOwnerCompare:
    def test_near_miss_owner_is_rejected(self, tmp_path: Path) -> None:
        """An owner id that differs by a SINGLE byte is still foreign → rejected.
        A naive prefix/short-circuit compare could leak match-progress; the
        all-but-one-byte match must NOT be admitted."""
        reg = ArtifactRegistry(retain_versions=True)
        svc = CoordinatorService(reg)
        a = uuid4()
        _register(reg, a, "plan.md", "BODY")

        owner_bytes = bytearray(uuid4().bytes)
        owner = UUID(bytes=bytes(owner_bytes))
        # Flip the LAST byte only — a near-miss that matches all but one byte.
        near_miss_bytes = bytearray(owner_bytes)
        near_miss_bytes[-1] ^= 0x01
        near_miss = UUID(bytes=bytes(near_miss_bytes))
        assert near_miss != owner

        session = svc.begin_session(read_set=[a], owner=owner)
        with pytest.raises(SessionInvalidated):
            svc.session_read(session.session_token, a, caller=near_miss)
        # The genuine owner still succeeds.
        assert isinstance(
            svc.session_read(session.session_token, a, caller=owner),
            VersionedContent,
        )

    def test_owner_validation_uses_compare_digest_not_equality(self) -> None:
        """Pin the timing-safe property at the source: the per-call owner
        validator's body references :func:`hmac.compare_digest`, never a bare
        ``==`` on the owner ids. (A regression to ``==`` would be a timing-side-
        channel; this guards the constant-time path explicitly.)"""
        src = inspect.getsource(CoordinatorService._validate_session_owner)
        assert "compare_digest" in src
        # The compare is over the stable 16-byte UUID encoding.
        assert ".bytes" in src
        # Sanity that the module imported the timing-safe primitive at all.
        assert hasattr(hmac, "compare_digest")


# ===========================================================================
# C — Owned-but-pinless fails closed (never served).
# ===========================================================================


class TestOwnedButPinlessFailsClosed:
    def test_owned_but_pinless_read_is_session_invalidated(
        self, tmp_path: Path
    ) -> None:
        """A token that is OWNED but carries NO live pins (an owner binding
        recorded with the cut dropped out from under it) is NOT served: the owner
        passes the isolation check, then the cut-absent path fails CLOSED with the
        wire-stable ``session_invalidated``, never a live-HEAD fall-through."""
        reg = ArtifactRegistry(retain_versions=True)
        svc = CoordinatorService(reg)
        owner = uuid4()
        a = uuid4()
        _register(reg, a, "plan.md", "PINNED-V1")
        session = svc.begin_session(read_set=[a], owner=owner)
        tok = session.session_token

        # Drop the pins but KEEP the owner binding → owned-but-pinless.
        reg.release_session(tok)
        assert reg.get_session_cut(tok) is None
        assert svc._session_owners.get(tok) == owner  # still owned

        r = svc.session_read(tok, a, caller=owner)
        assert isinstance(r, SessionReadRejection)
        assert r.reason == SESSION_INVALIDATED_REASON
        # Never served live HEAD.
        assert not isinstance(r, VersionedContent)

    def test_owned_but_pinless_commit_is_session_invalidated(
        self, tmp_path: Path
    ) -> None:
        """Same owned-but-pinless token on the commit path: a typed rejection
        (``session_invalidated``), never a live-HEAD commit. The artifact stays
        at its pre-session version."""
        reg = ArtifactRegistry(retain_versions=True)
        svc = CoordinatorService(reg)
        owner = uuid4()
        a = uuid4()
        _register(reg, a, "plan.md", "V1")
        session = svc.begin_session(read_set=[a], owner=owner)
        tok = session.session_token
        reg.release_session(tok)
        assert svc._session_owners.get(tok) == owner

        rc = svc.session_commit(tok, a, "WOULD-BE", caller=owner)
        assert isinstance(rc, SessionCommitRejection)
        assert rc.reason == SESSION_INVALIDATED_REASON
        assert reg.get_artifact(a).version == 1

    def test_foreign_caller_on_pinless_token_still_raises(
        self, tmp_path: Path
    ) -> None:
        """Isolation is checked BEFORE the cut-absent classification: a FOREIGN
        caller on an owned-but-pinless token RAISES (the isolation breach), not a
        benign ``session_invalidated`` rejection — fail-closed dominates."""
        reg = ArtifactRegistry(retain_versions=True)
        svc = CoordinatorService(reg)
        owner = uuid4()
        foreign = uuid4()
        a = uuid4()
        _register(reg, a, "plan.md", "V1")
        session = svc.begin_session(read_set=[a], owner=owner)
        tok = session.session_token
        reg.release_session(tok)  # pinless, but still owner-bound

        with pytest.raises(SessionInvalidated):
            svc.session_read(tok, a, caller=foreign)


# ===========================================================================
# D — Resource caps (R14): max_sessions, max_read_set_cardinality.
# ===========================================================================


class TestSessionCountCap:
    @pytest.mark.parametrize("arm", ["in_memory", "sqlite"])
    def test_opening_one_past_max_sessions_is_rejected(
        self, tmp_path: Path, arm: str
    ) -> None:
        """Opening ``max_sessions + 1`` concurrent sessions → the (N+1)th is a
        TYPED ``session_cap_exceeded`` rejection (no token minted, no cut pinned),
        never an exception. Releasing one frees a slot for a later open."""
        mem, sql = _registries(tmp_path)
        reg = mem if arm == "in_memory" else sql
        caps = _small_caps()  # max_sessions == 3
        try:
            svc = CoordinatorService(reg, session_caps=caps)
            a = uuid4()
            _register(reg, a, "plan.md", "BODY")

            tokens = []
            for _ in range(caps.max_sessions):
                s = svc.begin_session(read_set=[a], owner=uuid4())
                assert isinstance(s, SnapshotSession)
                tokens.append(s.session_token)

            # The (N+1)th is rejected — typed, no half-open session.
            rejected = svc.begin_session(read_set=[a], owner=uuid4())
            assert isinstance(rejected, VersionedReadRejection)
            assert rejected.reason == SESSION_CAP_EXCEEDED_REASON
            assert rejected.reason in SESSION_BEGIN_CAP_REASONS
            # The cap (not any artifact) is reported; current == the cap.
            assert rejected.current_version == caps.max_sessions
            assert rejected.coordinator_epoch == reg.coordinator_epoch

            # Free a slot → a subsequent open succeeds again.
            reg.release_session(tokens[0])
            svc._session_owners.pop(tokens[0], None)
            s_again = svc.begin_session(read_set=[a], owner=uuid4())
            assert isinstance(s_again, SnapshotSession)
        finally:
            sql.close()


class TestReadSetCardinalityCap:
    @pytest.mark.parametrize("arm", ["in_memory", "sqlite"])
    def test_read_set_over_cardinality_is_rejected(
        self, tmp_path: Path, arm: str
    ) -> None:
        """A ``read_set`` larger than ``max_read_set_cardinality`` → a TYPED
        ``read_set_too_large`` rejection (no token minted, no cut pinned). A
        read_set AT the cap still opens."""
        mem, sql = _registries(tmp_path)
        reg = mem if arm == "in_memory" else sql
        caps = _small_caps()  # max_read_set_cardinality == 2
        try:
            svc = CoordinatorService(reg, session_caps=caps)
            ids = [uuid4() for _ in range(caps.max_read_set_cardinality + 1)]
            for i, aid in enumerate(ids):
                _register(reg, aid, f"a{i}.md", "BODY")

            # AT the cap: opens fine.
            at_cap = svc.begin_session(
                read_set=ids[: caps.max_read_set_cardinality], owner=uuid4()
            )
            assert isinstance(at_cap, SnapshotSession)

            # OVER the cap: typed rejection, no session.
            rejected = svc.begin_session(read_set=ids, owner=uuid4())
            assert isinstance(rejected, VersionedReadRejection)
            assert rejected.reason == SESSION_READ_SET_TOO_LARGE_REASON
            assert rejected.reason in SESSION_BEGIN_CAP_REASONS
            # The rejection is about CARDINALITY, leaking no member id.
            assert rejected.requested_version == len(ids)
            assert rejected.current_version == caps.max_read_set_cardinality
        finally:
            sql.close()


# ===========================================================================
# E — Absolute-age ceiling (R14, threat-model #3): a session past
#     ``absolute_age_ticks`` is reaped EVEN WITH a fresh heartbeat.
# ===========================================================================


class TestAbsoluteAgeCeiling:
    @pytest.mark.parametrize("arm", ["in_memory", "sqlite"])
    def test_over_age_session_reaped_despite_fresh_heartbeat(
        self, tmp_path: Path, arm: str
    ) -> None:
        """The hard age ceiling is INDEPENDENT of the heartbeat lease. Heartbeat
        at the CURRENT sweep tick (so the lease is maximally fresh), then sweep at
        ``created_at + absolute_age_ticks``: the session is STILL reaped — a live
        heartbeat must not exempt the ceiling (bounds heartbeat-spoofing DoS).
        After the reap, read/commit fail closed with ``session_invalidated``."""
        mem, sql = _registries(tmp_path)
        reg = mem if arm == "in_memory" else sql
        caps = _small_caps()  # absolute_age_ticks == 1000
        try:
            svc = CoordinatorService(reg, session_caps=caps)
            owner = uuid4()
            a = uuid4()
            _register(reg, a, "plan.md", "PINNED-V1")
            created = 0
            session = svc.begin_session(
                read_set=[a], owner=owner, created_at_tick=created
            )
            tok = session.session_token
            sweep_tick = created + caps.absolute_age_ticks  # exactly at the ceiling

            # A MAXIMALLY-FRESH heartbeat: lease == the sweep tick, so the
            # heartbeat-staleness predicate alone would NOT reap it.
            assert svc.record_session_heartbeat(
                session_token=tok, owner=owner, now_tick=sweep_tick
            ) is True
            assert (sweep_tick - sweep_tick) < _HB_TIMEOUT  # lease is fresh

            reaped = svc.enforce_session_liveness(
                current_tick=sweep_tick, heartbeat_timeout_ticks=_HB_TIMEOUT
            )
            assert reaped == 1, "over-age session was NOT reaped despite the ceiling"

            # Pins released; read + commit fail closed (never live HEAD).
            assert reg.get_session_cut(tok) is None
            r = svc.session_read(tok, a, caller=owner)
            assert isinstance(r, SessionReadRejection)
            assert r.reason == SESSION_INVALIDATED_REASON
            rc = svc.session_commit(tok, a, "WOULD-BE", caller=owner)
            assert isinstance(rc, SessionCommitRejection)
            assert rc.reason == SESSION_INVALIDATED_REASON
        finally:
            sql.close()

    @pytest.mark.parametrize("arm", ["in_memory", "sqlite"])
    def test_just_under_age_with_fresh_heartbeat_survives(
        self, tmp_path: Path, arm: str
    ) -> None:
        """The boundary is ``>=``: one tick BEFORE the ceiling, with a fresh
        heartbeat, the session is NOT reaped — the ceiling does not fire early and
        the live lease keeps it alive."""
        mem, sql = _registries(tmp_path)
        reg = mem if arm == "in_memory" else sql
        caps = _small_caps()
        try:
            svc = CoordinatorService(reg, session_caps=caps)
            owner = uuid4()
            a = uuid4()
            _register(reg, a, "plan.md", "BODY")
            created = 0
            session = svc.begin_session(
                read_set=[a], owner=owner, created_at_tick=created
            )
            tok = session.session_token
            sweep_tick = created + caps.absolute_age_ticks - 1  # one BEFORE ceiling

            assert svc.record_session_heartbeat(
                session_token=tok, owner=owner, now_tick=sweep_tick
            ) is True
            reaped = svc.enforce_session_liveness(
                current_tick=sweep_tick, heartbeat_timeout_ticks=_HB_TIMEOUT
            )
            assert reaped == 0
            # Still serving the pinned bytes.
            assert isinstance(
                svc.session_read(tok, a, caller=owner), VersionedContent
            )
        finally:
            sql.close()


# ===========================================================================
# F — Happy path: owner calls within caps proceed (read + commit succeed).
# ===========================================================================


class TestHappyWithinCaps:
    @pytest.mark.parametrize("arm", ["in_memory", "sqlite"])
    def test_owner_read_and_commit_within_caps(
        self, tmp_path: Path, arm: str
    ) -> None:
        """With a small caps config but a within-bounds request, the OWNER's read
        serves the pinned bytes and the commit WINS — caps and isolation impose no
        cost on the legitimate owner."""
        mem, sql = _registries(tmp_path)
        reg = mem if arm == "in_memory" else sql
        caps = _small_caps()
        try:
            svc = CoordinatorService(reg, session_caps=caps)
            owner = uuid4()
            a = uuid4()
            b = uuid4()
            _register(reg, a, "plan.md", "PLAN-V1")
            _register(reg, b, "budget.md", "BUDGET-V1")
            # read_set of 2 == the cap exactly: within bounds.
            session = svc.begin_session(read_set=[a, b], owner=owner)
            assert isinstance(session, SnapshotSession)
            assert session.cut[a] == 1 and session.cut[b] == 1

            r = svc.session_read(session.session_token, a, caller=owner)
            assert isinstance(r, VersionedContent)
            assert r.content == "PLAN-V1"

            result = svc.session_commit(
                session.session_token, b, "BUDGET-V2", caller=owner
            )
            assert isinstance(result, tuple)
            updated, _signals = result
            assert updated.version == 2
        finally:
            sql.close()
