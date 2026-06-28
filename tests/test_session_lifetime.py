# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Unit 5 — pin lifetime: the heartbeat lease + the session-liveness sweep +
fail-closed ``SessionInvalidated`` (SB-17 / TX-1).

Plan: ``docs/plans/2026-06-26-002-feat-read-side-transaction-snapshot-plan.md``
Unit 5. Requirement trace: **R4** (a crashed session's pins are reaped; a
referenced-unavailable or post-restart-unknown session fails closed with the
typed ``session_invalidated`` — never wrong bytes / live HEAD). Builds on Unit
2's cut capture / pin store and Units 3/4's ``session_read`` / ``session_commit``.

A protocol proof bounds the REGISTRY, not the integration: ``Snapshot.tla`` bounds
the cut/pin model; this file is the integration-layer regression for the sweep +
heartbeat TIMING. Per the institutional steer, the sweep/heartbeat timing uses
**fixed logical ticks, never wall-clock sleeps** — every staleness boundary is an
explicit ``current_tick`` vs ``heartbeat_timeout_ticks`` arithmetic, so the test
is deterministic and the predicate (not a hard TTL) is what is asserted.

Reason matching is ALWAYS ``reason == CONSTANT`` against the imported wire-stable
constant — never a substring of a human message (the typed-signal-not-substring
house rule).

The load-bearing distinction under test: the session heartbeat is keyed to the
server-minted SESSION TOKEN, NOT the MESI ``agent_id``. A session holds no grant,
so the grant sweep can never reap it; the session-liveness sweep is a NEW axis.
"""

from __future__ import annotations

from pathlib import Path
from uuid import UUID, uuid4

import pytest

from ccs.coordinator.registry import ArtifactRegistry
from ccs.coordinator.retention import RetentionPolicy
from ccs.coordinator.service import (
    CoordinatorService,
    looks_like_session_token,
)
from ccs.coordinator.sqlite_registry import SqliteArtifactRegistry
from ccs.core.exceptions import (
    SESSION_COMMIT_REASONS,
    SESSION_INVALIDATED_REASON,
    SESSION_NOT_FOUND_REASON,
    SESSION_READ_REASONS,
    SessionInvalidated,
)
from ccs.core.states import MESIState
from ccs.core.types import (
    Artifact,
    SessionCommitRejection,
    SessionReadRejection,
    SnapshotSession,
    VersionedContent,
)

# Timing constants — fixed logical ticks (no wall-clock sleeps).
_HB_TIMEOUT = 120


# ---------------------------------------------------------------------------
# Builders — mirror tests/test_session_read.py / test_session_commit.py.
# ---------------------------------------------------------------------------


def _register(reg, artifact_id: UUID, name: str, body: str) -> None:
    art = Artifact(id=artifact_id, name=name, version=1, content_hash="h1")
    reg.register_artifact(art, content=body)


def _peer_commit_cas(
    reg, artifact_id: UUID, writer: UUID, expected: int, body: str | None
) -> None:
    """A peer OCC commit via the registry ``commit_cas`` WIN (advances current,
    captures the new version into history when retain is on)."""
    reg.set_agent_state(artifact_id, writer, MESIState.SHARED, tick=1)
    reg.commit_cas(
        artifact_id,
        writer,
        expected_version=expected,
        content_hash="h2",
        content=body,
        tick=2,
    )


def _lazy_registries(tmp_path: Path, policy: RetentionPolicy | None = None):
    """An (in_memory, sqlite) LAZY pair — ``retain_versions=True``."""
    pol = policy if policy is not None else RetentionPolicy(max_versions=8)
    mem = ArtifactRegistry(retain_versions=True, retention_policy=pol)
    sql = SqliteArtifactRegistry(
        tmp_path / "life_lazy.db", retain_versions=True, retention_policy=pol
    )
    return mem, sql


# ===========================================================================
# A — Happy path: a live heartbeat keeps the pins exempt; read/commit work.
# ===========================================================================


class TestLiveHeartbeatNotReaped:
    @pytest.mark.parametrize("arm", ["in_memory", "sqlite"])
    def test_live_heartbeat_keeps_session_alive(
        self, tmp_path: Path, arm: str
    ) -> None:
        mem, sql = _lazy_registries(tmp_path)
        reg = mem if arm == "in_memory" else sql
        try:
            svc = CoordinatorService(reg)
            owner = uuid4()
            a = uuid4()
            _register(reg, a, "plan.md", "PINNED-V1")
            session = svc.begin_session(read_set=[a], owner=owner, created_at_tick=0)
            assert isinstance(session, SnapshotSession)
            tok = session.session_token

            # Heartbeat recently, then sweep at a tick WITHIN the timeout window.
            assert svc.record_session_heartbeat(
                session_token=tok, owner=owner, now_tick=100
            ) is True
            reaped = svc.enforce_session_liveness(
                current_tick=100 + _HB_TIMEOUT - 1, heartbeat_timeout_ticks=_HB_TIMEOUT
            )
            assert reaped == 0

            # read still serves the pinned bytes; commit still wins.
            r = svc.session_read(tok, a)
            assert isinstance(r, VersionedContent)
            assert r.content == "PINNED-V1"

            result = svc.session_commit(tok, a, "NEW-V2")
            assert isinstance(result, tuple)  # WIN -> (artifact, signals)
            updated, _signals = result
            assert updated.version == 2
        finally:
            sql.close()

    def test_session_never_heartbeated_survives_until_timeout(
        self, tmp_path: Path
    ) -> None:
        # A session that NEVER heartbeats carries a creation-tick lease baseline
        # (seeded at begin_session), so it is not reaped on the first sweep —
        # only once the baseline goes stale.
        mem, sql = _lazy_registries(tmp_path)
        try:
            svc = CoordinatorService(mem)
            owner = uuid4()
            a = uuid4()
            _register(mem, a, "plan.md", "BODY")
            session = svc.begin_session(read_set=[a], owner=owner, created_at_tick=50)
            tok = session.session_token

            # Within the window from the CREATION baseline (50) -> not reaped.
            assert svc.enforce_session_liveness(
                current_tick=50 + _HB_TIMEOUT - 1, heartbeat_timeout_ticks=_HB_TIMEOUT
            ) == 0
            assert isinstance(svc.session_read(tok, a), VersionedContent)

            # Past the window from the baseline -> reaped.
            assert svc.enforce_session_liveness(
                current_tick=50 + _HB_TIMEOUT, heartbeat_timeout_ticks=_HB_TIMEOUT
            ) == 1
        finally:
            sql.close()


# ===========================================================================
# B — Crash: a stale heartbeat is reaped; pins released; read/commit fail closed.
# ===========================================================================


class TestStaleHeartbeatReaped:
    @pytest.mark.parametrize("arm", ["in_memory", "sqlite"])
    def test_stale_session_reaped_then_fail_closed(
        self, tmp_path: Path, arm: str
    ) -> None:
        mem, sql = _lazy_registries(tmp_path)
        reg = mem if arm == "in_memory" else sql
        try:
            svc = CoordinatorService(reg)
            owner = uuid4()
            a = uuid4()
            _register(reg, a, "plan.md", "PINNED-V1")
            session = svc.begin_session(read_set=[a], owner=owner, created_at_tick=0)
            tok = session.session_token
            svc.record_session_heartbeat(session_token=tok, owner=owner, now_tick=10)

            # The pin is held BEFORE the reap.
            assert reg.get_session_cut(tok) is not None

            # Sweep PAST the timeout from the last heartbeat (10) -> reaped.
            reaped = svc.enforce_session_liveness(
                current_tick=10 + _HB_TIMEOUT, heartbeat_timeout_ticks=_HB_TIMEOUT
            )
            assert reaped == 1

            # Pins released: the cut is gone.
            assert reg.get_session_cut(tok) is None

            # FAIL CLOSED on read: session_invalidated, never live HEAD.
            r = svc.session_read(tok, a)
            assert isinstance(r, SessionReadRejection)
            assert r.reason == SESSION_INVALIDATED_REASON
            assert r.reason in SESSION_READ_REASONS

            # FAIL CLOSED on commit: session_invalidated.
            rc = svc.session_commit(tok, a, "WOULD-BE-V2")
            assert isinstance(rc, SessionCommitRejection)
            assert rc.reason == SESSION_INVALIDATED_REASON
            assert rc.reason in SESSION_COMMIT_REASONS
        finally:
            sql.close()

    @pytest.mark.parametrize("arm", ["in_memory", "sqlite"])
    def test_reaped_pin_becomes_collectible_again(
        self, tmp_path: Path, arm: str
    ) -> None:
        # The reaped session's pinned version is exempt from GC WHILE live, and
        # collectible again AFTER the reap (the exemptions seam drops it).
        pol = RetentionPolicy(max_versions=1)  # aggressive: only current survives
        mem = ArtifactRegistry(retain_versions=True, retention_policy=pol)
        sql = SqliteArtifactRegistry(
            tmp_path / "life_gc.db", retain_versions=True, retention_policy=pol
        )
        reg = mem if arm == "in_memory" else sql
        try:
            svc = CoordinatorService(reg)
            owner = uuid4()
            a = uuid4()
            _register(reg, a, "plan.md", "V1")
            session = svc.begin_session(read_set=[a], owner=owner, created_at_tick=0)
            pinned = session.cut[a]
            assert pinned == 1

            # While LIVE the pinned version is exempt (a peer commit past it does
            # not collect v1 — the session can still read it).
            _peer_commit_cas(reg, a, uuid4(), expected=1, body="V2")
            assert svc.session_read(session.session_token, a).content == "V1"
            assert pinned in _live_pins(reg, a)

            # Reap -> the pin is dropped from the exemption set.
            svc.enforce_session_liveness(
                current_tick=10_000, heartbeat_timeout_ticks=_HB_TIMEOUT
            )
            assert pinned not in _live_pins(reg, a)

            # And the next capture/GC tick can now collect v1 (no longer exempt).
            _peer_commit_cas(reg, a, uuid4(), expected=2, body="V3")
            # v1 is now collectible (no live pin); the bounded policy drops it.
            assert reg.get_version_record(a, 1) is None
        finally:
            sql.close()


def _live_pins(reg, artifact_id: UUID) -> set[int]:
    """Read the live pin-set for an artifact across both registry shapes (the
    in-memory and sqlite GC-exemption accessors are named differently)."""
    if isinstance(reg, ArtifactRegistry):
        return reg._live_pins_for_artifact(artifact_id)
    return reg._live_pins_for_artifact_sql(artifact_id)


# ===========================================================================
# C — Restart (in-memory): a previously-valid token after a fresh registry
#     fails closed (session_invalidated), never live HEAD.
# ===========================================================================


class TestRestartFailsClosed:
    def test_in_memory_restart_token_fails_closed(self, tmp_path: Path) -> None:
        # Process 1: open a session, capture its (in-shape) token.
        reg1 = ArtifactRegistry(retain_versions=True)
        svc1 = CoordinatorService(reg1)
        owner = uuid4()
        a1 = uuid4()
        _register(reg1, a1, "plan.md", "V1")
        session = svc1.begin_session(read_set=[a1], owner=owner)
        tok = session.session_token
        assert looks_like_session_token(tok)

        # Process 2: a FRESH in-memory registry + service (the restart wipes the
        # in-memory _session_pins AND the service-layer lease/owner/tombstone).
        reg2 = ArtifactRegistry(retain_versions=True)
        svc2 = CoordinatorService(reg2)
        a2 = uuid4()
        _register(reg2, a2, "plan.md", "FRESH-HEAD")

        # The old token has no cut in the fresh process. It MUST fail closed as
        # session_invalidated (it was a real session) — never serve live HEAD.
        r = svc2.session_read(tok, a2)
        assert isinstance(r, SessionReadRejection)
        assert r.reason == SESSION_INVALIDATED_REASON
        # Crucially: NOT served as VersionedContent of the fresh head.
        assert not isinstance(r, VersionedContent)

        rc = svc2.session_commit(tok, a2, "WOULD-BE")
        assert isinstance(rc, SessionCommitRejection)
        assert rc.reason == SESSION_INVALIDATED_REASON

    def test_malformed_token_is_session_not_found(self, tmp_path: Path) -> None:
        # A genuinely never-opened / malformed token (out-of-shape) stays
        # session_not_found — the additive-reachable "clearly bogus" signal.
        reg = ArtifactRegistry(retain_versions=True)
        svc = CoordinatorService(reg)
        a = uuid4()
        _register(reg, a, "plan.md", "HEAD")

        for bogus in ["", "short", "not-a-real-token", "x" * 100]:
            r = svc.session_read(bogus, a)
            assert isinstance(r, SessionReadRejection)
            assert r.reason == SESSION_NOT_FOUND_REASON, bogus
            # Still fail-closed (never live HEAD), just a different signal.
            assert not isinstance(r, VersionedContent)


# ===========================================================================
# D — Slow-but-live: a recently-heartbeated session is NOT reaped past a naive
#     TTL (it is a predicate on the lease, not a hard TTL).
# ===========================================================================


class TestSlowButLiveNotReaped:
    def test_long_running_session_survives_far_past_naive_ttl(
        self, tmp_path: Path
    ) -> None:
        reg = ArtifactRegistry(retain_versions=True)
        svc = CoordinatorService(reg)
        owner = uuid4()
        a = uuid4()
        _register(reg, a, "plan.md", "BODY")
        session = svc.begin_session(read_set=[a], owner=owner, created_at_tick=0)
        tok = session.session_token

        # March far past a naive "absolute TTL": as long as the session keeps
        # heartbeating within the window, it is never reaped — the predicate is
        # on the LEASE, not a hard age ceiling (that ceiling is the separate
        # Unit-7 cap, not exercised here).
        last = 0
        for step in range(1, 50):  # 49 windows past creation — way past 1 TTL.
            tick = step * _HB_TIMEOUT  # exactly one timeout per step
            # Heartbeat at the window boundary, then sweep at the same tick.
            assert svc.record_session_heartbeat(
                session_token=tok, owner=owner, now_tick=tick
            ) is True
            reaped = svc.enforce_session_liveness(
                current_tick=tick, heartbeat_timeout_ticks=_HB_TIMEOUT
            )
            assert reaped == 0, f"slow-but-live reaped at step {step}"
            last = tick
        assert last == 49 * _HB_TIMEOUT
        # Still serving after the long run.
        assert isinstance(svc.session_read(tok, a), VersionedContent)


# ===========================================================================
# E — Live-session / dead-AGENT: the session heartbeat is keyed to the TOKEN,
#     so a session whose OWNER agent stopped heartbeating its (unrelated) MESI
#     grant is NOT reaped. The load-bearing token-vs-agent distinction.
# ===========================================================================


class TestLiveSessionDeadAgent:
    def test_dead_mesi_agent_does_not_reap_live_session(
        self, tmp_path: Path
    ) -> None:
        reg = ArtifactRegistry(retain_versions=True)
        svc = CoordinatorService(reg)
        owner = uuid4()
        a = uuid4()
        _register(reg, a, "plan.md", "PINNED")
        session = svc.begin_session(read_set=[a], owner=owner, created_at_tick=0)
        tok = session.session_token

        # The OWNER's MESI grant heartbeat (keyed by agent_id) is NEVER recorded
        # — the owner agent "crashed" w.r.t. its grant. But the SESSION heartbeat
        # (keyed by the token) IS kept fresh.
        svc.record_session_heartbeat(session_token=tok, owner=owner, now_tick=500)

        # The grant sweep walks only M∪E holders; the owner holds no grant here,
        # so it reclaims nothing — and even if it did, it does not touch sessions.
        assert svc.enforce_stable_grant_timeouts(
            current_tick=10_000,
            heartbeat_timeout_ticks=_HB_TIMEOUT,
            max_hold_ticks=900,
        ) == 0

        # The session sweep, at a tick WITHIN the SESSION heartbeat window (500),
        # does NOT reap it — the session heartbeat is live independent of the
        # agent's grant heartbeat.
        assert svc.enforce_session_liveness(
            current_tick=500 + _HB_TIMEOUT - 1, heartbeat_timeout_ticks=_HB_TIMEOUT
        ) == 0

        # Pins survive; the read still serves the pinned bytes.
        r = svc.session_read(tok, a)
        assert isinstance(r, VersionedContent)
        assert r.content == "PINNED"

    def test_session_heartbeat_does_not_touch_agent_heartbeat(
        self, tmp_path: Path
    ) -> None:
        # Symmetry guard: record_session_heartbeat must NOT write the agent
        # heartbeat store (they are distinct axes). The owner agent's
        # last_heartbeat stays None after a session heartbeat.
        reg = ArtifactRegistry(retain_versions=True)
        svc = CoordinatorService(reg)
        owner = uuid4()
        a = uuid4()
        _register(reg, a, "plan.md", "B")
        session = svc.begin_session(read_set=[a], owner=owner)
        svc.record_session_heartbeat(
            session_token=session.session_token, owner=owner, now_tick=999
        )
        # The MESI agent heartbeat for the owner is untouched.
        assert reg.last_heartbeat_tick(owner) is None


# ===========================================================================
# F — Owner-bound heartbeat (security): a FOREIGN caller cannot keep another's
#     session alive; the foreign heartbeat does NOT refresh the lease.
# ===========================================================================


class TestOwnerBoundHeartbeat:
    def test_foreign_caller_cannot_refresh_lease(self, tmp_path: Path) -> None:
        reg = ArtifactRegistry(retain_versions=True)
        svc = CoordinatorService(reg)
        owner = uuid4()
        foreign = uuid4()
        a = uuid4()
        _register(reg, a, "plan.md", "BODY")
        session = svc.begin_session(read_set=[a], owner=owner, created_at_tick=0)
        tok = session.session_token

        # Owner heartbeats at tick 10. A FOREIGN caller then tries to heartbeat
        # at a much-later tick 10_000 — it MUST be rejected and must NOT move the
        # lease forward.
        assert svc.record_session_heartbeat(
            session_token=tok, owner=owner, now_tick=10
        ) is True
        assert svc.record_session_heartbeat(
            session_token=tok, owner=foreign, now_tick=10_000
        ) is False

        # The lease is still at 10, so a sweep past 10 + timeout reaps it —
        # proving the foreign heartbeat did NOT extend the lease.
        assert svc.enforce_session_liveness(
            current_tick=10 + _HB_TIMEOUT, heartbeat_timeout_ticks=_HB_TIMEOUT
        ) == 1
        r = svc.session_read(tok, a)
        assert isinstance(r, SessionReadRejection)
        assert r.reason == SESSION_INVALIDATED_REASON

    def test_heartbeat_unknown_token_is_typed_noop(self, tmp_path: Path) -> None:
        reg = ArtifactRegistry(retain_versions=True)
        svc = CoordinatorService(reg)
        # An unknown / never-opened token: a typed no-op (False), never a crash,
        # never a resurrected lease.
        assert svc.record_session_heartbeat(
            session_token="never-opened-token", owner=uuid4(), now_tick=5
        ) is False

    def test_heartbeat_released_token_is_noop(self, tmp_path: Path) -> None:
        reg = ArtifactRegistry(retain_versions=True)
        svc = CoordinatorService(reg)
        owner = uuid4()
        a = uuid4()
        _register(reg, a, "plan.md", "B")
        session = svc.begin_session(read_set=[a], owner=owner)
        tok = session.session_token
        # Reap it, then a heartbeat on the dead token is a no-op (no resurrection).
        svc.enforce_session_liveness(current_tick=10_000, heartbeat_timeout_ticks=_HB_TIMEOUT)
        assert svc.record_session_heartbeat(
            session_token=tok, owner=owner, now_tick=10_001
        ) is False
        # Still dead.
        r = svc.session_read(tok, a)
        assert isinstance(r, SessionReadRejection)
        assert r.reason == SESSION_INVALIDATED_REASON

    def test_record_session_heartbeat_rejects_negative_tick(
        self, tmp_path: Path
    ) -> None:
        reg = ArtifactRegistry(retain_versions=True)
        svc = CoordinatorService(reg)
        owner = uuid4()
        a = uuid4()
        _register(reg, a, "plan.md", "B")
        session = svc.begin_session(read_set=[a], owner=owner)
        with pytest.raises(ValueError):
            svc.record_session_heartbeat(
                session_token=session.session_token, owner=owner, now_tick=-1
            )


# ===========================================================================
# G — The SessionInvalidated typed exception + the taxonomy/predicate units.
# ===========================================================================


class TestTaxonomyAndTypedResult:
    def test_session_invalidated_exception_reason(self) -> None:
        # The raise-form carries the same wire reason as the rejection.
        assert SessionInvalidated.reason == SESSION_INVALIDATED_REASON
        assert issubclass(SessionInvalidated, Exception)

    def test_reason_sets_are_additive_and_disjoint_from_read_at_version(self) -> None:
        from ccs.core.exceptions import READ_AT_VERSION_REASONS

        # session_invalidated is in BOTH session sets, additively.
        assert SESSION_INVALIDATED_REASON in SESSION_READ_REASONS
        assert SESSION_INVALIDATED_REASON in SESSION_COMMIT_REASONS
        # The original reasons are still reachable (not renamed/folded).
        assert SESSION_NOT_FOUND_REASON in SESSION_READ_REASONS
        # Still disjoint from the bare read-at-version vocabulary (R7).
        assert SESSION_READ_REASONS.isdisjoint(READ_AT_VERSION_REASONS)
        assert SESSION_COMMIT_REASONS.isdisjoint(READ_AT_VERSION_REASONS)

    def test_token_shape_predicate(self) -> None:
        import secrets

        # Every server-minted token is in-shape.
        for _ in range(20):
            assert looks_like_session_token(secrets.token_urlsafe(32))
        # Out-of-shape tokens are rejected.
        assert not looks_like_session_token("")
        assert not looks_like_session_token("short")
        assert not looks_like_session_token("A" * 42)  # one short
        assert not looks_like_session_token("A" * 44)  # one long
        assert not looks_like_session_token("A" * 42 + "!")  # bad char
        assert not looks_like_session_token("A" * 42 + "=")  # base64 padding

    def test_tombstone_attributes_reaped_token_precisely(
        self, tmp_path: Path
    ) -> None:
        # A reaped token is attributed via the tombstone (definitely reaped).
        reg = ArtifactRegistry(retain_versions=True)
        svc = CoordinatorService(reg)
        owner = uuid4()
        a = uuid4()
        _register(reg, a, "plan.md", "B")
        session = svc.begin_session(read_set=[a], owner=owner)
        tok = session.session_token
        svc.enforce_session_liveness(current_tick=10_000, heartbeat_timeout_ticks=_HB_TIMEOUT)
        assert tok in svc._reaped_tombstone
        assert svc._classify_no_cut_reason(tok) == SESSION_INVALIDATED_REASON

    def test_tombstone_is_bounded(self, tmp_path: Path) -> None:
        # The reaped tombstone is capped (oldest-evicted) so it cannot grow
        # without bound. Eviction is benign — an evicted IN-SHAPE token still
        # classifies invalidated via the shape predicate.
        from ccs.coordinator.service import _REAPED_TOMBSTONE_CAP

        reg = ArtifactRegistry(retain_versions=True)
        svc = CoordinatorService(reg)
        # Tombstone many synthetic tokens directly (the reap path's effect).
        first = None
        for i in range(_REAPED_TOMBSTONE_CAP + 10):
            t = f"tok-{i}"
            if first is None:
                first = t
            svc._tombstone_token(t)
        assert len(svc._reaped_tombstone) == _REAPED_TOMBSTONE_CAP
        # The oldest entries were evicted.
        assert first not in svc._reaped_tombstone


# ===========================================================================
# H — GC-race fail-closed: a pinned body that vanished under a live pin degrades
#     safely (never wrong bytes). The lazy serve degrades to data-plane-deferred
#     rather than serving a wrong/absent body.
# ===========================================================================


class TestGcRaceFailsClosedNotWrongBytes:
    def test_absent_pinned_body_under_live_pin_does_not_serve_wrong_bytes(
        self, tmp_path: Path
    ) -> None:
        from ccs.core.types import DataPlaneDeferredRead

        # retain on, but commit the peer body as None (content=None path): the
        # pinned version's body is NOT in history. A live pin still points at it.
        reg = ArtifactRegistry(retain_versions=True)
        svc = CoordinatorService(reg)
        owner = uuid4()
        a = uuid4()
        _register(reg, a, "plan.md", "V1")
        session = svc.begin_session(read_set=[a], owner=owner)
        # Drop the pinned body out from under the live pin (simulate a GC race):
        record = reg._records[a]
        record.version_history.pop(1, None)
        record.version_captured_at.pop(1, None)

        r = svc.session_read(session.session_token, a)
        # NEVER a wrong-bytes VersionedContent; degrades to the typed deferral.
        assert isinstance(r, DataPlaneDeferredRead)
        assert r.version == 1
