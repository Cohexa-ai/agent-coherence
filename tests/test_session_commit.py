# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Unit 4 — ``session.commit``: single-artifact OCC validation against the pinned
base (SB-17 / TX-1).

Plan: ``docs/plans/2026-06-26-002-feat-read-side-transaction-snapshot-plan.md``
Unit 4. Requirement trace: **R3** (validate one artifact's commit against its
pinned version via the shipped ``commit_cas``; preserve the typed taxonomy;
relies on the reconciled admit-on-absent fence). Builds on Unit 2's
``begin_session`` / cut-capture / pin store.

``session_commit`` reuses the shipped ``commit_cas`` arbitration VERBATIM and
adds only (a) the token/pin VALIDATION gate (typed
:class:`SessionCommitRejection`, never a silent fall-through) and (b) a
SESSION-SCOPED committer identity that is fence-claimless, so admit-on-absent
holds and the PINNED base version-CAS is the sole arbiter.

Outcome mapping under test (mirrors the service ``commit_cas`` orchestration):
- WIN → ``(Artifact, [InvalidationSignal])``; version → pinned + 1.
- lost race → :class:`ConflictDetail` RETURNED unchanged (HELD, non-mutating).
- corruption (``expected_version > current``) → registry returns
  :class:`CasCorruption`; the SERVICE RAISES ``CoherenceError`` (non-retryable).
- token/pin validation failure → :class:`SessionCommitRejection` RETURNED.

Reason matching is ALWAYS ``reason == CONSTANT`` against the imported wire-stable
constant — never a substring of a human message (the typed-signal-not-substring
house rule). The OCC outcomes are parametrized over BOTH registries; the
admit-on-absent / corruption / taxonomy proofs run on both where applicable.
"""

from __future__ import annotations

from pathlib import Path
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

import pytest

from ccs.coordinator.registry import ArtifactRegistry
from ccs.coordinator.retention import RetentionPolicy
from ccs.coordinator.service import CoordinatorService
from ccs.coordinator.sqlite_registry import SqliteArtifactRegistry
from ccs.core.exceptions import (
    SESSION_ARTIFACT_NOT_IN_CUT_REASON,
    SESSION_COMMIT_REASONS,
    SESSION_INVALIDATED_REASON,
    SESSION_NOT_FOUND_REASON,
    STALE_READ_GENERATION_REASON,
    CoherenceError,
)
from ccs.core.states import MESIState
from ccs.core.types import (
    Artifact,
    CasCorruption,
    ConflictDetail,
    InvalidationSignal,
    SessionCommitRejection,
    SnapshotSession,
)

# ---------------------------------------------------------------------------
# Builders — mirror tests/test_session_read.py's helpers.
# ---------------------------------------------------------------------------


def _register(reg, artifact_id: UUID, name: str, body: str, version: int = 1) -> None:
    art = Artifact(id=artifact_id, name=name, version=version, content_hash="h1")
    reg.register_artifact(art, content=body)


def _peer_commit_cas(
    reg, artifact_id: UUID, writer: UUID, expected: int, body: str | None
) -> None:
    """A PEER OCC commit via the registry ``commit_cas`` WIN — advances current
    past the pin so a subsequent ``session_commit`` at the captured version is
    HELD (``version_mismatch``)."""
    reg.set_agent_state(artifact_id, writer, MESIState.SHARED, tick=1)
    reg.commit_cas(
        artifact_id,
        writer,
        expected_version=expected,
        content_hash="h2",
        content=body,
        tick=2,
    )


def _registries(tmp_path: Path):
    """An (in_memory, sqlite) LAZY pair — ``retain_versions=True`` (bodies in
    history). ``session_commit`` is byte-source-agnostic, but LAZY keeps the
    served-body assertions concrete on both arms."""
    pol = RetentionPolicy(max_versions=8)
    mem = ArtifactRegistry(retain_versions=True, retention_policy=pol)
    sql = SqliteArtifactRegistry(
        tmp_path / "snap_commit.db", retain_versions=True, retention_policy=pol
    )
    return mem, sql


# ===========================================================================
# A — Happy path: pinned @vN, current @vN → WIN, version → N+1
# ===========================================================================


class TestHappyPath:
    @pytest.mark.parametrize("arm", ["in_memory", "sqlite"])
    def test_wins_on_unchanged_base(self, tmp_path: Path, arm: str) -> None:
        mem, sql = _registries(tmp_path)
        reg = mem if arm == "in_memory" else sql
        try:
            svc = CoordinatorService(reg)
            a = uuid4()
            _register(reg, a, "plan.md", "V1", version=1)
            session = svc.begin_session(read_set=[a], owner=uuid4())
            assert isinstance(session, SnapshotSession)
            assert session.cut[a] == 1

            result = svc.session_commit(session.session_token, a, "V2-BODY")

            assert isinstance(result, tuple)
            updated, signals = result
            assert isinstance(updated, Artifact)
            assert updated.version == 2  # pinned 1 → 2
            assert updated.id == a
            assert isinstance(signals, list)
            assert all(isinstance(s, InvalidationSignal) for s in signals)
            # The artifact actually moved.
            assert reg.get_artifact(a).version == 2
        finally:
            sql.close()

    @pytest.mark.parametrize("arm", ["in_memory", "sqlite"])
    def test_wins_at_a_higher_pinned_version(self, tmp_path: Path, arm: str) -> None:
        """The pinned version need not be 1 — the WIN bumps pinned + 1 generally."""
        mem, sql = _registries(tmp_path)
        reg = mem if arm == "in_memory" else sql
        try:
            svc = CoordinatorService(reg)
            a = uuid4()
            _register(reg, a, "plan.md", "V7", version=7)
            session = svc.begin_session(read_set=[a], owner=uuid4())
            assert session.cut[a] == 7

            result = svc.session_commit(session.session_token, a, "V8")
            assert isinstance(result, tuple)
            assert result[0].version == 8
        finally:
            sql.close()


# ===========================================================================
# B — HELD: a peer commits after capture → ConflictDetail RETURNED, non-mutating
# ===========================================================================


class TestHeldOnMovedBase:
    @pytest.mark.parametrize("arm", ["in_memory", "sqlite"])
    def test_version_mismatch_returned_not_raised(
        self, tmp_path: Path, arm: str
    ) -> None:
        mem, sql = _registries(tmp_path)
        reg = mem if arm == "in_memory" else sql
        try:
            svc = CoordinatorService(reg)
            a = uuid4()
            _register(reg, a, "plan.md", "V1", version=1)
            session = svc.begin_session(read_set=[a], owner=uuid4())  # pins {a:1}

            # A peer commits 1 → 2 AFTER the cut was captured.
            _peer_commit_cas(reg, a, uuid4(), expected=1, body="PEER-V2")
            assert reg.get_artifact(a).version == 2
            before = reg.get_artifact(a)

            # The session commits at the now-stale pinned base (1) → HELD, RETURNED.
            result = svc.session_commit(session.session_token, a, "MINE")

            assert isinstance(result, ConflictDetail)
            assert result.reason == "version_mismatch"
            assert result.current_version == 2
            # Non-mutating: the artifact is UNCHANGED by the held commit.
            after = reg.get_artifact(a)
            assert after.version == before.version == 2
            assert after.content_hash == before.content_hash
        finally:
            sql.close()

    @pytest.mark.parametrize("arm", ["in_memory", "sqlite"])
    def test_no_invalidation_signal_emitted_on_held(
        self, tmp_path: Path, arm: str
    ) -> None:
        """A non-win mutates nothing AND emits no invalidation — there is no
        ``InvalidationSignal`` to inspect because the typed return is a bare
        ``ConflictDetail`` (not a ``(_, signals)`` tuple)."""
        mem, sql = _registries(tmp_path)
        reg = mem if arm == "in_memory" else sql
        try:
            svc = CoordinatorService(reg)
            a = uuid4()
            _register(reg, a, "plan.md", "V1", version=1)
            # A peer holding SHARED at v1 is a witness for "no peer invalidated".
            peer = uuid4()
            reg.set_agent_state(a, peer, MESIState.SHARED, tick=0)
            session = svc.begin_session(read_set=[a], owner=uuid4())
            _peer_commit_cas(reg, a, uuid4(), expected=1, body="PEER-V2")

            result = svc.session_commit(session.session_token, a, "MINE")

            assert isinstance(result, ConflictDetail)
            # The result type itself carries no signals list — a held commit
            # cannot have invalidated anyone.
            assert not isinstance(result, tuple)
        finally:
            sql.close()


# ===========================================================================
# C — Corruption: expected_version > current → service RAISES CoherenceError
# ===========================================================================


class TestCorruptionRaisesAtServiceLayer:
    @pytest.mark.parametrize("arm", ["in_memory", "sqlite"])
    def test_cas_corruption_maps_to_raised_coherence_error(
        self, tmp_path: Path, arm: str
    ) -> None:
        """Force ``expected_version > current``: pin @v5, then rewind the
        artifact's current version below the pin. The registry returns the
        :class:`CasCorruption` sentinel; the SERVICE maps it to a RAISED
        ``CoherenceError`` (the explicit Unit-4 obligation, non-retryable)."""
        mem, sql = _registries(tmp_path)
        reg = mem if arm == "in_memory" else sql
        try:
            svc = CoordinatorService(reg)
            a = uuid4()
            _register(reg, a, "plan.md", "V5", version=5)
            session = svc.begin_session(read_set=[a], owner=uuid4())  # pins {a:5}
            assert session.cut[a] == 5

            # Rewind current (5 → 3) BELOW the pin so expected(5) > current(3).
            reg.remove_artifact(a)
            _register(reg, a, "plan.md", "V3", version=3)

            with pytest.raises(CoherenceError) as excinfo:
                svc.session_commit(session.session_token, a, "X")
            # The raise is the corruption mapping, not a generic precondition.
            assert "corruption" in str(excinfo.value).lower()
        finally:
            sql.close()

    @pytest.mark.parametrize("arm", ["in_memory", "sqlite"])
    def test_registry_returns_sentinel_service_raises(
        self, tmp_path: Path, arm: str
    ) -> None:
        """Pin down the layering explicitly: the REGISTRY ``commit_cas`` RETURNS
        the :class:`CasCorruption` sentinel (never raises), while the SERVICE
        ``session_commit`` RAISES. Asserts the sentinel→raise split lives at the
        service layer (the Unit-4 obligation, not inherited from the registry)."""
        mem, sql = _registries(tmp_path)
        reg = mem if arm == "in_memory" else sql
        try:
            svc = CoordinatorService(reg)
            a = uuid4()
            _register(reg, a, "plan.md", "V5", version=5)
            session = svc.begin_session(read_set=[a], owner=uuid4())
            reg.remove_artifact(a)
            _register(reg, a, "plan.md", "V3", version=3)

            # The session-scoped committer the service would use for this token.
            committer = uuid5(NAMESPACE_URL, session.session_token)
            # REGISTRY layer: returns the sentinel, does NOT raise.
            sentinel = reg.commit_cas(
                a,
                committer,
                expected_version=5,
                content_hash="hX",
                content=None,
                tick=0,
            )
            assert isinstance(sentinel, CasCorruption)
            assert sentinel.current_version == 3

            # SERVICE layer: raises for the same corruption.
            with pytest.raises(CoherenceError):
                svc.session_commit(session.session_token, a, "X")
        finally:
            sql.close()


# ===========================================================================
# D — Admit-on-absent (the load-bearing R3 path)
# ===========================================================================


class TestAdmitOnAbsent:
    @pytest.mark.parametrize("arm", ["in_memory", "sqlite"])
    def test_session_committer_has_no_read_generation(
        self, tmp_path: Path, arm: str
    ) -> None:
        """The session-scoped committer never established a fence claim, so the
        registry ADMITS it (no ``read_generation`` row) and version-CAS arbitrates
        → the commit succeeds on an unchanged base."""
        mem, sql = _registries(tmp_path)
        reg = mem if arm == "in_memory" else sql
        try:
            svc = CoordinatorService(reg)
            a = uuid4()
            _register(reg, a, "plan.md", "V1", version=1)
            session = svc.begin_session(read_set=[a], owner=uuid4())

            committer = uuid5(NAMESPACE_URL, session.session_token)
            # Precondition of the load-bearing path: the committer is fence-claimless.
            assert reg.get_read_generation(a, committer) is None

            result = svc.session_commit(session.session_token, a, "V2")
            assert isinstance(result, tuple)
            assert result[0].version == 2
        finally:
            sql.close()

    @pytest.mark.parametrize("arm", ["in_memory", "sqlite"])
    def test_owner_with_superseded_read_generation_still_commits(
        self, tmp_path: Path, arm: str
    ) -> None:
        """THE load-bearing proof. The OWNER agent carries a SUPERSEDED
        ``read_generation`` (acquired E, then a sweep reclamation bumped the
        artifact's ``owner_generation``). A direct ``commit_cas`` UNDER THAT OWNER
        would spuriously fail ``stale_read_generation``. But ``session_commit``
        uses a SESSION-SCOPED committer (not the owner's MESI agent), which is
        fence-claimless → admit-on-absent → the session still WINS.
        """
        mem, sql = _registries(tmp_path)
        reg = mem if arm == "in_memory" else sql
        try:
            svc = CoordinatorService(reg)
            a = uuid4()
            _register(reg, a, "plan.md", "V1", version=1)

            owner = uuid4()
            # Owner acquires E → captures read_generation == owner_generation (0).
            reg.set_agent_state(a, owner, MESIState.EXCLUSIVE, trigger="write", tick=1)
            # A sweep reclamation bumps owner_generation (→1) and INVALIDATES the
            # owner; the owner's captured read_generation (0) is now SUPERSEDED.
            reg.set_agent_state(
                a, owner, MESIState.INVALID, trigger="reclaim_heartbeat", tick=2
            )
            assert reg.get_read_generation(a, owner) == 0
            assert reg.get_owner_generation(a) == 1

            # CONTROL: a direct commit_cas UNDER THE OWNER is fenced out (proves
            # the owner identity would spuriously reject — the bug we avoid).
            reg.set_agent_state(a, owner, MESIState.SHARED, tick=3)  # no fresh claim
            control = reg.commit_cas(
                a, owner, expected_version=1, content_hash="hc", content=None, tick=4
            )
            assert isinstance(control, ConflictDetail)
            assert control.reason == STALE_READ_GENERATION_REASON

            # A session whose OWNER is that same agent still WINS, because the
            # session-scoped committer is fence-claimless (admit-on-absent).
            session = svc.begin_session(read_set=[a], owner=owner)
            result = svc.session_commit(session.session_token, a, "SESSION-WINS")
            assert isinstance(result, tuple), f"expected WIN, got {result!r}"
            assert result[0].version == 2
        finally:
            sql.close()

    def test_committer_id_is_deterministic_per_token(self) -> None:
        """The session committer is a stable ``uuid5`` of the token (so a
        session's repeated commits use ONE identity), and differs per token."""
        t1 = "tok-AAA"
        t2 = "tok-BBB"
        assert CoordinatorService._session_committer_id(t1) == uuid5(NAMESPACE_URL, t1)
        assert CoordinatorService._session_committer_id(t1) == CoordinatorService._session_committer_id(t1)
        assert CoordinatorService._session_committer_id(t1) != CoordinatorService._session_committer_id(t2)


# ===========================================================================
# E — Taxonomy preserved: the three ConflictDetail reasons surface unchanged
# ===========================================================================


class TestTaxonomyPreserved:
    @pytest.mark.parametrize("arm", ["in_memory", "sqlite"])
    def test_version_mismatch_surfaces(self, tmp_path: Path, arm: str) -> None:
        mem, sql = _registries(tmp_path)
        reg = mem if arm == "in_memory" else sql
        try:
            svc = CoordinatorService(reg)
            a = uuid4()
            _register(reg, a, "p", "V1", version=1)
            session = svc.begin_session(read_set=[a], owner=uuid4())
            _peer_commit_cas(reg, a, uuid4(), expected=1, body="V2")
            result = svc.session_commit(session.session_token, a, "X")
            assert isinstance(result, ConflictDetail)
            assert result.reason == "version_mismatch"
        finally:
            sql.close()

    @pytest.mark.parametrize("arm", ["in_memory", "sqlite"])
    def test_other_holder_surfaces(self, tmp_path: Path, arm: str) -> None:
        """A pessimistic peer holds M/E at the pinned (unchanged) version → the
        OCC-vs-pessimistic guard returns ``other_holder``, surfaced unchanged."""
        mem, sql = _registries(tmp_path)
        reg = mem if arm == "in_memory" else sql
        try:
            svc = CoordinatorService(reg)
            a = uuid4()
            _register(reg, a, "p", "V1", version=1)
            session = svc.begin_session(read_set=[a], owner=uuid4())  # pins {a:1}
            # A pessimistic peer takes EXCLUSIVE at the still-pinned version.
            reg.set_agent_state(a, uuid4(), MESIState.EXCLUSIVE, trigger="write", tick=1)

            result = svc.session_commit(session.session_token, a, "X")
            assert isinstance(result, ConflictDetail)
            assert result.reason == "other_holder"
            assert result.current_version == 1
        finally:
            sql.close()

    @pytest.mark.parametrize("arm", ["in_memory", "sqlite"])
    def test_stale_read_generation_surfaces_when_committer_carries_a_claim(
        self, tmp_path: Path, arm: str
    ) -> None:
        """The third reason is reachable through ``session_commit`` ONLY if the
        committer carries a superseded claim. The session committer normally
        never does (that is the whole point), so to exercise the reason we plant
        a superseded ``read_generation`` ON the session-derived committer id and
        confirm it surfaces UNCHANGED (the taxonomy is preserved end-to-end, not
        swallowed). This is an adversarial poke, not a normal path."""
        mem, sql = _registries(tmp_path)
        reg = mem if arm == "in_memory" else sql
        try:
            svc = CoordinatorService(reg)
            a = uuid4()
            _register(reg, a, "p", "V1", version=1)
            session = svc.begin_session(read_set=[a], owner=uuid4())
            committer = uuid5(NAMESPACE_URL, session.session_token)

            # Plant a fence claim ON the committer id, then supersede it.
            reg.set_agent_state(a, committer, MESIState.EXCLUSIVE, trigger="write", tick=1)
            reg.set_agent_state(
                a, committer, MESIState.INVALID, trigger="reclaim_heartbeat", tick=2
            )
            reg.set_agent_state(a, committer, MESIState.SHARED, tick=3)
            assert reg.get_read_generation(a, committer) == 0
            assert reg.get_owner_generation(a) == 1

            result = svc.session_commit(session.session_token, a, "X")
            assert isinstance(result, ConflictDetail)
            assert result.reason == STALE_READ_GENERATION_REASON
        finally:
            sql.close()

    @pytest.mark.parametrize("arm", ["in_memory", "sqlite"])
    def test_no_invalidation_signal_on_any_non_win(
        self, tmp_path: Path, arm: str
    ) -> None:
        """Across all three non-win reasons + the validation rejections, the
        result is never a ``(_, signals)`` tuple — nothing was invalidated."""
        mem, sql = _registries(tmp_path)
        reg = mem if arm == "in_memory" else sql
        try:
            svc = CoordinatorService(reg)
            # version_mismatch
            a = uuid4()
            _register(reg, a, "p", "V1", version=1)
            s = svc.begin_session(read_set=[a], owner=uuid4())
            _peer_commit_cas(reg, a, uuid4(), expected=1, body="V2")
            r1 = svc.session_commit(s.session_token, a, "X")
            # other_holder
            b = uuid4()
            _register(reg, b, "q", "V1", version=1)
            s2 = svc.begin_session(read_set=[b], owner=uuid4())
            reg.set_agent_state(b, uuid4(), MESIState.EXCLUSIVE, trigger="write", tick=1)
            r2 = svc.session_commit(s2.session_token, b, "X")
            # validation rejection
            r3 = svc.session_commit("unknown", a, "X")

            for r in (r1, r2, r3):
                assert not isinstance(r, tuple), f"unexpected signal-bearing win: {r!r}"
        finally:
            sql.close()


# ===========================================================================
# F — Validation gate: typed rejections, never a silent fall-through
# ===========================================================================


class TestValidationRejections:
    @pytest.mark.parametrize("arm", ["in_memory", "sqlite"])
    def test_unknown_token_is_session_not_found(self, tmp_path: Path, arm: str) -> None:
        mem, sql = _registries(tmp_path)
        reg = mem if arm == "in_memory" else sql
        try:
            svc = CoordinatorService(reg)
            a = uuid4()
            _register(reg, a, "p", "V1", version=1)
            result = svc.session_commit("never-minted-token", a, "X")
            assert isinstance(result, SessionCommitRejection)
            assert result.reason == SESSION_NOT_FOUND_REASON
            assert result.reason in SESSION_COMMIT_REASONS
            assert result.artifact_id == a
            assert result.coordinator_epoch == reg.coordinator_epoch
            # The artifact was NOT touched.
            assert reg.get_artifact(a).version == 1
        finally:
            sql.close()

    @pytest.mark.parametrize("arm", ["in_memory", "sqlite"])
    def test_released_token_is_session_invalidated(
        self, tmp_path: Path, arm: str
    ) -> None:
        mem, sql = _registries(tmp_path)
        reg = mem if arm == "in_memory" else sql
        try:
            svc = CoordinatorService(reg)
            a = uuid4()
            _register(reg, a, "p", "V1", version=1)
            session = svc.begin_session(read_set=[a], owner=uuid4())
            reg.release_session(session.session_token)  # pins dropped
            result = svc.session_commit(session.session_token, a, "X")
            assert isinstance(result, SessionCommitRejection)
            # Unit-5 taxonomy: a released (in-shape) token WAS a real session →
            # session_invalidated (fail closed, never a live-HEAD commit), not
            # session_not_found (which stays reachable for malformed tokens).
            assert result.reason == SESSION_INVALIDATED_REASON
        finally:
            sql.close()

    @pytest.mark.parametrize("arm", ["in_memory", "sqlite"])
    def test_artifact_not_in_cut_is_rejected(self, tmp_path: Path, arm: str) -> None:
        mem, sql = _registries(tmp_path)
        reg = mem if arm == "in_memory" else sql
        try:
            svc = CoordinatorService(reg)
            a = uuid4()
            b = uuid4()
            _register(reg, a, "plan.md", "VA", version=1)
            _register(reg, b, "budget.md", "VB", version=1)
            session = svc.begin_session(read_set=[b], owner=uuid4())  # pins {b}
            # Commit an artifact NOT in the cut → rejected, never live-HEAD commit.
            result = svc.session_commit(session.session_token, a, "X")
            assert isinstance(result, SessionCommitRejection)
            assert result.reason == SESSION_ARTIFACT_NOT_IN_CUT_REASON
            assert result.artifact_id == a
            # The un-pinned artifact was NOT committed.
            assert reg.get_artifact(a).version == 1
        finally:
            sql.close()

    def test_session_commit_reasons_is_additive_and_disjoint(self) -> None:
        """``SESSION_COMMIT_REASONS`` is the closed validation set; it is disjoint
        from the bare ``read_at_version`` surface (R7 additive-only)."""
        from ccs.core.exceptions import READ_AT_VERSION_REASONS, SESSION_READ_REASONS

        # Unit 5 ADDED session_invalidated (additive, R7) — the set grew, no
        # reason was renamed.
        assert SESSION_COMMIT_REASONS == {
            SESSION_NOT_FOUND_REASON,
            SESSION_ARTIFACT_NOT_IN_CUT_REASON,
            SESSION_INVALIDATED_REASON,
        }
        # Shares the token/pin vocabulary with session_read (one surface).
        assert SESSION_COMMIT_REASONS == SESSION_READ_REASONS
        # Disjoint from the separate read_at_version history surface.
        assert SESSION_COMMIT_REASONS.isdisjoint(READ_AT_VERSION_REASONS)


# ===========================================================================
# G — R11: "exactly one validated commit" is naturally enforced
# ===========================================================================


class TestExactlyOneValidatedCommit:
    @pytest.mark.parametrize("arm", ["in_memory", "sqlite"])
    def test_second_commit_at_same_pin_version_mismatches(
        self, tmp_path: Path, arm: str
    ) -> None:
        """After a WIN the artifact moved past the pin, so a SECOND
        ``session_commit`` at the same (now stale) pin version-mismatches — no
        explicit single-use machinery, the pin is neither consumed nor rewritten
        (so SB-18 multi-commit stays un-foreclosed, R11)."""
        mem, sql = _registries(tmp_path)
        reg = mem if arm == "in_memory" else sql
        try:
            svc = CoordinatorService(reg)
            a = uuid4()
            _register(reg, a, "p", "V1", version=1)
            session = svc.begin_session(read_set=[a], owner=uuid4())

            first = svc.session_commit(session.session_token, a, "V2")
            assert isinstance(first, tuple)
            assert first[0].version == 2

            # The pin still records version 1; a second commit is HELD.
            assert session.cut[a] == 1
            assert svc.registry.get_session_cut(session.session_token)[a] == 1
            second = svc.session_commit(session.session_token, a, "V3")
            assert isinstance(second, ConflictDetail)
            assert second.reason == "version_mismatch"
            assert second.current_version == 2
            # Only ONE commit landed: still at v2.
            assert reg.get_artifact(a).version == 2
        finally:
            sql.close()
