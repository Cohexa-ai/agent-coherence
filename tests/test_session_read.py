# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Unit 3 — ``session.read``: the non-mutating serve from the pinned cut
(SB-17 / TX-1).

Plan: ``docs/plans/2026-06-26-002-feat-read-side-transaction-snapshot-plan.md``
Unit 3. Requirement trace: **R2** (serve the pinned version even when it equals
current; non-mutating). Builds on Unit 2's ``begin_session`` / cut-capture / pin
store.

A protocol proof bounds the REGISTRY, not the integration: ``Snapshot.tla`` proves
``NoReadSkewWithinCut`` over the model; this file is the integration-layer
regression for the serve-across-a-peer-commit WINDOW. Per the split-comparand
learning the staleness probes use **fixed-stale buffers, not counters** — the
served body is compared against a buffer LITERAL captured before the peer commit,
never a re-derived expectation that could drift with the bug it is meant to
catch.

Reason matching is ALWAYS ``reason == CONSTANT`` against the imported wire-stable
constant — never a substring of a human message (the typed-signal-not-substring
house rule).

Branch coverage:
- LAZY (``retain_versions=True``) — the coordinator serves pinned bytes from
  retained history (``version==current`` AND ``version<current``, incl. the
  transition). Exercised on BOTH registries.
- EAGER (``retain_versions=False`` / ``content=None``) — the coordinator holds no
  body; ``session_read`` returns the typed data-plane-deferred result.
"""

from __future__ import annotations

import time
from pathlib import Path
from uuid import UUID, uuid4

import pytest

from ccs.coordinator.registry import ArtifactRegistry
from ccs.coordinator.retention import RetentionPolicy
from ccs.coordinator.service import CoordinatorService
from ccs.coordinator.sqlite_registry import SqliteArtifactRegistry
from ccs.core.exceptions import (
    SESSION_ARTIFACT_NOT_IN_CUT_REASON,
    SESSION_INVALIDATED_REASON,
    SESSION_NOT_FOUND_REASON,
    SESSION_READ_REASONS,
)
from ccs.core.states import MESIState
from ccs.core.types import (
    Artifact,
    DataPlaneDeferredRead,
    SessionReadRejection,
    SnapshotSession,
    VersionedContent,
)

# ---------------------------------------------------------------------------
# Builders — mirror tests/test_snapshot_cut_capture.py's helpers.
# ---------------------------------------------------------------------------


def _register(reg, artifact_id: UUID, name: str, body: str) -> None:
    art = Artifact(id=artifact_id, name=name, version=1, content_hash="h1")
    reg.register_artifact(art, content=body)


def _commit_cas(
    reg, artifact_id: UUID, writer: UUID, expected: int, body: str | None
) -> None:
    """A peer OCC commit via the registry ``commit_cas`` WIN (advances current,
    captures the new version into history when retain is on; ``body=None``
    advances the version WITHOUT a retained body — the content=None path)."""
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
    """An (in_memory, sqlite) LAZY pair — ``retain_versions=True``: the
    coordinator retains bodies in history (the serve-from-history branch)."""
    pol = policy if policy is not None else RetentionPolicy(max_versions=8)
    mem = ArtifactRegistry(retain_versions=True, retention_policy=pol)
    sql = SqliteArtifactRegistry(
        tmp_path / "snap_lazy.db", retain_versions=True, retention_policy=pol
    )
    return mem, sql


def _eager_registries(tmp_path: Path):
    """An (in_memory, sqlite) EAGER pair — ``retain_versions=False`` /
    ``content=None`` ICP: the coordinator holds NO body (bytes live in the data
    plane), so ``session_read`` defers."""
    mem = ArtifactRegistry(retain_versions=False)
    sql = SqliteArtifactRegistry(tmp_path / "snap_eager.db", retain_versions=False)
    return mem, sql


# ===========================================================================
# A — Happy path (LAZY): the pinned bytes are served
# ===========================================================================


class TestLazyHappyPath:
    """``session_read`` returns the pinned ``@vN`` bytes from retained history."""

    @pytest.mark.parametrize("arm", ["in_memory", "sqlite"])
    def test_serves_pinned_bytes(self, tmp_path: Path, arm: str) -> None:
        mem, sql = _lazy_registries(tmp_path)
        reg = mem if arm == "in_memory" else sql
        try:
            svc = CoordinatorService(reg)
            a = uuid4()
            _register(reg, a, "plan.md", "PINNED-V1")
            session = svc.begin_session(read_set=[a], owner=uuid4())
            assert isinstance(session, SnapshotSession)

            result = svc.session_read(session.session_token, a)
            assert isinstance(result, VersionedContent)
            assert result.version == 1
            assert result.content == "PINNED-V1"
            assert result.artifact_id == a
            assert result.coordinator_epoch == reg.coordinator_epoch
        finally:
            sql.close()


# ===========================================================================
# B — Edge (the gap): pinned == current is SERVED (not rejected current_version)
# ===========================================================================


class TestPinnedEqualsCurrentIsServed:
    """The gap the unit closes: ``session_read`` serves the pinned version even
    when it EQUALS current — where the bare ``read_at_version`` rejects with
    ``current_version``. ``begin_session`` is the coherence event that earns it."""

    @pytest.mark.parametrize("arm", ["in_memory", "sqlite"])
    def test_pinned_equals_current_served_not_rejected(
        self, tmp_path: Path, arm: str
    ) -> None:
        mem, sql = _lazy_registries(tmp_path)
        reg = mem if arm == "in_memory" else sql
        try:
            svc = CoordinatorService(reg)
            a = uuid4()
            _register(reg, a, "plan.md", "CURRENT-BODY")
            session = svc.begin_session(read_set=[a], owner=uuid4())
            assert isinstance(session, SnapshotSession)
            # The pin equals current (no peer commit yet).
            assert session.cut[a] == reg.get_artifact(a).version == 1

            result = svc.session_read(session.session_token, a)
            # SERVED — not a SessionReadRejection, not a current_version reject.
            assert isinstance(result, VersionedContent)
            assert result.version == 1
            assert result.content == "CURRENT-BODY"
        finally:
            sql.close()

    def test_bare_read_at_version_still_rejects_current(
        self, tmp_path: Path
    ) -> None:
        # Contrast harness: the SAME store, the bare read_at_version, STILL
        # rejects version==current (the shipped contract is untouched — Unit 3
        # added a NEW path, it did not relax read_at_version).
        from ccs.core.exceptions import CURRENT_VERSION_REASON
        from ccs.core.types import VersionedReadRejection

        mem, sql = _lazy_registries(tmp_path)
        try:
            svc = CoordinatorService(mem)
            a = uuid4()
            _register(mem, a, "plan.md", "x")
            rej = svc.read_at_version(a, 1)  # version==current
            assert isinstance(rej, VersionedReadRejection)
            assert rej.reason == CURRENT_VERSION_REASON
        finally:
            sql.close()


# ===========================================================================
# C — Edge (the transition): pinned==current becomes pinned<current after a peer
#     commit; still served at the PINNED version from history
# ===========================================================================


class TestTransitionAcrossPeerCommit:
    """A ``pinned==current`` read becomes ``pinned<current`` once a peer commits —
    after which the serve re-routes to retained history, still at the PINNED
    version (no read skew, no live-HEAD leak). FIXED-STALE BUFFER: the expected
    body is the literal captured before the peer commit."""

    @pytest.mark.parametrize("arm", ["in_memory", "sqlite"])
    def test_served_at_pin_after_peer_commit(self, tmp_path: Path, arm: str) -> None:
        mem, sql = _lazy_registries(tmp_path)
        reg = mem if arm == "in_memory" else sql
        try:
            svc = CoordinatorService(reg)
            a = uuid4()
            _register(reg, a, "budget.md", "PIN-V1")
            session = svc.begin_session(read_set=[a], owner=uuid4())
            assert isinstance(session, SnapshotSession)
            # FIXED-STALE BUFFER literal — never re-read from the moving registry.
            pinned_body = "PIN-V1"

            # Pre-commit: pinned == current, served from current.
            r1 = svc.session_read(session.session_token, a)
            assert isinstance(r1, VersionedContent)
            assert r1.version == 1 and r1.content == pinned_body

            # A peer advances the artifact: pin is now < current.
            writer = uuid4()
            _commit_cas(reg, a, writer, expected=1, body="PEER-V2")
            assert reg.get_artifact(a).version == 2

            # Re-routed to history — STILL the pinned v1 bytes, NOT the peer's v2.
            r2 = svc.session_read(session.session_token, a)
            assert isinstance(r2, VersionedContent)
            assert r2.version == 1, "served the moved version, not the pin"
            assert r2.content == pinned_body, "served peer bytes (read skew!)"
            assert r2.content != "PEER-V2"
        finally:
            sql.close()


# ===========================================================================
# D — Integration: the read is NON-MUTATING (no MESI grant, no read_generation)
# ===========================================================================


class TestNonMutating:
    """``session_read`` mints no MESI grant and captures no ``read_generation`` —
    the coherence state is byte-for-byte unchanged across the read (R2 / the
    shipped non-mutating invariant)."""

    @pytest.mark.parametrize("arm", ["in_memory", "sqlite"])
    def test_coherence_state_unchanged(self, tmp_path: Path, arm: str) -> None:
        mem, sql = _lazy_registries(tmp_path)
        reg = mem if arm == "in_memory" else sql
        try:
            svc = CoordinatorService(reg)
            a = uuid4()
            owner = uuid4()
            _register(reg, a, "plan.md", "v1")
            session = svc.begin_session(read_set=[a], owner=owner)
            assert isinstance(session, SnapshotSession)

            state_before = reg.get_state_map(a)
            svc.session_read(session.session_token, a)
            state_after = reg.get_state_map(a)

            # No grant was minted for ANY agent — the read is not an acquire.
            assert state_before == state_after
            assert state_after == {}, "session_read minted a MESI grant"
            # And no read_generation was captured for the owner (a reader is not
            # an owner; read-gen capture lives only in set_agent_state).
            assert reg.get_read_generation(a, owner) is None
        finally:
            sql.close()

    def test_repeated_reads_are_idempotent_on_state(self, tmp_path: Path) -> None:
        # Many reads never accrete state (no grant leak across calls).
        mem, sql = _lazy_registries(tmp_path)
        try:
            svc = CoordinatorService(mem)
            a = uuid4()
            _register(mem, a, "plan.md", "v1")
            session = svc.begin_session(read_set=[a], owner=uuid4())
            for _ in range(5):
                svc.session_read(session.session_token, a)
            assert mem.get_state_map(a) == {}
        finally:
            sql.close()


# ===========================================================================
# E — Error: read of an artifact NOT in the cut → typed (not live-HEAD)
# ===========================================================================


class TestArtifactNotInCut:
    """An artifact outside the captured cut is REJECTED — never served from live
    HEAD (the no-fall-through guarantee)."""

    @pytest.mark.parametrize("arm", ["in_memory", "sqlite"])
    def test_unpinned_artifact_rejected(self, tmp_path: Path, arm: str) -> None:
        mem, sql = _lazy_registries(tmp_path)
        reg = mem if arm == "in_memory" else sql
        try:
            svc = CoordinatorService(reg)
            pinned, unpinned = uuid4(), uuid4()
            _register(reg, pinned, "plan.md", "in-cut")
            _register(reg, unpinned, "secret.md", "NOT-in-cut-LIVE-HEAD")
            session = svc.begin_session(read_set=[pinned], owner=uuid4())
            assert isinstance(session, SnapshotSession)

            result = svc.session_read(session.session_token, unpinned)
            assert isinstance(result, SessionReadRejection)
            assert result.reason == SESSION_ARTIFACT_NOT_IN_CUT_REASON
            assert result.reason in SESSION_READ_REASONS
            assert result.artifact_id == unpinned
            # No body leaked: the rejection structurally carries none.
            assert not hasattr(result, "content")
        finally:
            sql.close()


# ===========================================================================
# F — Error: unknown / released token → typed not-found
# ===========================================================================


class TestSessionNotFound:
    """An unknown or released token is a typed ``session_not_found`` — never a
    serve. (Unit 5 splits the durable-liveness taxonomy; Unit 3 treats every
    no-cut token uniformly.)"""

    @pytest.mark.parametrize("arm", ["in_memory", "sqlite"])
    def test_unknown_token(self, tmp_path: Path, arm: str) -> None:
        mem, sql = _lazy_registries(tmp_path)
        reg = mem if arm == "in_memory" else sql
        try:
            svc = CoordinatorService(reg)
            a = uuid4()
            _register(reg, a, "plan.md", "v1")
            result = svc.session_read("never-minted-token", a)
            assert isinstance(result, SessionReadRejection)
            assert result.reason == SESSION_NOT_FOUND_REASON
            assert result.coordinator_epoch == reg.coordinator_epoch
        finally:
            sql.close()

    @pytest.mark.parametrize("arm", ["in_memory", "sqlite"])
    def test_released_token(self, tmp_path: Path, arm: str) -> None:
        mem, sql = _lazy_registries(tmp_path)
        reg = mem if arm == "in_memory" else sql
        try:
            svc = CoordinatorService(reg)
            a = uuid4()
            _register(reg, a, "plan.md", "v1")
            session = svc.begin_session(read_set=[a], owner=uuid4())
            assert isinstance(session, SnapshotSession)
            # Released → the pins are gone; the token no longer serves. Under the
            # Unit-5 liveness taxonomy a released token WAS a real (in-shape)
            # session, so it fails closed as session_invalidated ("re-establish
            # it"), never live HEAD — distinct from a never-opened/malformed
            # token (session_not_found). Wire-stable: session_not_found stays
            # reachable for malformed tokens (asserted in TestSessionNotFound).
            reg.release_session(session.session_token)
            result = svc.session_read(session.session_token, a)
            assert isinstance(result, SessionReadRejection)
            assert result.reason == SESSION_INVALIDATED_REASON
        finally:
            sql.close()


# ===========================================================================
# G — Edge (no coordinator bytes): EAGER / content=None → data-plane-deferred
# ===========================================================================


class TestDataPlaneDeferred:
    """When the coordinator holds no body for the pinned version, ``session_read``
    returns the typed data-plane-deferred result — pinned version + epoch (+ hash
    when known), NO bytes, NO crash. The actual eager byte serve is Unit 6."""

    @pytest.mark.parametrize("arm", ["in_memory", "sqlite"])
    def test_eager_branch_defers(self, tmp_path: Path, arm: str) -> None:
        mem, sql = _eager_registries(tmp_path)
        reg = mem if arm == "in_memory" else sql
        try:
            svc = CoordinatorService(reg)
            a = uuid4()
            _register(reg, a, "config.json", "body-lives-in-data-plane")
            session = svc.begin_session(read_set=[a], owner=uuid4())
            assert isinstance(session, SnapshotSession)
            assert session.retain_versions is False

            result = svc.session_read(session.session_token, a)
            assert isinstance(result, DataPlaneDeferredRead)
            assert result.version == 1  # the PINNED version
            assert result.artifact_id == a
            assert result.coordinator_epoch == reg.coordinator_epoch
            # content_hash is the pinned (== current) version's hash; never bytes.
            assert result.content_hash == "h1"
            assert not hasattr(result, "content")
        finally:
            sql.close()

    def test_content_none_under_retain_degrades_to_deferred(
        self, tmp_path: Path
    ) -> None:
        # retain_versions=True but the pinned version was committed content=None
        # (no retained body) → the serve degrades to data-plane-deferred, never a
        # crash or wrong bytes. (sqlite is the content=None ICP — KTD-13.)
        mem, sql = _lazy_registries(tmp_path)
        try:
            svc = CoordinatorService(sql)
            a = uuid4()
            _register(sql, a, "plan.md", "v1")
            writer = uuid4()
            # Advance to v2 with NO body retained (content=None).
            _commit_cas(sql, a, writer, expected=1, body=None)
            assert sql.get_artifact(a).version == 2
            # Pin the bodyless v2 (current).
            session = svc.begin_session(read_set=[a], owner=uuid4())
            assert session.cut[a] == 2

            result = svc.session_read(session.session_token, a)
            assert isinstance(result, DataPlaneDeferredRead)
            assert result.version == 2
        finally:
            sql.close()


# ===========================================================================
# H — T-expiry read-serve allowance: a pinned-but-age-collectible row is served
# ===========================================================================


class TestTExpiryAllowance:
    """A pinned version that is past the retention AGE bound is STILL served by
    ``session_read`` (the read-serve allowance) — distinct from the GC-hold the
    Unit-2 exemptions seam provides at the GC producers. The bare
    ``read_at_version`` would report it ``not_retained``; the live pin lifts the
    read-side logical T-expiry."""

    @pytest.mark.parametrize("arm", ["in_memory", "sqlite"])
    def test_pinned_age_collectible_still_served(
        self, tmp_path: Path, arm: str
    ) -> None:
        # max_age=50ms: pin v1, advance to v2, sleep past the age bound. v1 is now
        # age-collectible as history — but the live pin's read-serve allowance
        # serves it anyway.
        policy = RetentionPolicy(max_age_seconds=0.05)
        mem, sql = _lazy_registries(tmp_path, policy)
        reg = mem if arm == "in_memory" else sql
        try:
            svc = CoordinatorService(reg)
            a = uuid4()
            _register(reg, a, "plan.md", "AGED-PIN-V1")
            session = svc.begin_session(read_set=[a], owner=uuid4())
            assert session.cut[a] == 1

            writer = uuid4()
            _commit_cas(reg, a, writer, expected=1, body="V2")  # v1 → history
            time.sleep(0.1)  # v1 is now past the 50ms age bound

            # Contrast: the bare read_at_version reports it collectible
            # (not_retained) for in-memory — the pin allowance is what differs.
            result = svc.session_read(session.session_token, a)
            assert isinstance(result, VersionedContent), (
                "pinned-but-age-collectible row was not served (allowance "
                "regressed)"
            )
            assert result.version == 1
            assert result.content == "AGED-PIN-V1"
        finally:
            sql.close()


# ===========================================================================
# I — Parity: the no-cut empty-read-set degenerate case (documented divergence)
# ===========================================================================


class TestEmptyReadSetDegenerate:
    """An empty-read-set session pins nothing, so a ``session_read`` of any
    artifact is un-servable on both arms. The TAXONOMY diverges (sqlite has no
    durable empty-session marker → ``session_not_found``; in-memory keeps an
    empty ``{}`` entry → ``artifact_not_in_cut``) but BOTH are typed rejections —
    neither serves live HEAD. This pins the documented divergence so a future
    change cannot silently turn it into a serve."""

    def test_in_memory_empty_session_rejects_artifact_not_in_cut(
        self, tmp_path: Path
    ) -> None:
        mem = ArtifactRegistry(retain_versions=True, retention_policy=RetentionPolicy(max_versions=4))
        svc = CoordinatorService(mem)
        a = uuid4()
        _register(mem, a, "plan.md", "v1")
        session = svc.begin_session(read_set=[], owner=uuid4())
        result = svc.session_read(session.session_token, a)
        assert isinstance(result, SessionReadRejection)
        assert result.reason == SESSION_ARTIFACT_NOT_IN_CUT_REASON

    def test_sqlite_empty_session_rejects_session_invalidated(
        self, tmp_path: Path
    ) -> None:
        sql = SqliteArtifactRegistry(
            tmp_path / "empty.db",
            retain_versions=True,
            retention_policy=RetentionPolicy(max_versions=4),
        )
        try:
            svc = CoordinatorService(sql)
            a = uuid4()
            _register(sql, a, "plan.md", "v1")
            session = svc.begin_session(read_set=[], owner=uuid4())
            result = svc.session_read(session.session_token, a)
            assert isinstance(result, SessionReadRejection)
            # sqlite cannot durably mark an empty live session (zero pin rows →
            # None). Under the Unit-5 taxonomy the in-shape token classifies
            # session_invalidated rather than session_not_found; the divergence
            # from the in-memory arm (which keeps an empty {} and would reject
            # artifact_not_in_cut) stays BENIGN — no artifact is servable either
            # way, the request is fail-closed on both.
            assert result.reason == SESSION_INVALIDATED_REASON
        finally:
            sql.close()
