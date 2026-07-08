# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Unit 9 — SB-18 non-foreclosure constructibility HARNESS (SB-17 / TX-1).

Plan: ``docs/plans/2026-06-26-002-feat-read-side-transaction-snapshot-plan.md``
Unit 9. Requirement trace: **R11** (read + exactly one validated commit MUST NOT
foreclose SB-18 atomic multi-publish; falsifiable paper-signature; must not
pre-build it).

**This is a HARNESS, not an implementation.** It is the recurring MECHANIZATION
of the pre-freeze R11 DESIGN GATE that ran in Unit 2 (the
``✅ R11 GATE — PASSED 2026-06-28 (pre-Unit-2)`` note under Unit 2 of the plan).
The design gate asserted ON PAPER, before ``begin_session``'s public types froze,
that a paper SB-18 commit signature is CONSTRUCTIBLE from v1's return shape
WITHOUT changing v1's public types. This file re-runs that assertion MECHANICALLY
against the NOW-frozen surface, so the gate cannot silently regress as later work
touches the types.

**The paper SB-18 signature (verbatim from the Unit-2 R11 gate note):**

    session.commit_all(session_token, writes: Mapping[UUID, bytes | str])
        -> MultiCommitResult

all-or-nothing, each artifact validated at ``cut[artifact_id]``. SB-18 itself is
DEFERRED to a separate gate (A2) — there is NO ``commit_all`` / ``MultiCommitResult``
/ ``MultiCommitConflict`` anywhere in ``src/`` (asserted below). This harness only
proves v1 does not FORECLOSE it.

**What would FALSIFY non-foreclosure (i.e. make this file FAIL):**

1. ``SnapshotSession.cut`` becomes an OPAQUE handle instead of an inspectable
   ``{artifact_id: int}`` map → SB-18 has no PUBLIC way to read each artifact's
   ``expected_version`` → foreclosed. ``TestCutIsInspectable`` fails.
2. ``ConflictDetail`` / ``CasCorruption`` change shape so a paper
   ``MultiCommitConflict(Mapping[UUID, ConflictDetail])`` aggregate can no longer
   be CONSTRUCTED from real per-artifact instances without editing v1 →
   ``TestPerArtifactOutcomesComposeVerbatim`` fails.
3. ``session_commit`` (the per-artifact OCC step SB-18's loop reuses) stops
   accepting the ``cut[id]``-derived pinned base or stops returning the verbatim
   taxonomy → the in-test ``commit_all`` shim's per-artifact WIN/HELD breaks →
   ``TestPaperCommitAllShim`` fails.

The shim in ``TestPaperCommitAllShim`` is DEFINED IN THE TEST (never in ``src``):
it demonstrates the v1 surface ALREADY supplies everything SB-18 needs (a cut to
read ``expected_version`` from + a per-artifact validated commit + a verbatim
taxonomy to aggregate). The point is to FALSIFY foreclosure, not to ship SB-18.

Reason matching is ALWAYS ``reason == CONSTANT`` against the imported wire-stable
constant — never a substring of a human message (the typed-signal-not-substring
house rule).
"""

from __future__ import annotations

import inspect
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping
from uuid import UUID, uuid4

import pytest

from ccs.coordinator.registry import ArtifactRegistry
from ccs.coordinator.retention import RetentionPolicy
from ccs.coordinator.service import CoordinatorService
from ccs.coordinator.sqlite_registry import SqliteArtifactRegistry
from ccs.core.types import (
    Artifact,
    CasCorruption,
    ConflictDetail,
    InvalidationSignal,
    SessionCommitRejection,
    SnapshotSession,
)

# ---------------------------------------------------------------------------
# Builders — mirror tests/test_session_commit.py's helpers.
# ---------------------------------------------------------------------------


def _register(reg, artifact_id: UUID, name: str, body: str, version: int = 1) -> None:
    art = Artifact(id=artifact_id, name=name, version=version, content_hash="h1")
    reg.register_artifact(art, content=body)


def _registries(tmp_path: Path):
    """An (in_memory, sqlite) LAZY pair — ``retain_versions=True`` (bodies in
    history), matching the Unit-4 commit suite so the shim's per-artifact OCC
    runs identically on both arms."""
    pol = RetentionPolicy(max_versions=8)
    mem = ArtifactRegistry(retain_versions=True, retention_policy=pol)
    sql = SqliteArtifactRegistry(
        tmp_path / "sb18.db", retain_versions=True, retention_policy=pol
    )
    return mem, sql


# ===========================================================================
# A — SB-18 is NOT implemented in v1 (the harness proves non-FORECLOSURE, not
#     a pre-build; if SB-18 ever ships, this fence is what tells us to retire it)
# ===========================================================================


class TestSb18NowShipping:
    """SB-18 (``commit_all``) is now SHIPPING — un-gated 2026-07-07 by the founder
    substrate bet. The former "is NOT prebuilt" fence is RETIRED here, flipped to
    POSITIVE existence checks per this file's own note ("if SB-18 ever ships, this
    fence is what tells us to retire it"). The rest of the suite (the
    inspectable-cut substrate the write side builds on) still holds verbatim."""

    def test_multi_commit_types_exist_in_core(self) -> None:
        # The paper aggregates promoted into the frozen core types (Unit 1).
        import ccs.core.types as core_types

        for shipped in ("MultiCommitResult", "MultiCommitConflict", "CommitAllEntry"):
            assert hasattr(core_types, shipped), (
                f"{shipped} missing from ccs.core.types — SB-18 shipped; this "
                f"aggregate must exist"
            )

    def test_commit_all_on_both_registries(self) -> None:
        # The atomic multi-artifact publish primitive now exists on BOTH backends
        # (the correctness core; the service/session surface lands in Unit 3).
        from ccs.coordinator.registry import ArtifactRegistry
        from ccs.coordinator.sqlite_registry import SqliteArtifactRegistry

        assert hasattr(ArtifactRegistry, "commit_all")
        assert hasattr(SqliteArtifactRegistry, "commit_all")


# ===========================================================================
# B — The load-bearing R11 property: the cut is an INSPECTABLE map (not opaque)
# ===========================================================================


class TestCutIsInspectable:
    """SB-18's ``commit_all`` needs each artifact's ``expected_version``. The ONLY
    public way it can obtain them is ``SnapshotSession.cut[artifact_id]``. If the
    cut ever became an OPAQUE handle, that vector would be unobtainable from the
    public surface → SB-18 foreclosed. This is the FALSIFIABLE core of R11."""

    @pytest.mark.parametrize("arm", ["in_memory", "sqlite"])
    def test_every_expected_version_is_readable_by_artifact_id(
        self, tmp_path: Path, arm: str
    ) -> None:
        mem, sql = _registries(tmp_path)
        reg = mem if arm == "in_memory" else sql
        try:
            svc = CoordinatorService(reg)
            a, b, c = uuid4(), uuid4(), uuid4()
            _register(reg, a, "plan.md", "A1", version=1)
            _register(reg, b, "budget.md", "B7", version=7)
            _register(reg, c, "manifest.md", "C3", version=3)
            owner = uuid4()
            session = svc.begin_session(read_set=[a, b, c], owner=owner)
            assert isinstance(session, SnapshotSession)

            # SB-18's commit_all would loop these expected_versions out of the cut
            # BY ARTIFACT ID — assert every one is readable (no opaque handle).
            expected_versions = {aid: session.cut[aid] for aid in (a, b, c)}
            assert expected_versions == {a: 1, b: 7, c: 3}
            # The cut is keyed by the artifact UUIDs themselves, not a blob token.
            assert set(session.cut.keys()) == {a, b, c}
            # It is a real Mapping[UUID, int] — inspectable item access + iteration,
            # not an opaque object exposing only equality.
            assert isinstance(session.cut, Mapping)
            for aid, version in session.cut.items():
                assert isinstance(aid, UUID)
                assert isinstance(version, int)
        finally:
            sql.close()

    def test_cut_is_typed_as_a_mapping_not_an_opaque_token(self) -> None:
        # The frozen TYPE annotation pins the contract structurally: cut is a
        # Mapping[UUID, int], session_token is the SEPARATE opaque identity. A
        # refactor that folded the versions into the token (making cut opaque)
        # would change this annotation and trip the harness.
        ann = inspect.get_annotations(SnapshotSession, eval_str=True)
        cut_origin = getattr(ann["cut"], "__origin__", None)
        assert cut_origin is not None and issubclass(cut_origin, Mapping), (
            f"SnapshotSession.cut is no longer a Mapping (got {ann['cut']!r}) — an "
            f"opaque cut handle would forclose SB-18's expected-version vector"
        )
        # And the token is the identity, kept DISTINCT from the inspectable cut.
        assert ann["session_token"] is str


# ===========================================================================
# C — Per-artifact outcome types compose VERBATIM into the paper aggregate
# ===========================================================================


# The paper SB-18 aggregate — DEFINED HERE IN THE TEST, never in ``src``. It is a
# pure composition of the FROZEN v1 ``ConflictDetail`` (no field of v1 changes to
# make this constructible). If constructing it required editing ``ConflictDetail``
# / ``CasCorruption``, SB-18 would be foreclosed and this module would fail.
@dataclass(frozen=True)
class _PaperMultiCommitConflict:
    """A paper ``MultiCommitConflict`` — the all-or-nothing HELD aggregate SB-18
    would return, composing the per-artifact :class:`ConflictDetail` VERBATIM."""

    per_artifact: Mapping[UUID, ConflictDetail]


class TestPerArtifactOutcomesComposeVerbatim:
    """A paper ``MultiCommitConflict(Mapping[UUID, ConflictDetail])`` is
    CONSTRUCTIBLE from REAL ``ConflictDetail`` instances with NO change to
    ``ConflictDetail`` / ``CasCorruption``. Built from instances the SHIPPED
    ``session_commit`` actually returns — not synthetic ones — so the proof is
    against the live taxonomy, not a guess at it."""

    @pytest.mark.parametrize("arm", ["in_memory", "sqlite"])
    def test_real_conflict_details_aggregate_without_v1_change(
        self, tmp_path: Path, arm: str
    ) -> None:
        mem, sql = _registries(tmp_path)
        reg = mem if arm == "in_memory" else sql
        try:
            svc = CoordinatorService(reg)
            # Produce TWO real ConflictDetail instances from the shipped surface:
            # a version_mismatch (peer moved the base) and an other_holder (a
            # pessimistic peer holds E at the pinned version).
            a, b = uuid4(), uuid4()
            _register(reg, a, "plan.md", "A1", version=1)
            _register(reg, b, "budget.md", "B1", version=1)
            owner = uuid4()
            s_a = svc.begin_session(read_set=[a], owner=owner)
            s_b = svc.begin_session(read_set=[b], owner=owner)

            from ccs.core.states import MESIState

            # a → version_mismatch: a peer commits past the pin.
            reg.set_agent_state(a, uuid4(), MESIState.SHARED, tick=1)
            reg.commit_cas(
                a, uuid4(), expected_version=1, content_hash="h2", content="A2", tick=2
            )
            mismatch = svc.session_commit(s_a.session_token, a, "MINE", caller=owner)
            # b → other_holder: a pessimistic peer takes EXCLUSIVE at the pin.
            reg.set_agent_state(b, uuid4(), MESIState.EXCLUSIVE, trigger="write", tick=1)
            holder = svc.session_commit(s_b.session_token, b, "MINE", caller=owner)

            assert isinstance(mismatch, ConflictDetail)
            assert isinstance(holder, ConflictDetail)
            assert mismatch.reason == "version_mismatch"
            assert holder.reason == "other_holder"

            # CONSTRUCT the paper aggregate from the REAL per-artifact members —
            # verbatim, no field of ConflictDetail touched. This is the R11
            # composition proof: if it raised / required adaptation, SB-18's
            # all-or-nothing HELD shape would be foreclosed.
            aggregate = _PaperMultiCommitConflict(per_artifact={a: mismatch, b: holder})
            assert aggregate.per_artifact[a] is mismatch
            assert aggregate.per_artifact[b] is holder
            # The members keep their identity + full taxonomy inside the aggregate.
            assert aggregate.per_artifact[a].reason == "version_mismatch"
            assert aggregate.per_artifact[a].current_version == 2
            assert aggregate.per_artifact[b].reason == "other_holder"
            assert aggregate.per_artifact[b].current_version == 1
        finally:
            sql.close()

    def test_cas_corruption_is_reusable_verbatim_as_a_paper_member(self) -> None:
        # SB-18's per-artifact corruption signal reuses CasCorruption verbatim too
        # (the registry returns it; the service raises). Construct a paper member
        # map from a real CasCorruption — no field changes.
        corruption = CasCorruption(current_version=3)
        paper_members: Mapping[UUID, CasCorruption] = {uuid4(): corruption}
        (only_member,) = paper_members.values()
        assert only_member is corruption
        assert only_member.current_version == 3
        # The two per-artifact signals stay DISTINCT types (the service maps the
        # corruption sentinel to a raise; the conflict to a returned aggregate) —
        # SB-18 inherits that split unchanged.
        assert CasCorruption is not ConflictDetail


# ===========================================================================
# D — The paper ``commit_all`` SHIM: v1 already supplies everything SB-18 needs
# ===========================================================================


def _paper_commit_all(
    svc: CoordinatorService,
    session: SnapshotSession,
    writes: Mapping[UUID, bytes | str],
    *,
    caller: UUID,
) -> "tuple[str, object]":
    """A PAPER ``commit_all`` shim — DEFINED IN THE TEST, never in ``src``.

    Demonstrates the v1 surface ALREADY supplies everything the paper SB-18
    signature needs, with NO change to any v1 public type:

    - reads each ``expected_version`` straight out of the INSPECTABLE
      ``session.cut`` (the R11 binding constraint),
    - validates each artifact with the SHIPPED ``session_commit`` at ``cut[id]``,
    - aggregates the per-artifact outcomes using the VERBATIM ``ConflictDetail``
      into the paper :class:`_PaperMultiCommitConflict`.

    This is a PRE-FLIGHT (read-only intent) check + a non-atomic loop — NOT the
    real SB-18 (which adds a single atomic multi-row registry boundary, a NEW
    registry op, not a change to ``commit_cas``). Its ONLY job is to FALSIFY
    foreclosure: if v1's frozen types could not supply the cut / per-artifact
    validation / verbatim taxonomy, this shim could not be written from the public
    surface alone. Returns ``("conflict", aggregate)`` if ANY artifact is HELD
    (all-or-nothing: nothing is treated as committed), else ``("win", results)``.
    """
    # Every expected_version comes from the inspectable cut — the load-bearing
    # R11 read. An opaque cut would make this line unwritable from public v1.
    expected_versions = {aid: session.cut[aid] for aid in writes}

    held: dict[UUID, ConflictDetail] = {}
    wins: dict[UUID, object] = {}
    for artifact_id, body in writes.items():
        # The pinned base IS cut[artifact_id]; session_commit validates against it.
        assert artifact_id in expected_versions
        outcome = svc.session_commit(session.session_token, artifact_id, body, caller=caller)
        if isinstance(outcome, ConflictDetail):
            held[artifact_id] = outcome
        elif isinstance(outcome, SessionCommitRejection):
            # A validation rejection is also a non-win for the all-or-nothing
            # decision; surface it as a synthetic conflict so the aggregate is
            # uniform (the paper SB-18 would fold these into MultiCommitConflict).
            held[artifact_id] = ConflictDetail(
                reason="version_mismatch", current_version=-1
            )
        else:
            wins[artifact_id] = outcome
    if held:
        return "conflict", _PaperMultiCommitConflict(per_artifact=held)
    return "win", wins


class TestPaperCommitAllShim:
    """The shim runs end-to-end against the live v1 surface on BOTH registries —
    proving the public contract already supplies the cut, the per-artifact
    validated commit, and the verbatim taxonomy SB-18 composes. If any of those
    regressed, the shim would not produce the expected WIN / all-HELD outcomes."""

    @pytest.mark.parametrize("arm", ["in_memory", "sqlite"])
    def test_all_unchanged_bases_all_win(self, tmp_path: Path, arm: str) -> None:
        mem, sql = _registries(tmp_path)
        reg = mem if arm == "in_memory" else sql
        try:
            svc = CoordinatorService(reg)
            a, b = uuid4(), uuid4()
            _register(reg, a, "plan.md", "A1", version=1)
            _register(reg, b, "budget.md", "B5", version=5)
            owner = uuid4()
            session = svc.begin_session(read_set=[a, b], owner=owner)
            assert isinstance(session, SnapshotSession)

            status, payload = _paper_commit_all(
                svc, session, {a: "A2", b: "B6"}, caller=owner
            )
            assert status == "win", f"expected all-win, got {status}: {payload!r}"
            # Each per-artifact WIN is the shipped (Artifact, signals) tuple, bumped
            # off ITS OWN pinned base (cut[a]=1 → 2, cut[b]=5 → 6).
            updated_a, signals_a = payload[a]
            updated_b, _signals_b = payload[b]
            assert updated_a.version == 2 and updated_b.version == 6
            assert all(isinstance(s, InvalidationSignal) for s in signals_a)
            assert reg.get_artifact(a).version == 2
            assert reg.get_artifact(b).version == 6
        finally:
            sql.close()

    @pytest.mark.parametrize("arm", ["in_memory", "sqlite"])
    def test_one_moved_base_aggregates_a_paper_conflict(
        self, tmp_path: Path, arm: str
    ) -> None:
        mem, sql = _registries(tmp_path)
        reg = mem if arm == "in_memory" else sql
        try:
            from ccs.core.states import MESIState

            svc = CoordinatorService(reg)
            a, b = uuid4(), uuid4()
            _register(reg, a, "plan.md", "A1", version=1)
            _register(reg, b, "budget.md", "B1", version=1)
            owner = uuid4()
            session = svc.begin_session(read_set=[a, b], owner=owner)

            # A peer moves b past its pin AFTER capture → b will be HELD.
            reg.set_agent_state(b, uuid4(), MESIState.SHARED, tick=1)
            reg.commit_cas(
                b, uuid4(), expected_version=1, content_hash="h2", content="B2", tick=2
            )

            status, payload = _paper_commit_all(
                svc, session, {a: "A2", b: "B2-MINE"}, caller=owner
            )
            # All-or-nothing semantics: ANY HELD member → a paper conflict
            # aggregate composed from the VERBATIM ConflictDetail.
            assert status == "conflict"
            assert isinstance(payload, _PaperMultiCommitConflict)
            assert set(payload.per_artifact) == {b}
            assert payload.per_artifact[b].reason == "version_mismatch"
            assert payload.per_artifact[b].current_version == 2
        finally:
            sql.close()
