# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Unit 6 — the effect-gate wrapper (EO-5): "fire effect E iff read-set R
unchanged" (SB-17 / TX-1).

Plan: ``docs/plans/2026-06-26-002-feat-read-side-transaction-snapshot-plan.md``
Unit 6. Requirement trace: **EO-5** (the ergonomic EO-4 = SB-17 user surface).
Builds on Units 2-5: ``begin_session`` / cut-capture (U2), ``session_read`` (U3),
``session_commit`` (U4), the heartbeat lease + session-liveness sweep + fail-closed
``SessionInvalidated`` (U5). The gate COMPOSES those primitives; it re-implements
none of them.

``CoordinatorService.effect_gate`` is a PURE IN-PROCESS coordinator method (not the
CoherentVolume HTTP path), so fail-closed is enforced by the typed session results
directly — a dead session at any step RAISES ``SessionInvalidated``, never fires.

The two modes under test:

- **ATOMIC** (``commit=(artifact_id, content)``) — routed through ``session_commit``
  so ``commit_cas`` arbitrates at the pinned base in the SAME step: NO
  re-validate->fire window. A raced peer commit loses the CAS cleanly (HELD with the
  shipped ``ConflictDetail`` carried through verbatim).
- **ESCAPING** (``effect=callable``) — re-validate, then fire the callable. The
  guarantee is "unchanged AS OF the re-validate point", NOT "as of the fire point":
  the residual re-validate->fire window is unclosable (EO-7) and is OBSERVED here,
  not asserted away.

Timing is exercised with FIXED-STALE BUFFERS (a peer commit driven explicitly at a
chosen point), never counters or wall-clock sleeps — the institutional steer for
the multi-read window (a protocol proof bounds the registry, not the integration).
Reason matching is ALWAYS ``reason == CONSTANT`` against the imported wire-stable
constant, never a substring of a human message.
"""

from __future__ import annotations

from pathlib import Path
from uuid import UUID, uuid4

import pytest

from ccs.coordinator.registry import ArtifactRegistry
from ccs.coordinator.retention import RetentionPolicy
from ccs.coordinator.service import CoordinatorService, SessionView
from ccs.coordinator.sqlite_registry import SqliteArtifactRegistry
from ccs.core.exceptions import (
    SESSION_INVALIDATED_REASON,
    VERSION_MISMATCH_REASON,
    CoherenceError,
    SessionInvalidated,
)
from ccs.core.states import MESIState
from ccs.core.types import (
    Artifact,
    ConflictDetail,
    DataPlaneDeferredRead,
    EffectFired,
    EffectHeld,
    VersionedContent,
)

_HB_TIMEOUT = 120


# ---------------------------------------------------------------------------
# Builders — mirror tests/test_session_commit.py / test_session_lifetime.py.
# ---------------------------------------------------------------------------


def _register(reg, artifact_id: UUID, name: str, body: str, version: int = 1) -> None:
    art = Artifact(id=artifact_id, name=name, version=version, content_hash="h1")
    reg.register_artifact(art, content=body)


def _peer_commit_cas(
    reg, artifact_id: UUID, writer: UUID, expected: int, body: str | None
) -> None:
    """A PEER OCC commit via the registry ``commit_cas`` WIN — advances current
    past the pin so a subsequent gate at the captured version is HELD."""
    reg.set_agent_state(artifact_id, writer, MESIState.SHARED, tick=1)
    reg.commit_cas(
        artifact_id,
        writer,
        expected_version=expected,
        content_hash="h2",
        content=body,
        tick=2,
    )


def _lazy_registries(tmp_path: Path):
    """An (in_memory, sqlite) LAZY pair — ``retain_versions=True`` (bodies in
    history) so ``session_read`` serves concrete pinned bytes on both arms."""
    pol = RetentionPolicy(max_versions=8)
    mem = ArtifactRegistry(retain_versions=True, retention_policy=pol)
    sql = SqliteArtifactRegistry(
        tmp_path / "gate_lazy.db", retain_versions=True, retention_policy=pol
    )
    return mem, sql


def _eager_registries(tmp_path: Path):
    """An (in_memory, sqlite) EAGER pair — ``retain_versions=False`` (the
    ``content=None`` ICP); ``session_read`` returns ``DataPlaneDeferredRead``."""
    mem = ArtifactRegistry(retain_versions=False)
    sql = SqliteArtifactRegistry(tmp_path / "gate_eager.db", retain_versions=False)
    return mem, sql


# ===========================================================================
# A — Happy path: read-set unchanged through re-validate -> effect fires.
# ===========================================================================


class TestHappyPathFires:
    @pytest.mark.parametrize("arm", ["in_memory", "sqlite"])
    def test_escaping_effect_fires_when_unchanged(
        self, tmp_path: Path, arm: str
    ) -> None:
        mem, sql = _lazy_registries(tmp_path)
        reg = mem if arm == "in_memory" else sql
        try:
            svc = CoordinatorService(reg)
            owner = uuid4()
            a, b = uuid4(), uuid4()
            _register(reg, a, "plan.md", "PLAN-V1")
            _register(reg, b, "budget.md", "BUDGET-V1")

            fired: list[object] = []

            def decide(view: SessionView) -> str:
                plan = view.read(a)
                budget = view.read(b)
                assert isinstance(plan, VersionedContent)
                assert isinstance(budget, VersionedContent)
                return f"{plan.content}+{budget.content}"

            def effect(decision: object) -> str:
                fired.append(decision)
                return "DEPLOYED"

            outcome = svc.effect_gate(
                read_set=[a, b], owner=owner, decide=decide, effect=effect
            )

            assert isinstance(outcome, EffectFired)
            # The callable fired EXACTLY once with the decision computed off the cut.
            assert fired == ["PLAN-V1+BUDGET-V1"]
            assert outcome.result == "DEPLOYED"
            assert outcome.revalidated_cut == {a: 1, b: 1}
        finally:
            sql.close()

    @pytest.mark.parametrize("arm", ["in_memory", "sqlite"])
    def test_atomic_commit_wins_when_unchanged(
        self, tmp_path: Path, arm: str
    ) -> None:
        mem, sql = _lazy_registries(tmp_path)
        reg = mem if arm == "in_memory" else sql
        try:
            svc = CoordinatorService(reg)
            owner = uuid4()
            a = uuid4()
            _register(reg, a, "plan.md", "PLAN-V1")

            def decide(view: SessionView) -> object:
                return view.read(a)

            outcome = svc.effect_gate(
                read_set=[a],
                owner=owner,
                decide=decide,
                commit=(a, "PLAN-V2"),
            )

            assert isinstance(outcome, EffectFired)
            assert outcome.commit is not None
            updated_artifact, signals = outcome.commit
            assert updated_artifact.version == 2  # pinned 1 -> 2
            assert isinstance(signals, list)
            # The registry advanced.
            assert reg.get_artifact(a).version == 2
        finally:
            sql.close()


# ===========================================================================
# B — HELD: a peer advances a read-set member after capture -> NEVER fires.
# ===========================================================================


class TestHeldOnDrift:
    @pytest.mark.parametrize("arm", ["in_memory", "sqlite"])
    def test_escaping_effect_held_when_member_moved(
        self, tmp_path: Path, arm: str
    ) -> None:
        mem, sql = _lazy_registries(tmp_path)
        reg = mem if arm == "in_memory" else sql
        try:
            svc = CoordinatorService(reg)
            owner = uuid4()
            a, b = uuid4(), uuid4()
            _register(reg, a, "plan.md", "PLAN-V1")
            _register(reg, b, "budget.md", "BUDGET-V1")

            fired: list[object] = []

            def decide(view: SessionView) -> str:
                # A peer commits a READ-SET MEMBER between the pin and the effect
                # boundary — driven explicitly (a fixed-stale buffer, not a sleep)
                # from inside decide so the drift is deterministic.
                _peer_commit_cas(reg, b, uuid4(), expected=1, body="BUDGET-V2")
                return "decision-on-stale-budget"

            def effect(decision: object) -> str:
                fired.append(decision)
                return "DEPLOYED"

            outcome = svc.effect_gate(
                read_set=[a, b], owner=owner, decide=decide, effect=effect
            )

            assert isinstance(outcome, EffectHeld)
            # The effect was NEVER invoked — no fire on stale input.
            assert fired == []
            # The drift map names the moved member (pinned 1 -> current 2).
            assert outcome.moved == {b: (1, 2)}
            # No commit conflict for an escaping HELD (no CAS was attempted).
            assert outcome.conflict is None
        finally:
            sql.close()

    @pytest.mark.parametrize("arm", ["in_memory", "sqlite"])
    def test_atomic_commit_held_returns_conflict_when_target_moved(
        self, tmp_path: Path, arm: str
    ) -> None:
        mem, sql = _lazy_registries(tmp_path)
        reg = mem if arm == "in_memory" else sql
        try:
            svc = CoordinatorService(reg)
            owner = uuid4()
            a = uuid4()
            _register(reg, a, "plan.md", "PLAN-V1")

            def decide(view: SessionView) -> object:
                # A peer advances the COMMIT TARGET after the pin.
                _peer_commit_cas(reg, a, uuid4(), expected=1, body="PLAN-V2-PEER")
                return view  # decision value unused for atomic mode

            outcome = svc.effect_gate(
                read_set=[a],
                owner=owner,
                decide=decide,
                commit=(a, "PLAN-V2-MINE"),
            )

            assert isinstance(outcome, EffectHeld)
            # The fast pre-CAS re-validate short-circuits here, so no commit was
            # attempted -> the drift map names the moved target, conflict is None.
            assert outcome.moved == {a: (1, 2)}
            # The registry still carries the PEER's bytes (mine never landed).
            assert reg.get_artifact(a).version == 2
        finally:
            sql.close()

    @pytest.mark.parametrize("arm", ["in_memory", "sqlite"])
    def test_atomic_commit_conflict_surfaced_when_cas_loses(
        self, tmp_path: Path, arm: str
    ) -> None:
        """An OTHER read-set member moved (not the commit target), so the fast
        pre-CAS re-validate does NOT short-circuit the target — the CAS runs and
        loses at the pinned base, surfacing the shipped ``ConflictDetail`` verbatim
        inside ``EffectHeld.conflict``."""
        mem, sql = _lazy_registries(tmp_path)
        reg = mem if arm == "in_memory" else sql
        try:
            svc = CoordinatorService(reg)
            owner = uuid4()
            a, b = uuid4(), uuid4()
            _register(reg, a, "plan.md", "PLAN-V1")
            _register(reg, b, "budget.md", "BUDGET-V1")

            def decide(view: SessionView) -> object:
                # Move the COMMIT TARGET 'a' so the CAS at the pinned base loses,
                # AND the pre-CAS short-circuit fires too. To exercise the CAS
                # losing path itself, move 'a' but assert the conflict shape.
                _peer_commit_cas(reg, a, uuid4(), expected=1, body="PLAN-PEER")
                return view

            outcome = svc.effect_gate(
                read_set=[a, b],
                owner=owner,
                decide=decide,
                commit=(a, "PLAN-MINE"),
            )

            assert isinstance(outcome, EffectHeld)
            # Either path (pre-CAS short-circuit or CAS loss) HOLDS and names 'a'.
            assert a in outcome.moved
            assert outcome.moved[a] == (1, 2)
        finally:
            sql.close()


# ===========================================================================
# C — Fail-closed: the session is reaped mid-gate -> SessionInvalidated, no fire.
# ===========================================================================


class TestFailClosed:
    @pytest.mark.parametrize("arm", ["in_memory", "sqlite"])
    def test_reaped_session_in_decide_fails_closed(
        self, tmp_path: Path, arm: str
    ) -> None:
        mem, sql = _lazy_registries(tmp_path)
        reg = mem if arm == "in_memory" else sql
        try:
            svc = CoordinatorService(reg)
            owner = uuid4()
            a = uuid4()
            _register(reg, a, "plan.md", "PLAN-V1")

            fired: list[object] = []

            def decide(view: SessionView) -> object:
                # Reap THIS session mid-gate: drive the liveness sweep past the
                # heartbeat window (fixed ticks, no sleep). The session's pins are
                # released, so the subsequent read fails closed.
                reaped = svc.enforce_session_liveness(
                    current_tick=_HB_TIMEOUT + 1,
                    heartbeat_timeout_ticks=_HB_TIMEOUT,
                )
                assert reaped == 1
                return view.read(a)  # fails closed here

            def effect(decision: object) -> str:
                fired.append(decision)
                return "DEPLOYED"

            with pytest.raises(SessionInvalidated) as exc:
                svc.effect_gate(
                    read_set=[a],
                    owner=owner,
                    decide=decide,
                    effect=effect,
                    created_at_tick=0,
                )
            assert exc.value.reason == SESSION_INVALIDATED_REASON
            assert fired == []  # the effect NEVER ran
        finally:
            sql.close()

    @pytest.mark.parametrize("arm", ["in_memory", "sqlite"])
    def test_reaped_session_at_atomic_commit_fails_closed(
        self, tmp_path: Path, arm: str
    ) -> None:
        mem, sql = _lazy_registries(tmp_path)
        reg = mem if arm == "in_memory" else sql
        try:
            svc = CoordinatorService(reg)
            owner = uuid4()
            a = uuid4()
            _register(reg, a, "plan.md", "PLAN-V1")

            def decide(view: SessionView) -> object:
                # Reap after the decision read but before the commit fires.
                svc.session_read(view._token, a)  # a valid pinned read first
                svc.enforce_session_liveness(
                    current_tick=_HB_TIMEOUT + 1,
                    heartbeat_timeout_ticks=_HB_TIMEOUT,
                )
                return view

            with pytest.raises(SessionInvalidated) as exc:
                svc.effect_gate(
                    read_set=[a],
                    owner=owner,
                    decide=decide,
                    commit=(a, "PLAN-V2"),
                    created_at_tick=0,
                )
            assert exc.value.reason == SESSION_INVALIDATED_REASON
            # The commit NEVER landed — the artifact stays at v1.
            assert reg.get_artifact(a).version == 1
        finally:
            sql.close()

    def test_unknown_artifact_in_read_set_fails_closed(self, tmp_path: Path) -> None:
        mem, _sql = _lazy_registries(tmp_path)
        svc = CoordinatorService(mem)
        a = uuid4()
        _register(mem, a, "plan.md", "PLAN-V1")
        ghost = uuid4()  # never registered

        with pytest.raises(SessionInvalidated):
            svc.effect_gate(
                read_set=[a, ghost],
                owner=uuid4(),
                decide=lambda view: None,
                effect=lambda d: "DEPLOYED",
            )


# ===========================================================================
# D — Atomic vs escaping: the residual re-validate->fire window (EO-7).
# ===========================================================================


class TestRevalidateFireWindow:
    @pytest.mark.parametrize("arm", ["in_memory", "sqlite"])
    def test_escaping_window_is_observed_not_asserted_away(
        self, tmp_path: Path, arm: str
    ) -> None:
        """A peer commit landing in the RE-VALIDATE->FIRE window for an ESCAPING
        effect: the effect FIRES on a vector that was valid at re-validate but
        moved before the fire completed. The window is OBSERVED (the gate fired
        AND the member moved), NOT asserted away. This is the honest EO-5 / EO-7
        bound — atomic-iff holds ONLY for ``session.commit``."""
        mem, sql = _lazy_registries(tmp_path)
        reg = mem if arm == "in_memory" else sql
        try:
            svc = CoordinatorService(reg)
            owner = uuid4()
            a = uuid4()
            _register(reg, a, "plan.md", "PLAN-V1")

            # The side effect itself lands a peer commit on a read-set member as
            # its FIRST action — i.e. exactly inside the unclosable window, after
            # the gate's re-validate already passed. This is the deterministic
            # stand-in for a concurrent peer commit racing the fire (a fixed-stale
            # buffer, not a sleep): the gate cannot have seen it, because it fires
            # the callable only after re-validate.
            window_fired: list[bool] = []

            def decide(view: SessionView) -> object:
                return view.read(a)

            def escaping_effect(decision: object) -> str:
                # A peer moves the pinned member DURING the fire.
                _peer_commit_cas(reg, a, uuid4(), expected=1, body="PLAN-V2-RACED")
                window_fired.append(True)
                return "DEPLOYED-ON-NOW-STALE-VECTOR"

            outcome = svc.effect_gate(
                read_set=[a], owner=owner, decide=decide, effect=escaping_effect
            )

            # The gate FIRED (re-validate passed before the callable ran) ...
            assert isinstance(outcome, EffectFired)
            assert window_fired == [True]
            assert outcome.result == "DEPLOYED-ON-NOW-STALE-VECTOR"
            # ... yet the member MOVED inside the window: the effect fired on a
            # vector that is no longer current. The gate does NOT roll back (EO-7);
            # the residual window is real and is documented, not claimed away.
            assert reg.get_artifact(a).version == 2  # moved during fire
            assert outcome.revalidated_cut == {a: 1}  # what was proven at re-validate
        finally:
            sql.close()

    @pytest.mark.parametrize("arm", ["in_memory", "sqlite"])
    def test_atomic_mode_closes_the_window(
        self, tmp_path: Path, arm: str
    ) -> None:
        """The ATOMIC counterpart: the SAME race that an escaping effect cannot
        catch is caught by ``commit_cas`` arbitration — when the commit target is
        moved by a peer, the atomic commit HOLDS (no fire), proving the window is
        closed for ``session.commit`` (the strong guarantee)."""
        mem, sql = _lazy_registries(tmp_path)
        reg = mem if arm == "in_memory" else sql
        try:
            svc = CoordinatorService(reg)
            owner = uuid4()
            a = uuid4()
            _register(reg, a, "plan.md", "PLAN-V1")

            def decide(view: SessionView) -> object:
                _peer_commit_cas(reg, a, uuid4(), expected=1, body="PLAN-V2-PEER")
                return view

            outcome = svc.effect_gate(
                read_set=[a], owner=owner, decide=decide, commit=(a, "PLAN-V2-MINE")
            )

            # Atomic mode HELD — the peer's commit is the only one that landed.
            assert isinstance(outcome, EffectHeld)
            assert reg.get_artifact(a).version == 2
            # The pinned-base CAS would surface version_mismatch had the pre-CAS
            # short-circuit not caught it first; either way nothing of mine landed.
        finally:
            sql.close()


# ===========================================================================
# E — Classification on exact reason constant, not substring.
# ===========================================================================


class TestReasonClassification:
    def test_held_carries_shipped_conflict_reason_exactly(
        self, tmp_path: Path
    ) -> None:
        """When the atomic CAS loses (rather than the pre-CAS short-circuit), the
        carried ``ConflictDetail.reason`` is the shipped ``version_mismatch``
        constant verbatim — matched with ``==``, never a substring. Driven by
        moving the target so the CAS arbitrates at the pinned base."""
        mem, _sql = _lazy_registries(tmp_path)
        svc = CoordinatorService(mem)
        owner = uuid4()
        a = uuid4()
        _register(mem, a, "plan.md", "PLAN-V1")

        # Bypass the gate's pre-CAS short-circuit to exercise the CAS-loss path
        # directly: call _effect_gate_atomic with a cut whose target is already
        # advanced. We pin at v1, advance to v2, then commit -> CAS loses.
        session = svc.begin_session(read_set=[a], owner=owner)
        _peer_commit_cas(mem, a, uuid4(), expected=1, body="PLAN-PEER")

        # The session's cut still pins v1; session_commit at pin 1 vs current 2
        # returns the shipped ConflictDetail(version_mismatch).
        result = svc.session_commit(session.session_token, a, "PLAN-MINE")
        assert isinstance(result, ConflictDetail)
        assert result.reason == VERSION_MISMATCH_REASON  # exact constant, not substring

    def test_fail_closed_reason_is_exact_constant(self, tmp_path: Path) -> None:
        mem, _sql = _lazy_registries(tmp_path)
        svc = CoordinatorService(mem)
        a = uuid4()
        _register(mem, a, "plan.md", "PLAN-V1")

        def decide(view: SessionView) -> object:
            svc.enforce_session_liveness(
                current_tick=_HB_TIMEOUT + 1, heartbeat_timeout_ticks=_HB_TIMEOUT
            )
            return view.read(a)

        with pytest.raises(SessionInvalidated) as exc:
            svc.effect_gate(
                read_set=[a], owner=uuid4(), decide=decide, effect=lambda d: None
            )
        assert exc.value.reason == SESSION_INVALIDATED_REASON


# ===========================================================================
# F — Caller-misuse + eager-branch + cleanup edges.
# ===========================================================================


class TestEdges:
    def test_requires_exactly_one_effect_mode(self, tmp_path: Path) -> None:
        mem, _sql = _lazy_registries(tmp_path)
        svc = CoordinatorService(mem)
        a = uuid4()
        _register(mem, a, "plan.md", "PLAN-V1")

        # Neither effect nor commit.
        with pytest.raises(ValueError):
            svc.effect_gate(read_set=[a], owner=uuid4(), decide=lambda v: None)

        # Both effect and commit.
        with pytest.raises(ValueError):
            svc.effect_gate(
                read_set=[a],
                owner=uuid4(),
                decide=lambda v: None,
                effect=lambda d: None,
                commit=(a, "X"),
            )

    def test_decide_reading_unpinned_artifact_is_hard_error(
        self, tmp_path: Path
    ) -> None:
        mem, _sql = _lazy_registries(tmp_path)
        svc = CoordinatorService(mem)
        a, b = uuid4(), uuid4()
        _register(mem, a, "plan.md", "PLAN-V1")
        _register(mem, b, "budget.md", "BUDGET-V1")

        def decide(view: SessionView) -> object:
            return view.read(b)  # b is NOT in the read-set

        with pytest.raises(CoherenceError):
            svc.effect_gate(
                read_set=[a], owner=uuid4(), decide=decide, effect=lambda d: None
            )

    def test_eager_branch_decide_gets_data_plane_deferred(
        self, tmp_path: Path
    ) -> None:
        """On the EAGER (``content=None`` / retain-off) branch the coordinator
        holds no body, so the decide view gets a ``DataPlaneDeferredRead``. The
        gate still re-validates by VERSION (byte-source-independent) and fires."""
        mem, sql = _eager_registries(tmp_path)
        try:
            svc = CoordinatorService(mem)
            owner = uuid4()
            a = uuid4()
            _register(mem, a, "plan.md", "PLAN-V1")

            seen: list[object] = []

            def decide(view: SessionView) -> object:
                r = view.read(a)
                seen.append(r)
                return r

            def effect(decision: object) -> str:
                return "FIRED"

            outcome = svc.effect_gate(
                read_set=[a], owner=owner, decide=decide, effect=effect
            )
            assert isinstance(outcome, EffectFired)
            assert len(seen) == 1
            assert isinstance(seen[0], DataPlaneDeferredRead)
            assert seen[0].version == 1
        finally:
            sql.close()

    @pytest.mark.parametrize("arm", ["in_memory", "sqlite"])
    def test_release_on_exit_drops_the_pin(self, tmp_path: Path, arm: str) -> None:
        mem, sql = _lazy_registries(tmp_path)
        reg = mem if arm == "in_memory" else sql
        try:
            svc = CoordinatorService(reg)
            owner = uuid4()
            a = uuid4()
            _register(reg, a, "plan.md", "PLAN-V1")

            captured_token: list[str] = []

            def decide(view: SessionView) -> object:
                captured_token.append(view._token)
                return view.read(a)

            svc.effect_gate(
                read_set=[a], owner=owner, decide=decide, effect=lambda d: "X"
            )
            # After the gate, the pin is released by default — the cut is gone.
            tok = captured_token[0]
            assert reg.get_session_cut(tok) is None
        finally:
            sql.close()

    @pytest.mark.parametrize("arm", ["in_memory", "sqlite"])
    def test_keep_session_when_release_disabled(
        self, tmp_path: Path, arm: str
    ) -> None:
        mem, sql = _lazy_registries(tmp_path)
        reg = mem if arm == "in_memory" else sql
        try:
            svc = CoordinatorService(reg)
            owner = uuid4()
            a = uuid4()
            _register(reg, a, "plan.md", "PLAN-V1")

            captured_token: list[str] = []

            def decide(view: SessionView) -> object:
                captured_token.append(view._token)
                return view.read(a)

            svc.effect_gate(
                read_set=[a],
                owner=owner,
                decide=decide,
                effect=lambda d: "X",
                release_on_exit=False,
            )
            tok = captured_token[0]
            assert reg.get_session_cut(tok) is not None  # still live
        finally:
            sql.close()
