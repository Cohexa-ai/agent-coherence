# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Teeth: a deliberately-degraded backend MUST FAIL the kit (plan Unit 5).

A conformance kit that every plausible implementation passes proves nothing. This
test builds a backend that is correct on the version-CAS leg but DROPS one leg of
the R9 atomic boundary — the grant arbitration — and asserts the kit's
single-writer-under-contention scenario CATCHES it. If the degraded stub ever
passed that scenario, the kit would be decorative; the assertion here is that it
does NOT (``pytest.raises(AssertionError)``), and that the failure message NAMES
the missing tuple element so a real backend author knows what they skipped.

The degraded stub (:class:`_GrantBlindRegistry`) wraps a real in-memory registry
and delegates EVERYTHING to it EXCEPT ``commit_cas``, which it reimplements as a
VERSION-ONLY compare-and-swap: it checks the artifact version but SKIPS the
``other_holder`` grant check that the real ``commit_cas`` performs in the same
atomic step. That is exactly the bug a naive "just do a version CAS in the
backend" implementation would ship — the version matches, so a version-only CAS
lets an OCC writer win while a pessimistic peer still holds MODIFIED, producing
TWO writers. The kit's single-writer scenario is written to isolate precisely
this leg (the OCC writer commits at the CORRECT version, so only the grant check
can stop it).

The MUST-MATCH scenarios that do NOT depend on the grant leg (pure version-CAS
arbitration, admit-on-absent) still PASS the stub — proving the teeth test fails
the stub for the RIGHT reason (the dropped grant leg), not because the stub is
broken everywhere.
"""

from __future__ import annotations

from pathlib import Path
from uuid import UUID

import pytest

from ccs.coordinator.registry import ArtifactRegistry
from ccs.coordinator.registry_protocol import CasResult
from ccs.core.states import MESIState
from ccs.core.types import CasCorruption, ConflictDetail
from tests.backend_conformance import kit
from tests.backend_conformance.kit import RegistryFactory

_M_OR_E = frozenset({MESIState.MODIFIED, MESIState.EXCLUSIVE})


class _GrantBlindRegistry:
    """A degraded backend: correct version-CAS, but the grant-arbitration leg of
    the R9 boundary is DROPPED. Delegates everything to a wrapped real in-memory
    registry via ``__getattr__`` EXCEPT ``commit_cas``, which it reimplements to
    skip the ``other_holder`` check — so a version-matching OCC writer wins even
    while a pessimistic peer holds MODIFIED (single-writer violated)."""

    def __init__(self) -> None:
        self._inner = ArtifactRegistry(retain_versions=True)

    def __getattr__(self, name: str) -> object:
        # Every member except commit_cas comes straight from the real registry.
        return getattr(self._inner, name)

    def commit_cas(  # noqa: D401 - degraded on purpose
        self,
        artifact_id: UUID,
        agent_id: UUID,
        *,
        expected_version: int,
        content_hash: str,
        size_tokens: int | None = None,
        content: bytes | str | None = None,
        tick: int = 0,
        trigger: str = "commit_cas",
    ) -> CasResult:
        """Version-only CAS — the grant-arbitration leg is intentionally MISSING.

        It performs the version compare correctly (so the pure-CAS and
        admit-on-absent scenarios still pass), then WINS without checking whether a
        peer holds M/E. To win it delegates to the inner registry's real
        ``commit_cas`` AFTER neutralizing any peer M/E grant — mechanically the
        same effect as a backend that simply never consulted ``state_by_agent``:
        the peer grant does not block the write."""
        record = self._inner._records.get(artifact_id)  # noqa: SLF001 - test stub reaches in
        if record is None:
            raise KeyError(f"artifact {artifact_id} not in registry")
        current = record.artifact.version
        if expected_version > current:
            return CasCorruption(current_version=current)
        if expected_version < current:
            return ConflictDetail("version_mismatch", current)
        # THE DROPPED LEG: a correct commit_cas rejects here when a peer holds
        # M/E (ConflictDetail("other_holder")). This degraded stub does NOT —
        # it demotes every peer M/E grant so the inner real commit_cas cannot
        # see a competing holder, then wins on the version leg alone.
        for peer_id, state in list(record.state_by_agent.items()):
            if peer_id != agent_id and state in _M_OR_E:
                record.state_by_agent[peer_id] = MESIState.SHARED
        return self._inner.commit_cas(
            artifact_id,
            agent_id,
            expected_version=expected_version,
            content_hash=content_hash,
            size_tokens=size_tokens,
            content=content,
            tick=tick,
            trigger=trigger,
        )


class _DegradedFactory:
    """A :class:`RegistryFactory` minting :class:`_GrantBlindRegistry`. Single
    object per factory (the degraded stub is process-scoped, like in-memory), so
    ``db_path`` is ``None`` — the concurrency scenarios run against the one store
    object, which is all the single-writer teeth scenario needs."""

    def __init__(self) -> None:
        self._reg: _GrantBlindRegistry | None = None

    def __call__(self) -> _GrantBlindRegistry:
        if self._reg is None:
            self._reg = _GrantBlindRegistry()
        return self._reg

    def close_all(self) -> None:
        return None

    @property
    def db_path(self) -> Path | None:
        return None


def test_degraded_stub_fails_single_writer_scenario() -> None:
    """THE TEETH. The kit's single-writer-under-contention scenario MUST FAIL the
    grant-blind stub — proving the kit actually discriminates a backend that drops
    the grant-arbitration leg. The failure is a raised ``AssertionError`` whose
    message NAMES the missing tuple element (grant arbitration / other_holder)."""
    factory: RegistryFactory = _DegradedFactory()
    with pytest.raises(AssertionError) as excinfo:
        kit.assert_single_writer_under_contention(factory)
    message = str(excinfo.value)
    assert "single-writer VIOLATED" in message
    assert kit.OTHER_HOLDER_REASON in message, (
        "the teeth failure must name the missing tuple element (the grant-"
        "arbitration / other_holder leg) so a backend author knows what they "
        f"skipped; got: {message}"
    )


def test_degraded_stub_still_passes_grant_independent_scenarios() -> None:
    """The stub only drops the GRANT leg — so the scenarios that do not depend on
    it (pure version-CAS arbitration, fence admit-on-absent) still PASS. This
    proves the teeth test fails the stub for the RIGHT reason (the dropped grant
    leg), not because the stub is broken across the board (which would make the
    single-writer failure uninformative)."""
    factory: RegistryFactory = _DegradedFactory()
    # Pure version-CAS: two SHARED writers, one winner, loser version_mismatch —
    # no grant leg involved, so the degraded stub is still correct here.
    kit.assert_cas_arbitration_one_winner(_DegradedFactory())
    # Admit-on-absent: a plain OCC writer with no fence claim wins — again grant-
    # independent (a fresh factory so no cross-scenario state bleed).
    kit.assert_fence_admits_absent_read_generation(factory)


def test_degraded_stub_fence_reject_leg_still_holds() -> None:
    """The stub delegates the fence to the real inner registry, so the fence
    REJECT leg still works — the degradation is scoped to grant arbitration ONLY.
    Documents the exact blast radius of the injected bug (one leg, not the fence).
    """
    kit.assert_fence_rejects_superseded_read_generation(_DegradedFactory())
