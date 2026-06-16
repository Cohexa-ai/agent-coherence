# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""read-at-version service surface — typed outcomes, epoch stability, and
PROVABLE protocol non-interaction (plan item N v1, Unit 4).

Requirement trace: **R5** (service-level read-at-version; typed, distinguishable
rejections; responses carry ``coordinator_epoch``; ``version==current``
rejected), **R6** (fence non-capture — versioned reads never touch
``read_generation``), **R7** (off-protocol read — no MESI state, no invalidation
membership, no write rights at any version).

Both registry arms run via the parametrized ``rav_registry`` fixture (the
``tests/test_retention.py`` ``retention_registry`` pattern, here parameterized
per-test so the policy can vary). Reason matching is ALWAYS ``reason ==
CONSTANT`` against the imported wire-stable constants — never a substring of a
human message (the typed-signal-not-substring house rule).
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
    CURRENT_VERSION_REASON,
    EPOCH_MISMATCH_REASON,
    FUTURE_VERSION_REASON,
    NOT_RETAINED_REASON,
    READ_AT_VERSION_REASONS,
    RETENTION_OFF_REASON,
    UNKNOWN_ARTIFACT_REASON,
)
from ccs.core.states import MESIState, TransientState
from ccs.core.types import (
    Artifact,
    FetchRequest,
    VersionedContent,
    VersionedReadRejection,
)

# ---------------------------------------------------------------------------
# Registry factories — a fixture cannot take a per-test policy arg, so the
# parametrization yields a FACTORY ``make(policy=..., retain_versions=...)``
# that each test calls. Both arms are exercised by every test that uses it.
# ---------------------------------------------------------------------------


class _RegistryFactory:
    """Builds a registry of one arm; tracks instances to close at teardown."""

    def __init__(self, arm: str, tmp_path: Path) -> None:
        self._arm = arm
        self._tmp_path = tmp_path
        self._open: list[SqliteArtifactRegistry] = []
        self._n = 0

    def __call__(
        self,
        *,
        retain_versions: bool = True,
        retention_policy: RetentionPolicy | None = None,
    ):
        if self._arm == "in_memory":
            return ArtifactRegistry(
                retain_versions=retain_versions, retention_policy=retention_policy
            )
        self._n += 1
        reg = SqliteArtifactRegistry(
            self._tmp_path / f"state-{self._n}.db",
            retain_versions=retain_versions,
            retention_policy=retention_policy,
        )
        self._open.append(reg)
        return reg

    def close(self) -> None:
        for reg in self._open:
            reg.close()


@pytest.fixture(params=["in_memory", "sqlite"])
def make_registry(request, tmp_path: Path):
    """Yield a per-arm registry factory (closes any sqlite handles at teardown)."""
    factory = _RegistryFactory(request.param, tmp_path)
    try:
        yield factory
    finally:
        factory.close()


# ---------------------------------------------------------------------------
# Scenario builders
# ---------------------------------------------------------------------------


def _register(reg, *, content: str = "c1") -> Artifact:
    art = Artifact(id=uuid4(), name="plan.md", version=1, content_hash="h")
    reg.register_artifact(art, content=content)
    return art


def _commit_pessimistic(reg, art: Artifact, version: int, body: str | bytes) -> None:
    nxt = Artifact(id=art.id, name="plan.md", version=version, content_hash="h")
    reg.set_artifact_and_content(art.id, nxt, body)


def _chain(reg, bodies: tuple[str, ...]) -> Artifact:
    """register v1=bodies[0], then pessimistic-commit bodies[1:] as v2.., returns the artifact."""
    art = _register(reg, content=bodies[0])
    for i, body in enumerate(bodies[1:], start=2):
        _commit_pessimistic(reg, art, i, body)
    return art


def _read_generation_snapshot(reg, artifact_id: UUID, agents: list[UUID]) -> dict:
    """Snapshot the fence state both registries expose publicly (R6).

    Captures every agent's ``read_generation`` plus the artifact's
    ``owner_generation`` — the exact state a versioned read must NOT mutate.
    """
    snap = {
        "owner_generation": reg.get_owner_generation(artifact_id),
        "read_generation": {
            ag: reg.get_read_generation(artifact_id, ag) for ag in agents
        },
    }
    return snap


def _mesi_snapshot(reg, artifact_id: UUID) -> dict:
    """Snapshot the MESI state map + transient map + valid-holder membership (R7)."""
    return {
        "state_map": reg.get_state_map(artifact_id),
        "transient_map": reg.get_transient_map(artifact_id),
        "valid_holders": sorted(reg.valid_holders(artifact_id), key=str),
    }


# ===========================================================================
# Happy path — retained version served exactly (both registries, str + bytes)
# ===========================================================================


class TestHappyPath:
    def test_retained_str_version_served_exactly(self, make_registry):
        reg = make_registry(retention_policy=RetentionPolicy(max_versions=4))
        svc = CoordinatorService(reg)
        art = _chain(reg, ("c1", "c2", "c3"))  # current = 3
        out = svc.read_at_version(art.id, 2)
        assert isinstance(out, VersionedContent)
        assert out.artifact_id == art.id
        assert out.version == 2
        assert out.content == "c2"
        assert isinstance(out.captured_at, float)
        assert out.coordinator_epoch == reg.coordinator_epoch

    def test_retained_bytes_version_served_exactly(self, make_registry):
        # The in-process library path threads bytes through commit_cas; the
        # versioned read returns the EXACT bytes (affinity-NONE round-trip).
        reg = make_registry(retention_policy=RetentionPolicy(max_versions=4))
        svc = CoordinatorService(reg)
        art = _register(reg, content="seed")
        writer = uuid4()
        reg.set_agent_state(art.id, writer, MESIState.SHARED, tick=1)
        reg.commit_cas(
            art.id, writer, expected_version=1,
            content_hash="h2", content=b"\x00\x01bytes-v2", tick=2,
        )
        # commit_cas WIN advanced current to 2 and captured bytes under v2; do
        # one more commit so v2 is HISTORY (read_at_version serves history only).
        reg.set_agent_state(art.id, writer, MESIState.SHARED, tick=3)
        reg.commit_cas(
            art.id, writer, expected_version=2,
            content_hash="h3", content=b"v3", tick=4,
        )
        out = svc.read_at_version(art.id, 2)
        assert isinstance(out, VersionedContent)
        assert out.content == b"\x00\x01bytes-v2"
        assert isinstance(out.content, bytes)

    def test_captured_at_matches_a_monkeypatched_clock(self, make_registry, monkeypatch):
        # captured_at on the success surface is the wall-clock capture time.
        clock = {"now": 1000.0}
        monkeypatch.setattr(time, "time", lambda: clock["now"])
        reg = make_registry(retention_policy=RetentionPolicy(max_versions=4))
        svc = CoordinatorService(reg)
        art = _register(reg, content="c1")
        clock["now"] = 1234.5
        _commit_pessimistic(reg, art, 2, "c2")  # captured at 1234.5
        clock["now"] = 2000.0
        _commit_pessimistic(reg, art, 3, "c3")  # current = 3
        out = svc.read_at_version(art.id, 2)
        assert isinstance(out, VersionedContent)
        assert out.captured_at == 1234.5


# ===========================================================================
# Per-reason matrix — every reason on both registries, matched by == CONSTANT
# ===========================================================================


class TestReasonMatrix:
    def test_retention_never_enabled_is_retention_off(self, make_registry):
        # retain_versions=False ⇒ retention never on ⇒ retention_off (NOT
        # not_retained), even though the artifact exists and the version is
        # otherwise in range.
        reg = make_registry(retain_versions=False, retention_policy=None)
        svc = CoordinatorService(reg)
        art = _chain(reg, ("c1", "c2", "c3"))
        out = svc.read_at_version(art.id, 2)
        assert isinstance(out, VersionedReadRejection)
        assert out.reason == RETENTION_OFF_REASON
        assert out.reason in READ_AT_VERSION_REASONS
        assert out.current_version == 3
        assert out.coordinator_epoch == reg.coordinator_epoch

    def test_retention_on_unbounded_with_row_present_is_served(self, make_registry):
        # retain_versions=True + policy=None (NULL axes / unbounded) with the row
        # present ⇒ SERVED, not rejected. This is the v0.5 audit auto-wiring mode.
        reg = make_registry(retention_policy=None)
        svc = CoordinatorService(reg)
        art = _chain(reg, ("c1", "c2", "c3"))  # unbounded → v1,v2 both retained
        out = svc.read_at_version(art.id, 1)
        assert isinstance(out, VersionedContent)
        assert out.content == "c1"

    def test_unknown_uuid_is_unknown_artifact(self, make_registry):
        reg = make_registry(retention_policy=RetentionPolicy(max_versions=4))
        svc = CoordinatorService(reg)
        out = svc.read_at_version(uuid4(), 1)
        assert isinstance(out, VersionedReadRejection)
        assert out.reason == UNKNOWN_ARTIFACT_REASON
        # No current version exists for a missing artifact.
        assert out.current_version is None
        # Epoch is still known (the store has one even if the artifact doesn't).
        assert out.coordinator_epoch == reg.coordinator_epoch

    def test_stale_expected_epoch_is_epoch_mismatch(self, make_registry):
        reg = make_registry(retention_policy=RetentionPolicy(max_versions=4))
        svc = CoordinatorService(reg)
        art = _chain(reg, ("c1", "c2", "c3"))
        out = svc.read_at_version(art.id, 2, expected_epoch="not-the-epoch")
        assert isinstance(out, VersionedReadRejection)
        assert out.reason == EPOCH_MISMATCH_REASON
        # The response still carries the REAL store epoch (so the caller learns it).
        assert out.coordinator_epoch == reg.coordinator_epoch

    def test_matching_expected_epoch_is_served(self, make_registry):
        # The epoch guard passes when expected_epoch == the store epoch.
        reg = make_registry(retention_policy=RetentionPolicy(max_versions=4))
        svc = CoordinatorService(reg)
        art = _chain(reg, ("c1", "c2", "c3"))
        out = svc.read_at_version(art.id, 2, expected_epoch=reg.coordinator_epoch)
        assert isinstance(out, VersionedContent)
        assert out.content == "c2"

    def test_version_equals_current_is_current_version(self, make_registry):
        reg = make_registry(retention_policy=RetentionPolicy(max_versions=4))
        svc = CoordinatorService(reg)
        art = _chain(reg, ("c1", "c2", "c3"))  # current = 3
        out = svc.read_at_version(art.id, 3)
        assert isinstance(out, VersionedReadRejection)
        assert out.reason == CURRENT_VERSION_REASON
        assert out.current_version == 3

    def test_version_above_current_is_future_version(self, make_registry):
        reg = make_registry(retention_policy=RetentionPolicy(max_versions=4))
        svc = CoordinatorService(reg)
        art = _chain(reg, ("c1", "c2", "c3"))  # current = 3
        out = svc.read_at_version(art.id, 4)
        assert isinstance(out, VersionedReadRejection)
        assert out.reason == FUTURE_VERSION_REASON
        assert out.current_version == 3

    def test_version_zero_raises_value_error(self, make_registry):
        reg = make_registry(retention_policy=RetentionPolicy(max_versions=4))
        svc = CoordinatorService(reg)
        art = _chain(reg, ("c1", "c2"))
        with pytest.raises(ValueError, match="version must be >= 1"):
            svc.read_at_version(art.id, 0)

    def test_version_negative_raises_value_error(self, make_registry):
        reg = make_registry(retention_policy=RetentionPolicy(max_versions=4))
        svc = CoordinatorService(reg)
        art = _chain(reg, ("c1", "c2"))
        with pytest.raises(ValueError, match="version must be >= 1"):
            svc.read_at_version(art.id, -1)


class TestNotRetainedThreeConstructions:
    """An absent row while retention is ON must reject ``not_retained`` no matter
    HOW the gap arose — the deliberately-merged reason (plan Key Decisions)."""

    def test_committed_during_off_then_enabled(self, make_registry):
        # Construct: v1 committed while retention OFF (no row), then a retention-ON
        # registry over the SAME store reads v1. Only the sqlite arm can carry
        # state across registry instances on one store; the in-memory arm models
        # the same gap by capturing under a policy that never stored v1 (a
        # commit_cas(content=None) win — the canonical "row never captured" case).
        reg = make_registry(retention_policy=RetentionPolicy(max_versions=8))
        svc = CoordinatorService(reg)
        art = _register(reg, content="seed-v1")
        writer = uuid4()
        reg.set_agent_state(art.id, writer, MESIState.SHARED, tick=1)
        # content=None WIN advances to v2 but captures NO row for v2.
        reg.commit_cas(
            art.id, writer, expected_version=1, content_hash="h2", content=None, tick=2
        )
        # One more real commit so v2 is history (current = 3).
        reg.set_agent_state(art.id, writer, MESIState.SHARED, tick=3)
        reg.commit_cas(
            art.id, writer, expected_version=2, content_hash="h3", content="c3", tick=4
        )
        out = svc.read_at_version(art.id, 2)  # v2 never captured
        assert isinstance(out, VersionedReadRejection)
        assert out.reason == NOT_RETAINED_REASON

    def test_k_collected_row(self, make_registry):
        # K=2 over three commits drops v1; reading v1 ⇒ not_retained.
        reg = make_registry(retention_policy=RetentionPolicy(max_versions=2))
        svc = CoordinatorService(reg)
        art = _chain(reg, ("c1", "c2", "c3"))  # K=2 → v1 evicted, current=3
        out = svc.read_at_version(art.id, 1)
        assert isinstance(out, VersionedReadRejection)
        assert out.reason == NOT_RETAINED_REASON

    def test_t_expired_row_is_logically_not_retained(self, make_registry, monkeypatch):
        # T-axis logical-at-read: a row present on disk but older than max_age
        # reports not_retained WITHOUT being physically deleted by the read.
        clock = {"now": 0.0}
        monkeypatch.setattr(time, "time", lambda: clock["now"])
        # Large K so K never evicts; T=10s is the only axis that can age v2.
        reg = make_registry(
            retention_policy=RetentionPolicy(max_versions=100, max_age_seconds=10.0)
        )
        svc = CoordinatorService(reg)
        art = _register(reg, content="c1")
        clock["now"] = 5.0
        _commit_pessimistic(reg, art, 2, "c2")  # v2 captured at t=5
        clock["now"] = 6.0
        _commit_pessimistic(reg, art, 3, "c3")  # current=3 at t=6; v2 still fresh
        # v2 is present and fresh now.
        assert isinstance(svc.read_at_version(art.id, 2), VersionedContent)
        # Advance the clock past v2's horizon WITHOUT a new capture (no physical
        # cleanup runs) → the read must logically expire v2.
        clock["now"] = 100.0  # v2@5 now older than now-10=90
        out = svc.read_at_version(art.id, 2)
        assert isinstance(out, VersionedReadRejection)
        assert out.reason == NOT_RETAINED_REASON
        # Read is non-mutating: the row is still physically present (proven by
        # the raw getter still returning it — deletion only piggybacks on the
        # next capture, which has not happened).
        assert reg.get_content_at_version(art.id, 2) == "c2"


# ===========================================================================
# R6 — fence non-capture (LOAD-BEARING): versioned reads never touch the fence
# ===========================================================================


class TestFenceNonCaptureR6:
    def test_reads_do_not_mutate_read_generation(self, make_registry):
        reg = make_registry(retention_policy=RetentionPolicy(max_versions=8))
        svc = CoordinatorService(reg)
        art = _chain(reg, ("c1", "c2", "c3"))  # current = 3
        # An agent with an established fence claim (E acquire captures read_gen).
        agent = uuid4()
        reg.set_agent_state(art.id, agent, MESIState.EXCLUSIVE, trigger="write", tick=1)
        before = _read_generation_snapshot(reg, art.id, [agent])
        # A whole sequence of versioned reads spanning every reason branch.
        svc.read_at_version(art.id, 2)            # served history
        svc.read_at_version(art.id, 3)            # current_version
        svc.read_at_version(art.id, 4)            # future_version
        svc.read_at_version(art.id, 1)            # not_retained (K-evicted)
        svc.read_at_version(uuid4(), 1)           # unknown_artifact
        svc.read_at_version(art.id, 2, expected_epoch="x")  # epoch_mismatch
        after = _read_generation_snapshot(reg, art.id, [agent])
        assert after == before, "versioned reads mutated fence state (R6 violated)"

    def test_read_by_invalid_stale_agent_does_not_capture(self, make_registry):
        # The EXACT hazard R6 closes: a read performed in the context of an
        # INVALID/stale agent must NOT mint or refresh a fence claim for it. The
        # service read takes no agent arg, but we assert the agent's fence state
        # is byte-identical across reads regardless of its MESI state.
        reg = make_registry(retention_policy=RetentionPolicy(max_versions=8))
        svc = CoordinatorService(reg)
        art = _chain(reg, ("c1", "c2", "c3"))
        stale = uuid4()
        # Drive the agent to INVALID via a sweep reclamation so it is a genuine
        # unfenced zombie (owner_generation bumped past any claim it had).
        reg.set_agent_state(art.id, stale, MESIState.EXCLUSIVE, trigger="write", tick=1)
        reg.set_agent_state(art.id, stale, MESIState.INVALID, trigger="reclaim_heartbeat", tick=2)
        assert reg.get_agent_state(art.id, stale) == MESIState.INVALID
        before = _read_generation_snapshot(reg, art.id, [stale])
        for _ in range(3):
            svc.read_at_version(art.id, 2)
        after = _read_generation_snapshot(reg, art.id, [stale])
        assert after == before
        # And the agent gained NO read_generation it didn't already have.
        assert reg.get_read_generation(art.id, stale) == before["read_generation"][stale]


# ===========================================================================
# R7 — MESI non-interaction: state, transients, membership unchanged; a real
# fetch afterward behaves as if the reads never happened
# ===========================================================================


class TestMesiNonInteractionR7:
    def test_state_maps_transients_membership_unchanged(self, make_registry):
        reg = make_registry(retention_policy=RetentionPolicy(max_versions=8))
        svc = CoordinatorService(reg)
        art = _chain(reg, ("c1", "c2", "c3"))
        a, b = uuid4(), uuid4()
        reg.set_agent_state(art.id, a, MESIState.SHARED, trigger="fetch", tick=1)
        reg.set_agent_state(art.id, b, MESIState.SHARED, trigger="fetch", tick=1)
        reg.set_agent_transient(art.id, b, TransientState.ISG, entered_tick=1)
        before = _mesi_snapshot(reg, art.id)
        for v in (1, 2, 3, 4):
            svc.read_at_version(art.id, v)
        after = _mesi_snapshot(reg, art.id)
        assert after == before, "versioned reads mutated MESI/transient/membership (R7)"

    def test_subsequent_fetch_behaves_as_if_reads_never_happened(self, make_registry):
        # Two parallel runs: one interleaves versioned reads before a fetch, one
        # does not. The fetch grant + version + content must be identical.
        def run(with_reads: bool):
            reg = make_registry(retention_policy=RetentionPolicy(max_versions=8))
            svc = CoordinatorService(reg)
            art = _chain(reg, ("c1", "c2", "c3"))
            requester = uuid4()
            if with_reads:
                svc.read_at_version(art.id, 2)
                svc.read_at_version(art.id, 1)
                svc.read_at_version(art.id, 3)
            resp = svc.fetch(
                FetchRequest(artifact_id=art.id, requesting_agent_id=requester, requested_at_tick=10)
            )
            grant = resp.state_grant
            state = reg.get_agent_state(art.id, requester)
            owner_gen = reg.get_owner_generation(art.id)
            read_gen = reg.get_read_generation(art.id, requester)
            return resp.version, grant, state, owner_gen, read_gen

        assert run(with_reads=True) == run(with_reads=False)


# ===========================================================================
# Racing-commit atomicity — a commit interleaved with the read never mislabels
# (test_occ_commit_cas.py discipline: correctness HARD, realism as warnings)
# ===========================================================================


class TestRacingCommitAtomicity:
    def test_read_of_old_current_during_commit_is_history_or_current_never_wrong(
        self, make_registry
    ):
        # A "race" we can assert deterministically: read version N while N is the
        # current version, then commit to N+1, then read N again. The first read
        # must be current_version (N was current); the second must serve N as
        # exact history bytes — never wrong bytes, never a mislabeled reason.
        reg = make_registry(retention_policy=RetentionPolicy(max_versions=8))
        svc = CoordinatorService(reg)
        art = _chain(reg, ("c1", "c2"))  # current = 2, body "c2"
        # Before the commit: v2 is current.
        pre = svc.read_at_version(art.id, 2)
        assert isinstance(pre, VersionedReadRejection)
        assert pre.reason == CURRENT_VERSION_REASON
        # Commit to v3 — v2 becomes history.
        _commit_pessimistic(reg, art, 3, "c3")
        post = svc.read_at_version(art.id, 2)
        assert isinstance(post, VersionedContent), (
            "v2 must serve as history after the commit; got a rejection — a racing "
            "commit mislabeled the reason"
        )
        assert post.content == "c2"  # exact old-current bytes, never v3's body

    def test_outcome_is_always_one_of_two_allowed(self, make_registry):
        # Property-style assertion over the allowed outcome set for reading the
        # version that is current-at-entry: it is EITHER current_version (the
        # value that was current) OR — if a commit landed first — that version
        # served as history. Never wrong bytes, never another reason.
        reg = make_registry(retention_policy=RetentionPolicy(max_versions=8))
        svc = CoordinatorService(reg)
        art = _chain(reg, ("c1", "c2", "c3"))  # current = 3
        out = svc.read_at_version(art.id, 3)
        if isinstance(out, VersionedContent):
            assert out.content == "c3"  # would only happen if v3 became history
        else:
            assert out.reason == CURRENT_VERSION_REASON


# ===========================================================================
# Rejection-payload pin — NO content / hash / body material on a rejection
# ===========================================================================


class TestRejectionPayloadPin:
    # The exact, complete field set a VersionedReadRejection is allowed to carry.
    # No content, no content_hash, no body bytes — a rejection must never leak.
    _ALLOWED_FIELDS = frozenset(
        {"reason", "artifact_id", "requested_version", "current_version", "coordinator_epoch"}
    )
    _FORBIDDEN_SUBSTRINGS = ("content", "hash", "body", "bytes", "payload", "data")

    def test_dataclass_has_no_body_carrying_field(self):
        import dataclasses

        names = {f.name for f in dataclasses.fields(VersionedReadRejection)}
        assert names == self._ALLOWED_FIELDS, (
            f"VersionedReadRejection field set drifted to {names}; a rejection "
            f"must carry ONLY {self._ALLOWED_FIELDS} (no body material)."
        )
        for name in names:
            for bad in self._FORBIDDEN_SUBSTRINGS:
                assert bad not in name, f"field {name!r} hints at body material"

    def test_every_reason_rejection_instance_has_only_allowed_fields(self, make_registry):
        # Construct one rejection of each reason and assert the live instance's
        # __dict__ carries no extra (body-bearing) attribute.
        import dataclasses

        reg = make_registry(retention_policy=RetentionPolicy(max_versions=2))
        svc = CoordinatorService(reg)
        art = _chain(reg, ("c1", "c2", "c3"))  # K=2 → v1 evicted, current=3
        rejections = [
            svc.read_at_version(art.id, 1),                          # not_retained
            svc.read_at_version(art.id, 3),                          # current_version
            svc.read_at_version(art.id, 9),                          # future_version
            svc.read_at_version(uuid4(), 1),                         # unknown_artifact
            svc.read_at_version(art.id, 2, expected_epoch="x"),      # epoch_mismatch
        ]
        for rej in rejections:
            assert isinstance(rej, VersionedReadRejection)
            assert rej.reason in READ_AT_VERSION_REASONS
            assert {f.name for f in dataclasses.fields(rej)} == self._ALLOWED_FIELDS


# ===========================================================================
# Route-guard — the HTTP coordinator server exposes NO read-at-version /
# content-serving route (locks the v1 no-HTTP-exposure boundary). Modeled on
# tests/test_fence_signature_guard.py.
# ===========================================================================


class TestNoHttpReadAtVersionRoute:
    """v1 boundary: read-at-version is service-level + in-process CLI only. The
    hash-only HTTP topology has nothing to serve, so the route table must expose
    no content-serving / read-at-version route. The day one is added is a named
    follow-on gated on its own threat-model review (plan R5) — and it must fail
    this test until then. Same discipline as the fence signature guard."""

    # Path substrings that would indicate a content / version-history-serving
    # HTTP route crossed into the coordinator server.
    _FORBIDDEN_ROUTE_SUBSTRINGS = (
        "read-at-version",
        "read_at_version",
        "version",
        "content",
        "history",
        "retain",
        "snapshot",
    )

    def test_route_table_exposes_no_content_or_version_route(self):
        from ccs.adapters.claude_code.coordinator_server import _ROUTES

        offending = [
            (method, path)
            for (method, path) in _ROUTES
            for bad in self._FORBIDDEN_ROUTE_SUBSTRINGS
            if bad in path.lower()
        ]
        assert not offending, (
            f"coordinator server route table exposes a content/version route "
            f"{offending}; read-at-version must stay OFF the hash-only HTTP "
            f"topology in v1 (plan R5). Adding an HTTP content route is a named "
            f"follow-on behind its own threat-model review — remove the route or "
            f"open that work."
        )

    def test_handler_module_defines_no_read_at_version_handler(self):
        # Defense in depth: no ``_handle_*`` function whose name implies it serves
        # versioned content (a route could be wired later from such a handler).
        from ccs.adapters.claude_code import coordinator_server as srv

        suspects = [
            name
            for name in dir(srv)
            if name.startswith("_handle_")
            and any(b in name.lower() for b in ("read_at_version", "content", "version", "history"))
        ]
        assert not suspects, (
            f"coordinator server defines handler(s) {suspects} implying versioned "
            f"content service; the v1 HTTP topology serves no content (plan R5)."
        )
