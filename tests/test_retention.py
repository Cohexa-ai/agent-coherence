# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""Bounded version retention — policy unit tests + in-memory registry behavior.

Plan item N v1, Unit 2 (``docs/plans/2026-06-10-001-feat-version-retention-
read-at-version-plan.md``). Requirement trace: **R1** (bounded K/T retention,
amortized GC), **R3** (all three capture points retain), **R4** (GC invisible to
the protocol, current never collected, exemption-extensible).

This file ESTABLISHES the parametrized dual-registry fixture (the
``tests/test_fencing.py:27-35`` pattern). Unit 3 landed durable sqlite retention,
so the ``sqlite`` arm is now ACTIVE (the xfail marker was removed and the ctor
takes ``retention_policy``); the parity scenarios run against both registries.
Units 4-5 extend/consolidate this fixture; per-reason service scenarios arrive
with Unit 4's ``read_at_version``.
"""

from __future__ import annotations

import time
from pathlib import Path
from uuid import uuid4

import pytest

from ccs.coordinator.registry import ArtifactRegistry
from ccs.coordinator.retention import RetentionPolicy, collectible_versions
from ccs.coordinator.sqlite_registry import SqliteArtifactRegistry
from ccs.core.exceptions import StaleReadGeneration
from ccs.core.states import MESIState
from ccs.core.types import Artifact, CasCorruption, ConflictDetail

# ---------------------------------------------------------------------------
# Dual-registry fixture (both arms ACTIVE as of Unit 3)
# ---------------------------------------------------------------------------


@pytest.fixture(params=["in_memory", "sqlite"])
def retention_registry(request, tmp_path: Path):
    """Yield each registry with a bounded policy (K=2) so the SAME retention
    scenarios run against both. Unit 3 enabled the sqlite arm (durable
    ``artifact_versions`` table + ``retention_policy`` ctor param); the parity
    scenarios below run identically on both registries."""
    policy = RetentionPolicy(max_versions=2)
    if request.param == "in_memory":
        yield ArtifactRegistry(retain_versions=True, retention_policy=policy)
    else:
        with SqliteArtifactRegistry(
            tmp_path / "state.db", retain_versions=True, retention_policy=policy
        ) as reg:
            yield reg


def _register(reg, *, version: int = 1, content: str = "v1") -> Artifact:
    art = Artifact(id=uuid4(), name="plan.md", version=version, content_hash="h")
    reg.register_artifact(art, content=content)
    return art


# ===========================================================================
# Part A — collectible_versions: the single pure GC seam (R1, R4)
# ===========================================================================


class TestCollectibleVersions:
    """The pure GC decision function — exhaustively unit-tested in isolation so
    the eviction rule is pinned independent of either registry."""

    def test_empty_history_collects_nothing(self):
        # R4: nothing to collect, including the current version itself.
        assert collectible_versions([], current_version=1, policy=RetentionPolicy(max_versions=2), now=0.0) == set()

    def test_k_only_drops_oldest_beyond_k(self):
        # R1 K-axis: keep the K=2 most-recent (incl. current=3) → drop {1}.
        assert collectible_versions(
            [1, 2, 3], current_version=3, policy=RetentionPolicy(max_versions=2), now=0.0
        ) == {1}

    def test_k_one_keeps_only_current(self):
        # K=1 → only the current row survives; all older versions drop.
        assert collectible_versions(
            [1, 2, 3], current_version=3, policy=RetentionPolicy(max_versions=1), now=0.0
        ) == {1, 2}

    def test_current_never_collected_even_if_oldest(self):
        # R4: the current version is exempt regardless of K ranking. Here
        # current=1 is the OLDEST of {1,2,3}; K=2 would rank it out, but the
        # exempt floor keeps it — {2,3} kept by K, 1 kept as current → drop none.
        assert collectible_versions(
            [1, 2, 3], current_version=1, policy=RetentionPolicy(max_versions=2), now=0.0
        ) == set()

    def test_none_k_axis_disables_count_bound(self):
        # max_versions=None → K axis off; with T also None, nothing collects.
        assert collectible_versions(
            [1, 2, 3, 4, 5], current_version=5,
            policy=RetentionPolicy(max_versions=None, max_age_seconds=None), now=0.0,
        ) == set()

    def test_t_only_drops_versions_older_than_cutoff(self):
        # R1 T-axis: a version captured before now - max_age is collectible.
        # now=100, max_age=10 → cutoff=90; v1@50 and v2@80 expired, v3@95 fresh.
        ts = {1: 50.0, 2: 80.0, 3: 95.0}
        assert collectible_versions(
            ts, current_version=3, policy=RetentionPolicy(max_versions=None, max_age_seconds=10.0), now=100.0
        ) == {1, 2}

    def test_t_axis_current_never_expires(self):
        # R4: even an "old" current version is exempt from T expiry.
        ts = {1: 0.0, 2: 0.0}
        assert collectible_versions(
            ts, current_version=2, policy=RetentionPolicy(max_versions=None, max_age_seconds=1.0), now=1000.0
        ) == {1}  # v2 is current → exempt; v1 expired.

    def test_t_axis_noop_without_timestamps(self):
        # A bare-iterable call (no timestamps) cannot age anything — T is a
        # no-op; only the K axis can act on a plain list.
        assert collectible_versions(
            [1, 2, 3], current_version=3,
            policy=RetentionPolicy(max_versions=None, max_age_seconds=1.0), now=1e9,
        ) == set()

    def test_k_and_t_union(self):
        # Both axes active → drop set is the union. K=2 would drop {1}; T
        # (cutoff 90) also expires v2@80 → union {1, 2}.
        ts = {1: 50.0, 2: 80.0, 3: 95.0}
        assert collectible_versions(
            ts, current_version=3, policy=RetentionPolicy(max_versions=2, max_age_seconds=10.0), now=100.0
        ) == {1, 2}

    def test_exemptions_honored(self):
        # R4 SB-17 seam: a pinned version is never collected even when both
        # axes would drop it. K=1 + old timestamps would drop {1,2}; pinning 1
        # rescues it → only {2} drops.
        ts = {1: 0.0, 2: 0.0, 3: 100.0}
        assert collectible_versions(
            ts, current_version=3,
            policy=RetentionPolicy(max_versions=1, max_age_seconds=1.0), now=1000.0,
            exemptions={1},
        ) == {2}


class TestRetentionPolicyValidation:
    """__post_init__ rejects nonsensical bounds (house fail-fast style)."""

    def test_max_versions_zero_raises(self):
        with pytest.raises(ValueError, match="max_versions must be >= 1"):
            RetentionPolicy(max_versions=0)

    def test_max_versions_negative_raises(self):
        with pytest.raises(ValueError, match="max_versions must be >= 1"):
            RetentionPolicy(max_versions=-3)

    def test_max_age_zero_raises(self):
        with pytest.raises(ValueError, match="max_age_seconds must be > 0"):
            RetentionPolicy(max_age_seconds=0)

    def test_max_age_negative_raises(self):
        with pytest.raises(ValueError, match="max_age_seconds must be > 0"):
            RetentionPolicy(max_age_seconds=-1.0)

    def test_none_axes_are_valid(self):
        # Both axes disabled is a legal (unbounded) policy object.
        policy = RetentionPolicy(max_versions=None, max_age_seconds=None)
        assert policy.max_versions is None
        assert policy.max_age_seconds is None

    def test_defaults(self):
        # Documented defaults: K=16, T off.
        policy = RetentionPolicy()
        assert policy.max_versions == 16
        assert policy.max_age_seconds is None


# ===========================================================================
# Part B — in-memory ArtifactRegistry bounded retention (R1, R3, R4)
# ===========================================================================


class TestBoundedRetentionInMemory:
    """K-eviction across the capture points on the in-memory registry."""

    def test_k2_over_three_commits_keeps_two_drops_oldest(self):
        # R1: policy K=2 over 3 commits keeps {2,3}, drops 1 (current=3).
        reg = ArtifactRegistry(retain_versions=True, retention_policy=RetentionPolicy(max_versions=2))
        art = _register(reg, version=1, content="c1")
        for v, body in ((2, "c2"), (3, "c3")):
            nxt = Artifact(id=art.id, name="plan.md", version=v, content_hash="h")
            reg.set_artifact_and_content(art.id, nxt, body)
        assert reg.get_content_at_version(art.id, 1) is None  # collected
        assert reg.get_content_at_version(art.id, 2) == "c2"
        assert reg.get_content_at_version(art.id, 3) == "c3"

    def test_k1_keeps_only_current(self):
        # R4 floor: K=1 → only the current row survives each commit.
        reg = ArtifactRegistry(retain_versions=True, retention_policy=RetentionPolicy(max_versions=1))
        art = _register(reg, version=1, content="c1")
        nxt = Artifact(id=art.id, name="plan.md", version=2, content_hash="h")
        reg.set_artifact_and_content(art.id, nxt, "c2")
        assert reg.get_content_at_version(art.id, 1) is None
        assert reg.get_content_at_version(art.id, 2) == "c2"

    def test_unbounded_policy_none_keeps_all(self):
        # Back-compat: retain_versions=True + policy=None == today's unbounded
        # semantics. GC never runs; every version survives.
        reg = ArtifactRegistry(retain_versions=True, retention_policy=None)
        art = _register(reg, version=1, content="c1")
        for v, body in ((2, "c2"), (3, "c3"), (4, "c4")):
            nxt = Artifact(id=art.id, name="plan.md", version=v, content_hash="h")
            reg.set_artifact_and_content(art.id, nxt, body)
        assert reg.get_content_at_version(art.id, 1) == "c1"
        assert reg.get_content_at_version(art.id, 4) == "c4"

    def test_retain_versions_false_ignores_policy(self):
        # Retention only activates on retain_versions=True; a policy alone does
        # nothing when retain_versions is False (the active-iff contract).
        reg = ArtifactRegistry(retain_versions=False, retention_policy=RetentionPolicy(max_versions=2))
        art = _register(reg, version=1, content="c1")
        assert reg.get_content_at_version(art.id, 1) is None

    def test_t_expiry_via_monkeypatched_clock(self, monkeypatch):
        # R1 T-axis end-to-end: capture v1 at t=0, advance the clock past
        # max_age, capture v2 → v1 is physically dropped at the next capture.
        clock = {"now": 0.0}
        monkeypatch.setattr(time, "time", lambda: clock["now"])
        reg = ArtifactRegistry(
            retain_versions=True,
            retention_policy=RetentionPolicy(max_versions=None, max_age_seconds=10.0),
        )
        art = _register(reg, version=1, content="c1")
        assert reg.get_content_at_version(art.id, 1) == "c1"  # fresh at capture
        clock["now"] = 100.0  # well past the 10s horizon
        nxt = Artifact(id=art.id, name="plan.md", version=2, content_hash="h")
        reg.set_artifact_and_content(art.id, nxt, "c2")
        assert reg.get_content_at_version(art.id, 1) is None  # T-expired + swept
        assert reg.get_content_at_version(art.id, 2) == "c2"  # current survives


class TestCapturePointsRetain:
    """R3: all three capture points snapshot under retention (str and bytes)."""

    def test_register_artifact_captures(self):
        reg = ArtifactRegistry(retain_versions=True, retention_policy=RetentionPolicy(max_versions=4))
        art = _register(reg, version=1, content="seed")
        assert reg.get_content_at_version(art.id, 1) == "seed"

    def test_pessimistic_set_artifact_and_content_captures(self):
        reg = ArtifactRegistry(retain_versions=True, retention_policy=RetentionPolicy(max_versions=4))
        art = _register(reg, version=1, content="seed")
        nxt = Artifact(id=art.id, name="plan.md", version=2, content_hash="h")
        reg.set_artifact_and_content(art.id, nxt, "pessimistic-v2")
        assert reg.get_content_at_version(art.id, 2) == "pessimistic-v2"

    def test_commit_cas_win_captures_str_body(self):
        reg = ArtifactRegistry(retain_versions=True, retention_policy=RetentionPolicy(max_versions=4))
        art = _register(reg, version=1, content="seed")
        writer = uuid4()
        reg.set_agent_state(art.id, writer, MESIState.SHARED, tick=1)
        reg.commit_cas(art.id, writer, expected_version=1, content_hash="h-new", content="cas-v2", tick=2)
        assert reg.get_content_at_version(art.id, 2) == "cas-v2"

    def test_commit_cas_win_captures_bytes_body(self):
        # The in-process library path threads bytes; the corrected annotation
        # admits them and a versioned read returns the exact bytes.
        reg = ArtifactRegistry(retain_versions=True, retention_policy=RetentionPolicy(max_versions=4))
        art = _register(reg, version=1, content="seed")
        writer = uuid4()
        reg.set_agent_state(art.id, writer, MESIState.SHARED, tick=1)
        reg.commit_cas(art.id, writer, expected_version=1, content_hash="h-new", content=b"\x00\x01bytes-v2", tick=2)
        assert reg.get_content_at_version(art.id, 2) == b"\x00\x01bytes-v2"


class TestCommitCasContentNoneSkipsCapture:
    """The history-poisoning fix: content=None WIN retains NO row (R-fix)."""

    def test_content_none_win_skips_capture(self):
        # Pre-fix bug: the None path retained the OLD body under the NEW
        # version. Now next_version is simply not captured → read misses.
        reg = ArtifactRegistry(retain_versions=True, retention_policy=RetentionPolicy(max_versions=8))
        art = _register(reg, version=1, content="seed-v1")
        writer = uuid4()
        reg.set_agent_state(art.id, writer, MESIState.SHARED, tick=1)
        updated, _ = reg.commit_cas(art.id, writer, expected_version=1, content_hash="h-new", content=None, tick=2)
        # Version advanced per existing semantics ...
        assert updated.version == 2
        assert reg.get_artifact(art.id).version == 2
        # ... but the NEW version has NO retained snapshot (no stale OLD body).
        assert reg.get_content_at_version(art.id, 2) is None
        # The old version's snapshot is untouched (still the original body).
        assert reg.get_content_at_version(art.id, 1) == "seed-v1"

    def test_content_none_win_unbounded_also_skips(self):
        # Same fix under the unbounded (policy=None) mode — the v0.5 path.
        reg = ArtifactRegistry(retain_versions=True, retention_policy=None)
        art = _register(reg, version=1, content="seed-v1")
        writer = uuid4()
        reg.set_agent_state(art.id, writer, MESIState.SHARED, tick=1)
        reg.commit_cas(art.id, writer, expected_version=1, content_hash="h-new", content=None, tick=2)
        assert reg.get_content_at_version(art.id, 2) is None
        assert reg.get_content_at_version(art.id, 1) == "seed-v1"


class TestNoCaptureOnRejectedWrites:
    """R4/R3: a write that does NOT mutate leaves history unchanged."""

    def test_commit_cas_version_mismatch_no_capture(self):
        # ConflictDetail("version_mismatch") — expected < current → no mutation.
        reg = ArtifactRegistry(retain_versions=True, retention_policy=RetentionPolicy(max_versions=8))
        art = _register(reg, version=5, content="seed")
        writer = uuid4()
        reg.set_agent_state(art.id, writer, MESIState.SHARED, tick=1)
        before = dict(reg._records[art.id].version_history)
        res = reg.commit_cas(art.id, writer, expected_version=3, content_hash="h", content="should-not-store", tick=2)
        assert isinstance(res, ConflictDetail)
        assert reg._records[art.id].version_history == before

    def test_commit_cas_other_holder_no_capture(self):
        # ConflictDetail("other_holder") — a peer holds EXCLUSIVE → no mutation.
        reg = ArtifactRegistry(retain_versions=True, retention_policy=RetentionPolicy(max_versions=8))
        art = _register(reg, version=5, content="seed")
        writer, peer = uuid4(), uuid4()
        reg.set_agent_state(art.id, writer, MESIState.SHARED, tick=1)
        reg.set_agent_state(art.id, peer, MESIState.EXCLUSIVE, tick=2)
        before = dict(reg._records[art.id].version_history)
        res = reg.commit_cas(art.id, writer, expected_version=5, content_hash="h", content="should-not-store", tick=3)
        assert isinstance(res, ConflictDetail)
        assert reg._records[art.id].version_history == before

    def test_commit_cas_corruption_no_capture(self):
        # CasCorruption — expected > current → no mutation.
        reg = ArtifactRegistry(retain_versions=True, retention_policy=RetentionPolicy(max_versions=8))
        art = _register(reg, version=5, content="seed")
        writer = uuid4()
        reg.set_agent_state(art.id, writer, MESIState.SHARED, tick=1)
        before = dict(reg._records[art.id].version_history)
        res = reg.commit_cas(art.id, writer, expected_version=99, content_hash="h", content="should-not-store", tick=2)
        assert isinstance(res, CasCorruption)
        assert reg._records[art.id].version_history == before

    def test_set_artifact_and_content_fence_reject_no_capture(self):
        # StaleReadGeneration — the pessimistic-commit fence rejects a committer
        # whose captured read_generation was superseded by a sweep reclamation.
        # The raise must leave version_history untouched (no phantom snapshot).
        reg = ArtifactRegistry(retain_versions=True, retention_policy=RetentionPolicy(max_versions=8))
        art = _register(reg, version=1, content="seed")
        committer = uuid4()
        # Establish a fence claim (captures read_generation=0 at owner_gen=0).
        reg.set_agent_state(art.id, committer, MESIState.EXCLUSIVE, trigger="write", tick=1)
        # A sweep reclamation bumps owner_generation past the captured read_gen.
        reg.set_agent_state(art.id, committer, MESIState.INVALID, trigger="reclaim_heartbeat", tick=2)
        assert reg.get_owner_generation(art.id) == 1
        before = dict(reg._records[art.id].version_history)
        nxt = Artifact(id=art.id, name="plan.md", version=2, content_hash="h")
        with pytest.raises(StaleReadGeneration):
            reg.set_artifact_and_content(art.id, nxt, "should-not-store", fence_agent_id=committer)
        assert reg._records[art.id].version_history == before


class TestRemoveArtifactDropsHistory:
    """R4: deleting an artifact drops its retained history with the record."""

    def test_remove_drops_history(self):
        reg = ArtifactRegistry(retain_versions=True, retention_policy=RetentionPolicy(max_versions=8))
        art = _register(reg, version=1, content="seed")
        assert reg.get_content_at_version(art.id, 1) == "seed"
        reg.remove_artifact(art.id)
        # Deleted ≡ never-existed: the record (and its history) is gone.
        assert reg.get_content_at_version(art.id, 1) is None


# ===========================================================================
# Part C — dual-registry parity scaffold (in_memory now; sqlite via Unit 3)
# ===========================================================================


class TestDualRegistryRetentionParity:
    """The parametrized harness Units 3-5 extend. Runs the K=2 eviction
    scenario against the `retention_registry` fixture so the sqlite arm lights
    up the moment Unit 3 drops its xfail marker."""

    def test_k2_eviction_parity(self, retention_registry):
        reg = retention_registry  # K=2 policy from the fixture
        art = _register(reg, version=1, content="c1")
        for v, body in ((2, "c2"), (3, "c3")):
            nxt = Artifact(id=art.id, name="plan.md", version=v, content_hash="h")
            reg.set_artifact_and_content(art.id, nxt, body)
        assert reg.get_content_at_version(art.id, 1) is None  # evicted at K=2
        assert reg.get_content_at_version(art.id, 2) == "c2"
        assert reg.get_content_at_version(art.id, 3) == "c3"
