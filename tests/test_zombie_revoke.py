# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents
"""NoZombieRevoke — the read-generation fence as an OPERATION-CLASS property.

Regression suite for two defects in the grant-revoking path, both reproduced
single-host, in one process, under the registry ``RLock``. The shape is the one
Temporal closed in their semaphore pattern with signal pinning: "a late release
Signal from a holder whose lease already expired could revoke a subsequent
acquirer's permit".

The fence (``owner_generation`` vs a committer's captured ``read_generation``)
was checked on the three COMMIT paths only — ``commit_cas``, ``commit_all`` and
``set_artifact_and_content(fence_agent_id=…)`` — and neither checked nor
maintained by the other grant-revoking operation, ``invalidate``:

F1 — ZOMBIE REVOKE. ``CoordinatorService.invalidate`` revoked whatever grant it
     named with no validation at all, so a signal minted at one epoch destroyed
     a grant established at a later one. Fixed by pinning a PEER-issued
     invalidation to the target's ``last_observed_version``
     (``service._revoke_is_superseded``).

F2 — FENCE SUPPRESSION. ``invalidate`` moved an M/E holder to INVALID with
     ``trigger="invalidate"``, which was not in ``RECLAIM_TRIGGERS``, so
     ``owner_generation`` did not bump — and the sweep could never arm the fence
     afterwards, there being no M/E grant left to reclaim. Identical end-state,
     opposite verdict. Fixed by ``EPOCH_BUMP_TRIGGERS``.

The last two tests pin the guards that keep F1's fix from over-reaching:
dropping an invalidation is far more dangerous than applying one.
"""

from __future__ import annotations

import threading
from pathlib import Path
from uuid import uuid4

import pytest

from ccs.adapters.base import CoherenceAdapterCore
from ccs.bus.event_bus import InMemoryEventBus
from ccs.coordinator.registry import ArtifactRegistry
from ccs.coordinator.service import CoordinatorService
from ccs.coordinator.sqlite_registry import SqliteArtifactRegistry
from ccs.core.states import MESIState
from ccs.core.types import Artifact, ConflictDetail

_M_OR_E = (MESIState.MODIFIED, MESIState.EXCLUSIVE)


def _clear_observation(reg, artifact_id, agent_id) -> None:
    """Put a live grant into the never-observed state, on either backend.

    Reproduces what the v5->v6 schema migration leaves behind: an
    ``agent_states`` row that predates ``last_observed_version`` and so carries
    no recorded observation (absent key in memory, NULL column in sqlite) while
    the grant itself is live. ``set_agent_state`` records an observation on
    every non-INVALID transition, so this state cannot be reached forward from
    a fresh store -- only inherited from one written before SB-10.
    """
    records = getattr(reg, "_records", None)
    if records is not None:
        records[artifact_id].last_observed_version_by_agent.pop(agent_id, None)
    else:
        reg._conn.execute(
            "UPDATE agent_states SET last_observed_version = NULL "
            "WHERE artifact_id = ? AND agent_id = ?",
            (artifact_id.hex, agent_id.hex),
        )


@pytest.fixture(params=["in_memory", "sqlite"])
def registry(request, tmp_path: Path):
    """Both registries, identically — the bump and the capture are duplicated
    across them (see test_fencing.py), so a fix must land in both."""
    if request.param == "in_memory":
        yield ArtifactRegistry()
    else:
        with SqliteArtifactRegistry(tmp_path / "state.db") as reg:
            yield reg


# ---------------------------------------------------------------------------
# F1 — a revoke minted at a superseded epoch must not land
# ---------------------------------------------------------------------------


def test_invalidate_rejects_issuer_from_a_superseded_generation(registry) -> None:
    """A revoke whose issuer captured epoch g must not revoke a grant the
    registry established at epoch g+1.

    The interleaving (single host, one process, one coordinator):

      t0  A acquires EXCLUSIVE on X          -> owner_generation 0, A.rg 0
      t1  A commits                          -> version 2; a signal is minted
      t2  the sweep reclaims A's grant       -> owner_generation 1  (epoch moved)
      t3  B acquires a FRESH EXCLUSIVE       -> B.rg 1              (new epoch)
      t4  the t1 signal is finally delivered -> it must NOT touch B's grant

    Before the fix nothing between t1 and t4 was compared, and t4 destroyed a
    grant issued two steps after the signal was minted.

    Runs on BOTH registries: the pin compares a value each backend writes
    independently, so a one-sided regression has to be visible here.
    """
    svc = CoordinatorService(registry)
    art = svc.register_artifact(name="plan.md", content="v1")
    a, b = uuid4(), uuid4()

    svc.write(agent_id=a, artifact_id=art.id, issued_at_tick=1)
    svc.registry.set_agent_state(art.id, b, MESIState.SHARED, trigger="fetch", tick=1)
    updated, signals = svc.commit(agent_id=a, artifact_id=art.id, content="v2", issued_at_tick=2)
    # The genuine signal the coordinator minted for b at t1 — not a hand-written
    # one; it is replayed verbatim at t4 below.
    (signal,) = [s for s in signals if s.artifact_id == art.id]
    assert (signal.new_version, signal.issued_at_tick) == (updated.version, 2)

    svc.registry.set_agent_state(
        art.id, a, MESIState.INVALID, trigger="reclaim_heartbeat", tick=50
    )
    assert svc.registry.get_owner_generation(art.id) == 1

    svc.write(agent_id=b, artifact_id=art.id, issued_at_tick=60)
    assert svc.registry.get_read_generation(art.id, b) == 1
    assert svc.registry.get_agent_state(art.id, b) == MESIState.EXCLUSIVE

    svc.invalidate(
        agent_id=b,
        artifact_id=art.id,
        new_version=signal.new_version,
        issuer_agent_id=signal.issuer_agent_id,
        issued_at_tick=signal.issued_at_tick,
    )

    assert svc.registry.get_agent_state(art.id, b) in _M_OR_E, (
        "NoZombieRevoke: a revoke minted at epoch 0 destroyed a grant "
        f"established at epoch 1 (artifact is at version {updated.version})"
    )


def test_invalidate_rejects_signal_older_than_the_current_version(registry) -> None:
    """The weakest form of the same property, using only the operand
    ``invalidate`` already takes: a signal announcing ``new_version=N`` cannot
    be authority to revoke a claim the registry established at a version
    strictly greater than N."""
    svc = CoordinatorService(registry)
    art = svc.register_artifact(name="plan.md", content="v1")
    a, b = uuid4(), uuid4()

    svc.write(agent_id=a, artifact_id=art.id, issued_at_tick=1)
    svc.commit(agent_id=a, artifact_id=art.id, content="v2", issued_at_tick=2)
    svc.write(agent_id=b, artifact_id=art.id, issued_at_tick=3)
    svc.commit(agent_id=b, artifact_id=art.id, content="v3", issued_at_tick=4)

    current = svc.registry.get_artifact(art.id).version
    assert current == 3
    assert svc.registry.get_agent_state(art.id, b) == MESIState.MODIFIED

    svc.invalidate(
        agent_id=b,
        artifact_id=art.id,
        new_version=2,  # stale: minted before b's own commit
        issuer_agent_id=a,
        issued_at_tick=2,
    )

    assert svc.registry.get_agent_state(art.id, b) in _M_OR_E, (
        "a v2 invalidation revoked a claim held at v3"
    )


def test_adapter_concurrent_publish_does_not_revoke_a_fresher_grant() -> None:
    """Reachability on a shipped surface, single host, one process.

    ``CoherenceAdapterCore.write`` mints its invalidation signals INSIDE the
    registry lock (``service.write`` / ``service.commit`` under
    ``abort_guard``) and publishes them AFTER that lock is released. The class
    documents concurrent callers ("LangGraph parallel branches" —
    ``adapters/base.py`` ``_sweep_lock``), so a peer can complete an entire
    read → acquire → commit cycle inside that window. Delivery then calls
    ``AgentRuntime.handle_invalidation`` -> ``coordinator.invalidate(agent_id=
    recipient, …)``, which threads no abort Event — so ``abort_guard(None)`` is
    a plain lock acquire and closes nothing here. The pin is what closes it.

    The gate below stops thread A exactly at the publish boundary; it forces no
    state the process cannot reach on its own.
    """

    class _GatedBus(InMemoryEventBus):
        def __init__(self) -> None:
            super().__init__()
            self.reached = threading.Event()
            self.release = threading.Event()
            self._armed = True

        def publish_invalidation(self, signal, *, recipients):  # type: ignore[override]
            if self._armed:
                self._armed = False
                self.reached.set()
                self.release.wait(timeout=5)
            return super().publish_invalidation(signal, recipients=recipients)

    bus = _GatedBus()
    core = CoherenceAdapterCore(event_bus=bus)
    core.register_agent("A", now_tick=0)
    agent_b = core.register_agent("B", now_tick=0)
    art = core.register_artifact(name="plan.md", content="v1")

    core.read(agent_name="A", artifact_id=art.id, now_tick=1)
    core.read(agent_name="B", artifact_id=art.id, now_tick=1)

    failures: list[BaseException] = []

    def _a_writes() -> None:
        try:
            core.write(agent_name="A", artifact_id=art.id, content="a-v2", now_tick=2)
        except BaseException as exc:  # noqa: BLE001 — reported, not swallowed
            failures.append(exc)

    thread = threading.Thread(target=_a_writes, daemon=True)
    thread.start()
    try:
        assert bus.reached.wait(timeout=5), "A never reached the publish boundary"
        assert core.registry.get_artifact(art.id).version == 2

        # B's own full cycle, entirely inside A's publish window.
        core.write(agent_name="B", artifact_id=art.id, content="b-v3", now_tick=3)
        assert core.registry.get_artifact(art.id).version == 3
        assert core.registry.get_agent_state(art.id, agent_b) in _M_OR_E
    finally:
        bus.release.set()
        thread.join(timeout=5)

    assert not failures, f"A's write raised: {failures[0]!r}"
    assert core.registry.get_agent_state(art.id, agent_b) in _M_OR_E, (
        "the held v2 signal revoked B's claim, established at v3"
    )
    # Both halves, or the guarantee is half true. The coordinator keeping the
    # grant is worth nothing if the agent's own view was clamped on the way in:
    # handle_invalidation asks the coordinator BEFORE it touches the cache, so a
    # declined signal leaves B's cache alone too.
    entry = core._agents_by_name["B"].runtime.cache.get(art.id)
    assert entry is not None and entry.state in _M_OR_E, (
        "the coordinator kept B's grant but B's own cached view was invalidated "
        f"by the same stale signal (cache state {entry and entry.state})"
    )


# ---------------------------------------------------------------------------
# F2 — a revoke must leave the fence in the state a reclaim would have
# ---------------------------------------------------------------------------


def test_release_by_invalidate_arms_the_fence_like_a_sweep_reclaim(registry) -> None:
    """Same end-state, one differing step, opposite verdict.

    Both arms end identically: A's M/E grant is gone, a peer took and released
    the artifact, the version never moved, no M/E holder remains. They differ
    only in HOW A's grant went away.

      sweep reclaim  -> owner_generation bumps -> A's commit_cas is REJECTED
                        ``stale_read_generation`` (test_fencing.py pins this)
      invalidate     -> no bump -> the sweep has no M/E grant left to reclaim,
                        so the fence can never arm -> A's commit_cas is ADMITTED
                        and bumps the version

    The rejected commit is the one the fence exists for: the version never
    moved, so version-CAS structurally cannot see it (``backend_contract.py``
    R9_ATOMIC_BOUNDARY). Whichever verdict is right, the two paths cannot
    disagree — a revoked grant is a revoked grant.
    """
    reg = registry
    art = Artifact(id=uuid4(), name="plan.md", version=1, content_hash="h")
    reg.register_artifact(art, content="ignored")
    svc = CoordinatorService(reg)
    a, b = uuid4(), uuid4()

    # A takes a write claim at epoch 0 and holds a buffer of version 1.
    reg.set_agent_state(art.id, a, MESIState.EXCLUSIVE, trigger="write", tick=1)
    assert reg.get_read_generation(art.id, a) == 0

    # A's grant is revoked — by the OTHER grant-revoking operation.
    svc.invalidate(
        agent_id=a, artifact_id=art.id, new_version=1, issuer_agent_id=a, issued_at_tick=2
    )
    assert reg.get_agent_state(art.id, a) == MESIState.INVALID

    # The sweep runs and finds nothing to reclaim: A is already INVALID.
    reg.record_heartbeat(a, 1)
    assert (
        svc.enforce_stable_grant_timeouts(
            current_tick=100, heartbeat_timeout_ticks=10, max_hold_ticks=1000
        )
        == 0
    )

    # A peer takes the artifact and releases it without moving the version —
    # the shape a post-edit failure / session-stop release produces, and the
    # window in which the peer may have mutated shared bytes.
    reg.set_agent_state(art.id, b, MESIState.EXCLUSIVE, trigger="write", tick=101)
    svc.invalidate(
        agent_id=b, artifact_id=art.id, new_version=1, issuer_agent_id=b, issued_at_tick=102
    )
    assert reg.get_artifact(art.id).version == 1

    result = reg.commit_cas(art.id, a, expected_version=1, content_hash="zombie")

    assert isinstance(result, ConflictDetail), (
        "the zombie's commit was ADMITTED; the identical sweep-reclaim arm "
        "rejects it (test_parity_commit_cas_fence_rejects_superseded_reader)"
    )
    assert result.reason == "stale_read_generation"
    assert reg.get_artifact(art.id).version == 1, "phantom version bump"


def test_invalidate_of_an_me_holder_bumps_owner_generation(registry) -> None:
    """The mechanism behind F2, isolated: taking an M/E holder to INVALID is an
    epoch change however it is spelled. ``RECLAIM_TRIGGERS`` gates the bump on
    the sweep's three triggers, so the ``"invalidate"`` trigger — the only other
    way an M/E grant is revoked — silently leaves the epoch behind."""
    reg = registry
    art = Artifact(id=uuid4(), name="plan.md", version=1, content_hash="h")
    reg.register_artifact(art, content="ignored")
    a = uuid4()

    reg.set_agent_state(art.id, a, MESIState.EXCLUSIVE, trigger="write", tick=1)
    assert reg.get_owner_generation(art.id) == 0

    reg.set_agent_state(art.id, a, MESIState.INVALID, trigger="invalidate", tick=2)

    assert reg.get_owner_generation(art.id) == 1, (
        "an M/E grant was revoked without moving the ownership epoch"
    )


# ---------------------------------------------------------------------------
# Guards on the F1 pin — dropping an invalidation is worse than applying one
# ---------------------------------------------------------------------------


def test_a_genuinely_behind_peer_is_still_invalidated(registry) -> None:
    """The pin must never swallow a real invalidation. A holder whose recorded
    observation is BEHIND the announced version is exactly the stale-read ->
    write case this layer exists to close: it is invalidated, pin or no pin.

    The version is moved through ``set_artifact_and_content`` -- the one write
    path that advances a version WITHOUT touching holder states, so b is left
    genuinely non-INVALID and behind. Every service-level commit path
    invalidates its peers, which is precisely why the pin is sound; this test
    reaches the branch that soundness argument leaves open.
    """
    reg = registry
    svc = CoordinatorService(reg)
    art = svc.register_artifact(name="plan.md", content="v1")
    a, b = uuid4(), uuid4()

    reg.set_agent_state(art.id, b, MESIState.SHARED, trigger="fetch", tick=1)
    assert reg.last_observed_version_for(art.id, b) == 1

    bumped = Artifact(id=art.id, name="plan.md", version=2, content_hash="h2")
    reg.set_artifact_and_content(art.id, bumped, "v2", last_writer=a)
    assert reg.get_artifact(art.id).version == 2
    assert reg.get_agent_state(art.id, b) == MESIState.SHARED
    assert reg.last_observed_version_for(art.id, b) == 1  # behind, and still holding

    svc.invalidate(
        agent_id=b,
        artifact_id=art.id,
        new_version=2,
        issuer_agent_id=a,
        issued_at_tick=4,
    )

    assert reg.get_agent_state(art.id, b) == MESIState.INVALID, (
        "a peer holding v1 was not invalidated by a v2 announcement"
    )


def test_absent_observation_is_admitted_not_dropped(registry) -> None:
    """Admit-on-absent: a live grant with NO recorded observation is invalidated,
    never pinned.

    Absence is not evidence of freshness. This is the conservative leg of the
    predicate -- the one that keeps an unknown observation on the APPLY side --
    and it is reachable in the field: the v5->v6 migration leaves pre-SB-10
    grants with no recorded observation while the grant stays live. Without this
    test, flipping ``observed is not None`` to ``observed is None`` passes the
    whole suite while silently dropping real invalidations.
    """
    reg = registry
    svc = CoordinatorService(reg)
    art = svc.register_artifact(name="plan.md", content="v1")
    a, b = uuid4(), uuid4()

    reg.set_agent_state(art.id, b, MESIState.SHARED, trigger="fetch", tick=1)
    _clear_observation(reg, art.id, b)
    assert reg.last_observed_version_for(art.id, b) is None
    assert reg.get_agent_state(art.id, b) == MESIState.SHARED

    svc.invalidate(
        agent_id=b,
        artifact_id=art.id,
        new_version=1,
        issuer_agent_id=a,
        issued_at_tick=2,
    )

    assert reg.get_agent_state(art.id, b) == MESIState.INVALID, (
        "an absent observation was treated as proof of freshness and the "
        "invalidation was dropped"
    )


def test_eager_update_then_declined_invalidation_leaves_no_transient() -> None:
    """A declined invalidation must not strand the transient it would have cleared.

    ``_write_impl`` sets a peer's SIA/EIA transient together with the INVALID
    transition and leaves the bus-delivered invalidation to clear it. An eager
    content push can move that peer back to SHARED first -- and then the pin
    legitimately declines the late signal, so the clear never happens unless
    ``handle_update`` does it, the way ``fetch`` already does. A stranded
    transient makes the stable-grant sweep skip the pair and fails the peer's
    next ``commit_cas`` on its transient precondition, until the timeout
    fail-safe force-invalidates an agent that is holding correct data.

    Delivery is driven by hand rather than through the bus, so the invalidation
    lands after the update -- the ordering the failure needs.
    """
    core = CoherenceAdapterCore(event_bus=InMemoryEventBus())
    agent_a = core.register_agent("A", now_tick=0)
    agent_b = core.register_agent("B", now_tick=0)
    art = core.register_artifact(name="plan.md", content="v1")
    runtime_b = core._agents_by_name["B"].runtime

    core.read(agent_name="B", artifact_id=art.id, now_tick=1)
    assert core.registry.get_agent_state(art.id, agent_b) is not MESIState.INVALID

    # A takes exclusivity: B goes INVALID and its transient is set, but nothing
    # has delivered the signal yet.
    signals = core.coordinator.write(agent_id=agent_a, artifact_id=art.id, issued_at_tick=2)
    assert core.registry.get_agent_state(art.id, agent_b) == MESIState.INVALID
    assert core.registry.get_agent_transient(art.id, agent_b) is not None
    (signal,) = [s for s in signals if s.artifact_id == art.id]

    # An eager push moves B back to SHARED at the current version.
    runtime_b.handle_update(
        artifact_id=art.id, version=1, content="v1", now_tick=3, writer_agent_id=None
    )
    assert core.registry.get_agent_state(art.id, agent_b) == MESIState.SHARED

    # The late signal now arrives and is correctly declined -- B is not behind it.
    runtime_b.handle_invalidation(signal)
    assert core.registry.get_agent_state(art.id, agent_b) == MESIState.SHARED

    assert core.registry.get_agent_transient(art.id, agent_b) is None, (
        "the declined invalidation left B's arbitration transient set; the sweep "
        "will skip this pair and B's next commit_cas will fail its precondition"
    )


def test_self_issued_release_is_never_pinned(registry) -> None:
    """An agent handing back its OWN claim is not a cross-agent revoke and is
    never dropped — this is the shape every Claude Code adapter call site uses
    (post-edit failure, session-stop release, operator drain). Its
    ``last_observed_version`` always equals the current version, so a pin that
    did not exempt it would make session-stop unable to release anything."""
    svc = CoordinatorService(registry)
    art = svc.register_artifact(name="plan.md", content="v1")
    a = uuid4()

    svc.write(agent_id=a, artifact_id=art.id, issued_at_tick=1)
    current = svc.registry.get_artifact(art.id).version
    assert svc.registry.last_observed_version_for(art.id, a) == current

    svc.invalidate(
        agent_id=a,
        artifact_id=art.id,
        new_version=current,
        issuer_agent_id=a,  # self-release
        issued_at_tick=2,
    )

    assert svc.registry.get_agent_state(art.id, a) == MESIState.INVALID
    assert svc.registry.get_owner_generation(art.id) == 1, (
        "the release revoked a write claim without moving the version, so it "
        "must move the epoch (F2)"
    )


def test_peer_invalidation_still_clears_a_stranded_transient(registry) -> None:
    """``_write_impl`` invalidates peers directly and leaves their SIA/EIA
    transient for the bus-delivered invalidation to clear. The pin must not
    skip that: an already-INVALID target is pinned by nothing."""
    svc = CoordinatorService(ArtifactRegistry())
    art = svc.register_artifact(name="plan.md", content="v1")
    a, b = uuid4(), uuid4()

    svc.registry.set_agent_state(art.id, b, MESIState.SHARED, trigger="fetch", tick=1)
    svc.write(agent_id=a, artifact_id=art.id, issued_at_tick=2)

    assert svc.registry.get_agent_state(art.id, b) == MESIState.INVALID
    assert svc.registry.get_agent_transient(art.id, b) is not None

    svc.invalidate(
        agent_id=b,
        artifact_id=art.id,
        new_version=svc.registry.get_artifact(art.id).version,
        issuer_agent_id=a,
        issued_at_tick=3,
    )

    assert svc.registry.get_agent_transient(art.id, b) is None, (
        "the pin stranded a peer's invalidation transient"
    )
