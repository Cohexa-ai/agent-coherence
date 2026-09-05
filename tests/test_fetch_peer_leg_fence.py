# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents
"""The fetch peer leg — a stale_read_generation refusal is STICKY.

``read_generation`` is captured on a GENUINE read: the requester leg of
``fetch``, or an I/S -> M/E acquire. ``CoordinatorService.fetch`` used to
rewrite EVERY non-INVALID peer to SHARED with ``trigger="fetch"``, and the
registries capture on that trigger — so a peer already in SHARED was
re-captured on a read it never made, and re-stamped an observation it never
had. A holder the sweep reclaimed and a late same-version update re-granted
SHARED then sat one peer fetch away from a fresh claim: refused
``stale_read_generation`` once, admitted the next time, the version unmoved in
between. The reject was correct; it just was not sticky.

Two legs close it, and this suite pins both:

  SERVICE   ``fetch``'s loop downgrades only M/E holders and leaves a peer
            already in SHARED alone — which is what ``FetchAction`` in
            ``formal/tla/MESI.tla`` always said it did.
  REGISTRY  the trigger-armed capture also requires the agent not to be
            leaving M/E, so a downgrade never refreshes a superseded value
            (the parity half lives in ``test_fencing.py``).

Only the holder's OWN re-read or re-acquire clears the refusal. The suite runs
on both registries and covers both fenced commit paths, the recovery that
keeps ``write_cas`` converging, the EXCLUSIVE downgrade the loop still exists
for, untouched SHARED bystanders, and the reachability of the whole shape
through the public adapter seam.
"""

from __future__ import annotations

from collections import Counter
from functools import partial
from pathlib import Path
from typing import Callable
from uuid import uuid4

import pytest

from ccs.adapters.base import CoherenceAdapterCore
from ccs.bus.event_bus import InMemoryEventBus
from ccs.coordinator.registry import ArtifactRegistry
from ccs.coordinator.service import CoordinatorService
from ccs.coordinator.sqlite_registry import SqliteArtifactRegistry
from ccs.core.exceptions import STALE_READ_GENERATION_REASON
from ccs.core.states import MESIState
from ccs.core.types import Artifact, CommitAllEntry, ConflictDetail, FetchRequest, MultiCommitConflict


@pytest.fixture(params=["in_memory", "sqlite"])
def registry(request, tmp_path: Path):
    """Both registries, identically — the service guard and the capture guard
    must hold on each (see test_fencing.py for the registry-level parity)."""
    if request.param == "in_memory":
        yield ArtifactRegistry()
    else:
        with SqliteArtifactRegistry(tmp_path / "state.db") as reg:
            yield reg


@pytest.fixture(params=["in_memory", "sqlite"])
def logged_registry(request, tmp_path: Path):
    """Both registries with a state log attached, so a test can pin exactly
    which rows a service call emits — and which it does not."""
    log: list[dict] = []
    if request.param == "in_memory":
        yield ArtifactRegistry(state_log=log.append, instance_id="test"), log
    else:
        with SqliteArtifactRegistry(
            tmp_path / "state.db", state_log=log.append, instance_id="test"
        ) as reg:
            yield reg, log


def _seed_reclaimed_shared_holder(svc: CoordinatorService, art_id, writer, holder) -> None:
    """Build the reclaimed-then-re-granted holder THROUGH the service.

      t1    W acquires and commits                  -> version 2, epoch 0
      t3    A acquires EXCLUSIVE                    -> A.rg 0
      t100  the sweep reclaims A (never heartbeated) -> A INVALID, epoch 1, version 2
      t101  a late same-version update re-grants A SHARED
            (``AgentRuntime.handle_update``'s ``"update"`` shape: no capture)

    A now holds a SHARED grant whose captured generation (0) is behind the
    epoch (1) while the version is exactly the one it read — the reclaim the
    version CAS cannot see, which the fence exists to refuse.
    """
    svc.write(agent_id=writer, artifact_id=art_id, issued_at_tick=1)
    updated, _signals = svc.commit(agent_id=writer, artifact_id=art_id, content="v2", issued_at_tick=2)
    assert updated.version == 2

    svc.write(agent_id=holder, artifact_id=art_id, issued_at_tick=3)
    assert svc.registry.get_agent_state(art_id, holder) == MESIState.EXCLUSIVE
    assert svc.registry.get_read_generation(art_id, holder) == 0

    reclaimed = svc.enforce_stable_grant_timeouts(
        current_tick=100, heartbeat_timeout_ticks=10, max_hold_ticks=1000
    )
    assert reclaimed == 1
    assert svc.registry.get_agent_state(art_id, holder) == MESIState.INVALID
    assert svc.registry.get_owner_generation(art_id) == 1
    assert svc.registry.get_artifact(art_id).version == 2

    svc.registry.set_agent_state(art_id, holder, MESIState.SHARED, trigger="update", tick=101)
    assert svc.registry.get_read_generation(art_id, holder) == 0, '"update" is not a capture trigger'


def _fenced_commit_reason(reg, art_id, agent, *, path: str, tick: int) -> str | None:
    """The holder's commit at version 2 on ``path``: its refusal reason, or None on a win."""
    if path == "commit_cas":
        result = reg.commit_cas(art_id, agent, expected_version=2, content_hash="zombie", tick=tick)
        return result.reason if isinstance(result, ConflictDetail) else None
    result = reg.commit_all(
        agent,
        {art_id: CommitAllEntry(expected_version=2, content_hash="zombie", content="zombie")},
        tick=tick,
    )
    return result.per_artifact[art_id].reason if isinstance(result, MultiCommitConflict) else None


@pytest.mark.parametrize("commit_path", ["commit_cas", "commit_all"])
def test_peer_fetch_does_not_rearm_a_reclaimed_holders_fence(registry, commit_path) -> None:
    """A ``stale_read_generation`` refusal is sticky across a peer's fetch.

      t102  A's commit at version 2          -> REFUSED stale_read_generation
      t103  peer P fetches                    -> A is already SHARED: left alone
      t104  A's commit at version 2, again    -> REFUSED again; version still 2

    Before the fix t103 rewrote A to SHARED with ``trigger="fetch"``, the
    registry captured epoch 1 into A's ``read_generation``, and t104 landed
    (version 3) — a commit the fence had just refused, admitted because
    someone ELSE read the artifact. Both commit paths, both registries.
    """
    svc = CoordinatorService(registry)
    art = svc.register_artifact(name="plan.md", content="v1")
    w, a, p = uuid4(), uuid4(), uuid4()
    _seed_reclaimed_shared_holder(svc, art.id, w, a)

    assert _fenced_commit_reason(registry, art.id, a, path=commit_path, tick=102) == STALE_READ_GENERATION_REASON
    assert registry.get_artifact(art.id).version == 2

    svc.fetch(FetchRequest(artifact_id=art.id, requesting_agent_id=p, requested_at_tick=103))

    reason = _fenced_commit_reason(registry, art.id, a, path=commit_path, tick=104)
    assert reason == STALE_READ_GENERATION_REASON, (
        f"the reclaim-zombie's {commit_path} was refused stale_read_generation, a peer fetched, "
        f"and the identical commit was then {'ADMITTED' if reason is None else 'refused ' + reason} "
        f"(version now {registry.get_artifact(art.id).version})"
    )
    assert registry.get_artifact(art.id).version == 2, "phantom version bump"
    assert registry.get_read_generation(art.id, a) == 0, (
        "a peer's fetch re-captured the reclaim-zombie's read_generation on a read it never made"
    )


def test_own_reread_after_a_peer_fetch_recaptures_and_wins(registry) -> None:
    """Only the holder's OWN read clears the refusal: the requester leg of
    ``fetch`` still captures, SHARED -> SHARED included, so a reclaimed SHARED
    agent recovers through its re-read and the ``write_cas`` retry loop keeps
    converging. The fence is sticky, not permanent."""
    svc = CoordinatorService(registry)
    art = svc.register_artifact(name="plan.md", content="v1")
    w, a, p = uuid4(), uuid4(), uuid4()
    _seed_reclaimed_shared_holder(svc, art.id, w, a)
    svc.fetch(FetchRequest(artifact_id=art.id, requesting_agent_id=p, requested_at_tick=103))
    assert registry.get_agent_state(art.id, a) == MESIState.SHARED

    svc.fetch(FetchRequest(artifact_id=art.id, requesting_agent_id=a, requested_at_tick=104))

    assert registry.get_agent_state(art.id, a) == MESIState.SHARED
    assert registry.get_read_generation(art.id, a) == 1, (
        "the holder's own SHARED -> SHARED re-read did not capture the current epoch"
    )
    result = registry.commit_cas(art.id, a, expected_version=2, content_hash="fresh", tick=105)
    assert not isinstance(result, ConflictDetail), f"a re-read holder's commit was refused: {result.reason}"
    assert registry.get_artifact(art.id).version == 3


def test_peer_fetch_still_downgrades_an_exclusive_holder(logged_registry) -> None:
    """Teeth for the guard: the skip is for SHARED peers only. An EXCLUSIVE
    holder is still downgraded by a peer's fetch, with its E -> S ``"fetch"``
    row in the state log — the MESI downgrade the loop exists for."""
    reg, log = logged_registry
    svc = CoordinatorService(reg)
    art = svc.register_artifact(name="plan.md", content="v1")
    a, p = uuid4(), uuid4()

    svc.fetch(FetchRequest(artifact_id=art.id, requesting_agent_id=a, requested_at_tick=1))
    assert reg.get_agent_state(art.id, a) == MESIState.EXCLUSIVE
    log.clear()

    svc.fetch(FetchRequest(artifact_id=art.id, requesting_agent_id=p, requested_at_tick=2))

    assert reg.get_agent_state(art.id, a) == MESIState.SHARED, "an EXCLUSIVE holder survived a peer's fetch"
    rows = {(e["agent_id"], e["from_state"], e["to_state"]) for e in log if e["trigger"] == "fetch"}
    assert rows == {(str(a), "EXCLUSIVE", "SHARED"), (str(p), "INVALID", "SHARED")}


def test_peer_fetch_leaves_shared_bystanders_untouched(logged_registry) -> None:
    """One EXCLUSIVE holder, two SHARED bystanders, one peer fetch: only the
    holder is downgraded. The bystanders keep their captured generation and
    their recorded observation, and get no state-log row.

    Seeded directly — EXCLUSIVE cannot coexist with SHARED holders through the
    service — with the epoch and the version both moved past what the
    bystanders captured, so a rewrite would show on every axis.
    """
    reg, log = logged_registry
    svc = CoordinatorService(reg)
    art = svc.register_artifact(name="plan.md", content="v1")
    holder, b1, b2, p = uuid4(), uuid4(), uuid4(), uuid4()

    for bystander in (b1, b2):
        reg.set_agent_state(art.id, bystander, MESIState.SHARED, trigger="fetch", tick=1)
    bumped = Artifact(id=art.id, name="plan.md", version=2, content_hash="h2")
    reg.set_artifact_and_content(art.id, bumped, "v2")
    reg.set_agent_state(art.id, holder, MESIState.EXCLUSIVE, trigger="write", tick=2)
    reg.set_agent_state(art.id, holder, MESIState.INVALID, trigger="invalidate", tick=3)
    reg.set_agent_state(art.id, holder, MESIState.EXCLUSIVE, trigger="write", tick=4)
    assert reg.get_owner_generation(art.id) == 1
    for bystander in (b1, b2):
        assert reg.get_read_generation(art.id, bystander) == 0
        assert reg.last_observed_version_for(art.id, bystander) == 1
    log.clear()

    svc.fetch(FetchRequest(artifact_id=art.id, requesting_agent_id=p, requested_at_tick=5))

    assert reg.get_agent_state(art.id, holder) == MESIState.SHARED
    assert reg.get_agent_state(art.id, p) == MESIState.SHARED
    for bystander in (b1, b2):
        assert reg.get_read_generation(art.id, bystander) == 0, (
            "a bystander's read_generation was re-captured by a peer's fetch"
        )
        assert reg.last_observed_version_for(art.id, bystander) == 1, (
            "a bystander's observation was re-stamped by a peer's fetch"
        )
    expected = {str(holder), str(p)}
    touched = {e["agent_id"] for e in log}
    assert touched == expected, f"a peer's fetch wrote state for bystanders: {touched - expected}"


class _DeferredBus(InMemoryEventBus):
    """Holds every publish until ``release`` — the late-delivery shape.

    A broadcast is minted inside the registry lock and delivered after it is
    released (``adapters/base.py``); this bus stretches that window so the
    test can run a sweep reclaim inside it, in one thread, with no gating.
    """

    def __init__(self) -> None:
        super().__init__()
        self.held: list[Callable[[], int]] = []

    def publish_invalidation(self, signal, *, recipients):  # type: ignore[override]
        self.held.append(partial(InMemoryEventBus.publish_invalidation, self, signal, recipients=list(recipients)))
        return 0

    def publish_update(self, event, *, recipients):  # type: ignore[override]
        self.held.append(partial(InMemoryEventBus.publish_update, self, event, recipients=list(recipients)))
        return 0

    def release(self) -> None:
        held, self.held = self.held, []
        for deliver in held:
            deliver()


def test_adapter_fence_forces_a_reclaimed_writer_to_reread_before_write_cas(monkeypatch) -> None:
    """Reachability through the public adapter seam: the reclaimed writer
    cannot win ``write_cas`` on its stale claim; the fence forces a re-read.

      t1    W writes v2 through the adapter        -> W MODIFIED; the eager
            broadcast to P is HELD on the bus
      t2    Q registers (joined after the mint: not a recipient, cache cold)
      t200  the sweep reclaims W (no heartbeat)    -> W INVALID, epoch 1, v2
      rel   the held broadcast lands on P; P's ``handle_update`` writer leg
            re-grants W SHARED with ``"update"``   -> W.rg 0 < epoch 1
      t201  Q reads                                -> cache cold, reaches
            ``service.fetch``: the peer leg the fix guards
      t202  W calls ``write_cas``                  -> refused once, re-reads,
            wins at v3 with ITS bytes

    Before the fix t201 rewrote W SHARED -> SHARED with ``"fetch"``, the
    registry captured epoch 1 for W, and t202 landed with ZERO fetches — a
    claim the fence had refused, cleared because someone else read.

    Why a deferred bus: with the default synchronous ``InMemoryEventBus`` the
    broadcast is delivered inside W's own ``write`` call, so W is downgraded
    M -> SHARED before it ever returns, and there is no M/E grant left for a
    sweep to reclaim — the state cannot be reached. Why Q, not P, is the
    fetcher: the released broadcast leaves P holding a SHARED cache entry at
    v2, and under the eager strategy a SHARED entry is a cache hit
    (``requires_refresh`` only on INVALID), so P's read never reaches the
    coordinator. Neither is a second assertion path; both are why the
    choreography is shaped as it is.
    """
    bus = _DeferredBus()
    core = CoherenceAdapterCore(strategy_name="eager", event_bus=bus)
    w = core.register_agent("W", now_tick=0)
    p = core.register_agent("P", now_tick=0)
    art = core.register_artifact(name="plan.md", content="v1")

    fetches: Counter = Counter()
    real_fetch = core.coordinator.fetch

    def _counted_fetch(request):
        fetches[request.requesting_agent_id] += 1
        return real_fetch(request)

    monkeypatch.setattr(core.coordinator, "fetch", _counted_fetch)

    updated = core.write(agent_name="W", artifact_id=art.id, content="w-v2", now_tick=1)
    assert updated.version == 2
    assert core.registry.get_agent_state(art.id, w) == MESIState.MODIFIED
    assert bus.held, "the eager broadcast was not held"
    q = core.register_agent("Q", now_tick=2)

    # The adapter's own sweep fires only from read()/write(), and either would
    # fetch first — a read by W would take a spurious grant. Drive the
    # coordinator's sweep directly, as a scheduler would.
    reclaimed = core.coordinator.enforce_stable_grant_timeouts(
        current_tick=200, heartbeat_timeout_ticks=10, max_hold_ticks=1000
    )
    assert reclaimed == 1
    assert core.registry.get_agent_state(art.id, w) == MESIState.INVALID
    assert core.registry.get_owner_generation(art.id) == 1
    assert core.registry.get_artifact(art.id).version == 2

    bus.release()

    assert core.registry.get_agent_state(art.id, p) == MESIState.SHARED
    assert core.registry.get_agent_state(art.id, w) == MESIState.SHARED, (
        "the recipient's update handling did not re-grant the writer SHARED"
    )
    assert core.registry.get_read_generation(art.id, w) == 0, '"update" is not a capture trigger'

    core.read(agent_name="Q", artifact_id=art.id, now_tick=201)
    assert fetches[q] == 1, "Q's read was not the cache-cold fetch the scenario needs"
    assert core.registry.get_agent_state(art.id, q) == MESIState.SHARED
    w_generation_after_peer_fetch = core.registry.get_read_generation(art.id, w)
    # W's cache-cold fetch inside its t1 write is already on the counter; only
    # the fetches write_cas itself performs are the forced re-read.
    w_fetches_before = fetches[w]

    won = core.write_cas(
        agent_name="W",
        artifact_id=art.id,
        make_content=lambda _entry: ("w-v3", None),
        now_tick=202,
    )

    rereads = fetches[w] - w_fetches_before
    assert rereads >= 1, (
        f"the reclaimed writer's write_cas won with {rereads} coordinator fetches: a peer's "
        "read cleared a refusal only the writer's own re-read may clear"
    )
    assert won.version == 3
    assert core.registry.get_content(art.id) == "w-v3", "the committed bytes are not the writer's"
    assert w_generation_after_peer_fetch == 0, (
        "a peer's fetch re-captured the reclaimed writer's read_generation on a read it never made"
    )


def test_a_shared_requesters_own_reread_leaves_a_zombie_bystander_alone(registry) -> None:
    """The requester leg captures for the REQUESTER, and only for it.

    P is already SHARED when it re-reads (S -> S, the lease-expiry and
    ``write_cas``-retry shape), so the step is a genuine read for P and a no-op
    for everyone else. The reclaim-zombie A is a SHARED bystander in that same
    step: it must keep its superseded generation and its refusal, while P's own
    slot moves to the current epoch.
    """
    svc = CoordinatorService(registry)
    art = svc.register_artifact(name="plan.md", content="v1")
    w, a, p = uuid4(), uuid4(), uuid4()

    _seed_reclaimed_shared_holder(svc, art.id, w, a)
    # P joins as a reader first (I -> S): the seed's write and commit invalidate
    # every peer, so P can only reach SHARED after the zombie exists.
    svc.fetch(FetchRequest(artifact_id=art.id, requesting_agent_id=p, requested_at_tick=102))
    assert registry.get_agent_state(art.id, p) == MESIState.SHARED, "P must re-read from SHARED, not from INVALID"
    assert registry.get_read_generation(art.id, a) == 0, "P's first read must not have re-armed the zombie either"

    svc.fetch(FetchRequest(artifact_id=art.id, requesting_agent_id=p, requested_at_tick=103))

    assert registry.get_read_generation(art.id, p) == 1, "the requester's own S -> S re-read did not capture"
    assert registry.get_read_generation(art.id, a) == 0, (
        "an already-SHARED requester's re-read re-armed a bystander reclaim-zombie"
    )
    reason = _fenced_commit_reason(registry, art.id, a, path="commit_cas", tick=104)
    assert reason == STALE_READ_GENERATION_REASON, f"the zombie's commit was {reason or 'ADMITTED'}"
    assert registry.get_artifact(art.id).version == 2


def test_fetch_with_only_shared_peers_grants_shared_and_downgrades_nobody(logged_registry) -> None:
    """The skip narrows the WRITE, never the grant decision.

    ``other_holders`` still sees every non-INVALID peer, so a requester joining
    two SHARED readers is granted SHARED — not EXCLUSIVE beside them. Nothing
    is written for either bystander: no state row, no observation, no log entry.
    """
    reg, log = logged_registry
    svc = CoordinatorService(reg)
    art = svc.register_artifact(name="plan.md", content="v1")
    b1, b2, p = uuid4(), uuid4(), uuid4()

    svc.fetch(FetchRequest(artifact_id=art.id, requesting_agent_id=b1, requested_at_tick=1))
    svc.fetch(FetchRequest(artifact_id=art.id, requesting_agent_id=b2, requested_at_tick=2))
    assert reg.get_agent_state(art.id, b1) == MESIState.SHARED
    assert reg.get_agent_state(art.id, b2) == MESIState.SHARED
    before = {agent: reg.get_read_generation(art.id, agent) for agent in (b1, b2)}
    log.clear()

    response = svc.fetch(FetchRequest(artifact_id=art.id, requesting_agent_id=p, requested_at_tick=3))

    assert response.state_grant == MESIState.SHARED, "a requester was granted EXCLUSIVE beside live SHARED readers"
    for agent in (b1, b2):
        assert reg.get_agent_state(art.id, agent) == MESIState.SHARED
        assert reg.get_read_generation(art.id, agent) == before[agent]
    assert {e["agent_id"] for e in log} == {str(p)}, "a peer's fetch wrote state for a SHARED bystander"
