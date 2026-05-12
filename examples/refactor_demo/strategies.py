# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""Demo variant: CCSStore with peer invalidation suppressed.

Used by the blog's protocol-level proof paragraph and by the
``--variant=no-invalidation`` mode of ``examples.refactor_demo.main``.

Background — why this isn't a SyncStrategy subclass
----------------------------------------------------
The plan originally specified a ``NoInvalidationStrategy`` subclass of
``ccs.strategies.base.SyncStrategy``. While that's a clean object-shape, the
``SyncStrategy.invalidates_peers_on_commit()`` hook is only consumed by
``ccs.simulation.engine`` — the actual ``CCSStore`` path
(``src/ccs/adapters/base.py:126-160``) publishes invalidations
unconditionally. A strategy subclass therefore cannot suppress invalidations
on the real adapter path.

The cleanest mechanism that actually works against ``CCSStore`` is to
override the event bus' ``publish_invalidation`` to a no-op after the store
is constructed. That's what ``disable_invalidation`` does.

If the protocol later routes invalidation publishing through a strategy
method, the ``NoInvalidationStrategy`` class below becomes the natural
hook. Until then it is a forward-compatible placeholder kept here for the
blog post to reference; the operative function is ``disable_invalidation``.
"""

from __future__ import annotations

from typing import Any
from uuid import UUID

from ccs.adapters.ccsstore import CCSStore
from ccs.core.states import MESIState
from ccs.core.types import ArtifactCacheEntry, InvalidationSignal
from ccs.strategies.base import SyncStrategy


class NoInvalidationStrategy(SyncStrategy):
    """Strategy that declares it does not invalidate peers on commit.

    Forward-compatible placeholder. As of the current ``CCSStore``
    implementation this hook does NOT actually suppress invalidations
    on the real adapter path — that's gated by the event bus, not the
    strategy. Use ``disable_invalidation(store)`` for the working
    suppression mechanism.

    Behavior contract identical to ``LazyStrategy`` except for the hook.
    """

    name = "no-invalidation"

    def invalidates_peers_on_commit(self) -> bool:
        return False

    def requires_refresh(self, entry: ArtifactCacheEntry, *, now_tick: int) -> bool:
        del now_tick
        return entry.state == MESIState.INVALID

    def on_read(self, entry: ArtifactCacheEntry, *, now_tick: int) -> ArtifactCacheEntry:
        del now_tick
        return self._touch(entry)

    def on_fetch(
        self,
        *,
        artifact_id: UUID,
        version: int,
        state: MESIState,
        now_tick: int,
    ) -> ArtifactCacheEntry:
        return self._new_entry(artifact_id=artifact_id, version=version, state=state, now_tick=now_tick)


def disable_invalidation(store: CCSStore) -> None:
    """Suppress peer-invalidation publication on a CCSStore.

    Replaces the event bus' ``publish_invalidation`` with a no-op so that
    a writer's commit never marks any peer's cache as INVALID. Peers' caches
    therefore stay SHARED at the version they last read, and subsequent
    reads from the same agent return the stale cached value.

    This is the demo's protocol-level proof of why write-side coherence
    matters: with invalidations suppressed, the planner's v2 write does
    not reach the executor's cache, the executor's commit-time re-read
    returns the SHARED v1 from cache, and the four-caller rename misses
    ``src/utils/session.ts`` — producing a real ``tsc`` failure.

    Mutates ``store`` in place. Apply once, before invoking the graph.
    """

    def _noop_publish_invalidation(
        signal: InvalidationSignal,
        *,
        recipients: Any,
    ) -> int:
        del signal, recipients
        return 0

    store.core.event_bus.publish_invalidation = _noop_publish_invalidation  # type: ignore[assignment]
