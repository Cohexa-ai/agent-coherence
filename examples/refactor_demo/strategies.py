# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""Demo variant: CCSStore with peer invalidation suppressed.

Used by the blog's protocol-level proof paragraph and by the
``--variant=no-invalidation`` mode of ``examples.refactor_demo.main``.

Why this is a function, not a SyncStrategy subclass
----------------------------------------------------
The plan originally specified a ``NoInvalidationStrategy`` subclass of
``ccs.strategies.base.SyncStrategy``. While that's a clean object-shape, the
``SyncStrategy.invalidates_peers_on_commit()`` hook is only consumed by
``ccs.simulation.engine`` — the actual ``CCSStore`` path
(``src/ccs/adapters/base.py:126-160``) and ``CCSStore._apply_delete``
publish invalidations unconditionally. A strategy subclass therefore cannot
suppress invalidations on the real adapter path, and registering one via
``CCSStore(strategy='no-invalidation')`` would fail anyway because
``ccs.strategies.selector.build_strategy`` has no case for it.

The cleanest mechanism that actually works against ``CCSStore`` is to
override the event bus' ``publish_invalidation`` to a no-op after the store
is constructed. That's what ``disable_invalidation`` does.

If the protocol ever routes invalidation publishing through a strategy
method, this module is the natural place to add a real subclass. For now,
keeping a dead-code "forward-compatible placeholder" subclass would only
mislead readers, so we don't.
"""

from __future__ import annotations

from typing import Any

from ccs.adapters.ccsstore import CCSStore
from ccs.core.types import InvalidationSignal


def disable_invalidation(store: CCSStore) -> None:
    """Suppress peer-invalidation publication on a CCSStore.

    Replaces the event bus' ``publish_invalidation`` with a no-op so that a
    writer's commit never marks any peer's cache as INVALID. Peers' caches
    therefore stay SHARED at the version they last read, and subsequent
    reads from the same agent return the stale cached value.

    This is the demo's protocol-level proof of why write-side coherence
    matters: with invalidations suppressed, the planner's v2 write does not
    reach the executor's cache, the executor's commit-time re-read returns
    the SHARED v1 from cache, and the four-caller rename misses
    ``src/utils/session.ts`` — producing a real ``tsc`` failure.

    Scope note: ``CCSStore`` also calls ``publish_invalidation`` from
    ``_apply_delete`` (``ccsstore.py:306``), so this patch suppresses
    invalidation signaling for deletes as well as writes. The demo never
    deletes, so that's incidental — but a future demo that adds a delete
    step under ``--variant=no-invalidation`` will see the same suppression.

    Irreversibility: there is no ``enable_invalidation`` counterpart. Once
    applied, the store stays patched for its lifetime. The demo's
    orchestrator (``main.run``) creates a fresh ``CCSStore`` per call, so
    this is safe in context. Code that reuses a patched store across
    multiple graph invocations must construct a new store to get coherent
    behavior back — there is no in-place restore.

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
