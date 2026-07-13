# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Executable Tier-1 backend conformance kit (plan Unit 5, R16 + R18 check).

This package verifies a :class:`~ccs.coordinator.registry_protocol.RegistryBase`
implementation against the atomic-boundary contract formalized in
:mod:`ccs.coordinator.backend_contract` (Unit 4). It exists so that a future
networked backend has an *executable* target — not just a prose contract — for
the R9 single-writer atomic boundary, and so the two SHIPPED registries are
each proven conformant at their honest declared tier.

Two verification arms today:

- :class:`~ccs.coordinator.sqlite_registry.SqliteArtifactRegistry` — declares
  **Tier-1** (:data:`ccs.coordinator.backend_contract.Tier.TIER_1`): the full
  R9 tuple under one ``BEGIN IMMEDIATE`` transaction, PLUS durable
  restart-survival of ``session_meta`` / pins.
- :class:`~ccs.coordinator.registry.ArtifactRegistry` (in-memory) — declares
  its OWN honest tier: it hosts the full atomic boundary under a single process
  (GIL-atomic), so it passes every MUST-MATCH scenario, but it declares
  restart **LOSS** (process-scoped state). That is NOT a Tier-1 / HA-stateless
  claim — a fresh in-memory instance is a fresh, empty store. The kit asserts
  the in-memory arm's declared restart-loss *as declared*, never as a bug.

**The must-match vs backend-defined split is STRUCTURAL in the kit API, not a
convention** (see :mod:`tests.backend_conformance.kit`): MUST-MATCH scenarios
are functions over a bare :class:`~tests.backend_conformance.kit.RegistryFactory`
that hard-assert the same correctness property on every arm; BACKEND-DEFINED
scenarios take a per-arm :class:`~tests.backend_conformance.kit.RestartDeclaration`
and assert the arm behaves *as its own declaration says*. A backend-defined
function's signature DEMANDS a declaration — it cannot assert cross-arm
identity, so the split cannot silently erode into "assert everything matches"
(the flake / watering-down failure mode the plan's Risks table names).

**Honesty limit of the concurrency arm (declared, stated once, here).** The
concurrency scenarios use two independent SQLite handles on ONE WAL database
file inside ONE process. That is a genuine two-connection race against a shared
durable store — the real-concurrency arm the plan calls for — but it is NOT a
full cross-PROCESS exercise: both handles share the process, the GIL, and the
OS page cache. A true multi-process arm (separate interpreters over the same
store, or over a network socket) is a DECLARED kit limitation, deferred to the
first networked backend. No subprocess arm is built in v1 (subprocess pools
flake on constrained CI — the mp-pool learning). The cross-PROCESS coordinator
path IS separately exercised over HTTP by the existing e2e / ``protocol_corpus``
suites, but that is the SERVER seam, not the registry seam this kit targets.

Reason matching is ALWAYS ``reason == CONSTANT`` against the wire-stable
constants imported from :mod:`ccs.core.exceptions` / the ``ConflictDetail``
literal set — NEVER a substring of a human message (the
typed-signal-not-substring house rule).

Concurrency-realism discipline (from ``tests/test_occ_commit_cas.py``): the
ONLY hard assertion in a concurrency scenario is the correctness property; a
``threading.Barrier`` forces the race window, and "did the threads actually
overlap" is a :class:`RuntimeWarning`, never an assertion (a constrained runner
may serialize threads).
"""
