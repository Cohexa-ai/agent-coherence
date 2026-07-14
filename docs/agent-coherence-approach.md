# How agent-coherence Fills the Gap

This document describes one approach to the coherence gap documented in
[Why Coherence Matters](why-coherence-matters.md). Other approaches are
possible: CRDT-based merge for collaborative artifacts where concurrent
writes are semantically composable, MVCC-style snapshot isolation for
parallel branches where subagents need isolated reasoning before
reconciliation, or framework-level solutions at the storage or orchestration
layer. MESI suits read-heavy shared context with localized writes — the
dominant pattern across the frameworks documented in the companion piece.

---

## The approach

The [`agent-coherence`](https://github.com/Cohexa-ai/agent-coherence) library
provides coherence primitives for multi-agent shared state: version tracking,
invalidation signaling, and configurable synchronization strategies (eager,
lazy, lease-based). It implements a MESI-derived protocol adapted from CPU
cache coherence — a domain where the "multiple readers, shared mutable state"
problem has been solved for decades.

For LangGraph specifically, `agent-coherence` ships a drop-in `BaseStore`
replacement (`CCSStore`) that adds coherence semantics — swap the store
import and the protocol handles the rest. Agents that hold current data skip
re-reads; an agent holding a stale version isn't pushed a notification — its
cached view is invalidated, so its next read misses and re-fetches. The
protocol enforces single-writer exclusivity and monotonic versioning as
invariants, not conventions — modeled and machine-checked in
[`formal/tla/`](../formal/tla/README.md) (TLA+/TLC, run in CI).

The same primitives extend past orchestration state to **retrieval and
memory** ([Section 6](why-coherence-matters.md#6-the-same-gap-lands-on-rag-and-agent-memory)).
Because the lost update lives in the agent's cached view of a record — not in
the store — `CCSStore` works as the consistency layer under a memory store.
`CCSStore` provides read-side coherence: when a peer commits a new version,
your cached view is invalidated so your next read is a fresh miss. It does not
deny a stale write-back — `put` is not version-CAS. For write-side lost-update
prevention (a stale writer overwriting a peer), route writes through
[`CoherentVolume`](guide.md) or `write_cas`, which carry that write-side denial
to plain-file corpora and memory shared across processes. Neither stores
vectors nor ranks: they sit underneath whatever you already use to retrieve and
remember, keeping the readers honest and — on the write side — denying stale
write-backs.

## What this addresses

| Gap (from [Why Coherence Matters](why-coherence-matters.md)) | How agent-coherence responds |
|---|---|
| No defined isolation model (Section 1) | MESI states provide explicit read/write ownership semantics per agent per artifact |
| Concurrent writes unresolvable (Section 2) | Single-writer exclusivity prevents concurrent writes at the protocol level |
| Reducer pattern is append-only (Section 2) | Conflict is avoided by grant, not resolved by merge — only one agent holds write permission at a time |
| Users requesting optimistic locking (Section 4) | Version-tracked artifacts with monotonic versioning; a stale write-back is denied on the write side via `write_cas`/`CoherentVolume` (shipped, single-host). `CCSStore`'s `put` is not version-CAS — it is read-side coherence only |
| Full-context rebroadcasting (Section 5) | Invalidation marks a cache stale; only a stale cache re-fetches, on its next read (pull, not push) |
| RAG corpora & agent memory are shared mutable state (Section 6) | `CCSStore` and `CoherentVolume` keep the cached *reader* current under any store; the staleness lives in the agent's view, not the store |
| Concurrent same-key writes / true-race lost update (Section 2) | `write_cas`/`write_cas_at`: a version-checked optimistic write — a stale writer's CAS fails and the caller retries, so neither update is silently dropped (single-host, one coordinator) |
| Escaping side effects fire on a stale input (Sections 2, 5) | `gate()` orders an escaping effect (a deploy, an opened PR, a notification) against the input version it was decided from, and raises before firing if the input changed — it *orders* effects, it never rolls one back; single-host and cooperative |
| Non-LangGraph agents / any MCP client need coordinated file access (Section 1) | The `stale-write-guard-fs` MCP server exposes `CoherentVolume` as MCP tools; instances sharing one `SWG_ROOT` attach to a single coordinator, so a stale write is denied across sessions and peers fail closed if that coordinator exits |
| Prose project rules that can't be expressed as policy drift across sessions | The Claude Code plugin keeps that shared-rule subset coherent across sessions (single-host) |
| Multi-file edits must land together, never half-applied (Section 2) | Multi-artifact snapshot sessions (`atomic_publish`/`commit_all`, v0.12.0+) publish a set of files all-or-nothing, so a reader never sees a partially-applied edit (single-host) |

## What this does not address

The [open questions](why-coherence-matters.md#7-open-questions) in the
evidence document apply here too:

- The coordination-cost crossover point for small agent counts is not yet
  well-characterized.
- Whether *long-term* memory needs a stronger isolation level than the
  read-your-writes coherence we provide — snapshot isolation across a
  multi-step recall, say — is unresolved. Cached-reader coherence for memory
  and RAG is addressed (above); the right *level* for long-lived memory is not.
- The right isolation level for different workload shapes remains an open
  research question.
- Enforcement is single-host (one coordinator). Cross-host coordination is on
  the roadmap, demand-gated.

---

See the [User Guide](guide.md) for installation, configuration, and examples.
