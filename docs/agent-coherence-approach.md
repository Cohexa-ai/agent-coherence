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
import and the protocol handles the rest. Agents that hold current data skip re-reads; agents that hold
stale data are notified. The protocol enforces single-writer exclusivity and
monotonic versioning as invariants, not conventions.

The same primitives extend past orchestration state to **retrieval and
memory** ([Section 6](why-coherence-matters.md#6-the-same-gap-lands-on-rag-and-agent-memory)).
Because the lost update lives in the agent's cached view of a record — not in
the store — `CCSStore` works as the consistency layer under a memory store,
and [`CoherentVolume`](guide.md) brings the same guarantee to plain-file
corpora and memory shared across processes. It stores no vectors and does no
ranking: it sits underneath whatever you already use to retrieve and remember,
keeping the readers honest and refusing stale writes.

## What this addresses

| Gap (from [Why Coherence Matters](why-coherence-matters.md)) | How agent-coherence responds |
|---|---|
| No defined isolation model (Section 1) | MESI states provide explicit read/write ownership semantics per agent per artifact |
| Concurrent writes unresolvable (Section 2) | Single-writer exclusivity prevents concurrent writes at the protocol level |
| Reducer pattern is append-only (Section 2) | Conflict is avoided by grant, not resolved by merge — only one agent holds write permission at a time |
| Users requesting optimistic locking (Section 4) | Version-tracked artifacts with monotonic versioning; stale writes are rejected |
| Full-context rebroadcasting (Section 5) | Invalidation signals notify agents of changes; only stale caches re-fetch |
| RAG corpora & agent memory are shared mutable state (Section 6) | `CCSStore` and `CoherentVolume` keep the cached *reader* current under any store; the staleness lives in the agent's view, not the store |

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

---

See the [User Guide](guide.md) for installation, configuration, and examples.
