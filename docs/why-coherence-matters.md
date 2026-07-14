# Why Coherence Matters

Across today's multi-agent frameworks, concurrent writes are treated as a
crash to prevent, not a conflict to resolve. This document collects public
evidence of the gap: framework documentation, unresolved community questions,
and production bug reports. All sources are linked and dated so readers can
verify independently.

---

## 1. The consistency model is unspecified

On December 17, 2025, a backend engineer building an Aerospike-backed
`BaseStore` for LangGraph
[asked the LangChain team](https://forum.langchain.com/t/langgraph-store-batch-semantics-should-putops-be-applied-immediately-or-deferred-deduped-until-end/2545)
a precise question: when `batch()` receives a list of `PutOp` and `GetOp`
operations, do reads later in the list observe earlier writes, or does the
framework treat reads as a pre-batch snapshot?

As of May 2026 — five months later — the thread has no reply from LangChain
staff or community members.

The question is not obscure. It is the definition of an isolation model: does
a store provide read-your-writes consistency, snapshot isolation, or something
else? Without an answer, every `BaseStore` implementer must guess, and two
implementations of the same interface can silently diverge in behavior under
concurrent access.

## 2. The framework treats concurrent writes as unresolvable

LangGraph's
[INVALID_CONCURRENT_GRAPH_UPDATE](https://docs.langchain.com/oss/python/langgraph/errors/INVALID_CONCURRENT_GRAPH_UPDATE)
error page states that the framework raises this error because "there is
uncertainty around how to update the internal state" when parallel nodes
write to the same state key.

The recommended workaround is the **reducer pattern**: annotate the state key
with a merge function such as `Annotated[list, operator.add]`, which converts
the key to append-only. This eliminates the error by sidestepping conflict
resolution entirely — concurrent writes are concatenated, not merged.

Two limitations follow. First, append-only is not conflict resolution — if
two agents attempt to update the same item (not append new items), the
reducer produces duplicates rather than a resolved value. Second, even the
append pattern is fragile in practice: community threads report surprising
behavior including exponential duplication and unexpected list nesting when
the reducer interacts with LangGraph's `Command` API.[^1]

[^1]: See [exponential duplication with operator.add](https://forum.langchain.com/t/subject-operator-add-reducer-causes-exponential-duplication-in-annotated-list-state-fields-when-tools-update-state/1546) and [unexpected append behavior](https://forum.langchain.com/t/why-doesn-t-add-reducer-append-properly-to-my-state-list-in-command-update-even-when-i-always-pass-a-list/910).

## 3. The gap surfaced in LangChain's own product

In September 2025, a user
[reported](https://github.com/langchain-ai/deepagents/issues/96) the exact
`INVALID_CONCURRENT_GRAPH_UPDATE` error in Deep Agents — the multi-agent
harness LangChain promotes for production use. Parallel tool nodes writing to
a shared `todos` list triggered the crash.

The issue was closed in January 2026 after a maintainer submitted a fix
(PR [#34637](https://github.com/langchain-ai/langchain/pull/34637)). The fix
added a `_todos_reducer` — the same append-only pattern documented in the
error page. The crash was resolved; the underlying semantic question (what
happens when two agents update the *same* todo) was not.

The crash is gone; the semantic gap remains. The framework prevents the
failure mode without resolving it.

## 4. Users are independently requesting concurrency primitives

On October 30, 2025, a LangGraph user
[filed a feature request](https://forum.langchain.com/t/feature-request-support-concurrency-safe-store-put-operations/2014)
for concurrency-safe `store.put` operations after encountering race
conditions in `get → modify → put` workflows under `langmem`. The proposed
mechanism — "only update a row if the current row value meets the
expectation" — is optimistic locking, the same compare-and-swap primitive
that MVCC systems use.

As of May 2026, the request has no response from LangChain staff.

The pattern is notable because the user arrived at the primitive
independently, from production experience, without referencing database
theory. When users reinvent concurrency control vocabulary from first
principles, it suggests the underlying need is structural rather than
edge-case.

## 5. The pattern across frameworks

The detailed evidence above is drawn from LangGraph because it has the
largest public surface area (documentation, forum, issue tracker). The
underlying pattern — full-context rebroadcasting with no coherence
primitives — recurs across frameworks:

- **CrewAI** passes the complete raw output of upstream tasks to downstream
  consumers via `Task.context`. The framework's internal
  [`aggregate_raw_outputs_from_tasks`](https://github.com/crewAIInc/crewAI/blob/main/lib/crewai/src/crewai/crew.py)
  helper joins unmodified `TaskOutput.raw` strings, discarding any structured
  or Pydantic output. Downstream tasks cannot access typed fields from
  upstream results without manual templating
  ([Issue #1977](https://github.com/crewAIInc/crewAI/issues/1977)).
  The official docs confirm that task output is
  [relayed into the next task automatically](https://docs.crewai.com/en/concepts/tasks).

- **AutoGen** uses a shared-topic pub/sub model in its
  [GroupChat](https://microsoft.github.io/autogen/stable//user-guide/core-user-guide/design-patterns/group-chat.html)
  pattern: all participants subscribe to the same message thread, and a
  Group Chat Manager selects the next speaker upon receiving each message.
  The only context-management option is `BufferedChatCompletionContext` — a
  truncation window, not selective sharing or invalidation.

- **Claude Agent SDK** delegates multi-agent coordination entirely to the
  integrator. Its sandbox model provides isolated execution environments but
  no shared-state primitives, leaving coordination to external tooling.

The frameworks differ in *how* they handle the gap. LangGraph errors when
concurrent writes arise; CrewAI and AutoGen avoid the failure mode by
enforcing sequential execution (task DAGs and turn-taking respectively);
Claude Agent SDK delegates coordination to the integrator. None resolves the
underlying conflict — they prevent or sidestep it. In all cases, the default
context-passing mechanism is full rebroadcasting: correct (no stale reads)
but expensive, and scaling poorly as agent count or shared state size grows.

## 6. The same gap lands on RAG and agent memory

The evidence above is drawn from orchestration state, but the identical
failure lands on a surface most teams add later: **retrieval corpora and
agent memory**. A RAG index and a memory store are shared mutable state —
agents read records, recompute, and write them back — so the
stale-read→write lost update applies unchanged. Two agents read a memory
record at v1; one writes v2; the other, still holding the v1 it read, writes
an edit derived from v1 and silently clobbers v2. Section 4's
optimistic-locking request is this same need surfacing from the memory side:
a `get → modify → put` over `langmem` is exactly the read-recompute-write
that loses updates when the read is stale.

The non-obvious part: **a consistent store does not save you.** The staleness
is not in the store — it is in the *agent's cached view of a record*. Agents
cache retrieved state locally to avoid re-paying for the full history on every
step, and that local copy goes stale the moment a peer writes, no matter how
strongly consistent the backing store is. Coherence here is about keeping the
*readers* honest, not replacing the store: it is the consistency layer
underneath whatever vector store, memory library (Mem0, Letta, LlamaIndex), or
plain file you already use to retrieve and remember.

The boundary is worth stating plainly. CCSStore provides read-side coherence:
when a peer commits a new version, your cached view is invalidated so your next
read is a fresh miss. It does not deny a stale write-back — `put` is not
version-CAS. For write-side lost-update prevention (a stale writer overwriting a
peer), route writes through CoherentVolume or `write_cas`. Separately,
auto-watching an *unmanaged external source* that changes with no coordinator
write — a hand-edited corpus file, an out-of-band re-index — is a source-watcher
problem, not solved by keeping cached readers coherent.

### The same class lands on repos, configs, and CI

The evidence above is Segment A — shared memory and orchestration state. The
identical stale-read→write class also lands on a second surface: **shared
repositories, configuration, and CI inputs** worked by more than one automated
actor (parallel coding agents, bots, pipelines). The reports below are from
unrelated third-party projects. They validate the failure *class*, not this
library's mechanism, and none of them are users of it.

- **Renovate [#18804](https://github.com/renovatebot/renovate/issues/18804)**:
  the bot checks a branch as unmodified using a cached result, then rebases —
  but the user force-pushed between the check and the write, so the rebase
  clobbers their commits. The reporter names it directly: "a time-of-check vs
  time-of-use bug." A cached read that has gone stale, driving a write-back.
- **Terraform's `Saved plan is stale` error**: `terraform apply` refuses a
  saved plan once current state has moved past the snapshot the plan was
  computed against. The tool fails closed rather than apply a stale plan — an
  explicit guard against the stale-read→write hazard on infrastructure state.
- **Atlantis** (PR-driven Terraform automation) shows the same shape at team
  scale: parallel PRs plan against a base a merged peer has already advanced,
  so a later apply runs on a stale plan.
- **Parallel Claude Code sessions** on one repository exhibit it directly — two
  sessions read shared files, plans, or configs, and one writes back over a
  version a peer already advanced.

The pattern is invariant across surfaces: an automated actor reads shared state,
does work, and writes back against a version that has since moved. As with
Segment A, the shipped enforcement here is single-host (one coordinator);
cross-host coordination is on the roadmap, demand-gated.

## 7. Open questions

Several questions remain unanswered by any framework or library:

- **What isolation level do multi-agent workloads actually need?** Read-your-writes may suffice for most; some may need snapshot isolation or serializable access. The answer likely varies by workload shape.
- **Is coherence worth its coordination cost for small agent counts?** At 2–3 agents with small shared state, full rebroadcasting may be cheaper than maintaining coherence metadata. The crossover point is not well-characterized.
- **What isolation level does agent memory specifically need?** Section 6 argues the same read-your-writes coherence that applies to orchestration state applies to memory and RAG — the staleness is in the cached view, not the store. What remains open is whether *long-term* memory (e.g., `langmem`) needs a stronger level than ephemeral shared state — snapshot isolation across a multi-step recall, say — or whether read-your-writes suffices there too.

## 8. One approach — and an invitation

This document is written problem-first: it maps the gap, and names a shipped
surface only where a claim needs an honest boundary. For one concrete approach
to closing the gap — MESI-derived coherence over
multi-agent shared state — see
[How agent-coherence Fills the Gap](agent-coherence-approach.md).

If you are hitting this shape in production — a stale read that lands on a
write-back over a peer's update, on shared memory, a repo, or a config — we
would like to hear how it shows up for you. Independent reproductions of the
failure class are especially useful. The shipped enforcement is single-host
(one coordinator); cross-host coordination is on the roadmap, demand-gated.

---

*Last verified: May 7, 2026 (Segment-A evidence). Segment-B anchors (§6) added
and confirmed live July 14, 2026. All URLs were confirmed live and all claims
checked against current source material on these dates.*
