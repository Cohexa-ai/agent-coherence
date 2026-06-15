# Divergent memory — two sessions record contradictory beliefs from a stale read

A vendor-neutral, **single-file** demo of the failure mode where a memory or
observation layer ends up holding two conflicting answers to the same question —
not because a write was lost, but because two sessions *derived* their
conclusions from stale reads.

Two sessions evaluate the same decision. Session A concludes "PostgreSQL" and
records it. Session B, reasoning from the snapshot it cached *before* A committed,
independently concludes "SQLite" and records that. Both writes "succeed" — the
store is consistent in the sense that every write landed. The incoherence is in
*what the two sessions believe*. A future agent that reads both gets contradictory
context. With `CoherentVolume`, A's commit invalidates B's view, so B's write from
the stale snapshot is **denied fail-closed**; B reacquires, re-reads the current
decision, and records a conclusion consistent with it — the divergence never forms.

## Divergent-view vs. lost-update

This is the sibling of [`../shared_knowledge_base/`](../shared_knowledge_base/),
which shows a **lost update** (B's write clobbers A's finding — data disappears).
Here the sharper symptom is **divergence**: the two sessions reach contradictory
conclusions about the same decision. Same coherence primitive, different and — for
a memory layer — more visceral failure narrative. If you only show one to a
memory/RAG vendor, this is the one that names their pain: *your sessions now
believe two different things*.

## Run it

No repo checkout, no API keys, offline (the fixed case spawns a local coordinator
on `127.0.0.1` and tears it down):

```bash
uv run demo.py
# or
pip install "agent-coherence>=0.9.0" && python demo.py
```

It exits non-zero unless the invariant held both ways (BROKEN must diverge; FIXED
must converge via the denied-then-reacquired path) — trustworthy, not eyeballed.

## Honest scope

The library prevents the **stale read that causes** the divergence: B cannot
record a conclusion derived from a view a peer already superseded without
re-reading first. It does **not** detect, merge, or reconcile two conclusions an
agent has *already* recorded into separate stores — it stops the divergence
forming, it does not repair it after the fact. The honest demo therefore routes
B's conclusion through a coordinated write whose stale view is what gets denied.

Same boundary as the other CoherentVolume demos: v1 prevents the **sequential**
stale-read→write under a single-host coordinator; concurrent-write serialization
and cross-host coordination are out of scope.

## Why this shape is worth a demo

The divergent-view failure is real and recurring in agent-memory trackers, e.g.
[`thedotmack/claude-mem#2909`](https://github.com/thedotmack/claude-mem/issues/2909)
(no cross-session isolation — all sessions' observations mixed at injection
because context is filtered by project, never session) and
[`#2821`](https://github.com/thedotmack/claude-mem/issues/2821) (parallel sessions
silently drop observations under lock contention). It is the one shape the other
demos and the Mem0 probe do not frame.

## The cost side effect

Gating reads on version also cuts redundant re-fetches. A pre-registered,
reproducible sweep puts the re-fetch savings at **≥30% sustained for change-rates
r ≤ 0.30** (crossover r≈0.31, PASS at n=50). That is a regime map on synthetic
sources, not a measured invoice — but it reproduces from committed code:
[`benchmarks/cost_preregistration.md`](../../benchmarks/cost_preregistration.md).
