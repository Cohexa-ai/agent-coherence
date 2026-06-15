# Shared knowledge base — lost update in a RAG / agent-memory corpus

A vendor-neutral, **single-file** demo of the headline failure mode for any
system where agents retrieve from a shared corpus and write findings back to it:
a RAG knowledge base, a memory layer, a reasoning-RAG index, a team `learnings.md`.

Two research agents enrich one shared record. Both retrieve it; agent A appends a
finding and commits; agent B writes back from the snapshot it cached *before* A's
commit. Without coordination, B silently overwrites A's finding — a lost update,
no error. With `CoherentVolume`, B's stale write is **denied fail-closed**, B
re-reads the current record and rewrites, and both findings survive.

The point the demo makes: **the staleness was never in the store.** Both writes
"succeed" at the filesystem. The stale thing is *B's cached view of the record* —
which a consistent store can't catch but a coherence layer over the read/write
boundary can. The same guarantee maps onto a vector DB, a memory layer, or a
reasoning-RAG index.

## Run it

No repo checkout, no API keys, offline (the fixed case spawns a local coordinator
on `127.0.0.1` and tears it down):

```bash
uv run demo.py
# or
pip install "agent-coherence>=0.9.0" && python demo.py
```

It exits non-zero unless the invariant held both ways (BROKEN must lose A's
finding; FIXED must preserve both) — trustworthy, not eyeballed.

## Relationship to the other demos

- [`../coherent_volume/`](../coherent_volume/) — the same primitive, the original
  "two agents, one file" framing, reverse-engineered from a real federated fleet
  (76 production transcripts). That demo is the in-repo proof; **this one is the
  single-file, vendor-neutral, async-shareable version** for the RAG /
  shared-agent-memory problem class.
- Scope and honest caveats are identical to `coherent_volume`: v1 prevents the
  **sequential** stale-read→write lost update under a single-host coordinator;
  concurrent-write serialization and cross-host coordination are out of scope.
  See the `coherent_volume` README for the exact boundary.

## The cost side effect

Gating reads on version also cuts redundant re-fetches: a write publishes a
~12-token invalidation instead of rebroadcasting the full record, so a reader
holding a still-valid view doesn't re-pay for context that didn't change. A
pre-registered, reproducible sweep puts the re-fetch savings at **≥30% sustained
for change-rates r ≤ 0.30** (crossover r≈0.31, PASS at n=50). That is a regime
map on synthetic sources, not a measured invoice — but it reproduces from
committed code: [`benchmarks/cost_preregistration.md`](../../benchmarks/cost_preregistration.md)
+ `tools/run_cost_sweep.py`.
