# CCSStore read-side coherence — and the write-side boundary, honestly

Your LangGraph agent read a `BaseStore` key a peer already updated and acted on
the stale value. This demo shows exactly what `CCSStore` does about that — and,
just as precisely, what it does **not** do.

Two agents (`planner`, `reviewer`) share one artifact through the store. The
namespace convention carries agent identity in position 0, so
`("planner", "notes")` and `("reviewer", "notes")` address the **same**
artifact.

**Act 1 — read-side invalidation (the CCSStore guarantee).** The reviewer's
repeat read is a local cache hit — no re-fetch, no re-broadcast. The moment the
planner commits a new version, the reviewer's cached copy is invalidated: its
next `get()` is a fresh miss that serves the new version. The stale hit an
agent-local cache would have served never happens.

**Act 2 — the boundary (broken by design).** `put()` is **not** version-CAS. A
writer holding a snapshot from before a peer's commit can still write back, and
the write lands — the peer's update is silently erased, no error raised.
CCSStore keeps *readers* honest; it does not deny a stale *write-back*.

**Act 3 — the write-side fix, one call away.** The same edit routed through
`store.core.write_cas` (the OCC commit-CAS, shipped v0.9.1) re-applies the
writer's intent against the freshly read version on every attempt. Both the
peer's update and the writer's edit survive.

## Run it

```bash
uv run demo.py
# or
pip install "agent-coherence[langgraph]>=0.12.0" && python demo.py
# or, from a repo checkout
python -m examples.ccsstore_read_side.demo
```

Offline, deterministic, no API keys, no model calls. Exit 0 iff every
invariant held — it doubles as a CI gate.

## Honest scope

- One `CCSStore` instance, one Python process, one host. Two processes each
  constructing a `CCSStore` share nothing — for cross-process state on one
  host, put the shared state behind `CoherentVolume` or the
  `stale-write-guard-fs` MCP server.
- Read-side is the CCSStore guarantee. The write-side lost-update deny lives
  in `write_cas` / `CoherentVolume` — that split is the point of this demo,
  not a caveat hidden under it.
- Cross-host coordination is roadmap (design-partner co-build), not shipped.
