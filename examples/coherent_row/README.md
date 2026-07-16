# CoherentRow demo — invalidation before act, over a shared row

Two agents share one row. Agent A reads it and starts a multi-step edit. Agent B
commits a new version. The binding tells A its **cached view went stale before A
acts** — A's next binding-mediated write is denied before the row is touched, and
A reacquires fresh state and re-decides.

```
python -m examples.coherent_row.main             # the with-binding demo
python -m examples.coherent_row.main --baseline  # negative control first, then the binding
```

Offline, no credentials — it spawns a local coordinator subprocess. Exit code `0`
iff the contract holds: with the binding the stale act is **denied before the row
is touched** and A recovers via `reacquire`; with `--baseline` the un-coordinated
agent acts on its stale cache (the failure the binding catches), so the deny is
measured against its absence rather than asserted.

## What it shows

A reads the row and caches it, then reasons for a while. B commits a new version
through the binding. On A's next act, the binding's coordinator pre-read finds A's
view was invalidated by B's commit and **denies the act before A's write reaches
the row** — surfaced as the same typed `StaleView` that A's file and store
artifacts already speak. A calls `reacquire()`, reads fresh state, re-decides, and
commits.

## The value over the row's own compare-and-set

Postgres already gives you a lost-update reject: a conditional write

```sql
UPDATE profiles SET value = $1, version = version + 1 WHERE id = $2 AND version = $3
```

comes back with `rowcount = 0` when a peer moved the version first. That is real
and the binding rides it — but it fires **at write time**, and it hands A a
Postgres-shaped error. What a bare CAS never does:

- **Tell A its cached VIEW went stale before A acts.** A read the row, then spent
  time deciding. The bare CAS says nothing until A finally writes; the binding's
  next read/act is denied the moment A's view is invalid — the case that actually
  bites an agent that reads, reasons, then acts.
- **Speak one vocabulary across substrates.** The deny is the same typed
  `StaleView` / `reacquire` surface A uses for a file (`CoherentVolume`) or a
  store key (`CCSStore`) — one coherence vocabulary over a row, a file, an object,
  a store key. That uniformity is what the binding adds; it is not the row's CAS.

A builder who wraps every read and write in a `pg_advisory_lock` and never caches
a read can rationally stay on the bare CAS. The binding is for the agent that
reads, reasons, then acts.

## Honest scope

- **The read-generation fence is NOT demoed and NOT claimed.** v1 OCC writers ride
  admit-on-absent + the version-CAS; the binding surfaces invalidation, not a
  fence. The fence is a documented roadmap item, not shipped behaviour.
- **In-memory stand-in.** This demo models the row with a tiny in-memory
  dictionary so it runs offline with no credentials. The value it proves — the
  coordinator-mediated invalidation-before-act — is exercised faithfully by the
  real coordinator subprocess. The row's own atomic CAS is the substrate's, and it
  is proven against real Postgres in the conformance kit's `real_substrate` arm.
  In production you point the same binding at a real DSN.
- **Single-host, cooperative.** Two agents/processes coordinate one local
  coordinator over one shared row. Placing agents on two hosts against one
  distributed substrate is a different (harder) problem and is out of scope — do
  not configure it.

## The escaping-effect sibling

This demo is the *state-write* analog of `examples/effect_gate` — where `gate()`
holds an agent's **escaping effect** (a deploy, a PR) on a moved input. The row
binding holds a **state write** on an invalidated cached read. Same ordering
principle, two effect shapes.
