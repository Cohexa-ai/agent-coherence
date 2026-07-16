# CoherentObject demo — invalidation before act, over a shared object

Two agents share one object. Agent A reads it and starts a multi-step edit. Agent
B commits a new version. The binding tells A its **cached view went stale before A
acts** — A's next binding-mediated write is denied before the object is touched,
and A reacquires fresh state and re-decides.

```
python -m examples.coherent_object.main             # the with-binding demo
python -m examples.coherent_object.main --baseline  # negative control first, then the binding
```

Offline, no credentials — it spawns a local coordinator subprocess. Exit code `0`
iff the contract holds: with the binding the stale act is **denied before the
object is touched** and A recovers via `reacquire`; with `--baseline` the
un-coordinated agent acts on its stale cache (the failure the binding catches), so
the deny is measured against its absence rather than asserted.

## What it shows

A reads the object and caches it, then reasons for a while. B commits a new
version through the binding. On A's next act, the binding's coordinator pre-read
finds A's view was invalidated by B's commit and **denies the act before A's write
reaches the object** — surfaced as the same typed `StaleView` that A's file and
store artifacts already speak. A calls `reacquire()`, reads fresh state,
re-decides, and commits.

## The value over the object's own compare-and-set

S3 already gives you a lost-update reject: a conditional write

```
aws s3api put-object --bucket b --key k --if-match "$prior_etag" --body ...
```

fails with `412 PreconditionFailed` when a peer moved the object's ETag first.
That is real and the binding rides it — but it fires **at write time**, and it
hands A an S3-shaped error. What a bare CAS never does:

- **Tell A its cached VIEW went stale before A acts.** A read the object, then
  spent time deciding. The bare CAS says nothing until A finally writes; the
  binding's next read/act is denied the moment A's view is invalid — the case that
  actually bites an agent that reads, reasons, then acts.
- **Speak one vocabulary across substrates.** The deny is the same typed
  `StaleView` / `reacquire` surface A uses for a file (`CoherentVolume`) or a
  store key (`CCSStore`) — one coherence vocabulary over an object, a row, a file,
  a store key. That uniformity is what the binding adds; it is not the object's
  CAS.

A builder who serializes every read and write and never caches a read can
rationally stay on the bare CAS. The binding is for the agent that reads, reasons,
then acts.

## Honest scope

- **The read-generation fence is NOT demoed and NOT claimed.** v1 OCC writers ride
  admit-on-absent + the version-CAS; the binding surfaces invalidation, not a
  fence. The fence is a documented roadmap item, not shipped behaviour.
- **In-memory stand-in.** This demo models the object with a tiny in-memory
  dictionary so it runs offline with no credentials. The value it proves — the
  coordinator-mediated invalidation-before-act — is exercised faithfully by the
  real coordinator subprocess. The object's own atomic `If-Match` CAS is the
  substrate's, and it is proven against real S3 in the conformance kit's
  `real_substrate` arm. In production you point the same binding at a real bucket.
- **Single-host, cooperative.** Two agents/processes coordinate one local
  coordinator over one shared object. Placing agents on two hosts against one
  distributed substrate — an S3 Multi-Region Access Point or a cross-region
  replica — is a different (harder) problem and is out of scope; do not configure
  it, and never run the CAS loop through an MRAP.

## The escaping-effect sibling

This demo is the *state-write* analog of `examples/effect_gate` — where `gate()`
holds an agent's **escaping effect** (a deploy, a PR) on a moved input. The object
binding holds a **state write** on an invalidated cached read. Same ordering
principle, two effect shapes.
