# RAG stale-memory write-back — an agent clobbers a moved memory record

> "A better vector DB won't help. My agent cached a memory record, the source
> moved, and it wrote back a stale edit that erased the update."

Two agents share one memory record — a RAG/memory entry for a customer:
`{"summary": ..., "facts": [...]}`. Agent A reads it. Agent B learns a new fact
and appends it (the record moves). Then agent A writes back an edit it computed
from the snapshot it read *before* B's write — a refreshed summary carried on top
of the old fact list. Without coordination, A's write-back silently overwrites
B's appended fact: a lost update, and the incoherence is now baked into shared
memory that every future retrieval will read.

With `CoherentVolume`, B's write invalidates A's view, so A's stale write-back is
**denied** (`StaleView`, fail-closed). A reacquires the current record, re-applies
its edit intent (the summary refinement) on top of it, and writes — so B's fact
and A's summary both survive.

## What it proves

- **Broken path** (`broken.py`, plain files): A's edit lands, B's fact is erased →
  `lost_update = True`. This is the negative control.
- **Fixed path** (`fixed.py`, CoherentVolume): A's stale write-back is denied, A
  reacquires → re-reads the moved record → re-applies → converges. Both updates
  survive → `lost_update = False`.

The runner exits `0` **only if both hold** (broken must lose, fixed must prevent),
so the red→green is machine-checked, not eyeballed.

## Run it

```bash
cd /path/to/agent-coherence && source .venv/bin/activate
python -m examples.rag_stale_memory.main
```

Sequenced, not raced — deterministic and offline (no API keys, no cost, no
network). The fixed case spawns a local coordinator subprocess on `127.0.0.1`
and tears it down.

## Files

| File | Role |
|---|---|
| `broken.py` | No coherence — A's write-back from an older snapshot erases B's appended fact (the lost update) |
| `fixed.py` | CoherentVolume denies A's stale write; A reacquires and re-applies — no loss |
| `main.py` | Runs both side by side, prints the divergence, exits non-zero if the invariant fails |

## The supported API (explicit)

```python
from ccs.adapters.coherent_volume import CoherentVolume
from ccs.core.exceptions import CoherenceError

vol = CoherentVolume("/path/to/workspace", managed=("memory/**",))
record = json.loads(vol.read("memory/customer.json").decode())   # registers a SHARED view
# ... a peer appends a fact and writes it back, superseding this view ...
try:
    vol.write("memory/customer.json", edited(record))            # denied: your view is stale
except CoherenceError:
    fresh = json.loads(vol.reacquire("memory/customer.json").decode())  # re-mint + fresh read
    vol.write("memory/customer.json", reapply(edit_intent, fresh))      # rewritten from current bytes — lands
```

The deny is **sticky**: a bare re-read does not clear it (so a naive retry stays
denied rather than silently overwriting); recovery is `reacquire()`.

## Honest boundary

Prevents the **sequential** stale-read→write-back lost update on a single-host
coordinator: a write derived from a superseded read is **denied** (`StaleView`)
until the agent `reacquire()`s and re-applies. It does **not** merge two
conclusions an agent has already recorded, does **not** serialize concurrent
writers (use `write_cas` for the true race), and does **not** coordinate across
hosts. Single OS user, one host, cooperative opt-in — every instance coordinating
a workspace must declare the **same** `managed` globs. The recovery re-applies the
edit *intent* on the fresh record; it does not auto-rebase an arbitrary in-flight
buffer computed before the read.
