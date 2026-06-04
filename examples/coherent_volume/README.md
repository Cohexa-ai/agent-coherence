# CoherentVolume — sequential stale-overwrite demo

Two agents share one workspace file. Both read it; agent A commits an update;
then agent B writes a value it computed from the version it read *before* A's
commit. Without coordination, B's write silently overwrites A's (a lost update).
With `CoherentVolume`, B's stale write is **denied** — B reacquires the current
version and rewrites, so both updates survive.

```bash
python -m examples.coherent_volume.main
```

Sequenced, not raced — deterministic and offline (no API keys, no cost). The
fixed case spawns a local coordinator subprocess on `127.0.0.1`.

## What v1 prevents (and what it does not)

v1 prevents **stale-overwrite lost updates**: a write from a superseded read is
denied until the writer re-reads. The scope is deliberately narrow and exact:

- **Prevents** — the *sequential* stale-read→write lost update on a workspace the
  appliance itself spawned the coordinator for (single-spawner), where each agent
  writes from its most-recent read. A peer commit invalidates the other reader; a
  write from that stale view is denied fail-closed; the writer recovers with
  `reacquire()` (re-mint identity + a mandatory fresh read) and rewrites.
- **Out of scope in v1** — concurrent-write serialization (this is
  single-writer-by-invalidation, not a mutex; two writes whose critical sections
  interleave are not ordered for you) and multi-host coordination. Both are
  explicitly deferred.
- **Boundary** — a writer that re-reads the fresh bytes and then writes a buffer
  it computed *before* that read is not caught by any layer (the honest claim is
  "write from the bytes `read()`/`reacquire()` returned").

## Files

| File | Role |
|---|---|
| `broken.py` | No coherence — B's write from an older read clobbers A (the lost update) |
| `fixed.py` | CoherentVolume denies B's stale write; B reacquires and recovers — no loss |
| `main.py` | Runs both side by side, prints the divergence, exits non-zero if the invariant fails |

## The supported API (explicit)

The explicit `read` / `write` / `reacquire` calls are the supported primitive —
this is what `fixed.py` uses and what the guarantee is built on:

```python
from ccs.adapters.coherent_volume import CoherentVolume
from ccs.core.exceptions import CoherenceError

vol = CoherentVolume("/path/to/workspace", managed=("data/**",))
data = vol.read("data/ledger.txt")            # registers a SHARED view@hash
# ... a peer commits a newer version, invalidating this view ...
try:
    vol.write("data/ledger.txt", new_bytes)   # denied: your view is stale
except CoherenceError:
    fresh = vol.reacquire("data/ledger.txt")  # re-mint identity + mandatory fresh read
    vol.write("data/ledger.txt", recompute(fresh))   # rewritten from current bytes — lands
```

The deny is **sticky**: a bare re-read does not clear it (so a naive retry stays
denied rather than silently overwriting); recovery is `reacquire()`.

## Optional: zero-code-change `install()` shim

If you can't thread `vol.read`/`vol.write` through existing code, `install()`
patches `open()` (and `pathlib`) for managed paths so plain file I/O is
coordinated — a convenience layer over the *same* path above, not a separate
guarantee:

```python
from ccs.adapters.coherent_volume import coherent_workspace

with coherent_workspace("/path/to/workspace", managed=("data/**",)):
    text = open("data/ledger.txt").read()              # registers a SHARED view
    # ... a peer commits ...
    open("data/ledger.txt", "w").write(recompute(text))  # a stale write raises on close()
```

Coverage matrix (demo-grade, single-host): coordinates `builtins.open` and
`pathlib` (`Path.open`/`read_text`/`write_text`/`read_bytes`/`write_bytes`); does
**not** cover `os.open`/`os.write`, `subprocess`/shell redirection, or
append/update/exclusive modes. Recovery still uses the explicit `reacquire()`.
