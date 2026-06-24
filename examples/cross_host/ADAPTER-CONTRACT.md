# Substrate-adapter contract (DX-3)

The cross-host demo claims: *"same protocol, two topologies."* This document
makes that claim **testable** — defines the version-CAS contract any substrate
adapter must satisfy, lists the shipped implementations, and points at the
parity test that proves both adapters exhibit the same deny semantics against
the same scenario.

## Why this exists

Without a written contract, "the protocol is the same, only the substrate
differs" is an assertion. With this contract + the parity test, it's a *passing
test* — the version-CAS surface that prevents the silent lost update is shown
to generalize beyond any single substrate, on the same shipped primitives.

Anchored to the north-star [Slices + the path](../../docs/bizops/north-star-research-agenda.md) §
DX-3 ("Pluggable artifact substrate — the *same* protocol drives ≥2 adapters")
and the cross-host pilot one-pager § "Honest scope" NR-2.

## The contract

A *substrate adapter* is any object that exposes:

```python
class CoherenceAdapter(Protocol):
    """The version-CAS contract any substrate adapter satisfies."""

    def read_with_version(self, key: str) -> tuple[bytes, int]:
        """Return ``(data, version)`` for ``key``. Untracked / missing keys
        return ``(b'', 0)``. The returned version is the decision-time version
        a subsequent ``write_cas_at`` will CAS against."""

    def write_cas_at(self, key: str, expected_version: int, data: bytes) -> None:
        """Commit ``data`` for ``key`` iff the current version equals
        ``expected_version``. On a stale version, raise a typed
        ``CoherenceError`` (concretely ``CasVersionConflict`` for the shipped
        adapters) — never silently overwrite."""
```

The contract is **not** the storage shape (file, KV, blob). It is the
version-CAS *deny* — `write_cas_at(stale_version, ...)` raises rather than
overwrites, and `read_with_version` returns the version the caller must use as
the decision-time anchor.

### Recovery is part of the contract

A `CasVersionConflict` is **recoverable**, not terminal. The caller re-reads
(getting a fresh version), reconciles its proposed write against the new state,
and retries. This is the "broken-must-lose AND fixed-must-prevent" half of the
demo's exit-code contract.

## Shipped implementations

| Adapter | Substrate | Cross-host transport | Where |
|---|---|---|---|
| **`CoherentVolume`** | Files on a (local or shared) volume; per-key version held in the coordinator's registry | HTTP via the networked coordinator (`CCS_REMOTE_COORDINATOR=1`) | `src/ccs/adapters/coherent_volume.py` |
| **`CCSStore`** | LangGraph `BaseStore` (KV) | In-process today; the same version-CAS surface, no transport change required | `src/ccs/adapters/ccsstore.py` |

Both are first-class shipped adapters in the agent-coherence library. The
cross-host demo exercises the file substrate over the network; the KV substrate
is exercised by the LangGraph integration tests.

### Why this matters for cross-host

Adapter-substrate independence is **half the cross-host story** (the other half
is the networked coordinator itself, NR-1). A file-on-volume vendor and a
KV-store vendor coordinate concurrent writes through the *same* version-CAS
protocol; the substrate adapter abstracts the bytes, the coordinator owns the
version. A pilot can pick whichever substrate matches its workload without
buying into a different coordination model.

## Parity test

[`tests/test_adapter_contract_parity.py`](../../tests/test_adapter_contract_parity.py)
runs the **same scenario** through:

1. **`CoherentVolume`** against a real loopback coordinator (the shipped file
   adapter, the same code path the cross-host demo exercises).
2. **A minimal in-memory KV adapter** that satisfies the Protocol — *not* a
   shipped product; a reference implementation that proves the contract
   generalizes to substrates the shipped library does not provide.

The single scenario both adapters run:

```
A reads K @ version v_a
B reads K @ version v_a, writes (v_a, "from-b") — version advances to v_a+1
A writes (v_a, "from-a")                        — STALE, must deny
A reads K @ version v_a2 (= v_a+1)              — recovery read
A writes (v_a2, "from-a-2")                     — fresh CAS, must succeed
```

The test asserts both adapters:
- raise `CoherenceError` (the version-CAS deny) on A's stale write,
- successfully recover after A re-reads + retries against the fresh version.

If a future adapter is added, this test grows by one parametrized case — the
contract stays the same, the parity claim stays testable, and the demo's
DX-3 ("same protocol, two topologies") stops being asserted and starts being
proved.

## What this contract is NOT

- **Not a transport spec** — the contract defines the version-CAS *surface*,
  not how `read`/`write` reach the coordinator (HTTP, in-process, FUSE, MCP all
  qualify; the cross-host demo's NR-2 names which one is in-scope today).
- **Not a strict-mode spec** — the version-CAS deny is independent of strict
  mode's read-fence; both shipped adapters version-CAS write-deny without
  needing strict-mode enforcement (the cross-host demo runs `tracked != strict`
  per the README, NR-2).
- **Not a durability spec** — `synchronous=FULL` durability is a separate
  axis from version-CAS atomicity. A future adapter that backs onto a durable
  log adds durability without changing the CAS surface this contract defines.
- **Not a snapshot/transaction spec** — multi-key consistent reads (SB-17)
  and atomic multi-key publish (SB-18) are *additional* surfaces on top of
  this contract, gated behind separate demand signals. The slice-1 +
  slice-2 demo lives strictly inside this contract.
