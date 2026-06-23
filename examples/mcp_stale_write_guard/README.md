# stale-write-guard-fs — red→green front door

A constructed, deterministic, offline demo of the `stale-write-guard-fs` MCP
server. The **same** read→write sequence loses an update without the guard and is
prevented with it. The green path is driven through the server's own tool
contract, so it proves the server — not just the coordinator beneath it.

```bash
python -m examples.mcp_stale_write_guard.main
```

Exits `0` only if **all four** hold:

| Case | What it shows |
|------|---------------|
| **RED** — no coherence | the agent's stale buffer clobbers a peer's commit → the update is lost |
| **GREEN** sequential | the stale write is **denied** (`stale_view`); the agent reacquires and does not clobber → the peer's value survives *exactly* |
| **GREEN** concurrent | two writers compare-and-set the same version; the loser re-reads, re-merges, re-CASes → both lines land (the *exact* golden merge) |
| **CONTROL** | the green flow with the deny **disabled** → the loss returns, proving green depends on the deny, not on a refetch that masks it |

Two single-host regimes, both fail-closed:

- **sequential** stale-overwrite — denied, recover with reacquire.
- **concurrent** same-key — typed conflict, *not* auto-merge: you read, merge, retry.

## Scope (the honesty floor)

Single-host only. **Out of guarantee and not detected in v1:** writers on
different hosts or across a synced/network mount, divergent-history
reconciliation, semantic correctness, server-enforced auto-merge. The server
enforces *version lineage*, not that your content was derived from what you read.
