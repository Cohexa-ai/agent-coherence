# agent-coherence

**`agent-coherence` makes "agent A silently clobbered agent B's `plan.md`" impossible — a vendor-neutral MESI + optimistic-concurrency coordinator for agent state, with the safety invariants machine-checked in TLA+.**

Two agents share an artifact — a `plan.md`, a store key, a `memory.json`. One reads it and works; meanwhile a peer commits a newer version; the first writes back anyway. Last write wins, the peer's work is silently gone, nothing errors, and every downstream decision builds on the wrong version. `agent-coherence` turns that silent clobber into a loud, typed refusal: MESI-style ownership and invalidation over shared artifacts, optimistic commit-CAS for concurrent writers, and a read-generation fence for crash-reclaimed ones — a stale write is denied or returned as a retryable conflict, never silently applied. Same library, same protocol, across LangGraph, CrewAI, AutoGen, the OpenAI Agents SDK, plain files shared across processes (`CoherentVolume`), and any custom orchestrator. Same behavior regardless of which model provider (Anthropic, OpenAI, Google, Mistral, open-source) the agents talk to.

[![CI](https://github.com/hipvlady/agent-coherence/actions/workflows/ci.yml/badge.svg)](https://github.com/hipvlady/agent-coherence/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/agent-coherence)](https://pypi.org/project/agent-coherence/)
[![arXiv](https://img.shields.io/badge/arXiv-2603.15183-b31b1b)](https://arxiv.org/abs/2603.15183)
[![Discussions](https://img.shields.io/github/discussions/hipvlady/agent-coherence)](https://github.com/hipvlady/agent-coherence/discussions)

```bash
pip install "agent-coherence[langgraph]"        # LangGraph drop-in
pip install "agent-coherence[crewai]"           # CrewAI adapter
pip install "agent-coherence[openai-agents]"    # OpenAI Agents SDK adapter (experimental)
pip install "agent-coherence[diagnose]"         # ccs-diagnose CLI
pip install "agent-coherence[all]"              # everything
```

```python
# Before
from langgraph.store.memory import InMemoryStore
store = InMemoryStore()

# After — one import change, no node code changes
from ccs.adapters import CCSStore
store = CCSStore(strategy="lazy")
```

`store.get()`, `store.put()`, `store.search()` keep working unchanged — reads now serve the current version, and a write from a stale view can't silently land.

```python
# Plain files shared across processes / sessions — no framework required
from ccs.adapters.coherent_volume import CoherentVolume

vol = CoherentVolume(workspace_root, managed=("plans/**",))
plan = vol.read("plans/plan.md")           # tracked read — your view is registered
vol.write("plans/plan.md", revised_plan)   # stale view? denied fail-closed → vol.reacquire() and re-derive
```

`agent-coherence-replay` — invariant-replay for any CoherenceAdapterCore-mediated agent system. LangGraph capture verified in v1 via `CCSStore.record_to(path)`; CrewAI / AutoGen wired through the same seam but unverified — file an issue if it breaks.

## What it guarantees

Each row is a safety invariant model-checked with TLA+/TLC. `make tla-check` runs all five specs in CI on every push, and every spec carries a documented mutant that must fail — the invariants are load-bearing, not decorative.

| The silent failure | What happens instead | Mechanism | Invariant |
|---|---|---|---|
| **Stale-read overwrite** — an agent acts on an old snapshot and writes over a newer version (two sessions, one `plan.md`) | the write is **denied fail-closed**; the writer must `reacquire()` and read the current version | MESI single-writer ownership + invalidation | `SingleWriter`, `MonotonicVersion` |
| **Concurrent lost update** — two writers hit the same key and both "succeed" | exactly one wins; the loser gets a **typed conflict + bounded retry**, never a silent drop | optimistic commit-CAS (`write_cas`) | `NoLostUpdate` |
| **Reclaim-zombie write** — a stalled writer is reclaimed by crash recovery, wakes later, and lands its stale commit; the version never moved, so a version check passes | the commit is **rejected** with a typed `stale_read_generation` conflict | read-generation fence — reclamation bumps the artifact's ownership epoch, checked atomically at commit | `NoStaleApply` |
| **Dead owner blocks the fleet** — a crashed agent holds EXCLUSIVE forever | the heartbeat/TTL sweep reclaims the grant (on by default) | crash-recovery sweep | sweep invariants I3–I6 |

**Scope, honestly:** the guarantees hold for writers that go through the coordinator, under a single coordinator (one host). Concurrent same-key writers on one host are covered; cross-host fencing is on the roadmap, demand-gated — if you need it, [open an issue](https://github.com/hipvlady/agent-coherence/issues/new). Specs, the invariant ↔ implementation map, and the mutant recipes live in [`formal/tla/`](formal/tla/README.md).

**Correctness is the wedge; the token savings come with it.** Writes publish ~12-token invalidation signals instead of rebroadcasting full artifacts, so read-heavy fleets stop re-paying for state they already hold:

| Workload | Agents | Reads:Writes | Hit rate | Savings |
|---|---|---|---|---|
| Planning (read-heavy) | 4 | 12:1 | 75% | **69%** |
| Code review (moderate) | 3 | 8:3 | 60% | **47%** |
| High-churn (write-heavy) | 4 | 8:4 | 50% | **29%** |

*Measured on real LangGraph graphs; see [docs/reproduce.md](docs/reproduce.md) and the [user guide](docs/guide.md#real-workload-benchmarks).*

Those are the **spatial** savings (more agents sharing one artifact). The **temporal** dimension — a single agent whose source drifts between its turns — has its own pre-registered benchmark, **TC-1** (#116): a reproducible savings-regime map of how many re-fetches coherence-gating avoids as the change-rate rises. The metric is *re-fetches-avoided* — a proxy, a regime map, **not** a token/dollar invoice. Reproduce with `python tools/run_cost_sweep.py`; the locked verdict + numbers (PASS at n=50, crossover r≈0.31) live in [`benchmarks/cost_preregistration.md`](benchmarks/cost_preregistration.md). Shipped in `v0.9.3`.

## RAG & shared agent memory

RAG corpora and agent memory are **shared mutable state**, so the stale-read→write lost update lands there too — and *a consistent store doesn't save you*: the staleness is in the **agent's cached view of a record**, not the store. Two agents read a record at v1; one writes v2; the other, still on its v1, writes an edit computed from v1 and clobbers v2. `agent-coherence` keeps the readers honest: `CCSStore` is a drop-in for `langgraph.store` (and composes with Mem0, Letta, LlamaIndex, a vector store, or a plain file via `CoherentVolume`) — it stores no vectors and does no ranking; it's the consistency layer underneath whatever you already use to retrieve and remember.

- **Runnable, deterministic demo** (offline, no keys): `python -m examples.coherent_volume.main` reproduces the documented lost update, then prevents it.
- **Honest scope:** writes that go through the coordinator are caught. Auto-watching an *unmanaged external source* that changes with no coordinator write (a hand-edited file, an out-of-band re-index) is the source-watcher case — **on the roadmap, demand-gated, not shipped today.**
- **Positioning + FAQ:** [agent-coherence.dev/rag](https://agent-coherence.dev/rag/).

---

- 📖 [User guide](docs/guide.md) — installation, namespace convention, strategies, observability, telemetry, examples, full API reference
- 🔎 [RAG & shared memory](https://agent-coherence.dev/rag/) — coherence for retrieval corpora and agent memory stores, with the runnable lost-update demo
- 🗂️ [Coherent workspace](#coherent-workspace-the-data-plane-for-shared-files) — `CoherentVolume`, the data-plane appliance for plain files shared across processes
- 🧮 [Formal verification](formal/tla/README.md) — the five TLA+ specs, invariant ↔ implementation map, mutant recipes
- 🩺 [`ccs-diagnose` CLI](docs/ccs-diagnose.md) — find divergent reads in your existing LangGraph graph without changing any code
- 🧩 [Claude Code plugin](https://github.com/hipvlady/agent-coherence-plugin) — cross-session coherence for the prose rules (CLAUDE.md, plan.md) parallel Claude Code sessions share
- 🔍 [Why coherence matters](docs/why-coherence-matters.md) — the gap across LangGraph, CrewAI, AutoGen, and Claude Agent SDK
- 🔐 [Security & supply chain](docs/security.md) — kill switches, hash-pinned install, attestation verification, threat model
- 📜 [Changelog](CHANGELOG.md) — version history
- 📄 [Paper on arXiv (2603.15183)](https://arxiv.org/abs/2603.15183) — formal protocol, TLA+ verification, simulation results

## How it works

Each shared artifact is cached locally per agent and reads serve from the local cache when that copy is fresh. Writes commit to a coordinator, which sends lightweight invalidation signals (~12 tokens) to peers so the next read fetches the new version instead of rebroadcasting the full artifact. Consistency is single-writer-multiple-reader per artifact with bounded staleness — peers re-fetch on next read.

Two write disciplines share the same guarantee. **Pessimistic:** acquire EXCLUSIVE, commit; a writer whose view went stale is denied and must `reacquire()`. **Optimistic:** `write_cas` — read, compute, commit-CAS; the loser of a race gets a typed conflict and bounded retry. Crash recovery composes with both: reclaiming a stalled grant bumps the artifact's ownership epoch, so a reclaimed writer that completes later is rejected at commit even when the version is unchanged (the read-generation fence).

Five synchronization strategies ship out of the box: `lazy` (default), `eager`, `lease` (TTL-based), `access_count`, and `broadcast`. Pick the one that matches your workload's read/write ratio and how aggressively cached reads should refresh.

## Architecture

- **Protocol** (`ccs.core`, `ccs.strategies`) — coherence state machine and synchronization strategies; no framework dependencies.
- **Coordinator** (`ccs.coordinator`) — authority service tracking directory state, publishing invalidations, arbitrating commit-CAS, and reclaiming stale grants (crash recovery + read-generation fence).
- **Adapters** (`ccs.adapters`) — framework integrations for LangGraph, CrewAI, and AutoGen (~100 lines each), plus an experimental OpenAI Agents SDK adapter (`Session`-cache coherence + `RunHooks`).
- **Coherent workspace** (`ccs.adapters.coherent_volume`) — the **data-plane appliance**: an out-of-process coordinator client that brings the same guarantee to plain files on disk, no framework required. See [Coherent workspace](#coherent-workspace-the-data-plane-for-shared-files).
- **Simulation** (`ccs.simulation`) — deterministic tick-driven engine for scenario benchmarks with failure injection.
- **Event bus** (`ccs.bus`) — pluggable transport for invalidation signals; in-memory by default, swap in Redis, Kafka, NATS, or gRPC streams for production.

Protocol safety properties — single-writer, monotonic versioning, the crash-recovery sweep invariants, the OCC no-lost-update, the reclamation fence's no-stale-apply, and version retention's no-collected-read — are model-checked with [TLA+/TLC](formal/tla/README.md). The `tla-check` CI job runs all five specs on every push and PR.

## Coherent workspace: the data plane for shared files

The framework adapters wrap a store. `CoherentVolume` is the other half — the **data-plane appliance**, the building block that makes a shared *workspace* coherent for plain files on disk, with no framework in the loop. Architecturally it's an out-of-process coordinator *client*, not an in-process wrapper: it writes the policy, spawns (or attaches to) a local coordinator over SQLite-WAL, and routes reads and writes through it. Your content stays on the real filesystem; the coordinator holds only MESI state, a content hash, and a version per managed file. Point a sibling volume in another process at the same workspace and it attaches to the same coordinator, so a single-host fleet shares one coherent view.

```python
from ccs.adapters.coherent_volume import CoherentVolume

vol = CoherentVolume(workspace_root, managed=("plans/**", "memory/**"))
data = vol.read("plans/plan.md")              # bytes — registers a SHARED view
vol.write("plans/plan.md", revise(data))      # stale view? denied fail-closed
data = vol.reacquire("plans/plan.md")         # recover: re-mint identity + mandatory fresh read
```

The explicit `read` / `write` / `reacquire` / `write_cas` API is the **supported** primitive (`write_cas(path, make_content)` is the optimistic counterpart for same-key contention — the loser gets a typed conflict, never a silent drop). For code you'd rather not rewrite, an **opt-in, demo-grade** `open()` shim routes managed-path opens through the volume so existing `open()` / `pathlib` calls get coherence unchanged:

```python
from ccs.adapters.coherent_volume import coherent_workspace

with coherent_workspace(workspace_root, managed=("plans/**",)):
    text = open("plans/plan.md").read()       # registers a SHARED view
    open("plans/plan.md", "w").write(edit)    # stale view? raises out of close()
```

**Scope, honestly.** v1 prevents the **sequential** stale-read→write lost update for a single-host fleet sharing one workspace (A reads v1, B reads v1, A commits v2, B's stale write is denied → B re-reads). It does **not** serialize concurrent racing writers, nor catch an agent that re-reads fresh bytes and then writes a buffer computed from older ones. The `open()` shim is convenience, not the contract: it covers `open()`/`pathlib` text+binary read/write, but not raw `os.open`, subprocess redirection, `mmap`, or append/update modes — those delegate to the original `open()` unchanged. Run it yourself: `python -m examples.coherent_volume.main` (offline, deterministic, no keys), or read the [positioning + FAQ](https://agent-coherence.dev/rag/).

## Status

**`v0.10.1` released — an opt-in cross-host coordination demo (default-off) plus a bracketed-IPv6 fix to the coordinator's Host-allowlist.** A new, fully opt-in demo (`CCS_REMOTE_COORDINATOR`, default off) coordinates two clients across a host boundary against one centralized coordinator: a stale write is denied by version-CAS *across the boundary* and the loser recovers via re-read + retry (slice 1), and an effect gated on `config@vN` fires only when the config is unchanged (slice 2) — with a `--baseline` negative-control mode that runs the silent-lost-update / stale-fire failures first, so the deny is measured against its absence. Library fix: the coordinator's Host-allowlist check (`verify_host`) now parses bracketed IPv6 literals (`[fc00::1]:port`) and matches IP literals on their normalized form, with the loopback/IPv4 path byte-unchanged and DNS-rebind protection preserved. All cross-host behavior is gated by the flag; the default loopback path is unchanged. See [CHANGELOG.md](CHANGELOG.md).

See [CHANGELOG.md](CHANGELOG.md) for the full version history and [releases](https://github.com/hipvlady/agent-coherence/releases) for tagged artifacts. Alpha — APIs may change before `v1.0`.

## Paper

**Token Coherence: Adapting MESI Cache Protocols to Minimize Synchronization Overhead in Multi-Agent LLM Systems**
arXiv:[2603.15183](https://arxiv.org/abs/2603.15183)

<details>
<summary>BibTeX</summary>

```bibtex
@article{parakhin2026token,
  title   = {Token Coherence: Adapting MESI Cache Protocols to Minimize
             Synchronization Overhead in Multi-Agent LLM Systems},
  author  = {Parakhin, Vladyslav},
  journal = {arXiv preprint arXiv:2603.15183},
  year    = {2026}
}
```

</details>

## Community

Questions, war stories, and ideas welcome in [Discussions](https://github.com/hipvlady/agent-coherence/discussions). If you've hit a stale-read bug in a multi-agent workflow, [open an issue](https://github.com/hipvlady/agent-coherence/issues/new) — I'd like to hear about it.

## License

Apache-2.0. See [LICENSE](LICENSE).
