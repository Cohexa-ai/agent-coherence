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

Each row is a safety invariant model-checked with TLA+/TLC. `make tla-check` runs all four specs in CI on every push, and every spec carries a documented mutant that must fail — the invariants are load-bearing, not decorative.

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

---

- 📖 [User guide](docs/guide.md) — installation, namespace convention, strategies, observability, telemetry, examples, full API reference
- 🧮 [Formal verification](formal/tla/README.md) — the four TLA+ specs, invariant ↔ implementation map, mutant recipes
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
- **Adapters** (`ccs.adapters`) — framework integrations for LangGraph, CrewAI, and AutoGen (~100 lines each), an experimental OpenAI Agents SDK adapter (`Session`-cache coherence + `RunHooks`), and `CoherentVolume` for plain files shared across processes.
- **Simulation** (`ccs.simulation`) — deterministic tick-driven engine for scenario benchmarks with failure injection.
- **Event bus** (`ccs.bus`) — pluggable transport for invalidation signals; in-memory by default, swap in Redis, Kafka, NATS, or gRPC streams for production.

Protocol safety properties — single-writer, monotonic versioning, the crash-recovery sweep invariants, the OCC no-lost-update, and the reclamation fence's no-stale-apply — are model-checked with [TLA+/TLC](formal/tla/README.md). The `tla-check` CI job runs all four specs on every push and PR.

## Status

**`v0.9.0` released — crash recovery on by default, plus `CoherentVolume` and a temporal cost benchmark.** The crash-recovery default flips from `enabled=False` to **`enabled=True`**, so a bare `CCSStore()` / `CoherenceAdapterCore()` now reclaims stale grants automatically — pass `CrashRecoveryConfig(enabled=False)` to opt out. Byte-identity preservation under the default config now requires explicit `CrashRecoveryConfig(enabled=False)` to reproduce v0.8.x output. Adds `CoherentVolume` — a shared-workspace adapter that fails closed on stale overwrites — and a simulation cost benchmark for temporal source-drift. No wire-protocol changes. See [CHANGELOG.md](CHANGELOG.md).

**Landed on `dev`, unreleased:** the optimistic commit-CAS write API (`commit_cas` / `write_cas` — concurrent same-key lost updates resolve to one winner plus typed, retryable conflicts; `NoLostUpdate` model-checked) and the single-host read-generation fence (crash-reclaimed writers can't land stale commits; `NoStaleApply` model-checked).

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
