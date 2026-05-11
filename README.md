# agent-coherence

**The coherence layer for multi-agent systems — vendor-neutral, framework-agnostic.**

When two agents share state, one of them is usually reading a stale copy. `agent-coherence` makes that visible — and serves the fresh version on the next read instead of rebroadcasting the full artifact every turn. Same library, same protocol, across LangGraph, CrewAI, AutoGen, and any custom orchestrator. Same behavior regardless of which model provider (Anthropic, OpenAI, Google, Mistral, open-source) the agents talk to.

[![CI](https://github.com/hipvlady/agent-coherence/actions/workflows/ci.yml/badge.svg)](https://github.com/hipvlady/agent-coherence/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/agent-coherence)](https://pypi.org/project/agent-coherence/)
[![arXiv](https://img.shields.io/badge/arXiv-2603.15183-b31b1b)](https://arxiv.org/abs/2603.15183)
[![Discussions](https://img.shields.io/github/discussions/hipvlady/agent-coherence)](https://github.com/hipvlady/agent-coherence/discussions)

```bash
pip install "agent-coherence[langgraph]"   # LangGraph drop-in
pip install "agent-coherence[crewai]"      # CrewAI adapter
pip install "agent-coherence[diagnose]"    # ccs-diagnose CLI
pip install "agent-coherence[all]"         # everything
```

```python
# Before
from langgraph.store.memory import InMemoryStore
store = InMemoryStore()

# After — one import change, no node code changes
from ccs.adapters import CCSStore
store = CCSStore(strategy="lazy")
```

`store.get()`, `store.put()`, `store.search()` keep working unchanged. Savings show up immediately on any workload where multiple agents read the same artifact more often than they write it.

| Workload | Agents | Reads:Writes | Hit rate | Savings |
|---|---|---|---|---|
| Planning (read-heavy) | 4 | 12:1 | 75% | **69%** |
| Code review (moderate) | 3 | 8:3 | 60% | **47%** |
| High-churn (write-heavy) | 4 | 8:4 | 50% | **29%** |

*Measured on real LangGraph graphs; see [docs/REPRODUCE.md](docs/REPRODUCE.md) and the [user guide](docs/guide.md#real-workload-benchmarks).*

---

- 📖 [User guide](docs/guide.md) — installation, namespace convention, strategies, observability, telemetry, examples, full API reference
- 🩺 [`ccs-diagnose` CLI](docs/ccs-diagnose.md) — find divergent reads in your existing LangGraph graph without changing any code
- 🔍 [Why coherence matters](docs/why-coherence-matters.md) — the gap across LangGraph, CrewAI, AutoGen, and Claude Agent SDK
- 🔐 [Security & supply chain](docs/SECURITY.md) — kill switches, hash-pinned install, attestation verification, threat model
- 📜 [Changelog](CHANGELOG.md) — version history
- 📄 [Paper on arXiv (2603.15183)](https://arxiv.org/abs/2603.15183) — formal protocol, TLA+ verification, simulation results

## How it works

Each shared artifact is cached locally per agent and reads serve from the local cache when that copy is fresh. Writes commit to a coordinator, which sends lightweight invalidation signals (~12 tokens) to peers so the next read fetches the new version instead of rebroadcasting the full artifact. Consistency is single-writer-multiple-reader per artifact with bounded staleness — peers re-fetch on next read.

Five synchronization strategies ship out of the box: `lazy` (default), `eager`, `lease` (TTL-based), `access_count`, and `broadcast`. Pick the one that matches your workload's read/write ratio and freshness needs.

## Architecture

- **Protocol** (`ccs.core`, `ccs.strategies`) — coherence state machine and synchronization strategies; no framework dependencies.
- **Coordinator** (`ccs.coordinator`) — authority service tracking directory state, publishing invalidations, and reclaiming stale grants (crash recovery).
- **Adapters** (`ccs.adapters`) — framework integrations for LangGraph, CrewAI, and AutoGen; ~100 lines each.
- **Simulation** (`ccs.simulation`) — deterministic tick-driven engine for scenario benchmarks with failure injection.
- **Event bus** (`ccs.bus`) — pluggable transport for invalidation signals; in-memory by default, swap in Redis, Kafka, NATS, or gRPC streams for production.

Protocol safety properties (single-writer, monotonic versioning, crash-recovery sweep invariants) are model-checked with [TLA+/TLC](formal/tla/README.md). The `tla-check` CI job runs TLC on every push and PR.

## Status

`v0.7` released. See [CHANGELOG.md](CHANGELOG.md) for the version history and [releases](https://github.com/hipvlady/agent-coherence/releases) for tagged artifacts. Alpha — APIs may change before `v1.0`.

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
