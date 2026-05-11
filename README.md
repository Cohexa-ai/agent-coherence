# agent-coherence

**The coherence layer for multi-agent systems — vendor-neutral, framework-agnostic.**

When two agents share state, one of them is usually reading a stale copy. `agent-coherence` makes that visible — and serves the fresh version on the next read instead of rebroadcasting the full artifact every turn. Same library, same protocol, across LangGraph, CrewAI, AutoGen, and any custom orchestrator. Same behavior regardless of which model provider (Anthropic, OpenAI, Google, Mistral, open-source) the agents talk to.

[![CI](https://github.com/hipvlady/agent-coherence/actions/workflows/ci.yml/badge.svg)](https://github.com/hipvlady/agent-coherence/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/agent-coherence)](https://pypi.org/project/agent-coherence/)
[![arXiv](https://img.shields.io/badge/arXiv-2603.15183-b31b1b)](https://arxiv.org/abs/2603.15183)
[![Discussions](https://img.shields.io/github/discussions/hipvlady/agent-coherence)](https://github.com/hipvlady/agent-coherence/discussions)

```bash
# Pick the integration you need. Library is the same across all of them.
pip install "agent-coherence[langgraph]"   # LangGraph (drop-in CCSStore)
pip install "agent-coherence[crewai]"      # CrewAI adapter
pip install "agent-coherence[diagnose]"    # ccs-diagnose CLI (stale-read detector)
pip install "agent-coherence[all]"         # everything, including OTel + LangSmith
```

```python
# LangGraph drop-in — one import change, no node code changes
from langgraph.store.memory import InMemoryStore  # before
from ccs.adapters import CCSStore                  # after

store = CCSStore(strategy="lazy")
graph = builder.compile(store=store)
```

That's it. Node code stays identical; `store.get()`, `store.put()`, `store.search()` still work the same. The savings show up immediately on any workload where multiple agents read the same artifact more often than they write it.

```
$ python -m examples.shared_codebase.main

Example: 4-agent shared-codebase code review

  style_reviewer: 8 files scanned, 4 re-read, findings written
  security_reviewer: 8 files scanned, 4 re-read, findings written
  architecture_reviewer: 8 files scanned, 4 re-read, findings written
  synthesizer: 3 findings read, context re-read (12 issues total)

  CCSStore Benchmark Summary
  ──────────────────────────────────────
  Baseline tokens (no cache):     44702
  CCSStore tokens:                27882
  Tokens saved:                   16820
  Token reduction:                37.6%
  Cache hit rate:                35.3%  (51 get ops)
```

Saving 16,820 tokens at $3/MTok = **$0.050 per run**. At 1,000 runs/day: **$18K/year** on one codebase-review workload.

> **Baseline:** tokens you would pay if every agent re-read every shared artifact from scratch — equivalent to a graph without cross-agent caching. This is what `InMemoryStore` effectively does.

- 🔧 [User guide](docs/guide.md) — installation, strategies, observability, telemetry, examples, full API reference
- 🩺 [`ccs-diagnose` CLI](#ccs-diagnose--detect-stale-reads-in-your-graph) — find divergent reads in your existing LangGraph graph without changing any code
- 📊 [Real benchmarks](#real-workload-benchmarks) — measured on actual LangGraph graphs
- 🔍 [Why coherence matters](docs/why-coherence-matters.md) — the gap across LangGraph, CrewAI, AutoGen, and Claude Agent SDK, with citations
- 🛠 [Command-line tools](#command-line-tools) — full list of bundled CLIs
- 🔐 [Security & supply chain](docs/SECURITY.md) — kill switches, hash-pinned install, attestation verification, threat model
- 📄 [Paper on arXiv (2603.15183)](https://arxiv.org/abs/2603.15183) — formal protocol, TLA+ verification, simulation results

---

## How it works

Each shared artifact is cached locally per agent and reads serve from the local cache when that copy is fresh. Writes commit to a coordinator, which sends lightweight invalidation signals (~12 tokens) to peers so the next read fetches the new version instead of rebroadcasting the full artifact. Consistency is single-writer-multiple-reader per artifact with bounded staleness — peers re-fetch on next read.

Five synchronization strategies ship out of the box: `lazy` (default), `eager`, `lease` (TTL-based), `access_count`, and `broadcast`. Pick the one that matches your workload's read/write ratio and freshness needs; see the [strategies table](docs/guide.md#strategies) for guidance.

## Quick start

**Namespace convention.** `namespace[0]` is the agent identity; `namespace[1:]` is the artifact scope. Two agents writing to `("planner", "shared")` and `("reviewer", "shared")` address the same artifact.

```python
from ccs.adapters import CCSStore

store = CCSStore(strategy="lazy")

# planner writes
store.put(("planner", "shared"), "plan", {"step": 1})

# reviewer reads — same artifact, version 1
store.get(("reviewer", "shared"), "plan")
```

**Token-savings telemetry.** Pass `benchmark=True` to measure savings on your own graph, or `on_metric=callback` for per-operation events. Pass `telemetry="opentelemetry"` or `"langsmith"` to forward into your existing observability stack.

```python
store = CCSStore(strategy="lazy", benchmark=True)
# ... run your graph ...
store.print_benchmark_summary()
```

**Crash recovery.** When an agent crashes (OOM-kill, segfault) or livelocks holding a write grant, the coordinator reclaims it on a heartbeat-based sweep so other agents can proceed:

```python
from ccs.adapters import CCSStore
from ccs.coordinator.service import CrashRecoveryConfig

store = CCSStore(
    strategy="lazy",
    crash_recovery=CrashRecoveryConfig(
        enabled=True,
        heartbeat_timeout_ticks=10,
        max_hold_ticks=1000,
    ),
)

# Heartbeats piggyback on every read/write/batch automatically.
# After a process restart, call recover() to flush stale cache:
store.recover(agent_name="planner", now_tick=current_tick)
```

The same `crash_recovery=` kwarg works on `LangGraphAdapter`, `CrewAIAdapter`, `AutoGenAdapter`, and `CoherenceAdapterCore`. Default is `enabled=False`, opt-in for now.

**CrewAI and AutoGen.** The same protocol ships behind framework-specific adapters — no separate library, no separate concepts:

```python
# CrewAI
from ccs.adapters.crewai import CrewAIAdapter
adapter = CrewAIAdapter(strategy_name="lazy")

# AutoGen
from ccs.adapters.autogen import AutoGenAdapter
adapter = AutoGenAdapter(strategy_name="lazy")

# Anything else — the framework-agnostic core
from ccs.adapters.base import CoherenceAdapterCore
adapter = CoherenceAdapterCore(strategy_name="lazy")
```

All four adapters share the same `register_agent`, `register_artifact`, `before_node`, `commit_outputs`, `heartbeat`, and `recover` surface. See [docs/guide.md#low-level-adapter-api](docs/guide.md#low-level-adapter-api) for the full API.

See [docs/guide.md](docs/guide.md) for the full guide: namespace convention, strategies, observability, state transitions log, content audit log, crash recovery, telemetry, graceful degradation, examples, and API reference.

## `ccs-diagnose` — detect stale reads in your graph

Find divergent reads in your existing LangGraph graph without changing any code. `ccs-diagnose` attaches a passive callback to your graph, classifies its write pattern (`single_writer` / `shared_artifact` / `parallel_branch` / `mixed`), and reports artifacts whose reads were handed divergent versions across nodes.

```bash
pip install "agent-coherence[diagnose]"

ccs-diagnose --graph path/to/your_graph.py:build_graph
```

The factory must accept zero arguments (or accept an initial state via `--state-file`) and return a compiled LangGraph graph. The CLI runs the graph once with a passive observer and writes two reports:

| Output | Default path | Purpose |
|---|---|---|
| HTML report | `diagnose_report.html` | Human-readable: classification verdict, divergence witnesses, tracked-artifacts panel, cost extrapolation, CTA |
| JSON report | `diagnose_report.json` | Machine-readable: full schema for piping into `jq`, dashboards, or `--show-payload` re-rendering |

### Usage

```bash
# Basic — run the pipeline and write both reports
ccs-diagnose --graph my_graph.py:build_graph

# Add cost extrapolation (interactions per hour)
ccs-diagnose --graph my_graph.py:build_graph --volume 50

# Pipe JSON straight into jq (summary goes to stderr)
ccs-diagnose --graph my_graph.py:build_graph --output-json - | jq .verdict

# Re-render a previously emitted report without re-running the graph
ccs-diagnose --show-payload diagnose_report.json

# Strict mode — promote sequential-staleness exclusions back into the headline
ccs-diagnose --graph my_graph.py:build_graph --strict

# Restrict tracked keys
ccs-diagnose --graph my_graph.py:build_graph --ignore tmp_buf,debug_trace --track shared_doc
```

### Common flags

| Flag | Purpose |
|---|---|
| `--graph PATH:FUNCTION` | Path + factory name, e.g. `my_graph.py:build_graph` |
| `--state-file PATH` | JSON or YAML file passed to `graph.invoke()` |
| `--output-html PATH` | HTML output (default: `diagnose_report.html`) |
| `--output-json PATH` | JSON output (default: `diagnose_report.json`; `-` for stdout) |
| `--no-json` | Suppress JSON output |
| `--volume N` | Interactions per hour for annualized cost extrapolation |
| `--lead-pain-type {cost,auditability,auto}` | Which secondary KPI rides the headline |
| `--cost-per-1k-tokens FLOAT` | Token cost assumption for extrapolation |
| `--strict` | Promote sequential-staleness exclusions back into the headline count |
| `--ignore KEY1,KEY2` | State keys to skip (supports `__`-prefixed) |
| `--track KEY1,KEY2` | Force-track keys (wins over `--ignore`) |
| `--book-a-call-url URL` | Override CTA calendar URL (also: `CCS_DIAGNOSE_BOOK_A_CALL_URL`) |
| `--contact-email EMAIL` | Override CTA reply-to (also: `CCS_DIAGNOSE_CONTACT_EMAIL`) |
| `--show-payload PATH` | Re-render a previously emitted `report.json`; bypasses graph load |
| `--dry-run` | Print the payload that would be submitted (v0 has no network code) |
| `--no-network` / `--no-telemetry` | Suppress consent prompt and calibration write |
| `--yes` / `--non-interactive` | Skip consent prompt (required for non-TTY agent invocations) |
| `--calibration-record [PATH]` | Append payload to local JSONL calibration corpus |
| `--reset-token` | Regenerate the local installation token |

### Exit codes

| Code | Meaning |
|---|---|
| `0` | Success |
| `1` | Generic error (graph load failure, JSON parse, etc.) |
| `2` | Usage error (argparse, mutually exclusive flags, bad URL/email) |
| `3` | Dependency missing (import error from graph or extras) |
| `4` | Schema mismatch (`--show-payload` version not supported) |
| `5` | I/O error (write/read failed; oversized input file) |

### Trust posture

`ccs-diagnose` makes **zero outbound network requests** in v0 — see [docs/SECURITY.md](docs/SECURITY.md) for the full threat model and env-var kill switches (`DO_NOT_TRACK`, `DISABLE_TELEMETRY`, `CCS_DIAGNOSE_NO_TELEMETRY`). The optional calibration corpus is local-only; you choose whether to share it. The HTML renderer uses Jinja2 with `select_autoescape` and validates `book_a_call_url` / `contact_email` against an allowlist that rejects `javascript:`, `data:`, and `vbscript:` schemes.

### Calibration corpus (`ccs-diagnose --calibration-record`)

`ccs-diagnose` ships under the `langgraph-v0-preview` classifier. Submissions tagged `v0-preview` are excluded from the public benchmark — they accumulate in a local JSONL file you can share back with us if you'd like to contribute to v1 promotion.

To opt in:

```bash
ccs-diagnose --graph my_module:build_graph --calibration-record
```

This appends one entry per run to `$XDG_DATA_HOME/ccs-diagnose/calibration.jsonl` (or `~/.local/share/ccs-diagnose/calibration.jsonl` if `XDG_DATA_HOME` is unset). Override the path: `--calibration-record /tmp/my_calibration.jsonl`.

Each entry contains:

- Stack name and version (e.g., `LangGraph 1.1.10`)
- Classifier verdict + confidence
- Coverage shape (counts only — no key names, no content, no hashes)
- Timestamp
- Schema version + sequence number + instance ID

Nothing else. The file is `validate_log`-compatible — verify before sharing:

```bash
python -c "from ccs.validation import validate_log; \
    print(validate_log('$XDG_DATA_HOME/ccs-diagnose/calibration.jsonl', \
                       schema_version='ccs.diagnose.v0-preview'))"
```

A clean file returns `([], [])` — empty gap and schema-mismatch lists.

The append is gated by consent: runs without granted consent (or with `DO_NOT_TRACK` / `DISABLE_TELEMETRY` / `CCS_DIAGNOSE_NO_TELEMETRY` set, or `--no-telemetry` / `--no-network` passed) print a skip message and exit `0` without touching the file.

#### Promotion to `langgraph-v1`

The `v0-preview` → `v1` promotion gate requires:

1. **>= 5 real production graphs** validated across **>= 3 distinct supervisor topologies**
2. The Tracked-Artifacts panel produces **zero unknown `__`-prefix surprises** on a current LangGraph release
3. The append-only prefix-stability rule survives a workload that **compacts message history mid-run** (real-world `trim_messages` pattern)

Once promoted, `v1`-tagged submissions populate the public benchmark; `v0-preview` calibration data is not retroactively migrated.

#### Sharing calibration data

DM `vlad@fwdinc.net` (or open an issue on the repo) with your `calibration.jsonl` contents. We do not collect anything we haven't documented above; you can read every field before sharing.

## Command-line tools

All bundled CLIs are installed as console scripts when you `pip install agent-coherence`:

| Command | Extra needed | What it does |
|---|---|---|
| `ccs-diagnose` | `[diagnose]` | Detect stale reads / divergent versions in a LangGraph graph |
| `ccs-benchmark` | `[langgraph,benchmark]` | Measure token savings of CCSStore on your own LangGraph graph |
| `ccs-simulate` | — | Run a protocol-only simulation scenario from a YAML file |
| `ccs-compare` | — | Compare two or more strategies on the same scenario |
| `ccs-check-architecture` | — | Verify the four-layer architecture boundary (used in CI) |
| `ccs-check-release` | — | Verify PyPI Trusted Publishers + branch/tag protection before a `v*` tag push |

Run any command with `--help` for full option lists.

## Real-workload benchmarks

Measured on real LangGraph `StateGraph` executions using `GenericFakeChatModel` with no live LLM API calls, so the results are reproducible in CI. Run them yourself:

```bash
pip install "agent-coherence[langgraph,benchmark]"
make benchmark    # runs all three workloads, prints consolidated table
```

Or run individually:

```bash
python benchmarks/langgraph_real/bench_planner.py
python benchmarks/langgraph_real/bench_code_review.py
python benchmarks/langgraph_real/bench_high_churn.py
```

Savings scale with read/write ratio:

| Workload | Agents | Reads:Writes | Hit rate | Baseline tokens | CCSStore tokens | Savings |
|---|---|---|---|---|---|---|
| Planning (read-heavy) | 4 | 12:1 | 75% | 4,160 | 1,301 | **69%** |
| Code review (moderate) | 3 | 8:3 | 60% | 5,320 | 2,835 | **47%** |
| High-churn (write-heavy) | 4 | 8:4 | 50% | 3,250 | 2,317 | **29%** |

For protocol-only simulation methodology and reproduction instructions, see [docs/REPRODUCE.md](docs/REPRODUCE.md).

### Benchmark your own workload

```bash
pip install "agent-coherence[langgraph,benchmark]"
ccs-benchmark --graph path/to/your_graph.py:build_graph
```

The factory must accept a single `store` argument and return a compiled LangGraph graph (`builder.compile(store=store)`). The CLI runs the graph once and prints a token savings summary. Use `--initial-state '{"key": "value"}'` to pass a custom input dict.

## Architecture

- **Protocol** (`ccs.core`, `ccs.strategies`) — coherence state machine and synchronization strategies; no framework dependencies.
- **Coordinator** (`ccs.coordinator`) — authority service tracking directory state, publishing invalidations, and reclaiming stale grants (crash recovery).
- **Adapters** (`ccs.adapters`) — framework integrations for LangGraph, CrewAI, and AutoGen; ~100 lines each. Each adapter exposes `heartbeat()` and `recover()` for crash-recovery liveness.
- **Simulation** (`ccs.simulation`) — deterministic tick-driven engine for scenario benchmarks with failure injection (kill, busy, restore).
- **Event bus** (`ccs.bus`) — pluggable transport for invalidation signals; in-memory by default, swap in Redis, Kafka, NATS, or gRPC streams for production.

## Formal verification

Protocol safety properties (single-writer, monotonic versioning, crash-recovery sweep invariants) are model-checked with [TLA+/TLC](formal/tla/README.md). The `tla-check` CI job runs TLC on every push and PR.

## Status

`v0.6` released. See [releases](https://github.com/hipvlady/agent-coherence/releases) for full history. Alpha — APIs may change before `v1.0`.

**Next release (preview on `dev`) — `ccs-diagnose` CLI.** A passive, zero-network stale-read detector for existing LangGraph graphs. Classifies write pattern, surfaces divergent reads, ships under the `langgraph-v0-preview` classifier with a public-benchmark promotion gate. Install with `pip install "agent-coherence[diagnose]"`. See [`ccs-diagnose`](#ccs-diagnose--detect-stale-reads-in-your-graph) above for usage.

**What's new in v0.6 — crash recovery for stale grants.**
When an agent crashes (OOM-kill, segfault) or livelocks, its `MODIFIED` or `EXCLUSIVE` grant blocks every other agent from writing the same artifact. v0.6 reclaims those grants automatically: piggyback heartbeats on every read/write, an `enforce_stable_grant_timeouts` sweep on the coordinator, and a `recover()` primitive on every adapter for post-restart cache invalidation. Two reclaim triggers — `reclaim_heartbeat` (holder went silent) and `reclaim_max_hold` (held too long regardless of liveness) — surface in the state log so production incidents leave a trail. Composition fail-fast: `lease` strategy + crash recovery requires `max_hold_ticks > lease_ttl_ticks` or it raises at startup. Behind feature flag (`CrashRecoveryConfig(enabled=False)` default) for now; flip is the next deliberate release after dogfood validation. Every framework adapter — LangGraph, CrewAI, AutoGen, and `CCSStore` — accepts `crash_recovery=CrashRecoveryConfig(...)` and exposes `heartbeat()` / `recover()`.

**v0.5 — per-agent content audit log.** Opt-in `content_audit_log=callback` records every content delivery (cache hit, fetch, broadcast, write, search) with SHA-256 hashes, gap-free sequence numbers, and `instance_id` cross-validated against the state log. Pairs with v0.4's `state_log` to give debuggers a complete picture: state transitions × content delivered.

**v0.4 — sequence-numbered event stream.** `sequence_number`, `instance_id`, `schema_version` on every state-log entry. `ccs.validation.validate_log` helper for gap and schema-drift detection.

**v0.3 — state transitions log + reproducible benchmark harness.** Opt-in JSONL stream of every stable MESI state transition. `make benchmark` harness with committed baseline (`benchmarks/expected.json`).

**v0.2 — inline benchmark + telemetry + degradation visibility.** `benchmark=True`, `print_benchmark_summary()`, `CoherenceDegradedWarning`, OTel and LangSmith adapters, graceful degradation via `on_error="degrade"`.

**v0.1 — initial release.** MESI-style cache coherence for shared artifacts in multi-agent LLM systems.

## Security & supply chain

`agent-coherence` ships with PyPI Trusted Publishers OIDC, PEP 740 attestations, hash-pinned dependency lockfiles, and a documented threat model. See [docs/SECURITY.md](docs/SECURITY.md) for the full trust contract, env-var kill switches, and the canonical install command.

For security-sensitive installs:

    pip install --require-hashes -r requirements-diagnose.txt

Reset the calibration consent token any time:

    ccs-diagnose --reset-token

Report security issues via a [private GitHub security advisory](https://github.com/hipvlady/agent-coherence/security/advisories/new).

## Paper

**Token Coherence: Adapting MESI Cache Protocols to Minimize
Synchronization Overhead in Multi-Agent LLM Systems**
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

Debugging multi-agent failures often comes down to which agent saw what state when. `CCSStore(content_audit_log=my_callback)` records every content delivery — cache hits, fetches, broadcasts, writes, and searches — with SHA-256 hashes and gap-free sequence numbers. The state log tracks MESI transitions; the audit log tracks what content each agent actually saw. If you've hit a stale-read bug in a multi-agent workflow, I'd like to hear about it — [open an issue](https://github.com/hipvlady/agent-coherence/issues/new).

## Community

Questions, war stories, and ideas welcome in [Discussions](https://github.com/hipvlady/agent-coherence/discussions).

## License

Apache-2.0. See [LICENSE](LICENSE).
