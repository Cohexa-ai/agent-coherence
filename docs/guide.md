# agent-coherence User Guide

When two agents share state, one of them is usually reading a stale copy.
This guide shows how to drop in `agent-coherence` — surfacing those reads
and serving fresh artifacts on demand, with a one-line import change.

`agent-coherence` is **vendor-neutral by design**: the same protocol and
the same library work across LangGraph, CrewAI, AutoGen, and any custom
orchestrator, with any model provider (Anthropic, OpenAI, Google, Mistral,
open-source). Pick the integration extra that matches your stack; the
concepts below apply uniformly.

Below: installation, namespace convention, sync strategies, observability,
telemetry, graceful degradation, examples, the `ccs-diagnose` CLI, the
full command-line toolset, and the API reference.

---

## Contents

1. [Installation](#installation)
2. [Quick start](#quick-start)
3. [Namespace convention](#namespace-convention)
4. [Strategies](#strategies)
5. [Observability](#observability)
6. [State transitions log](#state-transitions-log)
7. [Content audit log](#content-audit-log)
8. [Crash recovery](#crash-recovery)
9. [Inline benchmark mode](#inline-benchmark-mode)
10. [Telemetry](#telemetry)
11. [Graceful degradation](#graceful-degradation)
12. [Examples](#examples)
13. [Real-workload benchmarks](#real-workload-benchmarks)
14. [Benchmarking your own workload](#benchmarking-your-own-workload)
15. [`ccs-diagnose` — detect stale reads](#ccs-diagnose--detect-stale-reads)
16. [Command-line tools](#command-line-tools)
17. [API reference](#api-reference)
18. [Low-level adapter API](#low-level-adapter-api)
19. [CrewAI and AutoGen adapters](#crewai-and-autogen-adapters)

---

## Installation

Pick the integration extra that matches your stack. The library is the same across all of them — only the adapter surface changes.

```bash
# LangGraph (drop-in CCSStore)
pip install "agent-coherence[langgraph]"

# CrewAI adapter
pip install "agent-coherence[crewai]"

# ccs-diagnose CLI (stale-read detector for LangGraph graphs)
pip install "agent-coherence[diagnose]"

# With OpenTelemetry metrics
pip install "agent-coherence[langgraph,otel]"

# With LangSmith tracing
pip install "agent-coherence[langgraph,langsmith]"

# Everything (langgraph + crewai + otel + langsmith + benchmark + diagnose)
pip install "agent-coherence[all]"
```

For security-sensitive installs with full transitive hash pinning, see [SECURITY.md](SECURITY.md#hash-pinned-install-for-security-sensitive-users) and the bundled `requirements-diagnose.txt`.

---

## Quick start

```python
# Before
from langgraph.store.memory import InMemoryStore
store = InMemoryStore()

# After — one import change, no node code changes
from ccs.adapters import CCSStore
store = CCSStore(strategy="lazy")

graph = builder.compile(store=store)
```

Node code stays identical — `store.get()`, `store.put()`, and `store.search()` all
work the same way.

---

## Namespace convention

CCSStore overloads the `namespace` tuple that LangGraph passes to `get` and `put`:

| Position | Meaning | Example |
|----------|---------|---------|
| `namespace[0]` | Agent identity | `"planner"`, `"reviewer"` |
| `namespace[1:]` | Artifact scope | `("shared",)`, `("project", "v2")` |

**Two agents share an artifact when their scopes match:**

```python
# Both address the same "codebase" artifact
store.put(("reviewer_a", "shared"), "codebase", {...})
store.get(("reviewer_b", "shared"), "codebase")  # reads what reviewer_a wrote
```

**Agent-private artifacts:** include the agent name in the scope.

```python
store.put(("planner", "planner", "scratch"), "draft", {...})
# scope is ("planner", "scratch") — other agents cannot see this key
```

This convention is required. Namespaces with fewer than two elements raise
`ValueError`.

---

## Strategies

Pass `strategy=` to `CCSStore(...)` to control when invalidated entries are
re-fetched.

| Strategy | Behaviour | Best for |
|----------|-----------|----------|
| `"lazy"` *(default)* | Fetch on next read after invalidation | Most workloads |
| `"eager"` | Pre-fetch as soon as an invalidation signal arrives | Low-latency reads |
| `"lease"` | Entries expire after a TTL regardless of writes | Time-sensitive data |
| `"access_count"` | Fetch on every N-th access | High-read, low-write |
| `"broadcast"` | Always fetch — no local caching | Debugging, correctness testing |

Strategy-specific kwargs are forwarded directly:

```python
store = CCSStore(strategy="lease", lease_ticks=10)
store = CCSStore(strategy="access_count", threshold=3)
```

---

## Observability

Pass `on_metric` to receive a `StoreMetricEvent` after every operation:

```python
from ccs.adapters import CCSStore, StoreMetricEvent

events: list[StoreMetricEvent] = []
store = CCSStore(strategy="lazy", on_metric=events.append)

# ... run your graph ...

hits   = [e for e in events if e.operation == "get" and e.cache_hit]
misses = [e for e in events if e.operation == "get" and not e.cache_hit]
saved  = sum(e.tokens_consumed for e in misses) - len(hits)  # rough savings
```

### `StoreMetricEvent` fields

| Field | Type | Description |
|-------|------|-------------|
| `operation` | `str` | `"get"`, `"put"`, `"search.hit"`, or `"degraded"` |
| `namespace` | `tuple[str, ...]` | Full namespace including agent name |
| `key` | `str` | Artifact key |
| `agent_name` | `str` | First element of `namespace` |
| `tokens_consumed` | `int` | `1` on cache hit; estimated content size on miss |
| `cache_hit` | `bool` | `True` when served from local cache |
| `tick` | `int` | Logical clock at the time of the operation |

Token estimation: `max(1, len(json.dumps(value)) // 4)`. Override by including
`"__ccs_size_tokens__": N` in your artifact value.

---

## State transitions log

Pass `state_log` to receive a structured dict for every stable MESI state transition.
Intended for external tools — debuggers, visualizers, audit pipelines — that need to
correlate agent behavior with coherence state changes without coupling to CCS internals.

```python
import json

log = []
store = CCSStore(strategy="lazy", state_log=log.append)

# ... run your graph ...

# Write JSONL
with open("transitions.jsonl", "w") as f:
    for entry in log:
        f.write(json.dumps(entry) + "\n")
```

### Log entry schema

Each entry is a flat `dict` with exactly these eight keys:

| Field | Type | Description |
|-------|------|-------------|
| `tick` | `int` | Monotonic operation counter within this `CCSStore` session |
| `artifact_id` | `str` | UUID of the artifact whose per-agent state changed |
| `agent_id` | `str` | UUID of the agent whose state changed |
| `agent_name` | `str \| None` | Agent display name (resolved from `namespace[0]`); `None` for low-level registry callers |
| `from_state` | `str` | Previous state: `"MODIFIED"`, `"EXCLUSIVE"`, `"SHARED"`, or `"INVALID"` |
| `to_state` | `str` | New state after the transition |
| `trigger` | `str` | Coordinator operation that caused the transition (see table below) |
| `version` | `int` | Artifact version number at the moment of the transition |

### Trigger vocabulary

| `trigger` | Fires when |
|-----------|-----------|
| `"register"` | Initial artifact registration; registering agent receives EXCLUSIVE |
| `"fetch"` | Fetch grant; agent transitions to SHARED or EXCLUSIVE |
| `"write"` | Write request; peers are invalidated (→ INVALID), requester receives EXCLUSIVE |
| `"commit"` | Write commit; peers are invalidated (→ INVALID), committer transitions to MODIFIED |
| `"invalidate"` | Explicit invalidation signal; agent transitions to INVALID |
| `"timeout"` | Transient state timeout; agent force-invalidated (→ INVALID) |
| `"reclaim_heartbeat"` | Crash recovery: agent's heartbeat older than `heartbeat_timeout_ticks` |
| `"reclaim_max_hold"` | Crash recovery: grant held for at least `max_hold_ticks` |

### Error handling

The callback is called synchronously on the critical path. An exception in `state_log`
propagates out of the coordinator operation and may leave the log incomplete for that
batch. Provide a callback that catches its own exceptions for production use:

```python
def safe_log(entry: dict) -> None:
    try:
        emit_to_pipeline(entry)
    except Exception:
        logger.exception("state_log callback failed")

store = CCSStore(strategy="lazy", state_log=safe_log)
```

`state_log=None` (default) adds no overhead — the guard is a single `is not None` check.

### Log validation

Verify a materialized JSONL log for gaps and schema drift:

```python
from ccs.validation import validate_log, CCS_STATE_LOG_SCHEMA_VERSION

gaps, mismatches = validate_log(
    "transitions.jsonl",
    schema_version=CCS_STATE_LOG_SCHEMA_VERSION,
)
# gaps: list of dropped-event positions; mismatches: list of schema version changes
# returns ([], []) on a clean log
```

`validate_log` is stdlib-only and importable independently of the CCS runtime, so log
consumers (audit pipelines, replay tools) can verify materialized logs without taking
on the rest of the CCS dependency surface.

---

## Content audit log

Pass `content_audit_log` to record every content delivery — what each agent actually saw,
when, and from which source. While `state_log` tracks MESI state transitions, the audit
log tracks content flow: cache hits, fetches, broadcasts, writes, and searches.

```python
audit = []
store = CCSStore(strategy="lazy", content_audit_log=audit.append)

# ... run your graph ...

# Each entry records one content delivery
for entry in audit:
    print(f"{entry['agent_name']} saw artifact {entry['artifact_id']} "
          f"via {entry['source']} (v{entry['version']})")
```

Enabling `content_audit_log` also enables version retention — the registry keeps a copy
of each artifact version so historical content can be retrieved for replay or debugging.

### Audit entry schema

| Field | Type | Description |
|-------|------|-------------|
| `tick` | `int` | Monotonic operation counter |
| `agent_id` | `str \| None` | UUID of the receiving agent; `None` for search records |
| `agent_name` | `str \| None` | Agent display name; `None` for search records |
| `artifact_id` | `str` | UUID of the artifact |
| `version` | `int \| None` | Artifact version at delivery; `None` on error |
| `content_hash` | `str \| None` | SHA-256 of the delivered content; `None` on error |
| `source` | `str` | `"cache_hit"`, `"fetch"`, `"broadcast"`, `"write"`, or `"search"` |
| `outcome` | `str` | `"content"`, `"empty"`, or `"error"` |
| `sequence_number` | `int` | Gap-free counter shared across all agents and sources |
| `instance_id` | `str` | Session identifier; matches `state_log` entries |
| `schema_version` | `str` | `"ccs.content_audit.v1"` |

### Source types

| `source` | Fires when |
|----------|-----------|
| `"cache_hit"` | Agent reads from its local cache (no coordinator round-trip) |
| `"fetch"` | Agent fetches from the coordinator (cache miss or refresh) |
| `"broadcast"` | Agent receives content pushed by a peer write (broadcast strategy) |
| `"write"` | Agent commits new content |
| `"search"` | Content returned via `SearchOp`; agent identity unknown |

### Cross-validation with state log

When both `content_audit_log` and `state_log` are enabled, `instance_id` is shared and
`content_hash` on write audit entries matches the corresponding state log commit entry.

---

## Crash recovery

When an agent crashes (OOM-kill, segfault) or livelocks (holds a grant indefinitely),
its `MODIFIED` or `EXCLUSIVE` grant blocks all other agents from writing to that artifact.
The crash-recovery extension reclaims stale grants automatically.

### Enabling

```python
from ccs.coordinator.service import CrashRecoveryConfig

store = CCSStore(
    strategy="lazy",
    crash_recovery=CrashRecoveryConfig(
        enabled=True,
        heartbeat_timeout_ticks=10,
        max_hold_ticks=1000,
    ),
)
```

The same `crash_recovery=` kwarg works on `LangGraphAdapter`, `CrewAIAdapter`,
`AutoGenAdapter`, and `CoherenceAdapterCore`.

### `CrashRecoveryConfig` fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | `bool` | `False` | Master switch. When `False`, `heartbeat()` and `recover()` are silent no-ops; the per-tick sweep does not run. |
| `heartbeat_timeout_ticks` | `int` | `10` | Reclaim a holder's grant if the gap between `now_tick` and the holder's last heartbeat is `>= heartbeat_timeout_ticks`. |
| `max_hold_ticks` | `int` | `1000` | Reclaim a holder's grant if it has been continuously held in `MODIFIED`/`EXCLUSIVE` for `>= max_hold_ticks`, regardless of heartbeat freshness. Bound the worst-case lock duration. |

**Tick semantics.** Ticks are a logical clock — the unit is whatever `now_tick` your application advances. For LangGraph, one node invocation per tick is a sensible default. For long-running tool calls or LLM calls, advance ticks at the granularity at which you can call `heartbeat()` or expect grants to be released.

### How it works

1. **Piggyback heartbeats.** Every `read()` / `write()` / `batch()` call automatically
   records a heartbeat for the calling agent. No application code change needed.
2. **Explicit heartbeat.** For long compute windows (LLM calls, blocking I/O) where no
   adapter method is invoked, call `heartbeat()` to signal liveness:
   ```python
   store.heartbeat(agent_name="planner", now_tick=current_tick)
   ```
3. **Reclamation sweep.** The coordinator reclaims any M/E grant whose holder either:
   - has not heartbeated within `heartbeat_timeout_ticks`, or
   - has held the grant for at least `max_hold_ticks` (regardless of heartbeat).
4. **Recovery after restart.** After a process restart or checkpoint reload, call
   `recover()` to invalidate the agent's stale local cache and re-seed its heartbeat:
   ```python
   store.recover(agent_name="planner", now_tick=current_tick)
   ```

### Cadence guidance

Call `heartbeat()` at least every `heartbeat_timeout_ticks / 3` ticks during long
compute windows. Per-step adapter methods (e.g., `before_node`, `batch`) already
heartbeat automatically.

### Composition rule

When using the `lease` strategy with crash recovery enabled, `max_hold_ticks` must
exceed `lease_ttl_ticks`. Equal or smaller values raise `ValueError` at startup.

### Flag-off behavior

With `enabled=False` (the default), `heartbeat()` and `recover()` are silent no-ops.
State-transition log output is byte-identical to v0.5. No behavior change for existing
users.

### Disabling / rollback

To turn crash recovery off, omit `crash_recovery=` (it defaults to disabled) or pass
`CrashRecoveryConfig(enabled=False)`. This is the rollback path if a default-on
release ever surfaces an issue in your workload — flip the kwarg back to disabled
and the sweep stops immediately. No data migration, no protocol incompatibility:
the protocol behavior with `enabled=False` is byte-identical to a build without
crash recovery.

If you've enabled crash recovery and want to verify it isn't reclaiming benign
holders, watch the state-transition log for `reclaim_heartbeat` and
`reclaim_max_hold` triggers. If they fire on agents that were healthy, raise
`heartbeat_timeout_ticks` or `max_hold_ticks` rather than disabling the feature.

### Reference

For the formal protocol model (TLA+/TLC) covering single-writer, monotonic
versioning, and crash-recovery sweep invariants, see
[`formal/tla/README.md`](../formal/tla/README.md).

---

## Inline benchmark mode

Measure token savings on your own workload without any external tooling:

```python
store = CCSStore(strategy="lazy", benchmark=True)

# ... run your graph ...

store.print_benchmark_summary()
```

`benchmark=False` (default) adds zero overhead — no counters are allocated.

For programmatic access, use `benchmark_summary()`:

```python
summary = store.benchmark_summary()
# {
#   "baseline_tokens": 4160,
#   "ccs_tokens": 1301,
#   "tokens_saved": 2859,
#   "token_reduction_pct": 68.7,
#   "cache_hit_rate": 0.75,
#   "n_operations": 16,
# }
```

`benchmark_summary()` raises `RuntimeError` if the store was not created with
`benchmark=True`.

---

## Telemetry

Structured metrics without changing node code.

### OpenTelemetry

```bash
pip install "agent-coherence[otel]"
```

```python
store = CCSStore(strategy="lazy", telemetry="opentelemetry")
```

CCSStore creates two Counter instruments on the globally-configured
`MeterProvider`:

| Instrument | Unit | Attributes |
|------------|------|------------|
| `ccs.store.operations` | `{operation}` | `ccs.operation`, `ccs.agent_name`, `ccs.cache_hit` |
| `ccs.store.tokens_consumed` | `{token}` | `ccs.operation`, `ccs.agent_name`, `ccs.cache_hit` |

If no SDK is configured, the OTel no-op provider discards everything at zero cost.

To use a specific provider instead of the global one:

```python
from ccs.adapters.telemetry.otel import OtelExporter
store = CCSStore(strategy="lazy", telemetry=OtelExporter(meter_provider=my_provider))
```

### LangSmith

```bash
pip install "agent-coherence[langsmith]"
```

```python
store = CCSStore(strategy="lazy", telemetry="langsmith")
```

Per-operation metadata is attached to the active LangSmith run tree via
`run.add_metadata(...)`. Keys attached to each event:

```
ccs.operation, ccs.agent_name, ccs.tokens_consumed, ccs.cache_hit, ccs.tick
```

If no LangSmith run is active, events are silently discarded.

### Custom exporter

```python
from ccs.adapters import TelemetryExporter, StoreMetricEvent

class DatadogExporter(TelemetryExporter):
    def on_event(self, event: StoreMetricEvent) -> None:
        statsd.increment("ccs.operations", tags=[f"agent:{event.agent_name}"])
        statsd.histogram("ccs.tokens", event.tokens_consumed)

store = CCSStore(strategy="lazy", telemetry=DatadogExporter())
```

`on_metric` and `telemetry` are independent — both fire for every event if both
are set.

---

## Graceful degradation

By default (`on_error="strict"`), a `CoherenceError` propagates and the graph
fails. Use `on_error="degrade"` to keep the graph running when the coherence
layer encounters an unexpected state:

```python
store = CCSStore(strategy="lazy", on_error="degrade")
```

In degrade mode:

- **`put`**: if `core.write` raises, the value is stored in a plain dict fallback
  and a `"degraded"` operation event is emitted.
- **`get`**: if `core.read` raises, the value is retrieved from the fallback dict
  (empty dict if nothing was previously stored there) and a `"degraded"` event
  is emitted.

A warning is logged at `WARNING` level for each degraded operation. Monitor
degradations via `on_metric`:

```python
events = []
store = CCSStore(strategy="lazy", on_error="degrade", on_metric=events.append)

# ... run graph ...

degraded = [e for e in events if e.operation == "degraded"]
if degraded:
    alert(f"{len(degraded)} degraded operations detected")
```

Use `on_error="strict"` (the default) in development and CI. Consider
`on_error="degrade"` in production environments where a coherence bug should not
take down the whole graph.

Two attributes let you check degradation state after the fact:

```python
store.is_degraded       # True after the first degraded operation
store.degradation_count  # total number of degraded operations
```

Use these to gate alerts or health checks without keeping a separate event list.

---

## Examples

All examples are runnable with `python -m examples.<name>.main` from the project root.

| Example | Command | What it shows |
|---------|---------|---------------|
| LangGraph planner | `python -m examples.langgraph_planner.main` | 4-agent, 1 artifact, 75% hit rate |
| Code review pipeline | `python -m examples.code_review.main` | 3-agent, SHARED state transitions |
| Research pipeline | `python -m examples.research_pipeline.main` | 4-agent, 3 artifacts, 60% hit rate |
| Shared codebase | `python -m examples.shared_codebase.main` | 4-agent code review, 37.6% savings, benchmark output |

### Code review pipeline

Three agents share a codebase artifact. The key behavior: `reviewer_b` reads the
same codebase that `reviewer_a` cached without either agent invalidating it, because
neither wrote to it. Both hold it in SHARED state simultaneously.

### Research pipeline

Four agents operate on three artifacts (`brief`, `findings`, `analysis`). The key
behavior: `researcher`'s write to `findings` does **not** invalidate `brief` held
by `analyst` — each artifact key has its own independent MESI state per agent.

---

## Real-workload benchmarks

Results from real LangGraph graph executions using `GenericFakeChatModel` (no live
LLM calls). Run them yourself:

```bash
pip install "agent-coherence[langgraph,benchmark]"
make benchmark    # all three workloads, prints consolidated table
```

Or run individually:

```bash
python benchmarks/langgraph_real/bench_planner.py
python benchmarks/langgraph_real/bench_code_review.py
python benchmarks/langgraph_real/bench_high_churn.py
```

| Workload | Agents | Hit rate | Baseline | CCSStore | Savings |
|----------|--------|----------|----------|----------|---------|
| Planning (read-heavy) | 4 | 75% | 4,160 | 1,301 | 69% |
| Code review (write-moderate) | 3 | 60% | 5,320 | 2,835 | 47% |
| High-churn (write-heavy) | 4 | 50% | 3,250 | 2,317 | 29% |

*Tokens are approximate; real LLM content will vary.*

Hit rate and savings are lower-bounded by write frequency: more writes mean more
invalidations, more misses. The planning workload has 1 write and 12 reads (75% hit
rate). The high-churn workload has 4 writes and 8 reads (50% hit rate).

For the simulation-based results from the paper (84–95% savings), see
[REPRODUCE.md](REPRODUCE.md).

---

## Benchmarking your own workload

```bash
pip install "agent-coherence[langgraph,benchmark]"
ccs-benchmark --graph path/to/my_graph.py:build_graph
```

The factory function must accept a single `store` argument and return a compiled
LangGraph graph:

```python
def build_graph(store):
    builder = StateGraph(...)
    # ... add nodes/edges ...
    return builder.compile(store=store)
```

Pass a custom input state with `--initial-state`:

```bash
ccs-benchmark --graph my_graph.py:build_graph --initial-state '{"query": "hello"}'
```

The CLI runs the graph once and prints `print_benchmark_summary()` output. For
inline benchmarking without the CLI, see [Inline benchmark mode](#inline-benchmark-mode).

---

## `ccs-diagnose` — detect stale reads

A standalone CLI for detecting divergent reads in an existing LangGraph graph without changing any code. Passive callback, zero outbound network in v0, HTML + JSON reports. Install with `pip install "agent-coherence[diagnose]"`.

See [docs/ccs-diagnose.md](ccs-diagnose.md) for the full reference: usage, flags, exit codes, trust posture, calibration corpus, and the `langgraph-v0-preview` → `v1` promotion gate.

---

## Command-line tools

All bundled CLIs are installed as console scripts when you
`pip install agent-coherence`.

| Command | Extra needed | What it does |
|---|---|---|
| `ccs-diagnose` | `[diagnose]` | Detect stale reads / divergent versions in a LangGraph graph |
| `ccs-benchmark` | `[langgraph,benchmark]` | Measure token savings of `CCSStore` on your own LangGraph graph |
| `ccs-simulate` | — | Run a protocol-only simulation scenario from a YAML file |
| `ccs-compare` | — | Compare two or more strategies on the same scenario |
| `ccs-check-architecture` | — | Verify the four-layer architecture boundary (also runs in CI) |
| `ccs-check-release` | — | Verify PyPI Trusted Publishers + branch/tag protection before a `v*` tag push |

Run any command with `--help` for the full option list.

### `ccs-simulate` and `ccs-compare`

```bash
# Run a single strategy against a YAML scenario
ccs-simulate --scenario benchmarks/scenarios/planning_canonical.yaml --strategy lazy

# Compare two or more strategies on the same scenario
ccs-compare --scenario benchmarks/scenarios/planning_canonical.yaml --strategies eager lazy
```

YAML scenarios in `benchmarks/scenarios/` define a deterministic workload
(agent count, artifacts, write probability, network latency/loss, strategy
config). Output is a `StrategyComparisonReport` printed to stdout — useful
when you want protocol-only numbers without spinning up a real LangGraph
graph. See [REPRODUCE.md](REPRODUCE.md) for the simulation methodology behind
the paper's headline numbers.

### `ccs-check-architecture` and `ccs-check-release`

```bash
# Architecture boundary check — fails non-zero if any layer imports upward
ccs-check-architecture

# Pre-release verification — fails non-zero if PyPI Trusted Publishers,
# branch protection on main, or v* tag protection are missing
ccs-check-release
```

Both are designed for CI gating. `ccs-check-architecture` runs on every push;
`ccs-check-release` runs as a preflight job before any wheel is built for a
`v*` tag.

---

## API reference

### `CCSStore(strategy, benchmark, on_metric, telemetry, on_error, state_log, content_audit_log, crash_recovery, **strategy_kwargs)`

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `strategy` | `str` | `"lazy"` | Synchronization strategy: `"lazy"`, `"eager"`, `"lease"`, `"access_count"`, `"broadcast"` |
| `benchmark` | `bool` | `False` | Enable inline token-savings measurement; access results via `benchmark_summary()` / `print_benchmark_summary()` |
| `on_metric` | `Callable[[StoreMetricEvent], None] \| None` | `None` | Callback fired after every operation with per-op metrics |
| `telemetry` | `str \| TelemetryExporter \| None` | `None` | `"opentelemetry"`, `"langsmith"`, a `TelemetryExporter` instance, or `None` |
| `on_error` | `str` | `"strict"` | `"strict"` to propagate `CoherenceError`; `"degrade"` to fall back silently |
| `state_log` | `Callable[[dict], None] \| None` | `None` | Callback fired on every stable MESI state transition; see [State transitions log](#state-transitions-log) |
| `content_audit_log` | `Callable[[dict], None] \| None` | `None` | Callback fired on every content delivery; see [Content audit log](#content-audit-log). Enables version retention. |
| `crash_recovery` | `CrashRecoveryConfig \| None` | `None` | Crash-recovery configuration; see [Crash recovery](#crash-recovery). `None` uses `CrashRecoveryConfig(enabled=False)`. |
| `**strategy_kwargs` | `Any` | — | Forwarded to the strategy constructor (`lease_ticks`, `threshold`, etc.) |

### Public imports

```python
from ccs.adapters import (
    CCSStore,
    StoreMetricEvent,
    TelemetryExporter,
    NoOpTelemetryExporter,
    OtelExporter,
    LangSmithExporter,
    build_telemetry,
)
from ccs.coordinator.service import CrashRecoveryConfig
```

---

## Low-level adapter API

For CrewAI, AutoGen, or custom integrations, use the `before_node` / `commit_outputs`
surface directly:

```python
from ccs.adapters.langgraph import LangGraphAdapter
from ccs.coordinator.service import CrashRecoveryConfig

adapter = LangGraphAdapter(
    strategy_name="lazy",
    crash_recovery=CrashRecoveryConfig(enabled=True, heartbeat_timeout_ticks=10, max_hold_ticks=1000),
)
for name in ("planner", "researcher", "executor"):
    adapter.register_agent(name, now_tick=0)
plan = adapter.register_artifact(name="plan.md", content="v1")

context = adapter.before_node(agent_name="planner", artifact_ids=[plan.id], now_tick=1)
adapter.commit_outputs(
    agent_name="planner",
    writes={plan.id: context[plan.id]["content"] + "\nStep 1"},
    now_tick=2,
)

# During long compute — bridge the heartbeat gap
adapter.core.heartbeat(agent_name="planner", now_tick=5)

# After process restart — invalidate stale cache and re-seed heartbeat
adapter.core.recover(agent_name="planner", now_tick=100)
```

The same pattern applies to `CrewAIAdapter` and `AutoGenAdapter` — all accept
`crash_recovery=` and expose `core.heartbeat()` / `core.recover()`.

Full example: [`examples/multi_agent_planning.py`](../examples/multi_agent_planning.py).

---

## CrewAI and AutoGen adapters

The protocol is framework-agnostic; only the adapter surface changes. Both
adapters share the same `register_agent`, `register_artifact`, `before_node`,
`commit_outputs`, `heartbeat`, and `recover` API as `LangGraphAdapter`.

### CrewAI

```bash
pip install "agent-coherence[crewai]"
```

```python
from ccs.adapters.crewai import CrewAIAdapter
from ccs.coordinator.service import CrashRecoveryConfig

adapter = CrewAIAdapter(
    strategy_name="lazy",
    crash_recovery=CrashRecoveryConfig(enabled=False),
)
for name in ("researcher", "writer", "editor"):
    adapter.register_agent(name, now_tick=0)
brief = adapter.register_artifact(name="brief.md", content="initial brief")

# Read shared state at task start
ctx = adapter.before_node(agent_name="researcher", artifact_ids=[brief.id], now_tick=1)

# Write task output back
adapter.commit_outputs(
    agent_name="researcher",
    writes={brief.id: ctx[brief.id]["content"] + "\nfindings: ..."},
    now_tick=2,
)
```

### AutoGen

```bash
pip install "agent-coherence[autogen]"
```

```python
from ccs.adapters.autogen import AutoGenAdapter

adapter = AutoGenAdapter(strategy_name="lazy")
# Same register_agent / register_artifact / before_node / commit_outputs surface.
```

### Custom orchestrators

```python
from ccs.adapters.base import CoherenceAdapterCore

adapter = CoherenceAdapterCore(strategy_name="lazy")
# Same surface. Wrap whatever framework you're using and call before_node /
# commit_outputs at the natural boundaries (typically: before a tool call or
# LLM step, and after the step produces new state).
```

Crash recovery (`heartbeat` / `recover`) is identical across all four adapters.
