# agent-coherence User Guide

When two agents share state, one of them is usually reading a stale copy.
This guide shows how to drop in `agent-coherence` — surfacing those reads
and serving fresh artifacts on demand, with a one-line import change.

`agent-coherence` is **vendor-neutral by design**: the same protocol and
the same library work across LangGraph, CrewAI, AutoGen, the OpenAI Agents
SDK, and any custom orchestrator, with any model provider (Anthropic, OpenAI,
Google, Mistral, open-source). Pick the integration extra that matches your
stack; the concepts below apply uniformly.

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
9. [Version retention and read-at-version](#version-retention-and-read-at-version)
10. [Inline benchmark mode](#inline-benchmark-mode)
11. [Telemetry](#telemetry)
12. [Graceful degradation](#graceful-degradation)
13. [Examples](#examples)
14. [Real-workload benchmarks](#real-workload-benchmarks)
15. [Benchmarking your own workload](#benchmarking-your-own-workload)
16. [`ccs-diagnose` — detect stale reads](#ccs-diagnose--detect-stale-reads)
17. [Replay (v0.8.2+)](#replay-v082)
18. [Command-line tools](#command-line-tools)
19. [API reference](#api-reference)
20. [Low-level adapter API](#low-level-adapter-api)
21. [CrewAI and AutoGen adapters](#crewai-and-autogen-adapters)
22. [OpenAI Agents SDK adapter (experimental)](#openai-agents-sdk-adapter-experimental)

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

# OpenAI Agents SDK adapter (experimental, 0.x)
pip install "agent-coherence[openai-agents]"

# Everything (langgraph + crewai + otel + langsmith + benchmark + diagnose + openai-agents + mistral)
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

> **Default flipped in v0.9.0.** As of **v0.9.0**, `CrashRecoveryConfig()`
> defaults to `enabled=True` (it was `enabled=False` through v0.8.x), so a bare
> `CCSStore()` / `CoherenceAdapterCore()` now runs crash recovery. The first
> `CrashRecoveryConfig` construction per process emits a one-shot transitional
> `RuntimeWarning` flagging the change for anyone upgrading straight from
> v0.8.2 (removed in v0.10.0). To pin behavior and silence it, pass `enabled=`
> explicitly — `CrashRecoveryConfig(enabled=True)` to keep the new default, or
> `CrashRecoveryConfig(enabled=False)` to opt out. The v0.9.0 defaults were
> also retuned (`heartbeat_timeout_ticks` 10 → 120, `max_hold_ticks` 1000 →
> 900); see [CHANGELOG.md](../CHANGELOG.md) for the full migration notes.

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
| `enabled` | `bool` | `True` | Master switch (**default-on as of v0.9.0**). When `False`, `heartbeat()` and `recover()` are silent no-ops and the sweep does not run. |
| `heartbeat_timeout_ticks` | `int` | `120` | Reclaim a holder's grant if the gap between `now_tick` and the holder's last heartbeat is `>= heartbeat_timeout_ticks`. |
| `max_hold_ticks` | `int` | `900` | Reclaim a holder's grant if it has been continuously held in `MODIFIED`/`EXCLUSIVE` for `>= max_hold_ticks`, regardless of how recently the holder heartbeated. Bound the worst-case lock duration. |

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

With `enabled=False` (the opt-out — the v0.9.0 default is `enabled=True`),
`heartbeat()` and `recover()` are silent no-ops and the sweep never runs.
State-transition log output is then byte-identical to a build without crash
recovery (R5). Note the inversion: omitting the `crash_recovery=` argument no
longer reproduces that output — pass `CrashRecoveryConfig(enabled=False)`
explicitly to get it.

### Disabling / rollback

To turn crash recovery off, pass `CrashRecoveryConfig(enabled=False)` explicitly.
(As of v0.9.0 the default is enabled, so omitting `crash_recovery=` no longer
disables it.) This is the rollback path if the default-on behavior ever surfaces
an issue in your workload — set `enabled=False` and the sweep stops immediately.
No data migration, no protocol incompatibility: the protocol behavior with
`enabled=False` is byte-identical to a build without crash recovery.

If you want to verify the sweep isn't reclaiming benign holders, watch the
state-transition log for `reclaim_heartbeat` and `reclaim_max_hold` triggers. If
they fire on agents that were healthy, raise `heartbeat_timeout_ticks` or
`max_hold_ticks` rather than disabling the feature.

### Reclamation diagnostics

When the sweep reclaims a grant, `CoherenceAdapterCore` logs a one-shot `WARNING`
on the `ccs.adapters.base` logger the **first** time that adapter instance
reclaims, with structured `extra` fields `trigger`, `agent_id_short`,
`artifact_id_short`, and `reclaim_count`. Subsequent reclamations on the same
instance are silent; a companion `DEBUG` log carries the full UUIDs. Bind a
handler to `ccs.adapters.base` to surface or ingest these events.

### Reference

For the formal protocol model (TLA+/TLC) covering single-writer, monotonic
versioning, and crash-recovery sweep invariants, see
[`formal/tla/README.md`](../formal/tla/README.md).

---

## Version retention and read-at-version

By default the coordinator keeps only the **current** version of each artifact.
Opt in to retaining a bounded history, and you can read back the exact bytes of
an earlier version.

### Enabling

Pass a `RetentionPolicy` to the registry (retention is off unless
`retain_versions=True`):

```python
from ccs.coordinator.retention import RetentionPolicy

policy = RetentionPolicy(max_versions=16, max_age_seconds=None)
```

`max_versions` keeps the K most-recent versions (including the current one,
which is never collected); `max_age_seconds` expires versions older than T
wall-clock seconds. Either axis can be `None` to disable it. GC is amortized —
it runs inline as new versions are committed; there is no background sweep.

### Durability

The in-memory `ArtifactRegistry` retains versions for the life of the process.
`SqliteArtifactRegistry` retains them **durably**, surviving a coordinator
restart, for in-process embedders. Enabling durable retention adds an
`artifact_versions` table via the store's first real schema-version bump
(v1 → v2), applied automatically and atomically the first time a v1 database is
opened. Durable retention is opt-in — a deployment that doesn't enable it stores
no version content. (The Claude Code hook/HTTP coordinator carries only content
hashes on the wire, so durable retention there is inert: there are no bodies to
store. It is an in-process-embedder feature.)

### Reading a version

```python
result = service.read_at_version(artifact_id, version)
```

The result is either a `VersionedContent` (`content`, `version`, `captured_at`,
`coordinator_epoch`) or a typed `VersionedReadRejection` whose `reason` is one of
six wire-stable constants:

| Reason | Meaning |
|---|---|
| `retention_off` | The registry is not retaining versions. |
| `unknown_artifact` | No such artifact. |
| `not_retained` | The version is not in the retained history (never captured, or collected / expired). |
| `current_version` | The requested version is the current one — read it through the normal `read` / `fetch` path, not the history surface. |
| `future_version` | The version is greater than the current version. |
| `epoch_mismatch` | The optional `expected_epoch` did not match (the store was reset). |

`read_at_version` is an **off-protocol read**: it grants no MESI state, joins no
invalidation set, and captures no read-generation fence claim — versioned reads
never affect a concurrent writer. It serves history only; current content is
always read through the protocol path.

### Reading from a stored coordinator

`agent-coherence-replay resolve` answers "bytes at version k" against a
`.coherence/state.db` on disk — useful for audit and post-hoc inspection:

```bash
agent-coherence-replay resolve --db ./.coherence/state.db \
    --artifact plans/plan.md --version 2 --json
```

The store is opened **read-only** (never created, never migrated). Output is
content-safe by default — `version`, `coordinator_epoch`, `captured_at`, content
hash and length — and emits the retained bytes only with `--include-content`
(base64 for binary) or `--output-file` (raw, written `0600`), so secrets don't
leak into terminals or CI logs. Each rejection reason and store error maps to a
distinct exit code with the wire-stable `reason` in the JSON envelope.

### Honesty boundary

Retention records and serves the bytes the coordinator committed at each version;
it does not make an agent's *use* of an old version safe. An agent that reads a
fresh current version through the protocol but writes content derived from an
older retained version still commits a fence-legal write of stale meaning — the
coherence guarantee is "write from the bytes your latest read returned," and
read-at-version makes an older version easy to fetch, so keep writes anchored to
the current read.

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
| Conversations stale-read | `python -m examples.conversations_stale_read.main` | Two agents share one conversation; client-cache invalidation (offline, no keys) |

### Code review pipeline

Three agents share a codebase artifact. The key behavior: `reviewer_b` reads the
same codebase that `reviewer_a` cached without either agent invalidating it, because
neither wrote to it. Both hold it in SHARED state simultaneously.

### Research pipeline

Four agents operate on three artifacts (`brief`, `findings`, `analysis`). The key
behavior: `researcher`'s write to `findings` does **not** invalidate `brief` held
by `analyst` — each artifact key has its own independent MESI state per agent.

### Conversations stale-read

Two agents share one conversation: one caches it locally, the other revises it, and
the first acts on a stale copy. `CoherenceAdapterCore` invalidates the stale cache so
the reader re-fetches before acting. Runs offline with no API keys. The companion
`probe.py` measured the OpenAI and Mistral Conversations *servers* as read-after-write
consistent (zero stale reads over 100 + 20 live trials), so the demo isolates the real
failure — the **client cache**, not the server. See
[`examples/conversations_stale_read/README.md`](../examples/conversations_stale_read/README.md)
for the full framing and the optional live Q6 probe. This is the same mechanism the
[OpenAI Agents SDK adapter](#openai-agents-sdk-adapter-experimental) applies to a live
`Session`.

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

### Temporal cost: source drift between turns (TC-1)

The table above is the **spatial** dimension — savings grow with more agents sharing one artifact. The **temporal** dimension is orthogonal: a *single* agent (a RAG/memory reader) whose source drifts between its turns, where coherence-gating avoids re-fetching a chunk that didn't change. TC-1 is the pre-registered benchmark for it — a savings-regime map across change-rate × answer-sensitivity:

```bash
python tools/run_cost_sweep.py --rates 0,0.05,0.1,0.15,0.2,0.25,0.3,0.35,0.5,0.75,1.0 \
  --sensitivities 0,0.5,1.0 --runs 50 --output benchmarks/results/cost_sweep_published.json
python tools/plot_cost_sweep.py    # savings-vs-change-rate curve
```

PASS at n=50: savings stay ≥ 30% while the source changes fewer than ~3 turns in 10 (`r ≤ 0.30`), crossing below 30% at `r ≈ 0.31` and falling to 0 at constant churn. The metric is **re-fetches-avoided** — a proxy / regime map, not a token-dollar invoice (`tools/cost_to_tokens.py` gives an assumption-parameterized dollar translation). Verdict + distinguisher triage: [`../benchmarks/cost_preregistration.md`](../benchmarks/cost_preregistration.md). **Don't splice these numbers into the spatial table above.** Shipped in `v0.9.3` (#116).

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

## Replay (v0.8.2+)

`agent-coherence-replay` is an invariant-replay tool that walks a captured
coordinator session and reports breaches of the four core MESI invariants —
single-writer, monotonic-version, stale-read, lost-write — without re-executing
any agents. Capture rides on the existing `state_log` and `content_audit_log`
callback seams via `CCSStore.record_to(path)`.

### Capture and replay (LangGraph quickstart)

```python
from langgraph.config import get_store as lg_get_store
from langgraph.graph import END, START, StateGraph
from typing import TypedDict

from ccs.adapters.ccsstore import CCSStore


class GraphState(TypedDict):
    log: list[str]


def planner_node(state: GraphState) -> dict:
    store: CCSStore = lg_get_store()  # type: ignore[assignment]
    store.put(("planner", "shared"), "plan", {"step": 1})
    return {"log": [*state["log"], "planner: wrote plan"]}


def reviewer_node(state: GraphState) -> dict:
    store: CCSStore = lg_get_store()  # type: ignore[assignment]
    item = store.get(("reviewer", "shared"), "plan")
    assert item is not None
    return {"log": [*state["log"], "reviewer: read plan"]}


def build_graph(store: CCSStore):
    builder = StateGraph(GraphState)
    builder.add_node("planner", planner_node)
    builder.add_node("reviewer", reviewer_node)
    builder.add_edge(START, "planner")
    builder.add_edge("planner", "reviewer")
    builder.add_edge("reviewer", END)
    return builder.compile(store=store)


# Wrap the store with record_to(...) for the duration of the run.
with CCSStore.record_to("/tmp/coherence-session", strategy="lazy") as store:
    graph = build_graph(store)
    graph.invoke({"log": []})
```

The session directory now contains a `manifest.json` plus one JSONL file per
captured stream (`state_log.jsonl`, `content_audit_log.jsonl`). Inspect it with:

```bash
# Human-readable findings + summary
agent-coherence-replay /tmp/coherence-session

# Machine-readable, one JSON object per line (per-finding + final summary)
agent-coherence-replay /tmp/coherence-session --json | jq .
```

### Exit codes

| Exit code | Meaning |
|---|---|
| `0` | Clean trace (or all SKIPPED entries are explicit compliance opt-outs). Also: `BrokenPipeError` from a closing consumer (e.g. `\| head -5`) — pipe-close is not a failure. |
| `1` | At least one CONFIRMED invariant breach |
| `2` | Capture-side bug: a manifest-declared stream is missing from the directory |
| `3` | Trace error (`MultiInstanceTraceError`, `TraceCorruptionError`, `ManifestMissingOrUnreadableError`, `SessionDirectoryNotFoundError`). Under `--json`, a final NDJSON line lands on stdout: `{"kind":"error","exit_code":3,"exception":"<ClassName>","message":"..."}` (in addition to the human-prose stderr line). |
| `4` | Internal error (uncaught exception inside replay — CLI bug, please file an issue). Distinct from exit 1 so agents can triage "tool crashed" vs "real coordination defect found." |

### Useful flags

- `--invariant <name>` (repeatable) — restrict to a subset of `single-writer`, `monotonic-version`, `stale-read`, `lost-write`.
- `--include-ambiguous` — show same-tick read/commit collisions as per-finding entries (suppressed from default output; always counted in summary).
- `--ambiguous-threshold N` (default `10`) — when the AMBIGUOUS count exceeds the threshold, the summary block emits a prominent callout naming both remedies (`--include-ambiguous` now; D+1 global-sequence-number capture eventually). Strict `>`; does not affect exit code.
- `--quiet` — suppress non-breach output; cron-friendly. Honored under both human and `--json` mode.
- `--json` — newline-delimited JSON conforming to the trace-format schema. Per-finding lines + one summary object; under exit 3, an `{"kind":"error", ...}` line lands on stdout (see Exit codes above).

### Capturing PII-constrained traces

Pass `streams={"state_log"}` to opt out of the content-audit stream while
keeping the other three invariants live:

```python
with CCSStore.record_to(
    "/tmp/coherence-session",
    streams={"state_log"},
    strategy="lazy",
) as store:
    ...
```

Replay then reports stale-read as SKIPPED with `opted_out=True` and the run
still exits 0.

### Refuse-if-exists safety

`CCSStore.record_to(path)` refuses to start when `path/manifest.json`
already exists (raises `SessionDirectoryNotEmptyError`). Without this
guard, a second capture against the same path would silently interleave
JSONL entries from two coordinator instances; `TRACE_CORRUPTION_DUPLICATE_SEQ`
would eventually fire at replay, but only AFTER findings from the mixed
session had already been emitted. Delete the directory or choose a
different path:

```python
import shutil
shutil.rmtree("/tmp/coherence-session", ignore_errors=True)
with CCSStore.record_to("/tmp/coherence-session", strategy="lazy") as store:
    ...
```

### Exception hierarchy

All replay-side exceptions inherit from `ccs.replay.ReplayError` with a
two-tier semantic split:

- `ReplayConfigurationError` — API misuse / wrong entry point
  (`UnverifiedAdapterCaptureError`, `SessionDirectoryNotEmptyError`).
- `ReplayTraceError` — trace structural defects (`ManifestMissingOrUnreadableError`,
  `MultiInstanceTraceError`, `TraceCorruptionError`,
  `SessionDirectoryNotFoundError`). The CLI catches this base class and
  maps every subclass to exit code 3, so future trace-error subclasses
  auto-route without touching the handler.

Catch the base class in your own scripts:

```python
from ccs.replay import ReplayTraceError, load, run_predicates

try:
    loaded = load(session_dir)
    findings, summary = run_predicates(loaded)
except ReplayTraceError as exc:
    # Manifest missing, multi-instance, duplicate seq, etc. — all caught here.
    log.error("Trace defect: %s", exc)
    raise
```

### Non-LangGraph adapters (CrewAI / AutoGen)

CrewAI and AutoGen capture is wired through the same `CoherenceAdapterCore`
seam via `ccs.replay.record_callbacks(...)`, but v1 only verifies the
LangGraph path end-to-end. Direct callers must pass `accept_unverified=True`
to acknowledge the v1 scope boundary — file an issue if the unverified path
breaks for your stack. Ergonomic per-adapter wrappers (`record_to` mirrors)
ship in the next release.

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
| `agent-coherence-replay` | `[langgraph]` | Replay a captured coordinator session and report invariant breaches |

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

### `ccs-check-architecture`

```bash
# Architecture boundary check — fails non-zero if any layer imports upward
ccs-check-architecture
```

Designed for CI gating; runs on every push. The companion script `tools/check_release_readiness.py` (also runs in CI as the release-workflow preflight) is maintainer-only and intentionally not exposed as a console script — it queries this repo's GitHub admin settings and has no end-user use case.

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
| `crash_recovery` | `CrashRecoveryConfig \| None` | `None` | Crash-recovery configuration; see [Crash recovery](#crash-recovery). `None` uses `CrashRecoveryConfig()` — enabled by default as of v0.9.0. |
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

# OpenAI Agents SDK adapter (experimental). These imports and wrap_session work on a
# bare install; only run_hooks() requires the openai-agents extra (deferred import).
from ccs.adapters import OpenAIAgentsAdapter, CoherenceSession
```

### `gate(volume, path, *, decide, effect)`

Order an escaping side effect (a deploy, an opened PR, a notification) against a shared input so it fires only on the input version it was decided from. `gate()` captures the input's version, runs `decide`, re-reads at the effect boundary, and fires `effect` only if the input is unchanged and confirmed — otherwise it raises `StaleView` (a HOLD) before the effect runs.

```python
from ccs.adapters import CoherentVolume, gate

vol = CoherentVolume(workspace_root, managed=("deploy/**",))

# fires run_deploy(plan) only if deploy/config.txt is unchanged since decide() read it;
# else raises StaleView before the deploy runs — reacquire() and re-decide.
gate(vol, "deploy/config.txt", decide=plan_deploy, effect=run_deploy)
```

| Parameter | Type | Description |
|-----------|------|-------------|
| `volume` | `CoherentVolume` | A volume attached to the coordinator that tracks `path`. |
| `path` | `str \| os.PathLike[str]` | The workspace-relative managed artifact whose version gates the effect. |
| `decide` | `Callable[[bytes], D]` | Keyword-only. Reads the captured bytes and returns a decision passed to `effect`. |
| `effect` | `Callable[[D], R]` | Keyword-only. The escaping side effect; fired only if the input is unchanged at the re-read. |

Returns whatever `effect` returns. Raises `StaleView` — carrying `expected_version` / `current_version` — if the input moved, vanished, or could not be confirmed; recover with `volume.reacquire(path)`, then re-decide and re-gate.

**Scope.** Escaping effects only — a pure *write* effect uses `volume.write_cas_at(path, expected_version, content)` directly. The gate *orders* effects and never rolls one back, so for an escaping effect there is a residual re-read→fire window it narrows but cannot close. Single-host and cooperative (the caller opts in). Gating several mutually-consistent inputs at once is a coordinator-side operation, not this single-input wrapper.

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

---

## OpenAI Agents SDK adapter (experimental)

> **Status: experimental (0.x).** The [OpenAI Agents SDK](https://openai.github.io/openai-agents-python/)
> is itself 0.x and its surface churns; this adapter is pinned to
> `openai-agents>=0.17,<0.18` and may change with it. Install with
> `pip install "agent-coherence[openai-agents]"`.

The OpenAI Agents SDK has no `BaseStore`-style seam, so this adapter does **not**
use the `before_node` / `commit_outputs` surface. The coherence target here is the
SDK's **`Session`** — the agent's local conversation memory
(`get_items` / `add_items` / `pop_item` / `clear_session`). A peer that mutates a
shared session leaves this agent's cached view stale, *regardless of how consistent
the durable store is*. (The Q6 probe measured the OpenAI and Mistral Conversations
servers read-after-write consistent — so the coherence value lives on the readers'
caches, not on the server. See the [Conversations stale-read example](#conversations-stale-read).)

The SDK exposes no Session hook/middleware API, so interception is by **composition**:
`wrap_session` wraps a caller-provided Session and overrides the four async methods.
Because the underlying Session is supplied by the caller, the adapter module imports
no `agents` symbol — it works against anything implementing the four-method protocol.

**Scope (v1):** in-process multi-agent coherence — peers registered on one
`OpenAIAgentsAdapter` / `CoherenceAdapterCore` per process, the same boundary as the
LangGraph / CrewAI / AutoGen adapters. Cross-service coherence needs the
out-of-process coordinator.

### Wrapping a Session

```python
from agents import Runner, SQLiteSession
from ccs.adapters import OpenAIAgentsAdapter

adapter = OpenAIAgentsAdapter(strategy_name="lazy")  # on_error defaults to "degrade"

# Every agent wraps the *same* session_id through the adapter, so their reads and
# writes coordinate on one shared coherence artifact (single registration, shared id).
planner_session = adapter.wrap_session(
    SQLiteSession("chat-1"), agent_name="planner", session_id="chat-1"
)
reviewer_session = adapter.wrap_session(
    SQLiteSession("chat-1"), agent_name="reviewer", session_id="chat-1"
)

await Runner.run(planner_agent, "draft the plan", session=planner_session)

# Before the reviewer acts, check whether a peer moved the conversation underneath it.
if reviewer_session.peer_mutated_since_read():
    await reviewer_session.get_items()  # take the cache miss, re-read the fresh version
```

`CoherenceSession` is a drop-in `Session`: pass it anywhere the SDK expects a session.
A mutation (`add_items` / `pop_item` / `clear_session`) persists to the underlying
Session **first**, then invalidates peers; `get_items` refreshes this agent's coherence
state so a prior peer write surfaces as a cache miss. The underlying Session stays the
durable source of truth for the items — the coherence layer governs *awareness*, not
storage.

`peer_mutated_since_read()` is conservative by design: it returns `True` when a peer
has mutated the session since this agent's last read, **and** when this agent has never
read yet (no baseline → "you must read first"). Call `get_items()` once to establish the
baseline; after that it reports only genuine peer mutations.

### RunHooks lifecycle (optional)

Attach `run_hooks(...)` to `Runner.run(..., hooks=...)` to thread coherence accounting
through a run. It tracks the active agent across handoffs and refreshes that agent's
coherence view at agent-start and tool-start, so a peer's mutation surfaces *before* the
agent acts. Writes remain the `CoherenceSession`'s job — the hooks coordinate awareness
and identity, not arbitrary tool side effects.

```python
hooks = adapter.run_hooks(session_id="chat-1")
await Runner.run(agent, "...", session=planner_session, hooks=hooks)
```

Importing `agents.RunHooks` is deferred until this call, so the module (and
`wrap_session`) stays usable on a bare install; `run_hooks` raises `ImportError` with an
install hint if the `openai-agents` extra is absent.

**Server-side conversations + handoffs.** Pass `server_conversation=True` when the run
uses a server-side `conversation_id`. Combined with a multi-agent handoff, the SDK
disables `input_filter` / nested handoff history, so handoff-history coherence is
unavailable — the first handoff then warns once with `CoherenceTopologyWarning` rather
than silently assuming it works. The supported topology is concurrent independent agents
sharing one `conversation_id` within one process.

### Parity and error handling

`OpenAIAgentsAdapter` mirrors the other adapters' constructor and exposes
`register_agent`, `register_artifact`, `heartbeat(agent_name=, now_tick=)`, and
`recover(agent_name=, now_tick=)`, plus `is_degraded` / `degradation_count`. It accepts
`crash_recovery=CrashRecoveryConfig(...)` like the rest. The SDK has no step counter, so
the adapter mints its own monotonic tick internally — you do not pass `now_tick` to
`wrap_session` / `run_hooks`.

One difference from `CCSStore`: `on_error` defaults to **`"degrade"`** here (best-effort
coherence that never swallows the underlying Session op), not `"strict"`. Use
`on_error="strict"` to propagate `CoherenceError`.

> **Degrade-mode caveat for concurrent writers.** Under `on_error="degrade"`, a
> `CoherenceError` from a mutation is swallowed after the Session write already
> succeeded. Because `core.write` grants EXCLUSIVE and then commits as two steps, a
> failed commit (e.g. a concurrent writer reclaimed the grant) can strand the writer
> holding a stable EXCLUSIVE grant with peers already invalidated. That grant is only
> reclaimed by the crash-recovery sweep. For concurrent-writer workloads on the same
> session under degrade, enable `CrashRecoveryConfig` so stranded grants self-heal.
