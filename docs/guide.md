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
10. [Coherent workspace (`CoherentVolume`)](#coherent-workspace-coherentvolume)
11. [BYO substrate bindings (`CoherentRow`, `CoherentObject`)](#byo-substrate-bindings-coherentrow--coherentobject)
12. [Multi-artifact snapshot sessions](#multi-artifact-snapshot-sessions)
13. [`stale-write-guard-fs` MCP server](#stale-write-guard-fs-mcp-server)
14. [Inline benchmark mode](#inline-benchmark-mode)
15. [Telemetry](#telemetry)
16. [Graceful degradation](#graceful-degradation)
17. [Examples](#examples)
18. [Real-workload benchmarks](#real-workload-benchmarks)
19. [Benchmarking your own workload](#benchmarking-your-own-workload)
20. [`ccs-diagnose` — detect stale reads](#ccs-diagnose--detect-stale-reads)
21. [Replay (v0.8.2+)](#replay-v082)
22. [Command-line tools](#command-line-tools)
23. [API reference](#api-reference)
24. [Low-level adapter API](#low-level-adapter-api)
25. [CrewAI and AutoGen adapters](#crewai-and-autogen-adapters)
26. [OpenAI Agents SDK adapter (experimental)](#openai-agents-sdk-adapter-experimental)

---

## Installation

Requires Python 3.11+. Pick the integration extra that matches your stack. The library is the same across all of them — only the adapter surface changes.

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

# stale-write-guard-fs MCP server (coordinated file access for any MCP client)
pip install "agent-coherence[mcp]"

# Everything (langgraph + crewai + otel + langsmith + benchmark + diagnose + openai-agents + mistral + mcp)
pip install "agent-coherence[all]"
```

For security-sensitive installs with full transitive hash pinning, see [the security guide](security.md#hash-pinned-install-for-security-sensitive-users) and the bundled `requirements-diagnose.txt`.

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

**What CCSStore does at the write boundary.** CCSStore provides read-side
coherence: when a peer commits a new version, your cached view is invalidated so
your next read is a fresh miss. It does not deny a stale write-back — `put` is not
version-CAS. For write-side lost-update prevention (a stale writer overwriting a
peer), route writes through [`CoherentVolume`](#coherent-workspace-coherentvolume)
or `write_cas`.

**In-process scope.** CCSStore coherence is in-process: two separate OS processes
each constructing their own CCSStore share nothing. For cross-process coordination
over files, use [`CoherentVolume`](#coherent-workspace-coherentvolume).

**The one-import swap assumes agent-carrying namespaces.** The drop-in is one import
change *only if* your namespaces already carry the agent identity in `namespace[0]`
(see [Namespace convention](#namespace-convention)) — that is what lets two agents
share one artifact while keeping private scratch private. A store keyed the
LangGraph-memory way, `(user_id, "memories")`, would collapse every user onto one
shared artifact, so migrate those call sites to put the agent in `namespace[0]`
before the swap.

**CCSStore and your existing store.** CCSStore's cached contents live for the
process lifetime, not on disk — it is not a database. If you already run Mem0,
Letta, LlamaIndex, or a LangGraph store, keep it: it stays your durability layer,
and CCSStore adds coherence for the cached view above it (a peer write invalidates a
stale read). It does not wrap or replace your backend's storage.

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
        heartbeat_timeout_ticks=120,
        max_hold_ticks=900,
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
recovery. Note the inversion: omitting the `crash_recovery=` argument no
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

## Coherent workspace (`CoherentVolume`)

`CoherentVolume` brings the coherence guarantee to **plain files on disk** — no
framework required. It is an out-of-process coordinator *client*: it spawns (or
attaches to) a local coordinator over SQLite-WAL and routes reads and writes
through it. Your content stays on the real filesystem; the coordinator holds only
per-file MESI state, a content hash, and a version. Point a second volume in
another process at the same workspace and it attaches to the same coordinator, so
every process on the host shares one coherent view.

```python
from ccs.adapters.coherent_volume import CoherentVolume

vol = CoherentVolume(workspace_root, managed=("plans/**", "memory/**"))
data = vol.read("plans/plan.md")            # bytes — registers a SHARED view
vol.write("plans/plan.md", revise(data))    # stale view? denied fail-closed
data = vol.reacquire("plans/plan.md")       # recover: fresh identity + fresh read
```

| Parameter | Default | Meaning |
|---|---|---|
| `workspace_root` | — | Directory the volume manages; the coordinator's state lives in `<root>/.coherence/` |
| `managed` | `()` | Glob patterns for the files under coordination; unmanaged paths bypass the volume |
| `on_error` | `"strict"` | `"degrade"` warns once and falls back to plain IO instead of raising on a coordination failure |
| `on_stale_read` | `"allow"` | `"raise"` — deny a re-read of a managed file whose on-disk bytes changed out-of-band |
| `on_stale_write` | `"raise"` | `"allow"` — restore last-writer-wins over a foreign edit (not recommended) |

### Concurrent writers: `write_cas`

Plain `write()` denies the *sequential* stale view. For **concurrent** same-key
contention, `write_cas(path, make_content)` is the optimistic path: it reads the
file, runs your `make_content` closure on the current bytes, and commits only if
the version is unchanged. The loser of a race gets the winner's value re-fed to
its closure through a bounded retry — one writer wins each round and no update is
silently dropped. A single-shot variant, `write_cas_at(path, expected_version,
content)`, commits against an explicit version with no retry loop. See the race
live: `python -m examples.concurrent_writers.main` runs two threads through the
identical update — a plain file loses one write, `write_cas` preserves both.

When a commit loses its race on the volume (or MCP) path, the raised
`CommitPreempted` is **terminal for that attempt, not a transient to retry blindly**:
the version the write assumed no longer holds. Recover by `reacquire()`-ing,
re-reading the fresh version, and reconciling your change onto it before committing
again — a plain retry of the same bytes just loses the same race.

### Atomic multi-file publish: `atomic_publish` (v0.12.0+)

`write_cas_at` lands one file. When an agent edits a *set* of files that must stay
consistent — a plan and its manifest, a config split across files —
`atomic_publish` lands them **all-or-nothing**:

```python
versions = vol.atomic_publish([
    ("proj/plan.md",     plan_version,     new_plan_bytes),
    ("proj/manifest.md", manifest_version, new_manifest_bytes),
])   # -> {"proj/plan.md": 2, "proj/manifest.md": 3}
```

Each member commits only if it is still at the `expected_version` you pass; if
every member matches, the batch **commits at the coordinator as one unit** and
every file is then materialized, and if any member moved, the **whole** publish is
held (`StaleView` / `CasVersionConflict`) with **nothing committed and no file
written** — a torn *commit* is never a reachable state. A single-member call takes
the direct CAS path; a multi-member call opens a
[snapshot session](#multi-artifact-snapshot-sessions) so the versions it checks
are captured at one point (no member read across a peer commit), which adds a
small capture→commit window — a peer winning it holds the publish rather than
tearing it. Recover the same way as a denied write: `reacquire()`, re-read the
fresh versions, and re-publish from them. A single-member publish accepts
arbitrary bytes; a multi-member publish requires UTF-8 text content.

The all-or-nothing guarantee is at the **coordinator commit**. Disk materialization
runs after it and is best-effort: every file is staged to a temp then renamed, so a
disk fault fails before any rename (disk stays uniformly old) and a rename failing
partway raises a typed `PublishMaterializationError` naming exactly which files
landed — never a bare error implying nothing published. A crash between renames can
still tear the on-disk set (no POSIX multi-file atomic rename exists); on that error
the coordinator is ahead of disk, so re-read each member at its current version and
re-materialize (don't retry the publish — it would version-mismatch). Run it:
`python -m examples.atomic_publish.main` (offline, deterministic, no keys), or add
`--baseline` to see the file-by-file torn pair it prevents.

### Foreign-edit guards

Files also change *outside* the fleet — a human edit, a formatter, a regenerating
script. The volume checks a content hash at both boundaries:

- **Write boundary (on by default).** If a managed file's on-disk bytes changed
  out-of-band since this volume last read or wrote them, `write()` raises
  `StaleView` instead of clobbering the foreign edit. Recover with `reacquire()`:
  fresh read → re-derive → re-write.
- **Read boundary (opt-in).** With `on_stale_read="raise"`, re-reading a managed
  file whose bytes changed out-of-band raises `StaleView` instead of returning
  bytes the rest of your state wasn't computed from. In strict mode the
  coordinator enforces the same check server-side.

A volume never denies its own just-written bytes: the benign window between a
commit and its disk write is recognized and suppressed.

These guards are **content-hash checks at the volume boundary** — best-effort
point-in-time detection, not filesystem interception. An edit that bypasses the
volume is caught at the *next* volume read or write of that file, not blocked as
it happens.

### The `open()` shim (demo-grade)

For code you'd rather not rewrite, `coherent_workspace()` / `install()` patch
`open()` and `pathlib` so managed-path opens route through the volume unchanged.
It covers text and binary read/write via `open()`/`pathlib` — not raw `os.open`,
subprocess redirection, `mmap`, or append/update modes, which delegate to the
original `open()` unchanged. The explicit `read`/`write`/`reacquire`/`write_cas`
API is the supported contract; the shim is a convenience.

Run the demo: `python -m examples.coherent_volume.main` (offline, deterministic,
no keys) — it reproduces the silent lost update, then prevents it.

### Worktrees and the workspace boundary

`CoherentVolume` coordinates by *path under one `workspace_root`*. Two git
worktrees of the same repo are separate directory trees, so `plans/plan.md` in
worktree A and `plans/plan.md` in worktree B are **different physical artifacts** to
the volume — a write in one does not invalidate the other unless both processes route
through the *same* shared workspace root. To make per-worktree sessions coordinate,
point every volume at one common root (for example the primary checkout). The Claude
Code plugin does this for you: it resolves the parent repo via
`git rev-parse --git-common-dir` so sessions in sibling worktrees share one
coordinator (`src/ccs/adapters/claude_code/resolver.py`).

### When you don't need this

Coherence is worth adding only when agents actually share mutable state through a
back channel your framework doesn't already serialize. You can skip it when:

- **Every agent owns an isolated workspace.** If sessions never write the same
  artifact — separate worktrees, separate branches, separate keys with no shared
  root — there is no lost update to prevent. (The moment they *do* converge on one
  file or one shared root, the race is back.)
- **A single database already arbitrates the writes.** If your shared state is rows
  behind one transactional store, its own transactions and row locks already give
  you last-committer-wins with no torn state. `CoherentVolume` targets *plain files*
  and in-memory agent state, where nothing is arbitrating.

The liveness tradeoff runs the other way: a crashed holder does not deadlock the
fleet. A stalled `MODIFIED`/`EXCLUSIVE` grant is bounded and auto-reclaimed by the
best-effort crash-recovery sweep (per artifact, never a global lock), and the
`write_cas` / `atomic_publish` CAS paths hold no lock at all — a loser just re-reads
and retries.

### Cross-host mode (experimental, default off)

Everything above is single-host. An experimental, demo-grade remote mode — gated
entirely by `CCS_REMOTE_COORDINATOR=1` (default off, loopback path byte-unchanged)
— lets a volume connect to a coordinator on another host: `CCS_REMOTE_HOST` /
`CCS_REMOTE_PORT` name the endpoint, and the bearer secret arrives via
`CCS_REMOTE_SECRET_FILE` (a mounted file — never an inline env var).

The client can speak **verified https** to a TLS-terminating front: set
`CCS_REMOTE_TLS=1` to use https with enforced certificate verification (and
`CCS_REMOTE_CA_FILE` to trust a private certificate authority). Verification is
fail-closed — an unverifiable certificate means the bearer is never sent — and a
verified-https connection needs no `CCS_REMOTE_INSECURE=1` acknowledgement. Without
https the transport is plaintext HTTP, so encryption is your out-of-band
responsibility (a WireGuard tunnel or a TLS-terminating proxy); to stop a silent
leak, the client **refuses to send the bearer to a non-loopback host** over
plaintext unless `CCS_REMOTE_INSECURE=1` acknowledges you secured the link
yourself, raising a typed `InsecureTransportRefused` otherwise. Symmetrically, a
coordinator that binds **beyond loopback** refuses to start unless the operator
asserts `CCS_TLS_TERMINATED=1` (a TLS front is present) or `CCS_SERVE_INSECURE=1`
(an acknowledged insecure link) — these are operator assertions, not enforcement.
Setup, the Docker two-container runner, and the full security boundary and
certificate requirements live in
[`examples/cross_host/README.md`](../examples/cross_host/README.md) and
[the security guide](security.md).

> An internal, experimental seam formalizes what a networked registry backend
> would have to provide; it is not a public extension point, and there is nothing
> for end users to configure today.

## BYO substrate bindings (`CoherentRow`, `CoherentObject`)

`CoherentVolume` brings coherence to files on disk. **BYO-substrate bindings** bring the same coherence to shared state that lives in a store you already run — a Postgres row, an S3 object — while the coordinator holds only coherence metadata (a monotonic version, per-agent MESI, a fixed-width `content_hash`, and optionally an opaque substrate token) and **never the bytes**. The bytes stay in your substrate; the coherence layer drops *under* it.

Install the binding you need (the drivers are optional extras):

```bash
pip install "agent-coherence[coherent-row]"     # Postgres — psycopg v3
pip install "agent-coherence[coherent-object]"  # S3 — boto3
```

### What you get over the substrate's own CAS

A substrate's native conditional write (`UPDATE … WHERE version = ?`, S3 `If-Match`) already rejects a single lost update — *at write time*. The binding adds the **cross-agent** layer over it:

- **Invalidation-before-act.** A peer's commit marks your cached read stale, so your next binding-mediated read/act is denied *before* you act on the moved state — the bare CAS never surfaces that.
- **Cross-substrate uniformity.** The same typed conflict (`StaleView` / `CasVersionConflict`) and the same `deny → reacquire()` recovery over a row, an object, a file (`CoherentVolume`), or a store key (`CCSStore`) — one coherence surface, not per-substrate error handling.

```python
from ccs.adapters.coherent_row import CoherentRow

row = CoherentRow(dsn="postgresql://…", table="workspaces")
data, token = row.read("ws-42")               # (bytes, token) from ONE read
# ... a peer commits a new version through the binding ...
row.commit("ws-42", expected_token=token, new_bytes=revised)
#   -> StaleView: your cached view moved. reacquire() for fresh bytes, re-decide, retry.
```

`CoherentObject` (S3) has the identical surface; its token is the object ETag, captured from the `put_object` response (never computed).

### Guarantee tiers (honest by construction)

Every binding declares a `CapabilityDescriptor` with a **tier**; the guarantee wording a user sees is derived from the tier, so a weaker binding can never present as enforcement:

| Tier | Substrate shape | Honest guarantee |
|---|---|---|
| `native-CAS` | atomic conditional write (PG version column, S3 `If-Match`) | no-lost-update on the version-CAS axis, **single-host**, with the timeout asterisk below |
| `detect-only` | no atomic conditional write | catches a *sequential* stale-read→write; cannot prevent a concurrent race |
| `forward-only` | an action / RPC (a Slack post, a Gmail send) — no object, no token | **effect ordering only**: decision-input freshness via deny-before-act; no CAS, no rollback, no duplicate-effect prevention |

### Coherence Manifest

A declarative manifest binds each artifact to a substrate + connection + tier, and is a named **trust boundary**. Credentials are references, never literals (`secret-file:PATH`, `aws-default`, `secret:uri`, or a least-preferred `env:VAR`); connection targets are SSRF-constrained — the deny runs on the *resolved* address (metadata/link-local hard-denied, RFC-1918 allowed only under an explicit `CCS_SUBSTRATE_ALLOW_PRIVATE` opt-in), and a plaintext credential to a routable host is refused unless `CCS_SUBSTRATE_INSECURE` is acked. `dry_run()` prints each artifact's tier at config time. See `docs/examples/manifest.example.yaml`.

### Least-privilege (provision the substrate down to what the binding needs)

- **Postgres** — a dedicated, login-limited **non-owner** role granted only `SELECT, UPDATE` on the one table (no `ALTER` / `TRIGGER` / `DELETE` / re-grant, `NOSUPERUSER NOCREATEDB NOCREATEROLE`), plus an **owner-managed** `BEFORE INSERT/UPDATE` trigger that mints `version := OLD.version + 1` from the *stored* prior — so a client that supplies its own `NEW.version` cannot forge it. `CoherentRow.provisioning_sql(...)` emits (never executes) both.
- **S3** — an IAM policy scoped to the exact key/prefix ARN with `s3:GetObject` + `s3:PutObject` only and explicit denies (no `s3:*`, no `s3:DeleteObject`), plus an **owner-managed** bucket policy that *requires* conditional writes (`Deny s3:PutObject` when `Null s3:if-match true`, with the `s3:ObjectCreationOperation` multipart exemption). `CoherentObject.conditional_write_bucket_policy(...)` / `least_privilege_iam_policy(...)` emit the verified shape.

### Honest scope

- **The read-generation fence over a substrate is a roadmap item, not shipped.** v1 OCC writers ride the fence's admit-on-absent path + the version-CAS. Nothing in these bindings claims the fence.
- **`native-CAS`, with the timeout asterisk.** The substrate CAS prevents the concurrent single-host lost update on the token axis; a coordinator-timeout *after* a durable substrate write is reconverged by a token-identity re-read, and registry↔substrate agreement is a *detectable signal*, not a held guarantee.
- **Single-host, subtractive.** When the substrate is itself distributed (S3, managed Postgres), the no-lost-update guarantee is the *substrate's* and is identical with or without this layer; the adapter's contribution (invalidation, uniformity) is single-host. Never run the S3 CAS loop through a Multi-Region Access Point / cross-Region replica, and never place agents on two hosts against one distributed substrate.
- **Coordinator-behind-substrate is unbounded in v1.** If a writer's substrate write lands but its process dies before the coordinator bump, peers are not invalidated until the *next* binding-mediated read of that artifact — a peer acting on an already-cached read is unprotected until it re-reads. Repair-forward is a roadmap item.
- **More backends are demand-gated, not shipped.** A Letta vendor-memory sidecar is a post-v1 candidate: Letta exposes no atomic conditional-write token, so a binding could only *detect* a sequential stale write via a client-held content shadow, not prevent a concurrent one — it would be a `detect-only` tier and lands only when a concrete need pulls it in.

### Try it, then harden

```bash
python -m examples.coherent_row.main               # offline, no keys — an in-memory substrate stand-in
python -m examples.coherent_object.main --baseline # see the silent stale act the binding catches
```

The demos run offline against a local coordinator with an in-memory substrate stand-in so the coordinator-mediated value (invalidation-before-act) is visible with zero setup. For production, point the same binding at a real Postgres / S3 and provision the least-privilege role/policy above. The tier-honesty conformance suite exercises both bindings against **real** substrates behind the `real_substrate` pytest marker (credentialed; `CCS_TEST_PG_DSN`, `CCS_REAL_S3_BUCKET`) — Moto/LocalStack are excluded because they serialize and would false-green a concurrency test.

## Multi-artifact snapshot sessions

An agent that reads *several* artifacts one at a time can see a torn combination:
`plan.md` from before a peer's commit and `config.json` from after it. Each
individual read was current; the set never coexisted — read-skew. A **snapshot
session** pins a consistent cut of the artifacts you name, captured at a single
point, and serves every session read from that cut while peers keep writing.

Over HTTP, against a running coordinator (the same one `CoherentVolume` spawns):

| Endpoint | Request | Result |
|---|---|---|
| `POST /session/begin` | `{session_id, read_set: ["plans/plan.md", …]}` | `{session_token, cut: {path: version}, coordinator_epoch, retain_versions}` |
| `POST /session/read` | `{session_id, session_token, path}` | the artifact at its **pinned** version — never a newer one |
| `POST /session/commit` | `{session_id, session_token, path, content}` | wins only if no peer moved the artifact since the cut was pinned |
| `POST /session/heartbeat` | `{session_id, session_token}` | keeps the session's lease alive |

Every session call carries both identifiers: `session_id` (your client identity,
from which the server derives the caller) and the server-minted `session_token`
(the handle to the pinned cut).

The `session_token` is server-minted and unguessable; the cut is an inspectable
`{path: version}` map, so you can see exactly which versions your session is
pinned to. In-process, the same surface is
`CoordinatorService.begin_session(read_set=…, owner=…)` →
`session_read(session_token, artifact_id, caller=…)` →
`session_commit(session_token, artifact_id, content, caller=…)`.

Semantics, precisely:

- **Reads serve only from the cut.** An artifact that was *not* in the pinned
  read-set is refused with a typed rejection (`artifact_not_in_cut`) — never
  silently served from live state.
- **Reads are non-mutating.** A session read grants no ownership and blocks no
  writer; peers keep committing while you read.
- **Commits validate against the pinned base.** `session_commit` is a
  single-artifact optimistic commit: it wins only if the artifact's version still
  equals the cut's pinned version, and returns a typed, retryable conflict
  otherwise.
- **Sessions fail closed.** Pins have a bounded lifetime backed by a heartbeat
  lease. A session whose heartbeat lapses — or that is lost to a coordinator
  restart — is invalidated: later reads return a typed `session_invalidated`
  rejection telling the agent to re-establish, never a quiet fall-through to
  whatever is current. A token that was never a session at all (malformed, or
  never opened) is distinguished as `session_not_found`.
- **Bytes vs versions.** When the coordinator retains version bodies
  ([version retention](#version-retention-and-read-at-version)), a session read
  serves the pinned bytes directly. Otherwise it returns the pinned version and
  content hash as a typed signal, and the caller fetches the exact pinned bytes
  from its own data plane.

The no-torn-read property is model-checked: `NoReadSkewWithinCut` and
`PinAlwaysRetained` in `formal/tla/Snapshot.tla`, run by `make tla-check` in CI.

**Honesty boundary.** Snapshot sessions prevent **read-skew** — torn reads across
artifacts. They do not add write-skew prevention: commits validate per-artifact
against the pinned base, so two sessions that read one cut and write *different*
artifacts can still interleave. Single coordinator, single host.

## `stale-write-guard-fs` MCP server

The coherent-workspace guarantee for agents that speak
[Model Context Protocol](https://modelcontextprotocol.io) — Claude Code, Cursor,
or a custom runtime — with no Python integration. The server wraps
`CoherentVolume` and exposes coordinated file access over stdio:

```bash
pip install "agent-coherence[mcp]"
```

Register it with your MCP client (the exact file depends on the client):

```json
{
  "mcpServers": {
    "stale-write-guard-fs": {
      "command": "stale-write-guard-fs",
      "env": { "SWG_ROOT": "/path/to/shared/workspace" }
    }
  }
}
```

`SWG_ROOT` selects the workspace (defaulting to the server's working directory).
By default the whole workspace is guarded; narrow it with `SWG_MANAGED`, a
comma-separated glob list (for example `SWG_MANAGED=plans/**,memory/**`).

| Tool | What it does |
|---|---|
| `swg_read` | Tracked read — registers the agent's view of the file |
| `swg_write` | Guarded write — a stale view or foreign edit returns a typed `stale_view` deny with `recover: reacquire`, never a silent overwrite |
| `swg_reacquire` | Recovery after a deny — fresh identity + mandatory fresh read |
| `swg_write_cas` | Single-shot version-checked write for concurrent same-key contention |
| `swg_status` | Three-state coordination health: `on` / `off` / `unknown` |

Denials are machine-readable: an agent parses the typed payload (for example
`reason: stale_view`, `recover: reacquire`) and self-heals instead of retrying
blindly. The server validates every file URI — path traversal and any access to
the coordinator's own state directory are rejected — and fails closed on IO
errors. Strict-mode, managed-path scoped.

**Multiple sessions, one workspace.** Multiple `stale-write-guard-fs` instances
pointed at the same `SWG_ROOT` attach to one coordinator, so a stale write is denied
across sessions; if the coordinator's session exits, peers fail closed.

**Wiring a client to prefer `swg_*` over native file tools.** Registering the
server exposes the `swg_*` tools, but an agent will still reach for its native
Write/Edit on a managed path unless you steer it there. Deny the native file tools
on managed paths — through the client's permission rules or a pre-tool hook — so
writes have to go through `swg_write` / `swg_write_cas` and inherit the stale-view
deny. The Claude Code adapter that wires this seam ships in
`ccs.adapters.claude_code`.

Run the red→green demo: `python -m examples.mcp_stale_write_guard.main`
(offline, deterministic, no keys).

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

All examples are runnable with `python -m examples.<name>.main` (or `.demo` where noted) from the project root.

Correctness demos lead; the token-savings / hit-rate demos follow.

| Example | Command | What it shows |
|---------|---------|---------------|
| Shared knowledge base | `python -m examples.shared_knowledge_base.demo` | Lost update in a shared RAG / memory corpus; `CoherentVolume` denies B's stale overwrite so both findings survive (offline, no keys) |
| Divergent memory | `python -m examples.divergent_memory.demo` | Two sessions record contradictory beliefs from a stale read; the stale write is denied fail-closed so the divergence never forms (offline, no keys) |
| CCSStore read side | `python -m examples.ccsstore_read_side.demo` | Read-side invalidation on a LangGraph `BaseStore` (peer commit → next `get()` serves the new version), the `put()`-is-not-version-CAS boundary, and the `write_cas` fix (offline, no keys) |
| Coherent volume | `python -m examples.coherent_volume.main` | Sequential stale-write deny + recovery on plain files (offline, no keys) |
| Concurrent writers | `python -m examples.concurrent_writers.main` | True-race lost update; `write_cas` preserves both updates (offline, no keys) |
| Effect gate | `python -m examples.effect_gate.main` | `gate()` holds an effect on a stale input; `--baseline` shows the stale fire (offline, no keys) |
| MCP stale-write guard | `python -m examples.mcp_stale_write_guard.main` | Red→green stale-write deny through the MCP server tools (offline, no keys) |
| Conversations stale-read | `python -m examples.conversations_stale_read.main` | Two agents share one conversation; client-cache invalidation (offline, no keys) |
| Cross-host (experimental) | `python examples/cross_host/main.py` | Stale-write deny + effect ordering across a host boundary (local smoke; Docker runner in `examples/cross_host/`) |
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
[reproduce.md](reproduce.md).

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
graph. See [reproduce.md](reproduce.md) for the simulation methodology behind
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
    crash_recovery=CrashRecoveryConfig(enabled=True, heartbeat_timeout_ticks=120, max_hold_ticks=900),
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
