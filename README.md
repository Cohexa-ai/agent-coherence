# agent-coherence

**`agent-coherence` stops one agent from silently clobbering another's work on a shared `plan.md`, store key, or `memory.json` — a vendor-neutral MESI + optimistic-concurrency coordinator for agent state on a single host, with the safety invariants machine-checked in TLA+.**

Two agents share an artifact — a `plan.md`, a store key, a `memory.json`. One reads it and works; meanwhile a peer commits a newer version; the first writes back anyway. Last write wins, the peer's work is silently gone, nothing errors, and every downstream decision builds on the wrong version. `agent-coherence` turns that silent clobber into a loud, typed refusal: MESI-style ownership and invalidation over shared artifacts, optimistic commit-CAS for concurrent writers, and a read-generation fence for crash-reclaimed ones — a stale write is denied or returned as a retryable conflict, never silently applied. Same library, same protocol, across LangGraph, CrewAI, AutoGen, the OpenAI Agents SDK, plain files shared across processes (`CoherentVolume`), any MCP client (the `stale-write-guard-fs` server, via the `mcp` extra), and any custom orchestrator. Same behavior regardless of which model provider (Anthropic, OpenAI, Google, Mistral, open-source) the agents talk to.

[![CI](https://github.com/Cohexa-ai/agent-coherence/actions/workflows/ci.yml/badge.svg)](https://github.com/Cohexa-ai/agent-coherence/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/agent-coherence)](https://pypi.org/project/agent-coherence/)
[![arXiv](https://img.shields.io/badge/arXiv-2603.15183-b31b1b)](https://arxiv.org/abs/2603.15183)
[![Discussions](https://img.shields.io/github/discussions/Cohexa-ai/agent-coherence)](https://github.com/Cohexa-ai/agent-coherence/discussions)

<!-- MCP Registry — PyPI ownership tag for the stale-write-guard-fs server -->
`mcp-name: io.github.Cohexa-ai/stale-write-guard-fs`

```bash
# Requires Python 3.11+
pip install "agent-coherence[langgraph]"        # LangGraph drop-in
pip install "agent-coherence[crewai]"           # CrewAI adapter
pip install "agent-coherence[openai-agents]"    # OpenAI Agents SDK adapter (experimental)
pip install "agent-coherence[diagnose]"         # ccs-diagnose CLI
pip install "agent-coherence[mcp]"              # stale-write-guard-fs MCP server
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

`store.get()`, `store.put()`, `store.search()` keep working unchanged. `CCSStore` adds **read-side** coherence: a peer's commit invalidates your cached view, so your next read is a fresh miss. It does **not** deny a stale write-back — `put` is not version-CAS; for write-side lost-update prevention, route writes through `CoherentVolume` or `write_cas` (below).

> The one-import swap assumes your store namespaces carry the agent identity in `namespace[0]` — a `(user_id, "memories")` shape would merge users onto one shared artifact. See the [namespace convention](docs/guide.md#namespace-convention).

```python
# Plain files shared across processes / sessions — no framework required
from ccs.adapters.coherent_volume import CoherentVolume

vol = CoherentVolume(workspace_root, managed=("plans/**",))
plan = vol.read("plans/plan.md")           # tracked read — your view is registered
vol.write("plans/plan.md", revised_plan)   # stale view? denied fail-closed → vol.reacquire() and re-derive
```

`agent-coherence-replay` — invariant-replay for any CoherenceAdapterCore-mediated agent system. LangGraph capture verified in v1 via `CCSStore.record_to(path)`; CrewAI / AutoGen wired through the same seam but unverified — file an issue if it breaks.

## What it guarantees

Each row is a safety invariant model-checked with TLA+/TLC. `make tla-check` runs all six specs in CI on every push, and every spec carries a documented mutant that must fail — the invariants are load-bearing, not decorative.

| The silent failure | What happens instead | Mechanism | Invariant |
|---|---|---|---|
| **Stale-read overwrite** — an agent acts on an old snapshot and writes over a newer version (two sessions, one `plan.md`) | the write is **denied fail-closed**; the writer must `reacquire()` and read the current version | MESI single-writer ownership + invalidation | `SingleWriter`, `MonotonicVersion` |
| **Concurrent lost update** — two writers hit the same key and both "succeed" | exactly one wins; the loser gets a **typed conflict + bounded retry**, never a silent drop | optimistic commit-CAS (`write_cas`) | `NoLostUpdate` |
| **Reclaim-zombie write** — a stalled writer is reclaimed by crash recovery, wakes later, and lands its stale commit; the version never moved, so a version check passes | the commit is **rejected** with a typed `stale_read_generation` conflict | read-generation fence — reclamation bumps the artifact's ownership epoch, checked atomically at commit | `NoStaleApply` |
| **Torn multi-artifact read (read-skew)** — an agent reads several artifacts one by one while a peer commits in between; each read was individually current, but the *combination* never coexisted | session reads serve from a **pinned consistent cut**; commits validate against the pinned base; a lapsed session **fails closed** with a typed rejection, never a silent fall-through to live state | [multi-artifact snapshot sessions](#multi-artifact-snapshot-sessions) | `NoReadSkewWithinCut`, `PinAlwaysRetained` |
| **Dead owner blocks the fleet** — a crashed agent holds EXCLUSIVE forever | the heartbeat/TTL sweep reclaims the grant (on by default; best-effort, rate-limited) | crash-recovery sweep | sweep invariants I3–I6 |

**Scope, honestly:** the guarantees hold for writers that go through the coordinator, under a single coordinator (one host). Concurrent same-key writers on one host are covered; cross-host fencing is on the roadmap, demand-gated — if you need it, [open an issue](https://github.com/Cohexa-ai/agent-coherence/issues/new). Edits that *bypass* the coordinator entirely (a human in an editor, a formatter, a regenerating script) are caught at the workspace boundary by content-hash checks — the [foreign-edit guards](#foreign-edit-guards-writes-that-bypass-the-coordinator) below, enforced by tests rather than TLA+. Specs, the invariant ↔ implementation map, and the mutant recipes live in [`formal/tla/`](formal/tla/README.md).

**Correctness is the wedge; the token savings come with it.** Writes publish ~12-token invalidation signals instead of rebroadcasting full artifacts, so read-heavy fleets stop re-paying for state they already hold:

| Workload | Agents | Reads:Writes | Hit rate | Savings |
|---|---|---|---|---|
| Planning (read-heavy) | 4 | 12:1 | 75% | **69%** |
| Code review (moderate) | 3 | 8:3 | 60% | **47%** |
| High-churn (write-heavy) | 4 | 8:4 | 50% | **29%** |

*Measured on real LangGraph graphs; see [docs/reproduce.md](docs/reproduce.md) and the [user guide](docs/guide.md#real-workload-benchmarks).*

Those are the **spatial** savings (more agents sharing one artifact). The **temporal** dimension — a single agent whose source drifts between its turns — has its own pre-registered benchmark, **TC-1** (#116): a reproducible savings-regime map of how many re-fetches coherence-gating avoids as the change-rate rises. The metric is *re-fetches-avoided* — a proxy, a regime map, **not** a token/dollar invoice. Reproduce with `python tools/run_cost_sweep.py`; the locked verdict + numbers (PASS at n=50, crossover r≈0.31) live in [`benchmarks/cost_preregistration.md`](benchmarks/cost_preregistration.md). Shipped in `v0.9.3`.

## RAG & shared agent memory

RAG corpora and agent memory are **shared mutable state**, so the stale-read→write lost update lands there too — and *a consistent store doesn't save you*: the staleness is in the **agent's cached view of a record**, not the store. Two agents read a record at v1; one writes v2; the other, still on its v1, writes an edit computed from v1 and clobbers v2. `agent-coherence` keeps the **readers** current — `CCSStore` is a drop-in for `langgraph.store` (composing with Mem0, Letta, LlamaIndex, a vector store, or a plain file underneath whatever you already use; it stores no vectors and does no ranking), so a peer's commit invalidates the stale cached view (read-side coherence). Preventing the stale **write-back** itself is the write side — route those writes through `CoherentVolume` or `write_cas`.

- **Runnable, deterministic demo** (offline, no keys): `python -m examples.coherent_volume.main` reproduces the documented lost update, then prevents it.
- **Honest scope:** writes that go through the coordinator are caught. Auto-watching an *unmanaged external source* that changes with no coordinator write (a hand-edited file, an out-of-band re-index) is the source-watcher case — **on the roadmap, demand-gated, not shipped today.**
- **Positioning + FAQ:** [agent-coherence.dev/rag](https://agent-coherence.dev/rag/).

---

- 📖 [User guide](docs/guide.md) — installation, namespace convention, strategies, observability, telemetry, examples, full API reference
- 🔎 [RAG & shared memory](https://agent-coherence.dev/rag/) — coherence for retrieval corpora and agent memory stores, with the runnable lost-update demo
- 🗂️ [Coherent workspace](#coherent-workspace-the-data-plane-for-shared-files) — `CoherentVolume`, the data-plane appliance for plain files shared across processes
- 🧱 [BYO substrate](#byo-substrate-coherence-over-the-store-you-already-run) — `CoherentRow` / `CoherentObject`, the same coherence over a Postgres row or an S3 object you already run
- 🛡️ [Foreign-edit guards](#foreign-edit-guards-writes-that-bypass-the-coordinator) — catch out-of-band edits (a human, a formatter, a script) at the read/write boundary
- 🔌 [MCP server](#mcp-server-stale-write-guard-fs) — `stale-write-guard-fs`, the same guarantee for any MCP client, no Python integration required
- 🚦 [Effect-ordering gate](#effect-ordering-gate) — `gate()`, fire an agent's effect only on the input version it decided from
- 📸 [Multi-artifact snapshot sessions](#multi-artifact-snapshot-sessions) — read several artifacts as one consistent cut; no torn reads
- 📦 [Atomic multi-file publish](#atomic-multi-file-publish) — `atomic_publish`, land a set of files all-or-nothing; never a torn pair
- 🧮 [Formal verification](formal/tla/README.md) — the TLA+ specs, invariant ↔ implementation map, mutant recipes
- 🩺 [`ccs-diagnose` CLI](docs/ccs-diagnose.md) — find divergent reads in your existing LangGraph graph without changing any code
- 🧩 [Claude Code plugin](https://github.com/Cohexa-ai/agent-coherence-plugin) — cross-session coherence for the prose rules (CLAUDE.md, plan.md) parallel Claude Code sessions share
- 🔍 [Why coherence matters](docs/why-coherence-matters.md) — the gap across LangGraph, CrewAI, AutoGen, and Claude Agent SDK
- 🧭 [The MESI-derived approach](docs/agent-coherence-approach.md) — how the protocol maps each documented gap to a shipped surface, with boundaries
- 🔐 [Security & supply chain](docs/security.md) — kill switches, hash-pinned install, attestation verification, threat model
- 📜 [Changelog](CHANGELOG.md) — version history
- 📄 [Paper on arXiv (2603.15183)](https://arxiv.org/abs/2603.15183) — formal protocol, TLA+ verification, simulation results

## How it works

Each shared artifact is cached locally per agent and reads serve from the local cache when that copy is fresh. Writes commit to a coordinator, which sends lightweight invalidation signals (~12 tokens) to peers so the next read fetches the new version instead of rebroadcasting the full artifact. Consistency is single-writer-multiple-reader per artifact with bounded staleness — peers re-fetch on next read.

Two write disciplines share the same guarantee. **Pessimistic:** acquire EXCLUSIVE, commit; a writer whose view went stale is denied and must `reacquire()`. **Optimistic:** `write_cas` — read, compute, commit-CAS; the loser of a race gets a typed conflict and bounded retry. Crash recovery composes with both: reclaiming a stalled grant bumps the artifact's ownership epoch, so a reclaimed writer that completes later is rejected at commit even when the version is unchanged (the read-generation fence). On the read side, a [snapshot session](#multi-artifact-snapshot-sessions) pins a consistent cut across several artifacts, so a multi-artifact read never sees a torn mix of versions.

Five synchronization strategies ship out of the box: `lazy` (default), `eager`, `lease` (TTL-based), `access_count`, and `broadcast`. Pick the one that matches your workload's read/write ratio and how aggressively cached reads should refresh.

## Architecture

- **Protocol** (`ccs.core`, `ccs.strategies`) — coherence state machine and synchronization strategies; no framework dependencies.
- **Coordinator** (`ccs.coordinator`) — authority service tracking directory state, publishing invalidations, arbitrating commit-CAS, and reclaiming stale grants (crash recovery + read-generation fence).
- **Adapters** (`ccs.adapters`) — framework integrations for LangGraph, CrewAI, and AutoGen (~100 lines each), plus an experimental OpenAI Agents SDK adapter (`Session`-cache coherence + `RunHooks`).
- **Coherent workspace** (`ccs.adapters.coherent_volume`) — the **data-plane appliance**: an out-of-process coordinator client that brings the same guarantee to plain files on disk, no framework required. See [Coherent workspace](#coherent-workspace-the-data-plane-for-shared-files).
- **MCP server** (`ccs.mcp`) — the `stale-write-guard-fs` stdio server that exposes the coherent-workspace guarantee to any [Model Context Protocol](https://modelcontextprotocol.io) client over five `swg_*` tools. See [MCP server](#mcp-server-stale-write-guard-fs).
- **Simulation** (`ccs.simulation`) — deterministic tick-driven engine for scenario benchmarks with failure injection.
- **Event bus** (`ccs.bus`) — the transport for invalidation signals; in-memory / in-process today (`InMemoryEventBus`). Networked transports (Redis, Kafka, NATS, gRPC) for a multi-host deployment are on the roadmap, demand-gated.

Protocol safety properties — single-writer, monotonic versioning, the crash-recovery sweep invariants, the OCC no-lost-update, the reclamation fence's no-stale-apply, version retention's no-collected-read, and the snapshot session's no-read-skew-within-cut — are model-checked with [TLA+/TLC](formal/tla/README.md). The `tla-check` CI job runs all six specs on every push and PR.

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

**Scope, honestly.** Plain `write()` prevents the **sequential** stale-read→write lost update for a single-host fleet sharing one workspace (A reads v1, B reads v1, A commits v2, B's stale write is denied → B re-reads); **concurrent** same-key racers go through `write_cas` — one winner, the loser gets a typed conflict and re-derives, never a silent drop. Edits that bypass the volume entirely are caught at the boundary by the [foreign-edit guards](#foreign-edit-guards-writes-that-bypass-the-coordinator). It does **not** catch an agent that re-reads fresh bytes and then writes a buffer computed from older ones. The `open()` shim is convenience, not the contract: it covers `open()`/`pathlib` text+binary read/write, but not raw `os.open`, subprocess redirection, `mmap`, or append/update modes — those delegate to the original `open()` unchanged. Run it yourself: `python -m examples.coherent_volume.main` (offline, deterministic, no keys), or read the [positioning + FAQ](https://agent-coherence.dev/rag/).

## Foreign-edit guards: writes that bypass the coordinator

Coordination covers writers that opt in — but real workspaces also get edited from *outside*: a human fixes a file in an editor, a formatter rewrites it, a CI script regenerates it. Without a guard, the next agent write silently buries that edit, and the next agent read silently builds on bytes the coordinator never saw. `CoherentVolume` guards both boundaries with a content-hash check:

- **Write boundary — on by default.** Before writing, the volume checks whether the managed file's on-disk bytes changed out-of-band since it last read or wrote them. If they did, the write raises `StaleView` instead of clobbering the foreign edit — recover with `reacquire()` (fresh read → re-derive → re-write). Opt out with `CoherentVolume(on_stale_write="allow")` to restore last-writer-wins.

- **Read boundary — opt-in.** With `CoherentVolume(on_stale_read="raise")`, re-reading a managed file whose bytes changed out-of-band raises `StaleView` instead of returning bytes your other state wasn't computed from; in strict mode the coordinator enforces the same check server-side. A volume never denies its own just-written bytes — the benign commit→disk-write lag window is recognized and suppressed.

```python
vol = CoherentVolume(workspace_root, managed=("plans/**",), on_stale_read="raise")
# a formatter rewrites plans/plan.md out-of-band …
vol.write("plans/plan.md", revised)   # StaleView — the foreign edit survives
fresh = vol.reacquire("plans/plan.md")  # recover: fresh read, re-derive, re-write
```

**Scope, honestly.** These are content-hash checks at the volume's read/write boundary — best-effort point-in-time detection, not filesystem interception. A write that never goes through the volume is *caught at the next volume read/write of that file*, not blocked as it happens; watching unmanaged external sources is on the roadmap, demand-gated. These guards are enforced by tests, not TLA+ — the model-checked invariants cover the protocol state machine, not disk bytes.

## MCP server: `stale-write-guard-fs`

The same guarantee for agents that speak [Model Context Protocol](https://modelcontextprotocol.io) — Claude Code, Cursor, or a custom runtime — with **no Python integration at all**. `stale-write-guard-fs` is a stdio MCP server that wraps `CoherentVolume` and exposes coordinated file access as five tools:

```bash
pip install "agent-coherence[mcp]"
```

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

| Tool | What it does |
|---|---|
| `swg_read` | Tracked read — registers the agent's view of the file |
| `swg_write` | Guarded write — a stale view or a foreign edit gets a typed `stale_view` deny with `recover: reacquire`, never a silent overwrite |
| `swg_reacquire` | Recovery — fresh identity + mandatory fresh read after a deny |
| `swg_write_cas` | Single-shot version-checked write for concurrent same-key contention |
| `swg_status` | Three-state coordination health: `on` / `off` / `unknown` |

The server binds one workspace per session (`SWG_ROOT`, defaulting to its working directory; the whole workspace is guarded unless `SWG_MANAGED` — a comma-separated glob list — narrows it), rejects path traversal and any access to the coordinator's own state directory, and fails closed on IO errors. Denials come back as typed, machine-readable payloads — an agent can parse `recover: reacquire` and self-heal instead of retrying blindly. Run the red→green demo: `python -m examples.mcp_stale_write_guard.main` (offline, deterministic, no keys).

**Scope, honestly.** Same contract as the volume it wraps: single-host, managed paths, cooperative — it guards agents that route file access through the tools; it cannot see edits made around them (those are caught at the next tool call on that file by the foreign-edit guards).

## Effect-ordering gate

Agents don't only overwrite files — they fire *effects* (a deploy, a PR, a shell command) computed from inputs they read earlier. If the input moved in between, the effect fires on stale state. `gate()` narrows that window: it captures the input's version at decision time, re-reads at the effect boundary, and fires only if the input is unchanged at that re-read — otherwise it holds the effect before it runs.

```python
from ccs.adapters import CoherentVolume, gate

vol = CoherentVolume(workspace_root, managed=("deploy/**",))

# fires run_deploy(plan) only if deploy/config.txt is unchanged since decide() read it;
# else raises StaleView before the deploy runs — reacquire() and re-decide.
gate(vol, "deploy/config.txt", decide=plan_deploy, effect=run_deploy)
```

It's plain Python, so the same call drops into a LangGraph node, a CrewAI task, or a raw script unchanged.

**Scope, honestly.** The gate *orders* effects, it does not roll them back: it fires pre-effect and never undoes one, so for an escaping effect there's a residual re-read→fire window it narrows but can't close. It's single-host and cooperative — the agent opts in. For a pure *write* effect, use `vol.write_cas_at(path, expected_version, content)` directly, which is the atomic, no-window path. Gating several mutually-consistent inputs at once is a [snapshot-session](#multi-artifact-snapshot-sessions) operation on the coordinator, not this single-input wrapper. Run it: `python -m examples.effect_gate.main` (offline, deterministic, no keys), or add `--baseline` to see the stale fire it catches.

## Multi-artifact snapshot sessions

`gate()` protects one input. But an agent that reads *several* artifacts one by one — a plan, a config, a memory file — can see a torn combination: `plan.md` from before a peer's commit and `config.json` from after it. Every individual read was current; the *set* never coexisted (read-skew). A snapshot session closes that window: it pins a consistent cut of the artifacts you name, captured at a single point, and serves every session read from that cut while peers keep writing.

Against a running coordinator (the same one `CoherentVolume` spawns), over HTTP:

```text
POST /session/begin      {session_id, read_set: ["plans/plan.md", "config/app.json"]}
                         → {session_token, cut: {path: version}, …}
POST /session/read       {session_id, session_token, path}
                         → the artifact at its PINNED version — never a newer one
POST /session/commit     {session_id, session_token, path, content}
                         → wins only if no peer moved the artifact since the cut
POST /session/heartbeat  {session_id, session_token} — keep the session's lease alive
```

Or in-process: `CoordinatorService.begin_session(read_set=…, owner=…)` → `session_read(…)` / `session_commit(…)`. The cut is an inspectable `{artifact: version}` map, not an opaque handle — you can read exactly which versions your session is pinned to.

Fail-closed by construction: reading an artifact that was **not** in the pinned read-set is refused with a typed rejection — never silently served from live state. Sessions have a bounded lifetime backed by a heartbeat lease: a session whose heartbeat lapses, or that is lost to a coordinator restart, is invalidated — later reads get a typed "session invalidated" rejection telling the agent to re-establish, never a quiet fall-through to whatever is current. Model-checked: `NoReadSkewWithinCut` and `PinAlwaysRetained` ([`formal/tla/Snapshot.tla`](formal/tla/Snapshot.tla)).

**Scope, honestly.** This prevents **read-skew** — torn reads across artifacts. It does not add write-skew prevention: commits validate per-artifact against the pinned base through the same optimistic CAS as `write_cas`, so two sessions that read one cut and write *different* artifacts can still interleave. Single coordinator, single host. When the coordinator retains version bodies it serves the pinned bytes directly; otherwise it returns the pinned version and content hash as a typed signal and the caller fetches the bytes from its own data plane.

## Atomic multi-file publish

`write_cas_at` lands one file if it hasn't moved. But an agent often edits a *set* of files that must stay consistent — a plan and its manifest, a config split across files — and must land them **together or not at all**, never a torn pair where one file references another that already changed. `atomic_publish` is that all-or-nothing batch:

```python
from ccs.adapters import CoherentVolume

vol = CoherentVolume(workspace_root, managed=("proj/**",))

# lands BOTH files iff each is still at the version the agent read; otherwise the
# WHOLE publish is held (StaleView / CasVersionConflict) with NO file written.
versions = vol.atomic_publish([
    ("proj/plan.md",     plan_version,     new_plan_bytes),
    ("proj/manifest.md", manifest_version, new_manifest_bytes),
])   # -> {"proj/plan.md": 2, "proj/manifest.md": 3}
```

Either every member's version advances or none does, and a moved member holds the whole batch — a torn *commit* is never a reachable state, formally specified as the `NoPartialPublish` invariant in [`formal/tla/AtomicPublish.tla`](formal/tla/AtomicPublish.tla). A single-file publish takes the direct CAS path; a multi-file publish opens a [snapshot session](#multi-artifact-snapshot-sessions) so the versions it checks are captured at one point (no member read across a peer commit). Run it: `python -m examples.atomic_publish.main` (offline, deterministic, no keys), or add `--baseline` to see the file-by-file torn pair it prevents.

**Scope, honestly.** The all-or-nothing guarantee is at the **coordinator commit** — that is what `NoPartialPublish` covers. Disk materialization happens *after* the commit and is best-effort: every file is staged to a temp then renamed into place, so a disk fault fails before any rename (disk stays uniformly old) and a rename failing partway raises a typed `PublishMaterializationError` naming exactly which files landed — never a bare error implying nothing published. This shrinks, but a crash between renames can't fully eliminate, the multi-file disk window (there is no POSIX multi-file atomic rename); on that error the coordinator is ahead of disk and you re-read + re-materialize. It is single-host and cooperative. The multi-file path also adds a small capture→commit window (the session open); a peer winning it **holds** the publish rather than tearing it. This is all-or-nothing *publish* of a file set — not rollback of effects that already escaped, and not write-skew prevention across sessions.

## BYO substrate: coherence over the store you already run

`CoherentVolume` puts coherence over files on disk. But shared agent state often lives in a store you already run — a Postgres row, an S3 object. **BYO-substrate bindings** bring the same coherence *over that store*, with the coordinator holding only metadata (a version, per-agent MESI, a fixed-width `content_hash`, an opaque substrate token) — **never the bytes**:

```python
from ccs.adapters.coherent_row import CoherentRow      # pip install "agent-coherence[coherent-row]"
from ccs.adapters.coherent_object import CoherentObject  # pip install "agent-coherence[coherent-object]"

# agent A reads a row it will edit over several steps
row = CoherentRow(dsn=..., table="workspaces", artifact_id="ws-42")
data, token = row.read("ws-42")
# ... meanwhile agent B commits a new version through the binding ...
# A's next binding-mediated read/act is DENIED before A writes:
row.commit("ws-42", expected_token=token, new_bytes=revised)  # -> StaleView; reacquire() and re-decide
```

The value over the substrate's own conditional write (`UPDATE … WHERE version=?`, S3 `If-Match`): a bare CAS rejects A's write *at write time*; the binding tells A its **cached view went stale before it acts**, in the **same typed vocabulary** a file (`CoherentVolume`) or a store key (`CCSStore`) uses — one coherence surface over a row, an object, or a file. A declarative [Coherence Manifest](docs/guide.md#byo-substrate-bindings-coherentrow--coherentobject) wires each artifact to a substrate and an honest guarantee **tier**, with credentials as references (`secret-file:` / `aws-default`, never literals) and SSRF-constrained connection targets.

**Scope, honestly.** v1 ships two `native-CAS` bindings (Postgres + S3) and a `forward-only` tier for action backends (a Slack post, a Gmail send — decision-input freshness only, no CAS). The value is **invalidation-before-act + cross-substrate uniformity**; the read-generation fence over a substrate is a documented roadmap item, **not claimed** — v1 OCC writers ride admit-on-absent + the version-CAS. Single-host and cooperative; when the substrate is itself distributed (S3, managed Postgres) the no-lost-update guarantee is the *substrate's* and identical with or without this layer. Run it: `python -m examples.coherent_row.main` / `examples.coherent_object.main` (offline, deterministic, no keys — an in-memory substrate stand-in; production points the same binding at real Postgres / S3). Full API, per-binding least-privilege, and the honest tier table: [BYO substrate bindings](docs/guide.md#byo-substrate-bindings-coherentrow--coherentobject).

## Status

**`v0.12.0` released — atomic multi-artifact publish (`atomic_publish` / `commit_all`), the gate-independent TLS-transport-guard slice, and the MCP Registry manifest for `stale-write-guard-fs`. First release published from the `Cohexa-ai` organization.** `atomic_publish` commits a *set* of files all-or-nothing: either every member's version advances as one unit or none does, so a reader never sees a torn, half-applied edit. All-or-nothing is at the coordinator commit (`NoPartialPublish`, formally specified in `formal/tla/AtomicPublish.tla`); disk materialization is best-effort staged-rename after the commit. Single-host; not rollback of already-escaped effects, and not cross-session write-skew prevention. The cross-host TLS slice adds client-side certificate verification and a fail-closed plaintext-bearer refusal (`InsecureTransportRefused`), but **no networked backend is built** — routed deployment stays experimental and demand-gated. The default single-host loopback path is unchanged. See [CHANGELOG.md](CHANGELOG.md).

See [CHANGELOG.md](CHANGELOG.md) for the full version history and [releases](https://github.com/Cohexa-ai/agent-coherence/releases) for tagged artifacts. Alpha — APIs may change before `v1.0`.

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

Questions, war stories, and ideas welcome in [Discussions](https://github.com/Cohexa-ai/agent-coherence/discussions). If you've hit a stale-read bug in a multi-agent workflow, [open an issue](https://github.com/Cohexa-ai/agent-coherence/issues/new) — I'd like to hear about it.

## License

Apache-2.0. See [LICENSE](LICENSE).
