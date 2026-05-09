# Crash Recovery — Stable-State Grant Reclamation

**Status:** Implementation shipped behind feature flag (`enabled=False` default).
**State-log schema:** `ccs.state_log.v2` (no schema change introduced; new
`trigger` strings only — see *Observability*).
**Date:** 2026-05-08

This spec accompanies the implementation shipped in commits
`7692b3c..4528dfa` on branch `feat/crash-recovery`. The decision history,
the brainstorm dialogue, and the carryover risks live in the origin
requirements doc — see `docs/brainstorms/crash-recovery-stale-grants-requirements.md`.
This document is the canonical reference for **what the protocol does**
and **what a TLA+ amendment must formalize**.

---

## 1. Overview

`CoordinatorService.enforce_transient_timeouts` recovers from stuck
*transient* states (ISG, IED, EIA, SIA, MWB, MSA). Stable-state grants
— `MODIFIED` and `EXCLUSIVE` — held by a crashed or livelocked agent
have no recovery path in the base protocol; the single-writer
invariant becomes a permanent liveness obstruction.

The crash-recovery extension adds a coordinator-only, periodic sweep —
`enforce_stable_grant_timeouts` — that reclaims stale stable-state
grants based on:

- a **per-agent heartbeat** (the agent must signal liveness within
  `heartbeat_timeout_ticks`), and
- a **per-grant max-hold ceiling** (a grant is reclaimed regardless
  of heartbeat after `max_hold_ticks`).

Reclamation forces the agent's MESI state for that artifact to
`INVALID`, leaves `artifact.version` unchanged, and records a
diagnostic slot the agent's later `commit()` consults to surface a
clear error.

The full design rationale (D1–D5, alternatives, carryover risks) is in
the origin doc. **This spec assumes those decisions and documents the
behavior they produced.**

---

## 2. Hard preconditions (must be true for the design to be safe)

These are reproduced verbatim from the origin doc; if any is violated,
the design's safety arguments do not hold.

1. **Single-coordinator topology.** Heartbeat-based reclaim relies on
   the coordinator being the sole arbiter of liveness. Enabling
   crash-recovery commits CCS to single-coordinator semantics until a
   partition-safe reclamation scheme (likely fencing tokens) lands.
2. **Coordinator is single-threaded with respect to the registry.**
   All sweeps and RPCs serialize through the coordinator's control
   plane on one tick boundary.
3. **Agent runtime contract on recovery.** Any agent process that has
   crashed and restarted (including checkpoint-restore) must
   invalidate its local `ArtifactCache` for every artifact it
   previously held a grant on, before issuing any further
   fetch/write/commit. Adapter implementations are responsible for
   honoring this contract. *Section 6 below makes the per-adapter
   contract explicit; the adapter wiring itself ships in a separate
   plan.*
4. **All shared-state-affecting agent activity routes through the
   coordinator.** An agent that takes externally-visible action
   without coordinator round-trip can desync from registry state
   during a partition; the coordinator cannot detect this.

---

## 3. Failure shapes covered

| Shape | Symptom | Caught by |
|---|---|---|
| Hard crash (OOM-kill, segfault) | Agent vanishes; no further heartbeat | Per-agent heartbeat timeout |
| Checkpoint short-circuit / livelock | Process alive, may still heartbeat, but never releases a grant | Per-grant max-hold ceiling |
| Long synchronous compute | Agent intentionally holds grant for a long step | Operator must configure thresholds to exceed legitimate hold time. Heartbeat and max-hold are not independent for synchronous compute (see *Carryover risk #3* in origin) |
| Network partition (single-coordinator only) | Agent unreachable | Per-agent heartbeat timeout. Safe **only** under hard precondition #1 |

---

## 4. Behavior

### 4.1 Liveness signals

- **Per-agent heartbeat.** Agents call
  `coordinator.record_heartbeat(agent_id, now_tick)` to signal
  liveness. The coordinator stores `last_heartbeat_tick` per agent
  in `ArtifactRegistry._heartbeat_by_agent`. On receipt of a
  heartbeat carrying tick `t`, the coordinator updates
  `last_heartbeat_tick := max(last_heartbeat_tick, t)` — never
  overwriting with a smaller value, to tolerate out-of-order
  delivery.
- **Per-grant max-hold.** Whenever an agent's MESI state for an
  artifact transitions *into* `MODIFIED` or `EXCLUSIVE` from a
  state outside that set, the coordinator records `granted_at_tick`
  on the per-record `ArtifactRecord.granted_at_tick_by_agent` dict.
  The slot is cleared when the state leaves `M ∪ E`. **Intra-M∪E
  transitions (E→M, M→E) preserve the original `granted_at_tick`** —
  the grant is conceptually continuous, and resetting the timer would
  let an agent extend its hold indefinitely by toggling between M
  and E.

### 4.2 Reclamation algorithm

`CoordinatorService.enforce_stable_grant_timeouts(*, current_tick,
heartbeat_timeout_ticks, max_hold_ticks) -> int`. Returns the count
of grants reclaimed.

The sweep is invoked once per tick by the engine (or adapter) when
`crash_recovery.enabled` is `True`. It executes the following steps,
in order:

1. **Snapshot.** Build a list of `(artifact_id, agent_id, mesi)`
   tuples by walking every artifact's per-agent state map and
   filtering to `mesi in {MODIFIED, EXCLUSIVE}`. Iterate this
   immutable list — do **not** iterate live registry state.
2. **Skip transient holders.** For each `(artifact_id, agent_id,
   _mesi)` in the snapshot, skip if
   `registry.get_agent_transient(artifact_id, agent_id)` is
   non-`None`. Transient holders are owned by the transient sweep
   (`enforce_transient_timeouts`), which the engine invokes
   immediately before the stable sweep on each tick.
3. **Trigger ordering — first match wins.** For each remaining
   pair, evaluate two conditions in order; the first match wins:
   - **Heartbeat:** if `last_heartbeat_tick(agent_id) is None` OR
     `current_tick - last_heartbeat_tick(agent_id) >= heartbeat_timeout_ticks`,
     reclaim with `trigger="reclaim_heartbeat"`.
   - **Max-hold:** else if `granted_at_tick(agent_id, artifact_id)
     is not None` AND `current_tick - granted_at_tick >= max_hold_ticks`,
     reclaim with `trigger="reclaim_max_hold"`.
   - Else: leave the grant alone.

   A grant violating both conditions is reported as
   `reclaim_heartbeat` because the agent-gone signal is more
   diagnostic — the agent is gone, the max-hold breach is
   incidental.
4. **Reclaim** = three operations in order:
   1. `set_agent_state(artifact_id, agent_id, INVALID,
      trigger=<reclaim_heartbeat|reclaim_max_hold>, tick=current_tick,
      content_hash=None)`. The `content_hash=None` matches the
      v0.5+ convention for state-only events (no content involved).
      Artifact `version` is **not** bumped.
   2. `record_last_reclamation(agent_id, artifact_id, trigger,
      current_tick)`. Writes to the per-record
      `ArtifactRecord.last_reclamation_by_agent` dict, keyed by
      `agent_id` (the artifact is implicit in the record location).
   3. `_validate_single_writer(artifact_id)`. Asserts
      `check_single_writer` holds post-reclaim.

### 4.3 Reclamation memory and the slot-survives-SHARED rule

The `last_reclamation_by_agent` slot is the diagnostic that powers
`commit()`'s `reclaimed_by=...` error message. Its lifecycle is:

- **Written** by step 4.2 above (reclamation).
- **Cleared** by `set_agent_state` only on `M ∪ E` re-acquire — i.e.,
  when a previously-reclaimed agent acquires a fresh grant via
  `coordinator.write()`. Transitioning from `INVALID` to `SHARED`
  (e.g., a recovered agent doing a `coordinator.fetch()` to read
  current state) **preserves** the slot. This is the
  **jessieibarra path**: a checkpoint-restored agent that fetches
  before attempting commit must still see the diagnostic message.

### 4.4 Reclamation-aware `commit()` error

When `coordinator.commit(agent_id, artifact_id, ...)` is called and
the agent's state is `INVALID`, the coordinator consults
`registry.get_last_reclamation(agent_id, artifact_id)`:

- If it returns `(trigger, tick)`, raise
  `CoherenceError("commit_not_allowed agent=<id> artifact=<id>
  state=INVALID reclaimed_by=<trigger> at_tick=<tick>")`.
- If it returns `None` (the agent is `INVALID` for an unrelated
  reason — never held a grant, or its slot was cleared by a later
  re-acquire), raise the original message format. This preserves
  R5 byte-identity for unrelated `INVALID` paths.

`coordinator.write()` is **not** enriched. In the existing protocol,
`write()` is an unconditional acquire — a stale-grant agent calling
`write()` re-acquires `EXCLUSIVE`, which clears the reclamation slot
via the M∪E-acquire rule. The diagnostic enrichment correctly lives
on `commit()` because that is where stale-work intent shows up.

---

## 5. Invariants

These are the safety properties the implementation guarantees and
that the future TLA+ amendment must formalize. All hold under
preconditions §2.

| ID | Name | Statement |
|---|---|---|
| **I1** | **SingleWriter** | At every state, at most one agent holds `MODIFIED ∪ EXCLUSIVE` per artifact. Reclamation transitions M or E to INVALID, which only reduces the set. |
| **I2** | **MonotonicVersion** | `artifact.version` never decreases, and reclamation never increases it (no version bump on reclaim). |
| **I3** | **SweepExclusivity** | A given (agent, artifact) pair is reclaimed by at most one sweep per tick. The transient sweep runs first; the stable sweep snapshots state after it completes and skips agents currently in any transient state. |
| **I4** | **TriggerExclusivity** | Each reclamation produces exactly one state-log entry with exactly one trigger. A grant violating both heartbeat and max-hold conditions is logged as `reclaim_heartbeat`, never both. |
| **I5** | **TickMonotonicity** | `last_heartbeat_tick(agent)` is non-decreasing across calls to `record_heartbeat` (out-of-order delivery is harmless because of the `max(prev, incoming)` rule). |
| **I6** | **SlotPreservedThroughSHARED** | A reclamation slot for `(agent, artifact)` is preserved across `INVALID → SHARED` transitions and only cleared on `INVALID → MODIFIED|EXCLUSIVE` re-acquire. |
| **I7** | **FlagOffByteIdentity** | When `crash_recovery.enabled = False`, the state-transition log is byte-identical to the v0.5 baseline (instance-id-normalized). New trigger strings, new dict mutations, and new accumulators introduce zero log emissions on the flag-off path. |

---

## 6. Adapter contract

The coordinator-side reclamation feature lands behind a feature flag
that defaults to `False`. **For a real adapter (LangGraph, CrewAI,
AutoGen, CCSStore) to safely enable the flag, the adapter must satisfy
the adapter contract defined below.**

Implementation plan:
`docs/plans/2026-05-07-002-feat-adapter-heartbeat-plumbing-plan.md`.
Integration tests: `tests/test_adapter_crash_recovery.py`.

### 6.1 Public surface

#### CoherenceAdapterCore (shared base)

| Method | Signature | Behavior |
|---|---|---|
| `heartbeat` | `heartbeat(*, agent_name: str, now_tick: int) -> None` | Records a heartbeat for the named agent. No-op when `crash_recovery.enabled=False`. |
| `recover` | `recover(*, agent_name: str, now_tick: int) -> None` | Invalidates the agent's full local `ArtifactCache` AND records a heartbeat at `now_tick`. Cache invalidation runs unconditionally; heartbeat is gated on `enabled`. |
| `register_agent` | `register_agent(name: str, *, now_tick: int = 0) -> UUID` | Seeds `last_heartbeat_tick` at `now_tick` when `enabled=True`. |

`read()` and `write()` emit a piggyback heartbeat (one
`coordinator.record_heartbeat` call) before delegating to the runtime
when the flag is on. This is the zero-config liveness baseline.

#### Framework adapters (LangGraph, CrewAI, AutoGen)

Each constructor accepts an optional
`crash_recovery: CrashRecoveryConfig | None = None` kwarg and forwards
it to `CoherenceAdapterCore`. When an externally-built `core` is
supplied, the `crash_recovery` kwarg is ignored — the external core's
configuration wins.

Framework adapter per-step methods (`before_node`/`commit_outputs`,
`prepare_task_context`/`commit_task_artifact`,
`pre_turn_context`/`post_turn_commit`) gain piggyback heartbeat
behavior automatically because they delegate to `core.read`/`core.write`.

#### CCSStore

| Method | Signature | Behavior |
|---|---|---|
| `heartbeat` | `heartbeat(*, agent_name: str, now_tick: int) -> None` | Pass-through to `core.heartbeat(...)`. Both parameters required. |
| `recover` | `recover(*, agent_name: str, now_tick: int) -> None` | Pass-through to `core.recover(...)`. Both parameters required. |

Constructor accepts `crash_recovery: CrashRecoveryConfig | None = None`
and forwards it to the internal `CoherenceAdapterCore`.

`batch()` operations gain piggyback heartbeat via `core.read`/`core.write`.

`now_tick` has no `None` default — callers that want CCSStore's
batch-monotonic tick read `store._tick` and pass it explicitly.

### 6.2 Heartbeat cadence

Call `heartbeat()` at least every `heartbeat_timeout_ticks / 3` ticks
during long compute windows where the adapter's normal per-step methods
(`before_node`, `prepare_task_context`, `pre_turn_context`, `batch`)
are not invoked. Per-step methods piggyback heartbeats automatically;
the explicit `heartbeat()` call is only needed to bridge gaps.

### 6.3 Recovery rule

Call `recover(agent_name, now_tick)` after any process restart,
checkpoint reload, or any case where in-memory agent state may have
diverged from the coordinator's view.

The call:
1. Invalidates the **entire** local `ArtifactCache` for that agent.
   Selective invalidation is unsafe because a fresh process has no
   in-memory record of past grants.
2. Records a heartbeat at `now_tick` so the recovered agent is not
   immediately reclaim-eligible on its next grant acquire.

The application is responsible for invoking `recover()`; the adapter
cannot detect recovery automatically.

**Cross-agent staleness:** `recover()` is per-agent local. Other
agents' caches continue to be kept fresh by normal v0.5 invalidation
events flowing through the event bus — `recover()` does not affect
them.

**Concurrent invalidation ordering:** The application MUST NOT deliver
invalidation events to a runtime while `recover()` is in flight on
that runtime. In practice today this is implicit (no event-bus
threading is documented); this rule makes it explicit. The
implementation does not add a lock — this matches the runtime's
single-threaded posture.

### 6.4 Late-joining agents

When registering an agent in a long-running adapter with the flag
enabled, pass `now_tick` to `register_agent()` so the heartbeat seed
is current. Without this, a fresh agent registered at high tick with
the default `now_tick=0` would be reclaim-eligible immediately.

### 6.5 Composition fail-fast

`CoherenceAdapterCore.__init__` enforces the same R11 composition rule
as the simulation engine: if `crash_recovery.enabled=True` and the
strategy has an inspectable `ttl_ticks >= max_hold_ticks`, startup
raises `ValueError`. Strategies without inspectable TTL (lazy, eager,
access_count) silently accept.

### 6.6 Async callers

All adapters are synchronous this iteration. Async callers (LangGraph
async nodes, CrewAI/AutoGen async tasks) must serialize calls
themselves (e.g., `asyncio.to_thread(adapter.heartbeat, ...)`).
Async-native adapter surface is out of scope.

### 6.7 Dimensioning

Real adapters need to dimension `heartbeat_timeout_ticks` per workload
because adapter ticks may correspond to wall-clock seconds, framework
steps, or LLM calls — the simulation default of `10` is calibrated to
the harness, not to production. The simulation harness reproduces the
false-reclaim-under-sync-compute shape via the `busy` failure-injection
action (`tests/test_engine.py`).

---

## 7. Observability

Reclamation is visible through the existing state-transition log
(`ArtifactRegistry.set_agent_state`'s `state_log` callback). Two new
free-form `trigger` strings are introduced; **no schema change**:

- `reclaim_heartbeat` — grant reclaimed because the holder's
  heartbeat is older than `heartbeat_timeout_ticks` (or never
  recorded).
- `reclaim_max_hold` — grant reclaimed because
  `current_tick - granted_at_tick >= max_hold_ticks`.

`ccs.state_log.v2` already carries `content_hash`, `sequence_number`,
`instance_id`, `schema_version` per entry. Reclamation entries set
`content_hash=None` (no content involved). `validate_log` continues to
detect gaps and schema mismatches.

`SimulationMetrics` gains a `stable_grant_reclamations: int = 0`
field, populated by the engine's per-tick accumulator. This is a
**metric-schema addition** (intentional, recorded in the plan's Risks
table) and is separate from the state-log schema. State-log byte
identity (I7) is preserved.

A future cross-log diagnostic — "what content did agent A see leading
up to its reclamation?" — can be answered by joining the state log
and the v0.5 audit log on `instance_id` and time-ordering by
sequence. No protocol change is needed; the seam is forward-compat
(see origin Carryover risk #5).

---

## 8. Configuration

Configuration is **global on the coordinator** (mirrors the existing
`enforce_transient_timeouts(timeout_ticks=...)` shape). The public
dataclass:

```
@dataclass(frozen=True)
class CrashRecoveryConfig:
    enabled: bool = False
    heartbeat_timeout_ticks: int = 10
    max_hold_ticks: int = 1000
```

Wired into the simulation engine via the optional top-level YAML
block:

```yaml
crash_recovery:
  enabled: false
  heartbeat_timeout_ticks: 10
  max_hold_ticks: 1000
```

Absent block → defaults (`enabled=False`).

**Composition rule (R11) — fail-fast.** If `enabled=True` AND the
strategy has a statically inspectable `ttl_ticks` AND
`max_hold_ticks <= ttl_ticks`, the engine refuses to start with a
`ValueError` naming the strategy class, the offending values, and
the rule. If `ttl_ticks` is non-int (custom strategy with an oddly
typed attribute), a `RuntimeWarning` is emitted. If the strategy has
no `ttl_ticks` attribute (lazy, eager, access_count, broadcast),
the rule silently accepts.

**Flag-off guard.** When `enabled=False`, the sweep is **never**
invoked at the call site. Heartbeat emission is also gated. The new
dict mutations inside `set_agent_state` (granted_at_tick lifecycle,
last_reclamation eviction on M∪E acquire) run unconditionally, but
they are dict updates only — no log emission, no serialization — so
flag-off byte-identity (I7) holds.

---

## 9. TLA+ exit criteria

The implementation ships the spec doc and a feature-flag
implementation now. The following formal-verification gates MUST be
cleared before either of:

- the feature flag becomes `enabled=True` by default, or
- backlog item H (OCC-style write API) begins formal verification.

**Gate 1 — TLA+ amendment merged.** A TLA+ amendment that formalizes
all invariants in §5 (single-writer, monotonic version, sweep
exclusivity, trigger exclusivity, tick monotonicity, slot survival,
flag-off byte-identity) MUST be merged.

**Gate 2 — H cannot front-run C's formalization.** H's formal
verification cannot begin until C's invariants are formalized. C's
TLA+ amendment establishes the shared model that H extends.

These gates exist so the moat-erosion concern (origin Carryover risk
#1) does not surface in a paper deadline or H planning.

---

## 10. TLA+ appendix — property list (transcription seed)

This appendix maps each invariant in §5 to a TLA+-shaped property,
intended as the transcription source for Gate 1. Each entry names the
property, restates the prose, and sketches its formal shape. The
shapes use an informal notation; the actual TLA+ module will tune the
exact temporal/state-predicate forms.

### I1 — SingleWriter (state predicate)

```
SingleWriter(a) ==
  Cardinality({ ag \in Agents :
    state[a][ag] \in {MODIFIED, EXCLUSIVE} }) <= 1
SingleWriterAlways == [] (\A a \in Artifacts : SingleWriter(a))
```

Holds in the initial state (no agent owns anything). Preserved by
every transition: `register_artifact`, `fetch`, `write`, `commit`,
`invalidate`, `delete`, `enforce_transient_timeouts`, and (new)
`enforce_stable_grant_timeouts`. The new sweep only writes
`INVALID`, which monotonically reduces the M∪E set.

### I2 — MonotonicVersion (temporal □)

```
MonotonicVersion ==
  [] (\A a \in Artifacts :
       version'[a] >= version[a])
ReclaimDoesNotBumpVersion ==
  [] (\A a \in Artifacts :
       (\E ag \in Agents : Reclaimed(ag, a)) =>
         version'[a] = version[a])
```

The new sweep never modifies `version`; only `commit` does.

### I3 — SweepExclusivity (per-tick property)

```
SweepExclusivity(t) ==
  \A (ag, a) :
    (TransientReclaimedAt(ag, a, t)) =>
      ~StableReclaimedAt(ag, a, t)
SweepOrdering ==
  [] \A t \in Ticks : SweepExclusivity(t)
```

The transient sweep at tick `t` runs first and clears any expired
transient. The stable sweep takes its snapshot afterward and skips
any (ag, a) where transient state is non-empty *at the moment of
the snapshot*. Each (ag, a) is reclaimed by at most one sweep per
tick.

### I4 — TriggerExclusivity (state-log property)

```
TriggerExclusivity ==
  \A entry \in StateLog :
    entry.trigger \in TriggerSet /\
    \A entry2 \in StateLog :
      (entry.tick = entry2.tick /\
       entry.agent_id = entry2.agent_id /\
       entry.artifact_id = entry2.artifact_id /\
       entry.trigger \in {"reclaim_heartbeat", "reclaim_max_hold"} /\
       entry2.trigger \in {"reclaim_heartbeat", "reclaim_max_hold"})
      => entry = entry2
```

A grant violating both heartbeat AND max-hold conditions emits
exactly one entry with `trigger = "reclaim_heartbeat"` (the
heartbeat-stale signal preempts max-hold per §4.2 step 3).

### I5 — TickMonotonicity (action property on `record_heartbeat`)

```
RecordHeartbeatPostcondition(ag, t) ==
  last_heartbeat_tick'[ag] = Max(last_heartbeat_tick[ag], t)
TickMonotonicity ==
  [] \A ag \in Agents :
       last_heartbeat_tick'[ag] >= last_heartbeat_tick[ag]
```

Out-of-order delivery (a heartbeat carrying a smaller tick arriving
after a larger one) cannot reduce the recorded value.

### I6 — SlotPreservedThroughSHARED (per-(agent, artifact) action property)

```
SlotPreservedThroughSHARED ==
  \A (ag, a) :
    (state[ag][a] = INVALID /\
     last_reclamation[ag][a] /= None /\
     state'[ag][a] = SHARED)
    => last_reclamation'[ag][a] = last_reclamation[ag][a]

SlotClearedOnMEReacquire ==
  \A (ag, a) :
    (state[ag][a] = INVALID /\
     state'[ag][a] \in {MODIFIED, EXCLUSIVE})
    => last_reclamation'[ag][a] = None
```

Two complementary clauses: SHARED transitions preserve; M∪E
transitions clear. Together they encode the §4.3 rule.

### I7 — FlagOffByteIdentity (refinement / observational equivalence)

```
FlagOffByteIdentity ==
  (crash_recovery.enabled = FALSE) =>
    (\A traces : trace_state_log = trace_state_log_v05_baseline)
```

Express as a refinement: the (`enabled=False`) crash-recovery
implementation refines the v0.5 baseline state-log behavior. No new
log emissions occur on the flag-off path. The new dict mutations on
`granted_at_tick_by_agent` and `last_reclamation_by_agent` happen
unconditionally but are not part of the observable state-log trace,
so refinement holds.

---

## 11. Implementation map

| Component | File | Status |
|---|---|---|
| Registry storage (heartbeat dict, granted_at_tick lifecycle, slot dict) | `src/ccs/coordinator/registry.py` | Shipped (commit `7692b3c`) |
| Coordinator `record_heartbeat` RPC | `src/ccs/coordinator/service.py` | Shipped (commit `7692b3c`) |
| Reclamation sweep + commit-error enrichment | `src/ccs/coordinator/service.py` | Shipped (commit `706e380`) |
| `CrashRecoveryConfig` + composition fail-fast | `src/ccs/coordinator/service.py` | Shipped (commit `dacb67d`) |
| YAML schema + engine `__init__` plumbing | `src/ccs/simulation/scenarios.py`, `src/ccs/simulation/engine.py` | Shipped (commit `dacb67d`) |
| Engine sweep call-site + heartbeat emission + failure injection | `src/ccs/simulation/engine.py` | Shipped (commit `00c574e`) |
| `failure_events` schema (kill / busy / restore) | `src/ccs/simulation/scenarios.py` | Shipped (commit `00c574e`) |
| Combined validation scenario + driver test | `benchmarks/scenarios/crash_recovery_validation.yaml`, `tests/test_crash_recovery.py` | Shipped (commit `4528dfa`) |
| Adapter contract wiring (`heartbeat`, `recover`, framework-adapter constructors) | `src/ccs/adapters/{base,langgraph,crewai,autogen,ccsstore}.py` | Shipped (adapter heartbeat plumbing plan) |
| TLA+ amendment | n/a | Deferred (Gate 1; separate backlog item) |

## 12. Sources

- Origin requirements: `docs/brainstorms/crash-recovery-stale-grants-requirements.md`
- Implementation plan: `docs/plans/2026-05-06-002-feat-crash-recovery-stale-grants-plan.md`
- Adapter follow-up plan: `docs/plans/2026-05-07-002-feat-adapter-heartbeat-plumbing-plan.md`
- Backlog item C in Notion: *Crash-recovery semantics for stale grants*
- Blocks backlog item H (OCC-style write API).
