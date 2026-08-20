# TLA+ Formal Verification

TLC model checking for the MESI coherence protocol, its crash-recovery extension, the optimistic commit-CAS (OCC), the read-generation fence, the effect-ordering gate, bounded version retention with read-at-version, consistent multi-artifact snapshot sessions, atomic multi-artifact publish, and workspace-restore registration.

## What is modeled

- **Stable MESI transitions** — the four coordinator operations (`fetch`, `write`, `commit`,
  `invalidate`) and their side-effects on peer agents. Corresponds to
  `src/ccs/coordinator/service.py`.
- **Crash-recovery sweep** — heartbeat-timeout and max-hold reclamation with first-match
  trigger ordering. Corresponds to `enforce_stable_grant_timeouts` in `service.py`.
- **Heartbeat liveness** — monotonic heartbeat recording per agent.
- **Reclamation slot lifecycle** — slot preserved through I→S, cleared on I→M∪E re-acquire.
- **Optimistic commit-CAS (OCC)** — a version-checked commit (`commit_cas`) that bypasses the pessimistic acquire: an S/I writer reads the version (`ObserveAction`), then commits only if its observed version still matches and no other agent holds M∪E. Closes the concurrent lost-update. Corresponds to `commit_cas` in `src/ccs/coordinator/`.
- **Read-generation fence (Fencing)** — a per-artifact ownership epoch (`ownerGeneration`) bumped atomically on every sweep reclamation, captured into `readGeneration` when an agent establishes its write-claim (`ObserveGenAction` — deliberately decoupled so the sweep can interleave between capture and commit), and enforced by a generation-guarded commit (`FencingCommitAction`): a writer whose captured generation was superseded by a reclamation is rejected even when the version is unchanged — the reclaim-zombie write the version CAS cannot see. Corresponds to `owner_generation` / `read_generation` in `src/ccs/coordinator/`.
- **Effect-ordering gate (EffectGate)** — the builder-facing `gate()` wrapper modeled over the fence world: a per-artifact gate machine captures the `(version, ownerGeneration)` pair at decision time (`EffectDecideAction` — one atomic read, matching the registry's pair-atomic `get_version_and_generation`), re-validates it at the effect boundary (`EffectAdmitAction`, guarded on BOTH comparands), HOLDs on a moved pair (`EffectHoldAction`), and fires from an unguarded separate action (`EffectFireAction`) — the admit/fire split keeps the disclaimed re-validate→fire residual window model-visible instead of proving it away. Proves `NoStaleAdmit`: the gate never admits an effect whose captured pair had moved as of the re-validate read — in particular the reclaim case, where a sweep bumps the generation while the version never moves. Two exact abstractions keep it CI-convergent: moved-since-capture (a boolean; both comparands are monotonic, so equality-with-captured IS unchanged-since-capture — no ABA) instead of concrete captured values, and one gate machine per artifact (the admit consults only per-artifact registry state; the caller's identity never enters the check). Corresponds to `gate()` in `src/ccs/adapters/effect_gate.py`.
- **Bounded version retention + read-at-version (Retention)** — a per-artifact K-bounded history of committed versions (`history`, content abstracted as the version number), extended and garbage-collected atomically inside the fence-guarded commit (`RetentionCommitAction` — commit + retain + K-GC are one action, mirroring the same-transaction capture discipline), plus an off-protocol read-at-version request (`VersionedReadAction`) proven to be a protocol no-op. Every inherited invariant is re-checked with retention composed in — safety **preservation**, not behavioral equivalence (no refinement mapping). Corresponds to the retention capture points and `CoordinatorService.read_at_version`.
- **Consistent multi-artifact snapshot sessions (Snapshot)** — a session captures a per-artifact version-vector at ONE atomic linearization point (`BeginSessionAction`), reads a coherent cut with no cross-artifact read skew (`NoReadSkewWithinCut`), and holds its pinned versions against the K-bounded GC for its lifetime via the exemptions seam (`PinAlwaysRetained` — `SnapRetainAndCollect` keeps the newest-K window ∪ live-session pins, with session liveness the state the GC reads). Single-artifact commits only — atomic multi-artifact *publish* is modeled separately in `AtomicPublish.tla` (below). The read-skew detector lives in the commit and is vacuous under atomic capture; the split mutant gives it teeth (the `staleApply`/`collectedRead` idiom). Corresponds to `begin_session` / the read-side transaction layer.
- **Atomic multi-artifact publish (AtomicPublish)** — a write-set-quantified commit action (no other spec in the chain has one; all inherited commits are single-`(agent, artifact)`): a batch of members either all advance to their next version atomically or none do (`NoPartialPublish`), and no peer observes an INVALID for a member of a batch that did not commit (broadcast-after-commit). `EXTENDS OCC` — the write race lives in the version-CAS, which keeps the state space lighter than extending Snapshot. Corresponds to `commit_all` in `src/ccs/coordinator/`. **Held out of the `make tla-check` CI sweep:** the write-set state space (≈ 2^|Artifacts| × MaxVersion^|Artifacts|) exceeds the 5-minute CI budget on the reference machine; the spec parses and the invariant is checkable on a smaller/longer local run, with CI-budget convergence tuning tracked as a follow-up.
- **Workspace-restore registration (WorkspaceVersion)** — the coordinator-registration layer of the Workspace-Versioning restore engine, modeled as the registration code implements it: the checkpoint status marker machine (`none → in_progress → registered → concluded`, `registered` one-shot and reachable only via a committed/empty registration), the member-class partition (written file members → one `commit_all` batch; S3 members manifest-side; deletes as records; the empty write-set as a no-commit transition), the hash-equality already-registered filter, the comparand-gap HELD → bounded re-drive, the fence-terminal refusal, crash/resume at every step, and a concurrent registered-writer bump. Proves `NoPartialRestoreRegistered` (all-or-nothing registration), `NoVersionRegression` (restore-as-forward-commit — old bytes at a strictly increasing version, never a rollback), and `ExactlyOnceRegistration` (the `registered` marker ∨ the hash filter across every crash window). **STANDALONE** — deliberately not `EXTENDS AtomicPublish`: `commit_all`'s internal all-or-nothing is AtomicPublish's proven theorem (`NoPartialPublish`) and the per-leg CAS arbitration is OCC's `NoLostUpdate`, so `commit_all` enters this model as ONE atomic action, keeping the state space CI-convergent (extending AtomicPublish would inherit exactly the state space that holds it out of the sweep). Corresponds to `WorkspaceVersioner._restore_locked` / `_registration_seam` / `_drive_registration` in `src/ccs/adapters/workspace.py` and `CoordinatorService.register_workspace_restore` in `src/ccs/coordinator/service.py`.

## What is deliberately out of scope

| Exclusion | Reason |
|-----------|--------|
| Transient states (ISG, IED, EIA, SIA, MWB, MSA) | Covered by `enforce_transient_timeouts`; do not interact with the crash-recovery sweep beyond the skip rule. Note the fence-coverage nuance: the implementation's `trigger="timeout"` transient eviction does **not** bump `owner_generation` — Fencing and Retention model only the sweep triggers (heartbeat / max_hold) and claim no eviction coverage beyond them |
| Coordinator restart / epoch reset | In TLA+ the model state IS the durable truth, so "lost in-memory mirror" has no analog (`Fencing.tla` restart exclusion). Retention inherits this: restart-survival of retained rows is an implementation/test property (replay-resolver restart proof), not modelable here |
| Network partitions | Deferred until partition-safe reclamation scheme lands |
| Agent-side caching / `ArtifactCache` | Data-plane concern; protocol model is control-plane only |
| Token-savings / cost metrics | Observability, not correctness |
| Strategy-specific behavior (lease TTL, access counts, broadcast) | Strategies compose atop the base protocol; invariants hold regardless of strategy |
| `delete` operation | Artifact deletion invalidates all holders but does not interact with the sweep. `Artifacts` is a CONSTANT throughout the chain — live-membership guards on every inherited action would be a disproportionate rewrite. Delete-drops-history (cascade) is owned by Python tests |
| `register_artifact` | No register action exists anywhere in the chain; Retention's initial state retains version 1, covering the trivial case. Per-capture-point coverage is owned by the Python parity suite |
| Retention policy changes across runs | `MaxRetained` (K) is a per-run CONSTANT; a re-opened store is equivalent to a fresh bounded store. Policy persistence/toggles are owned by Python tests |
| Behavioral equivalence (refinement mapping) | `Retention.tla` proves safety **preservation** — the inherited invariants re-checked with retention enabled — not behavioral equivalence to Fencing, which would need a refinement mapping |
| Cross-**session** write skew (two sessions each read a coherent cut, then each commit a *different* artifact, interleaving) | Session commits validate per-artifact against the pinned base through the inherited version-CAS; a serialization anomaly across two sessions writing disjoint artifacts is not prevented. Distinct from atomic multi-artifact *publish* (one agent committing N artifacts as a unit), which **is** modeled — see `AtomicPublish.tla` / `NoPartialPublish` above |
| Session read-serve / `session.commit` paths | The session read is a pure lookup of the pinned `snapshot[s]`; `session.commit` rides the inherited version-CAS. Neither adds protocol state, so both are owned by the Python suite; `Snapshot.tla` models the cut and its GC-safety |
| Full liveness proofs | TLC checks safety invariants; liveness is not checked (bounded models make temporal liveness checks infeasible at this scale). OCC's progress / no-starvation obligation is likewise discharged as a safety property (`NoLostUpdate` + a clean no-op conflict) plus a prose argument, not a temporal check. The Retention action property `[][...]_v` is a safety-shaped action check, not a liveness check |

## File layout

```
formal/tla/
├── MESI.tla               # base protocol actions (library, no Spec)
├── MESI_Standalone.tla     # standalone wrapper with Next + Spec
├── MESI_Standalone.cfg     # TLC config: 3 agents, 2 artifacts, MaxTicks=12
├── CrashRecovery.tla       # amendment: EXTENDS MESI, adds sweep + heartbeat
├── CrashRecovery.cfg       # TLC config: 3 agents (local deep runs)
├── CrashRecovery_CI.cfg    # TLC config: 2 agents (CI, fits 5-min budget)
├── OCC.tla                 # amendment: EXTENDS CrashRecovery, adds commit-CAS
├── OCC.cfg                 # TLC config: 3 agents (local deep runs)
├── OCC_CI.cfg              # TLC config: 2 agents (CI, fits 5-min budget)
├── Fencing.tla             # amendment: EXTENDS CrashRecovery, adds the read-generation fence
├── Fencing.cfg             # TLC config: 3 agents (local deep runs)
├── Fencing_CI.cfg          # TLC config: 2 agents (CI, fits 5-min budget)
├── EffectGate.tla          # amendment: EXTENDS Fencing, adds the effect-ordering gate machine
├── EffectGate.cfg          # TLC config: 3 agents (local deep runs)
├── EffectGate_CI.cfg       # TLC config: 2 agents, MaxTicks=3, HeartbeatTimeout=1 (CI; see budget notes)
├── Retention.tla           # amendment: EXTENDS Fencing, adds bounded retention + read-at-version
├── Retention.cfg           # TLC config: 3 agents (local deep runs)
├── Retention_CI.cfg        # TLC config: 2 agents, MaxRetained=2 (CI, fits 5-min budget)
├── Snapshot.tla            # amendment: EXTENDS Retention, adds consistent multi-artifact snapshot sessions
├── Snapshot.cfg            # TLC config: 2 agents, 2 artifacts (local deep runs)
├── Snapshot_CI.cfg         # TLC config: 1 agent, 2 artifacts, MaxTicks=2 (CI; cross-artifact cut needs >= 2 artifacts)
├── AtomicPublish.tla       # amendment: EXTENDS OCC, adds the write-set-quantified atomic commit (NoPartialPublish)
├── AtomicPublish.cfg       # TLC config: 2 agents, 2 artifacts (local deep runs)
├── AtomicPublish_CI.cfg    # TLC config: 2 agents, 2 artifacts (held out of make tla-check pending convergence tuning)
├── WorkspaceVersion.tla    # STANDALONE: workspace-restore registration (marker machine, hash filter, fence, crash/resume)
├── WorkspaceVersion.cfg    # TLC config: 2 members, MaxVersion=4, MaxAttempts=2 (local deep runs)
├── WorkspaceVersion_CI.cfg # TLC config: 2 members, MaxVersion=3, MaxAttempts=1 (CI, ~5s)
├── lib/
│   └── tla2tools.jar       # committed TLC binary (see version below)
└── README.md               # this file
```

## Running TLC

```bash
# All CI-swept specs (recommended)
make tla-check

# Individual models
java -XX:+UseParallelGC -cp formal/tla/lib/tla2tools.jar tlc2.TLC \
  -config formal/tla/MESI_Standalone.cfg formal/tla/MESI_Standalone.tla -workers auto

java -XX:+UseParallelGC -cp formal/tla/lib/tla2tools.jar tlc2.TLC \
  -config formal/tla/CrashRecovery.cfg formal/tla/CrashRecovery.tla -workers auto

java -XX:+UseParallelGC -cp formal/tla/lib/tla2tools.jar tlc2.TLC \
  -config formal/tla/OCC_CI.cfg formal/tla/OCC.tla -workers auto

java -XX:+UseParallelGC -cp formal/tla/lib/tla2tools.jar tlc2.TLC \
  -config formal/tla/Fencing_CI.cfg formal/tla/Fencing.tla -workers auto

java -XX:+UseParallelGC -cp formal/tla/lib/tla2tools.jar tlc2.TLC \
  -config formal/tla/Retention_CI.cfg formal/tla/Retention.tla -workers auto

java -XX:+UseParallelGC -cp formal/tla/lib/tla2tools.jar tlc2.TLC \
  -config formal/tla/Snapshot_CI.cfg formal/tla/Snapshot.tla -workers auto

java -XX:+UseParallelGC -cp formal/tla/lib/tla2tools.jar tlc2.TLC \
  -config formal/tla/WorkspaceVersion_CI.cfg formal/tla/WorkspaceVersion.tla -workers auto
```

Requires Java 17+. CI uses Temurin via `actions/setup-java`.

## Invariants

| ID | TLA+ Name | Checked In | Description |
|----|-----------|-----------|-------------|
| I1 | `SingleWriter` | All seven | At most one agent holds M∪E per artifact |
| I2 | `MonotonicVersion` | All seven | Artifact version never decreases (≥ 1) |
| — | `TypeOK` / `CRTypeOK` / `OCCTypeOK` / `FencingTypeOK` / `EffectGateTypeOK` / `RetentionTypeOK` / `SnapshotTypeOK` | All seven | State variables have correct types and bounds (Retention pins the history domain ⊆ `1..MaxVersion`, row count ≤ `MaxRetained`, and the marker-is-the-version abstraction; Snapshot relaxes the row-count bound to `MaxRetained + |Sessions|` for the exemptions seam and types the session vars) |
| I3 | `SweepExclusivity` | CrashRecovery, OCC, Fencing, Retention, Snapshot | No (agent, artifact) reclaimed twice in one tick |
| I4 | `TriggerExclusivity` | CrashRecovery, OCC, Fencing, Retention, Snapshot | Each reclamation has exactly one trigger |
| I5 | `TickMonotonicity` | CrashRecovery, OCC, Fencing, Retention, Snapshot | `lastHeartbeat` never decreases |
| I6 | `SlotPreservedThroughSHARED` | CrashRecovery, OCC, Fencing, Retention, Snapshot | Reclamation slot persists across I→S, cleared only on I→M∪E |
| — | `NoLostUpdate` | OCC | No successful `commit_cas` ever landed on a stale observed version — the concurrent lost-update is prevented |
| — | `ReadGenBounded` | Fencing, EffectGate, Retention, Snapshot | A captured read-generation never exceeds the artifact's current ownership epoch |
| — | `NoStaleApply` | Fencing, EffectGate, Retention, Snapshot | No commit ever applied a write whose captured read-generation was superseded by a reclamation — the reclaim-zombie write is prevented. Re-checked in Retention to prove retention preserves the fence |
| — | `NoStaleAdmit` | EffectGate | The effect gate never ADMITS an escaping effect whose captured `(version, ownerGeneration)` pair had moved as of the re-validate read — the reclaim-zombie *effect* (generation moved, version unchanged) is held. Scoped to the re-validate point; the admit→fire residual window is deliberately unguarded and model-visible (recipe 15 gives it teeth) |
| — | `PairMovedScoped` | EffectGate | Canonicalization sanity: the moved-since-capture flag is FALSE outside the decided window, so the abstraction adds no spurious state |
| — | `NoCollectedRead` | Retention, Snapshot | No versioned read ever observed a hole inside the promised K-window strictly below the current version — the GC never collects what the bounded-retention contract promises (current version included, by construction) |
| — | `ReadAtVersionIsProtocolNoOp` | Retention (action property, cfg `PROPERTY`) | `[][VersionedReadAction => UNCHANGED fenceVars]_retentionVars` — any transition satisfying the read action changes no MESI/crash-recovery/fence variable. A state invariant cannot express this: a fence-refreshing read is extensionally identical to a legitimate `ObserveGenAction`, so only an action-level check can catch it |
| — | `NoReadSkewWithinCut` | Snapshot | No commit ever interleaved a partially-captured session — every session reads a coherent multi-artifact cut. Atomic capture makes a partial cut unreachable (vacuous-TRUE in the correct spec); the split mutant (recipe 9) gives it teeth — the `staleApply`/`collectedRead` sticky-flag idiom |
| — | `PinAlwaysRetained` | Snapshot | Every version a live session pinned is still in the artifact's retained history — the K-bounded GC never collects a pin out from under its session (the exemptions seam: `SnapRetainAndCollect` keeps window ∪ live-session pins; recipe 10 gives it teeth) |
| — | `NoPartialPublish` | AtomicPublish (not in the CI sweep — see CI time budget) | No reachable state where a strict, non-empty subset of a batch's members advanced, and no peer observes an INVALID for a member of a batch that did not commit — a torn multi-artifact publish is unreachable. The split mutant (recipe 11) gives it teeth |
| — | `WVTypeOK` / `RegisteredImpliesTerminal` | WorkspaceVersion | State types + registration well-formedness: an armed batch is non-empty/unanswered/in a live run, and the `registered`/`concluded` statuses are reachable only after every member holds a terminal outcome |
| — | `NoPartialRestoreRegistered` | WorkspaceVersion | No restore registration ever landed a strict, non-empty subset of its commit batch — the WV-layer analog of `NoPartialPublish`, one level up (recipe 12 gives it teeth) |
| — | `NoVersionRegression` | WorkspaceVersion | A restored member's coordinator version strictly increases while carrying OLD bytes (restore-as-forward-commit) — never a decrement; mirrors `check_monotonic_version` defense-in-depth (recipe 14 gives it teeth) |
| — | `ExactlyOnceRegistration` | WorkspaceVersion | No registration commit ever re-bumped a member already at its manifest fingerprint — the `registered` marker ∨ the hash filter covers every crash window, incl. crash-after-commit-before-marker and crash-after-marker-before-conclude (recipe 13 gives it teeth) |

"All seven" above = the MESI-chain specs (`MESI_Standalone` … `Snapshot`, plus `EffectGate` off the Fencing branch); `WorkspaceVersion` is a **standalone** module (see its base-choice header) with its own type/monotonicity analogs, listed separately.

I7 (FlagOffByteIdentity) is a code-level property and is not modelable in TLA+.

## Relationship to implementation

| TLA+ | Implementation |
|------|---------------|
| `FetchAction` | `CoordinatorService.fetch()` in `src/ccs/coordinator/service.py` |
| `WriteAction` | `CoordinatorService.write()` / `upgrade()` |
| `CommitAction` | `CoordinatorService.commit()` |
| `InvalidateAction` | `CoordinatorService.invalidate()` |
| `SweepAction` | `CoordinatorService.enforce_stable_grant_timeouts()` |
| `HeartbeatAction` | `CoordinatorService.record_heartbeat()` |
| `ObserveAction` | the OCC read supplying `expected_version` (`ArtifactCacheEntry.local_version`) |
| `CommitCASAction` | `commit_cas()` — registry CAS + `CoordinatorService.commit_cas` |
| `States` | `MESIState` enum in `src/ccs/core/states.py` |
| `SingleWriter` | `check_single_writer()` in `src/ccs/core/invariants.py` |
| `MonotonicVersion` | `check_monotonic_version()` in `src/ccs/core/invariants.py` |
| `NoLostUpdate` | concurrent-writer test (`tests/test_occ_commit_cas.py`) |
| `ObserveGenAction` | `read_generation` capture in `set_agent_state` (fetch / E∪M acquire) |
| `FencingSweepAction` | the `owner_generation` bump on reclaim triggers in `set_agent_state` |
| `FencingCommitAction` | the generation guard in `commit_cas` + `set_artifact_and_content(fence_agent_id=…)` |
| `NoStaleApply` | dual-registry parity + regression suite (`tests/test_fencing.py`) |
| `EffectDecideAction` | `gate()`'s decision read — `CoherentVolume.read_with_version_generation()` → registry `get_version_and_generation()` (one pair-atomic snapshot on both registry arms) |
| `EffectAdmitAction` / `EffectHoldAction` | `gate()`'s effect-boundary re-read + pair comparison; the HOLD is the raised `StaleView` carrying the version and generation drift |
| `EffectFireAction` | `effect(decision)` after the admit — the disclaimed residual re-validate→fire window (unguarded in the model on purpose) |
| `NoStaleAdmit` | the wrapper regression suite (`tests/adapters/test_effect_gate_wrapper.py`: the strict fire-through-deny leg and the warn moved-epoch leg) |
| `RetentionCommitAction` | the version-bumping registry capture points — `set_artifact_and_content` and `commit_cas` WIN — retaining + inline K-GC (`collectible_versions`) in the same transaction / apply step as the commit; `register_artifact`'s capture is the model's initial state |
| `VersionedReadAction` | `CoordinatorService.read_at_version()` — off-protocol read; never calls `set_agent_state`/`set_agent_transient`, so no fence capture and no MESI transition |
| `NoCollectedRead` | bounded-retention parity suite (`tests/test_retention.py`) |
| `ReadAtVersionIsProtocolNoOp` | fence non-capture + MESI non-interaction regression tests |
| `BeginSessionAction` | `begin_session(read_set)` — the atomic consistent-cut capture; non-mutating, mints no MESI grant |
| `EndSessionAction` | session end / heartbeat-stale release — the pin-lifetime release that re-enables collection |
| `SnapRetainAndCollect` | `collectible_versions(exemptions=…)` — the K-GC keeping the window ∪ live-session pins (the exemptions seam) |
| `NoReadSkewWithinCut` | the consistent-cut regression suite — atomic capture across peer commits |
| `PinAlwaysRetained` | the exemptions-seam + session-liveness sweep tests |
| `StartRestore/Crash/Resume` (WorkspaceVersion) | `WorkspaceVersioner._restore_locked` — the status writes, durable member rows, and the run-local `prior_run` flag read from a `registered` status at resume (`src/ccs/adapters/workspace.py`) |
| `SeamPriorRun/SeamEmpty/ReadComparands/BudgetExhausted` | `WorkspaceVersioner._registration_seam` / `_drive_registration` — the member-class partition, the seam-level EMPTY, the bounded re-drive (`MAX_RESTORE_LEG_REDRIVES`) |
| `RegisterCommitAction` | `CoordinatorService.register_workspace_restore` — the hash filter, the service-level EMPTY, ONE `commit_all` batch (WIN/HELD/fence), `check_monotonic_version` defense-in-depth |
| `NoPartialRestoreRegistered` / `ExactlyOnceRegistration` / `NoVersionRegression` | the Unit-5 registration suites — `tests/coordinator/test_workspace_registration.py` (both registries) + the crash/race scenarios in `tests/adapters/test_workspace_versioner.py` |

The model abstracts away transient states — the implementation's
`enforce_transient_timeouts` and transient-skip rule in the sweep are not modeled.
All M∪E holders are sweep-eligible in the model, which is an over-approximation
(checks more behaviors, giving a stronger safety guarantee).

Version is bounded at `MaxVersion == MaxTicks + NumAgents` for finite model checking.
The implementation has no such bound, but the invariant (`version ≥ 1`, monotonically
non-decreasing) holds regardless of the bound.

## TLC version

`tla2tools.jar` v2026.05.04 from [tlaplus/tlaplus](https://github.com/tlaplus/tlaplus/releases).

## CI time budget

Target: **5 minutes** total across the specs run in CI (the original five measured 4min 32s sequential on the reference machine; Snapshot adds ~18s reference-equivalent on a tight 1-agent × 2-artifact config — see the Snapshot note below; WorkspaceVersion adds ~5s — its standalone base keeps the whole model two orders of magnitude below the chain specs, which is the point of the base choice). The budget stays snug; treat further spec additions as needing their own budget review. `AtomicPublish.tla` is deliberately **held out of this budget** — its write-set state space does not converge inside the 5-minute target on the reference machine, so `make tla-check` does not run it (convergence tuning is a tracked follow-up).

| Model | Config | Agents | Artifacts | MaxTicks | Distinct States | Wall Time |
|-------|--------|--------|-----------|----------|----------------|-----------|
| MESI_Standalone | `MESI_Standalone.cfg` | 3 | 2 | 12 | 557,037 | ~18s |
| CrashRecovery (CI) | `CrashRecovery_CI.cfg` | 2 | 1 | 6 | 258,854 | ~18s |
| CrashRecovery (local) | `CrashRecovery.cfg` | 3 | 2 | 12 | — | ~30+ min |
| OCC (CI) | `OCC_CI.cfg` | 2 | 1 | 4 | 1,372,720 | ~47s |
| OCC (local) | `OCC.cfg` | 3 | 1 | 6 | — | minutes |
| Fencing (CI) | `Fencing_CI.cfg` | 2 | 1 | 4 | 2,832,014 | ~67s |
| Fencing (local) | `Fencing.cfg` | 3 | 1 | 6 | — | minutes |
| Retention (CI) | `Retention_CI.cfg` | 2 | 1 | 4 | 2,832,014 | ~115s |
| Retention (local) | `Retention.cfg` | 3 | 1 | 6 | >95M | hours |
| Snapshot (CI) | `Snapshot_CI.cfg` | 1 | 2 | 2 | 375,180 | ~18s |
| Snapshot (local) | `Snapshot.cfg` | 2 | 2 | 4 | — | minutes |
| WorkspaceVersion (CI) | `WorkspaceVersion_CI.cfg` | — | 2 members | MaxVersion=3 | 104,738 | ~5s |
| WorkspaceVersion (local) | `WorkspaceVersion.cfg` | — | 2 members | MaxVersion=4 | 204,216 | ~6s |

Retention's distinct-state count **equals** Fencing's by design: the retained history is a
deterministic function of the version window (content abstracted as the version number)
and the read action is a stutter in the correct spec, so retention adds transitions and
per-transition checks (~1.7× Fencing's wall time; 30,483,363 generated vs 28,142,923)
but zero state-space dimensions. The local 3-agent config is overnight-class, not a
quick check: measured ≥95M distinct states (703M generated, queue still growing) at the
40-minute mark on 8 cores — and since the distinct space equals Fencing's, that is also
the true size of `Fencing.cfg`'s local space.

Snapshot **inverts** the usual CI shape — **1 agent × 2 artifacts** (the other CI specs are 2 agents × 1 artifact). Read skew is a cross-artifact phenomenon, so ≥ 2 artifacts is mandatory; the agent-contention re-check of the inherited fence invariants is already discharged by the other specs and by the local `Snapshot.cfg` (2 agents). The CI config also disables the sweep (`HeartbeatTimeout` > `MaxTicks`) — the session machinery is fence-uniform and adds no sweep interaction, so suppressing it keeps the run to ~18s without losing Snapshot-specific coverage. The local `Snapshot.cfg` (2 agents, `MaxTicks=4`, sweeps on) is the deep composition check and the home for the mutant recipes.

EffectGate multiplies Fencing's space by the gate machine (5 phases × the moved-flag), so its CI config **tightens the tick horizon instead of dropping coverage**: `MaxTicks=3, HeartbeatTimeout=1, MaxHoldTicks=2` keeps every hazard reachable — the mutant (recipe 15) still finds the reclaim-between-capture-and-admit trace in ~1s, which is the reachability proof — while the correct spec converges at 40,015,043 generated / 3,724,900 distinct states in ~1min40s on 8 cores (measured 2026-08-20). The naive encoding with concrete captured comparands diverges (>80M distinct, queue still growing); the moved-since-capture abstraction in the spec header is what makes the spec checkable at all. The local `EffectGate.cfg` (3 agents, `MaxTicks=6`) is overnight-class.

CI uses `CrashRecovery_CI.cfg` (2 agents, MaxTicks=6) to fit the budget.
The full 3-agent config (`CrashRecovery.cfg`) is for local deep runs:

```bash
# Full 3-agent deep run (exceeds CI budget)
java -XX:+UseParallelGC -Xmx8g -cp formal/tla/lib/tla2tools.jar tlc2.TLC \
  -config formal/tla/CrashRecovery.cfg formal/tla/CrashRecovery.tla -workers auto
```

## Mutant testing

To verify TLC catches real bugs, introduce a deliberate invariant-breaking mutation
and confirm TLC finds a counterexample:

1. **SingleWriter mutation**: In `MESI.tla`, comment out the peer invalidation
   in `WriteAction` (change `THEN "I"` to `THEN mesiState[art][peer]`). Run
   `make tla-check`. TLC should fail with a `SingleWriter` violation and print
   a counterexample trace showing two agents simultaneously in M∪E.

2. **MonotonicVersion mutation**: In `MESI.tla`, change `CommitAction`'s version
   update from `version[art] + 1` to `version[art] - 1`. Run `make tla-check`.
   TLC should fail with a `MonotonicVersion` violation.

3. **NoLostUpdate mutation**: In `OCC.tla`, remove the `/\ obs = cur` conjunct
   from `CommitCASAction`'s WIN branch (so a stale commit can win). Run
   `make tla-check`. TLC should fail with a `NoLostUpdate` violation, showing a
   trace where one writer commits on a version another writer already advanced.
   (Verified 2026-06-08: violation found in ~1s.)

4. **NoStaleApply mutation**: In `Fencing.tla`, remove the `/\ rg = og` conjunct
   from `FencingCommitAction`'s WIN branch (so a superseded writer can win). Run
   `make tla-check`. TLC should fail with a `NoStaleApply` violation, showing a
   trace where a sweep-reclaimed writer's commit lands on a bumped ownership
   epoch. (Verified 2026-06-09: violation found in <1s, 570 distinct states.)

5. **Retention atomicity mutation (crash window)**: In `Retention.tla`, split
   `RetentionCommitAction`'s retain from its version bump into two separately-
   interleavable actions: in the WIN branch replace the
   `LET newVer == ... IN /\ version' = ... /\ history' = RetainAndCollect(art, newVer)`
   block with `/\ version' = [version EXCEPT ![art] = version[art] + 1]`
   `/\ UNCHANGED history`, and add a standalone
   `RetainAction == \E art \in Artifacts : history' = RetainAndCollect(art, version[art]) /\ UNCHANGED <<every other variable>>`
   as a new disjunct of `RetentionNext`. Run TLC on `Retention_CI.cfg`. TLC
   should fail with a `NoCollectedRead` violation: two commits land with no
   retain between them and a versioned read observes the hole inside the
   K-window — the exact crash window the same-transaction capture discipline
   excludes. (Verified 2026-06-11: violation found in ~1s, 3,749 states
   generated.)

6. **Retention fence-refresh mutation**: In `Retention.tla`, make
   `VersionedReadAction` refresh the reader's fence: remove `readGeneration`
   from its `UNCHANGED` tuple, bind a reader (`\E ag \in Agents`), and add
   `readGeneration' = [readGeneration EXCEPT ![ag][art] = ownerGeneration[art]]`.
   Run TLC on `Retention_CI.cfg`. TLC should fail with an
   `Action property ReadAtVersionIsProtocolNoOp is violated` error. Note that
   every state INVARIANT — `NoStaleApply` included — stays green on the
   violating trace: the refreshed claim is extensionally identical to a
   legitimate `ObserveGenAction`, which is exactly why the read-no-op claim is
   checked as an action property. (Verified 2026-06-11: violation found in
   <1s, 1,370 states generated.)

7. **Retention GC-eats-current mutation**: In `Retention.tla`, flip the GC's
   oldest-row selection in `RetainAndCollect` from
   `CHOOSE m \in dom : \A w \in dom : m <= w` to `m >= w` (the GC now drops the
   NEWEST row — the just-committed current version — once the row count
   exceeds `MaxRetained`). Run TLC on `Retention_CI.cfg`. TLC should fail with
   a `NoCollectedRead` violation once a later commit moves the current version
   past the collected one. This also demonstrates the K-eviction path is
   genuinely exercised within the CI bounds. (Verified 2026-06-11: violation
   found in ~2s, 9,892 states generated.)

8. **Retention capture-skip mutation**: In `Retention.tla`, drop the retain
   from `RetentionCommitAction`'s WIN branch
   (`history' = RetainAndCollect(art, newVer)` → `UNCHANGED history`). Run TLC
   on `Retention_CI.cfg`. TLC should fail with a `NoCollectedRead` violation:
   commits advance the version while history still holds only the initial row,
   and a read inside the K-window observes the never-retained version.
   (Verified 2026-06-11: violation found in ~2s, 4,485 states generated.)

9. **Snapshot read-skew mutation (split the atomic capture)**: In `Snapshot.tla`,
   replace `BeginSessionAction`'s one-step capture with a per-artifact capture —
   `\E s \in Sessions, art \in Artifacts : ~sessionLive[s] /\ snapshot[s][art] = None`
   `/\ snapshot' = [snapshot EXCEPT ![s][art] = version[art]] /\ sessionLive' =`
   `[sessionLive EXCEPT ![s] = (\A a \in Artifacts : a = art \/ snapshot[s][a] /= None)]`
   `/\ UNCHANGED <<retentionVars, readSkew>>`. Run TLC on `Snapshot_CI.cfg`. TLC
   should fail with a `NoReadSkewWithinCut` violation: a commit interleaves a
   partially-captured session — the exact read-skew window the atomic capture
   excludes, which no inherited invariant can see. (Verified 2026-06-28:
   violation found in ~1s, 238 distinct states.)

10. **Snapshot exemption-drop mutation (GC eats a pin)**: In `Snapshot.tla`,
   change `SnapRetainAndCollect`'s `keepDom` from
   `(DOMAIN extended) \cap (window \cup PinnedVersions(art))` to
   `(DOMAIN extended) \cap window` (the GC ignores live-session pins). Run TLC on
   `Snapshot_CI.cfg`. TLC should fail with a `PinAlwaysRetained` violation once a
   commit slides the K-window past a pinned version — the exemptions seam the
   correct GC honors. (Verified 2026-06-28: violation found in ~1s, 821 distinct
   states.)

11. **AtomicPublish torn-batch mutation (apply the passing subset)**: In
   `AtomicPublish.tla`, relax `CommitAllAction`'s WIN guard from `winners = ws`
   to `winners /= {}` and apply `winners` instead of `ws` (so a batch where only
   a strict subset of members pass still commits that subset). Run TLC directly
   on `AtomicPublish.cfg` (or `AtomicPublish_CI.cfg`). TLC should fail with a
   `NoPartialPublish` violation, showing a trace where one member of a batch
   advanced while another was held — the torn publish the atomic apply excludes.
   A companion mutant **AP-2** (delete the `observedVersion[ag][art] = version[art]`
   version-CAS line in `MemberCommits`) lets a stale member land in a batch and
   fails the inherited `NoLostUpdate`. Because `AtomicPublish` is held out of the
   CI budget (see CI time budget), run these locally rather than via
   `make tla-check`; they are not yet part of the CI-verified recipe set.

12. **WorkspaceVersion torn-registration mutation (WV-1: apply the passing
   subset)**: In `WorkspaceVersion.tla`, in `RegisterCommitAction`'s WIN
   branch, relax the guard `winners = batch` to `winners /= {}` and change
   `LET applied == batch` to `LET applied == winners` (both lines carry the
   `MUTANT WV-1` marker) — a batch where only a strict subset of members
   passes the version-CAS now commits that subset. Run TLC on
   `WorkspaceVersion_CI.cfg`. TLC should fail with a
   `NoPartialRestoreRegistered` violation: one member of the registration
   batch advanced while a peer-bumped member was held — the torn
   registration the whole-batch atomic apply excludes. (Verified
   2026-08-09: violation found in ~1s, 88,660 states generated.)

13. **WorkspaceVersion double-registration mutation (WV-2: drop the hash
   filter)**: In `WorkspaceVersion.tla`, in `ReadComparandsAction`, change
   `LET batch == { m \in FileWrites : hash[m] /= "fp" }` to
   `LET batch == FileWrites` (the `MUTANT WV-2` marker) — the
   already-at-fingerprint skip is gone, so a member whose commit already
   landed (a coordinator-connected leg, or a crash-resumed run whose prior
   `commit_all` landed before the `registered` marker) is re-committed. Run
   TLC on `WorkspaceVersion_CI.cfg`. TLC should fail with an
   `ExactlyOnceRegistration` violation — the phantom second bump the
   marker + filter pair exists to exclude. (Verified 2026-08-09: violation
   found in ~1s, 36,649 states generated.)

14. **WorkspaceVersion rollback mutation (WV-3: restore-as-rollback)**: In
   `WorkspaceVersion.tla`, in `RegisterCommitAction`'s WIN branch, change
   `ApplyV(m) == version[m] + 1` to `ApplyV(m) == 1` (the `MUTANT WV-3`
   marker) — the registration now "restores" by rolling the coordinator
   version back to the checkpoint's instead of forward-committing old bytes
   at a new version. Run TLC on `WorkspaceVersion_CI.cfg`. TLC should fail
   with a `NoVersionRegression` violation once a peer commit has moved a
   member past version 1 — the silent un-publish the forward-commit
   discipline (`check_monotonic_version`) excludes. (Verified 2026-08-09:
   violation found in ~1s, 74,767 states generated.)

15. **NoStaleAdmit mutation (admit a moved pair)**: In `EffectGate.tla`, delete
    the `/\ ~pairMoved[art]` guard conjunct from `EffectAdmitAction` (so a gate
    whose captured `(version, ownerGeneration)` pair moved can still admit).
    Run TLC on `EffectGate_CI.cfg`. TLC should fail with a `NoStaleAdmit`
    violation, showing a trace where a sweep reclamation (or a peer commit)
    lands between the decide capture and the admit and the effect is admitted
    anyway. (Verified 2026-08-20: violation found in ~1s, 879 distinct states.)

These mutations are run manually during development to validate TLC's
bug-detection capability. The mutated files are not committed. Recipes 5–10
run TLC directly on their amendment's CI config (`Retention_CI.cfg` /
`Snapshot_CI.cfg`); recipe 11 runs on `AtomicPublish.cfg`; recipes 12–14 run
on `WorkspaceVersion_CI.cfg`; recipe 15 runs on `EffectGate_CI.cfg`. Mutating
one amendment cannot affect the other specs, so the full `make tla-check` adds
nothing.
