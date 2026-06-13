# TLA+ Formal Verification

TLC model checking for the MESI coherence protocol, its crash-recovery extension, the optimistic commit-CAS (OCC), the read-generation fence, and bounded version retention with read-at-version.

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
- **Bounded version retention + read-at-version (Retention)** — a per-artifact K-bounded history of committed versions (`history`, content abstracted as the version number), extended and garbage-collected atomically inside the fence-guarded commit (`RetentionCommitAction` — commit + retain + K-GC are one action, mirroring the same-transaction capture discipline), plus an off-protocol read-at-version request (`VersionedReadAction`) proven to be a protocol no-op. Every inherited invariant is re-checked with retention composed in — safety **preservation**, not behavioral equivalence (no refinement mapping). Corresponds to the retention capture points and `CoordinatorService.read_at_version` (plan: `docs/plans/2026-06-10-001-feat-version-retention-read-at-version-plan.md`).

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
├── Retention.tla           # amendment: EXTENDS Fencing, adds bounded retention + read-at-version
├── Retention.cfg           # TLC config: 3 agents (local deep runs)
├── Retention_CI.cfg        # TLC config: 2 agents, MaxRetained=2 (CI, fits 5-min budget)
├── lib/
│   └── tla2tools.jar       # committed TLC binary (see version below)
└── README.md               # this file
```

## Running TLC

```bash
# All five specs (recommended)
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
```

Requires Java 17+. CI uses Temurin via `actions/setup-java`.

## Invariants

| ID | TLA+ Name | Checked In | Description |
|----|-----------|-----------|-------------|
| I1 | `SingleWriter` | All five | At most one agent holds M∪E per artifact |
| I2 | `MonotonicVersion` | All five | Artifact version never decreases (≥ 1) |
| — | `TypeOK` / `CRTypeOK` / `OCCTypeOK` / `FencingTypeOK` / `RetentionTypeOK` | All five | State variables have correct types and bounds (Retention additionally pins the history domain ⊆ `1..MaxVersion`, row count ≤ `MaxRetained`, and the marker-is-the-version content abstraction) |
| I3 | `SweepExclusivity` | CrashRecovery, OCC, Fencing, Retention | No (agent, artifact) reclaimed twice in one tick |
| I4 | `TriggerExclusivity` | CrashRecovery, OCC, Fencing, Retention | Each reclamation has exactly one trigger |
| I5 | `TickMonotonicity` | CrashRecovery, OCC, Fencing, Retention | `lastHeartbeat` never decreases |
| I6 | `SlotPreservedThroughSHARED` | CrashRecovery, OCC, Fencing, Retention | Reclamation slot persists across I→S, cleared only on I→M∪E |
| — | `NoLostUpdate` | OCC | No successful `commit_cas` ever landed on a stale observed version — the concurrent lost-update is prevented |
| — | `ReadGenBounded` | Fencing, Retention | A captured read-generation never exceeds the artifact's current ownership epoch |
| — | `NoStaleApply` | Fencing, Retention | No commit ever applied a write whose captured read-generation was superseded by a reclamation — the reclaim-zombie write is prevented. Re-checked in Retention to prove retention preserves the fence |
| — | `NoCollectedRead` | Retention | No versioned read ever observed a hole inside the promised K-window strictly below the current version — the GC never collects what the bounded-retention contract promises (current version included, by construction) |
| — | `ReadAtVersionIsProtocolNoOp` | Retention (action property, cfg `PROPERTY`) | `[][VersionedReadAction => UNCHANGED fenceVars]_retentionVars` — any transition satisfying the read action changes no MESI/crash-recovery/fence variable. A state invariant cannot express this: a fence-refreshing read is extensionally identical to a legitimate `ObserveGenAction`, so only an action-level check can catch it |

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
| `RetentionCommitAction` | the version-bumping registry capture points — `set_artifact_and_content` and `commit_cas` WIN — retaining + inline K-GC (`collectible_versions`) in the same transaction / apply step as the commit; `register_artifact`'s capture is the model's initial state (plan Units 2–3) |
| `VersionedReadAction` | `CoordinatorService.read_at_version()` — off-protocol read; never calls `set_agent_state`/`set_agent_transient`, so no fence capture and no MESI transition (plan Unit 4) |
| `NoCollectedRead` | bounded-retention parity suite (`tests/test_retention.py`, plan Units 2–5) |
| `ReadAtVersionIsProtocolNoOp` | fence non-capture + MESI non-interaction regression tests (plan Unit 4) |

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

Target: **5 minutes** total across the five specs (measured 4min 32s sequential on a machine that reproduces the reference Fencing time — snug; treat further spec additions as needing their own budget review).

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

Retention's distinct-state count **equals** Fencing's by design: the retained history is a
deterministic function of the version window (content abstracted as the version number)
and the read action is a stutter in the correct spec, so retention adds transitions and
per-transition checks (~1.7× Fencing's wall time; 30,483,363 generated vs 28,142,923)
but zero state-space dimensions. The local 3-agent config is overnight-class, not a
quick check: measured ≥95M distinct states (703M generated, queue still growing) at the
40-minute mark on 8 cores — and since the distinct space equals Fencing's, that is also
the true size of `Fencing.cfg`'s local space.

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

These mutations are run manually during development to validate TLC's
bug-detection capability. The mutated files are not committed. Recipes 5–8
run TLC directly on `Retention_CI.cfg` (mutating `Retention.tla` cannot affect
the other four specs, so the full `make tla-check` adds nothing).
