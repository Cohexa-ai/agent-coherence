# TLA+ Formal Verification

TLC model checking for the MESI coherence protocol, its crash-recovery extension, the optimistic commit-CAS (OCC), and the read-generation fence.

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

## What is deliberately out of scope

| Exclusion | Reason |
|-----------|--------|
| Transient states (ISG, IED, EIA, SIA, MWB, MSA) | Covered by `enforce_transient_timeouts`; do not interact with the crash-recovery sweep beyond the skip rule |
| Network partitions | Deferred until partition-safe reclamation scheme lands |
| Agent-side caching / `ArtifactCache` | Data-plane concern; protocol model is control-plane only |
| Token-savings / cost metrics | Observability, not correctness |
| Strategy-specific behavior (lease TTL, access counts, broadcast) | Strategies compose atop the base protocol; invariants hold regardless of strategy |
| `delete` operation | Artifact deletion invalidates all holders but does not interact with the sweep. Model assumes a fixed artifact set |
| Full liveness proofs | TLC checks safety invariants; liveness is not checked (bounded models make temporal liveness checks infeasible at this scale). OCC's progress / no-starvation obligation is likewise discharged as a safety property (`NoLostUpdate` + a clean no-op conflict) plus a prose argument, not a temporal check |

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
├── lib/
│   └── tla2tools.jar       # committed TLC binary (see version below)
└── README.md               # this file
```

## Running TLC

```bash
# All four specs (recommended)
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
```

Requires Java 17+. CI uses Temurin via `actions/setup-java`.

## Invariants

| ID | TLA+ Name | Checked In | Description |
|----|-----------|-----------|-------------|
| I1 | `SingleWriter` | All four | At most one agent holds M∪E per artifact |
| I2 | `MonotonicVersion` | All four | Artifact version never decreases (≥ 1) |
| — | `TypeOK` / `CRTypeOK` / `OCCTypeOK` / `FencingTypeOK` | All four | State variables have correct types and bounds |
| I3 | `SweepExclusivity` | CrashRecovery, OCC, Fencing | No (agent, artifact) reclaimed twice in one tick |
| I4 | `TriggerExclusivity` | CrashRecovery, OCC, Fencing | Each reclamation has exactly one trigger |
| I5 | `TickMonotonicity` | CrashRecovery, OCC, Fencing | `lastHeartbeat` never decreases |
| I6 | `SlotPreservedThroughSHARED` | CrashRecovery, OCC, Fencing | Reclamation slot persists across I→S, cleared only on I→M∪E |
| — | `NoLostUpdate` | OCC | No successful `commit_cas` ever landed on a stale observed version — the concurrent lost-update is prevented |
| — | `ReadGenBounded` | Fencing | A captured read-generation never exceeds the artifact's current ownership epoch |
| — | `NoStaleApply` | Fencing | No commit ever applied a write whose captured read-generation was superseded by a reclamation — the reclaim-zombie write is prevented |

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

Target: **5 minutes** total across the four specs.

| Model | Config | Agents | Artifacts | MaxTicks | Distinct States | Wall Time |
|-------|--------|--------|-----------|----------|----------------|-----------|
| MESI_Standalone | `MESI_Standalone.cfg` | 3 | 2 | 12 | 557,037 | ~18s |
| CrashRecovery (CI) | `CrashRecovery_CI.cfg` | 2 | 1 | 6 | 258,854 | ~18s |
| CrashRecovery (local) | `CrashRecovery.cfg` | 3 | 2 | 12 | — | ~30+ min |
| OCC (CI) | `OCC_CI.cfg` | 2 | 1 | 4 | 1,372,720 | ~47s |
| OCC (local) | `OCC.cfg` | 3 | 1 | 6 | — | minutes |
| Fencing (CI) | `Fencing_CI.cfg` | 2 | 1 | 4 | 2,832,014 | ~67s |
| Fencing (local) | `Fencing.cfg` | 3 | 1 | 6 | — | minutes |

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

These mutations are run manually during development to validate TLC's
bug-detection capability. The mutated files are not committed.
