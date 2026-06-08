# TLA+ Formal Verification

TLC model checking for the MESI coherence protocol and crash-recovery extension.

## What is modeled

- **Stable MESI transitions** — the four coordinator operations (`fetch`, `write`, `commit`,
  `invalidate`) and their side-effects on peer agents. Corresponds to
  `src/ccs/coordinator/service.py`.
- **Crash-recovery sweep** — heartbeat-timeout and max-hold reclamation with first-match
  trigger ordering. Corresponds to `enforce_stable_grant_timeouts` in `service.py`.
- **Heartbeat liveness** — monotonic heartbeat recording per agent.
- **Reclamation slot lifecycle** — slot preserved through I→S, cleared on I→M∪E re-acquire.
- **Optimistic commit-CAS (OCC)** — a version-checked commit (`commit_cas`) that bypasses the pessimistic acquire: an S/I writer reads the version (`ObserveAction`), then commits only if its observed version still matches and no other agent holds M∪E. Closes the concurrent lost-update. Corresponds to the `commit_cas` implementation (see `docs/plans/2026-06-08-001-feat-occ-write-api-same-host-v1-plan.md`).

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
├── lib/
│   └── tla2tools.jar       # committed TLC binary (see version below)
└── README.md               # this file
```

## Running TLC

```bash
# Both models (recommended)
make tla-check

# Individual models
java -XX:+UseParallelGC -cp formal/tla/lib/tla2tools.jar tlc2.TLC \
  -config formal/tla/MESI_Standalone.cfg formal/tla/MESI_Standalone.tla -workers auto

java -XX:+UseParallelGC -cp formal/tla/lib/tla2tools.jar tlc2.TLC \
  -config formal/tla/CrashRecovery.cfg formal/tla/CrashRecovery.tla -workers auto

java -XX:+UseParallelGC -cp formal/tla/lib/tla2tools.jar tlc2.TLC \
  -config formal/tla/OCC_CI.cfg formal/tla/OCC.tla -workers auto
```

Requires Java 17+. CI uses Temurin via `actions/setup-java`.

## Invariants

| ID | TLA+ Name | Checked In | Description |
|----|-----------|-----------|-------------|
| I1 | `SingleWriter` | All three | At most one agent holds M∪E per artifact |
| I2 | `MonotonicVersion` | All three | Artifact version never decreases (≥ 1) |
| — | `TypeOK` / `CRTypeOK` / `OCCTypeOK` | All three | State variables have correct types and bounds |
| I3 | `SweepExclusivity` | CrashRecovery, OCC | No (agent, artifact) reclaimed twice in one tick |
| I4 | `TriggerExclusivity` | CrashRecovery, OCC | Each reclamation has exactly one trigger |
| I5 | `TickMonotonicity` | CrashRecovery, OCC | `lastHeartbeat` never decreases |
| I6 | `SlotPreservedThroughSHARED` | CrashRecovery, OCC | Reclamation slot persists across I→S, cleared only on I→M∪E |
| — | `NoLostUpdate` | OCC | No successful `commit_cas` ever landed on a stale observed version — the concurrent lost-update is prevented |

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

Target: **5 minutes** total for both models.

| Model | Config | Agents | Artifacts | MaxTicks | Distinct States | Wall Time |
|-------|--------|--------|-----------|----------|----------------|-----------|
| MESI_Standalone | `MESI_Standalone.cfg` | 3 | 2 | 12 | 557,037 | ~18s |
| CrashRecovery (CI) | `CrashRecovery_CI.cfg` | 2 | 1 | 6 | 258,854 | ~18s |
| CrashRecovery (local) | `CrashRecovery.cfg` | 3 | 2 | 12 | — | ~30+ min |
| OCC (CI) | `OCC_CI.cfg` | 2 | 1 | 4 | 1,372,720 | ~47s |
| OCC (local) | `OCC.cfg` | 3 | 1 | 6 | — | minutes |

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

These mutations are run manually during development to validate TLC's
bug-detection capability. The mutated files are not committed.
