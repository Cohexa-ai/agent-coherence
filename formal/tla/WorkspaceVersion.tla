------------------------- MODULE WorkspaceVersion -------------------------
(* Workspace-Versioning restore REGISTRATION (WV plan Unit 7 / R10) -- the
   coordinator-registration layer of the convergent restore engine. Proves
   that registering a restore run's written file members is ALL-OR-NOTHING
   (NoPartialRestoreRegistered), that a restore is a FORWARD commit carrying
   old bytes -- versions strictly increase, never a rollback
   (NoVersionRegression) -- and that the registration is EXACTLY-ONCE under
   crash-resume via its two durable idempotency mechanisms: the checkpoint
   `registered` status marker and the service-side hash filter
   (ExactlyOnceRegistration).

   Plan: docs/plans/2026-08-08-001-feat-workspace-versioning-e2e-plan.md
   (Unit 7; the modeled machine is Unit 5 as IMPLEMENTED -- code parity rule).

   THE PROBLEM. A restore drives per-member legs to terminal outcomes, then
   must register the written file members with the coordinator as ONE batch.
   Three hazards live in that step and nowhere else: (1) a torn registration
   -- some committed members' versions bump while the rest of the batch is
   held, leaving the coordinator half-telling the restore; (2) a crashed run
   resuming into a SECOND registration commit -- a phantom double bump for
   the same checkpoint; (3) restore-as-rollback -- the registration
   decrementing a member's coordinator version to the manifest's, silently
   un-publishing every commit since the checkpoint. The implemented protocol
   excludes all three; this spec model-checks that the MACHINE (not just the
   happy path) does.

   THE IMPLEMENTED STATE MACHINE (spec/code parity -- the code is
   authoritative; cross-references below):
     - checkpoint restore status: none -> in_progress -> registered ->
       concluded; `registered` is reachable ONLY after every member is
       terminal and ONLY via a committed/empty registration; a REFUSED
       registration goes in_progress -> concluded directly (NO marker, so a
       resume-before-conclusion may re-attempt); a resume of a `registered`
       checkpoint moves it back to in_progress carrying a RUN-LOCAL prior
       flag (the marker is one-shot; the hash filter is the backstop for a
       second crash inside that window).
     - member outcomes: restored / converged / conflict / held_unconfirmed /
       target_lost / forward_only_skipped -- terminal and absorbing per the
       modality sets in ccs/core/exceptions.py. The model adds a `deleted`
       outcome value abstracting the implementation's (outcome=restored,
       deleted_at_restore=timestamp) delete-leg pair -- representationally
       different, registration-semantically identical (a manifest-side
       record, never a commit member).
     - registration: partition terminal members -> written FILE members
       (no-arbiter tier) enter the commit batch; written S3 members are
       manifest-side only (the coordinator holds no artifact identity for a
       BYO member -- artifact_id NULL by design); deletes are
       deleted_at_restore records; the batch is filtered by HASH EQUALITY
       (an artifact already at the manifest fingerprint is skipped -- the
       already-registered filter, which also covers the first-observation
       mint: resolve_or_register mints v1 CARRYING the fingerprint, so the
       mint IS the registration). A non-empty remainder commits ATOMICALLY:
       every member's version bumps +1 and its hash becomes the manifest
       fingerprint, or NOTHING changes (HELD on version_mismatch -> bounded
       re-drive from fresh comparands; stale_read_generation fence ->
       terminal refusal, never retried). An EMPTY remainder -- at the seam
       (no written file members at all) or after the filter (all skipped) --
       is a no-commit transition straight to `registered`.

   Adds (all state is this module's own -- see the base choice):
     - outcome/tier/version/hash : per-member terminal outcome, member class
       (file vs s3 -- nondeterministic at Init so ONE run covers every
       partition incl. the degenerate delete-only / all-absent / all-s3 /
       empty-write-set cases), the member's coordinator version, and an
       abstracted content hash {"fp","live"} ("fp" = the coordinator
       artifact carries the manifest fingerprint). hash is Init-
       nondeterministic too: "fp" at Init is the unmoved-since-checkpoint /
       first-observation-mint skip case.
     - status/running/prior : the durable checkpoint status vs the RUN-LOCAL
       resume state. CrashAction wipes exactly the run-local variables
       (armed/observed/attempts/regOutcome/prior/fenced/running); status,
       outcomes, versions, hashes are the durable truth and survive.
     - armed/observed/attempts : the comparand gap. The service reads
       expected_version per member THEN commits the batch; a registered
       peer's commit landing in the gap HELDs the batch (version_mismatch)
       and the seam re-drives from fresh comparands, budget-bounded
       (MAX_RESTORE_LEG_REDRIVES=8 in code, MaxAttempts here). The
       sequential per-member reads are modeled as ONE atomic capture: any
       sequentially-mixed cut HELDs exactly when the atomic cut does (WIN
       needs EVERY comparand current), so WIN/HELD reachability is
       identical -- the code's own docstring makes the same argument ("no
       pinned cut: a racing commit simply HELDs; the CAS arbitrates").
     - fenced : the read-generation fence. SupersedeControllerAction is the
       sweep reclaiming this controller's grant mid-run; a fenced
       controller's commit answers a TERMINAL refusal (never re-driven --
       a superseded controller must not retry its way past the fence).
       Run-local: a resumed run re-establishes a fresh claim.
     - PeerCommitAction : a concurrent REGISTERED writer bumping a member
       (+1, new hash "fp" or "live") at any time -- including between
       terminality and registration (the named race) and between crash and
       resume (the documented residual: the re-registration then re-asserts
       the manifest hash over the peer's commit as a FORWARD bump -- never
       silent, never a double). Peer identity carries no state in this
       layer, so one anonymous action subsumes any number of peers.
     - STICKY HISTORY FLAGS (the staleApply/partialPublish idiom --
       detectors vacuous in the correct spec, teeth from the README
       mutants): partialRestoreRegistered (a registration ever landed SOME
       committed member's bump without ALL of the batch), versionRegressed
       (any member's coordinator version ever decreased), doubleRegistered
       (a registration commit ever bumped a member ALREADY at its manifest
       fingerprint -- the bump the marker + hash filter exist to exclude).

   KEY MODELING DECISION (the base choice). This spec is STANDALONE -- own
   state, no EXTENDS of the MESI chain. The real composition is: the
   registration seam calls commit_all ONCE per attempt, and commit_all's
   INTERNAL all-or-nothing atomicity over per-member version-CAS checks is
   ALREADY the proven theorem of AtomicPublish.tla (NoPartialPublish over
   MemberCommits), with the per-leg CAS arbitration itself proven as OCC's
   NoLostUpdate. EXTENDS AtomicPublish would (i) re-check a sibling's
   theorem at the cost of the exact state space that keeps AtomicPublish
   HELD OUT of the CI sweep -- defeating Unit 7's goal of a model-CHECKED
   artifact; (ii) drag in MESI/heartbeat/sweep dimensions orthogonal to
   every WV-new invariant; (iii) still not contain the WV state (paths,
   fingerprints, the status marker machine, crash-resume) -- grafting it on
   enlarges, not reuses. EXTENDS Snapshot is the wrong axis entirely
   (read-side cut + pin-GC; registration is write-side; pins are Unit 6 /
   Snapshot's own exemptions seam). So commit_all enters THIS model as one
   atomic action (RegisterCommitAction) whose WIN branch's guard
   `winners = batch` and whole-batch apply mirror AtomicPublish's
   CommitAllAction shape exactly, one layer up: the torn-registration
   detector is vacuous by construction, and the teeth come from the mutants
   (README WV-1..WV-3), never from a precondition the correct spec is
   missing. What is NEW here -- and nowhere in the chain -- is the
   checkpoint status machine, the TWO idempotency mechanisms and their
   crash windows, the seam-vs-service empty split, the fence-terminal vs
   HELD-retryable refusal split, and the member-class partition with its
   degenerate paths IN the model.

   DELIBERATELY OUT OF MODEL:
     (a) the substrate legs themselves (S3 If-Match/versionId, the file
         CAS) -- legs appear only as their terminal outcomes; the per-leg
         one-winner property is the substrate's (S3 native-CAS) or
         adapter-local detection (file, no-arbiter tier), and the
         coordinator-side per-leg CAS is OCC's NoLostUpdate. A restored
         FILE leg may nondeterministically land coordinator-side at leg
         time (a coordinator-connected volume's write_cas_at) -- modeled as
         the leg bumping version and setting hash "fp", which the filter
         then skips (never double-registered).
     (b) commit_all internals -- S/I caller preconditions (D4), atomic peer
         invalidation + broadcast-after-commit signals, and the
         OccCallerTransientError mid-transient retry (absorbed into the
         HELD re-drive shape): AtomicPublish/OCC's theorems and the Python
         suite's routes.
     (c) pins, legal hold, GC interaction -- Unit 6; the pin-GC seam is
         Snapshot.tla's PinAlwaysRetained.
     (d) budgets as real numbers -- MAX_RESTORE_LEG_REDRIVES=8 becomes the
         small MaxAttempts CONSTANT; the exhaustion->refused SHAPE is the
         property, not the count. Same for versions (MaxVersion bound; a
         batch member at the bound blocks WIN like AtomicPublish's
         MemberCommits bound -- the run then concludes via budget
         exhaustion, a finite-model artifact, not a protocol path).
     (e) durability of the registry writes -- house exclusion (the state IS
         the durable truth); CrashAction wipes exactly the run-local
         variables.
     (f) abort threading, HTTP routes, the manifest capture side
         (Units 3/8) and torn-cut detection (capture-side, not restore).

   WHAT IS PROVEN: NoPartialRestoreRegistered, NoVersionRegression,
   ExactlyOnceRegistration, plus WVTypeOK and RegisteredImpliesTerminal
   (`registered`/`concluded` only after every member is terminal), over a
   machine that includes crash/resume at every step, concurrent registered-
   writer bumps, fence supersession, and the degenerate empty/delete-only/
   all-absent paths as REACHABLE transitions (nondeterministic outcomes and
   tiers -- the implemented no-commit conclusions, not vacuous absences).
   LIVENESS is discharged as safety + prose per the repo's safety-only TLC
   convention (README).

   CODE CROSS-REFERENCES (parity reviewed against Units 4-5):
     - WorkspaceVersioner._restore_locked / _registration_seam /
       _drive_registration -- src/ccs/adapters/workspace.py (the status
       writes, the prior_run flag, the member-class partition, the bounded
       re-drive, the fence-terminal branch).
     - CoordinatorService.register_workspace_restore /
       _resolve_workspace_member_artifact -- src/ccs/coordinator/service.py
       (the hash filter, the seam-vs-service EMPTY split, the ONE
       commit_all batch, check_monotonic_version defense-in-depth).
     - RESTORE_STATUSES / WORKSPACE_REGISTRATION_STATUSES / the modality
       sets -- src/ccs/core/exceptions.py (the closed vocabularies this
       module's string sets mirror). *)

EXTENDS Naturals

CONSTANTS
    Members,      \* checkpoint member identities (model values)
    MaxVersion,   \* finite version bound (the implementation is unbounded)
    MaxAttempts,  \* registration re-drive budget (MAX_RESTORE_LEG_REDRIVES=8)
    None          \* absent-comparand marker (house style)

VARIABLES
    tier,       \* [Members -> {"file","s3"}] -- member class; Init-chosen,
                \* constant thereafter (one run covers every partition)
    outcome,    \* [Members -> {"pending"} \cup MemberOutcomes] -- durable rows
    version,    \* [Members -> 1..MaxVersion] -- coordinator version per member
    hash,       \* [Members -> {"fp","live"}] -- "fp" = at manifest fingerprint
    status,     \* durable checkpoint restore_status
    running,    \* a restore run is live (run-local liveness)
    prior,      \* run-local: this run resumed a `registered` checkpoint
    fenced,     \* run-local: this controller was superseded by the sweep
    armed,      \* run-local: comparands captured, batch chosen, commit pending
    observed,   \* [Members -> {None} \cup 1..MaxVersion] -- the comparand cut;
                \* observed[m] /= None <=> m is in the armed batch
    regOutcome, \* run-local seam answer (the WORKSPACE_REGISTRATION_* vocab)
    attempts,   \* run-local re-drive budget consumed
    partialRestoreRegistered, \* sticky: a torn registration ever landed
    versionRegressed,         \* sticky: a member's version ever decreased
    doubleRegistered          \* sticky: a commit ever re-bumped an
                              \* already-at-fingerprint member

wvVars == <<tier, outcome, version, hash, status, running, prior, fenced,
            armed, observed, regOutcome, attempts,
            partialRestoreRegistered, versionRegressed, doubleRegistered>>

flagVars == <<partialRestoreRegistered, versionRegressed, doubleRegistered>>

--------------------------------------------------------------------
(* Vocabularies (mirror the closed sets in ccs/core/exceptions.py) *)
--------------------------------------------------------------------

(* The six implemented terminal outcomes plus the model's `deleted`
   abstraction of the (restored, deleted_at_restore) delete-leg pair. *)
MemberOutcomes == {"restored", "converged", "conflict", "held_unconfirmed",
                   "target_lost", "forward_only_skipped", "deleted"}

Statuses    == {"none", "in_progress", "registered", "concluded"}

RegOutcomes == {"none", "committed", "empty_write_set",
                "registered_by_prior_run", "refused"}

AllTerminal == \A m \in Members : outcome[m] /= "pending"

(* The seam's commit candidates: written FILE members (restored, file tier;
   deletes carry their own outcome value and S3 members are manifest-side by
   design -- neither ever reaches the commit path). *)
FileWrites == { m \in Members : outcome[m] = "restored" /\ tier[m] = "file" }

(* The armed batch is carried in `observed` (non-None entries). *)
Batch == { m \in Members : observed[m] /= None }

NoObserved == [m \in Members |-> None]

--------------------------------------------------------------------
(* Initialization *)
--------------------------------------------------------------------

WVInit ==
    /\ tier \in [Members -> {"file", "s3"}]
    /\ outcome = [m \in Members |-> "pending"]
    /\ version = [m \in Members |-> 1]
    (* "fp" at Init = the member is unmoved since the checkpoint, or its
       artifact was first-observation-minted carrying the fingerprint --
       either way the filter's skip case. "live" = the workspace moved on. *)
    /\ hash \in [Members -> {"fp", "live"}]
    /\ status = "none"
    /\ running = FALSE /\ prior = FALSE /\ fenced = FALSE /\ armed = FALSE
    /\ observed = NoObserved
    /\ regOutcome = "none"
    /\ attempts = 0
    /\ partialRestoreRegistered = FALSE
    /\ versionRegressed = FALSE
    /\ doubleRegistered = FALSE

--------------------------------------------------------------------
(* Run lifecycle: start / crash / resume *)
--------------------------------------------------------------------

StartRestoreAction ==
    /\ status = "none" /\ ~running
    /\ status' = "in_progress"
    /\ running' = TRUE /\ prior' = FALSE
    /\ UNCHANGED <<tier, outcome, version, hash, fenced, armed, observed,
                   regOutcome, attempts>>
    /\ UNCHANGED flagVars

(* Crash wipes exactly the RUN-LOCAL state. Durable state -- status, the
   member outcome rows (written before the next member is driven), the
   coordinator versions/hashes -- survives. A fresh run gets a fresh budget
   and a fresh controller claim (fenced resets). *)
CrashAction ==
    /\ running
    /\ running' = FALSE /\ prior' = FALSE /\ fenced' = FALSE /\ armed' = FALSE
    /\ observed' = NoObserved
    /\ regOutcome' = "none"
    /\ attempts' = 0
    /\ UNCHANGED <<tier, outcome, version, hash, status>>
    /\ UNCHANGED flagVars

(* Resume mirrors _restore_locked's entry: prior_run is read from the
   durable status, then the status is set back to in_progress
   (unconditionally, in the code) -- the marker is ONE-SHOT, which is
   exactly why the hash filter must backstop a second crash inside the
   marker->conclude window. *)
ResumeAction ==
    /\ ~running /\ status \in {"in_progress", "registered"}
    /\ prior' = (status = "registered")
    /\ status' = "in_progress"
    /\ running' = TRUE
    /\ UNCHANGED <<tier, outcome, version, hash, fenced, armed, observed,
                   regOutcome, attempts>>
    /\ UNCHANGED flagVars

--------------------------------------------------------------------
(* Environment: a concurrent registered writer, and the sweep *)
--------------------------------------------------------------------

(* A live REGISTERED peer commits a member forward at any time -- during
   legs, in the comparand gap (-> HELD -> re-drive), or between crash and
   resume (-> the documented forward re-assert residual). It may write the
   manifest bytes ("fp" -- e.g. a second restorer) or anything else
   ("live"). Always +1: peers ride commit_cas, which cannot regress
   (OCC's theorem). *)
PeerCommitAction ==
    \E m \in Members, h \in {"fp", "live"} :
        /\ version[m] < MaxVersion
        /\ version' = [version EXCEPT ![m] = version[m] + 1]
        /\ hash' = [hash EXCEPT ![m] = h]
        /\ UNCHANGED <<tier, outcome, status, running, prior, fenced, armed,
                       observed, regOutcome, attempts>>
        /\ UNCHANGED flagVars

(* The sweep reclaims this controller's grant mid-run: its read generation
   is superseded, and its late registration apply must be fence-rejected --
   terminally (never re-driven past the fence). *)
SupersedeControllerAction ==
    /\ running /\ ~fenced
    /\ fenced' = TRUE
    /\ UNCHANGED <<tier, outcome, version, hash, status, running, prior,
                   armed, observed, regOutcome, attempts>>
    /\ UNCHANGED flagVars

--------------------------------------------------------------------
(* Member legs: each pending member concludes to a terminal, absorbing
   outcome (nondeterministic -- one run covers every outcome partition,
   including the degenerate delete-only / all-absent / all-conflict
   conclusions). A restored FILE leg may have ridden a coordinator-
   connected volume: its write_cas_at already committed the restore
   forward at leg time (+1, hash -> fingerprint) -- the hash filter later
   skips it, so it is never double-registered. *)
--------------------------------------------------------------------

MemberLegAction ==
    \E m \in Members, o \in MemberOutcomes :
        /\ running /\ status = "in_progress" /\ outcome[m] = "pending"
        /\ outcome' = [outcome EXCEPT ![m] = o]
        /\ IF o = "restored" /\ tier[m] = "file"
           THEN \/ (* coordinator-connected leg: landed at leg time *)
                   /\ version[m] < MaxVersion
                   /\ version' = [version EXCEPT ![m] = version[m] + 1]
                   /\ hash' = [hash EXCEPT ![m] = "fp"]
                \/ (* plain restore target: manifest-side until the seam *)
                   UNCHANGED <<version, hash>>
           ELSE UNCHANGED <<version, hash>>
        /\ UNCHANGED <<tier, status, running, prior, fenced, armed, observed,
                       regOutcome, attempts>>
        /\ UNCHANGED flagVars

--------------------------------------------------------------------
(* The registration seam (all-terminal, before conclusion) *)
--------------------------------------------------------------------

SeamReady ==
    /\ running /\ status = "in_progress"
    /\ AllTerminal /\ regOutcome = "none"

(* A prior (crashed) run already answered the registration: the durable
   marker was found at resume. Nothing re-registered -- exactly-once,
   mechanism one. Answers regardless of the member partition. *)
SeamPriorRunAction ==
    /\ SeamReady /\ prior
    /\ regOutcome' = "registered_by_prior_run"
    /\ UNCHANGED <<tier, outcome, version, hash, status, running, prior,
                   fenced, armed, observed, attempts>>
    /\ UNCHANGED flagVars

(* Seam-level EMPTY: no written file members at all (all deleted / S3 /
   absorbed / converged -- incl. the delete-only and all-absent degenerate
   restores). The service is never called, commit_all never runs, no budget
   is consumed -- a no-commit transition straight toward `registered`. *)
SeamEmptyAction ==
    /\ SeamReady /\ ~prior
    /\ FileWrites = {}
    /\ regOutcome' = "empty_write_set"
    /\ UNCHANGED <<tier, outcome, version, hash, status, running, prior,
                   fenced, armed, observed, attempts>>
    /\ UNCHANGED flagVars

(* One register_workspace_restore attempt begins: consume budget, apply the
   HASH FILTER (already-at-fingerprint members are skipped -- exactly-once,
   mechanism two), read the surviving members' comparands. An all-filtered
   batch answers the service-level EMPTY (a call was made; budget consumed).
   The sequential per-member reads are modeled as one atomic capture -- see
   the header (WIN/HELD reachability is identical). *)
ReadComparandsAction ==
    /\ SeamReady /\ ~prior /\ ~armed
    /\ FileWrites /= {}
    /\ attempts < MaxAttempts
    /\ attempts' = attempts + 1
    /\ LET batch == { m \in FileWrites : hash[m] /= "fp" }
           (* MUTANT WV-2: batch == FileWrites  (drop the hash filter) *)
       IN IF batch = {}
          THEN /\ regOutcome' = "empty_write_set"
               /\ UNCHANGED <<armed, observed>>
          ELSE /\ observed' = [m \in Members |->
                                  IF m \in batch THEN version[m] ELSE None]
               /\ armed' = TRUE
               /\ UNCHANGED regOutcome
    /\ UNCHANGED <<tier, outcome, version, hash, status, running, prior,
                   fenced>>
    /\ UNCHANGED flagVars

(* Budget exhausted under sustained contention: REFUSED, nothing registered
   (all-or-nothing), the restore still concludes -- without the marker, so
   a resume-before-conclusion may re-attempt. *)
BudgetExhaustedAction ==
    /\ SeamReady /\ ~prior /\ ~armed
    /\ FileWrites /= {}
    /\ attempts = MaxAttempts
    /\ regOutcome' = "refused"
    /\ UNCHANGED <<tier, outcome, version, hash, status, running, prior,
                   fenced, armed, observed, attempts>>
    /\ UNCHANGED flagVars

(* THE COMMIT: one commit_all batch, ONE atomic step (the sibling-proven
   abstraction -- see the header's base choice). Three branches:
     FENCE   -- a superseded controller's late apply: TERMINAL refusal,
                never re-driven (stale_read_generation is matched on sight).
     WIN     -- every comparand current (and in-bound): apply the WHOLE
                batch -- +1 and fingerprint per member -- in one step. The
                three sticky detectors are vacuous here by construction
                (applied = batch; ApplyV = +1; the filter kept "fp" members
                out and any post-read hash change rode a version bump ->
                HELD): the teeth are README mutants WV-1..WV-3.
     HELD    -- a registered writer moved a member inside the comparand gap
                (version_mismatch): all-or-nothing refusal, ZERO mutation,
                retry-eligible -- disarm and re-drive from fresh comparands
                under the budget. *)
RegisterCommitAction ==
    /\ armed
    /\ LET batch   == Batch
           winners == { m \in batch : observed[m] = version[m]
                                      /\ version[m] < MaxVersion }
       IN \/ (* FENCE: terminal on sight *)
             /\ fenced
             /\ regOutcome' = "refused"
             /\ armed' = FALSE
             /\ observed' = NoObserved
             /\ UNCHANGED <<version, hash>>
             /\ UNCHANGED flagVars
          \/ (* WIN: the whole batch, one atomic apply *)
             /\ ~fenced
             /\ winners = batch      (* MUTANT WV-1: winners /= {}        *)
             /\ LET applied == batch (* MUTANT WV-1: applied == winners   *)
                    ApplyV(m) == version[m] + 1
                    (* MUTANT WV-3: ApplyV(m) == 1  (restore-as-rollback) *)
                IN /\ version' = [m \in Members |->
                                     IF m \in applied THEN ApplyV(m)
                                     ELSE version[m]]
                   /\ hash' = [m \in Members |->
                                  IF m \in applied THEN "fp" ELSE hash[m]]
                   /\ partialRestoreRegistered' =
                          (partialRestoreRegistered \/ (applied /= batch))
                   /\ versionRegressed' =
                          (versionRegressed
                           \/ (\E m \in applied : ApplyV(m) < version[m]))
                   /\ doubleRegistered' =
                          (doubleRegistered
                           \/ (\E m \in applied : hash[m] = "fp"))
             /\ regOutcome' = "committed"
             /\ armed' = FALSE
             /\ observed' = NoObserved
          \/ (* HELD: version_mismatch -- clean no-op, re-drive *)
             /\ ~fenced
             /\ winners /= batch
             /\ armed' = FALSE
             /\ observed' = NoObserved
             /\ UNCHANGED <<version, hash, regOutcome>>
             /\ UNCHANGED flagVars
    /\ UNCHANGED <<tier, outcome, status, running, prior, fenced, attempts>>

--------------------------------------------------------------------
(* Conclusion: the durable marker, then concluded *)
--------------------------------------------------------------------

(* The one-shot idempotency marker -- written ONLY on a committed/empty
   registration (REFUSED skips it so a resume may re-attempt; PRIOR_RUN
   already had it and does not rewrite it). *)
MarkRegisteredAction ==
    /\ running /\ status = "in_progress"
    /\ regOutcome \in {"committed", "empty_write_set"}
    /\ status' = "registered"
    /\ UNCHANGED <<tier, outcome, version, hash, running, prior, fenced,
                   armed, observed, regOutcome, attempts>>
    /\ UNCHANGED flagVars

(* Conclude: from the marker (committed/empty answers), or directly from
   in_progress on a prior-run/refused answer. The run ends; run-local state
   resets. `concluded` is absorbing (a re-restore returns the durable
   report and drives nothing). *)
ConcludeAction ==
    /\ running
    /\ \/ status = "registered"
       \/ (status = "in_progress"
           /\ regOutcome \in {"registered_by_prior_run", "refused"})
    /\ status' = "concluded"
    /\ running' = FALSE /\ prior' = FALSE /\ fenced' = FALSE /\ armed' = FALSE
    /\ observed' = NoObserved
    /\ regOutcome' = "none"
    /\ attempts' = 0
    /\ UNCHANGED <<tier, outcome, version, hash>>
    /\ UNCHANGED flagVars

--------------------------------------------------------------------
(* Specification *)
--------------------------------------------------------------------

(* Explicit termination stutter: unlike the MESI chain (whose read/observe
   actions self-loop forever), this protocol genuinely TERMINATES -- a
   `concluded` checkpoint whose member versions have saturated the finite
   bound has no successor. The house TLC invocation checks deadlock, so
   the terminal state self-loops explicitly (the standard idiom). *)
TerminatedAction ==
    /\ status = "concluded"
    /\ UNCHANGED wvVars

WVNext ==
    \/ TerminatedAction
    \/ StartRestoreAction
    \/ CrashAction
    \/ ResumeAction
    \/ PeerCommitAction
    \/ SupersedeControllerAction
    \/ MemberLegAction
    \/ SeamPriorRunAction
    \/ SeamEmptyAction
    \/ ReadComparandsAction
    \/ BudgetExhaustedAction
    \/ RegisterCommitAction
    \/ MarkRegisteredAction
    \/ ConcludeAction

WVSpec == WVInit /\ [][WVNext]_wvVars

--------------------------------------------------------------------
(* Invariants *)
--------------------------------------------------------------------

WVTypeOK ==
    /\ tier \in [Members -> {"file", "s3"}]
    /\ outcome \in [Members -> {"pending"} \cup MemberOutcomes]
    /\ version \in [Members -> 1..MaxVersion]
    /\ hash \in [Members -> {"fp", "live"}]
    /\ status \in Statuses
    /\ running \in BOOLEAN /\ prior \in BOOLEAN
    /\ fenced \in BOOLEAN /\ armed \in BOOLEAN
    /\ observed \in [Members -> {None} \cup (1..MaxVersion)]
    /\ regOutcome \in RegOutcomes
    /\ attempts \in 0..MaxAttempts
    /\ partialRestoreRegistered \in BOOLEAN
    /\ versionRegressed \in BOOLEAN
    /\ doubleRegistered \in BOOLEAN
    (* well-formedness: an armed batch is non-empty, unanswered, in a live
       run; comparands exist only while armed *)
    /\ (armed => (Batch /= {} /\ regOutcome = "none" /\ running))
    /\ (~armed => observed = NoObserved)

(* `registered`/`concluded` are reachable only after every member holds a
   terminal outcome (the marker machine's ordering: legs -> seam answer ->
   marker -> concluded). *)
RegisteredImpliesTerminal ==
    status \in {"registered", "concluded"} => AllTerminal

(* HEADLINE 1 -- all-or-nothing registration: no reachable state where a
   registration landed a strict, non-empty subset of its commit batch. The
   WV-layer analog of AtomicPublish's NoPartialPublish, one level up.
   Vacuous-TRUE in the correct spec (the WIN branch applies exactly the
   batch); README mutant WV-1 gives it teeth. *)
NoPartialRestoreRegistered == partialRestoreRegistered = FALSE

(* HEADLINE 2 -- restore monotonicity: a restored member's coordinator
   version strictly increases while carrying OLD bytes (restore-as-forward-
   commit) -- never a decrement. Mirrors check_monotonic_version defense-
   in-depth in register_workspace_restore; README mutant WV-3 (restore-as-
   rollback) gives it teeth. *)
NoVersionRegression == versionRegressed = FALSE

(* HEADLINE 3 -- exactly-once registration under crash-resume: no
   registration commit ever re-bumped a member already carrying its
   manifest fingerprint. The conjunction of the two durable mechanisms --
   the `registered` marker (SeamPriorRunAction) and the hash filter
   (ReadComparandsAction) -- makes the double bump unreachable across every
   crash window, including crash-after-WIN-before-marker (the filter
   re-answers EMPTY) and crash-after-marker-before-conclude (the marker
   re-answers PRIOR_RUN). The peer-overwrites-then-resume forward re-assert
   is deliberately NOT a violation (hash /= "fp" at commit time -- the
   documented residual: a forward bump, never silent). README mutant WV-2
   gives it teeth. *)
ExactlyOnceRegistration == doubleRegistered = FALSE

==========================================================================
