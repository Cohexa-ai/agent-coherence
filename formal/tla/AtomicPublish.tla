-------------------------- MODULE AtomicPublish --------------------------
(* Atomic multi-artifact publish (SB-18 / commit_all) amendment to the
   version-CAS MESI protocol. Proves that a single agent publishing a WRITE-SET
   of several artifacts commits ALL-OR-NOTHING -- no reachable state ever shows a
   strict, non-empty subset of one batch's members advanced (a torn publish).
   This is the write-side dual of Snapshot.tla's read-side coherent cut: where
   Snapshot proves no cross-artifact READ skew within a session's captured cut,
   AtomicPublish proves no cross-artifact WRITE skew within a commit_all's batch.

   Plan: docs/plans/2026-07-07-001-feat-sb18-multi-artifact-atomic-transactions-plan.md
   (Unit 1). Origin: docs/brainstorms/2026-07-07-sb18-multi-artifact-atomic-transactions-requirements.md.

   BASE CHOICE (resolves the plan's Deferred question "new AtomicPublish.tla vs
   extend OCC.tla"). This spec EXTENDS OCC, not Snapshot. The SB-18 property is
   the N-artifact generalization of NoLostUpdate (OCC's version-CAS), and the
   concurrent multi-writer race a torn publish must survive lives in the
   version-CAS dimension -- reachable on the OCC base. Snapshot's read-side cut,
   retention history, and pin-GC are ORTHOGONAL to write atomicity and only
   inflate the state space; OCC is the lighter, in-budget base where the write
   race is exercised. Snapshot's own header names this gap ("WRITE SKEW ... is
   SB-18 (atomic multi-artifact write), out of this plan") -- that is exactly
   what this spec closes, one amendment over.

   Adds:
     - partialPublish : a sticky history flag -- TRUE iff any commit_all ever
                        applied a strict, non-empty subset of its write-set (a
                        torn batch). Under the atomic all-or-nothing
                        CommitAllAction the WIN branch applies EXACTLY the whole
                        write-set, so the flag is unreachable-TRUE in the correct
                        spec; README mutant AP-1 (apply the passing subset) gives
                        it teeth. Mirrors the lostUpdate / staleApply /
                        collectedRead sticky-flag idiom.
     - CommitAllAction : an agent picks a non-empty write-set ws (its held
                        vector) and commits it ALL-OR-NOTHING. WIN iff EVERY
                        member passes the per-artifact commit_cas checks
                        (version-CAS observed == current, from S/I, no other
                        M/E holder) -> bump the version of every member in ONE
                        step; CONFLICT iff ANY member is blocked -> a clean
                        no-op (ZERO mutation), the typed MultiCommitConflict
                        analog. Never a partial apply.

   KEY MODELING DECISION (the crux). All-or-nothing is a SINGLE atomic action
   over the whole write-set (one version' update across ws), exactly as
   OCC.CommitCASAction is atomic over one artifact. The WIN branch's guard
   `winners = ws` (every member commits) and its apply over `ws` are what make a
   torn state unreachable; the detector `partialPublish' = partialPublish \/
   (winners /= ws)` is therefore vacuous. The teeth come from the mutants (below),
   which make a torn or stale apply REACHABLE -- never from a precondition the
   correct spec is missing (which would risk false safety). NoLostUpdate is
   inherited and re-checked so composition is validated AND so mutant AP-2
   witnesses the concurrent multi-writer race.

   DELIBERATELY OUT OF MODEL:
     (a) the read-generation FENCE (Fencing.tla) -- a sibling amendment; a
         commit_all member's fence check is the single-artifact FencingCommit
         three-branch lifted verbatim per member, and NoStaleApply is proven
         there. This spec models the write-CAS atomicity dimension; the two
         compose exactly as OCC and Fencing do (neither subsumes the other).
     (b) the STAGE/APPLY/BROADCAST implementation seam -- buffering peer-INVALID
         and broadcasting only after apply is an implementation ordering; in TLA
         the state IS the truth and CommitAllAction is one atomic step, so a peer
         can never observe a mid-batch state. The buffer-then-broadcast discipline
         is the Python units' property (plan Units 2/3), tested there.
     (c) durability / synchronous=FULL -- inherited exclusion (the state is the
         durable truth).

   WHAT IS PROVEN: safety PRESERVATION -- every inherited invariant (NoLostUpdate,
   SingleWriter, MonotonicVersion, the CR I3-I6 family) re-checked with commit_all
   composed in, PLUS NoPartialPublish. LIVENESS is discharged as safety + prose
   per the repo's safety-only TLC convention (README). *)

EXTENDS OCC

VARIABLES partialPublish

apVars == <<occVars, partialPublish>>

--------------------------------------------------------------------
(* Initialization *)
--------------------------------------------------------------------

AtomicPublishInit ==
    /\ OCCInit
    /\ partialPublish = FALSE

--------------------------------------------------------------------
(* Per-member commit predicate: exactly OCC.CommitCASAction's WIN checks,
   applied to one member of a batch -- version-CAS (observed == current), the
   committer is S/I (D4: OCC from S/I only), it has read first, the version is
   unbounded, and no OTHER agent holds M/E (SingleWriter). A member that fails
   ANY of these blocks the whole batch (all-or-nothing). *)
--------------------------------------------------------------------

MemberCommits(ag, art) ==
    /\ mesiState[art][ag] \in {"S", "I"}
    /\ observedVersion[ag][art] /= None
    /\ observedVersion[ag][art] = version[art]      (* MUTANT AP-2: delete this line *)
    /\ version[art] < MaxVersion
    /\ ~(\E peer \in Agents : peer /= ag /\ mesiState[art][peer] \in MorE)

NonEmptyWriteSets == { ws \in SUBSET Artifacts : ws /= {} }

--------------------------------------------------------------------
(* CommitAllAction: the ATOMIC all-or-nothing multi-artifact commit_all.
   winners = the members of ws that pass MemberCommits.
     WIN  iff winners = ws  -> apply EVERY member of ws in ONE step: committer ->
              SHARED, non-invalid peers -> INVALID, version + 1 and observed + 1
              per member. partialPublish stays FALSE (applied = ws = winners).
     CONFLICT iff winners /= ws -> clean no-op, ZERO mutation (the typed
              MultiCommitConflict per-member analog).
   README mutant AP-1 relaxes the WIN guard to `winners /= {}` and applies
   `winners`, making a torn publish (winners a strict subset of ws) reachable ->
   NoPartialPublish fails. Mutant AP-2 deletes the version-CAS line in
   MemberCommits, letting a stale member land in a batch -> NoLostUpdate fails
   (proving the concurrent multi-writer race is reachable, not vacuously
   excluded). Grant-tick bookkeeping mirrors OCC.CommitCASAction (the inherited
   UpdatedGrantedAtTick / UpdatedLastReclamation compose over the multi-artifact
   mesiState' automatically). *)
--------------------------------------------------------------------

CommitAllAction ==
    \E ag \in Agents, ws \in NonEmptyWriteSets :
        LET winners == { art \in ws : MemberCommits(ag, art) }
        IN /\ \/ (* WIN: every member commits -> apply ALL of ws atomically *)
                 /\ winners = ws
                 /\ version' = [art \in Artifacts |->
                        IF art \in ws THEN version[art] + 1 ELSE version[art]]
                 /\ mesiState' = [art \in Artifacts |->
                        IF art \in ws
                        THEN [peer \in Agents |->
                                IF peer = ag THEN "S"
                                ELSE IF peer \in NonInvalidPeers(art, ag) THEN "I"
                                ELSE mesiState[art][peer]]
                        ELSE mesiState[art]]
                 /\ observedVersion' = [a \in Agents |-> [art \in Artifacts |->
                        IF a = ag /\ art \in ws THEN version[art] + 1
                        ELSE observedVersion[a][art]]]
                 /\ partialPublish' = (partialPublish \/ (winners /= ws))
                 /\ lostUpdate' = lostUpdate
              \/ (* CONFLICT: at least one member blocked -> clean no-op *)
                 /\ winners /= ws
                 /\ UNCHANGED <<version, mesiState, observedVersion,
                                partialPublish, lostUpdate>>
           /\ grantedAtTick'   = UpdatedGrantedAtTick
           /\ lastReclamation' = UpdatedLastReclamation
           /\ UNCHANGED <<clock, lastHeartbeat>>

--------------------------------------------------------------------
(* Specification *)
--------------------------------------------------------------------

(* Inherited OCC actions (which themselves fold in the CR pessimistic path and
   the single-artifact CommitCASAction) keep partialPublish unchanged. *)
AtomicPublishNext ==
    \/ (OCCNext /\ UNCHANGED partialPublish)
    \/ CommitAllAction

AtomicPublishSpec == AtomicPublishInit /\ [][AtomicPublishNext]_apVars

--------------------------------------------------------------------
(* Invariants *)
--------------------------------------------------------------------

AtomicPublishTypeOK ==
    /\ OCCTypeOK
    /\ partialPublish \in BOOLEAN

(* The headline safety property: no commit_all ever applied a strict, non-empty
   subset of its write-set -- every multi-artifact publish is all-or-nothing.
   The N-artifact generalization of NoLostUpdate. Vacuous-TRUE in the correct
   spec (the WIN branch applies exactly ws); README mutant AP-1 gives it teeth.
   NoLostUpdate, SingleWriter, MonotonicVersion, and the CR invariants are
   inherited and re-checked to validate composition; mutant AP-2 gives the
   concurrent multi-writer race teeth on the inherited NoLostUpdate. *)
NoPartialPublish == partialPublish = FALSE

==========================================================================
