----------------------------- MODULE Fencing -----------------------------
(* Read-generation fence amendment to the MESI + crash-recovery protocol.
   Closes the watchdog reclaim-zombie hole that OCC commit-CAS (OCC.tla)
   leaves open: a writer whose ownership/read-claim was reclaimed by the
   sweep, but whose version has not moved, can still apply a stale write
   (the "phantom version-bump"). The fence keys on a per-artifact
   generation, not on the version, so it catches the reclaim even when the
   version is unchanged.

   Plan: docs/plans/2026-06-09-001-feat-fencing-single-host-plan.md (Unit 1).
   Origin: docs/brainstorms/2026-06-09-fencing-single-host-requirements.md.
   Composes with CrashRecovery via EXTENDS (a sibling amendment to OCC.tla).

   Adds:
     - ownerGeneration : per-artifact monotonic counter, bumped on every
                         sweep reclamation (atomic with the M/E -> I
                         transition).
     - readGeneration  : the ownerGeneration each agent captured when it
                         last ESTABLISHED ITS WRITE-CLAIM (a genuine read
                         for the OCC path, an E/M acquire for the pessimistic
                         path). None until captured -- absent operand.
     - staleApply      : a sticky history flag -- TRUE iff any commit ever
                         applied a write from a read-generation older than
                         the artifact's current ownerGeneration (a stale
                         apply -- the exact failure the fence prevents).
     - ObserveGenAction  : an agent captures the current ownerGeneration into
                           its slot. DECOUPLED from the acquire/fetch on
                           purpose (mirrors OCC.tla's ObserveAction), so the
                           sweep can interleave BETWEEN capture and commit --
                           the non-atomic-capture hazard the plan-review
                           required this spec to model.
     - FencingSweepAction: SweepAction + bump ownerGeneration on each
                           reclaimed artifact, atomically.
     - FencingCommitAction: the generation-guarded commit. WIN iff
                            readGeneration = ownerGeneration (strict-> reject,
                            EQUALITY ADMITS); else a clean no-op conflict
                            (the typed-conflict analog -- never a silent
                            drop). Absent readGeneration => cannot commit
                            (absent-operand = reject).

   KEY MODELING DECISION (the crux). The fence is UNIFORM across both write
   paths, and its safety depends only on (readGeneration vs ownerGeneration),
   not on the committer's MESI state. So a single FencingCommitAction models
   BOTH the pessimistic commit and the OCC commit_cas, guarded. We therefore
   REPLACE the inherited unguarded commit (CRCommitAction) and the inherited
   SweepAction with their fenced equivalents -- in the fence world every
   version-bumping commit is generation-guarded and every reclaim bumps the
   generation. The capture is available to an agent in any state (it models
   "established a claim at this generation"): this is exactly the plan-review
   P0 fix -- capture at the claim-establishing point (fetch OR E/M acquire),
   not fetch-only, so a legitimate pessimistic acquire-without-content-read
   still has a valid operand and is admitted.

   LIVENESS is discharged as safety + prose, consistent with OCC.tla and the
   repo's safety-only TLC convention (README). TLC checks the NoStaleApply
   safety invariant only.

   DELIBERATELY OUT OF THIS SPEC (modeled empirically in the plan's Unit 8,
   or in a follow-on amendment): (a) coordinator restart -- in TLA the state
   IS the durable truth, so "lost in-memory mirror" has no analog; the
   durable-counter-rejects property is an implementation/test property.
   (b) the coordinator_epoch / delete-and-recreate wipe reset -- under the
   plan's RECOMMENDED migration option (a) (additive columns, no
   user_version bump) there is no wipe-reset, so the epoch is conditional on
   option (b) and is left to a spec amendment if (b) is chosen (see plan
   Unit 2). The CORE fence safety -- capture/sweep-bump/guarded-commit and
   the non-atomic-capture interleaving -- is what NoStaleApply proves here. *)

EXTENDS CrashRecovery

(* Generation space bound for finite model checking: each sweep bumps a
   reclaimed artifact's generation by 1, and sweeps are bounded by the tick
   horizon, so ownerGeneration never needs to exceed MaxTicks. *)
MaxGen == MaxTicks

VARIABLES ownerGeneration, readGeneration, staleApply

fenceVars == <<allVars, ownerGeneration, readGeneration, staleApply>>

--------------------------------------------------------------------
(* Initialization *)
--------------------------------------------------------------------

FencingInit ==
    /\ CRInit
    /\ ownerGeneration = [art \in Artifacts |-> 0]
    /\ readGeneration  = [ag \in Agents |-> [art \in Artifacts |-> None]]
    /\ staleApply = FALSE

--------------------------------------------------------------------
(* ObserveGenAction: an agent captures the current ownerGeneration into its
   slot -- the moment it establishes a write-claim (fetch read-set OR E/M
   acquire). Decoupled from the acquire/fetch action so the sweep can fire
   between capture and commit (the non-atomic-capture hazard). *)
--------------------------------------------------------------------

ObserveGenAction ==
    \E ag \in Agents, art \in Artifacts :
        /\ readGeneration' = [readGeneration EXCEPT ![ag][art] = ownerGeneration[art]]
        /\ UNCHANGED <<baseVars, crVars, ownerGeneration, staleApply>>

--------------------------------------------------------------------
(* FencingCommitAction: generation-guarded commit (both paths, uniform).
   WIN     iff readGeneration = ownerGeneration (equality admits).
   CONFLICT otherwise (read-generation superseded by a reclaim) -- clean
            no-op. Absent readGeneration => cannot fire (absent operand). *)
--------------------------------------------------------------------

FencingCommitAction ==
    \E ag \in Agents, art \in Artifacts :
        /\ readGeneration[ag][art] /= None          (* absent operand => reject (cannot commit) *)
        /\ version[art] < MaxVersion                (* finite bound *)
        /\ LET rg == readGeneration[ag][art]
               og == ownerGeneration[art]
           IN \/ (* WIN: claim is current -> apply the write (bump version). *)
                 /\ rg = og
                 /\ version' = [version EXCEPT ![art] = version[art] + 1]
                 (* Records a stale apply iff this commit landed on a
                    superseded read-generation. The WIN guard `rg = og`
                    makes `rg < og` always FALSE here. The mutant (remove
                    the `rg = og` conjunct above) lets a superseded commit
                    win and flips staleApply -> TRUE, which TLC reports as a
                    NoStaleApply violation. This is what gives the invariant
                    teeth. *)
                 /\ staleApply' = (staleApply \/ (rg < og))
              \/ (* CONFLICT: rg < og (reclaimed since the claim) -- clean
                    no-op: no version bump, no mutation. The formal analog of
                    "typed StaleReadGeneration conflict, never a silent
                    drop". *)
                 /\ ~(rg = og)
                 /\ UNCHANGED <<version, staleApply>>
        /\ UNCHANGED <<clock, mesiState, lastHeartbeat, grantedAtTick,
                       lastReclamation, ownerGeneration, readGeneration>>

--------------------------------------------------------------------
(* FencingSweepAction: SweepAction + bump ownerGeneration on every reclaimed
   artifact, atomically with the M/E -> I transition. Re-derives the
   reclaimSet from CrashRecovery.SweepAction (which cannot be reached from
   outside its LET) so the generation bump is part of the same atomic step. *)
--------------------------------------------------------------------

FencingSweepAction ==
    /\ clock > 0
    /\ LET snapshot == { <<ag, art>> \in (Agents \X Artifacts) :
                           mesiState[art][ag] \in MorE }
           reclaimSet == { <<ag, art>> \in snapshot :
                             SweepTrigger(ag, art) /= None }
           reclaimedArts == { art \in Artifacts :
                                \E ag \in Agents : <<ag, art>> \in reclaimSet }
       IN /\ reclaimSet /= {}
          /\ \A art \in reclaimedArts : ownerGeneration[art] < MaxGen
          /\ mesiState' = [art \in Artifacts |->
               [ag \in Agents |->
                   IF <<ag, art>> \in reclaimSet THEN "I"
                   ELSE mesiState[art][ag]]]
          /\ grantedAtTick' = [ag \in Agents |-> [art \in Artifacts |->
               IF <<ag, art>> \in reclaimSet THEN None
               ELSE grantedAtTick[ag][art]]]
          /\ lastReclamation' = [ag \in Agents |-> [art \in Artifacts |->
               IF <<ag, art>> \in reclaimSet
               THEN <<SweepTrigger(ag, art), clock>>
               ELSE lastReclamation[ag][art]]]
          /\ ownerGeneration' = [art \in Artifacts |->
               IF art \in reclaimedArts THEN ownerGeneration[art] + 1
               ELSE ownerGeneration[art]]
    /\ UNCHANGED <<clock, version, lastHeartbeat, readGeneration, staleApply>>

--------------------------------------------------------------------
(* Specification *)
--------------------------------------------------------------------

(* Inherited CR actions keep the fence variables unchanged. CRWriteAction
   models the pessimistic E/M acquire; CRFetchAction the read -> S. The
   unguarded CRCommitAction and the inherited SweepAction are DELIBERATELY
   replaced by FencingCommitAction and FencingSweepAction (the crux modeling
   decision above). *)
FencingNext ==
    \/ (CRFetchAction      /\ UNCHANGED <<ownerGeneration, readGeneration, staleApply>>)
    \/ (CRWriteAction      /\ UNCHANGED <<ownerGeneration, readGeneration, staleApply>>)
    \/ (CRInvalidateAction /\ UNCHANGED <<ownerGeneration, readGeneration, staleApply>>)
    \/ (CRTickAction       /\ UNCHANGED <<ownerGeneration, readGeneration, staleApply>>)
    \/ (HeartbeatAction    /\ UNCHANGED <<ownerGeneration, readGeneration, staleApply>>)
    \/ FencingSweepAction
    \/ ObserveGenAction
    \/ FencingCommitAction

FencingSpec == FencingInit /\ [][FencingNext]_fenceVars

--------------------------------------------------------------------
(* Invariants *)
--------------------------------------------------------------------

FencingTypeOK ==
    /\ CRTypeOK
    /\ staleApply \in BOOLEAN
    /\ \A art \in Artifacts : ownerGeneration[art] \in 0..MaxGen
    /\ \A ag \in Agents, art \in Artifacts :
         readGeneration[ag][art] \in ({None} \cup (0..MaxGen))

(* Sanity: a captured read-generation never exceeds the artifact's current
   ownerGeneration (the counter only grows; a captured value is a past
   value). If this ever failed, the WIN/CONFLICT discrimination would be
   ill-founded. *)
ReadGenBounded ==
    \A ag \in Agents, art \in Artifacts :
        readGeneration[ag][art] /= None =>
            readGeneration[ag][art] <= ownerGeneration[art]

(* The headline safety property: no commit ever applied a write whose
   read-generation was superseded by a reclaim. SingleWriter,
   MonotonicVersion, and the CR invariants (I3-I6) are inherited and
   re-checked to validate composition. *)
NoStaleApply == staleApply = FALSE

==========================================================================
