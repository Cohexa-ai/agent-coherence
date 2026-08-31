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
                         path). None until captured -- the absent operand the
                         commit ADMITS (a writer with no fence claim;
                         version-CAS arbitrates it -- see the commit bullet).
     - staleApply      : a sticky history flag -- TRUE iff any commit ever
                         applied a write from a CAPTURED read-generation older
                         than the artifact's current ownerGeneration (a stale
                         apply -- the exact failure the fence prevents).
     - ObserveGenAction  : the GENUINE-READ capture leg -- an agent captures
                           the current ownerGeneration into its slot on a read.
                           DECOUPLED from the read on purpose (mirrors OCC.tla's
                           ObserveAction), so the sweep can interleave BETWEEN
                           the read-set capture and the OCC commit -- the
                           non-atomic-capture hazard the plan-review required
                           this spec to model. (The E/M-acquire capture leg is
                           atomic and lives in FencingWriteAction.)
     - FencingWriteAction : the pessimistic E/M acquire (inherited
                            CRWriteAction) PLUS the read-generation capture,
                            keyed on the I/S -> M/E transition (atomic with the
                            acquire, exactly as the registry captures it). So a
                            reclaim-zombie always carries a captured operand and
                            is caught present-and-stale; absent means a writer
                            that never acquired and never read.
     - FencingSweepAction: SweepAction + bump ownerGeneration on each
                           reclaimed artifact, atomically.
     - silentRevoke    : a sticky history flag -- TRUE iff any step ever
                         revoked a write claim WITHOUT moving the ownership
                         epoch. NoStaleApply cannot see that failure (it is
                         defined relative to the very counter that fails to
                         move), so it is stated separately.
     - FencingInvalidateAction: the OTHER grant-revoking operation, likewise
                           bumping ownerGeneration on an M/E -> I transition.
                           The rule the bump keys on is NOT "the sweep did it"
                           but "a write claim was revoked WITHOUT the version
                           moving" -- and a release ends a claim at an unchanged
                           version exactly as a reclaim does. Modelling it as
                           the inherited unguarded CRInvalidateAction let a
                           release SUPPRESS the fence: the holder is already I,
                           so no later sweep has an M/E grant to reclaim and the
                           generation never moves at all -- a shipped-code defect
                           fixed alongside this amendment (EPOCH_BUMP_TRIGGERS
                           in registry_protocol.py).
     - FencingCommitAction: the generation-guarded commit. WIN iff
                            readGeneration = ownerGeneration (strict-> reject,
                            EQUALITY ADMITS); a present-but-superseded
                            readGeneration is a clean no-op conflict (the
                            typed-conflict analog -- never a silent drop). An
                            ABSENT readGeneration ADMITS -- a writer with no
                            fence claim, arbitrated by version-CAS (OCC.tla, a
                            sibling amendment), never by the fence.

   KEY MODELING DECISION (the crux). The fence is UNIFORM across both write
   paths, and its safety depends only on (readGeneration vs ownerGeneration),
   not on the committer's MESI state. So a single FencingCommitAction models
   BOTH the pessimistic commit and the OCC commit_cas, guarded. We therefore
   REPLACE the inherited unguarded commit (CRCommitAction), the inherited
   write (CRWriteAction), and the inherited SweepAction with their fenced
   equivalents -- in the fence world every version-bumping commit is
   generation-guarded, every E/M acquire captures its read-generation, and
   every reclaim bumps the generation. Capture happens at TWO claim-
   establishing points (the plan-review P0 fix -- not fetch-only): the E/M
   acquire and a genuine read. The acquire capture is ATOMIC with the acquire
   (FencingWriteAction, keyed on the I/S -> M/E transition -- the registry
   captures it the same way), so a pessimistic acquire-without-content-read
   still has a valid operand and is admitted, AND a reclaim-zombie can never be
   absent: it captured at acquire and kept the stale value through the reclaim,
   so it is always caught present-and-stale. The read capture stays DECOUPLED
   (ObserveGenAction), so the sweep can interleave between a read-set capture
   and the OCC commit -- the non-atomic-capture hazard the plan-review required
   this spec to model. The only way to reach a commit with an ABSENT operand is
   therefore to have neither acquired M/E nor read: a plain OCC writer whose
   lost-update protection is version-CAS (OCC.tla), not the fence. The fence
   ADMITS it; version-CAS arbitrates. The two invariants compose (NoStaleApply
   here, NoLostUpdate there); neither subsumes the other.

   SIBLING AMENDMENT: the pin on the revoke itself -- a signal minted at one
   epoch must not revoke a claim established later -- lives in ZombieRevoke.tla
   (NoZombieRevoke). It is kept OUT of this module on purpose: it
   needs a per-(agent, artifact) observation variable, and Fencing is EXTENDed
   by EffectGate, Retention and Snapshot, which would all pay that state-space
   cost for a property none of them is about. The epoch bump below adds no
   variable, so it stays here where every downstream amendment inherits it.

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

(* Generation space bound for finite model checking.

   MaxTicks alone is NOT enough. A sweep bump is tick-bounded, but
   FencingInvalidateAction's release bump is not: acquire/release cycles bump
   the epoch without advancing the clock, so the ceiling is reachable at
   clock 0. Both bump sites carry a `< MaxGen` guard, and a guard that fails
   DISABLES its action rather than violating anything -- so too low a ceiling
   silently stops TLC exploring revoke transitions while the run still reports
   success. Sized as MaxTicks + NumAgents, mirroring MESI.tla's MaxVersion,
   which solves the same problem (a counter advanced by agent actions rather
   than by the clock) the same way. *)
MaxGen == MaxTicks + NumAgents

VARIABLES ownerGeneration, readGeneration, staleApply, silentRevoke

fenceVars == <<allVars, ownerGeneration, readGeneration, staleApply,
               silentRevoke>>

(* The artifacts whose write claim this step revokes. Derived from unprimed vs
   primed state, so every conjunct that uses it must come AFTER mesiState' is
   assigned. Its binders are named apart from the callers' to avoid shadowing. *)
RevokedMorE ==
    { a \in Artifacts :
        \E g \in Agents : mesiState[a][g] \in MorE
                         /\ mesiState'[a][g] \notin MorE }

--------------------------------------------------------------------
(* Initialization *)
--------------------------------------------------------------------

FencingInit ==
    /\ CRInit
    /\ ownerGeneration = [art \in Artifacts |-> 0]
    /\ readGeneration  = [ag \in Agents |-> [art \in Artifacts |-> None]]
    /\ staleApply = FALSE
    /\ silentRevoke = FALSE

--------------------------------------------------------------------
(* ObserveGenAction: the genuine-read capture leg -- an agent captures the
   current ownerGeneration into its slot on a read (the OCC read-set capture).
   Decoupled from the read action so the sweep can fire between capture and
   commit (the non-atomic-capture hazard). The E/M-acquire capture is the OTHER
   leg, atomic with the acquire (FencingWriteAction). *)
--------------------------------------------------------------------

ObserveGenAction ==
    \E ag \in Agents, art \in Artifacts :
        /\ readGeneration' = [readGeneration EXCEPT ![ag][art] = ownerGeneration[art]]
        /\ UNCHANGED <<baseVars, crVars, ownerGeneration, staleApply,
                      silentRevoke>>

--------------------------------------------------------------------
(* FencingWriteAction: the pessimistic E/M acquire (inherited CRWriteAction)
   PLUS the atomic read-generation capture. The capture is keyed on the
   I/S -> M/E transition (computed from mesiState vs mesiState', the same
   discipline as CrashRecovery's UpdatedGrantedAtTick -- it never reaches into
   CRWriteAction's existential binding), exactly as the registry captures
   read_generation on the E/M-acquire state transition. An already-M/E holder
   re-writing (E -> E) does NOT re-capture, so its original operand is
   preserved. Modeling capture at the acquire (not via the decoupled
   ObserveGenAction) is what makes a reclaim-zombie always carry a captured
   operand: it is caught present-and-stale, never mistaken for an absent
   never-claimed writer. *)
--------------------------------------------------------------------

FencingWriteAction ==
    /\ CRWriteAction
    /\ readGeneration' = [ag \in Agents |-> [art \in Artifacts |->
         IF mesiState[art][ag] \notin MorE /\ mesiState'[art][ag] \in MorE
         THEN ownerGeneration[art]
         ELSE readGeneration[ag][art]]]
    /\ UNCHANGED <<ownerGeneration, staleApply, silentRevoke>>

--------------------------------------------------------------------
(* FencingCommitAction: generation-guarded commit (both paths, uniform).
   ADMIT (absent) iff readGeneration = None -- a writer with no fence claim;
            the fence does not arbitrate it, version-CAS (OCC.tla) does. It
            carries no captured generation, so it can never be a superseded
            apply: admit and bump the version.
   WIN     iff readGeneration = ownerGeneration (equality admits).
   CONFLICT iff readGeneration present but superseded by a reclaim
            (readGeneration < ownerGeneration) -- a clean no-op. *)
--------------------------------------------------------------------

FencingCommitAction ==
    \E ag \in Agents, art \in Artifacts :
        /\ version[art] < MaxVersion                (* finite bound *)
        /\ LET rg == readGeneration[ag][art]
               og == ownerGeneration[art]
           IN \/ (* ADMIT (absent operand): a plain OCC writer that never
                    established a fence claim. The fence does not arbitrate it
                    -- version-CAS (NoLostUpdate, OCC.tla) does. No captured
                    generation => can never be a superseded apply, so admit and
                    bump the version (staleApply cannot be implicated). *)
                 /\ rg = None
                 /\ version' = [version EXCEPT ![art] = version[art] + 1]
                 /\ UNCHANGED staleApply
              \/ (* WIN: claim present and current -> apply the write. *)
                 /\ rg /= None
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
              \/ (* CONFLICT: present but superseded (rg < og, reclaimed since
                    the claim) -- clean no-op: no version bump, no mutation. The
                    formal analog of "typed StaleReadGeneration conflict, never
                    a silent drop". *)
                 /\ rg /= None
                 /\ ~(rg = og)
                 /\ UNCHANGED <<version, staleApply>>
        /\ UNCHANGED <<clock, mesiState, lastHeartbeat, grantedAtTick,
                       lastReclamation, ownerGeneration, readGeneration,
                       silentRevoke>>

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
    /\ silentRevoke' = (silentRevoke
                       \/ (\E a2 \in RevokedMorE :
                             ownerGeneration'[a2] = ownerGeneration[a2]))
    /\ UNCHANGED <<clock, version, lastHeartbeat, readGeneration, staleApply>>

--------------------------------------------------------------------
(* FencingInvalidateAction: the voluntary release (service.invalidate) + the
   SAME generation bump the sweep performs, atomically with the M/E -> I
   transition. Both operations revoke a write claim without moving the version,
   which is the one condition version-CAS is structurally blind to, so both
   must move the epoch. Keyed on the M/E -> I transition computed from
   mesiState vs mesiState' (the UpdatedGrantedAtTick discipline), never reaching
   into CRInvalidateAction's existential binding. A release from S or I leaves
   the generation alone: no write claim was revoked. *)
--------------------------------------------------------------------

FencingInvalidateAction ==
    /\ CRInvalidateAction
    /\ \A a2 \in RevokedMorE : ownerGeneration[a2] < MaxGen   (* finite bound *)
    /\ ownerGeneration' = [a2 \in Artifacts |->
         IF a2 \in RevokedMorE THEN ownerGeneration[a2] + 1
         ELSE ownerGeneration[a2]]
    /\ silentRevoke' = (silentRevoke
                       \/ (\E a2 \in RevokedMorE :
                             ownerGeneration'[a2] = ownerGeneration[a2]))
    /\ UNCHANGED <<readGeneration, staleApply>>

--------------------------------------------------------------------
(* Specification *)
--------------------------------------------------------------------

(* Inherited CR actions keep the fence variables unchanged, EXCEPT the write
   and the invalidate: CRFetchAction models the read -> S. The unguarded
   CRCommitAction, the inherited CRWriteAction, the inherited SweepAction and
   the inherited CRInvalidateAction are DELIBERATELY replaced by
   FencingCommitAction, FencingWriteAction (which captures the read-generation
   at the E/M acquire), FencingSweepAction (the crux modeling decision above)
   and FencingInvalidateAction (which bumps the generation on the OTHER
   grant-revoking operation). *)
FencingNext ==
    \/ (CRFetchAction      /\ UNCHANGED <<ownerGeneration, readGeneration,
                                        staleApply, silentRevoke>>)
    \/ FencingWriteAction
    \/ FencingInvalidateAction
    \/ (CRTickAction       /\ UNCHANGED <<ownerGeneration, readGeneration, staleApply,
                                        silentRevoke>>)
    \/ (HeartbeatAction    /\ UNCHANGED <<ownerGeneration, readGeneration, staleApply,
                                        silentRevoke>>)
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
    /\ silentRevoke \in BOOLEAN
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
   CAPTURED read-generation was superseded by a reclaim. Scoped to a captured
   operand on purpose: an absent operand (a writer that never acquired M/E and
   never read) carries no read-generation to be superseded, so it is outside
   this property and is admitted (version-CAS, OCC.tla, is its lost-update
   guard). A reclaim-zombie is NEVER absent -- FencingWriteAction captures at
   the acquire -- so admit-on-absent removes no fence coverage. SingleWriter,
   MonotonicVersion, and the CR invariants (I3-I6) are inherited and re-checked
   to validate composition. *)
NoStaleApply == staleApply = FALSE

(* F2's own property, because NoStaleApply structurally cannot see it: that flag
   is defined RELATIVE to ownerGeneration, and the failure here is the
   generation not moving at all. (Confirmed by mutation: reverting the
   invalidate bump leaves NoStaleApply, ReadGenBounded and SingleWriter all
   satisfied -- the model reported no error.) So state it directly: a write
   claim is never revoked without the ownership epoch moving.

   Scoped to the two REVOKE actions (sweep reclaim, voluntary release), NOT to
   every M/E -> I transition. The acquire leg (WriteAction invalidating peers)
   also ends a claim at an unchanged version and deliberately does NOT bump, and
   that is sound because every exit from the acquirer's grant is covered: it
   commits (the version moves, version-CAS arbitrates), it is reclaimed (bump),
   or it releases (bump, since this amendment). There is no fourth exit. *)
NoSilentRevoke == silentRevoke = FALSE

==========================================================================
