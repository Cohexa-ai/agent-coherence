------------------------------- MODULE OCC -------------------------------
(* Optimistic-concurrency commit-CAS amendment to the MESI + crash-recovery
   protocol. Models a version-checked commit (commit_cas) that closes the
   concurrent lost-update the pessimistic write path leaves open.

   Plan: docs/plans/2026-06-08-001-feat-occ-write-api-same-host-v1-plan.md
   (Unit 1). Composes with CrashRecovery via EXTENDS.

   Adds:
     - observedVersion : the version each agent last read (its expected_version)
     - lostUpdate      : a sticky history flag — TRUE iff any commit_cas ever
                         succeeded on a stale observed version (a lost update)
     - ObserveAction   : an S/I agent reads the current version into its slot
     - CommitCASAction : optimistic commit — succeeds iff observed == current
                         AND there is no other M/E holder; else a clean no-op
                         conflict (no mutation, no silent drop).

   KEY MODELING DECISION (the crux). OCC writers stay in S/I and NEVER acquire
   EXCLUSIVE. So two concurrent OCC writers are *both* S; the version check
   elects exactly one winner and the loser observes a version_mismatch. We do
   NOT model two simultaneous EXCLUSIVE holders — that is a SingleWriter
   violation and not how OCC works. OCC-vs-pessimistic coexistence IS modeled:
   an inherited CRWriteAction can put a peer in E, and the OCC commit then sees
   `other_holder` (the `otherHolder` guard).

   LIVENESS is discharged as safety + prose (founder-resolved; see plan Key
   Decisions). TLC checks the NoLostUpdate safety invariant only; bounded
   progress is a prose argument (each OCC-vs-OCC conflict is a version_mismatch,
   so the version advanced => some writer committed => system-wide progress is
   monotonic). No temporal / fairness property is checked — consistent with the
   repo's safety-only TLC convention (README: "Full liveness proofs" out of
   scope).

   GrantUpdate is overridden (OCCGrantUpdate) to set a fresh grantedAtTick on
   the S->M commit_cas transition; the base CrashRecovery.GrantUpdate routes
   S->M through its OTHER branch and would leave the slot stale. This is a
   MODEL-ONLY correction: the live code already sets a fresh tick on any S->M
   (sqlite_registry.py, `new_in_me and not prev_in_me`). *)

EXTENDS CrashRecovery

VARIABLES observedVersion, lostUpdate

occVars == <<allVars, observedVersion, lostUpdate>>

--------------------------------------------------------------------
(* Initialization *)
--------------------------------------------------------------------

OCCInit ==
    /\ CRInit
    /\ observedVersion = [ag \in Agents |-> [art \in Artifacts |-> None]]
    /\ lostUpdate = FALSE

--------------------------------------------------------------------
(* grantedAtTick maintenance with the S->M fix (R-CR-1, model-only) *)
--------------------------------------------------------------------

OCCGrantUpdate(ag, art, oldSt, newSt) ==
    CASE oldSt \in MorE /\ newSt \in MorE         -> grantedAtTick[ag][art]
      [] oldSt \in {"I", "S"} /\ newSt \in MorE   -> clock   (* S included: the fix *)
      [] oldSt \in MorE /\ newSt \notin MorE       -> None
      [] OTHER                                      -> grantedAtTick[ag][art]

OCCUpdatedGrantedAtTick ==
    [ag \in Agents |-> [art \in Artifacts |->
        OCCGrantUpdate(ag, art, mesiState[art][ag], mesiState'[art][ag])]]

--------------------------------------------------------------------
(* ObserveAction: an S/I agent reads the current version into its slot.
   This is the OCC "read" that supplies expected_version. It is decoupled
   from FetchAction on purpose, so we never have to reach into FetchAction's
   existential binding (the defect the archived spec's OCCFetchAction hit). *)
--------------------------------------------------------------------

ObserveAction ==
    \E ag \in Agents, art \in Artifacts :
        /\ mesiState[art][ag] \in {"S", "I"}
        /\ observedVersion' = [observedVersion EXCEPT ![ag][art] = version[art]]
        /\ UNCHANGED <<baseVars, crVars, lostUpdate>>

--------------------------------------------------------------------
(* CommitCASAction: optimistic version-checked commit.
   WIN     iff observed == current AND no other M/E holder.
   CONFLICT otherwise (version_mismatch or other_holder) — a clean no-op. *)
--------------------------------------------------------------------

CommitCASAction ==
    \E ag \in Agents, art \in Artifacts :
        /\ mesiState[art][ag] \in {"S", "I"}             (* D4: OCC from S/I only *)
        /\ observedVersion[ag][art] /= None              (* must have read first *)
        /\ version[art] < MaxVersion                     (* finite bound *)
        /\ LET obs == observedVersion[ag][art]
               cur == version[art]
               otherHolder == \E peer \in Agents :
                                  peer /= ag /\ mesiState[art][peer] \in MorE
           IN \/ (* WIN: version matches, sole writer -> commit, S->M, invalidate *)
                 /\ obs = cur
                 /\ ~otherHolder
                 /\ mesiState' = [mesiState EXCEPT
                      ![art] = [peer \in Agents |->
                          IF peer = ag THEN "M"
                          ELSE IF peer \in NonInvalidPeers(art, ag) THEN "I"
                          ELSE mesiState[art][peer]]]
                 /\ version' = [version EXCEPT ![art] = cur + 1]
                 /\ observedVersion' = [observedVersion EXCEPT ![ag][art] = cur + 1]
                 (* Records a lost update iff this commit landed on a stale read.
                    In the correct spec `obs = cur` is required, so (obs # cur)
                    is always FALSE here. The mutant (remove the `obs = cur`
                    conjunct above) lets a stale commit win and flips
                    lostUpdate -> TRUE, which TLC reports as a NoLostUpdate
                    violation. This is what gives the invariant teeth. *)
                 /\ lostUpdate' = (lostUpdate \/ (obs /= cur))
              \/ (* CONFLICT: version_mismatch (obs # cur) or other_holder.
                    Clean no-op: no mutation of state, version, or the winner's
                    data. This is the formal analog of "typed conflict return,
                    never a silent drop". *)
                 /\ ~(obs = cur /\ ~otherHolder)
                 /\ UNCHANGED <<mesiState, version, observedVersion, lostUpdate>>
        /\ grantedAtTick'   = OCCUpdatedGrantedAtTick
        /\ lastReclamation' = UpdatedLastReclamation
        /\ UNCHANGED <<clock, lastHeartbeat>>

--------------------------------------------------------------------
(* Specification *)
--------------------------------------------------------------------

(* Inherited CR actions keep the OCC variables unchanged. CRWriteAction +
   CRCommitAction model the legacy pessimistic path so OCC-vs-pessimistic
   `other_holder` is exercised. *)
OCCNext ==
    \/ (CRFetchAction      /\ UNCHANGED <<observedVersion, lostUpdate>>)
    \/ (CRWriteAction      /\ UNCHANGED <<observedVersion, lostUpdate>>)
    \/ (CRCommitAction     /\ UNCHANGED <<observedVersion, lostUpdate>>)
    \/ (CRInvalidateAction /\ UNCHANGED <<observedVersion, lostUpdate>>)
    \/ (CRTickAction       /\ UNCHANGED <<observedVersion, lostUpdate>>)
    \/ (HeartbeatAction    /\ UNCHANGED <<observedVersion, lostUpdate>>)
    \/ (SweepAction        /\ UNCHANGED <<observedVersion, lostUpdate>>)
    \/ ObserveAction
    \/ CommitCASAction

OCCSpec == OCCInit /\ [][OCCNext]_occVars

--------------------------------------------------------------------
(* Invariants *)
--------------------------------------------------------------------

OCCTypeOK ==
    /\ CRTypeOK
    /\ lostUpdate \in BOOLEAN
    /\ \A ag \in Agents, art \in Artifacts :
         observedVersion[ag][art] \in ({None} \cup (1..MaxVersion))

(* The headline safety property: no successful commit_cas ever landed on a
   stale observed version. SingleWriter, MonotonicVersion, and the CR
   invariants (I3-I6) are inherited and re-checked to validate composition. *)
NoLostUpdate == lostUpdate = FALSE

==========================================================================
