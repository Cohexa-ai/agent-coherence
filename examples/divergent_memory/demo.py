# /// script
# requires-python = ">=3.11"
# dependencies = ["agent-coherence>=0.9.0"]
# ///
# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents
#
# Run it (no repo checkout, no API keys, offline):
#
#     uv run demo.py
# or
#     pip install "agent-coherence>=0.9.0" && python demo.py
#
"""Divergent memory — two sessions record contradictory beliefs from a stale read.

A memory layer captures what each session learned. When two sessions work the same
project, they read shared state, each reaches a conclusion, and each writes that
conclusion back. If a session reasons from a snapshot it cached *before* a peer
committed a decision, it records a belief that contradicts the peer's — and now the
durable memory holds two conflicting answers to the same question. A future agent
that reads both gets incoherent context. Nothing errored; the store is "consistent"
in the sense that every write landed. The incoherence is in *what the sessions
believed*, derived from stale reads.

This is the divergent-view failure mode — distinct from a lost update (where one
write clobbers another). Here both writes "succeed"; the problem is they encode
contradictory derived conclusions. It shows up in real agent-memory trackers as
"no cross-session isolation — all sessions' observations are mixed" and
"parallel sessions silently drop observations under lock contention."

Below: the divergence over plain files (BROKEN), then the same sequence routed
through `CoherentVolume` (FIXED). A's commit invalidates B's cached view, so B's
write from the stale snapshot is denied; B reacquires, re-reads the *current*
decision, and records a conclusion consistent with it — the two sessions no longer
diverge.

Honest scope: the library prevents the **stale read that causes** the divergence
(B can't record a conclusion derived from a view a peer already superseded without
re-reading first). It does **not** reconcile or merge two conclusions an agent has
*already* recorded into separate stores — it stops the divergence forming, it does
not repair divergence after the fact.

Sequenced, not raced: deterministic and offline. The FIXED case spawns a local
coordinator on 127.0.0.1 and tears it down. No network beyond localhost, no model
calls, no cost.
"""

from __future__ import annotations

import shutil
import tempfile
from pathlib import Path

from ccs.adapters.claude_code.lifecycle import LifecycleConfig, stop_coordinator
from ccs.adapters.coherent_volume import CoherentVolume
from ccs.core.exceptions import CoherenceError

# The shared decision record both sessions read and write their conclusion to —
# the durable cross-session memory: an observation store, a memory record, or a
# team decisions.md that a research fleet reads and writes back to.
_REL = "memory/db_decision.md"
_SEED = "# Decision: database engine\nStatus: open (no decision recorded yet)\n"

# What each session concludes. A goes first and decides Postgres; B, reasoning from
# the *pre-A* snapshot, independently concludes SQLite — contradicting A.
_A_CONCLUSION = "Decision: PostgreSQL (managed RDS). Recorded by: session-A.\n"
_B_CONCLUSION = "Decision: SQLite (embedded). Recorded by: session-B.\n"
# When B re-reads the current record (FIXED case) it sees A already decided, so its
# conclusion is derived from the current state instead of the stale one.
_B_CONCLUSION_INFORMED = "Reviewed: PostgreSQL already recorded by session-A; session-B concurs.\n"

_DEMO_CFG = LifecycleConfig(
    idle_shutdown_sec=0,
    sweep_interval_sec=0.1,
    port_file_retry_attempts=40,
    port_file_retry_interval_sec=0.05,
    connect_retry_attempts=20,
    connect_retry_interval_sec=0.05,
)


def _belief(record: str) -> str:
    """The single decision a reader of the record would carry away."""
    for line in record.splitlines():
        if line.startswith("Decision:") or line.startswith("Reviewed:"):
            return line
    return "(no decision)"


def run_broken() -> dict[str, object]:
    """Plain files, no coordination: B records a conclusion derived from its stale
    snapshot, contradicting A — the two sessions' memories diverge."""
    workspace = Path(tempfile.mkdtemp(prefix="divmem_broken_"))
    rec = workspace / _REL
    rec.parent.mkdir(parents=True, exist_ok=True)
    try:
        rec.write_text(_SEED)

        a_view = rec.read_text()  # session A reads the record (open)
        b_view = rec.read_text()  # session B reads the SAME record (its cached snapshot)

        # Session A reasons, decides PostgreSQL, records it.
        rec.write_text(a_view + _A_CONCLUSION)
        a_belief = "PostgreSQL"

        # Session B reasons from the snapshot it cached *before* A's commit — never
        # sees A's decision — concludes SQLite and records it over its stale view.
        rec.write_text(b_view + _B_CONCLUSION)
        b_belief = "SQLite"

        final = rec.read_text()
        record_says = _belief(final)
        return {
            "a_session_believes": a_belief,
            "b_session_believes": b_belief,
            "record_says": record_says,
            "final": final,
            # Divergence: the two sessions reached contradictory conclusions about
            # the same decision, and the durable record reflects only the later
            # stale one — A's decision and the fact one was ever made are gone.
            "diverged": a_belief != b_belief,
        }
    finally:
        shutil.rmtree(workspace, ignore_errors=True)


def run_fixed() -> dict[str, object]:
    """Same sequence through CoherentVolume: A's commit invalidates B's view, B's
    stale write is denied, B reacquires and re-reads the current decision, and
    records a conclusion consistent with it — no divergence."""
    workspace = Path(tempfile.mkdtemp(prefix="divmem_fixed_"))
    rec = workspace / _REL
    rec.parent.mkdir(parents=True, exist_ok=True)
    rec.write_text(_SEED)

    vol_a = CoherentVolume(workspace, managed=("memory/**",), config=_DEMO_CFG)
    try:
        vol_b = CoherentVolume(workspace, managed=("memory/**",), config=_DEMO_CFG)

        a_view = vol_a.read(_REL).decode()  # A reads (SHARED view registered)
        b_view = vol_b.read(_REL).decode()  # B reads (SHARED view registered)

        vol_a.write(_REL, (a_view + _A_CONCLUSION).encode())  # A commits -> B INVALID
        a_belief = "PostgreSQL"

        denied = False
        b_belief = "SQLite"  # what B would have recorded from its stale view
        try:
            # B attempts to record its stale-derived conclusion.
            vol_b.write(_REL, (b_view + _B_CONCLUSION).encode())
        except CoherenceError:
            # PREVENTION: B's view is stale (A superseded it); the write is denied.
            denied = True
            # RECOVERY: reacquire forces a fresh read of the current record. B now
            # sees A already decided PostgreSQL, so its recorded conclusion is
            # derived from the *current* state, not the stale snapshot.
            fresh = vol_b.reacquire(_REL).decode()
            vol_b.write(_REL, (fresh + _B_CONCLUSION_INFORMED).encode())
            b_belief = "PostgreSQL"  # B's conclusion now aligns with the current record

        final = rec.read_text()
        return {
            "b_write_denied": denied,
            "a_session_believes": a_belief,
            "b_session_believes": b_belief,
            "record_says": _belief(final),  # the decision a reader carries away: A's
            "final": final,
            "diverged": a_belief != b_belief,
        }
    finally:
        stop_coordinator(workspace)
        shutil.rmtree(workspace, ignore_errors=True)


def _print_record(label: str, text: str) -> None:
    print(f"  {label}:")
    for line in text.splitlines():
        print(f"    │ {line}")


def main() -> int:
    print("Divergent memory — two sessions record contradictory beliefs from a stale read.")
    print("Both sessions evaluate the same decision and write their conclusion back.")
    print("Same read→write sequence both times; only the coordination differs.\n")

    broken = run_broken()
    print("BROKEN (no coherence) — plain files")
    print("  Session A concludes PostgreSQL and records it. Session B, reasoning from")
    print("  the snapshot it cached before A's commit, concludes SQLite and records it.")
    _print_record("durable record", str(broken["final"]).rstrip())
    print(f"  Session A believes: {broken['a_session_believes']}   Session B believes: {broken['b_session_believes']}")
    print(f"  DIVERGED: {broken['diverged']}   <- two sessions, contradictory beliefs about one decision\n")

    fixed = run_fixed()
    print("FIXED (CoherentVolume) — B's stale write denied, B re-reads, then concurs")
    print(f"  B's stale write denied: {fixed['b_write_denied']}  (fail-closed)")
    _print_record("durable record", str(fixed["final"]).rstrip())
    print(f"  Session A believes: {fixed['a_session_believes']}   Session B believes: {fixed['b_session_believes']}")
    print(f"  DIVERGED: {fixed['diverged']}\n")

    print("The store was 'consistent' both times — every write landed. The incoherence")
    print("in BROKEN was in what the two sessions *believed*, derived from a stale read.")
    print("A coherence layer over the read/write boundary catches that: A's commit")
    print("invalidated B's view, so B couldn't record a conclusion from the superseded")
    print("snapshot without re-reading — the contradiction never forms. Same guarantee")
    print("maps onto a memory layer, an observation store, or a reasoning-RAG index.\n")

    print("Honest scope: the library prevents the stale read that *causes* divergence.")
    print("It does not reconcile two conclusions an agent already recorded into")
    print("separate stores — it stops the divergence forming, it does not repair it.")
    print("Cost side effect (gating reads also cuts redundant re-fetches): a")
    print("pre-registered sweep puts the savings at ≥30% for change-rates r ≤ 0.30")
    print("(reproducible from committed code, not a measured invoice) —")
    print("benchmarks/cost_preregistration.md.\n")

    # Trustworthy, not eyeballed: BROKEN must diverge; FIXED must converge via the
    # denied-then-reacquired path.
    ok = bool(broken["diverged"]) and not bool(fixed["diverged"]) and bool(fixed["b_write_denied"])
    if ok:
        print("Invariant held: BROKEN diverged; FIXED converged after the denied stale write. (exit 0)")
    else:
        print("Invariant FAILED — the demo did not reproduce as expected. (exit 1)")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())

# The divergent-view failure mode is real and recurring in agent-memory systems:
# sessions losing cross-session isolation (observations from separate sessions
# mixed), and parallel sessions silently dropping observations. Sibling demo (lost
# update, write-clobber):
#   https://github.com/hipvlady/agent-coherence/tree/main/examples/shared_knowledge_base
# RAG / shared-agent-memory positioning:
#   https://github.com/hipvlady/agent-coherence#agent-coherence
