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
#
# or, with a normal install:
#
#     pip install "agent-coherence>=0.9.0" && python demo.py
#
"""Shared knowledge base — a lost update in a RAG/agent-memory corpus, reproduced then prevented.

A RAG corpus and the notes agents write back to it are shared mutable state. A
"consistent store" (Notion, Confluence, a vector DB, a memory layer) doesn't save
the consumer here, because the staleness isn't in the store — it's in *an agent's
cached view of a record*. Two agents retrieve the same knowledge record; one
appends a finding and commits; the second writes back from the snapshot it cached
*before* that commit. Last write wins. The first agent's finding is silently gone,
nothing errors, and every downstream answer is now grounded in a record that
dropped a fact.

This is the exact shape reverse-engineered from a real federated agent fleet (76
production transcripts) that silently clobbered a shared learnings file.

Below: the lost update over plain files (BROKEN), then the same sequence routed
through `CoherentVolume` (FIXED) — the stale write is denied fail-closed, the
agent re-reads the current record and rewrites, and both findings survive.

Sequenced, not raced: deterministic and offline. The FIXED case spawns a local
coordinator subprocess on 127.0.0.1 and tears it down at the end. No network
beyond localhost, no model calls, no cost.
"""

from __future__ import annotations

import shutil
import tempfile
from pathlib import Path

from ccs.adapters.claude_code.lifecycle import LifecycleConfig, stop_coordinator
from ccs.adapters.coherent_volume import CoherentVolume
from ccs.core.exceptions import CoherenceError

# The shared knowledge-base record both agents retrieve from and write back to —
# the RAG-native generalization of "two agents share one file." Think: a team
# account/knowledge note, a learnings.md a research fleet appends to, a memory
# record N sessions enrich.
_REL = "knowledge/acme_corp.md"
_SEED = "# Acme Corp — account knowledge\n- Industry: logistics\n"
_FINDING_A = "- Auth: SSO via Okta (SAML)\n"  # security-research agent's finding
_FINDING_B = "- Region: us-west-2\n"  # ops-research agent's finding

# Snappy local coordinator spawn for a one-command demo (no idle shutdown mid-run).
_DEMO_CFG = LifecycleConfig(
    idle_shutdown_sec=0,
    sweep_interval_sec=0.1,
    port_file_retry_attempts=40,
    port_file_retry_interval_sec=0.05,
    connect_retry_attempts=20,
    connect_retry_interval_sec=0.05,
)


def run_broken() -> dict[str, object]:
    """Plain files, no coordination: agent B's write from its cached snapshot
    silently clobbers agent A's finding (the lost update)."""
    workspace = Path(tempfile.mkdtemp(prefix="skb_broken_"))
    kb = workspace / _REL
    kb.parent.mkdir(parents=True, exist_ok=True)
    try:
        kb.write_text(_SEED)

        a_view = kb.read_text()  # agent A retrieves the record
        b_view = kb.read_text()  # agent B retrieves the SAME record (its cached snapshot)

        # Agent A appends its finding and writes back the full record.
        kb.write_text(a_view + _FINDING_A)

        # Agent B appends its finding to the snapshot it cached *before* A's
        # commit, and writes that back — silently overwriting A's finding.
        kb.write_text(b_view + _FINDING_B)

        final = kb.read_text()
        return {
            "final": final,
            "a_finding_present": _FINDING_A.strip() in final,
            "b_finding_present": _FINDING_B.strip() in final,
            "lost_update": _FINDING_A.strip() not in final,  # A's finding dropped
        }
    finally:
        shutil.rmtree(workspace, ignore_errors=True)


def run_fixed() -> dict[str, object]:
    """Same sequence through CoherentVolume: B's stale write is denied, B
    reacquires the current record (now holding A's finding) and rewrites — both
    findings survive."""
    workspace = Path(tempfile.mkdtemp(prefix="skb_fixed_"))
    kb = workspace / _REL
    kb.parent.mkdir(parents=True, exist_ok=True)
    kb.write_text(_SEED)

    # Agent A spawns the coordinator (strict on the managed glob); agent B is a
    # sibling instance attaching to the same coordinator. Real fleets are
    # separate processes; here they are two instances, sequenced.
    vol_a = CoherentVolume(workspace, managed=("knowledge/**",), config=_DEMO_CFG)
    try:
        vol_b = CoherentVolume(workspace, managed=("knowledge/**",), config=_DEMO_CFG)

        a_view = vol_a.read(_REL).decode()  # A retrieves (SHARED view registered)
        b_view = vol_b.read(_REL).decode()  # B retrieves (SHARED view registered)

        vol_a.write(_REL, (a_view + _FINDING_A).encode())  # A commits -> B's view INVALID

        denied = False
        recovered = False
        try:
            # B attempts its write from the now-stale cached snapshot.
            vol_b.write(_REL, (b_view + _FINDING_B).encode())
        except CoherenceError:
            # PREVENTION: the lost update is denied, fail-closed.
            denied = True
            # RECOVERY: reacquire (re-mint identity + a mandatory fresh read),
            # then append to the CURRENT record (which now includes A's finding).
            fresh = vol_b.reacquire(_REL).decode()
            vol_b.write(_REL, (fresh + _FINDING_B).encode())
            recovered = True

        final = kb.read_text()
        return {
            "final": final,
            "b_write_denied": denied,
            "b_recovered_via_reacquire": recovered,
            "a_finding_present": _FINDING_A.strip() in final,
            "b_finding_present": _FINDING_B.strip() in final,
            "lost_update": _FINDING_A.strip() not in final,
        }
    finally:
        stop_coordinator(workspace)
        shutil.rmtree(workspace, ignore_errors=True)


def _print_kb(label: str, text: str) -> None:
    print(f"  {label}:")
    for line in text.splitlines():
        print(f"    │ {line}")


def main() -> int:
    print("Shared knowledge base — a lost update in a RAG/agent-memory corpus.")
    print("Two research agents enrich one shared record. Same read→write sequence")
    print("both times; only the coordination differs.\n")

    broken = run_broken()
    print("BROKEN (no coherence) — plain files, last write wins")
    print("  A appends 'Auth: SSO via Okta'; B (from its cached snapshot) appends 'Region: us-west-2'.")
    _print_kb("final record", str(broken["final"]).rstrip())
    print(f"  A's finding survived: {broken['a_finding_present']}")
    print(f"  LOST UPDATE: {broken['lost_update']}   <- A's Okta finding silently dropped\n")

    fixed = run_fixed()
    print("FIXED (CoherentVolume) — B's stale write denied, then recovered")
    print(f"  B's stale write denied: {fixed['b_write_denied']}  (fail-closed)")
    print(f"  B recovered via reacquire(): {fixed['b_recovered_via_reacquire']}")
    _print_kb("final record", str(fixed["final"]).rstrip())
    print(f"  A's finding survived: {fixed['a_finding_present']}   B's finding survived: {fixed['b_finding_present']}")
    print(f"  LOST UPDATE: {fixed['lost_update']}\n")

    print("The staleness was never in the store — both writes 'succeeded' at the")
    print("filesystem. It was in B's cached view of the record. A consistent store")
    print("can't catch that; a coherence layer over the read/write boundary can:")
    print("B's view was invalidated by A's commit, so B's write from it is refused")
    print("until B re-reads. Same guarantee maps onto a vector DB, a memory layer,")
    print("or a reasoning-RAG index — wherever agents cache a record and write back.\n")

    print("Side effect — gating reads on version also cuts cost. A write publishes")
    print("a ~12-token invalidation instead of rebroadcasting the full record, so a")
    print("reader holding a still-valid view doesn't re-pay for context that didn't")
    print("change. A pre-registered, reproducible sweep puts the re-fetch savings at")
    print("≥30% sustained for change-rates r ≤ 0.30 (crossover r≈0.31, PASS at n=50).")
    print("That's a regime map on synthetic sources, not a measured invoice — but it")
    print("reproduces from committed code:")
    print("  benchmarks/cost_preregistration.md  +  tools/run_cost_sweep.py\n")

    # Trustworthy, not eyeballed: exit non-zero unless the invariant held both
    # ways (BROKEN must lose A's finding; FIXED must keep it).
    ok = bool(broken["lost_update"]) and not bool(fixed["lost_update"]) and bool(fixed["b_recovered_via_reacquire"])
    if ok:
        print("Invariant held: BROKEN lost A's finding; FIXED preserved both. (exit 0)")
    else:
        print("Invariant FAILED — the demo did not reproduce as expected. (exit 1)")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())

# The lost update above is reverse-engineered from a real federated fleet:
#   https://github.com/Cohexa-ai/agent-coherence/tree/main/examples/coherent_volume
# RAG / shared-agent-memory positioning:
#   https://github.com/Cohexa-ai/agent-coherence#agent-coherence
# Cost pre-registration (PASS at n=50, caveats kept honest):
#   https://github.com/Cohexa-ai/agent-coherence/blob/main/benchmarks/cost_preregistration.md
