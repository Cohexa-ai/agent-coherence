# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Broken case: a stale memory write-back erases a peer's update (lost update).

A shared memory record holds an agent's working knowledge about a customer:
``{"summary": ..., "facts": [...]}``. Two agents touch it. Agent A reads the
record; agent B appends a freshly-learned fact and writes it back; then agent A
writes an *edit it computed from the snapshot it read before B's write* — a
refreshed summary carried on top of the OLD fact list. A's write-back silently
overwrites B's appended fact: the classic RAG failure where "a better vector DB
won't help — the agent cached a memory record, the source moved, and it wrote
back a stale edit that erased the update."

No coordinator is involved; that is the point. ``fixed.py`` shows the same
sequence denied and recovered through CoherentVolume.

Deterministic and offline — sequenced, not raced.
"""

from __future__ import annotations

import json
import shutil
import sys
import tempfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

# The record the two agents share (a RAG/memory entry for one customer).
_V1 = {"summary": "Customer prefers email contact.", "facts": ["channel: email"]}
# Fact B learns and appends while A is mid-edit.
_B_FACT = "timezone: PST"
# The summary refinement A wants to apply (A's edit intent).
_A_SUMMARY = "Customer prefers email contact; flagged VIP."
_SCENARIO = "two agents edit one shared memory record (A refines the summary, B appends a fact)"


def _dumps(record: dict) -> str:
    """Stable serialization so the demo is byte-deterministic."""
    return json.dumps(record, sort_keys=True)


def run_broken() -> dict[str, object]:
    """Sequenced read→write with no coherence; A's stale edit clobbers B's fact.

    Returns a structured trace so the runner and tests can assert the lost
    update without parsing prose.
    """
    workspace = Path(tempfile.mkdtemp(prefix="rag_stale_memory_broken_"))
    target = workspace / "memory.json"
    try:
        target.write_text(_dumps(_V1))

        a_view = json.loads(target.read_text())  # agent A reads the record (v1)

        # Agent B learns a new fact and writes the record back (this is v2).
        b_view = json.loads(target.read_text())
        b_view["facts"].append(_B_FACT)
        target.write_text(_dumps(b_view))  # v2 on disk: facts include B's new fact

        # Agent A now writes an edit computed from its PRE-B snapshot: it refreshes
        # the summary but carries A's OLD fact list. B's appended fact is erased.
        a_view["summary"] = _A_SUMMARY
        target.write_text(_dumps(a_view))  # clobbers v2

        final = json.loads(target.read_text())
        b_fact_survived = _B_FACT in final["facts"]
        a_edit_survived = final["summary"] == _A_SUMMARY
        return {
            "scenario": _SCENARIO,
            "b_fact": _B_FACT,
            "a_summary": _A_SUMMARY,
            "final_facts": final["facts"],
            "final_summary": final["summary"],
            "b_fact_survived": b_fact_survived,  # False — B's update was erased
            "a_edit_survived": a_edit_survived,
            # A converged copy where BOTH survive is the goal; the lost update is
            # exactly "A's edit landed but B's fact did not".
            "lost_update": a_edit_survived and not b_fact_survived,
        }
    finally:
        shutil.rmtree(workspace, ignore_errors=True)


def main() -> int:
    trace = run_broken()
    print("BROKEN (no coherence) — stale memory write-back over a plain file")
    print(f"  scenario: {trace['scenario']}")
    print(f"  B appended fact {trace['b_fact']!r}; A then wrote back its refreshed summary")
    print(f"  final summary: {trace['final_summary']!r}")
    print(f"  final facts:   {trace['final_facts']}")
    print(
        f"  LOST UPDATE: {trace['lost_update']}  "
        f"(A's edit survived={trace['a_edit_survived']}, "
        f"B's fact survived={trace['b_fact_survived']})"
    )
    # Exit code reflects the invariant so an agent can use this as a gate.
    return 0 if trace["lost_update"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
