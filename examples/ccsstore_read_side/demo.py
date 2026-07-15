# /// script
# requires-python = ">=3.11"
# dependencies = ["agent-coherence[langgraph]>=0.12.0"]
# ///
# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents
#
# Run it (no repo checkout, no API keys, offline):
#
#     uv run demo.py
# or
#     pip install "agent-coherence[langgraph]>=0.12.0" && python demo.py
#
"""CCSStore read-side coherence — and the write-side boundary, honestly.

Two agents share one artifact through a LangGraph-style ``BaseStore``. The
planner writes the decision; the reviewer reads it, caches it, and acts on it.

**Act 1 — what CCSStore ships: read-side invalidation.** The reviewer's second
read is a local cache hit (no re-fetch, no re-broadcast). The moment the
planner commits a new version, the reviewer's cached copy is invalidated — its
next ``get()`` is a fresh miss that returns the new version. The stale hit a
plain agent-local cache would have served never happens.

**Act 2 — what CCSStore does NOT do: ``put`` is not version-CAS.** A writer
holding a snapshot from before a peer's commit can still write back, and the
write lands — silently erasing the peer's update. No error is raised. This is
the documented boundary, not a bug in the demo: CCSStore keeps *readers*
honest; it does not deny a stale *write-back*.

**Act 3 — the write-side fix, one call away in the same library.** The same
edit routed through ``store.core.write_cas`` (the OCC commit-CAS shipped in
v0.9.1) re-applies the writer's intent against the freshly read version on
every attempt. Both the peer's update and the writer's edit survive — the
lost update never forms.

Honest scope: one CCSStore instance, one Python process, one host. Two
processes each constructing a CCSStore share nothing — for cross-process
state on one host use ``CoherentVolume`` or the ``stale-write-guard-fs`` MCP
server. Deterministic, offline, no model calls. Exit 0 iff every invariant
held.
"""

from __future__ import annotations

import json
import sys
from typing import Any

from ccs.adapters import CCSStore
from ccs.adapters.events import StoreMetricEvent
from ccs.core.identity import artifact_uuid

# namespace[0] is the agent identity; namespace[1:] is the shared artifact
# scope, so ("planner", "notes") and ("reviewer", "notes") address ONE artifact.
PLANNER = ("planner", "notes")
REVIEWER = ("reviewer", "notes")
KEY = "decision"


def _reviewer_gets(events: list[StoreMetricEvent]) -> list[StoreMetricEvent]:
    return [e for e in events if e.agent_name == "reviewer" and e.operation == "get"]


def run_readside() -> dict[str, Any]:
    """Act 1: peer commit invalidates the reviewer's cached view (FIXED-by-design)."""
    events: list[StoreMetricEvent] = []
    store = CCSStore(strategy="lazy", on_metric=events.append)

    store.put(PLANNER, KEY, {"db": "postgres", "reason": "relational joins"})  # v1
    first = store.get(REVIEWER, KEY)  # miss → coordinator fetch of v1
    second = store.get(REVIEWER, KEY)  # local cache hit — no re-fetch

    # The peer commits v2; the reviewer's cached copy goes INVALID.
    store.put(PLANNER, KEY, {"db": "postgres", "reason": "relational joins", "status": "approved-in-review"})
    third = store.get(REVIEWER, KEY)  # fresh miss → serves v2, never the stale hit

    gets = _reviewer_gets(events)
    return {
        "first_was_miss": gets[0].cache_hit is False,
        "second_was_local_hit": gets[1].cache_hit is True,
        "post_commit_was_fresh_miss": gets[2].cache_hit is False,
        "value_after_peer_commit": third.value if third else None,
        "saw_new_version": bool(third and third.value.get("status") == "approved-in-review"),
        "first_value": first.value if first else None,
        "second_value": second.value if second else None,
    }


def run_broken_writeback() -> dict[str, Any]:
    """Act 2: put() is not version-CAS — the stale write-back lands (BROKEN)."""
    store = CCSStore(strategy="lazy")

    store.put(PLANNER, KEY, {"db": "postgres"})
    snapshot = store.get(REVIEWER, KEY).value  # reviewer's basis: v1

    # The planner commits an update the reviewer never sees…
    store.put(PLANNER, KEY, {"db": "postgres", "budget": "approved"})

    # …and the reviewer writes back a value derived from its stale snapshot.
    edited = dict(snapshot)
    edited["owner"] = "reviewer"
    store.put(REVIEWER, KEY, edited)  # lands — no version check on put()

    final = store.get(PLANNER, KEY).value
    return {
        "planner_update_lost": "budget" not in final,
        "reviewer_edit_present": final.get("owner") == "reviewer",
        "final_value": final,
    }


def run_fixed_writeback() -> dict[str, Any]:
    """Act 3: the same intent through store.core.write_cas — nothing is lost (FIXED)."""
    store = CCSStore(strategy="lazy")

    store.put(PLANNER, KEY, {"db": "postgres"})
    store.get(REVIEWER, KEY)  # reviewer reads v1 (its stale basis)
    store.put(PLANNER, KEY, {"db": "postgres", "budget": "approved"})

    aid = artifact_uuid("notes", KEY)

    def apply_reviewer_edit(entry: Any) -> tuple[str, None]:
        # Invoked per attempt AFTER a fresh read — current is never stale here.
        current = json.loads(store.core.content(agent_name="reviewer", artifact_id=aid) or "{}")
        current["owner"] = "reviewer"
        return json.dumps(current, sort_keys=True, separators=(",", ":")), None

    store.core.write_cas(agent_name="reviewer", artifact_id=aid, make_content=apply_reviewer_edit, now_tick=0)

    final = store.get(PLANNER, KEY).value
    return {
        "planner_update_survived": final.get("budget") == "approved",
        "reviewer_edit_present": final.get("owner") == "reviewer",
        "final_value": final,
    }


def main() -> int:
    print("CCSStore read-side coherence — and the write-side boundary, honestly")
    print("=" * 72)

    print("\nAct 1 — read-side invalidation (what CCSStore ships)")
    act1 = run_readside()
    print(f"  reviewer's 1st get : coordinator fetch (miss)      → {act1['first_value']}")
    print(f"  reviewer's 2nd get : local cache hit, no re-fetch  → {act1['second_value']}")
    print("  planner commits v2 : reviewer's cached copy → INVALID")
    print(f"  reviewer's 3rd get : fresh miss, never a stale hit → {act1['value_after_peer_commit']}")
    ok1 = all(
        act1[k] for k in ("first_was_miss", "second_was_local_hit", "post_commit_was_fresh_miss", "saw_new_version")
    )
    print(f"  invariant: the read after the peer commit served the NEW version — {'HELD' if ok1 else 'VIOLATED'}")

    print("\nAct 2 — the boundary: put() is not version-CAS (BROKEN by design)")
    act2 = run_broken_writeback()
    print("  reviewer snapshots v1, planner commits budget=approved,")
    print(f"  reviewer puts a v1-derived edit → it LANDS: {act2['final_value']}")
    print(f"  the planner's update is silently gone: {act2['planner_update_lost']} — no error was raised")

    print("\nAct 3 — the fix, one call away: store.core.write_cas (OCC)")
    act3 = run_fixed_writeback()
    print(f"  same stale-based intent, re-applied against the fresh version → {act3['final_value']}")
    ok3 = act3["planner_update_survived"] and act3["reviewer_edit_present"]
    print(f"  invariant: BOTH the peer's update and the edit survived — {'HELD' if ok3 else 'VIOLATED'}")

    print("\nScope: one CCSStore, one process, one host. Cross-process on one host →")
    print("CoherentVolume / stale-write-guard-fs. Docs: read-side is the CCSStore")
    print("guarantee; the write-side deny lives in write_cas / CoherentVolume.")

    all_held = ok1 and act2["planner_update_lost"] and act2["reviewer_edit_present"] and ok3
    print(f"\n{'ALL INVARIANTS HELD — exit 0' if all_held else 'INVARIANT VIOLATED — exit 1'}")
    return 0 if all_held else 1


if __name__ == "__main__":
    sys.exit(main())
