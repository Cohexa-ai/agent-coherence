# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Red→green gate for stale-write-guard-fs: RED loses, GREEN holds, control proves it.

Runs four cases and exits 0 only if ALL hold:
  - RED       — no coherence → the update is lost.
  - GREEN seq — the stale overwrite is denied; the peer value survives EXACTLY.
  - GREEN cas — two writers merge to the EXACT golden value.
  - CONTROL   — the green flow with the deny disabled → the loss returns,
                proving green depends on the deny, not on a refetch/merge.

Constructed, deterministic, offline (a local coordinator subprocess; no network).

    python -m examples.mcp_stale_write_guard.main
"""

from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from examples.mcp_stale_write_guard import GOLDEN, PEER_VALUE, broken, fixed


def main() -> int:
    print("stale-write-guard-fs — red→green front door (single-host, sequential + concurrent).")
    print("Same read→write sequence each time; only the coordination differs.\n")

    red = broken.run_broken()
    green_seq = fixed.run_sequential(guarded=True)
    green_cas = fixed.run_concurrent()
    control = fixed.run_sequential(guarded=False)  # deny disabled

    # Exact-equal finals (not inequalities): a green that merely "differs from the
    # broken value" could still be wrong. Pin the exact expected content.
    checks = {
        "RED  no coherence loses the update": bool(red["lost_update"]),
        "GREEN sequential preserves the peer value (exact)": (
            green_seq["final"] == PEER_VALUE and bool(green_seq["stale_write_denied"])
        ),
        "GREEN concurrent merges to the golden value (exact)": (
            green_cas["final"] == GOLDEN and bool(green_cas["merged_golden"])
        ),
        "CONTROL with the deny off, the loss returns": not bool(control["preserved_peer_value"]),
    }

    for label, passed in checks.items():
        print(f"  [{'ok' if passed else 'FAIL'}] {label}")

    ok = all(checks.values())
    print("\nGREEN" if ok else "\nRED — a check failed; do not trust this build")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
