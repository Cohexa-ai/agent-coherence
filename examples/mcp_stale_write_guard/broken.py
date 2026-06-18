# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Broken case: no coherence → a stale buffer silently clobbers a peer's commit.

The agent reads the file (deriving its buffer from that version), a peer commits a
new line, then the agent writes its STALE buffer back — last-writer-wins, and the
peer's line is gone. Plain file writes, no coordinator: the lost update the guard
exists to prevent.

    python -m examples.mcp_stale_write_guard.broken
"""

from __future__ import annotations

import shutil
import sys
import tempfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from examples.mcp_stale_write_guard import AGENT_LINE, BASE, PEER_LINE, PEER_VALUE, REL


def run_broken() -> dict[str, object]:
    """Sequenced read→clobber with no coordination; returns a structured trace."""
    workspace = Path(tempfile.mkdtemp(prefix="swg_broken_"))
    target = workspace / REL
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(BASE)
    try:
        agent_buffer = target.read_text() + AGENT_LINE  # agent derived from BASE
        target.write_text(PEER_VALUE)  # peer commits its line
        target.write_text(agent_buffer)  # agent's STALE write clobbers the peer
        final = target.read_text()
        return {"final": final, "lost_update": PEER_LINE not in final}
    finally:
        shutil.rmtree(workspace, ignore_errors=True)


def main() -> int:
    trace = run_broken()
    print("BROKEN (no coherence) — the agent's stale write clobbers the peer")
    print(f"  LOST UPDATE: {trace['lost_update']}  (the peer's line is gone)")
    return 0 if trace["lost_update"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
