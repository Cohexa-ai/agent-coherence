# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Red→green front door for the stale-write-guard-fs MCP server.

A constructed, deterministic, offline demo: the SAME read→write sequence loses an
update without the guard (RED) and is denied/merged with it (GREEN). The GREEN
path drives the server's own tool contract (``ccs.mcp.server._do_*``) so the
demo proves the server, not just the coordinator. A NEGATIVE CONTROL re-runs the
green flow with the deny disabled and confirms the loss returns — so a passing
green cannot be a refetch/merge masking the loss.

Two single-host regimes:
  - sequential — a stale overwrite is DENIED; the peer's value survives untouched.
  - concurrent — two writers compare-and-set the same key; the loser re-reads,
    re-merges, and retries → both contributions land (the golden merge).

Generic agents over a shared notes file; no incident, no names, no network.
"""

from __future__ import annotations

# The shared artifact + the two regimes' content. Generic and de-named.
REL = "data/notes.md"

BASE = "# shared notes\n"
PEER_LINE = "- line from the peer\n"
AGENT_LINE = "- line from the agent\n"
LINE_A = "- line from agent a\n"
LINE_B = "- line from agent b\n"

# Sequential GREEN: the peer's commit survives, the agent's stale buffer does not.
PEER_VALUE = BASE + PEER_LINE
# Concurrent GREEN: agent A lands first, agent B re-merges on top — both lines.
GOLDEN = BASE + LINE_A + LINE_B
