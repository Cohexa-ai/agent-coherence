# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Minimal multi-agent coherence demo for post-v0.1 validation."""

from __future__ import annotations

from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from ccs.adapters.langgraph import LangGraphAdapter
from ccs.core.states import MESIState


def main() -> int:
    adapter = LangGraphAdapter(strategy_name="lazy")

    for name in ("planner", "researcher", "executor"):
        adapter.register_agent(name)

    artifact = adapter.register_artifact(name="plan.md", content="v1")

    planner_context = adapter.before_node(agent_name="planner", artifact_ids=[artifact.id], now_tick=1)
    adapter.before_node(agent_name="researcher", artifact_ids=[artifact.id], now_tick=1)
    adapter.before_node(agent_name="executor", artifact_ids=[artifact.id], now_tick=1)
    current_plan = str(planner_context[artifact.id]["content"])
    next_plan = current_plan + "\nStep 1: gather requirements"
    versions = adapter.commit_outputs(
        agent_name="planner",
        writes={artifact.id: next_plan},
        now_tick=2,
    )
    print(f"planner wrote plan.md v{versions[artifact.id]}")

    for peer in ("researcher", "executor"):
        entry = adapter.core.runtime(peer).cache.get(artifact.id)
        if entry is not None and entry.state == MESIState.INVALID:
            print(f"{peer} invalidated")

    print("")

    for tick, peer in ((3, "researcher"), (4, "executor")):
        peer_context = adapter.before_node(agent_name=peer, artifact_ids=[artifact.id], now_tick=tick)
        version = peer_context[artifact.id]["version"]
        print(f"{peer} fetched plan.md v{version}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

# Comparing notes on multi-agent coherence?
# https://github.com/Cohexa-ai/agent-coherence/discussions
