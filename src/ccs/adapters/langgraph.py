# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""LangGraph-oriented coherence adapter utilities."""

from __future__ import annotations

from typing import Any, Mapping
from uuid import UUID

from ccs.coordinator.service import CrashRecoveryConfig
from ccs.core.types import Artifact

from .base import CoherenceAdapterCore


class LangGraphAdapter:
    """Adapter exposing pre/post node hooks backed by CCS runtime."""

    def __init__(
        self,
        *,
        strategy_name: str = "lazy",
        core: CoherenceAdapterCore | None = None,
        crash_recovery: CrashRecoveryConfig | None = None,
        **kwargs: Any,
    ) -> None:
        self.core = core if core is not None else CoherenceAdapterCore(
            strategy_name=strategy_name, crash_recovery=crash_recovery, **kwargs
        )

    def register_agent(self, name: str) -> UUID:
        """Register node runtime identity."""
        return self.core.register_agent(name)

    def register_artifact(self, *, name: str, content: str, size_tokens: int | None = None) -> Artifact:
        """Register shared artifact used by LangGraph nodes."""
        return self.core.register_artifact(name=name, content=content, size_tokens=size_tokens)

    def before_node(
        self,
        *,
        agent_name: str,
        artifact_ids: list[UUID],
        now_tick: int,
    ) -> dict[UUID, dict[str, object]]:
        """Read artifacts before node execution and return context payload."""
        context: dict[UUID, dict[str, object]] = {}
        for artifact_id in artifact_ids:
            response = self.core.read(agent_name=agent_name, artifact_id=artifact_id, now_tick=now_tick)
            context[artifact_id] = {
                "version": response.version,
                "content": response.content,
                "state": response.state_grant.value,
            }
        return context

    def commit_outputs(
        self,
        *,
        agent_name: str,
        writes: Mapping[UUID, str],
        now_tick: int,
    ) -> dict[UUID, int]:
        """Commit node outputs and return artifact versions."""
        versions: dict[UUID, int] = {}
        for artifact_id, content in writes.items():
            artifact = self.core.write(
                agent_name=agent_name,
                artifact_id=artifact_id,
                content=content,
                now_tick=now_tick,
            )
            versions[artifact_id] = artifact.version
        return versions
