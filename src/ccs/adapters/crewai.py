# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""CrewAI-oriented coherence adapter utilities."""

from __future__ import annotations

from typing import Any
from uuid import UUID

from ccs.coordinator.service import CrashRecoveryConfig
from ccs.core.types import Artifact

from .base import CoherenceAdapterCore


class CrewAIAdapter:
    """Adapter exposing task lifecycle helpers for CrewAI-style flows."""

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
        """Register one crew member identity."""
        return self.core.register_agent(name)

    def register_artifact(self, *, name: str, content: str, size_tokens: int | None = None) -> Artifact:
        """Register shared artifact accessible to the crew."""
        return self.core.register_artifact(name=name, content=content, size_tokens=size_tokens)

    def prepare_task_context(
        self,
        *,
        agent_name: str,
        artifact_ids: list[UUID],
        now_tick: int,
    ) -> dict[UUID, str]:
        """Materialize artifact content for one task execution."""
        content_by_artifact: dict[UUID, str] = {}
        for artifact_id in artifact_ids:
            response = self.core.read(agent_name=agent_name, artifact_id=artifact_id, now_tick=now_tick)
            content_by_artifact[artifact_id] = response.content
        return content_by_artifact

    def commit_task_artifact(
        self,
        *,
        agent_name: str,
        artifact_id: UUID,
        content: str,
        now_tick: int,
    ) -> int:
        """Commit task output for one artifact and return resulting version."""
        artifact = self.core.write(
            agent_name=agent_name,
            artifact_id=artifact_id,
            content=content,
            now_tick=now_tick,
        )
        return artifact.version
