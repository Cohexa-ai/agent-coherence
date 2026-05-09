# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""AutoGen-oriented coherence adapter utilities."""

from __future__ import annotations

from typing import Any, Mapping
from uuid import UUID

from ccs.coordinator.service import CrashRecoveryConfig
from ccs.core.types import Artifact

from .base import CoherenceAdapterCore


class AutoGenAdapter:
    """Adapter exposing per-turn hooks for AutoGen-like conversations."""

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
        """Register one conversational agent identity."""
        return self.core.register_agent(name)

    def register_artifact(self, *, name: str, content: str, size_tokens: int | None = None) -> Artifact:
        """Register shared artifact for conversation context."""
        return self.core.register_artifact(name=name, content=content, size_tokens=size_tokens)

    def pre_turn_context(
        self,
        *,
        agent_name: str,
        artifact_ids: list[UUID],
        now_tick: int,
    ) -> dict[UUID, str]:
        """Fetch current context before an agent turn."""
        return {
            artifact_id: self.core.read(agent_name=agent_name, artifact_id=artifact_id, now_tick=now_tick).content
            for artifact_id in artifact_ids
        }

    def post_turn_commit(
        self,
        *,
        agent_name: str,
        updates: Mapping[UUID, str],
        now_tick: int,
    ) -> dict[UUID, int]:
        """Commit turn updates and return updated versions."""
        versions: dict[UUID, int] = {}
        for artifact_id, content in updates.items():
            artifact = self.core.write(
                agent_name=agent_name,
                artifact_id=artifact_id,
                content=content,
                now_tick=now_tick,
            )
            versions[artifact_id] = artifact.version
        return versions
