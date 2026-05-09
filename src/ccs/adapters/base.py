# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""Common adapter runtime that wires coordinator, agents, and event bus."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Callable
from uuid import NAMESPACE_URL, UUID, uuid5

from ccs.agent.runtime import AgentRuntime
from ccs.bus.event_bus import ArtifactUpdateEvent, InMemoryEventBus
from ccs.coordinator.registry import ArtifactRegistry
from ccs.coordinator.service import (
    CoordinatorService,
    CrashRecoveryConfig,
    _validate_crash_recovery_config,
)
from ccs.core.types import Artifact, FetchResponse
from ccs.strategies.base import SyncStrategy
from ccs.strategies.selector import build_strategy

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class AgentBinding:
    """Resolved identity/runtime tuple for an adapter-managed agent."""

    name: str
    agent_id: UUID
    runtime: AgentRuntime


class CoherenceAdapterCore:
    """Reusable cluster abstraction for framework adapters."""

    def __init__(
        self,
        *,
        strategy_name: str = "lazy",
        lease_ttl_ticks: int = 300,
        access_count_max_accesses: int = 100,
        event_bus: InMemoryEventBus | None = None,
        state_log: Callable[[dict[str, Any]], None] | None = None,
        instance_id: str | None = None,
        content_audit_log: Callable[[dict[str, Any]], None] | None = None,
        audit_seq: list[int] | None = None,
        retain_versions: bool = False,
        crash_recovery: CrashRecoveryConfig | None = None,
        **strategy_kwargs: Any,
    ) -> None:
        self._agent_names: dict[UUID, str] = {}
        self._instance_id = instance_id
        self._content_audit_log = content_audit_log
        self._audit_seq = audit_seq
        self._crash_recovery = crash_recovery if crash_recovery is not None else CrashRecoveryConfig()
        self.registry = ArtifactRegistry(
            state_log=state_log,
            agent_names=self._agent_names,
            instance_id=instance_id,
            retain_versions=retain_versions,
        )
        self.coordinator = CoordinatorService(self.registry)
        self.strategy: SyncStrategy = build_strategy(
            strategy_name,
            lease_ttl_ticks=lease_ttl_ticks,
            access_count_max_accesses=access_count_max_accesses,
        )
        _validate_crash_recovery_config(self._crash_recovery, self.strategy)
        self.event_bus = event_bus if event_bus is not None else InMemoryEventBus()
        self._agents_by_name: dict[str, AgentBinding] = {}

    def register_agent(self, name: str, *, now_tick: int = 0) -> UUID:
        """Register one agent runtime and subscribe it to bus events."""
        existing = self._agents_by_name.get(name)
        if existing is not None:
            return existing.agent_id

        agent_id = uuid5(NAMESPACE_URL, f"ccs-agent:{name}")
        self._agent_names[agent_id] = name
        runtime = AgentRuntime(
            agent_id=agent_id,
            coordinator=self.coordinator,
            strategy=self.strategy,
            content_audit_log=self._content_audit_log,
            audit_seq=self._audit_seq,
            agent_name=name,
            instance_id=self._instance_id,
        )
        self.event_bus.subscribe(
            agent_id=agent_id,
            on_invalidation=runtime.handle_invalidation,
            on_update=lambda event, runtime=runtime: runtime.handle_update(
                artifact_id=event.artifact_id,
                version=event.version,
                content=event.content,
                now_tick=event.issued_at_tick,
                writer_agent_id=event.issuer_agent_id,
            ),
        )
        self._agents_by_name[name] = AgentBinding(name=name, agent_id=agent_id, runtime=runtime)
        if self._crash_recovery.enabled:
            self.coordinator.record_heartbeat(agent_id=agent_id, now_tick=now_tick)
        return agent_id

    def register_artifact(
        self,
        *,
        name: str,
        content: str,
        size_tokens: int | None = None,
    ) -> Artifact:
        """Register a shared artifact in the coordinator directory."""
        return self.coordinator.register_artifact(name=name, content=content, size_tokens=size_tokens)

    def read(self, *, agent_name: str, artifact_id: UUID, now_tick: int) -> FetchResponse:
        """Read artifact through one registered runtime."""
        binding = self._binding(agent_name)
        if self._crash_recovery.enabled:
            self.coordinator.record_heartbeat(agent_id=binding.agent_id, now_tick=now_tick)
        return binding.runtime.read(artifact_id, now_tick=now_tick)

    def write(
        self,
        *,
        agent_name: str,
        artifact_id: UUID,
        content: str,
        now_tick: int,
    ) -> Artifact:
        """Write artifact through one runtime and dispatch peer events."""
        writer = self._binding(agent_name)
        if self._crash_recovery.enabled:
            self.coordinator.record_heartbeat(agent_id=writer.agent_id, now_tick=now_tick)
        updated, invalidation_signals = writer.runtime.write(
            artifact_id=artifact_id,
            content=content,
            now_tick=now_tick,
        )
        peers = [binding.agent_id for binding in self._agents_by_name.values() if binding.agent_id != writer.agent_id]

        for signal in invalidation_signals:
            self.event_bus.publish_invalidation(signal, recipients=peers)

        if self.strategy.broadcasts_content_on_commit():
            self.event_bus.publish_update(
                ArtifactUpdateEvent(
                    artifact_id=artifact_id,
                    version=updated.version,
                    content=content,
                    issued_at_tick=now_tick,
                    issuer_agent_id=writer.agent_id,
                ),
                recipients=peers,
            )

        return updated

    def content(self, *, agent_name: str, artifact_id: UUID) -> str | None:
        """Return local content cached by one agent runtime."""
        return self._binding(agent_name).runtime.content(artifact_id)

    def heartbeat(self, *, agent_name: str, now_tick: int) -> None:
        """Record a heartbeat for the named agent. No-op when crash recovery is disabled."""
        if not self._crash_recovery.enabled:
            return
        binding = self._binding(agent_name)
        self.coordinator.record_heartbeat(agent_id=binding.agent_id, now_tick=now_tick)

    def recover(self, *, agent_name: str, now_tick: int) -> None:
        """Invalidate agent's local cache and record a recovery heartbeat."""
        binding = self._binding(agent_name)
        binding.runtime.invalidate_all_cache()
        if self._crash_recovery.enabled:
            self.coordinator.record_heartbeat(agent_id=binding.agent_id, now_tick=now_tick)

    def runtime(self, agent_name: str) -> AgentRuntime:
        """Return concrete runtime for adapter extensions/testing."""
        return self._binding(agent_name).runtime

    def agent_names(self) -> list[str]:
        """Return registered adapter agent names."""
        return sorted(self._agents_by_name.keys())

    def agent_id_for(self, name: str) -> UUID:
        """Return the UUID for a registered agent by name."""
        return self._binding(name).agent_id

    def _binding(self, agent_name: str) -> AgentBinding:
        binding = self._agents_by_name.get(agent_name)
        if binding is None:
            raise KeyError(f"unknown_agent '{agent_name}'")
        return binding
