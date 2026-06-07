# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""Common adapter runtime that wires coordinator, agents, and event bus."""

from __future__ import annotations

import logging
import threading
from dataclasses import dataclass
from typing import Any, Callable
from uuid import NAMESPACE_URL, UUID, uuid5

from ccs.agent.runtime import AgentRuntime
from ccs.bus.event_bus import ArtifactUpdateEvent, InMemoryEventBus
from ccs.coordinator.registry import ArtifactRegistry
from ccs.coordinator.service import (
    CoordinatorService,
    CrashRecoveryConfig,
    validate_crash_recovery_config,
)
from ccs.core.exceptions import (  # re-exported; langgraph-free export point
    CoherenceDegradedWarning,
    CoherenceTopologyWarning,
)
from ccs.core.types import Artifact, FetchResponse
from ccs.strategies.base import SyncStrategy
from ccs.strategies.selector import build_strategy

__all__ = ["AgentBinding", "CoherenceAdapterCore", "CoherenceDegradedWarning", "CoherenceTopologyWarning"]

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
        # Bare CoherenceAdapterCore() / CCSStore() adopt the v0.9.0
        # enabled-by-default crash recovery: with no crash_recovery= argument,
        # construct a default CrashRecoveryConfig() (enabled=True). The first
        # such construction in a process emits the one-shot v0.9.0 transitional
        # RuntimeWarning — the migration heads-up for jump-upgraders.
        self._crash_recovery = (
            crash_recovery if crash_recovery is not None else CrashRecoveryConfig()
        )
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
        validate_crash_recovery_config(self._crash_recovery, self.strategy)
        self.event_bus = event_bus if event_bus is not None else InMemoryEventBus()
        self._agents_by_name: dict[str, AgentBinding] = {}
        # Rate-limited crash-recovery sweep state (v0.9.0, KD-4). The sweep is
        # invoked from read()/write() via _maybe_sweep; _sweep_lock serializes
        # the check-then-set so concurrent callers (e.g. LangGraph parallel
        # branches) cannot double-EMIT the diagnostic. The coordinator sweep
        # itself runs OUTSIDE the lock, so it may execute concurrently under
        # contention — any persistent on_reclaim consumer must be idempotent
        # for a repeated (artifact_id, agent_id, trigger) at the same tick.
        # _last_sweep_tick is None until the first sweep so the first eligible
        # call always fires; thereafter the gate rate-limits to once per
        # ``heartbeat_timeout_ticks // 2`` ticks.
        self._last_sweep_tick: int | None = None
        self._first_reclamation_emitted: bool = False
        self._sweep_lock: threading.Lock = threading.Lock()

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

    def agent_names_snapshot(self) -> dict[UUID, str]:
        """Return a snapshot copy of the agent_id → name registry.

        Stable public accessor for callers that need to enumerate
        registered agents (e.g., replay-recorder manifest finalization).
        Returns a fresh dict — mutation by the caller does NOT affect
        the coordinator's internal state.
        """
        return dict(self._agent_names)

    def artifact_names_snapshot(self) -> dict[UUID, str]:
        """Return a snapshot copy of the artifact_id → name registry.

        Mirrors ``agent_names_snapshot`` for symmetry. Drains the
        artifact registry via the public ``registry.artifact_ids()`` +
        ``registry.get_artifact()`` chain; returns a fresh dict.
        """
        names: dict[UUID, str] = {}
        for artifact_id in self.registry.artifact_ids():
            meta = self.registry.get_artifact(artifact_id)
            if meta is not None:
                names[artifact_id] = meta.name
        return names

    def _maybe_sweep(self, now_tick: int) -> None:
        """Rate-limited crash-recovery sweep (v0.9.0, KD-4 / KD-9).

        Invoked from read()/write() after heartbeat recording. Skips fast when
        crash recovery is disabled or when fewer than ``heartbeat_timeout_ticks
        // 2`` ticks have elapsed since the last sweep — so a CCSStore batch
        (which shares one tick across all ops) sweeps once on its first op and
        skips the rest. Three-phase, thread-safe (Finding #6 + ce:review ADV-01):

          1. Check the gate AND claim the slot under the lock (advance
             ``_last_sweep_tick`` before releasing). Claiming under the lock is
             what makes the coordinator sweep run AT MOST ONCE per gate window
             under contention: a concurrent caller at the same tick sees the
             advanced tick and skips, so the sweep / ``on_reclaim`` is not
             double-fired.
          2. Run the sweep WITHOUT holding the lock (arbitrary registry work).
          3. Re-acquire only to gate the one-shot per-instance diagnostic.

        Best-effort: a sweep failure is logged and never propagates into the
        adapter's read/write path (mirrors the plugin lifecycle sweep). Because
        the slot is claimed in Phase 1, a FAILED sweep is rate-limited — the next
        attempt waits for the gate window instead of retrying on every call.
        """
        if not self._crash_recovery.enabled:
            return

        gate = self._crash_recovery.heartbeat_timeout_ticks // 2

        # Fast path (no lock): a lock-free read of the last-sweep tick lets the
        # common "swept recently" case skip without contending on _sweep_lock —
        # this runs on every read()/write(). A stale read is benign: it can only
        # cause a skip this tick (the next eligible tick sweeps); the
        # authoritative gate check + claim still happens under the lock below.
        last = self._last_sweep_tick
        if last is not None and now_tick - last < gate:
            return

        # Phase 1: gate check + claim, under the lock (None == never swept ->
        # always fire). Advancing the tick here (the "claim") is what serializes
        # concurrent callers to a single coordinator sweep per gate window and
        # rate-limits retries after a failure. The max() keeps the claim
        # monotonic against a caller-supplied non-monotonic now_tick.
        with self._sweep_lock:
            if (
                self._last_sweep_tick is not None
                and now_tick - self._last_sweep_tick < gate
            ):
                return
            self._last_sweep_tick = (
                now_tick
                if self._last_sweep_tick is None
                else max(self._last_sweep_tick, now_tick)
            )

        # Phase 2: sweep WITHOUT holding the lock. Capture reclamations via the
        # existing on_reclaim callback — no service-layer signature change.
        reclaimed: list[tuple[UUID, UUID, str]] = []

        def _capture(artifact_id: UUID, agent_id: UUID, trigger: str) -> None:
            reclaimed.append((artifact_id, agent_id, trigger))

        try:
            self.coordinator.enforce_stable_grant_timeouts(
                current_tick=now_tick,
                heartbeat_timeout_ticks=self._crash_recovery.heartbeat_timeout_ticks,
                max_hold_ticks=self._crash_recovery.max_hold_ticks,
                on_reclaim=_capture,
            )
        except Exception:  # noqa: BLE001 — sweep is best-effort
            # logger.exception() already attaches the message + traceback via
            # exc_info. The slot was claimed in Phase 1, so the failed sweep is
            # rate-limited (retry waits for the gate window, not every call).
            logger.exception("agent-coherence: sweep tick failed")
            return

        # Phase 3: gate the one-shot per-instance diagnostic under the lock
        # (_last_sweep_tick was already advanced by Phase 1's claim).
        with self._sweep_lock:
            should_emit = bool(reclaimed) and not self._first_reclamation_emitted
            if should_emit:
                self._first_reclamation_emitted = True

        if should_emit:
            artifact_id, agent_id, trigger = reclaimed[0]
            logger.warning(
                "agent-coherence: sweep reclaimed %d agent(s); first reclamation "
                "per adapter instance is reported, subsequent are silent",
                len(reclaimed),
                extra={
                    "trigger": trigger,
                    "agent_id_short": str(agent_id)[:8],
                    "artifact_id_short": str(artifact_id)[:8],
                    "reclaim_count": len(reclaimed),
                },
            )
            logger.debug("sweep reclaimed (full tuples): %s", reclaimed)

    def read(self, *, agent_name: str, artifact_id: UUID, now_tick: int) -> FetchResponse:
        """Read artifact through one registered runtime."""
        binding = self._binding(agent_name)
        if self._crash_recovery.enabled:
            self.coordinator.record_heartbeat(agent_id=binding.agent_id, now_tick=now_tick)
        self._maybe_sweep(now_tick)
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
        self._maybe_sweep(now_tick)
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
        """Invalidate agent's local cache and record a recovery heartbeat.

        Cache invalidation runs unconditionally (useful as a manual flush
        primitive regardless of feature flag); heartbeat only when enabled.
        """
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
