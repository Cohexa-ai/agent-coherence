# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""Deterministic simulation engine for coherence strategy evaluation."""

from __future__ import annotations

import random
from dataclasses import dataclass
from typing import Any, Literal, Mapping, Sequence
from uuid import UUID

from ccs.agent.runtime import AgentRuntime
from ccs.coordinator.registry import ArtifactRegistry
from ccs.coordinator.service import (
    CoordinatorService,
    CrashRecoveryConfig,
    _validate_crash_recovery_config,
)
from ccs.core.clock import LogicalClock
from ccs.core.states import MESIState
from ccs.core.types import Artifact, InvalidationSignal
from ccs.strategies.base import SyncStrategy
from ccs.strategies.selector import build_strategy
from ccs.transport.network_sim import NetworkMessage, NetworkSimulator

from .aggregation import aggregate_comparison_runs, flatten_metrics
from .consistency import ConsistencyMonitor
from .metrics import SimulationMetrics, StrategyComparisonReport

_INVALIDATION_SIGNAL_TOKENS = 12
_POINTER_UPDATE_TOKENS = 8


@dataclass(frozen=True)
class _FailureEvent:
    """Internal canonical form of a parsed failure_events entry."""

    tick: int
    action: Literal["kill", "busy", "restore"]
    agent_id: UUID
    until_tick: int | None = None


def _build_crash_recovery_config(scenario_config: Mapping[str, Any]) -> CrashRecoveryConfig:
    """Read optional ``crash_recovery`` block from scenario; default to safe values."""
    block = scenario_config.get("crash_recovery") or {}
    defaults = CrashRecoveryConfig()
    return CrashRecoveryConfig(
        enabled=bool(block.get("enabled", defaults.enabled)),
        heartbeat_timeout_ticks=int(
            block.get("heartbeat_timeout_ticks", defaults.heartbeat_timeout_ticks)
        ),
        max_hold_ticks=int(block.get("max_hold_ticks", defaults.max_hold_ticks)),
    )


class SimulationEngine:
    """Runs one scenario/strategy pair and returns a metrics payload."""

    def __init__(
        self,
        scenario_config: Mapping[str, Any],
        *,
        strategy_name: str,
        seed: int | None = None,
    ) -> None:
        self._config = scenario_config
        simulation = scenario_config["simulation"]
        strategy_cfg = scenario_config.get("strategies", {})
        lease_cfg = strategy_cfg.get("lease", {})
        access_count_cfg = strategy_cfg.get("access_count", {})

        self.seed = int(simulation["seed"] if seed is None else seed)
        self._rng = random.Random(self.seed)
        self._clock = LogicalClock()
        self._registry = ArtifactRegistry()
        self._coordinator = CoordinatorService(self._registry)
        self._strategy: SyncStrategy = build_strategy(
            strategy_name,
            lease_ttl_ticks=int(lease_cfg.get("default_ttl_ticks", 300)),
            access_count_max_accesses=int(access_count_cfg.get("max_accesses", 100)),
        )
        self._crash_recovery = _build_crash_recovery_config(scenario_config)
        _validate_crash_recovery_config(self._crash_recovery, self._strategy)
        self._monitor = ConsistencyMonitor(self._strategy)
        self._network = NetworkSimulator(
            latency_ticks=int(scenario_config["network"]["latency_ticks"]),
            message_loss_rate=float(scenario_config["network"]["message_loss_rate"]),
            rng=self._rng,
        )

        self._agent_ids = [UUID(int=i + 1) for i in range(int(simulation["num_agents"]))]
        # Failure-injection bookkeeping. _alive_agents is the set of agents that
        # heartbeat and may issue actions on a given tick; _busy_agents is the
        # set in a fixed-window busy state (no heartbeat, no actions). On init
        # all agents are alive; failure events flip them.
        self._alive_agents: set[UUID] = set(self._agent_ids)
        self._busy_agents: set[UUID] = set()
        self._busy_until: dict[UUID, int] = {}
        self._agent_id_by_name: dict[str, UUID] = {
            f"agent_{i}": agent_id for i, agent_id in enumerate(self._agent_ids)
        }
        self._failure_events_by_tick: dict[int, list[_FailureEvent]] = (
            self._parse_failure_events(scenario_config.get("failure_events"))
        )
        self._artifact_ids: list[UUID] = []
        self._artifact_specs_by_id: dict[UUID, dict[str, Any]] = {}
        self._register_artifacts()
        self._runtime_by_agent: dict[UUID, AgentRuntime] = {
            agent_id: AgentRuntime(
                agent_id=agent_id,
                coordinator=self._coordinator,
                strategy=self._strategy,
            )
            for agent_id in self._agent_ids
        }

        # Counters collected into SimulationMetrics at end of run.
        self._total_actions = 0
        self._read_actions = 0
        self._write_actions = 0
        self._fetch_actions = 0
        self._cache_hits = 0
        self._cache_misses = 0
        self._invalidations_issued = 0
        self._invalidations_delivered = 0
        self._updates_issued = 0
        self._updates_delivered = 0
        self._tokens_fetch = 0
        self._tokens_broadcast = 0
        self._tokens_invalidation = 0
        self._context_injections = 0
        self._transient_state_timeouts = 0
        self._stable_grant_reclamations = 0

    def run(self) -> SimulationMetrics:
        """Run one deterministic simulation and return collected metrics."""
        duration_ticks = int(self._config["simulation"]["duration_ticks"])
        timeout_ticks = int(self._config.get("transient", {}).get("timeout_ticks", 5))
        for _ in range(duration_ticks):
            now = self._clock.now()
            self._apply_failure_events_for_tick(now)
            self._deliver_messages()
            self._execute_actions_for_tick()
            if self._strategy.broadcasts_every_tick():
                self._broadcast_all_to_all(now_tick=now)
            self._transient_state_timeouts += self._coordinator.enforce_transient_timeouts(
                current_tick=now,
                timeout_ticks=timeout_ticks,
            )
            if self._crash_recovery.enabled:
                # R5: flag-off path must be byte-identical to v0.5 baseline.
                # Guard at the call site so no heartbeat / sweep entries land
                # in the state-log when the feature is disabled.
                self._emit_heartbeats_for_alive_agents(now_tick=now)
                self._stable_grant_reclamations += (
                    self._coordinator.enforce_stable_grant_timeouts(
                        current_tick=now,
                        heartbeat_timeout_ticks=self._crash_recovery.heartbeat_timeout_ticks,
                        max_hold_ticks=self._crash_recovery.max_hold_ticks,
                    )
                )
            self._clock.advance()

        # Drain messages that become due exactly at final tick.
        self._deliver_messages()
        return self._build_metrics(duration_ticks)

    # ---- Failure-event injection ----------------------------------------

    def _parse_failure_events(
        self, raw_events: Any
    ) -> dict[int, list[_FailureEvent]]:
        """Parse and resolve agent-name references for ``failure_events``.

        Validation of structure and value ranges is done by the scenario
        validator. Here we only resolve agent names against the engine's
        registered agent set and bucket events by tick (preserving order).
        """
        if not raw_events:
            return {}
        bucketed: dict[int, list[_FailureEvent]] = {}
        for event in raw_events:
            agent_name = str(event["agent"])
            if agent_name not in self._agent_id_by_name:
                raise ValueError(
                    f"failure_events: unknown agent name '{agent_name}'; "
                    f"valid names: {sorted(self._agent_id_by_name)}"
                )
            tick = int(event["tick"])
            until_tick = event.get("until_tick")
            parsed = _FailureEvent(
                tick=tick,
                action=str(event["action"]),
                agent_id=self._agent_id_by_name[agent_name],
                until_tick=int(until_tick) if until_tick is not None else None,
            )
            bucketed.setdefault(tick, []).append(parsed)
        return bucketed

    def _apply_failure_events_for_tick(self, now_tick: int) -> None:
        """Apply kill/busy/restore events scheduled for ``now_tick``.

        Also auto-expires ``busy`` windows whose ``until_tick`` has been
        reached: those agents flip back to alive without a ``restore`` event.
        """
        # Auto-expire busy windows first so an event firing at the same tick
        # observes the agent as live again.
        if self._busy_until:
            expired = [a for a, u in self._busy_until.items() if now_tick >= u]
            for agent_id in expired:
                self._busy_until.pop(agent_id, None)
                self._busy_agents.discard(agent_id)
                self._alive_agents.add(agent_id)

        for event in self._failure_events_by_tick.get(now_tick, ()):
            if event.action == "kill":
                self._alive_agents.discard(event.agent_id)
                self._busy_agents.add(event.agent_id)
            elif event.action == "busy":
                if event.until_tick is None:
                    raise ValueError(
                        f"failure_event(action='busy') missing until_tick at tick={event.tick}"
                    )
                self._alive_agents.discard(event.agent_id)
                self._busy_agents.add(event.agent_id)
                self._busy_until[event.agent_id] = event.until_tick
            elif event.action == "restore":
                self._alive_agents.add(event.agent_id)
                self._busy_agents.discard(event.agent_id)
                self._busy_until.pop(event.agent_id, None)
            else:  # pragma: no cover — schema validator already rejects this.
                raise ValueError(f"unsupported failure-event action '{event.action}'")

    def _emit_heartbeats_for_alive_agents(self, *, now_tick: int) -> None:
        """Emit one heartbeat per alive agent for ``now_tick``."""
        for agent_id in self._alive_agents:
            self._coordinator.record_heartbeat(agent_id=agent_id, now_tick=now_tick)

    def _register_artifacts(self) -> None:
        for artifact_cfg in self._config["artifacts"]:
            artifact = Artifact(
                name=str(artifact_cfg["id"]),
                version=int(artifact_cfg.get("initial_version", 1)),
                size_tokens=int(artifact_cfg["size_tokens"]),
            )
            self._registry.register_artifact(
                artifact,
                content=f"{artifact.name}-v{artifact.version}",
            )
            self._artifact_ids.append(artifact.id)
            self._artifact_specs_by_id[artifact.id] = dict(artifact_cfg)

    def _execute_actions_for_tick(self) -> None:
        now = self._clock.now()
        scenario = self._config["scenario"]
        action_probability = scenario.get("action_probability")
        agent_velocity = scenario.get("agent_velocity")

        for agent_id in self._agent_ids:
            # Skip agents that are killed (not in _alive_agents) or in a busy
            # window. Filter is applied BEFORE action selection so RNG state
            # is unaffected for live agents and a killed agent never reaches
            # _perform_read / _perform_write.
            if agent_id not in self._alive_agents or agent_id in self._busy_agents:
                continue
            if agent_velocity is not None:
                for _ in range(int(agent_velocity)):
                    self._execute_single_action(agent_id=agent_id, now_tick=now)
                continue

            assert action_probability is not None
            if self._rng.random() < float(action_probability):
                self._execute_single_action(agent_id=agent_id, now_tick=now)

    def _execute_single_action(self, *, agent_id: UUID, now_tick: int) -> None:
        self._total_actions += 1
        artifact_id = self._choose_artifact_id()
        artifact_cfg = self._artifact_specs_by_id[artifact_id]
        write_probability = self._effective_write_probability()
        mutable = bool(artifact_cfg.get("mutable", True))
        is_write = mutable and self._rng.random() < write_probability

        if is_write:
            self._perform_write(agent_id=agent_id, artifact_id=artifact_id, now_tick=now_tick)
        else:
            self._perform_read(agent_id=agent_id, artifact_id=artifact_id, now_tick=now_tick)

    def _perform_read(self, *, agent_id: UUID, artifact_id: UUID, now_tick: int) -> None:
        self._read_actions += 1
        runtime = self._runtime_by_agent[agent_id]
        entry = runtime.cache.get(artifact_id)
        needs_refresh = (
            entry is None
            or self._strategy.requires_refresh(entry, now_tick=now_tick)
            or self._context_model() == "always_read"
        )
        if needs_refresh:
            self._cache_misses += 1
            self._fetch_actions += 1
            self._context_injections += 1
            self._tokens_fetch += self._artifact_token_size(artifact_id)
        else:
            self._cache_hits += 1

        runtime.read(artifact_id, now_tick=now_tick)
        if not needs_refresh:
            latest = runtime.cache.get(artifact_id)
            assert latest is not None
            canonical = self._registry.get_artifact(artifact_id)
            assert canonical is not None
            stale = latest.state != MESIState.INVALID and latest.local_version < canonical.version
            self._monitor.record_read(agent_id=agent_id, artifact_id=artifact_id, stale=stale)
            if self._context_model() == "conditional_injection":
                # Conditional model still injects local artifact content when needed by step.
                self._context_injections += 1

    def _perform_write(self, *, agent_id: UUID, artifact_id: UUID, now_tick: int) -> None:
        self._write_actions += 1
        runtime = self._runtime_by_agent[agent_id]
        entry = runtime.cache.get(artifact_id)
        needs_refresh = entry is None or self._strategy.requires_refresh(entry, now_tick=now_tick)
        if needs_refresh:
            self._cache_misses += 1
            self._fetch_actions += 1
            self._context_injections += 1
            self._tokens_fetch += self._artifact_token_size(artifact_id)

        peers_to_sync = [
            peer_id
            for peer_id, state in self._registry.get_state_map(artifact_id).items()
            if peer_id != agent_id and state != MESIState.INVALID
        ]
        previous = self._registry.get_artifact(artifact_id)
        assert previous is not None
        content = f"{previous.name}-v{previous.version + 1}-t{now_tick}"

        updated, _ = runtime.write(
            artifact_id=artifact_id,
            content=content,
            now_tick=now_tick,
            size_tokens=previous.size_tokens,
        )
        self._monitor.validate_monotonic(previous.version, updated.version)
        self._monitor.reset_stale_steps(agent_id=agent_id, artifact_id=artifact_id)

        if self._strategy.broadcasts_content_on_commit():
            self._broadcast_update(
                writer_agent_id=agent_id,
                peers=peers_to_sync,
                artifact_id=artifact_id,
                version=updated.version,
                content=content,
                now_tick=now_tick,
            )
        elif self._strategy.invalidates_peers_on_commit():
            self._emit_invalidations(
                writer_agent_id=agent_id,
                peers=peers_to_sync,
                artifact_id=artifact_id,
                version=updated.version,
                now_tick=now_tick,
            )

        self._monitor.validate_single_writer(self._registry.get_state_map(artifact_id))

    def _emit_invalidations(
        self,
        *,
        writer_agent_id: UUID,
        peers: Sequence[UUID],
        artifact_id: UUID,
        version: int,
        now_tick: int,
    ) -> None:
        for peer_id in peers:
            signal = InvalidationSignal(
                artifact_id=artifact_id,
                new_version=version,
                issued_at_tick=now_tick,
                issuer_agent_id=writer_agent_id,
            )
            self._network.send(
                payload=signal,
                source=writer_agent_id,
                destination=peer_id,
                current_tick=now_tick,
                message_type="invalidate",
            )
            self._invalidations_issued += 1
            self._tokens_invalidation += _INVALIDATION_SIGNAL_TOKENS

    def _broadcast_update(
        self,
        *,
        writer_agent_id: UUID,
        peers: Sequence[UUID],
        artifact_id: UUID,
        version: int,
        content: str,
        now_tick: int,
    ) -> None:
        for peer_id in peers:
            self._network.send(
                payload={
                    "artifact_id": artifact_id,
                    "version": version,
                    "content": content,
                    "writer_agent_id": writer_agent_id,
                },
                source=writer_agent_id,
                destination=peer_id,
                current_tick=now_tick,
                message_type="update",
            )
            self._updates_issued += 1
            self._tokens_broadcast += self._update_token_size(artifact_id)
            self._context_injections += 1

    def _broadcast_all_to_all(self, *, now_tick: int) -> None:
        """Inject full content of all artifacts to all agents on this tick."""
        for artifact_id in self._artifact_ids:
            artifact = self._registry.get_artifact(artifact_id)
            content = self._registry.get_content(artifact_id)
            if artifact is None or content is None:
                continue
            for agent_id in self._agent_ids:
                runtime = self._runtime_by_agent[agent_id]
                runtime.handle_update(
                    artifact_id=artifact_id,
                    version=artifact.version,
                    content=content,
                    now_tick=now_tick,
                    writer_agent_id=None,
                )
                self._updates_issued += 1
                self._updates_delivered += 1
                self._tokens_broadcast += self._artifact_token_size(artifact_id)
                self._context_injections += 1

    def _deliver_messages(self) -> None:
        for message in self._network.deliver_due(self._clock.now()):
            self._deliver_message(message)

    def _deliver_message(self, message: NetworkMessage) -> None:
        if message.message_type == "invalidate":
            self._apply_invalidation(message)
            return
        if message.message_type == "update":
            self._apply_update(message)
            return
        raise ValueError(f"unsupported message type '{message.message_type}'")

    def _apply_invalidation(self, message: NetworkMessage) -> None:
        signal = message.payload
        assert isinstance(signal, InvalidationSignal)
        runtime = self._runtime_by_agent[message.destination]
        runtime.handle_invalidation(signal)
        self._monitor.reset_stale_steps(agent_id=message.destination, artifact_id=signal.artifact_id)
        self._invalidations_delivered += 1
        self._monitor.validate_single_writer(self._registry.get_state_map(signal.artifact_id))

    def _apply_update(self, message: NetworkMessage) -> None:
        payload = message.payload
        artifact_id = payload["artifact_id"]
        version = int(payload["version"])
        content = str(payload.get("content", ""))
        writer_agent_id = payload["writer_agent_id"]
        runtime = self._runtime_by_agent[message.destination]
        runtime.handle_update(
            artifact_id=artifact_id,
            version=version,
            content=content,
            now_tick=self._clock.now(),
            writer_agent_id=writer_agent_id,
        )
        self._monitor.reset_stale_steps(agent_id=message.destination, artifact_id=artifact_id)
        self._updates_delivered += 1
        self._monitor.validate_single_writer(self._registry.get_state_map(artifact_id))

    def _artifact_token_size(self, artifact_id: UUID) -> int:
        artifact = self._registry.get_artifact(artifact_id)
        assert artifact is not None
        return int(artifact.size_tokens or 1)

    def _update_token_size(self, artifact_id: UUID) -> int:
        if self._context_model() == "pointer":
            return _POINTER_UPDATE_TOKENS
        return self._artifact_token_size(artifact_id)

    def _context_model(self) -> str:
        context_semantics = self._config.get("context_semantics", {})
        return str(context_semantics.get("model", "conditional_injection"))

    def _choose_artifact_id(self) -> UUID:
        workload = self._config["scenario"]["workload"]
        if workload != "large_artifact_reasoning":
            return self._rng.choice(self._artifact_ids)

        weights = [float(self._artifact_token_size(artifact_id)) for artifact_id in self._artifact_ids]
        return self._rng.choices(self._artifact_ids, weights=weights, k=1)[0]

    def _effective_write_probability(self) -> float:
        base = float(self._config["scenario"]["write_probability"])
        workload = self._config["scenario"]["workload"]
        if workload == "read_heavy":
            return min(base, 0.2)
        if workload == "write_heavy":
            return max(base, 0.7)
        if workload == "parallel_editing":
            return max(base, 0.5)
        if workload == "large_artifact_reasoning":
            return min(base, 0.3)
        return base

    def _build_metrics(self, duration_ticks: int) -> SimulationMetrics:
        return SimulationMetrics(
            scenario=str(self._config["scenario"]["name"]),
            strategy=self._strategy.name,
            seed=self.seed,
            duration_ticks=duration_ticks,
            agent_count=len(self._agent_ids),
            artifact_count=len(self._artifact_ids),
            total_actions=self._total_actions,
            read_actions=self._read_actions,
            write_actions=self._write_actions,
            fetch_actions=self._fetch_actions,
            cache_hits=self._cache_hits,
            cache_misses=self._cache_misses,
            stale_reads=self._monitor.stale_reads,
            max_stale_steps=self._monitor.max_stale_steps,
            staleness_bound_violations=self._monitor.staleness_bound_violations,
            swmr_violations=self._monitor.swmr_violations,
            monotonic_version_violations=self._monitor.monotonic_version_violations,
            invalidations_issued=self._invalidations_issued,
            invalidations_delivered=self._invalidations_delivered,
            updates_issued=self._updates_issued,
            updates_delivered=self._updates_delivered,
            message_overhead=self._network.message_overhead,
            tokens_fetch=self._tokens_fetch,
            tokens_broadcast=self._tokens_broadcast,
            tokens_invalidation=self._tokens_invalidation,
            context_injections=self._context_injections,
            transient_state_timeouts=self._transient_state_timeouts,
            stable_grant_reclamations=self._stable_grant_reclamations,
        )


def run_strategy_range(
    scenario_config: Mapping[str, Any],
    *,
    strategy_name: str,
    runs: int,
    seed_start: int,
) -> list[SimulationMetrics]:
    """Run one strategy across a contiguous seed range."""
    if runs < 1:
        raise ValueError("runs must be >= 1")
    metrics: list[SimulationMetrics] = []
    for offset in range(runs):
        engine = SimulationEngine(
            scenario_config,
            strategy_name=strategy_name,
            seed=seed_start + offset,
        )
        metrics.append(engine.run())
    return metrics


def run_strategy_comparison(
    scenario_config: Mapping[str, Any],
    *,
    strategies: Sequence[str],
    runs: int,
    seed_start: int = 0,
) -> StrategyComparisonReport:
    """Run multi-strategy comparison and return report payload."""
    metrics_by_strategy: dict[str, list[SimulationMetrics]] = {}
    for strategy_name in strategies:
        metrics_by_strategy[strategy_name] = run_strategy_range(
            scenario_config,
            strategy_name=strategy_name,
            runs=runs,
            seed_start=seed_start,
        )

    aggregated = [item.to_dict() for item in aggregate_comparison_runs(metrics_by_strategy)]
    scenario_name = str(scenario_config["scenario"]["name"])
    return StrategyComparisonReport(
        scenario=scenario_name,
        runs_per_strategy=runs,
        seed_start=seed_start,
        strategies=list(strategies),
        runs=flatten_metrics(metrics_by_strategy),
        aggregated=aggregated,
    )
