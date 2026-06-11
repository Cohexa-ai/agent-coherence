# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""In-memory artifact registry for coherence coordination."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Callable, Optional, TypeAlias
from uuid import UUID, uuid4

from ccs.core.exceptions import STALE_READ_GENERATION_REASON, StaleReadGeneration
from ccs.core.states import MESIState, TransientState
from ccs.core.types import Artifact, CasCorruption, ConflictDetail

from .retention import RetentionPolicy, collectible_versions

CCS_STATE_LOG_SCHEMA_VERSION = "ccs.state_log.v2"

ReclamationSlot: TypeAlias = tuple[str, int]  # (trigger, tick)
_M_OR_E_STATES: frozenset[MESIState] = frozenset({MESIState.MODIFIED, MESIState.EXCLUSIVE})

# The coordinator-side EVICTION triggers: the stable-grant sweep
# (CoordinatorService.enforce_stable_grant_timeouts -> reclaim_heartbeat /
# reclaim_max_hold) and the transient-timeout fail-safe
# (enforce_transient_timeouts -> "timeout"). An M/E -> INVALID transition
# carrying one of these bumps the artifact's owner_generation (the
# read-generation fence) -- the holder's claim was revoked WITHOUT a version
# move, which is exactly what version-CAS cannot see. A peer-invalidation
# INVALID (any other trigger) does NOT bump: that path moves the version, so
# version-CAS already catches a stale write. Duplicated in
# SqliteArtifactRegistry; pinned equal by the parity test.
RECLAIM_TRIGGERS: frozenset[str] = frozenset(
    {"reclaim_heartbeat", "reclaim_max_hold", "timeout"}
)

# Triggers that mark a GENUINE content read for read-generation capture (the
# E/M-acquire capture is keyed on the state transition, not the trigger).
# Service.fetch() emits "fetch"; a rename there without updating this constant
# would silently disable capture on reads -- pinned equal across registries by
# the parity test, same discipline as RECLAIM_TRIGGERS.
CLAIM_CAPTURE_TRIGGERS: frozenset[str] = frozenset({"fetch"})

# OCC commit-CAS result (plan Unit 2) — parity with SqliteArtifactRegistry.
# WIN = (updated_artifact, invalidated_agent_ids); loss = ConflictDetail;
# impossible state = CasCorruption. None is raised by the registry.
CasResult: TypeAlias = "tuple[Artifact, list[UUID]] | ConflictDetail | CasCorruption"


@dataclass
class ArtifactRecord:
    """Internal registry record for one artifact."""

    artifact: Artifact
    content: str
    state_by_agent: dict[UUID, MESIState] = field(default_factory=dict)
    transient_by_agent: dict[UUID, TransientState] = field(default_factory=dict)
    transient_tick_by_agent: dict[UUID, int] = field(default_factory=dict)
    last_writer: Optional[UUID] = None
    # Retained version snapshots, keyed by artifact version. The value is the
    # captured BODY. The annotation admits ``bytes`` because the ``commit_cas``
    # WIN path stores ``record.content``, which is ``bytes`` on the in-process
    # library path (``AgentRuntime.write_cas`` threads bytes through). The old
    # ``dict[int, str]`` annotation was wrong (a ``type: ignore`` papered over
    # it at the WIN site); corrected here as part of the value-shape change for
    # bounded retention. ``version_captured_at`` is a PARALLEL dict of capture
    # wall-clock timestamps (``time.time()``), one per retained version — kept
    # separate (rather than tupling the value) so the v0.5 pinned suites that
    # read ``version_history[v]`` as the body stay valid byte-for-byte. The two
    # dicts are always mutated together (capture and GC) so their key sets match.
    version_history: dict[int, bytes | str] = field(default_factory=dict)
    version_captured_at: dict[int, float] = field(default_factory=dict)
    granted_at_tick_by_agent: dict[UUID, int] = field(default_factory=dict)
    last_reclamation_by_agent: dict[UUID, ReclamationSlot] = field(default_factory=dict)
    # Read-generation fence (single-host Piece #2). owner_generation is the
    # per-artifact ownership epoch, bumped on every sweep reclamation;
    # read_generation_by_agent[ag] is the owner_generation an agent captured
    # when it last established its write-claim (a genuine read OR an E/M
    # acquire). An ABSENT key (== None) is the absent operand => reject at
    # commit. The counter only grows (resets to 0 per construction in-memory).
    owner_generation: int = 0
    read_generation_by_agent: dict[UUID, int] = field(default_factory=dict)


class ArtifactRegistry:
    """Canonical in-memory artifact directory and payload store."""

    def __init__(
        self,
        *,
        state_log: Callable[[dict[str, Any]], None] | None = None,
        agent_names: dict[UUID, str] | None = None,
        instance_id: str | None = None,
        retain_versions: bool = False,
        retention_policy: RetentionPolicy | None = None,
    ) -> None:
        if state_log is not None and instance_id is None:
            raise ValueError(
                "instance_id must be provided when state_log is set; "
                "pass instance_id=str(uuid4()) or route through CCSStore which manages it automatically"
            )
        self._records: dict[UUID, ArtifactRecord] = {}
        self._heartbeat_by_agent: dict[UUID, int] = {}
        self._state_log = state_log
        self._agent_names = agent_names
        self._instance_id: str = instance_id if instance_id is not None else str(uuid4())
        # coordinator_epoch: seeded for the CROSS-HOST fence follow-on
        # (demand-gated; a client-carried token would compare it). NOT used in
        # any guard yet -- the single-host fence keys only on owner_generation,
        # and a wiped/recreated store also wipes the read_generation rows, so
        # there is no pre-wipe claim for an epoch to fail. Kept so the durable
        # store identity exists from day one.
        self._coordinator_epoch: str = uuid4().hex
        self._seq: int = 0
        # Retention is active iff ``retain_versions`` is True. The attribute is
        # kept TRUTHY and named ``_retain_versions`` because the recorder test
        # (tests/test_replay_recorder.py) asserts on this private name.
        self._retain_versions = retain_versions
        # ``retention_policy=None`` with ``retain_versions=True`` == today's
        # UNBOUNDED semantics (no GC) — this is the back-compat contract that
        # keeps the four pinned v0.5/recorder suites green. A policy is an
        # explicit opt-in to BOUNDED retention: GC runs only when this is set.
        self._retention_policy = retention_policy

    def _capture_version(
        self, record: ArtifactRecord, version: int, content: bytes | str
    ) -> None:
        """Snapshot ``content`` under ``version`` and run inline GC (R1, R3, R4).

        The single retention apply path shared by all three capture points
        (``register_artifact``, ``set_artifact_and_content``, the ``commit_cas``
        WIN). Stores the body and its capture timestamp, then — ONLY when a
        bounded policy is set — drops the versions :func:`collectible_versions`
        marks (the current version always survives; unbounded mode skips GC
        entirely, preserving today's semantics).

        Lock-free posture (registry contract): this is plain GIL-atomic dict
        mutation. The store-then-drop is a sequence of individual dict ops, each
        atomic under the GIL; a concurrent reader of ``get_content_at_version``
        sees a consistent value at any single dict access. No locks, no
        ``BEGIN IMMEDIATE``.
        """
        captured_at = time.time()  # one wall-clock read: stamp == GC reference.
        record.version_history[version] = content
        record.version_captured_at[version] = captured_at
        if self._retention_policy is None:
            return  # unbounded mode (retain_versions=True, no policy): no GC.
        for dropped in collectible_versions(
            record.version_captured_at,
            current_version=version,
            policy=self._retention_policy,
            now=captured_at,
        ):
            record.version_history.pop(dropped, None)
            record.version_captured_at.pop(dropped, None)

    def register_artifact(self, artifact: Artifact, content: str) -> None:
        """Insert artifact record into registry."""
        record = ArtifactRecord(artifact=artifact, content=content)
        if self._retain_versions:
            self._capture_version(record, artifact.version, content)
        self._records[artifact.id] = record

    def has_artifact(self, artifact_id: UUID) -> bool:
        """Return whether an artifact exists in registry."""
        return artifact_id in self._records

    def artifact_ids(self) -> list[UUID]:
        """Return all known artifact ids."""
        return list(self._records.keys())

    def get_artifact(self, artifact_id: UUID) -> Optional[Artifact]:
        """Return artifact metadata if present."""
        record = self._records.get(artifact_id)
        return record.artifact if record else None

    def get_content(self, artifact_id: UUID) -> Optional[str]:
        """Return artifact content if present."""
        record = self._records.get(artifact_id)
        return record.content if record else None

    def get_owner_generation(self, artifact_id: UUID) -> int:
        """Return the artifact's ownership epoch (read-generation fence)."""
        return self._records[artifact_id].owner_generation

    def get_read_generation(self, artifact_id: UUID, agent_id: UUID) -> int | None:
        """Return the generation an agent captured at its last claim, or None if
        it never established a fence claim (a plain OCC writer that version-CAS,
        not the fence, arbitrates)."""
        return self._records[artifact_id].read_generation_by_agent.get(agent_id)

    def set_artifact_and_content(
        self,
        artifact_id: UUID,
        artifact: Artifact,
        content: str,
        *,
        last_writer: Optional[UUID] = None,
        fence_agent_id: Optional[UUID] = None,
    ) -> None:
        """Replace artifact metadata/content for an existing record.

        Read-generation fence (pessimistic ``commit()`` path): when
        ``fence_agent_id`` is given, reject -- atomically (GIL) with the persist
        -- if that committer's captured read_generation was superseded by a
        sweep reclamation (the race the service's earlier get_agent_state check
        misses). A ``None`` fence_agent_id (source-churn) is unguarded.
        """
        record = self._records[artifact_id]
        if fence_agent_id is not None:
            read_gen = record.read_generation_by_agent.get(fence_agent_id)
            if read_gen is not None and read_gen < record.owner_generation:
                raise StaleReadGeneration(
                    f"{STALE_READ_GENERATION_REASON} agent={fence_agent_id} "
                    f"artifact={artifact_id} read_gen={read_gen} "
                    f"owner_gen={record.owner_generation}"
                )
        if self._retain_versions:
            self._capture_version(record, artifact.version, content)
        record.artifact = artifact
        record.content = content
        record.last_writer = last_writer

    def get_content_at_version(self, artifact_id: UUID, version: int) -> str | None:
        """Return content for a specific version, if retained."""
        record = self._records.get(artifact_id)
        if record is None:
            return None
        return record.version_history.get(version)

    def get_state_map(self, artifact_id: UUID) -> dict[UUID, MESIState]:
        """Return copy of per-agent MESI states for an artifact."""
        return dict(self._records[artifact_id].state_by_agent)

    def get_agent_state(self, artifact_id: UUID, agent_id: UUID) -> MESIState | None:
        """Return MESI state for one agent/artifact pair if present."""
        return self._records[artifact_id].state_by_agent.get(agent_id)

    def set_agent_state(
        self,
        artifact_id: UUID,
        agent_id: UUID,
        state: MESIState,
        *,
        trigger: str = "unknown",
        tick: int = 0,
        content_hash: str | None = None,
    ) -> None:
        """Set MESI state for one agent/artifact pair."""
        record = self._records[artifact_id]
        from_state = record.state_by_agent.get(agent_id, MESIState.INVALID)
        record.state_by_agent[agent_id] = state

        # Crash-recovery bookkeeping (no log emit, no serialization). Runs
        # unconditionally and BEFORE the log emit so a state_log raise cannot
        # leave state_by_agent and granted_at_tick_by_agent inconsistent
        # (review fix COR-01 / REL-01: previously, a failed log emit left an
        # M∪E entry without a granted_at_tick slot, defeating max-hold reclaim).
        # Keeping the hot path branch-free preserves R5 byte-identity (these
        # are dict mutations only, never serialized) and avoids subtle
        # flag-on/flag-off divergence.
        new_in_me = state in _M_OR_E_STATES
        prev_in_me = from_state in _M_OR_E_STATES
        if new_in_me:
            if not prev_in_me:
                # Set granted_at_tick on M∪E acquire only; M↔E transitions preserve the
                # original grant tick (the agent has continuously held some M∪E grant).
                record.granted_at_tick_by_agent[agent_id] = tick
                # Slot clears on M∪E acquire ONLY (not on SHARED) — preserves the
                # checkpoint-restore diagnostic across SHARED re-fetches.
                record.last_reclamation_by_agent.pop(agent_id, None)
        elif prev_in_me:
            record.granted_at_tick_by_agent.pop(agent_id, None)
            # Read-generation fence: a sweep reclamation of this M/E grant bumps
            # the artifact's ownership epoch, atomically (GIL) with the INVALID
            # transition, so a commit by the reclaimed (or any pre-reclaim)
            # holder fails the generation check. Only sweep triggers bump.
            if trigger in RECLAIM_TRIGGERS:
                record.owner_generation += 1

        # Read-generation fence: capture the current ownership epoch into the
        # agent's read_generation when it establishes/refreshes a write-claim --
        # an E/M acquire (P0 fix: includes a pessimistic acquire with no prior
        # content read) or a genuine fetch read. Atomic (GIL) with the grant.
        # The INVALID guard hardens the fetch leg: no current fetch path grants
        # INVALID, but a future cache-miss-INVALID fetch must not mint a fresh
        # claim for an unfenced zombie.
        if (new_in_me and not prev_in_me) or (
            trigger in CLAIM_CAPTURE_TRIGGERS and state != MESIState.INVALID
        ):
            record.read_generation_by_agent[agent_id] = record.owner_generation

        if self._state_log is not None:
            self._seq += 1
            entry = {
                "tick": tick,
                "artifact_id": str(artifact_id),
                "agent_id": str(agent_id),
                "agent_name": self._agent_names.get(agent_id) if self._agent_names is not None else None,
                "from_state": from_state.name,
                "to_state": state.name,
                "trigger": trigger,
                "version": record.artifact.version,
                "content_hash": content_hash,
                "sequence_number": self._seq,
                "instance_id": self._instance_id,
                "schema_version": CCS_STATE_LOG_SCHEMA_VERSION,
            }
            try:
                self._state_log(entry)
            except Exception:
                # Sequence number is reserved on success, not on attempt.
                # Roll back so the next successful emission does not create a phantom gap.
                self._seq -= 1
                raise

    def commit_cas(
        self,
        artifact_id: UUID,
        agent_id: UUID,
        *,
        expected_version: int,
        content_hash: str,
        size_tokens: int | None = None,
        content: bytes | str | None = None,
        tick: int = 0,
        trigger: str = "commit_cas",
    ) -> CasResult:
        """In-memory optimistic-concurrency compare-and-swap (plan Unit 2,
        parity with :meth:`SqliteArtifactRegistry.commit_cas`). Written fresh
        from the contract — there is no ``resolve_or_register`` precedent here
        and the two registries share no base class. Plain GIL-atomic dict
        mutation; no ``BEGIN IMMEDIATE`` (single-process library callers).

        Same 3-outcome discrimination, version check BEFORE the holder check:

        - ``expected_version > current`` → :class:`CasCorruption` (no mutation).
        - ``expected_version < current`` → ``ConflictDetail("version_mismatch")``
          (no mutation).
        - version matches but another agent holds M/E → ``ConflictDetail(
          "other_holder")`` (no mutation).
        - else → WIN: version → ``current + 1``, committer S/I → SHARED (an OCC
          writer holds no grant — SHARED keeps its next commit_cas repeatable;
          MODIFIED would trip the service D4 precondition), peers → INVALID;
          returns ``(updated_artifact, invalidated_agent_ids)``.

        The state-log emit follows the same mutation-then-log + ``_seq``-rollback
        invariant as :meth:`set_agent_state`. To keep that invariant under a
        callback raise during peer/committer logging, the in-memory mutations
        are computed into a staging plan and applied only after all log entries
        emit successfully — so a raise leaves ``state_by_agent`` /
        ``granted_at_tick`` untouched (matching the sqlite ROLLBACK).

        ``content`` is the winning body. When provided (the in-process library
        path threads it from ``AgentRuntime.write_cas``) the WIN updates
        ``record.content`` to the NEW body, so a peer re-fetching after the win
        reads the winner's content at the new version — not the stale pre-CAS
        body, and (when versions are retained) captures that NEW body under
        ``next_version``. ``None`` (the cross-process / sqlite path, which stores
        no content) leaves the prior content-coherence behaviour unchanged AND
        skips the version capture (it is not retained under ``next_version``);
        previously the None path retained the stale OLD body under the new
        version, a latent history-poisoning bug now fixed.
        """
        record = self._records.get(artifact_id)
        if record is None:
            raise KeyError(f"artifact {artifact_id} not in registry")
        current = record.artifact.version

        if expected_version > current:
            return CasCorruption(current_version=current)
        if expected_version < current:
            return ConflictDetail("version_mismatch", current)
        other_holder = any(
            peer_id != agent_id and state in _M_OR_E_STATES
            for peer_id, state in record.state_by_agent.items()
        )
        if other_holder:
            return ConflictDetail("other_holder", current)

        # Read-generation fence: reject a committer whose CAPTURED read-claim
        # was superseded by a sweep reclamation. A reclaimed M/E holder kept its
        # stale read_generation (captured at acquire), so it is caught here even
        # though the version is unchanged and no peer holds M/E -- exactly what
        # version-CAS cannot catch. An ABSENT read_generation means the committer
        # never established a fence claim: a plain OCC writer whose lost-update
        # protection is version-CAS (checked above), so it is admitted. Strict->;
        # equality admits. Server-side; no commit_cas signature change.
        read_gen = record.read_generation_by_agent.get(agent_id)
        if read_gen is not None and read_gen < record.owner_generation:
            return ConflictDetail("stale_read_generation", current)

        # ---- WIN ----
        next_version = current + 1
        committer_from = record.state_by_agent.get(agent_id, MESIState.INVALID)
        peers = [
            (peer_id, state)
            for peer_id, state in record.state_by_agent.items()
            if peer_id != agent_id and state != MESIState.INVALID
        ]

        # Emit all state_log entries FIRST (peers then committer), reserving
        # _seq per emission. If any raises, _emit_state_log has already
        # decremented its own reservation; we decrement the ones that already
        # succeeded in this call and re-raise — nothing has mutated yet, so the
        # registry stays consistent (mutation-then-log parity).
        emitted_here = 0
        try:
            for peer_id, peer_from in peers:
                emitted_here += self._emit_state_log(
                    artifact_id=artifact_id,
                    agent_id=peer_id,
                    from_state=peer_from,
                    to_state=MESIState.INVALID,
                    trigger=trigger,
                    tick=tick,
                    version=next_version,
                    content_hash=None,
                )
            emitted_here += self._emit_state_log(
                artifact_id=artifact_id,
                agent_id=agent_id,
                from_state=committer_from,
                to_state=MESIState.SHARED,
                trigger=trigger,
                tick=tick,
                version=next_version,
                content_hash=content_hash,
            )
        except Exception:
            self._seq -= emitted_here
            raise

        # All logs emitted — now apply the mutations (cannot fail).
        updated = Artifact(
            id=artifact_id,
            name=record.artifact.name,
            version=next_version,
            content_hash=content_hash,
            size_tokens=size_tokens if size_tokens is not None else record.artifact.size_tokens,
            depends_on=record.artifact.depends_on,
        )
        # Content coherence: when the caller threaded the winning body, advance
        # record.content to it so a peer re-fetch reads the NEW content at the new
        # version (without this, version + content_hash bump but the body stays
        # stale). content is None on the cross-process / sqlite path (no content
        # stored) — keep the prior body unchanged there.
        if content is not None:
            record.content = content
        # Retention capture joins this APPLIED block (after every state_log emit
        # succeeded) so a callback raise above cannot leave phantom history —
        # parity with the stage-then-apply discipline for the MESI mutations.
        # content=None SKIPS capture entirely (R-fix): the old code retained the
        # OLD body under the NEW version when content was None — a latent
        # history-poisoning bug only observable through retention reads. Now an
        # unsupplied body simply means "no snapshot for this version"; a later
        # read of next_version misses (None), rather than returning stale bytes.
        if self._retain_versions and content is not None:
            self._capture_version(record, next_version, content)
        record.artifact = updated
        record.last_writer = agent_id

        invalidated: list[UUID] = []
        for peer_id, peer_from in peers:
            record.state_by_agent[peer_id] = MESIState.INVALID
            if peer_from in _M_OR_E_STATES:
                record.granted_at_tick_by_agent.pop(peer_id, None)
            invalidated.append(peer_id)

        # Committer S/I → SHARED, NOT MODIFIED: an OCC writer is optimistic and
        # holds no grant, so SHARED is the honest end-state and keeps the same
        # agent's next commit_cas repeatable (a sticky MODIFIED would trip the
        # service D4 "M/E callers use commit()" precondition). SHARED is not in
        # M∪E, so this is not an acquire — do NOT set granted_at_tick and do NOT
        # clear the reclaim slot (mirror set_agent_state's non-M/E-from-non-M/E
        # path, which leaves both untouched).
        record.state_by_agent[agent_id] = MESIState.SHARED

        return updated, invalidated

    def _emit_state_log(
        self,
        *,
        artifact_id: UUID,
        agent_id: UUID,
        from_state: MESIState,
        to_state: MESIState,
        trigger: str,
        tick: int,
        version: int,
        content_hash: str | None,
    ) -> int:
        """Emit one ``state_log`` entry for the inlined CAS region. Returns 1 if
        ``_seq`` was bumped (0 if no state_log configured). On callback raise,
        decrements its own reservation and re-raises (mutation-then-log parity
        with :meth:`set_agent_state`)."""
        if self._state_log is None:
            return 0
        self._seq += 1
        entry = {
            "tick": tick,
            "artifact_id": str(artifact_id),
            "agent_id": str(agent_id),
            "agent_name": self._agent_names.get(agent_id) if self._agent_names is not None else None,
            "from_state": from_state.name,
            "to_state": to_state.name,
            "trigger": trigger,
            "version": version,
            "content_hash": content_hash,
            "sequence_number": self._seq,
            "instance_id": self._instance_id,
            "schema_version": CCS_STATE_LOG_SCHEMA_VERSION,
        }
        try:
            self._state_log(entry)
        except Exception:
            self._seq -= 1
            raise
        return 1

    def record_heartbeat(self, agent_id: UUID, now_tick: int) -> None:
        """Record an agent's heartbeat tick using max(prev, incoming) (R12 monotonicity)."""
        prev = self._heartbeat_by_agent.get(agent_id)
        if prev is None or now_tick > prev:
            self._heartbeat_by_agent[agent_id] = now_tick

    def last_heartbeat_tick(self, agent_id: UUID) -> int | None:
        """Return the last recorded heartbeat tick for an agent, if any."""
        return self._heartbeat_by_agent.get(agent_id)

    def record_last_reclamation(
        self, agent_id: UUID, artifact_id: UUID, trigger: str, tick: int
    ) -> None:
        """Record the most recent reclamation slot for an (agent, artifact) pair."""
        self._records[artifact_id].last_reclamation_by_agent[agent_id] = (trigger, tick)

    def get_last_reclamation(
        self, agent_id: UUID, artifact_id: UUID
    ) -> ReclamationSlot | None:
        """Return the most recent reclamation slot for an (agent, artifact) pair, if any."""
        record = self._records.get(artifact_id)
        if record is None:
            return None
        return record.last_reclamation_by_agent.get(agent_id)

    def granted_at_tick(self, agent_id: UUID, artifact_id: UUID) -> int | None:
        """Return the tick at which agent acquired its current M/E grant on artifact, if any."""
        record = self._records.get(artifact_id)
        if record is None:
            return None
        return record.granted_at_tick_by_agent.get(agent_id)

    def get_agent_transient(self, artifact_id: UUID, agent_id: UUID) -> TransientState | None:
        """Return transient state for one agent/artifact pair if present."""
        return self._records[artifact_id].transient_by_agent.get(agent_id)

    def set_agent_transient(
        self,
        artifact_id: UUID,
        agent_id: UUID,
        transient_state: TransientState,
        *,
        entered_tick: int,
    ) -> None:
        """Set transient state and entry tick for one agent/artifact pair."""
        self._records[artifact_id].transient_by_agent[agent_id] = transient_state
        self._records[artifact_id].transient_tick_by_agent[agent_id] = entered_tick

    def clear_agent_transient(self, artifact_id: UUID, agent_id: UUID) -> None:
        """Clear transient state and timestamp for one agent/artifact pair."""
        self._records[artifact_id].transient_by_agent.pop(agent_id, None)
        self._records[artifact_id].transient_tick_by_agent.pop(agent_id, None)

    def get_transient_map(self, artifact_id: UUID) -> dict[UUID, TransientState]:
        """Return copy of per-agent transient states for an artifact."""
        return dict(self._records[artifact_id].transient_by_agent)

    def get_transient_tick(self, artifact_id: UUID, agent_id: UUID) -> int | None:
        """Return tick when agent entered transient state if present."""
        return self._records[artifact_id].transient_tick_by_agent.get(agent_id)

    def remove_artifact(self, artifact_id: UUID) -> None:
        """Remove artifact record and all associated state from registry."""
        self._records.pop(artifact_id, None)

    def valid_holders(self, artifact_id: UUID) -> list[UUID]:
        """Return agents that currently hold non-invalid entries."""
        return [
            agent_id
            for agent_id, state in self._records[artifact_id].state_by_agent.items()
            if state != MESIState.INVALID
        ]
