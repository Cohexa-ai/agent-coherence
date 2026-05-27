# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""Replay invariant predicates — Core 4 + SKIPPED dispatch (Unit 4 of D v1).

Consumer side of the replay-trace contract documented in
``docs/proposals/replay_trace_format.md`` §5.2-§5.3. Walks the merged
stream from :class:`ccs.replay.loader.LoadedTrace` exactly once and
emits structured findings for breaches of the four v1 invariants:

- ``single-writer`` — at most one agent in MODIFIED or EXCLUSIVE per
  artifact (reuses :func:`ccs.core.invariants.check_single_writer`).
- ``monotonic-version`` — committed versions strictly increase per
  artifact (reuses :func:`ccs.core.invariants.check_monotonic_version`).
- ``stale-read`` — content-audit reads of versions older than the
  latest committed version, with AMBIGUOUS carve-out for same-tick
  collisions (intra-tick ordering undetermined per §5.2).
- ``lost-write`` — writer commits whose ``from_state`` is not in
  ``{MODIFIED, EXCLUSIVE}``.

SKIPPED dispatch (§5.3): each predicate declares its
``required_streams``; predicates whose requirements aren't satisfied by
``loaded_trace.streams_present`` are omitted from the walk and produce
a :class:`SummaryFinding` instead. ``opted_out`` distinguishes user
opt-out (stream not in ``manifest.streams``) from capture bug (stream
declared but missing on disk) so Unit 5's CLI can pick the right exit
code.

Two folds shared across predicates keep the walk to a single pass:

- ``per_(agent, artifact)_state`` — updated from every state_log entry.
  Single-writer scans it after every transition; lost-write reads the
  writer's pre-commit state from each surviving commit entry's
  ``from_state``.
- ``per_artifact_latest_committed_version`` — updated only from
  state_log entries with ``trigger="commit"`` AND
  ``to_state ∈ {MODIFIED, EXCLUSIVE}``. Peer-invalidation transitions
  also carry ``trigger="commit"`` (per ``coordinator/service.py:277``)
  but resolve to ``to_state=INVALID``; counting them as commits would
  flood every clean trace with N false-positive lost-writes per commit
  (the LostWrite ADV-01 v1-blocker fix from document review).

The stale-read predicate compares ``tick`` fields directly (not merge
order) when deciding AMBIGUOUS vs CONFIRMED — the merge rule places
state_log before content_audit_log at the same tick, so relying on
merge order would falsely classify every same-tick collision as
CONFIRMED.
"""

from __future__ import annotations

import abc
from dataclasses import dataclass, field
from typing import Any, ClassVar, Iterable

from ccs.core.invariants import check_monotonic_version, check_single_writer
from ccs.core.exceptions import InvariantViolationError
from ccs.core.states import MESIState
from ccs.replay.loader import LoadedTrace

__all__ = [
    "Finding",
    "SummaryFinding",
    "Predicate",
    "SingleWriterPredicate",
    "MonotonicVersionPredicate",
    "StaleReadPredicate",
    "LostWritePredicate",
    "run_predicates",
    "CORE_PREDICATES",
]

_STATE_LOG = "state_log"
_CONTENT_AUDIT_LOG = "content_audit_log"

# State values that count as "the writer holds the line". Used by
# single-writer (count agents in this set per artifact) and lost-write
# (the surviving commit's from_state must be in this set).
_OWNER_STATES: frozenset[MESIState] = frozenset(
    {MESIState.MODIFIED, MESIState.EXCLUSIVE}
)


# ---------------------------------------------------------------------------
# Finding dataclasses
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Finding:
    """One invariant breach. Schema matches §7.1 of the trace-format spec.

    ``severity`` is ``"CONFIRMED"`` for breaches the predicate can
    attest from observable trace data, and ``"AMBIGUOUS"`` for the
    stale-read same-tick carve-out only — intra-tick ordering is
    undetermined per spec §5.2 so the finding is reported but suppressed
    from default --json output (Unit 5 surfaces it via
    ``--include-ambiguous``).
    """

    kind: str  # "single-writer-violation", "monotonic-version-violation", ...
    severity: str  # "CONFIRMED" or "AMBIGUOUS"
    invariant: str  # "single-writer", "monotonic-version", "stale-read", "lost-write"
    agents: tuple[str, ...]
    artifacts: tuple[str, ...]
    tick_range: tuple[int, int]  # (start, end) inclusive
    context: dict[str, Any] = field(default_factory=dict)
    details: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class SummaryFinding:
    """Summary-only entry for a predicate that was skipped.

    Distinct from :class:`Finding` so callers can route per-finding
    output (the breaches) and summary lines (the skips) separately —
    Unit 5's --json formatter and exit-code logic both depend on the
    distinction.

    ``opted_out=True`` means the missing stream is not in
    ``manifest.streams`` (the caller explicitly opted out, exit 0).
    ``opted_out=False`` means the stream is declared but absent on disk
    (capture bug, exit 2).
    """

    kind: str  # always "skipped" in v1
    invariant: str
    reason: str
    stream_required: str
    opted_out: bool


# ---------------------------------------------------------------------------
# Shared-fold state container
# ---------------------------------------------------------------------------


@dataclass
class _SharedState:
    """Two folds shared across predicates so the merged stream is walked once.

    Lives at engine scope; predicates read/write through reference. Not
    exported — predicates only access via the engine's ``_apply_state_log``.
    """

    # Per (agent_id, artifact_id) → current MESI state. Updated from every
    # state_log entry's to_state.
    state_by_agent_artifact: dict[tuple[str, str], MESIState] = field(default_factory=dict)
    # Per artifact_id → latest version committed by trigger="commit" AND
    # to_state ∈ {MODIFIED, EXCLUSIVE}. Other trigger="commit" entries
    # (peer invalidations to INVALID) do NOT update this fold.
    latest_committed_version: dict[str, int] = field(default_factory=dict)
    # Per artifact_id → tick at which latest_committed_version was last set.
    # StaleReadPredicate needs this to decide AMBIGUOUS vs CONFIRMED.
    latest_committed_tick: dict[str, int] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Predicate base class
# ---------------------------------------------------------------------------


class Predicate(abc.ABC):
    """Abstract base for the four v1 invariant predicates.

    Concrete predicates declare ``name`` (short kebab-case identifier
    matching the spec, e.g. ``"lost-write"``) and ``required_streams``
    (set of stream names this predicate needs). The engine intersects
    ``required_streams`` with ``loaded_trace.streams_present`` to decide
    whether to dispatch or skip.
    """

    name: ClassVar[str]
    required_streams: ClassVar[set[str]]

    @abc.abstractmethod
    def process(
        self,
        entry: dict[str, Any],
        stream_kind: str,
        shared: _SharedState,
    ) -> Iterable[Finding]:
        """Inspect one merged-stream entry and yield findings (zero or more).

        Implementations MUST NOT mutate ``shared`` — the engine owns
        fold updates so they apply uniformly across predicates and
        survive a SKIPPED predicate being absent from the run.
        """
        raise NotImplementedError

    def finalize(self) -> Iterable[Finding]:
        """Emit any findings deferred until end-of-stream. Default: none.

        v1 invariants are all online (decision per entry); reserved for
        D+1 invariants like heartbeat-liveness that need the end tick
        before deciding a finding.
        """
        return ()


# ---------------------------------------------------------------------------
# SingleWriterPredicate
# ---------------------------------------------------------------------------


class SingleWriterPredicate(Predicate):
    """Flag any tick where >1 agent holds M or E for the same artifact.

    Walks state_log entries; after the engine applies the entry to the
    shared fold, this predicate scans the per-artifact state map for
    M∪E owners and emits CONFIRMED on overlap. The check delegates the
    "any owners overlap?" decision to
    :func:`ccs.core.invariants.check_single_writer` (it raises on
    overlap; we translate that into a Finding rather than re-running
    the count locally — the runtime helper is the canonical predicate
    definition).
    """

    name = "single-writer"
    required_streams = {_STATE_LOG}

    def process(
        self,
        entry: dict[str, Any],
        stream_kind: str,
        shared: _SharedState,
    ) -> Iterable[Finding]:
        if stream_kind != _STATE_LOG:
            return ()
        artifact_id = entry.get("artifact_id")
        if not artifact_id:
            return ()
        state_by_agent = _collect_agents_for_artifact(
            shared.state_by_agent_artifact, artifact_id
        )
        try:
            check_single_writer(state_by_agent)
        except InvariantViolationError:
            owners = sorted(
                agent_id
                for agent_id, state in state_by_agent.items()
                if state in _OWNER_STATES
            )
            return [_make_single_writer_finding(entry, artifact_id, owners)]
        return ()


def _collect_agents_for_artifact(
    state_map: dict[tuple[str, str], MESIState],
    artifact_id: str,
) -> dict[str, MESIState]:
    """Project the (agent, artifact) fold onto one artifact."""
    return {
        agent_id: state
        for (agent_id, art_id), state in state_map.items()
        if art_id == artifact_id
    }


def _make_single_writer_finding(
    entry: dict[str, Any],
    artifact_id: str,
    owners: list[str],
) -> Finding:
    tick = int(entry["tick"])
    return Finding(
        kind="single-writer-violation",
        severity="CONFIRMED",
        invariant="single-writer",
        agents=tuple(owners),
        artifacts=(artifact_id,),
        tick_range=(tick, tick),
        context={
            "stream": _STATE_LOG,
            "sequence_number": entry.get("sequence_number"),
        },
        details={
            "expected": "<=1 agent in M or E",
            "observed": f"{len(owners)} agents in M or E",
        },
    )


# ---------------------------------------------------------------------------
# MonotonicVersionPredicate
# ---------------------------------------------------------------------------


class MonotonicVersionPredicate(Predicate):
    """Flag any commit whose version is not strictly greater than the prior.

    Only inspects state_log entries with ``trigger="commit"`` AND
    ``to_state ∈ {MODIFIED, EXCLUSIVE}`` — peer-invalidation
    transitions ALSO carry ``trigger="commit"`` (per
    ``coordinator/service.py:277``) but reach ``to_state=INVALID`` and
    do not introduce a new version; treating them as commits would
    spuriously fire the predicate when a peer is invalidated at the
    same version.

    Delegates the comparison to
    :func:`ccs.core.invariants.check_monotonic_version`. Note that
    helper checks ``current < previous``; the spec asks for *strict*
    increase (``current > previous``), so we shift previous by +1
    before calling — keeps the runtime predicate as canonical.
    """

    name = "monotonic-version"
    required_streams = {_STATE_LOG}

    def process(
        self,
        entry: dict[str, Any],
        stream_kind: str,
        shared: _SharedState,
    ) -> Iterable[Finding]:
        if stream_kind != _STATE_LOG:
            return ()
        if not _is_writer_commit(entry):
            return ()
        artifact_id = entry["artifact_id"]
        version = int(entry["version"])
        # -1 sentinel: any non-negative version satisfies strict increase
        # from the initial empty state.
        previous = shared.latest_committed_version.get(artifact_id, -1)
        try:
            # Spec asks for STRICT increase. The runtime helper raises only
            # on regression (current < previous); shifting previous by +1
            # turns the runtime check into a strict-monotonic check
            # without re-implementing it.
            check_monotonic_version(previous + 1, version)
        except InvariantViolationError:
            return [_make_monotonic_version_finding(entry, artifact_id, previous, version)]
        return ()


def _is_writer_commit(entry: dict[str, Any]) -> bool:
    """True iff this state_log entry is a writer self-transition commit.

    Excludes peer-invalidation transitions that also carry
    ``trigger="commit"`` per ``coordinator/service.py:277`` — those
    resolve to ``to_state=INVALID`` and must not advance the
    latest-version fold or trigger monotonic-version / lost-write.
    """
    if entry.get("trigger") != "commit":
        return False
    to_state = entry.get("to_state")
    return to_state in {MESIState.MODIFIED.name, MESIState.EXCLUSIVE.name}


def _make_monotonic_version_finding(
    entry: dict[str, Any],
    artifact_id: str,
    previous: int,
    observed: int,
) -> Finding:
    tick = int(entry["tick"])
    return Finding(
        kind="monotonic-version-violation",
        severity="CONFIRMED",
        invariant="monotonic-version",
        agents=(entry.get("agent_id") or "",),
        artifacts=(artifact_id,),
        tick_range=(tick, tick),
        context={
            "stream": _STATE_LOG,
            "sequence_number": entry.get("sequence_number"),
        },
        details={
            "expected": f"version > {previous}",
            "observed": f"version = {observed}",
        },
    )


# ---------------------------------------------------------------------------
# StaleReadPredicate
# ---------------------------------------------------------------------------


class StaleReadPredicate(Predicate):
    """Flag content-audit reads of versions older than latest committed.

    Compares ``entry.tick`` to ``latest_committed_tick`` DIRECTLY (not
    merge order). Merge order at the same tick puts state_log before
    content_audit_log per the loader's tiebreaker, so a merge-order
    decision would falsely classify every same-tick collision as
    CONFIRMED. The spec requires AMBIGUOUS for same-tick collisions
    (§5.2) and CONFIRMED only when the read tick strictly exceeds the
    commit tick.

    Entries with ``agent_id=None`` are skipped silently — the
    CCSStore-local search-miss path emits content_audit entries with
    null agent_id (spec §4.1); those are not reader activity and must
    not trigger the predicate.
    """

    name = "stale-read"
    required_streams = {_STATE_LOG, _CONTENT_AUDIT_LOG}

    def process(
        self,
        entry: dict[str, Any],
        stream_kind: str,
        shared: _SharedState,
    ) -> Iterable[Finding]:
        if stream_kind != _CONTENT_AUDIT_LOG:
            return ()
        if entry.get("outcome") != "content":
            return ()
        agent_id = entry.get("agent_id")
        if agent_id is None:
            # search-miss path; spec §4.1 says skip silently.
            return ()
        artifact_id = entry.get("artifact_id")
        observed_version = entry.get("version")
        if artifact_id is None or observed_version is None:
            return ()
        latest_version = shared.latest_committed_version.get(artifact_id)
        if latest_version is None or observed_version >= latest_version:
            return ()
        return _classify_stale_read(
            entry, agent_id, artifact_id,
            observed_version, latest_version,
            shared.latest_committed_tick[artifact_id],
        )


def _classify_stale_read(
    entry: dict[str, Any],
    agent_id: str,
    artifact_id: str,
    observed_version: int,
    latest_version: int,
    latest_commit_tick: int,
) -> Iterable[Finding]:
    """Decide CONFIRMED vs AMBIGUOUS based on tick comparison.

    AMBIGUOUS is reserved exclusively for same-tick collisions where
    intra-tick ordering is undetermined per spec §5.2. Strict
    later-tick reads of older versions are CONFIRMED. Strict
    earlier-tick reads cannot happen by merge-order construction; we
    skip silently as defensive code.
    """
    read_tick = int(entry["tick"])
    if read_tick > latest_commit_tick:
        severity = "CONFIRMED"
        kind = "stale-read"
    elif read_tick == latest_commit_tick:
        severity = "AMBIGUOUS"
        kind = "stale-read-ambiguous"
    else:
        return []
    return [Finding(
        kind=kind,
        severity=severity,
        invariant="stale-read",
        agents=(agent_id,),
        artifacts=(artifact_id,),
        tick_range=(latest_commit_tick, read_tick),
        context={
            "stream": _CONTENT_AUDIT_LOG,
            "sequence_number": entry.get("sequence_number"),
        },
        details={
            "expected": f"version >= {latest_version}",
            "observed": f"version = {observed_version}",
        },
    )]


# ---------------------------------------------------------------------------
# LostWritePredicate
# ---------------------------------------------------------------------------


class LostWritePredicate(Predicate):
    """Flag commit transitions from a non-owner state (M∪E required).

    CRITICAL FILTER: only inspects state_log entries with
    ``trigger="commit"`` AND ``to_state ∈ {MODIFIED, EXCLUSIVE}``. This
    excludes peer-invalidation transitions which ALSO carry
    ``trigger="commit"`` but reach ``to_state=INVALID`` (per
    ``coordinator/service.py:277``). Without the filter the predicate
    fires one false-positive finding per peer per commit on every
    clean trace — the LostWrite ADV-01 v1-blocker fix from document
    review.
    """

    name = "lost-write"
    required_streams = {_STATE_LOG}

    def process(
        self,
        entry: dict[str, Any],
        stream_kind: str,
        shared: _SharedState,
    ) -> Iterable[Finding]:
        if stream_kind != _STATE_LOG:
            return ()
        if not _is_writer_commit(entry):
            return ()
        from_state = entry.get("from_state")
        if from_state in {MESIState.MODIFIED.name, MESIState.EXCLUSIVE.name}:
            return ()
        return [_make_lost_write_finding(entry, from_state)]


def _make_lost_write_finding(
    entry: dict[str, Any],
    from_state: str | None,
) -> Finding:
    tick = int(entry["tick"])
    artifact_id = entry["artifact_id"]
    return Finding(
        kind="lost-write",
        severity="CONFIRMED",
        invariant="lost-write",
        agents=(entry.get("agent_id") or "",),
        artifacts=(artifact_id,),
        tick_range=(tick, tick),
        context={
            "stream": _STATE_LOG,
            "sequence_number": entry.get("sequence_number"),
        },
        details={
            "expected": "from_state in {MODIFIED, EXCLUSIVE}",
            "observed": f"from_state = {from_state}",
        },
    )


# ---------------------------------------------------------------------------
# Engine
# ---------------------------------------------------------------------------


CORE_PREDICATES: tuple[type[Predicate], ...] = (
    SingleWriterPredicate,
    MonotonicVersionPredicate,
    StaleReadPredicate,
    LostWritePredicate,
)


def run_predicates(
    loaded_trace: LoadedTrace,
    invariants: Iterable[str] | None = None,
) -> tuple[list[Finding], list[SummaryFinding]]:
    """Run the predicate engine over a loaded trace.

    Walks ``loaded_trace.merged()`` exactly once. Predicates whose
    ``required_streams`` are not a subset of
    ``loaded_trace.streams_present`` are omitted from the walk and
    produce SKIPPED summary findings instead.

    Args:
        loaded_trace: From :func:`ccs.replay.load`.
        invariants: Optional restriction to a subset of predicate names
            (e.g. ``["lost-write"]``). Predicates outside the subset
            are NOT dispatched AND NOT skipped — they're entirely
            absent from the result.

    Returns:
        ``(findings, summary_findings)`` — per-breach findings in
        merge order, plus one summary entry per skipped predicate per
        missing stream.
    """
    selected = _select_predicates(invariants)
    active, summary = _partition_active(selected, loaded_trace)
    findings = _walk(loaded_trace, active)
    return findings, summary


def _select_predicates(
    invariants: Iterable[str] | None,
) -> list[Predicate]:
    """Instantiate the predicates the caller asked for (or all four)."""
    if invariants is None:
        return [cls() for cls in CORE_PREDICATES]
    wanted = set(invariants)
    return [cls() for cls in CORE_PREDICATES if cls.name in wanted]


def _partition_active(
    predicates: list[Predicate],
    loaded_trace: LoadedTrace,
) -> tuple[list[Predicate], list[SummaryFinding]]:
    """Split predicates into active set vs SKIPPED summary findings.

    A predicate is active iff every stream in its ``required_streams``
    is in ``loaded_trace.streams_present``. Otherwise one
    SummaryFinding is emitted per missing stream — the spec's --json
    format keys SKIPPED entries by ``stream_required`` so partners can
    see exactly which file was absent.
    """
    declared_streams = set(loaded_trace.manifest.get("streams") or [])
    present = loaded_trace.streams_present
    active: list[Predicate] = []
    summary: list[SummaryFinding] = []
    for predicate in predicates:
        missing = predicate.required_streams - present
        if not missing:
            active.append(predicate)
            continue
        for stream_name in sorted(missing):
            summary.append(_make_skipped(predicate, stream_name, declared_streams))
    return active, summary


def _make_skipped(
    predicate: Predicate,
    stream_name: str,
    declared_streams: set[str],
) -> SummaryFinding:
    """Build a SKIPPED summary finding.

    ``opted_out=True`` when the missing stream is NOT in
    ``manifest.streams`` (caller explicitly opted out at capture time —
    Unit 5 maps this to exit code 0). ``opted_out=False`` when the
    stream IS declared but absent on disk (capture bug — Unit 5 maps
    this to exit code 2).
    """
    opted_out = stream_name not in declared_streams
    if opted_out:
        reason = (
            f"{predicate.name} requires {stream_name} stream — "
            f"caller opted out at capture time (not in manifest.streams)"
        )
    else:
        reason = (
            f"{predicate.name} requires {stream_name} stream — "
            f"declared in manifest.streams but missing from session "
            f"directory (capture-side bug)"
        )
    return SummaryFinding(
        kind="skipped",
        invariant=predicate.name,
        reason=reason,
        stream_required=stream_name,
        opted_out=opted_out,
    )


def _walk(
    loaded_trace: LoadedTrace,
    active: list[Predicate],
) -> list[Finding]:
    """Walk the merged stream once, dispatching to active predicates.

    Per-entry sequence is load-bearing:

    1. **state map update** (state_log only) — single-writer reads the
       NEW post-transition state map, so the apply must happen first.
    2. **predicate dispatch** — every active predicate sees the entry.
       Monotonic-version + stale-read read the OLD
       ``latest_committed_version`` here, so the version fold must be
       stale at this point.
    3. **version fold update** (state_log writer-commit only) — happens
       AFTER predicates so step 2's "old" reads are correct. This
       split is the only way to keep all four predicates correct with
       a single-pass walk.
    """
    findings: list[Finding] = []
    shared = _SharedState()
    for stream_kind, entry in loaded_trace.merged():
        if stream_kind == _STATE_LOG:
            _apply_state_map(entry, shared)
        for predicate in active:
            findings.extend(predicate.process(entry, stream_kind, shared))
        if stream_kind == _STATE_LOG and _is_writer_commit(entry):
            _apply_version_fold(entry, shared)
    for predicate in active:
        findings.extend(predicate.finalize())
    return findings


def _apply_state_map(entry: dict[str, Any], shared: _SharedState) -> None:
    """Update ``state_by_agent_artifact`` from one state_log entry.

    Runs BEFORE predicate dispatch so SingleWriterPredicate sees the
    new state. No-op for entries missing required fields or with an
    unknown to_state — defensive against partner-shaped traces.
    """
    agent_id = entry.get("agent_id")
    artifact_id = entry.get("artifact_id")
    to_state_name = entry.get("to_state")
    if not (agent_id and artifact_id and to_state_name):
        return
    try:
        to_state = MESIState[to_state_name]
    except KeyError:
        return
    shared.state_by_agent_artifact[(agent_id, artifact_id)] = to_state


def _apply_version_fold(entry: dict[str, Any], shared: _SharedState) -> None:
    """Update ``latest_committed_version`` + ``latest_committed_tick``.

    Runs AFTER predicate dispatch so monotonic-version + stale-read
    see the pre-update snapshot. Caller guarantees
    ``_is_writer_commit(entry)`` is true. The fold advances only when
    the new version is strictly greater than the running max — a
    regression entry (caught by monotonic-version) does NOT poison
    later reads against the regressed value.
    """
    artifact_id = entry.get("artifact_id")
    version = entry.get("version")
    tick = entry.get("tick")
    if not (artifact_id and isinstance(version, int) and isinstance(tick, int)):
        return
    previous = shared.latest_committed_version.get(artifact_id, -1)
    if version > previous:
        shared.latest_committed_version[artifact_id] = version
        shared.latest_committed_tick[artifact_id] = tick
