# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""Divergence detection engine for ``ccs-diagnose`` (Unit 4).

Consumes a ``DiagnoseEvent`` buffer (Unit 2) and a ``ClassifierVerdict``
(Unit 3) and produces a :class:`DetectionReport` describing artifacts whose
reads were *handed* divergent versions across nodes within a single run.

Witness-quality framing
=======================

Per the central honesty contract: the diagnose surface cannot prove that
node Z *read* artifact Y from the merged state — only that the runtime
*handed* node Z a state in which Y differed from what an earlier reader
saw. Every event in this module records "was handed", never "did read".
The :class:`DivergenceEvent` field names (``earlier_read``, ``later_read``)
follow ``ReadObservation`` semantics: an observation that the runtime
*passed* an artifact value to a node — not a guarantee the node consulted
it.

Reformulated divergence rule (AND clause)
=========================================

A divergence event is recorded when:

* node X is handed merged state with artifact Y at version ``V_a``
  (read view from a ``node_start`` event), AND
* node Z (later, no intervening write to Y) is handed merged state with
  Y at version ``V_b``, AND
* ``V_a != V_b`` AND ``content_hash(Y at V_a) != content_hash(Y at V_b)``

The AND clause prevents false positives from monotonic-stamp reducers
that bump ``channel_versions`` on no-op writes (the version differs but
the content hash is identical, so the read is *effectively* current).

Three exclusion classes
=======================

* **sequential staleness** — read came ``>= 2`` ticks after a prior write
  to that artifact AND there is at least one intervening write to the
  artifact between the prior write and this read; often expected in
  pipeline workflows.
* **cold-start reads** — the *later* read is the first observation of
  the artifact by *any* reader and there is no prior write to it; nothing
  to be stale against.
* **append-only artifacts** — keys in ``verdict.append_only_keys`` are
  skipped entirely (their version churn is expected). ``mutable_keys``
  artifacts ARE subject to detection.

``strict=True`` promotes sequential-staleness exclusions back into the
headline divergence count. Cold-start and append-only exclusions are
**not** affected by ``--strict``.

Cost extrapolation
==================

When ``volume_per_hour`` is supplied:

.. code-block::

    rework_tokens / observed_ticks
        × volume_per_hour × 8760
        × token_cost_per_1k / 1000

Interpretation: ``volume_per_hour`` is "interactions per hour" where one
interaction is treated as one super-step. Hours-per-year (``8760``) keeps
the formula self-contained. The renderer (Unit 5) labels the resulting
number as a *floor* — broadcast rebroadcasting and redundant fetch costs
are not modelled.

Honesty boundary on ``rework_tokens``
=====================================

Per divergence event ``rework_tokens`` is the token count of the artifact
*at the canonical (latest) version the stale reader did not see*. The
event buffer carries only content hashes (not raw values), so the cost
line is unmeasurable from the buffer alone. Callers who attached a
:class:`ccs.diagnose.checkpointer.DiagnoseCheckpointer` can populate
``value_token_estimates`` (UUID → token count); when omitted the report
emits zero rework tokens, zero cost-this-run, and a non-``None``
``cost_unmeasurable_reason`` so Unit 5 can render an explanatory note.
"""

from __future__ import annotations

import uuid
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from typing import Any

from ccs.diagnose import CCS_DIAGNOSE_LOG_SCHEMA_VERSION
from ccs.diagnose.callback import DiagnoseEvent
from ccs.diagnose.classifier import Bucket, ClassifierVerdict

__all__ = [
    "ReadObservation",
    "DivergenceEvent",
    "HeatmapRow",
    "ReaderPairCount",
    "ExclusionPanel",
    "DetectionReport",
    "detect",
    "build_report_json",
]


# Hours per year — intentionally a constant the renderer can quote.
_HOURS_PER_YEAR: int = 8760


# -------------------------------------------------------------------- #
# Public dataclasses
# -------------------------------------------------------------------- #


@dataclass(frozen=True)
class ReadObservation:
    """Single read view as handed to a node by the runtime.

    Witness-quality: ``node`` was *handed* an artifact at this ``version``
    / ``content_hash`` on this ``tick``. Whether the node actually consulted
    the value is unobservable.
    """

    node: str
    tick: int
    version: str
    content_hash: str


@dataclass(frozen=True)
class DivergenceEvent:
    """A pair of read views in the same run that disagree on an artifact.

    ``rework_tokens`` is the token count of the canonical (latest) version
    the ``later_read`` did not see. ``0`` when ``value_token_estimates`` is
    not provided to :func:`detect`.

    ``is_sequential_staleness`` and ``is_cold_start`` are *informational*
    and remain ``True`` even when ``strict=True`` promotes a sequential
    staleness event into the headline count; the renderer can still flag
    the original classification.
    """

    artifact_key: str
    artifact_id: uuid.UUID
    earlier_read: ReadObservation
    later_read: ReadObservation
    canonical_writer: str | None
    canonical_writer_tick: int | None
    rework_tokens: int
    is_sequential_staleness: bool
    is_cold_start: bool


@dataclass(frozen=True)
class HeatmapRow:
    """Per-artifact divergent vs total read counts (for the heatmap panel).

    ``divergent_reads`` is the number of *distinct read observations* handed
    a divergent version — i.e. the ``later_read`` of at least one headline
    divergence event. It is a subset of ``total_reads`` (the count of read
    observations for the artifact), so ``divergent_reads <= total_reads``
    always holds and the rendered ``share`` is bounded to ``[0, 100%]``. It
    is deliberately *not* the headline-event count: events are ordered read
    *pairs* (``O(n^2)``), which can far exceed the read count.
    """

    artifact_key: str
    artifact_id: uuid.UUID
    divergent_reads: int
    total_reads: int


@dataclass(frozen=True)
class ReaderPairCount:
    """Count of headline divergence events for an ``(earlier, later)`` pair."""

    earlier_reader: str
    later_reader: str
    event_count: int


@dataclass(frozen=True)
class ExclusionPanel:
    """Counts of divergence-rule pairs excluded by the default rules."""

    sequential_staleness_count: int
    cold_start_count: int
    append_only_skip_count: int


@dataclass(frozen=True)
class DetectionReport:
    """Frozen detection report consumed by Unit 5's renderer."""

    headline_divergence_events: tuple[DivergenceEvent, ...]
    excluded_events: tuple[DivergenceEvent, ...]
    heatmap: tuple[HeatmapRow, ...]
    reader_pair_matrix: tuple[ReaderPairCount, ...]
    top_event: DivergenceEvent | None
    exclusion_panel: ExclusionPanel
    agent_pain_count: int
    rework_tokens_this_run: int
    rework_cost_this_run: float
    rework_cost_annualized: float | None
    cost_unmeasurable_reason: str | None
    strict_mode: bool
    schema_version: str


# -------------------------------------------------------------------- #
# Public entry point
# -------------------------------------------------------------------- #


def detect(
    events: Sequence[DiagnoseEvent],
    *,
    verdict: ClassifierVerdict,
    key_index: Mapping[str, uuid.UUID],
    strict: bool = False,
    volume_per_hour: float | None = None,
    value_token_estimates: Mapping[uuid.UUID, int] | None = None,
    token_cost_per_1k: float = 0.003,
) -> DetectionReport:
    """Compute divergence events from an event buffer + classifier verdict.

    Pure function; deterministic on its inputs. No I/O.

    ``token_cost_per_1k`` is a placeholder cost-per-1k-tokens estimate; the
    Unit 5 renderer surfaces the assumption to the user. Calibration in
    later units may parameterise it from the corpus.

    The body is staged into helpers (each ≤ 30 lines): index resolution,
    per-artifact divergence walk, cost extrapolation, report assembly.
    """
    # Short-circuit: classifier already decided the buffer is uninformative.
    if verdict.bucket is Bucket.INSUFFICIENT or not events:
        return _empty_report(
            strict=strict,
            cost_unmeasurable_reason="verdict_insufficient",
        )

    indices = _resolve_indices(verdict=verdict, key_index=key_index)
    walk = _walk_divergence_pairs(
        events=events,
        indices=indices,
        strict=strict,
        value_token_estimates=value_token_estimates,
    )
    cost = _extrapolate_costs(
        events=events,
        headline=walk.headline,
        volume_per_hour=volume_per_hour,
        token_cost_per_1k=token_cost_per_1k,
        value_token_estimates=value_token_estimates,
    )
    return _assemble_detection_report(
        walk=walk,
        cost=cost,
        indices=indices,
        strict=strict,
    )


# -------------------------------------------------------------------- #
# Internals — staged helpers extracted from :func:`detect`
# -------------------------------------------------------------------- #


@dataclass(frozen=True)
class _ResolvedIndices:
    inverse: dict[uuid.UUID, str]
    tracked_uuids: set[uuid.UUID]
    append_only_uuids: set[uuid.UUID]


@dataclass(frozen=True)
class _DivergenceWalk:
    headline: tuple["DivergenceEvent", ...]
    excluded: tuple["DivergenceEvent", ...]
    sequential_staleness_count: int
    cold_start_count: int
    append_only_skip_count: int
    reads_per_artifact: dict[uuid.UUID, list["ReadObservation"]]


@dataclass(frozen=True)
class _CostExtrapolation:
    rework_tokens_this_run: int
    rework_cost_this_run: float
    rework_cost_annualized: float | None
    cost_unmeasurable_reason: str | None


def _resolve_indices(
    *,
    verdict: ClassifierVerdict,
    key_index: Mapping[str, uuid.UUID],
) -> _ResolvedIndices:
    """Resolve the inverse / tracked / append-only UUID sets in one pass."""
    return _ResolvedIndices(
        inverse=_build_inverse_index(key_index),
        tracked_uuids=_resolve_tracked_uuids(verdict, key_index),
        append_only_uuids=_resolve_uuids(verdict.append_only_keys, key_index),
    )


def _walk_divergence_pairs(
    *,
    events: Sequence[DiagnoseEvent],
    indices: _ResolvedIndices,
    strict: bool,
    value_token_estimates: Mapping[uuid.UUID, int] | None,
) -> _DivergenceWalk:
    """Walk reads pairwise per artifact, classifying each candidate.

    Applies the AND clause first, then sequential-staleness and
    cold-start exclusions. Returns the partitioned headline and
    excluded event tuples (sorted) plus the per-bucket counts.
    """
    writes_per_artifact = _collect_writes(events, tracked_uuids=indices.tracked_uuids)
    reads_per_artifact = _collect_reads(events, tracked_uuids=indices.tracked_uuids)

    headline: list[DivergenceEvent] = []
    excluded: list[DivergenceEvent] = []
    append_only_skip_count = 0
    cold_start_count = 0
    sequential_staleness_count = 0

    for aid, reads in reads_per_artifact.items():
        key = indices.inverse.get(aid)
        if key is None:
            continue
        if aid in indices.append_only_uuids:
            append_only_skip_count += _candidate_pair_count(reads)
            continue
        writes = writes_per_artifact.get(aid, [])
        seq_count, cold_count = _classify_pairs_for_artifact(
            aid=aid,
            key=key,
            reads=reads,
            writes=writes,
            strict=strict,
            value_token_estimates=value_token_estimates,
            headline_out=headline,
            excluded_out=excluded,
        )
        sequential_staleness_count += seq_count
        cold_start_count += cold_count

    return _DivergenceWalk(
        headline=tuple(sorted(headline, key=_event_sort_key)),
        excluded=tuple(sorted(excluded, key=_event_sort_key)),
        sequential_staleness_count=sequential_staleness_count,
        cold_start_count=cold_start_count,
        append_only_skip_count=append_only_skip_count,
        reads_per_artifact=reads_per_artifact,
    )


def _classify_pairs_for_artifact(
    *,
    aid: uuid.UUID,
    key: str,
    reads: list["ReadObservation"],
    writes: list[Any],
    strict: bool,
    value_token_estimates: Mapping[uuid.UUID, int] | None,
    headline_out: list["DivergenceEvent"],
    excluded_out: list["DivergenceEvent"],
) -> tuple[int, int]:
    """Walk one artifact's read pairs; append events to the right bucket.

    Returns ``(sequential_staleness_count, cold_start_count)`` for this
    artifact. The headline / excluded lists are mutated in place.
    """
    sequential_staleness_count = 0
    cold_start_count = 0
    for earlier, later in _ordered_pairs(reads):
        if not _and_clause_satisfied(earlier, later):
            continue
        # Sequential-staleness rule (plan): requires a prior write AND
        # at least one intervening write between that prior write and
        # ``later_read``, AND ``later_read.tick - w_prior >= 2``.
        if _is_sequential_staleness(writes, later.tick):
            event = _build_event(
                aid=aid, key=key, earlier=earlier, later=later, writes=writes,
                is_sequential_staleness=True, is_cold_start=False,
                value_token_estimates=value_token_estimates,
            )
            (headline_out if strict else excluded_out).append(event)
            if not strict:
                sequential_staleness_count += 1
            continue
        # Cold-start exclusion: no write at or before ``later_read.tick``.
        if _is_cold_start(writes, later.tick):
            excluded_out.append(_build_event(
                aid=aid, key=key, earlier=earlier, later=later, writes=writes,
                is_sequential_staleness=False, is_cold_start=True,
                value_token_estimates=value_token_estimates,
            ))
            cold_start_count += 1
            continue
        # Textbook divergence: AND clause holds, no exclusion applies.
        headline_out.append(_build_event(
            aid=aid, key=key, earlier=earlier, later=later, writes=writes,
            is_sequential_staleness=False, is_cold_start=False,
            value_token_estimates=value_token_estimates,
        ))
    return sequential_staleness_count, cold_start_count


def _extrapolate_costs(
    *,
    events: Sequence[DiagnoseEvent],
    headline: tuple["DivergenceEvent", ...],
    volume_per_hour: float | None,
    token_cost_per_1k: float,
    value_token_estimates: Mapping[uuid.UUID, int] | None,
) -> _CostExtrapolation:
    """Compute per-run + annualised rework costs from the headline events."""
    rework_tokens_this_run = sum(ev.rework_tokens for ev in headline)
    rework_cost_this_run = rework_tokens_this_run * token_cost_per_1k / 1000.0
    cost_unmeasurable_reason = (
        "value_token_estimates_missing" if value_token_estimates is None else None
    )
    rework_cost_annualized = _annualised_cost(
        rework_tokens_this_run=rework_tokens_this_run,
        observed_tick_count=_observed_tick_count(events),
        volume_per_hour=volume_per_hour,
        token_cost_per_1k=token_cost_per_1k,
    )
    return _CostExtrapolation(
        rework_tokens_this_run=rework_tokens_this_run,
        rework_cost_this_run=rework_cost_this_run,
        rework_cost_annualized=rework_cost_annualized,
        cost_unmeasurable_reason=cost_unmeasurable_reason,
    )


def _assemble_detection_report(
    *,
    walk: _DivergenceWalk,
    cost: _CostExtrapolation,
    indices: _ResolvedIndices,
    strict: bool,
) -> DetectionReport:
    """Build the final :class:`DetectionReport` from the staged outputs."""
    heatmap = _build_heatmap(
        reads_per_artifact=walk.reads_per_artifact,
        headline_events=walk.headline,
        inverse=indices.inverse,
    )
    reader_pair_matrix = _build_reader_pair_matrix(walk.headline)
    top_event = _pick_top_event(walk.headline, heatmap)
    agent_pain_count = len({ev.later_read.node for ev in walk.headline})
    return DetectionReport(
        headline_divergence_events=walk.headline,
        excluded_events=walk.excluded,
        heatmap=heatmap,
        reader_pair_matrix=reader_pair_matrix,
        top_event=top_event,
        exclusion_panel=ExclusionPanel(
            sequential_staleness_count=walk.sequential_staleness_count,
            cold_start_count=walk.cold_start_count,
            append_only_skip_count=walk.append_only_skip_count,
        ),
        agent_pain_count=agent_pain_count,
        rework_tokens_this_run=cost.rework_tokens_this_run,
        rework_cost_this_run=cost.rework_cost_this_run,
        rework_cost_annualized=cost.rework_cost_annualized,
        cost_unmeasurable_reason=cost.cost_unmeasurable_reason,
        strict_mode=strict,
        schema_version=CCS_DIAGNOSE_LOG_SCHEMA_VERSION,
    )


# -------------------------------------------------------------------- #
# Internals — index resolution
# -------------------------------------------------------------------- #


def _build_inverse_index(
    key_index: Mapping[str, uuid.UUID],
) -> dict[uuid.UUID, str]:
    return {aid: name for name, aid in key_index.items()}


def _resolve_tracked_uuids(
    verdict: ClassifierVerdict, key_index: Mapping[str, uuid.UUID]
) -> set[uuid.UUID]:
    return _resolve_uuids(verdict.tracked_keys, key_index)


def _resolve_uuids(
    keys: Sequence[str], key_index: Mapping[str, uuid.UUID]
) -> set[uuid.UUID]:
    return {key_index[k] for k in keys if k in key_index}


# -------------------------------------------------------------------- #
# Internals — write/read collection
# -------------------------------------------------------------------- #


@dataclass(frozen=True)
class _WriteRecord:
    tick: int
    node: str
    version: str
    content_hash: str


def _collect_writes(
    events: Sequence[DiagnoseEvent], *, tracked_uuids: set[uuid.UUID]
) -> dict[uuid.UUID, list[_WriteRecord]]:
    """Per-artifact list of writes ordered by ``(tick, sequence)``.

    Same-content repeat writes are kept (the canonical-writer attribution
    cares about ordering, not deduplication). The append-only classifier
    already decided whether to exclude an artifact entirely.
    """
    out: dict[uuid.UUID, list[_WriteRecord]] = {}
    for ev in events:
        if ev.event_type != "node_end":
            continue
        if not ev.artifact_versions:
            continue
        for aid, version in ev.artifact_versions.items():
            if aid not in tracked_uuids:
                continue
            content_hash = ev.content_hashes.get(aid, "")
            out.setdefault(aid, []).append(
                _WriteRecord(
                    tick=ev.tick,
                    node=ev.node or "",
                    version=version,
                    content_hash=content_hash,
                )
            )
    for aid in out:
        out[aid].sort(key=lambda w: w.tick)
    return out


def _collect_reads(
    events: Sequence[DiagnoseEvent], *, tracked_uuids: set[uuid.UUID]
) -> dict[uuid.UUID, list[ReadObservation]]:
    """Per-artifact list of read observations ordered by tick."""
    out: dict[uuid.UUID, list[ReadObservation]] = {}
    for ev in events:
        if ev.event_type != "node_start":
            continue
        if not ev.artifact_versions:
            continue
        for aid, version in ev.artifact_versions.items():
            if aid not in tracked_uuids:
                continue
            content_hash = ev.content_hashes.get(aid, "")
            out.setdefault(aid, []).append(
                ReadObservation(
                    node=ev.node or "",
                    tick=ev.tick,
                    version=version,
                    content_hash=content_hash,
                )
            )
    for aid in out:
        out[aid].sort(key=lambda r: r.tick)
    return out


# -------------------------------------------------------------------- #
# Internals — divergence detection helpers
# -------------------------------------------------------------------- #


def _ordered_pairs(
    reads: Sequence[ReadObservation],
) -> list[tuple[ReadObservation, ReadObservation]]:
    """All ``(earlier, later)`` read pairs from a tick-ordered list.

    O(n^2) in number of reads per artifact; n is small in v0-preview runs
    (single-process Pregel, < 100 ticks per run). Calibration may add a
    sliding window if real corpora outgrow this.
    """
    pairs: list[tuple[ReadObservation, ReadObservation]] = []
    n = len(reads)
    for i in range(n):
        for j in range(i + 1, n):
            pairs.append((reads[i], reads[j]))
    return pairs


def _and_clause_satisfied(
    earlier: ReadObservation, later: ReadObservation
) -> bool:
    """Reformulated rule: BOTH version and content hash must differ."""
    return (
        earlier.version != later.version
        and earlier.content_hash != later.content_hash
    )


def _is_sequential_staleness(
    writes: Sequence[_WriteRecord], later_tick: int
) -> bool:
    """Plan rule: ``w_prior`` + intervening write with ``later_tick - w_prior >= 2``.

    Requires at least two distinct writes at or before ``later_tick``: a
    "prior" write (``w_prior``) and at least one later "intervening" write
    that came after it but at or before ``later_tick``. The pipeline-y
    expected-staleness signal is the >= 2-tick gap from ``w_prior``.

    With only one write at or before ``later_tick``, this is regular
    divergence (the writer advanced; the lagging reader saw v1 instead of
    v2) — *not* sequential staleness.
    """
    relevant = [w for w in writes if w.tick <= later_tick]
    if len(relevant) < 2:
        return False
    # Distinct ticks only — two writes in the same super-step do not count
    # as a "prior + intervening" sequence. ``relevant`` is sorted by tick.
    distinct_ticks = sorted({w.tick for w in relevant})
    if len(distinct_ticks) < 2:
        return False
    w_prior_tick = distinct_ticks[-2]
    return (later_tick - w_prior_tick) >= 2


def _is_cold_start(
    writes: Sequence[_WriteRecord], later_tick: int
) -> bool:
    """``True`` if no write to this artifact occurred at or before ``later_tick``."""
    return not any(w.tick <= later_tick for w in writes)


def _build_event(
    *,
    aid: uuid.UUID,
    key: str,
    earlier: ReadObservation,
    later: ReadObservation,
    writes: Sequence[_WriteRecord],
    is_sequential_staleness: bool,
    is_cold_start: bool,
    value_token_estimates: Mapping[uuid.UUID, int] | None,
) -> DivergenceEvent:
    canonical = _canonical_writer(writes, later_tick=later.tick)
    rework_tokens = (
        int(value_token_estimates.get(aid, 0))
        if value_token_estimates is not None
        else 0
    )
    return DivergenceEvent(
        artifact_key=key,
        artifact_id=aid,
        earlier_read=earlier,
        later_read=later,
        canonical_writer=canonical[0] if canonical is not None else None,
        canonical_writer_tick=canonical[1] if canonical is not None else None,
        rework_tokens=rework_tokens,
        is_sequential_staleness=is_sequential_staleness,
        is_cold_start=is_cold_start,
    )


def _canonical_writer(
    writes: Sequence[_WriteRecord], *, later_tick: int
) -> tuple[str, int] | None:
    """Most-recent write at or before ``later_tick`` (informational only).

    Witness-quality: this is the writer of the version the stale reader
    did *not* see. Returned as ``None`` when there is no prior write
    (cold-start case).
    """
    candidates = [w for w in writes if w.tick <= later_tick]
    if not candidates:
        return None
    latest = candidates[-1]
    return latest.node, latest.tick


def _candidate_pair_count(reads: Sequence[ReadObservation]) -> int:
    n = len(reads)
    return n * (n - 1) // 2


# -------------------------------------------------------------------- #
# Internals — derived report fields
# -------------------------------------------------------------------- #


def _build_heatmap(
    *,
    reads_per_artifact: Mapping[uuid.UUID, list[ReadObservation]],
    headline_events: Sequence[DivergenceEvent],
    inverse: Mapping[uuid.UUID, str],
) -> tuple[HeatmapRow, ...]:
    # Count DISTINCT reads handed a divergent version (the ``later_read`` of
    # ≥1 headline event), NOT the number of headline events. Events are
    # ordered read pairs (O(n^2)); counting them would let ``divergent_reads``
    # exceed ``total_reads`` and overflow the report's "share" bar past 100%.
    # Key on object identity, not value: every ``later_read`` instance
    # originates from ``reads_per_artifact`` and is alive for this call, so
    # ``id()`` shares the positional basis of ``total_reads = len(reads)`` and
    # guarantees ``divergent_reads <= total_reads``.
    divergent_read_ids: dict[uuid.UUID, set[int]] = {}
    for ev in headline_events:
        divergent_read_ids.setdefault(ev.artifact_id, set()).add(id(ev.later_read))

    rows: list[HeatmapRow] = []
    for aid, read_ids in divergent_read_ids.items():
        key = inverse.get(aid)
        if key is None:
            continue
        rows.append(
            HeatmapRow(
                artifact_key=key,
                artifact_id=aid,
                divergent_reads=len(read_ids),
                total_reads=len(reads_per_artifact.get(aid, [])),
            )
        )
    rows.sort(key=lambda r: (-r.divergent_reads, r.artifact_key))
    return tuple(rows)


def _build_reader_pair_matrix(
    headline_events: Sequence[DivergenceEvent],
) -> tuple[ReaderPairCount, ...]:
    counts: dict[tuple[str, str], int] = {}
    for ev in headline_events:
        pair = (ev.earlier_read.node, ev.later_read.node)
        counts[pair] = counts.get(pair, 0) + 1
    pairs = [
        ReaderPairCount(
            earlier_reader=earlier, later_reader=later, event_count=count
        )
        for (earlier, later), count in counts.items()
    ]
    pairs.sort(key=lambda p: (-p.event_count, p.earlier_reader, p.later_reader))
    return tuple(pairs)


def _pick_top_event(
    headline_events: Sequence[DivergenceEvent],
    heatmap: Sequence[HeatmapRow],
) -> DivergenceEvent | None:
    """Pick the headline event with the highest impact.

    Tie-breakers (in order):

    1. Highest ``rework_tokens``.
    2. Artifact appearing earliest in the heatmap (most divergent reads).
    3. Smallest ``later_read.tick``.
    4. Smallest ``earlier_read.tick``.
    5. ``artifact_key`` lexicographic.
    """
    if not headline_events:
        return None

    heatmap_rank = {row.artifact_id: rank for rank, row in enumerate(heatmap)}

    def score(ev: DivergenceEvent) -> tuple[int, int, int, int, str]:
        return (
            -ev.rework_tokens,
            heatmap_rank.get(ev.artifact_id, len(heatmap_rank)),
            ev.later_read.tick,
            ev.earlier_read.tick,
            ev.artifact_key,
        )

    return min(headline_events, key=score)


# -------------------------------------------------------------------- #
# Internals — cost extrapolation
# -------------------------------------------------------------------- #


def _observed_tick_count(events: Sequence[DiagnoseEvent]) -> int:
    """Distinct super-step ticks observed (real ticks only — ``-1`` ignored)."""
    return len({ev.tick for ev in events if ev.tick >= 0})


def _annualised_cost(
    *,
    rework_tokens_this_run: int,
    observed_tick_count: int,
    volume_per_hour: float | None,
    token_cost_per_1k: float,
) -> float | None:
    """Compute USD/year extrapolation per the plan's formula.

    Returns ``None`` when ``volume_per_hour`` is omitted; Unit 5 will
    render a static fallback message in that case.
    """
    if volume_per_hour is None:
        return None
    denom = max(1, observed_tick_count)
    return (
        rework_tokens_this_run
        / denom
        * volume_per_hour
        * _HOURS_PER_YEAR
        * token_cost_per_1k
        / 1000.0
    )


# -------------------------------------------------------------------- #
# Internals — sort/empty helpers
# -------------------------------------------------------------------- #


def _event_sort_key(
    ev: DivergenceEvent,
) -> tuple[int, int, str, str, str]:
    return (
        ev.later_read.tick,
        ev.earlier_read.tick,
        ev.artifact_key,
        ev.earlier_read.node,
        ev.later_read.node,
    )


def _empty_report(
    *, strict: bool, cost_unmeasurable_reason: str | None
) -> DetectionReport:
    return DetectionReport(
        headline_divergence_events=(),
        excluded_events=(),
        heatmap=(),
        reader_pair_matrix=(),
        top_event=None,
        exclusion_panel=ExclusionPanel(
            sequential_staleness_count=0,
            cold_start_count=0,
            append_only_skip_count=0,
        ),
        agent_pain_count=0,
        rework_tokens_this_run=0,
        rework_cost_this_run=0.0,
        rework_cost_annualized=None,
        cost_unmeasurable_reason=cost_unmeasurable_reason,
        strict_mode=strict,
        schema_version=CCS_DIAGNOSE_LOG_SCHEMA_VERSION,
    )


# -------------------------------------------------------------------- #
# Public report-JSON serialization primitive
# -------------------------------------------------------------------- #


def build_report_json(
    verdict: ClassifierVerdict, report: DetectionReport
) -> dict[str, Any]:
    """Construct the ``report.json`` payload from a verdict + report.

    Returns a dict shaped like::

        {
            "schema_version": "ccs.diagnose.v0-preview",
            "verdict": {...},
            "report": {...},
        }

    Round-trips cleanly through :func:`json.dumps` with ``default=str`` to
    coerce any nested ``UUID`` / ``Path`` / enum values that survive
    :func:`dataclasses.asdict`. The wrapping payload's ``schema_version``
    is the canonical one — the nested ``report`` block has its own copy
    stripped so downstream tools don't have to choose which to trust.

    This is the public primitive behind ``ccs-diagnose --output-json``;
    extracted from the CLI so other surfaces (programmatic adapters,
    test harnesses, future v1 endpoints) can reuse it without dragging
    in argparse.
    """
    return {
        "schema_version": CCS_DIAGNOSE_LOG_SCHEMA_VERSION,
        "verdict": _verdict_to_dict(verdict),
        "report": _report_to_dict(report),
    }


def _verdict_to_dict(verdict: ClassifierVerdict) -> dict[str, Any]:
    raw = asdict(verdict)
    raw["bucket"] = verdict.bucket.value
    raw["confidence"] = verdict.confidence.value
    raw["coverage"]["verdict_confidence"] = verdict.coverage.verdict_confidence.value
    # ``writers_by_key`` keys are str already; coerce values to lists.
    raw["writers_by_key"] = {
        k: list(v) for k, v in verdict.writers_by_key.items()
    }
    return raw


def _report_to_dict(report: DetectionReport) -> dict[str, Any]:
    raw = asdict(report)
    # The wrapping payload already declares ``schema_version`` at the top
    # level. Strip the nested copy so callers don't have to special-case
    # which one to trust.
    raw.pop("schema_version", None)
    return raw
