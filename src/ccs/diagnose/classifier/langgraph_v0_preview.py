# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Write-pattern classifier for LangGraph runs (``langgraph-v0-preview``).

Consumes a ``DiagnoseEvent`` buffer produced by :class:`ccs.diagnose.callback.DiagnoseCallback`
and produces a :class:`ClassifierVerdict` that summarises the run's *write
pattern*. Five buckets are emitted: ``single_writer``, ``shared_artifact``,
``parallel_branch``, ``mixed_pattern``, ``insufficient``. Confidence is
qualified independently from the bucket via coverage thresholds.

Witness-quality framing
=======================

The classifier observes only what the LangGraph callback could see —
specifically the merged state passed to a node (read view) and the dict it
returned (write view). It does **not** prove which keys the node actually
read. ``ClassifierVerdict`` is therefore a *write-pattern* verdict; the
naming preserves intent across the codebase.

Coverage thresholds (v0)
========================

* ``high`` — ``tick_count >= 50`` AND ``read_count >= 100`` AND ``write_count >= 5``
* ``insufficient`` — ``tick_count < 10`` OR ``read_count < 10`` OR ``write_count == 0``
* ``preliminary`` — anything in between

These are starting points; calibration in later units will tighten them.

Append-only detection
=====================

For names matching ``messages``, ``*_log`` or ``*_history`` (case-sensitive)
the classifier compares the **set of message IDs** across super-steps. A
monotonically growing ID set classifies the artifact as ``append_only``;
if the set ever shrinks (e.g. ``trim_messages`` middleware or
``RemoveMessage``) the artifact is reclassified to ``mutable`` — *not* a
prefix-break alarm. Same-ID content updates (same id, new content hash)
are legitimate and do not affect classification.

Key index
=========

``DiagnoseEvent`` instances carry ``artifact_versions`` and
``content_hashes`` keyed by ``UUID`` (via
:func:`ccs.core.identity.artifact_uuid`). The classifier needs the original
string key name to apply ignore rules (``__*`` prefix, ephemera list,
overrides). Callers supply a ``key_index`` mapping name → UUID; tests
build it directly, the CLI builds it from the merged state schema. When
``key_index`` is omitted the classifier still runs but cannot apply any
ignore rule beyond the ``unsupported_execution_model`` short-circuit and
the coverage gate, returning ``insufficient`` if no keys can be resolved.
"""

from __future__ import annotations

import uuid
import warnings
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from ccs.core.identity import artifact_uuid
from ccs.diagnose.callback import DEFAULT_SCOPE, DiagnoseEvent, DiagnoseWarning

__all__ = [
    "Bucket",
    "Confidence",
    "CoverageReport",
    "ClassifierVerdict",
    "ClassifierOverrides",
    "classify",
    "build_key_index",
    "DEFAULT_EPHEMERA_KEYS",
    "KNOWN_FRAMEWORK_KEYS",
    "APPEND_ONLY_NAME_SUFFIXES",
]


# -------------------------------------------------------------------- #
# Public enums
# -------------------------------------------------------------------- #


class Bucket(str, Enum):
    """Write-pattern verdict buckets emitted by the v0-preview classifier.

    NOTE(v1): The string values for this enum mix space-separated
    (``"single_writer per artifact"``, ``"mixed pattern"``) and
    underscore-only (``"shared_artifact"``, ``"parallel_branch"``)
    forms. The langgraph-v1 promotion will normalise every bucket to
    underscore-only (e.g. ``"single_writer_per_artifact"``,
    ``"mixed_pattern"``) so values are jq-friendly and consistent.
    Until then, v0-preview consumers must NOT rely on the literal
    space-containing strings — use the enum members directly
    (``Bucket.SINGLE_WRITER``) and the bucket-display lookup in
    ``ccs.diagnose._labels`` for human-readable copy.
    """

    SINGLE_WRITER = "single_writer per artifact"
    SHARED_ARTIFACT = "shared_artifact"
    PARALLEL_BRANCH = "parallel_branch"
    MIXED_PATTERN = "mixed pattern"
    INSUFFICIENT = "insufficient"


class Confidence(str, Enum):
    """Confidence qualifier driven by coverage thresholds."""

    HIGH = "high"
    PRELIMINARY = "preliminary"
    INSUFFICIENT = "insufficient"


# -------------------------------------------------------------------- #
# Constants
# -------------------------------------------------------------------- #


DEFAULT_EPHEMERA_KEYS: frozenset[str] = frozenset(
    {"errors", "retries", "attempt_count", "_step_count"}
)
"""Top-level state keys ignored as ephemera in v0.

These keys carry per-run telemetry that the classifier should not
attribute as artifact writes (they would otherwise inflate ``shared_artifact``
verdicts spuriously).
"""

KNOWN_FRAMEWORK_KEYS: frozenset[str] = frozenset(
    {
        "__interrupt__",
        "__checkpoint_ns__",
        "__error__",
        "__metadata__",
    }
)
"""Concrete ``__*`` keys observed across LangGraph 0.2–0.7.

Any other ``__*`` key is also ignored as framework state but additionally
surfaced via :attr:`ClassifierVerdict.unknown_underscore_keys` so the
calibration corpus learns about new internals as they appear.
"""

_KNOWN_FRAMEWORK_PREFIXES: tuple[str, ...] = ("__pregel_",)
"""``__*`` *prefix* groups treated as known framework state."""


APPEND_ONLY_NAME_SUFFIXES: tuple[str, ...] = ("_log", "_history")
"""Suffixes whose values are checked for monotonic message-ID growth."""

_APPEND_ONLY_EXACT_NAMES: frozenset[str] = frozenset({"messages"})
"""Exact key names checked for monotonic message-ID growth."""


# -------------------------------------------------------------------- #
# Public dataclasses
# -------------------------------------------------------------------- #


@dataclass(frozen=True)
class ClassifierOverrides:
    """User-supplied overrides applied before verdict computation.

    ``ignore`` keys are unconditionally added to the ignored set even if no
    rule matches. ``track`` keys are unconditionally removed from the ignored
    set, even when matched by the ``__*`` prefix rule.

    Precedence: when a key appears in both ``ignore`` and ``track``, ``track``
    wins — the user is explicitly opting in to observation despite their own
    earlier ``ignore`` directive.
    """

    ignore: tuple[str, ...] = ()
    track: tuple[str, ...] = ()


@dataclass(frozen=True)
class CoverageReport:
    """Coverage statistics for the observed event buffer."""

    tick_count: int
    read_count: int
    write_count: int
    artifact_count: int
    # NOTE(v1): this field duplicates ``ClassifierVerdict.confidence``
    # and will be removed in the langgraph-v1 promotion. v0-preview
    # consumers should read confidence from the parent
    # ``ClassifierVerdict`` directly. Keeping the field for now to
    # preserve the existing JSON shape until the v1 schema bump.
    verdict_confidence: Confidence


@dataclass(frozen=True)
class ClassifierVerdict:
    """Frozen verdict consumed by the renderer / terminal summary stages."""

    bucket: Bucket
    confidence: Confidence
    coverage: CoverageReport
    tracked_keys: tuple[str, ...] = ()
    ignored_framework_keys: tuple[str, ...] = ()
    ignored_ephemera_keys: tuple[str, ...] = ()
    append_only_keys: tuple[str, ...] = ()
    mutable_keys: tuple[str, ...] = ()
    unknown_underscore_keys: tuple[str, ...] = ()
    reason: str | None = None
    writers_by_key: Mapping[str, tuple[str, ...]] = field(default_factory=dict)


# -------------------------------------------------------------------- #
# Public helpers
# -------------------------------------------------------------------- #


def build_key_index(
    keys: Sequence[str], *, scope: str = DEFAULT_SCOPE
) -> dict[str, uuid.UUID]:
    """Build the ``name → UUID`` index used by :func:`classify`.

    Mirrors the derivation in :class:`ccs.diagnose.callback.DiagnoseCallback`.
    """
    return {key: artifact_uuid(scope, key) for key in keys}


# -------------------------------------------------------------------- #
# Public entry point
# -------------------------------------------------------------------- #


def classify(
    events: Sequence[DiagnoseEvent],
    *,
    overrides: ClassifierOverrides | None = None,
    key_index: Mapping[str, uuid.UUID] | None = None,
) -> ClassifierVerdict:
    """Classify a ``DiagnoseEvent`` buffer into a :class:`ClassifierVerdict`.

    Pure function; deterministic on ``(events, overrides, key_index)``.
    No I/O.

    ``key_index`` maps top-level state-key names to the UUID computed by
    :func:`ccs.core.identity.artifact_uuid`. When omitted the classifier
    derives it from ``overrides`` (``track`` + ``ignore`` names) — the CLI
    will populate the index from the merged state schema; tests pass it
    directly. Without an index the classifier returns ``insufficient`` with
    ``reason='below coverage threshold'`` because no key can be attributed.

    The body delegates to staged helpers (each ≤ 30 lines):
    short-circuit on unsupported execution model, partition keys
    (apply ignore rules), analyse append-only patterns, compute the
    coverage/confidence qualifier, then pick a bucket.
    """
    overrides = overrides or ClassifierOverrides()

    # 1. Hard short-circuit: any unsupported execution-model signal wins.
    unsupported = _short_circuit_unsupported(events)
    if unsupported is not None:
        return unsupported

    # 2. Resolve the universe of keys, partition them, and analyse
    # append-only structure (steps 2-4 of the plan).
    analysis = _build_classification_analysis(
        events=events, overrides=overrides, key_index=key_index
    )

    if analysis.confidence is Confidence.INSUFFICIENT:
        return _insufficient_verdict_from_analysis(
            analysis, reason="below coverage threshold"
        )

    # 6-7. Pick a bucket and enforce single-writer consistency.
    writers_by_key = _writers_by_key(
        events, tracked_keys=analysis.tracked, key_index=analysis.resolved_index
    )
    bucket = _pick_bucket(
        events=events,
        writers_by_key=writers_by_key,
        confidence=analysis.confidence,
        key_index=analysis.resolved_index,
    )
    bucket = _enforce_single_writer_consistency(bucket, writers_by_key)
    return _build_verdict(
        analysis=analysis, bucket=bucket, writers_by_key=writers_by_key
    )


# -------------------------------------------------------------------- #
# Internals — staged helpers extracted from :func:`classify`
# -------------------------------------------------------------------- #


@dataclass(frozen=True)
class _ClassificationAnalysis:
    """Intermediate state shared across the classify pipeline stages."""

    resolved_index: Mapping[str, uuid.UUID]
    partition: "_KeyPartition"
    tracked: frozenset[str]
    append_only: frozenset[str]
    mutable: frozenset[str]
    coverage_stats: "_CoverageStats"
    confidence: Confidence


def _short_circuit_unsupported(
    events: Sequence[DiagnoseEvent],
) -> ClassifierVerdict | None:
    """Return an insufficient verdict when an unsupported execution-model
    signal is present, else ``None`` to let normal classification continue."""
    unsupported_signal = _first_unsupported_execution_signal(events)
    if unsupported_signal is None:
        return None
    return _insufficient_verdict(
        reason=unsupported_signal,
        tracked_keys=(),
        ignored_framework_keys=(),
        ignored_ephemera_keys=(),
        append_only_keys=(),
        mutable_keys=(),
        unknown_underscore_keys=(),
        tick_count=_distinct_tick_count(events),
        read_count=0,
        write_count=0,
    )


def _build_classification_analysis(
    *,
    events: Sequence[DiagnoseEvent],
    overrides: ClassifierOverrides,
    key_index: Mapping[str, uuid.UUID] | None,
) -> _ClassificationAnalysis:
    """Run the deterministic pre-bucket pipeline (steps 2-5 of the plan)."""
    resolved_index = _resolve_key_index(
        events=events, overrides=overrides, key_index=key_index
    )
    partition = _partition_keys(frozenset(resolved_index), overrides=overrides)
    tracked = partition.tracked
    append_only, mutable = _analyse_append_only(
        events, tracked_keys=tracked, key_index=resolved_index
    )
    coverage_stats = _coverage(
        events, tracked_keys=tracked, key_index=resolved_index
    )
    confidence = _confidence_from_coverage(
        tick_count=coverage_stats.tick_count,
        read_count=coverage_stats.read_count,
        write_count=coverage_stats.write_count,
    )
    return _ClassificationAnalysis(
        resolved_index=resolved_index,
        partition=partition,
        tracked=tracked,
        append_only=append_only,
        mutable=mutable,
        coverage_stats=coverage_stats,
        confidence=confidence,
    )


def _insufficient_verdict_from_analysis(
    analysis: _ClassificationAnalysis, *, reason: str
) -> ClassifierVerdict:
    """Build the ``insufficient`` verdict from an in-flight analysis."""
    return _insufficient_verdict(
        reason=reason,
        tracked_keys=tuple(sorted(analysis.tracked)),
        ignored_framework_keys=tuple(sorted(analysis.partition.framework)),
        ignored_ephemera_keys=tuple(sorted(analysis.partition.ephemera)),
        append_only_keys=tuple(sorted(analysis.append_only)),
        mutable_keys=tuple(sorted(analysis.mutable)),
        unknown_underscore_keys=tuple(sorted(analysis.partition.unknown_underscore)),
        tick_count=analysis.coverage_stats.tick_count,
        read_count=analysis.coverage_stats.read_count,
        write_count=analysis.coverage_stats.write_count,
    )


def _build_verdict(
    *,
    analysis: _ClassificationAnalysis,
    bucket: Bucket,
    writers_by_key: Mapping[str, set[str]],
) -> ClassifierVerdict:
    """Assemble the final :class:`ClassifierVerdict`."""
    return ClassifierVerdict(
        bucket=bucket,
        confidence=analysis.confidence,
        coverage=CoverageReport(
            tick_count=analysis.coverage_stats.tick_count,
            read_count=analysis.coverage_stats.read_count,
            write_count=analysis.coverage_stats.write_count,
            artifact_count=len(analysis.tracked),
            verdict_confidence=analysis.confidence,
        ),
        tracked_keys=tuple(sorted(analysis.tracked)),
        ignored_framework_keys=tuple(sorted(analysis.partition.framework)),
        ignored_ephemera_keys=tuple(sorted(analysis.partition.ephemera)),
        append_only_keys=tuple(sorted(analysis.append_only)),
        mutable_keys=tuple(sorted(analysis.mutable)),
        unknown_underscore_keys=tuple(sorted(analysis.partition.unknown_underscore)),
        writers_by_key={
            key: tuple(sorted(writers_by_key[key])) for key in sorted(writers_by_key)
        },
    )


# -------------------------------------------------------------------- #
# Internals — key resolution
# -------------------------------------------------------------------- #


def _resolve_key_index(
    *,
    events: Sequence[DiagnoseEvent],
    overrides: ClassifierOverrides,
    key_index: Mapping[str, uuid.UUID] | None,
) -> dict[str, uuid.UUID]:
    """Return a ``name → UUID`` mapping spanning every observable key.

    Strategy: start from the explicit ``key_index`` if supplied, then
    augment with any name surfaced via ``overrides`` (``track`` and
    ``ignore``). Keys whose UUIDs do not appear in any event are dropped,
    matching the "observed only" framing.
    """
    candidate: dict[str, uuid.UUID] = {}
    if key_index:
        candidate.update(key_index)
    for name in (*overrides.track, *overrides.ignore):
        candidate.setdefault(name, artifact_uuid(DEFAULT_SCOPE, name))

    observed_uuids = _observed_artifact_uuids(events)
    return {name: aid for name, aid in candidate.items() if aid in observed_uuids}


def _observed_artifact_uuids(events: Sequence[DiagnoseEvent]) -> set[uuid.UUID]:
    seen: set[uuid.UUID] = set()
    for ev in events:
        seen.update(ev.artifact_versions)
        seen.update(ev.content_hashes)
    return seen


# -------------------------------------------------------------------- #
# Internals — key partitioning
# -------------------------------------------------------------------- #


@dataclass(frozen=True)
class _KeyPartition:
    tracked: frozenset[str]
    framework: frozenset[str]
    ephemera: frozenset[str]
    unknown_underscore: frozenset[str]


def _partition_keys(
    all_keys: frozenset[str], *, overrides: ClassifierOverrides
) -> _KeyPartition:
    """Partition observed keys into tracked / framework / ephemera buckets."""
    track_set = set(overrides.track)
    ignore_set = set(overrides.ignore) - track_set  # ``track`` wins.

    tracked: set[str] = set()
    framework: set[str] = set()
    ephemera: set[str] = set()
    unknown_underscore: set[str] = set()

    for key in all_keys:
        if key in track_set:
            tracked.add(key)
            # If the user opted in to a __*-prefixed key, still surface it
            # as a staleness sensor so the calibration corpus sees it.
            if _is_framework_key(key) and not _is_known_framework_key(key):
                unknown_underscore.add(key)
            continue
        if key in ignore_set:
            if _is_framework_key(key):
                framework.add(key)
                if not _is_known_framework_key(key):
                    unknown_underscore.add(key)
            else:
                ephemera.add(key)
            continue
        if _is_framework_key(key):
            framework.add(key)
            if not _is_known_framework_key(key):
                unknown_underscore.add(key)
            continue
        if key in DEFAULT_EPHEMERA_KEYS:
            ephemera.add(key)
            continue
        tracked.add(key)

    return _KeyPartition(
        tracked=frozenset(tracked),
        framework=frozenset(framework),
        ephemera=frozenset(ephemera),
        unknown_underscore=frozenset(unknown_underscore),
    )


def _is_framework_key(key: str) -> bool:
    return key.startswith("__")


def _is_known_framework_key(key: str) -> bool:
    if key in KNOWN_FRAMEWORK_KEYS:
        return True
    return any(key.startswith(prefix) for prefix in _KNOWN_FRAMEWORK_PREFIXES)


# -------------------------------------------------------------------- #
# Internals — append-only detection
# -------------------------------------------------------------------- #


def _is_append_only_candidate(key: str) -> bool:
    if key in _APPEND_ONLY_EXACT_NAMES:
        return True
    return any(key.endswith(suffix) for suffix in APPEND_ONLY_NAME_SUFFIXES)


def _extract_message_ids(value: Any) -> frozenset[str] | None:
    """Return the set of message IDs in ``value``, or ``None`` if not list-of-IDs.

    Accepts a list/tuple of objects exposing an ``id`` or ``message_id``
    attribute, dicts with the same keys, or LangChain ``BaseMessage``-like
    objects. Anything else (raw strings, scalars, dicts-of-non-messages)
    fails the check and signals to the caller that the artifact cannot be
    treated as append-only.
    """
    if not isinstance(value, (list, tuple)):
        return None
    ids: set[str] = set()
    for item in value:
        item_id = _message_id_of(item)
        if item_id is None:
            return None
        ids.add(item_id)
    return frozenset(ids)


def _message_id_of(item: Any) -> str | None:
    if isinstance(item, Mapping):
        for field_name in ("id", "message_id"):
            if field_name in item and item[field_name] is not None:
                return str(item[field_name])
        return None
    for attr in ("id", "message_id"):
        candidate = getattr(item, attr, None)
        if candidate is not None:
            return str(candidate)
    return None


def _analyse_append_only(
    events: Sequence[DiagnoseEvent],
    *,
    tracked_keys: frozenset[str],
    key_index: Mapping[str, uuid.UUID],
) -> tuple[frozenset[str], frozenset[str]]:
    """Classify tracked candidate keys as ``append_only`` or ``mutable``.

    The append-only signal is computed from successive ``content_hashes``
    on ``node_end`` events for the artifact. Each new write produces a
    distinct hash; a strictly-growing-distinct-hash sequence signals
    monotonic ID-set growth (the append-only property). A repeat of an
    earlier non-immediately-prior hash signals an ID-set churn (shrink
    then regrow) which downgrades the artifact to ``mutable``. Repeated
    immediate hashes (no-op writes) are tolerated.

    Note: the v0-preview signal is content-hash based; calibration in
    later units will tighten this with the ``message_ids_per_tick`` overlay
    once the callback exposes it.
    """
    candidates = {key for key in tracked_keys if _is_append_only_candidate(key)}
    if not candidates:
        return frozenset(), frozenset()

    append_only: set[str] = set()
    mutable: set[str] = set()

    for key in candidates:
        aid = key_index.get(key)
        if aid is None:
            mutable.add(key)
            continue
        timeline = _value_hash_timeline(events, aid=aid)
        if not timeline:
            # Observed only as a read-only key — cannot prove append-only.
            mutable.add(key)
            continue
        if _is_monotonic_distinct(timeline):
            append_only.add(key)
        else:
            mutable.add(key)

    return frozenset(append_only), frozenset(mutable)


def _value_hash_timeline(
    events: Sequence[DiagnoseEvent], *, aid: uuid.UUID
) -> list[str]:
    """Return the ordered list of content hashes for ``aid`` across writes."""
    timeline: list[str] = []
    for ev in events:
        if ev.event_type != "node_end":
            continue
        content_hash = ev.content_hashes.get(aid)
        if content_hash is not None:
            timeline.append(content_hash)
    return timeline


def _is_monotonic_distinct(hashes: list[str]) -> bool:
    """``True`` if every hash is distinct from all earlier hashes.

    Repeated identical hashes (no-op writes) are tolerated — they imply the
    underlying value did not change. A re-emergence of an earlier hash that
    has since been superseded counts as a shrink-and-regrow and fails the
    check.
    """
    seen: set[str] = set()
    last_hash: str | None = None
    for h in hashes:
        if h == last_hash:
            # Immediate repeat — no-op write, tolerated.
            continue
        if h in seen:
            # Earlier hash reappeared after a different hash — churn.
            return False
        seen.add(h)
        last_hash = h
    return True


# -------------------------------------------------------------------- #
# Internals — coverage
# -------------------------------------------------------------------- #


@dataclass(frozen=True)
class _CoverageStats:
    tick_count: int
    read_count: int
    write_count: int


def _coverage(
    events: Sequence[DiagnoseEvent],
    *,
    tracked_keys: frozenset[str],
    key_index: Mapping[str, uuid.UUID],
) -> _CoverageStats:
    distinct_ticks: set[int] = set()
    read_count = 0
    write_count = 0

    tracked_uuids = {key_index[k] for k in tracked_keys if k in key_index}

    for ev in events:
        if ev.event_type not in ("node_start", "node_end"):
            continue
        if ev.tick >= 0:
            distinct_ticks.add(ev.tick)
        if ev.event_type == "node_start" and ev.artifact_versions:
            if any(aid in tracked_uuids for aid in ev.artifact_versions):
                read_count += 1
        if ev.event_type == "node_end" and ev.artifact_versions:
            if any(aid in tracked_uuids for aid in ev.artifact_versions):
                write_count += 1

    return _CoverageStats(
        tick_count=len(distinct_ticks),
        read_count=read_count,
        write_count=write_count,
    )


def _confidence_from_coverage(
    *, tick_count: int, read_count: int, write_count: int
) -> Confidence:
    if tick_count < 10 or read_count < 10 or write_count == 0:
        return Confidence.INSUFFICIENT
    if tick_count >= 50 and read_count >= 100 and write_count >= 5:
        return Confidence.HIGH
    return Confidence.PRELIMINARY


def _distinct_tick_count(events: Sequence[DiagnoseEvent]) -> int:
    return len({ev.tick for ev in events if ev.tick >= 0})


# -------------------------------------------------------------------- #
# Internals — bucket selection
# -------------------------------------------------------------------- #


def _writers_by_key(
    events: Sequence[DiagnoseEvent],
    *,
    tracked_keys: frozenset[str],
    key_index: Mapping[str, uuid.UUID],
) -> dict[str, set[str]]:
    """Map tracked key → set of node names that wrote to it."""
    inverse = {key_index[k]: k for k in tracked_keys if k in key_index}
    writers: dict[str, set[str]] = {key: set() for key in tracked_keys}
    for ev in events:
        if ev.event_type != "node_end" or not ev.artifact_versions:
            continue
        for aid in ev.artifact_versions:
            key = inverse.get(aid)
            if key is None or not ev.node:
                continue
            writers[key].add(ev.node)
    return writers


def _pick_bucket(
    *,
    events: Sequence[DiagnoseEvent],
    writers_by_key: Mapping[str, set[str]],
    confidence: Confidence,
    key_index: Mapping[str, uuid.UUID],
) -> Bucket:
    if not writers_by_key:
        # No tracked artifact had any writer at all but coverage cleared the
        # threshold (e.g. tracked-key was only ever read). Returning
        # ``SINGLE_WRITER`` here is vacuous-but-correct: zero writers
        # cannot disagree, and the consistency pass leaves it alone.
        return Bucket.SINGLE_WRITER

    multi_writer_keys = [k for k, w in writers_by_key.items() if len(w) >= 2]
    single_writer_keys = [k for k, w in writers_by_key.items() if len(w) == 1]

    if not multi_writer_keys:
        return Bucket.SINGLE_WRITER

    if multi_writer_keys and single_writer_keys:
        # Mixed pattern requires coverage ≥ preliminary per the decision
        # matrix; ``insufficient`` was filtered earlier so any non-insufficient
        # confidence qualifies.
        return Bucket.MIXED_PATTERN

    # All-multi-writer — could be parallel branch or shared.
    if _looks_like_parallel_branch(
        events=events,
        multi_writer_keys=multi_writer_keys,
        key_index=key_index,
    ):
        return Bucket.PARALLEL_BRANCH
    return Bucket.SHARED_ARTIFACT


def _looks_like_parallel_branch(
    *,
    events: Sequence[DiagnoseEvent],
    multi_writer_keys: list[str],
    key_index: Mapping[str, uuid.UUID],
) -> bool:
    """v0-preview parallel-branch heuristic.

    Conditions (all must hold):

    1. At least one tracked key has 2+ writers.
    2. For every multi-writer key, every distinct writer's ``node_end``
       fires at the *same* ``tick`` value (a single super-step window) —
       i.e. the writers run in parallel within one super-step.
    3. At least one downstream node ``node_start`` reads two or more of
       the multi-writer keys together (the merge consumer).

    If any condition fails the heuristic falls through to ``shared_artifact``.
    Calibration in later units will tighten this signal.
    """
    if not multi_writer_keys:
        return False

    multi_uuids = {key_index[k] for k in multi_writer_keys if k in key_index}

    # Condition 2: same-tick co-writers for every multi-writer key.
    for key in multi_writer_keys:
        aid = key_index.get(key)
        if aid is None:
            return False
        ticks_per_writer: dict[str, set[int]] = {}
        for ev in events:
            if ev.event_type != "node_end" or not ev.node:
                continue
            if aid in ev.artifact_versions:
                ticks_per_writer.setdefault(ev.node, set()).add(ev.tick)
        if len(ticks_per_writer) < 2:
            return False
        common = set.intersection(*ticks_per_writer.values())
        if not common:
            return False

    # Condition 3: a downstream consumer reads ≥2 multi-writer keys together.
    for ev in events:
        if ev.event_type != "node_start":
            continue
        consumed = sum(1 for aid in ev.artifact_versions if aid in multi_uuids)
        if consumed >= 2:
            return True
    return False


def _enforce_single_writer_consistency(
    bucket: Bucket, writers_by_key: Mapping[str, set[str]]
) -> Bucket:
    """Downgrade ``SINGLE_WRITER`` to ``MIXED_PATTERN`` if any key disagrees.

    The plan calls this the "Artifact Ownership Map" consistency check.
    Enforcing it here keeps the verdict internally consistent at the source
    rather than the renderer second-guessing the classifier.
    """
    if bucket is not Bucket.SINGLE_WRITER:
        return bucket
    offenders = [k for k, writers in writers_by_key.items() if len(writers) >= 2]
    if not offenders:
        return bucket
    warnings.warn(
        "single_writer verdict downgraded to mixed_pattern: "
        f"{sorted(offenders)} have multiple writers.",
        DiagnoseWarning,
        stacklevel=2,
    )
    return Bucket.MIXED_PATTERN


# -------------------------------------------------------------------- #
# Internals — utility
# -------------------------------------------------------------------- #


# Verdict signals that short-circuit classification to ``insufficient``.
# Mirrors ``RunVerdictSignal`` in ``ccs.diagnose.callback`` — kept as a
# module-level frozenset so the predicate is a fast membership check and
# the contract is documented in one place.
_UNSUPPORTED_EXECUTION_SIGNALS: frozenset[str] = frozenset(
    {
        "unsupported_execution_model",
        "subgraph_observed",
        "remote_graph_attached",
    }
)


def _first_unsupported_execution_signal(
    events: Sequence[DiagnoseEvent],
) -> str | None:
    """Return the first verdict-signal event that disqualifies classification.

    Returns the ``verdict_signal`` string (used as the verdict ``reason``) or
    ``None`` if no disqualifying signal is present.
    """
    for ev in events:
        if (
            ev.event_type == "verdict_signal"
            and ev.verdict_signal in _UNSUPPORTED_EXECUTION_SIGNALS
        ):
            return ev.verdict_signal
    return None


def _insufficient_verdict(
    *,
    reason: str,
    tracked_keys: tuple[str, ...],
    ignored_framework_keys: tuple[str, ...],
    ignored_ephemera_keys: tuple[str, ...],
    append_only_keys: tuple[str, ...],
    mutable_keys: tuple[str, ...],
    unknown_underscore_keys: tuple[str, ...],
    tick_count: int,
    read_count: int,
    write_count: int,
) -> ClassifierVerdict:
    confidence = Confidence.INSUFFICIENT
    return ClassifierVerdict(
        bucket=Bucket.INSUFFICIENT,
        confidence=confidence,
        coverage=CoverageReport(
            tick_count=tick_count,
            read_count=read_count,
            write_count=write_count,
            artifact_count=len(tracked_keys),
            verdict_confidence=confidence,
        ),
        tracked_keys=tracked_keys,
        ignored_framework_keys=ignored_framework_keys,
        ignored_ephemera_keys=ignored_ephemera_keys,
        append_only_keys=append_only_keys,
        mutable_keys=mutable_keys,
        unknown_underscore_keys=unknown_underscore_keys,
        reason=reason,
        writers_by_key={},
    )
