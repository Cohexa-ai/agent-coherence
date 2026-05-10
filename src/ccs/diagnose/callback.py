# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""LangGraph callback adapter — passive node-event observer.

``DiagnoseCallback`` is a ``BaseCallbackHandler`` subclass that records node
super-step boundaries to an in-process buffer, intended for downstream
classification (Unit 3) and divergence detection (Unit 4).

Witness-quality framing
=======================

Per the plan's Key Technical Decision: per-key read attribution is **not**
observable via any public LangGraph hook — the merged state is passed to a
node as a single dict argument with no read-time interception point. Events
emitted here capture *what the runtime handed the node* and *what the node
returned* — not which keys the node actually read.

Single-process execution-model assumption
=========================================

The `langgraph-v0-preview` classifier (Unit 3) assumes single-process,
totally-ordered LangGraph execution. ``DiagnoseCallback`` enforces this at
attach time by refusing to wire to a ``RemoteGraph`` instance via the
``attach()`` helper, and at runtime by tracking ``langgraph_step``
monotonicity per ``langgraph_checkpoint_ns``. A monotonicity break flips
the run's verdict to ``unsupported_execution_model`` rather than letting
the classifier render a wrong answer silently. The original graph is never
mutated and runs proceed unmodified — the downgrade is a metadata signal
the classifier consumes, not an exception that crashes the user's graph.

Event shape
===========

Each ``DiagnoseEvent`` carries the validate-log-compatible 3-tuple
(``sequence_number``, ``instance_id``, ``schema_version``) so the buffer
can be serialized to JSONL by Unit 9 and ingested by ``validate_log``
without schema changes.

Identity
========

Artifact identity uses the shared ``ccs.core.identity.artifact_uuid``
helper. Scope is the graph-level scope string the caller provides at
construction (default ``"langgraph"``); key is the top-level state-key
name. Cross-tool identity convention is the load-bearing claim of
Spike 0.
"""

from __future__ import annotations

import threading
import uuid
import warnings
from dataclasses import dataclass, field
from collections.abc import Mapping
from typing import Any, Literal

from langchain_core.callbacks import BaseCallbackHandler

from ccs.core.hashing import compute_content_hash
from ccs.core.identity import artifact_uuid

from . import CCS_DIAGNOSE_LOG_SCHEMA_VERSION

__all__ = [
    "DiagnoseCallback",
    "DiagnoseEvent",
    "DiagnoseWarning",
    "EventType",
    "RunVerdictSignal",
    "DEFAULT_NODE_NAME",
    "DEFAULT_SCOPE",
]


EventType = Literal["node_start", "node_end", "warning", "verdict_signal"]
"""Discriminator for ``DiagnoseEvent``.

* ``node_start`` — runtime is about to invoke a node; ``artifact_versions``
  / ``content_hashes`` describe the merged state as observed *before* the
  node ran.
* ``node_end`` — runtime received the node's return dict; ``artifact_versions``
  / ``content_hashes`` describe the node's write set (return-dict keys).
* ``warning`` — a non-fatal observation that downstream stages should
  surface (e.g. missing ``metadata["langgraph_node"]``). Carries no
  state hashes.
* ``verdict_signal`` — a hard signal that the run cannot be classified
  under ``langgraph-v0-preview`` (e.g. ``RemoteGraph`` instance, namespace
  monotonicity break). Carries no state hashes.
"""


RunVerdictSignal = Literal[
    "unsupported_execution_model",
    "subgraph_observed",
    "remote_graph_attached",
]


DEFAULT_SCOPE: str = "langgraph"
"""Scope string for ``artifact_uuid`` when caller does not override."""

DEFAULT_NODE_NAME: str = "unknown"
"""Fallback node attribution when ``metadata['langgraph_node']`` is absent.

The plan's spec is to attribute as ``"unknown"`` and emit a ``warning``
event rather than crashing the user's graph.
"""


class DiagnoseWarning(UserWarning):
    """Issued when the callback observes a non-fatal anomaly.

    Examples include ``RemoteGraph`` attach attempts and missing node
    attribution metadata. Tests can ``pytest.warns(DiagnoseWarning)`` to
    assert the user-facing surface.
    """


@dataclass(frozen=True)
class DiagnoseEvent:
    """Single observation emitted by ``DiagnoseCallback``.

    Attributes:
        sequence_number: Monotonic counter within a single callback instance.
            Starts at 1.
        instance_id: UUID4 generated once per ``DiagnoseCallback``. Used to
            correlate events across log streams; matches the
            ``validate_log`` 3-tuple convention.
        schema_version: ``CCS_DIAGNOSE_LOG_SCHEMA_VERSION``. Same value for
            every event in the buffer.
        tick: Super-step index (``metadata["langgraph_step"]`` for node
            events; ``-1`` for warnings/verdict signals that fire outside
            a super-step boundary).
        node: Node name (``metadata["langgraph_node"]`` or
            ``DEFAULT_NODE_NAME`` if missing). For non-node events, the
            empty string.
        event_type: One of ``EventType``.
        artifact_versions: Map of artifact UUID (derived via
            ``artifact_uuid``) to opaque version string. Versions come from
            ``Checkpoint.channel_versions`` when a ``DiagnoseCheckpointer``
            is attached; otherwise the value's content hash is used as a
            synthetic version. Empty for non-node events.
        content_hashes: Map of artifact UUID to SHA-256 hex content hash
            (via ``compute_content_hash``). Empty for non-node events.
        run_id: ``run_id`` from the LangGraph callback. Empty string when
            absent.
        namespace: ``metadata["langgraph_checkpoint_ns"]`` for node events;
            empty string otherwise.
        verdict_signal: Optional explicit signal; populated only for
            ``event_type == "verdict_signal"``.
        message: Human-readable string for warning / verdict events.
            Empty for node events.
    """

    sequence_number: int
    instance_id: uuid.UUID
    schema_version: str
    tick: int
    node: str
    event_type: EventType
    artifact_versions: Mapping[uuid.UUID, str] = field(default_factory=dict)
    content_hashes: Mapping[uuid.UUID, str] = field(default_factory=dict)
    run_id: str = ""
    namespace: str = ""
    verdict_signal: RunVerdictSignal | None = None
    message: str = ""


def _stringify_for_hash(value: Any) -> str:
    """Return a stable string suitable for ``compute_content_hash``.

    LangGraph state values can be any Python object. The diagnose layer
    needs a deterministic byte representation for cross-run comparison —
    perfect canonical-JSON is overkill for a witness-quality signal, but
    we must avoid ``repr`` because dict ordering is insertion-stable but
    not key-sorted. ``str(sorted(...))`` is sufficient for the v0-preview
    contract; calibration in Unit 9 will tighten if needed.
    """
    if isinstance(value, dict):
        try:
            return repr({k: _stringify_for_hash(value[k]) for k in sorted(value)})
        except TypeError:
            # Non-sortable keys: fall back to insertion order.
            return repr({k: _stringify_for_hash(v) for k, v in value.items()})
    if isinstance(value, (list, tuple)):
        return repr([_stringify_for_hash(item) for item in value])
    return repr(value)


def _is_remote_graph(graph: Any) -> bool:
    """Return ``True`` if ``graph`` looks like a distributed/remote graph.

    Imported lazily to avoid a hard dependency on the optional
    ``langgraph.pregel.remote`` module.
    """
    try:
        from langgraph.pregel.remote import RemoteGraph  # type: ignore[import-not-found]
    except Exception:  # pragma: no cover — defensive: import path may move.
        return False
    return isinstance(graph, RemoteGraph)


def _is_subgraph_namespace(namespace: str) -> bool:
    """Return ``True`` if ``namespace`` indicates a subgraph hop.

    Top-level node namespaces have the form ``"<node>:<uuid>"`` (no ``|``).
    Subgraph namespaces concatenate frames with ``|`` (e.g.
    ``"sub:<uuid>|inner:<uuid>"``).
    """
    return "|" in namespace


class DiagnoseCallback(BaseCallbackHandler):
    """Sync callback handler that records node-level super-step events.

    Use:
        cb = DiagnoseCallback()
        graph.invoke(state, config={"callbacks": [cb]})
        events = cb.events
        cb.finalize()  # idempotent; freezes the buffer

    The callback is thread-safe for the single-process case (Pregel
    issues callbacks serially in a totally-ordered reducer pass). A
    lock guards the buffer so async callback variants in a future
    `AsyncDiagnoseCallback` can share the same instance.
    """

    raise_on_remote: bool
    """If ``True``, ``attach()`` raises on a ``RemoteGraph`` instance.

    Default is ``False`` — the safer choice per the plan: emit a
    ``verdict_signal`` event with ``remote_graph_attached`` and let the
    classifier downgrade the run to ``unsupported_execution_model``
    rather than crash the user's graph.
    """

    def __init__(
        self,
        *,
        scope: str = DEFAULT_SCOPE,
        raise_on_remote: bool = False,
        instance_id: uuid.UUID | None = None,
    ) -> None:
        super().__init__()
        self._scope = scope
        self.raise_on_remote = raise_on_remote
        self._instance_id: uuid.UUID = instance_id if instance_id is not None else uuid.uuid4()
        self._sequence: int = 0
        self._buffer: list[DiagnoseEvent] = []
        self._finalized = False
        self._lock = threading.Lock()

        # Per-namespace last-seen langgraph_step for monotonicity tracking.
        self._last_step_by_ns: dict[str, int] = {}
        self._monotonicity_broken: bool = False
        self._subgraph_observed: bool = False

        # External version overlay populated by ``DiagnoseCheckpointer``
        # when one is attached. Keyed by artifact UUID.
        self._channel_versions: dict[uuid.UUID, str] = {}

    # ---------------------------------------------------------------- #
    # Public API
    # ---------------------------------------------------------------- #

    @property
    def instance_id(self) -> uuid.UUID:
        """UUID4 of this callback instance; stable for the run."""
        return self._instance_id

    @property
    def scope(self) -> str:
        """Identity scope passed to ``artifact_uuid``."""
        return self._scope

    @property
    def events(self) -> tuple[DiagnoseEvent, ...]:
        """Return an immutable snapshot of all events recorded so far."""
        with self._lock:
            return tuple(self._buffer)

    @property
    def is_finalized(self) -> bool:
        """``True`` once ``finalize()`` has been called."""
        return self._finalized

    def attach(self, graph: Any) -> None:
        """Inspect ``graph`` and record verdict signals if unsupported.

        Public hook so callers can opt-in to early refusal:

            cb = DiagnoseCallback()
            cb.attach(graph)               # records verdict signal if RemoteGraph
            graph.invoke(state, config={"callbacks": [cb]})

        Calling ``attach`` is optional — the callback also self-protects
        on the first observed event.
        """
        if _is_remote_graph(graph):
            self._record_verdict_signal(
                signal="remote_graph_attached",
                message=(
                    "DiagnoseCallback attached to a RemoteGraph; verdict "
                    "downgraded to unsupported_execution_model."
                ),
            )
            if self.raise_on_remote:
                raise UnsupportedExecutionModelError(
                    "DiagnoseCallback cannot observe RemoteGraph instances."
                )

    def finalize(self) -> tuple[DiagnoseEvent, ...]:
        """Mark the buffer as final and return the event tuple.

        Idempotent — repeated calls return the same snapshot.
        """
        with self._lock:
            self._finalized = True
            return tuple(self._buffer)

    # ---------------------------------------------------------------- #
    # BaseCallbackHandler overrides
    # ---------------------------------------------------------------- #

    def on_chain_start(  # noqa: D401 — overrides parent docstring shape
        self,
        serialized: dict[str, Any],
        inputs: dict[str, Any] | Any,
        *,
        run_id: uuid.UUID,
        parent_run_id: uuid.UUID | None = None,
        tags: list[str] | None = None,
        metadata: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        md = metadata or {}
        node = md.get("langgraph_node")
        if node is None:
            # The outermost "LangGraph" wrapper has no node attribution;
            # ignore it silently. Inner nodes that lack attribution emit
            # a warning event so downstream stages know the buffer is
            # incomplete, but do NOT crash.
            tags_list = tags or []
            if any(tag.startswith("graph:step") for tag in tags_list):
                self._record_warning(
                    f"on_chain_start with graph:step tag but no langgraph_node "
                    f"in metadata; attributed as {DEFAULT_NODE_NAME!r}.",
                )
                self._record_node_event(
                    event_type="node_start",
                    node=DEFAULT_NODE_NAME,
                    tick=int(md.get("langgraph_step", -1)) if md else -1,
                    namespace=str(md.get("langgraph_checkpoint_ns", "")),
                    state_dict=inputs if isinstance(inputs, dict) else {},
                    run_id=str(run_id),
                )
            return

        namespace = str(md.get("langgraph_checkpoint_ns", ""))
        step_raw = md.get("langgraph_step")
        try:
            step = int(step_raw) if step_raw is not None else -1
        except (TypeError, ValueError):
            step = -1

        self._track_namespace_step(namespace=namespace, step=step)

        self._record_node_event(
            event_type="node_start",
            node=str(node),
            tick=step,
            namespace=namespace,
            state_dict=inputs if isinstance(inputs, dict) else {},
            run_id=str(run_id),
        )

    def on_chain_end(  # noqa: D401
        self,
        outputs: dict[str, Any] | Any,
        *,
        run_id: uuid.UUID,
        parent_run_id: uuid.UUID | None = None,
        **kwargs: Any,
    ) -> None:
        # ``on_chain_end`` does not receive metadata; we cannot reliably
        # attribute to a node from this callback alone. Record an end
        # event with the node name resolved by run_id when possible. The
        # simplest correlation is via the most recent ``node_start`` with
        # the same run_id.
        node, namespace, tick = self._resolve_end_attribution(str(run_id))
        if node == "":
            # The LangGraph outer wrapper end — ignore.
            return

        self._record_node_event(
            event_type="node_end",
            node=node,
            tick=tick,
            namespace=namespace,
            state_dict=outputs if isinstance(outputs, dict) else {},
            run_id=str(run_id),
        )

    # ---------------------------------------------------------------- #
    # Internal helpers
    # ---------------------------------------------------------------- #

    def _next_sequence(self) -> int:
        self._sequence += 1
        return self._sequence

    def _ensure_writable(self) -> None:
        if self._finalized:
            raise RuntimeError(
                "DiagnoseCallback is finalized; cannot record additional events."
            )

    def _track_namespace_step(self, *, namespace: str, step: int) -> None:
        """Track ``langgraph_step`` monotonicity per namespace.

        On any monotonicity break we record a verdict signal so the
        classifier can downgrade. Subgraph hops (``|`` in the namespace)
        also trigger a verdict signal once per run.
        """
        if step < 0:
            return

        if _is_subgraph_namespace(namespace) and not self._subgraph_observed:
            self._subgraph_observed = True
            self._record_verdict_signal(
                signal="subgraph_observed",
                message=(
                    f"Subgraph hop observed in namespace {namespace!r}; "
                    "verdict downgraded — diagnose v0-preview supports "
                    "single-graph runs only."
                ),
            )

        # NOTE: LangGraph can emit ``on_chain_start`` twice at the same
        # ``(step, namespace)`` for nodes attached to a conditional edge
        # — the edge resolver and the node body both raise the callback
        # under the same metadata. A re-entry at the *same* step is
        # benign and must NOT trip monotonicity. Only a strictly
        # decreasing ``step`` within an unchanged namespace indicates
        # an unsupported execution model.
        last = self._last_step_by_ns.get(namespace)
        if last is not None and step < last and not self._monotonicity_broken:
            self._monotonicity_broken = True
            self._record_verdict_signal(
                signal="unsupported_execution_model",
                message=(
                    f"langgraph_step monotonicity broken in namespace "
                    f"{namespace!r}: saw {step} after {last}."
                ),
            )
        self._last_step_by_ns[namespace] = max(last or step, step)

    def _record_node_event(
        self,
        *,
        event_type: EventType,
        node: str,
        tick: int,
        namespace: str,
        state_dict: Mapping[str, Any],
        run_id: str,
    ) -> None:
        with self._lock:
            self._ensure_writable()
            versions, hashes = self._snapshot_state(state_dict)
            event = DiagnoseEvent(
                sequence_number=self._next_sequence(),
                instance_id=self._instance_id,
                schema_version=CCS_DIAGNOSE_LOG_SCHEMA_VERSION,
                tick=tick,
                node=node,
                event_type=event_type,
                artifact_versions=versions,
                content_hashes=hashes,
                run_id=run_id,
                namespace=namespace,
            )
            self._buffer.append(event)

    def record_warning(self, message: str) -> None:
        """Append a ``warning`` event to the buffer and emit a Python warning.

        Public hook for callers (e.g. the CLI) that need to surface a
        non-fatal observation into the same buffer the classifier consumes —
        this lets the verdict downgrade with a clean ``reason`` instead of
        only writing to ``warnings.warn``.
        """
        with self._lock:
            self._ensure_writable()
            event = DiagnoseEvent(
                sequence_number=self._next_sequence(),
                instance_id=self._instance_id,
                schema_version=CCS_DIAGNOSE_LOG_SCHEMA_VERSION,
                tick=-1,
                node="",
                event_type="warning",
                message=message,
            )
            self._buffer.append(event)
        warnings.warn(message, DiagnoseWarning, stacklevel=2)

    # Deprecated alias retained for any private callers; prefer
    # :meth:`record_warning`.
    _record_warning = record_warning

    def _record_verdict_signal(
        self, *, signal: RunVerdictSignal, message: str
    ) -> None:
        with self._lock:
            self._ensure_writable()
            event = DiagnoseEvent(
                sequence_number=self._next_sequence(),
                instance_id=self._instance_id,
                schema_version=CCS_DIAGNOSE_LOG_SCHEMA_VERSION,
                tick=-1,
                node="",
                event_type="verdict_signal",
                verdict_signal=signal,
                message=message,
            )
            self._buffer.append(event)
        warnings.warn(message, DiagnoseWarning, stacklevel=2)

    def _snapshot_state(
        self, state_dict: Mapping[str, Any]
    ) -> tuple[dict[uuid.UUID, str], dict[uuid.UUID, str]]:
        """Compute (versions, content_hashes) for a state dict.

        Versions come from the checkpointer overlay when present; otherwise
        we synthesize from the content hash so divergence detection still
        has something comparable.
        """
        versions: dict[uuid.UUID, str] = {}
        hashes: dict[uuid.UUID, str] = {}
        for key, value in state_dict.items():
            aid = artifact_uuid(self._scope, str(key))
            content_hash = compute_content_hash(_stringify_for_hash(value))
            hashes[aid] = content_hash
            overlay = self._channel_versions.get(aid)
            versions[aid] = overlay if overlay is not None else content_hash
        return versions, hashes

    def _resolve_end_attribution(self, run_id: str) -> tuple[str, str, int]:
        """Look up the most recent ``node_start`` with this run_id."""
        for event in reversed(self._buffer):
            if event.event_type == "node_start" and event.run_id == run_id:
                return event.node, event.namespace, event.tick
        return "", "", -1

    # ---------------------------------------------------------------- #
    # Wiring with DiagnoseCheckpointer (Unit 2 cross-validation)
    # ---------------------------------------------------------------- #

    def _ingest_channel_versions(
        self,
        *,
        channel_versions: Mapping[str, Any],
        channel_values: Mapping[str, Any] | None = None,
    ) -> None:
        """Update the version overlay from a checkpoint.

        Called by ``DiagnoseCheckpointer.put`` to forward authoritative
        per-channel versions from LangGraph's built-in checkpoint state.
        """
        del channel_values  # Reserved for future cross-validation work.
        with self._lock:
            for key, version in channel_versions.items():
                aid = artifact_uuid(self._scope, str(key))
                self._channel_versions[aid] = str(version)

    # ---------------------------------------------------------------- #
    # Diagnostics for tests
    # ---------------------------------------------------------------- #

    def has_verdict_signal(self, signal: RunVerdictSignal) -> bool:
        """Return ``True`` if any event with ``signal`` is in the buffer."""
        with self._lock:
            return any(
                ev.event_type == "verdict_signal" and ev.verdict_signal == signal
                for ev in self._buffer
            )

    def warning_messages(self) -> tuple[str, ...]:
        """Return all warning event messages, in order."""
        with self._lock:
            return tuple(
                ev.message for ev in self._buffer if ev.event_type == "warning"
            )

    def node_events(self) -> tuple[DiagnoseEvent, ...]:
        """Return node_start/node_end events as an immutable tuple."""
        with self._lock:
            return tuple(
                ev
                for ev in self._buffer
                if ev.event_type in ("node_start", "node_end")
            )


class UnsupportedExecutionModelError(RuntimeError):
    """Raised when ``raise_on_remote=True`` and a ``RemoteGraph`` is attached."""
