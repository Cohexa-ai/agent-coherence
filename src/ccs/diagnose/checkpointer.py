# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""High-fidelity checkpointer for ``ccs-diagnose``.

``DiagnoseCheckpointer`` is an opt-in ``MemorySaver`` subclass that
forwards LangGraph's authoritative per-channel ``Checkpoint.channel_versions``
to a paired ``DiagnoseCallback``. It exists for the high-fidelity power-user
path described in the plan:

    cb = DiagnoseCallback()
    cp = DiagnoseCheckpointer(callback=cb)
    graph = builder.compile(checkpointer=cp)
    graph.invoke(state, config={
        "callbacks": [cb],
        "configurable": {"thread_id": "diagnose-run"},
    })

The callback works without the checkpointer — divergence detection then
falls back to content-hash-derived synthetic versions. With the
checkpointer attached, divergence detection becomes calibration-grade
because LangGraph's own monotonic version stamps are observed directly.

The checkpointer is a thin observer; it does NOT replace MemorySaver's
storage. ``put`` and ``put_writes`` invoke ``super()`` so the underlying
checkpoint contract is unchanged. If LangGraph evolves
``Checkpoint.channel_versions`` shape between minor versions, the
forwarding path degrades gracefully — we read with ``.get(...)`` and
forward whatever string-form we receive.
"""

from __future__ import annotations

from typing import Any, Mapping, Sequence

from langgraph.checkpoint.memory import MemorySaver

from .callback import DiagnoseCallback

__all__ = ["DiagnoseCheckpointer"]


class DiagnoseCheckpointer(MemorySaver):
    """``MemorySaver`` that forwards channel versions to a ``DiagnoseCallback``.

    Args:
        callback: The ``DiagnoseCallback`` instance attached to the same
            graph invocation. The checkpointer pushes ``channel_versions``
            into the callback's overlay each time LangGraph commits a
            checkpoint.

    Notes:
        * The callback is held by reference. Multiple
          ``DiagnoseCheckpointer`` instances can share one callback if
          a user runs several graphs in sequence and aggregates their
          observations into a single buffer.
        * A ``None`` callback is permitted (the checkpointer becomes a
          regular ``MemorySaver``). This is useful for tests that want
          to compile-then-attach.
    """

    def __init__(
        self, *, callback: DiagnoseCallback | None = None, **kwargs: Any
    ) -> None:
        super().__init__(**kwargs)
        self._diagnose_callback: DiagnoseCallback | None = callback

    @property
    def callback(self) -> DiagnoseCallback | None:
        """The attached callback (may be ``None``)."""
        return self._diagnose_callback

    def attach_callback(self, callback: DiagnoseCallback) -> None:
        """Attach (or replace) the callback after construction."""
        self._diagnose_callback = callback

    # ---------------------------------------------------------------- #
    # MemorySaver overrides
    # ---------------------------------------------------------------- #

    def put(
        self,
        config: Any,
        checkpoint: Any,
        metadata: Any,
        new_versions: Any,
    ) -> Any:
        """Forward channel versions, then delegate to the parent.

        The forwarding is wrapped in a broad except — diagnose must NEVER
        crash a user's graph. A broken forward leaves the parent's
        contract intact and the callback simply has no overlay for that
        super-step (synthetic versions will be used instead).
        """
        try:
            cb = self._diagnose_callback
            if cb is not None:
                channel_versions: Mapping[str, Any] = (
                    checkpoint.get("channel_versions", {}) if isinstance(checkpoint, Mapping)
                    else getattr(checkpoint, "channel_versions", {})
                )
                channel_values: Mapping[str, Any] = (
                    checkpoint.get("channel_values", {}) if isinstance(checkpoint, Mapping)
                    else getattr(checkpoint, "channel_values", {})
                )
                cb._ingest_channel_versions(
                    channel_versions=channel_versions,
                    channel_values=channel_values,
                )
        except Exception:  # pragma: no cover — defensive: never crash user's graph.
            pass
        return super().put(config, checkpoint, metadata, new_versions)

    def put_writes(
        self,
        config: Any,
        writes: Sequence[tuple[str, Any]],
        task_id: str,
        task_path: str = "",
    ) -> None:
        """Delegate to parent. ``writes`` are channel-level; the callback
        sees the merged state via ``put`` already, so no forward needed.
        """
        return super().put_writes(config, writes, task_id, task_path)
