# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Effect-ordering gate wrapper (EO-5) -- the builder-facing surface.

``gate()`` is a plain-Python drop-in over a :class:`CoherentVolume` handle:
capture an input's decision-time version, run the caller's decision, re-read at
the effect boundary, and fire the escaping effect only if the input is unchanged;
else HOLD (raise :class:`~ccs.core.exceptions.StaleView`) before the effect runs.
It reuses the shipped ``CoherentVolume`` optimistic-concurrency primitives and
never reimplements the coordinator gate.

Honest scope:

- **Escaping effects only.** A pure *write* effect uses
  :meth:`CoherentVolume.write_cas_at` directly -- that is the atomic, no-window
  path; wrapping it here would add nothing over the shipped CAS.
- **Ordering, not rollback** (the gate fires pre-effect and never rolls back).
  For an escaping effect there is a residual re-validate -> fire window this
  layer cannot close (the effect escapes), so the gate *narrows* a stale fire, it
  does not *eliminate* it.
- **Single-host, cooperative opt-in, deny is pull-not-push, correctness first.**
- **Single-artifact read-set.** Gating on several mutually-consistent inputs is
  the shipped in-process ``CoordinatorService.effect_gate`` (a coherent cut), not
  this wrapper.

The HOLD is *raised* (a drop-in guard). Recover with ``volume.reacquire(path)``
for fresh bytes, then re-decide and re-gate.
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING, Callable, TypeVar

from ccs.core.exceptions import StaleView

if TYPE_CHECKING:
    from ccs.adapters.coherent_volume import CoherentVolume

_Decision = TypeVar("_Decision")
_Result = TypeVar("_Result")


def gate(
    volume: "CoherentVolume",
    path: str | os.PathLike[str],
    *,
    decide: Callable[[bytes], _Decision],
    effect: Callable[[_Decision], _Result],
) -> _Result:
    """Fire ``effect`` only if ``path`` is unchanged from the version ``decide``
    saw; otherwise raise :class:`~ccs.core.exceptions.StaleView` before firing.

    Steps:

    1. Capture ``(bytes, version)`` from ONE ``read_with_version`` (the value and
       the OCC comparand come from the same read -- the split-comparand
       discipline that keeps a stale-derived decision from firing).
    2. ``decision = decide(bytes)``.
    3. Re-read the current version at the effect boundary. If it moved, the file
       vanished, or the coordinator could not confirm the version (a degraded
       read surfaces version ``0``), the gate HOLDs: it raises ``StaleView``
       carrying ``expected_version`` and ``current_version``, and the effect
       NEVER runs on unconfirmed or stale input.
    4. Otherwise fire ``effect(decision)`` and return its result.

    Escaping effects only, single-host, ordering-not-rollback. The
    re-validate -> fire window is unclosable for an escaping effect. For a write
    effect use ``volume.write_cas_at`` directly.

    Args:
        volume: a ``CoherentVolume`` attached to the coordinator that tracks
            ``path``.
        path: the workspace-relative managed artifact whose version gates the
            effect (the single-artifact read-set).
        decide: ``(bytes) -> decision`` -- reads the captured bytes and returns a
            decision threaded to ``effect``.
        effect: ``(decision) -> result`` -- the escaping side effect, fired only
            if the input is unchanged at the re-validate point.

    Returns:
        The value ``effect`` returned.

    Raises:
        StaleView: the input moved (or vanished) between capture and fire; the
            effect did not run. Recover via ``volume.reacquire(path)`` then
            re-decide.
    """
    if not callable(decide):
        raise TypeError("gate() requires a callable decide=")
    if not callable(effect):
        raise TypeError("gate() requires a callable effect=")

    data, expected_version = volume.read_with_version(path)
    decision = decide(data)

    try:
        _, current_version = volume.read_with_version(path)
    except FileNotFoundError:
        raise _held(path, expected_version, None) from None

    # HOLD unless the coordinator CONFIRMED an unchanged version. Version 0 is the
    # "could not resolve" sentinel (an older/degraded coordinator, or a
    # degrade-mode volume whose read did not fail closed); firing on it would act
    # on input the coordinator never confirmed. Treating an unconfirmed version as
    # a HOLD keeps the gate fail-closed by construction, independent of the
    # volume's on_error mode.
    unconfirmed = expected_version == 0 or current_version == 0
    if unconfirmed or current_version != expected_version:
        raise _held(path, expected_version, current_version)

    # Re-validate passed: fire. The residual re-validate -> fire window is
    # unclosable for an escaping effect; the gate gates pre-fire and never rolls
    # back.
    return effect(decision)


def _held(
    path: str | os.PathLike[str],
    expected_version: int,
    current_version: int | None,
) -> StaleView:
    """Build the HOLD exception, carrying the drift, for a moved / vanished /
    unconfirmed input."""
    if current_version is None:
        detail = "vanished"
    elif expected_version == 0 or current_version == 0:
        detail = "could not be confirmed (coordinator degraded or unresolved)"
    else:
        detail = f"moved to v{current_version}"
    exc = StaleView(
        f"effect held: {os.fspath(path)} {detail} since it was read at "
        f"v{expected_version}; effect not fired (reacquire and re-decide)"
    )
    exc.expected_version = expected_version
    exc.current_version = current_version
    return exc
