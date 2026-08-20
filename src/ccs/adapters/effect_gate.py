# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Effect-ordering gate wrapper (EO-5) -- the builder-facing surface.

``gate()`` is a plain-Python drop-in over a :class:`CoherentVolume` handle:
capture an input's decision-time ``(version, owner_generation)`` pair, run the
caller's decision, re-read the pair at the effect boundary, and fire the
escaping effect only if BOTH are unchanged; else HOLD (raise
:class:`~ccs.core.exceptions.StaleView`) before the effect runs. The two
comparands answer different questions: the version answers "is the value still
the one ``decide`` saw", the ownership generation answers "is the grant it was
read under still standing" -- a sweep reclamation of a stalled holder advances
the generation WITHOUT a version move, which a version-only check cannot see
(the same distinction the read-generation fence draws at the commit seam).
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
    saw AND the grant it was read under still stands; otherwise raise
    :class:`~ccs.core.exceptions.StaleView` before firing.

    Steps:

    1. Capture ``(bytes, version, owner_generation)`` from ONE
       ``read_with_version_generation`` (the value and both comparands come from
       the same read -- the split-comparand discipline that keeps a
       stale-derived decision from firing).
    2. ``decision = decide(bytes)``.
    3. Re-read the current ``(version, owner_generation)`` at the effect
       boundary. If either moved, the file vanished, or the coordinator could
       not confirm either comparand (a degraded read surfaces version ``0``; an
       older / denying / degraded coordinator surfaces generation ``None``),
       the gate HOLDs: it raises ``StaleView`` carrying
       ``expected_version`` / ``current_version`` and ``expected_generation`` /
       ``current_generation``, and the effect NEVER runs on unconfirmed or
       stale input. The generation leg is what catches the reclaim case: a
       sweep reclaimed this holder's grant (its decision is a zombie's) while
       the bytes -- and so the version -- never moved.
    4. Otherwise fire ``effect(decision)`` and return its result.

    Escaping effects only, single-host, ordering-not-rollback. The
    re-validate -> fire window is unclosable for an escaping effect. For a write
    effect use ``volume.write_cas_at`` directly.

    Args:
        volume: a ``CoherentVolume`` attached to the coordinator that tracks
            ``path``.
        path: the workspace-relative managed artifact whose
            ``(version, owner_generation)`` pair gates the effect (the
            single-artifact read-set).
        decide: ``(bytes) -> decision`` -- reads the captured bytes and returns a
            decision threaded to ``effect``.
        effect: ``(decision) -> result`` -- the escaping side effect, fired only
            if the input is unchanged at the re-validate point.

    Returns:
        The value ``effect`` returned.

    Raises:
        StaleView: the input moved, vanished, or lost its grant between capture
            and fire; the effect did not run. Recover via
            ``volume.reacquire(path)`` then re-decide.
    """
    if not callable(decide):
        raise TypeError("gate() requires a callable decide=")
    if not callable(effect):
        raise TypeError("gate() requires a callable effect=")

    data, expected_version, expected_generation = volume.read_with_version_generation(
        path
    )
    decision = decide(data)

    try:
        _, current_version, current_generation = volume.read_with_version_generation(
            path
        )
    except FileNotFoundError:
        raise _held(path, expected_version, None, expected_generation, None) from None

    # HOLD unless the coordinator CONFIRMED an unchanged (version, generation)
    # pair. Version 0 is the "could not resolve" sentinel (an older/degraded
    # coordinator, or a degrade-mode volume whose read did not fail closed);
    # generation None is its sibling (an older coordinator that predates this
    # release's generation reporting, a strict-mode deny, or a degraded read). Firing on
    # either would act on input the coordinator never confirmed. Treating an
    # unconfirmed comparand as a HOLD keeps the gate fail-closed by
    # construction, independent of the volume's on_error mode -- and it is why
    # this wrapper against a pre-fence coordinator HOLDs loudly instead of
    # silently reverting to the generation-blind check.
    unconfirmed = (
        expected_version == 0
        or current_version == 0
        or expected_generation is None
        or current_generation is None
    )
    if (
        unconfirmed
        or current_version != expected_version
        or current_generation != expected_generation
    ):
        raise _held(
            path,
            expected_version,
            current_version,
            expected_generation,
            current_generation,
        )

    # Re-validate passed: fire. The residual re-validate -> fire window is
    # unclosable for an escaping effect; the gate gates pre-fire and never rolls
    # back.
    return effect(decision)


def _held(
    path: str | os.PathLike[str],
    expected_version: int,
    current_version: int | None,
    expected_generation: int | None,
    current_generation: int | None,
) -> StaleView:
    """Build the HOLD exception, carrying the drift, for a moved / vanished /
    unconfirmed / reclaimed input."""
    generation_unconfirmed = expected_generation is None or current_generation is None
    if current_version is None:
        detail = "vanished"
    elif expected_version == 0 or current_version == 0:
        detail = "could not be confirmed (coordinator degraded or unresolved)"
    elif current_version != expected_version:
        detail = f"moved to v{current_version}"
    elif generation_unconfirmed:
        detail = (
            "has no confirmed ownership generation (coordinator predates "
            "generation reporting, denied the read, or degraded)"
        )
    else:
        # Version unchanged, both generations confirmed: the grant was reclaimed
        # out from under the decision -- the failure class a version-only check
        # cannot see.
        detail = (
            f"had its grant reclaimed (ownership generation "
            f"g{expected_generation} -> g{current_generation}, version unchanged)"
        )
    # A real CoherentVolume rejects a non-PathLike path before gate() runs, but
    # volume is duck-typed at runtime -- never let fspath() mask the HOLD.
    try:
        target = os.fspath(path)
    except TypeError:
        target = str(path)
    exc = StaleView(
        f"effect held: {target} {detail} since it was read at "
        f"v{expected_version}; effect not fired (reacquire and re-decide)"
    )
    exc.expected_version = expected_version
    exc.current_version = current_version
    exc.expected_generation = expected_generation
    exc.current_generation = current_generation
    return exc
