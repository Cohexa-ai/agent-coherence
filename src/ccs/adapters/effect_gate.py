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
(the same distinction the read-generation fence draws at the commit seam). One
revocation moves neither comparand: a peer's pessimistic write-acquire ends
the caller's grant with no commit (version unmoved) and no epoch bump
(``trigger="write"`` is deliberately outside ``EPOCH_BUMP_TRIGGERS``), so the
re-validate additionally requires the read that answers it to be served under
a STANDING grant -- a stale-status re-read HOLDs. That re-read travels
``verify_only`` so the coordinator does NOT re-grant the preempted holder on
it; the HOLD is level-triggered, so a bare re-check re-HOLDs until
``reacquire()`` re-mints a live grant rather than silently re-arming the
preempted decision on retry.
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

from ccs.core.exceptions import (
    HOLD_GENERATION_UNCONFIRMED,
    HOLD_GRANT_PREEMPTED,
    HOLD_GRANT_RECLAIMED,
    HOLD_INPUT_VANISHED,
    HOLD_READ_DENIED,
    HOLD_VERSION_MOVED,
    HOLD_VERSION_UNCONFIRMED,
    StaleView,
)

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
       the bytes -- and so the version -- never moved. The re-read must also
       itself be served under a STANDING grant: a peer's pessimistic
       write-acquire preempts this holder while moving NEITHER comparand (no
       commit yet, and ``trigger="write"`` does not bump the epoch), so a
       stale-status re-read HOLDs even with the pair unchanged.
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
    check_fence(
        volume,
        path,
        expected_version=expected_version,
        expected_generation=expected_generation,
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
    *,
    denied: bool = False,
    lapsed: bool = False,
) -> StaleView:
    """Build the HOLD exception, carrying the drift, for a moved / vanished /
    unconfirmed / reclaimed / preempted input. ``lapsed`` says the re-validate
    read itself came back stale — the caller had no standing grant at that read
    (the coordinator declines to re-grant a ``verify_only`` fence read) — the
    only signal left when a peer's write-claim acquire preempted the grant
    while moving neither comparand."""
    generation_unconfirmed = expected_generation is None or current_generation is None
    if current_version is None:
        cause = HOLD_INPUT_VANISHED
        detail = "vanished"
    elif expected_version == 0 or current_version == 0:
        cause = HOLD_VERSION_UNCONFIRMED
        detail = "could not be confirmed (coordinator degraded or unresolved)"
    elif current_version != expected_version:
        cause = HOLD_VERSION_MOVED
        detail = f"moved to v{current_version}"
    elif denied and generation_unconfirmed:
        # The coordinator REFUSED the re-read (strict mode). That is a distinct,
        # recoverable answer — and on the strict path it is how a sweep reclaim
        # actually reaches the client, so folding it into the residual bucket
        # would hide the very case this fence exists for.
        cause = HOLD_READ_DENIED
        detail = "was denied at re-read by the coordinator (view is INVALID)"
    elif generation_unconfirmed:
        cause = HOLD_GENERATION_UNCONFIRMED
        detail = (
            "has no confirmed ownership generation (degraded read, an "
            "unconfirmable out-of-band edit, or a coordinator that does not "
            "report generations)"
        )
    elif current_generation != expected_generation:
        cause = HOLD_GRANT_RECLAIMED
        # Version unchanged, both generations confirmed: the grant was reclaimed
        # out from under the decision -- the failure class a version-only check
        # cannot see.
        detail = (
            f"had its grant reclaimed (ownership generation "
            f"g{expected_generation} -> g{current_generation}, version unchanged)"
        )
    elif lapsed:
        cause = HOLD_GRANT_PREEMPTED
        # Both comparands unchanged AND confirmed, yet the re-validate read was
        # served WITHOUT a standing grant (``lapsed``): the caller's grant did
        # not stand at the re-validate. The usual cause is a peer write-claim
        # acquire that preempted the caller between capture and fire -- no
        # commit has landed (version unmoved) and trigger="write" deliberately
        # does not bump the ownership epoch, so the pair is structurally blind
        # here; only the grant-state answer on the re-read itself sees it. A
        # grantless re-read with NO peer lands here too (a re-minted identity
        # gating with an earlier read's comparands), so the detail says
        # "typically" rather than asserting a peer the fence never observed.
        detail = (
            f"was not under a standing grant at re-validate (version and "
            f"ownership generation g{expected_generation} unchanged -- "
            f"typically a peer write-claim preemption)"
        )
    else:
        # Unreachable by construction: check_fence calls _held only when a
        # comparand is unconfirmed, moved, or ``lapsed``, and every such
        # condition is handled above. A future disjunct in check_fence, or a
        # new _held caller, that reaches here would otherwise silently mislabel
        # its HOLD as grant_preempted -- fail loud so the drift is caught.
        raise AssertionError(
            "internal: _held() reached its residual branch with an unchanged, "
            f"confirmed, standing pair (v{expected_version}, "
            f"g{expected_generation}) -- no HOLD condition holds; check_fence "
            "and _held have drifted"
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
    exc.hold_cause = cause
    return exc


def check_fence(
    volume: "CoherentVolume",
    path: str | os.PathLike[str],
    *,
    expected_version: int,
    expected_generation: int | None,
) -> None:
    """Re-validate a comparand the caller captured earlier; raise
    :class:`~ccs.core.exceptions.StaleView` (a HOLD) unless the input is
    provably unchanged AND still under the grant it was read from.

    This is the pull-based half of :func:`gate` -- the same check, for callers
    whose decision step happens somewhere this process cannot reach with a
    callable: an agent that read through one tool call, reasoned, and is about
    to dispatch an irreversible effect through another (the ``swg_gate`` MCP
    tool is exactly that shape). Such a caller holds ``(expected_version,
    expected_generation)`` from the earlier read and pulls a verdict here
    immediately before dispatching.

    Returns None when the effect may proceed; raises otherwise -- including when
    either comparand is UNCONFIRMED, since firing on something the coordinator
    never confirmed is precisely what this layer exists to prevent, and when
    the re-read itself was served WITHOUT a standing grant (a stale-status
    re-grant), since a peer's write-claim preemption moves neither comparand. Same honest
    boundary as :func:`gate`: the verdict is true as of THIS check, and the
    caller's dispatch still follows it.
    """

    try:
        # observe=False: this read exists only to COMPARE comparands and its
        # bytes are discarded, so it must not advance the volume's foreign-edit
        # baseline. Otherwise checking freshness would quietly absolve an
        # out-of-band edit the caller never saw, and the caller's next write —
        # which the foreign-edit guard would have denied — would clobber it.
        _, current_version, current_generation = volume.read_with_version_generation(
            path, observe=False
        )
    except FileNotFoundError:
        raise _held(path, expected_version, None, expected_generation, None) from None
    denied = bool(getattr(volume, "_last_read_denied", False))
    # The third leg of the fence: the pair answers "did the value or the epoch
    # move", but a peer's pessimistic write-acquire ends the caller's grant
    # while moving NEITHER (no commit yet, and trigger="write" is outside
    # EPOCH_BUMP_TRIGGERS -- the epoch is per-artifact and cannot even see an
    # S-holder's preemption). The re-validate read itself carries the answer:
    # a stale-status response (warn re-grant or deny) means the grant the
    # decision was read under did NOT stand at this check. getattr keeps
    # duck-typed volumes that predate the flag on the pair-only behavior.
    lapsed = bool(getattr(volume, "_last_read_stale", False))

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
        or lapsed
    ):
        raise _held(
            path,
            expected_version,
            current_version,
            expected_generation,
            current_generation,
            denied=denied,
            lapsed=lapsed,
        )

