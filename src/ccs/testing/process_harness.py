# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Cross-process contender harness for the conformance kit (plan U1, R-1/R-8).

Why this exists: the kit's original race assertions were hand-sequenced
single-process interleavings, and in one process the coordinator's own RLock
satisfies any race test — a binding whose "CAS" is read-then-write under a
local lock passes. The prior-art instance shipped green (`yc-software/qm`'s
single-process CAS test over a cross-process-broken implementation). The
contract consequence is stated where backends read it: **in-process
serialization does not satisfy any clause of the conformance contract.**

The harness gives a race three properties an in-process test cannot fake:

- **Contenders are separate OS processes** (``multiprocessing`` *spawn*
  context, for platform parity and import-cleanliness), so no parent-process
  lock serializes them.
- **The barrier is the concurrency witness.** Every contender rendezvouses at
  a real ``multiprocessing.Barrier`` with ``parties == n`` before its racing
  section; a harness that ran contenders serially would deadlock and hit the
  timeout, so a green run is itself proof of overlap. No wall-clock heuristics.
- **The injected delay only widens windows.** Each contender may carry a
  ``delay_seconds`` applied via ``ctx.delay()`` between its read and its
  write; every assertion must also hold at delay 0 (the race is legal at any
  interleaving), so the delay is never load-bearing for correctness — it
  forces the interesting interleaving instead of hoping for it.

The harness also owns the **declared-exemption seam** (R-8): a binding that
cannot be raced cross-process (``in_process_only = True``) is *refused with a
recorded reason*, never silently skipped. KTD-4 (the in-memory registry and
in-memory bindings) is the first consumer.

Test-infrastructure layer: nothing in ``ccs.coordinator`` or the adapters may
import this module; production modules gain no test-only branches.
"""

from __future__ import annotations

import multiprocessing
import time
import traceback
from dataclasses import dataclass
from typing import Any, Callable

__all__ = [
    "ContenderContext",
    "ContenderError",
    "ContenderOutcome",
    "ContenderSpec",
    "DeclaredExemption",
    "HarnessFailure",
    "ProcessRaceHarness",
    "RaceResult",
    "run_in_subprocess",
]


class HarnessFailure(AssertionError):
    """The harness itself failed (hang, lost child, undeclared exemption).

    An ``AssertionError`` subclass so a conformance run treats harness trouble
    as a failed assertion — never as a pass, never as a silent skip.
    """


class ContenderError(Exception):
    """A contender raised: carries the child's exception repr + traceback text."""

    def __init__(self, index: int, exc_repr: str, tb_text: str) -> None:
        self.index = index
        self.exc_repr = exc_repr
        self.tb_text = tb_text
        super().__init__(f"contender[{index}] raised {exc_repr}")


@dataclass(frozen=True)
class ContenderSpec:
    """One contender: a *top-level, picklable* callable plus args and delay.

    The callable's first parameter is the :class:`ContenderContext` the harness
    builds in the child; it must call ``ctx.barrier_wait()`` exactly once
    before its racing section and may call ``ctx.delay()`` between its read
    and its write.
    """

    fn: Callable[..., Any]
    args: tuple[Any, ...] = ()
    delay_seconds: float = 0.0


@dataclass(frozen=True)
class ContenderOutcome:
    """One contender's result: ``value`` on return, ``error`` on raise."""

    index: int
    value: Any = None
    error: ContenderError | None = None


@dataclass(frozen=True)
class RaceResult:
    """All contender outcomes, in contender order, plus the error subset."""

    outcomes: tuple[ContenderOutcome, ...]

    @property
    def errors(self) -> list[ContenderOutcome]:
        return [o for o in self.outcomes if o.error is not None]


@dataclass(frozen=True)
class DeclaredExemption:
    """A refused race, with the binding's own declared reason (R-8)."""

    subject: str
    clause: str
    reason: str


class ContenderContext:
    """What the harness hands each child: the rendezvous and its delay."""

    def __init__(self, barrier: Any, delay_seconds: float) -> None:
        self._barrier = barrier
        self.delay_seconds = delay_seconds

    def barrier_wait(self, timeout: float | None = None) -> None:
        """Rendezvous with every other contender before the racing section."""
        self._barrier.wait(timeout)

    def delay(self) -> None:
        """Apply this contender's injected delay (a no-op at 0.0)."""
        if self.delay_seconds > 0:
            time.sleep(self.delay_seconds)


def _child_main(
    index: int,
    fn: Callable[..., Any],
    args: tuple[Any, ...],
    delay_seconds: float,
    barrier: Any,
    queue: Any,
) -> None:  # pragma: no cover - runs in the spawned child
    ctx = ContenderContext(barrier, delay_seconds)
    try:
        value = fn(ctx, *args)
        queue.put((index, "ok", value, None, None))
    except BaseException as exc:  # noqa: BLE001 - the channel carries everything
        queue.put((index, "error", None, repr(exc), traceback.format_exc()))


class ProcessRaceHarness:
    """Spawn contenders, rendezvous them, collect typed outcomes, report exemptions."""

    def __init__(self, timeout_sec: float = 60.0) -> None:
        self._timeout_sec = timeout_sec
        self._exemptions: list[DeclaredExemption] = []
        self._ctx = multiprocessing.get_context("spawn")

    # -- racing -----------------------------------------------------------

    def race(self, contenders: list[ContenderSpec] | tuple[ContenderSpec, ...]) -> RaceResult:
        """Run every contender in its own OS process; return outcomes in order.

        A hung or lost child is a :class:`HarnessFailure`, never a pass: the
        remaining children are terminated (then killed) and the run aborts.
        """
        if len(contenders) < 2:
            raise HarnessFailure("a race needs at least two contenders")
        barrier = self._ctx.Barrier(len(contenders))
        queue = self._ctx.SimpleQueue()
        procs = [
            self._ctx.Process(
                target=_child_main,
                args=(i, spec.fn, spec.args, spec.delay_seconds, barrier, queue),
                daemon=True,
            )
            for i, spec in enumerate(contenders)
        ]
        for proc in procs:
            proc.start()
        deadline = time.monotonic() + self._timeout_sec
        raw: dict[int, tuple[Any, ...]] = {}
        try:
            while len(raw) < len(procs):
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    missing = sorted(set(range(len(procs))) - set(raw))
                    raise HarnessFailure(
                        f"race timed out after {self._timeout_sec}s; "
                        f"contender(s) {missing} never reported (killed)"
                    )
                # SimpleQueue has no timeout; poll the underlying reader.
                if queue._reader.poll(min(remaining, 0.1)):  # noqa: SLF001
                    item = queue.get()
                    raw[item[0]] = item
        finally:
            for proc in procs:
                if proc.is_alive():
                    proc.terminate()
            for proc in procs:
                proc.join(timeout=5.0)
                if proc.is_alive():  # pragma: no cover - stubborn child
                    proc.kill()
                    proc.join(timeout=5.0)
        outcomes = []
        for i in range(len(procs)):
            index, kind, value, exc_repr, tb_text = raw[i]
            if kind == "ok":
                outcomes.append(ContenderOutcome(index=index, value=value))
            else:
                outcomes.append(
                    ContenderOutcome(index=index, error=ContenderError(index, exc_repr, tb_text))
                )
        return RaceResult(outcomes=tuple(outcomes))

    # -- the declared-exemption seam (R-8) --------------------------------

    def refuse_to_race(self, binding: Any, clause: str) -> DeclaredExemption:
        """Refuse a race for an ``in_process_only`` binding, recording why.

        The reason must be *declared by the binding* (``in_process_only_reason``);
        an undeclared refusal is a :class:`HarnessFailure` — the whole point of
        R-8 is that nothing is exempted without a stated reason.
        """
        if not getattr(binding, "in_process_only", False):
            raise HarnessFailure(
                f"{type(binding).__name__} is not declared in_process_only; race it instead"
            )
        reason = getattr(binding, "in_process_only_reason", "")
        if not reason:
            raise HarnessFailure(
                f"{type(binding).__name__} declares in_process_only without a reason; "
                "a declared exemption requires one (R-8)"
            )
        exemption = DeclaredExemption(subject=type(binding).__name__, clause=clause, reason=reason)
        self._exemptions.append(exemption)
        return exemption

    def report(self) -> list[str]:
        """The run report: one line per declared exemption (empty = none)."""
        return [
            f"EXEMPT [{e.clause}] {e.subject}: {e.reason}"
            for e in self._exemptions
        ]


def run_in_subprocess(spec: ContenderSpec, *, timeout_sec: float = 60.0) -> Any:
    """Run ONE contender in its own OS process and return its value.

    The single-step sibling of :meth:`ProcessRaceHarness.race` for sequential
    cross-process scenarios (acquire in one process, commit from another):
    same spawn context, same typed channel, same kill-on-hang discipline. The
    child's :class:`ContenderContext` carries a one-party barrier, so the
    contender's ``ctx.barrier_wait()`` returns immediately.

    A raise in the child re-raises here as :class:`ContenderError`; a hang is
    a :class:`HarnessFailure` — never a pass.
    """
    ctx = multiprocessing.get_context("spawn")
    barrier = ctx.Barrier(1)
    queue = ctx.SimpleQueue()
    proc = ctx.Process(
        target=_child_main,
        args=(0, spec.fn, spec.args, spec.delay_seconds, barrier, queue),
        daemon=True,
    )
    proc.start()
    deadline = time.monotonic() + timeout_sec
    try:
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise HarnessFailure(f"subprocess step timed out after {timeout_sec}s (killed)")
            if queue._reader.poll(min(remaining, 0.1)):  # noqa: SLF001
                index, kind, value, exc_repr, tb_text = queue.get()
                break
    finally:
        if proc.is_alive():
            proc.terminate()
        proc.join(timeout=5.0)
        if proc.is_alive():  # pragma: no cover - stubborn child
            proc.kill()
            proc.join(timeout=5.0)
    if kind == "error":
        raise ContenderError(index, exc_repr, tb_text)
    return value
