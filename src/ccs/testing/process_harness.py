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
import os
import queue as queue_module
import signal
import threading
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
    "park_forever",
    "run_in_subprocess",
    "run_kill_after_ack",
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
    """What the harness hands each child: the rendezvous and its delay.

    Under :func:`run_kill_after_ack` the context additionally carries the ack
    channel; ``ack()`` on a plain race context is a loud error, never a no-op.
    """

    def __init__(
        self, barrier: Any, delay_seconds: float, ack_channel: Any = None
    ) -> None:
        self._barrier = barrier
        self.delay_seconds = delay_seconds
        self._ack_channel = ack_channel

    def barrier_wait(self, timeout: float | None = None) -> None:
        """Rendezvous with every other contender before the racing section."""
        self._barrier.wait(timeout)

    def delay(self) -> None:
        """Apply this contender's injected delay (a no-op at 0.0)."""
        if self.delay_seconds > 0:
            time.sleep(self.delay_seconds)

    def ack(self, value: Any) -> None:
        """Acknowledge the durable-claimed write, then PARK — never returns.

        The ack IS the synchronization: the parent SIGKILLs this process the
        moment it arrives — no sleeps order the kill. Parking is built into
        the call so the violation is impossible by construction: a contender
        cannot run past its ack, so a clean interpreter exit can never flush
        the user-space buffers whose loss the case exists to catch. The
        contender's frame stays alive through the park, keeping its open
        handles and buffered state exactly as they were at the ack.
        """
        if self._ack_channel is None:
            raise RuntimeError(
                "ack() is only available under run_kill_after_ack; a plain "
                "race contender has no ack channel"
            )
        self._ack_channel.put(("ack", value))
        park_forever()


def park_forever() -> None:  # pragma: no cover - only ever exits via SIGKILL
    """Block the calling process until it is killed from outside.

    Not a sleep-as-synchronization: nothing is ordered by this wait — the
    ordering event is the ack the parent already received. The park only
    keeps user-space state (open handles, unflushed buffers) alive so the
    SIGKILL destroys it rather than a clean exit flushing it.
    """
    threading.Event().wait()


def _next_message(q: Any, deadline: float) -> Any:
    """Next queue message before ``deadline``, or ``None`` on timeout.

    ``multiprocessing.Queue.get(timeout=...)`` is the stdlib's own bounded
    wait — one shared implementation for every harness entry point."""
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        return None
    try:
        return q.get(timeout=remaining)
    except queue_module.Empty:
        return None


def _terminate_and_join(procs: "list[Any]") -> None:
    """Terminate-then-join every live child; escalate to kill on a holdout."""
    for proc in procs:
        if proc.is_alive():
            proc.terminate()
    for proc in procs:
        proc.join(timeout=5.0)
        if proc.is_alive():  # pragma: no cover - stubborn child
            proc.kill()
            proc.join(timeout=5.0)


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
        A contender that crashes *before* the rendezvous aborts the barrier,
        so its siblings unblock immediately (surfacing as
        ``BrokenBarrierError`` outcomes) instead of stranding until the
        timeout.
        """
        if len(contenders) < 2:
            raise HarnessFailure("a race needs at least two contenders")
        barrier = self._ctx.Barrier(len(contenders))
        queue = self._ctx.Queue()
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
        barrier_aborted = False
        try:
            while len(raw) < len(procs):
                item = _next_message(queue, deadline)
                if item is None:
                    missing = sorted(set(range(len(procs))) - set(raw))
                    message = (
                        f"race timed out after {self._timeout_sec}s; "
                        f"contender(s) {missing} never reported (killed)"
                    )
                    collected = ", ".join(
                        f"contender[{i}] {raw[i][3]}"
                        for i in sorted(raw)
                        if raw[i][1] == "error"
                    )
                    if collected:
                        message += f"; collected errors: {collected}"
                    raise HarnessFailure(message)
                raw[item[0]] = item
                if item[1] == "error" and not barrier_aborted:
                    # A pre-barrier crash must not strand its siblings at the
                    # rendezvous until the timeout: abort the barrier so
                    # current and future waiters unblock immediately with
                    # BrokenBarrierError (which _child_main reports as their
                    # own error outcomes, pointing at the root cause alongside
                    # the crasher's error). Contenders already past the
                    # barrier are unaffected — abort only breaks current and
                    # future waits.
                    barrier.abort()
                    barrier_aborted = True
        finally:
            _terminate_and_join(procs)
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

    def declare_exemption(
        self, *, subject: str, clause: str, reason: str
    ) -> DeclaredExemption:
        """Record a declared exemption directly (R-8's generic entry point).

        For exemptions that are not a refused *race* — a verification grade no
        local case can reach (fsync-grade durability), a run deferred to a
        release procedure (the managed kill-the-primary run, KTD-5), a
        platform without the kill primitive. The reason is mandatory: an
        empty reason is a :class:`HarnessFailure`, same teeth as
        :meth:`refuse_to_race`.
        """
        if not reason:
            raise HarnessFailure(
                f"exemption for {subject!r} ({clause}) declared without a "
                "reason; R-8 exempts nothing silently"
            )
        exemption = DeclaredExemption(subject=subject, clause=clause, reason=reason)
        self._exemptions.append(exemption)
        return exemption

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
    queue = ctx.Queue()
    proc = ctx.Process(
        target=_child_main,
        args=(0, spec.fn, spec.args, spec.delay_seconds, barrier, queue),
        daemon=True,
    )
    proc.start()
    deadline = time.monotonic() + timeout_sec
    try:
        message = _next_message(queue, deadline)
        if message is None:
            raise HarnessFailure(f"subprocess step timed out after {timeout_sec}s (killed)")
        index, kind, value, exc_repr, tb_text = message
    finally:
        _terminate_and_join([proc])
    if kind == "error":
        raise ContenderError(index, exc_repr, tb_text)
    return value


def _ack_child_main(
    fn: Callable[..., Any],
    args: tuple[Any, ...],
    queue: Any,
) -> None:  # pragma: no cover - runs in the spawned child
    ctx = ContenderContext(
        multiprocessing.get_context("spawn").Barrier(1), 0.0, ack_channel=queue
    )
    try:
        fn(ctx, *args)
        # Reaching here means fn returned WITHOUT acking (ctx.ack parks and
        # never returns) — a contract violation reported deterministically:
        # no ack was sent, so the parent's first message is this one.
        queue.put(("returned", None))
    except BaseException as exc:  # noqa: BLE001 - the channel carries everything
        queue.put(("error", repr(exc), traceback.format_exc()))


def run_kill_after_ack(spec: ContenderSpec, *, timeout_sec: float = 60.0) -> Any:
    """Run ONE contender until it ACKS its durable-claimed write, then SIGKILL
    it mid-life and return the acked value (guarantee-ladder U6 / KTD-5).

    Protocol: the contender performs its write and calls ``ctx.ack(value)``
    exactly once — the ack parks the process and never returns, so a
    contender cannot outlive its own acknowledgement. The parent kills the
    child THE MOMENT the ack arrives (the ack is the synchronization; no
    sleeps order anything) and asserts the child died by SIGKILL, so the
    caller can then verify the acknowledged write against the substrate with
    the writer genuinely destroyed:

    - a contender that RETURNS (necessarily without acking — the ack parks)
      is a :class:`HarnessFailure`, reported deterministically;
    - a raise re-raises as :class:`ContenderError`; a hang is killed and is a
      :class:`HarnessFailure` — never a pass.

    SIGKILL destroys USER-SPACE state only: bytes already handed to the OS
    survive in the page cache, which is exactly the process-crash grade this
    primitive can honestly verify (fsync-grade discrimination is a declared
    exemption, see the conformance kit).
    """
    if not hasattr(signal, "SIGKILL"):  # pragma: no cover - non-POSIX
        raise HarnessFailure(
            f"kill primitive unavailable on this platform; declare an R-8 "
            f"exemption instead of running the case (os.name={os.name!r})"
        )
    ctx = multiprocessing.get_context("spawn")
    queue = ctx.Queue()
    proc = ctx.Process(target=_ack_child_main, args=(spec.fn, spec.args, queue), daemon=True)
    proc.start()
    deadline = time.monotonic() + timeout_sec
    try:
        message = _next_message(queue, deadline)
        if message is None:
            raise HarnessFailure(
                f"kill-after-ack timed out after {timeout_sec}s awaiting the "
                "ack (child killed)"
            )
        if message[0] == "error":
            raise ContenderError(0, message[1], message[2])
        if message[0] == "returned":
            raise HarnessFailure(
                "contender returned without acking; the case needs an "
                "acknowledged write to kill against — call ctx.ack(facts), "
                "which parks the process for the kill"
            )
        assert message[0] == "ack"
        os.kill(proc.pid, signal.SIGKILL)  # type: ignore[arg-type]
        proc.join(timeout=10.0)
        if proc.is_alive():  # pragma: no cover - stubborn child
            raise HarnessFailure("child survived SIGKILL; cannot certify the case")
        if proc.exitcode != -signal.SIGKILL:
            raise HarnessFailure(
                f"child exited with {proc.exitcode}, not -SIGKILL: the death "
                "was not the injected kill, so the case proves nothing"
            )
        return message[1]
    finally:
        if proc.is_alive():
            proc.kill()
            proc.join(timeout=5.0)
