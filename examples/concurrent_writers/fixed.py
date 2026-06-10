# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""Fixed case: the same concurrent race, made safe by CoherentVolume.write_cas.

N threads, each with its own CoherentVolume attached to one local coordinator,
run the identical read→write race as ``broken.py`` — but through ``write_cas``,
the optimistic (commit-CAS) write path shipped in v0.9.1. Each writer reads the
shared total and attempts to commit; the coordinator's serialized commit elects
one winner, and the loser is told ``version_mismatch`` (a typed, retryable
conflict — never a silent drop), reacquires (re-mint identity + mandatory fresh
read), re-derives its update from the winner's value via ``make_content``, and
retries. Every update survives.

This is the rung-2 guarantee the *sequential* demo (``examples/coherent_volume``)
cannot make: surviving a TRUE concurrent same-key race, not just a sequenced
stale-overwrite. The winner is non-deterministic; the invariant — final total =
start + the sum of every delta, no silent loss — is not. ``make_content`` is
re-invoked per attempt, so re-deriving intent against the latest state is what
turns the retry into an *update* rather than a stale overwrite.

Single-host scope (loopback coordinator + SQLite-WAL); spawns a local
coordinator subprocess (no network, no API keys) and tears it down in finally.
"""

from __future__ import annotations

import os
import shutil
import sys
import tempfile
import threading
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))
# A spawned coordinator subprocess must import ``ccs`` too; propagate the src
# path so the demo also runs from a bare checkout (harmless when installed).
_pp = os.environ.get("PYTHONPATH", "")
if str(SRC_ROOT) not in _pp.split(os.pathsep):
    os.environ["PYTHONPATH"] = f"{SRC_ROOT}{os.pathsep}{_pp}" if _pp else str(SRC_ROOT)

from ccs.adapters.claude_code.lifecycle import LifecycleConfig, stop_coordinator
from ccs.adapters.coherent_volume import CoherentVolume

_START = 100
_A_ADDS = 10
_B_ADDS = 5
_REL = "data/tally.txt"
_MANAGED = ("data/**",)
_SCENARIO = (
    f"two agents CONCURRENTLY update a shared total "
    f"(start={_START}; A adds {_A_ADDS}, B adds {_B_ADDS})"
)

# Snappy local spawn for a one-command demo; no idle shutdown mid-run.
_DEMO_CFG = LifecycleConfig(
    idle_shutdown_sec=0,
    sweep_interval_sec=0.1,
    port_file_retry_attempts=40,
    port_file_retry_interval_sec=0.05,
    connect_retry_attempts=20,
    connect_retry_interval_sec=0.05,
)


def _race_write_cas(deltas: list[int], start: int) -> tuple[int, list[int]]:
    """Run ``len(deltas)`` threads that each add their delta to a shared tally
    via ``write_cas``, concurrently, against one coordinator.

    Returns ``(final_total, attempts_per_writer)``. ``attempts_per_writer[i] > 1``
    means writer ``i`` lost a race and re-applied on the fresh value (the typed
    conflict + retry), surfaced observably without reaching into the protocol.
    Raises if any writer fails (e.g. ``CasRetriesExhausted``) — a failed guarantee
    must never be swallowed.
    """
    n = len(deltas)
    workspace = Path(tempfile.mkdtemp(prefix="concurrent_writers_fixed_"))
    target = workspace / _REL
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(str(start))

    # The first instance spawns the coordinator; the rest are siblings that
    # attach to it. One instance per thread — write_cas is not thread-safe
    # within a single instance, so each writer owns its own volume.
    volumes = [CoherentVolume(workspace, managed=_MANAGED, config=_DEMO_CFG)]
    try:
        volumes += [
            CoherentVolume(workspace, managed=_MANAGED, config=_DEMO_CFG)
            for _ in range(n - 1)
        ]

        at_the_line = threading.Barrier(n)  # maximize commit overlap → exercise the race
        attempts = [0] * n
        errors: dict[int, BaseException] = {}

        def writer(i: int) -> None:
            def make_content(current: bytes) -> bytes:
                attempts[i] += 1  # one per attempt; > 1 ⇒ this writer retried
                return str(int(current.decode().strip() or "0") + deltas[i]).encode()

            try:
                at_the_line.wait()
                volumes[i].write_cas(_REL, make_content)
            except BaseException as exc:  # noqa: BLE001 — capture; re-raise in main thread
                errors[i] = exc

        threads = [threading.Thread(target=writer, args=(i,)) for i in range(n)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        if errors:
            i, exc = next(iter(errors.items()))
            raise RuntimeError(f"writer {i} failed: {exc!r}") from exc

        return int(target.read_text().strip()), attempts
    finally:
        stop_coordinator(workspace)
        shutil.rmtree(workspace, ignore_errors=True)


def run_fixed() -> dict[str, object]:
    """Two threads race ``write_cas``; both updates survive (the rung-2 analog of
    ``run_broken``). Returns a structured trace for side-by-side asserts."""
    final, attempts = _race_write_cas([_A_ADDS, _B_ADDS], _START)
    expected = _START + _A_ADDS + _B_ADDS
    return {
        "scenario": _SCENARIO,
        "expected_total": expected,  # 115
        "final_total": final,  # 115 — both updates survived
        "lost_update": final != expected,
        "attempts_a": attempts[0],
        "attempts_b": attempts[1],
        "total_attempts": sum(attempts),
        "race_observed": sum(attempts) > len(attempts),  # ≥1 writer retried
    }


def main() -> int:
    trace = run_fixed()
    print("FIXED (CoherentVolume.write_cas) — two concurrent writers, both updates survive")
    print(f"  scenario: {trace['scenario']}")
    print(
        f"  attempts: A={trace['attempts_a']}, B={trace['attempts_b']}  "
        f"(race observed: {trace['race_observed']})"
    )
    print(
        f"  LOST UPDATE: {trace['lost_update']}  "
        f"(final={trace['final_total']}, expected={trace['expected_total']})"
    )
    # Exit code reflects the invariant so an agent can use this as a gate.
    return 0 if not trace["lost_update"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
