# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents
"""Deploy-on-moved-base demo — order a deploy against the base it was planned from.

Buyer symptom (agentic SDLC / CI): "the deploy ran on a config/base that had
already moved." A build agent reads the release base (image sha + replicas),
plans a deploy from it, and then FIRES the deploy. In between, a peer promotes a
new base. Without ordering, the agent fires the deploy it planned from the OLD
base — it ships a superseded artifact and the peer's promotion is silently
skipped. `gate()` re-reads the base at the effect boundary, sees the version
moved, and HOLDs (raises the shipped `StaleView`) instead of firing on stale
state; the agent reacquires the fresh base, re-plans, and fires on it.

    python -m examples.gate_effect_ordering.main

Runs ALL THREE acts and exits 0 iff the honest contract holds:
  * negative control (no gate) FIRES the deploy on the stale base (the failure),
  * the gated path HOLDs that same deploy, then fires on the fresh base after
    reacquire, and
  * the RECLAIMED-LEASE act: the agent stalls mid-decision, the coordinator's
    sweep reclaims its grant (ownership epoch g -> g+1, version UNCHANGED), and
    the gate HOLDs the deploy anyway — revoked authority, not changed data, is
    the second axis a version-only check cannot see.

Single-host, offline, no API keys, no real deploy — it spawns a local coordinator
subprocess and the "deploy" is an in-process ledger append.

Honest boundary: `gate()` ORDERS effects (check-before-fire); it does NOT roll
back a fired deploy. For an escaping effect there is a residual re-validate->fire
window this layer narrows but cannot eliminate. Single-host, cooperative opt-in.
A pure *write* effect uses `CoherentVolume.write_cas_at` directly (the atomic,
no-window path). Sibling demo `examples/effect_gate` shows the same primitive on
a bare `replicas=` config; this one dresses it in the CI/release-base workload.
"""

from __future__ import annotations

import dataclasses
import json
import sys
import tempfile
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from ccs.adapters.claude_code.lifecycle import (  # noqa: E402  (after sys.path setup)
    LifecycleConfig,
    stop_coordinator,
)
from ccs.adapters.coherent_volume import CoherentVolume  # noqa: E402
from ccs.adapters.effect_gate import gate  # noqa: E402
from ccs.core.exceptions import StaleView  # noqa: E402

BASE = "build/base.json"
BASE_A = b'{"image": "app@sha-A", "replicas": 2}'
BASE_B = b'{"image": "app@sha-B", "replicas": 5}'


def _fast_cfg() -> LifecycleConfig:
    return LifecycleConfig(
        idle_shutdown_sec=0,
        sweep_interval_sec=0.1,
        notice_evict_max_age_sec=1.0,
        port_file_retry_attempts=20,
        port_file_retry_interval_sec=0.05,
        connect_retry_attempts=10,
        connect_retry_interval_sec=0.05,
    )


def _plan_deploy(base_bytes: bytes) -> str:
    """Turn a release base into the artifact a deploy would ship."""
    base = json.loads(base_bytes)
    return f"{base['image']} x{base['replicas']}"


def run_baseline(root: Path) -> dict:
    """Negative control: no gate. The agent plans from base@A, a peer promotes
    base@B, and the agent fires the deploy it planned anyway — shipping the
    superseded artifact. Returns a trace with the deploy ledger."""
    (root / "build").mkdir(parents=True, exist_ok=True)
    agent = CoherentVolume(root, managed=("build/**",), config=_fast_cfg())
    ledger: list[str] = []
    trace: dict = {"fired_stale": False, "shipped": None, "ledger": ledger}
    try:
        agent.write(BASE, BASE_A)
        peer = CoherentVolume(root, managed=("build/**",), config=_fast_cfg())

        base, _ = agent.read_with_version(BASE)
        plan = _plan_deploy(base)  # planned from base@A

        peer.write(BASE, BASE_B)  # base promoted to @B; no gate to catch it

        ledger.append(plan)  # deploy FIRES on the plan from the moved base
        trace["fired_stale"] = True
        trace["shipped"] = plan
        current = _plan_deploy(agent.reacquire(BASE))  # what SHOULD have shipped
        print(
            f"  x (no gate) deploy fired shipping {plan!r} while the base had "
            f"already moved to {current!r} — peer's promotion silently skipped"
        )
        return trace
    finally:
        stop_coordinator(root)


def run_gated(root: Path) -> dict:
    """With the gate: a peer promotes the base mid-plan, so the deploy is HELD;
    the agent reacquires, re-plans on base@B, and fires on fresh state."""
    (root / "build").mkdir(parents=True, exist_ok=True)
    agent = CoherentVolume(root, managed=("build/**",), config=_fast_cfg())
    ledger: list[str] = []
    trace: dict = {"held": False, "shipped": None, "ledger": ledger}
    try:
        agent.write(BASE, BASE_A)
        peer = CoherentVolume(root, managed=("build/**",), config=_fast_cfg())

        def decide(base_bytes: bytes) -> str:
            # A peer promotes the base AFTER the agent read it (fixed-stale buffer).
            peer.write(BASE, BASE_B)
            return _plan_deploy(base_bytes)

        def fire(plan: str) -> str:
            ledger.append(plan)  # the mock deploy: append to the ledger, no network
            return plan

        print("  Agent reads base, plans a deploy, gates the deploy on that version...")
        try:
            gate(agent, BASE, decide=decide, effect=fire)
            print("  x deploy FIRED on stale base (gate did not hold)")
        except StaleView as held:
            trace["held"] = True
            print(
                f"  ok HELD: base moved v{held.expected_version} -> "
                f"v{held.current_version}; deploy NOT fired on stale base"
            )

        agent.reacquire(BASE)  # recover: fresh base, then re-plan + fire
        shipped = gate(agent, BASE, decide=_plan_deploy, effect=fire)
        trace["shipped"] = shipped
        print(f"  ok after reacquire, deploy fired on fresh base: {shipped!r}")
        return trace
    finally:
        stop_coordinator(root)


def run_gated_reclaim(root: Path) -> dict:
    """Act two: the RECLAIMED grant. The worker plans a deploy, then stalls past
    its heartbeat lease; the coordinator's sweep reclaims the grant (ownership
    epoch advances) while the base bytes — and so the version — never move. The
    gate HOLDs at the effect boundary: the decision belongs to a revoked grant,
    even though a version-only comparand would still match. An OBSERVER volume
    (a different session, so its reads never refresh the worker's heartbeat)
    watches the epoch so the stall is a bounded wait on the real sweep, not a
    blind sleep."""
    (root / "build").mkdir(parents=True, exist_ok=True)
    # A short heartbeat lease so the stalled worker's grant is reclaimed by the
    # REAL background sweep in a few seconds (ticks are wall-clock secs).
    cfg = dataclasses.replace(_fast_cfg(), grant_heartbeat_timeout_sec=3)
    worker = CoherentVolume(root, managed=("build/**",), config=cfg)
    ledger: list[str] = []
    trace: dict = {
        "held": False,
        "version_unchanged": False,
        "epoch_moved": False,
        "shipped": None,
        "ledger": ledger,
    }
    try:
        # The observer exists BEFORE the worker's grant, so the write -> gate
        # window stays microseconds wide; the lease cannot lapse inside it.
        observer = CoherentVolume(root, managed=("build/**",), config=cfg)
        worker.write(BASE, BASE_A)  # M grant; the lease clock starts here

        def decide(base_bytes: bytes) -> str:
            # The stall: the worker goes quiet past its lease while the sweep
            # runs. The observer (a separate session, so its reads never renew
            # the worker's heartbeat) watches the ownership epoch RELATIVE to
            # its own first observation inside the stall — no pre-captured
            # baseline a setup-time reclaim could stale out — and resumes the
            # moment the epoch moves.
            g_first: int | None = None
            deadline = time.monotonic() + 30.0
            while time.monotonic() < deadline:
                _, _v, g = observer.read_with_version_generation(BASE)
                if g is not None:
                    if g_first is None:
                        g_first = g
                    elif g > g_first:
                        trace["epoch_moved"] = True
                        print(
                            f"  observer: base unchanged at v{_v}, but ownership "
                            f"epoch moved g{g_first} -> g{g} — the worker's grant "
                            "was reclaimed while it stalled"
                        )
                        break
                time.sleep(0.1)
            return _plan_deploy(base_bytes)

        def fire(plan: str) -> str:
            ledger.append(plan)
            return plan

        print("  Worker plans the deploy, then stalls past its heartbeat lease...")
        try:
            gate(worker, BASE, decide=decide, effect=fire)
            print("  x deploy FIRED on a reclaimed grant (gate did not hold)")
        except StaleView as held:
            trace["held"] = True
            trace["version_unchanged"] = (
                held.current_version == held.expected_version
            )
            print(
                "  ok HELD: version unchanged at "
                f"v{held.expected_version}, but the grant the decision was read "
                "under is gone — deploy NOT fired on revoked authority"
            )

        worker.reacquire(BASE)  # recover: a live grant, then re-plan + fire
        shipped = gate(worker, BASE, decide=_plan_deploy, effect=fire)
        trace["shipped"] = shipped
        print(
            f"  ok after reacquire, deploy fired under a live grant: {shipped!r} "
            "(same plan — the data never changed; the AUTHORITY had)"
        )
        return trace
    finally:
        stop_coordinator(root)


def main(argv: list[str] | None = None) -> int:
    print(
        "Deploy-on-moved-base — fire the deploy only on the release base you "
        "planned from.\n"
    )

    print("Negative control (no gate):")
    with tempfile.TemporaryDirectory() as d:
        baseline = run_baseline(Path(d))
    print("")

    print("With the gate:")
    with tempfile.TemporaryDirectory() as d:
        gated = run_gated(Path(d))
    print("")

    print("With the gate — reclaimed lease (version unchanged):")
    with tempfile.TemporaryDirectory() as d:
        reclaim = run_gated_reclaim(Path(d))

    # The failure the gate prevents: baseline shipped the stale plan (from base@A).
    baseline_fired_stale = bool(baseline["fired_stale"]) and baseline["shipped"] == (
        "app@sha-A x2"
    )
    # The fix: the gate HELD the stale deploy, then shipped the fresh plan (base@B).
    gated_held = bool(gated["held"])
    gated_fired_fresh = gated["shipped"] == "app@sha-B x5"
    gated_never_shipped_stale = "app@sha-A x2" not in gated["ledger"]
    # Act two: HELD on a reclaimed grant even though the version never moved,
    # the reclaim was actually observed, and the deploy shipped exactly once —
    # after reacquire, never while the grant was revoked.
    reclaim_held = bool(reclaim["held"]) and bool(reclaim["version_unchanged"])
    reclaim_epoch_moved = bool(reclaim["epoch_moved"])
    reclaim_fired_once_after = reclaim["ledger"] == ["app@sha-A x2"]

    print("\nContract:")
    print(f"  baseline fired the deploy on the STALE base : {baseline_fired_stale}")
    print(f"  gate HELD the stale deploy                  : {gated_held}")
    print(f"  gate fired on the FRESH base after reacquire: {gated_fired_fresh}")
    print(f"  gate never shipped the stale artifact       : {gated_never_shipped_stale}")
    print(f"  gate HELD on a reclaimed grant (same version): {reclaim_held}")
    print(f"  the reclaim was real (epoch g -> g+1 observed): {reclaim_epoch_moved}")
    print(f"  deploy shipped once, only under a live grant : {reclaim_fired_once_after}")

    print("\nTakeaway: without ordering, the agent ships an artifact planned from a")
    print("base that already moved and the peer's promotion is silently skipped. The")
    print("gate holds that deploy at the effect boundary, then fires it on fresh state")
    print("after reacquire. The reclaimed-lease act shows the second axis: the gate")
    print("holds on REVOKED AUTHORITY (ownership epoch moved) even when the data —")
    print("and so the version — never changed. Ordering, not rollback; single-host,")
    print("cooperative — the freshness check Terraform does for a saved plan,")
    print("brought to a deploy.")

    ok = (
        baseline_fired_stale
        and gated_held
        and gated_fired_fresh
        and gated_never_shipped_stale
        and reclaim_held
        and reclaim_epoch_moved
        and reclaim_fired_once_after
    )
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())

# Comparing notes on multi-agent coherence?
# https://github.com/Cohexa-ai/agent-coherence/discussions
