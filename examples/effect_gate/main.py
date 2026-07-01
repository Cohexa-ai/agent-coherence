# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents
"""Effect-ordering gate demo — hold a deploy on a moved input, fire after reacquire.

An agent reads shared config, decides a deploy, and gates the deploy on the config
version it decided from. If a peer moves the config first, the gate HOLDs the
deploy (it never fires on stale input); the agent reacquires fresh config,
re-decides, and fires. Single-host, offline, no API keys — spawns a local
coordinator subprocess.

    python -m examples.effect_gate.main             # the with-gate demo
    python -m examples.effect_gate.main --baseline  # negative control first (no gate -> stale deploy fires)

Exit 0 iff the honest contract holds: with the gate the stale deploy is HELD then
fires on fresh state; with --baseline, the no-gate path fires on stale input (the
failure the gate prevents) — the hold is measured against its absence, not asserted.

Builder demo, NOT an enterprise CI product. The same shape is the freshness check
serious infra already ships — Terraform refuses to apply a plan built on a moved
state ("Error: Saved plan is stale"). The gate brings that to an agent's effect.
"""

from __future__ import annotations

import argparse
import sys
import tempfile
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

CONFIG = "deploy/config.txt"


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


def run_gated(root: Path) -> dict:
    """With the gate: a peer moves config mid-decision, so the deploy is HELD;
    the agent reacquires and fires on fresh state. Returns a trace."""
    (root / "deploy").mkdir(parents=True, exist_ok=True)
    agent = CoherentVolume(root, managed=("deploy/**",), config=_fast_cfg())
    trace: dict = {"held": False, "fired_on": None}
    try:
        agent.write(CONFIG, b"replicas=2")
        peer = CoherentVolume(root, managed=("deploy/**",), config=_fast_cfg())

        def decide(cfg: bytes) -> str:
            # A peer changes the config AFTER the agent read it (fixed-stale buffer).
            peer.write(CONFIG, b"replicas=5")
            return f"deploy({cfg.decode()})"

        def do_deploy(decision: str) -> str:
            return decision

        print("  Agent reads config, decides a deploy, gates it on that version...")
        try:
            gate(agent, CONFIG, decide=decide, effect=do_deploy)
            print("  x deploy FIRED on stale config (gate did not hold)")
        except StaleView as held:
            trace["held"] = True
            print(
                f"  ok HELD: config moved v{held.expected_version} -> "
                f"v{held.current_version}; deploy NOT fired on stale input"
            )

        agent.reacquire(CONFIG)  # recover: fresh view, then re-decide + fire
        fresh = gate(
            agent, CONFIG, decide=lambda c: f"deploy({c.decode()})", effect=do_deploy
        )
        trace["fired_on"] = fresh
        print(f"  ok after reacquire, deploy fired on fresh config: {fresh}")
        return trace
    finally:
        stop_coordinator(root)


def run_baseline(root: Path) -> dict:
    """Negative control: no gate. The agent fires the deploy on config it read
    before a peer moved it, so the stale deploy lands. Returns a trace."""
    (root / "deploy").mkdir(parents=True, exist_ok=True)
    agent = CoherentVolume(root, managed=("deploy/**",), config=_fast_cfg())
    trace: dict = {"fired_stale": False}
    try:
        agent.write(CONFIG, b"replicas=2")
        peer = CoherentVolume(root, managed=("deploy/**",), config=_fast_cfg())

        cfg, _ = agent.read_with_version(CONFIG)
        decision = f"deploy({cfg.decode()})"
        peer.write(CONFIG, b"replicas=5")  # config moves; no gate to catch it
        print(
            f"  x (no gate) deploy fired on STALE config: {decision} "
            "while config already moved to replicas=5"
        )
        trace["fired_stale"] = True
        return trace
    finally:
        stop_coordinator(root)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Effect-ordering gate demo (single-host)."
    )
    parser.add_argument(
        "--baseline",
        action="store_true",
        help="run the no-gate negative control first (stale deploy fires)",
    )
    args = parser.parse_args(argv)

    print("Effect-ordering gate demo — fire the deploy only on the config you decided from.\n")

    baseline_ok = True
    if args.baseline:
        print("Negative control (no gate):")
        with tempfile.TemporaryDirectory() as d:
            baseline = run_baseline(Path(d))
        baseline_ok = bool(baseline["fired_stale"])  # the failure the gate prevents
        print("")

    print("With the gate:")
    with tempfile.TemporaryDirectory() as d:
        gated = run_gated(Path(d))

    print("\nTakeaway: the gate holds an effect that would fire on a version that")
    print("already moved, then fires it on fresh state after reacquire. Ordering,")
    print("not rollback; single-host, cooperative. The same freshness check")
    print("Terraform does for a saved plan, brought to an agent's effect.")

    gated_ok = bool(gated["held"]) and gated["fired_on"] is not None
    ok = gated_ok and baseline_ok
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())

# Comparing notes on multi-agent coherence?
# https://github.com/hipvlady/agent-coherence/discussions
