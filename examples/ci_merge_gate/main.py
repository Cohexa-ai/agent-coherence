# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents
"""CI merge-gate demo — merge a PR only against the base SHA its CI validated.

Buyer symptom (agentic SDLC / CI): "three agents, three clean PRs, three green
CI runs, one broken product." An agent validates its PR against base@SHA-A, a
peer's PR merges first and moves the base to SHA-B, and the first agent's merge
fires against a base that no longer exists — its green CI run never saw the
peer's changes, so the integration break lands silently. `gate()` re-reads the
base pointer at the effect boundary, sees the version moved, and HOLDs (raises
the shipped `StaleView`) instead of merging on stale validation; the agent
reacquires the fresh base, re-validates against it, and merges cleanly.

    python -m examples.ci_merge_gate.main             # the with-gate demo
    python -m examples.ci_merge_gate.main --baseline  # negative control first (no gate -> stale merge fires)

Exit 0 iff the honest contract holds: with the gate the stale merge is HELD then
fires against the fresh base after reacquire; with --baseline, the no-gate path
merges on the stale validation (the failure the gate prevents) — the hold is
measured against its absence, not asserted.

Single-host, offline, no network, no git — the repo state is a mock: one JSON
file holds the base-branch SHA plus a commit log, and the "merge" is an
in-process ledger append.

Honest boundary: `gate()` ORDERS effects (check-before-fire); it never rolls
back a fired merge. For an escaping effect (a real merge API call) there is a
residual re-check->fire window this layer narrows but cannot close. Cooperative
opt-in, single-host. The same TOCTOU that Renovate hit branch-side (#18804) and
the same freshness check Terraform ships ("Error: Saved plan is stale") and
Atlantis needs for stale plans — brought to an agent's merge decision.
"""

from __future__ import annotations

import argparse
import json
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

BASE = "ci/base.json"
SHA_A = "a1f9c04"
SHA_B = "b7e2d55"
BASE_AT_A = {
    "sha": SHA_A,
    "log": [{"sha": SHA_A, "subject": "chore(trunk): baseline"}],
}


def _fast_cfg() -> LifecycleConfig:
    return LifecycleConfig(
        idle_shutdown_sec=0,
        sweep_interval_sec=0.1,
        notice_evict_max_age_sec=1.0,
        port_file_retry_attempts=40,
        port_file_retry_interval_sec=0.05,
        connect_retry_attempts=20,
        connect_retry_interval_sec=0.05,
    )


def _validate_pr(base_bytes: bytes) -> str:
    """Mock CI: validate PR #1 against the base SHA it sees, decide the merge."""
    base = json.loads(base_bytes)
    return f"merge PR#1 onto {base['sha']} (CI green vs {base['sha']})"


def _peer_merge(base_bytes: bytes) -> bytes:
    """Peer B's PR #2 lands first: append its commit, move the base pointer."""
    base = json.loads(base_bytes)
    base["log"].append({"sha": SHA_B, "subject": "feat(api): PR#2 merged by peer"})
    base["sha"] = SHA_B
    return json.dumps(base).encode()


def run_baseline(root: Path) -> dict:
    """Negative control: no gate. Agent A validates PR#1 against base@SHA-A, peer
    B merges PR#2 first (base -> SHA-B), and A's merge fires anyway — against a
    base that no longer exists. Returns a trace with the merge ledger."""
    (root / "ci").mkdir(parents=True, exist_ok=True)
    agent = CoherentVolume(root, managed=("ci/**",), config=_fast_cfg())
    ledger: list[str] = []
    trace: dict = {"fired_stale": False, "merged": None, "base_now": None, "ledger": ledger}
    try:
        agent.write(BASE, json.dumps(BASE_AT_A).encode())
        peer = CoherentVolume(root, managed=("ci/**",), config=_fast_cfg())

        base_bytes, _ = agent.read_with_version(BASE)
        decision = _validate_pr(base_bytes)  # CI green — against base@SHA-A

        peer.write(BASE, _peer_merge(base_bytes))  # PR#2 lands; base moves to SHA-B

        ledger.append(decision)  # A's merge FIRES against the moved base
        trace["fired_stale"] = True
        trace["merged"] = decision
        base_now = json.loads(agent.reacquire(BASE))["sha"]
        trace["base_now"] = base_now
        print(
            f"  x (no gate) merge fired: {decision!r} while the base had already "
            f"moved to {base_now} — PR#1's green CI never saw PR#2's changes"
        )
        return trace
    finally:
        stop_coordinator(root)


def run_gated(root: Path) -> dict:
    """With the gate: peer B merges mid-validation, so A's merge is HELD; A
    reacquires the fresh base, re-validates against SHA-B, and merges cleanly."""
    (root / "ci").mkdir(parents=True, exist_ok=True)
    agent = CoherentVolume(root, managed=("ci/**",), config=_fast_cfg())
    ledger: list[str] = []
    trace: dict = {"held": False, "merged": None, "ledger": ledger}
    try:
        agent.write(BASE, json.dumps(BASE_AT_A).encode())
        peer = CoherentVolume(root, managed=("ci/**",), config=_fast_cfg())

        def validate(base_bytes: bytes) -> str:
            # Peer B's PR#2 merges AFTER agent A read the base (fixed-stale buffer).
            peer.write(BASE, _peer_merge(base_bytes))
            return _validate_pr(base_bytes)

        def merge(decision: str) -> str:
            ledger.append(decision)  # the mock merge: ledger append, no network
            return decision

        print("  Agent A validates PR#1 against the base, gates the merge on that version...")
        try:
            gate(agent, BASE, decide=validate, effect=merge)
            print("  x merge FIRED on stale base (gate did not hold)")
        except StaleView as held:
            trace["held"] = True
            print(
                f"  ok HELD: base moved v{held.expected_version} -> "
                f"v{held.current_version}; merge NOT fired on the stale validation"
            )

        agent.reacquire(BASE)  # recover: fresh base, then re-validate + merge
        merged = gate(agent, BASE, decide=_validate_pr, effect=merge)
        trace["merged"] = merged
        print(f"  ok after reacquire, merge fired against the fresh base: {merged!r}")
        return trace
    finally:
        stop_coordinator(root)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="CI merge-gate demo (single-host)."
    )
    parser.add_argument(
        "--baseline",
        action="store_true",
        help="run the no-gate negative control first (stale merge fires)",
    )
    args = parser.parse_args(argv)

    print(
        "CI merge-gate — merge a PR only against the base SHA its CI actually "
        "validated.\n"
    )

    baseline_ok = True
    if args.baseline:
        print("Negative control (no gate):")
        with tempfile.TemporaryDirectory() as d:
            baseline = run_baseline(Path(d))
        baseline_ok = (
            bool(baseline["fired_stale"])  # the failure the gate prevents
            and baseline["merged"] == f"merge PR#1 onto {SHA_A} (CI green vs {SHA_A})"
            and baseline["base_now"] == SHA_B
        )
        print("")

    print("With the gate:")
    with tempfile.TemporaryDirectory() as d:
        gated = run_gated(Path(d))

    gated_held = bool(gated["held"])
    gated_fired_fresh = gated["merged"] == f"merge PR#1 onto {SHA_B} (CI green vs {SHA_B})"
    gated_never_merged_stale = (
        f"merge PR#1 onto {SHA_A} (CI green vs {SHA_A})" not in gated["ledger"]
    )

    print("\nContract:")
    if args.baseline:
        print(f"  baseline merged against the STALE base       : {baseline_ok}")
    print(f"  gate HELD the stale merge                    : {gated_held}")
    print(f"  gate merged against the FRESH base           : {gated_fired_fresh}")
    print(f"  gate never merged on the stale validation    : {gated_never_merged_stale}")

    print("\nTakeaway: without ordering, the agent merges a PR whose green CI ran")
    print("against a base that no longer exists — the integration break lands")
    print("silently. The gate holds that merge at the effect boundary, then fires")
    print("it against the fresh base after reacquire. Ordering, not rollback;")
    print("single-host, cooperative — the freshness check Terraform does for a")
    print("saved plan, brought to an agent's merge decision.")

    ok = baseline_ok and gated_held and gated_fired_fresh and gated_never_merged_stale
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())

# Comparing notes on multi-agent coherence?
# https://github.com/Cohexa-ai/agent-coherence/discussions
