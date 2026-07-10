# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents
"""Atomic multi-file publish demo — land a coherent SET of files together, or none.

An agent edits a plan split across two files (a plan and its manifest) that must
stay consistent. It publishes them as ONE unit against the versions it read. If a
peer moves one of them first, the whole publish is HELD (rather than landing a
torn pair where the plan references a manifest that already moved); the agent
re-reads the fresh versions and publishes the set atomically. Single-host,
offline, no API keys — spawns a local coordinator subprocess.

    python -m examples.atomic_publish.main             # the atomic-publish demo
    python -m examples.atomic_publish.main --baseline  # negative control first (file-by-file -> torn pair)

Exit 0 iff the honest contract holds: with atomic_publish a moved member holds the
WHOLE publish (no file written) then lands atomically after re-read; with
--baseline, publishing file-by-file lands one file and rejects the other, leaving
a torn pair on disk — the failure the atomic publish prevents, measured against
its absence, not asserted.

Builder demo, NOT an enterprise product. The same shape is the all-or-nothing
apply serious infra already ships — a database COMMITs a multi-row transaction
whole or rolls it back. This brings that to a set of files an agent publishes.
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
from ccs.core.exceptions import CasVersionConflict, StaleView  # noqa: E402

PLAN = "proj/plan.md"
MANIFEST = "proj/manifest.md"


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
    """With atomic_publish: a peer moves the manifest before the publish, so the
    whole publish is HELD (no torn pair on disk); the agent re-reads and publishes
    the set atomically. Returns a trace."""
    (root / "proj").mkdir(parents=True, exist_ok=True)
    (root / PLAN).write_bytes(b"plan: build A\n")  # seed the coherent pair on disk
    (root / MANIFEST).write_bytes(b"manifest: A@1\n")
    agent = CoherentVolume(root, managed=("proj/**",), config=_fast_cfg())
    peer = CoherentVolume(root, managed=("proj/**",), config=_fast_cfg())
    trace: dict = {"held": False, "published": None}
    try:
        _pb, plan_v = agent.read_with_version(PLAN)  # read the pair (SHARED@v1)
        _mb, manifest_v = agent.read_with_version(MANIFEST)

        # A peer moves the manifest AFTER the agent read the coherent pair.
        peer.write_cas_at(MANIFEST, manifest_v, b"manifest: A@2 (peer)\n")

        print("  Agent publishes {plan, manifest} atomically against the versions it read...")
        try:
            agent.atomic_publish(
                [
                    (PLAN, plan_v, b"plan: build A+B\n"),
                    (MANIFEST, manifest_v, b"manifest: A,B\n"),
                ]
            )
            print("  x published a TORN pair (the hold did not fire)")
        except (StaleView, CasVersionConflict):
            trace["held"] = True
            print(
                "  ok HELD: a member moved since it was read; the WHOLE publish "
                "was held — neither file written (no torn pair)."
            )

        # The plan was NOT written (all-or-nothing) — it still holds its pre-publish
        # bytes. Recover: re-read the fresh versions, then re-publish the set atomically.
        agent.reacquire(PLAN)
        _pb2, plan_v2 = agent.read_with_version(PLAN)
        _mb2, manifest_v2 = agent.read_with_version(MANIFEST)
        versions = agent.atomic_publish(
            [
                (PLAN, plan_v2, b"plan: build A+B\n"),
                (MANIFEST, manifest_v2, b"manifest: A,B\n"),
            ]
        )
        trace["published"] = versions
        print(f"  ok after re-read, both files published atomically: {versions}")
        return trace
    finally:
        stop_coordinator(root)


def run_baseline(root: Path) -> dict:
    """Negative control: no atomic_publish. The agent publishes the pair
    file-by-file; a peer moves the manifest between the two writes, so the plan
    lands but the manifest CAS is rejected — a TORN pair on disk. Returns a trace."""
    (root / "proj").mkdir(parents=True, exist_ok=True)
    (root / PLAN).write_bytes(b"plan: build A\n")  # seed the coherent pair on disk
    (root / MANIFEST).write_bytes(b"manifest: A@1\n")
    agent = CoherentVolume(root, managed=("proj/**",), config=_fast_cfg())
    peer = CoherentVolume(root, managed=("proj/**",), config=_fast_cfg())
    trace: dict = {"torn": False}
    try:
        _pb, plan_v = agent.read_with_version(PLAN)  # read the pair (SHARED@v1)
        _mb, manifest_v = agent.read_with_version(MANIFEST)

        # File-by-file publish: the plan lands first ...
        agent.write_cas_at(PLAN, plan_v, b"plan: build A+B\n")
        # ... then a peer moves the manifest, so the agent's manifest CAS is rejected.
        peer.write_cas_at(MANIFEST, manifest_v, b"manifest: A@2 (peer)\n")
        try:
            agent.write_cas_at(MANIFEST, manifest_v, b"manifest: A,B\n")
        except CasVersionConflict:
            pass

        plan_disk = (root / PLAN).read_bytes()
        manifest_disk = (root / MANIFEST).read_bytes()
        # Torn: the plan references B, but the manifest on disk does not — the pair
        # the agent meant to publish together is half-applied.
        trace["torn"] = b"A+B" in plan_disk and b"A,B" not in manifest_disk
        print(
            f"  x (no atomic_publish) TORN pair on disk: "
            f"plan={plan_disk.decode().strip()!r} / manifest={manifest_disk.decode().strip()!r}"
        )
        return trace
    finally:
        stop_coordinator(root)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Atomic multi-file publish demo (single-host)."
    )
    parser.add_argument(
        "--baseline",
        action="store_true",
        help="run the file-by-file negative control first (torn pair lands)",
    )
    args = parser.parse_args(argv)

    print("Atomic multi-file publish demo — land the whole set, or nothing.\n")

    baseline_ok = True
    if args.baseline:
        print("Negative control (file-by-file, no atomic_publish):")
        with tempfile.TemporaryDirectory() as d:
            baseline = run_baseline(Path(d))
        baseline_ok = bool(baseline["torn"])  # the failure atomic_publish prevents
        print("")

    print("With atomic_publish:")
    with tempfile.TemporaryDirectory() as d:
        gated = run_gated(Path(d))

    print("\nTakeaway: atomic_publish lands a coherent set of files together or not")
    print("at all — a moved member holds the WHOLE publish (no torn pair on disk),")
    print("then it lands atomically after a re-read. All-or-nothing, single-host,")
    print("cooperative. The same whole-or-nothing apply a database COMMIT gives a")
    print("transaction, brought to a set of files an agent publishes.")

    gated_ok = bool(gated["held"]) and gated["published"] is not None
    ok = gated_ok and baseline_ok
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())

# Comparing notes on multi-agent coherence?
# https://github.com/hipvlady/agent-coherence/discussions
