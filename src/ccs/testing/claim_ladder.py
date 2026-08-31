# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""The machine-checked claim ladder (guarantee-ladder plan U7, R-6/KTD-10).

Every guarantee the project claims is a RUNG: a stable slug, the guarantee
text, the exact README claim-phrases it backs (string-pinned), and the pytest
node-ids of the tests that would fail without it. The repo-side meta-test
(``tests/conformance/test_claim_ladder.py``) resolves every node-id at
collection time and drift-guards the pins against the README in BOTH
directions — a claim without a proving test, or a proving test naming a rung
nobody claims, fails CI with the rung named.

The registry ships in the package so a foreign consumer of the conformance
kit sees the rungs and guarantee text; the node-id resolution lives in the
repo test tree only, so importing the kit never inherits repo-local test
references.

**There is deliberately NO cross-host rung.** The single-coordinator boundary
is the ladder's ceiling today; the meta-test asserts the absence so a
cross-host claim can only ever be added together with the tests that earn it.
"""

from __future__ import annotations

from dataclasses import dataclass

__all__ = ["CLAIM_LADDER", "ClaimRung"]


@dataclass(frozen=True)
class ClaimRung:
    """One claimed guarantee level.

    - ``rung`` — stable slug (add rungs, never rename one).
    - ``guarantee`` — the claim, in the same honest wording the README carries.
    - ``readme_pins`` — exact substrings that must appear in the README; each
      claim-table row lead must be some rung's pin (the two-way drift guard).
    - ``proving_tests`` — repo-relative pytest node-ids that would fail if the
      guarantee regressed. Resolved by the repo meta-test, opaque to foreign
      consumers.
    """

    rung: str
    guarantee: str
    readme_pins: tuple[str, ...]
    proving_tests: tuple[str, ...]


CLAIM_LADDER: tuple[ClaimRung, ...] = (
    ClaimRung(
        rung="sequential-single-host",
        guarantee=(
            "Single host, cooperative writers, one coordinator: a sequentially "
            "stale read-then-write is denied fail-closed (the writer must "
            "re-read); multi-artifact reads serve from a pinned consistent cut "
            "(no read-skew inside a session); a dead owner's grant is "
            "reclaimed by the heartbeat/TTL sweep so the fleet never blocks "
            "forever."
        ),
        readme_pins=(
            "Stale-read overwrite",
            "Torn multi-artifact read (read-skew)",
            "Dead owner blocks the fleet",
        ),
        proving_tests=(
            "tests/test_coherent_volume_demo.py::test_fixed_denies_stale_write_then_recovers",
            "tests/test_snapshot_cut_capture.py::TestConsistentCut::"
            "test_peer_commit_between_reads_does_not_taint_cut",
            "tests/test_crash_recovery.py::test_heartbeat_stale_exclusive_is_reclaimed",
        ),
    ),
    ClaimRung(
        rung="concurrent-single-host-occ-fence",
        guarantee=(
            "Concurrent same-key writers on one host: exactly one commit-CAS "
            "winner, the loser gets a typed conflict (never a silent drop); a "
            "sweep-reclaimed zombie's commit is rejected by the "
            "read-generation fence even with the version unmoved; the same "
            "zombie's escaping effect is HELD at the gate() boundary on the "
            "(version, ownership-generation) pair."
        ),
        readme_pins=(
            "Concurrent lost update",
            "Reclaim-zombie write",
            "Reclaim-zombie effect",
        ),
        proving_tests=(
            "tests/test_registry.py::test_commit_cas_version_mismatch_no_mutation",
            "tests/test_fencing.py::test_parity_commit_cas_fence_rejects_superseded_reader",
            "tests/adapters/test_effect_gate_wrapper.py::"
            "test_escaping_effect_held_when_grant_reclaimed_version_unchanged",
        ),
    ),
    ClaimRung(
        rung="cross-process-single-host",
        guarantee=(
            "The single-host guarantees hold across OS PROCESS boundaries, "
            "not just threads: racing writers in separate spawned processes "
            "get exactly one winner (substrate CAS and coordinator commit "
            "alike), the zombie fence rejects across processes with the "
            "version unmoved, and an acknowledged sqlite commit survives "
            "SIGKILL of its writer (process-crash durability grade; deeper "
            "grades are declared exemptions)."
        ),
        readme_pins=("plain files shared across processes",),
        proving_tests=(
            "tests/conformance/test_cross_process_substrate.py::"
            "test_two_contenders_native_cas_exactly_one_winner",
            "tests/conformance/test_cross_process_coordinator.py::"
            "test_two_client_processes_racing_commits_exactly_one_wins",
            "tests/conformance/test_cross_process_coordinator.py::"
            "test_zombie_fence_version_unmoved_stale_read_generation",
            "tests/conformance/test_durability_regime.py::"
            "test_sqlite_acknowledged_commit_survives_sigkill",
        ),
    ),
)
