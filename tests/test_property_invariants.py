# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Deterministic randomized invariant tests for coordinator protocol."""

from __future__ import annotations

import random
from uuid import UUID

from ccs.coordinator.registry import ArtifactRegistry
from ccs.coordinator.service import CoordinatorService
from ccs.core.exceptions import CoherenceError, InvariantViolationError
from ccs.core.hashing import compute_content_hash
from ccs.core.invariants import check_monotonic_version, check_single_writer
from ccs.core.types import ConflictDetail, FetchRequest


def test_randomized_sequences_preserve_swmr_and_monotonic_versions() -> None:
    for seed in range(25):
        rng = random.Random(seed)
        svc = CoordinatorService(ArtifactRegistry())
        artifact = svc.register_artifact(name=f"plan-{seed}.md", content="v1")
        agents = [UUID(int=seed * 100 + i + 1) for i in range(4)]

        previous_version = artifact.version
        # No-lost-update tracking (plan Unit 7): the highest version the
        # registry has ever ACKNOWLEDGED as committed (returned a win for). The
        # registry must never regress below this — an acknowledged write that
        # later "disappears" is exactly the lost update the OCC CAS prevents.
        highest_acked_version = artifact.version
        for step in range(180):
            agent = rng.choice(agents)
            operation = rng.choice(
                ("fetch", "write", "commit", "commit_cas", "upgrade", "invalidate")
            )

            try:
                if operation == "fetch":
                    svc.fetch(
                        FetchRequest(
                            artifact_id=artifact.id,
                            requesting_agent_id=agent,
                            requested_at_tick=step,
                        )
                    )
                elif operation == "write":
                    svc.write(agent_id=agent, artifact_id=artifact.id, issued_at_tick=step)
                elif operation == "commit":
                    svc.commit(
                        agent_id=agent,
                        artifact_id=artifact.id,
                        content=f"v{step}",
                        issued_at_tick=step,
                    )
                elif operation == "commit_cas":
                    # OCC compare-and-swap. Source expected_version from the
                    # current registry version, but deliberately go stale on some
                    # steps (current - 1) so both the win branch AND the typed
                    # version_mismatch conflict are exercised. ConflictDetail is a
                    # typed RETURN (never an exception) — handle it explicitly so
                    # a lost win is not silently swallowed by the except below.
                    current = svc.registry.get_artifact(artifact.id).version
                    expected = max(0, current - rng.choice((0, 0, 1)))
                    result = svc.commit_cas(
                        agent_id=agent,
                        artifact_id=artifact.id,
                        expected_version=expected,
                        content_hash=compute_content_hash(f"cas-{step}"),
                        issued_at_tick=step,
                    )
                    if not isinstance(result, ConflictDetail):
                        updated, _signals = result
                        # A win must strictly advance and be acknowledged. Record
                        # it so a later regression below this is caught as a lost
                        # update.
                        assert updated.version == current + 1, (
                            f"CAS win must bump by exactly 1 at seed={seed} step={step}: "
                            f"current={current} got={updated.version}"
                        )
                        highest_acked_version = max(highest_acked_version, updated.version)
                elif operation == "upgrade":
                    svc.upgrade(agent_id=agent, artifact_id=artifact.id, issued_at_tick=step)
                else:
                    svc.invalidate(
                        agent_id=agent,
                        artifact_id=artifact.id,
                        new_version=svc.registry.get_artifact(artifact.id).version,
                        issuer_agent_id=agent,
                        issued_at_tick=step,
                    )
            except CoherenceError:
                # Random operation sequences intentionally include invalid actions
                # (e.g. commit_cas from an M/E holder, corruption expected>current,
                # commit without a grant). These RAISE; a lost-update would instead
                # be a silent version REGRESSION, caught by the assertion below.
                pass

            state_map = svc.registry.get_state_map(artifact.id)
            try:
                check_single_writer(state_map)
            except InvariantViolationError as exc:
                raise AssertionError(f"SWMR violation at seed={seed} step={step}: {state_map}") from exc

            current_version = svc.registry.get_artifact(artifact.id).version
            try:
                check_monotonic_version(previous_version, current_version)
            except InvariantViolationError as exc:
                raise AssertionError(
                    f"Monotonic version violation at seed={seed} step={step}: "
                    f"prev={previous_version} current={current_version}"
                ) from exc
            # No-lost-update: the registry must never fall below any version it
            # already acknowledged as committed (across the whole op sequence,
            # including the new commit_cas ops).
            assert current_version >= highest_acked_version, (
                f"lost update at seed={seed} step={step}: registry regressed to "
                f"{current_version} below acknowledged-committed {highest_acked_version}"
            )
            previous_version = current_version
