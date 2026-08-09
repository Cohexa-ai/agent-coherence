# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""``CoordinatorService.register_workspace_restore`` — WV plan Unit 5 (R4/R5).

Service-level coverage of the restore-registration primitive, run against BOTH
registries (the fencing-suite house pattern) so the resolution split is
parity-proven:

- the durable registry resolves member paths through ``resolve_or_register``
  (SqliteExtended); the in-memory registry through the service's name-scan +
  hash-only first-observation seed — BOTH mint at version 1 carrying the
  fingerprint, and neither ever stores member bytes;
- an empty write-set answers the typed ``empty_write_set`` and NEVER calls
  ``commit_all`` (which raises on empty, by contract);
- hash-identical members are ``skipped`` (the exactly-once filter);
- a differing member commits through ONE all-or-nothing ``commit_all``:
  monotonic forward version, peers invalidated atomically, signals returned;
- HELD batches surface as the typed ``refused`` (``other_holder`` /
  ``stale_read_generation`` — the fence rejecting a superseded controller);
- the abort Event threads into the commit path (A6) and fails it closed;
- the new ``registered`` restore status round-trips both registries.
"""

from __future__ import annotations

import threading
from pathlib import Path
from uuid import uuid4

import pytest

from ccs.coordinator.registry import ArtifactRegistry
from ccs.coordinator.registry_protocol import CheckpointMember
from ccs.coordinator.service import CoordinatorService
from ccs.coordinator.sqlite_registry import SqliteArtifactRegistry
from ccs.core.exceptions import (
    CHECKPOINT_UNKNOWN_REASON,
    RESTORE_STATUS_REGISTERED,
    STALE_READ_GENERATION_REASON,
    WORKSPACE_REGISTRATION_COMMITTED,
    WORKSPACE_REGISTRATION_EMPTY,
    WORKSPACE_REGISTRATION_REFUSED,
    CheckpointUnknown,
    WatchdogAbandoned,
)
from ccs.core.states import MESIState
from ccs.core.substrate import sha256_hex
from ccs.core.types import WorkspaceRestoreWrite

OWNER = uuid4()

FP_OLD = sha256_hex(b"old-body")
FP_NEW = sha256_hex(b"restored-body")


class _CountingService(CoordinatorService):
    """Counts commit_all invocations (the never-called-on-empty proof)."""

    def __init__(self, registry) -> None:
        super().__init__(registry)
        self.commit_all_calls = 0

    def commit_all(self, **kwargs):
        self.commit_all_calls += 1
        return super().commit_all(**kwargs)


@pytest.fixture(params=["in_memory", "sqlite"])
def registry(request, tmp_path: Path):
    """Both registry implementations, identically (the fencing house shape)."""
    if request.param == "in_memory":
        yield ArtifactRegistry()
    else:
        with SqliteArtifactRegistry(tmp_path / "state.db") as reg:
            yield reg


@pytest.fixture
def service(registry) -> _CountingService:
    return _CountingService(registry)


def _mint_checkpoint(service: CoordinatorService) -> str:
    member = CheckpointMember(
        member_path="notes/plan.md",
        artifact_id=None,
        native_token="7",
        fingerprint=FP_NEW,
        captured_at=100.0,
    )
    record = service.create_workspace_checkpoint(
        name="reg-cp",
        owner=OWNER,
        members=[member],
        window_min=100.0,
        window_max=100.0,
        issued_at_tick=101,
    )
    return record.checkpoint_id


def _artifact_named(registry, name: str):
    for artifact_id in registry.artifact_ids():
        artifact = registry.get_artifact(artifact_id)
        if artifact is not None and artifact.name == name:
            return artifact
    return None


# ---------------------------------------------------------------------------
# Typed empties and pre-flight refusals
# ---------------------------------------------------------------------------


def test_empty_writes_typed_empty_never_calls_commit_all(service) -> None:
    checkpoint_id = _mint_checkpoint(service)
    result = service.register_workspace_restore(
        checkpoint_id=checkpoint_id, controller=OWNER, writes=()
    )
    assert result.status is WORKSPACE_REGISTRATION_EMPTY
    assert result.versions == {} and result.signals == ()
    assert service.commit_all_calls == 0


def test_unknown_checkpoint_raises_typed_checkpoint_unknown(service) -> None:
    with pytest.raises(CheckpointUnknown) as excinfo:
        service.register_workspace_restore(
            checkpoint_id="no-such-checkpoint",
            controller=OWNER,
            writes=(WorkspaceRestoreWrite("notes/plan.md", FP_NEW),),
        )
    assert excinfo.value.reason is CHECKPOINT_UNKNOWN_REASON


def test_duplicate_paths_and_blank_fingerprint_rejected(service, registry) -> None:
    checkpoint_id = _mint_checkpoint(service)
    ids_before = set(registry.artifact_ids())
    with pytest.raises(ValueError, match="duplicate member_path"):
        service.register_workspace_restore(
            checkpoint_id=checkpoint_id,
            controller=OWNER,
            writes=(
                WorkspaceRestoreWrite("notes/plan.md", FP_NEW),
                WorkspaceRestoreWrite("notes/plan.md", FP_OLD),
            ),
        )
    with pytest.raises(ValueError, match="fingerprint"):
        service.register_workspace_restore(
            checkpoint_id=checkpoint_id,
            controller=OWNER,
            writes=(WorkspaceRestoreWrite("notes/plan.md", ""),),
        )
    # Nothing resolved or minted by either reject (fail-closed before work).
    assert set(registry.artifact_ids()) == ids_before
    assert service.commit_all_calls == 0


# ---------------------------------------------------------------------------
# Resolution — hash-only, first-observation parity across both registries
# ---------------------------------------------------------------------------


def test_first_observation_mints_hash_only_at_v1_and_skips(service, registry) -> None:
    """A path the coordinator never saw: the mint IS the registration —
    version 1 carrying the fingerprint, no commit, no member bytes stored."""
    checkpoint_id = _mint_checkpoint(service)
    result = service.register_workspace_restore(
        checkpoint_id=checkpoint_id,
        controller=OWNER,
        writes=(WorkspaceRestoreWrite("notes/plan.md", FP_NEW),),
    )
    assert result.status is WORKSPACE_REGISTRATION_EMPTY
    assert result.skipped == ("notes/plan.md",)
    assert service.commit_all_calls == 0

    artifact = _artifact_named(registry, "notes/plan.md")
    assert artifact is not None
    assert artifact.version == 1
    assert artifact.content_hash == FP_NEW
    # Hash-only: the coordinator holds NO member bytes for the mint.
    content = registry.get_content(artifact.id)
    assert content in (None, "", b"")

    # Idempotent: a second registration resolves the SAME artifact and skips.
    ids_before = set(registry.artifact_ids())
    again = service.register_workspace_restore(
        checkpoint_id=checkpoint_id,
        controller=OWNER,
        writes=(WorkspaceRestoreWrite("notes/plan.md", FP_NEW),),
    )
    assert again.status is WORKSPACE_REGISTRATION_EMPTY
    assert set(registry.artifact_ids()) == ids_before
    assert _artifact_named(registry, "notes/plan.md").version == 1


# ---------------------------------------------------------------------------
# The commit path — monotonic forward version, invalidation, all-or-nothing
# ---------------------------------------------------------------------------


def test_known_artifact_commits_forward_and_invalidates_peer(service, registry) -> None:
    checkpoint_id = _mint_checkpoint(service)
    artifact = service.register_artifact(
        name="notes/plan.md", content="old-body", content_hash=FP_OLD
    )
    peer = uuid4()
    registry.set_agent_state(artifact.id, peer, MESIState.SHARED, trigger="fetch", tick=1)

    result = service.register_workspace_restore(
        checkpoint_id=checkpoint_id,
        controller=OWNER,
        writes=(WorkspaceRestoreWrite("notes/plan.md", FP_NEW),),
        issued_at_tick=7,
    )

    assert result.status is WORKSPACE_REGISTRATION_COMMITTED
    assert result.versions == {"notes/plan.md": 2}
    assert service.commit_all_calls == 1
    updated = registry.get_artifact(artifact.id)
    assert updated.version == 2  # forward, never a re-stamp or decrement
    assert updated.content_hash == FP_NEW
    # The registered peer was invalidated atomically; the signal names it.
    assert registry.get_agent_state(artifact.id, peer) == MESIState.INVALID
    (signal,) = result.signals
    assert signal.artifact_id == artifact.id
    assert signal.new_version == 2
    assert signal.issuer_agent_id == OWNER


def test_hash_identical_member_skipped_no_bump(service, registry) -> None:
    checkpoint_id = _mint_checkpoint(service)
    artifact = service.register_artifact(
        name="notes/plan.md", content="restored-body", content_hash=FP_NEW
    )
    result = service.register_workspace_restore(
        checkpoint_id=checkpoint_id,
        controller=OWNER,
        writes=(WorkspaceRestoreWrite("notes/plan.md", FP_NEW),),
    )
    assert result.status is WORKSPACE_REGISTRATION_EMPTY
    assert result.skipped == ("notes/plan.md",)
    assert registry.get_artifact(artifact.id).version == 1
    assert service.commit_all_calls == 0


def test_pessimistic_holder_refuses_batch_all_or_nothing(service, registry) -> None:
    """One member blocked by an M/E holder HOLDS the whole batch: nothing
    registered, per-member typed reasons returned."""
    checkpoint_id = _mint_checkpoint(service)
    blocked = service.register_artifact(
        name="notes/plan.md", content="old-body", content_hash=FP_OLD
    )
    healthy = service.register_artifact(
        name="docs/readme.md", content="old-doc", content_hash=sha256_hex(b"old-doc")
    )
    holder = uuid4()
    registry.set_agent_state(
        blocked.id, holder, MESIState.EXCLUSIVE, trigger="write", tick=1
    )

    result = service.register_workspace_restore(
        checkpoint_id=checkpoint_id,
        controller=OWNER,
        writes=(
            WorkspaceRestoreWrite("notes/plan.md", FP_NEW),
            WorkspaceRestoreWrite("docs/readme.md", sha256_hex(b"new-doc")),
        ),
    )

    assert result.status is WORKSPACE_REGISTRATION_REFUSED
    assert result.refused["notes/plan.md"].reason == "other_holder"
    # All-or-nothing: the healthy member did NOT advance either.
    assert registry.get_artifact(blocked.id).version == 1
    assert registry.get_artifact(healthy.id).version == 1
    assert result.versions == {} and result.signals == ()


def test_superseded_controller_refused_by_fence(service, registry) -> None:
    """The read-generation fence at the service seam: a controller whose grant
    a sweep reclaimed is rejected stale_read_generation — the late apply never
    lands (no phantom bump)."""
    checkpoint_id = _mint_checkpoint(service)
    artifact = service.register_artifact(
        name="notes/plan.md", content="old-body", content_hash=FP_OLD
    )
    registry.set_agent_state(
        artifact.id, OWNER, MESIState.EXCLUSIVE, trigger="write", tick=1
    )
    registry.set_agent_state(
        artifact.id, OWNER, MESIState.INVALID, trigger="reclaim_heartbeat", tick=10
    )

    result = service.register_workspace_restore(
        checkpoint_id=checkpoint_id,
        controller=OWNER,
        writes=(WorkspaceRestoreWrite("notes/plan.md", FP_NEW),),
    )

    assert result.status is WORKSPACE_REGISTRATION_REFUSED
    assert result.refused["notes/plan.md"].reason == STALE_READ_GENERATION_REASON
    updated = registry.get_artifact(artifact.id)
    assert updated.version == 1
    assert updated.content_hash == FP_OLD


def test_abort_event_fails_commit_path_closed(service, registry) -> None:
    """A6: a pre-set abort Event (the watchdog already timed out) fails the
    registration closed AT the registry write lock — no version bump lands
    after the client saw the degraded response."""
    checkpoint_id = _mint_checkpoint(service)
    artifact = service.register_artifact(
        name="notes/plan.md", content="old-body", content_hash=FP_OLD
    )
    abort = threading.Event()
    abort.set()
    with pytest.raises(WatchdogAbandoned):
        service.register_workspace_restore(
            checkpoint_id=checkpoint_id,
            controller=OWNER,
            writes=(WorkspaceRestoreWrite("notes/plan.md", FP_NEW),),
            abort=abort,
        )
    assert registry.get_artifact(artifact.id).version == 1
    assert registry.get_artifact(artifact.id).content_hash == FP_OLD


# ---------------------------------------------------------------------------
# The 'registered' restore status (the Unit-5 durable idempotency marker)
# ---------------------------------------------------------------------------


def test_registered_status_accepted_and_round_trips(service, registry) -> None:
    checkpoint_id = _mint_checkpoint(service)
    service.set_workspace_checkpoint_restore_status(
        checkpoint_id, RESTORE_STATUS_REGISTERED, updated_at=123.0
    )
    record = registry.get_checkpoint(checkpoint_id)
    assert record is not None
    assert record.restore_status == RESTORE_STATUS_REGISTERED
    assert record.restore_updated_at == 123.0


# ---------------------------------------------------------------------------
# The read-only registration query (the FIX-1 rebuild seam)
# ---------------------------------------------------------------------------


def test_workspace_member_registered_pure_read_never_mints(service, registry) -> None:
    """workspace_member_registered answers the idempotency-filter predicate as
    a PURE read: unknown path → False with nothing minted; known path → the
    content-hash comparison; never a resolve, never a mutation."""
    ids_before = set(registry.artifact_ids())
    assert service.workspace_member_registered("notes/plan.md", FP_NEW) is False
    assert set(registry.artifact_ids()) == ids_before  # no first-observation mint

    artifact = service.register_artifact(
        name="notes/plan.md", content="old-body", content_hash=FP_OLD
    )
    assert service.workspace_member_registered("notes/plan.md", FP_NEW) is False
    assert service.workspace_member_registered("notes/plan.md", FP_OLD) is True
    assert registry.get_artifact(artifact.id).version == 1  # untouched either way


# ---------------------------------------------------------------------------
# The in-memory first-observation mint race (the FIX-4 serialization)
# ---------------------------------------------------------------------------


class _ScanGateRegistry(ArtifactRegistry):
    """``artifact_ids()`` blocks ONCE mid-scan — widens the scan→mint window
    so the two-thread first-observation race is deterministic, not timing-
    dependent."""

    def __init__(self) -> None:
        super().__init__()
        self.scan_entered = threading.Event()
        self.release_scan = threading.Event()
        self._armed = True

    def artifact_ids(self):
        ids = super().artifact_ids()
        if self._armed:
            self._armed = False
            self.scan_entered.set()
            assert self.release_scan.wait(timeout=5.0)
        return ids


def test_in_memory_first_observation_race_mints_one_artifact() -> None:
    """FIX-4 regression (in-memory mint race): two concurrent first
    observations of one never-seen member_path on the IN-MEMORY registry must
    resolve to ONE artifact — the scan-then-mint is serialized service-side,
    honoring the parity claim with sqlite's atomic resolve_or_register."""
    registry = _ScanGateRegistry()
    service = CoordinatorService(registry)
    results: list = []

    def resolve() -> None:
        results.append(
            service._resolve_workspace_member_artifact("notes/plan.md", FP_NEW)
        )

    first = threading.Thread(target=resolve)
    first.start()
    assert registry.scan_entered.wait(timeout=5.0)  # first observer is mid-scan
    second = threading.Thread(target=resolve)
    second.start()
    # Serialized: the second observer must NOT complete while the first still
    # holds the mint lock mid-scan (pre-fix it minted a duplicate right here).
    second.join(timeout=0.5)
    assert second.is_alive()
    registry.release_scan.set()
    first.join(timeout=5.0)
    second.join(timeout=5.0)
    assert not first.is_alive() and not second.is_alive()

    assert len(results) == 2
    assert len(set(results)) == 1  # ONE artifact id, both observers agree
    named = [
        artifact
        for artifact in (registry.get_artifact(a) for a in registry.artifact_ids())
        if artifact is not None and artifact.name == "notes/plan.md"
    ]
    assert len(named) == 1  # exactly one mint
    assert named[0].version == 1
    assert named[0].content_hash == FP_NEW
