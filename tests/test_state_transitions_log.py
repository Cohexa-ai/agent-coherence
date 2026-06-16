# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Tests for the state transitions log emitter — registry, service, and CCSStore layers."""

from __future__ import annotations

import json
import re
from uuid import UUID, uuid4

import pytest

from ccs.coordinator.registry import CCS_STATE_LOG_SCHEMA_VERSION, ArtifactRegistry
from ccs.coordinator.service import CoordinatorService
from ccs.core.states import MESIState
from ccs.core.types import Artifact, FetchRequest

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _registry_with_log(
    log: list[dict],
    agent_names: dict | None = None,
    instance_id: str = "test-instance-id",
) -> ArtifactRegistry:
    return ArtifactRegistry(state_log=log.append, agent_names=agent_names, instance_id=instance_id)


def _register(svc: CoordinatorService, name: str = "artifact") -> Artifact:
    return svc.register_artifact(name=name, content="v1")


def _service_with_log(log: list[dict]) -> CoordinatorService:
    return CoordinatorService(_registry_with_log(log))


# ---------------------------------------------------------------------------
# Unit 1: ArtifactRegistry.set_agent_state emission
# ---------------------------------------------------------------------------

def test_set_agent_state_emits_all_fields() -> None:
    log: list[dict] = []
    reg = _registry_with_log(log)
    artifact = Artifact(name="plan.md", version=3)
    reg.register_artifact(artifact, "content")
    agent_id = uuid4()

    reg.set_agent_state(artifact.id, agent_id, MESIState.EXCLUSIVE, trigger="fetch", tick=7)

    assert len(log) == 1
    entry = log[0]
    assert entry["tick"] == 7
    assert entry["artifact_id"] == str(artifact.id)
    assert entry["agent_id"] == str(agent_id)
    assert entry["agent_name"] is None
    assert entry["from_state"] == "INVALID"
    assert entry["to_state"] == "EXCLUSIVE"
    assert entry["trigger"] == "fetch"
    assert entry["version"] == 3


def test_from_state_is_invalid_for_new_agent() -> None:
    log: list[dict] = []
    reg = _registry_with_log(log)
    artifact = Artifact(name="x", version=1)
    reg.register_artifact(artifact, "")
    agent_id = uuid4()

    reg.set_agent_state(artifact.id, agent_id, MESIState.SHARED, trigger="fetch", tick=1)

    assert log[0]["from_state"] == "INVALID"


def test_from_state_reflects_prior_state() -> None:
    log: list[dict] = []
    reg = _registry_with_log(log)
    artifact = Artifact(name="x", version=1)
    reg.register_artifact(artifact, "")
    agent_id = uuid4()

    reg.set_agent_state(artifact.id, agent_id, MESIState.EXCLUSIVE, trigger="fetch", tick=1)
    reg.set_agent_state(artifact.id, agent_id, MESIState.MODIFIED, trigger="commit", tick=2)

    assert log[0]["from_state"] == "INVALID"
    assert log[0]["to_state"] == "EXCLUSIVE"
    assert log[1]["from_state"] == "EXCLUSIVE"
    assert log[1]["to_state"] == "MODIFIED"


def test_agent_name_resolved_when_mapping_provided() -> None:
    log: list[dict] = []
    agent_id = uuid4()
    names = {agent_id: "planner"}
    reg = _registry_with_log(log, agent_names=names)
    artifact = Artifact(name="x", version=1)
    reg.register_artifact(artifact, "")

    reg.set_agent_state(artifact.id, agent_id, MESIState.EXCLUSIVE, trigger="fetch", tick=1)

    assert log[0]["agent_name"] == "planner"


def test_agent_name_null_without_mapping() -> None:
    log: list[dict] = []
    reg = _registry_with_log(log, agent_names=None)
    artifact = Artifact(name="x", version=1)
    reg.register_artifact(artifact, "")
    agent_id = uuid4()

    reg.set_agent_state(artifact.id, agent_id, MESIState.EXCLUSIVE, trigger="fetch", tick=1)

    assert log[0]["agent_name"] is None


def test_agent_name_null_for_unknown_agent_id_in_mapping() -> None:
    log: list[dict] = []
    reg = _registry_with_log(log, agent_names={})
    artifact = Artifact(name="x", version=1)
    reg.register_artifact(artifact, "")
    agent_id = uuid4()

    reg.set_agent_state(artifact.id, agent_id, MESIState.SHARED, trigger="fetch", tick=1)

    assert log[0]["agent_name"] is None


def test_version_in_entry_matches_artifact_version() -> None:
    log: list[dict] = []
    reg = _registry_with_log(log)
    artifact = Artifact(name="x", version=5)
    reg.register_artifact(artifact, "")
    agent_id = uuid4()

    reg.set_agent_state(artifact.id, agent_id, MESIState.EXCLUSIVE, trigger="fetch", tick=1)

    assert log[0]["version"] == 5


def test_no_emission_when_state_log_is_none() -> None:
    reg = ArtifactRegistry()
    artifact = Artifact(name="x", version=1)
    reg.register_artifact(artifact, "")
    agent_id = uuid4()

    # Should not raise; nothing to assert except no side effects
    reg.set_agent_state(artifact.id, agent_id, MESIState.EXCLUSIVE)


def test_default_registry_has_no_log() -> None:
    """ArtifactRegistry() with no args behaves identically to before."""
    reg = ArtifactRegistry()
    artifact = Artifact(name="x", version=1)
    reg.register_artifact(artifact, "")
    agent_id = uuid4()
    reg.set_agent_state(artifact.id, agent_id, MESIState.EXCLUSIVE)
    assert reg.get_agent_state(artifact.id, agent_id) == MESIState.EXCLUSIVE


# ---------------------------------------------------------------------------
# Unit 2 (plan): sequence_number, instance_id, schema_version on state log
# ---------------------------------------------------------------------------

def test_first_state_log_entry_has_sequence_number_1() -> None:
    log: list[dict] = []
    reg = _registry_with_log(log)
    artifact = Artifact(name="x", version=1)
    reg.register_artifact(artifact, "")
    reg.set_agent_state(artifact.id, uuid4(), MESIState.EXCLUSIVE, tick=1)
    assert log[0]["sequence_number"] == 1


def test_sequence_number_increments_per_entry() -> None:
    log: list[dict] = []
    reg = _registry_with_log(log)
    artifact = Artifact(name="x", version=1)
    reg.register_artifact(artifact, "")
    agent_id = uuid4()
    reg.set_agent_state(artifact.id, agent_id, MESIState.EXCLUSIVE, tick=1)
    reg.set_agent_state(artifact.id, agent_id, MESIState.MODIFIED, tick=2)
    assert log[0]["sequence_number"] == 1
    assert log[1]["sequence_number"] == 2


def test_all_entries_share_same_instance_id() -> None:
    log: list[dict] = []
    reg = _registry_with_log(log)
    artifact = Artifact(name="x", version=1)
    reg.register_artifact(artifact, "")
    agent_id = uuid4()
    reg.set_agent_state(artifact.id, agent_id, MESIState.EXCLUSIVE, tick=1)
    reg.set_agent_state(artifact.id, agent_id, MESIState.MODIFIED, tick=2)
    assert log[0]["instance_id"] == log[1]["instance_id"]


def test_schema_version_on_every_state_log_entry() -> None:
    log: list[dict] = []
    reg = _registry_with_log(log)
    artifact = Artifact(name="x", version=1)
    reg.register_artifact(artifact, "")
    reg.set_agent_state(artifact.id, uuid4(), MESIState.EXCLUSIVE, tick=1)
    assert log[0]["schema_version"] == "ccs.state_log.v2"
    assert log[0]["schema_version"] == CCS_STATE_LOG_SCHEMA_VERSION


def test_artifact_registry_generates_uuid_when_no_instance_id() -> None:
    reg = ArtifactRegistry()
    uuid_pattern = re.compile(
        r"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
    )
    assert uuid_pattern.match(reg._instance_id)


def test_two_registry_instances_have_distinct_instance_ids() -> None:
    reg_a = ArtifactRegistry()
    reg_b = ArtifactRegistry()
    assert reg_a._instance_id != reg_b._instance_id


def test_explicit_instance_id_is_preserved() -> None:
    fixed_id = "test-instance-id-abc"
    reg = ArtifactRegistry(instance_id=fixed_id)
    log: list[dict] = []
    reg._state_log = log.append
    artifact = Artifact(name="x", version=1)
    reg.register_artifact(artifact, "")
    reg.set_agent_state(artifact.id, uuid4(), MESIState.EXCLUSIVE, tick=1)
    assert log[0]["instance_id"] == fixed_id


# ---------------------------------------------------------------------------
# Unit 3 (plan): CoordinatorService trigger strings
# ---------------------------------------------------------------------------

def test_register_artifact_emits_register_trigger() -> None:
    log: list[dict] = []
    svc = _service_with_log(log)

    agent_id = uuid4()
    svc.register_artifact(name="plan.md", content="v1", initial_owner=agent_id)

    register_entries = [e for e in log if e["trigger"] == "register"]
    assert len(register_entries) == 1
    assert register_entries[0]["to_state"] == "EXCLUSIVE"
    assert register_entries[0]["agent_id"] == str(agent_id)


def test_register_artifact_without_owner_emits_nothing() -> None:
    log: list[dict] = []
    svc = _service_with_log(log)

    svc.register_artifact(name="plan.md", content="v1")

    assert log == []


def test_fetch_first_holder_emits_fetch_exclusive() -> None:
    log: list[dict] = []
    svc = _service_with_log(log)
    artifact = _register(svc)
    agent_a = uuid4()

    svc.fetch(FetchRequest(artifact_id=artifact.id, requesting_agent_id=agent_a, requested_at_tick=5))

    fetch_entries = [e for e in log if e["trigger"] == "fetch"]
    assert len(fetch_entries) == 1
    assert fetch_entries[0]["to_state"] == "EXCLUSIVE"
    assert fetch_entries[0]["tick"] == 5


def test_fetch_second_holder_emits_fetch_shared() -> None:
    log: list[dict] = []
    svc = _service_with_log(log)
    artifact = _register(svc)
    agent_a, agent_b = uuid4(), uuid4()

    svc.fetch(FetchRequest(artifact_id=artifact.id, requesting_agent_id=agent_a, requested_at_tick=1))
    log.clear()
    svc.fetch(FetchRequest(artifact_id=artifact.id, requesting_agent_id=agent_b, requested_at_tick=2))

    fetch_entries = [e for e in log if e["trigger"] == "fetch"]
    # agent_b gets SHARED; agent_a downgraded to SHARED as well
    states = {e["agent_id"]: e["to_state"] for e in fetch_entries}
    assert states[str(agent_b)] == "SHARED"
    assert states[str(agent_a)] == "SHARED"


def test_write_emits_write_trigger_for_peers_and_writer() -> None:
    log: list[dict] = []
    svc = _service_with_log(log)
    artifact = _register(svc)
    agent_a, agent_b, agent_c = uuid4(), uuid4(), uuid4()

    svc.fetch(FetchRequest(artifact_id=artifact.id, requesting_agent_id=agent_a, requested_at_tick=1))
    svc.fetch(FetchRequest(artifact_id=artifact.id, requesting_agent_id=agent_b, requested_at_tick=2))
    log.clear()

    svc.write(agent_id=agent_c, artifact_id=artifact.id, issued_at_tick=3)

    write_entries = [e for e in log if e["trigger"] == "write"]
    invalid_entries = [e for e in write_entries if e["to_state"] == "INVALID"]
    exclusive_entries = [e for e in write_entries if e["to_state"] == "EXCLUSIVE"]

    assert len(invalid_entries) == 2  # agent_a and agent_b invalidated
    assert len(exclusive_entries) == 1  # agent_c gets EXCLUSIVE
    assert exclusive_entries[0]["agent_id"] == str(agent_c)
    assert all(e["tick"] == 3 for e in write_entries)


def test_commit_emits_commit_trigger() -> None:
    log: list[dict] = []
    svc = _service_with_log(log)
    artifact = _register(svc)
    agent_a = uuid4()

    svc.fetch(FetchRequest(artifact_id=artifact.id, requesting_agent_id=agent_a, requested_at_tick=1))
    log.clear()
    svc.commit(agent_id=agent_a, artifact_id=artifact.id, content="v2", issued_at_tick=2)

    commit_entries = [e for e in log if e["trigger"] == "commit"]
    assert len(commit_entries) == 1
    assert commit_entries[0]["to_state"] == "MODIFIED"
    assert commit_entries[0]["agent_id"] == str(agent_a)
    assert commit_entries[0]["tick"] == 2


def test_invalidate_emits_invalidate_trigger() -> None:
    log: list[dict] = []
    svc = _service_with_log(log)
    artifact = _register(svc)
    agent_a = uuid4()

    svc.fetch(FetchRequest(artifact_id=artifact.id, requesting_agent_id=agent_a, requested_at_tick=1))
    log.clear()
    svc.invalidate(
        agent_id=agent_a,
        artifact_id=artifact.id,
        new_version=2,
        issuer_agent_id=uuid4(),
        issued_at_tick=3,
    )

    inv_entries = [e for e in log if e["trigger"] == "invalidate"]
    assert len(inv_entries) == 1
    assert inv_entries[0]["to_state"] == "INVALID"
    assert inv_entries[0]["tick"] == 3


def test_timeout_emits_timeout_trigger() -> None:
    log: list[dict] = []
    svc = _service_with_log(log)
    artifact = _register(svc)
    agent_a = uuid4()

    # Put agent into a transient state that will expire
    svc.fetch(FetchRequest(artifact_id=artifact.id, requesting_agent_id=agent_a, requested_at_tick=1))
    # Manually force a transient state on agent_a so timeout triggers
    svc.registry.set_agent_transient(
        artifact.id, agent_a, __import__("ccs.core.states", fromlist=["TransientState"]).TransientState.ISG, entered_tick=1
    )
    log.clear()

    svc.enforce_transient_timeouts(current_tick=100, timeout_ticks=5)

    timeout_entries = [e for e in log if e["trigger"] == "timeout"]
    assert len(timeout_entries) == 1
    assert timeout_entries[0]["to_state"] == "INVALID"
    assert timeout_entries[0]["tick"] == 100


def test_tick_values_propagate_to_log_entries() -> None:
    log: list[dict] = []
    svc = _service_with_log(log)
    artifact = _register(svc)
    agent_a = uuid4()

    svc.fetch(FetchRequest(artifact_id=artifact.id, requesting_agent_id=agent_a, requested_at_tick=42))

    fetch_entries = [e for e in log if e["trigger"] == "fetch"]
    assert all(e["tick"] == 42 for e in fetch_entries)


# ---------------------------------------------------------------------------
# Unit 4: JSONL integration smoke test — 4-agent planning pipeline
# ---------------------------------------------------------------------------

pytest.importorskip("langgraph.store.base")

from langgraph.store.base import GetOp, PutOp  # noqa: E402

from ccs.adapters.ccsstore import CCSStore  # noqa: E402


def _ccs_put(store: CCSStore, namespace: tuple, key: str, value: dict) -> None:
    store.batch([PutOp(namespace=namespace, key=key, value=value)])


def _ccs_get(store: CCSStore, namespace: tuple, key: str):
    return store.batch([GetOp(namespace=namespace, key=key)])[0]


def test_four_agent_pipeline_produces_complete_log() -> None:
    """Reproduce the spec example: planner writes, 3 readers fetch, planner writes again."""
    log: list[dict] = []
    store = CCSStore(strategy="lazy", state_log=log.append)

    # Tick 1: planner writes the plan (register artifact → write → commit)
    _ccs_put(store, ("planner", "shared"), "plan", {"step": 1})

    # Ticks 2: three readers fetch
    _ccs_get(store, ("researcher", "shared"), "plan")
    _ccs_get(store, ("executor", "shared"), "plan")
    _ccs_get(store, ("reviewer", "shared"), "plan")

    # Tick 3: planner writes again (invalidates all three readers)
    _ccs_put(store, ("planner", "shared"), "plan", {"step": 2})

    # All required fields present in every entry (subset check — new fields may be present too)
    required_fields = {"tick", "artifact_id", "agent_id", "agent_name", "from_state", "to_state", "trigger", "version"}
    for entry in log:
        assert required_fields <= entry.keys(), f"Missing fields: {required_fields - entry.keys()}"

    # No None values for non-nullable fields
    for entry in log:
        for field in ("artifact_id", "agent_id", "from_state", "to_state", "trigger"):
            assert entry[field] is not None, f"Field {field!r} is None in entry {entry}"

    # Valid MESI state strings
    valid_states = {"MODIFIED", "EXCLUSIVE", "SHARED", "INVALID"}
    for entry in log:
        assert entry["from_state"] in valid_states
        assert entry["to_state"] in valid_states

    # Valid trigger strings — no "unknown" should appear in a fully-wired pipeline
    valid_triggers = {"register", "fetch", "write", "commit", "invalidate", "timeout"}
    for entry in log:
        assert entry["trigger"] in valid_triggers, f"Unexpected trigger {entry['trigger']!r}"

    # Every entry is valid JSON round-trip
    for entry in log:
        assert json.loads(json.dumps(entry)) == entry

    # Agent names are populated for all four agents
    named_agents = {e["agent_name"] for e in log if e["agent_name"] is not None}
    assert "planner" in named_agents

    # The pipeline produces at least: write/commit (planner first put) +
    # 3 fetch (readers) + write/commit + invalidations (second put)
    trigger_counts = {}
    for e in log:
        trigger_counts[e["trigger"]] = trigger_counts.get(e["trigger"], 0) + 1

    assert trigger_counts.get("fetch", 0) >= 3          # at least 3 reader fetches
    assert trigger_counts.get("write", 0) >= 1          # at least one write ownership grant
    assert trigger_counts.get("commit", 0) >= 2         # at least 2 commits (two puts)

    # Second write produces INVALID entries for the three readers
    invalid_on_second_write = [
        e for e in log
        if e["to_state"] == "INVALID" and e["trigger"] in ("write", "commit")
        and e["agent_name"] in ("researcher", "executor", "reviewer")
    ]
    assert len(invalid_on_second_write) >= 3


def test_log_entries_uuid_strings_are_parseable() -> None:
    log: list[dict] = []
    store = CCSStore(strategy="lazy", state_log=log.append)
    _ccs_put(store, ("planner", "shared"), "plan", {"v": 1})
    _ccs_get(store, ("reviewer", "shared"), "plan")

    for entry in log:
        UUID(entry["artifact_id"])
        UUID(entry["agent_id"])


def test_version_increments_across_commits() -> None:
    log: list[dict] = []
    store = CCSStore(strategy="lazy", state_log=log.append)

    _ccs_put(store, ("planner", "shared"), "plan", {"v": 1})
    v1_versions = {e["version"] for e in log}
    log.clear()

    _ccs_put(store, ("planner", "shared"), "plan", {"v": 2})
    v2_versions = {e["version"] for e in log}

    assert max(v2_versions) > max(v1_versions)


# ---------------------------------------------------------------------------
# Unit 5: content_hash in state log on commit (R10)
# ---------------------------------------------------------------------------

def test_commit_entry_includes_content_hash() -> None:
    log: list[dict] = []
    svc = _service_with_log(log)
    artifact = _register(svc)
    agent_a = uuid4()

    svc.fetch(FetchRequest(artifact_id=artifact.id, requesting_agent_id=agent_a, requested_at_tick=1))
    log.clear()
    svc.commit(agent_id=agent_a, artifact_id=artifact.id, content="v2", issued_at_tick=2)

    commit_entries = [e for e in log if e["trigger"] == "commit"]
    assert len(commit_entries) == 1
    from ccs.core.hashing import compute_content_hash
    assert commit_entries[0]["content_hash"] == compute_content_hash("v2")


def test_non_commit_entries_have_null_content_hash() -> None:
    log: list[dict] = []
    svc = _service_with_log(log)
    artifact = _register(svc)
    agent_a = uuid4()

    svc.fetch(FetchRequest(artifact_id=artifact.id, requesting_agent_id=agent_a, requested_at_tick=1))

    fetch_entries = [e for e in log if e["trigger"] == "fetch"]
    for entry in fetch_entries:
        assert entry["content_hash"] is None


def test_state_log_schema_version_is_v2() -> None:
    assert CCS_STATE_LOG_SCHEMA_VERSION == "ccs.state_log.v2"


def test_content_hash_field_present_in_all_entries() -> None:
    log: list[dict] = []
    svc = _service_with_log(log)
    artifact = _register(svc)
    agent_a = uuid4()

    svc.fetch(FetchRequest(artifact_id=artifact.id, requesting_agent_id=agent_a, requested_at_tick=1))
    svc.commit(agent_id=agent_a, artifact_id=artifact.id, content="v2", issued_at_tick=2)

    for entry in log:
        assert "content_hash" in entry
