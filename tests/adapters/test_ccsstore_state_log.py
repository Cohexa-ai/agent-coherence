# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""Tests for CCSStore state_log parameter — end-to-end wiring from CCSStore to registry."""

from __future__ import annotations

import json

import pytest

pytest.importorskip("langgraph.store.base")

from langgraph.store.base import GetOp, PutOp

from ccs.adapters.ccsstore import CCSStore

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _put(store: CCSStore, namespace: tuple, key: str, value: dict) -> None:
    store.batch([PutOp(namespace=namespace, key=key, value=value)])


def _get(store: CCSStore, namespace: tuple, key: str):
    return store.batch([GetOp(namespace=namespace, key=key)])[0]


# ---------------------------------------------------------------------------
# Wiring tests
# ---------------------------------------------------------------------------

def test_state_log_none_by_default_no_error() -> None:
    store = CCSStore(strategy="lazy")
    _put(store, ("planner", "shared"), "plan", {"v": 1})
    result = _get(store, ("planner", "shared"), "plan")
    assert result is not None


def test_state_log_receives_entries_on_put() -> None:
    log: list[dict] = []
    store = CCSStore(strategy="lazy", state_log=log.append)

    _put(store, ("planner", "shared"), "plan", {"v": 1})

    assert len(log) > 0
    triggers = {e["trigger"] for e in log}
    # write+commit path fires "write" and "commit" triggers
    assert "write" in triggers or "commit" in triggers


def test_state_log_receives_fetch_on_get_by_peer() -> None:
    log: list[dict] = []
    store = CCSStore(strategy="lazy", state_log=log.append)

    _put(store, ("planner", "shared"), "plan", {"v": 1})
    log.clear()

    _get(store, ("reviewer", "shared"), "plan")

    fetch_entries = [e for e in log if e["trigger"] == "fetch"]
    assert len(fetch_entries) >= 1


def test_agent_name_populated_in_log_entries() -> None:
    log: list[dict] = []
    store = CCSStore(strategy="lazy", state_log=log.append)

    _put(store, ("planner", "shared"), "plan", {"v": 1})

    named = [e for e in log if e["agent_name"] == "planner"]
    assert len(named) > 0


def test_log_entries_have_all_required_fields() -> None:
    log: list[dict] = []
    store = CCSStore(strategy="lazy", state_log=log.append)

    _put(store, ("planner", "shared"), "plan", {"v": 1})
    _get(store, ("reviewer", "shared"), "plan")

    required = {"tick", "artifact_id", "agent_id", "agent_name", "from_state", "to_state", "trigger", "version"}
    for entry in log:
        missing = required - entry.keys()
        assert not missing, f"Entry missing fields: {missing}"


def test_log_entries_are_json_serializable() -> None:
    log: list[dict] = []
    store = CCSStore(strategy="lazy", state_log=log.append)

    _put(store, ("planner", "shared"), "plan", {"v": 1})
    _get(store, ("reviewer", "shared"), "plan")

    for entry in log:
        serialized = json.dumps(entry)
        parsed = json.loads(serialized)
        assert parsed == entry


def test_state_values_are_valid_mesi_strings() -> None:
    log: list[dict] = []
    store = CCSStore(strategy="lazy", state_log=log.append)

    _put(store, ("planner", "shared"), "plan", {"v": 1})
    _get(store, ("reviewer", "shared"), "plan")

    valid_states = {"MODIFIED", "EXCLUSIVE", "SHARED", "INVALID"}
    for entry in log:
        assert entry["from_state"] in valid_states, f"Invalid from_state: {entry['from_state']}"
        assert entry["to_state"] in valid_states, f"Invalid to_state: {entry['to_state']}"


def test_trigger_values_are_from_vocabulary() -> None:
    log: list[dict] = []
    store = CCSStore(strategy="lazy", state_log=log.append)

    _put(store, ("planner", "shared"), "plan", {"v": 1})
    _get(store, ("reviewer", "shared"), "plan")

    valid_triggers = {"register", "fetch", "write", "commit", "invalidate", "timeout", "unknown"}
    for entry in log:
        assert entry["trigger"] in valid_triggers, f"Unexpected trigger: {entry['trigger']}"


def test_artifact_id_and_agent_id_are_uuid_strings() -> None:
    log: list[dict] = []
    store = CCSStore(strategy="lazy", state_log=log.append)

    _put(store, ("planner", "shared"), "plan", {"v": 1})

    from uuid import UUID
    for entry in log:
        UUID(entry["artifact_id"])   # raises if not valid UUID string
        UUID(entry["agent_id"])


def test_peer_write_invalidates_other_agents_log() -> None:
    log: list[dict] = []
    store = CCSStore(strategy="lazy", state_log=log.append)

    _put(store, ("planner", "shared"), "plan", {"v": 1})
    _get(store, ("reviewer", "shared"), "plan")
    log.clear()

    # planner writes again — reviewer should be invalidated
    _put(store, ("planner", "shared"), "plan", {"v": 2})

    invalid_entries = [e for e in log if e["to_state"] == "INVALID" and e["agent_name"] == "reviewer"]
    assert len(invalid_entries) >= 1
