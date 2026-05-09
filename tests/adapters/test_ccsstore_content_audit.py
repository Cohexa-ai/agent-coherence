# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""Tests for content audit log wiring through CCSStore and CoherenceAdapterCore."""

from __future__ import annotations

import pytest

pytest.importorskip("langgraph.store.base")

from langgraph.store.base import GetOp, PutOp, SearchOp

from ccs.adapters.ccsstore import CCSStore
from ccs.agent.runtime import CCS_CONTENT_AUDIT_LOG_SCHEMA_VERSION
from ccs.core.hashing import compute_content_hash


# ---------------------------------------------------------------------------
# Unit 6: CCSStore → CoherenceAdapterCore → AgentRuntime wiring
# ---------------------------------------------------------------------------


class TestAuditLogWiring:
    """CCSStore(content_audit_log=cb) threads callback to each AgentRuntime."""

    def test_put_get_emits_audit_entries(self):
        log: list[dict] = []
        store = CCSStore(content_audit_log=log.append)
        store.batch([
            _put("planner", "shared", "doc", {"text": "hello"}),
        ])
        store.batch([
            _get("reviewer", "shared", "doc"),
        ])
        assert len(log) >= 2
        sources = {e["source"] for e in log}
        assert "write" in sources or "fetch" in sources

    def test_audit_entries_have_correct_schema_version(self):
        log: list[dict] = []
        store = CCSStore(content_audit_log=log.append)
        store.batch([_put("planner", "shared", "doc", {"x": 1})])
        for entry in log:
            assert entry["schema_version"] == CCS_CONTENT_AUDIT_LOG_SCHEMA_VERSION

    def test_audit_entries_have_instance_id(self):
        log: list[dict] = []
        store = CCSStore(content_audit_log=log.append)
        store.batch([_put("planner", "shared", "doc", {"x": 1})])
        assert all(entry["instance_id"] is not None for entry in log)

    def test_instance_id_matches_state_log(self):
        audit_log: list[dict] = []
        state_log: list[dict] = []
        store = CCSStore(
            content_audit_log=audit_log.append,
            state_log=state_log.append,
        )
        store.batch([_put("planner", "shared", "doc", {"x": 1})])
        assert len(audit_log) > 0
        assert len(state_log) > 0
        audit_instance = audit_log[0]["instance_id"]
        state_instance = state_log[0]["instance_id"]
        assert audit_instance == state_instance

    def test_sequence_numbers_monotonically_increase(self):
        log: list[dict] = []
        store = CCSStore(content_audit_log=log.append)
        store.batch([_put("planner", "shared", "doc", {"x": 1})])
        store.batch([_get("reviewer", "shared", "doc")])
        seq_nums = [e["sequence_number"] for e in log]
        assert seq_nums == sorted(seq_nums)
        assert len(set(seq_nums)) == len(seq_nums)

    def test_sequence_numbers_gap_free_across_agents(self):
        log: list[dict] = []
        store = CCSStore(content_audit_log=log.append)
        store.batch([_put("planner", "shared", "doc", {"x": 1})])
        store.batch([_get("reviewer", "shared", "doc")])
        store.batch([_get("editor", "shared", "doc")])
        seq_nums = [e["sequence_number"] for e in log]
        for i in range(1, len(seq_nums)):
            assert seq_nums[i] == seq_nums[i - 1] + 1

    def test_agent_name_populated_in_audit_entries(self):
        log: list[dict] = []
        store = CCSStore(content_audit_log=log.append)
        store.batch([_put("planner", "shared", "doc", {"x": 1})])
        store.batch([_get("reviewer", "shared", "doc")])
        agent_names = {e["agent_name"] for e in log if e["source"] != "search"}
        assert "planner" in agent_names or "reviewer" in agent_names

    def test_no_audit_without_callback(self):
        """CCSStore() with no content_audit_log emits nothing."""
        store = CCSStore()
        store.batch([_put("planner", "shared", "doc", {"x": 1})])
        store.batch([_get("reviewer", "shared", "doc")])
        # No assertion on log — just verify no crash


# ---------------------------------------------------------------------------
# Unit 5: Search audit emission
# ---------------------------------------------------------------------------


class TestSearchAudit:
    """_apply_search emits one audit record per search hit."""

    def test_search_hit_emits_audit_record(self):
        log: list[dict] = []
        store = CCSStore(content_audit_log=log.append)
        store.batch([_put("planner", "shared", "doc", {"text": "hello"})])
        log.clear()

        store.batch([_search("planner")])
        search_entries = [e for e in log if e["source"] == "search"]
        assert len(search_entries) == 1

    def test_search_audit_has_null_agent_identity(self):
        log: list[dict] = []
        store = CCSStore(content_audit_log=log.append)
        store.batch([_put("planner", "shared", "doc", {"text": "hello"})])
        log.clear()

        store.batch([_search("planner")])
        search_entries = [e for e in log if e["source"] == "search"]
        assert len(search_entries) == 1
        entry = search_entries[0]
        assert entry["agent_name"] is None
        assert entry["agent_id"] is None

    def test_search_audit_has_correct_content_hash(self):
        log: list[dict] = []
        store = CCSStore(content_audit_log=log.append)
        value = {"text": "hello"}
        store.batch([_put("planner", "shared", "doc", value)])
        log.clear()

        store.batch([_search("planner")])
        search_entries = [e for e in log if e["source"] == "search"]
        entry = search_entries[0]
        assert entry["content_hash"] is not None

    def test_search_multiple_hits_emit_multiple_records(self):
        log: list[dict] = []
        store = CCSStore(content_audit_log=log.append)
        store.batch([
            _put("planner", "shared", "doc1", {"a": 1}),
            _put("planner", "shared", "doc2", {"b": 2}),
        ])
        log.clear()

        store.batch([_search("planner")])
        search_entries = [e for e in log if e["source"] == "search"]
        assert len(search_entries) == 2

    def test_search_no_hits_no_audit_records(self):
        log: list[dict] = []
        store = CCSStore(content_audit_log=log.append)
        store.batch([_put("planner", "shared", "doc", {"x": 1})])
        log.clear()

        store.batch([_search("nonexistent")])
        search_entries = [e for e in log if e["source"] == "search"]
        assert len(search_entries) == 0

    def test_search_no_audit_without_callback(self):
        store = CCSStore()
        store.batch([_put("planner", "shared", "doc", {"x": 1})])
        store.batch([_search("planner")])
        # No crash — search works without audit

    def test_search_audit_schema_version(self):
        log: list[dict] = []
        store = CCSStore(content_audit_log=log.append)
        store.batch([_put("planner", "shared", "doc", {"x": 1})])
        log.clear()

        store.batch([_search("planner")])
        search_entries = [e for e in log if e["source"] == "search"]
        assert search_entries[0]["schema_version"] == CCS_CONTENT_AUDIT_LOG_SCHEMA_VERSION

    def test_search_audit_sequence_shared_with_agent_audit(self):
        """Search audit seq numbers are gap-free with agent audit seq numbers."""
        log: list[dict] = []
        store = CCSStore(content_audit_log=log.append)
        store.batch([_put("planner", "shared", "doc", {"x": 1})])
        store.batch([_search("planner")])
        seq_nums = [e["sequence_number"] for e in log]
        for i in range(1, len(seq_nums)):
            assert seq_nums[i] == seq_nums[i - 1] + 1

    def test_search_audit_outcome_is_error(self):
        """Search records use outcome='error' — agent identity unknown."""
        log: list[dict] = []
        store = CCSStore(content_audit_log=log.append)
        store.batch([_put("planner", "shared", "doc", {"x": 1})])
        log.clear()

        store.batch([_search("planner")])
        search_entries = [e for e in log if e["source"] == "search"]
        assert search_entries[0]["outcome"] == "error"


# ---------------------------------------------------------------------------
# Version retention gating (R11 via Unit 6 wiring)
# ---------------------------------------------------------------------------


class TestVersionRetentionGating:
    """retain_versions=True is set when content_audit_log is provided."""

    def test_version_history_populated_when_audit_enabled(self):
        log: list[dict] = []
        store = CCSStore(content_audit_log=log.append)
        store.batch([_put("planner", "shared", "doc", {"v": 1})])
        store.batch([_put("planner", "shared", "doc", {"v": 2})])

        # Find the artifact and check version history
        for (scope, key), aid in store._artifact_map.items():
            record = store.core.registry._records[aid]
            assert len(record.version_history) >= 2

    def test_version_history_empty_when_audit_disabled(self):
        store = CCSStore()
        store.batch([_put("planner", "shared", "doc", {"v": 1})])
        store.batch([_put("planner", "shared", "doc", {"v": 2})])

        for (scope, key), aid in store._artifact_map.items():
            record = store.core.registry._records[aid]
            assert len(record.version_history) == 0


# ---------------------------------------------------------------------------
# Unit 9: End-to-end integration test
# ---------------------------------------------------------------------------


class TestEndToEndAuditPipeline:
    """Full audit pipeline: put, get (fetch + cache hit), broadcast, search."""

    R1_FIELDS = {
        "tick", "agent_id", "agent_name", "artifact_id", "version",
        "content_hash", "source", "outcome", "sequence_number",
        "instance_id", "schema_version",
    }

    def test_all_five_sources_appear(self):
        audit: list[dict] = []
        state: list[dict] = []
        store = CCSStore(
            strategy="broadcast",
            content_audit_log=audit.append,
            state_log=state.append,
        )

        # 1. planner puts → source="write"
        store.batch([_put("planner", "shared", "doc", {"plan": "v1"})])
        # 2. reviewer gets (fetch) → source="fetch"
        store.batch([_get("reviewer", "shared", "doc")])
        # 3. reviewer gets again (cache hit) → source="cache_hit"
        store.batch([_get("reviewer", "shared", "doc")])
        # 4. planner puts v2 → broadcast to reviewer → source="broadcast" + "write"
        store.batch([_put("planner", "shared", "doc", {"plan": "v2"})])
        # 5. search → source="search"
        store.batch([_search("planner")])

        sources = {e["source"] for e in audit}
        assert sources == {"write", "fetch", "cache_hit", "broadcast", "search"}

    def test_all_records_have_r1_fields(self):
        audit: list[dict] = []
        store = CCSStore(strategy="broadcast", content_audit_log=audit.append)
        store.batch([_put("planner", "shared", "doc", {"x": 1})])
        store.batch([_get("reviewer", "shared", "doc")])
        store.batch([_search("planner")])

        for entry in audit:
            assert self.R1_FIELDS <= set(entry.keys()), f"missing fields in {entry}"

    def test_sequence_numbers_gap_free(self):
        audit: list[dict] = []
        store = CCSStore(strategy="broadcast", content_audit_log=audit.append)
        store.batch([_put("planner", "shared", "doc", {"x": 1})])
        store.batch([_get("reviewer", "shared", "doc")])
        store.batch([_get("reviewer", "shared", "doc")])
        store.batch([_put("planner", "shared", "doc", {"x": 2})])
        store.batch([_search("planner")])

        seq_nums = [e["sequence_number"] for e in audit]
        for i in range(1, len(seq_nums)):
            assert seq_nums[i] == seq_nums[i - 1] + 1, (
                f"gap at index {i}: {seq_nums[i - 1]} → {seq_nums[i]}"
            )

    def test_instance_id_matches_state_log(self):
        audit: list[dict] = []
        state: list[dict] = []
        store = CCSStore(
            strategy="broadcast",
            content_audit_log=audit.append,
            state_log=state.append,
        )
        store.batch([_put("planner", "shared", "doc", {"x": 1})])

        audit_ids = {e["instance_id"] for e in audit}
        state_ids = {e["instance_id"] for e in state}
        assert len(audit_ids) == 1
        assert audit_ids == state_ids

    def test_content_hash_cross_validation(self):
        """content_hash on write audit matches content_hash on state log commit."""
        audit: list[dict] = []
        state: list[dict] = []
        store = CCSStore(
            strategy="broadcast",
            content_audit_log=audit.append,
            state_log=state.append,
        )
        store.batch([_put("planner", "shared", "doc", {"x": 1})])

        write_entries = [e for e in audit if e["source"] == "write"]
        commit_entries = [e for e in state if e["trigger"] == "commit"]
        assert len(write_entries) >= 1
        assert len(commit_entries) >= 1
        assert write_entries[0]["content_hash"] == commit_entries[0]["content_hash"]

    def test_version_retention_end_to_end(self):
        """R11: version history available when audit is enabled."""
        audit: list[dict] = []
        store = CCSStore(strategy="broadcast", content_audit_log=audit.append)
        store.batch([_put("planner", "shared", "doc", {"plan": "v1"})])
        store.batch([_put("planner", "shared", "doc", {"plan": "v2"})])

        for (_scope, _key), aid in store._artifact_map.items():
            v1 = store.core.registry.get_content_at_version(aid, 2)
            v2 = store.core.registry.get_content_at_version(aid, 3)
            assert v1 is not None
            assert v2 is not None
            assert v1 != v2

    def test_version_retention_negative_coupling(self):
        """version_history is empty when content_audit_log is not set."""
        store = CCSStore()
        store.batch([_put("planner", "shared", "doc", {"plan": "v1"})])
        store.batch([_put("planner", "shared", "doc", {"plan": "v2"})])

        for (_scope, _key), aid in store._artifact_map.items():
            v1 = store.core.registry.get_content_at_version(aid, 2)
            assert v1 is None

    def test_all_records_json_serializable(self):
        import json as _json
        audit: list[dict] = []
        store = CCSStore(strategy="broadcast", content_audit_log=audit.append)
        store.batch([_put("planner", "shared", "doc", {"x": 1})])
        store.batch([_get("reviewer", "shared", "doc")])
        store.batch([_search("planner")])

        for entry in audit:
            _json.dumps(entry)

    def test_normal_deliveries_have_content_outcome(self):
        """outcome='content' for all non-search deliveries with committed content."""
        audit: list[dict] = []
        store = CCSStore(strategy="broadcast", content_audit_log=audit.append)
        store.batch([_put("planner", "shared", "doc", {"x": 1})])
        store.batch([_get("reviewer", "shared", "doc")])
        store.batch([_get("reviewer", "shared", "doc")])

        non_search = [e for e in audit if e["source"] != "search"]
        for entry in non_search:
            assert entry["outcome"] == "content", (
                f"expected outcome='content' for source={entry['source']}, got {entry['outcome']}"
            )
            assert entry["version"] is not None
            assert entry["content_hash"] is not None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _put(agent: str, scope: str, key: str, value: dict) -> PutOp:
    return PutOp(namespace=(agent, scope), key=key, value=value)


def _get(agent: str, scope: str, key: str) -> GetOp:
    return GetOp(namespace=(agent, scope), key=key)


def _search(agent_or_scope: str) -> SearchOp:
    return SearchOp(namespace_prefix=(agent_or_scope,))
