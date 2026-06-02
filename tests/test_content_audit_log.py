# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""Tests for per-agent content audit log (Units 1, 2, 3, 7)."""

from __future__ import annotations

import hashlib
import json
from uuid import uuid4

import pytest

from ccs.agent.runtime import CCS_CONTENT_AUDIT_LOG_SCHEMA_VERSION, AgentRuntime
from ccs.coordinator.registry import ArtifactRegistry
from ccs.coordinator.service import CoordinatorService
from ccs.core.hashing import compute_content_hash
from ccs.strategies.lazy import LazyStrategy

# ---------------------------------------------------------------------------
# Unit 1: Hash utility and schema version constant
# ---------------------------------------------------------------------------


class TestComputeContentHash:
    def test_empty_string(self):
        expected = hashlib.sha256(b"").hexdigest()
        assert compute_content_hash("") == expected

    def test_hello(self):
        expected = hashlib.sha256(b"hello").hexdigest()
        assert compute_content_hash("hello") == expected

    def test_deterministic(self):
        assert compute_content_hash("same") == compute_content_hash("same")

    def test_different_content_different_hash(self):
        assert compute_content_hash("a") != compute_content_hash("b")

    def test_utf8_encoding(self):
        content = "éàü"
        expected = hashlib.sha256(content.encode("utf-8")).hexdigest()
        assert compute_content_hash(content) == expected


class TestSchemaVersionConstant:
    def test_starts_with_ccs(self):
        assert CCS_CONTENT_AUDIT_LOG_SCHEMA_VERSION.startswith("ccs.")

    def test_value(self):
        assert CCS_CONTENT_AUDIT_LOG_SCHEMA_VERSION == "ccs.content_audit.v1"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

AGENT_ID = uuid4()
ARTIFACT_ID = uuid4()
INSTANCE_ID = "test-audit-instance"


def _audit_runtime(
    log: list[dict],
    *,
    coordinator: CoordinatorService | None = None,
    agent_id=AGENT_ID,
    audit_seq: list[int] | None = None,
) -> AgentRuntime:
    service = coordinator or CoordinatorService(ArtifactRegistry())
    return AgentRuntime(
        agent_id=agent_id,
        coordinator=service,
        strategy=LazyStrategy(),
        content_audit_log=log.append,
        audit_seq=audit_seq if audit_seq is not None else [0],
        agent_name="test-agent",
        instance_id=INSTANCE_ID,
    )


R1_FIELDS = {
    "tick", "agent_id", "agent_name", "artifact_id", "version",
    "content_hash", "source", "outcome", "sequence_number",
    "instance_id", "schema_version",
}


# ---------------------------------------------------------------------------
# Unit 2: _record_content_view and _record_search_view
# ---------------------------------------------------------------------------


class TestRecordContentView:
    def test_emits_all_r1_fields(self):
        log: list[dict] = []
        rt = _audit_runtime(log)
        rt._record_content_view(
            artifact_id=ARTIFACT_ID, version=1, content="hello",
            source="fetch", now_tick=10,
        )
        assert len(log) == 1
        assert set(log[0].keys()) == R1_FIELDS

    def test_source_matches(self):
        log: list[dict] = []
        rt = _audit_runtime(log)
        for src in ("cache_hit", "fetch", "broadcast", "write"):
            rt._record_content_view(
                artifact_id=ARTIFACT_ID, version=1, content="x",
                source=src, now_tick=1,
            )
        sources = [e["source"] for e in log]
        assert sources == ["cache_hit", "fetch", "broadcast", "write"]

    def test_outcome_content_with_hash(self):
        log: list[dict] = []
        rt = _audit_runtime(log)
        h = rt._record_content_view(
            artifact_id=ARTIFACT_ID, version=1, content="hello",
            source="fetch", now_tick=1,
        )
        assert log[0]["outcome"] == "content"
        assert log[0]["content_hash"] == compute_content_hash("hello")
        assert h == compute_content_hash("hello")

    def test_outcome_content_for_empty_string_at_version_1(self):
        log: list[dict] = []
        rt = _audit_runtime(log)
        h = rt._record_content_view(
            artifact_id=ARTIFACT_ID, version=1, content="",
            source="fetch", now_tick=1,
        )
        assert log[0]["outcome"] == "content"
        assert log[0]["content_hash"] == compute_content_hash("")
        assert log[0]["version"] == 1
        assert h is not None

    def test_outcome_empty_when_version_none(self):
        log: list[dict] = []
        rt = _audit_runtime(log)
        h = rt._record_content_view(
            artifact_id=ARTIFACT_ID, version=None, content="",
            source="fetch", now_tick=1,
        )
        assert log[0]["outcome"] == "empty"
        assert log[0]["version"] is None
        assert log[0]["content_hash"] is None
        assert h is None

    def test_outcome_empty_when_version_zero(self):
        log: list[dict] = []
        rt = _audit_runtime(log)
        h = rt._record_content_view(
            artifact_id=ARTIFACT_ID, version=0, content="",
            source="fetch", now_tick=1,
        )
        assert log[0]["outcome"] == "empty"
        assert log[0]["version"] is None
        assert log[0]["content_hash"] is None
        assert h is None

    def test_outcome_error_when_content_none(self):
        log: list[dict] = []
        rt = _audit_runtime(log)
        h = rt._record_content_view(
            artifact_id=ARTIFACT_ID, version=1, content=None,
            source="fetch", now_tick=1,
        )
        assert log[0]["outcome"] == "error"
        assert log[0]["version"] is None
        assert log[0]["content_hash"] is None
        assert h is None

    def test_sequence_number_increments(self):
        log: list[dict] = []
        rt = _audit_runtime(log)
        for i in range(3):
            rt._record_content_view(
                artifact_id=ARTIFACT_ID, version=1, content="x",
                source="fetch", now_tick=i,
            )
        assert [e["sequence_number"] for e in log] == [1, 2, 3]

    def test_sequence_number_rolls_back_on_exception(self):
        seq = [0]
        calls = []

        def failing_log(entry):
            calls.append(entry)
            raise RuntimeError("boom")

        rt = AgentRuntime(
            agent_id=AGENT_ID,
            coordinator=CoordinatorService(ArtifactRegistry()),
            strategy=LazyStrategy(),
            content_audit_log=failing_log,
            audit_seq=seq,
            agent_name="test",
            instance_id=INSTANCE_ID,
        )
        with pytest.raises(RuntimeError, match="boom"):
            rt._record_content_view(
                artifact_id=ARTIFACT_ID, version=1, content="x",
                source="fetch", now_tick=1,
            )
        assert seq[0] == 0

    def test_no_emission_when_callback_none(self):
        rt = AgentRuntime(
            agent_id=AGENT_ID,
            coordinator=CoordinatorService(ArtifactRegistry()),
            strategy=LazyStrategy(),
        )
        seq_before = rt._audit_seq[0]
        rt._record_content_view(
            artifact_id=ARTIFACT_ID, version=1, content="x",
            source="fetch", now_tick=1,
        )
        assert rt._audit_seq[0] == seq_before

    def test_updates_content_by_artifact(self):
        log: list[dict] = []
        rt = _audit_runtime(log)
        rt._record_content_view(
            artifact_id=ARTIFACT_ID, version=1, content="new",
            source="fetch", now_tick=1,
        )
        assert rt._content_by_artifact[ARTIFACT_ID] == "new"

    def test_cache_hit_skips_dict_write(self):
        log: list[dict] = []
        rt = _audit_runtime(log)
        rt._content_by_artifact[ARTIFACT_ID] = "existing"
        rt._record_content_view(
            artifact_id=ARTIFACT_ID, version=1, content="existing",
            source="cache_hit", now_tick=1,
        )
        assert rt._content_by_artifact[ARTIFACT_ID] == "existing"
        assert len(log) == 1

    def test_tick_matches_now_tick(self):
        log: list[dict] = []
        rt = _audit_runtime(log)
        rt._record_content_view(
            artifact_id=ARTIFACT_ID, version=1, content="x",
            source="fetch", now_tick=42,
        )
        assert log[0]["tick"] == 42

    def test_agent_fields_match_constructor(self):
        log: list[dict] = []
        rt = _audit_runtime(log)
        rt._record_content_view(
            artifact_id=ARTIFACT_ID, version=1, content="x",
            source="fetch", now_tick=1,
        )
        assert log[0]["agent_id"] == str(AGENT_ID)
        assert log[0]["agent_name"] == "test-agent"
        assert log[0]["instance_id"] == INSTANCE_ID
        assert log[0]["schema_version"] == CCS_CONTENT_AUDIT_LOG_SCHEMA_VERSION

    def test_records_json_serializable(self):
        log: list[dict] = []
        rt = _audit_runtime(log)
        rt._record_content_view(
            artifact_id=ARTIFACT_ID, version=1, content="hello",
            source="fetch", now_tick=1,
        )
        roundtrip = json.loads(json.dumps(log[0]))
        assert roundtrip == log[0]


# ---------------------------------------------------------------------------
# Unit 3: Wired delivery paths
# ---------------------------------------------------------------------------


class TestWiredCacheHit:
    def test_cache_hit_emits_audit(self):
        log: list[dict] = []
        coordinator = CoordinatorService(ArtifactRegistry())
        artifact = coordinator.register_artifact(name="doc", content="v1")
        rt = _audit_runtime(log, coordinator=coordinator)
        rt.read(artifact.id, now_tick=1)  # fetch
        log.clear()
        rt.read(artifact.id, now_tick=2)  # cache hit
        assert len(log) == 1
        assert log[0]["source"] == "cache_hit"
        assert log[0]["tick"] == 2


class TestWiredFetch:
    def test_fetch_emits_audit(self):
        log: list[dict] = []
        coordinator = CoordinatorService(ArtifactRegistry())
        artifact = coordinator.register_artifact(name="doc", content="v1")
        rt = _audit_runtime(log, coordinator=coordinator)
        rt.read(artifact.id, now_tick=1)
        assert len(log) == 1
        assert log[0]["source"] == "fetch"
        assert log[0]["tick"] == 1
        assert log[0]["version"] == 1


class TestWiredBroadcast:
    def test_broadcast_emits_audit(self):
        log: list[dict] = []
        coordinator = CoordinatorService(ArtifactRegistry())
        artifact = coordinator.register_artifact(name="doc", content="v1")
        rt = _audit_runtime(log, coordinator=coordinator)
        rt.handle_update(
            artifact_id=artifact.id, version=2, content="v2", now_tick=5,
        )
        assert len(log) == 1
        assert log[0]["source"] == "broadcast"
        assert log[0]["tick"] == 5
        assert log[0]["version"] == 2


class TestWiredWrite:
    def test_write_emits_audit(self):
        log: list[dict] = []
        coordinator = CoordinatorService(ArtifactRegistry())
        artifact = coordinator.register_artifact(name="doc", content="v1")
        rt = _audit_runtime(log, coordinator=coordinator)
        rt.read(artifact.id, now_tick=1)
        log.clear()
        rt.write(artifact.id, content="v2", now_tick=2)
        write_records = [e for e in log if e["source"] == "write"]
        assert len(write_records) == 1
        assert write_records[0]["version"] == 2

    def test_write_valid_hash_succeeds(self):
        log: list[dict] = []
        coordinator = CoordinatorService(ArtifactRegistry())
        artifact = coordinator.register_artifact(name="doc", content="v1")
        rt = _audit_runtime(log, coordinator=coordinator)
        rt.read(artifact.id, now_tick=1)
        correct_hash = compute_content_hash("v2")
        rt.write(artifact.id, content="v2", now_tick=2, content_hash=correct_hash)

    def test_write_mismatched_hash_raises(self):
        log: list[dict] = []
        coordinator = CoordinatorService(ArtifactRegistry())
        artifact = coordinator.register_artifact(name="doc", content="v1")
        rt = _audit_runtime(log, coordinator=coordinator)
        rt.read(artifact.id, now_tick=1)
        with pytest.raises(ValueError, match="content_hash mismatch"):
            rt.write(artifact.id, content="v2", now_tick=2, content_hash="wrong")


class TestEndToEndAuditCycle:
    def test_read_write_broadcast_sequence(self):
        log: list[dict] = []
        seq = [0]
        coordinator = CoordinatorService(ArtifactRegistry())
        artifact = coordinator.register_artifact(name="doc", content="v1")
        rt_a = _audit_runtime(log, coordinator=coordinator, agent_id=uuid4(), audit_seq=seq)
        rt_b = _audit_runtime(log, coordinator=coordinator, agent_id=uuid4(), audit_seq=seq)

        rt_a.read(artifact.id, now_tick=1)  # fetch
        rt_b.read(artifact.id, now_tick=1)  # fetch
        rt_a.read(artifact.id, now_tick=2)  # cache hit
        rt_b.handle_update(artifact_id=artifact.id, version=2, content="v2", now_tick=3)

        sources = [e["source"] for e in log]
        assert "fetch" in sources
        assert "cache_hit" in sources
        assert "broadcast" in sources
        seq_nums = [e["sequence_number"] for e in log]
        assert seq_nums == sorted(seq_nums)
        assert len(set(seq_nums)) == len(seq_nums)  # no duplicates


# ---------------------------------------------------------------------------
# Unit 7: Opt-in version retention in ArtifactRegistry
# ---------------------------------------------------------------------------

from ccs.core.types import Artifact


class TestVersionRetention:
    def test_retain_versions_stores_history(self):
        reg = ArtifactRegistry(retain_versions=True)
        artifact_v1 = Artifact(name="doc", version=1)
        reg.register_artifact(artifact_v1, "content-v1")
        artifact_v2 = Artifact(id=artifact_v1.id, name="doc", version=2)
        reg.set_artifact_and_content(artifact_v1.id, artifact_v2, "content-v2")

        assert reg.get_content_at_version(artifact_v1.id, 1) == "content-v1"
        assert reg.get_content_at_version(artifact_v1.id, 2) == "content-v2"

    def test_default_no_retention(self):
        reg = ArtifactRegistry()
        artifact_v1 = Artifact(name="doc", version=1)
        reg.register_artifact(artifact_v1, "content-v1")
        artifact_v2 = Artifact(id=artifact_v1.id, name="doc", version=2)
        reg.set_artifact_and_content(artifact_v1.id, artifact_v2, "content-v2")

        assert reg.get_content_at_version(artifact_v1.id, 1) is None
        assert reg.get_content_at_version(artifact_v1.id, 2) is None

    def test_get_content_returns_latest(self):
        reg = ArtifactRegistry(retain_versions=True)
        artifact_v1 = Artifact(name="doc", version=1)
        reg.register_artifact(artifact_v1, "content-v1")
        artifact_v2 = Artifact(id=artifact_v1.id, name="doc", version=2)
        reg.set_artifact_and_content(artifact_v1.id, artifact_v2, "content-v2")

        assert reg.get_content(artifact_v1.id) == "content-v2"

    def test_version_history_empty_on_new_record(self):
        from ccs.coordinator.registry import ArtifactRecord
        record = ArtifactRecord(artifact=Artifact(name="x", version=1), content="c")
        assert record.version_history == {}


# ---------------------------------------------------------------------------
# Unit 9: End-to-end integration test (core-level, no CCSStore/langgraph)
# ---------------------------------------------------------------------------


class TestEndToEndCoreAuditPipeline:
    """Full audit pipeline at the runtime level: fetch, cache_hit, write, broadcast."""

    R1_FIELDS = {
        "tick", "agent_id", "agent_name", "artifact_id", "version",
        "content_hash", "source", "outcome", "sequence_number",
        "instance_id", "schema_version",
    }

    def test_four_sources_via_runtime(self):
        audit: list[dict] = []
        state: list[dict] = []
        agent_a = uuid4()
        agent_b = uuid4()
        audit_seq: list[int] = [0]
        instance_id = "e2e-test"
        agent_names = {agent_a: "agent-a", agent_b: "agent-b"}

        reg = ArtifactRegistry(
            state_log=state.append,
            agent_names=agent_names,
            instance_id=instance_id,
            retain_versions=True,
        )
        coord = CoordinatorService(reg)

        rt_a = AgentRuntime(
            agent_id=agent_a, coordinator=coord,
            strategy=LazyStrategy(),
            content_audit_log=audit.append, audit_seq=audit_seq,
            agent_name="agent-a", instance_id=instance_id,
        )
        rt_b = AgentRuntime(
            agent_id=agent_b, coordinator=coord,
            strategy=LazyStrategy(),
            content_audit_log=audit.append, audit_seq=audit_seq,
            agent_name="agent-b", instance_id=instance_id,
        )

        artifact = coord.register_artifact(name="doc", content="initial")

        # 1. agent-a writes → source="write"
        rt_a.write(artifact.id, content="v1-content", now_tick=1)
        # 2. agent-b reads (fetch) → source="fetch"
        rt_b.read(artifact.id, now_tick=2)
        # 3. agent-b reads again (cache hit) → source="cache_hit"
        rt_b.read(artifact.id, now_tick=3)
        # 4. agent-a writes v2 → source="write", then broadcast to agent-b
        updated, signals = rt_a.write(artifact.id, content="v2-content", now_tick=4)
        for sig in signals:
            rt_b.handle_update(
                artifact_id=artifact.id, version=updated.version,
                content="v2-content", now_tick=4,
            )

        sources = {e["source"] for e in audit}
        assert {"write", "fetch", "cache_hit", "broadcast"} <= sources

    def test_all_records_have_r1_fields(self):
        audit: list[dict] = []
        audit_seq: list[int] = [0]
        reg = ArtifactRegistry()
        coord = CoordinatorService(reg)
        rt = AgentRuntime(
            agent_id=uuid4(), coordinator=coord, strategy=LazyStrategy(),
            content_audit_log=audit.append, audit_seq=audit_seq,
            agent_name="test", instance_id="i",
        )
        artifact = coord.register_artifact(name="d", content="c")
        rt.write(artifact.id, content="new", now_tick=1)
        rt.read(artifact.id, now_tick=2)

        for entry in audit:
            assert self.R1_FIELDS <= set(entry.keys()), f"missing fields: {self.R1_FIELDS - set(entry.keys())}"

    def test_sequence_numbers_gap_free_across_agents(self):
        audit: list[dict] = []
        audit_seq: list[int] = [0]
        reg = ArtifactRegistry()
        coord = CoordinatorService(reg)
        rt_a = AgentRuntime(
            agent_id=uuid4(), coordinator=coord, strategy=LazyStrategy(),
            content_audit_log=audit.append, audit_seq=audit_seq,
            agent_name="a", instance_id="i",
        )
        rt_b = AgentRuntime(
            agent_id=uuid4(), coordinator=coord, strategy=LazyStrategy(),
            content_audit_log=audit.append, audit_seq=audit_seq,
            agent_name="b", instance_id="i",
        )
        artifact = coord.register_artifact(name="d", content="c")
        rt_a.write(artifact.id, content="x", now_tick=1)
        rt_b.read(artifact.id, now_tick=2)
        rt_b.read(artifact.id, now_tick=3)

        seq_nums = [e["sequence_number"] for e in audit]
        for i in range(1, len(seq_nums)):
            assert seq_nums[i] == seq_nums[i - 1] + 1

    def test_content_hash_cross_validates_with_state_log(self):
        audit: list[dict] = []
        state: list[dict] = []
        agent_id = uuid4()
        audit_seq: list[int] = [0]
        reg = ArtifactRegistry(
            state_log=state.append,
            agent_names={agent_id: "writer"},
            instance_id="xval",
        )
        coord = CoordinatorService(reg)
        rt = AgentRuntime(
            agent_id=agent_id, coordinator=coord, strategy=LazyStrategy(),
            content_audit_log=audit.append, audit_seq=audit_seq,
            agent_name="writer", instance_id="xval",
        )
        artifact = coord.register_artifact(name="d", content="c")
        rt.write(artifact.id, content="payload", now_tick=1)

        write_audit = [e for e in audit if e["source"] == "write"]
        commit_state = [e for e in state if e["trigger"] == "commit"]
        assert len(write_audit) >= 1
        assert len(commit_state) >= 1
        assert write_audit[0]["content_hash"] == commit_state[0]["content_hash"]

    def test_all_records_json_serializable(self):
        import json as _json
        audit: list[dict] = []
        audit_seq: list[int] = [0]
        reg = ArtifactRegistry()
        coord = CoordinatorService(reg)
        rt = AgentRuntime(
            agent_id=uuid4(), coordinator=coord, strategy=LazyStrategy(),
            content_audit_log=audit.append, audit_seq=audit_seq,
            agent_name="t", instance_id="i",
        )
        artifact = coord.register_artifact(name="d", content="c")
        rt.write(artifact.id, content="new", now_tick=1)
        rt.read(artifact.id, now_tick=2)

        for entry in audit:
            _json.dumps(entry)

    def test_version_retention_cross_validates(self):
        audit: list[dict] = []
        audit_seq: list[int] = [0]
        reg = ArtifactRegistry(retain_versions=True)
        coord = CoordinatorService(reg)
        rt = AgentRuntime(
            agent_id=uuid4(), coordinator=coord, strategy=LazyStrategy(),
            content_audit_log=audit.append, audit_seq=audit_seq,
            agent_name="t", instance_id="i",
        )
        artifact = coord.register_artifact(name="d", content="initial")
        rt.write(artifact.id, content="v1", now_tick=1)
        rt.write(artifact.id, content="v2", now_tick=2)

        v1_content = reg.get_content_at_version(artifact.id, 2)
        v2_content = reg.get_content_at_version(artifact.id, 3)
        assert v1_content == "v1"
        assert v2_content == "v2"

    def test_no_audit_without_callback(self):
        """AgentRuntime without content_audit_log emits nothing."""
        reg = ArtifactRegistry()
        coord = CoordinatorService(reg)
        rt = AgentRuntime(
            agent_id=uuid4(), coordinator=coord, strategy=LazyStrategy(),
        )
        artifact = coord.register_artifact(name="d", content="c")
        rt.write(artifact.id, content="new", now_tick=1)
        rt.read(artifact.id, now_tick=2)
