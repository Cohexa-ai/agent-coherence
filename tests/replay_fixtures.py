# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""Shared on-disk fixture builders for replay tests.

Hoisted from per-file ``_write_manifest`` / ``_state_log_*`` / ``_audit_*``
duplicates. Imported explicitly (not via ``conftest.py``) so failing tests
point at the right file instead of a hidden fixture pyramid.

The canonical versions take the most-detailed superset of fields seen
across the four prior copies — every keyword arg has a default so older
call sites stay terse.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def write_manifest(
    session_dir: Path,
    *,
    streams: list[str],
    instance_id: str | None = "instance-A",
    adapter_type: str = "test-fixture",
    start_tick: int = 0,
    end_tick: int = 10,
) -> None:
    """Write a manifest.json with the v1 schema fields."""
    session_dir.mkdir(parents=True, exist_ok=True)
    manifest = {
        "schema_version": 0,
        "schema_note": "test fixture",
        "adapter_type": adapter_type,
        "start_tick": start_tick,
        "end_tick": end_tick,
        "instance_id": instance_id,
        "streams": streams,
        "agents": {},
        "artifacts": {},
    }
    (session_dir / "manifest.json").write_text(
        json.dumps(manifest), encoding="utf-8"
    )


def state_log_entry(
    *,
    tick: int,
    sequence_number: int,
    agent_id: str = "agent-1",
    artifact_id: str = "art-1",
    from_state: str = "INVALID",
    to_state: str = "EXCLUSIVE",
    trigger: str = "write",
    version: int = 1,
    instance_id: str = "instance-A",
    agent_name: str | None = None,
) -> dict[str, Any]:
    """Synthetic state_log entry shaped per replay_trace_format §3."""
    return {
        "tick": tick,
        "artifact_id": artifact_id,
        "agent_id": agent_id,
        "agent_name": agent_name if agent_name is not None else agent_id,
        "from_state": from_state,
        "to_state": to_state,
        "trigger": trigger,
        "version": version,
        "content_hash": "abc",
        "sequence_number": sequence_number,
        "instance_id": instance_id,
        "schema_version": "ccs.state_log.v2",
    }


def audit_entry(
    *,
    tick: int,
    sequence_number: int,
    agent_id: str | None = "agent-1",
    artifact_id: str = "art-1",
    version: int | None = 1,
    outcome: str = "content",
    instance_id: str = "instance-A",
    agent_name: str | None = None,
) -> dict[str, Any]:
    """Synthetic content_audit_log entry shaped per replay_trace_format §4."""
    return {
        "tick": tick,
        "agent_id": agent_id,
        "agent_name": (
            agent_name if agent_name is not None
            else (agent_id if agent_id else None)
        ),
        "artifact_id": artifact_id,
        "version": version,
        "content_hash": "abc",
        "source": "fetch",
        "outcome": outcome,
        "sequence_number": sequence_number,
        "instance_id": instance_id,
        "schema_version": "ccs.content_audit.v1",
    }


def write_jsonl(path: Path, entries: list[dict[str, Any]]) -> None:
    """Write a list of entries as JSONL to ``path``."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        for entry in entries:
            fh.write(json.dumps(entry) + "\n")
