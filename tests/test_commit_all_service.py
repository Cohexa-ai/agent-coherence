# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents
"""Service-level tests for the SB-18 atomic multi-artifact publish.

Covers ``CoordinatorService.commit_all`` (the standalone service surface, D4
preconditions + InvalidationSignal construction) and ``session_commit_all`` (the
thin session convenience that sources comparands from the pinned cut).
"""
from __future__ import annotations

from uuid import uuid4

import pytest

from ccs.coordinator.registry import ArtifactRegistry
from ccs.coordinator.service import CoordinatorService
from ccs.core.exceptions import CoherenceError
from ccs.core.states import MESIState
from ccs.core.types import (
    CommitAllEntry,
    MultiCommitConflict,
    MultiCommitResult,
    SessionCommitRejection,
)


def _svc():
    return CoordinatorService(ArtifactRegistry())


def test_service_commit_all_win_returns_result_and_signals():
    svc = _svc()
    a = svc.register_artifact(name="a.md", content="v1")
    b = svc.register_artifact(name="b.md", content="v1")
    caller = uuid4()
    svc.registry.set_agent_state(a.id, caller, MESIState.SHARED, tick=1)
    svc.registry.set_agent_state(b.id, caller, MESIState.SHARED, tick=1)
    out = svc.commit_all(
        agent_id=caller,
        writes={
            a.id: CommitAllEntry(expected_version=a.version, content_hash="ha2", content="a-v2"),
            b.id: CommitAllEntry(expected_version=b.version, content_hash="hb2", content="b-v2"),
        },
    )
    assert isinstance(out, tuple)
    result, signals = out
    assert isinstance(result, MultiCommitResult)
    assert result.versions == {a.id: a.version + 1, b.id: b.version + 1}


def test_service_commit_all_rejects_pessimistic_caller():
    svc = _svc()
    a = svc.register_artifact(name="a.md", content="v1")
    caller = uuid4()
    svc.registry.set_agent_state(a.id, caller, MESIState.EXCLUSIVE, tick=1)  # M/E -> use commit()
    with pytest.raises(CoherenceError):
        svc.commit_all(
            agent_id=caller,
            writes={a.id: CommitAllEntry(expected_version=a.version, content_hash="h2")},
        )


def test_service_commit_all_one_drifted_holds_batch():
    svc = _svc()
    a = svc.register_artifact(name="a.md", content="v1")
    b = svc.register_artifact(name="b.md", content="v1")
    caller, peer = uuid4(), uuid4()
    svc.registry.set_agent_state(a.id, caller, MESIState.SHARED, tick=1)
    svc.registry.set_agent_state(b.id, caller, MESIState.SHARED, tick=1)
    # a peer advances b past the caller's comparand.
    svc.registry.set_agent_state(b.id, peer, MESIState.SHARED, tick=1)
    svc.registry.commit_cas(b.id, peer, expected_version=b.version, content_hash="hb-p", content="b-p", tick=1)
    out = svc.commit_all(
        agent_id=caller,
        writes={
            a.id: CommitAllEntry(expected_version=a.version, content_hash="ha2", content="a-v2"),
            b.id: CommitAllEntry(expected_version=b.version, content_hash="hb2", content="b-v2"),
        },
    )
    assert isinstance(out, MultiCommitConflict)
    assert set(out.per_artifact) == {b.id}
    assert svc.registry.get_artifact(a.id).version == a.version  # all-or-nothing


def test_session_commit_all_win_against_pinned_cut():
    svc = _svc()
    a = svc.register_artifact(name="a.md", content="v1")
    b = svc.register_artifact(name="b.md", content="v1")
    owner = uuid4()
    session = svc.begin_session(read_set=[a.id, b.id], owner=owner)
    out = svc.session_commit_all(
        session.session_token,
        {a.id: ("a-v2", None), b.id: ("b-v2", None)},
        caller=owner,
    )
    assert isinstance(out, tuple)
    result, _signals = out
    assert isinstance(result, MultiCommitResult)
    assert result.versions == {a.id: a.version + 1, b.id: b.version + 1}


def test_session_commit_all_member_not_in_cut_is_rejected():
    svc = _svc()
    a = svc.register_artifact(name="a.md", content="v1")
    b = svc.register_artifact(name="b.md", content="v1")
    owner = uuid4()
    session = svc.begin_session(read_set=[a.id], owner=owner)  # b NOT pinned
    out = svc.session_commit_all(
        session.session_token,
        {a.id: ("a-v2", None), b.id: ("b-v2", None)},
        caller=owner,
    )
    assert isinstance(out, SessionCommitRejection)
    # all-or-nothing: nothing published
    assert svc.registry.get_artifact(a.id).version == a.version
