"""SB-25 Unit 1 — composite subagent identity (Python side).

Byte-parity vectors are shared with the Node backend's
``src/test/agent_id_subagent.test.ts`` — a one-char fold-string divergence
would silently fork the shared ``agent_states`` rows.
"""

from __future__ import annotations

from ccs.adapters.claude_code.coordinator_server import (
    read_subagent_id,
    session_to_agent_id,
    session_to_agent_name,
)

SID = "deadbeef"
SUB_A = "a0826622451ec196f"
SUB_B = "b1937733562fd2a7e"


class TestCompositeDerivation:
    def test_parent_derivation_unchanged(self) -> None:
        # Pre-SB-25 identity preserved byte-for-byte (backward compat).
        assert session_to_agent_id(SID).hex == "c72c9b5c603054adbc7fa70a4887d327"
        assert session_to_agent_id(SID, None) == session_to_agent_id(SID)
        assert session_to_agent_id(SID, "") == session_to_agent_id(SID)

    def test_subagent_derivation_distinct_stable_node_parity(self) -> None:
        # Node-parity vectors (agent_id_subagent.test.ts pins the same hexes).
        assert session_to_agent_id(SID, SUB_A).hex == "00bc1e3b20975c899de6d7b138acc202"
        assert session_to_agent_id(SID, SUB_B).hex == "be73a85d9be95187a61e421795012f85"
        assert session_to_agent_id(SID, SUB_A) == session_to_agent_id(SID, SUB_A)
        assert session_to_agent_id(SID, SUB_A) != session_to_agent_id(SID, SUB_B)
        assert session_to_agent_id(SID, SUB_A) != session_to_agent_id(SID)

    def test_agent_name_forms(self) -> None:
        assert session_to_agent_name(SID) == "claude-session-deadbeef"
        assert (
            session_to_agent_name(SID, SUB_A)
            == f"claude-session-deadbeef:subagent-{SUB_A}"
        )


class TestReadSubagentId:
    def test_snake_case_preferred_camel_fallback(self) -> None:
        assert read_subagent_id({"agent_id": SUB_A}) == SUB_A
        assert read_subagent_id({"agentId": SUB_A}) == SUB_A
        # snake wins when both present.
        assert read_subagent_id({"agent_id": SUB_A, "agentId": SUB_B}) == SUB_A

    def test_absent_or_invalid_resolves_to_parent(self) -> None:
        assert read_subagent_id({}) is None
        assert read_subagent_id({"agent_id": ""}) is None
        assert read_subagent_id({"agent_id": 42}) is None
        assert read_subagent_id({"agent_id": "x" * 65}) is None
        assert read_subagent_id({"agent_id": "bad!chars"}) is None


class TestRegistrationAndReverse:
    def test_register_session_composite_and_reverse_attribution(self) -> None:
        from ccs.adapters.claude_code.coordinator_server import (
            CoordinatorHTTPServer,
            _agent_id_to_session,
        )

        # Minimal object exercising just the registration/name surface.
        coordinator = CoordinatorHTTPServer.__new__(CoordinatorHTTPServer)
        import threading

        coordinator._agent_names = {}
        coordinator._agent_names_lock = threading.Lock()

        parent = coordinator.register_session(SID)
        sub = coordinator.register_session(SID, SUB_A)
        assert parent != sub
        # Idempotent.
        assert coordinator.register_session(SID, SUB_A) == sub

        # R2 attribution: reverse lookup names the SUBAGENT, not the parent.
        assert _agent_id_to_session(coordinator, parent) == SID
        assert _agent_id_to_session(coordinator, sub) == SUB_A


class TestClientThreading:
    def test_builders_thread_agent_id(self, tmp_path) -> None:
        from ccs.cli.coherence_hook_client import (
            _build_pre_bash,
            _build_pre_grep,
            _build_session_stop,
        )

        cc = {
            "session_id": SID,
            "agent_id": SUB_A,
            "tool_input": {"command": "cat plan.md", "path": ""},
        }
        assert _build_session_stop(cc)["agent_id"] == SUB_A
        assert _build_pre_bash(cc)["agent_id"] == SUB_A
        assert _build_pre_grep(cc, tmp_path)["agent_id"] == SUB_A
        # camelCase fallback + absent stays absent.
        cc_camel = {"session_id": SID, "agentId": SUB_B}
        assert _build_session_stop(cc_camel)["agent_id"] == SUB_B
        assert "agent_id" not in _build_session_stop({"session_id": SID})


class TestSubagentStopSafety:
    """P1: a present-but-malformed agent_id must never release the parent."""

    def test_client_rejects_malformed_agent_id(self) -> None:
        from ccs.cli.coherence_hook_client import _build_subagent_stop, _SkipHook

        # Valid → forwarded.
        assert _build_subagent_stop({"session_id": "s", "agent_id": "ok_id"}) == {
            "session_id": "s",
            "agent_id": "ok_id",
        }
        assert _build_subagent_stop({"session_id": "s", "agentId": "ok2"}) == {
            "session_id": "s",
            "agent_id": "ok2",
        }
        # Absent AND present-but-malformed both raise _SkipHook (fail-open {}).
        import pytest

        for bad in (None, "", "bad.id", "has space", "x" * 65, "trailing\n", "a/b"):
            payload = {"session_id": "s"} if bad is None else {"session_id": "s", "agent_id": bad}
            with pytest.raises(_SkipHook):
                _build_subagent_stop(payload)

    def test_has_subagent_id_field(self) -> None:
        from ccs.adapters.claude_code.coordinator_server import has_subagent_id_field

        assert has_subagent_id_field({"agent_id": "bad.id"}) is True
        assert has_subagent_id_field({"agentId": "x"}) is True
        assert has_subagent_id_field({}) is False
        assert has_subagent_id_field({"agent_id": ""}) is False
        assert has_subagent_id_field({"agent_id": None}) is False
        # P2: present NON-STRING values are "present" (must be refused on the
        # session-stop path, not degraded to the parent identity).
        assert has_subagent_id_field({"agent_id": 42}) is True
        assert has_subagent_id_field({"agent_id": [1]}) is True
        assert has_subagent_id_field({"agent_id": {"k": 1}}) is True
        assert has_subagent_id_field({"agent_id": True}) is True


class TestSubagentIdTrailingNewline:
    """P2 parity: fullmatch rejects a trailing newline (Node `$` already does)."""

    def test_trailing_newline_rejected(self) -> None:
        from ccs.adapters.claude_code.coordinator_server import read_subagent_id

        assert read_subagent_id({"agent_id": "abc"}) == "abc"
        assert read_subagent_id({"agent_id": "abc\n"}) is None
        assert read_subagent_id({"agent_id": "a\nb"}) is None
