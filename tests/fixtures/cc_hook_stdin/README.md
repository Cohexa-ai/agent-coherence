# Claude Code hook stdin fixtures (contract test)

Recorded 2026-05-14 against `claude` v2.1.131 in an isolated mktemp git repo.
The probe plugin declared command-type hooks; these are the verbatim stdin
JSON payloads Claude Code sent each hook script.

Used by `tests/test_claude_code_e2e.py::test_hook_payload_contract`.
CI flags drift on every Claude Code minor version bump.

File contents:
- session_start.json  — SessionStart (source: startup)
- pre_read.json       — PreToolUse:Read
- post_read.json      — PostToolUse:Read
- pre_edit.json       — PreToolUse:Edit
- post_edit.json      — PostToolUse:Edit
- stop.json           — Stop (end of turn)

To re-record after a v2.1.x → v2.(1+x).0 bump, re-run the probe that recorded
these fixtures.
