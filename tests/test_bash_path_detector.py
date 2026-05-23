# Copyright (c) 2026 Arbiter contributors.

"""Tests for the Bash file-path detector (v0.1.1 KTD-N H4 mitigation).

Per the v0.1.1 plan KTD-N test-scenarios block, these cover:
- Happy path: tracked command + tracked-artifact arg → detected
- Edge case: literal-string substring match must NOT false-positive
- Edge case: eval-pattern body scan (python -c "open('plan.md')")
- Adversarial: xargs / pipeline routing
- False-negative bias: clever bypass is acceptable miss
"""
from __future__ import annotations

from ccs.adapters.claude_code.bash_path_detector import detect_tracked_paths


def _is_tracked(path: str) -> bool:
    """Test fixture: track `plan.md`, `spec.md`, and `DECISIONS.md` only."""
    return path in {"plan.md", "spec.md", "DECISIONS.md"}


# ----------------------------------------------------------------------
# Happy path: tracked command + tracked-artifact arg
# ----------------------------------------------------------------------


def test_h4_bash_cat_tracked_artifact_detected() -> None:
    assert detect_tracked_paths("cat plan.md", _is_tracked) == ["plan.md"]


def test_h4_bash_head_tracked_artifact_detected() -> None:
    assert detect_tracked_paths("head -n 20 plan.md", _is_tracked) == ["plan.md"]


def test_h4_bash_less_tracked_artifact_detected() -> None:
    assert detect_tracked_paths("less plan.md", _is_tracked) == ["plan.md"]


def test_h4_bash_grep_tracked_artifact_detected() -> None:
    # `grep PATTERN plan.md` — the PATTERN looks like a positional arg.
    # The detector's false-negative bias means we may pick up "PATTERN"
    # as a candidate, but is_tracked("PATTERN") = False, so we only
    # emit tracked paths.
    assert detect_tracked_paths("grep TODO plan.md", _is_tracked) == ["plan.md"]


def test_h4_bash_multiple_tracked_args() -> None:
    assert detect_tracked_paths("cat plan.md spec.md", _is_tracked) == [
        "plan.md",
        "spec.md",
    ]


def test_h4_bash_dedupes_repeated_arg() -> None:
    assert detect_tracked_paths("cat plan.md plan.md", _is_tracked) == ["plan.md"]


# ----------------------------------------------------------------------
# False-positive resistance
# ----------------------------------------------------------------------


def test_h4_bash_untracked_arg_not_detected() -> None:
    # README.md is not in our test-fixture tracked set.
    assert detect_tracked_paths("cat README.md", _is_tracked) == []


def test_h4_bash_unrelated_command_does_not_fire() -> None:
    assert detect_tracked_paths("ls /etc", _is_tracked) == []
    assert detect_tracked_paths("echo hello", _is_tracked) == []
    assert detect_tracked_paths("pwd", _is_tracked) == []


def test_h4_bash_grep_pattern_literal_string_rejected() -> None:
    """`grep "cat plan.md is a tracked file" notes.txt` — the literal
    string contains "plan.md" but it's NOT a file-path arg to grep.
    Detector extracts file-path args ONLY after recognized commands;
    the literal lives inside a quoted string after `grep`, which IS
    the search pattern (positional arg #1). plan.md inside the quoted
    string is NOT extracted; notes.txt is the actual file arg and is
    not tracked. → no detection.

    This is the KTD-N false-positive test scenario from the plan.
    """
    paths = detect_tracked_paths('grep "cat plan.md is a tracked file" notes.txt', _is_tracked)
    assert paths == []  # quoted pattern is not extracted as path; notes.txt untracked


# ----------------------------------------------------------------------
# Eval patterns (python -c "...", perl -e "...", ruby -e "...")
# ----------------------------------------------------------------------


def test_h4_eval_python_c_reading_tracked() -> None:
    cmd = 'python3 -c "open(\'plan.md\').read()"'
    assert "plan.md" in detect_tracked_paths(cmd, _is_tracked)


def test_h4_eval_python_c_double_quoted_body() -> None:
    cmd = "python3 -c \"open('plan.md').read()\""
    assert "plan.md" in detect_tracked_paths(cmd, _is_tracked)


def test_h4_eval_perl_e_reading_tracked() -> None:
    cmd = "perl -e 'open my $f, \"<\", \"plan.md\"; print <$f>;'"
    assert "plan.md" in detect_tracked_paths(cmd, _is_tracked)


def test_h4_eval_ruby_e_reading_tracked() -> None:
    cmd = "ruby -e 'puts File.read(\"plan.md\")'"
    assert "plan.md" in detect_tracked_paths(cmd, _is_tracked)


def test_h4_eval_pattern_with_untracked_path_not_detected() -> None:
    cmd = 'python3 -c "open(\'random.txt\').read()"'
    assert detect_tracked_paths(cmd, _is_tracked) == []


# ----------------------------------------------------------------------
# Pipeline / xargs routing
# ----------------------------------------------------------------------


def test_h4_bash_pipeline_with_tracked_arg() -> None:
    # `echo plan.md | xargs cat` — xargs takes from stdin. Detector's
    # pipeline split sees two segments: ["echo plan.md", "xargs cat"].
    # Segment 1 ("echo") isn't a tracked command, no detection.
    # Segment 2 ("xargs cat") — xargs IS a tracked command, but `cat`
    # is the FOLLOWING tracked command, so detector breaks rather than
    # claiming `cat` as a path arg. False-negative — this is the
    # documented bias.
    paths = detect_tracked_paths("echo plan.md | xargs cat", _is_tracked)
    # Either empty (false-negative bias) or detects via some path —
    # we just assert no spurious matches.
    for p in paths:
        assert _is_tracked(p), f"unexpected path: {p}"


def test_h4_bash_double_pipe_or_separator() -> None:
    # `cat README.md || cat plan.md` — split on ||, then segment 2
    # detects plan.md.
    assert "plan.md" in detect_tracked_paths(
        "cat README.md || cat plan.md", _is_tracked
    )


def test_h4_bash_semicolon_sequence() -> None:
    assert "plan.md" in detect_tracked_paths("ls; cat plan.md; pwd", _is_tracked)


def test_h4_bash_ampersand_background() -> None:
    assert "plan.md" in detect_tracked_paths("cat plan.md & true", _is_tracked)


# ----------------------------------------------------------------------
# Malformed input — graceful degradation
# ----------------------------------------------------------------------


def test_h4_bash_unmatched_quote_skipped() -> None:
    # shlex.split raises ValueError on unmatched quotes; segment skipped.
    paths = detect_tracked_paths('cat "plan.md', _is_tracked)
    # Either no detection (segment skipped) or detected the bare-token
    # before quote — both are acceptable behavior. Just assert no crash.
    assert isinstance(paths, list)


def test_h4_bash_empty_command() -> None:
    assert detect_tracked_paths("", _is_tracked) == []


def test_h4_bash_whitespace_only() -> None:
    assert detect_tracked_paths("   \t\n  ", _is_tracked) == []


# ----------------------------------------------------------------------
# Env-var-prefixed commands
# ----------------------------------------------------------------------


def test_h4_bash_env_prefix_then_tracked_command() -> None:
    # `LANG=C cat plan.md` — env-var assignment skipped, then `cat` detected.
    assert detect_tracked_paths("LANG=C cat plan.md", _is_tracked) == ["plan.md"]


# ----------------------------------------------------------------------
# Flag handling
# ----------------------------------------------------------------------


def test_h4_bash_flag_before_tracked_arg() -> None:
    assert detect_tracked_paths("head --lines=20 plan.md", _is_tracked) == ["plan.md"]


def test_h4_bash_short_flag_with_value() -> None:
    # `head -n 20 plan.md` — `-n` is a flag; `20` should NOT be claimed
    # as a path (is_tracked("20") = False); plan.md detected.
    assert detect_tracked_paths("head -n 20 plan.md", _is_tracked) == ["plan.md"]


# ----------------------------------------------------------------------
# ADV-006 — wrapper command pass-through (eval, command, exec, builtin)
# ----------------------------------------------------------------------


def test_adv006_eval_wrapper_pass_through() -> None:
    """ADV-006: `eval cat plan.md` was previously NOT detected — first
    token 'eval' isn't a tracked command, detector broke immediately.
    Now: skip the wrapper, re-evaluate the next position."""
    assert detect_tracked_paths("eval cat plan.md", _is_tracked) == ["plan.md"]


def test_adv006_command_wrapper_pass_through() -> None:
    """`command cat plan.md` — `command` is a POSIX wrapper that
    bypasses shell function lookup; pass through to `cat`."""
    assert detect_tracked_paths("command cat plan.md", _is_tracked) == ["plan.md"]


def test_adv006_exec_wrapper_pass_through() -> None:
    """`exec cat plan.md` — `exec` replaces the shell process."""
    assert detect_tracked_paths("exec cat plan.md", _is_tracked) == ["plan.md"]


def test_adv006_builtin_wrapper_pass_through() -> None:
    """`builtin cat plan.md` — bash builtin keyword (rare in practice)."""
    assert detect_tracked_paths("builtin cat plan.md", _is_tracked) == ["plan.md"]


def test_adv006_nested_wrappers_capped_at_max_depth() -> None:
    """A pathological `eval eval eval eval eval cat plan.md` (5 wrappers)
    exceeds the MAX_WRAPPERS=4 cap; the bash detector stops walking
    after 4 wrappers + an unrecognized token. Defensive bound."""
    # 4 wrappers + cat → still detected
    assert detect_tracked_paths("eval eval eval eval cat plan.md", _is_tracked) == ["plan.md"]
    # 5 wrappers + cat → stops at the 5th 'eval' (now treated as command), no match
    assert detect_tracked_paths("eval eval eval eval eval cat plan.md", _is_tracked) == []


def test_adv006_wrapper_without_tracked_command_returns_empty() -> None:
    """`eval ls -la` — wrapper skipped, but `ls` isn't a tracked
    command. No false positive."""
    assert detect_tracked_paths("eval ls -la", _is_tracked) == []
