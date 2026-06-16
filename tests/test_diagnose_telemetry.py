# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Unit 8 tests — opt-in consent flow + telemetry payload constructor.

Coverage map (mirrors the Unit 8 spec scenario list):

* Path resolution under ``XDG_CONFIG_HOME`` and the ``~/.config``
  fallback.
* Env-var kill switches (``DO_NOT_TRACK``, ``DISABLE_TELEMETRY``,
  ``CCS_DIAGNOSE_NO_TELEMETRY``) — truthy / falsy values.
* TTY-aware ``is_interactive`` (``CI`` env var, ``isatty``).
* Consent prompt: ``y`` / ``yes`` / ``YES`` / empty / ``N`` / ``always``
  / unknown-then-default.
* ``load_consent`` / ``save_consent`` round-trip + atomicity + file-mode
  enforcement.
* ``reset_token`` regenerates UUID4 with policy_version=CURRENT.
* ``resolve_consent`` — kill switch precedence, policy-version drift,
  non-interactive default-deny.
* ``payload_for`` schema strictness — exact field set, no leakage of
  agent / artifact / content fields.
* ``payload_for_from_json`` — graceful default for missing nested fields.
* ``_detect_stack`` — version present / absent / metadata error paths.
"""

from __future__ import annotations

import io
import json
import stat
import threading
import uuid
from pathlib import Path

import pytest

from ccs.diagnose import CCS_DIAGNOSE_LOG_SCHEMA_VERSION
from ccs.diagnose.classifier import ClassifierVerdict, classify
from ccs.diagnose.detection import DetectionReport, detect
from ccs.diagnose.telemetry import (
    CONSENT_FILE_NAME,
    CURRENT_POLICY_VERSION,
    ENV_KILL_SWITCHES,
    ConsentState,
    consent_path,
    env_kill_switch_active,
    is_interactive,
    load_consent,
    payload_for,
    payload_for_from_json,
    prompt_for_consent,
    reset_token,
    resolve_consent,
    save_consent,
)

# -------------------------------------------------------------------- #
# Fixtures
# -------------------------------------------------------------------- #


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Strip env vars that would leak between tests."""
    for name in (*ENV_KILL_SWITCHES, "CI", "XDG_CONFIG_HOME"):
        monkeypatch.delenv(name, raising=False)


@pytest.fixture
def isolated_xdg(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Path:
    """Point ``XDG_CONFIG_HOME`` at a fresh per-test directory."""
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path))
    return tmp_path


@pytest.fixture
def fake_verdict_and_report() -> tuple[ClassifierVerdict, DetectionReport]:
    """Build a minimal verdict + report tuple for payload tests.

    Uses :func:`ccs.diagnose.classifier.classify` against an empty event
    buffer (insufficient verdict). That gives us a frozen, valid pair
    without sourcing fixtures from elsewhere.
    """
    verdict = classify(events=(), key_index={}, overrides=None)
    report = detect(events=(), verdict=verdict, key_index={})
    return verdict, report


# -------------------------------------------------------------------- #
# Path resolution
# -------------------------------------------------------------------- #


def test_consent_path_uses_xdg_config_home(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path))
    assert consent_path() == tmp_path / "ccs-diagnose" / CONSENT_FILE_NAME


def test_consent_path_falls_back_when_xdg_unset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("XDG_CONFIG_HOME", raising=False)
    assert consent_path() == Path.home() / ".config" / "ccs-diagnose" / CONSENT_FILE_NAME


def test_consent_path_falls_back_when_xdg_empty_string(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("XDG_CONFIG_HOME", "")
    assert consent_path() == Path.home() / ".config" / "ccs-diagnose" / CONSENT_FILE_NAME


# -------------------------------------------------------------------- #
# Env-var kill switches
# -------------------------------------------------------------------- #


@pytest.mark.parametrize("name", ENV_KILL_SWITCHES)
def test_env_kill_switch_active_truthy_one(
    name: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(name, "1")
    assert env_kill_switch_active() == name


@pytest.mark.parametrize("name", ENV_KILL_SWITCHES)
def test_env_kill_switch_active_truthy_true(
    name: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(name, "true")
    assert env_kill_switch_active() == name


@pytest.mark.parametrize("name", ENV_KILL_SWITCHES)
def test_env_kill_switch_active_truthy_yes(
    name: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(name, "yes")
    assert env_kill_switch_active() == name


@pytest.mark.parametrize("name", ENV_KILL_SWITCHES)
def test_env_kill_switch_active_falsy_empty(
    name: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(name, "")
    assert env_kill_switch_active() is None


@pytest.mark.parametrize("name", ENV_KILL_SWITCHES)
def test_env_kill_switch_active_falsy_zero(
    name: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(name, "0")
    assert env_kill_switch_active() is None


def test_env_kill_switch_active_returns_first(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When multiple are set, the function returns the first match."""
    for name in ENV_KILL_SWITCHES:
        monkeypatch.setenv(name, "1")
    assert env_kill_switch_active() == ENV_KILL_SWITCHES[0]


def test_env_kill_switch_active_none_set(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in ENV_KILL_SWITCHES:
        monkeypatch.delenv(name, raising=False)
    assert env_kill_switch_active() is None


# -------------------------------------------------------------------- #
# is_interactive
# -------------------------------------------------------------------- #


def test_is_interactive_false_when_ci_set(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("CI", "true")
    assert is_interactive() is False


def test_is_interactive_false_when_ci_one(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("CI", "1")
    assert is_interactive() is False


def test_is_interactive_with_empty_ci_falls_through(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``CI=`` (empty) is falsy, so the TTY check decides.

    In the pytest harness stdin is typically not a TTY, so the function
    should still return False — but the empty-string CI must not
    short-circuit by itself.
    """
    monkeypatch.setenv("CI", "")
    # The exact return depends on whether stdin is a TTY in the test
    # runner, but the function must not raise.
    assert isinstance(is_interactive(), bool)


def test_is_interactive_false_when_stdin_not_tty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("CI", raising=False)

    class _FakeStdin:
        def isatty(self) -> bool:
            return False

    monkeypatch.setattr("sys.stdin", _FakeStdin())
    assert is_interactive() is False


def test_is_interactive_true_when_tty_and_no_ci(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("CI", raising=False)

    class _FakeStdin:
        def isatty(self) -> bool:
            return True

    monkeypatch.setattr("sys.stdin", _FakeStdin())
    assert is_interactive() is True


# -------------------------------------------------------------------- #
# Prompt response classification
# -------------------------------------------------------------------- #


@pytest.mark.parametrize("response", ["y", "Y", "yes", "YES", "Yes", "yeah"])
def test_prompt_accepts_affirmative(response: str) -> None:
    sin = io.StringIO(response + "\n")
    sout = io.StringIO()
    state = prompt_for_consent(stream_in=sin, stream_out=sout)
    assert state.granted is True
    assert state.installation_token is not None
    # UUID4 sanity.
    assert isinstance(state.installation_token, uuid.UUID)
    assert state.installation_token.version == 4
    assert state.policy_version == CURRENT_POLICY_VERSION


@pytest.mark.parametrize("response", ["n", "N", "no", "NO", "No"])
def test_prompt_accepts_negative(response: str) -> None:
    sin = io.StringIO(response + "\n")
    sout = io.StringIO()
    state = prompt_for_consent(stream_in=sin, stream_out=sout)
    assert state.granted is False
    assert state.installation_token is None
    assert state.policy_version == CURRENT_POLICY_VERSION


def test_prompt_empty_enter_is_default_n() -> None:
    sin = io.StringIO("\n")
    sout = io.StringIO()
    state = prompt_for_consent(stream_in=sin, stream_out=sout)
    assert state.granted is False
    assert state.installation_token is None


@pytest.mark.parametrize(
    "response", ["always", "Always", "ALWAYS", "always-yes-for-this-machine"]
)
def test_prompt_always_grants(response: str) -> None:
    sin = io.StringIO(response + "\n")
    sout = io.StringIO()
    state = prompt_for_consent(stream_in=sin, stream_out=sout)
    assert state.granted is True
    assert state.installation_token is not None


def test_prompt_unknown_then_negative_falls_back() -> None:
    """Unknown input re-prompts once; second 'n' is honored as no."""
    sin = io.StringIO("xyz\nn\n")
    sout = io.StringIO()
    state = prompt_for_consent(stream_in=sin, stream_out=sout)
    assert state.granted is False
    # The re-prompt copy should appear in stdout output.
    assert "Please answer" in sout.getvalue()


def test_prompt_unknown_twice_defaults_to_no() -> None:
    sin = io.StringIO("xyz\nasdf\n")
    sout = io.StringIO()
    state = prompt_for_consent(stream_in=sin, stream_out=sout)
    assert state.granted is False


def test_prompt_copy_lists_collected_and_not_collected() -> None:
    """Defensive: prompt copy must enumerate what is and isn't collected."""
    sin = io.StringIO("n\n")
    sout = io.StringIO()
    prompt_for_consent(stream_in=sin, stream_out=sout)
    text = sout.getvalue()
    # Collected items.
    assert "stack" in text.lower()
    assert "verdict" in text.lower()
    assert "coverage" in text.lower() or "counts only" in text.lower()
    # Not-collected items.
    assert "agent names" in text.lower()
    assert "artifact names" in text.lower()
    assert "ip addresses" in text.lower()
    # ``installation_token`` must be disclosed to satisfy the consent
    # contract: it links multiple runs from a single machine.
    assert "installation_token" in text
    assert "--reset-token" in text


# -------------------------------------------------------------------- #
# save_consent / load_consent round-trip
# -------------------------------------------------------------------- #


def test_save_consent_creates_dir_with_mode_0700(isolated_xdg: Path) -> None:
    state = ConsentState(
        granted=True,
        policy_version=CURRENT_POLICY_VERSION,
        installation_token=uuid.uuid4(),
    )
    save_consent(state)
    parent = isolated_xdg / "ccs-diagnose"
    assert parent.exists()
    parent_mode = stat.S_IMODE(parent.stat().st_mode)
    assert parent_mode & 0o077 == 0, f"parent mode {oct(parent_mode)} leaks group/other bits"


def test_save_consent_creates_file_with_mode_0600(isolated_xdg: Path) -> None:
    state = ConsentState(
        granted=True,
        policy_version=CURRENT_POLICY_VERSION,
        installation_token=uuid.uuid4(),
    )
    save_consent(state)
    target = isolated_xdg / "ccs-diagnose" / CONSENT_FILE_NAME
    assert target.exists()
    file_mode = stat.S_IMODE(target.stat().st_mode)
    assert file_mode & 0o077 == 0, f"file mode {oct(file_mode)} leaks group/other bits"


def test_save_then_load_round_trip_granted(isolated_xdg: Path) -> None:
    token = uuid.uuid4()
    original = ConsentState(
        granted=True, policy_version=CURRENT_POLICY_VERSION, installation_token=token
    )
    save_consent(original)
    loaded = load_consent()
    assert loaded == original


def test_save_then_load_round_trip_denied(isolated_xdg: Path) -> None:
    original = ConsentState(
        granted=False, policy_version=CURRENT_POLICY_VERSION, installation_token=None
    )
    save_consent(original)
    loaded = load_consent()
    assert loaded == original


def test_load_consent_missing_returns_denied(isolated_xdg: Path) -> None:
    """No file present -> denied with policy_version=0 (signals 'no decision yet')."""
    loaded = load_consent()
    assert loaded is not None
    assert loaded.granted is False
    assert loaded.policy_version == 0
    assert loaded.installation_token is None


def test_load_consent_corrupt_json_returns_denied(isolated_xdg: Path) -> None:
    parent = isolated_xdg / "ccs-diagnose"
    parent.mkdir(mode=0o700, parents=True)
    (parent / CONSENT_FILE_NAME).write_text("not json", encoding="utf-8")
    loaded = load_consent()
    assert loaded is not None
    assert loaded.granted is False
    assert loaded.policy_version == 0


def test_load_consent_missing_fields_returns_denied(isolated_xdg: Path) -> None:
    parent = isolated_xdg / "ccs-diagnose"
    parent.mkdir(mode=0o700, parents=True)
    (parent / CONSENT_FILE_NAME).write_text(
        json.dumps({"random": "stuff"}), encoding="utf-8"
    )
    loaded = load_consent()
    assert loaded is not None
    assert loaded.granted is False


def test_load_consent_returns_none_when_kill_switch_active(
    isolated_xdg: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    save_consent(
        ConsentState(
            granted=True,
            policy_version=CURRENT_POLICY_VERSION,
            installation_token=uuid.uuid4(),
        )
    )
    monkeypatch.setenv("DO_NOT_TRACK", "1")
    assert load_consent() is None


def test_load_consent_granted_but_no_token_treated_as_corrupt(
    isolated_xdg: Path,
) -> None:
    parent = isolated_xdg / "ccs-diagnose"
    parent.mkdir(mode=0o700, parents=True)
    (parent / CONSENT_FILE_NAME).write_text(
        json.dumps(
            {
                "granted": True,
                "policy_version": CURRENT_POLICY_VERSION,
                "installation_token": None,
            }
        ),
        encoding="utf-8",
    )
    loaded = load_consent()
    assert loaded is not None
    assert loaded.granted is False
    assert loaded.policy_version == 0


def test_save_consent_overwrites_existing(isolated_xdg: Path) -> None:
    save_consent(
        ConsentState(
            granted=True,
            policy_version=CURRENT_POLICY_VERSION,
            installation_token=uuid.uuid4(),
        )
    )
    new_token = uuid.uuid4()
    save_consent(
        ConsentState(
            granted=True,
            policy_version=CURRENT_POLICY_VERSION,
            installation_token=new_token,
        )
    )
    loaded = load_consent()
    assert loaded is not None
    assert loaded.installation_token == new_token


def test_save_consent_concurrent_writes_no_corruption(
    isolated_xdg: Path,
) -> None:
    """Two threads racing on save_consent must produce a valid file."""
    n_threads = 8
    barrier = threading.Barrier(n_threads)

    def _worker() -> None:
        barrier.wait()
        save_consent(
            ConsentState(
                granted=True,
                policy_version=CURRENT_POLICY_VERSION,
                installation_token=uuid.uuid4(),
            )
        )

    threads = [threading.Thread(target=_worker) for _ in range(n_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    loaded = load_consent()
    assert loaded is not None
    assert loaded.granted is True
    assert loaded.installation_token is not None


# -------------------------------------------------------------------- #
# reset_token
# -------------------------------------------------------------------- #


def test_reset_token_writes_file(isolated_xdg: Path) -> None:
    new_token = reset_token()
    assert isinstance(new_token, uuid.UUID)
    assert new_token.version == 4
    loaded = load_consent()
    assert loaded is not None
    assert loaded.granted is True
    assert loaded.installation_token == new_token
    assert loaded.policy_version == CURRENT_POLICY_VERSION


def test_reset_token_overwrites_existing_token(isolated_xdg: Path) -> None:
    first = reset_token()
    second = reset_token()
    assert first != second
    loaded = load_consent()
    assert loaded is not None
    assert loaded.installation_token == second


def test_reset_token_file_mode_0600(isolated_xdg: Path) -> None:
    reset_token()
    target = isolated_xdg / "ccs-diagnose" / CONSENT_FILE_NAME
    assert stat.S_IMODE(target.stat().st_mode) & 0o077 == 0


# -------------------------------------------------------------------- #
# resolve_consent
# -------------------------------------------------------------------- #


def test_resolve_consent_kill_switch_returns_denied(
    isolated_xdg: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("DO_NOT_TRACK", "1")
    state = resolve_consent()
    assert state.granted is False
    # No file written even if dir exists.
    target = isolated_xdg / "ccs-diagnose" / CONSENT_FILE_NAME
    assert not target.exists()


def test_resolve_consent_non_interactive_no_existing_returns_denied(
    isolated_xdg: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("CI", "1")
    state = resolve_consent()
    assert state.granted is False
    target = isolated_xdg / "ccs-diagnose" / CONSENT_FILE_NAME
    assert not target.exists(), "non-interactive must not write consent.json"


def test_resolve_consent_existing_at_current_version_returned(
    isolated_xdg: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """CI on, existing valid consent at CURRENT_POLICY_VERSION -> use it."""
    token = uuid.uuid4()
    save_consent(
        ConsentState(
            granted=True,
            policy_version=CURRENT_POLICY_VERSION,
            installation_token=token,
        )
    )
    monkeypatch.setenv("CI", "1")
    state = resolve_consent()
    assert state.granted is True
    assert state.installation_token == token


def test_resolve_consent_existing_at_older_version_non_interactive_denied(
    isolated_xdg: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Older policy_version + non-interactive -> denied (no re-prompt)."""
    save_consent(
        ConsentState(
            granted=True,
            policy_version=CURRENT_POLICY_VERSION - 1
            if CURRENT_POLICY_VERSION > 1
            else 0,
            installation_token=uuid.uuid4(),
        )
    )
    monkeypatch.setenv("CI", "1")
    state = resolve_consent()
    assert state.granted is False


def test_resolve_consent_kill_switch_beats_consent(
    isolated_xdg: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    save_consent(
        ConsentState(
            granted=True,
            policy_version=CURRENT_POLICY_VERSION,
            installation_token=uuid.uuid4(),
        )
    )
    monkeypatch.setenv("DISABLE_TELEMETRY", "1")
    state = resolve_consent()
    assert state.granted is False


# -------------------------------------------------------------------- #
# payload_for schema strictness
# -------------------------------------------------------------------- #


_EXPECTED_FIELDS = frozenset(
    {
        "schema_version",
        "stack",
        "agent_coherence_version",
        "python_version",
        "classifier_version",
        "verdict_bucket",
        "verdict_confidence",
        "coverage",
        "timestamp_utc",
        "installation_token",
        "policy_version",
    }
)

_FORBIDDEN_FIELDS = frozenset(
    {
        "agent_names",
        "artifact_names",
        "content",
        "hashes",
        "tool_calls",
        "prompts",
        "ip_address",
        "divergence_events",
    }
)


def test_payload_for_field_set_is_strict(
    fake_verdict_and_report: tuple[ClassifierVerdict, DetectionReport],
) -> None:
    verdict, _report = fake_verdict_and_report
    payload = payload_for(verdict)
    assert frozenset(payload.keys()) == _EXPECTED_FIELDS


def test_payload_for_does_not_leak_forbidden_fields(
    fake_verdict_and_report: tuple[ClassifierVerdict, DetectionReport],
) -> None:
    verdict, _report = fake_verdict_and_report
    payload = payload_for(verdict)
    leaked = _FORBIDDEN_FIELDS & set(payload.keys())
    assert leaked == set(), f"payload leaked forbidden fields: {leaked}"
    # Defensive: nested structures shouldn't either.
    serialized = json.dumps(payload, default=str)
    for forbidden in _FORBIDDEN_FIELDS:
        assert forbidden not in serialized


def test_payload_for_consent_granted_includes_token(
    fake_verdict_and_report: tuple[ClassifierVerdict, DetectionReport],
) -> None:
    verdict, _report = fake_verdict_and_report
    token = uuid.uuid4()
    payload = payload_for(
        verdict,
        consent=ConsentState(
            granted=True,
            policy_version=CURRENT_POLICY_VERSION,
            installation_token=token,
        ),
    )
    assert payload["installation_token"] == str(token)
    assert payload["policy_version"] == CURRENT_POLICY_VERSION


def test_payload_for_consent_denied_no_token(
    fake_verdict_and_report: tuple[ClassifierVerdict, DetectionReport],
) -> None:
    verdict, _report = fake_verdict_and_report
    payload = payload_for(
        verdict,
        consent=ConsentState(
            granted=False,
            policy_version=CURRENT_POLICY_VERSION,
            installation_token=None,
        ),
    )
    assert payload["installation_token"] is None
    assert payload["policy_version"] == CURRENT_POLICY_VERSION


def test_payload_for_consent_none_defaults_to_denied(
    fake_verdict_and_report: tuple[ClassifierVerdict, DetectionReport],
) -> None:
    verdict, _report = fake_verdict_and_report
    payload = payload_for(verdict)
    assert payload["installation_token"] is None


def test_payload_for_coverage_shape(
    fake_verdict_and_report: tuple[ClassifierVerdict, DetectionReport],
) -> None:
    verdict, _report = fake_verdict_and_report
    payload = payload_for(verdict)
    coverage = payload["coverage"]
    assert set(coverage.keys()) == {
        "tick_count",
        "read_count",
        "write_count",
        "artifact_count",
    }


def test_payload_for_timestamp_is_iso_utc(
    fake_verdict_and_report: tuple[ClassifierVerdict, DetectionReport],
) -> None:
    verdict, _report = fake_verdict_and_report
    payload = payload_for(verdict)
    ts = payload["timestamp_utc"]
    assert isinstance(ts, str)
    # ISO-8601 with TZ info.
    assert "T" in ts
    assert ts.endswith("+00:00")


def test_payload_for_schema_version_pinned(
    fake_verdict_and_report: tuple[ClassifierVerdict, DetectionReport],
) -> None:
    verdict, _report = fake_verdict_and_report
    payload = payload_for(verdict)
    assert payload["schema_version"] == CCS_DIAGNOSE_LOG_SCHEMA_VERSION
    assert payload["classifier_version"] == CCS_DIAGNOSE_LOG_SCHEMA_VERSION


def test_payload_for_token_only_when_granted_and_present(
    fake_verdict_and_report: tuple[ClassifierVerdict, DetectionReport],
) -> None:
    """granted=True but token=None must still emit None (defensive)."""
    verdict, _report = fake_verdict_and_report
    payload = payload_for(
        verdict,
        consent=ConsentState(
            granted=True,
            policy_version=CURRENT_POLICY_VERSION,
            installation_token=None,
        ),
    )
    assert payload["installation_token"] is None


# -------------------------------------------------------------------- #
# payload_for_from_json
# -------------------------------------------------------------------- #


def test_payload_for_from_json_full_structure() -> None:
    loaded = {
        "schema_version": CCS_DIAGNOSE_LOG_SCHEMA_VERSION,
        "verdict": {
            "bucket": "single_writer",
            "confidence": "high",
            "coverage": {
                "tick_count": 5,
                "read_count": 10,
                "write_count": 3,
                "artifact_count": 2,
            },
        },
    }
    payload = payload_for_from_json(loaded)
    assert payload["verdict_bucket"] == "single_writer"
    assert payload["verdict_confidence"] == "high"
    assert payload["coverage"]["tick_count"] == 5
    assert payload["coverage"]["read_count"] == 10


def test_payload_for_from_json_missing_coverage_uses_defaults() -> None:
    loaded = {
        "schema_version": CCS_DIAGNOSE_LOG_SCHEMA_VERSION,
        "verdict": {"bucket": "mixed", "confidence": "preliminary"},
    }
    payload = payload_for_from_json(loaded)
    assert payload["coverage"] == {
        "tick_count": 0,
        "read_count": 0,
        "write_count": 0,
        "artifact_count": 0,
    }


def test_payload_for_from_json_with_consent_includes_token() -> None:
    token = uuid.uuid4()
    payload = payload_for_from_json(
        {
            "schema_version": CCS_DIAGNOSE_LOG_SCHEMA_VERSION,
            "verdict": {"bucket": "single_writer", "confidence": "high"},
        },
        consent=ConsentState(
            granted=True,
            policy_version=CURRENT_POLICY_VERSION,
            installation_token=token,
        ),
    )
    assert payload["installation_token"] == str(token)


def test_payload_for_from_json_field_set_strict() -> None:
    payload = payload_for_from_json(
        {
            "schema_version": CCS_DIAGNOSE_LOG_SCHEMA_VERSION,
            "verdict": {"bucket": "x", "confidence": "y"},
        }
    )
    assert frozenset(payload.keys()) == _EXPECTED_FIELDS


# -------------------------------------------------------------------- #
# Stack detection
# -------------------------------------------------------------------- #


def test_detect_stack_returns_string(
    fake_verdict_and_report: tuple[ClassifierVerdict, DetectionReport],
) -> None:
    """payload['stack'] must be a non-empty string."""
    verdict, _report = fake_verdict_and_report
    payload = payload_for(verdict)
    stack = payload["stack"]
    assert isinstance(stack, str)
    assert stack.startswith("LangGraph")


def test_detect_stack_handles_missing_package(
    monkeypatch: pytest.MonkeyPatch,
    fake_verdict_and_report: tuple[ClassifierVerdict, DetectionReport],
) -> None:
    """When importlib.metadata raises PackageNotFoundError, fall back to the marker."""
    from importlib.metadata import PackageNotFoundError

    import ccs.diagnose.telemetry as telemetry_mod

    def _fake_version(name: str) -> str:
        raise PackageNotFoundError(name)

    # Re-import the version function inside _detect_stack via monkeypatch
    # against importlib.metadata.version (it's imported lazily inside).
    monkeypatch.setattr(
        "importlib.metadata.version", _fake_version
    )
    verdict, _report = fake_verdict_and_report
    payload = telemetry_mod.payload_for(verdict)
    assert payload["stack"] == "LangGraph (version-unknown)"


# -------------------------------------------------------------------- #
# Defensive: re-import / type guards
# -------------------------------------------------------------------- #


def test_consent_state_is_frozen() -> None:
    state = ConsentState(
        granted=True,
        policy_version=CURRENT_POLICY_VERSION,
        installation_token=uuid.uuid4(),
    )
    with pytest.raises(Exception):  # noqa: PT011 - dataclass FrozenInstanceError
        state.granted = False  # type: ignore[misc]
