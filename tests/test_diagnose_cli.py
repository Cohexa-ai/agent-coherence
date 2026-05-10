# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""Unit 7 tests — ``ccs-diagnose`` CLI integration.

Coverage map:

* End-to-end pipeline against ``build_graph_no_store`` (the load-bearing
  integration test).
* Argparse / flag handling — required-flag rules, CSV parsing,
  invalid-choice rejection.
* Subcommand-style flags (``--show-payload``, ``--reset-token``).
* Trust-posture flag stubs (``--dry-run`` etc.).
* Defensive / edge cases (graph errors, empty buffers, custom token cost).
* Determinism — repeated runs produce byte-identical artefacts.

Tests prefer direct ``main(argv)`` calls for speed; one subprocess test
verifies the ``python -m ccs.cli.diagnose`` entrypoint actually runs.
"""

from __future__ import annotations

import io
import json
import subprocess
import sys
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path

import pytest

from ccs.diagnose import CCS_DIAGNOSE_LOG_SCHEMA_VERSION


GRAPH_PATH = (
    Path(__file__).resolve().parent.parent
    / "examples"
    / "langgraph_planner"
    / "main.py"
)
GRAPH_FACTORY = f"{GRAPH_PATH}:build_graph_no_store"


def _have_langgraph() -> bool:
    try:
        import langgraph  # noqa: F401
    except ImportError:
        return False
    return True


pytestmark = pytest.mark.skipif(
    not _have_langgraph(),
    reason="ccs-diagnose CLI tests require the [diagnose] extra (langgraph + jinja2).",
)


# -------------------------------------------------------------------- #
# Helpers
# -------------------------------------------------------------------- #


@pytest.fixture(autouse=True)
def _isolate_consent(
    tmp_path_factory: pytest.TempPathFactory, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Ensure CLI tests never touch the user's real consent file.

    Sets ``XDG_CONFIG_HOME`` to a per-test temp dir and ``CI=1`` so the
    consent resolver never prompts (Unit 8 makes the resolver
    non-interactive when ``CI`` is truthy).
    """
    cfg_dir = tmp_path_factory.mktemp("xdg")
    monkeypatch.setenv("XDG_CONFIG_HOME", str(cfg_dir))
    monkeypatch.setenv("CI", "1")
    # Defensive: clear kill switches so the few tests that exercise the
    # actual flow don't see an inherited DO_NOT_TRACK from the developer's
    # shell.
    for name in ("DO_NOT_TRACK", "DISABLE_TELEMETRY", "CCS_DIAGNOSE_NO_TELEMETRY"):
        monkeypatch.delenv(name, raising=False)


def _invoke(argv: list[str]) -> tuple[int, str, str]:
    """Call ``main(argv)`` and capture stdout/stderr."""
    # Late import so the skip marker can short-circuit when langgraph is
    # missing.
    from ccs.cli.diagnose import main

    out_buf = io.StringIO()
    err_buf = io.StringIO()
    try:
        with redirect_stdout(out_buf), redirect_stderr(err_buf):
            code = main(argv)
    except SystemExit as exc:
        code = int(exc.code) if exc.code is not None else 0
    return code, out_buf.getvalue(), err_buf.getvalue()


def _basic_argv(tmp_path: Path, *extra: str) -> list[str]:
    return [
        "--graph",
        GRAPH_FACTORY,
        "--output-html",
        str(tmp_path / "r.html"),
        "--output-json",
        str(tmp_path / "r.json"),
        *extra,
    ]


# -------------------------------------------------------------------- #
# 1-6: End-to-end pipeline
# -------------------------------------------------------------------- #


def test_pipeline_emits_html_and_json(tmp_path: Path) -> None:
    code, stdout, _ = _invoke(_basic_argv(tmp_path))
    assert code == 0, stdout
    html_path = tmp_path / "r.html"
    json_path = tmp_path / "r.json"
    assert html_path.exists()
    assert json_path.exists()
    html = html_path.read_text(encoding="utf-8")
    assert "<html" in html.lower()
    assert "Your write pattern" in html
    payload = json.loads(json_path.read_text(encoding="utf-8"))
    assert payload["schema_version"] == CCS_DIAGNOSE_LOG_SCHEMA_VERSION
    assert "verdict" in payload
    assert "report" in payload
    # Terminal summary printed.
    assert "Your write pattern" in stdout
    assert str(html_path) in stdout


def test_pipeline_with_volume_renders_cost(tmp_path: Path) -> None:
    code, _, _ = _invoke(_basic_argv(tmp_path, "--volume", "50"))
    assert code == 0
    html = (tmp_path / "r.html").read_text(encoding="utf-8")
    # When --volume is set, the cost-KPI path renders. Even when token
    # estimates are unavailable the renderer surfaces an explanatory
    # block; assert the cost-side wiring fired.
    assert "cost" in html.lower()


def test_pipeline_strict_propagates_to_report(tmp_path: Path) -> None:
    code, _, _ = _invoke(_basic_argv(tmp_path, "--strict"))
    assert code == 0
    payload = json.loads((tmp_path / "r.json").read_text(encoding="utf-8"))
    assert payload["report"]["strict_mode"] is True


def test_pipeline_lead_pain_type_cost_no_volume_renders_unmeasurable(
    tmp_path: Path,
) -> None:
    code, _, _ = _invoke(_basic_argv(tmp_path, "--lead-pain-type", "cost"))
    assert code == 0
    html = (tmp_path / "r.html").read_text(encoding="utf-8")
    # Cost-unmeasurable copy fires when value_token_estimates is missing.
    assert "Rework cost" in html or "cost cannot be measured" in html.lower()


def test_pipeline_warm_lead_renders_warm_cta(tmp_path: Path) -> None:
    code, _, _ = _invoke(_basic_argv(tmp_path, "--warm-lead"))
    assert code == 0
    html = (tmp_path / "r.html").read_text(encoding="utf-8")
    # Warm-lead variant: no soft-ask, has 2 seed questions.
    assert "30-min" not in html
    # The seed-question helper produces two questions ending with '?'.
    assert html.count("?") >= 2


def test_pipeline_no_json_suppresses_json_output(tmp_path: Path) -> None:
    code, _, _ = _invoke(_basic_argv(tmp_path, "--no-json"))
    assert code == 0
    assert (tmp_path / "r.html").exists()
    assert not (tmp_path / "r.json").exists()


# -------------------------------------------------------------------- #
# 7-11: Argparse / flag handling
# -------------------------------------------------------------------- #


def test_no_flags_errors_on_missing_graph() -> None:
    code, _, stderr = _invoke([])
    assert code == 2
    assert "--graph" in stderr


def test_nonexistent_module_path_errors(tmp_path: Path) -> None:
    code, _, stderr = _invoke(
        [
            "--graph",
            "nonexistent.py:foo",
            "--output-html",
            str(tmp_path / "r.html"),
            "--no-json",
        ]
    )
    assert code == 1
    assert "graph file not found" in stderr.lower()


def test_nonexistent_function_errors(tmp_path: Path) -> None:
    code, _, stderr = _invoke(
        [
            "--graph",
            f"{GRAPH_PATH}:does_not_exist",
            "--output-html",
            str(tmp_path / "r.html"),
            "--no-json",
        ]
    )
    assert code == 1
    assert "does_not_exist" in stderr


def test_csv_flags_parse_to_tuples(tmp_path: Path) -> None:
    # Smoke test: pipeline accepts CSV ignore/track without crashing.
    code, _, _ = _invoke(
        _basic_argv(tmp_path, "--ignore", "foo, bar", "--track", "baz")
    )
    assert code == 0
    payload = json.loads((tmp_path / "r.json").read_text(encoding="utf-8"))
    # Override-only names that don't appear in events are dropped by the
    # classifier's resolver — the report still serialises cleanly.
    assert "verdict" in payload


def test_invalid_lead_pain_type_rejected(tmp_path: Path) -> None:
    code, _, stderr = _invoke(_basic_argv(tmp_path, "--lead-pain-type", "wrong"))
    assert code == 2
    assert "invalid choice" in stderr.lower() or "wrong" in stderr.lower()


# -------------------------------------------------------------------- #
# 12-16: Subcommand-style flags
# -------------------------------------------------------------------- #


def test_show_payload_prints_payload(tmp_path: Path) -> None:
    code, _, _ = _invoke(_basic_argv(tmp_path))
    assert code == 0
    json_path = tmp_path / "r.json"

    code, stdout, _ = _invoke(["--show-payload", str(json_path)])
    assert code == 0
    payload = json.loads(stdout)
    assert payload["schema_version"] == CCS_DIAGNOSE_LOG_SCHEMA_VERSION
    assert "verdict_bucket" in payload
    assert "coverage" in payload
    assert payload["installation_token"] is None  # Unit 8 stub


def test_show_payload_with_graph_warns_and_succeeds(tmp_path: Path) -> None:
    code, _, _ = _invoke(_basic_argv(tmp_path))
    assert code == 0
    json_path = tmp_path / "r.json"

    code, stdout, stderr = _invoke(
        ["--show-payload", str(json_path), "--graph", "ignored.py:bar"]
    )
    assert code == 0
    assert "warning: --graph ignored when --show-payload is set" in stderr
    assert json.loads(stdout)["schema_version"] == CCS_DIAGNOSE_LOG_SCHEMA_VERSION


def test_show_payload_missing_file_errors(tmp_path: Path) -> None:
    code, _, stderr = _invoke(["--show-payload", str(tmp_path / "nope.json")])
    assert code == 1
    assert "cannot read" in stderr.lower()


def test_show_payload_schema_mismatch_errors(tmp_path: Path) -> None:
    bad = tmp_path / "bad.json"
    bad.write_text(json.dumps({"random": "stuff"}), encoding="utf-8")
    code, _, stderr = _invoke(["--show-payload", str(bad)])
    assert code == 1
    assert "schema version mismatch" in stderr.lower()


def test_reset_token_prints_new_uuid(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Unit 8: --reset-token regenerates consent.json with a fresh UUID4."""
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path))
    code, stdout, _ = _invoke(["--reset-token"])
    assert code == 0
    assert "installation token regenerated:" in stdout
    consent_file = tmp_path / "ccs-diagnose" / "consent.json"
    assert consent_file.exists()
    consent_data = json.loads(consent_file.read_text(encoding="utf-8"))
    assert consent_data["granted"] is True
    assert consent_data["policy_version"] == 1
    # UUID4 string survives round-trip.
    import uuid as _uuid
    _uuid.UUID(consent_data["installation_token"])


# -------------------------------------------------------------------- #
# 17-20: Trust-posture flag stubs
# -------------------------------------------------------------------- #


def test_dry_run_prints_telemetry_payload(tmp_path: Path) -> None:
    code, stdout, _ = _invoke(_basic_argv(tmp_path, "--dry-run"))
    assert code == 0
    assert "would submit (dry-run)" in stdout
    # Find the JSON block after the marker.
    marker = "would submit (dry-run):"
    block = stdout.split(marker, 1)[1].strip()
    payload = json.loads(block)
    assert payload["schema_version"] == CCS_DIAGNOSE_LOG_SCHEMA_VERSION
    assert payload["installation_token"] is None


def test_no_network_is_noop_in_v0(tmp_path: Path) -> None:
    code, _, _ = _invoke(_basic_argv(tmp_path, "--no-network"))
    assert code == 0
    assert (tmp_path / "r.html").exists()


def test_no_telemetry_is_noop_in_v0(tmp_path: Path) -> None:
    code, _, _ = _invoke(_basic_argv(tmp_path, "--no-telemetry"))
    assert code == 0
    assert (tmp_path / "r.html").exists()


def test_calibration_record_prints_stub(tmp_path: Path) -> None:
    cal = tmp_path / "cal.jsonl"
    code, _, stderr = _invoke(
        _basic_argv(tmp_path, "--calibration-record", str(cal))
    )
    assert code == 0
    assert "calibration" in stderr.lower()
    # No write happened in v0.
    assert not cal.exists()


# -------------------------------------------------------------------- #
# 21-25: Defensive / edge cases
# -------------------------------------------------------------------- #


def test_volume_zero_is_treated_as_no_volume(tmp_path: Path) -> None:
    code, _, _ = _invoke(_basic_argv(tmp_path, "--volume", "0"))
    assert code == 0
    payload = json.loads((tmp_path / "r.json").read_text(encoding="utf-8"))
    # auto + volume==0 → auditability path; no annualised cost ridden.
    # (volume=0.0 still drives the detection cost extrapolation to 0.)
    assert payload["report"]["rework_cost_annualized"] == 0.0


def test_custom_token_cost_respected(tmp_path: Path) -> None:
    code, _, _ = _invoke(
        _basic_argv(
            tmp_path, "--volume", "50", "--cost-per-1k-tokens", "0.001"
        )
    )
    assert code == 0


def test_graph_invoke_failure_surfaces_warning(tmp_path: Path) -> None:
    """Graph that can't be invoked: pipeline finishes with insufficient verdict."""
    bad_module = tmp_path / "bad_graph.py"
    bad_module.write_text(
        """
def build_bad():
    class _Graph:
        def invoke(self, state, config=None):
            raise RuntimeError('boom')
    return _Graph()
""",
        encoding="utf-8",
    )
    code, stdout, _ = _invoke(
        [
            "--graph",
            f"{bad_module}:build_bad",
            "--output-html",
            str(tmp_path / "r.html"),
            "--no-json",
        ]
    )
    # The user still gets a (partial) report; exit 0 is the chosen
    # behaviour (graceful degradation).
    assert code == 0
    assert "Your write pattern" in stdout


def test_writers_by_key_consistent_for_clean_run(tmp_path: Path) -> None:
    """No inconsistency warning fires for a clean run against the substrate."""
    code, _, stderr = _invoke(_basic_argv(tmp_path))
    assert code == 0
    assert "writers_by_key inconsistency" not in stderr


def test_state_file_is_loaded_when_provided(tmp_path: Path) -> None:
    state_path = tmp_path / "state.json"
    state_path.write_text(
        json.dumps({"plan": {}, "log": [], "iteration": 0}),
        encoding="utf-8",
    )
    code, _, _ = _invoke(
        _basic_argv(tmp_path, "--state-file", str(state_path))
    )
    assert code == 0


# -------------------------------------------------------------------- #
# 26: Determinism
# -------------------------------------------------------------------- #


def test_repeat_runs_produce_identical_html(tmp_path: Path) -> None:
    out_a = tmp_path / "a.html"
    out_b = tmp_path / "b.html"
    code, _, _ = _invoke(
        [
            "--graph",
            GRAPH_FACTORY,
            "--output-html",
            str(out_a),
            "--no-json",
        ]
    )
    assert code == 0
    code, _, _ = _invoke(
        [
            "--graph",
            GRAPH_FACTORY,
            "--output-html",
            str(out_b),
            "--no-json",
        ]
    )
    assert code == 0
    assert out_a.read_bytes() == out_b.read_bytes()


# -------------------------------------------------------------------- #
# Subprocess form — verifies the ``python -m`` entrypoint
# -------------------------------------------------------------------- #


def test_python_m_entrypoint(tmp_path: Path) -> None:
    """One subprocess test pins the ``python -m ccs.cli.diagnose`` entrypoint."""
    out_path = tmp_path / "r.html"
    json_path = tmp_path / "r.json"
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "ccs.cli.diagnose",
            "--graph",
            GRAPH_FACTORY,
            "--output-html",
            str(out_path),
            "--output-json",
            str(json_path),
        ],
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, result.stderr
    assert out_path.exists()
    assert json_path.exists()


def test_python_m_help_exits_zero() -> None:
    result = subprocess.run(
        [sys.executable, "-m", "ccs.cli.diagnose", "--help"],
        capture_output=True,
        text=True,
        timeout=15,
    )
    assert result.returncode == 0
    assert "ccs-diagnose" in result.stdout
