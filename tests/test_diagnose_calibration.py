# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""Unit 9 tests — calibration corpus JSONL append.

Coverage map (mirrors the Unit 9 spec scenario list):

* Path resolution under ``XDG_DATA_HOME`` and the ``~/.local/share``
  fallback.
* Consent gating — denied state short-circuits without I/O.
* Successful append produces a JSONL line with the validated 3-tuple
  + flattened payload fields.
* Multiple appends produce distinct ``instance_id`` values, both with
  ``sequence_number == 1``.
* The Spike 0 falsification check — :func:`ccs.validation.validate_log`
  ingests the file with no gaps and no schema mismatches.
* Filesystem mode + permission edge cases.
* No import-time side effects.
* CLI integration — both with and without an explicit path.
"""

from __future__ import annotations

import json
import os
import stat
import subprocess
import sys
import textwrap
import uuid
from pathlib import Path

import pytest

from ccs.diagnose import CCS_DIAGNOSE_LOG_SCHEMA_VERSION
from ccs.diagnose.calibration import (
    DEFAULT_CALIBRATION_PATH_RELATIVE,
    CalibrationWriteResult,
    append_calibration_entry,
    calibration_path,
)
from ccs.diagnose.classifier import classify
from ccs.diagnose.detection import detect
from ccs.diagnose.telemetry import CURRENT_POLICY_VERSION, ConsentState
from ccs.validation import validate_log


# -------------------------------------------------------------------- #
# Fixtures
# -------------------------------------------------------------------- #


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Strip env vars that would leak between tests."""
    for name in (
        "XDG_DATA_HOME",
        "DO_NOT_TRACK",
        "DISABLE_TELEMETRY",
        "CCS_DIAGNOSE_NO_TELEMETRY",
        "CI",
        "XDG_CONFIG_HOME",
    ):
        monkeypatch.delenv(name, raising=False)


@pytest.fixture
def granted_consent() -> ConsentState:
    return ConsentState(
        granted=True,
        policy_version=CURRENT_POLICY_VERSION,
        installation_token=uuid.uuid4(),
    )


@pytest.fixture
def denied_consent() -> ConsentState:
    return ConsentState(
        granted=False,
        policy_version=CURRENT_POLICY_VERSION,
        installation_token=None,
    )


@pytest.fixture
def fake_verdict_and_report():
    """Build a minimal verdict + report tuple via the empty-buffer path."""
    verdict = classify(events=(), key_index={}, overrides=None)
    report = detect(events=(), verdict=verdict, key_index={})
    return verdict, report


# -------------------------------------------------------------------- #
# Path resolution
# -------------------------------------------------------------------- #


def test_calibration_path_uses_xdg_data_home(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("XDG_DATA_HOME", str(tmp_path))
    assert calibration_path() == tmp_path / DEFAULT_CALIBRATION_PATH_RELATIVE


def test_calibration_path_empty_xdg_uses_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("XDG_DATA_HOME", "")
    expected = Path.home() / ".local" / "share" / DEFAULT_CALIBRATION_PATH_RELATIVE
    assert calibration_path() == expected


def test_calibration_path_unset_xdg_uses_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("XDG_DATA_HOME", raising=False)
    expected = Path.home() / ".local" / "share" / DEFAULT_CALIBRATION_PATH_RELATIVE
    assert calibration_path() == expected


# -------------------------------------------------------------------- #
# Consent gating
# -------------------------------------------------------------------- #


def test_denied_consent_short_circuits(
    tmp_path: Path,
    fake_verdict_and_report,
    denied_consent: ConsentState,
) -> None:
    verdict, report = fake_verdict_and_report
    target = tmp_path / "cal.jsonl"
    result = append_calibration_entry(
        verdict=verdict,
        report=report,
        consent=denied_consent,
        path=target,
    )
    assert isinstance(result, CalibrationWriteResult)
    assert result.written is False
    assert result.reason == "consent_not_granted"
    assert not target.exists()


def test_granted_consent_writes_file(
    tmp_path: Path,
    fake_verdict_and_report,
    granted_consent: ConsentState,
) -> None:
    verdict, report = fake_verdict_and_report
    target = tmp_path / "cal.jsonl"
    result = append_calibration_entry(
        verdict=verdict,
        report=report,
        consent=granted_consent,
        path=target,
    )
    assert result.written is True
    assert result.reason == "ok"
    assert target.exists()
    assert target.stat().st_size > 0


# -------------------------------------------------------------------- #
# JSONL shape
# -------------------------------------------------------------------- #


def _read_lines(path: Path) -> list[dict]:
    with path.open(encoding="utf-8") as fh:
        return [json.loads(line) for line in fh if line.strip()]


def test_each_line_is_valid_json(
    tmp_path: Path,
    fake_verdict_and_report,
    granted_consent: ConsentState,
) -> None:
    verdict, report = fake_verdict_and_report
    target = tmp_path / "cal.jsonl"
    for _ in range(3):
        append_calibration_entry(
            verdict=verdict,
            report=report,
            consent=granted_consent,
            path=target,
        )
    entries = _read_lines(target)
    assert len(entries) == 3


def test_top_level_fields_present(
    tmp_path: Path,
    fake_verdict_and_report,
    granted_consent: ConsentState,
) -> None:
    verdict, report = fake_verdict_and_report
    target = tmp_path / "cal.jsonl"
    append_calibration_entry(
        verdict=verdict,
        report=report,
        consent=granted_consent,
        path=target,
    )
    entries = _read_lines(target)
    entry = entries[0]
    assert "sequence_number" in entry
    assert "instance_id" in entry
    assert "schema_version" in entry
    assert entry["schema_version"] == CCS_DIAGNOSE_LOG_SCHEMA_VERSION
    # UUID4 round-trips
    parsed = uuid.UUID(entry["instance_id"])
    assert parsed.version == 4


def test_no_duplicate_schema_version(
    tmp_path: Path,
    fake_verdict_and_report,
    granted_consent: ConsentState,
) -> None:
    """``schema_version`` must appear exactly once at top level (no nested duplicate)."""
    verdict, report = fake_verdict_and_report
    target = tmp_path / "cal.jsonl"
    append_calibration_entry(
        verdict=verdict,
        report=report,
        consent=granted_consent,
        path=target,
    )
    raw_line = target.read_text(encoding="utf-8")
    # JSON keys are unique by definition; we additionally verify the raw
    # text contains the schema_version key exactly once at the top level.
    assert raw_line.count('"schema_version"') == 1


def test_payload_fields_flattened_at_top_level(
    tmp_path: Path,
    fake_verdict_and_report,
    granted_consent: ConsentState,
) -> None:
    """Documented payload fields appear flat (no nested 'payload' object)."""
    verdict, report = fake_verdict_and_report
    target = tmp_path / "cal.jsonl"
    append_calibration_entry(
        verdict=verdict,
        report=report,
        consent=granted_consent,
        path=target,
    )
    entry = _read_lines(target)[0]
    for field in (
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
    ):
        assert field in entry, f"missing top-level field: {field}"
    assert "payload" not in entry  # no nested wrapper


def test_two_appends_distinct_instance_ids(
    tmp_path: Path,
    fake_verdict_and_report,
    granted_consent: ConsentState,
) -> None:
    verdict, report = fake_verdict_and_report
    target = tmp_path / "cal.jsonl"
    append_calibration_entry(
        verdict=verdict, report=report, consent=granted_consent, path=target
    )
    append_calibration_entry(
        verdict=verdict, report=report, consent=granted_consent, path=target
    )
    entries = _read_lines(target)
    assert len(entries) == 2
    assert entries[0]["sequence_number"] == 1
    assert entries[1]["sequence_number"] == 1
    assert entries[0]["instance_id"] != entries[1]["instance_id"]


def test_trailing_newline_no_double_blanks(
    tmp_path: Path,
    fake_verdict_and_report,
    granted_consent: ConsentState,
) -> None:
    verdict, report = fake_verdict_and_report
    target = tmp_path / "cal.jsonl"
    append_calibration_entry(
        verdict=verdict, report=report, consent=granted_consent, path=target
    )
    append_calibration_entry(
        verdict=verdict, report=report, consent=granted_consent, path=target
    )
    raw = target.read_text(encoding="utf-8")
    assert raw.endswith("\n")
    assert "\n\n" not in raw


# -------------------------------------------------------------------- #
# Spike 0 falsification check — validate_log compatibility
# -------------------------------------------------------------------- #


def test_validate_log_round_trip(
    tmp_path: Path,
    fake_verdict_and_report,
    granted_consent: ConsentState,
) -> None:
    """The load-bearing falsification gate: validate_log must accept the file."""
    verdict, report = fake_verdict_and_report
    target = tmp_path / "cal.jsonl"
    for _ in range(3):
        result = append_calibration_entry(
            verdict=verdict,
            report=report,
            consent=granted_consent,
            path=target,
        )
        assert result.written

    gaps, mismatches = validate_log(
        target,
        stream="diagnose_calibration",
        schema_version=CCS_DIAGNOSE_LOG_SCHEMA_VERSION,
    )
    assert gaps == []
    assert mismatches == []


# -------------------------------------------------------------------- #
# Filesystem behavior
# -------------------------------------------------------------------- #


def test_creates_parent_dir_with_restrictive_mode(
    tmp_path: Path,
    fake_verdict_and_report,
    granted_consent: ConsentState,
) -> None:
    verdict, report = fake_verdict_and_report
    target = tmp_path / "nested" / "deeply" / "cal.jsonl"
    assert not target.parent.exists()
    result = append_calibration_entry(
        verdict=verdict,
        report=report,
        consent=granted_consent,
        path=target,
    )
    assert result.written
    parent_mode = stat.S_IMODE(target.parent.stat().st_mode)
    # 0o700 was requested at creation. Some umask interactions may relax,
    # so accept anything no broader than 0o700.
    assert parent_mode & 0o077 == 0, f"parent mode too permissive: {oct(parent_mode)}"


def test_creates_file_with_0600_mode(
    tmp_path: Path,
    fake_verdict_and_report,
    granted_consent: ConsentState,
) -> None:
    verdict, report = fake_verdict_and_report
    target = tmp_path / "cal.jsonl"
    append_calibration_entry(
        verdict=verdict,
        report=report,
        consent=granted_consent,
        path=target,
    )
    file_mode = stat.S_IMODE(target.stat().st_mode)
    assert file_mode & 0o077 == 0, f"file mode too permissive: {oct(file_mode)}"


def test_existing_file_mode_preserved(
    tmp_path: Path,
    fake_verdict_and_report,
    granted_consent: ConsentState,
) -> None:
    """Don't aggressively re-chmod a pre-existing file."""
    verdict, report = fake_verdict_and_report
    target = tmp_path / "cal.jsonl"
    target.write_text("", encoding="utf-8")
    os.chmod(target, 0o644)
    pre = stat.S_IMODE(target.stat().st_mode)
    append_calibration_entry(
        verdict=verdict,
        report=report,
        consent=granted_consent,
        path=target,
    )
    post = stat.S_IMODE(target.stat().st_mode)
    assert pre == post, f"existing mode changed: {oct(pre)} -> {oct(post)}"


def test_readonly_parent_returns_io_error(
    tmp_path: Path,
    fake_verdict_and_report,
    granted_consent: ConsentState,
) -> None:
    if os.geteuid() == 0:
        pytest.skip("root bypasses POSIX permission checks")
    verdict, report = fake_verdict_and_report
    locked_parent = tmp_path / "locked"
    locked_parent.mkdir()
    target = locked_parent / "subdir" / "cal.jsonl"
    os.chmod(locked_parent, 0o500)  # r-x only, mkdir(subdir) will fail
    try:
        result = append_calibration_entry(
            verdict=verdict,
            report=report,
            consent=granted_consent,
            path=target,
        )
        assert result.written is False
        assert result.reason.startswith("io_error")
        assert not target.exists()
    finally:
        os.chmod(locked_parent, 0o700)


# -------------------------------------------------------------------- #
# Determinism (modulo per-call nondeterministic fields)
# -------------------------------------------------------------------- #


def test_entry_is_deterministic_modulo_id_and_timestamp(
    tmp_path: Path,
    fake_verdict_and_report,
    granted_consent: ConsentState,
) -> None:
    verdict, report = fake_verdict_and_report
    target = tmp_path / "cal.jsonl"
    for _ in range(2):
        append_calibration_entry(
            verdict=verdict,
            report=report,
            consent=granted_consent,
            path=target,
        )
    entries = _read_lines(target)

    # Strip the documented per-call non-deterministic fields. Everything
    # else must match across runs.
    nondet = {"instance_id", "timestamp_utc"}
    e0 = {k: v for k, v in entries[0].items() if k not in nondet}
    e1 = {k: v for k, v in entries[1].items() if k not in nondet}
    assert e0 == e1


# -------------------------------------------------------------------- #
# Result dataclass
# -------------------------------------------------------------------- #


def test_calibration_write_result_is_frozen() -> None:
    result = CalibrationWriteResult(written=False, path=None, reason="ok")
    with pytest.raises(Exception):  # FrozenInstanceError is dataclasses-internal
        result.written = True  # type: ignore[misc]


# -------------------------------------------------------------------- #
# No import-time side effects
# -------------------------------------------------------------------- #


_AUDIT_HARNESS = textwrap.dedent(
    """
    import os
    import sys

    # Point the user-data dir somewhere bogus so the calibration module
    # cannot accidentally touch a real directory even if the guard misfires.
    os.environ["XDG_DATA_HOME"] = "/tmp/_ccs_diagnose_calibration_guard_DOES_NOT_EXIST"
    os.environ["XDG_CONFIG_HOME"] = "/tmp/_ccs_diagnose_calibration_guard_CFG"

    violations: list[str] = []
    BANNED_EVENTS = {
        "socket.connect",
        "socket.bind",
        "socket.gethostbyname",
        "urllib.Request",
    }
    AUDIT_FRAMES = ("ccs.diagnose.calibration",)
    PATH_HINTS = (
        os.environ["XDG_DATA_HOME"],
        os.environ["XDG_CONFIG_HOME"],
        "ccs-diagnose",
        "calibration.jsonl",
    )

    def _from_audited_frame() -> str | None:
        frame = sys._getframe(2)
        while frame is not None:
            modname = frame.f_globals.get("__name__", "")
            if modname in AUDIT_FRAMES:
                return modname
            frame = frame.f_back
        return None

    def _is_suspect_open(args) -> bool:
        if not args:
            return False
        path = args[0]
        if isinstance(path, (bytes, bytearray)):
            try:
                path = path.decode("utf-8", errors="replace")
            except Exception:
                return False
        if not isinstance(path, str):
            return False
        return any(hint in path for hint in PATH_HINTS)

    def _hook(event: str, args) -> None:
        if event in BANNED_EVENTS:
            modname = _from_audited_frame()
            if modname is not None:
                violations.append(f"{event} from {modname}: args={args!r}")
        elif event == "open" and _is_suspect_open(args):
            modname = _from_audited_frame()
            if modname is not None:
                violations.append(f"open from {modname}: args={args!r}")

    sys.addaudithook(_hook)

    import ccs.diagnose.calibration  # noqa: F401

    if violations:
        print("VIOLATIONS:")
        for v in violations:
            print(f"  {v}")
        sys.exit(1)
    print("OK")
    """
)


def test_import_calibration_has_no_side_effects() -> None:
    result = subprocess.run(
        [sys.executable, "-c", _AUDIT_HARNESS],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, (
        f"audit hook flagged a side effect:\n"
        f"stdout:\n{result.stdout}\n"
        f"stderr:\n{result.stderr}"
    )
    assert "OK" in result.stdout


# -------------------------------------------------------------------- #
# CLI integration
# -------------------------------------------------------------------- #


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


cli_skip = pytest.mark.skipif(
    not _have_langgraph(),
    reason="ccs-diagnose CLI tests require [diagnose] extra (langgraph + jinja2).",
)


def _grant_consent(consent_dir: Path) -> uuid.UUID:
    """Pre-write a granted consent.json so resolve_consent skips the prompt."""
    token = uuid.uuid4()
    cdir = consent_dir / "ccs-diagnose"
    cdir.mkdir(parents=True, exist_ok=True)
    payload = {
        "granted": True,
        "policy_version": CURRENT_POLICY_VERSION,
        "installation_token": str(token),
    }
    (cdir / "consent.json").write_text(
        json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8"
    )
    return token


def _invoke(argv: list[str]) -> tuple[int, str, str]:
    import io
    from contextlib import redirect_stderr, redirect_stdout

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


@cli_skip
def test_cli_calibration_with_explicit_path_and_consent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cfg_dir = tmp_path / "xdg-config"
    monkeypatch.setenv("XDG_CONFIG_HOME", str(cfg_dir))
    # CI=1 so the consent prompt is non-interactive; pre-write a granted
    # consent so resolve_consent honors it instead of defaulting to denied.
    monkeypatch.setenv("CI", "1")
    _grant_consent(cfg_dir)
    cal = tmp_path / "cal.jsonl"
    code, stdout, stderr = _invoke(
        _basic_argv(tmp_path, "--calibration-record", str(cal))
    )
    assert code == 0, f"stderr:\n{stderr}\nstdout:\n{stdout}"
    assert cal.exists()
    assert "calibration entry appended" in stdout
    entries = _read_lines(cal)
    assert len(entries) == 1
    assert entries[0]["schema_version"] == CCS_DIAGNOSE_LOG_SCHEMA_VERSION


@cli_skip
def test_cli_calibration_skipped_when_consent_denied(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cfg_dir = tmp_path / "xdg-config"
    monkeypatch.setenv("XDG_CONFIG_HOME", str(cfg_dir))
    monkeypatch.setenv("CI", "1")  # forces non-interactive denied default
    cal = tmp_path / "cal.jsonl"
    code, stdout, _ = _invoke(
        _basic_argv(tmp_path, "--calibration-record", str(cal))
    )
    assert code == 0
    assert "calibration write skipped" in stdout
    assert "consent not granted" in stdout
    assert not cal.exists()


@cli_skip
def test_cli_calibration_default_path_used_when_flag_no_arg(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cfg_dir = tmp_path / "xdg-config"
    data_dir = tmp_path / "xdg-data"
    monkeypatch.setenv("XDG_CONFIG_HOME", str(cfg_dir))
    monkeypatch.setenv("XDG_DATA_HOME", str(data_dir))
    monkeypatch.setenv("CI", "1")
    _grant_consent(cfg_dir)

    code, stdout, stderr = _invoke(_basic_argv(tmp_path, "--calibration-record"))
    assert code == 0, f"stderr:\n{stderr}\nstdout:\n{stdout}"

    expected = data_dir / DEFAULT_CALIBRATION_PATH_RELATIVE
    assert expected.exists(), f"default path not used; {data_dir} contents: {list(data_dir.rglob('*'))}"
    assert "calibration entry appended" in stdout


@cli_skip
def test_cli_no_telemetry_forces_skip_even_with_calibration(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cfg_dir = tmp_path / "xdg-config"
    monkeypatch.setenv("XDG_CONFIG_HOME", str(cfg_dir))
    monkeypatch.setenv("CI", "1")
    _grant_consent(cfg_dir)  # consent pre-granted, but --no-telemetry overrides
    cal = tmp_path / "cal.jsonl"
    code, stdout, _ = _invoke(
        _basic_argv(
            tmp_path, "--no-telemetry", "--calibration-record", str(cal)
        )
    )
    assert code == 0
    assert "calibration write skipped" in stdout
    assert not cal.exists()


@cli_skip
def test_cli_do_not_track_forces_skip_even_with_calibration(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cfg_dir = tmp_path / "xdg-config"
    monkeypatch.setenv("XDG_CONFIG_HOME", str(cfg_dir))
    monkeypatch.setenv("CI", "1")
    monkeypatch.setenv("DO_NOT_TRACK", "1")
    _grant_consent(cfg_dir)
    cal = tmp_path / "cal.jsonl"
    code, stdout, _ = _invoke(
        _basic_argv(tmp_path, "--calibration-record", str(cal))
    )
    assert code == 0
    assert "calibration write skipped" in stdout
    assert not cal.exists()


# -------------------------------------------------------------------- #
# ccs-diagnose CLI documentation
# -------------------------------------------------------------------- #


def test_ccs_diagnose_doc_has_calibration_section() -> None:
    """The dedicated ccs-diagnose CLI reference at docs/ccs-diagnose.md
    must document the calibration corpus, the --calibration-record flag,
    validate_log compatibility, and the v0-preview -> v1 promotion gate.

    Previously inline in README.md; relocated to a dedicated doc during the
    v0.7 README groom so the README stays product-focused. README now
    surfaces this content via a TOC pointer at docs/ccs-diagnose.md.
    """
    doc = Path(__file__).resolve().parent.parent / "docs" / "ccs-diagnose.md"
    text = doc.read_text(encoding="utf-8")
    assert "## Calibration corpus" in text
    assert "--calibration-record" in text
    assert "calibration.jsonl" in text
    assert "validate_log" in text
    # Promotion gate criteria mentioned.
    assert "v0-preview" in text
    assert "Promotion to" in text


# -------------------------------------------------------------------- #
# Concurrent-append atomicity (POSIX flock)
# -------------------------------------------------------------------- #


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="fcntl.flock is POSIX-only; Windows uses best-effort O_APPEND.",
)
def test_concurrent_appends_do_not_interleave_bytes(
    tmp_path: Path,
    fake_verdict_and_report,
    granted_consent: ConsentState,
) -> None:
    """Two threads each append 100 entries to the same JSONL file.

    With ``fcntl.flock``, all 200 lines must remain valid JSON and
    the total line count must be exactly 200 — no partial-line
    interleaving from PIPE_BUF underestimation on macOS.
    """
    import threading

    verdict, report = fake_verdict_and_report
    target = tmp_path / "concurrent.jsonl"
    errors: list[BaseException] = []

    def producer(n: int) -> None:
        try:
            for _ in range(n):
                result = append_calibration_entry(
                    verdict=verdict,
                    report=report,
                    consent=granted_consent,
                    path=target,
                )
                assert result.written is True, result.reason
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    t1 = threading.Thread(target=producer, args=(100,))
    t2 = threading.Thread(target=producer, args=(100,))
    t1.start()
    t2.start()
    t1.join(timeout=30)
    t2.join(timeout=30)

    assert not errors, errors
    lines = target.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 200, f"expected 200 lines, got {len(lines)}"
    # Every line is valid JSON — no torn writes.
    for line in lines:
        json.loads(line)
    # validate_log accepts the whole file as one stream per instance_id.
    gaps, mismatches = validate_log(
        target,
        stream="diagnose_calibration",
        schema_version=CCS_DIAGNOSE_LOG_SCHEMA_VERSION,
    )
    assert gaps == []
    assert mismatches == []
