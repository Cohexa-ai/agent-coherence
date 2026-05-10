# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""Tests for ``ccs.hardening.release_readiness``.

Covers the three automated ``gh api`` checks plus the report formatter.
``_gh_api`` is monkeypatched throughout so the suite never shells out
to the real ``gh`` binary.
"""

from __future__ import annotations

import json
from typing import Any

import pytest

from ccs.hardening import release_readiness


# -------------------------------------------------------------------- #
# Helpers
# -------------------------------------------------------------------- #


def _gh_stub(rc: int, stdout: str = "", stderr: str = ""):
    """Return a ``_gh_api`` replacement that always yields the given tuple."""

    def _stub(path: str) -> tuple[int, str, str]:  # noqa: ARG001
        return rc, stdout, stderr

    return _stub


def _routed_gh_stub(routes: dict[str, tuple[int, str, str]]):
    """Return a ``_gh_api`` replacement that dispatches by api path."""

    def _stub(path: str) -> tuple[int, str, str]:
        if path not in routes:
            raise AssertionError(f"unexpected gh api path: {path!r}")
        return routes[path]

    return _stub


# -------------------------------------------------------------------- #
# Individual checks
# -------------------------------------------------------------------- #


def test_pypi_environment_check_passes_on_200(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        release_readiness,
        "_gh_api",
        _gh_stub(0, json.dumps({"name": "pypi"})),
    )
    result = release_readiness.check_pypi_environment("o", "r", "pypi")
    assert result.ok is True
    assert "pypi" in result.name


def test_pypi_environment_check_fails_on_404(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        release_readiness,
        "_gh_api",
        _gh_stub(1, "", "HTTP 404: Not Found"),
    )
    result = release_readiness.check_pypi_environment("o", "r", "pypi")
    assert result.ok is False
    assert "404" in result.detail


def test_pypi_environment_check_fails_on_malformed_json(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        release_readiness, "_gh_api", _gh_stub(0, "not-json")
    )
    result = release_readiness.check_pypi_environment("o", "r", "pypi")
    assert result.ok is False
    assert "valid JSON" in result.detail


def test_branch_protection_check_passes_on_200(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        release_readiness, "_gh_api", _gh_stub(0, json.dumps({"required": True}))
    )
    result = release_readiness.check_branch_protection("o", "r", "main")
    assert result.ok is True


def test_branch_protection_check_fails_on_404(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        release_readiness, "_gh_api", _gh_stub(1, "", "HTTP 404: Not Found")
    )
    result = release_readiness.check_branch_protection("o", "r", "main")
    assert result.ok is False


def test_tag_protection_check_passes_when_pattern_present(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rules = [{"pattern": "v*"}, {"pattern": "release-*"}]
    monkeypatch.setattr(
        release_readiness, "_gh_api", _gh_stub(0, json.dumps(rules))
    )
    result = release_readiness.check_tag_protection("o", "r", "v*")
    assert result.ok is True


def test_tag_protection_check_fails_when_pattern_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rules: list[dict[str, Any]] = [{"pattern": "release-*"}]
    monkeypatch.setattr(
        release_readiness, "_gh_api", _gh_stub(0, json.dumps(rules))
    )
    result = release_readiness.check_tag_protection("o", "r", "v*")
    assert result.ok is False
    assert "no tag-protection rule" in result.detail


def test_tag_protection_check_fails_on_non_list_response(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        release_readiness,
        "_gh_api",
        _gh_stub(0, json.dumps({"message": "Not Found"})),
    )
    result = release_readiness.check_tag_protection("o", "r", "v*")
    assert result.ok is False
    assert "unexpected response shape" in result.detail


# -------------------------------------------------------------------- #
# Aggregate + CLI
# -------------------------------------------------------------------- #


def _all_ok_routes(owner: str, repo: str) -> dict[str, tuple[int, str, str]]:
    return {
        f"repos/{owner}/{repo}/environments/pypi": (
            0, json.dumps({"name": "pypi"}), ""
        ),
        f"repos/{owner}/{repo}/branches/main/protection": (
            0, json.dumps({"required": True}), ""
        ),
        f"repos/{owner}/{repo}/tags/protection": (
            0, json.dumps([{"pattern": "v*"}]), ""
        ),
    }


def test_run_automated_checks_all_pass(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        release_readiness, "_gh_api", _routed_gh_stub(_all_ok_routes("o", "r"))
    )
    results = release_readiness.run_automated_checks(
        owner="o", repo="r", environment="pypi", branch="main", tag_pattern="v*",
    )
    assert all(r.ok for r in results)
    assert len(results) == 3


def test_main_exits_zero_when_all_checks_pass(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(
        release_readiness, "_gh_api", _routed_gh_stub(_all_ok_routes("o", "r"))
    )
    rc = release_readiness.main(["--owner", "o", "--repo", "r"])
    assert rc == 0
    captured = capsys.readouterr().out
    assert "Automated release-readiness checks" in captured
    assert "Manual verification still required" in captured


def test_main_exits_nonzero_when_any_check_fails(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    routes = _all_ok_routes("o", "r")
    routes["repos/o/r/branches/main/protection"] = (1, "", "HTTP 404: Not Found")
    monkeypatch.setattr(
        release_readiness, "_gh_api", _routed_gh_stub(routes)
    )
    rc = release_readiness.main(["--owner", "o", "--repo", "r"])
    assert rc == 1
    captured = capsys.readouterr().out
    assert "FAIL" in captured


def test_main_handles_missing_gh_binary(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """When the real ``gh`` is missing, every check reports ``gh CLI not found``."""
    monkeypatch.setattr(
        release_readiness.shutil, "which", lambda _name: None
    )
    rc = release_readiness.main(["--owner", "o", "--repo", "r"])
    assert rc == 1
    captured = capsys.readouterr().out
    assert "gh CLI not found" in captured


def test_format_report_lists_manual_items() -> None:
    results = (
        release_readiness.CheckResult(name="x", ok=True, detail="ok"),
    )
    report = release_readiness.format_report(results)
    assert "Manual verification still required" in report
    assert "2FA" in report
    assert "Typosquat" in report
    assert "audit-log" in report
    assert "SBOM" in report
