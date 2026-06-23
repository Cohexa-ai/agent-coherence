"""Unit 4 (R1): cross-host bind validation + configurable Host-allowlist.

Function-level, platform-agnostic (a real non-loopback bind is Linux-only and
lives in the netns harness, Unit 5). Covers the explicit-range bind validator,
the derived allowlist, and that a relaxed allowlist still 403s a third host
(the forged-Host logic, R7 #1).
"""

from __future__ import annotations

import pytest

from ccs.adapters.claude_code.auth import (
    build_host_allowlist,
    validate_bind_host,
    verify_host,
)


@pytest.mark.parametrize("host", ["127.0.0.1", "localhost"])
def test_loopback_always_allowed_without_flag(host, monkeypatch):
    monkeypatch.delenv("CCS_REMOTE_COORDINATOR", raising=False)
    validate_bind_host(host)  # no raise


def test_nonloopback_requires_flag(monkeypatch):
    monkeypatch.delenv("CCS_REMOTE_COORDINATOR", raising=False)
    with pytest.raises(ValueError, match="CCS_REMOTE_COORDINATOR"):
        validate_bind_host("10.0.0.5")


@pytest.mark.parametrize("host", ["10.0.0.5", "172.16.3.4", "192.168.1.1"])
def test_private_range_accepted_with_flag(host, monkeypatch):
    monkeypatch.setenv("CCS_REMOTE_COORDINATOR", "1")
    validate_bind_host(host)  # no raise


@pytest.mark.parametrize(
    "host",
    ["0.0.0.0", "127.0.0.2", "169.254.1.1", "100.64.0.1", "8.8.8.8", "notanip"],
)
def test_disallowed_bind_hosts_rejected_even_with_flag(host, monkeypatch):
    """is_private would wrongly admit 0.0.0.0 and 127.0.0.2 — explicit ranges reject."""
    monkeypatch.setenv("CCS_REMOTE_COORDINATOR", "1")
    with pytest.raises(ValueError):
        validate_bind_host(host)


def test_build_allowlist_loopback_is_default(monkeypatch):
    monkeypatch.delenv("CCS_REMOTE_COORDINATOR", raising=False)
    assert build_host_allowlist("127.0.0.1") == frozenset({"localhost", "127.0.0.1"})


def test_build_allowlist_adds_validated_bind_host(monkeypatch):
    monkeypatch.setenv("CCS_REMOTE_COORDINATOR", "1")
    allow = build_host_allowlist("10.0.0.5")
    assert "10.0.0.5" in allow
    assert {"localhost", "127.0.0.1"} <= allow


def test_relaxed_allowlist_still_rejects_third_host():
    # R7 #1: relaxing to admit the bind host must NOT admit anything else.
    allow = frozenset({"localhost", "127.0.0.1", "10.0.0.5"})
    assert verify_host("10.0.0.5", allow) is True
    assert verify_host("10.0.0.5:8080", allow) is True  # port stripped
    assert verify_host("attacker.example.com", allow) is False
    assert verify_host("10.0.0.6", allow) is False


def test_verify_host_default_allowlist_unchanged():
    assert verify_host("127.0.0.1") is True
    assert verify_host("localhost") is True
    assert verify_host("attacker.example.com") is False
    assert verify_host(None) is False


def test_coordinator_rejects_bad_bind_host(tmp_path, monkeypatch):
    """Constructing a coordinator with a disallowed bind raises at construction."""
    monkeypatch.delenv("CCS_REMOTE_COORDINATOR", raising=False)
    from ccs.adapters.claude_code.coordinator_server import CoordinatorHTTPServer

    with pytest.raises(ValueError):
        CoordinatorHTTPServer(tmp_path, bind_host="10.0.0.5")  # non-loopback, flag off


def test_coordinator_cli_has_bind_host_flag():
    """The coordinator CLI exposes --bind-host (default loopback) so the README's
    documented cross-host command actually works."""
    from ccs.cli.coherence_coordinator import build_parser

    assert build_parser().parse_args([]).bind_host == "127.0.0.1"
    assert build_parser().parse_args(["--bind-host", "10.0.0.5"]).bind_host == "10.0.0.5"
