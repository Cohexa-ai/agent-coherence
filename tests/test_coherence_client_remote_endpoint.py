"""Unit 1 (R0a): remote-endpoint transport + the default-off cross-host flag.

Verifies the loopback-only local path is byte-unchanged (``host`` defaults to
127.0.0.1) and that a remote host plumbs into ``base_url`` + the ``Host`` header,
plus the default-OFF ``RemoteCoordinatorConfig`` gate.
"""

from __future__ import annotations

import pytest

from ccs.cli import _coherence_client as cc
from ccs.cli._coherence_client import (
    CoordinatorEndpoint,
    CoordinatorUnavailable,
    RemoteCoordinatorConfig,
    resolve_remote_endpoint,
)

# --- CoordinatorEndpoint: local path unchanged, remote host plumbed ---------

def test_endpoint_default_host_is_loopback():
    ep = CoordinatorEndpoint(port=51234, bearer="deadbeef")
    assert ep.host == "127.0.0.1"
    assert ep.base_url == "http://127.0.0.1:51234"


def test_endpoint_positional_construction_still_works():
    # resolve_endpoint builds CoordinatorEndpoint(port=..., bearer=...); the new
    # defaulted host field must not break existing keyword/positional callers.
    ep = CoordinatorEndpoint(8080, "secret")
    assert ep.host == "127.0.0.1"


def test_endpoint_remote_host_in_base_url():
    ep = CoordinatorEndpoint(port=8080, bearer="deadbeef", host="10.0.0.5")
    assert ep.base_url == "http://10.0.0.5:8080"


@pytest.mark.parametrize("method", ["get", "post"])
def test_request_host_header_follows_endpoint_host(monkeypatch, method):
    captured: dict[str, str | None] = {}

    def fake_execute(req):
        captured["host"] = req.get_header("Host")
        return {"ok": True}

    monkeypatch.setattr(cc, "_execute", fake_execute)
    ep = CoordinatorEndpoint(port=8080, bearer="deadbeef", host="10.0.0.5")
    if method == "get":
        cc.get(ep, "/status")
    else:
        cc.post(ep, "/hooks/pre-read", {"path": "x"})
    assert captured["host"] == "10.0.0.5"


# --- resolve_remote_endpoint -----------------------------------------------

def test_resolve_remote_endpoint_builds_endpoint():
    ep = resolve_remote_endpoint("10.0.0.5", 8080, "s3cr3t")
    assert (ep.host, ep.port, ep.bearer) == ("10.0.0.5", 8080, "s3cr3t")
    assert ep.base_url == "http://10.0.0.5:8080"


@pytest.mark.parametrize("host,secret", [("", "s"), ("10.0.0.5", "")])
def test_resolve_remote_endpoint_rejects_empty(host, secret):
    with pytest.raises(CoordinatorUnavailable):
        resolve_remote_endpoint(host, 8080, secret)


# --- RemoteCoordinatorConfig: default OFF ----------------------------------

def test_flag_off_by_default():
    cfg = RemoteCoordinatorConfig.from_env(env={})
    assert cfg.enabled is False
    assert (cfg.host, cfg.port, cfg.secret) == (None, None, None)


@pytest.mark.parametrize("val", ["1", "true", "TRUE", "yes", "on"])
def test_flag_truthy_enables(val):
    cfg = RemoteCoordinatorConfig.from_env(env={"CCS_REMOTE_COORDINATOR": val})
    assert cfg.enabled is True


@pytest.mark.parametrize("val", ["", "0", "false", "no", "off", "  "])
def test_flag_falsy_stays_off(val):
    cfg = RemoteCoordinatorConfig.from_env(env={"CCS_REMOTE_COORDINATOR": val})
    assert cfg.enabled is False


def test_from_env_parses_host_port_and_secret_file(tmp_path):
    secret_file = tmp_path / "remote.secret"
    secret_file.write_text("  abc123  \n", encoding="utf-8")
    cfg = RemoteCoordinatorConfig.from_env(
        env={
            "CCS_REMOTE_COORDINATOR": "1",
            "CCS_REMOTE_HOST": "10.0.0.5",
            "CCS_REMOTE_PORT": "8080",
            "CCS_REMOTE_SECRET_FILE": str(secret_file),
        }
    )
    assert cfg.enabled is True
    assert (cfg.host, cfg.port, cfg.secret) == ("10.0.0.5", 8080, "abc123")


def test_from_env_missing_secret_file_yields_none(tmp_path):
    cfg = RemoteCoordinatorConfig.from_env(
        env={
            "CCS_REMOTE_COORDINATOR": "1",
            "CCS_REMOTE_SECRET_FILE": str(tmp_path / "nope.secret"),
        }
    )
    assert cfg.enabled is True
    assert cfg.secret is None


def test_from_env_non_numeric_port_is_none():
    cfg = RemoteCoordinatorConfig.from_env(
        env={"CCS_REMOTE_COORDINATOR": "1", "CCS_REMOTE_PORT": "notaport"}
    )
    assert cfg.port is None


def test_secret_file_symlink_is_rejected(tmp_path):
    # Hardening: a symlinked secret file is refused (O_NOFOLLOW) so an attacker
    # who can set CCS_REMOTE_SECRET_FILE cannot repoint it at another file.
    real = tmp_path / "real.secret"
    real.write_text("s3cr3t", encoding="utf-8")
    link = tmp_path / "link.secret"
    link.symlink_to(real)
    cfg = RemoteCoordinatorConfig.from_env(
        env={"CCS_REMOTE_COORDINATOR": "1", "CCS_REMOTE_SECRET_FILE": str(link)}
    )
    assert cfg.secret is None
