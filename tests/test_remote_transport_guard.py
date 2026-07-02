# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Fail-closed transport guard (Phase-1.5 client-side sliver).

``resolve_remote_endpoint`` refuses to mint an endpoint for a NON-loopback host
over plaintext HTTP unless the operator acknowledges the link is secured
(``CCS_REMOTE_INSECURE``). Loopback is byte-unchanged. The transport is always
``http://`` (encryption is operator-provided out-of-band), so the ack is an
explicit operator assertion, not an in-band TLS signal.
"""

from __future__ import annotations

import pytest

from ccs.cli._coherence_client import (
    CoordinatorEndpoint,
    CoordinatorUnavailable,
    RemoteCoordinatorConfig,
    resolve_remote_endpoint,
)
from ccs.core.exceptions import CoherenceError, InsecureTransportRefused


# --- loopback passes freely (no ack, byte-unchanged) -----------------------
# Includes 127.0.0.0/8, ::1, and IPv4-mapped loopback (::ffff:127.0.0.1) — the last
# is genuinely loopback (ipaddress.is_loopback unwraps the mapped IPv4), traffic
# never leaves the host, so no ack is needed.
@pytest.mark.parametrize(
    "host", ["127.0.0.1", "localhost", "::1", "127.0.0.5", "::ffff:127.0.0.1"]
)
def test_loopback_passes_without_ack(host: str) -> None:
    ep = resolve_remote_endpoint(host, 8080, "secret", env={})
    assert isinstance(ep, CoordinatorEndpoint)
    assert ep.host == host


# --- non-loopback without ack is refused (fail closed) ---------------------
# A non-IP hostname and a mapped-PUBLIC address (::ffff:8.8.8.8, is_loopback False)
# are correctly non-loopback — no bypass via the mapped form.
@pytest.mark.parametrize(
    "host", ["10.0.0.5", "172.28.0.2", "coordinator.internal", "::ffff:8.8.8.8"]
)
def test_non_loopback_without_ack_refused(host: str) -> None:
    with pytest.raises(InsecureTransportRefused):
        resolve_remote_endpoint(host, 8080, "secret", env={})


def test_insecure_transport_refused_is_coherence_error() -> None:
    assert issubclass(InsecureTransportRefused, CoherenceError)


def test_refusal_message_is_actionable() -> None:
    with pytest.raises(InsecureTransportRefused) as exc:
        resolve_remote_endpoint("10.0.0.5", 8080, "secret", env={})
    msg = str(exc.value)
    assert "10.0.0.5" in msg
    assert "CCS_REMOTE_INSECURE" in msg


# --- non-loopback WITH ack sends (behavior unchanged) ----------------------
@pytest.mark.parametrize("ack", ["1", "true", "yes", "on"])
def test_non_loopback_with_ack_passes(ack: str) -> None:
    ep = resolve_remote_endpoint(
        "10.0.0.5", 8080, "secret", env={"CCS_REMOTE_INSECURE": ack}
    )
    assert ep.host == "10.0.0.5"


@pytest.mark.parametrize("ack", ["", "0", "false", "no"])
def test_non_loopback_falsey_ack_refused(ack: str) -> None:
    with pytest.raises(InsecureTransportRefused):
        resolve_remote_endpoint(
            "10.0.0.5", 8080, "secret", env={"CCS_REMOTE_INSECURE": ack}
        )


# --- ordering: empty-input checks fire BEFORE the guard --------------------
def test_empty_secret_raises_unavailable_not_refused() -> None:
    # Non-loopback host but empty secret -> CoordinatorUnavailable (empty check),
    # NOT the transport guard. Proves the guard sits after the empty checks so
    # the pre-existing rejects-empty test stays green.
    with pytest.raises(CoordinatorUnavailable):
        resolve_remote_endpoint("10.0.0.5", 8080, "", env={})


def test_empty_host_raises_unavailable() -> None:
    with pytest.raises(CoordinatorUnavailable):
        resolve_remote_endpoint("", 8080, "secret", env={})


# --- default env is os.environ when not injected ---------------------------
def test_default_env_reads_os_environ(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("CCS_REMOTE_INSECURE", raising=False)
    with pytest.raises(InsecureTransportRefused):
        resolve_remote_endpoint("10.0.0.5", 8080, "secret")  # no env -> os.environ
    monkeypatch.setenv("CCS_REMOTE_INSECURE", "1")
    ep = resolve_remote_endpoint("10.0.0.5", 8080, "secret")
    assert ep.host == "10.0.0.5"


# --- integration: minting from a from_env-derived host is guarded ----------
def test_from_env_nonloopback_without_ack_is_guarded_on_mint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("CCS_REMOTE_COORDINATOR", "1")
    monkeypatch.setenv("CCS_REMOTE_HOST", "10.0.0.5")
    monkeypatch.delenv("CCS_REMOTE_INSECURE", raising=False)
    cfg = RemoteCoordinatorConfig.from_env()
    assert cfg.enabled and cfg.host == "10.0.0.5"
    with pytest.raises(InsecureTransportRefused):
        resolve_remote_endpoint(cfg.host, cfg.port or 8080, "secret")
