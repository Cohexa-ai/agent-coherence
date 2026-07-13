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

import logging

import pytest

from ccs.cli._coherence_client import (
    CoordinatorEndpoint,
    CoordinatorUnavailable,
    RemoteCoordinatorConfig,
    resolve_remote_endpoint,
)
from ccs.core.exceptions import CoherenceError, InsecureTransportRefused


# --- loopback passes freely (no ack, byte-unchanged) -----------------------
# 127.0.0.0/8 and ::1. IPv4-mapped loopback (::ffff:127.0.0.1) is deliberately NOT
# here — it is classified non-loopback (fail-closed); see the refused matrix below.
@pytest.mark.parametrize("host", ["127.0.0.1", "localhost", "::1", "127.0.0.5"])
def test_loopback_passes_without_ack(host: str) -> None:
    ep = resolve_remote_endpoint(host, 8080, "secret", env={})
    assert isinstance(ep, CoordinatorEndpoint)
    assert ep.host == host


# --- non-loopback without ack is refused (fail closed) ---------------------
# A non-IP hostname, a mapped-PUBLIC address (::ffff:8.8.8.8), AND a mapped-LOOPBACK
# form (::ffff:127.0.0.1) all fail closed: mapped IPv6 forms are classified
# non-loopback deterministically (stdlib is_loopback for the mapped form varies
# across CPython patch releases), so the ack is required regardless of interpreter.
@pytest.mark.parametrize(
    "host",
    [
        "10.0.0.5",
        "172.28.0.2",
        "coordinator.internal",
        "::ffff:8.8.8.8",
        "::ffff:127.0.0.1",
    ],
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


# --- base_url brackets IPv6 literals so the authority parses ----------------
# A loopback ::1 passes the guard, so its endpoint must yield a *usable* URL —
# http://[::1]:8080, not the ambiguous http://::1:8080 (urllib misparses the port).
@pytest.mark.parametrize(
    ("host", "env", "expected"),
    [
        ("::1", {}, "http://[::1]:8080"),  # loopback IPv6, no ack
        ("2001:db8::1", {"CCS_REMOTE_INSECURE": "1"}, "http://[2001:db8::1]:8080"),
        ("127.0.0.1", {}, "http://127.0.0.1:8080"),  # IPv4 stays unbracketed
    ],
)
def test_base_url_brackets_ipv6(host: str, env: dict, expected: str) -> None:
    ep = resolve_remote_endpoint(host, 8080, "secret", env=env)
    assert ep.base_url == expected


# --- the acknowledgement log line names the host, never the secret (R3) -----
def test_ack_log_names_host_never_secret(caplog: pytest.LogCaptureFixture) -> None:
    secret = "topsecret-bearer-value"
    with caplog.at_level(logging.WARNING):
        resolve_remote_endpoint(
            "10.0.0.5", 8080, secret, env={"CCS_REMOTE_INSECURE": "1"}
        )
    assert secret not in caplog.text
    assert "10.0.0.5" in caplog.text


# ===========================================================================
# Unit 2: verified-TLS positive signal (scheme=https passes the guard)
# ===========================================================================
#
# In THIS client ``https`` ALWAYS means enforced certificate verification —
# :func:`build_tls_context` has no insecure-https mode (the footgun rule). So a
# verified-TLS endpoint carries an in-band trust signal the plaintext guard
# lacked: it passes at mint with NO ack and NO warning, retiring the
# permanent-``CCS_REMOTE_INSECURE=1`` wart. The ``http`` rows above are
# byte-unchanged — the guard's plaintext refusal is not weakened.
#
# The matrix is scheme × host-class × ack:
#   scheme     ∈ {https, http}
#   host-class ∈ {loopback, routed}
#   ack        ∈ {no-ack, ack, falsey-ack}
# The http quadrant is already pinned by the tests above; these rows add the
# https quadrant (always-pass) plus the one https×ack log-cleanliness edge.


# --- https passes for every host class / ack value, with NO warning ---------
@pytest.mark.parametrize("host", ["10.0.0.5", "coordinator.internal", "127.0.0.1", "::1"])
@pytest.mark.parametrize("env", [{}, {"CCS_REMOTE_INSECURE": "1"}, {"CCS_REMOTE_INSECURE": "0"}])
def test_https_passes_regardless_of_host_or_ack(
    host: str, env: dict, caplog: pytest.LogCaptureFixture
) -> None:
    with caplog.at_level(logging.WARNING):
        ep = resolve_remote_endpoint(host, 8443, "secret", scheme="https", env=env)
    assert isinstance(ep, CoordinatorEndpoint)
    assert ep.host == host
    assert ep.scheme == "https"
    # No plaintext-ack warning is ever emitted on an https path — the ack is
    # irrelevant when verification is enforced (https short-circuits the guard
    # before the ack branch that would otherwise WARNING).
    assert caplog.text == ""


# --- the new positive signal: https + routed + no ack passes, no warning ----
def test_https_routed_no_ack_passes(caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level(logging.WARNING):
        ep = resolve_remote_endpoint(
            "10.0.0.5", 8443, "secret", scheme="https", env={}
        )
    assert ep.host == "10.0.0.5"
    assert ep.scheme == "https"
    assert ep.base_url == "https://10.0.0.5:8443"
    assert caplog.text == ""


# --- https + routed + CCS_REMOTE_INSECURE=1: passes, NO ack warning, no secret
def test_https_with_insecure_ack_emits_no_plaintext_warning(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # https short-circuits BEFORE the ack branch, so even with the ack set the
    # plaintext-bearer WARNING must not fire (and the secret must never appear).
    secret = "topsecret-bearer-value"
    with caplog.at_level(logging.WARNING):
        ep = resolve_remote_endpoint(
            "10.0.0.5", 8443, secret, scheme="https", env={"CCS_REMOTE_INSECURE": "1"}
        )
    assert ep.host == "10.0.0.5"
    assert secret not in caplog.text
    assert "plaintext" not in caplog.text.lower()
    assert caplog.text == ""


# --- ordering: empty checks fire BEFORE any guard/scheme logic, https or not -
def test_empty_secret_raises_unavailable_even_on_https() -> None:
    # Non-loopback host + scheme=https but empty secret -> CoordinatorUnavailable
    # (the empty-secret check), NOT a pass via the https positive signal. Pins the
    # #135 ordering contract: empty-input checks precede the transport guard, and
    # the bearer is never assembled on this path.
    with pytest.raises(CoordinatorUnavailable):
        resolve_remote_endpoint("10.0.0.5", 8443, "", scheme="https", env={})


def test_empty_host_raises_unavailable_even_on_https() -> None:
    with pytest.raises(CoordinatorUnavailable):
        resolve_remote_endpoint("", 8443, "secret", scheme="https", env={})


# --- http rows still refuse: the plaintext guard is NOT weakened by https ----
# Explicitly re-pin the routed-http refusal alongside the new https passes so a
# future edit that made scheme="http" fall through would fail here too.
def test_http_routed_no_ack_still_refused_with_explicit_scheme() -> None:
    with pytest.raises(InsecureTransportRefused):
        resolve_remote_endpoint("10.0.0.5", 8080, "secret", scheme="http", env={})
