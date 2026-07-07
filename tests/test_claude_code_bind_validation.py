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


def test_build_allowlist_adds_validated_ipv6_bind_host(monkeypatch):
    monkeypatch.setenv("CCS_REMOTE_COORDINATOR", "1")
    allow = build_host_allowlist("fc00::1")  # RFC-4193 unique-local
    assert "fc00::1" in allow
    assert {"localhost", "127.0.0.1"} <= allow


def test_verify_host_accepts_bracketed_ipv6():
    """A Host header for an IPv6 bind is bracketed ([addr]:port). The port-strip
    must parse the bracket form, not split on the address's own colons."""
    allow = frozenset({"localhost", "127.0.0.1", "fc00::1"})
    assert verify_host("[fc00::1]:8080", allow) is True
    assert verify_host("[fc00::1]", allow) is True  # no explicit port


def test_verify_host_rejects_other_bracketed_ipv6():
    """Relaxing to admit one IPv6 bind must NOT admit a different IPv6 host."""
    allow = frozenset({"localhost", "127.0.0.1", "fc00::1"})
    assert verify_host("[fc00::2]:8080", allow) is False
    assert verify_host("[::1]:8080", allow) is False  # not in allowlist


def test_verify_host_matches_equivalent_ipv6_spelling():
    """Equivalent IPv6 spellings (expanded vs compressed) resolve to the same
    allowlist entry — matching is on the normalized address, not the raw string."""
    allow = frozenset({"localhost", "127.0.0.1", "fc00::1"})
    assert verify_host("[fc00:0:0:0:0:0:0:1]:8080", allow) is True


def test_verify_host_rejects_malformed_bracket_and_hostname():
    """Fail-closed: a malformed bracket or a DNS-rebind hostname is rejected."""
    allow = frozenset({"localhost", "127.0.0.1", "fc00::1"})
    assert verify_host("[fc00::1", allow) is False  # missing closing bracket
    assert verify_host("[fc00::1]junk:8080", allow) is False  # junk after "]"
    assert verify_host("attacker.example.com", allow) is False  # DNS-rebind guard


@pytest.mark.parametrize(
    "host",
    [
        "[::ffff:127.0.0.1]:8080",  # IPv4-mapped IPv6 must NOT alias 127.0.0.1
        "[fc00::1%eth0]:8080",  # scope id must NOT match the non-scoped entry
        "2130706433",  # integer form of 127.0.0.1
        "0177.0.0.1",  # octal IPv4
        "127.1",  # abbreviated IPv4
        "localhost\r",  # trailing control char (no longer stripped)
    ],
)
def test_verify_host_rejects_ip_aliasing_and_malformed_forms(host):
    """Security: the normalized-IP fallback admits ONLY a host whose canonical IP
    is an allowlist entry — never an aliased/mapped/scoped/alt-radix spelling of
    one, and never a control-char-padded name."""
    allow = frozenset({"localhost", "127.0.0.1", "fc00::1"})
    assert verify_host(host, allow) is False


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


# ---------------------------------------------------------------------------
# Unit 3 (R5): coordinator-side routed-bind TLS/insecure-ack guard.
#
# Fail-closed symmetry with the client's #135 plaintext-bearer guard: a
# non-loopback (private-range, already CCS_REMOTE_COORDINATOR-gated) bind must
# either assert a TLS-terminating front (CCS_TLS_TERMINATED) or explicitly
# acknowledge the insecure link (CCS_SERVE_INSECURE); otherwise construction
# raises. Loopback binds read NEITHER env and are byte-unchanged.
# ---------------------------------------------------------------------------

from ccs.adapters.claude_code.auth import assert_serve_transport_acknowledged


def _clear_serve_envs(monkeypatch):
    monkeypatch.delenv("CCS_TLS_TERMINATED", raising=False)
    monkeypatch.delenv("CCS_SERVE_INSECURE", raising=False)


@pytest.mark.parametrize("host", ["127.0.0.1", "localhost"])
def test_serve_guard_loopback_reads_no_env(host, monkeypatch, caplog):
    """Loopback: the guard returns without reading either env and logs nothing."""
    # Set BOTH to truthy — a loopback bind must ignore them entirely (byte-unchanged).
    monkeypatch.setenv("CCS_TLS_TERMINATED", "1")
    monkeypatch.setenv("CCS_SERVE_INSECURE", "1")
    with caplog.at_level("INFO", logger="ccs.adapters.claude_code.auth"):
        assert_serve_transport_acknowledged(host)  # no raise
    assert caplog.records == []


def test_serve_guard_tls_terminated_logs_posture(monkeypatch, caplog):
    """Private-range bind + CCS_TLS_TERMINATED=1 → passes with an INFO posture log."""
    _clear_serve_envs(monkeypatch)
    monkeypatch.setenv("CCS_TLS_TERMINATED", "1")
    with caplog.at_level("INFO", logger="ccs.adapters.claude_code.auth"):
        assert_serve_transport_acknowledged("10.0.0.5")  # no raise
    assert any(r.levelname == "INFO" for r in caplog.records)
    assert "CCS_TLS_TERMINATED" in caplog.text
    assert "10.0.0.5" in caplog.text
    # The bind host is named; the guard never logs a secret (there is none here).
    assert not any(r.levelname == "WARNING" for r in caplog.records)


def test_serve_guard_serve_insecure_logs_warning(monkeypatch, caplog):
    """Private-range bind + CCS_SERVE_INSECURE=1 → passes with a WARNING."""
    _clear_serve_envs(monkeypatch)
    monkeypatch.setenv("CCS_SERVE_INSECURE", "1")
    with caplog.at_level("WARNING", logger="ccs.adapters.claude_code.auth"):
        assert_serve_transport_acknowledged("10.0.0.5")  # no raise
    assert any(r.levelname == "WARNING" for r in caplog.records)
    assert "CCS_SERVE_INSECURE" in caplog.text
    assert "10.0.0.5" in caplog.text


def test_serve_guard_neither_env_raises_and_names_both(monkeypatch):
    """Private-range bind + neither env → raises; message names BOTH envs actionably."""
    _clear_serve_envs(monkeypatch)
    with pytest.raises(ValueError) as excinfo:
        assert_serve_transport_acknowledged("10.0.0.5")
    message = str(excinfo.value)
    assert "CCS_TLS_TERMINATED" in message
    assert "CCS_SERVE_INSECURE" in message
    assert "10.0.0.5" in message


def test_serve_guard_both_envs_assertion_wins_single_log(monkeypatch, caplog):
    """Both envs set → passes; the TLS assertion wins the log (INFO, no WARNING)."""
    _clear_serve_envs(monkeypatch)
    monkeypatch.setenv("CCS_TLS_TERMINATED", "1")
    monkeypatch.setenv("CCS_SERVE_INSECURE", "1")
    with caplog.at_level("INFO", logger="ccs.adapters.claude_code.auth"):
        assert_serve_transport_acknowledged("10.0.0.5")  # no raise
    info_records = [r for r in caplog.records if r.levelname == "INFO"]
    warn_records = [r for r in caplog.records if r.levelname == "WARNING"]
    assert len(info_records) == 1  # single posture line, no double-warn
    assert warn_records == []
    assert "CCS_TLS_TERMINATED" in caplog.text


@pytest.mark.parametrize("falsey", ["0", "false", "no", "off", "", "  ", "nope"])
def test_serve_guard_falsey_values_behave_as_unset(falsey, monkeypatch):
    """Falsey/empty values are treated as unset → private-range bind still raises."""
    _clear_serve_envs(monkeypatch)
    monkeypatch.setenv("CCS_TLS_TERMINATED", falsey)
    monkeypatch.setenv("CCS_SERVE_INSECURE", falsey)
    with pytest.raises(ValueError):
        assert_serve_transport_acknowledged("10.0.0.5")


def test_coordinator_routed_bind_requires_serve_ack(tmp_path, monkeypatch):
    """A routed bind with the cross-host flag on but NO transport ack raises at
    construction — fail-closed symmetry with the client guard."""
    monkeypatch.setenv("CCS_REMOTE_COORDINATOR", "1")
    _clear_serve_envs(monkeypatch)
    from ccs.adapters.claude_code.coordinator_server import CoordinatorHTTPServer

    with pytest.raises(ValueError, match="CCS_TLS_TERMINATED"):
        CoordinatorHTTPServer(tmp_path, bind_host="10.0.0.5")


def test_coordinator_routed_bind_constructs_with_insecure_ack(tmp_path, monkeypatch):
    """A routed bind constructs once CCS_SERVE_INSECURE acknowledges the link.

    Uses 127.0.0.1 for the actual socket bind (port 0) so the test is
    platform-agnostic, but drives the guard by pre-validating the routed host:
    the guard path is exercised in the function-level tests above; here we assert
    the routed construction path does not raise once the ack is present.
    """
    monkeypatch.setenv("CCS_REMOTE_COORDINATOR", "1")
    _clear_serve_envs(monkeypatch)
    monkeypatch.setenv("CCS_SERVE_INSECURE", "1")
    from ccs.adapters.claude_code.coordinator_server import CoordinatorHTTPServer

    # A private-range bind cannot open a real socket on most CI hosts, so we
    # assert the guard admits it by construction up to the socket bind. The
    # OSError from the unbindable address (if any) is distinct from the guard's
    # ValueError; a ValueError here would mean the ack was not honored.
    try:
        server = CoordinatorHTTPServer(tmp_path, bind_host="10.0.0.5")
    except ValueError:  # pragma: no cover - would signal the ack was ignored
        raise
    except OSError:
        return  # address not assignable on this host — guard already passed
    server.shutdown()
