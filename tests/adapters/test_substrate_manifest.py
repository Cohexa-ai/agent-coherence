# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Tests for the Coherence Manifest: secret-safe loader, SSRF-constrained
connection targets, TLS-required guard, and config-time tier visibility.

Security-first: the allowlist-of-forms credential reject and every SSRF/TLS
reject are validation-layer assertions — none are gated behind a real
substrate. The DNS resolver is INJECTED (a callable seam), so no test touches
real DNS: a "hostname resolving to a denied IP" is expressed by the injected
mapping, and decimal/octal/hex IPv4 literals are expressed as the resolver
normalizing them to the real address a string check never classifies.
"""

from __future__ import annotations

import logging
from pathlib import Path

import pytest
import yaml

from ccs.adapters.substrate_manifest import (
    ALLOW_PRIVATE_ENV,
    INSECURE_ACK_ENV,
    ManifestArtifact,
    SubstrateManifest,
    load_manifest,
    resolve_host_addresses,
)
from ccs.core.exceptions import (
    ManifestError,
    SubstrateCredentialRefused,
    SubstrateInsecureTransport,
    SubstrateTargetDenied,
)
from ccs.core.substrate import Tier

PUBLIC_IP = "93.184.216.34"
#: A distinctive credential value: asserted to NEVER appear in any log/exception.
UNIQUE_SECRET_REF = "secret-file:/run/secrets/UNIQUE-do-not-leak-me"


def make_resolver(mapping: dict[str, list[str]]):
    """A hermetic DNS resolver seam: hostname -> resolved IP strings."""

    def _resolve(host: str) -> tuple[str, ...]:
        return tuple(mapping.get(host, ()))

    return _resolve


def write_manifest(tmp_path: Path, data: object, *, mode: int = 0o600) -> Path:
    """Serialize ``data`` to a YAML manifest file and tighten its mode."""
    path = tmp_path / "manifest.yaml"
    path.write_text(yaml.safe_dump(data), encoding="utf-8")
    path.chmod(mode)
    return path


def load(
    tmp_path: Path,
    data: object,
    *,
    resolver=None,
    env: dict[str, str] | None = None,
    mode: int = 0o600,
) -> SubstrateManifest:
    """Write + load a manifest with an injected resolver and empty env."""
    path = write_manifest(tmp_path, data, mode=mode)
    return load_manifest(
        path,
        resolver=resolver or make_resolver({"db.example.com": [PUBLIC_IP]}),
        env={} if env is None else env,
    )


def pg_artifact(dsn: str, *, credential: str = "secret-file:/run/secrets/pg") -> dict:
    return {
        "id": "config-row",
        "substrate": "postgres",
        "tier": "native-cas",
        "version_source": "trigger-managed version column",
        "connection": {"dsn": dsn, "credential": credential},
    }


def s3_artifact(connection: object) -> dict:
    return {
        "id": "brief-object",
        "substrate": "s3",
        "tier": "native-cas",
        "version_source": "object ETag",
        "connection": connection,
    }


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


def test_secret_file_and_aws_default_manifest_loads(tmp_path):
    data = {
        "artifacts": [
            pg_artifact("host=db.example.com port=5432 dbname=app sslmode=require"),
            s3_artifact("aws-default"),
        ]
    }
    manifest = load(tmp_path, data)

    assert isinstance(manifest, SubstrateManifest)
    assert len(manifest.artifacts) == 2
    assert all(isinstance(a, ManifestArtifact) for a in manifest.artifacts)
    assert manifest.artifacts[0].descriptor.tier is Tier.NATIVE_CAS


def test_dry_run_prints_id_substrate_tier(tmp_path, capsys):
    data = {
        "artifacts": [
            pg_artifact("host=db.example.com sslmode=require"),
            s3_artifact("aws-default"),
        ]
    }
    rows = load(tmp_path, data).dry_run()

    assert ("config-row", "postgres", Tier.NATIVE_CAS) in rows
    assert ("brief-object", "s3", Tier.NATIVE_CAS) in rows
    out = capsys.readouterr().out
    assert "config-row" in out and "postgres" in out and "native_cas" in out


def test_forward_only_without_version_source_loads_and_prints_tier(tmp_path, capsys):
    data = {
        "artifacts": [
            {
                "id": "notify-slack",
                "substrate": "slack",
                "tier": "forward-only",
                "connection": "secret-file:/run/secrets/slack_token",
            }
        ]
    }
    manifest = load(tmp_path, data)

    assert manifest.artifacts[0].descriptor.tier is Tier.FORWARD_ONLY
    assert manifest.artifacts[0].version_source is None
    rows = manifest.dry_run()
    assert ("notify-slack", "slack", Tier.FORWARD_ONLY) in rows
    assert "forward_only" in capsys.readouterr().out


def test_public_target_loads_normally(tmp_path):
    data = {"artifacts": [pg_artifact("host=db.example.com sslmode=require")]}

    assert len(load(tmp_path, data).artifacts) == 1


# ---------------------------------------------------------------------------
# Credential reference forms — allowlist; reject suspected literals
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "credential",
    [
        "postgresql://user:pass@db.example.com/app",  # inline DSN
        "AKIAIOSFODNN7EXAMPLE",  # bare key
        "vault-ref:/kv/data/pg",  # unknown prefix
        "s3://bucket/key",  # unknown prefix
        "",  # empty
    ],
)
def test_non_reference_form_credential_rejected(tmp_path, credential):
    data = {"artifacts": [s3_artifact({"credential": credential})]}

    with pytest.raises(SubstrateCredentialRefused):
        load(tmp_path, data)


@pytest.mark.parametrize(
    "credential",
    ["aws-default", "secret-file:/run/secrets/pg", "env:PG_PASSWORD"],
)
def test_reference_form_credentials_accepted(tmp_path, credential):
    data = {"artifacts": [s3_artifact({"credential": credential})]}

    assert len(load(tmp_path, data).artifacts) == 1


def test_secret_uri_allowlisted_provider_accepted(tmp_path):
    data = {"artifacts": [s3_artifact({"credential": "secret:aws-secretsmanager:prod/pg"})]}

    assert len(load(tmp_path, data).artifacts) == 1


def test_secret_uri_unknown_provider_rejected(tmp_path):
    data = {"artifacts": [s3_artifact({"credential": "secret:sketchy-provider:prod/pg"})]}

    with pytest.raises(SubstrateCredentialRefused):
        load(tmp_path, data)


# ---------------------------------------------------------------------------
# SSRF — deny on the RESOLVED address, not the literal string
# ---------------------------------------------------------------------------


def test_dns_rebind_to_metadata_rejected(tmp_path):
    # A benign-looking hostname that RESOLVES to the AWS/GCP IMDS address.
    resolver = make_resolver({"rebind.evil.test": ["169.254.169.254"]})
    data = {"artifacts": [pg_artifact("host=rebind.evil.test sslmode=require")]}

    with pytest.raises(SubstrateTargetDenied):
        load(tmp_path, data, resolver=resolver)


@pytest.mark.parametrize(
    "literal",
    ["2852039166", "0xA9FEA9FE", "0251.0376.0251.0376"],  # decimal/hex/octal 169.254.169.254
)
def test_decimal_octal_hex_ipv4_rejected_via_normalization(tmp_path, literal):
    # These are not classifiable by a string/ipaddress check; getaddrinfo (here,
    # the injected resolver) normalizes them to the real denied address.
    resolver = make_resolver({literal: ["169.254.169.254"]})
    data = {"artifacts": [pg_artifact(f"host={literal} sslmode=require")]}

    with pytest.raises(SubstrateTargetDenied):
        load(tmp_path, data, resolver=resolver)


@pytest.mark.parametrize(
    "endpoint",
    [
        "https://[::ffff:169.254.169.254]:443",  # mapped IPv6 IMDS
        "https://[::ffff:100.100.100.200]:443",  # mapped IPv6 CGNAT metadata
    ],
)
def test_mapped_ipv6_metadata_rejected(tmp_path, endpoint):
    # The mapped form must NOT bypass the IPv4 denylist — unwrap-or-fail-closed.
    data = {"artifacts": [s3_artifact({"endpoint_url": endpoint, "credential": "aws-default"})]}

    with pytest.raises(SubstrateTargetDenied):
        load(tmp_path, data)


@pytest.mark.parametrize(
    "host",
    [
        "metadata.google.internal",  # GCP alias (hostname denylist)
        "100.100.100.200",  # Alibaba CGNAT metadata
        "169.254.169.254",  # AWS/Azure IMDS link-local
    ],
)
def test_metadata_hostnames_and_ipv4_literals_rejected(tmp_path, host):
    resolver = make_resolver({"metadata.google.internal": ["169.254.169.254"]})
    data = {"artifacts": [pg_artifact(f"host={host} sslmode=require")]}

    with pytest.raises(SubstrateTargetDenied):
        load(tmp_path, data, resolver=resolver)


@pytest.mark.parametrize("host", ["fd00:ec2::254", "fe80::1"])
def test_ipv6_metadata_and_link_local_rejected(tmp_path, host):
    # host= carries a bracketed literal so decomposition treats it as one host.
    data = {"artifacts": [pg_artifact(f"host=[{host}] sslmode=require")]}

    with pytest.raises(SubstrateTargetDenied):
        load(tmp_path, data)


# ---------------------------------------------------------------------------
# RFC-1918 is SOFT/opt-in — a distinct per-manifest allow flag
# ---------------------------------------------------------------------------


def test_rfc1918_denied_without_optin(tmp_path):
    data = {"artifacts": [pg_artifact("host=10.0.0.5 sslmode=require")]}

    with pytest.raises(SubstrateTargetDenied):
        load(tmp_path, data, env={})


def test_rfc1918_loads_under_distinct_optin(tmp_path):
    data = {"artifacts": [pg_artifact("host=10.0.0.5 sslmode=require")]}

    manifest = load(tmp_path, data, env={ALLOW_PRIVATE_ENV: "1"})
    assert len(manifest.artifacts) == 1


def test_rfc1918_optin_is_not_the_coordinator_flag(tmp_path):
    # The coordinator's remote-mode flag must NOT relax substrate SSRF.
    data = {"artifacts": [pg_artifact("host=10.0.0.5 sslmode=require")]}

    for foreign_flag in ("CCS_REMOTE_COORDINATOR", "CCS_REMOTE_INSECURE"):
        with pytest.raises(SubstrateTargetDenied):
            load(tmp_path, data, env={foreign_flag: "1"})


# ---------------------------------------------------------------------------
# Postgres host= DSN decomposition
# ---------------------------------------------------------------------------


def test_multi_host_metadata_in_second_slot_rejected(tmp_path):
    resolver = make_resolver(
        {"ok.example.com": [PUBLIC_IP], "evil.example.com": ["169.254.169.254"]}
    )
    dsn = "host=ok.example.com,evil.example.com sslmode=require"
    data = {"artifacts": [pg_artifact(dsn)]}

    with pytest.raises(SubstrateTargetDenied):
        load(tmp_path, data, resolver=resolver)


def test_hostaddr_denied_is_rejected(tmp_path):
    # hostaddr is the authoritative dialed target when both are present.
    dsn = "host=db.example.com hostaddr=169.254.169.254 sslmode=require"
    data = {"artifacts": [pg_artifact(dsn)]}

    with pytest.raises(SubstrateTargetDenied):
        load(tmp_path, data)


def test_unix_socket_host_allowed_and_not_resolved(tmp_path):
    # A leading '/' is a unix socket — a local target, exempt from the IP
    # denylist and NOT misclassified as a hostname to resolve.
    def _boom(host: str):
        raise AssertionError(f"unix socket must not be resolved (got {host!r})")

    data = {"artifacts": [pg_artifact("host=/var/run/postgresql")]}
    manifest = load(tmp_path, data, resolver=_boom)

    assert len(manifest.artifacts) == 1


def test_uri_dsn_inline_password_rejected(tmp_path):
    dsn = "postgresql://user:S3cr3tP@ss@db.example.com/app"
    data = {"artifacts": [pg_artifact(dsn)]}

    with pytest.raises(SubstrateCredentialRefused):
        load(tmp_path, data)


def test_uri_dsn_hostaddr_query_param_denied(tmp_path):
    # libpq honors ?hostaddr= in a URI DSN and dials it directly (host is only for
    # TLS SNI). The URI form must apply the same hostaddr guard as the keyword
    # form, or a metadata target slips past behind a benign hostname.
    dsn = "postgresql://db.example.com/app?hostaddr=169.254.169.254&sslmode=require"
    data = {"artifacts": [pg_artifact(dsn)]}

    with pytest.raises(SubstrateTargetDenied):
        load(tmp_path, data)


def test_uri_dsn_password_query_param_rejected(tmp_path):
    # An inline password given as a URI query parameter is refused, same as one in
    # the userinfo — libpq honors ?password=.
    dsn = "postgresql://db.example.com/app?password=s3cr3t&sslmode=require"
    data = {"artifacts": [pg_artifact(dsn)]}

    with pytest.raises(SubstrateCredentialRefused):
        load(tmp_path, data)


def test_keyword_dsn_spaced_equals_metadata_denied(tmp_path):
    # libpq tolerates whitespace around '='; the guard must too, or a space-padded
    # host tokenizes to nothing and slips past the SSRF denylist.
    dsn = "host = 169.254.169.254 sslmode = require"
    data = {"artifacts": [pg_artifact(dsn)]}

    with pytest.raises(SubstrateTargetDenied):
        load(tmp_path, data)


def test_keyword_dsn_spaced_inline_password_rejected(tmp_path):
    # A space-padded inline password must still be refused (the same tokenization
    # gap would otherwise drop it and skip the inline-secret guard).
    dsn = "host = db.example.com  password = s3cr3t  sslmode = require"
    data = {"artifacts": [pg_artifact(dsn)]}

    with pytest.raises(SubstrateCredentialRefused):
        load(tmp_path, data)


def test_keyword_dsn_quoted_host_metadata_denied(tmp_path):
    # A single-quoted value is one token; the metadata host inside it is still
    # resolved and denied.
    dsn = "host = '169.254.169.254'  sslmode=require"
    data = {"artifacts": [pg_artifact(dsn)]}

    with pytest.raises(SubstrateTargetDenied):
        load(tmp_path, data)


def test_keyword_dsn_unterminated_quote_fails_closed(tmp_path):
    # A malformed DSN (unterminated quote) fails closed rather than silently
    # dropping the host and skipping the guards.
    dsn = "host = 'db.example.com sslmode=require"
    data = {"artifacts": [pg_artifact(dsn)]}

    with pytest.raises(ManifestError):
        load(tmp_path, data)


def test_uri_dsn_duplicate_host_metadata_denied(tmp_path):
    # A repeated ?host= must have EVERY value checked (libpq merges duplicates to
    # one value in a version-dependent order) — benign-first, metadata-last is
    # still denied.
    dsn = "postgresql:///app?host=safe.example&host=169.254.169.254&sslmode=require"
    data = {"artifacts": [pg_artifact(dsn)]}
    resolver = make_resolver({"safe.example": [PUBLIC_IP]})

    with pytest.raises(SubstrateTargetDenied):
        load(tmp_path, data, resolver=resolver)


def test_uri_dsn_duplicate_hostaddr_metadata_denied(tmp_path):
    # The fix's own headline case, reopened by a duplicate: benign-first,
    # metadata-last hostaddr.
    dsn = "postgresql:///app?hostaddr=1.2.3.4&hostaddr=169.254.169.254&sslmode=require"
    data = {"artifacts": [pg_artifact(dsn)]}

    with pytest.raises(SubstrateTargetDenied):
        load(tmp_path, data)


def test_uri_dsn_duplicate_sslmode_tls_downgrade_blocked(tmp_path):
    # A safe-first, unsafe-last sslmode must not mask the plaintext mode: the
    # most-restrictive wins, so the plaintext-credential guard still fires.
    dsn = "postgresql:///app?hostaddr=203.0.113.9&sslmode=require&sslmode=disable"
    data = {"artifacts": [pg_artifact(dsn)]}

    with pytest.raises(SubstrateInsecureTransport):
        load(tmp_path, data)


def test_keyword_dsn_hostaddr_comma_gap_checks_unpaired_host(tmp_path):
    # host has more entries than hostaddr (a trailing comma drops a slot); libpq
    # dials the unpaired host, so it must be checked — never skipped by an
    # 'hostaddr is authoritative' shortcut.
    dsn = "host=127.0.0.1,evil.example hostaddr=127.0.0.1, sslmode=require"
    data = {"artifacts": [pg_artifact(dsn)]}
    resolver = make_resolver({"evil.example": ["169.254.169.254"]})

    with pytest.raises(SubstrateTargetDenied):
        load(tmp_path, data, resolver=resolver)


def test_uri_dsn_hostaddr_comma_gap_checks_unpaired_host(tmp_path):
    # Same comma-gap, URI form: the unpaired ?host= entry is still checked.
    dsn = "postgresql:///app?host=127.0.0.1,evil.example&hostaddr=127.0.0.1,"
    data = {"artifacts": [pg_artifact(dsn)]}
    resolver = make_resolver({"evil.example": ["169.254.169.254"]})

    with pytest.raises(SubstrateTargetDenied):
        load(tmp_path, data, resolver=resolver)


def test_keyword_dsn_inline_password_rejected(tmp_path):
    dsn = "host=db.example.com sslmode=require password=S3cr3tP@ss"
    data = {"artifacts": [pg_artifact(dsn)]}

    with pytest.raises(SubstrateCredentialRefused):
        load(tmp_path, data)


# ---------------------------------------------------------------------------
# TLS-required for a routable target (distinct ack, NOT CCS_REMOTE_INSECURE)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("dsn", ["host=db.example.com sslmode=disable", "host=db.example.com"])
def test_pg_plaintext_routable_refused_without_ack(tmp_path, dsn):
    # sslmode=disable AND unset (libpq 'prefer' silent downgrade) are both refused.
    data = {"artifacts": [pg_artifact(dsn)]}

    with pytest.raises(SubstrateInsecureTransport):
        load(tmp_path, data)


def test_pg_plaintext_routable_loads_with_distinct_ack(tmp_path):
    data = {"artifacts": [pg_artifact("host=db.example.com sslmode=disable")]}

    manifest = load(tmp_path, data, env={INSECURE_ACK_ENV: "1"})
    assert len(manifest.artifacts) == 1


def test_pg_plaintext_ack_is_not_the_coordinator_flag(tmp_path):
    data = {"artifacts": [pg_artifact("host=db.example.com sslmode=disable")]}

    with pytest.raises(SubstrateInsecureTransport):
        load(tmp_path, data, env={"CCS_REMOTE_INSECURE": "1"})


def test_pg_plaintext_loopback_is_exempt(tmp_path):
    # Loopback is byte-unchanged — no TLS ack required (local dev).
    data = {"artifacts": [pg_artifact("host=127.0.0.1 sslmode=disable")]}

    assert len(load(tmp_path, data).artifacts) == 1


def test_s3_http_endpoint_routable_refused_without_ack(tmp_path):
    resolver = make_resolver({"minio.example.com": [PUBLIC_IP]})
    conn = {"endpoint_url": "http://minio.example.com:9000", "credential": "aws-default"}
    data = {"artifacts": [s3_artifact(conn)]}

    with pytest.raises(SubstrateInsecureTransport):
        load(tmp_path, data, resolver=resolver)


def test_s3_http_endpoint_loads_with_distinct_ack(tmp_path):
    resolver = make_resolver({"minio.example.com": [PUBLIC_IP]})
    conn = {"endpoint_url": "http://minio.example.com:9000", "credential": "aws-default"}
    data = {"artifacts": [s3_artifact(conn)]}

    manifest = load(tmp_path, data, resolver=resolver, env={INSECURE_ACK_ENV: "1"})
    assert len(manifest.artifacts) == 1


# ---------------------------------------------------------------------------
# MRAP / replica endpoint -> warning (forbidden distributed-substrate locality)
# ---------------------------------------------------------------------------


def test_mrap_endpoint_warns(tmp_path, caplog):
    host = "myapp.mrap.accesspoint.s3-global.amazonaws.com"
    resolver = make_resolver({host: [PUBLIC_IP]})
    conn = {"endpoint_url": f"https://{host}", "credential": "aws-default"}
    data = {"artifacts": [s3_artifact(conn)]}

    with caplog.at_level(logging.WARNING):
        manifest = load(tmp_path, data, resolver=resolver)

    assert len(manifest.artifacts) == 1
    assert "multi-region" in caplog.text.lower() or "mrap" in caplog.text.lower()


# ---------------------------------------------------------------------------
# Secret never reaches a log or an exception
# ---------------------------------------------------------------------------


def test_credential_refused_message_has_no_password(tmp_path):
    dsn = "postgresql://user:S3cr3tP@ss@db.example.com/app"
    data = {"artifacts": [pg_artifact(dsn)]}

    with pytest.raises(SubstrateCredentialRefused) as exc:
        load(tmp_path, data)
    assert "S3cr3tP@ss" not in str(exc.value)


def test_refused_target_warning_names_host_never_secret(tmp_path, caplog):
    # The TLS-ack WARN names the host/posture; the credential ref never appears.
    conn = {"endpoint_url": "http://minio.example.com:9000", "credential": UNIQUE_SECRET_REF}
    data = {"artifacts": [s3_artifact(conn)]}
    resolver = make_resolver({"minio.example.com": [PUBLIC_IP]})

    with caplog.at_level(logging.WARNING):
        load(tmp_path, data, resolver=resolver, env={INSECURE_ACK_ENV: "1"})

    assert "minio.example.com" in caplog.text
    assert UNIQUE_SECRET_REF not in caplog.text
    assert "UNIQUE-do-not-leak-me" not in caplog.text


def test_denied_target_exception_carries_host_and_posture(tmp_path):
    data = {"artifacts": [pg_artifact("host=169.254.169.254 sslmode=require")]}

    with pytest.raises(SubstrateTargetDenied) as exc:
        load(tmp_path, data)
    assert exc.value.host == "169.254.169.254"
    assert exc.value.posture


# ---------------------------------------------------------------------------
# Schema discipline
# ---------------------------------------------------------------------------


def test_unknown_top_level_key_rejected(tmp_path):
    data = {"artifacts": [s3_artifact("aws-default")], "bogus_top_level": 1}

    with pytest.raises(ManifestError):
        load(tmp_path, data)


def test_unknown_artifact_key_rejected(tmp_path):
    art = s3_artifact("aws-default")
    art["surprise"] = "value"
    data = {"artifacts": [art]}

    with pytest.raises(ManifestError):
        load(tmp_path, data)


def test_missing_tier_rejected(tmp_path):
    art = s3_artifact("aws-default")
    del art["tier"]
    data = {"artifacts": [art]}

    with pytest.raises(ManifestError):
        load(tmp_path, data)


def test_unknown_tier_rejected(tmp_path):
    art = s3_artifact("aws-default")
    art["tier"] = "make-believe"
    data = {"artifacts": [art]}

    with pytest.raises(ManifestError):
        load(tmp_path, data)


def test_non_mapping_root_rejected(tmp_path):
    with pytest.raises(ManifestError):
        load(tmp_path, ["not", "a", "mapping"])


def test_missing_artifacts_key_rejected(tmp_path):
    with pytest.raises(ManifestError):
        load(tmp_path, {"version": 1})


# ---------------------------------------------------------------------------
# Forward-only + version_source -> reject (descriptor's own validation)
# ---------------------------------------------------------------------------


def test_forward_only_with_version_source_rejected(tmp_path):
    data = {
        "artifacts": [
            {
                "id": "notify-slack",
                "substrate": "slack",
                "tier": "forward-only",
                "version_source": "etag",
                "connection": "secret-file:/run/secrets/slack",
            }
        ]
    }
    with pytest.raises(ManifestError):
        load(tmp_path, data)


def test_native_cas_without_version_source_rejected(tmp_path):
    art = s3_artifact("aws-default")
    del art["version_source"]
    data = {"artifacts": [art]}

    with pytest.raises(ManifestError):
        load(tmp_path, data)


# ---------------------------------------------------------------------------
# Manifest file permissions
# ---------------------------------------------------------------------------


def test_world_readable_manifest_warns(tmp_path, caplog):
    data = {"artifacts": [s3_artifact("aws-default")]}

    with caplog.at_level(logging.WARNING):
        load(tmp_path, data, mode=0o644)

    assert "world-readable" in caplog.text.lower() or "0644" in caplog.text


def test_tight_manifest_does_not_warn_on_perms(tmp_path, caplog):
    data = {"artifacts": [s3_artifact("aws-default")]}

    with caplog.at_level(logging.WARNING):
        load(tmp_path, data, mode=0o600)

    assert "world-readable" not in caplog.text.lower()


# ---------------------------------------------------------------------------
# validate() + default resolver
# ---------------------------------------------------------------------------


def test_validate_is_idempotent_on_a_good_manifest(tmp_path):
    data = {"artifacts": [pg_artifact("host=db.example.com sslmode=require")]}
    manifest = load(tmp_path, data)

    manifest.validate()  # must not raise


def test_default_resolver_returns_ip_literals_unchanged():
    # An IP literal needs no network round-trip: getaddrinfo echoes it.
    assert resolve_host_addresses("127.0.0.1") == ("127.0.0.1",)
