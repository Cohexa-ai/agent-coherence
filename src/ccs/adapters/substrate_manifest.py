# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""The Coherence Manifest: a declarative, secret-safe, SSRF-constrained wiring
of artifact identities to substrate connections.

The manifest is a named TRUST BOUNDARY. It binds an artifact id to a substrate
type, a connection target, a credential REFERENCE (never a literal), a
version-source, and an honest guarantee :class:`~ccs.core.substrate.Tier`. Every
security decision is taken at LOAD/VALIDATE time — before any driver is
constructed — so a bad target or an inline literal secret can never reach a
substrate. The discipline mirrors the shipped plaintext-bearer guard
(:func:`ccs.cli._coherence_client._guard_plaintext_bearer`): classify the target
deterministically, fail closed on anything unclassifiable, and log the HOST and
POSTURE only — never the credential.

Two guards are distinct from the coordinator's own remote-mode flags on purpose
(relaxing coordinator transport must never relax substrate egress):

- ``CCS_SUBSTRATE_ALLOW_PRIVATE`` — opt in to RFC-1918/4193 private targets (a
  customer's internal RDS/Postgres is commonly RFC-1918). NOT
  ``CCS_REMOTE_COORDINATOR``.
- ``CCS_SUBSTRATE_INSECURE`` — acknowledge a plaintext credential to a routable
  host (an out-of-band-secured link). NOT ``CCS_REMOTE_INSECURE``.

The DNS resolver is an INJECTABLE seam (:data:`HostResolver`): the SSRF deny
runs on the RESOLVED address(es), and injecting the resolver keeps validation
hermetic and lets later bindings (Postgres/S3) reuse the same
resolve-then-check primitive.
"""

from __future__ import annotations

import ipaddress
import logging
import os
import re
import socket
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import TypeAlias
from urllib.parse import parse_qs, urlsplit

import yaml

from ccs.core.exceptions import (
    ManifestError,
    SubstrateCredentialRefused,
    SubstrateInsecureTransport,
    SubstrateTargetDenied,
)
from ccs.core.substrate import CapabilityDescriptor, Tier

logger = logging.getLogger(__name__)

#: Opt-in for RFC-1918/4193 private substrate targets (distinct from the
#: coordinator's ``CCS_REMOTE_COORDINATOR`` — see the module docstring).
ALLOW_PRIVATE_ENV = "CCS_SUBSTRATE_ALLOW_PRIVATE"
#: Acknowledge a plaintext credential to a routable host (distinct from
#: ``CCS_REMOTE_INSECURE``).
INSECURE_ACK_ENV = "CCS_SUBSTRATE_INSECURE"

#: Truthy env values (mirrors the shipped remote-flag parser).
_TRUTHY_ENV_VALUES: frozenset[str] = frozenset({"1", "true", "yes", "on"})

# --- schema keys ------------------------------------------------------------
_ALLOWED_TOP_LEVEL_KEYS: frozenset[str] = frozenset({"artifacts"})
_ALLOWED_ARTIFACT_KEYS: frozenset[str] = frozenset(
    {"id", "substrate", "tier", "version_source", "connection"}
)
_ALLOWED_TARGET_KEYS: frozenset[str] = frozenset({"dsn", "endpoint_url", "region"})

# --- credential reference forms (allowlist; anything else is a suspected literal)
_ZERO_SECRET_FORMS: frozenset[str] = frozenset({"aws-default"})
_SECRET_FILE_PREFIX = "secret-file:"
_ENV_PREFIX = "env:"
_SECRET_URI_PREFIX = "secret:"
#: ``secret:`` resolvers are constrained to an allowlisted provider set.
_ALLOWED_SECRET_PROVIDERS: frozenset[str] = frozenset(
    {"aws-secretsmanager", "vault", "gcp-secretmanager", "azure-keyvault"}
)

# --- SSRF classification (explicit networks; NO reliance on stdlib is_private/
#     is_link_local, which vary across CPython patch releases) -----------------
_LINK_LOCAL_V4_NET = ipaddress.ip_network("169.254.0.0/16")  # AWS/Azure IMDS
_CGNAT_V4_NET = ipaddress.ip_network("100.64.0.0/10")  # Alibaba metadata
_HARD_DENY_V4_NETS = (_LINK_LOCAL_V4_NET, _CGNAT_V4_NET)
_LINK_LOCAL_V6_NET = ipaddress.ip_network("fe80::/10")
#: EC2 IMDS over IPv6 lives inside ``fc00::/7`` (ULA), so "private ⇒ allowed" is
#: unsafe for egress — hard-deny the exact address.
_HARD_DENY_V6_ADDRS: frozenset[ipaddress.IPv6Address] = frozenset(
    {ipaddress.IPv6Address("fd00:ec2::254")}
)
_METADATA_HOSTNAMES: frozenset[str] = frozenset({"metadata.google.internal"})
_PRIVATE_V4_NETS = (
    ipaddress.ip_network("10.0.0.0/8"),
    ipaddress.ip_network("172.16.0.0/12"),
    ipaddress.ip_network("192.168.0.0/16"),
)
_ULA_V6_NET = ipaddress.ip_network("fc00::/7")
_LOOPBACK_V4_NET = ipaddress.ip_network("127.0.0.0/8")
_IPV6_LOOPBACK = ipaddress.IPv6Address("::1")

#: Postgres sslmodes that guarantee an encrypted link; ``disable``/``allow``/
#: ``prefer`` (and unset → libpq ``prefer``) can silently ship the credential in
#: cleartext.
_TLS_SAFE_SSLMODES: frozenset[str] = frozenset({"require", "verify-ca", "verify-full"})

#: A DNS resolver seam: a hostname maps to a tuple of resolved IP strings (empty
#: ⇒ unresolvable ⇒ fail closed). Injected in tests and reused by later bindings.
HostResolver: TypeAlias = Callable[[str], "tuple[str, ...]"]


def resolve_host_addresses(host: str) -> tuple[str, ...]:
    """Default resolver: the deduplicated ``getaddrinfo`` addresses for ``host``.

    Returns an empty tuple on resolution failure so the caller can fail closed
    (an unresolvable target is denied, never admitted). An IP literal is echoed
    back with no network round-trip.
    """
    try:
        infos = socket.getaddrinfo(host, None, type=socket.SOCK_STREAM)
    except OSError:
        return ()
    return tuple(dict.fromkeys(info[4][0] for info in infos))


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ManifestArtifact:
    """One validated artifact wiring.

    ``connection`` is the read-only, form-validated reference bundle (a
    ``credential`` reference plus any target — ``dsn`` / ``endpoint_url`` /
    ``region``) that a later binding resolves; it never carries a literal
    secret. ``descriptor`` is the tier-derived honesty declaration.
    """

    id: str
    substrate: str
    tier: Tier
    descriptor: CapabilityDescriptor
    connection: Mapping[str, str]
    version_source: str | None = None


@dataclass(frozen=True)
class SubstrateManifest:
    """A loaded, validated set of artifact wirings."""

    artifacts: tuple[ManifestArtifact, ...]

    def validate(self) -> None:
        """Re-assert per-artifact tier consistency (reusing the descriptor's own
        validation): a ``forward-only`` artifact must declare no version_source,
        a ``native-cas`` artifact must declare one. Raises :class:`ManifestError`."""
        for artifact in self.artifacts:
            _build_descriptor(artifact.tier, artifact.version_source)

    def dry_run(self) -> tuple[tuple[str, str, Tier], ...]:
        """Print and return each artifact's ``(id, substrate, tier)`` so the tier
        is visible at config time before any substrate is touched."""
        rows = tuple((a.id, a.substrate, a.tier) for a in self.artifacts)
        for artifact_id, substrate, tier in rows:
            print(f"  {artifact_id}\t{substrate}\t{tier.value}")
        return rows


# ---------------------------------------------------------------------------
# Loader
# ---------------------------------------------------------------------------


def load_manifest(
    path: str | Path,
    *,
    resolver: HostResolver = resolve_host_addresses,
    env: Mapping[str, str] | None = None,
) -> SubstrateManifest:
    """Load, secret-form-validate, and SSRF/TLS-check a coherence manifest.

    Rejects (all at load, before any driver): unknown top-level/artifact keys, a
    missing/unknown tier, a non-reference-form credential, a target that
    resolves into the metadata/link-local class or (without opt-in) a private
    range, and a plaintext credential to a routable host (without the ack).
    Warns on a world-readable manifest and on a multi-region/replica endpoint.
    """
    manifest_path = Path(path)
    resolved_env = os.environ if env is None else env
    _warn_if_world_readable(manifest_path)
    data = _load_yaml(manifest_path)
    _reject_unknown_keys(data, _ALLOWED_TOP_LEVEL_KEYS, "manifest top-level")
    raw_artifacts = data.get("artifacts")
    if not isinstance(raw_artifacts, list) or not raw_artifacts:
        raise ManifestError("manifest requires a non-empty 'artifacts' list")
    artifacts = tuple(
        _parse_artifact(entry, resolver=resolver, env=resolved_env)
        for entry in raw_artifacts
    )
    manifest = SubstrateManifest(artifacts=artifacts)
    manifest.validate()
    return manifest


def _load_yaml(path: Path) -> Mapping[str, object]:
    """Read + ``yaml.safe_load`` the manifest; require a mapping root."""
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise ManifestError(f"cannot read manifest {str(path)!r}: {exc}") from exc
    try:
        data = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        raise ManifestError(f"manifest {str(path)!r} is not valid YAML: {exc}") from exc
    if not isinstance(data, Mapping):
        raise ManifestError("manifest root must be a mapping with an 'artifacts' key")
    return data


def _warn_if_world_readable(path: Path) -> None:
    """Warn on a world-readable manifest (it names hosts + secret-ref paths)."""
    try:
        mode = path.stat().st_mode
    except OSError:
        return
    if mode & 0o004:
        logger.warning(
            "manifest %s is world-readable (mode %o); it names connection targets "
            "and secret-reference paths — tighten it (e.g. 0600)",
            str(path),
            mode & 0o777,
        )


def _reject_unknown_keys(
    mapping: Mapping[str, object], allowed: frozenset[str], label: str
) -> None:
    """Fail closed on any key outside ``allowed`` (strict schema discipline)."""
    unknown = set(mapping) - allowed
    if unknown:
        raise ManifestError(f"unknown {label} key(s): {sorted(unknown)}")


# ---------------------------------------------------------------------------
# Per-artifact parse + validate
# ---------------------------------------------------------------------------


def _parse_artifact(
    entry: object, *, resolver: HostResolver, env: Mapping[str, str]
) -> ManifestArtifact:
    """Validate one artifact entry end-to-end and build its wiring."""
    if not isinstance(entry, Mapping):
        raise ManifestError(f"each artifact must be a mapping (got {type(entry).__name__})")
    _reject_unknown_keys(entry, _ALLOWED_ARTIFACT_KEYS, "artifact")
    artifact_id = _require_str(entry, "id")
    substrate = _require_str(entry, "substrate")
    tier = _parse_tier(entry.get("tier"))
    version_source = _optional_str(entry, "version_source")
    credential_ref, target = _extract_connection(entry)
    _validate_credential_form(credential_ref)
    _check_target(target, resolver=resolver, env=env)
    descriptor = _build_descriptor(tier, version_source)
    connection = MappingProxyType({"credential": credential_ref, **target})
    return ManifestArtifact(
        id=artifact_id,
        substrate=substrate,
        tier=tier,
        descriptor=descriptor,
        connection=connection,
        version_source=version_source,
    )


def _require_str(mapping: Mapping[str, object], key: str) -> str:
    value = mapping.get(key)
    if not isinstance(value, str) or not value:
        raise ManifestError(f"artifact key {key!r} must be a non-empty string")
    return value


def _optional_str(mapping: Mapping[str, object], key: str) -> str | None:
    value = mapping.get(key)
    if value is not None and not isinstance(value, str):
        raise ManifestError(f"artifact key {key!r} must be a string when present")
    return value


def _parse_tier(raw: object) -> Tier:
    if not isinstance(raw, str) or not raw:
        raise ManifestError(f"artifact 'tier' is required and must be a string (got {raw!r})")
    try:
        return Tier(raw.strip().lower().replace("-", "_"))
    except ValueError as exc:
        valid = ", ".join(t.value.replace("_", "-") for t in Tier)
        raise ManifestError(f"unknown tier {raw!r}; expected one of: {valid}") from exc


def _build_descriptor(tier: Tier, version_source: str | None) -> CapabilityDescriptor:
    """Reuse the descriptor's own tier/version_source validation."""
    try:
        return CapabilityDescriptor(tier=tier, version_source=version_source)
    except ValueError as exc:
        raise ManifestError(str(exc)) from exc


def _extract_connection(entry: Mapping[str, object]) -> tuple[str, dict[str, str]]:
    """Split ``connection`` into ``(credential_ref, target)``.

    A bare string is a credential-only reference (no network target — the
    forward-only / ambient-credential case). A mapping carries the credential
    plus an allowlisted target (``dsn`` / ``endpoint_url`` / ``region``).
    """
    conn = entry.get("connection")
    if isinstance(conn, str):
        return conn, {}
    if isinstance(conn, Mapping):
        credential = conn.get("credential")
        if not isinstance(credential, str):
            raise ManifestError("connection mapping requires a string 'credential'")
        target = {key: value for key, value in conn.items() if key != "credential"}
        _reject_unknown_keys(target, _ALLOWED_TARGET_KEYS, "connection")
        for key, value in target.items():
            if not isinstance(value, str):
                raise ManifestError(f"connection {key!r} must be a string")
        return credential, target
    raise ManifestError("connection must be a reference string or a mapping")


# ---------------------------------------------------------------------------
# Credential reference forms
# ---------------------------------------------------------------------------


def _validate_credential_form(ref: str) -> None:
    """Accept only an allowlisted reference form; reject a suspected literal.

    NEVER echoes ``ref`` (it may be the secret) — the message names the
    category of violation only.
    """
    if not ref:
        raise SubstrateCredentialRefused("the credential is empty")
    if ref in _ZERO_SECRET_FORMS:
        return
    if ref.startswith(_SECRET_FILE_PREFIX):
        if not ref[len(_SECRET_FILE_PREFIX):]:
            raise SubstrateCredentialRefused("a secret-file: reference has no path")
        return
    if ref.startswith(_ENV_PREFIX):
        if not ref[len(_ENV_PREFIX):]:
            raise SubstrateCredentialRefused("an env: reference has no variable name")
        return
    if ref.startswith(_SECRET_URI_PREFIX):
        _validate_secret_uri(ref)
        return
    raise SubstrateCredentialRefused("the credential value is not a recognized reference form")


def _validate_secret_uri(ref: str) -> None:
    body = ref[len(_SECRET_URI_PREFIX):]
    provider = re.split(r"[:/]", body, maxsplit=1)[0].lower() if body else ""
    if provider not in _ALLOWED_SECRET_PROVIDERS:
        raise SubstrateCredentialRefused("a secret: reference names a non-allowlisted provider")


# ---------------------------------------------------------------------------
# Connection targets (SSRF + TLS)
# ---------------------------------------------------------------------------


def _check_target(
    target: Mapping[str, str], *, resolver: HostResolver, env: Mapping[str, str]
) -> None:
    """Run the substrate-appropriate SSRF/TLS checks for a target."""
    if "dsn" in target:
        _check_pg_dsn(target["dsn"], resolver=resolver, env=env)
    if "endpoint_url" in target or "region" in target:
        _check_s3_endpoint(
            endpoint_url=target.get("endpoint_url"),
            region=target.get("region"),
            resolver=resolver,
            env=env,
        )


@dataclass(frozen=True)
class _PgTarget:
    hosts: list[str]
    hostaddrs: list[str]
    sslmode: str | None
    has_inline_password: bool


def _check_pg_dsn(dsn: str, *, resolver: HostResolver, env: Mapping[str, str]) -> None:
    """SSRF-check every dialed Postgres host and enforce TLS for a routable one."""
    target = _parse_pg_dsn(dsn)
    if target.has_inline_password:
        raise SubstrateCredentialRefused("the connection DSN carries an inline password")
    routable, network_hosts = _check_pg_hosts(target, resolver=resolver, env=env)
    if routable and target.sslmode not in _TLS_SAFE_SSLMODES:
        _require_tls_or_ack(network_hosts[0], "plaintext-capable sslmode", env=env)
    if len(network_hosts) > 1:
        logger.warning(
            "manifest postgres target lists %d network hosts %r; coordinating agents "
            "against a multi-host/replica substrate is out of scope (single-host only)",
            len(network_hosts),
            network_hosts,
        )


def _check_pg_hosts(
    target: _PgTarget, *, resolver: HostResolver, env: Mapping[str, str]
) -> tuple[bool, list[str]]:
    """Classify the dialed hosts; return ``(routable, checked_network_hosts)``.

    Every ``host`` AND every ``hostaddr`` entry is checked — the UNION, not
    ``hostaddr`` alone. libpq pairs ``host[i]``/``hostaddr[i]`` per position and
    dials whichever the position resolves to, and a comma-list count mismatch (a
    trailing/empty slot) can leave a ``host`` entry unpaired and dialed. Checking
    the union never skips a candidate a positional ``hostaddr``-authoritative rule
    would miss; over-checking a name used only for TLS SNI fails closed, which is
    the safe direction. A leading ``/`` host is a unix socket — a local target,
    exempt and NOT resolved.

    Rebind residual (parity with :func:`_check_s3_endpoint`): for a bare-hostname
    target (no ``hostaddr=``), libpq re-resolves the name at connect time,
    decoupled from this load-time resolver check, so a hostname whose DNS is
    attacker-controlled can rebind to a denied target after validation passes.
    The load-time check still catches static misconfiguration; pinning the
    resolved IP into ``hostaddr`` (host retained for TLS SNI) would close the
    window and is the intended future hardening.
    """
    seen: set[str] = set()
    routable = False
    network_hosts: list[str] = []
    for host in (*target.hostaddrs, *target.hosts):
        if host.startswith("/") or host in seen:
            continue
        seen.add(host)
        if not _reject_denied_target(host, resolver=resolver, env=env):
            routable = True
        network_hosts.append(host)
    return routable, network_hosts


def _check_s3_endpoint(
    *,
    endpoint_url: str | None,
    region: str | None,
    resolver: HostResolver,
    env: Mapping[str, str],
) -> None:
    """SSRF-check an explicit S3 ``endpoint_url`` and enforce TLS for a routable one.

    There is no psycopg-style resolve-then-pin for S3 (pinning the endpoint to
    the resolved IP breaks SigV4 host signing + TLS SNI, and botocore re-resolves
    at connect time), so this is a resolve-and-re-check with a documented narrow
    rebind residual. A custom endpoint is a privileged opt-in, not free-form.
    """
    if endpoint_url:
        parts = urlsplit(endpoint_url)
        scheme = (parts.scheme or "").lower()
        if scheme not in ("http", "https"):
            raise ManifestError(f"S3 endpoint_url scheme must be http or https (got {scheme!r})")
        host = parts.hostname
        if not host:
            raise ManifestError("S3 endpoint_url is missing a host")
        is_loopback = _reject_denied_target(host, resolver=resolver, env=env)
        if not is_loopback and scheme == "http":
            _require_tls_or_ack(host, "plaintext endpoint_url (http)", env=env)
        _warn_mrap(host)
    if region:
        _warn_mrap(region)


# ---------------------------------------------------------------------------
# Postgres DSN decomposition
# ---------------------------------------------------------------------------


def _parse_pg_dsn(dsn: str) -> _PgTarget:
    text = dsn.strip()
    if text.startswith(("postgresql://", "postgres://")):
        return _parse_pg_uri(text)
    return _parse_pg_keywords(text)


def _parse_pg_keywords(dsn: str) -> _PgTarget:
    params = _conninfo_params(dsn)
    hosts = [_debracket(h) for h in _split_csv(params.get("host"))]
    hostaddrs = [_debracket(h) for h in _split_csv(params.get("hostaddr"))]
    sslmode = (params.get("sslmode") or "").lower() or None
    return _PgTarget(hosts, hostaddrs, sslmode, has_inline_password=bool(params.get("password")))


def _conninfo_params(dsn: str) -> dict[str, str]:
    """Decompose a libpq keyword/value conninfo string the way libpq parses it.

    Tolerates whitespace around ``=`` and single-quoted values with backslash
    escapes, so a space-padded DSN (``host = h  password = p``) cannot tokenize to
    nothing and slip a host/password/sslmode past the SSRF, inline-secret, and TLS
    guards. Fails closed (:class:`ManifestError`) on a malformed pair or an
    unterminated quote rather than silently dropping it. Keyword case is preserved
    (libpq keywords are canonical lowercase; an uppercase key is an unknown libpq
    keyword that would not dial anyway), and the security-relevant keys are looked
    up lowercase by the caller.
    """
    params: dict[str, str] = {}
    i, n = 0, len(dsn)
    while i < n:
        while i < n and dsn[i].isspace():
            i += 1
        if i >= n:
            break
        start = i
        while i < n and not dsn[i].isspace() and dsn[i] != "=":
            i += 1
        keyword = dsn[start:i]
        while i < n and dsn[i].isspace():
            i += 1
        if i >= n or dsn[i] != "=":
            raise ManifestError("malformed Postgres DSN: expected '=' after keyword")
        i += 1
        while i < n and dsn[i].isspace():
            i += 1
        value, i = _read_conninfo_value(dsn, i, n)
        if keyword:
            params[keyword] = value
    return params


def _read_conninfo_value(dsn: str, i: int, n: int) -> tuple[str, int]:
    """Read one conninfo value (single-quoted or bare) from ``dsn[i:]``."""
    chars: list[str] = []
    if i < n and dsn[i] == "'":
        i += 1
        while i < n and dsn[i] != "'":
            if dsn[i] == "\\" and i + 1 < n:
                i += 1
            chars.append(dsn[i])
            i += 1
        if i >= n:
            raise ManifestError("malformed Postgres DSN: unterminated quoted value")
        return "".join(chars), i + 1
    while i < n and not dsn[i].isspace():
        if dsn[i] == "\\" and i + 1 < n:
            i += 1
        chars.append(dsn[i])
        i += 1
    return "".join(chars), i


def _parse_pg_uri(dsn: str) -> _PgTarget:
    parts = urlsplit(dsn)
    userinfo, _, hostpart = parts.netloc.rpartition("@")
    query = parse_qs(parts.query)
    # libpq honors host/hostaddr/password/sslmode given as URI query parameters and
    # dials them. A repeated query key is merged by libpq to a SINGLE value
    # (documented last-wins, but version-dependent), so this parser never picks one
    # occurrence: it collects ALL host/hostaddr values (the union, checked by
    # _check_pg_hosts) and takes the most-restrictive sslmode — immune to the merge
    # order. This closes the URI-form guards (an unchecked ``?hostaddr=``, or a
    # ``?host=safe&host=metadata`` duplicate, would otherwise reach a denied target
    # past a benign-looking value).
    has_password = (":" in userinfo and userinfo.split(":", 1)[1] != "") or any(
        _query_all(query, "password")
    )
    hosts = [_strip_port(h) for h in _split_csv(hostpart)]
    for value in _query_all(query, "host"):
        hosts += [_debracket(h) for h in _split_csv(value)]
    hostaddrs: list[str] = []
    for value in _query_all(query, "hostaddr"):
        hostaddrs += [_debracket(h) for h in _split_csv(value)]
    sslmode = _least_safe_sslmode(_query_all(query, "sslmode"))
    return _PgTarget(hosts, hostaddrs=hostaddrs, sslmode=sslmode, has_inline_password=has_password)


def _split_csv(value: str | None) -> list[str]:
    if not value:
        return []
    return [part for part in (item.strip() for item in value.split(",")) if part]


def _debracket(host: str) -> str:
    stripped = host.strip()
    if stripped.startswith("[") and stripped.endswith("]"):
        return stripped[1:-1]
    return stripped


def _strip_port(hostport: str) -> str:
    text = hostport.strip()
    if text.startswith("["):
        end = text.find("]")
        return text[1:end] if end != -1 else text
    head, sep, tail = text.rpartition(":")
    return head if sep and tail.isdigit() else text


def _query_all(query: dict[str, list[str]], key: str) -> list[str]:
    """Every value for ``key`` in a ``parse_qs`` mapping (order preserved).

    All occurrences are returned, not one: a repeated security-relevant key must
    have every candidate checked, since a parser that picked a single occurrence
    could disagree with libpq's merge order and miss a denied target. Case is
    preserved (a password is case-sensitive).
    """
    return list(query.get(key, ()))


def _least_safe_sslmode(values: list[str]) -> str | None:
    """The most-restrictive sslmode across ``values`` (duplicate-key safe).

    Returns None when unset (treated as plaintext-capable downstream). If ANY
    occurrence is not TLS-safe, that unsafe mode is returned so the TLS guard
    fires — never trusting a safe occurrence to mask an unsafe one.
    """
    lowered = [v.lower() for v in values]
    if not lowered:
        return None
    for mode in lowered:
        if mode not in _TLS_SAFE_SSLMODES:
            return mode
    return lowered[0]


# ---------------------------------------------------------------------------
# Resolved-address SSRF classification
# ---------------------------------------------------------------------------


def _reject_denied_target(host: str, *, resolver: HostResolver, env: Mapping[str, str]) -> bool:
    """Reject a target resolving into a denied range; return whether it is loopback.

    A hostname in the metadata denylist is rejected pre-resolution; otherwise the
    deny runs on the resolved IP(s) so a DNS-rebind and decimal/octal/hex IPv4
    encodings are all classified. An unresolvable host fails closed (denied).
    """
    if host.lower() in _METADATA_HOSTNAMES:
        raise SubstrateTargetDenied(host, "cloud metadata alias hostname")
    is_loopback = True
    for ip in _resolve_ips(host, resolver):
        _reject_denied_ip(ip, host, env=env)
        if not _is_loopback_ip(ip):
            is_loopback = False
    return is_loopback


def _resolve_ips(
    host: str, resolver: HostResolver
) -> list[ipaddress.IPv4Address | ipaddress.IPv6Address]:
    try:
        return [ipaddress.ip_address(host)]
    except ValueError:
        pass
    resolved = resolver(host)
    if not resolved:
        raise SubstrateTargetDenied(host, "unresolvable host (fail-closed)")
    try:
        return [ipaddress.ip_address(address) for address in resolved]
    except ValueError as exc:
        raise SubstrateTargetDenied(host, "resolver returned an unparseable address") from exc


def _reject_denied_ip(
    ip: ipaddress.IPv4Address | ipaddress.IPv6Address,
    host: str,
    *,
    env: Mapping[str, str],
) -> None:
    """Reject a hard-denied address (metadata/link-local) or, absent the opt-in,
    a private-range address — evaluated through any mapped/compat IPv6 wrapper."""
    for candidate in _denylist_candidates(ip):
        if _is_hard_denied(candidate):
            raise SubstrateTargetDenied(host, _posture(candidate))
        if _is_private(candidate) and not _env_truthy(env, ALLOW_PRIVATE_ENV):
            raise SubstrateTargetDenied(
                host, "RFC-1918/4193 private range (set CCS_SUBSTRATE_ALLOW_PRIVATE to allow)"
            )


def _denylist_candidates(
    ip: ipaddress.IPv4Address | ipaddress.IPv6Address,
) -> Iterator[ipaddress.IPv4Address | ipaddress.IPv6Address]:
    """The address plus any IPv4 embedded in a mapped/6to4/Teredo IPv6 wrapper.

    Unwrapping is the denylist INVERSION of ``verify_host``'s deliberate
    no-unwrap allowlist rule: a mapped form (``::ffff:169.254.169.254``) must not
    slip past the IPv4 denylist.
    """
    yield ip
    if ip.version == 6:
        for attr in ("ipv4_mapped", "sixtofour"):
            embedded = getattr(ip, attr, None)
            if embedded is not None:
                yield embedded
        teredo = getattr(ip, "teredo", None)
        if teredo is not None:
            yield teredo[1]


def _is_hard_denied(ip: ipaddress.IPv4Address | ipaddress.IPv6Address) -> bool:
    if ip.version == 4:
        return any(ip in net for net in _HARD_DENY_V4_NETS)
    return ip in _LINK_LOCAL_V6_NET or ip in _HARD_DENY_V6_ADDRS


def _is_private(ip: ipaddress.IPv4Address | ipaddress.IPv6Address) -> bool:
    if ip.version == 4:
        return any(ip in net for net in _PRIVATE_V4_NETS)
    return ip in _ULA_V6_NET


def _is_loopback_ip(ip: ipaddress.IPv4Address | ipaddress.IPv6Address) -> bool:
    if ip.version == 4:
        return ip in _LOOPBACK_V4_NET
    return ip == _IPV6_LOOPBACK


def _posture(ip: ipaddress.IPv4Address | ipaddress.IPv6Address) -> str:
    if ip.version == 4:
        if ip in _LINK_LOCAL_V4_NET:
            return "link-local / cloud metadata range (169.254.0.0/16)"
        return "CGNAT / cloud metadata range (100.64.0.0/10)"
    if ip in _LINK_LOCAL_V6_NET:
        return "IPv6 link-local range (fe80::/10)"
    return "IPv6 cloud metadata address"


# ---------------------------------------------------------------------------
# TLS + locality guards
# ---------------------------------------------------------------------------


def _require_tls_or_ack(host: str, posture: str, *, env: Mapping[str, str]) -> None:
    """Refuse a plaintext credential to a routable host without the distinct ack."""
    if _env_truthy(env, INSECURE_ACK_ENV):
        logger.warning(
            "sending a substrate credential to routable host %r over plaintext (%s); "
            "%s acknowledged — ensure the link is encrypted out-of-band",
            host,
            posture,
            INSECURE_ACK_ENV,
        )
        return
    raise SubstrateInsecureTransport(host, posture)


def _warn_mrap(value: str) -> None:
    """Warn on a multi-region access point / replica endpoint (out-of-scope
    distributed-substrate locality)."""
    lowered = value.lower()
    if "mrap" in lowered or "s3-global" in lowered or lowered == "aws-global":
        logger.warning(
            "manifest S3 target %r looks like a multi-region access point / replica; "
            "coordinating agents across hosts against one distributed substrate is out "
            "of scope (single-host only)",
            value,
        )


def _env_truthy(env: Mapping[str, str], name: str) -> bool:
    return env.get(name, "").strip().lower() in _TRUTHY_ENV_VALUES
