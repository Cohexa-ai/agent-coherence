# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Shared HTTP client helpers for the four agent-coherence-* console scripts.

The coordinator binds to 127.0.0.1 with shared-secret Bearer auth
(KTD-12). Each console script needs:

1. Resolve the workspace root.
2. Read the port from ``<root>/.coherence/server.pid``.
3. Read the bearer secret from ``<root>/.coherence/hook.secret``.
4. Make an authenticated request with a short timeout.

Failures degrade gracefully — these scripts run interactively and should
print a one-line human message + exit 1 rather than dump a stack trace.
"""

from __future__ import annotations

import ipaddress
import json
import logging
import os
import ssl
import sys
import urllib.error
import urllib.request
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ccs.adapters.claude_code.lifecycle import read_port_from_file as _read_port_from_file
from ccs.core.exceptions import (
    InsecureTransportRefused,
    RedirectRefused,
    TlsConfigError,
    TlsVerificationFailed,
)

logger = logging.getLogger(__name__)

#: HTTP timeout for CLI requests. The coordinator's per-request watchdog is
#: 4s; we add headroom for connection setup. CLI users are interactive so a
#: 6s ceiling is preferable to retrying.
CLI_HTTP_TIMEOUT_SEC = 6.0


def err(message: str) -> None:
    """Write a diagnostic / error line to stderr.

    P2 ce-review fix #15 (cli-readiness): error output must go to stderr so
    agents composing workflows can read machine-parseable data from stdout
    (e.g., ``port=$(agent-coherence-coordinator)``) without prose pollution.
    Success output stays on stdout via plain ``print()``.
    """
    print(message, file=sys.stderr, flush=True)


def validate_relative_path(p: str) -> str | None:
    """Client-side path pre-check before sending to the coordinator.

    Reject absolute paths, ``..`` traversal, and empty input. Returns
    None on valid, a reason string on invalid.

    M-02 layer-distinction note: this is the CLI-side check (light;
    designed for friendly operator error messages before the request
    is built). The server-side check is
    :func:`ccs.adapters.claude_code.coordinator_server.validate_path`
    — STRICTER (also rejects backslash-leading paths, control characters,
    paths longer than MAX_PATH_LEN, non-string types). The server-side
    check is the authoritative gate; this CLI check is for fast feedback
    without a coordinator round-trip. Do NOT remove either — they live
    at different layers of the trust boundary.

    P2 ce-review fix #6 (maintainability): consolidates the
    ``_validate_path`` helper that previously existed byte-for-byte in
    both coherence_track.py and coherence_untrack.py — divergence risk
    eliminated. Both scripts now import this single source of truth.

    Note: this is a PURE-STRING validator. Callers that want to accept
    absolute paths inside the workspace (operator-UX path, exposed via
    ``/agent-coherence:track`` skill template that passes ``$ARGUMENTS``
    verbatim) should call :func:`normalize_workspace_path` instead, which
    handles absolute-vs-relative + workspace-containment before delegating
    to this function."""
    if not p:
        return "empty"
    if p.startswith("/"):
        return "path must be relative (no leading '/')"
    if ".." in Path(p).parts:
        return "path must not contain '..' traversal"
    return None


def normalize_workspace_path(p: str, root: Path) -> tuple[str, str | None]:
    """Normalize a CLI path argument to workspace-relative form.

    Returns ``(normalized_path, None)`` if the path is valid; returns
    ``(original_path, reason_string)`` if invalid. Accepts both relative
    and absolute paths:

    - **Empty** → ``("", "empty")``
    - **Relative** (e.g., ``"docs/plan.md"``) → validated as-is via
      :func:`validate_relative_path`; returned unchanged on success.
    - **Absolute and inside workspace root** (e.g.,
      ``"/Users/x/repo/docs/plan.md"`` with ``root=/Users/x/repo``) →
      stripped to workspace-relative (``"docs/plan.md"``); re-validated
      for ``..`` traversal defense.
    - **Absolute and outside workspace root** (e.g., ``"/etc/passwd"``)
      → rejected with ``"path outside workspace root"``.

    This helper exists because the Claude Code plugin's
    ``/agent-coherence:track`` skill template substitutes ``$ARGUMENTS``
    verbatim — operators routinely type absolute paths (autocomplete from
    their shell or IDE). Pre-2026-05-26 the CLI rejected those outright;
    this helper normalizes them so the operator UX matches the skill UX.

    The normalized form is what gets written to ``tracked.yaml`` /
    ``ignored.yaml`` — absolute paths must NEVER leak into those files
    because they're per-machine / per-worktree and would break cross-host
    state sharing if the coordinator-backed state.db is ever migrated.

    M-02 trust-boundary note: the server-side validator
    (:func:`ccs.adapters.claude_code.coordinator_server.validate_path`)
    still independently rejects absolute paths in the coordinator
    request body. This client-side normalization happens BEFORE the
    request is built — by the time the request hits the wire, the path
    is workspace-relative. The server check remains the authoritative
    gate against malformed direct-HTTP calls that bypass this CLI.
    """
    if not p:
        return p, "empty"
    if Path(p).is_absolute():
        try:
            normalized = str(Path(p).resolve().relative_to(root.resolve()))
        except ValueError:
            return p, "path outside workspace root"
        # Re-validate the normalized form against the pure-string rules
        # (catches e.g. a resolved path that still contains '..' — defensive)
        reason = validate_relative_path(normalized)
        return (normalized, None) if reason is None else (p, reason)
    reason = validate_relative_path(p)
    return (p, None) if reason is None else (p, reason)


@dataclass(frozen=True)
class CoordinatorEndpoint:
    """Resolved (host, port, bearer_token) for a coordinator.

    ``host`` defaults to loopback so the local path is byte-unchanged; the
    cross-host demo (gated by :class:`RemoteCoordinatorConfig`) supplies a
    routable host via :func:`resolve_remote_endpoint`.

    ``scheme`` defaults to ``"http"`` (the loopback path is byte-unchanged);
    ``"https"`` selects verified TLS — :func:`_execute` builds a hardened
    context via :func:`build_tls_context` (there is no insecure ``https`` mode).
    ``ca_file`` optionally names an exclusive private-CA bundle for that
    context; it is validated and read once at request time (see
    :func:`build_tls_context`).
    """

    port: int
    bearer: str
    host: str = "127.0.0.1"
    scheme: str = "http"
    ca_file: str | None = None

    @property
    def base_url(self) -> str:
        # Bracket an IPv6 literal so the authority parses: http://[::1]:8080, not
        # the ambiguous http://::1:8080 (urllib reads the last colon as the port
        # separator). A hostname / IPv4 raises ValueError -> no brackets. The
        # bracketing branch fires for BOTH schemes (https://[::1]:8443 too).
        authority = self.host
        try:
            if ipaddress.ip_address(self.host).version == 6:
                authority = f"[{self.host}]"
        except ValueError:
            pass
        return f"{self.scheme}://{authority}:{self.port}"


class CoordinatorUnavailable(Exception):
    """Coordinator is not running or its auth surface is missing.

    Carries a human-readable message the console script prints verbatim.
    """


def _read_ca_bundle(ca_file: str) -> str:
    """Read a private-CA PEM bundle with the same discipline as ``_read_secret``.

    A CA bundle is a *trust anchor* — a symlink swap or a writable file between
    check and use is the same attack class as a swapped bearer file, so we open
    with ``O_NOFOLLOW`` (refuse symlinks), reject a group/world-*writable* file
    (the ``0o022`` bits — readable is fine, certs are public), and read ONCE via
    the fd (no path re-open → no TOCTOU). The bytes are handed to the SSL context
    as ``cadata`` so the path is never re-opened.

    Any failure (missing / unreadable / symlink / loose perms) is normalized to
    a :class:`~ccs.core.exceptions.TlsConfigError` naming the path — never a raw
    ``OSError`` / ``ssl.SSLError`` leaking out.
    """
    try:
        fd = os.open(ca_file, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    except OSError as exc:
        # Missing, or a symlink (ELOOP), or otherwise unopenable — fail closed.
        raise TlsConfigError(
            f"CCS_REMOTE_CA_FILE {ca_file!r} could not be opened "
            f"({exc.strerror or exc}); it must be a regular, readable PEM file "
            "(symlinks are refused)",
            path=ca_file,
        ) from exc
    try:
        with os.fdopen(fd, encoding="utf-8") as handle:
            mode = os.fstat(handle.fileno()).st_mode
            # Only the WRITABLE bits are the attack (a swapped trust anchor);
            # a public cert may be group/world-readable.
            if mode & 0o022:
                raise TlsConfigError(
                    f"CCS_REMOTE_CA_FILE {ca_file!r} is group/world-writable "
                    f"(mode {mode & 0o777:o}); tighten it so the trust anchor "
                    "cannot be swapped (e.g. 0644)",
                    path=ca_file,
                )
            return handle.read()
    except UnicodeDecodeError as exc:
        raise TlsConfigError(
            f"CCS_REMOTE_CA_FILE {ca_file!r} is not a text PEM file ({exc})",
            path=ca_file,
        ) from exc
    except OSError as exc:
        raise TlsConfigError(
            f"CCS_REMOTE_CA_FILE {ca_file!r} could not be read ({exc.strerror or exc})",
            path=ca_file,
        ) from exc


def build_tls_context(ca_file: str | None = None) -> ssl.SSLContext:
    """Build the ONE verified-TLS context for the coordinator client.

    This is the single mTLS-forward-compat choke point: a later mTLS phase adds
    ``load_cert_chain`` config keys here and nowhere else. There is deliberately
    NO cert-verification off-switch — ``CERT_NONE`` / ``check_hostname=False`` is
    unrepresentable through any parameter (the footgun rule).

    - ``create_default_context`` (secure defaults on 3.11+: ``CERT_REQUIRED`` +
      ``check_hostname`` + ``VERIFY_X509_STRICT``); with ``cadata`` from an
      exclusive private-CA bundle when ``ca_file`` is given, else the system
      trust store.
    - The TLS floor is pinned to 1.2 explicitly (do not rely on the default).
    - ``OP_NO_RENEGOTIATION`` is intentionally NOT set (mTLS forward-compat).
    - Hardening invariant asserted: if ``check_hostname`` or ``CERT_REQUIRED``
      were ever weakened by a future edit, this raises :class:`TlsConfigError`
      rather than silently shipping an insecure client.
    """
    cadata = _read_ca_bundle(ca_file) if ca_file else None
    try:
        ctx = ssl.create_default_context(cadata=cadata)
    except ssl.SSLError as exc:
        # e.g. cadata present but not valid PEM — normalize to a typed config error.
        raise TlsConfigError(
            f"CCS_REMOTE_CA_FILE {ca_file!r} does not contain a valid PEM certificate "
            f"({exc})",
            path=ca_file,
        ) from exc

    ctx.minimum_version = ssl.TLSVersion.TLSv1_2
    # IP-literal endpoints: OpenSSL matches IP SANs natively; disabling the
    # legacy CN fallback keeps verification to SAN-only (no effect on the SAN
    # match, tightens the no-SAN case). Harmless where the attribute is absent.
    if hasattr(ctx, "hostname_checks_common_name"):
        ctx.hostname_checks_common_name = False

    # Invariant: verification MUST be enforced. This is the assertion the guard's
    # positive signal (Unit 2) relies on — https means enforced verification.
    if not (ctx.check_hostname and ctx.verify_mode == ssl.CERT_REQUIRED):
        raise TlsConfigError(
            "internal error: the TLS context is not enforcing certificate "
            "verification (check_hostname/CERT_REQUIRED invariant violated)"
        )
    return ctx


def resolve_endpoint(coordinator_root: Path) -> CoordinatorEndpoint:
    """Read port + secret from ``<root>/.coherence/`` or raise
    :class:`CoordinatorUnavailable` with an operator-friendly message."""
    coherence_dir = coordinator_root / ".coherence"
    pid_file = coherence_dir / "server.pid"
    secret_file = coherence_dir / "hook.secret"

    port = _read_port_from_file(pid_file)
    if port is None:
        raise CoordinatorUnavailable(
            "no coordinator running for this workspace "
            f"(no port in {pid_file}); start one with `agent-coherence-coordinator`"
        )

    if not secret_file.is_file():
        raise CoordinatorUnavailable(
            "coordinator authentication unavailable "
            f"(missing {secret_file}); restart with `agent-coherence-coordinator`"
        )

    try:
        bearer = secret_file.read_text(encoding="utf-8").strip()
    except OSError as exc:
        raise CoordinatorUnavailable(
            f"could not read {secret_file}: {exc}"
        ) from exc

    if not bearer:
        raise CoordinatorUnavailable(
            f"{secret_file} is empty; restart with `agent-coherence-coordinator`"
        )

    return CoordinatorEndpoint(port=port, bearer=bearer)


#: Truthy env values that enable cross-host remote mode (mirrors the
#: telemetry kill-switch parser). Everything else (incl. "" and "0") is OFF.
_REMOTE_TRUTHY_ENV_VALUES: frozenset[str] = frozenset({"1", "true", "yes", "on"})


def _is_loopback_transport_host(host: str) -> bool:
    """True for hosts that may receive a bearer over plaintext HTTP WITHOUT an ack.

    Broader than the Host-allowlist :func:`~ccs.adapters.claude_code.auth.is_loopback_host`:
    covers ``127.0.0.0/8`` and ``::1`` (via :mod:`ipaddress`) plus ``"localhost"``.
    IPv4-mapped IPv6 forms (``::ffff:127.0.0.1``) are treated as NON-loopback and
    require the ack: ``is_loopback`` for the mapped form varies across CPython patch
    releases, so we classify it deterministically as non-loopback rather than depend
    on the stdlib version (fail-closed either way — genuine local dev uses
    ``127.0.0.1`` / ``::1``). A non-IP hostname (a name we cannot classify) is
    likewise NON-loopback, so the guard fails closed on it.
    """
    if host == "localhost":
        return True
    try:
        ip = ipaddress.ip_address(host)
    except ValueError:
        return False
    # IPv4-mapped IPv6 (::ffff:a.b.c.d): fail closed deterministically (see above).
    if getattr(ip, "ipv4_mapped", None) is not None:
        return False
    return ip.is_loopback


def _guard_plaintext_bearer(host: str, env: Mapping[str, str]) -> None:
    """Fail closed on a plaintext bearer to a non-loopback host (Phase-1.5 guard).

    The remote transport is always ``http://`` (encryption is operator-provided
    out-of-band — WireGuard or a TLS-terminating proxy), so there is no in-band TLS
    signal. For a non-loopback host the operator must set ``CCS_REMOTE_INSECURE``
    (truthy) to acknowledge the link is secured, or the bearer is never sent
    (:class:`InsecureTransportRefused`). Reduces-not-eliminates: it removes the
    SILENT plaintext-bearer footgun, not the operator's duty to secure the link.
    """
    if _is_loopback_transport_host(host):
        return
    if env.get("CCS_REMOTE_INSECURE", "").strip().lower() in _REMOTE_TRUTHY_ENV_VALUES:
        # Names host/posture only — never the bearer/secret value.
        logger.warning(
            "sending a bearer to non-loopback host %r over plaintext HTTP "
            "(CCS_REMOTE_INSECURE acknowledged — ensure the link is encrypted)",
            host,
        )
        return
    raise InsecureTransportRefused(host)


def resolve_remote_endpoint(
    host: str,
    port: int,
    secret: str,
    *,
    scheme: str = "http",
    ca_file: str | None = None,
    env: Mapping[str, str] | None = None,
) -> CoordinatorEndpoint:
    """Build an endpoint for a REMOTE coordinator (cross-host demo).

    Unlike :func:`resolve_endpoint`, this reads nothing from the local
    ``.coherence/`` directory — host, port, and the bearer secret are supplied
    by the caller (typically via :meth:`RemoteCoordinatorConfig.from_env`).
    Gated by :class:`RemoteCoordinatorConfig`.

    ``scheme`` (default ``"http"``) and ``ca_file`` (default ``None``) are
    additive: they thread the verified-TLS surface onto the endpoint without
    changing any existing caller. ``scheme="https"`` selects the verified-TLS
    request path in :func:`_execute`.

    Fail-closed transport guard: for a NON-loopback host the bearer is only sent
    when ``CCS_REMOTE_INSECURE`` (read from ``env``, default ``os.environ``) is
    truthy — otherwise :class:`InsecureTransportRefused` is raised (the transport
    is plaintext HTTP; the ack acknowledges an out-of-band-secured link). Loopback
    is byte-unchanged. (Unit 2 will consult ``scheme`` inside the guard so a
    verified-TLS endpoint passes without the ack; this unit only threads it
    through — the guard decision is unchanged here.)
    """
    if not host:
        raise CoordinatorUnavailable("remote coordinator host is empty")
    if not secret:
        raise CoordinatorUnavailable("remote coordinator bearer secret is empty")
    _guard_plaintext_bearer(host, os.environ if env is None else env)
    return CoordinatorEndpoint(
        port=port, bearer=secret, host=host, scheme=scheme, ca_file=ca_file
    )


@dataclass(frozen=True)
class RemoteCoordinatorConfig:
    """Default-OFF gate for cross-host remote-coordinator mode.

    Absent the ``CCS_REMOTE_COORDINATOR`` env flag, :meth:`from_env` returns a
    disabled config and every existing loopback-only behavior is byte-unchanged
    — the cross-host relaxation never reaches local users.

    Secret channel: the bearer is read from a FILE whose path is given by
    ``CCS_REMOTE_SECRET_FILE`` — never inline in an env var, which would leak in
    ``ps`` / ``docker inspect`` (the R7 security pass owns this). The file
    mirrors the local ``hook.secret`` (mode 0600, mounted into the remote
    container).
    """

    enabled: bool
    host: str | None = None
    port: int | None = None
    secret: str | None = None
    #: ``"https"`` when ``CCS_REMOTE_TLS`` is truthy, else ``"http"`` (default).
    #: Selects verified TLS on the resolved endpoint (Unit 1).
    scheme: str = "http"
    #: Optional private-CA bundle PATH from ``CCS_REMOTE_CA_FILE`` (file-not-inline,
    #: mirroring ``CCS_REMOTE_SECRET_FILE``). Validated/read at request time.
    ca_file: str | None = None

    @classmethod
    def from_env(cls, env: dict[str, str] | None = None) -> RemoteCoordinatorConfig:
        """Parse the cross-host flag from the environment (default OFF)."""
        env = os.environ if env is None else env
        flag = env.get("CCS_REMOTE_COORDINATOR", "").strip().lower()
        if flag not in _REMOTE_TRUTHY_ENV_VALUES:
            return cls(enabled=False)
        host = (env.get("CCS_REMOTE_HOST") or "").strip() or None
        port_raw = (env.get("CCS_REMOTE_PORT") or "").strip()
        port = int(port_raw) if port_raw.isdigit() else None
        if port is not None and not (1 <= port <= 65535):
            port = None  # out of TCP range -> treat as unset (fail closed downstream)
        tls = env.get("CCS_REMOTE_TLS", "").strip().lower() in _REMOTE_TRUTHY_ENV_VALUES
        ca_file = (env.get("CCS_REMOTE_CA_FILE") or "").strip() or None
        return cls(
            enabled=True,
            host=host,
            port=port,
            secret=cls._read_secret(env),
            scheme="https" if tls else "http",
            ca_file=ca_file,
        )

    @staticmethod
    def _read_secret(env: dict[str, str]) -> str | None:
        """Read the bearer from ``CCS_REMOTE_SECRET_FILE`` (not an inline env var).

        Hardened: refuses to follow a symlinked secret file (``O_NOFOLLOW`` — an
        attacker able to set the env var could otherwise repoint it at any
        readable file), and warns if the file is group/world-accessible (``0600``
        expected, like the local ``hook.secret``). Fails closed (returns ``None``)
        on any error.
        """
        secret_path = (env.get("CCS_REMOTE_SECRET_FILE") or "").strip()
        if not secret_path:
            return None
        try:
            fd = os.open(secret_path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
        except OSError:
            return None  # missing, or a symlink (ELOOP) — fail closed
        try:
            with os.fdopen(fd, encoding="utf-8") as handle:
                mode = os.fstat(handle.fileno()).st_mode
                if mode & 0o077:
                    logger.warning(
                        "CCS_REMOTE_SECRET_FILE %s is group/world-accessible (mode %o); "
                        "tighten it to 0600",
                        secret_path,
                        mode & 0o777,
                    )
                secret = handle.read().strip()
        except OSError:
            return None
        return secret or None


def get(
    endpoint: CoordinatorEndpoint,
    path: str,
    *,
    extra_headers: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Authenticated GET. Raises :class:`CoordinatorUnavailable` on network
    error; raises :class:`urllib.error.HTTPError` for non-2xx so the caller
    can format status codes explicitly.

    R12 (Unit 6): ``extra_headers`` lets local-operator CLIs (e.g.,
    ``agent-coherence-status``) add ``Coherence-Local-Operator: true``
    for the elevated ``/status?detail=full`` tier without hard-coding
    that header here."""
    headers: dict[str, str] = {
        "Authorization": f"Bearer {endpoint.bearer}",
        "Host": endpoint.host,
    }
    if extra_headers:
        headers.update(extra_headers)
    req = urllib.request.Request(
        url=f"{endpoint.base_url}{path}",
        method="GET",
        headers=headers,
    )
    # Carry the CA bundle to _execute (only consulted for https requests).
    req._ccs_ca_file = endpoint.ca_file  # type: ignore[attr-defined]
    return _execute(req)


def post(
    endpoint: CoordinatorEndpoint,
    path: str,
    body: dict[str, Any],
    *,
    extra_headers: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Authenticated POST with JSON body.

    M-04 / finding #28: ``extra_headers`` mirrors the pattern on ``get()``
    so callers can add e.g. ``Coherence-Local-Operator: true`` without
    reimplementing the urllib transport layer.
    """
    payload = json.dumps(body).encode("utf-8")
    headers: dict[str, str] = {
        "Authorization": f"Bearer {endpoint.bearer}",
        "Host": endpoint.host,
        "Content-Type": "application/json",
    }
    if extra_headers:
        headers.update(extra_headers)
    req = urllib.request.Request(
        url=f"{endpoint.base_url}{path}",
        data=payload,
        method="POST",
        headers=headers,
    )
    # Carry the CA bundle to _execute (only consulted for https requests).
    req._ccs_ca_file = endpoint.ca_file  # type: ignore[attr-defined]
    return _execute(req)


class _NoRedirectHandler(urllib.request.HTTPRedirectHandler):
    """Refuse ANY 3xx instead of following it.

    The coordinator is one fixed, operator-configured endpoint — no redirect is
    ever legitimate. Critically, urllib's default ``HTTPRedirectHandler`` COPIES
    the ``Authorization`` header onto the redirected hop *before* returning, so a
    post-hoc check on the final response cannot protect the bearer. Refusing here,
    inside ``redirect_request`` (called before the second request is issued),
    ensures the bearer never leaves the configured endpoint.
    """

    def redirect_request(  # type: ignore[override]
        self, req, fp, code, msg, headers, newurl
    ):  # noqa: ANN001, ANN201 - matches the stdlib handler signature
        raise RedirectRefused(newurl, status=code)


def _build_opener(context: ssl.SSLContext | None) -> urllib.request.OpenerDirector:
    """A private opener whose redirect handler refuses every 3xx.

    ``build_opener`` with our ``_NoRedirectHandler`` REPLACES the default
    ``HTTPRedirectHandler`` (``build_opener`` de-dupes by handler class). For
    https, the ``HTTPSHandler(context=...)`` carries the verified-TLS context.
    """
    handlers: list[urllib.request.BaseHandler] = [_NoRedirectHandler()]
    if context is not None:
        handlers.append(urllib.request.HTTPSHandler(context=context))
    return urllib.request.build_opener(*handlers)


def _execute(req: urllib.request.Request) -> dict[str, Any]:
    context: ssl.SSLContext | None = None
    if req.type == "https":
        # build_tls_context may raise TlsConfigError (typed, fail-closed) — that
        # is a config bug, not a transient network failure, so it propagates.
        ca_file = getattr(req, "_ccs_ca_file", None)
        context = build_tls_context(ca_file)

    opener = _build_opener(context)
    try:
        with opener.open(req, timeout=CLI_HTTP_TIMEOUT_SEC) as resp:
            raw = resp.read()
    except urllib.error.HTTPError:
        # Caller handles status-code-specific paths.
        raise
    except RedirectRefused:
        # Typed refusal from _NoRedirectHandler — fail closed, do not degrade.
        raise
    except urllib.error.URLError as exc:
        # A TLS certificate-verification failure surfaces here (SSLError wrapped
        # in URLError). It is a TRUST decision, not a transient hiccup: map it to
        # the typed refusal so the bearer is never retried over plaintext, and
        # keep it distinct from CoordinatorUnavailable (which callers may treat
        # as retryable). Every other URLError stays CoordinatorUnavailable.
        if isinstance(exc.reason, ssl.SSLCertVerificationError):
            raise TlsVerificationFailed(
                _host_of(req), str(exc.reason)
            ) from exc
        raise CoordinatorUnavailable(
            f"could not reach coordinator at {req.full_url}: {exc.reason}"
        ) from exc
    except (OSError, TimeoutError) as exc:
        raise CoordinatorUnavailable(
            f"network error talking to coordinator: {exc}"
        ) from exc

    if not raw:
        return {}
    try:
        return json.loads(raw.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise CoordinatorUnavailable(
            f"coordinator returned non-JSON response: {exc}"
        ) from exc


def _host_of(req: urllib.request.Request) -> str:
    """Best-effort host for a TLS-verification error message (never the bearer).

    Prefer the explicit ``Host`` header (a bare host, set by ``get``/``post`` from
    ``endpoint.host``) over ``req.host`` (which carries ``host:port``) so the
    typed refusal names the clean host the operator configured.
    """
    return req.get_header("Host") or getattr(req, "host", "") or req.full_url


def http_status_from_error(exc: urllib.error.HTTPError) -> dict[str, Any] | None:
    """Best-effort JSON decode of an HTTPError body, for one-line user output."""
    try:
        raw = exc.read()
    except Exception:
        return None
    if not raw:
        return None
    try:
        return json.loads(raw.decode("utf-8"))
    except Exception:
        return None
