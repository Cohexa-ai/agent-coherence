# Security & supply chain

This document covers what end users need to know to install, configure, and run
`agent-coherence` safely: the outbound-traffic posture, the kill switches that
disable telemetry-shaped behavior, the local files the package writes, how to
install with hash pinning, and how to verify the cryptographic provenance of a
published wheel.

## Outbound network destinations

`agent-coherence` and its `[diagnose]` extra make **zero outbound network
requests** in v0. No submission code ships in v0.

If you find any outbound traffic from this package in v0, please [open a
security advisory](https://github.com/hipvlady/agent-coherence/security/advisories)
— it would be a bug.

**Cross-host mode (default OFF).** Setting `CCS_REMOTE_COORDINATOR=1` and pointing
a `CoherentVolume` at a remote coordinator (`CCS_REMOTE_HOST` / `CCS_REMOTE_PORT` /
`CCS_REMOTE_SECRET_FILE`) makes the client open HTTP connections to that
**user-configured, private-range coordinator endpoint** — never the internet, and
no telemetry. This is the only network traffic the library generates, it is opt-in,
and the coordinator only binds beyond loopback to an RFC-1918/4193 address (see the
cross-host demo, `examples/cross_host/`). With the flag unset the zero-outbound
posture above is unchanged.

## Env-var kill switches

Set any of these to a truthy value (`1`, `true`, `yes`) to disable
telemetry-shaped output completely (no consent prompt, no calibration write,
no payload generation even in `--dry-run`):

- `DO_NOT_TRACK=1` (cross-tool consensus per consoledonottrack.com)
- `DISABLE_TELEMETRY=1`
- `CCS_DIAGNOSE_NO_TELEMETRY=1`

The CLI flags `--no-telemetry` and `--no-network` provide the same suppression
at the invocation level.

### Rendering defaults

Two environment variables override the report CTA defaults baked into the
HTML report. They are read at import time of `ccs.diagnose.render`:

- `CCS_DIAGNOSE_BOOK_A_CALL_URL` — replaces the default cal.com link. Must
  start with `http://` or `https://`; `javascript:`, `data:`, `vbscript:` are
  rejected by `RenderOptions.__post_init__`.
- `CCS_DIAGNOSE_CONTACT_EMAIL` — replaces the default reply-to address. Must
  match a plain `local@host` form; URL schemes embedded in the value are
  rejected.

Both env vars are validated by the same allowlist that gates caller-supplied
`RenderOptions(book_a_call_url=...)` / `contact_email=...` arguments — so an
attacker who can set the env cannot smuggle an XSS sink past the renderer.

## Local config and data files

`ccs-diagnose` writes to two well-known locations under XDG paths:

| File | Path | Mode | Created when |
|---|---|---|---|
| Consent state | `$XDG_CONFIG_HOME/ccs-diagnose/consent.json` | `0600` | First TTY run with consent prompt |
| Calibration corpus | `$XDG_DATA_HOME/ccs-diagnose/calibration.jsonl` | `0600` | First `--calibration-record` invocation |

Both directories are created with mode `0700`. Both fall back to `~/.config/...`
and `~/.local/share/...` when the XDG vars are unset. Reset the consent token any
time with `ccs-diagnose --reset-token`.

`CCSStore.record_to(path)` (the v0.8.2+ replay capture API) writes to a
caller-supplied directory. Files are mode `0o600`; the directory is created
with `mkdir(parents=True, exist_ok=True)` so the caller controls the path
and the surrounding-directory permissions.

| File | Path | Mode | Created when |
|---|---|---|---|
| Capture manifest | `<path>/manifest.json` | `0600` | `CCSStore.record_to.__enter__` (atomic write via tempfile + `os.replace`) |
| MESI state log | `<path>/state_log.jsonl` | `0600` | First emitted event when `state_log` stream is enabled (default) |
| Content audit log | `<path>/content_audit_log.jsonl` | `0600` | First emitted event when `content_audit_log` stream is enabled (default). Pass `streams={"state_log"}` to opt out — useful for PII-constrained partners. |

The capture path **refuses to start** when `<path>/manifest.json` already
exists (`SessionDirectoryNotEmptyError`) to prevent silent multi-instance
trace interleave. No content is read by `agent-coherence-replay` from any
location other than the explicit `session_dir` argument.

### Durable version retention (opt-in)

`SqliteArtifactRegistry` can durably retain a bounded history of committed
artifact versions (enabled with `retain_versions=True` plus a `RetentionPolicy`;
**off by default**). This is the one place the coordinator's durable store holds
artifact **content bytes** rather than only `content_hash` — a deliberate,
bounded reversal of the prior hash-only posture, scoped to retained versions and
to in-process embedders (the Claude Code hook/HTTP coordinator topology is
unchanged and stays hash-only).

| File | Path | Mode | Holds |
|---|---|---|---|
| Version store | `<workspace>/.coherence/state.db` (`artifact_versions` table) | `0600` | Retained version bodies (str/bytes), bounded by the configured `max_versions` / `max_age_seconds` |

What this means for sensitive content:

- **Content on disk.** With retention on, version bodies are written to
  `state.db`. The file and its `-wal` / `-shm` sidecars are created `0600`
  *before* the first write — there is no umask window — and `.coherence` is
  `0700`. Migrating an older database re-applies `0600` and warns once. Bodies
  can contain whatever your artifacts contain (credentials, PII), so treat the
  file as sensitive: never commit it to git, and copy it only with mode
  preserved (`install -m 0600` / `cp -p`) — a copy is as sensitive as the
  content.
- **Capture at registration.** A version body is captured when an artifact is
  first registered, not only on write, so a merely-observed artifact's initial
  body can land on disk.
- **Deletion, not unreachability.** Collecting a version (policy GC) or an epoch
  reset (delete-and-recreate of `state.db`) *deletes* the rows, but SQLite may
  keep freed-page residue in the file and its WAL until a checkpoint / `VACUUM`.
  To purge rotated content fully, remove `state.db` together with its `-wal` and
  `-shm` sidecars. Disabling retention does **not** purge existing rows — they
  stay readable; purge is a re-open under a tighter policy or an epoch reset.
- **What a retained version is.** The store records the content the coordinator
  *committed* at that version — not necessarily the bytes a client later
  persisted elsewhere.

The read side (`CoordinatorService.read_at_version`, `agent-coherence-replay
resolve`) opens the store **read-only** and returns retained bytes only on
explicit request; the replay CLI is metadata-only by default and emits bodies
only via `--include-content` / `--output-file`, so terminals, CI logs, and shell
history don't capture content inadvertently. No HTTP route serves version
content.

## Hash-pinned install for security-sensitive users

For reproducible installs with full dependency-graph pinning:

    pip install --require-hashes -r requirements-diagnose.txt

The `requirements-diagnose.txt` file in the repo root is regenerated on each
release via `uv export --format requirements-txt --frozen --extra diagnose
--no-emit-project --no-dev`. It pins every transitive dependency by SHA-256
hash.

`uv.lock` in the repo is the developer lockfile. Downstream installers should
prefer `requirements-diagnose.txt` for reproducible installs.

## Verifying release attestations (PEP 740)

Each wheel published to PyPI ships with a Sigstore-backed PEP 740 attestation
tied to the GitHub Actions workflow that built it. To verify before installing:

    pip install pypi-attestations
    pypi-attestations verify --provenance \
        --repo hipvlady/agent-coherence \
        --workflow release.yml \
        agent_coherence-X.Y.Z-py3-none-any.whl

The PyPI page also displays the verified provenance in the release sidebar.
You can also inspect the raw signed attestation directly:

    curl -s \
      https://pypi.org/integrity/agent-coherence/X.Y.Z/agent_coherence-X.Y.Z-py3-none-any.whl/provenance \
      | python3 -m json.tool

The `publisher` block in each attestation bundle should report
`{kind: GitHub, repository: hipvlady/agent-coherence, workflow: release.yml, environment: pypi}`.
A publisher mismatch is the signature of a Trusted Publisher misconfiguration —
do not install if the values diverge from those above.

> **Note on `gh attestation verify`.** That command queries GitHub's SLSA
> build-provenance attestation store, which the current release workflow does
> not populate. It will return HTTP 404 against this package's wheels. The
> PEP 740 attestation lives on PyPI; use `pypi-attestations verify` or the
> raw `curl` inspection above. A future release-workflow enhancement could
> add an `actions/attest-build-provenance` step to also publish SLSA
> attestations to GitHub, at which point `gh attestation verify` would work.

## CycloneDX SBOM

Each GitHub Release attaches a CycloneDX SBOM (`sbom.cyclonedx.json`) listing
the full transitive dependency surface at build time. Diff across releases to
see dependency-graph changes. Generated in CI via `cyclonedx-py environment`.

## Supply-chain threat model

The deepeval incident (April 2026, GitHub deepeval#2497) is the canonical
recent example of a malicious release published under a trusted name. The
controls in place to mitigate similar attacks:

| Threat | Control |
|---|---|
| Stolen/leaked maintainer PyPI token | PyPI Trusted Publishers via OIDC — no static token exists to steal |
| Hijacked maintainer GitHub account | Required PR review on `.github/workflows/release.yml`; ruleset protecting `refs/tags/v*` (admin-only bypass) |
| Typosquat package install | Reserved typo variants (`agent-coherance`, `agentcoherence`, `agent_coherence`, `ccs-diagnose`, `ccsdiagnose`) under the same publisher |
| Dependency-confusion attack via `--extra-index-url` | Canonical install command documented (below); no private mirror references |
| Unverified release tampering | PEP 740 attestations + SBOM (see sections above) |
| Malicious local package shadows `langgraph` | `_detect_stack()` reads `importlib.metadata.version("langgraph")`; a shadowing package could spoof version. Low risk for the calibration-only v0 surface (no live submission), hardening note for v1 |
| Runtime exfiltration | No import-time side effects (audit-hook test); `--no-network` / kill switches; consent-gated calibration write to local file only |

## Canonical install command

    pip install --index-url https://pypi.org/simple/ "agent-coherence[diagnose]"

Avoid `--extra-index-url` to a private mirror — that's the dependency-confusion
attack vector. If you must use a private mirror, ensure `agent-coherence` is
served only from the official PyPI index.

## Reporting security issues

Open a private security advisory at
`https://github.com/hipvlady/agent-coherence/security/advisories/new` rather
than a public issue. We aim to respond within 72 hours.
