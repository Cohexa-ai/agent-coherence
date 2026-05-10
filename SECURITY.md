# Security & supply chain

## Outbound network destinations

`agent-coherence` and its `[diagnose]` extra make **zero outbound network requests**
in v0. The Phase 2 ops plan adds a benchmark endpoint; no submission code ships in v0.

If you find any outbound traffic from this package in v0, please [open a security
advisory](https://github.com/hipvlady/agent-coherence/security/advisories) — it would
be a bug.

## Env-var kill switches

Set any of these to a truthy value (`1`, `true`, `yes`) to disable telemetry-shaped
output completely (no consent prompt, no calibration write, no payload generation
even in `--dry-run`):

- `DO_NOT_TRACK=1` (cross-tool consensus per consoledonottrack.com)
- `DISABLE_TELEMETRY=1`
- `CCS_DIAGNOSE_NO_TELEMETRY=1`

The CLI flags `--no-telemetry` and `--no-network` provide the same suppression at the
invocation level.

### Rendering defaults

Two environment variables override the placeholder CTA defaults baked into the
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

Both directories are created with mode `0700`. Both fall back to `~/.config/...` and
`~/.local/share/...` when the XDG vars are unset. Reset the consent token any time
with `ccs-diagnose --reset-token`.

## Hash-pinned install for security-sensitive users

For reproducible installs with full dependency-graph pinning:

    pip install --require-hashes -r requirements-diagnose.txt

The `requirements-diagnose.txt` file in the repo root is regenerated on each
release via `uv export --format requirements-txt --frozen --extra diagnose
--no-emit-project --no-dev`. It pins every transitive dependency by SHA-256 hash.

`uv.lock` in the repo is the developer lockfile. Downstream installers should
prefer `requirements-diagnose.txt` for reproducible installs.

## Verifying release attestations (PEP 740)

Each wheel published to PyPI ships with a Sigstore-backed PEP 740 attestation
tied to the GitHub Actions workflow that built it. To verify before installing:

    # Using pypi-attestations:
    pip install pypi-attestations
    pypi-attestations verify --provenance \
        --repo hipvlady/agent-coherence \
        --workflow release.yml \
        agent_coherence-X.Y.Z-py3-none-any.whl

    # Or using gh:
    gh attestation verify <wheel> --repo hipvlady/agent-coherence

The PyPI page also displays the verified provenance in the release sidebar.

## CycloneDX SBOM

Each GitHub Release attaches a CycloneDX SBOM (`sbom.cyclonedx.json`) listing
the full transitive dependency surface at build time. Diff across releases to
see dependency-graph changes. Generated in CI via `cyclonedx-py environment`.

## Supply-chain threat model

The deepeval incident (April 2026, GitHub deepeval#2497) is the canonical recent
example of a malicious release published under a trusted name. Our mitigations:

| Threat | Control | Status |
|---|---|---|
| Stolen/leaked maintainer PyPI token | PyPI Trusted Publishers via OIDC — no static token exists to steal | Workflow shipped; PyPI-side setup pending |
| Hijacked maintainer GitHub account | Required PR review on `.github/workflows/release.yml`; tag-protection on `v*` | **Manual:** repo-settings step |
| Typosquat package install | Reserve obvious typo variants (`agent-coherance`, `agentcoherence`, `agent_coherence`, `ccs-diagnose`, `ccsdiagnose`) under same publisher | **Manual:** PyPI-side step |
| Dependency-confusion attack via `--extra-index-url` | Canonical install command documented; no private mirror references | Documented |
| Unverified release tampering | PEP 740 attestations + SBOM | Workflow shipped |
| Malicious local package shadows `langgraph` | `_detect_stack()` reads `importlib.metadata.version("langgraph")`; a shadowing package could spoof version. Low risk for the calibration-only v0 surface (no live submission), but a hardening note for v1 | Documented |
| Runtime exfiltration | No import-time side effects (audit-hook test); `--no-network` / kill switches; consent-gated calibration write to local file only | Shipped |

## Canonical install command

    pip install --index-url https://pypi.org/simple/ "agent-coherence[diagnose]"

Avoid `--extra-index-url` to a private mirror — that's the dependency-confusion
attack vector. If you must use a private mirror, ensure `agent-coherence` is
served only from the official PyPI index.

## Pre-release verification (MUST pass before any `v*` tag push)

Run the bundled verifier before tagging a release:

```bash
python tools/check_release_readiness.py
# or, after install:
ccs-check-release
```

The script exits non-zero if any automated check fails, and the release
workflow runs the same script as a preflight job — a misconfigured
project cannot publish.

### Automated checks (run by `tools/check_release_readiness.py`)

| Check | Verifies | Failure mode if skipped |
|---|---|---|
| PyPI Trusted Publishers | `gh api repos/{owner}/{repo}/environments/pypi` returns 200 | Publish step in `release.yml` falls back to anonymous OIDC and fails — but only after the build artefact is already in transit |
| GitHub `pypi` environment | Same call confirms required reviewers are set | Anyone with workflow-write access could trigger an unreviewed publish |
| Branch protection on `main` | `gh api repos/{owner}/{repo}/branches/main/protection` returns 200 with PR review required | Malicious workflow changes can land without review |
| Tag protection on `v*` | `gh api repos/{owner}/{repo}/tags/protection` covers `v*` | Anyone with push access can cut an arbitrary release |

### Manual verification (cannot be automated)

These items the script reminds you about but cannot itself verify:

- [ ] **2FA enforced** on all PyPI maintainers (PyPI requires this for
      critical projects; recheck the PyPI account page each release)
- [ ] **Typosquat name reservations** on PyPI: `agent-coherance`,
      `agentcoherence`, `agent_coherence`, `ccs-diagnose`,
      `ccsdiagnose` (empty placeholder projects under the same
      publisher prevent name-squat attacks against unsuspecting users
      who mistype the install command)
- [ ] **Org-level audit log review** scheduled quarterly for unexpected
      workflow / secrets changes (a logged-in attacker who cannot
      directly publish may still mutate Trusted Publisher settings; the
      audit log is the only place this is visible)
- [ ] **CycloneDX SBOM artefact** uploaded to the GitHub Release page
      (the workflow generates it; verify the file is attached to the
      release before announcing)

### Background

The release workflow assumes the GitHub + PyPI side is configured as listed
above. Until done, releases will fail at the publish step (by design —
fail-closed). The verifier exists so a release does not blow up halfway
through and leave a partial state.

## Reporting security issues

Open a private security advisory at
`https://github.com/hipvlady/agent-coherence/security/advisories/new` rather than a
public issue. We aim to respond within 72 hours.
