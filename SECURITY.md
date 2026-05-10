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

## Manual setup checklist (for maintainers)

The release workflow assumes the following are configured on the PyPI / GitHub
side. Until done, releases will fail at the publish step (by design — fail-closed):

- [ ] **PyPI Trusted Publishers configured** on `agent-coherence`'s PyPI project page
      (Settings → Publishing → Add a new publisher → GitHub → repo + workflow filename
      `release.yml` + environment name `pypi`)
- [ ] **GitHub environment `pypi` created** with required reviewers (the workflow
      uses `environment: name: pypi` to gate publish steps)
- [ ] **Branch protection on `main`** requires PR review for `.github/workflows/`
      changes
- [ ] **Tag protection rule** on `v*` tags (only allow specific actors / require
      PR-reviewed commits)
- [ ] **2FA enforced** on all PyPI maintainers (PyPI now requires this for critical
      projects, but verify)
- [ ] **Typosquat name reservations** on PyPI: `agent-coherance`, `agentcoherence`,
      `agent_coherence`, `ccs-diagnose`, `ccsdiagnose` (create empty placeholder
      projects under the same publisher)
- [ ] **Org-level audit log review** quarterly for unexpected workflow / secrets
      changes

## Reporting security issues

Open a private security advisory at
`https://github.com/hipvlady/agent-coherence/security/advisories/new` rather than a
public issue. We aim to respond within 72 hours.
