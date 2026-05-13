# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""Unit 10 — supply-chain hardening artifact tests.

Validates the presence and structural correctness of:

* ``.github/workflows/release.yml`` — Trusted Publishers OIDC + PEP 740 attestations
* ``requirements-diagnose.txt`` — hash-pinned reproducible install path
* ``docs/security.md`` — end-user trust contract (env-var kill switches,
  hash-pinned install, attestation verification, threat model, canonical
  install command, security-issue reporting)
* ``uv.lock`` — committed developer lockfile
* ``README.md`` — security & supply chain pointer subsection

The maintainer-side pre-release verification gate (``ccs-check-release`` and
its manual checklist) is documented in a local-only operations file at the
repo root and is not asserted here, because it intentionally never lands in
the public tree.

These are file-existence + content-grep + YAML-validity checks. We do not try to
validate full GitHub Actions workflow semantics (that's GitHub's job) — we only
assert the load-bearing properties this unit shipped.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent

RELEASE_WORKFLOW = REPO_ROOT / ".github" / "workflows" / "release.yml"
REQUIREMENTS_DIAGNOSE = REPO_ROOT / "requirements-diagnose.txt"
SECURITY_MD = REPO_ROOT / "docs" / "security.md"
UV_LOCK = REPO_ROOT / "uv.lock"
README = REPO_ROOT / "README.md"


# ---------------------------------------------------------------------------
# release.yml
# ---------------------------------------------------------------------------


def test_release_workflow_exists() -> None:
    assert RELEASE_WORKFLOW.is_file(), "release.yml must exist"


def test_release_workflow_is_valid_yaml() -> None:
    parsed = yaml.safe_load(RELEASE_WORKFLOW.read_text())
    assert isinstance(parsed, dict)
    assert "jobs" in parsed


def test_release_workflow_triggers_on_v_tags() -> None:
    parsed = yaml.safe_load(RELEASE_WORKFLOW.read_text())
    # PyYAML parses bare "on:" as boolean True under YAML 1.1 — handle either form.
    on_block = parsed.get("on") if "on" in parsed else parsed.get(True)
    assert on_block is not None, "release.yml must declare a trigger block"
    push = on_block.get("push", {})
    tags = push.get("tags", [])
    assert any("v*" in t for t in tags), "release.yml must trigger on v* tags"


def test_release_workflow_declares_id_token_write() -> None:
    parsed = yaml.safe_load(RELEASE_WORKFLOW.read_text())
    jobs = parsed["jobs"]
    found_id_token_write = False
    for _job_name, job in jobs.items():
        permissions = job.get("permissions", {}) or {}
        if permissions.get("id-token") == "write":
            found_id_token_write = True
            break
    assert found_id_token_write, (
        "At least one job must declare permissions.id-token: write for OIDC"
    )


def test_release_workflow_uses_pypa_publish_with_attestations() -> None:
    text = RELEASE_WORKFLOW.read_text()
    # The workflow must reference pypa/gh-action-pypi-publish — either by
    # the floating ``release/v1`` tag or pinned to a commit SHA with the
    # tag preserved as a trailing comment (the supply-chain hardened form).
    floating = "pypa/gh-action-pypi-publish@release/v1"
    sha_pinned = "pypa/gh-action-pypi-publish@" in text and (
        "# release/v1" in text or "#release/v1" in text
    )
    assert floating in text or sha_pinned, (
        "release.yml must use pypa/gh-action-pypi-publish "
        "(release/v1 or SHA-pinned with a release/v1 comment)"
    )
    assert "attestations: true" in text, (
        "release.yml must enable PEP 740 attestations: attestations: true"
    )


def test_release_workflow_has_no_static_pypi_token() -> None:
    text = RELEASE_WORKFLOW.read_text()
    forbidden = ("PYPI_TOKEN", "PYPI_API_TOKEN", "password:")
    for needle in forbidden:
        assert needle not in text, (
            f"release.yml must not reference {needle!r} — Trusted Publishers OIDC only"
        )


def test_release_workflow_generates_sbom() -> None:
    text = RELEASE_WORKFLOW.read_text()
    assert "cyclonedx" in text.lower(), (
        "release.yml must generate a CycloneDX SBOM"
    )


# ---------------------------------------------------------------------------
# requirements-diagnose.txt
# ---------------------------------------------------------------------------


def test_requirements_diagnose_exists_and_nonempty() -> None:
    assert REQUIREMENTS_DIAGNOSE.is_file(), "requirements-diagnose.txt must exist"
    assert REQUIREMENTS_DIAGNOSE.stat().st_size > 0


def test_requirements_diagnose_contains_hash_pins() -> None:
    text = REQUIREMENTS_DIAGNOSE.read_text()
    assert "--hash=sha256:" in text, (
        "requirements-diagnose.txt must contain --hash=sha256: pins"
    )
    # Every non-comment, non-blank line group should have at least one hash;
    # cheap sanity check: many hash lines.
    hash_count = text.count("--hash=sha256:")
    assert hash_count >= 50, (
        f"Expected many hash pins (>=50), saw {hash_count}"
    )


@pytest.mark.parametrize("dep", ["jinja2", "langgraph", "langchain-core"])
def test_requirements_diagnose_pins_load_bearing_deps(dep: str) -> None:
    text = REQUIREMENTS_DIAGNOSE.read_text()
    # Each dep should appear with == version pin somewhere.
    assert f"\n{dep}==" in text or text.startswith(f"{dep}=="), (
        f"requirements-diagnose.txt must pin {dep}=="
    )


def test_requirements_diagnose_excludes_self() -> None:
    """The file is generated with --no-emit-project; agent-coherence itself
    should not appear as a pinned line."""
    text = REQUIREMENTS_DIAGNOSE.read_text()
    assert "agent-coherence==" not in text, (
        "requirements-diagnose.txt must not include the project itself "
        "(generated with --no-emit-project)"
    )


# ---------------------------------------------------------------------------
# SECURITY.md
# ---------------------------------------------------------------------------


def test_security_md_exists() -> None:
    assert SECURITY_MD.is_file(), "docs/security.md must exist"


@pytest.mark.parametrize(
    "kill_switch",
    ["DO_NOT_TRACK", "DISABLE_TELEMETRY", "CCS_DIAGNOSE_NO_TELEMETRY"],
)
def test_security_md_documents_kill_switches(kill_switch: str) -> None:
    text = SECURITY_MD.read_text()
    assert kill_switch in text, (
        f"docs/security.md must document {kill_switch} env-var kill switch"
    )


# The pre-release verification gate (``ccs-check-release`` and its manual
# checklist) is intentionally maintainer-only and lives in a local-only file
# at the repo root. It is not asserted here.


def test_security_md_documents_canonical_install() -> None:
    text = SECURITY_MD.read_text()
    assert "https://pypi.org/simple/" in text, (
        "docs/security.md must document the canonical pypi.org install command"
    )
    assert "agent-coherence[diagnose]" in text


def test_security_md_documents_attestation_verification() -> None:
    text = SECURITY_MD.read_text()
    assert "PEP 740" in text
    assert "pypi-attestations" in text or "gh attestation" in text


def test_security_md_documents_hash_pinned_install() -> None:
    text = SECURITY_MD.read_text()
    assert "--require-hashes" in text
    assert "requirements-diagnose.txt" in text


def test_security_md_threat_model_present() -> None:
    text = SECURITY_MD.read_text()
    assert "threat model" in text.lower()
    # Spec-required threat-model rows.
    for needle in (
        "Trusted Publishers",
        "Typosquat",
        "Dependency-confusion",
        "deepeval",
    ):
        assert needle in text, (
            f"docs/security.md threat model must mention {needle!r}"
        )


# ---------------------------------------------------------------------------
# uv.lock
# ---------------------------------------------------------------------------


def test_uv_lock_committed() -> None:
    assert UV_LOCK.is_file(), "uv.lock must be committed at repo root"
    # uv.lock for this project is multi-thousand lines.
    line_count = sum(1 for _ in UV_LOCK.open())
    assert line_count > 100, f"uv.lock looks truncated ({line_count} lines)"


# ---------------------------------------------------------------------------
# README.md security pointer
# ---------------------------------------------------------------------------


def test_readme_links_to_security_doc() -> None:
    """The README is the first artifact a visitor reads. It must surface a
    discoverable pointer to docs/security.md so the trust contract,
    kill switches, and threat model are one click away. We do not
    require a specific heading shape — the pointer may live inline in
    a TOC, a dedicated subsection, or anywhere else in the README.
    """
    text = README.read_text()
    assert "docs/security.md" in text, (
        "README.md must link to docs/security.md so end users can find "
        "the public trust contract"
    )
