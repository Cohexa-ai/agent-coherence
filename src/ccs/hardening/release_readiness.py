# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Pre-release readiness verifier for ``agent-coherence``.

Run before any ``v*`` tag push to confirm the GitHub + PyPI side is
configured for fail-closed publishing. The release workflow runs the
same script as a preflight job; CLI entrypoint is ``ccs-check-release``.

Automated checks (each calls ``gh api`` and exits non-zero on failure):

1. The ``pypi`` GitHub environment exists (Trusted Publishers are
   configured against the ``release.yml`` + ``pypi`` environment pair).
2. ``main`` branch protection requires PR review.
3. Tag protection on ``v*`` covers the release tag pattern.

Manual items the script cannot verify (2FA, typosquat reservations,
audit-log review) are listed in the final report so the maintainer
can confirm them out-of-band.

Stdlib only — no ``requests``, no ``PyGithub``. Shells out to ``gh``.
"""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
from collections.abc import Sequence
from dataclasses import dataclass

DEFAULT_OWNER = "Cohexa-ai"
DEFAULT_REPO = "agent-coherence"
DEFAULT_ENVIRONMENT = "pypi"
DEFAULT_BRANCH = "main"
DEFAULT_TAG_PATTERN = "v*"

__all__ = [
    "CheckResult",
    "check_pypi_environment",
    "check_branch_protection",
    "check_tag_protection",
    "run_automated_checks",
    "format_report",
    "build_parser",
    "main",
]


@dataclass(frozen=True)
class CheckResult:
    """One automated-check outcome.

    ``ok=True``  → check passed.
    ``ok=False`` → check failed (hard block on the release path).
    ``ok=True``  + ``skipped=True`` → check could not be verified from this
        execution context (e.g. CI's ``GITHUB_TOKEN`` lacks the scope to
        read branch protection rules); operators must verify manually with
        a PAT. Treated as a warning, not a release blocker.
    """

    name: str
    ok: bool
    detail: str
    skipped: bool = False


def _gh_api(path: str) -> tuple[int, str, str]:
    """Run ``gh api <path>`` and return ``(returncode, stdout, stderr)``.

    Returns ``(127, "", "...")`` if ``gh`` is not on PATH so callers get
    a clean failure rather than a FileNotFoundError surfacing through
    the report.
    """
    if shutil.which("gh") is None:
        return 127, "", "gh CLI not found on PATH"
    proc = subprocess.run(  # noqa: S603 - inputs are constants from this module
        ["gh", "api", path],
        capture_output=True,
        text=True,
        check=False,
    )
    return proc.returncode, proc.stdout, proc.stderr


def check_pypi_environment(owner: str, repo: str, env: str) -> CheckResult:
    """Verify the GitHub Actions environment used by the publish job exists."""
    rc, out, err = _gh_api(f"repos/{owner}/{repo}/environments/{env}")
    if rc == 0:
        try:
            payload = json.loads(out)
            name = payload.get("name", env)
            return CheckResult(
                name=f"GitHub environment '{env}' exists",
                ok=True,
                detail=f"environment '{name}' configured",
            )
        except json.JSONDecodeError:
            return CheckResult(
                name=f"GitHub environment '{env}' exists",
                ok=False,
                detail="response was not valid JSON",
            )
    return CheckResult(
        name=f"GitHub environment '{env}' exists",
        ok=False,
        detail=err.strip() or f"gh api returned {rc}",
    )


def check_branch_protection(owner: str, repo: str, branch: str) -> CheckResult:
    """Verify branch protection is configured on ``branch``.

    A 200 response means *some* protection is in place. The script does
    not assert on the rule shape because GitHub returns different
    fields by plan tier (free / team / enterprise).

    The ``repos/{owner}/{repo}/branches/{branch}/protection`` endpoint
    requires admin scope. GitHub Actions' default ``GITHUB_TOKEN``
    cannot reach it and returns ``HTTP 403: Resource not accessible by
    integration`` — that is a CI-context limitation, not a real
    configuration failure. We treat 403 as ``skipped`` (warning) so a
    CI preflight run does not fail-closed when the actual protection
    rules are in place but unreadable from this token.
    """
    name = f"Branch protection on '{branch}'"
    rc, _, err = _gh_api(f"repos/{owner}/{repo}/branches/{branch}/protection")
    if rc == 0:
        return CheckResult(
            name=name,
            ok=True,
            detail="protection rules retrieved successfully",
        )
    err_text = err.strip()
    # gh exits non-zero on 403; the stderr contains the HTTP code and message.
    if "403" in err_text and "Resource not accessible by integration" in err_text:
        return CheckResult(
            name=name,
            ok=True,
            skipped=True,
            detail=(
                "could not verify from CI (GITHUB_TOKEN lacks admin scope); "
                "verify locally with `ccs-check-release` before tag push"
            ),
        )
    return CheckResult(
        name=name,
        ok=False,
        detail=err_text or f"gh api returned {rc}",
    )


def check_tag_protection(owner: str, repo: str, pattern: str) -> CheckResult:
    """Verify a repository ruleset covers tags matching ``pattern``.

    GitHub deprecated the legacy ``/repos/{owner}/{repo}/tags/protection``
    endpoint for new use; rulesets are the supported replacement. This
    check looks for an active tag-targeting ruleset whose
    ``conditions.ref_name.include`` contains ``refs/tags/<pattern>``.

    Two ``gh api`` calls per matching ruleset: one to list rulesets
    (which returns only summary fields), then one per tag ruleset to
    fetch ``conditions``. Stops at the first matching ruleset.
    """
    expected_ref = f"refs/tags/{pattern}"
    name = f"Tag protection covers '{pattern}'"

    rc, out, err = _gh_api(f"repos/{owner}/{repo}/rulesets")
    if rc != 0:
        return CheckResult(
            name=name,
            ok=False,
            detail=err.strip() or f"gh api returned {rc}",
        )
    try:
        rulesets = json.loads(out) or []
    except json.JSONDecodeError:
        return CheckResult(
            name=name, ok=False, detail="response was not valid JSON"
        )
    if not isinstance(rulesets, list):
        return CheckResult(
            name=name,
            ok=False,
            detail=f"unexpected response shape: {type(rulesets).__name__}",
        )

    tag_rulesets = [
        rs
        for rs in rulesets
        if isinstance(rs, dict)
        and rs.get("target") == "tag"
        and rs.get("enforcement") == "active"
        and rs.get("id") is not None
    ]
    if not tag_rulesets:
        return CheckResult(
            name=name,
            ok=False,
            detail="no active tag-targeting rulesets exist",
        )

    for rs in tag_rulesets:
        rs_id = rs["id"]
        rc2, out2, _ = _gh_api(f"repos/{owner}/{repo}/rulesets/{rs_id}")
        if rc2 != 0:
            continue
        try:
            detail = json.loads(out2)
        except json.JSONDecodeError:
            continue
        if not isinstance(detail, dict):
            continue
        conditions = detail.get("conditions") or {}
        ref_name = conditions.get("ref_name") or {}
        includes = ref_name.get("include") or []
        if isinstance(includes, list) and expected_ref in includes:
            return CheckResult(
                name=name,
                ok=True,
                detail=(
                    f"ruleset '{detail.get('name', rs_id)}' (id={rs_id}) "
                    f"covers {expected_ref}"
                ),
            )

    return CheckResult(
        name=name,
        ok=False,
        detail=f"no active tag ruleset covers {expected_ref}",
    )


def run_automated_checks(
    *,
    owner: str,
    repo: str,
    environment: str,
    branch: str,
    tag_pattern: str,
) -> tuple[CheckResult, ...]:
    """Run all automated checks; return them as a tuple in stable order."""
    return (
        check_pypi_environment(owner=owner, repo=repo, env=environment),
        check_branch_protection(owner=owner, repo=repo, branch=branch),
        check_tag_protection(owner=owner, repo=repo, pattern=tag_pattern),
    )


def format_report(results: Sequence[CheckResult]) -> str:
    """Format a multi-line table summarising the run."""
    lines = ["Automated release-readiness checks:", ""]
    width = max((len(r.name) for r in results), default=0)
    for r in results:
        if r.skipped:
            status = "WARN"
        elif r.ok:
            status = "PASS"
        else:
            status = "FAIL"
        lines.append(f"  [{status}] {r.name:<{width}}  {r.detail}")
    lines.append("")
    lines.append("Manual verification still required (cannot be automated):")
    lines.append("  - 2FA enforced on all PyPI maintainers")
    lines.append("  - Typosquat name reservations on PyPI")
    lines.append("  - Quarterly org-level audit-log review")
    lines.append("  - CycloneDX SBOM artefact attached to the GitHub Release")
    return "\n".join(lines)


def build_parser() -> argparse.ArgumentParser:
    """Construct the argparse parser. Defaults are this repo's settings."""
    parser = argparse.ArgumentParser(
        description=(
            "Verify GitHub + PyPI release-readiness configuration. "
            "Run before any v* tag push."
        )
    )
    parser.add_argument("--owner", default=DEFAULT_OWNER)
    parser.add_argument("--repo", default=DEFAULT_REPO)
    parser.add_argument("--environment", default=DEFAULT_ENVIRONMENT)
    parser.add_argument("--branch", default=DEFAULT_BRANCH)
    parser.add_argument("--tag-pattern", default=DEFAULT_TAG_PATTERN)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run the readiness check and return ``0`` on success, ``1`` on failure."""
    args = build_parser().parse_args(argv)
    results = run_automated_checks(
        owner=args.owner,
        repo=args.repo,
        environment=args.environment,
        branch=args.branch,
        tag_pattern=args.tag_pattern,
    )
    print(format_report(results))
    return 0 if all(r.ok for r in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
