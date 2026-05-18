#!/usr/bin/env bash
# Configure GitHub-side release protections for agent-coherence.
#
# Solo-dev tuned: keeps an admin escape hatch and does not require
# co-maintainer approvals. The human-in-the-loop is provided by the
# tag-protection rule + the `pypi` environment reviewer gate.
#
# Requires: gh CLI, authenticated as a repo admin.
# Run from anywhere; uses absolute API paths.

set -euo pipefail

OWNER="hipvlady"
REPO="agent-coherence"

echo "==> Verifying gh auth..."
gh auth status

USER_LOGIN="$(gh api user --jq .login)"
USER_ID="$(gh api user --jq .id)"
echo "==> Authenticated as ${USER_LOGIN} (id=${USER_ID})"

if [[ "${USER_LOGIN}" != "${OWNER}" ]]; then
  echo "WARN: authenticated user (${USER_LOGIN}) is not the repo owner (${OWNER})."
  echo "      Continuing anyway — proceed only if this account has admin rights on ${OWNER}/${REPO}."
fi

# ---------------------------------------------------------------------------
# 1. Branch protection on main (solo-dev tuned)
#
#    - required_approving_review_count: 0     => PR flow encouraged, but you
#                                                can self-merge without a
#                                                second human approval.
#    - enforce_admins: false                  => admin escape hatch — you can
#                                                bypass when you need to.
#    - required_linear_history: true          => keeps `main` clean.
#    - dismiss_stale_reviews / no-force-push  => sane defaults.
#
#    The readiness check only verifies that *some* protection exists, so this
#    passes ccs-check-release while staying convenient for solo work.
# ---------------------------------------------------------------------------
echo "==> [1/3] Configuring branch protection on main..."
gh api --method PUT "repos/${OWNER}/${REPO}/branches/main/protection" \
  --input - <<'JSON'
{
  "required_status_checks": null,
  "enforce_admins": false,
  "required_pull_request_reviews": {
    "required_approving_review_count": 0,
    "dismiss_stale_reviews": true,
    "require_code_owner_reviews": false
  },
  "restrictions": null,
  "required_linear_history": true,
  "allow_force_pushes": false,
  "allow_deletions": false,
  "required_conversation_resolution": false,
  "lock_branch": false,
  "allow_fork_syncing": false
}
JSON
echo "    OK: branch protection applied to main."

# ---------------------------------------------------------------------------
# 2. Tag protection on v* (via repository ruleset)
#
#    GitHub deprecated the legacy /repos/{owner}/{repo}/tags/protection
#    endpoint for new use; rulesets are the supported replacement.
#
#    Bypass actor is the Admin repository role (actor_id=5,
#    actor_type=RepositoryRole) so you (admin) can still cut releases.
#    Anyone with mere write access cannot create/update/delete v* tags.
#    This is the actual gate against an attacker with push access
#    cutting a rogue release.
# ---------------------------------------------------------------------------
echo "==> [2/3] Configuring tag protection on v* (rulesets API)..."
RULESET_NAME="Protect v* release tags"

EXISTING_ID="$(gh api "repos/${OWNER}/${REPO}/rulesets" \
  --jq ".[] | select(.target == \"tag\" and .name == \"${RULESET_NAME}\") | .id" \
  2>/dev/null || true)"

if [[ -n "${EXISTING_ID}" ]]; then
  echo "    OK: tag ruleset '${RULESET_NAME}' already exists (id=${EXISTING_ID})."
else
  gh api --method POST "repos/${OWNER}/${REPO}/rulesets" \
    --input - <<JSON
{
  "name": "${RULESET_NAME}",
  "target": "tag",
  "enforcement": "active",
  "conditions": {
    "ref_name": {
      "include": ["refs/tags/v*"],
      "exclude": []
    }
  },
  "rules": [
    {"type": "creation"},
    {"type": "deletion"},
    {"type": "update"}
  ],
  "bypass_actors": [
    {
      "actor_id": 5,
      "actor_type": "RepositoryRole",
      "bypass_mode": "always"
    }
  ]
}
JSON
  echo "    OK: tag ruleset '${RULESET_NAME}' created (admins can bypass)."
fi

# ---------------------------------------------------------------------------
# 3. `pypi` environment with required reviewer + v* tag policy
#
#    This is the real human-in-the-loop:
#      - Publish job is `environment: pypi` (release.yml).
#      - Required reviewer = you. Workflow halts and pings you to click
#        Approve before the OIDC publish runs.
#      - deployment_branch_policy with a `v*` tag-only rule => the
#        environment can only be invoked from a v* tag push, not from
#        main or any branch.
# ---------------------------------------------------------------------------
echo "==> [3/3] Configuring pypi environment..."
gh api --method PUT "repos/${OWNER}/${REPO}/environments/pypi" \
  --input - <<JSON
{
  "wait_timer": 0,
  "prevent_self_review": false,
  "reviewers": [{"type": "User", "id": ${USER_ID}}],
  "deployment_branch_policy": {
    "protected_branches": false,
    "custom_branch_policies": true
  }
}
JSON
echo "    OK: pypi environment created with ${USER_LOGIN} as required reviewer."

# Add the v*-tag-only deployment policy. Idempotent: GitHub 422s on dup.
echo "==> Adding v* tag deployment policy to pypi env..."
if gh api --method POST \
     "repos/${OWNER}/${REPO}/environments/pypi/deployment-branch-policies" \
     -f name='v*' -f type='tag' 2>/tmp/envpol.err; then
  echo "    OK: v* tag policy added."
else
  if grep -qiE 'already exists|same name' /tmp/envpol.err; then
    echo "    OK: v* tag policy already present."
  else
    echo "    ERROR adding deployment policy:"
    cat /tmp/envpol.err
    exit 1
  fi
fi
rm -f /tmp/envpol.err

echo ""
echo "==> All three automated protections configured."
echo "==> Run: python tools/check_release_readiness.py"
echo "    (or: ccs-check-release)"
echo "    to confirm all three checks now PASS."
