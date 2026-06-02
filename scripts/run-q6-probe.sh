#!/usr/bin/env bash
# Run the Q6 Conversations consistency probe with credentials injected by
# 1Password. Keys are resolved into THIS subprocess only — they never touch
# disk, shell history, or any agent's context.
#
# One-time setup:
#   1. Install the 1Password CLI:   brew install 1password-cli
#   2. 1Password app -> Settings -> Developer -> enable "Integrate with 1Password CLI"
#   3. cp .env.op.example .env.op   and edit the op:// references to match your items
#
# Usage:
#   scripts/run-q6-probe.sh [--vendor openai|mistral|both] [--trials N] [--out PATH]
#
# Overrides:
#   COHERENCE_OP_ENV_FILE   secret-reference env file (default: .env.op)
#   PYTHON                  interpreter (default: .venv/bin/python)
#
# This wrapper is the general op-run pattern applied to the probe; replicate the
# `op run --env-file=... -- <command>` shape for any other live-API command.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

env_file="${COHERENCE_OP_ENV_FILE:-.env.op}"
python_bin="${PYTHON:-.venv/bin/python}"

if ! command -v op >/dev/null 2>&1; then
  echo "1Password CLI 'op' not found. Install it: brew install 1password-cli" >&2
  echo "Then enable the desktop integration (Settings -> Developer)." >&2
  exit 127
fi

if [ ! -f "$env_file" ]; then
  echo "Missing $env_file. Copy the template and fill in your op:// references:" >&2
  echo "  cp .env.op.example .env.op" >&2
  exit 1
fi

if [ ! -x "$python_bin" ]; then
  echo "Interpreter '$python_bin' not found. Set PYTHON=... or create the project venv." >&2
  exit 1
fi

exec op run --env-file="$env_file" -- "$python_bin" -m examples.conversations_stale_read.probe "$@"
