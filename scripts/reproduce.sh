#!/usr/bin/env bash
# reproduce.sh — Reproduce CCS simulation benchmark artifacts.
# Requirements: Python 3.11+

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

echo "==> Installing dependencies"
python -m pip install -e ".[dev]" --quiet

echo "==> Step 5 benchmark suite"
python tools/run_step5_benchmarks.py

echo "==> S-scaling sweep"
python tools/run_step_scaling.py --output benchmarks/results/step_scaling.json

echo "==> Artifact-size scaling sweep"
python tools/run_artifact_scaling.py --output benchmarks/results/artifact_scaling.json

echo "==> Canonical benchmark suite"
python tools/run_canonical_benchmarks.py

echo "==> Verifying results against committed baseline"
python tools/verify_baseline.py \
  --baseline benchmarks/results/step5/SUMMARY.md \
  --tolerance 0.005

echo "==> All reproduction steps complete."
