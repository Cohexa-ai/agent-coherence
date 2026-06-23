#!/usr/bin/env bash
# Genuine cross-container slice-1/slice-2 demo: two containers (separate network
# namespaces) on a private-range bridge, one centralized coordinator. Exits with
# the client's code (0 = stale write denied then recovered).
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
cd "$HERE"
docker compose up --build --abort-on-container-exit --exit-code-from client
code=$?
docker compose down -v >/dev/null 2>&1 || true
exit "$code"
