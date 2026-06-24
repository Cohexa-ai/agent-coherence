#!/usr/bin/env bash
# Genuine cross-container demo (both scenarios): two containers (separate network
# namespaces) on a private-range bridge, one centralized coordinator. Exits with
# the client's code (0 = stale write denied then recovered).
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
cd "$HERE"
# Clean slate: drop any coordstate volume left by an interrupted prior run, so
# the client never reads a stale server.pid / port from a dead coordinator.
docker compose down -v >/dev/null 2>&1 || true
docker compose up --build --abort-on-container-exit --exit-code-from client
code=$?
docker compose down -v >/dev/null 2>&1 || true
exit "$code"
