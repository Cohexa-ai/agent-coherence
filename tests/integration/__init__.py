# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""Integration tests requiring external resources (live ``claude`` CLI,
hard-gated launch verification harnesses). Marked separately so unit
test runs (pytest -q) skip them by default; CI / pre-launch runs invoke
them via ``pytest -m launch_gate`` or ``pytest tests/integration/``."""
