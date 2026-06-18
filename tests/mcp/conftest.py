# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Defensive collection-skip for the stale-write-guard-fs MCP tests.

``ccs.mcp.server`` / ``ccs.mcp.deny`` import the official ``mcp`` SDK, declared
in the ``[mcp]`` optional extra. On a bare ``pip install -e ".[dev]"`` those
modules ImportError at COLLECTION, so pytest would error before any test runs.

CI's Tests job installs ``.[dev,diagnose,mcp]`` so these tests run there; this
guard is the second layer of defense for bare local installs and for CI jobs
that do not install mcp (e.g. the ``protocol_corpus`` job, which still collects
the whole tree before deselecting). Mirrors the ``[diagnose]`` / live-API guards
in the parent ``tests/conftest.py``.
"""

from __future__ import annotations

import importlib.util

collect_ignore_glob: list[str] = []


def _mcp_server_available() -> bool:
    # find_spec on a submodule raises ModuleNotFoundError when an ancestor is
    # absent (mcp missing, OR a partial transitive mcp without mcp.server).
    try:
        return importlib.util.find_spec("mcp.server.fastmcp") is not None
    except ModuleNotFoundError:
        return False


if not _mcp_server_available():
    collect_ignore_glob.append("test_*.py")
