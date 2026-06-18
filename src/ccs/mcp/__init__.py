# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""MCP-C — the ``stale-write-guard-fs`` coherence MCP server (interface layer).

A thin stdio MCP binding over the already-shipped ``CoherentVolume`` appliance.
Imports are lazy via ``__getattr__`` so ``import ccs.mcp`` stays cheap and does
NOT pull in the optional ``mcp`` SDK until a symbol that needs it is accessed
(the SDK lives behind the ``mcp`` extra; the deny mapper imports ``mcp.types``).
"""

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING, Any

__all__ = ["coordinator_unavailable_result", "deny_result"]

if TYPE_CHECKING:  # static analysers / IDEs resolve the real symbols
    from ccs.mcp.deny import coordinator_unavailable_result, deny_result


def __getattr__(name: str) -> Any:
    """Lazily resolve the public API from submodules (PEP 562)."""
    if name in __all__:
        return getattr(importlib.import_module("ccs.mcp.deny"), name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
