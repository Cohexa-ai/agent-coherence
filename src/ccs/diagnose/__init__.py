# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""ccs-diagnose — runtime callback + write-pattern classifier for LangGraph graphs.

Diagnostic surface intended to inform Phase 2 recorder design via shared
callback adapter and telemetry schema. Not CCSStore-required; the CLI is
a no-CCSStore observer of a user's existing LangGraph graph.

Optional install:
    pip install agent-coherence[diagnose]

Modules in this package are loaded lazily so that ``import ccs.diagnose``
remains zero-side-effect (no thread spawning, no socket open, no file read)
even if the user has not installed the optional dependencies (jinja2,
langgraph, langchain-core). See Tier 2 plan, Unit 10 trust posture.
"""

from __future__ import annotations

CCS_DIAGNOSE_LOG_SCHEMA_VERSION: str = "ccs.diagnose.v0-preview"
"""Schema version emitted in every diagnose JSONL entry.

The ``-preview`` suffix means submissions tagged with this version are
calibration-corpus only and excluded from the public benchmark.json.
Promoted to ``ccs.diagnose.v1`` after the calibration gate (>=5 real graphs,
>=3 supervisor topologies, message-trim survival, zero unknown ``__``-prefix
surprises) is satisfied.
"""

__all__ = ["CCS_DIAGNOSE_LOG_SCHEMA_VERSION"]
