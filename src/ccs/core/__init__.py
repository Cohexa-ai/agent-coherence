# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Domain-level core primitives."""

from ccs.core.granularity import (
    CANONICAL_ARTIFACT_TOKENS,
    DEFAULT_GRANULARITY,
    GRANULARITY_SPECS,
    GranularityLevel,
    GranularitySpec,
)
from ccs.core.substrate import (
    CapabilityDescriptor,
    Tier,
)

__all__ = [
    "CANONICAL_ARTIFACT_TOKENS",
    "CapabilityDescriptor",
    "DEFAULT_GRANULARITY",
    "GRANULARITY_SPECS",
    "GranularityLevel",
    "GranularitySpec",
    "Tier",
]
