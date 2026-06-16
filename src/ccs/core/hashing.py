# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Content hashing for audit and cross-validation."""

from __future__ import annotations

import hashlib


def compute_content_hash(content: str) -> str:
    """Return SHA-256 hex digest of UTF-8-encoded content."""
    return hashlib.sha256(content.encode("utf-8")).hexdigest()
