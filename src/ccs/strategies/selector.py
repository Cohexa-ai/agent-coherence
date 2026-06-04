# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""Strategy name selection and factory helpers."""

from __future__ import annotations

from typing import Mapping

from .access_count import AccessCountStrategy
from .base import SyncStrategy
from .blind_cache import BlindCacheStrategy
from .broadcast import BroadcastStrategy
from .eager import EagerStrategy
from .lazy import LazyStrategy
from .lease import LeaseStrategy


def select_strategy_name_for_role(
    role: str,
    *,
    role_overrides: Mapping[str, str] | None = None,
    default: str = "lazy",
) -> str:
    """Return strategy name selected for role with optional per-role override."""
    if role_overrides and role in role_overrides:
        return role_overrides[role]
    return default


def build_strategy(
    strategy_name: str,
    *,
    lease_ttl_ticks: int = 300,
    access_count_max_accesses: int = 100,
) -> SyncStrategy:
    """Create strategy instance from normalized strategy name."""
    normalized = strategy_name.strip().lower()
    if normalized == "blind":
        return BlindCacheStrategy()
    if normalized == "broadcast":
        return BroadcastStrategy()
    if normalized == "eager":
        return EagerStrategy()
    if normalized == "lazy":
        return LazyStrategy()
    if normalized == "lease":
        return LeaseStrategy(ttl_ticks=lease_ttl_ticks)
    if normalized in {"access_count", "access-count", "accesscount"}:
        return AccessCountStrategy(max_accesses=access_count_max_accesses)
    raise ValueError(f"unknown strategy '{strategy_name}'")
