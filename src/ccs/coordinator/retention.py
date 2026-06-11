# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""Bounded version-retention policy and the single GC seam (plan item N v1).

This module owns the *policy* (a frozen :class:`RetentionPolicy` modelled on
``CrashRecoveryConfig``) and the *one* pure GC decision function
(:func:`collectible_versions`) that every retention capture point routes
through. Both registries (in-memory ``ArtifactRegistry`` and the durable
``SqliteArtifactRegistry`` in Unit 3) import these — same ``coordinator``
(application) layer, so there is no duplication to pin equal.

Layer discipline (R-arch): ``coordinator`` may import ``core`` only. This module
imports nothing above ``core`` — it depends on no other ccs module at all — so
``tools/check_architecture.py`` stays green.

Requirement trace:

- **R1** — bounded retention (K versions / age T), amortized GC, no unbounded
  write-path pause. K and T are the two axes here; a ``None`` axis disables it.
- **R4** — GC is invisible to the protocol, the current version is never
  collectible, and the seam is exemption-extensible (the SB-17 pin-hold seam:
  :func:`collectible_versions` accepts an ``exemptions`` set today and honors it,
  with no other v1 use).
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass

__all__ = ["RetentionPolicy", "collectible_versions"]


@dataclass(frozen=True)
class RetentionPolicy:
    """Per-registry bound on how many / how old retained versions are kept.

    Modelled on ``CrashRecoveryConfig`` (``service.py``): a frozen dataclass that
    validates in ``__post_init__`` and is otherwise an inert value object. The
    registry that owns it decides *when* GC runs (inline at each capture point);
    this object only describes the *bound*.

    Retention is active iff the registry was constructed with
    ``retain_versions=True``. A registry with ``retain_versions=True`` and
    ``retention_policy=None`` keeps **today's unbounded** semantics (no GC at
    all) — that is the back-compat contract for the v0.5 audit auto-wiring.
    A policy is therefore an explicit opt-in to *bounded* retention.

    The two axes are independent and either may be disabled with ``None``:

    Attributes:
        max_versions: **K axis.** Keep at most this many of the most-recent
            versions, *including the current version's row* (so the effective
            floor is the current row, which is never collectible regardless of
            K). Versions older than the K newest are collectible. ``None``
            disables the K axis (no count bound). Must be ``>= 1`` when set;
            ``max_versions < 1`` raises :class:`ValueError` (a zero/negative
            bound would try to collect the current row — caller misuse).
        max_age_seconds: **T axis.** A version whose capture timestamp is older
            than ``now - max_age_seconds`` is collectible. ``None`` disables the
            T axis (no age bound). Must be ``> 0`` when set; ``max_age_seconds
            <= 0`` raises :class:`ValueError`. **T is WALL-CLOCK seconds**
            (``time.time()``), matching the sqlite ``captured_at`` / artifact
            ``updated_at`` columns (also ``time.time()``); a backward clock jump
            can misfire logical expiry — documented, accepted (monotonic clocks
            do not survive a process restart, so they cannot back a durable
            store). T is the secrets-rotation knob: a lowered T expires rotated
            secrets from retained history sooner (logical-at-read; physical
            deletion piggybacks on the next capture).
    """

    max_versions: int | None = 16
    max_age_seconds: float | None = None

    def __post_init__(self) -> None:
        """Reject nonsensical bounds at construction (house style: fail fast).

        A ``max_versions`` below 1 would mark the current row collectible (the
        current row is the floor); a non-positive ``max_age_seconds`` would
        expire everything including the just-captured current version. Both are
        caller misuse, so both raise rather than silently clamping.
        """
        if self.max_versions is not None and self.max_versions < 1:
            raise ValueError(
                f"RetentionPolicy.max_versions must be >= 1 when set "
                f"(got {self.max_versions}); the current version's row is "
                f"never collectible, so a K below 1 is meaningless. Use "
                f"max_versions=None to disable the count bound."
            )
        if self.max_age_seconds is not None and self.max_age_seconds <= 0:
            raise ValueError(
                f"RetentionPolicy.max_age_seconds must be > 0 when set "
                f"(got {self.max_age_seconds}); a non-positive age would "
                f"expire the just-captured version. Use max_age_seconds=None "
                f"to disable the age bound."
            )


def collectible_versions(
    versions: Iterable[int] | Mapping[int, float],
    current_version: int,
    policy: RetentionPolicy,
    now: float,
    exemptions: Iterable[int] = (),
) -> set[int]:
    """Return the set of versions that GC may drop under ``policy`` (R1, R4).

    The **single** GC seam: every retention capture point (in-memory and, in
    Unit 3, sqlite) computes the drop set with this one pure, fully-unit-testable
    function so the eviction rule lives in exactly one place.

    Rules:

    - The **current version is never collectible**, regardless of policy (R4).
    - Versions in **``exemptions`` are never collectible** (R4 — the SB-17
      pin-hold seam: a future pinned-snapshot set plugs in here, no redesign).
    - **K axis** (``policy.max_versions``): keep at most K of the most-recent
      versions *including the current one*; mark the oldest beyond K collectible.
      ``None`` disables the axis.
    - **T axis** (``policy.max_age_seconds``): a version whose capture timestamp
      is older than ``now - max_age_seconds`` is collectible. ``None`` disables
      the axis. The T axis needs per-version timestamps, so ``versions`` must be
      a mapping ``{version: captured_at}`` for T to apply; when ``versions`` is a
      bare iterable (no timestamps) the T axis is a no-op for that call.

    Args:
        versions: The retained versions. Either a bare iterable of version ints
            (K axis only) or a ``{version: captured_at}`` mapping (enables T).
        current_version: The artifact's current version — always exempt.
        policy: The bound to apply.
        now: Wall-clock reference time (``time.time()``) for the T axis.
        exemptions: Versions the caller pins (never collected). Honored now;
            v1 callers pass the default empty set.

    Returns:
        The set of versions to drop. Empty when both axes are ``None`` (the
        unbounded case never reaches here — the registry only calls this with a
        policy set — but the function is total regardless).
    """
    timestamps: Mapping[int, float] | None = (
        versions if isinstance(versions, Mapping) else None
    )
    all_versions = set(versions)
    exempt = set(exemptions)
    exempt.add(current_version)

    collectible: set[int] = set()

    # K axis: keep the K most-recent versions (descending), drop the older tail.
    # The current version is exempt below, so a survivor that is "kept by K" but
    # also exempt is simply not collected; the floor is preserved by the final
    # exempt-difference, not by special-casing the current version inside the
    # ranking.
    if policy.max_versions is not None:
        kept_by_k = set(sorted(all_versions, reverse=True)[: policy.max_versions])
        collectible |= all_versions - kept_by_k

    # T axis: logical expiry by capture timestamp. Requires per-version
    # timestamps; a bare-iterable call (no timestamps) cannot age anything.
    if policy.max_age_seconds is not None and timestamps is not None:
        cutoff = now - policy.max_age_seconds
        collectible |= {
            v for v, captured_at in timestamps.items() if captured_at < cutoff
        }

    # Exemptions (incl. the current version) always survive — applied last so
    # neither axis can ever mark the floor or a pinned version collectible (R4).
    return collectible - exempt
