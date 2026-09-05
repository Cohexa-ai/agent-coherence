# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Capability descriptor and honesty floor for bring-your-own-substrate bindings.

A substrate binding brings coherence to shared mutable state the builder
already runs — a Postgres row, an S3 object, an action backend. Each binding
declares what it can honestly guarantee through a :class:`CapabilityDescriptor`
carrying a :class:`Tier`; the guarantee wording a user sees is DERIVED from the
tier, so a weaker binding can never present itself as enforcement.

This module also holds the never-ship-a-store floor: the coordinator keeps
coherence metadata only — a fixed-width content fingerprint plus an opaque
substrate token — never content, nor anything whose size is proportional to
content (a body, a compressed body, a diff, a sample). The checks are data-in
by design: this module cannot import coordinator state (the layering boundary
puts ``core`` below the application layer), so callers extract the
coordinator-side state — a payload mapping, a dump of the retention table's
rows — and pass it in.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from enum import Enum
from typing import Iterable, Iterator, Mapping
from uuid import UUID


def sha256_hex(data: bytes) -> str:
    """The one content fingerprint the coordinator may hold: a sha-256 hex digest.

    Single source of truth for what ``content_hash`` means, shared by every
    substrate binding — so a future change to the fingerprint (normalization,
    encoding) can never silently diverge one binding from another.
    """
    return hashlib.sha256(data).hexdigest()


class Tier(Enum):
    """Honest guarantee tier a substrate binding declares.

    - ``NATIVE_CAS`` — the substrate offers an atomic compare-and-set (a
      conditional write keyed on a substrate-minted token), so the binding can
      reject a lost update on the version axis.
    - ``DETECT_ONLY`` — the substrate offers no atomic conditional write; the
      binding can only detect a sequential stale-read-then-write after the
      fact, never prevent a concurrent race.
    - ``FORWARD_ONLY`` — the substrate "write" is an action (an RPC-style
      effect such as posting a message), not an object mutation: there is no
      token and nothing to compare, so the only honest offer is freshness of
      the decision inputs before the effect fires.
    """

    NATIVE_CAS = "native_cas"
    DETECT_ONLY = "detect_only"
    FORWARD_ONLY = "forward_only"


class DurabilityRegime(Enum):
    """Honest durability grade a substrate binding declares (guarantee-ladder
    U6 / R-5). A third ADDITIVE axis on the :class:`CapabilityDescriptor` —
    Tier speaks to write arbitration, RestoreTier to bringing state back;
    this axis speaks to what an ACKNOWLEDGED write survives. Values are
    wire-stable constants matched by identity; add, never rename.

    - ``IN_PROCESS`` — acknowledged state lives in process memory only and
      dies with the process (the kit's in-memory fakes; an honest floor,
      never a defect — the declaration is what makes it honest).
    - ``PROCESS_CRASH`` — an acknowledged write's bytes have at least reached
      the OS before the ack, so SIGKILL (user-space destruction) cannot lose
      it; an OS crash or power loss still can. This is the grade the local
      kill-the-primary case can actually verify.
    - ``OS_CRASH`` — fsync-grade: the acknowledged write survives kernel
      panic/power loss. NO local case can discriminate this from
      ``PROCESS_CRASH`` (a SIGKILL leaves the page cache intact), so a
      binding claiming it must point at the configuration facts that carry
      the claim; verification is a declared exemption until a
      crash-simulating VFS closes it.
    """

    IN_PROCESS = "in-process"
    PROCESS_CRASH = "process-crash"
    OS_CRASH = "os-crash"


class RestoreTier(Enum):
    """Honest per-member RESTORE capability a Workspace-Versioning member declares.

    A second, ADDITIVE axis on the :class:`CapabilityDescriptor` — it extends the
    shipped :class:`Tier` vocabulary, it never replaces it (Tier speaks to write
    arbitration; RestoreTier speaks to whether a captured state can be brought
    back). Values are wire-stable constants matched by identity; add, never
    rename.

    - ``RESTORABLE`` — the substrate retains the captured version under a pin
      (e.g. an S3 versioned bucket with a legal hold on the pinned version), so
      a later restore read is guaranteed servable.
    - ``RESTORABLE_UNPINNED`` — the substrate retains history (versioning is
      on) but the captured version could NOT be pinned, so a lifecycle rule or
      an operator delete may silently expire it before restore. Honest
      downgrade, never silently presented as ``RESTORABLE``.
    - ``FORWARD_ONLY`` — the substrate keeps no history for this member (an
      unversioned bucket, an action/effect): the checkpoint can only describe
      it, a restore must skip it and say so.
    """

    RESTORABLE = "restorable"
    RESTORABLE_UNPINNED = "restorable-unpinned"
    FORWARD_ONLY = "forward_only"


class ArbitrationTier(Enum):
    """Who arbitrates a FOREIGN writer racing a restore leg on this member.

    - ``NATIVE_CAS`` — the substrate itself arbitrates: the restore leg is a
      conditional write keyed on a substrate-minted token, so a foreign writer
      that moved the token wins and the leg surfaces a typed conflict.
    - ``NO_ARBITER`` — no substrate arbitration exists (a plain file member):
      the binding can only DETECT a foreign edit adapter-locally; the outcome
      must be labeled detection, never presented as substrate arbitration.
    """

    NATIVE_CAS = "native-cas"
    NO_ARBITER = "no-arbiter"


def derive_restore_tier(*, versioned: bool, pinnable: bool) -> RestoreTier:
    """The honest :class:`RestoreTier` for one member, from what capture observed.

    ONE derivation rule shared by every binding so capture-time tiering cannot
    drift: no versioning → ``FORWARD_ONLY`` (never ``restorable``, the
    fail-closed floor); versioning without a pin → ``RESTORABLE_UNPINNED``;
    versioning with an established pin → ``RESTORABLE``.
    """
    if not versioned:
        return RestoreTier.FORWARD_ONLY
    return RestoreTier.RESTORABLE if pinnable else RestoreTier.RESTORABLE_UNPINNED


# Guarantee wording is a closed, tier-keyed table rather than free text so the
# honesty floor is enforceable: the non-native tiers must never carry
# enforcement or compare-and-set claims, and a per-binding rewording cannot
# drift past that line.
_GUARANTEE_TEXT_BY_TIER: dict[Tier, str] = {
    Tier.NATIVE_CAS: (
        "enforces no-lost-update on the version-CAS axis only; a "
        "coordinator-timeout after a durable substrate write is reconverged by "
        "the token-identity reconciliation; single-host"
    ),
    Tier.DETECT_ONLY: (
        "detection only: catches a sequential stale-read→write; "
        "cannot prevent a concurrent race; single-host"
    ),
    Tier.FORWARD_ONLY: (
        "effect ordering only: decision-input freshness via the coordinator's "
        "invalidation and deny-before-act gating on the coherence-managed "
        "inputs a decision is derived from; the effect itself is forward-only "
        "— this layer does not compare-and-swap it, does not undo it, and "
        "does not prevent a duplicate effect"
    ),
}


@dataclass(frozen=True)
class CapabilityDescriptor:
    """Machine-readable capability declaration for one substrate binding.

    - ``tier`` — the binding's honest guarantee class; it alone determines the
      surfaced :attr:`guarantee_text`.
    - ``version_source`` — where the substrate-minted version token comes from
      (a trigger-managed version column, an object ETag). Required for
      ``NATIVE_CAS`` (its whole claim rides on the token), optional for
      ``DETECT_ONLY``, and forbidden for ``FORWARD_ONLY`` — an action has no
      token.
    - ``least_privilege`` — the minimal substrate credential the binding needs,
      stated so an operator can scope access down to it.
    - ``consistency_note`` — the substrate's own consistency caveat, surfaced
      verbatim so the builder sees the substrate's limits, not just ours.

    Workspace-Versioning axes (ADDITIVE — every pre-WV construction is
    unchanged; the defaults claim nothing):

    - ``versioned_pinnable`` — True iff the substrate offers BOTH version
      history and a per-version pin (e.g. an S3 versioned bucket with Object
      Lock legal hold). The capability flag :data:`RestoreTier.RESTORABLE`
      rides on; False claims nothing.
    - ``restore_tier`` — the member's honest restore capability, or ``None``
      when the binding is not (yet) speaking the restore axis. ``RESTORABLE``
      without ``versioned_pinnable`` is refused fail-closed — a descriptor may
      under-claim (a loud downgrade) but never over-claim.
    - ``arbitration_tier`` — who arbitrates a foreign writer racing a restore
      leg, or ``None`` when the binding is not speaking the restore axis.
    """

    tier: Tier
    version_source: str | None = None
    least_privilege: str = ""
    consistency_note: str = ""
    versioned_pinnable: bool = False
    restore_tier: RestoreTier | None = None
    arbitration_tier: ArbitrationTier | None = None
    # Durability axis (guarantee-ladder U6 / R-5, ADDITIVE — the defaults
    # claim nothing): the regime an acknowledged write honestly survives,
    # plus the configuration facts the claim rides on (e.g. "sqlite
    # journal_mode=WAL synchronous=NORMAL"). ``None`` means the binding is
    # not (yet) speaking the axis — the kit's declaration assertion refuses
    # such a descriptor with a typed reason rather than assuming a grade.
    durability_regime: DurabilityRegime | None = None
    durability_facts: str = ""

    def __post_init__(self) -> None:
        if not isinstance(self.tier, Tier):
            valid = ", ".join(tier.value for tier in Tier)
            raise ValueError(f"unknown tier: {self.tier!r} (expected one of: {valid})")
        if self.tier is Tier.FORWARD_ONLY and self.version_source is not None:
            raise ValueError(
                "forward_only forbids a version_source: an action/RPC mints no "
                f"token to compare (got {self.version_source!r})"
            )
        if self.tier is Tier.NATIVE_CAS and self.version_source is None:
            raise ValueError(
                "native_cas requires a version_source: the tier's guarantee is "
                "keyed on the substrate-minted token"
            )
        if self.restore_tier is not None and not isinstance(self.restore_tier, RestoreTier):
            valid = ", ".join(t.value for t in RestoreTier)
            raise ValueError(
                f"unknown restore_tier: {self.restore_tier!r} (expected one of: {valid})"
            )
        if self.arbitration_tier is not None and not isinstance(
            self.arbitration_tier, ArbitrationTier
        ):
            valid = ", ".join(t.value for t in ArbitrationTier)
            raise ValueError(
                f"unknown arbitration_tier: {self.arbitration_tier!r} "
                f"(expected one of: {valid})"
            )
        if self.restore_tier is RestoreTier.RESTORABLE and not self.versioned_pinnable:
            raise ValueError(
                "restore_tier 'restorable' requires versioned_pinnable=True: a "
                "descriptor may under-claim but never over-claim (fail-closed) — "
                "an unpinned member is 'restorable-unpinned' at best"
            )
        if self.durability_regime is not None and not isinstance(
            self.durability_regime, DurabilityRegime
        ):
            valid = ", ".join(t.value for t in DurabilityRegime)
            raise ValueError(
                f"unknown durability_regime: {self.durability_regime!r} "
                f"(expected one of: {valid})"
            )
        if self.durability_facts and self.durability_regime is None:
            raise ValueError(
                "durability_facts requires a declared durability_regime: facts "
                "qualifying no regime are an over-claim vector (fail-closed)"
            )

    @property
    def guarantee_text(self) -> str:
        """The user-facing guarantee wording, derived from the tier only."""
        return _GUARANTEE_TEXT_BY_TIER[self.tier]


# --- never-ship-a-store -----------------------------------------------------

# Field names that carry content or a content-proportional shadow. Their
# presence with ANY value (other than None, the shipped content-free shape) is
# a violation regardless of size — a one-byte body is still a body.
_CONTENT_FIELD_NAMES = frozenset({"content", "body", "compressed_body", "diff", "sample"})
# The one fingerprint the coordinator may hold: a fixed-width sha-256 hex digest.
_SHA256_HEX_RE = re.compile(r"\A[0-9a-f]{64}\Z")
# Opaque tokens and identifiers are short. Longer text is treated as a
# content-proportional shadow: the floor fails closed rather than trusting a
# field name.
_MAX_OPAQUE_TEXT_LEN = 256
# Coherence metadata is a handful of scalars per artifact. A large collection or
# an arbitrary-precision integer is a body wearing another shape (bytes as a list
# of byte-value ints, or as one big int), so both are bounded, not just strings.
_MAX_COLLECTION_LEN = 256
_MAX_INT_BITS = 64
_ALLOWED_SCALARS = (bool, int, float, UUID, type(None))


def never_ship_a_store(state: Mapping[str, object]) -> bool:
    """True iff extracted coordinator-side state holds coherence metadata only.

    Data-in predicate: pass a mapping extracted from the coordinator (a wire
    payload, a registry-record summary, nested rows). Allowed values are the
    fixed-width ``content_hash`` fingerprint, opaque bounded text (tokens,
    names), numbers, booleans, UUIDs, ``None``, and nested mappings/sequences
    of the same. Raw bytes, any content-named field, oversized text, and
    unrecognized value types are violations — the floor fails closed.
    """
    return not never_ship_violations(state)


def never_ship_violations(state: Mapping[str, object]) -> tuple[str, ...]:
    """Name every content-proportional leak in extracted coordinator-side state.

    The diagnostic companion of :func:`never_ship_a_store`: each entry is a
    ``"path: reason"`` string so a failing conformance run says exactly which
    field crossed the floor.
    """
    return tuple(_walk_violations(state, path=""))


def _walk_violations(value: object, path: str) -> Iterator[str]:
    """Recursively yield floor violations for one extracted value."""
    if isinstance(value, Mapping):
        yield from _mapping_violations(value, path)
    elif isinstance(value, (bytes, bytearray, memoryview)):
        yield f"{path or '<root>'}: raw bytes are content"
    elif isinstance(value, str):
        if len(value) > _MAX_OPAQUE_TEXT_LEN:
            yield (
                f"{path or '<root>'}: text longer than {_MAX_OPAQUE_TEXT_LEN} "
                "chars is treated as content-proportional"
            )
    elif isinstance(value, (list, tuple, set, frozenset)):
        if len(value) > _MAX_COLLECTION_LEN:
            # A body encoded as a list of byte-value ints is content-proportional;
            # name it and stop (walking a body-sized collection is itself a cost).
            yield (
                f"{path or '<root>'}: collection of {len(value)} items exceeds "
                f"{_MAX_COLLECTION_LEN} (content-proportional)"
            )
            return
        for index, item in enumerate(value):
            yield from _walk_violations(item, f"{path}[{index}]")
    elif isinstance(value, bool):
        pass  # a flag, not a magnitude
    elif isinstance(value, int):
        if value.bit_length() > _MAX_INT_BITS:
            # A body packed into one big integer (int.from_bytes) is a shadow too.
            yield (
                f"{path or '<root>'}: integer wider than {_MAX_INT_BITS} bits is "
                "content-proportional"
            )
    elif not isinstance(value, _ALLOWED_SCALARS):
        # Fail closed: a value type this floor cannot classify is rejected
        # rather than waved through.
        yield f"{path or '<root>'}: unrecognized value type {type(value).__name__}"


def _mapping_violations(state: Mapping[str, object], path: str) -> Iterator[str]:
    """Yield floor violations for one mapping level."""
    for key, item in state.items():
        key_path = f"{path}.{key}" if path else str(key)
        if key in _CONTENT_FIELD_NAMES:
            if item is not None:
                yield f"{key_path}: content-proportional field"
        elif key == "content_hash":
            if item is not None and not (
                isinstance(item, str) and _SHA256_HEX_RE.match(item)
            ):
                yield f"{key_path}: fingerprint must be a fixed-width sha-256 hex digest"
        else:
            yield from _walk_violations(item, key_path)


# --- retention-table leg ----------------------------------------------------


def retention_is_empty_for(
    rows: Iterable[Mapping[str, object]],
    artifact_ids: Iterable[UUID],
) -> bool:
    """True iff the retention table holds ZERO rows for the binding's artifacts.

    Even with retention enabled, a substrate binding's artifacts must leave the
    coordinator's version-history table empty — retained rows are bodies, and
    bodies never live coordinator-side for a binding. The check is
    leg-agnostic on purpose: it takes rows regardless of whether the
    registration leg or the commit leg produced them, so a binding cannot pass
    by keeping only its commit path content-free.
    """
    return not retained_rows_for(rows, artifact_ids)


def retained_rows_for(
    rows: Iterable[Mapping[str, object]],
    artifact_ids: Iterable[UUID],
) -> tuple[Mapping[str, object], ...]:
    """The retention-table rows belonging to the binding's artifacts.

    Data-in: the caller dumps the retention table and passes the rows (each a
    mapping with at least an ``artifact_id`` key). Any returned row is a
    violation of the zero-rows floor — including a row with no body column, as
    the floor is zero ROWS, not zero bytes per row.
    """
    wanted = set(artifact_ids)
    return tuple(row for row in rows if row.get("artifact_id") in wanted)
