# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""The bytes-I/O contract a bring-your-own-substrate binding implements.

This module is the CONTRACT half that lives with its callers: every
implementer and consumer of the byte-level substrate surface is an adapter,
so the Protocol sits in the adapters layer while the capability descriptor,
tier vocabulary, and never-ship-a-store floor live in
:mod:`ccs.core.substrate` (importable by the conformance kit without pulling
in any I/O).

Conformance is structural, mirroring the registry-contract discipline: the
Protocol is :func:`~typing.runtime_checkable` so ``isinstance`` verifies
presence of the surface, each binding adds a ``TYPE_CHECKING``-guarded static
assertion, and the descriptor-parametrized conformance suite is the parity
test. Bindings never inherit the Protocol.

Shared contract vocabulary (type aliases and the typed compare-and-set
outcomes) is DEFINED here and imported by bindings, so no two bindings can
drift apart on what a win, a conflict, or an unknown outcome means.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol, TypeAlias, runtime_checkable

from ccs.core.substrate import CapabilityDescriptor

# The substrate-minted version token: an object ETag, a row-version rendered
# opaque, etc. Two properties are contractual: it comes from the SAME read (or
# write response) as the bytes it vouches for, and it is never portable across
# artifact refs — identical bytes elsewhere can mint an identical token, so a
# token for one ref must never arbitrate a write to another.
SubstrateToken: TypeAlias = str


@dataclass(frozen=True)
class CasWritten:
    """WIN: the substrate accepted the compare-and-set write.

    ``token`` is the token the substrate minted FOR THIS WRITE, captured from
    the write response — never recomputed client-side (a recomputed token can
    silently vouch for bytes the substrate never acknowledged).
    """

    token: SubstrateToken


@dataclass(frozen=True)
class CasConflict:
    """LOSS: the substrate rejected the compare-and-set — the token moved.

    No write landed. Recovery is a fresh read (bytes and token from ONE read),
    re-derive, retry; the stale comparand must never be re-driven.
    """


@dataclass(frozen=True)
class CasUnknown:
    """UNKNOWN, not failed: the write's outcome could not be confirmed.

    A transport failure after the request left the client means the write may
    or may not be durable. Treat it as unconfirmed: reconcile by re-reading the
    token before doing anything else, never blindly re-drive the write, and
    never advance coordinator state on it.
    """


# The three-way compare-and-set outcome. A typed return, never an exception:
# conflict and unknown are expected protocol states a caller must branch on.
CasWriteResult: TypeAlias = "CasWritten | CasConflict | CasUnknown"


@runtime_checkable
class CoherenceSubstrate(Protocol):
    """The byte-level surface a substrate binding exposes to the coherence layer.

    Implementations wrap one substrate (a Postgres row, an S3 object) and keep
    ALL content substrate-side; the coherence layer above only ever sees the
    bytes in transit plus the opaque token. A binding for an action backend
    (the ``forward_only`` tier) does not implement this Protocol — an action
    surface has no bytes to read back and no token to compare; it declares a
    descriptor only.
    """

    @property
    def descriptor(self) -> CapabilityDescriptor:
        """The binding's declared capability tier and guarantee metadata.

        Part of the Protocol so a binding cannot satisfy ``isinstance`` while
        omitting its honesty declaration — the conformance suite reads the
        tier from here.
        """
        ...

    def read(self, artifact_ref: str) -> tuple[bytes, SubstrateToken]:
        """Return ``(bytes, token)`` observed by ONE substrate read.

        Both values must come from the same read operation: a token fetched by
        a second call can vouch for bytes it never described, which silently
        loses a concurrent update.
        """
        ...

    def cas_write(
        self,
        artifact_ref: str,
        *,
        expected_token: SubstrateToken,
        new_bytes: bytes,
    ) -> CasWriteResult:
        """Conditionally write ``new_bytes`` keyed on ``expected_token``.

        The comparison is the SUBSTRATE'S atomic conditional write, never a
        client-side check-then-write. Returns a typed outcome: a win carries
        the newly minted token; a conflict means no write landed; an unknown
        outcome means unconfirmed and must be reconciled by re-read.
        """
        ...


class TwoPartCommitMixin:
    """Ordering seam for the binding commit: substrate CAS first, coordinator
    bump second.

    The order is load-bearing. Bumping the coordinator first would advance the
    version and invalidate peers for a write that can still lose the substrate
    compare-and-set — a phantom invalidation with the coordinator ahead of the
    substrate, indistinguishable from a real advance and therefore
    unrecoverable. Substrate-first is self-healing: if the coordinator leg is
    lost, the next read reconciles because the token moved.

    This mixin defines the seam only. The substrate leg is the binding's
    :meth:`~CoherenceSubstrate.cas_write`; the coordinator leg
    (:meth:`_bump_coordinator`) is wired by each binding.
    """

    def commit_two_part(
        self,
        artifact_ref: str,
        *,
        expected_token: SubstrateToken,
        new_bytes: bytes,
    ) -> CasWriteResult:
        """Run the two-part commit and return the substrate outcome.

        The coordinator leg fires ONLY on a confirmed substrate win: a conflict
        never reaches the coordinator (no write landed), and an unknown outcome
        never advances coordinator state (the write is unconfirmed until a
        re-read proves otherwise).
        """
        outcome = self.cas_write(
            artifact_ref, expected_token=expected_token, new_bytes=new_bytes
        )
        if isinstance(outcome, CasWritten):
            self._bump_coordinator(artifact_ref, written=outcome)
        return outcome

    def cas_write(
        self,
        artifact_ref: str,
        *,
        expected_token: SubstrateToken,
        new_bytes: bytes,
    ) -> CasWriteResult:
        """The substrate leg — supplied by the binding's substrate surface."""
        raise NotImplementedError(
            "the binding supplies the substrate leg (CoherenceSubstrate.cas_write)"
        )

    def _bump_coordinator(self, artifact_ref: str, *, written: CasWritten) -> None:
        """The coordinator leg — version bump plus peer invalidation.

        Wired by each binding; this seam only guarantees WHEN it may run
        (after, and only after, a confirmed substrate win).
        """
        raise NotImplementedError(
            "the binding wires the coordinator bump for a confirmed substrate win"
        )
