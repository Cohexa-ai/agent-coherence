# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""U2 — cross-process substrate-CAS assertions (guarantee-ladder plan, R-1/R-2).

The conforming arm races the packaged S3 stand-in's NATIVE conditional put
(one proxied call, atomic inside the substrate-owner process); the negative
control assembles its conditional client-side from separate proxied calls —
the read-then-write shape that passes every in-process test and loses updates
across processes (the qm defect class). The suite must pass the first and
demonstrably catch the second.
"""

from __future__ import annotations

from ccs.testing.substrate_conformance import (
    CrossProcessLostUpdate,
    assert_cross_process_one_winner_native_cas,
    assert_cross_process_rejects_read_then_write,
)

# Covers AE1: a real cross-process race; exactly one winner; loser leaves no trace.


def test_two_contenders_native_cas_exactly_one_winner() -> None:
    assert_cross_process_one_winner_native_cas(contenders=2, delays=(0.0, 0.05))


def test_four_contenders_native_cas_exactly_one_winner() -> None:
    assert_cross_process_one_winner_native_cas(contenders=4, delays=(0.0, 0.02, 0.04, 0.06))


def test_native_cas_holds_at_zero_delay() -> None:
    # The race is legal at any interleaving; the injected delay only widens
    # windows and must never be load-bearing (KTD-8).
    assert_cross_process_one_winner_native_cas(contenders=2, delays=(0.0, 0.0))


def test_read_then_write_binding_is_caught() -> None:
    # Teeth certification (Covers AE1's negative control): the client-side
    # check-then-write shape must produce a detected lost update — the race
    # would be vacuous if this passed as one-winner.
    assert_cross_process_rejects_read_then_write()


def test_read_then_write_detection_is_a_typed_failure() -> None:
    import pytest

    from ccs.testing.substrate_conformance import _run_broken_read_then_write_race

    with pytest.raises(CrossProcessLostUpdate, match="lost update"):
        _run_broken_read_then_write_race(raise_on_loss=True)
