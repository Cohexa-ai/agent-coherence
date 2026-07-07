# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Drift guards for the generalized backend atomic-boundary contract.

:mod:`ccs.coordinator.backend_contract` is pure documented vocabulary (enums,
frozen dataclasses, string constants) formalizing what a networked registry
backend must provide to re-home the coordinator's atomic boundary. These tests
are the DRIFT GUARDS the plan calls for:

- the member-classification map covers EXACTLY the 48 ``RegistryBase`` +
  ``SqliteExtended`` members — no more, no fewer — so a Protocol member added or
  removed in ``registry_protocol.py`` without a matching contract update FAILS
  CI (bidirectional guard);
- the ``coordinator_epoch`` PROPERTY is present in the map (property-omission
  teeth — ``@runtime_checkable`` cannot see properties, per the #133 lesson);
- every statelessness item names a disposition and ``coordinator_epoch``'s is
  never ``SAFELY_LOST`` (R12);
- a classification value outside the enum is unrepresentable (typed);
- the tier enum has exactly ``TIER_1`` and ``TIER_2``.

The expected 48-member set is FROZEN here (imported from the parity test's own
frozen name-sets), NOT derived from the Protocol at runtime — so a silent
Protocol edit cannot move the goalposts this test guards (the same discipline
``tests/test_registry_protocol_parity.py`` uses).
"""

from __future__ import annotations

from ccs.coordinator.backend_contract import (
    MEMBER_CLASSIFICATION,
    R9_ATOMIC_BOUNDARY,
    R12_EPOCH,
    R13_CONTENT_POSTURE,
    R14_CREDENTIAL,
    R15_BACKEND_IDENTITY,
    R18_LIVENESS_SOURCE,
    STATELESSNESS_INVENTORY,
    TIER_DECLARATIONS,
    Disposition,
    MemberClass,
    Tier,
)

# The 48-member expected surface, imported from the parity test's FROZEN
# name-sets (34 base methods + 13 extended methods + 1 base property). Reusing
# those frozensets means this contract and the Protocol parity share ONE
# source of truth for the surface: if either the parity test or the Protocol
# changes the surface, the two drift guards fire together.
from tests.test_registry_protocol_parity import (  # noqa: E402
    BASE_METHODS,
    BASE_PROPERTIES,
    EXTENDED_ONLY_METHODS,
)

EXPECTED_MEMBERS = BASE_METHODS | EXTENDED_ONLY_METHODS | BASE_PROPERTIES


# ---------------------------------------------------------------------------
# Member classification — exactly the 48 Protocol members, no drift
# ---------------------------------------------------------------------------


def test_member_map_covers_exactly_the_protocol_surface() -> None:
    """The classification map keys equal the 48-member Protocol surface EXACTLY
    — no more, no fewer. A member added to (or removed from) ``registry_protocol.py``
    without a matching contract update fails HERE (bidirectional drift guard)."""
    assert set(MEMBER_CLASSIFICATION) == EXPECTED_MEMBERS


def test_member_map_has_exactly_48_members() -> None:
    """Pin the count explicitly: 34 base methods + 13 extended methods + 1 base
    property = 48. Guards against a same-size add+remove that would slip past the
    set-equality check on cardinality alone."""
    assert len(MEMBER_CLASSIFICATION) == 48
    assert len(EXPECTED_MEMBERS) == 48


def test_coordinator_epoch_property_is_in_the_map() -> None:
    """The ``coordinator_epoch`` PROPERTY is classified (property-omission teeth,
    the #133 lesson: ``@runtime_checkable`` cannot see properties, so a
    method-only map would silently drop it — the exact gap that lesson exists to
    prevent)."""
    assert "coordinator_epoch" in MEMBER_CLASSIFICATION
    assert "coordinator_epoch" in BASE_PROPERTIES


def test_every_member_has_a_typed_classification() -> None:
    """Every mapped member carries a :class:`MemberClass` enum value — a
    classification outside the enum is unrepresentable (typed, not a bare
    string)."""
    for name, contract in MEMBER_CLASSIFICATION.items():
        assert isinstance(contract.member_class, MemberClass), name
        assert contract.name == name
        assert contract.surface in {"base", "sqlite_extended"}
        assert contract.rationale  # non-empty rationale authored from service.py


def test_member_surface_matches_the_protocol_split() -> None:
    """Each member's declared ``surface`` matches which Protocol frozenset it
    belongs to (base methods + the base property are "base"; extended-only
    methods are "sqlite_extended")."""
    base_surface = BASE_METHODS | BASE_PROPERTIES
    for name, contract in MEMBER_CLASSIFICATION.items():
        if name in base_surface:
            assert contract.surface == "base", name
        else:
            assert contract.surface == "sqlite_extended", name


def test_the_atomic_class_boundary_members_are_classified_atomic() -> None:
    """The members the service touches INSIDE its atomic mutation paths (authored
    from the ``service.py`` call sites) are ATOMIC_CLASS. This pins the core
    classification decision so a later edit that silently downgrades one to
    READ_ONLY/INDEPENDENT fails."""
    expected_atomic = {
        "commit_cas",
        "set_artifact_and_content",
        "set_agent_state",
        "set_agent_transient",
        "clear_agent_transient",
        "capture_version_vector",
        "abort_guard",
        "get_state_map",
    }
    actual_atomic = {
        name
        for name, contract in MEMBER_CLASSIFICATION.items()
        if contract.member_class is MemberClass.ATOMIC_CLASS
    }
    assert actual_atomic == expected_atomic


def test_classification_enum_has_exactly_three_classes() -> None:
    """The member-class taxonomy is exactly {ATOMIC_CLASS, INDEPENDENT,
    READ_ONLY} — a value outside it cannot be represented."""
    assert {m.name for m in MemberClass} == {
        "ATOMIC_CLASS",
        "INDEPENDENT",
        "READ_ONLY",
    }


# ---------------------------------------------------------------------------
# R11 statelessness inventory + R12 epoch never-safely-lost
# ---------------------------------------------------------------------------


def test_every_state_item_names_a_typed_disposition() -> None:
    """Every statelessness item names a :class:`Disposition` — nothing implicit
    (R11: every listed state has a stated re-home or safe-loss disposition)."""
    assert STATELESSNESS_INVENTORY  # non-empty
    for item in STATELESSNESS_INVENTORY:
        assert isinstance(item.disposition, Disposition), item.name
        assert item.consequence  # the stated consequence if it does not re-home


def test_coordinator_epoch_is_must_rehome_and_never_safely_lost() -> None:
    """``coordinator_epoch``'s disposition is MUST_REHOME and — the R12 assertion
    — is NEVER ``SAFELY_LOST`` (losing it invalidates every client-carried token
    at once)."""
    epoch_items = [
        item for item in STATELESSNESS_INVENTORY if item.name == "coordinator_epoch"
    ]
    assert len(epoch_items) == 1, "coordinator_epoch must appear exactly once"
    (epoch_item,) = epoch_items
    assert epoch_item.disposition is Disposition.MUST_REHOME
    assert epoch_item.disposition is not Disposition.SAFELY_LOST


def test_disposition_enum_has_exactly_three_values() -> None:
    assert {d.name for d in Disposition} == {
        "MUST_REHOME",
        "DERIVABLE",
        "SAFELY_LOST",
    }


# ---------------------------------------------------------------------------
# R10 tiers
# ---------------------------------------------------------------------------


def test_tier_enum_has_exactly_tier_1_and_tier_2() -> None:
    """Exactly two tiers (R10) — TIER_1 full-tuple and TIER_2 lease-decomposed."""
    assert {t.name for t in Tier} == {"TIER_1", "TIER_2"}


def test_tier_declarations_cover_both_tiers() -> None:
    assert set(TIER_DECLARATIONS) == {Tier.TIER_1, Tier.TIER_2}
    assert TIER_DECLARATIONS[Tier.TIER_1].ha_qualifies is True
    assert TIER_DECLARATIONS[Tier.TIER_2].ha_qualifies is False


# ---------------------------------------------------------------------------
# R9 atomic boundary — fence admit-on-absent reproduced exactly
# ---------------------------------------------------------------------------


def test_r9_boundary_names_the_three_tuple_legs() -> None:
    """The boundary names all three legs — version-CAS + grant arbitration +
    fence — not the version compare alone."""
    assert len(R9_ATOMIC_BOUNDARY.tuple_elements) == 3
    joined = " ".join(R9_ATOMIC_BOUNDARY.tuple_elements).lower()
    assert "version-cas" in joined
    assert "grant arbitration" in joined
    assert "read-generation fence" in joined


def test_r9_fence_admit_on_absent_is_stated_exactly() -> None:
    """The admit-on-absent asymmetry is reproduced EXACTLY (the fence-parity
    lesson — it has drifted once before): an ABSENT read_generation is ADMITTED
    (version-CAS arbitrates); only a PRESENT-and-superseded one is REJECTED."""
    text = R9_ATOMIC_BOUNDARY.fence_admit_on_absent
    assert "ABSENT read_generation is ADMITTED" in text
    assert "version-CAS arbitrates" in text
    assert "superseded" in text and "REJECTED" in text
    # The load-bearing `is not None` predicate is named (not treated as defensive).
    assert "is not None" in text


def test_r9_reference_semantics_include_the_same_lock_sweep() -> None:
    """Reference semantics = the single-process serialization AS A WHOLE — atomic
    mutations PLUS the same-lock sweep — and liveness eviction is stated to be a
    separate same-lock sweep, NOT inside commit_cas's transaction."""
    assert "as a whole" in R9_ATOMIC_BOUNDARY.reference_semantics.lower()
    assert "sweep" in R9_ATOMIC_BOUNDARY.reference_semantics.lower()
    note = R9_ATOMIC_BOUNDARY.liveness_eviction_note.lower()
    assert "separate" in note and "sweep" in note
    assert "not read inside commit_cas" in note


# ---------------------------------------------------------------------------
# R18 / R12 / R13 / R14 / R15 obligation records exist and are honest
# ---------------------------------------------------------------------------


def test_r18_liveness_source_names_shipped_source_as_non_ha() -> None:
    """R18: the shipped source is caller-supplied logical ticks under a single
    coordinator; it does NOT survive N coordinators."""
    assert R18_LIVENESS_SOURCE.conforming_sources
    assert "caller-supplied" in R18_LIVENESS_SOURCE.shipped_source.lower()
    assert "single coordinator" in R18_LIVENESS_SOURCE.shipped_source.lower()


def test_r12_epoch_backend_is_monotonic_int_local_stays_uuid() -> None:
    """R12: backend epoch is a monotonic-increasing integer; the shipped local
    epoch stays an opaque uuid4 string; the migration is deferred."""
    assert "monotonic" in R12_EPOCH.backend_contract.lower()
    assert "uuid4" in R12_EPOCH.shipped_local.lower()
    assert "deferred" in R12_EPOCH.migration_scope.lower()


def test_r13_content_posture_is_hash_only_with_declared_retention() -> None:
    """R13: hash-only baseline; retention is a declared opt-in that widens the
    disclosure surface."""
    assert "hash-only" in R13_CONTENT_POSTURE.baseline.lower()
    assert "retain_versions=true" in R13_CONTENT_POSTURE.retention_capability.lower()
    assert "widen" in R13_CONTENT_POSTURE.disclosure_note.lower()


def test_r14_r15_are_documented_obligations_only() -> None:
    """R14/R15 are documented obligations with typed placeholders — the module
    carries their vocabulary without any networked/connect code."""
    assert R14_CREDENTIAL.requirement_id == "R14"
    assert "0600" in R14_CREDENTIAL.obligation
    assert "O_NOFOLLOW" in R14_CREDENTIAL.discipline
    assert R15_BACKEND_IDENTITY.requirement_id == "R15"
    assert "fingerprint" in R15_BACKEND_IDENTITY.obligation.lower()
    assert "fail-closed" in R15_BACKEND_IDENTITY.obligation.lower()


# ---------------------------------------------------------------------------
# Non-goal (R2 trip-wire) + no networked code — module hygiene
# ---------------------------------------------------------------------------


def test_module_states_the_never_own_the_store_non_goal() -> None:
    """R2 trip-wire: the module docstring states the never-own-the-durable-store
    non-goal ("operated infrastructure we depend on, never a store we ship")."""
    import ccs.coordinator.backend_contract as mod

    assert mod.__doc__ is not None
    doc = mod.__doc__.lower()
    assert "never" in doc and "store we ship" in doc


def test_module_imports_no_networked_code() -> None:
    """The module is pure vocabulary — it imports no I/O, networking, or higher
    layers. Guards against a networked dependency creeping in."""
    import ccs.coordinator.backend_contract as mod

    source = mod.__file__
    assert source is not None
    with open(source, encoding="utf-8") as fh:
        text = fh.read()
    for forbidden in ("import socket", "import ssl", "import urllib", "import http", "requests"):
        assert forbidden not in text, forbidden
