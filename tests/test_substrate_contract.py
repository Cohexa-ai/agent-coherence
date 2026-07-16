# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Contract tests for the BYO-substrate capability descriptor and I/O Protocol.

Covers the four Unit-1 surfaces:

- ``Tier`` + ``CapabilityDescriptor`` construction validation and the
  tier-derived guarantee text (the honesty floor: a weaker tier must never
  present as enforcement).
- ``never_ship_a_store`` — the data-in predicate rejecting any
  content-proportional material in extracted coordinator-side state.
- The retention-leg helper: the retention table must hold ZERO rows for a
  binding's artifacts, regardless of which leg (registration or commit)
  produced them.
- The ``CoherenceSubstrate`` runtime-checkable Protocol and the two-part-commit
  ordering seam (substrate CAS first; coordinator bump only on a WIN).
"""

from __future__ import annotations

import hashlib
from pathlib import Path
from uuid import uuid4

import pytest

from ccs.adapters.substrate import (
    CasWritten,
    CoherenceSubstrate,
)
from ccs.core.substrate import (
    CapabilityDescriptor,
    Tier,
    never_ship_a_store,
    never_ship_violations,
    retained_rows_for,
    retention_is_empty_for,
)
from ccs.hardening.architecture import run_architecture_checks

SHA256_HEX = hashlib.sha256(b"canonical bytes").hexdigest()

# The wording a non-native tier must never carry: enforcement/CAS/rollback/
# duplicate-effect claims. Checked as case-insensitive substrings, so the
# guarantee text has to express its disclaimers without these trigger words.
FORBIDDEN_ENFORCEMENT_WORDS = ("enforce", "cas", "rollback", "dedup")


def make_descriptor(tier: Tier, **overrides: object) -> CapabilityDescriptor:
    """Arrange helper: a valid descriptor for any tier."""
    defaults: dict[str, object] = {
        Tier.NATIVE_CAS: {"version_source": "trigger-managed version column"},
        Tier.DETECT_ONLY: {"version_source": None},
        Tier.FORWARD_ONLY: {"version_source": None},
    }[tier]
    kwargs = {
        "least_privilege": "read+conditional-write on the bound target",
        "consistency_note": "read-after-write consistent per key",
        **defaults,
        **overrides,
    }
    return CapabilityDescriptor(tier=tier, **kwargs)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Guarantee text: happy paths per tier
# ---------------------------------------------------------------------------


def test_native_cas_guarantee_text_carries_timeout_asterisk():
    text = make_descriptor(Tier.NATIVE_CAS).guarantee_text

    assert "enforces no-lost-update on the version-CAS axis only" in text
    assert "coordinator-timeout after a durable substrate write" in text
    assert "token-identity reconciliation" in text
    assert "single-host" in text


def test_detect_only_guarantee_text_is_detection_wording():
    text = make_descriptor(Tier.DETECT_ONLY).guarantee_text

    assert "catches a sequential stale-read" in text
    assert "cannot prevent a concurrent race" in text


def test_detect_only_guarantee_text_never_claims_enforcement():
    lowered = make_descriptor(Tier.DETECT_ONLY).guarantee_text.lower()

    for word in FORBIDDEN_ENFORCEMENT_WORDS:
        assert word not in lowered, f"detect-only text must not contain {word!r}"


def test_forward_only_guarantee_text_is_effect_ordering_only():
    text = make_descriptor(Tier.FORWARD_ONLY).guarantee_text

    assert "effect ordering only" in text.lower()
    assert "freshness" in text
    assert "deny-before-act" in text


def test_forward_only_guarantee_text_never_claims_enforcement():
    lowered = make_descriptor(Tier.FORWARD_ONLY).guarantee_text.lower()

    for word in FORBIDDEN_ENFORCEMENT_WORDS:
        assert word not in lowered, f"forward-only text must not contain {word!r}"


def test_guarantee_text_is_distinct_per_tier():
    texts = {make_descriptor(tier).guarantee_text for tier in Tier}

    assert len(texts) == len(list(Tier))


# ---------------------------------------------------------------------------
# Descriptor validation: error paths
# ---------------------------------------------------------------------------


def test_unknown_tier_string_rejected_at_construction():
    with pytest.raises(ValueError):
        CapabilityDescriptor(tier="native_cas")  # type: ignore[arg-type]


def test_unknown_tier_value_rejected_by_enum():
    with pytest.raises(ValueError):
        Tier("bogus_tier")


def test_missing_tier_rejected_at_construction():
    with pytest.raises(TypeError):
        CapabilityDescriptor()  # type: ignore[call-arg]


def test_forward_only_with_version_source_rejected():
    with pytest.raises(ValueError, match="version_source"):
        make_descriptor(Tier.FORWARD_ONLY, version_source="etag")


def test_native_cas_without_version_source_rejected():
    with pytest.raises(ValueError, match="version_source"):
        make_descriptor(Tier.NATIVE_CAS, version_source=None)


def test_detect_only_version_source_is_optional():
    with_source = make_descriptor(Tier.DETECT_ONLY, version_source="client shadow")
    without_source = make_descriptor(Tier.DETECT_ONLY, version_source=None)

    assert with_source.version_source == "client shadow"
    assert without_source.version_source is None


def test_descriptor_is_frozen():
    descriptor = make_descriptor(Tier.NATIVE_CAS)

    with pytest.raises(AttributeError):
        descriptor.tier = Tier.DETECT_ONLY  # type: ignore[misc]


# ---------------------------------------------------------------------------
# never_ship_a_store: payload-shape leg
# ---------------------------------------------------------------------------


def metadata_only_state() -> dict[str, object]:
    """A coordinator-side state extract carrying ONLY coherence metadata."""
    return {
        "artifact_id": uuid4(),
        "version": 7,
        "content_hash": SHA256_HEX,
        "token": 'W/"etag-0xabc123"',
    }


def test_metadata_only_state_passes():
    assert never_ship_a_store(metadata_only_state()) is True
    assert never_ship_violations(metadata_only_state()) == ()


@pytest.mark.parametrize(
    "field", ["content", "body", "compressed_body", "diff", "sample"]
)
def test_content_proportional_field_rejected(field):
    state = {**metadata_only_state(), field: "even a short value is content"}

    assert never_ship_a_store(state) is False
    assert any(field in violation for violation in never_ship_violations(state))


def test_content_field_set_to_none_is_allowed():
    # content=None is the shipped content-free coordinator shape: the field
    # naming content with NO value is exactly "no content stored".
    state = {**metadata_only_state(), "content": None}

    assert never_ship_a_store(state) is True


def test_raw_bytes_rejected_under_any_field_name():
    state = {**metadata_only_state(), "note": b"raw bytes are content"}

    assert never_ship_a_store(state) is False


def test_oversized_text_rejected_as_content_proportional():
    state = {**metadata_only_state(), "annotation": "x" * 300}

    assert never_ship_a_store(state) is False


def test_content_hash_must_be_fixed_width():
    truncated = {**metadata_only_state(), "content_hash": "abc123"}
    inflated = {**metadata_only_state(), "content_hash": "a" * 4096}

    assert never_ship_a_store(truncated) is False
    assert never_ship_a_store(inflated) is False


def test_content_hash_none_is_allowed():
    # A never-written artifact legitimately has no fingerprint yet.
    state = {**metadata_only_state(), "content_hash": None}

    assert never_ship_a_store(state) is True


def test_nested_rows_are_walked():
    clean = {"rows": [metadata_only_state(), metadata_only_state()]}
    leaking = {"rows": [metadata_only_state(), {**metadata_only_state(), "body": "shadow"}]}

    assert never_ship_a_store(clean) is True
    assert never_ship_a_store(leaking) is False


def test_unrecognized_value_type_fails_closed():
    state = {**metadata_only_state(), "opaque": object()}

    assert never_ship_a_store(state) is False


def test_unbounded_collection_rejected_as_content_proportional():
    # A body encoded as a list of byte-value ints is content-proportional even
    # though every element is an allowed scalar.
    state = {**metadata_only_state(), "payload": list(range(256)) * 40}

    assert never_ship_a_store(state) is False
    assert any("collection" in v for v in never_ship_violations(state))


def test_small_collection_of_scalars_allowed():
    # A handful of tokens/ids is legitimate coherence metadata.
    state = {**metadata_only_state(), "peers": ["agent-a", "agent-b", "agent-c"]}

    assert never_ship_a_store(state) is True


def test_arbitrary_precision_integer_rejected():
    # A body packed into one big integer (int.from_bytes) is a content shadow.
    state = {**metadata_only_state(), "blob": int.from_bytes(b"x" * 5000, "big")}

    assert never_ship_a_store(state) is False
    assert any("integer" in v for v in never_ship_violations(state))


def test_ordinary_version_integer_allowed():
    state = {**metadata_only_state(), "version": 42, "tick": 1_000_000}

    assert never_ship_a_store(state) is True


# ---------------------------------------------------------------------------
# never_ship_a_store: retention-rows leg
# ---------------------------------------------------------------------------


def test_zero_retained_rows_passes():
    binding_ids = {uuid4(), uuid4()}

    assert retention_is_empty_for([], binding_ids) is True
    assert retained_rows_for([], binding_ids) == ()


def test_rows_for_other_artifacts_do_not_fail_the_binding():
    binding_ids = {uuid4()}
    foreign_row = {"artifact_id": uuid4(), "version": 1, "content": "not ours"}

    assert retention_is_empty_for([foreign_row], binding_ids) is True


def test_registration_leg_row_fails_the_check():
    # The registration leg seeds a version-1 body under retention; the check
    # takes rows regardless of which leg produced them.
    artifact_id = uuid4()
    row = {"artifact_id": artifact_id, "version": 1, "content": "seeded body"}

    assert retention_is_empty_for([row], {artifact_id}) is False
    assert retained_rows_for([row], {artifact_id}) == (row,)


def test_commit_leg_row_fails_the_check():
    artifact_id = uuid4()
    row = {"artifact_id": artifact_id, "version": 5, "content": "captured body"}

    assert retention_is_empty_for([row], {artifact_id}) is False


def test_retained_row_fails_even_without_a_body_column():
    # ANY retained row for the binding's artifacts is a violation: the floor is
    # zero rows, not zero bytes-per-row.
    artifact_id = uuid4()
    row = {"artifact_id": artifact_id, "version": 2}

    assert retention_is_empty_for([row], {artifact_id}) is False


# ---------------------------------------------------------------------------
# CoherenceSubstrate Protocol: structural conformance
# ---------------------------------------------------------------------------


class ConformingSubstrate:
    """Minimal stub satisfying the bytes-I/O contract."""

    descriptor = CapabilityDescriptor(
        tier=Tier.NATIVE_CAS,
        version_source="stub token",
        least_privilege="stub",
        consistency_note="stub",
    )

    def read(self, artifact_ref: str) -> tuple[bytes, str]:
        return b"payload", "token-1"

    def cas_write(self, artifact_ref: str, *, expected_token: str, new_bytes: bytes):
        return CasWritten(token="token-2")


class ReadOnlyStub:
    """Missing ``cas_write`` — must NOT satisfy the Protocol."""

    descriptor = ConformingSubstrate.descriptor

    def read(self, artifact_ref: str) -> tuple[bytes, str]:
        return b"payload", "token-1"


def test_conforming_stub_satisfies_protocol():
    assert isinstance(ConformingSubstrate(), CoherenceSubstrate)


def test_non_conforming_stub_rejected_by_protocol():
    assert not isinstance(ReadOnlyStub(), CoherenceSubstrate)


def test_read_returns_bytes_and_token_from_one_read():
    data, token = ConformingSubstrate().read("artifact-ref")

    assert isinstance(data, bytes)
    assert isinstance(token, str)


# ---------------------------------------------------------------------------
# The two-part-commit ordering (substrate CAS first, coordinator bump second) is
# no longer a standalone mixin — it lives in CoordinatedSubstrate, and its
# ordering guarantees (a conflict/unknown never reaches the coordinator) are
# covered end-to-end in tests/adapters/test_substrate_cross_agent.py.
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Integration: layer boundaries stay green
# ---------------------------------------------------------------------------


def test_descriptor_and_tier_reexported_from_core():
    import ccs.core as core

    assert core.CapabilityDescriptor is CapabilityDescriptor
    assert core.Tier is Tier
    assert "CapabilityDescriptor" in core.__all__
    assert "Tier" in core.__all__


def test_architecture_boundaries_stay_green():
    # Descriptor/predicates in core, Protocol in adapters: the 4-layer checker
    # must stay green (core imports nothing above core).
    src_root = Path(__file__).resolve().parents[1] / "src"

    report = run_architecture_checks(src_root)

    assert report.ok, (report.boundary_violations, report.cycles)
