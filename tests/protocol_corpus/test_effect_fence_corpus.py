# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Cross-implementation effect-fence verdict corpus (plan Unit 5, R9 + R10).

``POST /hooks/effect-fence`` answers "may this irreversible effect still fire,
and if not, WHY". The verdict envelope and the reason vocabulary are declared
protocol, and this module is where that claim stops being prose: every member
of :data:`~ccs.core.exceptions.HOLD_REASONS` is driven from a fixture through
the real HTTP route, the affirmative ``proceed`` arm is driven beside them, and
each fixture's asserted body is the FULL response, so a reason rename or an
extra key fails rather than passing a subset match.

Two silent failures this module exists to defeat, both of which report green:

1. **A fixture directory nothing names loads as empty.** Each corpus module
   loads fixtures by literal directory name, so a new directory no module names
   is collected by nothing at all. ``test_fixture_directory_is_actually_loaded``
   pins the count against a frozen literal, so an unloaded or silently emptied
   directory is red rather than vacuously green.
2. **A Node-backend row skips itself when the sibling build is unresolvable.**
   The warn-mode module ``xfail``s such rows, which is right for a parity
   fixture and WRONG for R10: the whole claim is that the sibling answers 404,
   and an xfail records that nothing asked. Node rows here therefore
   ``pytest.fail`` instead, and ``test_node_asymmetry_row_cannot_be_satisfied_
   by_a_skip`` asserts the resolution directly.

Marked ``protocol_corpus`` — opt-in via ``pytest -m protocol_corpus``."""

from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from ccs.core.exceptions import HOLD_REASONS
from tests.protocol_corpus.harness import (
    BACKEND_NODE,
    Fixture,
    load_fixtures,
    normalize_response,
    resolve_node_dist_path,
    run_scenario,
)

pytestmark = pytest.mark.protocol_corpus

_FIXTURE_DIR = "effect_fence"

# Frozen expectations. Deliberate duplicates of the code under test, per the
# tests/CLAUDE.md house rule: a set DERIVED from the route would move its own
# goalposts, so the edit that breaks the contract would also update the
# expectation and this module would report green. Cardinality is pinned
# separately so a same-size add+remove cannot slip past set equality.
_EXPECTED_HOLD_REASONS: frozenset[str] = frozenset({
    "version_moved",
    "grant_reclaimed",
    "grant_preempted",
    "input_vanished",
    "version_unconfirmed",
    "read_denied",
    "content_claim_absent",
    "generation_unconfirmed",
})
_EXPECTED_HOLD_REASON_COUNT = 8
_EXPECTED_FIXTURE_COUNT = 13

# The complete verdict/error envelope. Every key any fixture may assert.
# ``degraded``/``held_by`` belong to the two degraded arms, which a fixture
# cannot drive (see test_degraded_arms_are_documented_as_unreachable_here).
_ENVELOPE_KEYS: frozenset[str] = frozenset({
    "verdict", "reason", "degraded", "held_by", "error",
})

# The content-hash convention, pinned as bytes rather than described: lowercase
# sha-256 hex over the EXACT bytes the caller holds, no normalization. The
# anchor fixture's file content carries trailing spaces AND a trailing newline
# precisely so a normalizing implementation produces a visibly different digest.
_CONVENTION_FIXTURE = "effect-fence-proceed-all-legs-confirmed"
_CONVENTION_PATH = "docs/plan.md"


def _all_effect_fence_fixtures() -> list[Fixture]:
    """Loaded once at collection so parametrize ids stay stable."""
    return load_fixtures(_FIXTURE_DIR)


def _parametrize_rows() -> list[tuple[Fixture, str]]:
    rows: list[tuple[Fixture, str]] = []
    for fixture in _all_effect_fence_fixtures():
        for backend in fixture.backends:
            rows.append((fixture, backend))
    return rows


def _row_id(row: tuple[Fixture, str]) -> str:
    fixture, backend = row
    return f"{fixture.name}[{backend}]"


_ROWS = _parametrize_rows()
_NODE_DIST_PATH = resolve_node_dist_path()

_NODE_DIST_UNRESOLVED = (
    "R10 needs a REAL answer from the sibling coordinator, and this row got "
    "none: the plugin dist could not be resolved. Failing rather than xfailing "
    "is deliberate — the claim under test is that the Node backend answers 404 "
    "for /hooks/effect-fence, and an xfail records that nobody asked, which is "
    "how a cross-implementation claim silently stops being checked. Build the "
    "plugin checkout (npm ci && npm run build) or set "
    "AGENT_COHERENCE_PLUGIN_DIST_PATH to the absolute dist/coordinator.js path."
)


@pytest.mark.parametrize("row", _ROWS, ids=[_row_id(r) for r in _ROWS] if _ROWS else None)
def test_effect_fence_fixture_response_matches_expected(
    row: tuple[Fixture, str],
    tmp_path: Path,
) -> None:
    """The FULL response body is the assertion, not a subset of it.

    A changed reason identifier, an added key, or a dropped one all fail here.
    That is the point of pinning the contract in the corpus: the vocabulary is
    add-never-rename, and a rename un-matches every consumer's ``reason ==
    CONSTANT`` branch, downgrading a HOLD each of them no longer recognises."""
    fixture, backend = row
    if backend == BACKEND_NODE and _NODE_DIST_PATH is None:
        pytest.fail(f"{fixture.name}: {_NODE_DIST_UNRESOLVED}")

    actual_status, actual_body = run_scenario(
        fixture=fixture,
        backend_id=backend,
        workspace=tmp_path,
        node_dist_path=_NODE_DIST_PATH,
    )

    expected_status = fixture.expected["status"]
    expected_body = normalize_response(
        fixture.expected["body"],
        ignore_keys=fixture.ignore_keys,
        optional_keys=fixture.optional_keys,
    )

    assert actual_status == expected_status, (
        f"{fixture.name}[{backend}]: status mismatch — "
        f"expected {expected_status}, got {actual_status}\nbody={actual_body!r}"
    )
    assert actual_body == expected_body, (
        f"{fixture.name}[{backend}]: body mismatch\n"
        f"expected={expected_body!r}\nactual=  {actual_body!r}"
    )


def test_fixture_directory_is_actually_loaded() -> None:
    """Silent failure #1: a fixture directory no module names loads as EMPTY.

    ``load_fixtures`` returns ``[]`` for a directory that does not exist, and
    an empty parametrize list is collected as nothing — so a misspelled
    directory name, a moved fixtures root, or a wholesale deletion all report
    green. Pinning the count against a frozen literal is what makes any of
    those red."""
    fixtures = _all_effect_fence_fixtures()
    assert len(fixtures) == _EXPECTED_FIXTURE_COUNT, (
        f"Expected exactly {_EXPECTED_FIXTURE_COUNT} fixtures in "
        f"tests/protocol_corpus/fixtures/{_FIXTURE_DIR}/, found {len(fixtures)}. "
        f"Adding or removing one is a deliberate change to the pinned verdict "
        f"contract — update this literal in the same diff."
    )
    assert len(_ROWS) >= len(fixtures), "every fixture must contribute at least one row"


def test_every_hold_reason_has_a_fixture() -> None:
    """R9 EXHAUSTIVENESS, with NO carve-out.

    Every member of ``HOLD_REASONS`` is returnable by this route and is driven
    by a fixture here — including ``input_vanished``, which an earlier reading
    of the plan excluded on the grounds that the coordinator never stats the
    caller's workspace. It does not need to: the no-record path (untracked,
    never observed, or a row that vanished between lookup and read) builds
    comparands with ``current_version=None``, the vanish sentinel, so the
    branch table answers ``input_vanished`` on its first leg.

    A reason added to the vocabulary without a fixture therefore turns the
    corpus red, which is the enforcement this unit exists to add: the eighth
    reason (``content_claim_absent``) was split out of the residual bucket
    precisely because two cases were byte-identical on the wire, and nothing
    but a fixture pair can keep them distinguishable."""
    assert _EXPECTED_HOLD_REASONS == HOLD_REASONS, (
        "The published reason vocabulary drifted from this module's frozen "
        f"expectation.\n  only in HOLD_REASONS: {sorted(HOLD_REASONS - _EXPECTED_HOLD_REASONS)}"
        f"\n  only in expectation:  {sorted(_EXPECTED_HOLD_REASONS - HOLD_REASONS)}"
    )
    assert len(HOLD_REASONS) == _EXPECTED_HOLD_REASON_COUNT, (
        f"HOLD_REASONS cardinality moved: expected {_EXPECTED_HOLD_REASON_COUNT}, "
        f"got {len(HOLD_REASONS)}. Pinned separately from set equality so a "
        f"same-size add+remove cannot slip past."
    )

    covered = {
        f.expected["body"]["reason"]
        for f in _all_effect_fence_fixtures()
        if f.expected["body"].get("verdict") == "hold"
    }
    missing = HOLD_REASONS - covered
    assert not missing, (
        f"No fixture drives these HOLD reasons: {sorted(missing)}. Every member "
        f"of HOLD_REASONS is reachable from this route — if you believe one is "
        f"not, say so at the site rather than dropping it from the set."
    )
    assert covered <= HOLD_REASONS, (
        f"A fixture asserts a reason outside the published vocabulary: "
        f"{sorted(covered - HOLD_REASONS)}"
    )


def test_content_claim_absent_and_generation_unconfirmed_stay_distinguishable() -> None:
    """The one forced fixture the eighth reason costs.

    Fixtures 06 and 07 send BYTE-IDENTICAL request bodies. The only difference
    is whether the preflight pre-read carried a ``content_hash``, i.e. whether
    the coordinator holds a content claim at all. Before ``content_claim_absent``
    existed these two answered the same thing, with the same hint and the same
    retryability, for cases whose recovery differs: re-read your bytes, versus
    check the daemon's version and call an operator.

    Asserting the request bodies are identical is what makes the pair a
    CONTROL. Two fixtures that merely happen to return different reasons prove
    nothing if their inputs differ in five ways."""
    by_name = {f.name: f for f in _all_effect_fence_fixtures()}
    absent = by_name["effect-fence-hold-content-claim-absent-no-recorded-hash"]
    unconfirmed = by_name["effect-fence-hold-generation-unconfirmed-null-comparand"]

    assert absent.request == unconfirmed.request, (
        "The pair is only a control while the two requests are identical; they "
        f"differ:\n  absent=     {absent.request!r}\n  unconfirmed={unconfirmed.request!r}"
    )
    assert absent.expected["body"]["reason"] == "content_claim_absent"
    assert unconfirmed.expected["body"]["reason"] == "generation_unconfirmed"
    assert absent.expected["body"] != unconfirmed.expected["body"], (
        "identical requests must produce DIFFERENT wire answers, or the split "
        "bought nothing"
    )

    # And the one input that differs is the coordinator's content claim.
    def _preflight_hashes(fixture: Fixture) -> list[str | None]:
        return [
            req["body"].get("content_hash")
            for req in fixture.setup["preflight_requests"]
        ]

    assert _preflight_hashes(absent) == [None], (
        "the content_claim_absent arm must observe the artifact WITHOUT a "
        "content hash — that absent claim is the whole input under test"
    )
    assert _preflight_hashes(unconfirmed) == [
        unconfirmed.request["body"]["content_hash"]
    ], "the generation_unconfirmed arm must observe WITH the caller's own hash"


def test_content_hash_convention_is_sha256_of_exact_bytes() -> None:
    """The convention, recomputed rather than trusted.

    ``content_hash`` is the lowercase sha-256 hex of the EXACT bytes the caller
    holds, with no normalization. A fixture literal alone proves nothing — it
    is just a hex string both sides agree on — so this recomputes the digest
    from the anchor fixture's own file content and asserts the match, then
    asserts that the two obvious normalizations (``strip``, trailing-newline
    trim) produce DIFFERENT digests. That second half is the load-bearing one:
    without it, an implementation that stripped before hashing would satisfy a
    test written only against its own output."""
    by_name = {f.name: f for f in _all_effect_fence_fixtures()}
    anchor = by_name[_CONVENTION_FIXTURE]

    content: str = anchor.setup["files"][_CONVENTION_PATH]
    pinned: str = anchor.request["body"]["content_hash"]

    assert content.encode() == b"# coherence corpus fixture  \n", (
        "the anchor's bytes are pinned literally here too — trailing spaces and "
        "the trailing newline are what make the normalization arms below bite"
    )
    assert pinned == hashlib.sha256(content.encode()).hexdigest(), (
        f"content_hash is not the sha-256 of the exact bytes of "
        f"{_CONVENTION_PATH}.\n  pinned=   {pinned}\n  recomputed="
        f"{hashlib.sha256(content.encode()).hexdigest()}"
    )
    assert pinned == pinned.lower() and len(pinned) == 64, (
        "the wire form is 64 lowercase hex characters"
    )

    for label, variant in (
        ("strip()", content.strip()),
        ("rstrip newline", content.rstrip("\n")),
        ("rstrip whitespace", content.rstrip()),
    ):
        assert hashlib.sha256(variant.encode()).hexdigest() != pinned, (
            f"a {label} normalization produced the SAME digest as the exact "
            f"bytes, so this fixture cannot tell a normalizing implementation "
            f"from a conforming one — change the anchor's content"
        )

    # And the convention has teeth on the wire: the mismatch twin sends a digest
    # of different bytes against the same workspace and HOLDS where 01 proceeds.
    mismatch = by_name["effect-fence-hold-generation-unconfirmed-content-hash-mismatch"]
    assert mismatch.setup["files"] == anchor.setup["files"]
    assert mismatch.request["body"]["expected_version"] == anchor.request["body"]["expected_version"]
    assert mismatch.request["body"]["expected_generation"] == anchor.request["body"]["expected_generation"]
    assert mismatch.request["body"]["content_hash"] != pinned, (
        "the mismatch twin must differ from the anchor in its DIGEST alone"
    )
    assert anchor.expected["body"] == {"verdict": "proceed"}
    assert mismatch.expected["body"]["verdict"] == "hold"


def test_node_asymmetry_row_cannot_be_satisfied_by_a_skip() -> None:
    """Silent failure #2, asserted directly.

    R10 claims the sibling Node coordinator does not implement this route and
    answers 404. That claim is checked only if the Node row actually RAN, and
    the harness's own default for an unresolvable dist is ``xfail`` — a green
    result that recorded no answer. This asserts the resolution itself, so a
    machine or CI job without the built plugin fails the gate rather than
    quietly satisfying it.

    Point ``AGENT_COHERENCE_PLUGIN_DIST_PATH`` at a nonexistent file with
    ``HOME`` redirected (the resolver's third fallback is ``~/projects/...``)
    to watch this go red — that is the mutation this assertion exists to fail
    against."""
    assert _NODE_DIST_PATH is not None, _NODE_DIST_UNRESOLVED
    assert _NODE_DIST_PATH.exists(), (
        f"resolved dist path does not exist: {_NODE_DIST_PATH}"
    )

    node_rows = [f for f in _all_effect_fence_fixtures() if BACKEND_NODE in f.backends]
    assert len(node_rows) == 1, (
        f"expected exactly one Node-backed fixture recording the asymmetry, "
        f"found {[f.name for f in node_rows]}"
    )
    (asymmetry,) = node_rows
    assert asymmetry.expected["status"] == 404, (
        "the asymmetry fixture's whole content is the 404; a different status "
        "means Node grew the route and the two branch tables now need comparing"
    )
    assert asymmetry.request["path"] == "/hooks/effect-fence"
    assert "headers" not in asymmetry.request, (
        "the bearer must stay the harness's VALID one: the Node coordinator "
        "rejects auth before routing, so a bad bearer answers 401 and the 404 "
        "would say nothing about which routes exist"
    )


def test_expected_bodies_carry_only_the_verdict_envelope() -> None:
    """No unnormalized non-deterministic value hides in an asserted body.

    The harness normalizes by KEY NAME, and its UUID/ISO-8601 string scrubbers
    do not touch a non-dashed hexadecimal value — an epoch, a generation token
    or a bare digest in a response would diff literally and flake. Constraining
    every asserted body to the fixed envelope means such a field cannot appear
    without someone adding it here and deciding whether it needs an
    ``ignore_keys`` entry. Today none does, which is why no fixture declares
    one."""
    for fixture in _all_effect_fence_fixtures():
        extra = set(fixture.expected["body"]) - _ENVELOPE_KEYS
        assert not extra, (
            f"{fixture.name}: asserted body carries {sorted(extra)}, outside the "
            f"verdict/error envelope {sorted(_ENVELOPE_KEYS)}. If the route grew "
            f"a field, add it here and say whether its value is deterministic."
        )
        assert not fixture.ignore_keys, (
            f"{fixture.name} declares ignore_keys={sorted(fixture.ignore_keys)}; "
            f"each entry is a hole in the catch surface and the verdict envelope "
            f"needs none. Document the non-deterministic value in the fixture's "
            f"description if you are adding the first one."
        )
        assert not fixture.optional_keys, (
            f"{fixture.name} declares optional_keys; the verdict envelope is "
            f"identical on every backend that implements it"
        )
        body = fixture.expected["body"]
        if body.get("verdict") == "hold":
            assert set(body) == {"verdict", "reason"}, (
                f"{fixture.name}: a non-degraded hold is exactly "
                f"{{verdict, reason}}, got {sorted(body)}"
            )
        elif body.get("verdict") == "proceed":
            assert set(body) == {"verdict"}, (
                f"{fixture.name}: proceed carries NO reason key — its absence is "
                f"how a client tells it from a hold whose reason it does not know"
            )


def test_degraded_arms_are_documented_as_unreachable_here() -> None:
    """Honest limit of this corpus, stated rather than left to inference.

    The two degraded envelopes (``watchdog_timeout``, ``handler_error``) reuse
    ``version_unconfirmed`` and add ``degraded``/``held_by``. Neither is
    drivable from a fixture: one needs the watchdog to abandon the work body,
    the other needs the verdict helper to raise. Both are exercised by the unit
    tests around the route. The reason itself is covered here by fixture 03, so
    the vocabulary stays exhaustively pinned — what is NOT pinned here is the
    two-key degraded suffix, and this test exists so that gap is a written
    decision rather than an oversight."""
    degraded = [
        f for f in _all_effect_fence_fixtures()
        if "degraded" in f.expected["body"] or "held_by" in f.expected["body"]
    ]
    assert degraded == [], (
        "A fixture now drives a degraded arm — good. Rewrite this test to "
        f"assert its shape instead of its absence: {[f.name for f in degraded]}"
    )
    covered = {
        f.expected["body"]["reason"]
        for f in _all_effect_fence_fixtures()
        if f.expected["body"].get("verdict") == "hold"
    }
    assert "version_unconfirmed" in covered, (
        "the reason the degraded arms reuse must stay covered by a normal "
        "fixture, or dropping it here would silently uncover them too"
    )
