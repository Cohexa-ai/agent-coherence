# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Postgres three-leg boundary spike — operator-run, research-only (U1–U3).

Adjudicates the origin BRD's §9.2 mechanism claim with evidence: a
``SECURITY DEFINER`` plpgsql function plus a written anchor row delivers the
full three-leg deny (version-CAS, grant arbitration, read-generation fence) as
one atomic step at READ COMMITTED — and the naive single-statement construction
demonstrably loses the grant leg. Plan:
``docs/plans/2026-09-01-1824-test-pg-three-leg-spike-plan.md``.

Requires ``CCS_TEST_PG_DSN`` (owner DSN; a local throwaway container is the
documented default) and the ``coherent-row`` extra. Without either, every test
skips and the module still collects cleanly. Serial-only: provisioning DDL
drops and recreates one shared schema, so the module is not xdist-safe.

Driver-error hygiene: surfaced classifications carry the exception TYPE only —
never the driver message, which may embed the DSN password (the shipped
``CoherentRow._scrubbed`` discipline).
"""

from __future__ import annotations

import os

import pytest

from tests.conformance.pg_spike_sql import (
    ARBITRATE_SQL,
    BUMP_OWNER_GENERATION_SQL,
    GRANT_TRANSITION_SQL,
    INSERT_ANCHOR_SQL,
    INSERT_ARTIFACT_SQL,
    OUTCOME_CORRUPTION,
    OUTCOME_OTHER_HOLDER,
    OUTCOME_STALE_READ_GENERATION,
    OUTCOME_VERSION_MISMATCH,
    OUTCOME_WIN,
    PAIR_READ_SQL,
    READ_AGENT_STATES_SQL,
    READ_ANCHOR_SQL,
    READ_ARTIFACT_SQL,
    SPIKE_SCHEMA,
    build_spike_sql,
)

pytestmark = pytest.mark.real_substrate

_ARTIFACT = "artifact-1"
_AGENT_A = "agent-a"
_AGENT_B = "agent-b"

_CONNECT_TIMEOUT_SEC = 10
_STATEMENT_TIMEOUT_MS = 15_000


def _connect(dsn: str, *, autocommit: bool, statement_timeout_ms: int = _STATEMENT_TIMEOUT_MS):
    """A bounded-wait connection: connect + per-statement timeouts ALWAYS set
    (KTD5). Arbitration calls use ``autocommit=True`` so the single SELECT is
    one self-contained transaction (KTD2); provisioning uses explicit commit."""
    import psycopg  # noqa: PLC0415 - lazy so collection never needs the extra

    return psycopg.connect(
        dsn,
        autocommit=autocommit,
        connect_timeout=_CONNECT_TIMEOUT_SEC,
        options=f"-c statement_timeout={statement_timeout_ms}",
    )


@pytest.fixture
def spike_pg():
    """Provision the spike schema + functions on the operator's Postgres.

    Functions DDL (CREATE + REVOKE/GRANT) applies in ONE transaction (R6).
    Fails loudly when the server's default isolation is not READ COMMITTED —
    every claim this spike adjudicates is isolation-specific.
    """
    dsn = os.environ.get("CCS_TEST_PG_DSN")
    if not dsn:
        pytest.skip("set CCS_TEST_PG_DSN (owner DSN) to run the pg three-leg spike")
    pytest.importorskip("psycopg", reason="the spike needs the coherent-row extra (psycopg v3)")

    sql = build_spike_sql()
    with _connect(dsn, autocommit=False) as conn:
        with conn.cursor() as cur:
            cur.execute(f"DROP SCHEMA IF EXISTS {SPIKE_SCHEMA} CASCADE")
            cur.execute(sql.schema_ddl)
            cur.execute(sql.functions_ddl)
        conn.commit()
        with conn.cursor() as cur:
            cur.execute("SHOW transaction_isolation")
            (isolation,) = cur.fetchone()
        if isolation != "read committed":
            pytest.fail(
                f"spike requires the Postgres default READ COMMITTED, got {isolation!r}; "
                "the construction's claims are isolation-specific — do not run it elsewhere"
            )
    try:
        yield dsn
    finally:
        with _connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
            cur.execute(f"DROP SCHEMA IF EXISTS {SPIKE_SCHEMA} CASCADE")


@pytest.fixture
def conn(spike_pg):
    """An autocommit connection for direct-call tests (KTD2's client shape)."""
    with _connect(spike_pg, autocommit=True) as c:
        yield c


def _seed(conn, *, version: int = 1, owner_generation: int = 0) -> None:
    """Single-threaded pre-race seeding (KTD4): artifact + anchor rows."""
    with conn.cursor() as cur:
        cur.execute(INSERT_ARTIFACT_SQL, (_ARTIFACT, version, owner_generation))
        cur.execute(INSERT_ANCHOR_SQL, (_ARTIFACT,))


def _grant(conn, agent: str, state: str, read_generation: int | None) -> None:
    """Seed or change a grant through the sanctioned anchor lock chain."""
    with conn.cursor() as cur:
        cur.execute(GRANT_TRANSITION_SQL, (_ARTIFACT, agent, state, read_generation))


def _arbitrate(conn, *, agent: str, expected_version: int, content_hash: str = "h", commit_token: str = "t"):
    with conn.cursor() as cur:
        cur.execute(ARBITRATE_SQL, (_ARTIFACT, agent, expected_version, content_hash, commit_token))
        return cur.fetchone()


def _snapshot(conn):
    """Everything the no-mutation guarantee covers: the artifacts row and the
    agent_states rows. The anchor row is deliberately EXCLUDED — a denied call
    still commits its anchor bump by design (the lock chain IS a write)."""
    with conn.cursor() as cur:
        cur.execute(READ_ARTIFACT_SQL, (_ARTIFACT,))
        artifact = cur.fetchone()
        cur.execute(READ_AGENT_STATES_SQL, (_ARTIFACT,))
        states = cur.fetchall()
    return artifact, states


def _anchor_count(conn) -> int:
    with conn.cursor() as cur:
        cur.execute(READ_ANCHOR_SQL, (_ARTIFACT,))
        (count,) = cur.fetchone()
    return count


# --- U1: single-process leg proofs ------------------------------------------


def test_win_bumps_version_and_lands_payload_and_grant(conn) -> None:
    _seed(conn, version=1)
    outcome, current = _arbitrate(conn, agent=_AGENT_A, expected_version=1, content_hash="c1", commit_token="tok1")
    assert (outcome, current) == (OUTCOME_WIN, 2)
    artifact, states = _snapshot(conn)
    assert artifact == (2, 0, "c1", "tok1")
    assert states == [(_AGENT_A, "MODIFIED", 0)], "the winner's grant lands in the same step"


def test_expected_behind_yields_version_mismatch_without_mutation(conn) -> None:
    _seed(conn, version=1)
    assert _arbitrate(conn, agent=_AGENT_A, expected_version=1)[0] == OUTCOME_WIN  # version is now 2
    before = _snapshot(conn)
    anchor_before = _anchor_count(conn)

    outcome, current = _arbitrate(conn, agent=_AGENT_B, expected_version=1, content_hash="loser")
    assert (outcome, current) == (OUTCOME_VERSION_MISMATCH, 2)
    assert _snapshot(conn) == before, "a denied CAS must not touch artifacts or agent_states"
    assert _anchor_count(conn) == anchor_before + 1, "the denied call still commits its anchor bump"


def test_peer_exclusive_yields_other_holder_without_mutation(conn) -> None:
    _seed(conn, version=1)
    _grant(conn, _AGENT_B, "EXCLUSIVE", 0)
    before = _snapshot(conn)

    outcome, current = _arbitrate(conn, agent=_AGENT_A, expected_version=1)
    assert (outcome, current) == (OUTCOME_OTHER_HOLDER, 1)
    assert _snapshot(conn) == before


def test_superseded_generation_with_version_unmoved_yields_stale_read_generation(conn) -> None:
    _seed(conn, version=1, owner_generation=0)
    _grant(conn, _AGENT_A, "INVALID", 0)  # the zombie captured generation 0 at its acquire
    with conn.cursor() as cur:
        cur.execute(BUMP_OWNER_GENERATION_SQL, (_ARTIFACT,))  # the reclaim supersedes it
    before = _snapshot(conn)

    outcome, current = _arbitrate(conn, agent=_AGENT_A, expected_version=1)
    assert (outcome, current) == (OUTCOME_STALE_READ_GENERATION, 1), "fence fires exactly when the version is unmoved"
    assert _snapshot(conn) == before

    # Precedence pin (R4): once a peer commit MOVES the version, the same
    # zombie sees version_mismatch — the version leg fires first.
    assert _arbitrate(conn, agent=_AGENT_B, expected_version=1)[0] == OUTCOME_WIN
    outcome, current = _arbitrate(conn, agent=_AGENT_A, expected_version=1)
    assert (outcome, current) == (OUTCOME_VERSION_MISMATCH, 2)


def test_absent_read_generation_is_admitted(conn) -> None:
    # Contract verbatim: ABSENT is ADMITTED — version-CAS arbitrates it.
    _seed(conn, version=1, owner_generation=3)  # generations have moved; the writers never fenced
    # (a) No agent_states row at all: the plain OCC writer.
    outcome, current = _arbitrate(conn, agent=_AGENT_A, expected_version=1)
    assert (outcome, current) == (OUTCOME_WIN, 2)
    # (b) A row PRESENT with NULL read_generation: still absent, still admitted.
    # First downgrade the winner's own MODIFIED grant (landed by the win path),
    # so the grant leg is quiet and the fence disposition is what decides.
    _grant(conn, _AGENT_A, "SHARED", None)
    _grant(conn, _AGENT_B, "SHARED", None)
    outcome, current = _arbitrate(conn, agent=_AGENT_B, expected_version=2)
    assert (outcome, current) == (OUTCOME_WIN, 3)


def test_corruption_ahead_and_outranks_other_holder(conn) -> None:
    _seed(conn, version=1)
    outcome, current = _arbitrate(conn, agent=_AGENT_A, expected_version=5)
    assert (outcome, current) == (OUTCOME_CORRUPTION, 1)

    # KTD8: precedence is only proven when legs genuinely COMPETE — corruption
    # must outrank a simultaneously-true other_holder condition.
    _grant(conn, _AGENT_B, "MODIFIED", 0)
    before = _snapshot(conn)
    outcome, current = _arbitrate(conn, agent=_AGENT_A, expected_version=5)
    assert (outcome, current) == (OUTCOME_CORRUPTION, 1)
    assert _snapshot(conn) == before


def test_pair_read_returns_both_halves_from_one_statement(conn) -> None:
    _seed(conn, version=1, owner_generation=0)
    with conn.cursor() as cur:
        cur.execute(PAIR_READ_SQL, (_ARTIFACT,))
        assert cur.fetchone() == (1, 0)
        # A committed generation bump between two separate calls IS visible —
        # the atomicity claim lives in the single statement, not the session.
        cur.execute(BUMP_OWNER_GENERATION_SQL, (_ARTIFACT,))
        cur.execute(PAIR_READ_SQL, (_ARTIFACT,))
        assert cur.fetchone() == (1, 1)


def test_missing_artifact_is_a_loud_error_not_a_typed_outcome(conn) -> None:
    import psycopg  # noqa: PLC0415

    # No anchor row: the lock-chain entry itself refuses, loudly.
    with pytest.raises(psycopg.Error) as excinfo:
        _arbitrate(conn, agent=_AGENT_A, expected_version=1)
    assert excinfo.value.sqlstate == "P0001"  # RAISE EXCEPTION

    # Anchor present but artifact row missing: the STRICT pair-read refuses.
    with conn.cursor() as cur:
        cur.execute(INSERT_ANCHOR_SQL, (_ARTIFACT,))
    with pytest.raises(psycopg.Error) as excinfo:
        _arbitrate(conn, agent=_AGENT_A, expected_version=1)
    assert excinfo.value.sqlstate == "P0002"  # NO_DATA_FOUND from SELECT ... INTO STRICT


def test_function_hardening_search_path_pinned_and_no_public_execute(conn) -> None:
    for fn in ("spike_commit_cas", "spike_grant_transition"):
        with conn.cursor() as cur:
            cur.execute(
                "SELECT prosecdef, proconfig, proacl::text[] FROM pg_proc "
                "WHERE proname = %s AND pronamespace = %s::regnamespace",
                (fn, SPIKE_SCHEMA),
            )
            row = cur.fetchone()
        assert row is not None, fn
        secdef, config, acl = row
        assert secdef is True, f"{fn} must be SECURITY DEFINER"
        assert config == [f"search_path={SPIKE_SCHEMA}, pg_temp"], f"{fn} search_path not pinned: {config}"
        # A NULL proacl means PUBLIC keeps its default EXECUTE; after the R6
        # revoke it must be non-NULL with no PUBLIC entry (grantee '' in acl text).
        assert acl is not None, f"{fn} has default ACLs — the R6 revoke did not land"
        assert not any(entry.startswith("=") for entry in acl), f"{fn} still grants PUBLIC: {acl}"
