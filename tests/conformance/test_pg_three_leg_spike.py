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
import threading
import time
import warnings

import pytest

from ccs.testing.process_harness import ContenderSpec, ProcessRaceHarness
from tests.conformance.pg_spike_sql import (
    ARBITRATE_SQL,
    BUMP_OWNER_GENERATION_SQL,
    GRANT_TRANSITION_SQL,
    INSERT_ANCHOR_SQL,
    INSERT_ARTIFACT_SQL,
    LOCK_ARTIFACT_ROW_SQL,
    MUTANT_ARBITRATE_SQL,
    NAIVE_COMMIT_CAS_SQL,
    OUTCOME_CORRUPTION,
    OUTCOME_OTHER_HOLDER,
    OUTCOME_STALE_READ_GENERATION,
    OUTCOME_VERSION_MISMATCH,
    OUTCOME_WIN,
    PAIR_READ_SQL,
    READ_AGENT_STATES_SQL,
    READ_ANCHOR_SQL,
    READ_ARTIFACT_SQL,
    RESET_ROWS_SQL,
    SPIKE_SCHEMA,
    build_mutant_arbitration_ddl,
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


# --- U2: negative control — the naive construction loses the grant leg ------
#
# The decisive interleaving is FORCED, never hoped for (delay-free by design):
#   1. Contender A takes the artifact's row lock with a non-version-bumping
#      UPDATE inside an open transaction.
#   2. Contender B fires the naive single statement and BLOCKS on that lock —
#      observed directly in pg_stat_activity, so B is provably in-flight
#      across A's commit (no scheduling coin-flip).
#   3. A writes an EXCLUSIVE grant via the sanctioned transition helper (the
#      anchor lock chain) and commits.
#   4. B's row re-check re-evaluates the version qual against A's committed
#      tuple, while the grant subquery keeps its pre-grant statement snapshot —
#      §9.2 predicts the admit lands.
#
# Refutation predicate: a DENY under exactly this interleaving is a genuine
# refutation of the origin's §9.2 mechanism claim (reported loudly, flags the
# C-3 pause). A run where the forcing did not hold is INCONCLUSIVE — rerun,
# never escalate. Never silently green either way.

_NAIVE_APP_NAME = "ccs-spike-naive"
_BLOCK_POLL_TIMEOUT_SEC = 10.0
_U2_ATTEMPTS = 3


def _contender_connect(dsn: str, *, autocommit: bool, application_name: str | None = None):
    """Contender-side connect with the DSN-scrubbing discipline: a failure
    surfaces the exception TYPE only, because the harness transports child
    tracebacks and a driver connect error may echo conninfo details."""
    import psycopg  # noqa: PLC0415

    kwargs: dict[str, object] = {}
    if application_name is not None:
        kwargs["application_name"] = application_name
    try:
        return psycopg.connect(
            dsn,
            autocommit=autocommit,
            connect_timeout=_CONNECT_TIMEOUT_SEC,
            options=f"-c statement_timeout={_STATEMENT_TIMEOUT_MS}",
            **kwargs,
        )
    except Exception as exc:  # noqa: BLE001 - re-raised scrubbed
        raise RuntimeError(
            f"spike contender connect failed with {type(exc).__name__}; "
            "details suppressed to avoid leaking DSN credentials"
        ) from None


def _await_backend_blocked(monitor_conn, application_name: str, timeout_sec: float = _BLOCK_POLL_TIMEOUT_SEC) -> bool:
    """True once a backend with ``application_name`` is observed in a Lock wait.

    Must run on an AUTOCOMMIT connection: backend-status views are frozen at
    first access within a transaction, so polling inside one would never see
    the state change.
    """
    deadline = time.monotonic() + timeout_sec
    while time.monotonic() < deadline:
        with monitor_conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM pg_stat_activity "
                "WHERE application_name = %s AND wait_event_type = 'Lock'",
                (application_name,),
            )
            (count,) = cur.fetchone()
        if count:
            return True
        time.sleep(0.02)
    return False


def _u2_locker_contender(ctx, dsn: str) -> dict:
    """Contender A: hold the artifact row lock, observe B blocked, then commit
    an EXCLUSIVE grant through the anchor lock chain while B waits."""
    conn = _contender_connect(dsn, autocommit=False)
    monitor = _contender_connect(dsn, autocommit=True)
    try:
        ctx.barrier_wait()  # phase 1: both contenders are running
        with conn.cursor() as cur:
            cur.execute(LOCK_ARTIFACT_ROW_SQL, (_ARTIFACT,))  # row lock held; version untouched
        ctx.barrier_wait()  # phase 2: the lock is provably held before B fires
        observed_blocked = _await_backend_blocked(monitor, _NAIVE_APP_NAME)
        with conn.cursor() as cur:
            cur.execute(GRANT_TRANSITION_SQL, (_ARTIFACT, _AGENT_A, "EXCLUSIVE", 0))
        conn.commit()  # B unblocks HERE, mid-flight across this grant commit
        return {"observed_blocked": observed_blocked}
    finally:
        monitor.close()
        conn.close()


def _u2_naive_contender(ctx, dsn: str) -> dict:
    """Contender B: fire the naive single-statement CAS+grant-check and report
    whether it admitted (rowcount 1) or denied (rowcount 0)."""
    conn = _contender_connect(dsn, autocommit=True, application_name=_NAIVE_APP_NAME)
    try:
        ctx.barrier_wait()  # phase 1
        ctx.barrier_wait()  # phase 2: A holds the artifact row lock
        started = time.monotonic()
        try:
            with conn.cursor() as cur:
                cur.execute(NAIVE_COMMIT_CAS_SQL, ("naive-c", "naive-t", _ARTIFACT, 1, _AGENT_B))
                rowcount = cur.rowcount
        except Exception as exc:  # noqa: BLE001 - classified, scrubbed (KTD5)
            return {"outcome": "unknown", "error_type": type(exc).__name__}
        return {"rowcount": rowcount, "elapsed": time.monotonic() - started}
    finally:
        conn.close()


def _reset_rows(dsn: str, *, version: int = 1, owner_generation: int = 0) -> None:
    with _connect(dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(RESET_ROWS_SQL)
        _seed(conn, version=version, owner_generation=owner_generation)


def test_naive_single_statement_loses_the_grant_leg(spike_pg) -> None:
    """R8 — and the origin §10 second-pass caveat this control discharges: the
    naive arm must admit a write while a peer's committed EXCLUSIVE grant
    should have denied it. This test PASSING means the naive construction
    FAILED in the predicted shape."""
    inconclusive: list[str] = []
    for _attempt in range(_U2_ATTEMPTS):
        _reset_rows(spike_pg)
        harness = ProcessRaceHarness(timeout_sec=60.0)
        result = harness.race(
            [
                ContenderSpec(_u2_locker_contender, args=(spike_pg,)),
                ContenderSpec(_u2_naive_contender, args=(spike_pg,)),
            ]
        )
        for outcome in result.outcomes:
            if outcome.error is not None:
                raise outcome.error
        locker = result.outcomes[0].value
        naive = result.outcomes[1].value

        if naive.get("outcome") == "unknown":
            inconclusive.append(f"driver error {naive['error_type']} (unknown outcome)")
            continue
        if not locker["observed_blocked"]:
            inconclusive.append("naive backend never observed in a Lock wait")
            continue

        # The forcing held: B was provably blocked across A's grant commit.
        with _connect(spike_pg, autocommit=True) as check:
            with check.cursor() as cur:
                cur.execute(READ_ARTIFACT_SQL, (_ARTIFACT,))
                version, _gen, content_hash, _token = cur.fetchone()
                cur.execute(READ_AGENT_STATES_SQL, (_ARTIFACT,))
                states = dict((agent, state) for agent, state, _g in cur.fetchall())
        assert states.get(_AGENT_A) == "EXCLUSIVE", "scaffold broke: A's grant transition did not commit"

        if naive["rowcount"] == 0:
            pytest.fail(
                "SPIKE FINDING — §9.2 REFUTED: the naive single statement DENIED under the "
                "forced interleaving (blocked across a committed EXCLUSIVE grant, then rejected). "
                "The origin BRD's mechanism argument does not hold as stated; flag the C-3 pause "
                "and record this in the findings report before any backend scoping."
            )
        # The predicted lost arbitration: the naive statement admitted a write
        # while a peer held a committed EXCLUSIVE grant.
        assert naive["rowcount"] == 1
        assert version == 2 and content_hash == "naive-c", (
            "the admitted write should be the naive contender's payload"
        )
        return
    pytest.fail(
        "INCONCLUSIVE (not a §9.2 refutation): the forced interleaving could not be "
        f"established in {_U2_ATTEMPTS} attempts ({'; '.join(inconclusive)}). Rerun the spike; "
        "do not escalate an unforced run to the C-3 pause."
    )


# --- U3: cross-process affirmative demonstration ----------------------------
#
# Two-phase contenders per the shipped pattern: barrier 1 (everyone running),
# each contender pair-reads its comparands, barrier 2 (everyone holds the same
# pre-race version), optional delay, then ONE arbitration call on its own
# autocommit connection. Concurrent calls serialize on the anchor row lock;
# the loser's pair-read after the wait takes a fresh statement snapshot and
# sees the winner's committed bump — the mechanism under test, cross-process.

_U3_APP_PREFIX = "ccs-spike-u3"


class _LockWaitMonitor:
    """KTD6's primary affirmative signal: a parent-side connection samples
    pg_stat_activity during the race window; a contender backend seen with
    ``wait_event_type='Lock'`` proves the block-then-fresh-read path fired.
    Runs on an AUTOCOMMIT connection (backend-status views freeze at first
    access inside a transaction)."""

    def __init__(self, dsn: str, app_prefix: str = _U3_APP_PREFIX) -> None:
        self._dsn = dsn
        self._pattern = app_prefix + "%"
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self.observed = False

    def __enter__(self) -> "_LockWaitMonitor":
        conn = _connect(self._dsn, autocommit=True)

        def _poll() -> None:
            try:
                while not self._stop.is_set():
                    with conn.cursor() as cur:
                        cur.execute(
                            "SELECT count(*) FROM pg_stat_activity "
                            "WHERE application_name LIKE %s AND wait_event_type = 'Lock'",
                            (self._pattern,),
                        )
                        (count,) = cur.fetchone()
                    if count:
                        self.observed = True
                        return
                    time.sleep(0.005)
            finally:
                conn.close()

        self._thread = threading.Thread(target=_poll, daemon=True)
        self._thread.start()
        return self

    def __exit__(self, *exc_info) -> None:
        self._stop.set()
        assert self._thread is not None
        self._thread.join(timeout=5.0)


def _u3_arbitrate_contender(ctx, dsn: str, agent: str, payload: str, app_name: str) -> dict:
    """Generic racing committer: pair-read the comparand pre-barrier-2, then
    one arbitration SELECT on an autocommit connection (KTD2)."""
    import psycopg  # noqa: PLC0415

    conn = _contender_connect(dsn, autocommit=True, application_name=app_name)
    try:
        ctx.barrier_wait()  # phase 1: everyone is running
        with conn.cursor() as cur:
            cur.execute(PAIR_READ_SQL, (_ARTIFACT,))
            read_version, _generation = cur.fetchone()
        ctx.barrier_wait()  # phase 2: everyone holds the same pre-race state
        ctx.delay()
        started = time.monotonic()
        try:
            with conn.cursor() as cur:
                cur.execute(ARBITRATE_SQL, (_ARTIFACT, agent, read_version, payload, f"tok-{agent}"))
                outcome, current = cur.fetchone()
        except (psycopg.OperationalError, psycopg.InterfaceError) as exc:
            # KTD5: the write MAY have landed server-side — classify, discard
            # the connection, and let the parent's authoritative read settle it.
            return {"agent": agent, "payload": payload, "outcome": "unknown", "error_type": type(exc).__name__}
        return {
            "agent": agent,
            "payload": payload,
            "outcome": outcome,
            "current_version": current,
            "read_version": read_version,
            "elapsed": time.monotonic() - started,
        }
    finally:
        conn.close()


def _u3_grant_transition_contender(ctx, dsn: str, agent: str, state: str, read_generation: int | None) -> dict:
    """Mid-race grant transition through the sanctioned anchor lock chain —
    INSERTs a NEW agent_states row, the phantom no row lock can see."""
    conn = _contender_connect(dsn, autocommit=True, application_name=f"{_U3_APP_PREFIX}-grant")
    try:
        ctx.barrier_wait()  # phase 1
        ctx.barrier_wait()  # phase 2
        with conn.cursor() as cur:
            cur.execute(GRANT_TRANSITION_SQL, (_ARTIFACT, agent, state, read_generation))
        return {"agent": agent, "outcome": "granted"}
    finally:
        conn.close()


def _u3_grant_locker_contender(ctx, dsn: str) -> dict:
    """Paired-contrast forcer: hold an UNCOMMITTED grant transition (anchor
    lock + EXCLUSIVE upsert), observe the function-arm contender blocked on the
    anchor, then commit mid-flight — the same commit-across-a-blocked-peer
    shape as U2, at the construction's OWN serialization point."""
    conn = _contender_connect(dsn, autocommit=False)
    monitor = _contender_connect(dsn, autocommit=True)
    try:
        ctx.barrier_wait()  # phase 1
        with conn.cursor() as cur:
            cur.execute(GRANT_TRANSITION_SQL, (_ARTIFACT, _AGENT_A, "EXCLUSIVE", 0))  # uncommitted
        ctx.barrier_wait()  # phase 2: the anchor lock is provably held before B fires
        observed_blocked = _await_backend_blocked(monitor, f"{_U3_APP_PREFIX}-fn")
        conn.commit()  # B unblocks HERE, mid-flight across this grant commit
        return {"observed_blocked": observed_blocked}
    finally:
        monitor.close()
        conn.close()


def _u3_function_contender(ctx, dsn: str) -> dict:
    """Paired-contrast function arm: fires the arbitration function while the
    locker holds the anchor; blocks at the function's FIRST statement."""
    import psycopg  # noqa: PLC0415

    conn = _contender_connect(dsn, autocommit=True, application_name=f"{_U3_APP_PREFIX}-fn")
    try:
        ctx.barrier_wait()  # phase 1
        ctx.barrier_wait()  # phase 2: the locker holds the anchor lock
        try:
            with conn.cursor() as cur:
                cur.execute(ARBITRATE_SQL, (_ARTIFACT, _AGENT_B, 1, "fn-c", "fn-t"))
                outcome, current = cur.fetchone()
        except (psycopg.OperationalError, psycopg.InterfaceError) as exc:
            return {"outcome": "unknown", "error_type": type(exc).__name__}
        return {"outcome": outcome, "current_version": current}
    finally:
        conn.close()


def _uncontended_baseline_sec(dsn: str) -> float:
    """Median of a few solo arbitration calls on freshly seeded rows — the
    comparison floor for the loser-elapsed fallback signal (KTD6)."""
    samples = []
    with _connect(dsn, autocommit=True) as conn:
        for i in range(3):
            _reset_rows(dsn)
            started = time.monotonic()
            _arbitrate(conn, agent=f"baseline-{i}", expected_version=1)
            samples.append(time.monotonic() - started)
    return sorted(samples)[1]


def _signal_lock_wait_or_warn(observed: bool, results: list[dict], baseline_sec: float) -> None:
    """R9: report the affirmative lock-wait signal, warn (never fail) when
    unproven — barrier overlap alone is necessary but not sufficient."""
    if observed:
        return
    slowest_loser = max((r["elapsed"] for r in results if r["outcome"] != OUTCOME_WIN and "elapsed" in r), default=0.0)
    if slowest_loser > baseline_sec * 3 + 0.005:
        return
    warnings.warn(
        RuntimeWarning(
            "spike lock-wait path UNPROVEN this run: no contender backend was observed in a "
            "Lock wait and loser elapsed times are indistinguishable from the uncontended "
            "baseline. Barrier overlap proves process concurrency, not that the "
            "block-then-fresh-read path fired. The forced-interleaving contrast test carries "
            "the deterministic observation."
        ),
        stacklevel=2,
    )


def _race_committers(dsn: str, *, agents: list[str], delays: tuple[float, ...]) -> tuple[list[dict], bool]:
    assert len(delays) == len(agents), "one delay per contender"
    harness = ProcessRaceHarness(timeout_sec=60.0)
    with _LockWaitMonitor(dsn) as monitor:
        result = harness.race(
            [
                ContenderSpec(
                    _u3_arbitrate_contender,
                    args=(dsn, agent, f"payload-{agent}", f"{_U3_APP_PREFIX}-{i}"),
                    delay_seconds=delays[i],
                )
                for i, agent in enumerate(agents)
            ]
        )
    for outcome in result.outcomes:
        if outcome.error is not None:
            raise outcome.error
    return [o.value for o in result.outcomes], monitor.observed


def _assert_exactly_one_winner(dsn: str, results: list[dict], *, pre_version: int = 1) -> None:
    """One winner, typed losses in the shipped precedence, and the parent's
    authoritative read settles what landed — including any unknown-outcome
    contender whose write may have landed after a driver error (KTD5)."""
    with _connect(dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(READ_ARTIFACT_SQL, (_ARTIFACT,))
            version, _generation, content_hash, _token = cur.fetchone()
    wins = [r for r in results if r["outcome"] == OUTCOME_WIN]
    unknowns = [r for r in results if r["outcome"] == "unknown"]
    losers = [r for r in results if r["outcome"] not in (OUTCOME_WIN, "unknown")]

    assert version == pre_version + 1, f"exactly one write must land, got version {version}"
    assert len(wins) <= 1, f"two winners is a lost update: {results}"
    # Peer-committed ⇒ the version leg fires first (the shipped precedence
    # pin): every typed loser must see version_mismatch, never other_holder
    # against the winner's own MODIFIED grant.
    assert all(r["outcome"] == OUTCOME_VERSION_MISMATCH for r in losers), results
    if wins:
        assert content_hash == wins[0]["payload"], "the winner's bytes did not survive — a loser left a trace"
        assert wins[0]["current_version"] == pre_version + 1
    else:
        # No typed winner: only an unknown-outcome contender may own the landed
        # write (the documented indeterminate case, settled authoritatively).
        assert unknowns, f"version moved with no winner and no unknown: {results}"
        assert content_hash in {u["payload"] for u in unknowns}, results


def test_race_two_contenders_exactly_one_winner(spike_pg) -> None:
    _reset_rows(spike_pg)
    baseline = _uncontended_baseline_sec(spike_pg)
    _reset_rows(spike_pg)
    results, observed = _race_committers(spike_pg, agents=["r2-a", "r2-b"], delays=(0.0, 0.05))
    _assert_exactly_one_winner(spike_pg, results)
    _signal_lock_wait_or_warn(observed, results, baseline)


def test_race_four_contenders_exactly_one_winner(spike_pg) -> None:
    _reset_rows(spike_pg)
    baseline = _uncontended_baseline_sec(spike_pg)
    _reset_rows(spike_pg)
    results, observed = _race_committers(
        spike_pg, agents=["r4-a", "r4-b", "r4-c", "r4-d"], delays=(0.0, 0.02, 0.04, 0.06)
    )
    _assert_exactly_one_winner(spike_pg, results)
    _signal_lock_wait_or_warn(observed, results, baseline)


def test_race_zero_delay_exactly_one_winner(spike_pg) -> None:
    """Delays are never load-bearing: every assertion holds at all-zero."""
    _reset_rows(spike_pg)
    baseline = _uncontended_baseline_sec(spike_pg)
    _reset_rows(spike_pg)
    results, observed = _race_committers(spike_pg, agents=["z-a", "z-b"], delays=(0.0, 0.0))
    _assert_exactly_one_winner(spike_pg, results)
    _signal_lock_wait_or_warn(observed, results, baseline)


def test_race_fence_zombie_vs_fresh_peer(spike_pg) -> None:
    """The fence cross-process: a reclaimed-generation committer races a fresh
    peer. With the version unmoved the zombie sees stale_read_generation; once
    the peer's commit moves it, version_mismatch — the shipped precedence pin."""
    _reset_rows(spike_pg)
    with _connect(spike_pg, autocommit=True) as conn:
        _grant(conn, "zombie", "INVALID", 0)  # captured generation 0 at its acquire
        with conn.cursor() as cur:
            cur.execute(BUMP_OWNER_GENERATION_SQL, (_ARTIFACT,))  # the reclaim supersedes it

    results, _observed = _race_committers(spike_pg, agents=["zombie", "fresh"], delays=(0.0, 0.0))
    zombie, fresh = results[0], results[1]

    # The fresh peer has NO read_generation — admit-on-absent, cross-process:
    # it must never be fenced. It wins unless the zombie somehow won first,
    # and the zombie can never win at a superseded generation.
    assert zombie["outcome"] in (OUTCOME_STALE_READ_GENERATION, OUTCOME_VERSION_MISMATCH), results
    assert fresh["outcome"] == OUTCOME_WIN, results
    # Reason correlates with whether the version had moved at arbitration time:
    # stale_read_generation is observable EXACTLY when the version is unmoved.
    if zombie["outcome"] == OUTCOME_STALE_READ_GENERATION:
        assert zombie["current_version"] == 1, results
    else:
        assert zombie["current_version"] == 2, results
    # Deterministic half of the pin: after the peer's commit, the same zombie
    # retrying its stale comparand is denied by the VERSION leg first.
    with _connect(spike_pg, autocommit=True) as conn:
        outcome, current = _arbitrate(conn, agent="zombie", expected_version=1)
    assert (outcome, current) == (OUTCOME_VERSION_MISMATCH, 2)


def test_race_pessimistic_holder_denies_optimistic_committers(spike_pg) -> None:
    """A pessimistic EXCLUSIVE holder blocks every optimistic committer with
    other_holder in the same atomic step: zero wins, version unmoved, no trace."""
    _reset_rows(spike_pg)
    with _connect(spike_pg, autocommit=True) as conn:
        _grant(conn, "holder", "EXCLUSIVE", 0)

    results, _observed = _race_committers(spike_pg, agents=["opt-a", "opt-b"], delays=(0.0, 0.0))
    assert [r["outcome"] for r in results] == [OUTCOME_OTHER_HOLDER, OUTCOME_OTHER_HOLDER], results

    with _connect(spike_pg, autocommit=True) as conn:
        artifact, states = _snapshot(conn)
    assert artifact == (1, 0, None, None), "a denied committer left a trace"
    assert states == [("holder", "EXCLUSIVE", 0)]


def test_race_phantom_grant_insert_vs_committer(spike_pg) -> None:
    """A mid-race grant transition INSERTs a NEW agent_states row — the
    phantom case only the anchor lock chain forces into view. The racing
    committer still resolves to exactly one typed outcome, consistent with
    the authoritative read."""
    _reset_rows(spike_pg)
    harness = ProcessRaceHarness(timeout_sec=60.0)
    result = harness.race(
        [
            ContenderSpec(_u3_grant_transition_contender, args=(spike_pg, "newcomer", "EXCLUSIVE", None)),
            ContenderSpec(
                _u3_arbitrate_contender,
                args=(spike_pg, "committer", "payload-committer", f"{_U3_APP_PREFIX}-0"),
            ),
        ]
    )
    for outcome in result.outcomes:
        if outcome.error is not None:
            raise outcome.error
    committer = result.outcomes[1].value

    with _connect(spike_pg, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(READ_ARTIFACT_SQL, (_ARTIFACT,))
            version, _generation, content_hash, _token = cur.fetchone()
            cur.execute(READ_AGENT_STATES_SQL, (_ARTIFACT,))
            states = {agent: state for agent, state, _g in cur.fetchall()}
    assert states.get("newcomer") == "EXCLUSIVE", "the grant transition must land either way"
    if committer["outcome"] == OUTCOME_WIN:
        # The committer beat the insert through the anchor chain.
        assert (version, content_hash) == (2, "payload-committer"), (committer, version, content_hash)
    elif committer["outcome"] == OUTCOME_OTHER_HOLDER:
        # The insert committed first; the committer's fresh grant-leg read saw it.
        assert (version, content_hash) == (1, None), (committer, version, content_hash)
    else:
        pytest.fail(f"the racing committer must resolve to a typed outcome, got {committer}")


def test_statement_timeout_mid_function_is_a_safe_no_effect(spike_pg) -> None:
    """KTD5/R10: a statement_timeout that fires INSIDE the function (blocked on
    the anchor lock) cancels the WHOLE statement — no partial application, not
    even the anchor bump. Distinguishable from an unknown outcome by SQLSTATE
    57014 (query_canceled), which the findings report records as the
    timeout-vs-indeterminate distinction."""
    import psycopg  # noqa: PLC0415

    _reset_rows(spike_pg)
    blocker = _connect(spike_pg, autocommit=False)
    try:
        with blocker.cursor() as cur:
            cur.execute(GRANT_TRANSITION_SQL, (_ARTIFACT, "blocker", "EXCLUSIVE", 0))  # anchor lock held
        with _connect(spike_pg, autocommit=True, statement_timeout_ms=300) as victim:
            with pytest.raises(psycopg.errors.QueryCanceled):
                _arbitrate(victim, agent="victim", expected_version=1)
    finally:
        blocker.rollback()
        blocker.close()

    with _connect(spike_pg, autocommit=True) as conn:
        artifact, states = _snapshot(conn)
        anchor = _anchor_count(conn)
    assert artifact == (1, 0, None, None), "a cancelled arbitration mutated the artifact"
    assert states == [], "a cancelled arbitration left a grant behind"
    assert anchor == 0, "the whole statement must roll back — anchor bump included"


def test_paired_contrast_function_denies_under_forced_interleaving(spike_pg) -> None:
    """U2's core contrast: the SAME commit-across-a-blocked-peer shape that the
    naive statement ADMITTED is DENIED by the function. Each arm blocks at its
    own construction's serialization point (the naive arm on the artifact row,
    the function at its first anchor write — the naive arm's absence from the
    anchor chain IS its defect); in both, the blocked statement is provably
    in-flight across the peer's EXCLUSIVE grant commit. This test also carries
    R9's deterministic lock-wait observation for the suite."""
    inconclusive: list[str] = []
    for _attempt in range(_U2_ATTEMPTS):
        _reset_rows(spike_pg)
        harness = ProcessRaceHarness(timeout_sec=60.0)
        result = harness.race(
            [
                ContenderSpec(_u3_grant_locker_contender, args=(spike_pg,)),
                ContenderSpec(_u3_function_contender, args=(spike_pg,)),
            ]
        )
        for outcome in result.outcomes:
            if outcome.error is not None:
                raise outcome.error
        locker = result.outcomes[0].value
        fn_arm = result.outcomes[1].value

        if fn_arm.get("outcome") == "unknown":
            inconclusive.append(f"driver error {fn_arm['error_type']} (unknown outcome)")
            continue
        if not locker["observed_blocked"]:
            inconclusive.append("function backend never observed in a Lock wait")
            continue

        # The forcing held: the function was blocked across the grant commit,
        # its post-wait statements took fresh snapshots, and the grant leg saw
        # the committed EXCLUSIVE — the deny the naive construction cannot make.
        assert fn_arm["outcome"] == OUTCOME_OTHER_HOLDER, fn_arm
        with _connect(spike_pg, autocommit=True) as conn:
            artifact, states = _snapshot(conn)
        assert artifact == (1, 0, None, None), "the denied function arm left a trace"
        assert states == [(_AGENT_A, "EXCLUSIVE", 0)]
        return
    pytest.fail(
        "INCONCLUSIVE: the forced interleaving could not be established in "
        f"{_U2_ATTEMPTS} attempts ({'; '.join(inconclusive)}). Rerun the spike."
    )


def test_mutant_without_grant_leg_admits_what_the_real_function_denies(spike_pg) -> None:
    """The Verification Contract's teeth check: with the grant leg disabled the
    other_holder scenario flips — the mutant ADMITS across a peer's EXCLUSIVE
    grant where the real function denies. A spike that cannot fail proves
    nothing; this proves it can."""
    with _connect(spike_pg, autocommit=False) as conn:
        with conn.cursor() as cur:
            cur.execute(build_mutant_arbitration_ddl())
        conn.commit()
        _reset_rows(spike_pg)
        _grant(conn, "holder", "EXCLUSIVE", 0)

        outcome, _current = _arbitrate(conn, agent="writer", expected_version=1)
        assert outcome == OUTCOME_OTHER_HOLDER, "the real function must deny across the peer grant"
        with conn.cursor() as cur:
            cur.execute(MUTANT_ARBITRATE_SQL, (_ARTIFACT, "writer", 1, "mutant-c", "mutant-t"))
            mutant_outcome, mutant_version = cur.fetchone()
        conn.commit()
    assert (mutant_outcome, mutant_version) == (OUTCOME_WIN, 2), (
        "the grant-leg mutant should have admitted — if it denies too, the other_holder "
        "scenarios are not exercising the grant leg and the spike proves nothing"
    )
