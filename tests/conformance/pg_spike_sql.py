# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""SQL for the Postgres three-leg boundary spike — test tree ONLY, nothing ships.

Research spike (plan ``docs/plans/2026-09-01-1824-test-pg-three-leg-spike-plan.md``,
origin BRD §9): can Postgres at READ COMMITTED host the full three-leg deny —
version-CAS, grant arbitration, read-generation fence — as ONE atomic step a
client invokes as a single statement? The construction under test is a
``SECURITY DEFINER`` plpgsql function plus an actually-WRITTEN per-artifact
anchor row; the deliberately naive single-statement construction is emitted too,
as the negative control that must lose the grant leg.

The load-bearing mechanism: at READ COMMITTED every *statement* inside a
VOLATILE plpgsql function takes a fresh snapshot, so the grant-leg read that
runs after blocking on a peer's anchor-row lock sees the peer's committed
grant — which the naive single statement structurally cannot, because its one
snapshot predates the wait. The anchor-row write forces every grant transition
into the same lock chain (including INSERTs of NEW agent_states rows, which no
row lock on existing rows can see).

Emission follows the ``provisioning_sql()`` frozen-dataclass style from
``ccs.adapters.coherent_row``: constants + a validated-identifier build step,
executed by the test fixture, never at package runtime. This module imports no
driver, so collection never requires the ``coherent-row`` extra.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from ccs.core.exceptions import STALE_READ_GENERATION_REASON, VERSION_MISMATCH_REASON

__all__ = [
    "ARBITRATE_SQL",
    "BUMP_OWNER_GENERATION_SQL",
    "GRANT_TRANSITION_SQL",
    "INSERT_ANCHOR_SQL",
    "INSERT_ARTIFACT_SQL",
    "LOCK_AGENT_STATE_ROW_SQL",
    "LOCK_ARTIFACT_ROW_SQL",
    "MUTANT_ARBITRATE_SQL",
    "MUTANT_FN_NAME",
    "NAIVE_COMMIT_CAS_SQL",
    "OUTCOME_CORRUPTION",
    "OUTCOME_OTHER_HOLDER",
    "OUTCOME_STALE_READ_GENERATION",
    "OUTCOME_VERSION_MISMATCH",
    "OUTCOME_WIN",
    "PAIR_READ_SQL",
    "READ_AGENT_STATES_SQL",
    "READ_ANCHOR_SQL",
    "READ_ARTIFACT_SQL",
    "RESET_ROWS_SQL",
    "SPIKE_SCHEMA",
    "SpikeSql",
    "build_bump_transition_ddl",
    "build_mutant_arbitration_ddl",
    "build_spike_sql",
]


_IDENTIFIER_RE = re.compile(r"^[a-z_][a-z0-9_]*$")


def _validate_identifier(name: str) -> str:
    """Refuse any identifier that could smuggle SQL into the emitted DDL."""
    if not _IDENTIFIER_RE.fullmatch(name):
        raise ValueError(f"unsafe SQL identifier for the spike schema: {name!r}")
    return name


SPIKE_SCHEMA = _validate_identifier("ccs_spike")


# Typed positional outcomes of the arbitration step (R3) — the shipped reason
# vocabulary. version_mismatch / stale_read_generation ALIAS the shipped
# constants so "matched verbatim" holds by construction. "other_holder" is
# pinned by tests/backend_conformance/kit.py:OTHER_HOLDER_REASON — importing
# the kit would drag the coordinator stack into this module's import graph, so
# it is mirrored here. win/corruption have no shipped reason constant
# (corruption ships as a raised error, not a ConflictDetail reason).
OUTCOME_WIN = "win"
OUTCOME_VERSION_MISMATCH = VERSION_MISMATCH_REASON
OUTCOME_OTHER_HOLDER = "other_holder"
OUTCOME_STALE_READ_GENERATION = STALE_READ_GENERATION_REASON
OUTCOME_CORRUPTION = "corruption"

# The M/E holder predicate — ONE shared fragment for the real grant leg and
# the naive control, so the two race arms always compare the same predicate.
_HOLDER_STATES_SQL = "('MODIFIED', 'EXCLUSIVE')"


@dataclass(frozen=True)
class SpikeSql:
    """The spike's one-time DDL, in apply order.

    ``schema_ddl`` creates the schema + three tables. ``functions_ddl`` creates
    both functions AND performs the privilege REVOKE/GRANT in the SAME emitted
    block — the fixture must apply it inside one transaction (R6, the origin's
    RD-79 defensive floor: no window where the function exists PUBLIC-executable).
    Deliberately no joined-script accessor: a single flattened script invites
    applying the function DDL outside one transaction, reopening that window.
    """

    schema_ddl: str
    functions_ddl: str


def build_spike_sql(schema: str = SPIKE_SCHEMA) -> SpikeSql:
    """Emit the spike schema, arbitration function, and grant-transition helper."""
    sch = _validate_identifier(schema)

    schema_ddl = f"""\
CREATE SCHEMA {sch};

-- version + owner_generation are CO-LOCATED on one row (KTD3): the pair-read
-- is a single-row SELECT, untearable by construction — splitting them would
-- manufacture a torn-pair proof obligation the spike does not need.
-- commit_token is present-but-unreconciled (R10): build-phase reconciliation
-- would re-read it after an unknown-outcome driver error; the spike only
-- proves the column travels in the same atomic step.
CREATE TABLE {sch}.artifacts (
    id               text PRIMARY KEY,
    version          bigint NOT NULL,
    owner_generation bigint NOT NULL DEFAULT 0,
    content_hash     text,
    commit_token     text
);

CREATE TABLE {sch}.agent_states (
    artifact        text NOT NULL,
    agent           text NOT NULL,
    state           text NOT NULL,
    read_generation bigint,
    PRIMARY KEY (artifact, agent)
);

-- The forcing-write target: every arbitration call and every grant transition
-- UPDATEs this artifact's anchor row FIRST, so they all serialize on one row
-- lock regardless of which agent_states rows they touch (KTD3).
CREATE TABLE {sch}.anchor (
    artifact      text PRIMARY KEY,
    forced_writes bigint NOT NULL DEFAULT 0
);"""

    arbitrate_sig = f"{sch}.spike_commit_cas(text, text, bigint, text, text)"
    transition_sig = f"{sch}.spike_grant_transition(text, text, text, bigint)"

    functions_ddl = f"""\
-- The construction under test (KTD1): one SECURITY DEFINER VOLATILE plpgsql
-- function owning all three legs, invoked as one SELECT. It runs INSIDE the
-- invoking statement's transaction (plpgsql cannot commit); with an autocommit
-- client the whole ladder is one self-contained transaction.
{_arbitration_fn_ddl(sch, "spike_commit_cas", _GRANT_LEG_SQL)}

-- The ONLY sanctioned way any test code changes a grant: the anchor
-- forced-write and the agent_states upsert in ONE transaction, SAME lock
-- order as the arbitration function (anchor first). KTD3's lock-chain
-- discipline applies to scaffolding too — a grant written outside this chain
-- would be invisible to a concurrently-arbitrating call and prove nothing.
CREATE FUNCTION {sch}.spike_grant_transition(
    p_artifact text,
    p_agent text,
    p_state text,
    p_read_generation bigint
)
RETURNS void
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path = {sch}, pg_temp
AS $spike$
BEGIN
    UPDATE anchor SET forced_writes = forced_writes + 1
        WHERE artifact = p_artifact;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'spike: no anchor row for artifact %', p_artifact;
    END IF;
    INSERT INTO agent_states (artifact, agent, state, read_generation)
        VALUES (p_artifact, p_agent, p_state, p_read_generation)
        ON CONFLICT (artifact, agent) DO UPDATE
            SET state = excluded.state, read_generation = excluded.read_generation;
END;
$spike$;

-- R6 (RD-79 defensive floor): revoke and re-grant in the SAME transaction as
-- creation, throwaway schema or not. PUBLIC gets no EXECUTE window, ever.
REVOKE ALL ON FUNCTION {arbitrate_sig} FROM PUBLIC;
REVOKE ALL ON FUNCTION {transition_sig} FROM PUBLIC;
GRANT EXECUTE ON FUNCTION {arbitrate_sig} TO CURRENT_USER;
GRANT EXECUTE ON FUNCTION {transition_sig} TO CURRENT_USER;"""

    return SpikeSql(schema_ddl=schema_ddl, functions_ddl=functions_ddl)


MUTANT_FN_NAME = "spike_commit_cas_mutant"


def build_mutant_arbitration_ddl(schema: str = SPIKE_SCHEMA) -> str:
    """The SAME arbitration function with the grant leg STRIPPED — the
    Verification Contract's teeth check: the other_holder scenarios must flip
    red under this mutant, or the spike cannot fail and proves nothing. Emitted
    from the same template as the real function so the two can never drift."""
    sch = _validate_identifier(schema)
    mutant_leg = "-- MUTANT: the grant-arbitration leg is deliberately removed."
    sig = f"{sch}.{MUTANT_FN_NAME}(text, text, bigint, text, text)"
    return (
        f"{_arbitration_fn_ddl(sch, MUTANT_FN_NAME, mutant_leg)}\n\n"
        f"REVOKE ALL ON FUNCTION {sig} FROM PUBLIC;\n"
        f"GRANT EXECUTE ON FUNCTION {sig} TO CURRENT_USER;"
    )


_GRANT_LEG_SQL = f"""\
-- Leg 2: grant arbitration. This statement takes a FRESH snapshot: if the
    -- anchor UPDATE above waited out a peer's grant-transition commit, the
    -- rows read here are the peer's COMMITTED rows — the mechanism the naive
    -- single-statement construction structurally lacks.
    PERFORM 1 FROM agent_states s
        WHERE s.artifact = p_artifact
          AND s.agent <> p_agent
          AND s.state IN {_HOLDER_STATES_SQL}
        FOR UPDATE;
    IF FOUND THEN
        outcome := '{OUTCOME_OTHER_HOLDER}'; current_version := v_version; RETURN;
    END IF;"""


def _arbitration_fn_ddl(sch: str, fn_name: str, grant_leg: str) -> str:
    """One template for the real arbitration function and its teeth-check mutant."""
    _validate_identifier(fn_name)
    return f"""\
CREATE FUNCTION {sch}.{fn_name}(
    p_artifact text,
    p_agent text,
    p_expected_version bigint,
    p_content_hash text,
    p_commit_token text,
    OUT outcome text,
    OUT current_version bigint
)
RETURNS record
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path = {sch}, pg_temp
AS $spike$
DECLARE
    v_version bigint;
    v_owner_generation bigint;
    v_read_generation bigint;
BEGIN
    -- Lock chain first: force a write on this artifact's anchor row. Acquiring
    -- it serializes this call against every concurrent grant transition —
    -- including INSERTs of NEW agent_states rows that row locks on existing
    -- rows cannot see. A DENIED call still commits this bump by design: the
    -- no-mutation guarantee is scoped to artifacts + agent_states.
    UPDATE anchor SET forced_writes = forced_writes + 1
        WHERE artifact = p_artifact;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'spike: no anchor row for artifact %', p_artifact;
    END IF;

    -- Pair-read: version + owner_generation from ONE single-row read (KTD3).
    -- STRICT: a missing artifact is a loud error, never a typed outcome.
    SELECT a.version, a.owner_generation
        INTO STRICT v_version, v_owner_generation
        FROM artifacts a WHERE a.id = p_artifact;

    -- Leg 1: version-CAS. Corruption (comparand AHEAD of stored) outranks
    -- everything; behind is the ordinary conflict. Checked before the grant
    -- read, so corruption structurally outranks other_holder (KTD8).
    IF p_expected_version > v_version THEN
        outcome := '{OUTCOME_CORRUPTION}'; current_version := v_version; RETURN;
    END IF;
    IF p_expected_version < v_version THEN
        outcome := '{OUTCOME_VERSION_MISMATCH}'; current_version := v_version; RETURN;
    END IF;

    {grant_leg}

    -- Leg 3: read-generation fence. Contract verbatim: an ABSENT
    -- read_generation (no row, or NULL) is ADMITTED — version-CAS arbitrates;
    -- ONLY a PRESENT-and-superseded read_generation is rejected. Reachable
    -- only with the version UNMOVED, which is the shipped precedence pin.
    SELECT s.read_generation INTO v_read_generation
        FROM agent_states s
        WHERE s.artifact = p_artifact AND s.agent = p_agent
        FOR UPDATE;
    IF v_read_generation IS NOT NULL AND v_read_generation < v_owner_generation THEN
        outcome := '{OUTCOME_STALE_READ_GENERATION}'; current_version := v_version; RETURN;
    END IF;

    -- WIN: payload, commit token, and the committer's grant land in the SAME
    -- transaction as the decision.
    UPDATE artifacts
        SET version = version + 1,
            content_hash = p_content_hash,
            commit_token = p_commit_token
        WHERE id = p_artifact;
    INSERT INTO agent_states (artifact, agent, state, read_generation)
        VALUES (p_artifact, p_agent, 'MODIFIED', v_owner_generation)
        ON CONFLICT (artifact, agent) DO UPDATE
            SET state = excluded.state, read_generation = excluded.read_generation;
    outcome := '{OUTCOME_WIN}'; current_version := v_version + 1;
END;
$spike$;"""


# --- client-side statements (positional %s params, order in the comment) -----

# (artifact, agent, expected_version, content_hash, commit_token)
ARBITRATE_SQL = (
    f"SELECT outcome, current_version FROM {SPIKE_SCHEMA}.spike_commit_cas(%s, %s, %s, %s, %s)"
)

# Same signature against the teeth-check mutant (grant leg removed).
# (artifact, agent, expected_version, content_hash, commit_token)
MUTANT_ARBITRATE_SQL = (
    f"SELECT outcome, current_version FROM {SPIKE_SCHEMA}.{MUTANT_FN_NAME}(%s, %s, %s, %s, %s)"
)

# (artifact, agent, state, read_generation)
GRANT_TRANSITION_SQL = f"SELECT {SPIKE_SCHEMA}.spike_grant_transition(%s, %s, %s, %s)"

# The pair-read accessor: BOTH halves from one single-row statement (KTD3).
# (artifact)
PAIR_READ_SQL = f"SELECT version, owner_generation FROM {SPIKE_SCHEMA}.artifacts WHERE id = %s"

# The negative control (U2, §9.2's predicted loser): version-CAS and the grant
# check assembled into ONE statement whose single snapshot predates any lock
# wait — after blocking, the row re-check sees the peer's committed tuple but
# the NOT EXISTS subquery keeps the stale statement snapshot.
# (content_hash, commit_token, artifact, expected_version, agent)
NAIVE_COMMIT_CAS_SQL = f"""\
UPDATE {SPIKE_SCHEMA}.artifacts a
   SET version = a.version + 1, content_hash = %s, commit_token = %s
 WHERE a.id = %s
   AND a.version = %s
   AND NOT EXISTS (
       SELECT 1 FROM {SPIKE_SCHEMA}.agent_states s
        WHERE s.artifact = a.id
          AND s.agent <> %s
          AND s.state IN {_HOLDER_STATES_SQL})"""

# U2's forcing scaffold: take the artifact's row lock WITHOUT bumping the
# version (no trigger mints versions in the spike schema), so a blocked naive
# statement still passes its version qual on the EvalPlanQual re-check.
# (artifact)
LOCK_ARTIFACT_ROW_SQL = (
    f"UPDATE {SPIKE_SCHEMA}.artifacts SET content_hash = content_hash WHERE id = %s"
)

# --- seeding / inspection scaffolding (single-threaded, pre-barrier — KTD4) --

# (id, version, owner_generation)
INSERT_ARTIFACT_SQL = (
    f"INSERT INTO {SPIKE_SCHEMA}.artifacts (id, version, owner_generation) VALUES (%s, %s, %s)"
)

# (artifact)
INSERT_ANCHOR_SQL = f"INSERT INTO {SPIKE_SCHEMA}.anchor (artifact) VALUES (%s)"

# Fence-precondition scaffold: supersede outstanding read_generations by
# bumping the artifact's owner_generation, as the shipped sweep/invalidate
# would. Routed through the SAME anchor lock chain as every other conflicting
# transition (see build_bump_transition_ddl below): the shipped contract
# serializes the generation-bumping sweep with the atomic mutations, and the
# scaffold models that discipline so no test can accidentally rely on an
# unserialized bump.
# (artifact)
BUMP_OWNER_GENERATION_SQL = f"SELECT {SPIKE_SCHEMA}.spike_bump_owner_generation(%s)"

# (artifact)
READ_ARTIFACT_SQL = (
    f"SELECT version, owner_generation, content_hash, commit_token "
    f"FROM {SPIKE_SCHEMA}.artifacts WHERE id = %s"
)

# (artifact)
READ_AGENT_STATES_SQL = (
    f"SELECT agent, state, read_generation FROM {SPIKE_SCHEMA}.agent_states "
    f"WHERE artifact = %s ORDER BY agent"
)

# (artifact)
READ_ANCHOR_SQL = f"SELECT forced_writes FROM {SPIKE_SCHEMA}.anchor WHERE artifact = %s"

# Per-attempt reset for the cross-process cases: rows only, schema untouched.
RESET_ROWS_SQL = f"TRUNCATE {SPIKE_SCHEMA}.artifacts, {SPIKE_SCHEMA}.agent_states, {SPIKE_SCHEMA}.anchor"

# Timeout-forcing scaffold: a raw row lock on one agent_states row that stays
# deliberately OUTSIDE the anchor chain, so a victim arbitration call EXECUTES
# its anchor bump and blocks only at the grant leg's FOR UPDATE — giving the
# statement_timeout cancel a real intra-function write to roll back.
# (artifact, agent)
LOCK_AGENT_STATE_ROW_SQL = (
    f"SELECT 1 FROM {SPIKE_SCHEMA}.agent_states WHERE artifact = %s AND agent = %s FOR UPDATE"
)


def build_bump_transition_ddl(schema: str = SPIKE_SCHEMA) -> str:
    """The generation-bump scaffold as a function in the anchor lock chain.

    Scaffolding, not the construction under test — but the shipped contract
    requires the generation-bumping sweep serialized under the SAME lock as
    the atomic mutations, so even seeding models the discipline a build must
    honor. Applied by the fixture in the same provisioning transaction as the
    other functions (R6's revoke/grant pairing included).
    """
    sch = _validate_identifier(schema)
    sig = f"{sch}.spike_bump_owner_generation(text)"
    return f"""\
CREATE FUNCTION {sch}.spike_bump_owner_generation(p_artifact text)
RETURNS void
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path = {sch}, pg_temp
AS $spike$
BEGIN
    UPDATE anchor SET forced_writes = forced_writes + 1
        WHERE artifact = p_artifact;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'spike: no anchor row for artifact %', p_artifact;
    END IF;
    UPDATE artifacts SET owner_generation = owner_generation + 1
        WHERE id = p_artifact;
END;
$spike$;

REVOKE ALL ON FUNCTION {sig} FROM PUBLIC;
GRANT EXECUTE ON FUNCTION {sig} TO CURRENT_USER;"""
