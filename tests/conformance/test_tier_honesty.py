# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Drive the tier-honesty conformance kit over BOTH v1 descriptors (a row-shaped
and an object-shaped native-CAS binding) plus the mandatory negative controls.

The default run uses the in-memory fake substrate (real coordinator subprocess,
fake bytes+token store) so the coordinator-mediated value — invalidation before
act, the never-ship-a-store commit path — is exercised without credentials. The
``real_substrate``-marked tests re-run the substrate-agnostic native-CAS arm
against a REAL Postgres and a REAL S3 (skip unless ``CCS_TEST_PG_DSN`` /
``CCS_REAL_S3_BUCKET`` are set); they are deselected from the default ``pytest -q``
run by the ``real_substrate`` marker.
"""

from __future__ import annotations

import os

import pytest

from ccs.adapters.claude_code.lifecycle import LifecycleConfig
from tests.conformance.substrate_conformance import (
    CoordinatorHarness,
    InMemoryBinding,
    assert_abort_after_partial_visibility,
    assert_coordinator_retention_empty,
    assert_detect_only_silent_lost_update,
    assert_forward_only_honest,
    assert_rejects_split_comparand,
    assert_split_view_is_rejected,
    run_native_cas_conformance,
)


@pytest.fixture
def fast_cfg() -> LifecycleConfig:
    """Coordinator config tuned for fast tests (no idle shutdown)."""
    return LifecycleConfig(
        idle_shutdown_sec=0,
        sweep_interval_sec=0.1,
        notice_evict_max_age_sec=1.0,
        port_file_retry_attempts=20,
        port_file_retry_interval_sec=0.05,
        connect_retry_attempts=10,
        connect_retry_interval_sec=0.05,
    )


@pytest.fixture
def harness(tmp_path, fast_cfg: LifecycleConfig):
    """A coordinator harness: mints agent identities on ONE root, torn down after."""
    handle = CoordinatorHarness(tmp_path, fast_cfg)
    try:
        yield handle
    finally:
        handle.close()


# ===========================================================================
# Default arm — the in-memory fake over BOTH v1 descriptors (row + object).
# ===========================================================================


@pytest.mark.parametrize("arm", ["row", "object"])
def test_native_cas_conformance_fake(arm: str, harness: CoordinatorHarness) -> None:
    """Each v1 native-CAS descriptor passes: (i) the bare CAS arbitrates one
    winner, (ii) a peer commit denies the other's act before the substrate CAS,
    and the commit path never ships a store."""
    run_native_cas_conformance(InMemoryBinding(arm), harness)


def test_abort_after_partial_visibility_reconciles(harness: CoordinatorHarness) -> None:
    """A commit whose write LANDED but whose ack aborted reconciles by
    token-identity (CONVERGE) — Unit-5 Case 3 verified end-to-end."""
    assert_abort_after_partial_visibility(InMemoryBinding("object"), harness)


# ===========================================================================
# Negative controls — the kit must REJECT an overclaiming binding.
# ===========================================================================


def test_split_comparand_conforming_view_passes() -> None:
    """A conforming (single-read) view passes the read-pair-consistency control."""
    assert_rejects_split_comparand("object")


def test_split_comparand_must_fail() -> None:
    """MANDATORY: a deliberately-split (bytes from read A, token from read B) view
    FAILS the kit — the PR-#107 lost update a version-CAS / NoLostUpdate check
    does not catch."""
    assert_split_view_is_rejected("object")


def test_detect_only_forced_silent_lost_update() -> None:
    """A detect-only substrate under a forced interleave silently loses an update
    (no raise), and its descriptor text is detection-only, never enforcement."""
    assert_detect_only_silent_lost_update()


def test_forward_only_tier_honest() -> None:
    """The forward-only tier is effect-ordering-only (no enforcement/CAS/rollback/
    dedup wording) and forbids a version_source — a pre-network descriptor arm."""
    assert_forward_only_honest()


# ===========================================================================
# never-ship-a-store — the coordinator RETENTION backstop (retain_versions=True).
# ===========================================================================


def test_never_ship_a_store_retention_backstop(tmp_path) -> None:
    """With retention ENABLED, the binding's hash-only registration leaves
    ``artifact_versions`` empty, while a content-bearing register does not (teeth)."""
    assert_coordinator_retention_empty(tmp_path)


# ===========================================================================
# real_substrate arm — the SAME native-CAS conformance against real backends.
# Deselected by default (the `real_substrate` marker); skip unless credentialed.
# ===========================================================================


_IT_PG_TABLE = "coherence_conformance_it"


class _PgConformanceBinding:
    """A ConformanceBinding over a real Postgres table (one row per ref)."""

    def __init__(self, dsn: str, table: str) -> None:
        self._dsn = dsn
        self._table = table

    @property
    def descriptor(self):  # noqa: ANN201 - CoherentRow's shipped descriptor
        from ccs.adapters.coherent_row import CoherentRow

        return CoherentRow(table=self._table, dsn=self._dsn).descriptor

    def seed(self, ref: str, data: bytes) -> None:
        import psycopg  # noqa: PLC0415

        with psycopg.connect(self._dsn) as conn, conn.cursor() as cur:
            cur.execute(
                f'INSERT INTO "{self._table}" (id, value) VALUES (%s, %s) '
                "ON CONFLICT (id) DO UPDATE SET value = EXCLUDED.value",
                (ref, data),
            )
            conn.commit()

    def foreign_write(self, ref: str, data: bytes) -> None:
        import psycopg  # noqa: PLC0415

        # A non-coordinated writer moves the row version (the trigger bumps it).
        with psycopg.connect(self._dsn) as conn, conn.cursor() as cur:
            cur.execute(f'UPDATE "{self._table}" SET value = %s WHERE id = %s', (data, ref))
            conn.commit()

    def make_view(self):  # noqa: ANN201
        from ccs.adapters.coherent_row import CoherentRow

        return CoherentRow(table=self._table, dsn=self._dsn)


@pytest.fixture
def real_pg():
    """Provision a real Postgres table + version trigger; yield ``(dsn, table)``."""
    dsn = os.environ.get("CCS_TEST_PG_DSN")
    if not dsn:
        pytest.skip("set CCS_TEST_PG_DSN (owner DSN) to run the real Postgres conformance arm")
    import psycopg  # noqa: PLC0415

    from ccs.adapters.coherent_row import provisioning_sql

    ddl = provisioning_sql(_IT_PG_TABLE)
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(f'DROP TABLE IF EXISTS "{_IT_PG_TABLE}" CASCADE')
        cur.execute(
            f'CREATE TABLE "{_IT_PG_TABLE}" '
            "(id text PRIMARY KEY, value bytea NOT NULL, version int NOT NULL DEFAULT 0)"
        )
        cur.execute(ddl.trigger_function)
        cur.execute(ddl.trigger_bindings)
        conn.commit()
    try:
        yield dsn, _IT_PG_TABLE
    finally:
        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS "{_IT_PG_TABLE}" CASCADE')
            conn.commit()


@pytest.mark.real_substrate
def test_real_postgres_native_cas_conformance(real_pg, harness: CoordinatorHarness) -> None:
    dsn, table = real_pg
    run_native_cas_conformance(_PgConformanceBinding(dsn, table), harness)


class _S3ConformanceBinding:
    """A ConformanceBinding over a real S3 bucket (one object per ref)."""

    def __init__(self, bucket: str, region: str | None) -> None:
        self._bucket = bucket
        self._region = region

    @property
    def descriptor(self):  # noqa: ANN201
        return self.make_view().descriptor

    def _object(self):  # noqa: ANN202
        from ccs.adapters.coherent_object import CoherentObject

        return CoherentObject(self._bucket, region=self._region)

    def seed(self, ref: str, data: bytes) -> None:
        from ccs.adapters.coherent_object import CREATE_IF_ABSENT

        self._object().cas_write(ref, expected_token=CREATE_IF_ABSENT, new_bytes=data)

    def foreign_write(self, ref: str, data: bytes) -> None:
        # A non-coordinated conditional put moves the object's ETag.
        obj = self._object()
        _bytes, token = obj.read(ref)
        obj.cas_write(ref, expected_token=token, new_bytes=data)

    def make_view(self):  # noqa: ANN201
        return self._object()


@pytest.fixture
def real_s3():
    """Yield a real S3 bucket + region, or skip if uncredentialed."""
    bucket = os.environ.get("CCS_REAL_S3_BUCKET")
    if not bucket:
        pytest.skip("set CCS_REAL_S3_BUCKET (a real, non-Moto bucket) to run the real S3 arm")
    pytest.importorskip("boto3")
    return bucket, os.environ.get("CCS_REAL_S3_REGION")


@pytest.mark.real_substrate
def test_real_s3_native_cas_conformance(real_s3, harness: CoordinatorHarness) -> None:
    bucket, region = real_s3
    run_native_cas_conformance(_S3ConformanceBinding(bucket, region), harness)
