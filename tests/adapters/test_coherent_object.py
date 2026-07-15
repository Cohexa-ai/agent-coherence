# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Driver-free unit tests for the CoherentObject S3 native-CAS binding.

The unit tests inject a FAKE S3 client (``FakeS3Client``) that scripts
``get_object`` / ``put_object`` and raises botocore-``ClientError``-shaped stubs
— no boto3, no real S3. The ``real_substrate`` integration tests below document
the same guarantees against a real S3 / S3-compatible endpoint; they are gated on
a bucket env var so they never run (or error) in a driver-free environment.
"""

from __future__ import annotations

import hashlib
import json
import os

import pytest

from ccs.adapters.coherent_object import (
    CREATE_IF_ABSENT,
    CoherentObject,
    ReconcileVerdict,
    S3PutOutcome,
    classify_put_exception,
    conditional_write_bucket_policy,
    least_privilege_iam_policy,
    s3_policy_docs,
)
from ccs.adapters.substrate import (
    CasConflict,
    CasUnknown,
    CasWritten,
    CoherenceSubstrate,
)
from ccs.core.exceptions import CoherenceError
from ccs.core.substrate import Tier


def _sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _md5_etag(data: bytes) -> str:
    """A quoted MD5 ETag, mimicking a general-purpose bucket's plaintext ETag."""
    return '"' + hashlib.md5(data).hexdigest() + '"'


# --- botocore-shaped stubs (no boto3) ------------------------------------------


class _FakeClientError(Exception):
    """A botocore ``ClientError``-shaped stub: carries ``.response['Error']['Code']``."""

    def __init__(self, code: str, message: str = "") -> None:
        self.response = {"Error": {"Code": code, "Message": message}}
        super().__init__(f"{code}: {message}")


class _FakeConnectionError(Exception):
    """A botocore connection-error-shaped stub: NO ``.response`` (ambiguous transport)."""


class _FakeBody:
    def __init__(self, data: bytes) -> None:
        self._data = data

    def read(self) -> bytes:
        return self._data


class FakeS3Client:
    """A minimal in-memory S3 client honoring If-Match / If-None-Match.

    ``objects`` maps key -> (bytes, etag). Set ``put_error`` / ``get_error`` to
    inject a scripted exception on the NEXT put / get (consumed once). Set
    ``next_etag`` to force the ETag the next put mints (used for opaque
    multipart/SSE-shaped ETags).
    """

    def __init__(self, objects: dict[str, tuple[bytes, str]] | None = None) -> None:
        self.objects: dict[str, tuple[bytes, str]] = dict(objects or {})
        self.put_calls: list[dict[str, object]] = []
        self.get_calls: list[dict[str, object]] = []
        self.put_error: Exception | None = None
        self.get_error: Exception | None = None
        self.next_etag: str | None = None

    def get_object(self, *, Bucket: str, Key: str) -> dict[str, object]:  # noqa: N803 (boto3 kwargs)
        self.get_calls.append({"Bucket": Bucket, "Key": Key})
        if self.get_error is not None:
            err, self.get_error = self.get_error, None
            raise err
        if Key not in self.objects:
            raise _FakeClientError("NoSuchKey", "missing")
        data, etag = self.objects[Key]
        return {"Body": _FakeBody(data), "ETag": etag}

    def put_object(  # noqa: N803 (boto3 kwargs)
        self,
        *,
        Bucket: str,
        Key: str,
        Body: bytes,
        IfMatch: str | None = None,
        IfNoneMatch: str | None = None,
    ) -> dict[str, object]:
        self.put_calls.append(
            {"Bucket": Bucket, "Key": Key, "Body": Body, "IfMatch": IfMatch, "IfNoneMatch": IfNoneMatch}
        )
        if self.put_error is not None:
            err, self.put_error = self.put_error, None
            raise err
        exists = Key in self.objects
        if IfNoneMatch == "*" and exists:
            raise _FakeClientError("PreconditionFailed", "object exists")
        if IfMatch is not None:
            if not exists:
                raise _FakeClientError("NoSuchKey", "raced delete")
            if self.objects[Key][1] != IfMatch:
                raise _FakeClientError("PreconditionFailed", "etag moved")
        etag = self.next_etag or _md5_etag(Body)
        self.next_etag = None
        self.objects[Key] = (Body, etag)
        return {"ETag": etag}


def _make(objects: dict[str, tuple[bytes, str]] | None = None) -> tuple[CoherentObject, FakeS3Client]:
    client = FakeS3Client(objects)
    return CoherentObject("test-bucket", client=client), client


# --- module import / descriptor / conformance ----------------------------------


def test_module_imports_without_boto3() -> None:
    # boto3 is not installed in this environment; the module (and an injected-client
    # construction) must work regardless — the driver import is deferred.
    import importlib.util

    assert importlib.util.find_spec("boto3") is None
    obj, _ = _make()
    assert isinstance(obj, CoherentObject)


def test_descriptor_is_native_cas_over_object_etag() -> None:
    obj, _ = _make()
    desc = obj.descriptor
    assert desc.tier is Tier.NATIVE_CAS
    assert desc.version_source == "object ETag"
    # Guarantee wording is derived from the tier (never hand-written per binding).
    assert "version-CAS" in desc.guarantee_text


def test_satisfies_coherence_substrate_protocol_structurally() -> None:
    obj, _ = _make()
    # runtime_checkable structural conformance — presence of descriptor/read/cas_write.
    assert isinstance(obj, CoherenceSubstrate)


def test_coordinator_content_is_none_and_flag_false() -> None:
    obj, _ = _make()
    # content=None exposure: the mixin reads BOTH the method (what to pass) and the
    # class flag (the honest signal) — never thread bytes coordinator-side.
    assert obj.coordinator_commit_content() is None
    assert CoherentObject.SENDS_CONTENT_TO_COORDINATOR is False


# --- read ----------------------------------------------------------------------


def test_read_returns_bytes_and_etag_from_one_response() -> None:
    obj, client = _make({"k": (b"hello", '"etag-1"')})
    data, token = obj.read("k")
    assert data == b"hello"
    assert token == '"etag-1"'
    # ONE get_object — never Head-then-Get for the (bytes, token) pair.
    assert len(client.get_calls) == 1


def test_read_absent_object_raises_keyerror() -> None:
    obj, _ = _make()
    with pytest.raises(KeyError):
        obj.read("missing")


# --- cas_write: happy update ---------------------------------------------------


def test_cas_write_update_wins_with_response_etag() -> None:
    obj, client = _make({"k": (b"v1", '"old"')})
    result = obj.cas_write("k", expected_token='"old"', new_bytes=b"v2")
    assert isinstance(result, CasWritten)
    # The token is the ETag from the put RESPONSE, never computed client-side.
    assert result.token == _md5_etag(b"v2")
    assert client.put_calls[-1]["IfMatch"] == '"old"'


def test_cas_write_update_is_single_request_put_not_multipart() -> None:
    obj, client = _make({"k": (b"v1", '"old"')})
    obj.cas_write("k", expected_token='"old"', new_bytes=b"v2")
    # Structural pin: exactly one put_object, no transfer-manager / multipart calls.
    assert len(client.put_calls) == 1
    assert not hasattr(client, "create_multipart_upload")


# --- cas_write: create (If-None-Match) -----------------------------------------


def test_cas_write_create_on_absent_succeeds_then_second_conflicts() -> None:
    obj, client = _make()
    first = obj.cas_write("k", expected_token=CREATE_IF_ABSENT, new_bytes=b"seed")
    assert isinstance(first, CasWritten)
    assert client.put_calls[-1]["IfNoneMatch"] == "*"
    # A second create → 412 → CasConflict (the object already exists).
    second = obj.cas_write("k", expected_token=CREATE_IF_ABSENT, new_bytes=b"again")
    assert isinstance(second, CasConflict)


# --- cas_write: typed classification (412 / 409 / 404 / transport) -------------


def test_cas_write_412_precondition_failed_is_conflict() -> None:
    obj, client = _make({"k": (b"v1", '"current"')})
    # Stale comparand: the object is at "current", we present "stale".
    result = obj.cas_write("k", expected_token='"stale"', new_bytes=b"v2")
    assert isinstance(result, CasConflict)


def test_stale_token_write_returns_cas_conflict() -> None:
    # The substrate leg returns the typed CasConflict on a stale token; the
    # mapping to the coordinator-versioned CasVersionConflict is the cross-agent
    # layer's job (tested in tests/adapters/test_substrate_cross_agent.py), so the
    # binding no longer carries a private mapping seam.
    obj, _ = _make({"k": (b"v1", '"current"')})
    assert isinstance(obj.cas_write("k", expected_token='"stale"', new_bytes=b"v2"), CasConflict)


def test_cas_write_409_classified_retryable_but_no_write_landed() -> None:
    obj, client = _make({"k": (b"v1", '"cur"')})
    client.put_error = _FakeClientError("ConditionalRequestConflict", "retry")
    result = obj.cas_write("k", expected_token='"cur"', new_bytes=b"v2")
    # Contract 3-way: no write landed → CasConflict...
    assert isinstance(result, CasConflict)
    # ...but the fine-grained classifier marks it RETRYABLE (re-read etag, retry).
    assert classify_put_exception(_FakeClientError("ConditionalRequestConflict")) is S3PutOutcome.RETRYABLE


def test_cas_write_404_raced_delete_is_conflict_not_recreate() -> None:
    obj, client = _make()  # object absent → If-Match put 404s
    result = obj.cas_write("k", expected_token='"gone"', new_bytes=b"v2")
    assert isinstance(result, CasConflict)
    assert classify_put_exception(_FakeClientError("NoSuchKey")) is S3PutOutcome.RACED_DELETE


def test_cas_write_transport_failure_is_unknown() -> None:
    obj, client = _make({"k": (b"v1", '"cur"')})
    client.put_error = _FakeConnectionError("connection reset")
    result = obj.cas_write("k", expected_token='"cur"', new_bytes=b"v2")
    # "may or may not have landed" — never a confirmed loss.
    assert isinstance(result, CasUnknown)


def test_cas_write_non_cas_error_propagates() -> None:
    # A modeled but non-CAS error (AccessDenied) must NOT be swallowed as a CAS
    # outcome — it is a configuration fault, so it propagates.
    obj, client = _make({"k": (b"v1", '"cur"')})
    client.put_error = _FakeClientError("AccessDenied", "no perms")
    with pytest.raises(_FakeClientError):
        obj.cas_write("k", expected_token='"cur"', new_bytes=b"v2")
    assert classify_put_exception(_FakeClientError("AccessDenied")) is None


def test_cas_write_rejects_sentinel_token() -> None:
    # An absent/blank token may never seed an If-Match comparand (the Sentinel rule).
    obj, _ = _make({"k": (b"v1", '"cur"')})
    with pytest.raises(CoherenceError):
        obj.cas_write("k", expected_token="   ", new_bytes=b"v2")


def test_cas_write_never_computes_missing_etag() -> None:
    # A put response without an ETag is unverifiable → fail closed, never mint a token.
    class _NoEtagClient(FakeS3Client):
        def put_object(self, **kwargs: object) -> dict[str, object]:  # type: ignore[override]
            super().put_object(**kwargs)  # type: ignore[arg-type]
            return {}  # response missing ETag

    obj = CoherentObject("b", client=_NoEtagClient({"k": (b"v1", '"cur"')}))
    with pytest.raises(CoherenceError):
        obj.cas_write("k", expected_token='"cur"', new_bytes=b"v2")


# --- the four reconciliation arms ----------------------------------------------


def test_reconcile_404_holds() -> None:
    # (i) absent object / delete-marker → HOLD; never auto re-create, never a match
    # against sha256(b"").
    obj, _ = _make()
    decision = obj.reconcile_after_unknown("k", expected_token='"T_old"', intended_hash=_sha256_hex(b""))
    assert decision.verdict is ReconcileVerdict.HOLD
    assert decision.observed_bytes is None and decision.observed_token is None
    assert decision.bump_fires is False


def test_reconcile_etag_unmoved_re_drives_under_if_match_t_old() -> None:
    # (ii) token unmoved → not landed → RE_DRIVE, only under If-Match=T_old.
    obj, _ = _make({"k": (b"old", '"T_old"')})
    decision = obj.reconcile_after_unknown(
        "k", expected_token='"T_old"', intended_hash=_sha256_hex(b"intended")
    )
    assert decision.verdict is ReconcileVerdict.RE_DRIVE
    assert decision.re_drive_token == '"T_old"'
    assert decision.bump_fires is False


def test_reconcile_moved_and_bytes_match_converges_and_bump_still_fires() -> None:
    # (iii) token moved AND bytes byte-identical → CONVERGE; the coordinator bump
    # STILL fires and the surface says "converged", never "landed".
    intended = b"the-intended-bytes"
    obj, _ = _make({"k": (intended, '"T_new"')})
    decision = obj.reconcile_after_unknown(
        "k", expected_token='"T_old"', intended_hash=_sha256_hex(intended)
    )
    assert decision.verdict is ReconcileVerdict.CONVERGE
    assert decision.bump_fires is True  # the load-bearing invariant
    assert decision.observed_token == '"T_new"'  # adopt the observed ETag as comparand
    assert "converged" in decision.summary
    assert "landed" not in decision.summary  # attribution is NOT claimed


def test_reconcile_moved_and_bytes_differ_conflicts() -> None:
    # (iv) token moved AND bytes differ → typed conflict; never re-drive.
    obj, _ = _make({"k": (b"a-peer-write", '"T_new"')})
    decision = obj.reconcile_after_unknown(
        "k", expected_token='"T_old"', intended_hash=_sha256_hex(b"my-intended")
    )
    assert decision.verdict is ReconcileVerdict.CONFLICT
    assert decision.re_drive_token is None
    assert decision.bump_fires is False


def test_never_converge_wedge_negative_control() -> None:
    # NEGATIVE CONTROL: in the my-write-landed world (token moved + bytes match), a
    # decision that REFUSED to converge would strand the coordinator bump and wedge
    # every peer (ViewWedged, no v1 repair-forward). Assert this impl converges AND
    # signals the bump — never HOLD/CONFLICT here.
    intended = b"landed-write"
    obj, _ = _make({"k": (intended, '"T_new"')})
    decision = obj.reconcile_after_unknown(
        "k", expected_token='"T_old"', intended_hash=_sha256_hex(intended)
    )
    assert decision.verdict is ReconcileVerdict.CONVERGE
    assert decision.bump_fires is True
    assert decision.verdict not in {ReconcileVerdict.HOLD, ReconcileVerdict.CONFLICT}


def test_reconcile_converges_under_opaque_multipart_shaped_etag() -> None:
    # The converge test keys on sha256(bytes) and adopts the ETag as OPAQUE, so an
    # SSE-KMS/multipart-shaped ETag (not a content digest) still converges safely —
    # never fail closed on an opaque ETag format when the bytes match.
    intended = b"converge-me"
    opaque_etag = '"a1b2c3d4-9"'  # multipart-shaped: not an MD5 of the bytes
    obj, _ = _make({"k": (intended, opaque_etag)})
    decision = obj.reconcile_after_unknown(
        "k", expected_token='"T_old"', intended_hash=_sha256_hex(intended)
    )
    assert decision.verdict is ReconcileVerdict.CONVERGE
    assert decision.observed_token == opaque_etag


def test_reconcile_read_transport_failure_propagates() -> None:
    # A transport failure DURING the reconcile read is not a decision — it must
    # propagate (retry the reconcile), never silently become HOLD/CONVERGE.
    obj, client = _make({"k": (b"v", '"T_new"')})
    client.get_error = _FakeConnectionError("reset mid-reconcile")
    with pytest.raises(_FakeConnectionError):
        obj.reconcile_after_unknown("k", expected_token='"T_old"', intended_hash=_sha256_hex(b"v"))


# --- no-op short-circuit -------------------------------------------------------


def test_noop_write_short_circuits_before_the_put() -> None:
    # intended hash == current hash → no put, no coordinator bump (no phantom advance).
    current = b"unchanged"
    obj, client = _make({"k": (current, '"cur"')})
    result = obj.cas_write_if_changed(
        "k", expected_token='"cur"', new_bytes=current, current_hash=_sha256_hex(current)
    )
    assert result is None  # the None signals: skip the coordinator bump too
    assert client.put_calls == []  # the put was never issued


def test_changed_write_falls_through_to_cas_write() -> None:
    obj, client = _make({"k": (b"old", '"cur"')})
    result = obj.cas_write_if_changed(
        "k", expected_token='"cur"', new_bytes=b"new", current_hash=_sha256_hex(b"old")
    )
    assert isinstance(result, CasWritten)
    assert len(client.put_calls) == 1


# --- least-privilege / bucket-policy doc helpers -------------------------------


def test_policy_helpers_return_verified_shape() -> None:
    docs = s3_policy_docs("my-bucket", "prefix/key")
    blob = json.dumps(docs)
    # The verified 2026-07 shape: conditional-write enforcement + multipart
    # exemption + the writer's GetObject grant.
    assert "s3:if-match" in blob
    assert "s3:ObjectCreationOperation" in blob
    assert "s3:GetObject" in blob


def test_writer_iam_denies_delete_and_grants_get_put() -> None:
    policy = least_privilege_iam_policy("b", "k")
    statements = policy["Statement"]
    allow = next(s for s in statements if s["Effect"] == "Allow")
    deny = next(s for s in statements if s["Effect"] == "Deny")
    assert set(allow["Action"]) == {"s3:GetObject", "s3:PutObject"}
    assert "s3:DeleteObject" in deny["Action"]


def test_bucket_policy_denies_unconditional_put() -> None:
    policy = conditional_write_bucket_policy("b")
    stmt = policy["Statement"][0]
    assert stmt["Effect"] == "Deny"
    assert stmt["Condition"]["Null"] == {"s3:if-match": "true"}


# --- integration (real S3 / S3-compatible; deselected by default) --------------
#
# Marked ``real_substrate`` (deselected via ``-m 'not real_substrate'``, added by
# the Unit-5 orchestrator) AND gated on a real bucket env var, so they never run
# — or error — in this driver-free environment. They document the same guarantees
# against a real endpoint; Moto/LocalStack are excluded (they serialize → false
# green). Written to run correctly against a real bucket, not runnable here.

real_substrate = pytest.mark.real_substrate

_REAL_BUCKET = os.environ.get("CCS_REAL_S3_BUCKET")
_needs_real_s3 = pytest.mark.skipif(
    not _REAL_BUCKET, reason="set CCS_REAL_S3_BUCKET to a real (non-Moto) S3 bucket to run"
)


@real_substrate
@_needs_real_s3
def test_real_concurrent_lost_update_one_winner_loser_converges() -> None:
    # Two writers read the same ETag; one wins the If-Match put, the loser 412s →
    # CasConflict; the loser re-reads and converges/re-derives against the winner.
    obj = CoherentObject(_REAL_BUCKET, region=os.environ.get("CCS_REAL_S3_REGION"))
    key = "coherence-it/lost-update"
    obj.cas_write(key, expected_token=CREATE_IF_ABSENT, new_bytes=b"seed")
    _seed_bytes, token = obj.read(key)
    winner = obj.cas_write(key, expected_token=token, new_bytes=b"winner")
    assert isinstance(winner, CasWritten)
    loser = obj.cas_write(key, expected_token=token, new_bytes=b"loser-stale")
    assert isinstance(loser, CasConflict)


@real_substrate
@_needs_real_s3
def test_real_timed_out_put_that_landed_converges_on_moved_bytes_match() -> None:
    # A put whose ack was lost but which DID land: the reconcile read shows the ETag
    # moved and the bytes match intended → CONVERGE, adopting the observed ETag; the
    # coordinator bump still fires (the leg is never stranded). Surface says
    # "converged", never "landed".
    obj = CoherentObject(_REAL_BUCKET, region=os.environ.get("CCS_REAL_S3_REGION"))
    key = "coherence-it/timeout-landed"
    obj.cas_write(key, expected_token=CREATE_IF_ABSENT, new_bytes=b"seed")
    _bytes, t_old = obj.read(key)
    intended = b"landed-despite-timeout"
    obj.cas_write(key, expected_token=t_old, new_bytes=intended)  # simulate: ack lost
    decision = obj.reconcile_after_unknown(key, expected_token=t_old, intended_hash=_sha256_hex(intended))
    assert decision.verdict is ReconcileVerdict.CONVERGE
    assert decision.bump_fires is True
    assert "landed" not in decision.summary


@real_substrate
@_needs_real_s3
def test_real_raced_delete_404_holds_caller_recreates() -> None:
    # A foreign DELETE during UNKNOWN → the If-Match re-read 404s → HOLD; re-create
    # is the CALLER's decision via CREATE_IF_ABSENT after reacquire, never automatic.
    obj = CoherentObject(_REAL_BUCKET, region=os.environ.get("CCS_REAL_S3_REGION"))
    key = "coherence-it/raced-delete"
    decision = obj.reconcile_after_unknown(key, expected_token='"stale"', intended_hash=_sha256_hex(b"x"))
    assert decision.verdict is ReconcileVerdict.HOLD
