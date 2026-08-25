# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""The fidelity contract of the deterministic S3-semantics fake.

``ccs.testing.s3_local.LocalS3Client`` models exactly the semantics the
``real_substrate`` arm verifies against a real bucket — versioning, conditional
writes, delete markers, legal hold, the ``"null"`` versionId. THIS suite is the
fake's own contract: every modeled behavior is pinned here, so a drift between
the fake and the documented subset fails loudly rather than false-greening a
consumer. Pure fake — no adapter import; the adapter-through-the-fake scenarios
live in ``tests/adapters/test_coherent_object.py``.
"""

from __future__ import annotations

import hashlib

import pytest

from ccs.testing.s3_local import LocalS3Client, S3LocalClientError


def _quoted_md5(data: bytes) -> str:
    return '"' + hashlib.md5(data).hexdigest() + '"'


def _code(excinfo: pytest.ExceptionInfo[S3LocalClientError]) -> str:
    return excinfo.value.response["Error"]["Code"]


def _versioned(*, object_lock: bool = False) -> LocalS3Client:
    client = LocalS3Client()
    client.create_bucket("b", versioned=True, object_lock=object_lock)
    return client


def _unversioned() -> LocalS3Client:
    client = LocalS3Client()
    client.create_bucket("b", versioned=False)
    return client


# --- bucket rules ----------------------------------------------------------------


def test_create_bucket_rejects_duplicates_and_empty_names() -> None:
    client = LocalS3Client()
    client.create_bucket("b", versioned=True)
    with pytest.raises(ValueError):
        client.create_bucket("b", versioned=False)
    with pytest.raises(ValueError):
        client.create_bucket("")


def test_object_lock_requires_versioning_the_s3_rule() -> None:
    client = LocalS3Client()
    with pytest.raises(ValueError):
        client.create_bucket("locked", versioned=False, object_lock=True)


def test_missing_bucket_is_nosuchbucket_on_every_op() -> None:
    client = LocalS3Client()
    with pytest.raises(S3LocalClientError) as excinfo:
        client.get_object(Bucket="ghost", Key="k")
    assert _code(excinfo) == "NoSuchBucket"
    with pytest.raises(S3LocalClientError) as excinfo:
        client.put_object(Bucket="ghost", Key="k", Body=b"x")
    assert _code(excinfo) == "NoSuchBucket"
    with pytest.raises(S3LocalClientError) as excinfo:
        client.delete_object(Bucket="ghost", Key="k")
    assert _code(excinfo) == "NoSuchBucket"


# --- the "null" sentinel on unversioned buckets -----------------------------------


def test_unversioned_put_and_get_report_the_null_literal() -> None:
    # Real S3 OMITS the header on a never-versioned bucket; the fake always
    # emits the literal so the sentinel path is exercised deterministically —
    # the adapter treats absent and "null" identically (both UNCONFIRMED).
    client = _unversioned()
    put = client.put_object(Bucket="b", Key="k", Body=b"v1")
    assert put["VersionId"] == "null"
    assert put["ETag"] == _quoted_md5(b"v1")
    got = client.get_object(Bucket="b", Key="k")
    assert got["VersionId"] == "null"
    assert got["Body"].read() == b"v1"
    assert got["ETag"] == _quoted_md5(b"v1")


def test_unversioned_overwrite_keeps_no_history() -> None:
    client = _unversioned()
    client.put_object(Bucket="b", Key="k", Body=b"v1")
    client.put_object(Bucket="b", Key="k", Body=b"v2")
    assert client.get_object(Bucket="b", Key="k")["Body"].read() == b"v2"
    # No history to pin: the only addressable version is the null sentinel.
    with pytest.raises(S3LocalClientError) as excinfo:
        client.get_object(Bucket="b", Key="k", VersionId="v000001")
    assert _code(excinfo) == "NoSuchVersion"


# --- version minting and pinned reads ---------------------------------------------


def test_versioned_put_mints_monotonic_deterministic_ids() -> None:
    client = _versioned()
    first = client.put_object(Bucket="b", Key="k", Body=b"v1")
    second = client.put_object(Bucket="b", Key="k", Body=b"v2")
    assert first["VersionId"] == "v000001"
    assert second["VersionId"] == "v000002"


def test_versioned_get_serves_latest_and_pinned_history() -> None:
    client = _versioned()
    first = client.put_object(Bucket="b", Key="k", Body=b"old")
    client.put_object(Bucket="b", Key="k", Body=b"new")
    assert client.get_object(Bucket="b", Key="k")["Body"].read() == b"new"
    pinned = client.get_object(Bucket="b", Key="k", VersionId=first["VersionId"])
    assert pinned["Body"].read() == b"old"
    assert pinned["ETag"] == _quoted_md5(b"old")
    assert pinned["VersionId"] == first["VersionId"]


def test_get_unknown_version_is_nosuchversion() -> None:
    client = _versioned()
    client.put_object(Bucket="b", Key="k", Body=b"v")
    with pytest.raises(S3LocalClientError) as excinfo:
        client.get_object(Bucket="b", Key="k", VersionId="v999999")
    assert _code(excinfo) == "NoSuchVersion"


def test_get_missing_key_is_nosuchkey() -> None:
    client = _versioned()
    with pytest.raises(S3LocalClientError) as excinfo:
        client.get_object(Bucket="b", Key="ghost")
    assert _code(excinfo) == "NoSuchKey"


# --- conditional writes -----------------------------------------------------------


def test_if_match_success_then_stale_comparand_412() -> None:
    client = _versioned()
    first = client.put_object(Bucket="b", Key="k", Body=b"v1")
    win = client.put_object(Bucket="b", Key="k", Body=b"v2", IfMatch=first["ETag"])
    assert win["VersionId"] == "v000002"
    with pytest.raises(S3LocalClientError) as excinfo:
        client.put_object(Bucket="b", Key="k", Body=b"v3", IfMatch=first["ETag"])
    assert _code(excinfo) == "PreconditionFailed"


def test_if_match_on_absent_key_is_404() -> None:
    client = _versioned()
    with pytest.raises(S3LocalClientError) as excinfo:
        client.put_object(Bucket="b", Key="ghost", Body=b"x", IfMatch='"e"')
    assert _code(excinfo) == "NoSuchKey"


def test_if_none_match_creates_once_then_412() -> None:
    client = _versioned()
    created = client.put_object(Bucket="b", Key="k", Body=b"seed", IfNoneMatch="*")
    assert created["VersionId"] == "v000001"
    with pytest.raises(S3LocalClientError) as excinfo:
        client.put_object(Bucket="b", Key="k", Body=b"again", IfNoneMatch="*")
    assert _code(excinfo) == "PreconditionFailed"


# --- delete markers ---------------------------------------------------------------


def test_versioned_delete_mints_marker_and_latest_get_404s() -> None:
    client = _versioned()
    put = client.put_object(Bucket="b", Key="k", Body=b"v1")
    deletion = client.delete_object(Bucket="b", Key="k")
    assert deletion["DeleteMarker"] is True
    assert deletion["VersionId"] == "v000002"  # the marker is its OWN version
    with pytest.raises(S3LocalClientError) as excinfo:
        client.get_object(Bucket="b", Key="k")
    assert _code(excinfo) == "NoSuchKey"
    # Pinned history serves THROUGH the marker (the restore-read guarantee).
    pinned = client.get_object(Bucket="b", Key="k", VersionId=put["VersionId"])
    assert pinned["Body"].read() == b"v1"


def test_delete_marker_current_if_match_404_and_if_none_match_creates() -> None:
    client = _versioned()
    put = client.put_object(Bucket="b", Key="k", Body=b"v1")
    client.delete_object(Bucket="b", Key="k")
    # A delete-marker-current key counts as ABSENT for both conditions.
    with pytest.raises(S3LocalClientError) as excinfo:
        client.put_object(Bucket="b", Key="k", Body=b"x", IfMatch=put["ETag"])
    assert _code(excinfo) == "NoSuchKey"
    recreated = client.put_object(Bucket="b", Key="k", Body=b"re-created", IfNoneMatch="*")
    assert recreated["VersionId"] == "v000003"
    assert client.get_object(Bucket="b", Key="k")["Body"].read() == b"re-created"


def test_get_of_a_delete_marker_version_is_method_not_allowed() -> None:
    client = _versioned()
    client.put_object(Bucket="b", Key="k", Body=b"v1")
    marker = client.delete_object(Bucket="b", Key="k")
    with pytest.raises(S3LocalClientError) as excinfo:
        client.get_object(Bucket="b", Key="k", VersionId=marker["VersionId"])
    assert _code(excinfo) == "MethodNotAllowed"


def test_deleting_the_delete_marker_undeletes() -> None:
    client = _versioned()
    client.put_object(Bucket="b", Key="k", Body=b"v1")
    marker = client.delete_object(Bucket="b", Key="k")
    undelete = client.delete_object(Bucket="b", Key="k", VersionId=marker["VersionId"])
    assert undelete["DeleteMarker"] is True
    assert client.get_object(Bucket="b", Key="k")["Body"].read() == b"v1"


def test_delete_specific_version_is_permanent() -> None:
    client = _versioned()
    old = client.put_object(Bucket="b", Key="k", Body=b"old")
    client.put_object(Bucket="b", Key="k", Body=b"new")
    client.delete_object(Bucket="b", Key="k", VersionId=old["VersionId"])
    with pytest.raises(S3LocalClientError) as excinfo:
        client.get_object(Bucket="b", Key="k", VersionId=old["VersionId"])
    assert _code(excinfo) == "NoSuchVersion"
    assert client.get_object(Bucket="b", Key="k")["Body"].read() == b"new"


def test_delete_missing_key_versioned_still_mints_marker() -> None:
    # The real-S3 shape: a versioned DELETE of a missing key still marks it.
    client = _versioned()
    deletion = client.delete_object(Bucket="b", Key="never-existed")
    assert deletion["DeleteMarker"] is True


def test_delete_missing_key_unversioned_is_a_silent_success() -> None:
    client = _unversioned()
    assert client.delete_object(Bucket="b", Key="never-existed") == {}


def test_unversioned_delete_is_permanent() -> None:
    client = _unversioned()
    client.put_object(Bucket="b", Key="k", Body=b"v1")
    resp = client.delete_object(Bucket="b", Key="k")
    assert "DeleteMarker" not in resp
    with pytest.raises(S3LocalClientError) as excinfo:
        client.get_object(Bucket="b", Key="k")
    assert _code(excinfo) == "NoSuchKey"


# --- legal hold -------------------------------------------------------------------


def test_legal_hold_requires_object_lock_invalid_request() -> None:
    client = _versioned()  # versioned but NO Object Lock
    put = client.put_object(Bucket="b", Key="k", Body=b"v")
    with pytest.raises(S3LocalClientError) as excinfo:
        client.put_object_legal_hold(
            Bucket="b", Key="k", VersionId=put["VersionId"], LegalHold={"Status": "ON"}
        )
    assert _code(excinfo) == "InvalidRequest"
    with pytest.raises(S3LocalClientError) as excinfo:
        client.get_object_legal_hold(Bucket="b", Key="k", VersionId=put["VersionId"])
    assert _code(excinfo) == "InvalidRequest"


def test_legal_hold_defaults_off_and_roundtrips() -> None:
    client = _versioned(object_lock=True)
    put = client.put_object(Bucket="b", Key="k", Body=b"v")
    vid = put["VersionId"]
    status = client.get_object_legal_hold(Bucket="b", Key="k", VersionId=vid)
    assert status["LegalHold"]["Status"] == "OFF"
    client.put_object_legal_hold(Bucket="b", Key="k", VersionId=vid, LegalHold={"Status": "ON"})
    status = client.get_object_legal_hold(Bucket="b", Key="k", VersionId=vid)
    assert status["LegalHold"]["Status"] == "ON"


def test_legal_hold_blocks_version_targeted_delete_until_released() -> None:
    client = _versioned(object_lock=True)
    put = client.put_object(Bucket="b", Key="k", Body=b"held")
    vid = put["VersionId"]
    client.put_object_legal_hold(Bucket="b", Key="k", VersionId=vid, LegalHold={"Status": "ON"})
    with pytest.raises(S3LocalClientError) as excinfo:
        client.delete_object(Bucket="b", Key="k", VersionId=vid)
    assert _code(excinfo) == "AccessDenied"  # the pin's teeth
    assert client.get_object(Bucket="b", Key="k", VersionId=vid)["Body"].read() == b"held"
    client.put_object_legal_hold(Bucket="b", Key="k", VersionId=vid, LegalHold={"Status": "OFF"})
    client.delete_object(Bucket="b", Key="k", VersionId=vid)
    with pytest.raises(S3LocalClientError):
        client.get_object(Bucket="b", Key="k", VersionId=vid)


def test_legal_hold_on_unknown_version_and_delete_marker() -> None:
    client = _versioned(object_lock=True)
    client.put_object(Bucket="b", Key="k", Body=b"v")
    with pytest.raises(S3LocalClientError) as excinfo:
        client.put_object_legal_hold(
            Bucket="b", Key="k", VersionId="v999999", LegalHold={"Status": "ON"}
        )
    assert _code(excinfo) == "NoSuchVersion"
    marker = client.delete_object(Bucket="b", Key="k")
    with pytest.raises(S3LocalClientError) as excinfo:
        client.put_object_legal_hold(
            Bucket="b", Key="k", VersionId=marker["VersionId"], LegalHold={"Status": "ON"}
        )
    assert _code(excinfo) == "MethodNotAllowed"


def test_legal_hold_rejects_a_malformed_status() -> None:
    client = _versioned(object_lock=True)
    put = client.put_object(Bucket="b", Key="k", Body=b"v")
    with pytest.raises(S3LocalClientError) as excinfo:
        client.put_object_legal_hold(
            Bucket="b", Key="k", VersionId=put["VersionId"], LegalHold={"Status": "MAYBE"}
        )
    assert _code(excinfo) == "MalformedXML"


# --- the lifecycle simulation seam ------------------------------------------------


def test_lifecycle_expire_noncurrent_reclaims_unheld_and_refuses_held() -> None:
    client = _versioned(object_lock=True)
    first = client.put_object(Bucket="b", Key="k", Body=b"first")
    second = client.put_object(Bucket="b", Key="k", Body=b"second")
    client.put_object(Bucket="b", Key="k", Body=b"current")
    client.put_object_legal_hold(
        Bucket="b", Key="k", VersionId=second["VersionId"], LegalHold={"Status": "ON"}
    )
    expired, held = client.lifecycle_expire_noncurrent("b", "k")
    assert expired == [first["VersionId"]]
    assert held == [second["VersionId"]]
    # The held version still serves; the reclaimed one is gone.
    assert (
        client.get_object(Bucket="b", Key="k", VersionId=second["VersionId"])["Body"].read()
        == b"second"
    )
    with pytest.raises(S3LocalClientError):
        client.get_object(Bucket="b", Key="k", VersionId=first["VersionId"])


def test_lifecycle_expire_on_missing_key_is_empty() -> None:
    client = _versioned()
    assert client.lifecycle_expire_noncurrent("b", "ghost") == ([], [])


# --- determinism ------------------------------------------------------------------


def _scripted_run() -> list[object]:
    """One fixed call sequence; returns every observable response value."""
    client = LocalS3Client()
    client.create_bucket("b", versioned=True, object_lock=True)
    out: list[object] = []
    first = client.put_object(Bucket="b", Key="k", Body=b"v1")
    out.append(first)
    out.append(client.put_object(Bucket="b", Key="k", Body=b"v2", IfMatch=first["ETag"]))
    got = client.get_object(Bucket="b", Key="k", VersionId=first["VersionId"])
    out.append({"ETag": got["ETag"], "VersionId": got["VersionId"], "Body": got["Body"].read()})
    out.append(client.delete_object(Bucket="b", Key="k"))
    return out


def test_two_fresh_instances_yield_byte_identical_responses() -> None:
    # No randomness, no clock: the determinism the e2e --baseline contract
    # (and any recorded fixture) rides on.
    assert _scripted_run() == _scripted_run()
