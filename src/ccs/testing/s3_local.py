# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""A deterministic, in-process S3-semantics fake (the Workspace-Versioning arm).

:class:`LocalS3Client` duck-types the exact boto3 S3 client subset
``ccs.adapters.coherent_object.CoherentObject`` uses — ``get_object`` /
``put_object`` / ``delete_object`` / ``put_object_legal_hold`` /
``get_object_legal_hold`` — so the shipped binding runs against it unchanged via
its client-injection seam::

    from ccs.adapters.coherent_object import CoherentObject
    from ccs.testing.s3_local import LocalS3Client

    client = LocalS3Client()
    client.create_bucket("demo", versioned=True, object_lock=True)
    obj = CoherentObject("demo", client=client)

Pure Python, zero boto3 dependency, zero randomness, zero clock reads: version
ids are minted from a per-instance monotonic counter (``v000001``, ...), ETags
are quoted MD5 digests (a general-purpose bucket's plaintext-put shape), so two
identical call sequences on two fresh instances yield byte-identical responses
— the determinism the e2e example's ``--baseline`` contract rides on.

Modeled semantics (the fidelity suite ``tests/testing/test_s3_local_fidelity.py``
is the contract):

- buckets WITH and WITHOUT versioning; Object Lock (legal hold) only on
  versioned buckets, as real S3 requires;
- conditional PUT: ``IfMatch`` (success / 412 ``PreconditionFailed`` on a moved
  ETag / 404 ``NoSuchKey`` when absent or delete-marker-current) and
  ``IfNoneMatch="*"`` (create-once; a delete-marker-current key counts as
  absent);
- DELETE without ``VersionId`` mints a delete marker on a versioned bucket
  (history and pins survive) and permanently removes on an unversioned one;
  DELETE with ``VersionId`` permanently removes that one version — REFUSED with
  ``AccessDenied`` while the version carries a legal hold (the pin's teeth);
- version-pinned GET serves history even when the latest is a delete marker;
  GET of a delete-marker version is ``MethodNotAllowed`` (the real-S3 shape);
- the ``"null"`` versionId: on an unversioned bucket every put/get response
  reports ``VersionId == "null"``. Real S3 OMITS the header on a never-versioned
  bucket and reports the literal ``"null"`` on a suspended one; this fake always
  emits the literal so the sentinel path is exercised deterministically — the
  adapter treats absent and ``"null"`` identically (both UNCONFIRMED), so no
  adapter-visible behavior diverges.

Deliberately NOT modeled: the 409 ``ConditionalRequestConflict`` transient (a
real-S3-internal race this deterministic fake never reaches — inject it with a
scripted stub) and multipart/transfer-manager writes (the binding is
single-request-put by structure). The real-arm ``real_substrate`` suite remains
the fidelity oracle; this fake models the verified subset only.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from typing import Any, Final

__all__ = [
    "LocalS3Client",
    "S3LocalClientError",
]

#: The literal versionId S3 associates with an object that has no real version.
#: Kept as the raw S3 wire literal (the adapter carries its own constant for the
#: same protocol value; the fidelity suite locks the two together).
_NULL_VERSION_ID: Final[str] = "null"


class S3LocalClientError(Exception):
    """A botocore ``ClientError``-shaped error: carries ``.response['Error']['Code']``.

    Duck-typed on the ``.response`` payload — exactly what
    ``coherent_object._s3_error_code`` classifies — so the fake and a real
    botocore ``ClientError`` are indistinguishable to the adapter.
    """

    def __init__(self, code: str, message: str = "") -> None:
        self.response: dict[str, Any] = {"Error": {"Code": code, "Message": message}}
        super().__init__(f"{code}: {message}")


class _LocalBody:
    """The streaming-body stand-in: one ``read()`` returning the full bytes."""

    def __init__(self, data: bytes) -> None:
        self._data = data

    def read(self) -> bytes:
        return self._data


def _quoted_md5(data: bytes) -> str:
    """A general-purpose bucket's plaintext-put ETag: the quoted MD5 hex digest."""
    return '"' + hashlib.md5(data).hexdigest() + '"'


@dataclass
class _Version:
    """One entry in a key's version stack (a real version or a delete marker)."""

    version_id: str
    body: bytes | None  # None iff delete marker
    etag: str | None  # None iff delete marker
    is_delete_marker: bool = False
    legal_hold: bool = False


@dataclass
class _Bucket:
    versioned: bool
    object_lock: bool
    # key -> chronologically appended versions; the LATEST is the last entry.
    versions: dict[str, list[_Version]] = field(default_factory=dict)


class LocalS3Client:
    """The deterministic multi-bucket S3 fake, shaped as ONE boto3-style client.

    Construct once, :meth:`create_bucket` per bucket, then hand the instance to
    ``CoherentObject(bucket, client=...)``. Call logs (``get_calls`` /
    ``put_calls`` / ``delete_calls``) record every request for
    one-read-per-pair assertions. Single-threaded by design — determinism is
    the point; concurrency is exercised by interleaving calls.
    """

    def __init__(self) -> None:
        self._buckets: dict[str, _Bucket] = {}
        self._version_counter = 0
        self.get_calls: list[dict[str, Any]] = []
        self.put_calls: list[dict[str, Any]] = []
        self.delete_calls: list[dict[str, Any]] = []

    # --- test-harness surface (not part of the boto3 duck-type) ---------------

    def create_bucket(
        self, name: str, *, versioned: bool = False, object_lock: bool = False
    ) -> None:
        """Create one bucket. ``object_lock`` requires ``versioned`` (the S3 rule)."""
        if not name:
            raise ValueError("bucket name must be non-empty")
        if name in self._buckets:
            raise ValueError(f"bucket {name!r} already exists")
        if object_lock and not versioned:
            raise ValueError(
                "Object Lock requires bucket versioning (the S3 rule); "
                "create with versioned=True"
            )
        self._buckets[name] = _Bucket(versioned=versioned, object_lock=object_lock)

    def lifecycle_expire_noncurrent(self, bucket: str, key: str) -> tuple[list[str], list[str]]:
        """Simulate a lifecycle rule expiring every NONCURRENT version of ``key``.

        The Unit-6 pin-teeth seam: returns ``(expired_ids, held_ids)`` — a
        version under legal hold is REFUSED (survives, listed in ``held_ids``),
        exactly the semantics a pinned checkpoint's restorability rides on.
        Delete markers are never the current version's peer here: the CURRENT
        (last) entry is kept, every earlier entry is a candidate.
        """
        b = self._require_bucket(bucket)
        stack = b.versions.get(key)
        if not stack:
            return ([], [])
        expired: list[str] = []
        held: list[str] = []
        survivors: list[_Version] = []
        for version in stack[:-1]:
            if version.legal_hold:
                held.append(version.version_id)
                survivors.append(version)
            else:
                expired.append(version.version_id)
        survivors.append(stack[-1])
        b.versions[key] = survivors
        return (expired, held)

    # --- the boto3-shaped duck-type -------------------------------------------

    def get_object(  # noqa: N803 (boto3 kwargs)
        self, *, Bucket: str, Key: str, VersionId: str | None = None
    ) -> dict[str, Any]:
        self.get_calls.append({"Bucket": Bucket, "Key": Key, "VersionId": VersionId})
        b = self._require_bucket(Bucket)
        stack = b.versions.get(Key)
        if VersionId is not None:
            version = self._find_version(stack, VersionId)
            if version is None:
                raise S3LocalClientError("NoSuchVersion", "no such version")
            if version.is_delete_marker:
                # The real-S3 shape: a GET addressed AT a delete marker is 405.
                raise S3LocalClientError("MethodNotAllowed", "version is a delete marker")
            assert version.body is not None and version.etag is not None
            return self._read_response(b, version)
        if not stack or stack[-1].is_delete_marker:
            raise S3LocalClientError("NoSuchKey", "missing or delete-marker-current")
        return self._read_response(b, stack[-1])

    def put_object(  # noqa: N803 (boto3 kwargs)
        self,
        *,
        Bucket: str,
        Key: str,
        Body: bytes,
        IfMatch: str | None = None,
        IfNoneMatch: str | None = None,
    ) -> dict[str, Any]:
        self.put_calls.append(
            {
                "Bucket": Bucket,
                "Key": Key,
                "Body": Body,
                "IfMatch": IfMatch,
                "IfNoneMatch": IfNoneMatch,
            }
        )
        b = self._require_bucket(Bucket)
        stack = b.versions.get(Key)
        current = stack[-1] if stack else None
        exists = current is not None and not current.is_delete_marker
        if IfNoneMatch == "*" and exists:
            raise S3LocalClientError("PreconditionFailed", "object exists")
        if IfMatch is not None:
            if not exists:
                # Absent AND delete-marker-current both fail If-Match as 404.
                raise S3LocalClientError("NoSuchKey", "raced delete / delete marker")
            assert current is not None
            if current.etag != IfMatch:
                raise S3LocalClientError("PreconditionFailed", "etag moved")
        body = bytes(Body)
        version = _Version(
            version_id=self._mint_version_id(b),
            body=body,
            etag=_quoted_md5(body),
        )
        self._append_version(b, Key, version)
        return {"ETag": version.etag, "VersionId": version.version_id}

    def delete_object(  # noqa: N803 (boto3 kwargs)
        self, *, Bucket: str, Key: str, VersionId: str | None = None
    ) -> dict[str, Any]:
        self.delete_calls.append({"Bucket": Bucket, "Key": Key, "VersionId": VersionId})
        b = self._require_bucket(Bucket)
        if VersionId is not None:
            return self._delete_specific_version(b, Key, VersionId)
        if not b.versioned:
            # Permanent removal; deleting a missing key is still a success (S3).
            b.versions.pop(Key, None)
            return {}
        # Versioned: mint a delete marker — even for a missing key (S3 does).
        marker = _Version(
            version_id=self._mint_version_id(b),
            body=None,
            etag=None,
            is_delete_marker=True,
        )
        self._append_version(b, Key, marker)
        return {"DeleteMarker": True, "VersionId": marker.version_id}

    def put_object_legal_hold(  # noqa: N803 (boto3 kwargs)
        self, *, Bucket: str, Key: str, VersionId: str, LegalHold: dict[str, Any]
    ) -> dict[str, Any]:
        b = self._require_bucket(Bucket)
        version = self._require_lockable_version(b, Key, VersionId)
        status = LegalHold.get("Status")
        if status not in ("ON", "OFF"):
            raise S3LocalClientError("MalformedXML", f"bad LegalHold Status {status!r}")
        version.legal_hold = status == "ON"
        return {}

    def get_object_legal_hold(  # noqa: N803 (boto3 kwargs)
        self, *, Bucket: str, Key: str, VersionId: str
    ) -> dict[str, Any]:
        b = self._require_bucket(Bucket)
        version = self._require_lockable_version(b, Key, VersionId)
        return {"LegalHold": {"Status": "ON" if version.legal_hold else "OFF"}}

    # --- internals ------------------------------------------------------------

    def _require_bucket(self, name: str) -> _Bucket:
        bucket = self._buckets.get(name)
        if bucket is None:
            raise S3LocalClientError("NoSuchBucket", f"no bucket {name!r}")
        return bucket

    def _mint_version_id(self, bucket: _Bucket) -> str:
        """Deterministic pointer minting; the null literal on unversioned buckets."""
        if not bucket.versioned:
            return _NULL_VERSION_ID
        self._version_counter += 1
        return f"v{self._version_counter:06d}"

    def _append_version(self, bucket: _Bucket, key: str, version: _Version) -> None:
        if not bucket.versioned:
            # No history on an unversioned bucket: the new state REPLACES it.
            bucket.versions[key] = [version]
            return
        bucket.versions.setdefault(key, []).append(version)

    @staticmethod
    def _find_version(stack: list[_Version] | None, version_id: str) -> _Version | None:
        if not stack:
            return None
        for version in stack:
            if version.version_id == version_id:
                return version
        return None

    def _delete_specific_version(
        self, bucket: _Bucket, key: str, version_id: str
    ) -> dict[str, Any]:
        stack = bucket.versions.get(key)
        version = self._find_version(stack, version_id)
        if version is None:
            raise S3LocalClientError("NoSuchVersion", "no such version")
        if version.legal_hold:
            # The pin's teeth: a held version is undeletable until released.
            raise S3LocalClientError("AccessDenied", "version is under legal hold")
        assert stack is not None
        stack.remove(version)
        if not stack:
            del bucket.versions[key]
        if version.is_delete_marker:
            # Removing a delete marker is the S3 "undelete": report the marker.
            return {"DeleteMarker": True, "VersionId": version.version_id}
        return {"VersionId": version.version_id}

    def _require_lockable_version(
        self, bucket: _Bucket, key: str, version_id: str
    ) -> _Version:
        if not bucket.object_lock:
            # The real-S3 missing-Object-Lock signal the adapter classifies.
            raise S3LocalClientError(
                "InvalidRequest", "Bucket is missing Object Lock Configuration"
            )
        version = self._find_version(bucket.versions.get(key), version_id)
        if version is None:
            raise S3LocalClientError("NoSuchVersion", "no such version")
        if version.is_delete_marker:
            raise S3LocalClientError("MethodNotAllowed", "delete markers hold no lock")
        return version

    def _read_response(self, bucket: _Bucket, version: _Version) -> dict[str, Any]:
        assert version.body is not None and version.etag is not None
        return {
            "Body": _LocalBody(version.body),
            "ETag": version.etag,
            "VersionId": version.version_id if bucket.versioned else _NULL_VERSION_ID,
        }
