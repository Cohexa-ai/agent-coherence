# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""CoherentObject — coherence over an S3 object (native-CAS binding).

Bring coherence to an object you already keep in S3. The object body never
leaves S3 — the coordinator holds only a monotonic version plus a fixed-width
fingerprint, never the bytes. No-lost-update rides S3's own conditional write:
a write is ``put_object(Body=..., IfMatch=prior_etag)``, so a peer who moved the
object's ETag wins the race and the stale writer's put fails with ``412
PreconditionFailed``.

**The version is the object's ETag, minted by S3, captured from the write
response.** The binding holds zero version state and never computes the ETag —
under SSE-KMS/SSE-C, multipart, or a directory bucket the ETag is not a content
digest, so it is treated as an OPAQUE token. It is also NOT portable across
artifact refs: identical bytes elsewhere can mint an identical ETag, so a token
for one key must never arbitrate a write to another.

The binding reads ``(bytes, ETag)`` from a single ``get_object`` — the token and
the bytes it vouches for always come from the same response, so a concurrent
update can never be silently lost across a split read (never Head-then-Get for
the pair).

Install::

    pip install "agent-coherence[coherent-object]"

The ``boto3`` driver is imported lazily, so importing this module without the
driver installed is fine — the clear install error only fires when a real client
is actually built (a caller may instead inject a client at construction).

**Writes are single-request puts, structurally.** The binding writes via
``put_object`` exclusively — never the multipart API or boto3's transfer manager
— so ``put_object``'s 5 GiB request ceiling is the size bound. This is what keeps
the ETag round-trip and the conditional-write semantics well-defined.
"""

from __future__ import annotations

from enum import Enum
from typing import TYPE_CHECKING, Any, Final

from ccs.adapters.substrate import (
    CasConflict,
    CasUnknown,
    CasWriteResult,
    CasWritten,
    ReconcileDecision,
    ReconcileVerdict,
    SubstrateToken,
)
from ccs.core.exceptions import CoherenceError
from ccs.core.substrate import CapabilityDescriptor, Tier
from ccs.core.substrate import sha256_hex as _sha256_hex

if TYPE_CHECKING:
    from ccs.adapters.substrate import CoherenceSubstrate

__all__ = [
    "CREATE_IF_ABSENT",
    "CoherentObject",
    "ReconcileDecision",
    "ReconcileVerdict",
    "S3PutOutcome",
    "classify_put_exception",
    "conditional_write_bucket_policy",
    "least_privilege_iam_policy",
    "s3_policy_docs",
]

# The one capability this binding declares. NATIVE_CAS because S3's own atomic
# conditional write (put_object with If-Match) rejects a lost update on the
# version axis; the guarantee wording is derived from the tier, not written here.
_DESCRIPTOR = CapabilityDescriptor(
    tier=Tier.NATIVE_CAS,
    version_source="object ETag",
    least_privilege=(
        "a writer principal with s3:GetObject + s3:PutObject scoped to the exact "
        "key/prefix ARN only — the If-Match writer needs s3:GetObject to fetch the "
        "ETag; explicit denies for s3:DeleteObject and any wildcard (no s3:*), and "
        "no s3:PutBucketPolicy so the conditional-write bucket policy stays "
        "owner-managed and agent-unremovable"
    ),
    consistency_note=(
        "reconciliation reads target the SAME Region/endpoint as the write (one "
        "pinned client); GET/HEAD-after-PUT is strongly consistent per key. Never "
        "run the CAS loop through a Multi-Region Access Point or a read "
        "replica/CRR target — either can serve a stale or missing object and miss "
        "a conflict."
    ),
)

# Pass as ``expected_token`` to request a CREATE (maps to S3 ``IfNoneMatch="*"``):
# the write lands only if the object is ABSENT; a second create → 412. A distinct
# control-char-wrapped sentinel so it can never collide with a real (opaque) ETag,
# and so the Sentinel rule holds — an absent comparand is made explicit, never a
# blank token silently used as an If-Match.
CREATE_IF_ABSENT: Final[str] = "\x00ccs-create-if-absent\x00"

# S3 error codes we classify as compare-and-set outcomes. Branch on
# ``response["Error"]["Code"]`` (the typed signal) — 412/409 are NOT modeled
# boto3 exceptions, and 404 may surface as the modeled ``NoSuchKey`` subclass, so
# never write except-ordering that assumes 404 is a bare ClientError.
_CODE_PRECONDITION_FAILED: Final[str] = "PreconditionFailed"  # 412 — definitive loss
_CODE_CONDITIONAL_CONFLICT: Final[str] = "ConditionalRequestConflict"  # 409 — retryable
# 404 on the If-Match path: a raced delete OR a delete-marker-current on a
# versioned bucket (both fail If-Match with 404, not 412). Surfaced as a typed
# conflict — re-create is a CALLER decision (via CREATE_IF_ABSENT after
# reacquire), never an automatic re-drive: a delete is itself an update.
_NOT_FOUND_CODES: Final[frozenset[str]] = frozenset({"NoSuchKey", "NoSuchVersion", "404"})


def _s3_error_code(exc: BaseException) -> str | None:
    """The S3 error code from a botocore ``ClientError``-shaped exception, or None.

    Duck-typed on the ``.response`` payload so the real ``ClientError`` (and its
    modeled ``NoSuchKey`` subclass) and a driver-free test stub classify
    identically. A connection/timeout error carries no ``.response`` → ``None``.
    """
    response = getattr(exc, "response", None)
    if not isinstance(response, dict):
        return None
    error = response.get("Error")
    if not isinstance(error, dict):
        return None
    code = error.get("Code")
    return code if isinstance(code, str) else None


# --- put-outcome classification (fine-grained; the contract 3-way is coarser) --


class S3PutOutcome(Enum):
    """The fine-grained outcome of one conditional ``put_object``.

    Richer than the contract's three-way :data:`CasWriteResult` so a caller
    (Unit 5) can distinguish a definitive loss from a retryable conflict:

    - ``WON`` — the put landed; the response minted a fresh ETag.
    - ``CONFLICT`` — 412 ``PreconditionFailed``: the ETag moved, NOTHING landed;
      re-read and re-derive (never re-drive the stale comparand).
    - ``RETRYABLE`` — 409 ``ConditionalRequestConflict``: a transient S3-internal
      conflict, nothing landed; re-read the ETag and retry the same intent.
    - ``RACED_DELETE`` — 404 on the If-Match path (raced delete / delete-marker):
      nothing landed; re-create is a CALLER decision after reacquire, never auto.
    - ``UNKNOWN`` — a connect/timeout/ambiguous failure: the put may or may not
      have landed; reconcile by re-reading before doing anything else.

    ``CONFLICT`` / ``RETRYABLE`` / ``RACED_DELETE`` all map to the contract's
    :class:`~ccs.adapters.substrate.CasConflict` (no write landed); ``UNKNOWN``
    maps to :class:`~ccs.adapters.substrate.CasUnknown`.
    """

    WON = "won"
    CONFLICT = "conflict"
    RETRYABLE = "retryable"
    RACED_DELETE = "raced_delete"
    UNKNOWN = "unknown"


def classify_put_exception(exc: BaseException) -> S3PutOutcome | None:
    """Classify a ``put_object`` failure into an :class:`S3PutOutcome`, or None.

    ``None`` means the error is NOT a compare-and-set outcome (an ``AccessDenied``,
    a missing bucket, a client-side param fault) and must PROPAGATE — it is a
    configuration fault, never a CAS state. A failure with no S3 error code is an
    ambiguous transport failure → ``UNKNOWN`` (the put's outcome is two-world).
    """
    code = _s3_error_code(exc)
    if code is None:
        return S3PutOutcome.UNKNOWN
    if code == _CODE_PRECONDITION_FAILED:
        return S3PutOutcome.CONFLICT
    if code == _CODE_CONDITIONAL_CONFLICT:
        return S3PutOutcome.RETRYABLE
    if code in _NOT_FOUND_CODES:
        return S3PutOutcome.RACED_DELETE
    return None


def _cas_result_for_outcome(outcome: S3PutOutcome) -> CasWriteResult:
    """Map a non-win :class:`S3PutOutcome` to the contract's three-way result."""
    if outcome is S3PutOutcome.UNKNOWN:
        # UNKNOWN, not failed: the write may or may not have landed. The finer
        # retryable/raced-delete distinction that a landed-nothing conflict carries
        # is preserved by classify_put_exception for callers that branch on it.
        return CasUnknown()
    return CasConflict()


# --- unknown-after-put reconciliation (the total four-arm branch) --------------
#
# The verdict vocabulary (:class:`ReconcileVerdict`) and the decision type
# (:class:`ReconcileDecision`, with ``bump_fires`` / ``re_drive_token`` /
# ``summary``) are the UNIFIED types from ``ccs.adapters.substrate`` — one
# language shared with the Postgres arm so the cross-agent commit dispatches
# uniformly. This binding's four-arm branch reaches HOLD (404 / delete-marker),
# RE_DRIVE (ETag unmoved), CONVERGE (ETag moved + bytes match — the bump STILL
# fires), and CONFLICT (ETag moved + bytes differ). Re-exported below for callers.


# --- the binding ---------------------------------------------------------------


def _require_boto3_client(region: str | None, endpoint_url: str | None) -> Any:
    """Build a real boto3 S3 client, or raise a clear install error naming the extra."""
    try:
        import boto3  # deferred by design — see the module docstring
    except ImportError as exc:  # pragma: no cover - exercised only without the driver
        raise ImportError(
            "CoherentObject requires the boto3 driver. Install it with: "
            'pip install "agent-coherence[coherent-object]"'
        ) from exc
    return boto3.client("s3", region_name=region, endpoint_url=endpoint_url)


class CoherentObject:
    """Coherence over a single S3 object, keyed by its object key.

    Construct with either an injected ``client`` (any object exposing S3's
    ``get_object`` / ``put_object``) or a ``region`` / ``endpoint_url`` from which
    a boto3 client is built lazily on construction. The binding implements the
    :class:`~ccs.adapters.substrate.CoherenceSubstrate` surface: :meth:`read`
    returns ``(bytes, ETag)`` from one ``get_object`` and :meth:`cas_write` maps
    to a conditional ``put_object`` and returns a typed win / conflict / unknown.

    The token is the object's ETag, captured from the response and treated as
    OPAQUE. It is NOT portable across artifact refs — the coordinator record is
    keyed by artifact id, so a token minted for one key can never arbitrate a
    write to another.

    A single client serves BOTH read and write, so a reconciliation read is
    inherently pinned to the write's Region/endpoint (never routed through an MRAP)
    — the consistency the four-arm reconciliation depends on.
    """

    #: Bytes threaded to the coordinator on commit: never any. The coordinator
    #: holds only a version + a fixed-width fingerprint for this binding, so the
    #: object body is never shadowed coordinator-side (never-ship-a-store). This is
    #: the binding's self-declaration; the conformance kit asserts it, and
    #: :class:`~ccs.adapters.substrate.CoordinatedSubstrate` refuses at composition
    #: any binding that sets it True. The runtime enforcement lives in
    #: ``SubstrateCoordinatorSession.commit_cas`` (a content_hash-only payload).
    SENDS_CONTENT_TO_COORDINATOR: bool = False

    def __init__(
        self,
        bucket: str,
        *,
        client: Any | None = None,
        region: str | None = None,
        endpoint_url: str | None = None,
    ) -> None:
        if not bucket:
            raise ValueError("CoherentObject needs a bucket name (fail-closed)")
        self._bucket = bucket
        self._client = client if client is not None else _require_boto3_client(region, endpoint_url)

    # --- capability -----------------------------------------------------------

    @property
    def descriptor(self) -> CapabilityDescriptor:
        """The binding's honest capability declaration (native-CAS)."""
        return _DESCRIPTOR

    def coordinator_commit_content(self) -> None:
        """The content threaded to the coordinator on commit: always ``None``.

        The object body stays in S3; the coordinator holds only a version +
        fingerprint (no retention shadow — never-ship-a-store). A declarative
        companion to :attr:`SENDS_CONTENT_TO_COORDINATOR`, asserted by the
        conformance kit; the actual content-free payload is built in
        ``SubstrateCoordinatorSession.commit_cas``.
        """
        return None

    # --- read -----------------------------------------------------------------

    def read(self, artifact_ref: str) -> tuple[bytes, SubstrateToken]:
        """Return ``(bytes, ETag)`` for one object from a single ``get_object``.

        Both values come from the SAME response (never Head-then-Get — a token
        fetched by a second call can vouch for bytes it never described). Raises
        :class:`KeyError` if the object is absent (404 / ``NoSuchKey``); any other
        error propagates.
        """
        try:
            resp = self._client.get_object(Bucket=self._bucket, Key=artifact_ref)
        except Exception as exc:
            if _s3_error_code(exc) in _NOT_FOUND_CODES:
                raise KeyError(f"no object {artifact_ref!r} in s3://{self._bucket}") from None
            raise
        return resp["Body"].read(), self._require_response_etag(resp)

    # --- compare-and-set (the Protocol write leg) -----------------------------

    def cas_write(
        self,
        artifact_ref: str,
        *,
        expected_token: SubstrateToken,
        new_bytes: bytes,
    ) -> CasWriteResult:
        """Conditionally write ``new_bytes`` iff the object is still at ``expected_token``.

        Maps to ``put_object(Body=new_bytes, IfMatch=expected_token)`` (or, when
        ``expected_token`` is :data:`CREATE_IF_ABSENT`, ``IfNoneMatch="*"`` — the
        write lands only if the object is absent). Returns a typed outcome: a win
        carries the ETag minted BY S3 for this write (from the response, never
        computed); a 412/409/404 is a :class:`~ccs.adapters.substrate.CasConflict`
        (no write landed); a connect/timeout/ambiguous failure is a
        :class:`~ccs.adapters.substrate.CasUnknown` — the write may or may not have
        landed, so reconcile via :meth:`reconcile_after_unknown` before anything else.
        """
        kwargs = self._put_kwargs(artifact_ref, expected_token, new_bytes)
        try:
            resp = self._client.put_object(**kwargs)
        except Exception as exc:
            outcome = classify_put_exception(exc)
            if outcome is None:
                raise  # not a CAS state (AccessDenied, missing bucket, param fault)
            return _cas_result_for_outcome(outcome)
        return CasWritten(token=self._require_response_etag(resp))

    def cas_write_if_changed(
        self,
        artifact_ref: str,
        *,
        expected_token: SubstrateToken,
        new_bytes: bytes,
        current_hash: str,
    ) -> CasWriteResult | None:
        """CAS-write ``new_bytes`` UNLESS they already equal the current object.

        Returns ``None`` for the byte-identical no-op — NO ``put_object`` is issued
        and the caller MUST skip the coordinator bump too (issuing either would
        advance the version for a no-change write, a phantom advance every peer
        would then reconcile against). Otherwise delegates to :meth:`cas_write`.
        """
        if self.is_noop_write(current_hash=current_hash, intended_hash=_sha256_hex(bytes(new_bytes))):
            return None
        return self.cas_write(artifact_ref, expected_token=expected_token, new_bytes=new_bytes)

    @staticmethod
    def is_noop_write(*, current_hash: str, intended_hash: str) -> bool:
        """True when the intended bytes already equal the object's current bytes.

        A byte-identical write must skip the put AND the coordinator bump; the
        caller checks this before :meth:`cas_write` (or uses
        :meth:`cas_write_if_changed`, which folds the check in).
        """
        return current_hash == intended_hash

    # --- unknown reconciliation (the total four-arm branch) -------------------

    def reconcile_after_unknown(
        self,
        artifact_ref: str,
        *,
        expected_token: SubstrateToken,
        intended_hash: str,
    ) -> ReconcileDecision:
        """Decide what an unconfirmed put should do, from ONE consistent re-read.

        Runs under the client-held pending intent ``(T_old=expected_token,
        intended_hash, ...)``. Does exactly one ``get_object`` (never Head-then-Get)
        and returns exactly one verdict — see :class:`ReconcileVerdict` for the four
        arms. The read is inherently Region/endpoint-pinned to the write (one
        client), so it is authoritative (strong read-after-write per key).

        A transport failure DURING the reconciliation read is not a decision — it
        propagates so the caller can retry the reconcile; only a definitive 404
        becomes ``HOLD``.
        """
        try:
            observed_bytes, observed_token = self.read(artifact_ref)
        except KeyError:
            # (i) 404 / delete-marker-current → UNCONFIRMED → HOLD. Never a match
            # against sha256(b"") and never an auto re-create.
            return ReconcileDecision(ReconcileVerdict.HOLD, None, None)
        if observed_token == expected_token:
            # (ii) token unmoved → not landed yet → re-drive under If-Match=T_old.
            return ReconcileDecision(ReconcileVerdict.RE_DRIVE, observed_bytes, observed_token)
        if _sha256_hex(observed_bytes) == intended_hash:
            # (iii) token moved AND bytes byte-identical → converge on the token
            # axis; the coordinator bump STILL fires (never-converge would strand it).
            return ReconcileDecision(ReconcileVerdict.CONVERGE, observed_bytes, observed_token)
        # (iv) token moved AND bytes differ → a real peer write → typed conflict.
        return ReconcileDecision(ReconcileVerdict.CONFLICT, observed_bytes, observed_token)

    # --- internals ------------------------------------------------------------

    def _put_kwargs(
        self, artifact_ref: str, expected_token: SubstrateToken, new_bytes: bytes
    ) -> dict[str, Any]:
        if not isinstance(new_bytes, (bytes, bytearray)):
            raise TypeError("CoherentObject.cas_write expects bytes")
        kwargs: dict[str, Any] = {
            "Bucket": self._bucket,
            "Key": artifact_ref,
            "Body": bytes(new_bytes),
        }
        if expected_token == CREATE_IF_ABSENT:
            kwargs["IfNoneMatch"] = "*"
        else:
            kwargs["IfMatch"] = self._require_usable_token(expected_token)
        return kwargs

    @staticmethod
    def _require_usable_token(token: SubstrateToken) -> SubstrateToken:
        """Fail closed on an absent/sentinel token (the Sentinel rule).

        An absent/blank ETag is UNCONFIRMED and may never seed an If-Match
        comparand — hold and reacquire, or create explicitly via
        :data:`CREATE_IF_ABSENT`.
        """
        if not isinstance(token, str) or not token.strip():
            raise CoherenceError(
                "unusable substrate token (absent/sentinel); it may not seed a CAS "
                "comparand — hold and reacquire, or create via CREATE_IF_ABSENT"
            )
        return token

    @staticmethod
    def _require_response_etag(resp: object) -> SubstrateToken:
        """The ETag from an S3 response, or fail closed — NEVER computed.

        The ETag is opaque (SSE-KMS/SSE-C/multipart/directory-bucket ETags are not
        content digests), so it is only ever read from the response. An absent ETag
        means the write's outcome is unverifiable → fail closed rather than mint a
        token that vouches for bytes S3 never acknowledged.
        """
        etag = resp.get("ETag") if isinstance(resp, dict) else None
        if not isinstance(etag, str) or not etag:
            raise CoherenceError(
                "S3 response carried no ETag; cannot mint a token for a write whose "
                "outcome is unverifiable — hold and reconcile"
            )
        return etag


# --- least-privilege / bucket-policy doc helpers (never applied at runtime) ----


def least_privilege_iam_policy(bucket: str, key_or_prefix: str) -> dict[str, Any]:
    """The minimal WRITER IAM policy: read the ETag + conditional-put the key.

    Grants only ``s3:GetObject`` (an If-Match writer must fetch the ETag) and
    ``s3:PutObject`` on the exact key/prefix ARN, and explicitly DENIES delete and
    bucket-policy writes — the object can never be removed out from under
    coherence, and the writer can never relax the conditional-write bucket policy.
    """
    obj_arn = f"arn:aws:s3:::{bucket}/{key_or_prefix}"
    bucket_arn = f"arn:aws:s3:::{bucket}"
    return {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "CoherenceWriterLeastPrivilege",
                "Effect": "Allow",
                "Action": ["s3:GetObject", "s3:PutObject"],
                "Resource": obj_arn,
            },
            {
                "Sid": "CoherenceDenyDestructive",
                "Effect": "Deny",
                "Action": ["s3:DeleteObject", "s3:DeleteObjectVersion", "s3:PutBucketPolicy"],
                "Resource": [obj_arn, bucket_arn],
            },
        ],
    }


def conditional_write_bucket_policy(bucket: str, key_or_prefix: str = "*") -> dict[str, Any]:
    """The owner-managed bucket policy that REQUIRES a conditional write.

    Denies any single-object ``s3:PutObject`` that carries no ``If-Match``
    (``Null s3:if-match`` true), so a writer cannot bypass compare-and-set with an
    unconditional overwrite. Multipart uploads cannot carry conditional headers, so
    the Deny is scoped to ``s3:ObjectCreationOperation`` == ``PutObject`` and thus
    exempts them (this binding never uses multipart — the exemption only keeps the
    policy from blocking a legitimate MPU by another principal). Owner-managed and
    agent-unremovable (the writer role holds no ``s3:PutBucketPolicy``).
    """
    obj_arn = f"arn:aws:s3:::{bucket}/{key_or_prefix}"
    return {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "RequireIfMatchOnOverwrite",
                "Effect": "Deny",
                "Principal": "*",
                "Action": "s3:PutObject",
                "Resource": obj_arn,
                "Condition": {
                    "Null": {"s3:if-match": "true"},
                    "StringEquals": {"s3:ObjectCreationOperation": "PutObject"},
                },
            },
        ],
    }


def s3_policy_docs(bucket: str, key_or_prefix: str) -> dict[str, Any]:
    """Both policies an operator applies to enforce coherence at the bucket.

    Returns the writer's least-privilege IAM policy and the owner-managed
    conditional-write-enforcing bucket policy. A documentation helper (the Unit-8
    docs surface) — the binding never applies either at runtime.
    """
    return {
        "writer_iam_policy": least_privilege_iam_policy(bucket, key_or_prefix),
        "bucket_policy": conditional_write_bucket_policy(bucket, key_or_prefix),
    }


if TYPE_CHECKING:
    # Structural conformance: CoherentObject must satisfy the CoherenceSubstrate
    # Protocol (descriptor + read + cas_write). Checked statically, never by
    # inheritance — mirrors the registry-contract discipline and the sibling
    # CoherentRow binding.
    _protocol_check: type[CoherenceSubstrate] = CoherentObject
