# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""WorkspaceVersioner capture engine — WV plan Unit 3 (R1/R2/R8).

Covers, per the unit's test scenarios:

- clean capture over heterogeneous members (file + S3 + forward-only): tiers
  honest per member, the restore POINTER manifested (S3 versionId / file
  coordinator version — never the ETag CAS comparand), the skew-declared
  window recorded as [min, max] of the capture timestamps;
- ABSENT is a fact distinct from present-empty (no token/fingerprint vs
  ``sha256(b"")``);
- torn-cut detection: a write landing inside the window flags EXACTLY that
  member ``dirty_during_window`` (driven deterministically via the scripted
  file fake AND a second S3 writer racing between capture and verify);
- the unversioned-S3 honest refusal path (typed discovery, member described
  but ``forward_only`` — never ``restorable``);
- forward-only members enumerated, never token-captured;
- binary file member → typed capture-time refusal, nothing persisted;
- coordinator down → typed ``CheckpointPersistFailed``, NO partial manifest;
  and the abort Event threads end-to-end into the registry's ``abort_guard``;
- route level (the live-server house pattern): ``POST /workspace/checkpoint``
  round-trips through ``GET /workspace/checkpoints``; boundary validation
  fails closed on malformed member rows.
"""

from __future__ import annotations

import time
import uuid
from pathlib import Path
from typing import Any

import pytest

from ccs.adapters.coherent_object import CoherentObject
from ccs.adapters.workspace import (
    BINARY_FILE_MEMBER_REASON,
    CHECKPOINT_NOT_PERSISTED_REASON,
    MAX_RESTORE_LEG_REDRIVES,
    BinaryFileMemberRefused,
    CheckpointPersistFailed,
    WorkspaceVersioner,
)
from ccs.coordinator.registry import ArtifactRegistry
from ccs.coordinator.service import CoordinatorService
from ccs.core.exceptions import (
    CHECKPOINT_UNKNOWN_REASON,
    PIN_STATE_HELD,
    PIN_STATE_RELEASED,
    PIN_STATE_UNAVAILABLE,
    PIN_STATE_UNPINNED,
    RESTORE_OUTCOME_CONFLICT,
    RESTORE_OUTCOME_CONVERGED,
    RESTORE_OUTCOME_FORWARD_ONLY_SKIPPED,
    RESTORE_OUTCOME_HELD_UNCONFIRMED,
    RESTORE_OUTCOME_RESTORED,
    RESTORE_OUTCOME_TARGET_LOST,
    RESTORE_STATUS_CONCLUDED,
    RESTORE_STATUS_IN_PROGRESS,
    RESTORE_STATUS_NONE,
    RESTORE_STATUS_REGISTERED,
    STALE_READ_GENERATION_REASON,
    WORKSPACE_REGISTRATION_COMMITTED,
    WORKSPACE_REGISTRATION_EMPTY,
    WORKSPACE_REGISTRATION_PRIOR_RUN,
    WORKSPACE_REGISTRATION_REFUSED,
    CasVersionConflict,
    CheckpointUnknown,
    CommitUnconfirmed,
    WatchdogAbandoned,
)
from ccs.core.invariants import check_monotonic_version
from ccs.core.states import MESIState
from ccs.core.substrate import sha256_hex
from ccs.core.types import ConflictDetail
from ccs.testing.s3_local import LocalS3Client

OWNER = uuid.uuid4()


# ---------------------------------------------------------------------------
# Deterministic fakes
# ---------------------------------------------------------------------------


class _TickClock:
    """Injectable monotonic_seconds stand-in: strictly increasing int ticks."""

    def __init__(self, start: int = 100) -> None:
        self._next = start

    def __call__(self) -> int:
        tick = self._next
        self._next += 1
        return tick


class _ScriptedFileSource:
    """A ``read_with_version`` fake with a per-path response script.

    Each programmed response is either ``(bytes, version)`` or an exception
    instance to raise. Responses are consumed in order; the LAST one repeats —
    so a one-entry script models a quiescent member (capture and verify see
    the same state) and a two-entry script models a write landing inside the
    window (capture sees the first, verify sees the second).
    """

    def __init__(self) -> None:
        self._script: dict[str, list[Any]] = {}

    def program(self, path: str, *responses: Any) -> None:
        self._script[path] = list(responses)

    def read_with_version(self, path: str) -> tuple[bytes, int]:
        script = self._script.get(path)
        if not script:
            raise FileNotFoundError(f"no such file in workspace: {path}")
        item = script.pop(0) if len(script) > 1 else script[0]
        if isinstance(item, BaseException):
            raise item
        return item


class _ForeignWriterOnSecondRead:
    """Wraps a LocalS3Client: the SECOND ``get_object`` of ``key`` first lands
    a foreign put — the deterministic "a peer wrote between the capture read
    and the verification read" race for the torn-cut test. Everything else
    delegates untouched."""

    def __init__(self, inner: LocalS3Client, bucket: str, key: str, body: bytes) -> None:
        self._inner = inner
        self._bucket = bucket
        self._key = key
        self._body = body
        self._reads = 0

    def get_object(self, **kwargs: Any) -> Any:
        if kwargs.get("Key") == self._key:
            self._reads += 1
            if self._reads == 2:
                self._inner.put_object(Bucket=self._bucket, Key=self._key, Body=self._body)
        return self._inner.get_object(**kwargs)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)


class _DownService:
    """A persist seam whose coordinator is unreachable — every registration
    raises the transport-shaped error."""

    def create_workspace_checkpoint(self, **_kwargs: Any) -> Any:
        raise ConnectionError("coordinator unreachable")


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def registry() -> ArtifactRegistry:
    return ArtifactRegistry()


@pytest.fixture
def service(registry: ArtifactRegistry) -> CoordinatorService:
    return CoordinatorService(registry)


def _versioner(
    service: Any, clock: Any | None = None, resolver: Any | None = None
) -> WorkspaceVersioner:
    return WorkspaceVersioner(
        service=service,
        owner=OWNER,
        clock=clock if clock is not None else _TickClock(),
        file_resolver=resolver,
    )


def _s3(versioned: bool = True) -> tuple[LocalS3Client, CoherentObject]:
    client = LocalS3Client()
    client.create_bucket("demo", versioned=versioned, object_lock=versioned)
    return client, CoherentObject("demo", client=client)


# ---------------------------------------------------------------------------
# Clean capture — tiers, pointers, window
# ---------------------------------------------------------------------------


def test_clean_capture_records_manifest_tiers_and_window(
    registry: ArtifactRegistry, service: CoordinatorService
) -> None:
    client, obj = _s3()
    put = client.put_object(Bucket="demo", Key="cfg.json", Body=b"cfg-body")
    files = _ScriptedFileSource()
    files.program("notes/plan.md", (b"plan text", 7))

    versioner = _versioner(service, clock=_TickClock(100))
    versioner.add_file_member(files, "notes/plan.md")
    versioner.add_object_member(obj, "cfg.json")
    versioner.add_forward_only_member("effects/slack-notify")

    result = versioner.checkpoint("pre-refactor")

    # Persisted and readable back — the manifest is durable-facts only.
    record = registry.get_checkpoint(result.record.checkpoint_id)
    assert record is not None
    assert record.name == "pre-refactor"
    assert record.owner == OWNER
    # Window = [min, max] of the three capture ticks (100, 101, 102); the
    # persist tick (103) stamps created_at, never the window.
    assert (record.window_min, record.window_max) == (100.0, 102.0)
    assert record.created_at_tick == 103

    members = {m.member_path: m for m in registry.get_checkpoint_members(record.checkpoint_id)}
    assert set(members) == {"notes/plan.md", "s3://cfg.json", "effects/slack-notify"}

    file_row = members["notes/plan.md"]
    assert file_row.native_token == "7"  # the coordinator content-state pointer
    assert file_row.fingerprint == sha256_hex(b"plan text")
    assert file_row.arbitration_tier == "no-arbiter"
    assert file_row.restore_tier == "restorable-unpinned"  # retention pin is Unit 6
    assert file_row.absent is False and file_row.dirty_during_window is False

    s3_row = members["s3://cfg.json"]
    # The manifest pointer is the versionId; the ETag CAS comparand is NEVER
    # manifested (re-read live at restore time — the F4 split).
    assert s3_row.native_token == put["VersionId"]
    assert s3_row.native_token != put["ETag"]
    assert s3_row.fingerprint == sha256_hex(b"cfg-body")
    assert s3_row.arbitration_tier == "native-cas"
    assert s3_row.restore_tier == "restorable"
    assert s3_row.absent is False and s3_row.dirty_during_window is False

    fwd_row = members["effects/slack-notify"]
    assert fwd_row.native_token is None and fwd_row.fingerprint is None
    assert fwd_row.restore_tier == "forward_only"
    assert fwd_row.arbitration_tier == "no-arbiter"


def test_manifest_survives_via_registry_list(service: CoordinatorService, registry) -> None:
    files = _ScriptedFileSource()
    files.program("a.txt", (b"a", 1))
    versioner = _versioner(service)
    versioner.add_file_member(files, "a.txt")
    result = versioner.checkpoint("cp")
    assert [c.checkpoint_id for c in registry.list_checkpoints()] == [
        result.record.checkpoint_id
    ]


# ---------------------------------------------------------------------------
# ABSENT ≠ empty
# ---------------------------------------------------------------------------


def test_absent_member_distinct_from_present_empty(
    registry: ArtifactRegistry, service: CoordinatorService
) -> None:
    _client, obj = _s3()
    obj_client = obj  # readable alias
    # Present-EMPTY S3 member: a real zero-byte body.
    _client.put_object(Bucket="demo", Key="empty.bin", Body=b"")
    # ABSENT file member: never written.
    files = _ScriptedFileSource()  # nothing programmed -> FileNotFoundError

    versioner = _versioner(service)
    versioner.add_file_member(files, "gone.md")
    versioner.add_object_member(obj_client, "empty.bin")
    result = versioner.checkpoint("absent-vs-empty")

    members = {m.member_path: m for m in registry.get_checkpoint_members(result.record.checkpoint_id)}
    absent = members["gone.md"]
    empty = members["s3://empty.bin"]

    # ABSENT is a recorded FACT: no token, no fingerprint, absent=True.
    assert absent.absent is True
    assert absent.native_token is None and absent.fingerprint is None
    # Present-empty is a different fact: captured, fingerprinted as sha256(b"").
    assert empty.absent is False
    assert empty.fingerprint == sha256_hex(b"")
    assert empty.native_token is not None
    # The two records can never be conflated.
    assert (absent.absent, absent.fingerprint) != (empty.absent, empty.fingerprint)


# ---------------------------------------------------------------------------
# Torn-cut detection (dirty_during_window)
# ---------------------------------------------------------------------------


def test_intra_window_write_flags_exactly_the_dirty_member(
    registry: ArtifactRegistry, service: CoordinatorService
) -> None:
    _client, obj = _s3()
    _client.put_object(Bucket="demo", Key="steady.json", Body=b"steady")
    files = _ScriptedFileSource()
    # Capture sees (one, v3); the verification re-read sees (two, v4) — a
    # writer landed inside the window on THIS member only.
    files.program("torn.md", (b"one", 3), (b"two", 4))
    files.program("calm.md", (b"calm", 5))

    versioner = _versioner(service)
    versioner.add_file_member(files, "torn.md")
    versioner.add_file_member(files, "calm.md")
    versioner.add_object_member(obj, "steady.json")
    result = versioner.checkpoint("torn-cut")

    members = {m.member_path: m for m in registry.get_checkpoint_members(result.record.checkpoint_id)}
    assert members["torn.md"].dirty_during_window is True
    # The manifest still records the CAPTURED state, not the raced one.
    assert members["torn.md"].native_token == "3"
    assert members["torn.md"].fingerprint == sha256_hex(b"one")
    # Exactly that member — its peers verified quiescent.
    assert members["calm.md"].dirty_during_window is False
    assert members["s3://steady.json"].dirty_during_window is False


def test_s3_foreign_writer_between_capture_and_verify_flags_dirty(
    registry: ArtifactRegistry, service: CoordinatorService
) -> None:
    inner = LocalS3Client()
    inner.create_bucket("demo", versioned=True, object_lock=True)
    inner.put_object(Bucket="demo", Key="raced.json", Body=b"original")
    racing = _ForeignWriterOnSecondRead(inner, "demo", "raced.json", b"foreign-write")
    obj = CoherentObject("demo", client=racing)

    versioner = _versioner(service)
    versioner.add_object_member(obj, "raced.json")
    result = versioner.checkpoint("s3-race")

    (member,) = registry.get_checkpoint_members(result.record.checkpoint_id)
    assert member.dirty_during_window is True
    assert member.fingerprint == sha256_hex(b"original")  # the captured state


def test_member_vanishing_inside_window_flags_dirty(
    registry: ArtifactRegistry, service: CoordinatorService
) -> None:
    files = _ScriptedFileSource()
    files.program(
        "vanish.md", (b"here", 2), FileNotFoundError("no such file in workspace: vanish.md")
    )
    versioner = _versioner(service)
    versioner.add_file_member(files, "vanish.md")
    result = versioner.checkpoint("vanish")
    (member,) = registry.get_checkpoint_members(result.record.checkpoint_id)
    assert member.absent is False  # captured present…
    assert member.dirty_during_window is True  # …but not verified quiescent


# ---------------------------------------------------------------------------
# Honest refusals / honest tiers
# ---------------------------------------------------------------------------


def test_unversioned_s3_member_honest_refusal_path(
    registry: ArtifactRegistry, service: CoordinatorService
) -> None:
    """No pre-probe: the typed VersionPointerUnconfirmed on the capture read IS
    the discovery. The member is still DESCRIBED — fingerprint, presence — but
    holds no pointer and is tiered forward_only, never restorable."""
    client, obj = _s3(versioned=False)
    client.put_object(Bucket="demo", Key="plain.txt", Body=b"unversioned")

    versioner = _versioner(service)
    versioner.add_object_member(obj, "plain.txt")
    result = versioner.checkpoint("honest")

    (member,) = registry.get_checkpoint_members(result.record.checkpoint_id)
    assert member.restore_tier == "forward_only"
    assert member.native_token is None  # the "null" sentinel NEVER lands in a manifest
    assert member.fingerprint == sha256_hex(b"unversioned")
    assert member.absent is False


def test_file_pointer_unconfirmed_version_is_forward_only(
    registry: ArtifactRegistry, service: CoordinatorService
) -> None:
    """A file member whose coordinator version cannot be resolved (the volume's
    version-0 fallback) carries an UNCONFIRMED pointer: never manifested,
    never above forward_only (the Sentinel rule, file edition)."""
    files = _ScriptedFileSource()
    files.program("orphan.md", (b"body", 0))
    versioner = _versioner(service)
    versioner.add_file_member(files, "orphan.md")
    result = versioner.checkpoint("orphan")
    (member,) = registry.get_checkpoint_members(result.record.checkpoint_id)
    assert member.native_token is None
    assert member.restore_tier == "forward_only"
    assert member.fingerprint == sha256_hex(b"body")


def test_forward_only_members_enumerated_never_token_captured(
    registry: ArtifactRegistry, service: CoordinatorService
) -> None:
    files = _ScriptedFileSource()
    files.program("a.txt", (b"a", 1))
    versioner = _versioner(service)
    versioner.add_file_member(files, "a.txt")
    versioner.add_forward_only_member("effects/send-email")
    versioner.add_forward_only_member("effects/charge-card")
    result = versioner.checkpoint("effects")
    members = {m.member_path: m for m in result.members}
    for path in ("effects/send-email", "effects/charge-card"):
        row = members[path]
        assert row.native_token is None and row.fingerprint is None
        assert row.restore_tier == "forward_only"
        assert row.dirty_during_window is False
    # Enumerated in the DURABLE manifest too.
    stored = registry.get_checkpoint_members(result.record.checkpoint_id)
    assert {m.member_path for m in stored} >= {"effects/send-email", "effects/charge-card"}


def test_binary_file_member_typed_capture_refusal(
    registry: ArtifactRegistry, service: CoordinatorService
) -> None:
    files = _ScriptedFileSource()
    files.program("blob.bin", (b"\xff\xfe\x00\x01", 4))
    versioner = _versioner(service)
    versioner.add_file_member(files, "blob.bin")
    with pytest.raises(BinaryFileMemberRefused) as excinfo:
        versioner.checkpoint("binary")
    # Typed reason matched by IDENTITY (add-never-rename), naming the member.
    assert excinfo.value.reason is BINARY_FILE_MEMBER_REASON
    assert excinfo.value.member_path == "blob.bin"
    # Capture-time refusal: NOTHING persisted.
    assert registry.list_checkpoints() == []


# ---------------------------------------------------------------------------
# Persist failures — typed, no partial manifest
# ---------------------------------------------------------------------------


def test_coordinator_down_typed_failure_no_partial_manifest(
    registry: ArtifactRegistry,
) -> None:
    files = _ScriptedFileSource()
    files.program("a.txt", (b"a", 1))
    versioner = WorkspaceVersioner(service=_DownService(), owner=OWNER)
    versioner.add_file_member(files, "a.txt")
    with pytest.raises(CheckpointPersistFailed) as excinfo:
        versioner.checkpoint("down")
    assert excinfo.value.reason is CHECKPOINT_NOT_PERSISTED_REASON
    assert isinstance(excinfo.value.__cause__, ConnectionError)
    # The registry this test holds was never touched (the down service owns no
    # registry): the guarantee under test is the TYPED failure + the wording's
    # single-transaction claim, which the abort test below pins registry-side.
    assert registry.list_checkpoints() == []


def test_abort_event_threads_into_registry_guard(
    registry: ArtifactRegistry, service: CoordinatorService
) -> None:
    """The A6 lesson, proven at the service seam: a pre-set abort Event (the
    watchdog already timed out) fails the registration closed AT the registry
    write lock — no manifest lands after the client saw the degraded
    checkpoint_unconfirmed response."""
    import threading

    from ccs.coordinator.registry_protocol import CheckpointMember

    abort = threading.Event()
    abort.set()
    member = CheckpointMember(
        member_path="a.txt",
        artifact_id=None,
        native_token="1",
        fingerprint=sha256_hex(b"a"),
        captured_at=100.0,
    )
    with pytest.raises(WatchdogAbandoned):
        service.create_workspace_checkpoint(
            name="aborted",
            owner=OWNER,
            members=[member],
            window_min=100.0,
            window_max=100.0,
            issued_at_tick=101,
            abort=abort,
        )
    assert registry.list_checkpoints() == []


def test_service_validation_fails_closed(service: CoordinatorService, registry) -> None:
    from ccs.coordinator.registry_protocol import CheckpointMember

    member = CheckpointMember(
        member_path="a.txt",
        artifact_id=None,
        native_token=None,
        fingerprint=None,
        captured_at=1.0,
    )
    with pytest.raises(ValueError):
        service.create_workspace_checkpoint(
            name="  ", owner=OWNER, members=[member], window_min=1.0, window_max=2.0
        )
    with pytest.raises(ValueError):
        service.create_workspace_checkpoint(
            name="cp", owner=OWNER, members=[], window_min=1.0, window_max=2.0
        )
    with pytest.raises(ValueError):
        service.create_workspace_checkpoint(
            name="cp", owner=OWNER, members=[member], window_min=2.0, window_max=1.0
        )
    assert registry.list_checkpoints() == []


# ---------------------------------------------------------------------------
# Registration-time guards
# ---------------------------------------------------------------------------


def test_duplicate_member_path_rejected_at_registration(
    service: CoordinatorService,
) -> None:
    files = _ScriptedFileSource()
    versioner = _versioner(service)
    versioner.add_file_member(files, "a.txt")
    with pytest.raises(ValueError):
        versioner.add_file_member(files, "a.txt")
    with pytest.raises(ValueError):
        versioner.add_forward_only_member("a.txt")


def test_checkpoint_requires_members_and_name(service: CoordinatorService) -> None:
    versioner = _versioner(service)
    with pytest.raises(ValueError):
        versioner.checkpoint("empty-workspace")
    versioner.add_forward_only_member("fx")
    with pytest.raises(ValueError):
        versioner.checkpoint("   ")


# ---------------------------------------------------------------------------
# Route level — the live-server house pattern (mirrors
# tests/test_claude_code_coordinator_server.py)
# ---------------------------------------------------------------------------


@pytest.fixture
def coordinator(tmp_path: Path):
    from ccs.adapters.claude_code.coordinator_server import CoordinatorHTTPServer

    server = CoordinatorHTTPServer(tmp_path, port=0, instance_id="test-instance")
    server.serve_in_thread()
    time.sleep(0.05)
    try:
        yield server
    finally:
        server.shutdown()


@pytest.fixture
def client(coordinator):
    import json as _json
    from urllib import error as urlerror
    from urllib import request as urlrequest

    from ccs.adapters.claude_code.auth import load_secret

    secret = load_secret(coordinator.coordinator_root)
    assert secret is not None
    base = f"http://127.0.0.1:{coordinator.port}"
    headers = {
        "Authorization": f"Bearer {secret}",
        "Host": "127.0.0.1",
        "Content-Type": "application/json",
    }

    def request(method: str, path: str, body: dict | None = None) -> tuple[int, dict]:
        data = _json.dumps(body).encode("utf-8") if body is not None else None
        req = urlrequest.Request(base + path, data=data, method=method, headers=headers)
        try:
            with urlrequest.urlopen(req, timeout=10) as resp:
                return resp.status, _json.loads(resp.read().decode("utf-8") or "{}")
        except urlerror.HTTPError as e:
            return e.code, _json.loads(e.read().decode("utf-8") or "{}")

    return request


def _member_wire(path: str, **overrides: Any) -> dict:
    wire = {
        "member_path": path,
        "native_token": "v000001",
        "fingerprint": sha256_hex(b"body"),
        "captured_at": 100.0,
        "absent": False,
        "dirty_during_window": False,
        "arbitration_tier": "native-cas",
        "restore_tier": "restorable",
    }
    wire.update(overrides)
    return wire


def test_route_checkpoint_roundtrip(client) -> None:
    sid = str(uuid.uuid4())
    status, body = client(
        "POST",
        "/workspace/checkpoint",
        {
            "session_id": sid,
            "name": "route-cp",
            "window_min": 100.0,
            "window_max": 102.0,
            "members": [
                _member_wire("s3://cfg.json"),
                _member_wire(
                    "notes/plan.md",
                    native_token="7",
                    arbitration_tier="no-arbiter",
                    restore_tier="restorable-unpinned",
                    dirty_during_window=True,
                ),
                _member_wire(
                    "effects/notify",
                    native_token=None,
                    fingerprint=None,
                    arbitration_tier="no-arbiter",
                    restore_tier="forward_only",
                ),
            ],
        },
    )
    assert status == 200 and body["ok"] is True
    checkpoint_id = body["checkpoint_id"]
    assert body["window_min"] == 100.0 and body["window_max"] == 102.0

    status, listing = client("GET", "/workspace/checkpoints")
    assert status == 200 and listing["ok"] is True
    (cp,) = listing["checkpoints"]
    assert cp["checkpoint_id"] == checkpoint_id
    assert cp["name"] == "route-cp"
    assert cp["restore_status"] == "none"
    members = {m["member_path"]: m for m in cp["members"]}
    assert members["notes/plan.md"]["dirty_during_window"] is True
    assert members["notes/plan.md"]["restore_tier"] == "restorable-unpinned"
    assert members["s3://cfg.json"]["arbitration_tier"] == "native-cas"
    assert members["effects/notify"]["restore_tier"] == "forward_only"
    assert members["effects/notify"]["native_token"] is None


def test_route_checkpoint_boundary_validation(client) -> None:
    sid = str(uuid.uuid4())

    def post(payload: dict) -> tuple[int, dict]:
        return client("POST", "/workspace/checkpoint", payload)

    base = {
        "session_id": sid,
        "name": "cp",
        "window_min": 1.0,
        "window_max": 2.0,
        "members": [_member_wire("a.txt")],
    }
    # Missing / blank name.
    status, _ = post({**base, "name": "   "})
    assert status == 400
    # Empty member list.
    status, _ = post({**base, "members": []})
    assert status == 400
    # Unknown tier vocabulary — closed set, fail-closed.
    status, _ = post({**base, "members": [_member_wire("a.txt", restore_tier="magic")]})
    assert status == 400
    # Malformed fingerprint.
    status, _ = post({**base, "members": [_member_wire("a.txt", fingerprint="beef")]})
    assert status == 400
    # Duplicate member paths.
    status, _ = post({**base, "members": [_member_wire("a.txt"), _member_wire("a.txt")]})
    assert status == 400
    # Inverted window.
    status, _ = post({**base, "window_min": 5.0, "window_max": 1.0})
    assert status == 400
    # Absolute member path refused (the server-side authoritative path gate).
    status, _ = post({**base, "members": [_member_wire("/etc/passwd")]})
    assert status == 400
    # Nothing persisted by any of the rejects.
    status, listing = client("GET", "/workspace/checkpoints")
    assert status == 200 and listing["checkpoints"] == []


# ===========================================================================
# Restore — WV plan Unit 4 (R3): per-member conditional legs under the
# TERMINATION CONTRACT (bounded re-drive, absorbing outcomes, complete
# per-member terminal report, crash-resume from durable state).
# ===========================================================================


class _FakeFileStore:
    """State-based file member store speaking the full FileRestoreTarget
    surface (``read_with_version`` + ``write_cas_at``), with schedulable
    foreign-edit interleaves.

    ``write_cas_at`` mirrors ``CoherentVolume``'s contract: commit iff the
    current version equals ``expected_version`` (else the typed
    ``CasVersionConflict``), new version = expected + 1 — DETECTION-guarded,
    no arbiter. A scheduled foreign edit lands immediately AFTER a read
    returns, so the returned (bytes, version) pair is already stale by CAS
    time — the file half of the delay-injection harness.
    """

    def __init__(self) -> None:
        self._state: dict[str, tuple[bytes, int]] = {}
        self._foreign_after_read: dict[str, list[bytes]] = {}
        self.cas_calls: dict[str, int] = {}

    def put(self, path: str, data: bytes, version: int) -> None:
        self._state[path] = (bytes(data), version)

    def remove(self, path: str) -> None:
        self._state.pop(path, None)

    def state(self, path: str) -> tuple[bytes, int]:
        return self._state[path]

    def schedule_foreign_edit_after_read(self, path: str, *bodies: bytes) -> None:
        self._foreign_after_read.setdefault(path, []).extend(bodies)

    def read_with_version(self, path: str) -> tuple[bytes, int]:
        if path not in self._state:
            raise FileNotFoundError(f"no such file in workspace: {path}")
        data, version = self._state[path]
        queue = self._foreign_after_read.get(path)
        if queue:
            foreign = queue.pop(0)
            self._state[path] = (bytes(foreign), version + 1)
        return data, version

    def write_cas_at(self, path: str, expected_version: int, new_content: bytes) -> None:
        self.cas_calls[path] = self.cas_calls.get(path, 0) + 1
        if path not in self._state:
            raise CasVersionConflict(path, expected_version, 0)
        _data, version = self._state[path]
        if version != expected_version:
            raise CasVersionConflict(path, expected_version, version)
        self._state[path] = (bytes(new_content), version + 1)


class _FakeResolver:
    """FileContentResolver fake: an explicit (path, version) -> bytes history."""

    def __init__(self) -> None:
        self._history: dict[tuple[str, int], bytes] = {}

    def keep(self, path: str, version: int, data: bytes) -> None:
        self._history[(path, int(version))] = bytes(data)

    def content_at(self, member_path: str, version: int) -> bytes:
        try:
            return self._history[(member_path, int(version))]
        except KeyError:
            raise KeyError(f"no retained version {version} for {member_path}") from None


class _ForeignWriterOnLiveReads:
    """The S3 half of the delay-injection harness (Unit-4 execution note).

    Interleaves a foreign put between the engine's live comparand read and its
    CAS put: the (bytes, ETag) the engine holds is already stale by CAS time.
    Pinned (VersionId) reads are exempt — the pinned target is immutable
    history. ``times=None`` = every live read (sustained contention);
    an integer bounds the interleaves (a single race → one retry).
    """

    def __init__(
        self, inner: LocalS3Client, bucket: str, key: str, *, times: int | None = None
    ) -> None:
        self._inner = inner
        self._bucket = bucket
        self._key = key
        self._times = times
        self.landed = 0

    def get_object(self, **kwargs: Any) -> Any:
        resp = self._inner.get_object(**kwargs)
        if kwargs.get("Key") == self._key and kwargs.get("VersionId") is None:
            if self._times is None or self.landed < self._times:
                self.landed += 1
                self._inner.put_object(
                    Bucket=self._bucket,
                    Key=self._key,
                    Body=f"foreign-{self.landed}".encode(),
                )
        return resp

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)


class _VanishOnCasPut:
    """UNKNOWN-outcome harness: an If-Match put of ``key`` deletes the object
    underneath, then dies transport-shaped (no S3 error code) — the put's
    outcome is UNKNOWN and the reconciliation read finds the object gone
    (HOLD), driving the ``held_unconfirmed`` terminal."""

    def __init__(self, inner: LocalS3Client, bucket: str, key: str) -> None:
        self._inner = inner
        self._bucket = bucket
        self._key = key

    def put_object(self, **kwargs: Any) -> Any:
        if kwargs.get("Key") == self._key and kwargs.get("IfMatch") is not None:
            self._inner.delete_object(Bucket=self._bucket, Key=self._key)
            raise ConnectionError("socket dropped mid-put")
        return self._inner.put_object(**kwargs)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)


class _CrashOnNthMemberRecord:
    """Kill-after-N-members crash: forwards to the real service but raises on
    the Nth durable member-progress write — a crash AFTER that member's leg
    landed on the substrate and BEFORE its outcome record.

    The CheckpointRestoreStore members are delegated EXPLICITLY (not via
    ``__getattr__``): runtime-checkable Protocol isinstance uses static
    attribute lookup on 3.12+, which a ``__getattr__`` fallthrough never
    satisfies.
    """

    def __init__(self, inner: Any, fail_on_call: int) -> None:
        self._inner = inner
        self._fail_on = fail_on_call
        self._calls = 0

    def get_workspace_checkpoint(self, checkpoint_id: str) -> Any:
        return self._inner.get_workspace_checkpoint(checkpoint_id)

    def get_workspace_checkpoint_members(self, checkpoint_id: str) -> Any:
        return self._inner.get_workspace_checkpoint_members(checkpoint_id)

    def set_workspace_checkpoint_restore_status(self, *args: Any, **kwargs: Any) -> Any:
        return self._inner.set_workspace_checkpoint_restore_status(*args, **kwargs)

    def set_workspace_checkpoint_member_restore(self, *args: Any, **kwargs: Any) -> Any:
        self._calls += 1
        if self._calls == self._fail_on:
            raise RuntimeError("simulated crash (kill -9)")
        return self._inner.set_workspace_checkpoint_member_restore(*args, **kwargs)

    def register_workspace_restore(self, *args: Any, **kwargs: Any) -> Any:
        return self._inner.register_workspace_restore(*args, **kwargs)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)


# ---------------------------------------------------------------------------
# Happy full restore — create + modify + delete legs, all terminal outcomes
# ---------------------------------------------------------------------------


def test_happy_full_restore_all_terminal_outcomes(
    registry: ArtifactRegistry, service: CoordinatorService
) -> None:
    client, obj = _s3()
    client.put_object(Bucket="demo", Key="cfg.json", Body=b"cfg-v1")
    client.put_object(Bucket="demo", Key="gone.json", Body=b"keep-me")
    files = _FakeFileStore()
    files.put("notes/plan.md", b"plan text", 7)
    files.put("notes/calm.md", b"calm", 5)
    resolver = _FakeResolver()
    resolver.keep("notes/plan.md", 7, b"plan text")
    # Unit 6: the default checkpoint() runs the pin legs — the file
    # verification pin consults the resolver, so the converged member's
    # version must be retained too (else it downgrades loudly to
    # forward_only, which the retention-gap pin tests cover).
    resolver.keep("notes/calm.md", 5, b"calm")

    versioner = _versioner(service, resolver=resolver)
    versioner.add_file_member(files, "notes/plan.md")
    versioner.add_file_member(files, "notes/calm.md")
    versioner.add_object_member(obj, "cfg.json")
    versioner.add_object_member(obj, "gone.json")
    versioner.add_object_member(obj, "ghost.json")  # ABSENT at capture
    versioner.add_forward_only_member("effects/notify")
    cp = versioner.checkpoint("full")

    # Post-capture divergence: a modify, a delete, a foreign create, a file edit.
    client.put_object(Bucket="demo", Key="cfg.json", Body=b"cfg-v2")  # modify leg
    client.delete_object(Bucket="demo", Key="gone.json")  # create leg (marker)
    client.put_object(Bucket="demo", Key="ghost.json", Body=b"intruder")  # delete leg
    files.put("notes/plan.md", b"edited", 9)  # file modify leg

    report = versioner.restore(cp.record.checkpoint_id)

    assert report.status == RESTORE_STATUS_CONCLUDED
    by_path = report.members_by_path
    assert by_path["s3://cfg.json"].outcome == RESTORE_OUTCOME_RESTORED
    assert by_path["s3://cfg.json"].new_native_token is not None
    assert by_path["s3://gone.json"].outcome == RESTORE_OUTCOME_RESTORED
    assert by_path["s3://ghost.json"].outcome == RESTORE_OUTCOME_RESTORED
    assert by_path["s3://ghost.json"].deleted_at_restore is not None
    assert by_path["notes/plan.md"].outcome == RESTORE_OUTCOME_RESTORED
    assert by_path["notes/plan.md"].new_native_token == "10"  # 9 (live) + 1
    assert by_path["notes/calm.md"].outcome == RESTORE_OUTCOME_CONVERGED
    assert by_path["effects/notify"].outcome == RESTORE_OUTCOME_FORWARD_ONLY_SKIPPED

    # The substrates hold the manifest state again.
    data, _etag = obj.read("cfg.json")
    assert data == b"cfg-v1"
    data, _etag = obj.read("gone.json")
    assert data == b"keep-me"
    with pytest.raises(KeyError):
        obj.read("ghost.json")  # deleted (marker-current)
    assert files.state("notes/plan.md")[0] == b"plan text"

    # Durably mirrored: outcomes on the member rows, concluded on the header.
    record = registry.get_checkpoint(cp.record.checkpoint_id)
    assert record is not None and record.restore_status == RESTORE_STATUS_CONCLUDED
    stored = {m.member_path: m for m in registry.get_checkpoint_members(cp.record.checkpoint_id)}
    for path, outcome in by_path.items():
        assert stored[path].restore_outcome == outcome.outcome


# ---------------------------------------------------------------------------
# Foreign-writer races (the delay-injection harness)
# ---------------------------------------------------------------------------


def _checkpoint_one_s3_member(
    service: CoordinatorService, client: LocalS3Client, key: str, body: bytes
) -> str:
    obj = CoherentObject("demo", client=client)
    versioner = _versioner(service)
    versioner.add_object_member(obj, key)
    return versioner.checkpoint(f"cp-{key}").record.checkpoint_id


def test_single_interleaved_foreign_write_retries_once_and_lands(
    service: CoordinatorService,
) -> None:
    client = LocalS3Client()
    client.create_bucket("demo", versioned=True, object_lock=True)
    client.put_object(Bucket="demo", Key="raced.json", Body=b"original")
    checkpoint_id = _checkpoint_one_s3_member(service, client, "raced.json", b"original")
    # Diverge live so the leg must write, then interleave EXACTLY one foreign
    # put between the engine's live read and its CAS.
    client.put_object(Bucket="demo", Key="raced.json", Body=b"diverged")
    racing = _ForeignWriterOnLiveReads(client, "demo", "raced.json", times=1)

    restorer = _versioner(service)
    restorer.add_object_member(CoherentObject("demo", client=racing), "raced.json")
    report = restorer.restore(checkpoint_id)

    (member,) = report.members
    assert member.outcome == RESTORE_OUTCOME_RESTORED
    assert member.attempts == 2  # attempt 1 lost the If-Match race; attempt 2 landed
    data, _etag = CoherentObject("demo", client=client).read("raced.json")
    assert data == b"original"


def test_sustained_contention_exhausts_budget_into_conflict_no_livelock(
    registry: ArtifactRegistry, service: CoordinatorService
) -> None:
    client = LocalS3Client()
    client.create_bucket("demo", versioned=True, object_lock=True)
    client.put_object(Bucket="demo", Key="hot.json", Body=b"original")
    client.put_object(Bucket="demo", Key="calm.json", Body=b"steady")

    obj = CoherentObject("demo", client=client)
    versioner = _versioner(service)
    versioner.add_object_member(obj, "hot.json")
    versioner.add_object_member(obj, "calm.json")
    checkpoint_id = versioner.checkpoint("contended").record.checkpoint_id

    client.put_object(Bucket="demo", Key="hot.json", Body=b"diverged")
    racing = _ForeignWriterOnLiveReads(client, "demo", "hot.json", times=None)
    put_count_before = len(client.put_calls)

    restorer = _versioner(service)
    restorer.add_object_member(CoherentObject("demo", client=racing), "hot.json")
    restorer.add_object_member(CoherentObject("demo", client=racing), "calm.json")
    report = restorer.restore(checkpoint_id)

    by_path = report.members_by_path
    hot = by_path["s3://hot.json"]
    # Budget exhausted, absorbed as conflict — the restore still CONCLUDED and
    # the report names the losing member; the healthy peer converged untouched.
    assert hot.outcome == RESTORE_OUTCOME_CONFLICT
    assert hot.attempts == MAX_RESTORE_LEG_REDRIVES + 1
    assert by_path["s3://calm.json"].outcome == RESTORE_OUTCOME_CONVERGED
    assert report.status == RESTORE_STATUS_CONCLUDED
    record = registry.get_checkpoint(checkpoint_id)
    assert record is not None and record.restore_status == RESTORE_STATUS_CONCLUDED
    # NO livelock: the engine issued exactly budget-many If-Match puts (the
    # foreign writer's own unconditional puts ride the same log).
    engine_cas_puts = [
        c
        for c in client.put_calls[put_count_before:]
        if c["Key"] == "hot.json" and c["IfMatch"] is not None
    ]
    assert len(engine_cas_puts) == MAX_RESTORE_LEG_REDRIVES + 1


def test_file_member_foreign_edit_detection_labeled_no_arbiter(
    service: CoordinatorService,
) -> None:
    files = _FakeFileStore()
    files.put("doc.md", b"captured", 3)
    resolver = _FakeResolver()
    resolver.keep("doc.md", 3, b"captured")
    versioner = _versioner(service, resolver=resolver)
    versioner.add_file_member(files, "doc.md")
    checkpoint_id = versioner.checkpoint("file-race").record.checkpoint_id

    files.put("doc.md", b"edited", 5)
    # One foreign edit between the engine's read and its CAS: detection fires
    # (typed CasVersionConflict), ONE retry lands.
    files.schedule_foreign_edit_after_read("doc.md", b"foreign-edit")

    restorer = _versioner(service, resolver=resolver)
    restorer.add_file_member(files, "doc.md")
    report = restorer.restore(checkpoint_id)

    (member,) = report.members
    assert member.outcome == RESTORE_OUTCOME_RESTORED
    assert member.attempts == 2
    # The no-arbiter honesty label: a file outcome is DETECTION, never
    # presented as substrate arbitration.
    assert "no-arbiter" in member.detail
    assert "detection" in member.detail.lower()
    assert "never substrate arbitration" in member.detail
    assert files.state("doc.md")[0] == b"captured"


def test_file_member_sustained_foreign_edits_conflict_no_arbiter(
    service: CoordinatorService,
) -> None:
    files = _FakeFileStore()
    files.put("doc.md", b"captured", 3)
    resolver = _FakeResolver()
    resolver.keep("doc.md", 3, b"captured")
    versioner = _versioner(service, resolver=resolver)
    versioner.add_file_member(files, "doc.md")
    checkpoint_id = versioner.checkpoint("file-storm").record.checkpoint_id

    files.put("doc.md", b"edited", 5)
    # More foreign edits than the budget: every CAS detects a moved version.
    files.schedule_foreign_edit_after_read(
        "doc.md", *[f"foreign-{i}".encode() for i in range(2 * MAX_RESTORE_LEG_REDRIVES)]
    )

    restorer = _versioner(service, resolver=resolver)
    restorer.add_file_member(files, "doc.md")
    report = restorer.restore(checkpoint_id)

    (member,) = report.members
    assert member.outcome == RESTORE_OUTCOME_CONFLICT
    assert member.attempts == MAX_RESTORE_LEG_REDRIVES + 1
    assert "no-arbiter" in member.detail
    assert files.cas_calls["doc.md"] == MAX_RESTORE_LEG_REDRIVES + 1  # bounded, no livelock


# ---------------------------------------------------------------------------
# Crash-resume (durable progress; no double-apply)
# ---------------------------------------------------------------------------


def test_crash_mid_restore_fresh_engine_resumes_without_double_apply(
    registry: ArtifactRegistry, service: CoordinatorService
) -> None:
    client = LocalS3Client()
    client.create_bucket("demo", versioned=True, object_lock=True)
    for key in ("a.json", "b.json", "c.json"):
        client.put_object(Bucket="demo", Key=key, Body=f"{key}-v1".encode())

    obj = CoherentObject("demo", client=client)
    versioner = _versioner(service)
    for key in ("a.json", "b.json", "c.json"):
        versioner.add_object_member(obj, key)
    checkpoint_id = versioner.checkpoint("crashy").record.checkpoint_id

    for key in ("a.json", "b.json", "c.json"):
        client.put_object(Bucket="demo", Key=key, Body=f"{key}-diverged".encode())

    # Crash on the SECOND member's durable outcome write: member a concluded
    # durably; member b's substrate write LANDED but its record did not.
    crashing = _CrashOnNthMemberRecord(service, fail_on_call=2)
    crashed = _versioner(crashing)
    for key in ("a.json", "b.json", "c.json"):
        crashed.add_object_member(obj, key)
    with pytest.raises(RuntimeError, match="simulated crash"):
        crashed.restore(checkpoint_id)

    # Mid-crash durable state: in_progress, member a terminal, b/c pending.
    record = registry.get_checkpoint(checkpoint_id)
    assert record is not None and record.restore_status == RESTORE_STATUS_IN_PROGRESS
    stored = {m.member_path: m for m in registry.get_checkpoint_members(checkpoint_id)}
    assert stored["s3://a.json"].restore_outcome == RESTORE_OUTCOME_RESTORED
    assert stored["s3://b.json"].restore_outcome is None
    assert stored["s3://c.json"].restore_outcome is None

    puts_before_resume = [c["Key"] for c in client.put_calls]

    # A FRESH engine resumes from durable state alone.
    resumed = _versioner(service)
    for key in ("a.json", "b.json", "c.json"):
        resumed.add_object_member(obj, key)
    report = resumed.restore(checkpoint_id)

    by_path = report.members_by_path
    assert by_path["s3://a.json"].resumed_from_prior_run is True
    assert by_path["s3://a.json"].outcome == RESTORE_OUTCOME_RESTORED
    # Member b's pre-crash write already landed: token identity concludes it
    # CONVERGED with NO second write (no double-apply).
    assert by_path["s3://b.json"].outcome == RESTORE_OUTCOME_CONVERGED
    assert by_path["s3://c.json"].outcome == RESTORE_OUTCOME_RESTORED
    puts_after_resume = [c["Key"] for c in client.put_calls[len(puts_before_resume):]]
    assert "a.json" not in puts_after_resume  # skipped, not re-driven
    assert "b.json" not in puts_after_resume  # converged, not re-applied
    assert puts_after_resume.count("c.json") == 1
    record = registry.get_checkpoint(checkpoint_id)
    assert record is not None and record.restore_status == RESTORE_STATUS_CONCLUDED
    # Every substrate holds the manifest bytes exactly once.
    for key in ("a.json", "b.json", "c.json"):
        data, _etag = obj.read(key)
        assert data == f"{key}-v1".encode()


def test_concluded_checkpoint_restore_is_idempotent_report_only(
    service: CoordinatorService,
) -> None:
    client, obj = _s3()
    client.put_object(Bucket="demo", Key="cfg.json", Body=b"v1")
    versioner = _versioner(service)
    versioner.add_object_member(obj, "cfg.json")
    checkpoint_id = versioner.checkpoint("twice").record.checkpoint_id

    first = versioner.restore(checkpoint_id)
    puts_after_first = len(client.put_calls)
    second = versioner.restore(checkpoint_id)

    assert first.status == second.status == RESTORE_STATUS_CONCLUDED
    assert [m.outcome for m in second.members] == [m.outcome for m in first.members]
    assert all(m.resumed_from_prior_run for m in second.members)
    assert len(client.put_calls) == puts_after_first  # nothing re-driven


# ---------------------------------------------------------------------------
# Absorbing outcomes: expired pin, forward-only, degenerate shapes
# ---------------------------------------------------------------------------


def test_expired_pin_yields_target_lost_and_restore_concludes(
    service: CoordinatorService,
) -> None:
    client, obj = _s3()
    client.put_object(Bucket="demo", Key="cfg.json", Body=b"v1")
    versioner = _versioner(service)
    versioner.add_object_member(obj, "cfg.json")
    # pin=False: the deliberate capture-only shape — no legal hold lands, so
    # the manifested version has nothing protecting it (the Unit-6 pin-path
    # twin of this test proves the held version SURVIVES the same expiry).
    checkpoint_id = versioner.checkpoint("pinned", pin=False).record.checkpoint_id

    # A live writer makes the captured version noncurrent; a lifecycle rule
    # then expires it (no legal hold established): the pin target is gone.
    client.put_object(Bucket="demo", Key="cfg.json", Body=b"v2")
    expired, held = client.lifecycle_expire_noncurrent("demo", "cfg.json")
    assert expired and not held

    report = versioner.restore(checkpoint_id)
    (member,) = report.members
    assert member.outcome == RESTORE_OUTCOME_TARGET_LOST
    assert report.status == RESTORE_STATUS_CONCLUDED
    data, _etag = obj.read("cfg.json")
    assert data == b"v2"  # the live state was never touched


def test_forward_only_and_unversioned_members_skipped_and_enumerated(
    service: CoordinatorService,
) -> None:
    client, obj = _s3(versioned=False)
    client.put_object(Bucket="demo", Key="plain.txt", Body=b"unversioned")
    files = _FakeFileStore()
    files.put("a.txt", b"a", 1)
    resolver = _FakeResolver()
    resolver.keep("a.txt", 1, b"a")

    versioner = _versioner(service, resolver=resolver)
    versioner.add_file_member(files, "a.txt")
    versioner.add_object_member(obj, "plain.txt")  # tiered forward_only (no pointer)
    versioner.add_forward_only_member("effects/send-email")
    checkpoint_id = versioner.checkpoint("skips").record.checkpoint_id

    report = versioner.restore(checkpoint_id)
    by_path = report.members_by_path
    # BOTH forward-only shapes are enumerated in the report, never silent:
    # the declared action surface AND the unversioned-S3 capture refusal.
    assert by_path["effects/send-email"].outcome == RESTORE_OUTCOME_FORWARD_ONLY_SKIPPED
    assert by_path["s3://plain.txt"].outcome == RESTORE_OUTCOME_FORWARD_ONLY_SKIPPED
    assert by_path["a.txt"].outcome == RESTORE_OUTCOME_CONVERGED
    assert report.status == RESTORE_STATUS_CONCLUDED


def test_all_absent_degenerate_shape_concludes_cleanly(
    registry: ArtifactRegistry, service: CoordinatorService
) -> None:
    """Every member ABSENT at capture and still absent live: nothing to write,
    nothing to delete — all converged, the restore concludes (no registration
    exists in this unit, so conclusion == status update + report)."""
    _client, obj = _s3()
    files = _FakeFileStore()  # nothing stored -> FileNotFoundError
    versioner = _versioner(service)
    versioner.add_file_member(files, "gone.md")
    versioner.add_object_member(obj, "gone.json")
    checkpoint_id = versioner.checkpoint("all-absent").record.checkpoint_id

    report = versioner.restore(checkpoint_id)
    assert {m.outcome for m in report.members} == {RESTORE_OUTCOME_CONVERGED}
    assert report.status == RESTORE_STATUS_CONCLUDED
    record = registry.get_checkpoint(checkpoint_id)
    assert record is not None and record.restore_status == RESTORE_STATUS_CONCLUDED


def test_absent_manifest_file_present_live_is_labeled_conflict(
    service: CoordinatorService,
) -> None:
    """The v1 residual: a live file the manifest records ABSENT cannot be
    deleted through the file seam — absorbed as conflict, labeled no-arbiter,
    and the restore still concludes."""
    files = _FakeFileStore()  # absent at capture
    versioner = _versioner(service)
    versioner.add_file_member(files, "late.md")
    checkpoint_id = versioner.checkpoint("late-file").record.checkpoint_id

    files.put("late.md", b"appeared later", 2)
    report = versioner.restore(checkpoint_id)
    (member,) = report.members
    assert member.outcome == RESTORE_OUTCOME_CONFLICT
    assert "no-arbiter" in member.detail
    assert report.status == RESTORE_STATUS_CONCLUDED
    assert files.state("late.md")[0] == b"appeared later"  # never touched


# ---------------------------------------------------------------------------
# HOLD terminals (UNCONFIRMED — never best-effort)
# ---------------------------------------------------------------------------


def test_s3_unknown_write_outcome_reconciles_to_held_unconfirmed(
    service: CoordinatorService,
) -> None:
    client = LocalS3Client()
    client.create_bucket("demo", versioned=True, object_lock=True)
    client.put_object(Bucket="demo", Key="flaky.json", Body=b"original")
    checkpoint_id = _checkpoint_one_s3_member(service, client, "flaky.json", b"original")

    client.put_object(Bucket="demo", Key="flaky.json", Body=b"diverged")
    vanishing = _VanishOnCasPut(client, "demo", "flaky.json")
    restorer = _versioner(service)
    restorer.add_object_member(CoherentObject("demo", client=vanishing), "flaky.json")
    report = restorer.restore(checkpoint_id)

    (member,) = report.members
    assert member.outcome == RESTORE_OUTCOME_HELD_UNCONFIRMED
    assert "HELD" in member.detail and "best-effort" in member.detail
    assert report.status == RESTORE_STATUS_CONCLUDED


def test_file_commit_unconfirmed_is_held(service: CoordinatorService) -> None:
    class _UnconfirmedStore(_FakeFileStore):
        def write_cas_at(self, path: str, expected_version: int, new_content: bytes) -> None:
            raise CommitUnconfirmed("transport failed mid-commit")

    files = _UnconfirmedStore()
    files.put("doc.md", b"captured", 3)
    resolver = _FakeResolver()
    resolver.keep("doc.md", 3, b"captured")
    versioner = _versioner(service, resolver=resolver)
    versioner.add_file_member(files, "doc.md")
    checkpoint_id = versioner.checkpoint("unconfirmed").record.checkpoint_id

    files.put("doc.md", b"edited", 5)
    report = versioner.restore(checkpoint_id)
    (member,) = report.members
    assert member.outcome == RESTORE_OUTCOME_HELD_UNCONFIRMED
    assert report.status == RESTORE_STATUS_CONCLUDED


# ---------------------------------------------------------------------------
# Pre-flight refusals (typed; nothing started)
# ---------------------------------------------------------------------------


def test_unknown_checkpoint_typed_preflight_refusal(service: CoordinatorService) -> None:
    versioner = _versioner(service)
    with pytest.raises(CheckpointUnknown) as excinfo:
        versioner.restore("no-such-checkpoint")
    assert excinfo.value.reason is CHECKPOINT_UNKNOWN_REASON
    assert excinfo.value.checkpoint_id == "no-such-checkpoint"


def test_capture_only_service_refused_before_any_state(
    service: CoordinatorService,
) -> None:
    files = _FakeFileStore()
    files.put("a.txt", b"a", 1)
    versioner = _versioner(service)
    versioner.add_file_member(files, "a.txt")
    checkpoint_id = versioner.checkpoint("cap-only").record.checkpoint_id

    # A capture-only seam (create_workspace_checkpoint only) cannot record a
    # crash-resumable restore: refused with TypeError, before any progress.
    crippled = WorkspaceVersioner(service=_DownService(), owner=OWNER)
    crippled.add_file_member(files, "a.txt")
    with pytest.raises(TypeError, match="CheckpointRestoreStore"):
        crippled.restore(checkpoint_id)


def test_preflight_missing_binding_and_resolver_fail_before_status(
    registry: ArtifactRegistry, service: CoordinatorService
) -> None:
    files = _FakeFileStore()
    files.put("doc.md", b"body", 2)
    resolver = _FakeResolver()
    resolver.keep("doc.md", 2, b"body")
    versioner = _versioner(service, resolver=resolver)
    versioner.add_file_member(files, "doc.md")
    checkpoint_id = versioner.checkpoint("preflight").record.checkpoint_id

    # Fresh engine with NO member bindings declared.
    bare = _versioner(service, resolver=resolver)
    with pytest.raises(ValueError, match="no declared file/object member binding"):
        bare.restore(checkpoint_id)

    # Bindings declared but no resolver for an actionable file member.
    no_resolver = _versioner(service)
    no_resolver.add_file_member(files, "doc.md")
    with pytest.raises(ValueError, match="FileContentResolver"):
        no_resolver.restore(checkpoint_id)

    # Nothing was started: the checkpoint never left status "none".
    record = registry.get_checkpoint(checkpoint_id)
    assert record is not None and record.restore_status == RESTORE_STATUS_NONE


# ---------------------------------------------------------------------------
# Service-level restore vocabulary (closed sets, fail-closed)
# ---------------------------------------------------------------------------


def test_service_restore_vocabulary_fails_closed(
    registry: ArtifactRegistry, service: CoordinatorService
) -> None:
    files = _FakeFileStore()
    files.put("a.txt", b"a", 1)
    versioner = _versioner(service)
    versioner.add_file_member(files, "a.txt")
    checkpoint_id = versioner.checkpoint("vocab").record.checkpoint_id

    with pytest.raises(ValueError, match="unknown restore status"):
        service.set_workspace_checkpoint_restore_status(
            checkpoint_id, "magic", updated_at=1.0
        )
    with pytest.raises(ValueError, match="unknown restore outcome"):
        service.set_workspace_checkpoint_member_restore(
            checkpoint_id, "a.txt", restore_outcome="magic"
        )
    # Nothing landed from either reject.
    record = registry.get_checkpoint(checkpoint_id)
    assert record is not None and record.restore_status == RESTORE_STATUS_NONE
    (member,) = registry.get_checkpoint_members(checkpoint_id)
    assert member.restore_outcome is None


# ===========================================================================
# Registration — WV plan Unit 5 (R4/R5): coordinator registration of restore
# results (all-or-nothing for writes), registered-writer concurrency semantics
# (invalidation + fence), and restore monotonicity (forward commit carrying
# old bytes).
# ===========================================================================


class _RegistrationProbeService(CoordinatorService):
    """CoordinatorService instrumented for the Unit-5 tests: call counters,
    the durable status-write trace, and deterministic crash / race injection
    hooks (the delay-injection harness's registration half)."""

    def __init__(self, registry: ArtifactRegistry) -> None:
        super().__init__(registry)
        self.commit_all_calls = 0
        self.register_calls = 0
        self.status_writes: list[str] = []
        #: Callable fired INSIDE the first commit_all, before it runs — lands
        #: a peer commit between the registration's comparand read and its CAS.
        self.before_first_commit_all: Any | None = None
        #: 1-based register call to crash BEFORE delegating (kill between the
        #: legs and the registration).
        self.crash_on_register_call: int | None = None
        #: Crash AFTER the registration landed (the marker-write window).
        self.crash_after_register_once: bool = False
        #: Status value whose FIRST durable write crashes (e.g. "concluded").
        self.crash_on_status_once: str | None = None

    def commit_all(self, **kwargs: Any) -> Any:
        self.commit_all_calls += 1
        if self.commit_all_calls == 1 and self.before_first_commit_all is not None:
            self.before_first_commit_all()
        return super().commit_all(**kwargs)

    def register_workspace_restore(self, **kwargs: Any) -> Any:
        self.register_calls += 1
        if self.crash_on_register_call == self.register_calls:
            raise RuntimeError("simulated crash before registration")
        result = super().register_workspace_restore(**kwargs)
        if self.crash_after_register_once:
            self.crash_after_register_once = False
            raise RuntimeError("simulated crash after registration landed")
        return result

    def set_workspace_checkpoint_restore_status(
        self, checkpoint_id: str, status: str, **kwargs: Any
    ) -> None:
        if self.crash_on_status_once == status:
            self.crash_on_status_once = None
            raise RuntimeError(f"simulated crash on status write {status!r}")
        self.status_writes.append(status)
        super().set_workspace_checkpoint_restore_status(checkpoint_id, status, **kwargs)


def _probe_service() -> tuple[ArtifactRegistry, _RegistrationProbeService]:
    registry = ArtifactRegistry()
    return registry, _RegistrationProbeService(registry)


_FP_PLAN = sha256_hex(b"plan text")


def _checkpoint_diverged_file(
    service: Any,
) -> tuple[str, _FakeFileStore, _FakeResolver]:
    """One file member captured at (b'plan text', v7) then diverged live, so
    the restore leg must WRITE — the written-member registration shape."""
    files = _FakeFileStore()
    files.put("notes/plan.md", b"plan text", 7)
    resolver = _FakeResolver()
    resolver.keep("notes/plan.md", 7, b"plan text")
    versioner = _versioner(service, resolver=resolver)
    versioner.add_file_member(files, "notes/plan.md")
    checkpoint_id = versioner.checkpoint("reg").record.checkpoint_id
    files.put("notes/plan.md", b"edited", 9)
    return checkpoint_id, files, resolver


def _restorer_for(service: Any, files: _FakeFileStore, resolver: _FakeResolver) -> Any:
    restorer = _versioner(service, resolver=resolver)
    restorer.add_file_member(files, "notes/plan.md")
    return restorer


def test_restored_file_member_registers_via_commit_all_and_invalidates_peer() -> None:
    """The written-file leg of the registration split: ONE all-or-nothing
    commit_all, hash-only, forward version bump, registered peer invalidated."""
    registry, service = _probe_service()
    art = service.register_artifact(
        name="notes/plan.md", content="old-body", content_hash=sha256_hex(b"old-body")
    )
    peer = uuid.uuid4()
    registry.set_agent_state(art.id, peer, MESIState.SHARED, trigger="fetch", tick=1)
    checkpoint_id, files, resolver = _checkpoint_diverged_file(service)

    report = _restorer_for(service, files, resolver).restore(checkpoint_id)

    (member,) = report.members
    assert member.outcome == RESTORE_OUTCOME_RESTORED
    reg = report.registration
    assert reg is not None
    # Typed status matched by IDENTITY (the constant object flows through).
    assert reg.status is WORKSPACE_REGISTRATION_COMMITTED
    assert reg.registered == {"notes/plan.md": 2}
    assert reg.attempts == 1
    assert service.commit_all_calls == 1

    # The coordinator side: forward commit carrying OLD bytes' hash — version
    # strictly increases, content hash is the manifest fingerprint, and the
    # body NEVER entered the coordinator (hash-only registration).
    updated = registry.get_artifact(art.id)
    assert updated is not None
    assert updated.version == 2
    check_monotonic_version(1, updated.version)  # never a decrement
    assert updated.content_hash == _FP_PLAN
    assert registry.get_content(art.id) == "old-body"  # unchanged: no bytes in

    # Registered-writer concurrency: the peer holding SHARED was invalidated
    # atomically by the commit and the signal count surfaces on the report.
    assert registry.get_agent_state(art.id, peer) == MESIState.INVALID
    assert reg.invalidated_peers == 1

    # The durable idempotency marker walked in_progress -> registered -> concluded.
    assert service.status_writes == [
        RESTORE_STATUS_IN_PROGRESS,
        RESTORE_STATUS_REGISTERED,
        RESTORE_STATUS_CONCLUDED,
    ]


def test_s3_written_member_registration_is_manifest_side_only() -> None:
    """The S3 leg of the split: a written BYO-substrate member registers
    MANIFEST-SIDE (its durable outcome row) — no coordinator artifact row is
    ever forced (the substrate owns identity), and the commit_all write-set
    holds ONLY the file member."""
    registry, service = _probe_service()
    service.register_artifact(
        name="notes/plan.md", content="old-body", content_hash=sha256_hex(b"old-body")
    )
    client, obj = _s3()
    client.put_object(Bucket="demo", Key="cfg.json", Body=b"cfg-v1")
    files = _FakeFileStore()
    files.put("notes/plan.md", b"plan text", 7)
    resolver = _FakeResolver()
    resolver.keep("notes/plan.md", 7, b"plan text")
    versioner = _versioner(service, resolver=resolver)
    versioner.add_file_member(files, "notes/plan.md")
    versioner.add_object_member(obj, "cfg.json")
    checkpoint_id = versioner.checkpoint("split").record.checkpoint_id
    files.put("notes/plan.md", b"edited", 9)
    client.put_object(Bucket="demo", Key="cfg.json", Body=b"cfg-v2")

    restorer = _versioner(service, resolver=resolver)
    restorer.add_file_member(files, "notes/plan.md")
    restorer.add_object_member(obj, "cfg.json")
    report = restorer.restore(checkpoint_id)

    by_path = report.members_by_path
    assert by_path["notes/plan.md"].outcome == RESTORE_OUTCOME_RESTORED
    assert by_path["s3://cfg.json"].outcome == RESTORE_OUTCOME_RESTORED
    reg = report.registration
    assert reg is not None
    assert reg.status == WORKSPACE_REGISTRATION_COMMITTED
    assert reg.registered == {"notes/plan.md": 2}
    assert reg.substrate_registered == ("s3://cfg.json",)
    assert service.commit_all_calls == 1
    # No coordinator artifact identity was minted for the S3 member.
    names = {registry.get_artifact(aid).name for aid in registry.artifact_ids()}
    assert "s3://cfg.json" not in names
    # Its manifest-side registration IS the durable outcome row.
    stored = {m.member_path: m for m in registry.get_checkpoint_members(checkpoint_id)}
    assert stored["s3://cfg.json"].restore_outcome == RESTORE_OUTCOME_RESTORED


def test_delete_only_restore_records_only_never_calls_commit_all() -> None:
    """The delete leg of the split: manifest-side deleted_at_restore records
    ONLY — commit_all is NEVER called (asserted via the counting service), and
    the registration concludes typed-EMPTY."""
    registry, service = _probe_service()
    client, obj = _s3()
    versioner = _versioner(service)
    versioner.add_object_member(obj, "ghost.json")  # ABSENT at capture
    checkpoint_id = versioner.checkpoint("del-only").record.checkpoint_id
    client.put_object(Bucket="demo", Key="ghost.json", Body=b"intruder")

    restorer = _versioner(service)
    restorer.add_object_member(obj, "ghost.json")
    report = restorer.restore(checkpoint_id)

    (member,) = report.members
    assert member.outcome == RESTORE_OUTCOME_RESTORED
    assert member.deleted_at_restore is not None
    reg = report.registration
    assert reg is not None
    assert reg.status == WORKSPACE_REGISTRATION_EMPTY
    assert reg.deleted_recorded == ("s3://ghost.json",)
    assert service.commit_all_calls == 0
    assert service.register_calls == 0  # the empty write-set never leaves the seam
    (stored,) = registry.get_checkpoint_members(checkpoint_id)
    assert stored.deleted_at_restore is not None
    record = registry.get_checkpoint(checkpoint_id)
    assert record is not None and record.restore_status == RESTORE_STATUS_CONCLUDED


def test_all_converged_empty_restore_concludes_with_empty_registration() -> None:
    """Every member converged/skipped: the commit write-set is empty — the
    status-update conclusion alone, commit_all never called."""
    registry, service = _probe_service()
    files = _FakeFileStore()
    files.put("calm.md", b"calm", 5)
    resolver = _FakeResolver()
    resolver.keep("calm.md", 5, b"calm")
    versioner = _versioner(service, resolver=resolver)
    versioner.add_file_member(files, "calm.md")
    versioner.add_forward_only_member("effects/notify")
    checkpoint_id = versioner.checkpoint("noop").record.checkpoint_id

    report = versioner.restore(checkpoint_id)

    by_path = report.members_by_path
    assert by_path["calm.md"].outcome == RESTORE_OUTCOME_CONVERGED
    assert by_path["effects/notify"].outcome == RESTORE_OUTCOME_FORWARD_ONLY_SKIPPED
    reg = report.registration
    assert reg is not None
    assert reg.status == WORKSPACE_REGISTRATION_EMPTY
    assert reg.registered == {} and reg.deleted_recorded == ()
    assert service.commit_all_calls == 0
    assert service.register_calls == 0
    record = registry.get_checkpoint(checkpoint_id)
    assert record is not None and record.restore_status == RESTORE_STATUS_CONCLUDED


def test_crash_between_legs_and_registration_resume_registers_exactly_once() -> None:
    """Registration all-or-nothing under crash injection: a kill between the
    legs and the registration leaves NO partial coordinator state; the resumed
    run registers exactly once."""
    registry, service = _probe_service()
    art = service.register_artifact(
        name="notes/plan.md", content="old-body", content_hash=sha256_hex(b"old-body")
    )
    checkpoint_id, files, resolver = _checkpoint_diverged_file(service)

    service.crash_on_register_call = 1
    with pytest.raises(RuntimeError, match="before registration"):
        _restorer_for(service, files, resolver).restore(checkpoint_id)

    # No partial coordinator state: the artifact never moved, the marker never
    # landed, the member outcome is durable (the legs concluded).
    assert registry.get_artifact(art.id).version == 1
    record = registry.get_checkpoint(checkpoint_id)
    assert record is not None and record.restore_status == RESTORE_STATUS_IN_PROGRESS
    (stored,) = registry.get_checkpoint_members(checkpoint_id)
    assert stored.restore_outcome == RESTORE_OUTCOME_RESTORED

    # Resume: the registration runs EXACTLY once (one commit_all, one bump).
    service.crash_on_register_call = None
    report = _restorer_for(service, files, resolver).restore(checkpoint_id)
    (member,) = report.members
    assert member.resumed_from_prior_run is True
    reg = report.registration
    assert reg is not None
    assert reg.status == WORKSPACE_REGISTRATION_COMMITTED
    assert reg.registered == {"notes/plan.md": 2}
    assert service.commit_all_calls == 1
    assert registry.get_artifact(art.id).version == 2
    assert service.status_writes == [
        RESTORE_STATUS_IN_PROGRESS,  # crashed run
        RESTORE_STATUS_IN_PROGRESS,  # resume
        RESTORE_STATUS_REGISTERED,
        RESTORE_STATUS_CONCLUDED,
    ]


def test_crash_after_registration_landed_before_marker_no_double_commit() -> None:
    """The marker-write crash window: the commit landed but 'registered' never
    did. The resumed registration re-runs and the hash filter answers it
    EMPTY — exactly-once, no second version bump."""
    registry, service = _probe_service()
    art = service.register_artifact(
        name="notes/plan.md", content="old-body", content_hash=sha256_hex(b"old-body")
    )
    checkpoint_id, files, resolver = _checkpoint_diverged_file(service)

    service.crash_after_register_once = True
    with pytest.raises(RuntimeError, match="after registration landed"):
        _restorer_for(service, files, resolver).restore(checkpoint_id)

    # The commit landed; the marker did not.
    assert registry.get_artifact(art.id).version == 2
    record = registry.get_checkpoint(checkpoint_id)
    assert record is not None and record.restore_status == RESTORE_STATUS_IN_PROGRESS

    report = _restorer_for(service, files, resolver).restore(checkpoint_id)
    reg = report.registration
    assert reg is not None
    assert reg.status == WORKSPACE_REGISTRATION_EMPTY  # already at the fingerprint
    assert reg.skipped == ("notes/plan.md",)
    assert service.commit_all_calls == 1  # never a second commit
    assert registry.get_artifact(art.id).version == 2  # exactly one bump
    record = registry.get_checkpoint(checkpoint_id)
    assert record is not None and record.restore_status == RESTORE_STATUS_CONCLUDED


def test_crash_after_registered_marker_resume_skips_registration() -> None:
    """The durable marker honored: a crash AFTER 'registered' but before
    'concluded' resumes WITHOUT re-entering the registration step at all."""
    registry, service = _probe_service()
    art = service.register_artifact(
        name="notes/plan.md", content="old-body", content_hash=sha256_hex(b"old-body")
    )
    checkpoint_id, files, resolver = _checkpoint_diverged_file(service)

    service.crash_on_status_once = RESTORE_STATUS_CONCLUDED
    with pytest.raises(RuntimeError, match="concluded"):
        _restorer_for(service, files, resolver).restore(checkpoint_id)

    record = registry.get_checkpoint(checkpoint_id)
    assert record is not None and record.restore_status == RESTORE_STATUS_REGISTERED
    assert registry.get_artifact(art.id).version == 2
    register_calls_before = service.register_calls

    report = _restorer_for(service, files, resolver).restore(checkpoint_id)
    reg = report.registration
    assert reg is not None
    assert reg.status is WORKSPACE_REGISTRATION_PRIOR_RUN
    assert service.register_calls == register_calls_before  # step never re-entered
    assert service.commit_all_calls == 1
    assert registry.get_artifact(art.id).version == 2  # exactly once
    record = registry.get_checkpoint(checkpoint_id)
    assert record is not None and record.restore_status == RESTORE_STATUS_CONCLUDED


def test_registered_live_writer_racing_registration_retries_bounded_and_wins() -> None:
    """A registered live writer's commit lands between the registration's
    comparand read and its commit_all: the batch is HELD version_mismatch
    (all-or-nothing, nothing mutated), the seam re-drives from fresh
    comparands (bounded), and the landed registration invalidates the peer."""
    registry, service = _probe_service()
    art = service.register_artifact(
        name="notes/plan.md", content="old-body", content_hash=sha256_hex(b"old-body")
    )
    peer = uuid.uuid4()
    registry.set_agent_state(art.id, peer, MESIState.SHARED, trigger="fetch", tick=1)
    checkpoint_id, files, resolver = _checkpoint_diverged_file(service)

    def _peer_commit_lands_first() -> None:
        result = registry.commit_cas(
            art.id,
            peer,
            expected_version=1,
            content_hash=sha256_hex(b"peer-body"),
            content="peer-body",
            tick=5,
        )
        assert not isinstance(result, ConflictDetail)  # the peer WON (v1 -> v2)

    service.before_first_commit_all = _peer_commit_lands_first
    report = _restorer_for(service, files, resolver).restore(checkpoint_id)

    reg = report.registration
    assert reg is not None
    assert reg.status == WORKSPACE_REGISTRATION_COMMITTED
    assert reg.attempts == 2  # attempt 1 HELD version_mismatch; attempt 2 landed
    assert service.commit_all_calls == 2
    # Forward past the peer's commit: v1 -> (peer) v2 -> (registration) v3.
    updated = registry.get_artifact(art.id)
    assert updated.version == 3
    check_monotonic_version(2, updated.version)
    assert updated.content_hash == _FP_PLAN
    assert reg.registered == {"notes/plan.md": 3}
    # The peer (SHARED after its own OCC win) was invalidated by the landing.
    assert registry.get_agent_state(art.id, peer) == MESIState.INVALID
    assert reg.invalidated_peers == 1


def test_superseded_controller_late_apply_rejected_by_fence() -> None:
    """The read-generation fence: a controller whose grant a sweep reclaimed
    is SUPERSEDED — its late registration apply is rejected
    (stale_read_generation), NEVER retried, and nothing registers
    (all-or-nothing). The restore still concludes, refusal reported."""
    registry, service = _probe_service()
    art = service.register_artifact(
        name="notes/plan.md", content="old-body", content_hash=sha256_hex(b"old-body")
    )
    # The controller (the versioner's owner) held EXCLUSIVE (read_gen 0), then
    # the sweep reclaimed it (owner_gen 1): superseded.
    registry.set_agent_state(art.id, OWNER, MESIState.EXCLUSIVE, trigger="write", tick=1)
    registry.set_agent_state(
        art.id, OWNER, MESIState.INVALID, trigger="reclaim_heartbeat", tick=2
    )
    checkpoint_id, files, resolver = _checkpoint_diverged_file(service)

    report = _restorer_for(service, files, resolver).restore(checkpoint_id)

    reg = report.registration
    assert reg is not None
    assert reg.status is WORKSPACE_REGISTRATION_REFUSED
    assert reg.refused == {"notes/plan.md": STALE_READ_GENERATION_REASON}
    assert service.commit_all_calls == 1  # the fence is terminal on sight
    # The late apply was fenced out: no phantom bump, hash untouched.
    updated = registry.get_artifact(art.id)
    assert updated.version == 1
    assert updated.content_hash == sha256_hex(b"old-body")
    # The restore still CONCLUDES; the refusal skipped the 'registered' marker
    # (a later resume may re-attempt; the fence rejects it again).
    assert report.status == RESTORE_STATUS_CONCLUDED
    assert service.status_writes == [
        RESTORE_STATUS_IN_PROGRESS,
        RESTORE_STATUS_CONCLUDED,
    ]


def test_restore_monotonicity_forward_commit_carrying_old_bytes() -> None:
    """R5: restore registers as a FORWARD commit — the coordinator version
    strictly increases past every live-writer commit while the content hash
    returns to the captured (old) fingerprint; never a decrement anywhere."""
    registry, service = _probe_service()
    art = service.register_artifact(
        name="notes/plan.md", content="v1-body", content_hash=sha256_hex(b"v1-body")
    )
    checkpoint_id, files, resolver = _checkpoint_diverged_file(service)

    # Live writers advance the coordinator past the capture: v1 -> v4.
    observed_versions = [registry.get_artifact(art.id).version]
    writer = uuid.uuid4()
    for i in range(3):
        current = registry.get_artifact(art.id).version
        result = registry.commit_cas(
            art.id,
            writer,
            expected_version=current,
            content_hash=sha256_hex(f"writer-{i}".encode()),
            content=f"writer-{i}",
            tick=10 + i,
        )
        assert not isinstance(result, ConflictDetail)
        observed_versions.append(registry.get_artifact(art.id).version)
    assert observed_versions == [1, 2, 3, 4]

    report = _restorer_for(service, files, resolver).restore(checkpoint_id)

    reg = report.registration
    assert reg is not None
    assert reg.status == WORKSPACE_REGISTRATION_COMMITTED
    updated = registry.get_artifact(art.id)
    # Strictly increasing across the whole history — old bytes, NEW version.
    observed_versions.append(updated.version)
    assert observed_versions == [1, 2, 3, 4, 5]
    for previous, current in zip(observed_versions, observed_versions[1:]):
        check_monotonic_version(previous, current)
        assert current > previous  # strict: a restore is never a re-stamp
    assert updated.content_hash == _FP_PLAN  # the captured state, re-imposed


def test_concluded_report_from_durable_rows_carries_no_registration() -> None:
    """A report rebuilt from durable rows (idempotent re-restore of a
    concluded checkpoint) carries registration=None — the detail is run-local
    by design; the durable truths are the marker and the artifact versions."""
    _registry, service = _probe_service()
    files = _FakeFileStore()
    files.put("calm.md", b"calm", 5)
    resolver = _FakeResolver()
    resolver.keep("calm.md", 5, b"calm")
    versioner = _versioner(service, resolver=resolver)
    versioner.add_file_member(files, "calm.md")
    checkpoint_id = versioner.checkpoint("re").record.checkpoint_id
    first = versioner.restore(checkpoint_id)
    assert first.registration is not None
    second = versioner.restore(checkpoint_id)
    assert second.registration is None
    assert second.status == RESTORE_STATUS_CONCLUDED


# ---------------------------------------------------------------------------
# Route level — Unit-5 restore progress + registration endpoints (the remote
# half of the CheckpointRestoreStore seam; mirrors the checkpoint routes)
# ---------------------------------------------------------------------------


def _route_checkpoint(client, *, member_path: str = "notes/plan.md") -> str:
    sid = str(uuid.uuid4())
    status, body = client(
        "POST",
        "/workspace/checkpoint",
        {
            "session_id": sid,
            "name": "route-restore-cp",
            "window_min": 100.0,
            "window_max": 100.0,
            "members": [
                _member_wire(
                    member_path,
                    native_token="7",
                    fingerprint=sha256_hex(b"plan text"),
                    arbitration_tier="no-arbiter",
                    restore_tier="restorable-unpinned",
                )
            ],
        },
    )
    assert status == 200 and body["ok"] is True
    return body["checkpoint_id"]


def test_route_restore_progress_roundtrip(client) -> None:
    sid = str(uuid.uuid4())
    checkpoint_id = _route_checkpoint(client)

    status, body = client(
        "POST",
        "/workspace/restore/status",
        {"session_id": sid, "checkpoint_id": checkpoint_id, "status": "in_progress"},
    )
    assert status == 200 and body["ok"] is True

    status, body = client(
        "POST",
        "/workspace/restore/member",
        {
            "session_id": sid,
            "checkpoint_id": checkpoint_id,
            "member_path": "notes/plan.md",
            "restore_outcome": "restored",
        },
    )
    assert status == 200 and body["ok"] is True

    # The full status walk lands durably, including the Unit-5 marker.
    for next_status in ("registered", "concluded"):
        status, body = client(
            "POST",
            "/workspace/restore/status",
            {"session_id": sid, "checkpoint_id": checkpoint_id, "status": next_status},
        )
        assert status == 200 and body["ok"] is True

    status, listing = client("GET", "/workspace/checkpoints")
    assert status == 200
    (cp,) = listing["checkpoints"]
    assert cp["restore_status"] == "concluded"
    (member,) = cp["members"]
    assert member["restore_outcome"] == "restored"


def test_route_restore_progress_validation_fails_closed(client) -> None:
    sid = str(uuid.uuid4())
    checkpoint_id = _route_checkpoint(client)

    # Unknown status — closed vocabulary, boundary-gated.
    status, _ = client(
        "POST",
        "/workspace/restore/status",
        {"session_id": sid, "checkpoint_id": checkpoint_id, "status": "magic"},
    )
    assert status == 400
    # Unknown outcome — closed vocabulary.
    status, _ = client(
        "POST",
        "/workspace/restore/member",
        {
            "session_id": sid,
            "checkpoint_id": checkpoint_id,
            "member_path": "notes/plan.md",
            "restore_outcome": "magic",
        },
    )
    assert status == 400
    # Unknown checkpoint — typed reject body, nothing recorded.
    status, body = client(
        "POST",
        "/workspace/restore/status",
        {"session_id": sid, "checkpoint_id": "nope", "status": "in_progress"},
    )
    assert status == 200 and body["ok"] is False
    status, listing = client("GET", "/workspace/checkpoints")
    (cp,) = listing["checkpoints"]
    assert cp["restore_status"] == "none"


def test_route_restore_register_first_observation_then_commit(client) -> None:
    """Over HTTP against the sqlite-backed coordinator: the first registration
    of an unknown path mints hash-only via resolve_or_register (skipped —
    empty_write_set), and a later differing fingerprint COMMITS forward."""
    sid = str(uuid.uuid4())
    checkpoint_id = _route_checkpoint(client)
    fp_one = sha256_hex(b"plan text")
    fp_two = sha256_hex(b"restored text")

    # Empty writes: typed empty, never a commit.
    status, body = client(
        "POST",
        "/workspace/restore/register",
        {"session_id": sid, "checkpoint_id": checkpoint_id, "writes": []},
    )
    assert status == 200 and body["ok"] is True
    assert body["status"] == "empty_write_set"

    # First observation: the mint IS the registration (skipped, no commit).
    status, body = client(
        "POST",
        "/workspace/restore/register",
        {
            "session_id": sid,
            "checkpoint_id": checkpoint_id,
            "writes": [{"member_path": "notes/plan.md", "fingerprint": fp_one}],
        },
    )
    assert status == 200 and body["ok"] is True
    assert body["status"] == "empty_write_set"
    assert body["skipped"] == ["notes/plan.md"]

    # A differing fingerprint commits FORWARD (v1 -> v2), all-or-nothing.
    status, body = client(
        "POST",
        "/workspace/restore/register",
        {
            "session_id": sid,
            "checkpoint_id": checkpoint_id,
            "writes": [{"member_path": "notes/plan.md", "fingerprint": fp_two}],
        },
    )
    assert status == 200 and body["ok"] is True
    assert body["status"] == "committed"
    assert body["versions"] == {"notes/plan.md": 2}
    assert body["refused"] == {}


def test_route_restore_register_boundary_validation(client) -> None:
    sid = str(uuid.uuid4())
    checkpoint_id = _route_checkpoint(client)
    fp = sha256_hex(b"plan text")

    # Unknown checkpoint — the typed identity-stable reason.
    status, body = client(
        "POST",
        "/workspace/restore/register",
        {
            "session_id": sid,
            "checkpoint_id": "nope",
            "writes": [{"member_path": "notes/plan.md", "fingerprint": fp}],
        },
    )
    assert status == 200 and body["ok"] is False
    assert body["reason"] == CHECKPOINT_UNKNOWN_REASON

    base = {"session_id": sid, "checkpoint_id": checkpoint_id}
    # Malformed fingerprint.
    status, _ = client(
        "POST",
        "/workspace/restore/register",
        {**base, "writes": [{"member_path": "a.md", "fingerprint": "beef"}]},
    )
    assert status == 400
    # Duplicate member paths.
    status, _ = client(
        "POST",
        "/workspace/restore/register",
        {
            **base,
            "writes": [
                {"member_path": "a.md", "fingerprint": fp},
                {"member_path": "a.md", "fingerprint": fp},
            ],
        },
    )
    assert status == 400
    # Non-list writes.
    status, _ = client(
        "POST", "/workspace/restore/register", {**base, "writes": "nope"}
    )
    assert status == 400
    # Absolute member path (the authoritative server-side path gate).
    status, _ = client(
        "POST",
        "/workspace/restore/register",
        {**base, "writes": [{"member_path": "/etc/passwd", "fingerprint": fp}]},
    )
    assert status == 400


# ---------------------------------------------------------------------------
# WV Unit 6 — pin legs (minimal, fail-closed): "restorable means restorable,
# or says restorable-unpinned loudly"
# ---------------------------------------------------------------------------


class _CaptureOnlyService:
    """A persist-only seam (no pin surface): delegates the single-transaction
    registration to a real service but speaks NOTHING else — the shape the
    fold-in's fail-fast gate must refuse when pins are needed."""

    def __init__(self, inner: CoordinatorService) -> None:
        self._inner = inner

    def create_workspace_checkpoint(self, **kwargs: Any) -> Any:
        return self._inner.create_workspace_checkpoint(**kwargs)


def _s3_no_lock() -> tuple[LocalS3Client, CoherentObject]:
    """A VERSIONED bucket WITHOUT Object Lock: history exists, no pin can."""
    client = LocalS3Client()
    client.create_bucket("demo", versioned=True, object_lock=False)
    return client, CoherentObject("demo", client=client)


def test_pin_established_survives_lifecycle_expiry_and_restore_succeeds(
    registry: ArtifactRegistry, service: CoordinatorService
) -> None:
    """The pin's whole point: hold ON at checkpoint → the manifested version
    survives a lifecycle expiry of noncurrent versions → restore serves it."""
    client, obj = _s3()  # object_lock=True
    put = client.put_object(Bucket="demo", Key="cfg.json", Body=b"v1")
    versioner = _versioner(service)
    versioner.add_object_member(obj, "cfg.json")

    cp = versioner.checkpoint("pinned")  # pin=True is the default

    (member,) = cp.members
    assert member.pin_state == PIN_STATE_HELD
    assert member.restore_tier == "restorable"  # the claim is now BACKED
    assert obj.legal_hold_status("cfg.json", version_id=put["VersionId"]) is True
    record = registry.get_checkpoint(cp.record.checkpoint_id)
    assert record is not None and record.pin_refcount == 1

    # A live writer makes the captured version noncurrent; the lifecycle rule
    # REFUSES the held version (the fake's teeth mirror real S3 semantics).
    client.put_object(Bucket="demo", Key="cfg.json", Body=b"v2")
    expired, held = client.lifecycle_expire_noncurrent("demo", "cfg.json")
    assert held == [put["VersionId"]] and expired == []

    report = versioner.restore(cp.record.checkpoint_id)
    assert report.members_by_path["s3://cfg.json"].outcome == RESTORE_OUTCOME_RESTORED
    data, _etag = obj.read("cfg.json")
    assert data == b"v1"  # the pinned bytes are back


def test_no_object_lock_pin_downgrades_loudly_and_durably(
    registry: ArtifactRegistry, service: CoordinatorService
) -> None:
    """No Object Lock → the typed LegalHoldUnavailable → the LOUD durable
    downgrade: tier restorable-unpinned + pin_state pin_unavailable land in
    one registry write, surfaced in the checkpoint return AND the registry."""
    client, obj = _s3_no_lock()
    client.put_object(Bucket="demo", Key="cfg.json", Body=b"v1")
    versioner = _versioner(service)
    versioner.add_object_member(obj, "cfg.json")

    cp = versioner.checkpoint("unpinnable")

    (member,) = cp.members
    assert member.restore_tier == "restorable-unpinned"  # never restorable
    assert member.pin_state == PIN_STATE_UNAVAILABLE
    # Durable, not just the in-memory return: the registry rows agree.
    (stored,) = registry.get_checkpoint_members(cp.record.checkpoint_id)
    assert stored.restore_tier == "restorable-unpinned"
    assert stored.pin_state == PIN_STATE_UNAVAILABLE
    record = registry.get_checkpoint(cp.record.checkpoint_id)
    assert record is not None and record.pin_refcount == 0


def test_downgraded_unpinned_member_expired_is_target_lost(
    service: CoordinatorService,
) -> None:
    """The pin-path extension of the Unit-4 expired-pin scenario: a member the
    pin legs DOWNGRADED (no Object Lock) is exactly the member a lifecycle
    expiry can take — restore then says target_lost, never best-effort."""
    client, obj = _s3_no_lock()
    client.put_object(Bucket="demo", Key="cfg.json", Body=b"v1")
    versioner = _versioner(service)
    versioner.add_object_member(obj, "cfg.json")
    cp = versioner.checkpoint("unpinnable")
    (member,) = cp.members
    assert member.pin_state == PIN_STATE_UNAVAILABLE  # the loud label, upfront

    client.put_object(Bucket="demo", Key="cfg.json", Body=b"v2")
    expired, held = client.lifecycle_expire_noncurrent("demo", "cfg.json")
    assert expired and not held  # nothing protected it

    report = versioner.restore(cp.record.checkpoint_id)
    assert (
        report.members_by_path["s3://cfg.json"].outcome == RESTORE_OUTCOME_TARGET_LOST
    )


def test_internal_release_decrements_and_releases_hold(
    registry: ArtifactRegistry, service: CoordinatorService
) -> None:
    client, obj = _s3()
    put = client.put_object(Bucket="demo", Key="cfg.json", Body=b"v1")
    versioner = _versioner(service)
    versioner.add_object_member(obj, "cfg.json")
    cp = versioner.checkpoint("held")
    assert obj.legal_hold_status("cfg.json", version_id=put["VersionId"]) is True

    rows = versioner._release_checkpoint_pins(cp.record.checkpoint_id)

    (member,) = rows
    assert member.pin_state == PIN_STATE_RELEASED
    # Releasing un-backs the claim: the tier downgrade rides the same write.
    assert member.restore_tier == "restorable-unpinned"
    assert obj.legal_hold_status("cfg.json", version_id=put["VersionId"]) is False
    record = registry.get_checkpoint(cp.record.checkpoint_id)
    assert record is not None and record.pin_refcount == 0

    # Idempotent: nothing held, nothing changes, no refcount underflow.
    rows = versioner._release_checkpoint_pins(cp.record.checkpoint_id)
    assert rows[0].pin_state == PIN_STATE_RELEASED
    record = registry.get_checkpoint(cp.record.checkpoint_id)
    assert record is not None and record.pin_refcount == 0

    # A released pin is terminal for the re-drive too (deliberate drop —
    # pin_checkpoint must not resurrect it).
    rows_after = versioner.pin_checkpoint(cp.record.checkpoint_id)
    assert rows_after[0].pin_state == PIN_STATE_RELEASED
    assert obj.legal_hold_status("cfg.json", version_id=put["VersionId"]) is False

    # And the lifecycle can now take the version (the release is real).
    client.put_object(Bucket="demo", Key="cfg.json", Body=b"v2")
    expired, held = client.lifecycle_expire_noncurrent("demo", "cfg.json")
    assert expired == [put["VersionId"]] and held == []


def test_shared_version_cross_checkpoint_release_keeps_hold(
    registry: ArtifactRegistry, service: CoordinatorService
) -> None:
    """Two checkpoints pin the SAME (member_path, versionId). S3's hold is a
    flag, not a counter — the release's cross-checkpoint scan is the counter:
    the first release must leave the hold standing, the LAST drops it."""
    client, obj = _s3()
    put = client.put_object(Bucket="demo", Key="cfg.json", Body=b"v1")
    versioner = _versioner(service)
    versioner.add_object_member(obj, "cfg.json")
    cp1 = versioner.checkpoint("first")
    cp2 = versioner.checkpoint("second")  # no intervening write: same version
    assert cp1.members[0].native_token == cp2.members[0].native_token
    assert cp1.members[0].pin_state == PIN_STATE_HELD
    assert cp2.members[0].pin_state == PIN_STATE_HELD

    versioner._release_checkpoint_pins(cp1.record.checkpoint_id)

    # cp1's record moved; cp2 still holds, so the SUBSTRATE hold survives.
    (row1,) = registry.get_checkpoint_members(cp1.record.checkpoint_id)
    (row2,) = registry.get_checkpoint_members(cp2.record.checkpoint_id)
    assert row1.pin_state == PIN_STATE_RELEASED
    assert row2.pin_state == PIN_STATE_HELD
    assert obj.legal_hold_status("cfg.json", version_id=put["VersionId"]) is True
    client.put_object(Bucket="demo", Key="cfg.json", Body=b"v2")
    expired, held = client.lifecycle_expire_noncurrent("demo", "cfg.json")
    assert held == [put["VersionId"]] and expired == []

    # Last-out: cp2's release finds no other holder and drops the hold.
    versioner._release_checkpoint_pins(cp2.record.checkpoint_id)
    assert obj.legal_hold_status("cfg.json", version_id=put["VersionId"]) is False
    expired, held = client.lifecycle_expire_noncurrent("demo", "cfg.json")
    assert expired == [put["VersionId"]] and held == []


def test_pin_checkpoint_redrive_is_idempotent(
    registry: ArtifactRegistry, service: CoordinatorService
) -> None:
    """pin=False leaves the documented (restorable, unpinned) claimed-but-not-
    backed pair; pin_checkpoint completes it; a second re-drive changes
    nothing (held members are skipped — the refcount cannot double-count)."""
    client, obj = _s3()
    put = client.put_object(Bucket="demo", Key="cfg.json", Body=b"v1")
    versioner = _versioner(service)
    versioner.add_object_member(obj, "cfg.json")

    cp = versioner.checkpoint("deferred", pin=False)
    (member,) = cp.members
    assert member.restore_tier == "restorable" and member.pin_state == PIN_STATE_UNPINNED
    assert obj.legal_hold_status("cfg.json", version_id=put["VersionId"]) is False

    rows = versioner.pin_checkpoint(cp.record.checkpoint_id)
    assert rows[0].pin_state == PIN_STATE_HELD
    assert obj.legal_hold_status("cfg.json", version_id=put["VersionId"]) is True
    record = registry.get_checkpoint(cp.record.checkpoint_id)
    assert record is not None and record.pin_refcount == 1

    rows = versioner.pin_checkpoint(cp.record.checkpoint_id)  # re-drive: no-op
    assert rows[0].pin_state == PIN_STATE_HELD
    record = registry.get_checkpoint(cp.record.checkpoint_id)
    assert record is not None and record.pin_refcount == 1

    with pytest.raises(CheckpointUnknown):
        versioner.pin_checkpoint("nope")


def test_capture_only_service_fails_fast_when_pins_needed(
    service: CoordinatorService,
) -> None:
    """The fold-in's fail-fast gate: a capture-only seam cannot record a pin
    outcome, so pin=True with pinnable members refuses BEFORE any capture
    read — never a silently unpinned 'restorable' manifest. pin=False is the
    explicit capture-only opt-out and still persists the honest pair."""
    client, obj = _s3()
    client.put_object(Bucket="demo", Key="cfg.json", Body=b"v1")
    capture_only = _CaptureOnlyService(service)
    versioner = _versioner(capture_only)
    versioner.add_object_member(obj, "cfg.json")

    with pytest.raises(TypeError):
        versioner.checkpoint("needs-pins")

    cp = versioner.checkpoint("explicitly-unpinned", pin=False)
    (member,) = cp.members
    assert member.restore_tier == "restorable" and member.pin_state == PIN_STATE_UNPINNED

    # No pinnable work (a file member with NO resolver): the capture-only seam
    # is fine even at pin=True — nothing above restorable-unpinned is claimed.
    files = _ScriptedFileSource()
    files.program("a.txt", (b"a", 1))
    plain = _versioner(capture_only)
    plain.add_file_member(files, "a.txt")
    cp2 = plain.checkpoint("no-pin-work")
    assert cp2.members[0].restore_tier == "restorable-unpinned"
    assert cp2.members[0].pin_state == PIN_STATE_UNPINNED


def test_file_member_pin_verified_held_and_survives_registry_reopen(
    tmp_path: Path,
) -> None:
    """The file verification pin is durable state: held on the sqlite arm,
    read back HELD (refcount included) after a full registry reopen — the
    coordinator-restart survival the plan's Unit 6 scenario names."""
    from ccs.coordinator.sqlite_registry import SqliteArtifactRegistry

    db_path = tmp_path / "state.db"
    files = _FakeFileStore()
    files.put("notes/plan.md", b"plan text", 7)
    resolver = _FakeResolver()
    resolver.keep("notes/plan.md", 7, b"plan text")

    reg = SqliteArtifactRegistry(db_path)
    try:
        service = CoordinatorService(reg)
        versioner = _versioner(service, resolver=resolver)
        versioner.add_file_member(files, "notes/plan.md")
        cp = versioner.checkpoint("durable-pin")
        (member,) = cp.members
        assert member.pin_state == PIN_STATE_HELD
        # v1 honesty: the verification pin never upgrades the tier — the
        # coordinator offers no per-version hold (the R13 disclosure).
        assert member.restore_tier == "restorable-unpinned"
        checkpoint_id = cp.record.checkpoint_id
    finally:
        reg.close()

    reopened = SqliteArtifactRegistry(db_path)
    try:
        (stored,) = reopened.get_checkpoint_members(checkpoint_id)
        assert stored.pin_state == PIN_STATE_HELD
        assert stored.restore_tier == "restorable-unpinned"
        record = reopened.get_checkpoint(checkpoint_id)
        assert record is not None and record.pin_refcount == 1
    finally:
        reopened.close()


def test_file_member_retention_gap_downgrades_loudly(
    registry: ArtifactRegistry, service: CoordinatorService
) -> None:
    """Retention off / the version not retained → KeyError from the resolver
    seam → the captured state is ALREADY unreachable: the loud downgrade to
    forward_only; the restore then honestly skips and says so."""
    files = _FakeFileStore()
    files.put("notes/plan.md", b"plan text", 7)
    resolver = _FakeResolver()  # nothing retained: the retention-off shape

    versioner = _versioner(service, resolver=resolver)
    versioner.add_file_member(files, "notes/plan.md")
    cp = versioner.checkpoint("gap")

    (member,) = cp.members
    assert member.pin_state == PIN_STATE_UNAVAILABLE
    assert member.restore_tier == "forward_only"
    record = registry.get_checkpoint(cp.record.checkpoint_id)
    assert record is not None and record.pin_refcount == 0

    report = versioner.restore(cp.record.checkpoint_id)
    assert (
        report.members_by_path["notes/plan.md"].outcome
        == RESTORE_OUTCOME_FORWARD_ONLY_SKIPPED
    )


def test_file_member_retained_bytes_mismatch_downgrades_loudly(
    service: CoordinatorService,
) -> None:
    """Retained bytes that no longer hash to the captured fingerprint are NOT
    the captured state — restoring them would be a silent wrong-content
    restore, so the pin refuses and downgrades loudly instead."""
    files = _FakeFileStore()
    files.put("notes/plan.md", b"plan text", 7)
    resolver = _FakeResolver()
    resolver.keep("notes/plan.md", 7, b"NOT the captured bytes")

    versioner = _versioner(service, resolver=resolver)
    versioner.add_file_member(files, "notes/plan.md")
    cp = versioner.checkpoint("mismatch")

    (member,) = cp.members
    assert member.pin_state == PIN_STATE_UNAVAILABLE
    assert member.restore_tier == "forward_only"


def test_file_member_without_resolver_left_unpinned(
    service: CoordinatorService,
) -> None:
    """No resolver → no verification is possible and nothing above
    restorable-unpinned was ever claimed: the member is left untouched
    (unpinned), never downgraded and never falsely held."""
    files = _FakeFileStore()
    files.put("notes/plan.md", b"plan text", 7)
    versioner = _versioner(service)  # no resolver
    versioner.add_file_member(files, "notes/plan.md")
    cp = versioner.checkpoint("unverified")
    (member,) = cp.members
    assert member.pin_state == PIN_STATE_UNPINNED
    assert member.restore_tier == "restorable-unpinned"


def test_pin_vanished_version_downgrades_loudly(
    service: CoordinatorService,
) -> None:
    """The captured version is gone by pin time (raced version-targeted
    delete): nothing left to hold — the loud downgrade, and restore reports
    target_lost for it."""
    client, obj = _s3()
    put = client.put_object(Bucket="demo", Key="cfg.json", Body=b"v1")
    versioner = _versioner(service)
    versioner.add_object_member(obj, "cfg.json")
    cp = versioner.checkpoint("race", pin=False)  # capture first, no hold yet
    client.put_object(Bucket="demo", Key="cfg.json", Body=b"v2")
    client.delete_object(Bucket="demo", Key="cfg.json", VersionId=put["VersionId"])

    rows = versioner.pin_checkpoint(cp.record.checkpoint_id)

    assert rows[0].pin_state == PIN_STATE_UNAVAILABLE
    assert rows[0].restore_tier == "restorable-unpinned"
    report = versioner.restore(cp.record.checkpoint_id)
    assert (
        report.members_by_path["s3://cfg.json"].outcome == RESTORE_OUTCOME_TARGET_LOST
    )


def test_pin_checkpoint_missing_binding_preflight_fails_before_any_write(
    registry: ArtifactRegistry, service: CoordinatorService
) -> None:
    client, obj = _s3()
    client.put_object(Bucket="demo", Key="cfg.json", Body=b"v1")
    versioner = _versioner(service)
    versioner.add_object_member(obj, "cfg.json")
    cp = versioner.checkpoint("orphan", pin=False)

    fresh = _versioner(service)  # no declared members: cannot reach the bucket
    with pytest.raises(ValueError):
        fresh.pin_checkpoint(cp.record.checkpoint_id)
    # Nothing was written: the member is still the unpinned capture shape.
    (stored,) = registry.get_checkpoint_members(cp.record.checkpoint_id)
    assert stored.pin_state == PIN_STATE_UNPINNED
    record = registry.get_checkpoint(cp.record.checkpoint_id)
    assert record is not None and record.pin_refcount == 0


def test_service_pin_vocabulary_fails_closed(
    registry: ArtifactRegistry, service: CoordinatorService
) -> None:
    """The service refuses unvetted pin/tier strings BEFORE any write — the
    (restore_tier, pin_state) pair is the honesty surface and must stay
    readable by identity."""
    files = _ScriptedFileSource()
    files.program("a.txt", (b"a", 1))
    versioner = _versioner(service)
    versioner.add_file_member(files, "a.txt")
    cp = versioner.checkpoint("vocab")
    checkpoint_id = cp.record.checkpoint_id

    with pytest.raises(ValueError):
        service.set_workspace_checkpoint_member_pin(
            checkpoint_id, "a.txt", pin_state="super-pinned"
        )
    with pytest.raises(ValueError):
        service.set_workspace_checkpoint_member_pin(
            checkpoint_id, "a.txt", pin_state=PIN_STATE_HELD, restore_tier="better"
        )
    with pytest.raises(ValueError):  # refcount never below zero (fail-closed)
        service.adjust_workspace_checkpoint_pin_refcount(checkpoint_id, -1)
    (stored,) = registry.get_checkpoint_members(checkpoint_id)
    assert stored.pin_state == PIN_STATE_UNPINNED  # nothing landed
