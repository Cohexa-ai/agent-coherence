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
    BinaryFileMemberRefused,
    CheckpointPersistFailed,
    WorkspaceVersioner,
)
from ccs.coordinator.registry import ArtifactRegistry
from ccs.coordinator.service import CoordinatorService
from ccs.core.exceptions import WatchdogAbandoned
from ccs.core.substrate import sha256_hex
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


def _versioner(service: Any, clock: Any | None = None) -> WorkspaceVersioner:
    return WorkspaceVersioner(
        service=service, owner=OWNER, clock=clock if clock is not None else _TickClock()
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
