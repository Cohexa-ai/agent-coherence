# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""``agent-coherence-workspace`` CLI tests (WV plan Unit 8 / R6, R8).

Covers, per the plan's Unit-8 scenarios:

- per-verb happy paths (checkpoint / list / status / restore) in BOTH the human
  and the ``--json`` output modes;
- every enumerated honesty placement asserted present in the output:
  (1) the typed binary-member refusal naming the UTF-8 limitation,
  (2) the ``--help`` constraint note (single-host scope; no-arbiter file members),
  (3) ``status`` labeling EVERY member with the ``(restore_tier, pin_state)``
      pair — ``(restorable, unpinned)`` rendered explicitly as
      claimed-but-not-yet-backed — plus ``dirty_during_window`` and
      ``restore_outcome``, with ``pin_refcount`` + ``restore_status`` in the
      header,
  (4) the ``checkpoint`` retention caveat (file pins are verification, not a
      guarantee);
- error paths: binary member refusal (typed, non-zero), unknown checkpoint id
  (typed, non-zero), path traversal rejection, empty member set;
- the e2e example: exits 0 iff baseline-shows-loss AND guarded-prevents,
  offline, deterministic across two consecutive runs, with the origin
  Success-Criterion-1 trio (delete leg + forward-only skip + S3 foreign-writer
  conflict) visible in the run output.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from uuid import uuid4

import pytest

from ccs.cli.workspace import (
    CLAIMED_NOT_BACKED_LABEL,
    FILE_RETENTION_CAVEAT,
    MEMBER_PATH_REFUSED_REASON,
)
from ccs.cli.workspace import (
    main as workspace_main,
)
from ccs.coordinator.registry_protocol import CheckpointMember
from ccs.coordinator.service import CoordinatorService
from ccs.coordinator.sqlite_registry import SqliteArtifactRegistry
from ccs.core.substrate import ArbitrationTier, RestoreTier

REPO_ROOT = Path(__file__).resolve().parents[2]
EXAMPLE = REPO_ROOT / "examples" / "workspace_versioning" / "main.py"


def _run(capsys, *args: str) -> tuple[int, str, str]:
    rc = workspace_main(list(args))
    captured = capsys.readouterr()
    return rc, captured.out, captured.err


def _seed_file(root: Path, rel: str = "docs/plan.md", body: bytes = b"plan v1\n") -> Path:
    path = root / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(body)
    return path


def _checkpoint_id(capsys, root: Path) -> str:
    rc, out, _ = _run(capsys, "list", "--json", "--root", str(root))
    assert rc == 0
    return json.loads(out)["checkpoints"][0]["checkpoint_id"]


def _fabricate_checkpoint(root: Path, member: CheckpointMember) -> str:
    """Persist a manifest row the CLI cannot mint itself (an S3-shaped member)
    directly through the CLI's own registry, so ``status``/``restore`` render
    real durable state rather than a mock."""
    db_path = root / ".coherence" / "workspace.db"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    registry = SqliteArtifactRegistry(db_path, retain_versions=True)
    try:
        service = CoordinatorService(registry)
        record = service.create_workspace_checkpoint(
            name="fabricated",
            owner=uuid4(),
            members=[member],
            window_min=1.0,
            window_max=2.0,
        )
        return record.checkpoint_id
    finally:
        registry.close()


# --- checkpoint: happy paths (human + JSON) + honesty placement #4 --------------


def test_checkpoint_happy_human_carries_pair_and_retention_caveat(
    tmp_path: Path, capsys
) -> None:
    _seed_file(tmp_path)
    rc, out, err = _run(
        capsys, "checkpoint", "cp1", "--file", "docs/plan.md", "--root", str(tmp_path)
    )
    assert rc == 0, err
    assert "checkpoint 'cp1' persisted:" in out
    # The member line carries the honesty PAIR; the resolver-backed
    # verification pin lands "held" while the tier honestly stays unpinned-class.
    assert "docs/plan.md  (restorable-unpinned, held)" in out
    # Honesty placement #4: the retention caveat rides the checkpoint output.
    assert "retention caveat:" in out
    assert "VERIFICATION of the retained bytes, not a retention guarantee" in out
    assert "restorable-unpinned" in FILE_RETENTION_CAVEAT


def test_checkpoint_happy_json_shape(tmp_path: Path, capsys) -> None:
    _seed_file(tmp_path)
    rc, out, err = _run(
        capsys,
        "checkpoint",
        "cp1",
        "--file",
        "docs/plan.md",
        "--forward-only",
        "actions/deploy",
        "--json",
        "--root",
        str(tmp_path),
    )
    assert rc == 0, err
    payload = json.loads(out)
    assert payload["checkpoint"]["name"] == "cp1"
    assert payload["checkpoint"]["restore_status"] == "none"
    assert payload["retention_caveat"] == FILE_RETENTION_CAVEAT  # placement #4, JSON
    members = {m["member_path"]: m for m in payload["members"]}
    assert members["docs/plan.md"]["pair"] == "(restorable-unpinned, held)"
    assert members["docs/plan.md"]["claimed_not_backed"] is False
    assert members["actions/deploy"]["restore_tier"] == "forward_only"


def test_checkpoint_no_pin_persists_the_unpinned_pair(tmp_path: Path, capsys) -> None:
    _seed_file(tmp_path)
    rc, out, _ = _run(
        capsys,
        "checkpoint",
        "cp1",
        "--file",
        "docs/plan.md",
        "--no-pin",
        "--root",
        str(tmp_path),
    )
    assert rc == 0
    assert "docs/plan.md  (restorable-unpinned, unpinned)" in out


# --- list: happy paths ----------------------------------------------------------


def test_list_happy_human_and_empty(tmp_path: Path, capsys) -> None:
    rc, out, _ = _run(capsys, "list", "--root", str(tmp_path))
    assert rc == 0
    assert "no checkpoints persisted" in out

    _seed_file(tmp_path)
    _run(capsys, "checkpoint", "cp1", "--file", "docs/plan.md", "--root", str(tmp_path))
    rc, out, _ = _run(capsys, "list", "--root", str(tmp_path))
    assert rc == 0
    assert "'cp1'" in out
    assert "restore_status=none" in out
    assert "pin_refcount=1" in out


def test_list_happy_json(tmp_path: Path, capsys) -> None:
    _seed_file(tmp_path)
    _run(capsys, "checkpoint", "cp1", "--file", "docs/plan.md", "--root", str(tmp_path))
    rc, out, _ = _run(capsys, "list", "--json", "--root", str(tmp_path))
    assert rc == 0
    payload = json.loads(out)
    assert len(payload["checkpoints"]) == 1
    record = payload["checkpoints"][0]
    assert record["name"] == "cp1"
    assert record["pin_refcount"] == 1
    assert record["restore_status"] == "none"


# --- status: happy paths + honesty placement #3 ---------------------------------


def test_status_happy_human_header_and_pairs(tmp_path: Path, capsys) -> None:
    _seed_file(tmp_path)
    _run(
        capsys,
        "checkpoint",
        "cp1",
        "--file",
        "docs/plan.md",
        "--forward-only",
        "actions/deploy",
        "--root",
        str(tmp_path),
    )
    ckpt = _checkpoint_id(capsys, tmp_path)
    rc, out, _ = _run(capsys, "status", ckpt, "--root", str(tmp_path))
    assert rc == 0
    # Placement #3 header half: restore_status + pin_refcount.
    assert "restore_status=none  pin_refcount=1" in out
    # Placement #3 member half: EVERY member labeled with the pair.
    assert "docs/plan.md  (restorable-unpinned, held)" in out
    assert "actions/deploy  (forward_only, unpinned)" in out


def test_status_happy_json(tmp_path: Path, capsys) -> None:
    _seed_file(tmp_path)
    _run(capsys, "checkpoint", "cp1", "--file", "docs/plan.md", "--root", str(tmp_path))
    ckpt = _checkpoint_id(capsys, tmp_path)
    rc, out, _ = _run(capsys, "status", ckpt, "--json", "--root", str(tmp_path))
    assert rc == 0
    payload = json.loads(out)
    assert payload["checkpoint"]["pin_refcount"] == 1
    member = payload["members"][0]
    assert member["pair"] == "(restorable-unpinned, held)"
    assert member["claimed_not_backed"] is False
    assert member["dirty_during_window"] is False


def test_status_renders_claimed_but_not_yet_backed_pair(tmp_path: Path, capsys) -> None:
    """Placement #3's load-bearing case: (restorable, unpinned) is a CLAIM
    nothing backs yet and must be rendered as exactly that."""
    ckpt = _fabricate_checkpoint(
        tmp_path,
        CheckpointMember(
            member_path="s3://reports/summary.txt",
            artifact_id=None,
            native_token="v000001",
            fingerprint="a" * 64,
            captured_at=1.0,
            arbitration_tier=ArbitrationTier.NATIVE_CAS.value,
            restore_tier=RestoreTier.RESTORABLE.value,
        ),
    )
    rc, out, _ = _run(capsys, "status", ckpt, "--root", str(tmp_path))
    assert rc == 0
    assert "(restorable, unpinned)" in out
    assert CLAIMED_NOT_BACKED_LABEL in out

    rc, out, _ = _run(capsys, "status", ckpt, "--json", "--root", str(tmp_path))
    assert rc == 0
    member = json.loads(out)["members"][0]
    assert member["pair"] == "(restorable, unpinned)"
    assert member["claimed_not_backed"] is True


def test_status_renders_dirty_during_window(tmp_path: Path, capsys) -> None:
    ckpt = _fabricate_checkpoint(
        tmp_path,
        CheckpointMember(
            member_path="docs/plan.md",
            artifact_id=None,
            native_token="3",
            fingerprint="b" * 64,
            captured_at=1.0,
            dirty_during_window=True,
            arbitration_tier=ArbitrationTier.NO_ARBITER.value,
            restore_tier=RestoreTier.RESTORABLE_UNPINNED.value,
        ),
    )
    rc, out, _ = _run(capsys, "status", ckpt, "--root", str(tmp_path))
    assert rc == 0
    assert "dirty-during-window" in out
    rc, out, _ = _run(capsys, "status", ckpt, "--json", "--root", str(tmp_path))
    assert json.loads(out)["members"][0]["dirty_during_window"] is True


# --- restore: happy paths (human + JSON) ----------------------------------------


def test_restore_happy_human_brings_the_file_back(tmp_path: Path, capsys) -> None:
    plan = _seed_file(tmp_path)
    _run(
        capsys,
        "checkpoint",
        "cp1",
        "--file",
        "docs/plan.md",
        "--forward-only",
        "actions/deploy",
        "--root",
        str(tmp_path),
    )
    ckpt = _checkpoint_id(capsys, tmp_path)
    plan.write_bytes(b"corrupted by a failed attempt\n")

    rc, out, _ = _run(capsys, "restore", ckpt, "--root", str(tmp_path))
    assert rc == 0
    assert "CONCLUDED" in out
    assert "docs/plan.md  outcome=restored" in out
    assert "actions/deploy  outcome=forward_only_skipped" in out
    # The file leg's honesty label: detection, never substrate arbitration.
    assert "no-arbiter" in out
    assert plan.read_bytes() == b"plan v1\n"

    # status now surfaces the durable outcomes (placement #3, outcome half).
    rc, out, _ = _run(capsys, "status", ckpt, "--root", str(tmp_path))
    assert rc == 0
    assert "restore_status=concluded" in out
    assert "outcome=restored" in out


def test_restore_happy_json(tmp_path: Path, capsys) -> None:
    plan = _seed_file(tmp_path)
    _run(capsys, "checkpoint", "cp1", "--file", "docs/plan.md", "--root", str(tmp_path))
    ckpt = _checkpoint_id(capsys, tmp_path)
    plan.write_bytes(b"corrupted\n")

    rc, out, _ = _run(capsys, "restore", ckpt, "--json", "--root", str(tmp_path))
    assert rc == 0
    payload = json.loads(out)
    assert payload["status"] == "concluded"
    member = payload["members"][0]
    assert member["member_path"] == "docs/plan.md"
    assert member["outcome"] == "restored"
    assert member["new_native_token"] is not None
    assert "registration" in payload
    assert plan.read_bytes() == b"plan v1\n"


def test_restore_absent_divergence_absorbed_as_no_arbiter_conflict(
    tmp_path: Path, capsys
) -> None:
    """A member captured ABSENT that exists live: the v1 file leg has no delete
    surface — the divergence is ABSORBED as a labeled conflict (exit 3), never
    a silent skip and never presented as arbitration."""
    _seed_file(tmp_path)  # a second, present member keeps the manifest non-empty
    rc, _, _ = _run(
        capsys,
        "checkpoint",
        "cp1",
        "--file",
        "docs/plan.md",
        "--file",
        "docs/ghost.md",  # does not exist -> the ABSENT fact
        "--root",
        str(tmp_path),
    )
    assert rc == 0
    ckpt = _checkpoint_id(capsys, tmp_path)
    (tmp_path / "docs" / "ghost.md").write_bytes(b"appeared after capture\n")

    rc, out, _ = _run(capsys, "restore", ckpt, "--root", str(tmp_path))
    assert rc == 3  # concluded, with an absorbed outcome
    assert "docs/ghost.md  outcome=conflict" in out
    assert "no-arbiter" in out
    assert "absorbed outcomes above are the honest per-member truth" in out


# --- error paths ----------------------------------------------------------------


def test_binary_member_typed_refusal_names_the_utf8_limitation(
    tmp_path: Path, capsys
) -> None:
    _seed_file(tmp_path, rel="blob.bin", body=b"\xff\xfe\x00\x01binary")
    rc, out, err = _run(
        capsys, "checkpoint", "cp1", "--file", "blob.bin", "--root", str(tmp_path)
    )
    assert rc == 2
    # Honesty placement #1: the typed reason + the UTF-8 limitation, verbatim.
    assert "binary_file_member_unsupported" in err
    assert "non-UTF-8" in err
    assert "v1 limitation" in err
    # Nothing persisted (the refusal fires BEFORE any manifest write).
    rc, out, _ = _run(capsys, "list", "--root", str(tmp_path))
    assert "no checkpoints persisted" in out


def test_unknown_checkpoint_id_is_a_clean_typed_error(tmp_path: Path, capsys) -> None:
    for verb in ("status", "restore"):
        rc, _, err = _run(capsys, verb, "no-such-checkpoint", "--root", str(tmp_path))
        assert rc == 2
        assert "checkpoint 'no-such-checkpoint' is unknown" in err


def test_checkpoint_rejects_traversal_and_outside_paths(tmp_path: Path, capsys) -> None:
    rc, _, err = _run(
        capsys, "checkpoint", "cp1", "--file", "../outside.md", "--root", str(tmp_path)
    )
    assert rc == 1
    assert "rejected" in err


def test_checkpoint_requires_at_least_one_member(tmp_path: Path, capsys) -> None:
    rc, _, err = _run(capsys, "checkpoint", "cp1", "--root", str(tmp_path))
    assert rc == 1
    assert "at least one member" in err


def test_restore_refuses_s3_members_with_a_clean_pointer_to_the_api(
    tmp_path: Path, capsys
) -> None:
    ckpt = _fabricate_checkpoint(
        tmp_path,
        CheckpointMember(
            member_path="s3://reports/summary.txt",
            artifact_id=None,
            native_token="v000001",
            fingerprint="a" * 64,
            captured_at=1.0,
            arbitration_tier=ArbitrationTier.NATIVE_CAS.value,
            restore_tier=RestoreTier.RESTORABLE.value,
        ),
    )
    rc, _, err = _run(capsys, "restore", ckpt, "--root", str(tmp_path))
    assert rc == 1
    assert "S3 object members" in err
    assert "nothing was started" in err
    # status still renders the member honestly.
    rc, out, _ = _run(capsys, "status", ckpt, "--root", str(tmp_path))
    assert rc == 0
    assert "s3://reports/summary.txt" in out


# --- member-path containment (symlink escape / .coherence / directory) ----------


def _escape_fixture(tmp_path: Path) -> tuple[Path, Path]:
    """A workspace root plus a sibling OUTSIDE directory holding a secret."""
    root = tmp_path / "ws"
    root.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "secret.txt").write_text("outside secret\n")
    return root, outside


def test_checkpoint_refuses_symlinked_intermediate_dir_escape(
    tmp_path: Path, capsys
) -> None:
    """`--file evil_link/secret.txt` with evil_link -> an outside dir must be a
    typed exit-2 refusal — never a capture of the outside file."""
    root, outside = _escape_fixture(tmp_path)
    (root / "evil_link").symlink_to(outside)
    rc, _, err = _run(
        capsys,
        "checkpoint",
        "cp1",
        "--file",
        "evil_link/secret.txt",
        "--root",
        str(root),
    )
    assert rc == 2
    assert "refused" in err
    assert "symlink" in err
    # Nothing persisted: the refusal fires before any manifest write.
    rc, out, _ = _run(capsys, "list", "--root", str(root))
    assert "no checkpoints persisted" in out


def test_restore_refuses_persisted_member_crossing_symlink(
    tmp_path: Path, capsys
) -> None:
    """Restore replays PERSISTED member_paths that never re-pass the CLI arg
    pre-check: a fabricated manifest row crossing a symlink (the re-pointed-
    between-checkpoint-and-restore shape) must refuse at the filesystem-access
    seam and never write outside the root."""
    root, outside = _escape_fixture(tmp_path)
    victim = outside / "secret.txt"
    ckpt = _fabricate_checkpoint(
        root,
        CheckpointMember(
            member_path="evil_link/secret.txt",
            artifact_id=None,
            native_token="1",
            fingerprint="c" * 64,
            captured_at=1.0,
            arbitration_tier=ArbitrationTier.NO_ARBITER.value,
            restore_tier=RestoreTier.RESTORABLE_UNPINNED.value,
        ),
    )
    (root / "evil_link").symlink_to(outside)
    rc, _, err = _run(capsys, "restore", ckpt, "--root", str(root))
    assert rc == 2
    assert "symlink" in err
    assert victim.read_text() == "outside secret\n"  # never written through


def test_checkpoint_refuses_leaf_symlink(tmp_path: Path, capsys) -> None:
    root, outside = _escape_fixture(tmp_path)
    (root / "leaf.md").symlink_to(outside / "secret.txt")
    rc, _, err = _run(
        capsys, "checkpoint", "cp1", "--file", "leaf.md", "--root", str(root)
    )
    assert rc == 2
    assert "symlink" in err

    # An IN-root leaf symlink is refused too: ANY symlink component (leaf
    # included) can be re-pointed after the check (the TOCTOU window).
    (root / "real.md").write_text("real\n")
    (root / "alias.md").symlink_to(root / "real.md")
    rc, _, err = _run(
        capsys, "checkpoint", "cp1", "--file", "alias.md", "--root", str(root)
    )
    assert rc == 2
    assert "symlink" in err


def test_checkpoint_refuses_coherence_state_member(tmp_path: Path, capsys) -> None:
    """`.coherence/**` is coordinator state (the hook HMAC secret lives there)
    — capturing it into workspace.db is an info-disclosure, refused typed."""
    secret = tmp_path / ".coherence" / "hook.secret"
    secret.parent.mkdir(mode=0o700)
    secret.write_text("hmac-secret\n")
    rc, _, err = _run(
        capsys,
        "checkpoint",
        "cp1",
        "--file",
        ".coherence/hook.secret",
        "--root",
        str(tmp_path),
    )
    assert rc == 2
    assert ".coherence" in err
    rc, out, _ = _run(capsys, "list", "--root", str(tmp_path))
    assert "no checkpoints persisted" in out


def test_restore_refuses_persisted_coherence_member(tmp_path: Path, capsys) -> None:
    ckpt = _fabricate_checkpoint(
        tmp_path,
        CheckpointMember(
            member_path=".coherence/hook.secret",
            artifact_id=None,
            native_token="1",
            fingerprint="d" * 64,
            captured_at=1.0,
            arbitration_tier=ArbitrationTier.NO_ARBITER.value,
            restore_tier=RestoreTier.RESTORABLE_UNPINNED.value,
        ),
    )
    rc, _, err = _run(capsys, "restore", ckpt, "--root", str(tmp_path))
    assert rc == 2
    assert ".coherence" in err


def test_directory_member_is_a_typed_refusal_not_a_traceback(
    tmp_path: Path, capsys
) -> None:
    (tmp_path / "somedir").mkdir()
    rc, _, err = _run(
        capsys, "checkpoint", "cp1", "--file", "somedir", "--root", str(tmp_path)
    )
    assert rc == 2
    assert "refused" in err
    assert "Traceback" not in err


# --- fresh-workspace .coherence hardening (0700 + gitignore) --------------------


def test_fresh_workspace_coherence_dir_is_0700_and_gitignored(
    tmp_path: Path, capsys
) -> None:
    """A fresh workspace's .coherence/ must come from the lifecycle's single
    implementation: 0700 perms + the '*' gitignore, so `git add .` can never
    stage workspace.db."""
    subprocess.run(["git", "init", "-q", str(tmp_path)], check=True)
    _seed_file(tmp_path)
    rc, _, err = _run(
        capsys, "checkpoint", "cp1", "--file", "docs/plan.md", "--root", str(tmp_path)
    )
    assert rc == 0, err
    coherence = tmp_path / ".coherence"
    assert (coherence.stat().st_mode & 0o777) == 0o700
    assert (coherence / ".gitignore").read_text() == "*\n"
    status = subprocess.run(
        ["git", "-C", str(tmp_path), "status", "--ignored", "--porcelain"],
        capture_output=True,
        text=True,
        check=True,
    )
    assert "!! .coherence/" in status.stdout
    assert "workspace.db" not in status.stdout


# --- --json error envelope ------------------------------------------------------


def test_json_error_envelope_carries_typed_reason(tmp_path: Path, capsys) -> None:
    """Every error path under --json emits a parseable envelope on stdout
    carrying the typed reason; the stderr prose stays for humans."""
    # Typed refusal: binary member (exit 2).
    _seed_file(tmp_path, rel="blob.bin", body=b"\xff\xfe\x00\x01binary")
    rc, out, err = _run(
        capsys,
        "checkpoint",
        "cp1",
        "--file",
        "blob.bin",
        "--json",
        "--root",
        str(tmp_path),
    )
    assert rc == 2
    envelope = json.loads(out)
    assert envelope["kind"] == "error"
    assert envelope["exit_code"] == 2
    assert envelope["reason"] == "binary_file_member_unsupported"
    assert "non-UTF-8" in err  # stderr prose retained

    # Typed refusal: unknown checkpoint (exit 2).
    rc, out, _ = _run(capsys, "status", "nope", "--json", "--root", str(tmp_path))
    assert rc == 2
    envelope = json.loads(out)
    assert envelope["reason"] == "checkpoint_unknown"

    # Typed refusal: directory member (exit 2, member-path slug).
    (tmp_path / "somedir").mkdir()
    rc, out, _ = _run(
        capsys,
        "checkpoint",
        "cp1",
        "--file",
        "somedir",
        "--json",
        "--root",
        str(tmp_path),
    )
    assert rc == 2
    envelope = json.loads(out)
    assert envelope["reason"] == MEMBER_PATH_REFUSED_REASON

    # Validation error: traversal path (exit 1).
    rc, out, _ = _run(
        capsys,
        "checkpoint",
        "cp1",
        "--file",
        "../outside.md",
        "--json",
        "--root",
        str(tmp_path),
    )
    assert rc == 1
    envelope = json.loads(out)
    assert envelope["kind"] == "error"
    assert envelope["exit_code"] == 1
    assert envelope["reason"] == "invalid_member_path"


# --- restore registration honesty fields ----------------------------------------


def test_restore_surfaces_invalidated_peers_and_attempts(
    tmp_path: Path, capsys
) -> None:
    plan = _seed_file(tmp_path)
    _run(capsys, "checkpoint", "cp1", "--file", "docs/plan.md", "--root", str(tmp_path))
    ckpt = _checkpoint_id(capsys, tmp_path)
    plan.write_bytes(b"corrupted\n")

    rc, out, _ = _run(capsys, "restore", ckpt, "--json", "--root", str(tmp_path))
    assert rc == 0
    registration = json.loads(out)["registration"]
    assert isinstance(registration["invalidated_peers"], int)
    assert isinstance(registration["attempts"], int)

    # A fresh first restore in HUMAN mode (this run performs registration, so
    # the report always carries it) surfaces the same honesty fields.
    _run(capsys, "checkpoint", "cp2", "--file", "docs/plan.md", "--root", str(tmp_path))
    rc, out, _ = _run(capsys, "list", "--json", "--root", str(tmp_path))
    records = json.loads(out)["checkpoints"]
    ckpt2 = next(r["checkpoint_id"] for r in records if r["name"] == "cp2")
    plan.write_bytes(b"corrupted again\n")
    rc, out, _ = _run(capsys, "restore", ckpt2, "--root", str(tmp_path))
    assert rc == 0
    assert "peers invalidated by registration:" in out
    assert "attempts=" in out


# --- duplicate-name disclosure --------------------------------------------------


def test_checkpoint_duplicate_name_is_disclosed_not_refused(
    tmp_path: Path, capsys
) -> None:
    _seed_file(tmp_path)
    rc, out, _ = _run(
        capsys, "checkpoint", "cp1", "--file", "docs/plan.md", "--root", str(tmp_path)
    )
    assert rc == 0
    assert "prior checkpoint" not in out  # first mint: nothing to disclose
    first_id = _checkpoint_id(capsys, tmp_path)

    rc, out, _ = _run(
        capsys, "checkpoint", "cp1", "--file", "docs/plan.md", "--root", str(tmp_path)
    )
    assert rc == 0  # disclosed, never refused — names are labels
    assert first_id in out
    assert "not unique keys" in out

    rc, out, _ = _run(
        capsys,
        "checkpoint",
        "cp1",
        "--file",
        "docs/plan.md",
        "--json",
        "--root",
        str(tmp_path),
    )
    assert rc == 0
    existing = json.loads(out)["existing_with_same_name"]
    assert first_id in existing
    assert len(existing) == 2


# --- live-coordinator dual-write warning ----------------------------------------


def test_restore_warns_when_live_coordinator_is_serving(tmp_path: Path, capsys) -> None:
    plan = _seed_file(tmp_path)
    _run(capsys, "checkpoint", "cp1", "--file", "docs/plan.md", "--root", str(tmp_path))
    ckpt = _checkpoint_id(capsys, tmp_path)
    plan.write_bytes(b"corrupted\n")
    # Fake a LIVE pidfile: this test process's own pid is provably alive.
    (tmp_path / ".coherence" / "server.pid").write_text(f"{os.getpid()}\n12345\n")

    rc, out, err = _run(capsys, "restore", ckpt, "--json", "--root", str(tmp_path))
    assert rc == 0  # WARN, never refuse (v1)
    assert "live hook coordinator" in err
    assert "bypass" in err
    payload = json.loads(out)
    assert payload["live_coordinator_warning"] is True
    assert plan.read_bytes() == b"plan v1\n"  # the restore still ran


def test_restore_does_not_warn_without_a_live_coordinator(
    tmp_path: Path, capsys
) -> None:
    plan = _seed_file(tmp_path)
    _run(capsys, "checkpoint", "cp1", "--file", "docs/plan.md", "--root", str(tmp_path))
    ckpt = _checkpoint_id(capsys, tmp_path)
    plan.write_bytes(b"corrupted\n")
    # A malformed pidfile (no parseable pid) is NOT a live coordinator.
    (tmp_path / ".coherence" / "server.pid").write_text("garbage\n")

    rc, out, err = _run(capsys, "restore", ckpt, "--json", "--root", str(tmp_path))
    assert rc == 0
    assert "live hook coordinator" not in err
    assert "live_coordinator_warning" not in json.loads(out)


# --- honesty placement #2: the --help constraint note ---------------------------


def test_help_carries_the_constraint_note(capsys) -> None:
    with pytest.raises(SystemExit) as excinfo:
        workspace_main(["--help"])
    assert excinfo.value.code == 0
    out = capsys.readouterr().out
    assert "single-host" in out
    assert "no-arbiter" in out
    assert "detection-only" in out
    assert "cross-host" in out


# --- the e2e example: exit-code contract + determinism --------------------------


def _run_example(*flags: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, str(EXAMPLE), *flags],
        capture_output=True,
        text=True,
        timeout=180,
        cwd=str(REPO_ROOT),
    )


def test_example_baseline_arm_contract_and_determinism() -> None:
    """Exit 0 iff baseline-shows-loss AND guarded-prevents; two consecutive
    runs are byte-identical (offline + deterministic). The origin
    Success-Criterion-1 trio must be VISIBLE in the run output."""
    first = _run_example("--baseline")
    second = _run_example("--baseline")
    assert first.returncode == 0, first.stdout + first.stderr
    assert second.returncode == 0
    assert first.stdout == second.stdout  # deterministic across consecutive runs

    out = first.stdout
    assert "LOSS demonstrated" in out  # baseline-shows-loss
    # Success-Criterion 1: delete leg + forward-only skip + S3 conflict visible.
    assert "delete marker minted" in out
    assert "outcome=forward_only_skipped" in out
    assert "outcome=conflict" in out
    assert "outcome=restored" in out
    # Honesty labels ride the report.
    assert "no-arbiter" in out


def test_example_default_arm_guarded_only() -> None:
    result = _run_example()
    assert result.returncode == 0, result.stdout + result.stderr
    assert "Negative control" not in result.stdout
    assert "outcome=restored" in result.stdout
