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
import subprocess
import sys
from pathlib import Path
from uuid import uuid4

import pytest

from ccs.cli.workspace import (
    CLAIMED_NOT_BACKED_LABEL,
    FILE_RETENTION_CAVEAT,
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
