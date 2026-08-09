# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""The Workspace-Versioning conformance family — WV plan Unit 9 (R11).

Drives the PACKAGED corpus (:mod:`ccs.testing.substrate_conformance`) over its
three default arms (versioned+pinnable / versioned-unpinnable / unversioned),
proves the family's teeth (the torn-cut-blind stub fails EXACTLY its scenario
with the dropped leg named, and passes every unrelated one), proves the
Protocol drift guard bidirectionally, and proves importability: the whole
family runs through ONLY the :class:`WorkspaceConformanceBinding` members (a
proxy that refuses every non-Protocol attribute), so a foreign implementation
satisfying the Protocol can run the corpus unchanged. The ``real_substrate``
arm re-runs the family against a real versioned bucket (skip unless
``CCS_REAL_S3_BUCKET`` is set; ``CCS_REAL_S3_OBJECT_LOCK=1`` upgrades the pin
declaration on a bucket with Object Lock enabled).

The relocation is covered too: the ``tests/conformance/`` runner shim must
re-export the packaged corpus by IDENTITY (one conformance home, no fork).
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

import ccs.testing.substrate_conformance as corpus
from ccs.testing.substrate_conformance import (
    WORKSPACE_BINDING_MEMBERS,
    LocalWorkspaceBinding,
    TornCutBlindWorkspaceBinding,
    WorkspaceConformanceBinding,
    assert_torn_cut_blind_binding_is_rejected,
    assert_workspace_binding_declares_all,
    assert_wv_absent_is_a_fact,
    assert_wv_declared_pin_honest,
    assert_wv_declared_versioning_honest,
    assert_wv_monotonic_restore,
    assert_wv_restart_survival_per_declaration,
    assert_wv_restore_one_winner,
    assert_wv_restore_terminates_under_contention,
    run_workspace_conformance,
    workspace_protocol_members,
)


def _binding(tmp_path: Path, **kwargs: Any) -> LocalWorkspaceBinding:
    return LocalWorkspaceBinding(tmp_path, **kwargs)


# ---------------------------------------------------------------------------
# Default arms — the full family over the deterministic S3-semantics fake.
# ---------------------------------------------------------------------------


def test_wv_conformance_versioned_pinnable_arm(tmp_path: Path) -> None:
    """The full-claim arm: versioned bucket + per-version pin — every
    MUST-MATCH scenario plus the 'restorable'-backed pin declaration."""
    binding = _binding(tmp_path, versioned=True, pinnable=True)
    try:
        run_workspace_conformance(binding)
    finally:
        binding.close()


def test_wv_conformance_versioned_unpinnable_arm(tmp_path: Path) -> None:
    """The loud-downgrade arm: history without a pin — MUST-MATCH still holds;
    the declaration scenarios pin 'restorable-unpinned' + honest target loss."""
    binding = _binding(tmp_path, versioned=True, pinnable=False)
    try:
        run_workspace_conformance(binding)
    finally:
        binding.close()


def test_wv_conformance_unversioned_arm(tmp_path: Path) -> None:
    """The no-history arm: capture describes, tiers 'forward_only', restore
    skips and says so — and torn-cut detection still fires (fingerprints move
    even where no pointer exists)."""
    binding = _binding(tmp_path, versioned=False, pinnable=False)
    try:
        run_workspace_conformance(binding)
    finally:
        binding.close()


def test_pin_without_versioning_is_refused(tmp_path: Path) -> None:
    """Error path: a per-version pin claim without version history is a
    contradiction (the S3 rule) — refused at construction, never a silent
    downgrade the arms would then mislabel."""
    with pytest.raises(ValueError, match="version history"):
        _binding(tmp_path, versioned=False, pinnable=True)


# ---------------------------------------------------------------------------
# Protocol drift guard — bidirectional, properties included.
# ---------------------------------------------------------------------------


def test_workspace_protocol_drift_guard_bidirectional() -> None:
    """The frozenset anchor and the Protocol's actual members must be EQUAL:
    a member added to either side alone fails here (house rule: Protocol
    members incl. properties + frozenset drift guard + teeth)."""
    assert workspace_protocol_members() == WORKSPACE_BINDING_MEMBERS


def test_reference_binding_satisfies_the_protocol(tmp_path: Path) -> None:
    """The reference arm satisfies the runtime-checkable Protocol statically
    (explicit members, no __getattr__ fallthrough) and passes the
    instance-level drift guard."""
    binding = _binding(tmp_path)
    try:
        assert isinstance(binding, WorkspaceConformanceBinding)
        assert_workspace_binding_declares_all(binding)
    finally:
        binding.close()


def test_drift_guard_names_a_missing_member(tmp_path: Path) -> None:
    """Teeth for the drift guard itself: a binding missing one WV member is
    refused with the member NAMED."""

    class _Partial:
        pass

    with pytest.raises(AssertionError, match="restore_workspace"):
        assert_workspace_binding_declares_all(_Partial())  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Teeth — the degraded stub drops EXACTLY ONE leg (torn-cut detection).
# ---------------------------------------------------------------------------


def test_torn_cut_blind_stub_fails_exactly_its_scenario(tmp_path: Path) -> None:
    """MANDATORY teeth: the stub that drops the torn-cut leg FAILS the
    torn-cut scenario, and the failure message names the dropped leg."""
    assert_torn_cut_blind_binding_is_rejected(tmp_path)


def test_torn_cut_blind_stub_passes_every_unrelated_scenario(tmp_path: Path) -> None:
    """The other half of the teeth contract: failures LOCALIZE. The stub broke
    one leg, so every scenario not exercising that leg must still pass — the
    stub doubles as the foreign stand-in for the rest of the family."""
    stub = TornCutBlindWorkspaceBinding(tmp_path)
    try:
        assert_workspace_binding_declares_all(stub)
        assert_wv_declared_versioning_honest(stub)
        assert_wv_absent_is_a_fact(stub)
        assert_wv_restore_one_winner(stub)
        assert_wv_restore_terminates_under_contention(stub)
        assert_wv_monotonic_restore(stub)
        assert_wv_declared_pin_honest(stub)
        assert_wv_restart_survival_per_declaration(stub)
    finally:
        stub.close()


# ---------------------------------------------------------------------------
# Importability — the corpus runs through ONLY the Protocol.
# ---------------------------------------------------------------------------


class _ProtocolOnlyProxy:
    """Exposes ONLY the :data:`WORKSPACE_BINDING_MEMBERS` of the wrapped
    binding; any other attribute access raises. Running the full family
    through this proxy PROVES the scenarios touch nothing but the Protocol —
    the property a foreign implementation's authors rely on."""

    def __init__(self, inner: LocalWorkspaceBinding) -> None:
        object.__setattr__(self, "_inner", inner)

    def __getattr__(self, name: str) -> Any:
        if name in WORKSPACE_BINDING_MEMBERS:
            return getattr(object.__getattribute__(self, "_inner"), name)
        raise AttributeError(
            f"{name!r} is not a WorkspaceConformanceBinding member — the "
            "conformance family may only use the Protocol surface"
        )


def test_family_runs_via_the_protocol_surface_only(tmp_path: Path) -> None:
    binding = _binding(tmp_path)
    try:
        run_workspace_conformance(_ProtocolOnlyProxy(binding))  # type: ignore[arg-type]
    finally:
        binding.close()


# ---------------------------------------------------------------------------
# Relocation — one conformance home; the runner shim re-exports by identity.
# ---------------------------------------------------------------------------


def test_runner_shim_reexports_the_packaged_corpus_by_identity() -> None:
    """``tests/conformance/substrate_conformance.py`` is a THIN shim: every
    re-exported name must be the packaged object itself (no duplicated corpus
    logic in the test tree)."""
    from tests.conformance import substrate_conformance as shim

    for name in shim.__all__:
        assert getattr(shim, name) is getattr(corpus, name), (
            f"shim name {name!r} is not the packaged object — the corpus must "
            "have exactly ONE home"
        )


# ---------------------------------------------------------------------------
# Packaged-importable teeth (R11) — the corpus without pytest installed.
# ---------------------------------------------------------------------------


def test_corpus_all_names_resolve() -> None:
    """Every name the corpus exports via ``__all__`` resolves — the public
    surface a foreign implementation imports is real, not aspirational."""
    for name in corpus.__all__:
        assert hasattr(corpus, name), f"__all__ exports missing name {name!r}"


_PYTEST_FREE_IMPORT_SCRIPT = """\
import sys


class _BlockPytest:
    # Raise on ANY pytest import attempt — a clean venv, deterministically.
    def find_spec(self, name, path=None, target=None):
        if name == "pytest" or name.startswith("pytest."):
            raise ModuleNotFoundError("pytest is blocked for the clean-import test")
        return None


sys.meta_path.insert(0, _BlockPytest())
sys.modules.pop("pytest", None)

import ccs.testing.substrate_conformance as corpus

# Import SUCCEEDED without pytest; the guarded import degraded to the sentinel.
assert corpus.pytest is None

# Raise-expectation scenarios RUN without pytest (the internal shim).
corpus.assert_forward_only_honest()
corpus.assert_split_view_is_rejected()

# The one pytest-only surface (skip reporting) fails AT USE, actionably.
try:
    corpus._skip_scenario("needs a scriptable substrate")
except ModuleNotFoundError as err:
    assert "agent-coherence[conformance]" in str(err), str(err)
    assert "needs a scriptable substrate" in str(err), str(err)
else:
    raise AssertionError("_skip_scenario without pytest must raise the install hint")

assert "pytest" not in sys.modules
print("PYTEST-FREE-OK")
"""


def test_corpus_imports_and_runs_with_pytest_blocked() -> None:
    """R11 packaged-importable teeth: a clean venv (``pip install
    agent-coherence`` — no pytest) must import the corpus and run its
    scenarios; only skip REPORTING needs the ``conformance`` extra, and its
    absence fails at use time naming that extra. A subprocess with a
    meta-path blocker plays the clean venv."""
    src_dir = Path(__file__).resolve().parents[2] / "src"
    env = dict(os.environ)
    env["PYTHONPATH"] = str(src_dir)
    proc = subprocess.run(
        [sys.executable, "-c", _PYTEST_FREE_IMPORT_SCRIPT],
        capture_output=True,
        text=True,
        env=env,
        timeout=120,
    )
    assert proc.returncode == 0, (
        f"pytest-free corpus run failed\nstdout:\n{proc.stdout}\nstderr:\n{proc.stderr}"
    )
    assert "PYTEST-FREE-OK" in proc.stdout


# ---------------------------------------------------------------------------
# real_substrate arm — the SAME family against a real versioned bucket.
# Deselected by default; skip unless credentialed (the shipped kit's pattern).
# ---------------------------------------------------------------------------


@pytest.fixture
def real_s3_client():
    """A boto3 client + bucket for the real arm, or skip if uncredentialed.

    Requires ``CCS_REAL_S3_BUCKET`` to have VERSIONING ENABLED;
    ``CCS_REAL_S3_OBJECT_LOCK=1`` declares the bucket also has Object Lock
    (upgrading the pin declaration the DECLARED scenarios verify).
    """
    bucket = os.environ.get("CCS_REAL_S3_BUCKET")
    if not bucket:
        pytest.skip("set CCS_REAL_S3_BUCKET (a real, non-Moto bucket) to run the real WV arm")
    boto3 = pytest.importorskip("boto3")
    client = boto3.client("s3", region_name=os.environ.get("CCS_REAL_S3_REGION"))
    pinnable = os.environ.get("CCS_REAL_S3_OBJECT_LOCK", "").lower() in ("1", "true", "yes")
    return client, bucket, pinnable


@pytest.mark.real_substrate
def test_real_s3_workspace_conformance(real_s3_client, tmp_path: Path) -> None:
    client, bucket, pinnable = real_s3_client
    binding = LocalWorkspaceBinding(
        tmp_path, versioned=True, pinnable=pinnable, client=client, bucket=bucket
    )
    try:
        run_workspace_conformance(binding)
    finally:
        binding.close()
