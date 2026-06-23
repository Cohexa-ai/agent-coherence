# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Smoke tests for the single-file, vendor-neutral PEP 723 demos.

These demos (``examples/shared_knowledge_base/`` and ``examples/divergent_memory/``)
are meant to be *run*, not imported — they carry inline deps and self-test, exiting
non-zero unless their invariant holds both ways. They have no ``__init__.py``, so we
exercise them as a recipient would: run the script and assert on exit code +
output markers. The fixed case spawns a local coordinator subprocess (no network).
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[1]

# (relative script path, required stdout markers proving both directions reproduced)
_DEMOS = [
    pytest.param(
        "examples/shared_knowledge_base/demo.py",
        ("LOST UPDATE: True", "LOST UPDATE: False", "Invariant held"),
        id="shared_knowledge_base",
    ),
    pytest.param(
        "examples/divergent_memory/demo.py",
        (
            "DIVERGED: True",
            "DIVERGED: False",
            "B's stale write denied: True",
            "Invariant held",
        ),
        id="divergent_memory",
    ),
]


@pytest.mark.parametrize(("script_rel", "markers"), _DEMOS)
def test_single_file_demo_runs_and_self_asserts(script_rel: str, markers: tuple[str, ...]) -> None:
    script = _REPO_ROOT / script_rel
    env = dict(os.environ)
    env["PYTHONPATH"] = str(_REPO_ROOT / "src")  # resolve `ccs` from source in CI

    result = subprocess.run(
        [sys.executable, str(script)],
        cwd=str(_REPO_ROOT),
        capture_output=True,
        text=True,
        env=env,
        check=False,
        timeout=120,
    )

    # The demo's own exit code is the contract: non-zero means it failed to
    # reproduce broken→fixed. Surface stderr on failure for debuggability.
    assert result.returncode == 0, result.stderr
    for marker in markers:
        assert marker in result.stdout, f"missing marker {marker!r} in:\n{result.stdout}"
