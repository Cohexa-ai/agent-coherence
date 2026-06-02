# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""Smoke test for the multi-agent demo script."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path


def test_multi_agent_planning_demo_runs() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    script = repo_root / "examples" / "multi_agent_planning.py"
    env = dict(os.environ)
    env["PYTHONPATH"] = str(repo_root / "src")

    result = subprocess.run(
        [sys.executable, str(script)],
        cwd=str(repo_root),
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert "planner wrote plan.md v2" in result.stdout
    assert "researcher invalidated" in result.stdout
    assert "executor invalidated" in result.stdout
    assert "researcher fetched plan.md v2" in result.stdout
    assert "executor fetched plan.md v2" in result.stdout
