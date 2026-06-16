# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Run CCS architecture boundary and cycle checks."""

from __future__ import annotations

from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from ccs.hardening.architecture import format_report, run_architecture_checks


def main() -> int:
    report = run_architecture_checks(SRC_ROOT)
    print(format_report(report))
    return 0 if report.ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
