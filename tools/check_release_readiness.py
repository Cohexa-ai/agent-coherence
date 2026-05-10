# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""Run CCS release-readiness checks (preflight for ``v*`` tag push).

Mirrors the structure of ``tools/check_architecture.py``. The reusable
logic lives in :mod:`ccs.hardening.release_readiness`; this script
exists so the same checks can be invoked from a checkout without
installing the package.
"""

from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from ccs.hardening.release_readiness import main  # noqa: E402

if __name__ == "__main__":
    raise SystemExit(main())
