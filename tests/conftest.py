# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""Pytest configuration: defensive collection-skip for optional-extra tests.

`tests/test_diagnose_*.py` import the public surface of ``ccs.diagnose``,
which transitively imports ``langchain_core`` and ``langgraph`` (declared
in the ``[diagnose]`` optional extra). Users running ``pytest`` after a
bare ``pip install -e ".[dev]"`` would otherwise hit cryptic ImportError
during collection.

CI installs ``.[dev,diagnose]`` so all diagnose tests run. This guard is
the second layer of defense for local installs and for any future CI
matrix that exercises bare-install paths.
"""

from __future__ import annotations

import importlib.util

collect_ignore_glob: list[str] = []

if importlib.util.find_spec("langchain_core") is None:
    collect_ignore_glob.append("test_diagnose_*.py")

# Live-API tests (Q6 probe smoke, OpenAI-adapter smoke) import `openai` /
# `mistralai` at module level. On a bare `[dev]` install those SDKs are absent;
# ignore the live-test glob so collection does not ImportError. The live tests
# also carry an in-module `pytest.importorskip` as a second layer, and the
# `live_api` marker keeps them out of the default `pytest -q` run regardless.
if importlib.util.find_spec("openai") is None or importlib.util.find_spec("mistralai") is None:
    collect_ignore_glob.append("test_*_live.py")
