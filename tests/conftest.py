# Copyright (c) 2026 agent-coherence contributors.
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

import pytest

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


def _mcp_server_available() -> bool:
    # find_spec on a submodule raises ModuleNotFoundError when an ancestor is
    # absent (mcp missing, OR a partial transitive mcp without mcp.server).
    try:
        return importlib.util.find_spec("mcp.server.fastmcp") is not None
    except ModuleNotFoundError:
        return False


# The front-door demo test imports `examples.mcp_stale_write_guard`, which imports
# `ccs.mcp.server` -> the `mcp` SDK (the `[mcp]` extra). Skip it on a bare install
# (the tests/mcp/ package carries its own conftest guard for the rest). CI's Tests
# job installs `.[...,mcp]` so it runs there.
if not _mcp_server_available():
    collect_ignore_glob.append("test_mcp_front_door_demo.py")


def pytest_configure(config):
    """Neutralize the v0.9.0 transitional RuntimeWarning at COLLECTION time.

    Module-level ``CrashRecoveryConfig`` constructions in test files (e.g. the
    ``CR_CFG`` constant in test_adapter_crash_recovery.py) run at import /
    collection — before the autouse fixture below ever fires. Pre-setting the
    flag here keeps a ``-W error::RuntimeWarning`` run from failing collection
    on that first construction. Dedicated warning-assertion tests still reset
    the flag to ``False`` to observe the warning.
    """
    from ccs.coordinator import service as _service

    _service._V090_FIRST_USE_WARNED = True


@pytest.fixture(autouse=True)
def _neutralize_v090_first_use_warning():
    """Suite-wide neutralizer for the v0.9.0 ``CrashRecoveryConfig`` transitional
    ``RuntimeWarning`` (C-flip plan, Unit 5).

    The warning fires once per process on the FIRST ``CrashRecoveryConfig``
    construction — including explicit ones. Without this, whichever test
    constructs a config first (especially under ``warnings.simplefilter
    ("error")``) would observe the warning order-dependently. We mark the
    module-level flag "already warned" before every test so the warning is a
    no-op by default.

    The dedicated tests in ``tests/test_coordinator.py`` that ASSERT the
    warning opt back in via their own ``reset_v090_first_use_flag`` fixture
    (or an inline reset), which pytest runs after this autouse fixture and
    flips the flag to ``False`` so the warning fires for those tests only.
    """
    from ccs.coordinator import service as _service

    _service._V090_FIRST_USE_WARNED = True
    yield
