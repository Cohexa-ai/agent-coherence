"""Tests for ccs-benchmark CLI (src/ccs/cli/benchmark.py).

All tests run without LangGraph installed — CCSStore and graph invocation
are mocked via MagicMock and monkeypatching.
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

_REPO_ROOT = Path(__file__).parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from src.ccs.cli import benchmark as bm

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_mock_store():
    store = MagicMock()
    store.print_benchmark_summary = MagicMock()
    return store


def _write_factory_file(tmp_path: Path, fn_name: str = "build_graph", body: str = "") -> Path:
    """Write a minimal factory Python file to tmp_path."""
    code = f"def {fn_name}(store):\n    return store._mock_graph\n{body}\n"
    path = tmp_path / "my_graph.py"
    path.write_text(code)
    return path


# ---------------------------------------------------------------------------
# Test 1: Happy path — returns 0 and calls print_benchmark_summary
# ---------------------------------------------------------------------------

def test_happy_path_returns_zero_and_prints_summary(tmp_path):
    factory_path = _write_factory_file(tmp_path)
    mock_store = _make_mock_store()
    mock_graph = MagicMock()
    mock_store._mock_graph = mock_graph

    with patch.object(bm, "CCSStore", return_value=mock_store, create=True):
        # Patch the import inside main()
        with patch.dict("sys.modules", {"ccs.adapters.ccsstore": MagicMock(CCSStore=lambda **kw: mock_store)}):
            result = bm.main(["--graph", f"{factory_path}:build_graph"])

    assert result == 0


# ---------------------------------------------------------------------------
# Test 2: --initial-state passes dict to graph.invoke
# ---------------------------------------------------------------------------

def test_initial_state_passed_to_invoke(tmp_path):
    factory_path = tmp_path / "my_graph.py"
    factory_path.write_text(
        "def build_graph(store):\n    return store._mock_graph\n"
    )
    mock_store = _make_mock_store()
    mock_graph = MagicMock()
    mock_store._mock_graph = mock_graph

    ccsstore_mod = MagicMock()
    ccsstore_mod.CCSStore.return_value = mock_store

    with patch.dict("sys.modules", {"ccs.adapters.ccsstore": ccsstore_mod}):
        result = bm.main([
            "--graph", f"{factory_path}:build_graph",
            "--initial-state", '{"messages": ["hello"]}',
        ])

    assert result == 0
    mock_graph.invoke.assert_called_once_with({"messages": ["hello"]})


# ---------------------------------------------------------------------------
# Test 3: Default --initial-state '{}' → empty dict
# ---------------------------------------------------------------------------

def test_default_initial_state_is_empty_dict(tmp_path):
    factory_path = tmp_path / "my_graph.py"
    factory_path.write_text("def build_graph(store):\n    return store._mock_graph\n")
    mock_store = _make_mock_store()
    mock_graph = MagicMock()
    mock_store._mock_graph = mock_graph

    ccsstore_mod = MagicMock()
    ccsstore_mod.CCSStore.return_value = mock_store

    with patch.dict("sys.modules", {"ccs.adapters.ccsstore": ccsstore_mod}):
        result = bm.main(["--graph", f"{factory_path}:build_graph"])

    assert result == 0
    mock_graph.invoke.assert_called_once_with({})


# ---------------------------------------------------------------------------
# Test 4: Invalid JSON in --initial-state → exit 1
# ---------------------------------------------------------------------------

def test_invalid_initial_state_json_returns_one(tmp_path, capsys):
    factory_path = _write_factory_file(tmp_path)
    result = bm.main(["--graph", f"{factory_path}:build_graph", "--initial-state", "not json"])
    assert result == 1
    err = capsys.readouterr().err
    assert "not valid JSON" in err


# ---------------------------------------------------------------------------
# Test 5: Non-existent graph path → SystemExit(1)
# ---------------------------------------------------------------------------

def test_nonexistent_graph_path_exits(tmp_path):
    with pytest.raises(SystemExit) as exc_info:
        bm.main(["--graph", "/nonexistent/path.py:build_graph"])
    assert exc_info.value.code == 1


# ---------------------------------------------------------------------------
# Test 6: Function not found on module → SystemExit(1)
# ---------------------------------------------------------------------------

def test_missing_function_exits(tmp_path, capsys):
    factory_path = tmp_path / "my_graph.py"
    factory_path.write_text("# no build_graph here\n")

    with pytest.raises(SystemExit) as exc_info:
        bm.main(["--graph", f"{factory_path}:build_graph"])
    assert exc_info.value.code == 1
    err = capsys.readouterr().err
    assert "build_graph" in err


# ---------------------------------------------------------------------------
# Test 7: Factory has a sibling file import (sys.path prepend test)
# ---------------------------------------------------------------------------

def test_factory_with_sibling_import_resolves(tmp_path):
    """sys.path prepend lets the factory import a sibling module."""
    sibling = tmp_path / "helpers.py"
    sibling.write_text("VALUE = 42\n")
    factory_path = tmp_path / "my_graph.py"
    factory_path.write_text(
        "import helpers\n"
        "def build_graph(store):\n"
        "    assert helpers.VALUE == 42\n"
        "    return store._mock_graph\n"
    )

    mock_store = _make_mock_store()
    mock_graph = MagicMock()
    mock_store._mock_graph = mock_graph

    ccsstore_mod = MagicMock()
    ccsstore_mod.CCSStore.return_value = mock_store

    with patch.dict("sys.modules", {"ccs.adapters.ccsstore": ccsstore_mod}):
        result = bm.main(["--graph", f"{factory_path}:build_graph"])

    assert result == 0


# ---------------------------------------------------------------------------
# Test 8: --help includes install command
# ---------------------------------------------------------------------------

def test_help_includes_install_command(capsys):
    with pytest.raises(SystemExit):
        bm.build_parser().parse_args(["--help"])
    out = capsys.readouterr().out
    assert "agent-coherence[langgraph,benchmark]" in out


# ---------------------------------------------------------------------------
# Test 9: --initial-state non-dict JSON (list) → exit 1
# ---------------------------------------------------------------------------

def test_initial_state_non_dict_returns_one(tmp_path, capsys):
    factory_path = _write_factory_file(tmp_path)
    result = bm.main(["--graph", f"{factory_path}:build_graph", "--initial-state", "[1,2,3]"])
    assert result == 1
    err = capsys.readouterr().err
    assert "JSON object" in err
