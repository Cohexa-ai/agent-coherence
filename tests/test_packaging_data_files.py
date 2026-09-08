# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Guard: every non-Python asset under ``src/ccs/`` ships in the distribution.

``ccs/output/report.py`` and ``ccs/diagnose/render.py`` read their HTML
templates off the filesystem next to the module. Nothing in the import system
notices when such a file is missing from the wheel — the failure surfaces only
at render time, in the installed-from-PyPI environment, as a bare
``FileNotFoundError``. The repo's own test run never sees it, because a source
checkout always has the file.

So this module asserts the packaging *declaration* instead of the built
artifact: it replays setuptools' own ``build_py.find_data_files`` algorithm
against ``pyproject.toml`` and fails if any asset in the tree is left
undeclared. That runs in a plain ``pytest -q`` with no wheel build and no
setuptools import; ``test_replayed_data_files_match_setuptools`` pins the
replay to the real backend wherever setuptools happens to be importable.
"""

from __future__ import annotations

import fnmatch
import os
import tomllib
from glob import glob
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
PYPROJECT_PATH = REPO_ROOT / "pyproject.toml"
SRC_ROOT = REPO_ROOT / "src"

# setuptools always folds these two in on top of the declared patterns
# (``setuptools.command.build_py._IMPLICIT_DATA_FILES``), so a ``py.typed``
# added later is covered without anyone touching pyproject.toml.
_IMPLICIT_DATA_PATTERNS = ("*.pyi", "py.typed")


def _iter_packages(src_root: Path) -> list[str]:
    """Dotted names of every directory setuptools would treat as a package.

    ``[tool.setuptools.packages.find]`` runs with ``namespaces = true`` by
    default under pyproject.toml, so discovery is NOT gated on ``__init__.py``
    — ``ccs.output.templates`` is itself a (namespace) package. Enumerating
    every directory therefore matches the backend, and keeps this replay from
    rejecting a valid ``"ccs.output.templates" = ["*.html"]`` declaration.
    ``test_replayed_data_files_match_setuptools`` is what holds that claim
    honest: if setuptools disagreed about any package, the two resolved sets
    would part ways there.
    """
    return sorted(
        str(directory.relative_to(src_root)).replace(os.sep, ".")
        for directory in src_root.glob("**/")
        if directory != src_root and "__pycache__" not in directory.parts
    )


def _patterns_for(spec: dict[str, list[str]], package: str, implicit: tuple[str, ...] = ()) -> list[str]:
    """Patterns ``spec`` applies to ``package``, joined to its directory.

    ``implicit`` carries setuptools' built-in includes; the exclude side
    passes none, matching ``exclude_data_files``, which does not fold them in.
    """
    package_dir = SRC_ROOT / Path(*package.split("."))
    raw = [*implicit, *spec.get("", []), *spec.get(package, [])]
    return [os.path.join(str(package_dir), *pattern.split("/")) for pattern in raw]


def _included_files(patterns: list[str]) -> set[str]:
    """Files matched by include patterns — setuptools' ``find_data_files``.

    ``glob(..., recursive=True)`` is the call it makes, so ``*`` stops at a
    path separator and only an explicit ``**`` descends. That is precisely why
    a bare ``"*"`` never reaches ``templates/comparison_report.html``.
    """
    return {hit for pattern in patterns for hit in glob(pattern, recursive=True) if os.path.isfile(hit)}


def _excluded_files(patterns: list[str], candidates: set[str]) -> set[str]:
    """Files dropped by exclude patterns — setuptools' ``exclude_data_files``.

    Note the asymmetry with the include side: excludes are ``fnmatch``ed
    against the already-globbed paths rather than re-globbed, so here a ``*``
    *does* cross a path separator.
    """
    return {hit for pattern in patterns for hit in fnmatch.filter(sorted(candidates), pattern)}


def _declared_data_files() -> set[Path]:
    """Every file the packaging config declares as package data."""
    config = tomllib.loads(PYPROJECT_PATH.read_text(encoding="utf-8"))
    setuptools_table = config.get("tool", {}).get("setuptools", {})
    package_data = setuptools_table.get("package-data", {})
    exclude_data = setuptools_table.get("exclude-package-data", {})

    declared: set[Path] = set()
    for package in _iter_packages(SRC_ROOT):
        included = _included_files(_patterns_for(package_data, package, _IMPLICIT_DATA_PATTERNS))
        declared |= {Path(hit) for hit in included - _excluded_files(_patterns_for(exclude_data, package), included)}
    return declared


def _shipped_assets() -> set[Path]:
    """Every non-Python, non-cache file living under ``src/ccs/``."""
    return {
        path
        for path in (SRC_ROOT / "ccs").glob("**/*")
        if path.is_file() and path.suffix != ".py" and "__pycache__" not in path.parts
    }


def test_every_non_python_asset_is_declared_package_data() -> None:
    """A new asset that nobody declared fails here, not in a user's install."""
    undeclared = sorted(path.relative_to(REPO_ROOT) for path in _shipped_assets() - _declared_data_files())

    assert not undeclared, (
        "These files live under src/ccs/ but would NOT ship in the wheel or sdist:\n"
        + "\n".join(f"  - {path}" for path in undeclared)
        + "\n\nAnything read at runtime via Path(__file__) must be declared, or an\n"
        "installed-from-PyPI import raises FileNotFoundError. Add the owning\n"
        "package to [tool.setuptools.package-data] in pyproject.toml, naming the\n"
        'subdirectory explicitly (e.g. "ccs.output" = ["templates/*.html"]) — a\n'
        'bare "*" is globbed against the package directory and stops at the\n'
        "path separator, so it never reaches a file one level down."
    )


def test_the_html_templates_are_declared() -> None:
    """The two templates whose absence this guard was written for."""
    declared = _declared_data_files()

    for relative in (
        "ccs/diagnose/templates/diagnose_report.html",
        "ccs/output/templates/comparison_report.html",
    ):
        assert SRC_ROOT / relative in declared, f"{relative} is not declared package data"


def test_replayed_data_files_match_setuptools(monkeypatch: pytest.MonkeyPatch) -> None:
    """Pin the replay above to what the real build backend resolves.

    The ``dev`` extra installs setuptools precisely so this runs in CI. It
    still degrades to a skip rather than an error, because the guard above
    must keep working in a bare venv — neither a fresh ``python -m venv`` nor
    GitHub's setup-python leaves setuptools importable on 3.12, so a hard
    requirement here would take the whole module down with it.
    """
    setuptools = pytest.importorskip("setuptools", reason="build backend not installed in this env")
    from setuptools.config.pyprojecttoml import apply_configuration

    # setuptools resolves package_dir and the dynamic version relative to cwd.
    monkeypatch.chdir(REPO_ROOT)
    distribution = setuptools.Distribution()
    apply_configuration(distribution, str(PYPROJECT_PATH))

    # ``include_package_data`` defaults on under pyproject.toml, and its leg of
    # find_data_files shells out to egg_info to read SOURCES.txt — which would
    # write *.egg-info into the working tree mid-test. Both sides of this
    # comparison are the *declared* patterns, which is the thing under guard;
    # the red-state build confirmed the manifest leg contributes nothing here
    # (no MANIFEST.in, no revision-control plugin).
    distribution.include_package_data = False

    build_py = distribution.get_command_obj("build_py")
    build_py.ensure_finalized()
    # setuptools reports source dirs relative to cwd; this comparison is
    # against absolute paths, so anchor them back on the repo root.
    resolved = {
        (REPO_ROOT / source_dir / name).resolve()
        for _package, source_dir, _build_dir, names in build_py.data_files
        for name in names
    }

    assert resolved == _declared_data_files()
