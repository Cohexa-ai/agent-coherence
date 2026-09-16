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
import subprocess
import sys
import tomllib
from glob import glob
from pathlib import Path

import pytest

#: This module's own object, for the tests that monkeypatch its helpers.
#: ``sys.modules[__name__]`` rather than a dotted re-import: tests/ is not a
#: package, and a re-import would hand the patches a different module object
#: than the one under test.
mod = sys.modules[__name__]

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


def _git_ignored_paths() -> set[Path]:
    """Absolute paths under ``src/ccs/`` that git reports as IGNORED.

    A file git ignores is not in the repository, so no wheel or sdist built
    from a checkout can carry it — it is not an asset this guard has any
    business demanding a declaration for.

    FAILS CLOSED. Every failure path returns an empty set, so nothing is
    subtracted and the guard stays at its strictest. That asymmetry is the
    whole reason this subtracts the IGNORED set instead of switching the walk
    to ``git ls-files`` (tracked-only): a tracked-only walk fails OPEN — no
    git means no assets, no undeclared set, and a module that reports green
    while checking nothing. ``--exclude-standard`` reads ``.gitignore``,
    ``.git/info/exclude`` and ``core.excludesFile``, and resolves the middle
    one from the COMMON git dir, so this is correct inside a worktree too.
    """
    try:
        completed = subprocess.run(
            [
                "git", "-C", str(REPO_ROOT), "ls-files",
                "--others", "--ignored", "--exclude-standard", "-z",
                "--", "src/ccs",
            ],
            capture_output=True,
            text=True,
            timeout=30,
            check=True,
        )
    # ValueError is not decoration: ``text=True`` decodes git's output, so a
    # path this locale cannot decode raises UnicodeDecodeError (a ValueError)
    # from inside ``run`` — neither OSError nor SubprocessError covers it, and
    # without it here the helper would crash rather than fail closed.
    except (OSError, ValueError, subprocess.SubprocessError):
        return set()
    return {REPO_ROOT / rel for rel in completed.stdout.split("\0") if rel}


def _shipped_assets() -> set[Path]:
    """Every non-Python, non-cache, non-git-ignored file under ``src/ccs/``.

    The git-ignored subtraction is what keeps this honest on a developer
    machine: local-only files (per-package ``CLAUDE.md`` instructions, a
    macOS ``.DS_Store``) sit under ``src/ccs/`` but are absent from the CI
    checkout, so without it this guard was red locally and green in CI — for
    files that were never going to ship either way.
    """
    ignored = _git_ignored_paths()
    return {
        path
        for path in (SRC_ROOT / "ccs").glob("**/*")
        if path.is_file()
        and path.suffix != ".py"
        and "__pycache__" not in path.parts
        and path not in ignored
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


# ----------------------------------------------------------------------
# The ignore filter: what git hides can never ship, so it is not an asset
# ----------------------------------------------------------------------
#
# ``_shipped_assets`` walks the filesystem, so it used to count every
# non-.py file under src/ccs/ — including local-only files git ignores.
# Six ``CLAUDE.md`` files (and, on macOS, any ``.DS_Store``) made this guard
# permanently red on a developer machine while staying green in CI, where
# ``actions/checkout`` materializes tracked content only. A guard that is
# always red is a guard nobody reads.


def test_shipped_assets_excludes_paths_git_reports_ignored(monkeypatch) -> None:
    """The walk consults the ignore filter. Proven with a sentinel — a real
    on-disk asset forced into the ignored set must drop out of the walk."""
    sentinel = SRC_ROOT / "ccs" / "output" / "templates" / "comparison_report.html"
    assert sentinel.is_file(), "fixture drifted: the sentinel asset must exist"
    assert sentinel in mod._shipped_assets(), "sentinel must be present before filtering"

    monkeypatch.setattr(mod, "_git_ignored_paths", lambda: {sentinel})
    assert sentinel not in mod._shipped_assets()


def test_ignored_lookup_fails_closed_when_git_is_unavailable(monkeypatch) -> None:
    """No git → an EMPTY ignored set, so nothing is subtracted and the guard
    stays at its strictest.

    This is why the filter subtracts the IGNORED set rather than switching the
    walk to `git ls-files` (tracked-only): a tracked-only walk would fail OPEN
    — a missing git yields no assets at all, no undeclared set, and a silently
    green guard that has stopped checking anything.
    """
    def _boom(*_a, **_kw):
        raise FileNotFoundError("git not on PATH")

    monkeypatch.setattr(mod.subprocess, "run", _boom)
    assert mod._git_ignored_paths() == set()

    monkeypatch.setattr(
        mod.subprocess, "run",
        lambda *_a, **_kw: (_ for _ in ()).throw(
            mod.subprocess.CalledProcessError(128, "git")
        ),
    )
    assert mod._git_ignored_paths() == set()

    # `text=True` decodes git's output, so a path this locale cannot decode
    # raises UnicodeDecodeError — a ValueError, which neither OSError nor
    # SubprocessError covers. Without it in the except clause the helper
    # CRASHES instead of failing closed, contradicting its own docstring.
    def _undecodable(*_a, **_kw):
        raise UnicodeDecodeError("utf-8", b"\xff", 0, 1, "invalid start byte")

    monkeypatch.setattr(mod.subprocess, "run", _undecodable)
    assert mod._git_ignored_paths() == set()


def test_declared_templates_are_never_filtered_out() -> None:
    """Anti-fail-open: the two tracked, declared assets must survive the
    filter on every machine. An over-broad ignore filter would empty the
    walk and turn this whole module green while checking nothing."""
    shipped = mod._shipped_assets()
    for rel in (
        "ccs/output/templates/comparison_report.html",
        "ccs/diagnose/templates/diagnose_report.html",
    ):
        assert SRC_ROOT / rel in shipped, f"{rel} vanished from the asset walk"

    ignored = mod._git_ignored_paths()
    assert not ignored & shipped, "the filter and the walk must not overlap"
    # Under src/ccs specifically, not merely somewhere in the repo:
    # SRC_ROOT.parent is REPO_ROOT, which is a parent of EVERY repo path, so
    # asserting on it would pass for docs/ and tests/ alike — a check that
    # cannot see the case it names.
    package_root = SRC_ROOT / "ccs"
    for path in ignored:
        assert package_root in path.parents, (
            f"ignore filter reached outside the coordinated package: {path}"
        )
