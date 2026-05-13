"""Pre-commit hook: verify README benchmark table values match expected.json.

Reads benchmarks/expected.json, rounds each token_reduction_pct to the nearest integer,
and checks that each rounded value appears as **NN%** in the README's benchmark
table.

The table is located in priority order:
  1. The block under a ``## Real-workload benchmarks`` section heading, if present.
  2. Failing that, the block starting at the table's column-header line
     (``| Workload | Agents | Reads:Writes | Hit rate | Savings |``) and
     extending until the first non-table line.

This works whether the README places the table at the top with no section
heading (current main) or under a dedicated heading (some prior shapes).

Exits 0 if all values match. Exits 1 if any value is missing, or if required files
are absent.

Usage (pre-commit invokes this automatically when expected.json or README.md is staged):
    python tools/check_readme_numbers.py
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).parent.parent
_EXPECTED_PATH = _REPO_ROOT / "benchmarks" / "expected.json"
_README_PATH = _REPO_ROOT / "README.md"

_SECTION_HEADER = "## Real-workload benchmarks"
_TABLE_HEADER = "| Workload | Agents | Reads:Writes | Hit rate | Savings |"
_BOLD_PCT_RE = re.compile(r"\*\*(\d+)%\*\*")


def _extract_readme_section(readme_text: str) -> str:
    """Return the README block that contains the benchmark table.

    Tries the named section heading first; if absent, falls back to extracting
    the table block by its column-header line. Returns "" if neither anchor
    is found.
    """
    # Path 1: dedicated section heading.
    start = readme_text.find(_SECTION_HEADER)
    if start != -1:
        end = readme_text.find("\n## ", start + len(_SECTION_HEADER))
        return readme_text[start:end] if end != -1 else readme_text[start:]

    # Path 2: direct table-header anchor. Extract from the column-header line
    # downward until a non-table line (blank line, non-`|` line, or `## `).
    table_start = readme_text.find(_TABLE_HEADER)
    if table_start == -1:
        return ""

    lines = readme_text[table_start:].splitlines(keepends=True)
    block: list[str] = []
    for line in lines:
        stripped = line.lstrip()
        if not stripped or stripped.startswith("## ") or not stripped.startswith("|"):
            break
        block.append(line)
    return "".join(block)


def check_readme_numbers(expected_path: Path, readme_path: Path) -> bool:
    """Check that rounded expected values appear in the README table; return True if OK."""
    if not expected_path.exists():
        print(
            f"ERROR: {expected_path} not found — "
            "run `make benchmark` to establish a baseline, then commit the file.",
            file=sys.stderr,
        )
        return False

    if not readme_path.exists():
        print(f"ERROR: {readme_path} not found.", file=sys.stderr)
        return False

    workloads = json.loads(expected_path.read_text()).get("workloads", [])
    if not workloads:
        print("ERROR: expected.json contains no workloads.", file=sys.stderr)
        return False

    section = _extract_readme_section(readme_path.read_text())
    if not section:
        print(
            f"ERROR: Could not locate the benchmark table in README.md. "
            f"Expected either a '{_SECTION_HEADER}' section heading or a "
            f"table beginning with the column-header line "
            f"'{_TABLE_HEADER}'.",
            file=sys.stderr,
        )
        return False

    readme_pcts = {int(m) for m in _BOLD_PCT_RE.findall(section)}

    missing: list[str] = []
    for w in workloads:
        display = round(w["token_reduction_pct"])
        if display not in readme_pcts:
            missing.append(
                f"  expected **{display}%** (from '{w['name']}'"
                f" token_reduction_pct={w['token_reduction_pct']:.2f})"
                " — not found in README ## Real-workload benchmarks table"
            )

    if missing:
        print("README benchmark numbers are stale:", file=sys.stderr)
        for msg in missing:
            print(msg, file=sys.stderr)
        print(
            "\nUpdate the Savings column in README.md ## Real-workload benchmarks to match expected.json.",
            file=sys.stderr,
        )
        return False

    print("README benchmark numbers are up to date.")
    return True


def main() -> None:
    passed = check_readme_numbers(_EXPECTED_PATH, _README_PATH)
    sys.exit(0 if passed else 1)


if __name__ == "__main__":
    main()
