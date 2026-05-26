# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""Import-boundary and cycle checks for CCS modules."""

from __future__ import annotations

import ast
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

_LAYER_BY_NAMESPACE = {
    "core": "core",
    "artifacts": "core",
    "coordinator": "application",
    "strategies": "application",
    "agent": "application",
    "bus": "infrastructure",
    "transport": "infrastructure",
    "simulation": "infrastructure",
    "output": "interface",
    "cli": "interface",
    "adapters": "interface",
    "validation": "interface",
    "diagnose": "interface",
    "replay": "interface",
}

_ALLOWED_LAYER_IMPORTS = {
    "core": {"core"},
    "application": {"core", "application"},
    "infrastructure": {"core", "application", "infrastructure"},
    "interface": {"core", "application", "infrastructure", "interface"},
}


@dataclass(frozen=True)
class ArchitectureReport:
    """Summary of architecture gate checks."""

    modules_scanned: int
    edges_scanned: int
    boundary_violations: list[str]
    cycles: list[list[str]]

    @property
    def ok(self) -> bool:
        return not self.boundary_violations and not self.cycles


def run_architecture_checks(src_root: Path) -> ArchitectureReport:
    """Run boundary and cycle checks for module graph rooted at src_root."""
    graph, _ = build_import_graph(src_root)
    violations = find_boundary_violations(graph)
    cycles = find_cycles(graph)
    return ArchitectureReport(
        modules_scanned=len(graph),
        edges_scanned=sum(len(edges) for edges in graph.values()),
        boundary_violations=violations,
        cycles=cycles,
    )


def format_report(report: ArchitectureReport) -> str:
    """Return human-readable architecture gate report text."""
    lines = [
        f"Modules scanned: {report.modules_scanned}",
        f"Import edges: {report.edges_scanned}",
    ]
    if report.boundary_violations:
        lines.append("")
        lines.append("Boundary violations:")
        lines.extend(f"- {item}" for item in report.boundary_violations)
    if report.cycles:
        lines.append("")
        lines.append("Import cycles:")
        lines.extend("- " + " -> ".join(cycle) for cycle in report.cycles)
    lines.append("")
    lines.append("Architecture checks passed." if report.ok else "Architecture checks failed.")
    return "\n".join(lines)


def main() -> int:
    """CLI entrypoint for package-level architecture checks."""
    src_root = Path.cwd() / "src"
    report = run_architecture_checks(src_root)
    print(format_report(report))
    return 0 if report.ok else 1


def build_import_graph(src_root: Path) -> tuple[dict[str, set[str]], dict[str, Path]]:
    """Return internal import graph keyed by full module path."""
    files = list(_iter_python_files(src_root / "ccs"))
    module_by_path: dict[Path, str] = {path: _module_from_path(src_root, path) for path in files}
    known_modules = set(module_by_path.values())

    graph: dict[str, set[str]] = {module: set() for module in known_modules}
    module_paths: dict[str, Path] = {module: path for path, module in module_by_path.items()}

    for path, module in module_by_path.items():
        for imported in _parse_internal_imports(path, module):
            candidate = _resolve_module_candidate(imported, known_modules)
            if candidate is not None and candidate != module:
                graph[module].add(candidate)

    return graph, module_paths


def find_boundary_violations(graph: dict[str, set[str]]) -> list[str]:
    """Return import edges that violate allowed layer boundaries."""
    violations: list[str] = []
    for source, targets in graph.items():
        source_layer = _layer_for_module(source)
        if source_layer is None:
            continue
        allowed = _ALLOWED_LAYER_IMPORTS[source_layer]
        for target in targets:
            target_layer = _layer_for_module(target)
            if target_layer is None:
                continue
            if target_layer not in allowed:
                violations.append(
                    f"{source} ({source_layer}) must not import {target} ({target_layer})"
                )
    return sorted(violations)


def find_cycles(graph: dict[str, set[str]]) -> list[list[str]]:
    """Return strongly-connected components with cycle cardinality >= 2."""
    visited: set[str] = set()
    stack: list[str] = []
    in_stack: set[str] = set()
    index: dict[str, int] = {}
    lowlink: dict[str, int] = {}
    components: list[list[str]] = []
    i = 0

    def strongconnect(node: str) -> None:
        nonlocal i
        index[node] = i
        lowlink[node] = i
        i += 1
        stack.append(node)
        in_stack.add(node)

        for target in graph.get(node, ()):
            if target not in index:
                strongconnect(target)
                lowlink[node] = min(lowlink[node], lowlink[target])
            elif target in in_stack:
                lowlink[node] = min(lowlink[node], index[target])

        if lowlink[node] == index[node]:
            component: list[str] = []
            while stack:
                member = stack.pop()
                in_stack.remove(member)
                component.append(member)
                if member == node:
                    break
            if len(component) > 1:
                components.append(sorted(component))

    for node in sorted(graph):
        if node not in visited:
            # Track visited via discovery index for minimal overhead.
            strongconnect(node)
            visited.update(index.keys())

    components.sort()
    return components


def _iter_python_files(root: Path) -> Iterable[Path]:
    for path in sorted(root.rglob("*.py")):
        if "__pycache__" in path.parts:
            continue
        yield path


def _module_from_path(src_root: Path, path: Path) -> str:
    relative = path.relative_to(src_root).with_suffix("")
    parts = list(relative.parts)
    if parts[-1] == "__init__":
        parts = parts[:-1]
    return ".".join(parts)


def _parse_internal_imports(path: Path, current_module: str) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    imports: set[str] = set()

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name.startswith("ccs."):
                    imports.add(alias.name)
        elif isinstance(node, ast.ImportFrom):
            resolved = _resolve_from_import(current_module, node.module, node.level)
            if resolved is not None and resolved.startswith("ccs"):
                imports.add(resolved)
    return imports


def _resolve_from_import(current_module: str, module: str | None, level: int) -> str | None:
    if level == 0:
        return module

    base_parts = current_module.split(".")
    if level > len(base_parts):
        return None

    prefix_parts = base_parts[: len(base_parts) - level]
    if module:
        prefix_parts.extend(module.split("."))
    return ".".join(prefix_parts)


def _resolve_module_candidate(imported: str, known_modules: set[str]) -> str | None:
    candidate = imported
    while candidate:
        if candidate in known_modules:
            return candidate
        if "." not in candidate:
            break
        candidate = candidate.rsplit(".", 1)[0]
    return None


def _layer_for_module(module: str) -> str | None:
    parts = module.split(".")
    if len(parts) < 2:
        return None
    return _LAYER_BY_NAMESPACE.get(parts[1])


if __name__ == "__main__":
    raise SystemExit(main())
