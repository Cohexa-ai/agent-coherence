# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Full-surface conformance contract for the registry Protocols (Phase 1).

Before the Phase-1 extraction the two registries
(:class:`~ccs.coordinator.registry.ArtifactRegistry` and
:class:`~ccs.coordinator.sqlite_registry.SqliteArtifactRegistry`) shared no base
class and their parity was asserted piecemeal (one behaviour at a time). This
module pins the WHOLE surface:

- both registries are ``isinstance`` of :class:`RegistryBase`, and the SQLite one
  is additionally ``isinstance`` of :class:`SqliteExtended`;
- the exact expected method names (34 base + 13 extended) are present + callable
  on each registry;
- each registry method's parameter STRUCTURE (names, kinds, defaults — annotation
  strings ignored, since ``from __future__ import annotations`` stringifies them
  and they legitimately differ, e.g. ``get_content``'s ``str`` vs ``bytes``)
  matches the Protocol method's;
- a deliberately-incomplete stub missing one base method is NOT
  ``isinstance(RegistryBase)`` — proving conformance has teeth;
- :class:`CoordinatorService` (typed against :class:`RegistryBase`) works with a
  real registry end-to-end (register + read smoke).
"""

from __future__ import annotations

import inspect
from collections.abc import Iterator
from pathlib import Path
from uuid import uuid4

import pytest

from ccs.coordinator.registry import ArtifactRegistry
from ccs.coordinator.registry_protocol import RegistryBase, SqliteExtended
from ccs.coordinator.service import CoordinatorService
from ccs.coordinator.sqlite_registry import SqliteArtifactRegistry
from ccs.core.types import Artifact, FetchRequest

# The full expected surface, frozen here as the contract (not derived from the
# Protocol at runtime so a silent Protocol edit cannot also silently move the
# goalposts this test guards).
BASE_METHODS = frozenset(
    {
        "abort_guard",
        "all_session_meta",
        "artifact_ids",
        "capture_version_vector",
        "clear_agent_transient",
        "commit_cas",
        "get_agent_state",
        "get_agent_transient",
        "get_artifact",
        "get_content",
        "get_content_at_version",
        "get_last_reclamation",
        "get_owner_generation",
        "get_read_generation",
        "get_session_cut",
        "get_session_meta",
        "get_state_map",
        "get_transient_map",
        "get_transient_tick",
        "get_version_record",
        "granted_at_tick",
        "has_artifact",
        "last_heartbeat_tick",
        "record_heartbeat",
        "record_last_reclamation",
        "register_artifact",
        "release_session",
        "remove_artifact",
        "retention_meta",
        "session_count",
        "set_agent_state",
        "set_agent_transient",
        "set_artifact_and_content",
        "valid_holders",
    }
)

EXTENDED_ONLY_METHODS = frozenset(
    {
        "artifact_names_under_prefix",
        "artifacts_held_by_agent",
        "close",
        "evict_stale_notices",
        "get_artifact_updated_at",
        "last_writer_for",
        "lookup_artifact_id_by_name",
        "peek_preemption_notice",
        "pop_pending_notices",
        "pop_preemption_notice",
        "record_preemption_notice",
        "resolve_or_register",
        "status_snapshot",
    }
)


@pytest.fixture
def inmem_registry() -> ArtifactRegistry:
    return ArtifactRegistry()


@pytest.fixture
def sqlite_registry(tmp_path: Path) -> Iterator[SqliteArtifactRegistry]:
    reg = SqliteArtifactRegistry(tmp_path / "parity.db")
    yield reg
    reg.close()


def _param_structure(func: object) -> list[tuple[str, object, object]]:
    """Return ``(name, kind, default)`` per parameter, EXCLUDING ``self`` and
    annotations. Annotations are excluded on purpose: ``from __future__
    import annotations`` makes them strings that legitimately differ between a
    registry and the Protocol (and between the two registries), so structure —
    names + kinds + defaults — is the conformance invariant we pin."""
    sig = inspect.signature(func)  # type: ignore[arg-type]
    return [
        (name, p.kind, p.default)
        for name, p in sig.parameters.items()
        if name != "self"
    ]


# ---------------------------------------------------------------------------
# isinstance — structural conformance via the runtime_checkable Protocols
# ---------------------------------------------------------------------------


def test_inmem_registry_is_registry_base(inmem_registry: ArtifactRegistry) -> None:
    assert isinstance(inmem_registry, RegistryBase)


def test_sqlite_registry_is_registry_base(
    sqlite_registry: SqliteArtifactRegistry,
) -> None:
    assert isinstance(sqlite_registry, RegistryBase)


def test_sqlite_registry_is_sqlite_extended(
    sqlite_registry: SqliteArtifactRegistry,
) -> None:
    assert isinstance(sqlite_registry, SqliteExtended)


# ---------------------------------------------------------------------------
# Full expected surface — names present + callable
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("name", sorted(BASE_METHODS))
def test_base_method_present_on_inmem(
    inmem_registry: ArtifactRegistry, name: str
) -> None:
    assert callable(getattr(inmem_registry, name))


@pytest.mark.parametrize("name", sorted(BASE_METHODS))
def test_base_method_present_on_sqlite(
    sqlite_registry: SqliteArtifactRegistry, name: str
) -> None:
    assert callable(getattr(sqlite_registry, name))


@pytest.mark.parametrize("name", sorted(EXTENDED_ONLY_METHODS))
def test_extended_method_present_on_sqlite(
    sqlite_registry: SqliteArtifactRegistry, name: str
) -> None:
    assert callable(getattr(sqlite_registry, name))


def test_protocol_surface_matches_expected_names() -> None:
    """The Protocols expose EXACTLY the frozen expected names (no drift)."""
    base_names = {
        n
        for n in dir(RegistryBase)
        if not n.startswith("_") and callable(getattr(RegistryBase, n))
    }
    extended_names = {
        n
        for n in dir(SqliteExtended)
        if not n.startswith("_") and callable(getattr(SqliteExtended, n))
    }
    assert base_names == BASE_METHODS
    assert extended_names == BASE_METHODS | EXTENDED_ONLY_METHODS


# ---------------------------------------------------------------------------
# Parameter STRUCTURE parity (names + kinds + defaults; annotations ignored)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("name", sorted(BASE_METHODS))
def test_inmem_base_method_structure_matches_protocol(
    inmem_registry: ArtifactRegistry, name: str
) -> None:
    proto = getattr(RegistryBase, name)
    impl = getattr(type(inmem_registry), name)
    assert _param_structure(impl) == _param_structure(proto)


@pytest.mark.parametrize("name", sorted(BASE_METHODS))
def test_sqlite_base_method_structure_matches_protocol(
    sqlite_registry: SqliteArtifactRegistry, name: str
) -> None:
    proto = getattr(RegistryBase, name)
    impl = getattr(type(sqlite_registry), name)
    assert _param_structure(impl) == _param_structure(proto)


@pytest.mark.parametrize("name", sorted(EXTENDED_ONLY_METHODS))
def test_sqlite_extended_method_structure_matches_protocol(
    sqlite_registry: SqliteArtifactRegistry, name: str
) -> None:
    proto = getattr(SqliteExtended, name)
    impl = getattr(type(sqlite_registry), name)
    assert _param_structure(impl) == _param_structure(proto)


# ---------------------------------------------------------------------------
# Teeth — an incomplete stub is rejected
# ---------------------------------------------------------------------------


def test_incomplete_stub_is_not_registry_base() -> None:
    """A class implementing every base method EXCEPT one is not an instance of
    RegistryBase — so the structural check actually discriminates."""

    class _AlmostRegistry:
        pass

    # Attach every base method except a deliberately-omitted one.
    omitted = "commit_cas"
    for name in BASE_METHODS:
        if name == omitted:
            continue
        setattr(_AlmostRegistry, name, lambda self, *a, **k: None)

    instance = _AlmostRegistry()
    assert not hasattr(instance, omitted)
    assert not isinstance(instance, RegistryBase)


# ---------------------------------------------------------------------------
# Service smoke — CoordinatorService (typed RegistryBase) drives a real registry
# ---------------------------------------------------------------------------


def test_service_against_protocol_typed_registry_smoke() -> None:
    """CoordinatorService is constructed against the RegistryBase parameter type
    and a trivial register/read round-trips through it."""
    registry: RegistryBase = ArtifactRegistry()
    svc = CoordinatorService(registry)

    artifact = svc.register_artifact(name="plan.md", content="seed-v1")
    assert isinstance(artifact, Artifact)

    resp = svc.fetch(
        FetchRequest(
            artifact_id=artifact.id,
            requesting_agent_id=uuid4(),
            requested_at_tick=1,
        )
    )
    assert resp.content == "seed-v1"
    assert resp.version == artifact.version
