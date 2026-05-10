# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""Write-pattern classifiers for ``ccs-diagnose``.

The default classifier is ``langgraph-v0-preview``. The package mirrors the
``ccs.strategies.selector`` registry pattern: classifiers are registered by
name so the CLI (Unit 7) can switch implementations as the corpus grows.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence

from ccs.diagnose.callback import DiagnoseEvent

from .langgraph_v0_preview import (
    APPEND_ONLY_NAME_SUFFIXES,
    DEFAULT_EPHEMERA_KEYS,
    KNOWN_FRAMEWORK_KEYS,
    Bucket,
    ClassifierOverrides,
    ClassifierVerdict,
    Confidence,
    CoverageReport,
    build_key_index,
    classify as _classify_langgraph_v0_preview,
)

__all__ = [
    "Bucket",
    "Confidence",
    "CoverageReport",
    "ClassifierVerdict",
    "ClassifierOverrides",
    "build_key_index",
    "classify",
    "select_classifier",
    "DEFAULT_CLASSIFIER_NAME",
    "DEFAULT_EPHEMERA_KEYS",
    "KNOWN_FRAMEWORK_KEYS",
    "APPEND_ONLY_NAME_SUFFIXES",
    "langgraph_v0_preview",
]


# Re-export the langgraph-v0-preview module as a convenience alias for
# callers that want explicit attribute access (matches the `selector.py`
# strategy lookup convention).
from . import langgraph_v0_preview  # noqa: E402  (positioned for re-export)


DEFAULT_CLASSIFIER_NAME: str = "langgraph-v0-preview"


_REGISTRY: Mapping[str, Callable[..., ClassifierVerdict]] = {
    "langgraph-v0-preview": _classify_langgraph_v0_preview,
}


def select_classifier(name: str) -> Callable[..., ClassifierVerdict]:
    """Return the ``classify`` entry point for ``name``.

    Raises ``ValueError`` for unknown names. Mirrors
    :func:`ccs.strategies.selector.build_strategy`.
    """
    normalized = name.strip().lower()
    if normalized not in _REGISTRY:
        raise ValueError(f"unknown classifier {name!r}")
    return _REGISTRY[normalized]


def classify(
    events: Sequence[DiagnoseEvent],
    *,
    overrides: ClassifierOverrides | None = None,
    classifier_name: str = DEFAULT_CLASSIFIER_NAME,
    **kwargs: object,
) -> ClassifierVerdict:
    """Run the named classifier (default: ``langgraph-v0-preview``)."""
    fn = select_classifier(classifier_name)
    return fn(events, overrides=overrides, **kwargs)
