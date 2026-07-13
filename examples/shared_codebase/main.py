"""4-agent code review pipeline with inline benchmark mode.

Demonstrates CCSStore token savings through intra-session cache hits:
each reviewer reads files in two passes (miss → hit), and the synthesizer
re-reads context files after ingesting all findings.

Graph: style_reviewer → security_reviewer → architecture_reviewer → synthesizer

Run:
    python -m examples.shared_codebase.main
"""

from __future__ import annotations

import pathlib
from typing import TypedDict

from langgraph.config import get_store as lg_get_store
from langgraph.graph import END, START, StateGraph

from ccs.adapters.ccsstore import CCSStore

# ---------------------------------------------------------------------------
# Fixture file loading
# ---------------------------------------------------------------------------

_FIXTURE_DIR = pathlib.Path(__file__).parent / "fixture_repo"

# Keys in ("code",) namespace (shared across all agents)
_FIXTURE_FILES: dict[str, str] = {
    str(p.relative_to(_FIXTURE_DIR)): p.read_text()
    for p in sorted(_FIXTURE_DIR.rglob("*.py"))
    if not p.name.startswith("_")
}

# File sets assigned to each reviewer (first-pass and focus-pass keys)
_STYLE_FIRST_PASS = [
    "models/user.py", "models/product.py", "models/order.py", "models/review.py",
    "api/users.py", "api/products.py", "api/orders.py", "utils/validators.py",
]
_STYLE_FOCUS_PASS = ["models/user.py", "models/product.py", "api/users.py", "api/products.py"]

_SECURITY_FIRST_PASS = [
    "api/auth.py", "api/payments.py", "utils/auth.py", "utils/cache.py",
    "models/payment.py", "models/session.py", "api/users.py", "api/orders.py",
]
_SECURITY_FOCUS_PASS = ["api/auth.py", "api/payments.py", "utils/auth.py", "models/payment.py"]

_ARCH_FIRST_PASS = [
    "utils/db.py", "utils/cache.py", "utils/auth.py", "models/inventory.py",
    "api/search.py", "tests/test_users.py", "tests/test_products.py", "tests/test_orders.py",
]
_ARCH_FOCUS_PASS = ["utils/db.py", "utils/cache.py", "api/search.py", "tests/test_orders.py"]

_SYNTH_CONTEXT = [
    "api/users.py", "api/auth.py", "utils/auth.py",
    "models/user.py", "models/payment.py", "utils/db.py",
]

SCOPE = "code"
REVIEWS_NS = "reviews"


# ---------------------------------------------------------------------------
# Graph state
# ---------------------------------------------------------------------------

class ReviewState(TypedDict):
    log: list[str]


# ---------------------------------------------------------------------------
# Reviewer nodes
# ---------------------------------------------------------------------------

def _read_files(store: CCSStore, agent: str, keys: list[str]) -> int:
    """Read a list of fixture files; return count of reads performed."""
    count = 0
    for key in keys:
        if key in _FIXTURE_FILES:
            store.get((agent, SCOPE), key)
            count += 1
    return count


def style_reviewer_node(state: ReviewState) -> dict:
    """Read files in two passes: broad scan then focused style critique."""
    store: CCSStore = lg_get_store()  # type: ignore[assignment]
    _read_files(store, "style_reviewer", _STYLE_FIRST_PASS)
    _read_files(store, "style_reviewer", _STYLE_FOCUS_PASS)  # hits
    store.put(
        ("style_reviewer", REVIEWS_NS), "style",
        {
            "reviewer": "style_reviewer",
            "issues": [
                "user.py: missing type hints on _validate_* helpers",
                "product.py: ProductVariant.reserve should log the reservation",
                "api/users.py: list_users missing input sanitisation on search param",
                "api/products.py: create_product slug not auto-generated on save",
            ],
            "verdict": "REQUEST_CHANGES",
        },
    )
    return {"log": [*state["log"], "style_reviewer: 8 files scanned, 4 re-read, findings written"]}


def security_reviewer_node(state: ReviewState) -> dict:
    """Read auth and payment files in two passes: broad then focused."""
    store: CCSStore = lg_get_store()  # type: ignore[assignment]
    _read_files(store, "security_reviewer", _SECURITY_FIRST_PASS)
    _read_files(store, "security_reviewer", _SECURITY_FOCUS_PASS)  # hits
    store.put(
        ("security_reviewer", REVIEWS_NS), "security",
        {
            "reviewer": "security_reviewer",
            "issues": [
                "auth.py: password reset token not invalidated after single use",
                "payments.py: _verify_stripe_signature uses hmac.new (deprecated), use hmac.new correctly",
                "utils/auth.py: require_permission doesn't validate token expiry before permission check",
                "models/session.py: access_token not rotated on privilege escalation",
            ],
            "severity": "HIGH",
            "verdict": "REQUEST_CHANGES",
        },
    )
    return {"log": [*state["log"], "security_reviewer: 8 files scanned, 4 re-read, findings written"]}


def architecture_reviewer_node(state: ReviewState) -> dict:
    """Review cross-cutting concerns: DB, caching, test coverage."""
    store: CCSStore = lg_get_store()  # type: ignore[assignment]
    _read_files(store, "architecture_reviewer", _ARCH_FIRST_PASS)
    _read_files(store, "architecture_reviewer", _ARCH_FOCUS_PASS)  # hits
    store.put(
        ("architecture_reviewer", REVIEWS_NS), "architecture",
        {
            "reviewer": "architecture_reviewer",
            "issues": [
                "utils/db.py: execute_query and execute_count reference self._session but __init__ doesn't set it",
                "utils/cache.py: RedisCache.connect doesn't actually initialise self._client",
                "api/search.py: global_search runs per-type queries sequentially; should parallelise",
                "tests/: test coverage missing for payment webhook idempotency path",
            ],
            "verdict": "REQUEST_CHANGES",
        },
    )
    return {"log": [*state["log"], "architecture_reviewer: 8 files scanned, 4 re-read, findings written"]}


def synthesizer_node(state: ReviewState) -> dict:
    """Read all findings, then read and re-read context files for cross-referencing."""
    store: CCSStore = lg_get_store()  # type: ignore[assignment]

    # First pass: read context files to understand the codebase (misses)
    _read_files(store, "synthesizer", _SYNTH_CONTEXT)

    # Read all three sets of findings (misses — first time for synthesizer)
    style = store.get(("synthesizer", REVIEWS_NS), "style")
    security = store.get(("synthesizer", REVIEWS_NS), "security")
    architecture = store.get(("synthesizer", REVIEWS_NS), "architecture")

    # Second pass: re-read context files to cross-reference findings (hits)
    _read_files(store, "synthesizer", _SYNTH_CONTEXT)

    issue_count = sum(
        len(r.value.get("issues", [])) if r is not None else 0
        for r in [style, security, architecture]
    )
    store.put(
        ("synthesizer", REVIEWS_NS), "final",
        {
            "total_issues": issue_count,
            "blockers": 2,
            "decision": "REQUEST_CHANGES",
            "summary": "Security issues (HIGH severity) must be resolved before merge.",
        },
    )
    return {"log": [*state["log"], f"synthesizer: 3 findings read, context re-read ({issue_count} issues total)"]}


# ---------------------------------------------------------------------------
# Graph construction
# ---------------------------------------------------------------------------

def build_graph(store: CCSStore):
    builder = StateGraph(ReviewState)
    builder.add_node("style_reviewer", style_reviewer_node)
    builder.add_node("security_reviewer", security_reviewer_node)
    builder.add_node("architecture_reviewer", architecture_reviewer_node)
    builder.add_node("synthesizer", synthesizer_node)
    builder.add_edge(START, "style_reviewer")
    builder.add_edge("style_reviewer", "security_reviewer")
    builder.add_edge("security_reviewer", "architecture_reviewer")
    builder.add_edge("architecture_reviewer", "synthesizer")
    builder.add_edge("synthesizer", END)
    return builder.compile(store=store)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run() -> None:
    store = CCSStore(strategy="lazy", benchmark=True)

    # Pre-populate all fixture files into the shared ("code",) namespace.
    for file_key, content in _FIXTURE_FILES.items():
        store.put(("setup", SCOPE), file_key, {"path": file_key, "content": content})

    graph = build_graph(store)
    final_state = graph.invoke({"log": []})

    print()
    print("Example: 4-agent shared-codebase code review")
    print()
    for entry in final_state["log"]:
        print(f"  {entry}")
    print()
    store.print_benchmark_summary()


if __name__ == "__main__":
    run()

# Comparing notes on multi-agent coherence?
# https://github.com/Cohexa-ai/agent-coherence/discussions
