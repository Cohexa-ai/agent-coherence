"""Unit 6 — the red→green front-door artifact is a trustworthy gate.

RED loses, both GREEN regimes hold to their EXACT finals, and the negative
control proves the green depends on the deny (not on a refetch that masks the
loss). Deterministic and de-named.
"""

from __future__ import annotations

from pathlib import Path

from examples.mcp_stale_write_guard import GOLDEN, PEER_VALUE, broken, fixed
from examples.mcp_stale_write_guard.main import main


def test_demo_gate_exits_zero() -> None:
    assert main() == 0


def test_red_broken_loses_the_update() -> None:
    assert broken.run_broken()["lost_update"] is True


def test_green_sequential_preserves_peer_value_exactly() -> None:
    trace = fixed.run_sequential(guarded=True)
    assert trace["stale_write_denied"] is True
    assert trace["deny_reason"] == "stale_view"
    assert trace["final"] == PEER_VALUE  # exact, not an inequality


def test_green_concurrent_merges_to_golden_exactly() -> None:
    trace = fixed.run_concurrent()
    assert trace["a_committed"] is True
    assert trace["b_first_conflicted"] is True
    assert trace["b_merged_and_committed"] is True
    assert trace["final"] == GOLDEN  # both lines, exact


def test_negative_control_loses_with_deny_off() -> None:
    """The SAME green flow with the path unguarded (deny disabled) must lose —
    proving the deny is load-bearing, not a masking refetch."""
    trace = fixed.run_sequential(guarded=False)
    assert trace["stale_write_denied"] is False
    assert trace["preserved_peer_value"] is False


def test_concurrent_is_deterministic() -> None:
    assert fixed.run_concurrent()["final"] == GOLDEN
    assert fixed.run_concurrent()["final"] == GOLDEN


def test_demo_artifact_is_de_named() -> None:
    """The shipped demo carries no incident/codenames (publication honesty)."""
    import examples.mcp_stale_write_guard as pkg

    pkg_dir = Path(pkg.__file__).parent
    forbidden = ("viktor", "jacniacki", "zeta labs", "zeta-labs", "zetalabs")
    files = list(pkg_dir.glob("*.py")) + list(pkg_dir.glob("*.md"))
    for path in files:
        text = path.read_text().lower()
        for name in forbidden:
            assert name not in text, f"{path.name} contains forbidden name {name!r}"
