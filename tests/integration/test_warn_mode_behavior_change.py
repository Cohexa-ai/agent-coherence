# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""Re-read rate hard-gate harness (Unit 9 / R7 / origin §12 SC4).

This is THE launch gate. The warning is the plugin's value mechanism;
if the agent ignores it, the plugin has no edge. v0.1 does NOT ship
without ≥70% score on two consecutive N=40 runs.

## Operation

The harness:
1. Loads scenarios from ``scenarios/warn_mode_behaviors.yaml``
2. For each scenario:
   a. Spawns a fresh isolated git workspace
   b. Spawns a coordinator
   c. Pre-seeds SQLite state so the artifact's next Read returns stale
      with ``stale_versions`` version delta (≥3 per plan)
   d. Writes the "fresh" content to the file on disk so a re-read will
      surface the actual change
   e. Optionally seeds CLAUDE.md with a tool-restriction rule
      (phpmac_shape variants)
   f. Invokes ``claude --plugin-dir <plugin> --include-hook-events
      --output-format stream-json --print --model haiku "<prompt>"``
   g. Parses the stream-json output and classifies the post-warning
      trajectory: re-read | acknowledged | ignored | degenerate
3. Scores: ``(re-read + acknowledged) / (re-read + acknowledged + ignored)``
4. Hard gate: ≥ 70% on two consecutive runs

## Marker

Marked ``@pytest.mark.launch_gate`` so unit-test runs (``pytest -q``)
skip it. Invoke explicitly:

  pytest -m launch_gate tests/integration/test_warn_mode_behavior_change.py

## Cost

Single N=40 run: ~$1.60 in API tokens. Iteration cycle (up to 3
template variants): $200 hard cap per stopping rule. Pilot mode
(N=4, one per category) costs under $0.40.

## Time cost (operational)

Each scenario takes ~5min wall-clock end-to-end on a real claude
CLI invocation — dominated by OTHER user-installed plugins firing
their own hooks (hookify, claude-mem, compound-engineering, etc.)
on every event in the session. Not our plugin's overhead.

- N=4 pilot: ~20 min
- N=40 launch gate: ~3 hours per run; 2 consecutive runs = ~6 hours

Operational optimization (deferred to v0.1.1):
- Parallelize scenarios via ``ProcessPoolExecutor`` (8 workers ≈ 8x
  speedup → 20-25 min per full run)
- Use ``--bare --settings <auth_file>`` with an ANTHROPIC_API_KEY to
  skip OTHER plugins entirely (would drop per-scenario time to ~30s)

For v0.1 alpha, the harness ships and the launch-gate run is an
overnight operational task — kicked off before the AS-phpmac install
walkthrough invitations go out.

## Status

**Harness landed; full N=40 launch-gate run is a pre-launch
operational deliverable.** The current scenario file ships 16 of the
planned 40 scenarios (4 per category × 4 categories, including
phpmac-shaped variants per plan §SC4). Remaining 24 scenarios extend
trivially — the schema is stable and the harness scales linearly with
scenario count.

## Transcript handling (security)

Per plan: stream-json transcripts contain `additionalContext` warning
text, model reasoning, tool I/O (file content the agent read), and
the Bearer token. These are sensitive:

- Transcripts written to ``tempfile.mkdtemp()`` — never under ``tests/``
- Scored in-memory as the stream is consumed
- Only the classification (re-read/acknowledged/ignored/degenerate) +
  the warning text are retained for debugging — not the full transcript
- CI guard ensures no ``tests/`` directory contains transcript files
"""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Optional

import pytest
import yaml

from ccs.adapters.claude_code.lifecycle import (
    LifecycleConfig,
    ensure_coordinator,
    stop_coordinator,
)

SCENARIO_FILE = Path(__file__).parent / "scenarios" / "warn_mode_behaviors.yaml"
PLUGIN_REPO = Path("/Users/vladparakhin/projects/agent-coherence-plugin")
CLAUDE_BIN = Path("/Users/vladparakhin/.npm-global/bin/claude")
HARD_GATE_THRESHOLD = 0.70
MIN_N_FOR_LAUNCH = 40
# Per-scenario wall-clock cap. The harness docstring notes ~5min/scenario
# under realistic conditions (dominated by OTHER user-installed plugins
# firing on every hook event). Setting the timeout to exactly that figure
# guarantees borderline scenarios trip it, so we give 2× headroom and
# treat a timeout as a DEGENERATE classification (see _run_scenario).
SCENARIO_TIMEOUT_SEC = 600


# ----------------------------------------------------------------------
# Classification
# ----------------------------------------------------------------------


class Verdict(Enum):
    RE_READ = "re-read"
    ACKNOWLEDGED = "acknowledged"
    IGNORED = "ignored"
    DEGENERATE = "degenerate"


@dataclass
class ScenarioResult:
    scenario_id: str
    category: str
    phpmac_shape: bool
    verdict: Verdict
    warning_seen: bool
    additional_context_text: str = ""  # retained for debugging only
    notes: str = ""


# ----------------------------------------------------------------------
# Scenario loading
# ----------------------------------------------------------------------


def load_scenarios() -> list[dict[str, Any]]:
    """Load and validate scenario specs from the YAML file."""
    raw = yaml.safe_load(SCENARIO_FILE.read_text())
    scenarios = raw.get("scenarios", [])
    # Schema validation
    required = {"id", "category", "prompt", "seed", "expected_artifact_in_warning"}
    for sc in scenarios:
        missing = required - sc.keys()
        assert not missing, f"scenario {sc.get('id', '?')} missing: {missing}"
        assert sc["seed"].get("stale_versions", 0) >= 3, (
            f"scenario {sc['id']}: stale_versions must be ≥3 per plan §Unit 9"
        )
    return scenarios


# ----------------------------------------------------------------------
# Scenario execution
# ----------------------------------------------------------------------


def _build_workspace(scenario: dict[str, Any], tmp_root: Path) -> Path:
    """Create the workspace directory tree with the seeded file +
    optional CLAUDE.md for phpmac-shaped scenarios."""
    workspace = Path(tempfile.mkdtemp(prefix="warn-mode-", dir=tmp_root))
    subprocess.run(["git", "init", "-q"], cwd=workspace, check=True)

    seed = scenario["seed"]
    artifact_path = workspace / seed["artifact_path"]
    artifact_path.parent.mkdir(parents=True, exist_ok=True)
    # Write the FRESH content to disk (the version a re-read would surface)
    artifact_path.write_text(seed["stale_content"])

    if seed.get("phpmac_shape"):
        # Seed CLAUDE.md if not already the artifact under test
        if seed["artifact_path"] != "CLAUDE.md":
            (workspace / "CLAUDE.md").write_text(
                "# Project conventions\n"
                "Read all spec files before acting. Honor any restrictions\n"
                "noted in tracked artifacts.\n"
            )

    return workspace


def _seed_coordinator_state(workspace: Path, scenario: dict[str, Any], port: int) -> None:
    """Pre-seed SQLite state so the agent's next Read returns stale.

    Uses agent-coherence-track to register the artifact in the policy,
    then writes directly to SQLite to bump the version (simulating a
    peer session having committed updates)."""
    from ccs.adapters.claude_code.lifecycle import _read_port_from_file
    import sqlite3

    seed = scenario["seed"]
    artifact_path_rel = seed["artifact_path"]

    # Track the artifact
    track_bin = _resolve_bin("agent-coherence-track")
    subprocess.run(
        [track_bin, "--root", str(workspace), artifact_path_rel],
        check=True, capture_output=True, text=True,
    )

    # Use a synthetic stale-read injection: insert an artifact row with a
    # high version + a different last_writer than the registering session
    # will be. The plugin's first PreToolUse:Read on this path will see
    # a version-delta mismatch and emit a stale warning.
    #
    # Schema note: there's no `agents` table — agent identity is held in
    # the coordinator's in-memory _agent_names dict; last_writer_id is a
    # bare TEXT field. We just need an artifact row with the right shape.
    db_path = workspace / ".coherence" / "state.db"
    fake_writer = str(uuid.uuid4()).replace("-", "")  # hex form
    art_id = str(uuid.uuid4()).replace("-", "")
    now = float(time.time())
    conn = sqlite3.connect(str(db_path), isolation_level=None)
    try:
        conn.execute("BEGIN IMMEDIATE")
        # INSERT OR REPLACE by name (the UNIQUE constraint) so reseeding
        # the same artifact_path across re-runs is idempotent.
        conn.execute(
            """
            INSERT OR REPLACE INTO artifacts
              (id, name, version, content_hash, size_tokens, last_writer_id, updated_at)
            VALUES (?, ?, ?, ?, NULL, ?, ?)
            """,
            (
                art_id,
                artifact_path_rel,
                seed["stale_versions"],
                "f" * 64,  # synthetic stale hash — guarantees hash_differs
                fake_writer,
                now,
            ),
        )
        conn.execute("COMMIT")
    finally:
        conn.close()


def _resolve_bin(name: str) -> str:
    """Resolve a console script — prefer the project venv, fallback PATH."""
    repo_root = Path(__file__).parent.parent.parent
    candidate = repo_root / ".venv" / "bin" / name
    if candidate.is_file():
        return str(candidate)
    found = shutil.which(name)
    if found is None:
        pytest.skip(f"binary not available: {name}")
    return found


def _claude_available() -> bool:
    return CLAUDE_BIN.is_file()


def _plugin_dir_available() -> bool:
    return (PLUGIN_REPO / ".claude-plugin" / "plugin.json").is_file()


def _run_scenario(scenario: dict[str, Any], tmp_root: Path) -> ScenarioResult:
    """Execute one scenario end-to-end, returning the classified verdict."""
    if not _claude_available() or not _plugin_dir_available():
        return ScenarioResult(
            scenario_id=scenario["id"],
            category=scenario["category"],
            phpmac_shape=scenario["seed"].get("phpmac_shape", False),
            verdict=Verdict.DEGENERATE,
            warning_seen=False,
            notes="claude CLI or plugin not available — skipped",
        )

    workspace = _build_workspace(scenario, tmp_root)
    try:
        # Spawn coordinator + seed state
        cfg = LifecycleConfig(
            idle_shutdown_sec=0, sweep_interval_sec=0,
            spawn_self_probe_attempts=30,
        )
        port = ensure_coordinator(workspace, config=cfg)
        if port == -1:
            return ScenarioResult(
                scenario_id=scenario["id"],
                category=scenario["category"],
                phpmac_shape=scenario["seed"].get("phpmac_shape", False),
                verdict=Verdict.DEGENERATE,
                warning_seen=False,
                notes="coordinator failed to spawn",
            )
        try:
            _seed_coordinator_state(workspace, scenario, port)

            # Invoke claude (stream-json transcript written to tempdir)
            transcript_dir = Path(tempfile.mkdtemp(prefix="transcript-"))
            transcript_path = transcript_dir / f"{scenario['id']}.stream.jsonl"
            try:
                env = {**os.environ, "PATH": f"{CLAUDE_BIN.parent}:{os.environ.get('PATH','')}"}
                try:
                    proc = subprocess.run(
                        [
                            str(CLAUDE_BIN),
                            # `--setting-sources project` skips the user-level
                            # settings.json, which is where the OTHER plugins
                            # (hookify, claude-mem, compound-engineering, etc.)
                            # are configured. Without this, those plugins'
                            # hooks fire on every event in the test session
                            # and dominate wall-clock time (measured 26× slower
                            # in 2026-05-18 calibration). `--plugin-dir` still
                            # loads the agent-coherence plugin under test —
                            # the two flags are orthogonal.
                            "--setting-sources", "project",
                            "--plugin-dir", str(PLUGIN_REPO),
                            "--include-hook-events",
                            "--output-format", "stream-json",
                            "--print",
                            "--verbose",
                            "--permission-mode", "bypassPermissions",
                            "--model", "haiku",
                            scenario["prompt"],
                        ],
                        cwd=workspace,
                        env=env,
                        capture_output=True,
                        text=True,
                        timeout=SCENARIO_TIMEOUT_SEC,
                    )
                except subprocess.TimeoutExpired:
                    # A claude invocation that exceeds the per-scenario
                    # wall-clock budget is treated as DEGENERATE rather
                    # than crashing the whole gate run — preserves the
                    # signal from the other N-1 scenarios and surfaces
                    # via the degenerate_rate < 10% instrumentation gate.
                    return ScenarioResult(
                        scenario_id=scenario["id"],
                        category=scenario["category"],
                        phpmac_shape=scenario["seed"].get("phpmac_shape", False),
                        verdict=Verdict.DEGENERATE,
                        warning_seen=False,
                        notes=f"claude timeout after {SCENARIO_TIMEOUT_SEC}s",
                    )
                transcript_path.write_text(proc.stdout)
                # Score IN-MEMORY immediately
                verdict, warning_seen, ac_text = _classify(
                    proc.stdout, scenario["seed"]["artifact_path"]
                )
                return ScenarioResult(
                    scenario_id=scenario["id"],
                    category=scenario["category"],
                    phpmac_shape=scenario["seed"].get("phpmac_shape", False),
                    verdict=verdict,
                    warning_seen=warning_seen,
                    additional_context_text=ac_text[:1024],  # cap for debug
                    notes="" if proc.returncode == 0 else f"claude exit={proc.returncode}",
                )
            finally:
                # Security: delete the full transcript immediately — only
                # the classification + warning text are retained
                shutil.rmtree(transcript_dir, ignore_errors=True)
        finally:
            stop_coordinator(workspace)
    finally:
        shutil.rmtree(workspace, ignore_errors=True)


# ----------------------------------------------------------------------
# Stream-json classifier
# ----------------------------------------------------------------------


def _classify(
    stream_json: str, artifact_path: str
) -> tuple[Verdict, bool, str]:
    """Parse stream-json and classify the post-warning trajectory.

    Returns (verdict, warning_seen, additional_context_text).
    """
    events = []
    for line in stream_json.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            events.append(json.loads(line))
        except json.JSONDecodeError:
            continue

    # Find the PreToolUse:Read hook response that contained our stale-read
    # warning (additionalContext referencing the artifact_path)
    warning_text = ""
    warning_at_idx = -1
    for i, ev in enumerate(events):
        if ev.get("subtype") != "hook_response":
            continue
        if ev.get("hook_event") != "PreToolUse":
            continue
        stdout = ev.get("stdout", "")
        if not stdout or "additionalContext" not in stdout:
            continue
        if artifact_path in stdout and ("stale" in stdout.lower() or "preempt" in stdout.lower() or "updated by session" in stdout.lower()):
            warning_text = stdout
            warning_at_idx = i
            break

    warning_seen = warning_at_idx >= 0

    if not warning_seen:
        # No warning fired — instrumentation problem (likely scenario
        # didn't actually trigger stale-read path)
        return Verdict.DEGENERATE, False, ""

    # Look AT and AFTER the warning point for behavior signals.
    # re-read: a second Read tool_use for the same artifact AFTER the warning
    # acknowledged: assistant text mentions "updated", "stale", "re-read", "another session"
    # ignored: assistant proceeds to Edit/Write or summarizes without re-reading
    after = events[warning_at_idx + 1:]

    # Count Read tool_uses for our artifact after the warning
    re_read_count = 0
    acknowledged_phrases = re.compile(
        r"(another session|stale|outdated|re[- ]?read|updated since|preempted|re-check|let me re-read|i'll re-read)",
        re.IGNORECASE,
    )
    saw_acknowledged_text = False

    for ev in after:
        if ev.get("type") == "assistant":
            content = ev.get("message", {}).get("content", [])
            if not isinstance(content, list):
                continue
            for c in content:
                if not isinstance(c, dict):
                    continue
                if c.get("type") == "tool_use" and c.get("name") == "Read":
                    fp = (c.get("input") or {}).get("file_path", "")
                    if artifact_path in fp:
                        re_read_count += 1
                if c.get("type") == "text" and acknowledged_phrases.search(c.get("text", "")):
                    saw_acknowledged_text = True
                if c.get("type") == "thinking" and acknowledged_phrases.search(c.get("thinking", "")):
                    saw_acknowledged_text = True

    if re_read_count > 0:
        return Verdict.RE_READ, True, warning_text
    if saw_acknowledged_text:
        return Verdict.ACKNOWLEDGED, True, warning_text
    return Verdict.IGNORED, True, warning_text


# ----------------------------------------------------------------------
# Scoring
# ----------------------------------------------------------------------


@dataclass
class RunReport:
    results: list[ScenarioResult] = field(default_factory=list)

    @property
    def n_total(self) -> int:
        return len(self.results)

    @property
    def n_re_read(self) -> int:
        return sum(1 for r in self.results if r.verdict == Verdict.RE_READ)

    @property
    def n_acknowledged(self) -> int:
        return sum(1 for r in self.results if r.verdict == Verdict.ACKNOWLEDGED)

    @property
    def n_ignored(self) -> int:
        return sum(1 for r in self.results if r.verdict == Verdict.IGNORED)

    @property
    def n_degenerate(self) -> int:
        return sum(1 for r in self.results if r.verdict == Verdict.DEGENERATE)

    @property
    def score(self) -> float:
        scoreable = self.n_re_read + self.n_acknowledged + self.n_ignored
        if scoreable == 0:
            return 0.0
        return (self.n_re_read + self.n_acknowledged) / scoreable

    @property
    def degenerate_rate(self) -> float:
        return self.n_degenerate / self.n_total if self.n_total else 1.0

    def summary(self) -> str:
        return (
            f"N={self.n_total}  "
            f"re-read={self.n_re_read}  "
            f"acknowledged={self.n_acknowledged}  "
            f"ignored={self.n_ignored}  "
            f"degenerate={self.n_degenerate}  "
            f"score={self.score:.0%}  "
            f"degenerate_rate={self.degenerate_rate:.0%}"
        )


# ----------------------------------------------------------------------
# Pytest entry points
# ----------------------------------------------------------------------


def test_scenario_yaml_schema_valid() -> None:
    """Sanity: scenarios load without error and pass schema validation.
    Always runs (not gated on launch_gate marker) so YAML edits are
    immediately validated."""
    scenarios = load_scenarios()
    assert len(scenarios) >= 16, (
        f"need at least 16 scenarios (4 per category); got {len(scenarios)}"
    )
    # Category coverage
    by_category: dict[str, int] = {}
    for sc in scenarios:
        by_category[sc["category"]] = by_category.get(sc["category"], 0) + 1
    for cat in ("planning", "code-change", "review", "debugging"):
        assert by_category.get(cat, 0) >= 4, (
            f"category {cat} needs ≥4 scenarios; got {by_category.get(cat, 0)}"
        )
    # Each category must have at least one phpmac-shape scenario
    phpmac_by_cat: dict[str, int] = {}
    for sc in scenarios:
        if sc["seed"].get("phpmac_shape"):
            phpmac_by_cat[sc["category"]] = phpmac_by_cat.get(sc["category"], 0) + 1
    for cat in ("planning", "code-change", "review", "debugging"):
        assert phpmac_by_cat.get(cat, 0) >= 1, (
            f"category {cat} missing phpmac-shape variant (plan §SC4)"
        )


def test_classifier_re_read_path() -> None:
    """Classifier unit test: synthetic stream-json with a Read AFTER the
    warning → RE_READ verdict."""
    artifact = "docs/specs/test.md"
    stream = "\n".join([
        json.dumps({
            "subtype": "hook_response",
            "hook_event": "PreToolUse",
            "stdout": json.dumps({
                "hookSpecificOutput": {
                    "hookEventName": "PreToolUse",
                    "permissionDecision": "allow",
                    "additionalContext": f"⚠ Stale read: {artifact} was updated by session abc",
                }
            }),
        }),
        json.dumps({
            "type": "assistant",
            "message": {"content": [{"type": "tool_use", "name": "Read",
                                     "input": {"file_path": f"/x/{artifact}"}}]}
        }),
    ])
    verdict, warning_seen, _ = _classify(stream, artifact)
    assert warning_seen
    assert verdict == Verdict.RE_READ


def test_classifier_acknowledged_path() -> None:
    """Acknowledged: assistant text mentions 'another session updated' style
    language without a second Read."""
    artifact = "docs/plan.md"
    stream = "\n".join([
        json.dumps({
            "subtype": "hook_response",
            "hook_event": "PreToolUse",
            "stdout": json.dumps({
                "hookSpecificOutput": {
                    "hookEventName": "PreToolUse",
                    "additionalContext": f"⚠ Stale read: {artifact} was updated by session xyz",
                }
            }),
        }),
        json.dumps({
            "type": "assistant",
            "message": {"content": [{"type": "text",
                                     "text": "Another session has updated this; I'll note the stale-read warning."}]}
        }),
    ])
    verdict, _, _ = _classify(stream, artifact)
    assert verdict == Verdict.ACKNOWLEDGED


def test_classifier_ignored_path() -> None:
    """Ignored: no re-read, no acknowledged text → IGNORED."""
    artifact = "x.md"
    stream = "\n".join([
        json.dumps({
            "subtype": "hook_response",
            "hook_event": "PreToolUse",
            "stdout": json.dumps({
                "hookSpecificOutput": {
                    "hookEventName": "PreToolUse",
                    "additionalContext": f"⚠ Stale read: {artifact} was updated by session abc",
                }
            }),
        }),
        json.dumps({
            "type": "assistant",
            "message": {"content": [{"type": "text", "text": "Done."}]}
        }),
    ])
    verdict, _, _ = _classify(stream, artifact)
    assert verdict == Verdict.IGNORED


def test_classifier_no_warning_degenerate() -> None:
    """No warning fired → DEGENERATE (instrumentation problem)."""
    stream = json.dumps({
        "type": "assistant",
        "message": {"content": [{"type": "text", "text": "ok"}]}
    })
    verdict, warning_seen, _ = _classify(stream, "x.md")
    assert not warning_seen
    assert verdict == Verdict.DEGENERATE


def test_no_transcripts_committed_under_tests_dir() -> None:
    """CI guard: ensure no scenario run accidentally wrote a transcript
    under tests/ (transcripts contain sensitive content per plan)."""
    tests_root = Path(__file__).parent.parent
    leaked = list(tests_root.rglob("*.stream.jsonl"))
    assert not leaked, f"transcript files leaked under tests/: {leaked}"


# ----------------------------------------------------------------------
# The hard launch gate (live claude required)
# ----------------------------------------------------------------------


@pytest.mark.launch_gate
def test_launch_gate_re_read_rate(tmp_path: Path) -> None:
    """N=40 hard launch gate per R7. Skipped unless ``-m launch_gate``
    invoked. Requires live claude CLI + cost budget ~$4 per run.

    Pre-launch CI invokes this twice for the consecutive-runs requirement.
    Failure surfaces a structured go/no-go report (see plan §Unit 9
    failure path)."""
    if not _claude_available() or not _plugin_dir_available():
        pytest.skip("claude CLI or plugin not available")

    scenarios = load_scenarios()
    if len(scenarios) < MIN_N_FOR_LAUNCH:
        pytest.skip(
            f"need {MIN_N_FOR_LAUNCH} scenarios for launch gate; "
            f"scenario file has {len(scenarios)} — extend before running gate"
        )

    report = RunReport()
    for sc in scenarios[:MIN_N_FOR_LAUNCH]:
        result = _run_scenario(sc, tmp_path)
        report.results.append(result)

    print(f"\n{report.summary()}")

    # Plan §Unit 9 instrumentation gate: ≥10% degenerate → flag
    assert report.degenerate_rate < 0.10, (
        f"{report.degenerate_rate:.0%} degenerate scenarios — instrumentation issue, "
        f"do not draw conclusions"
    )

    assert report.score >= HARD_GATE_THRESHOLD, (
        f"launch gate FAILED: score={report.score:.0%} < {HARD_GATE_THRESHOLD:.0%}\n"
        f"{report.summary()}"
    )


@pytest.mark.launch_gate_pilot
def test_launch_gate_pilot(tmp_path: Path) -> None:
    """Pilot run: 4 scenarios (one per category) to validate the harness
    works against a real claude CLI before committing to the full N=40
    run. Cost: ~$0.40. Skipped unless ``-m launch_gate_pilot``."""
    if not _claude_available() or not _plugin_dir_available():
        pytest.skip("claude CLI or plugin not available")

    scenarios = load_scenarios()
    pilot = []
    seen_cats = set()
    for sc in scenarios:
        if sc["category"] not in seen_cats:
            pilot.append(sc)
            seen_cats.add(sc["category"])
        if len(seen_cats) == 4:
            break

    report = RunReport()
    for sc in pilot:
        result = _run_scenario(sc, tmp_path)
        report.results.append(result)
        print(f"\n  {sc['id']:50s} → {result.verdict.value}")

    print(f"\nPilot: {report.summary()}")
    # Pilot doesn't enforce the gate — it validates mechanics
    assert report.n_total == 4
    # If pilot is degenerate-heavy, the harness has a wiring issue
    assert report.degenerate_rate < 0.75, (
        "pilot mostly degenerate — harness probably broken"
    )
