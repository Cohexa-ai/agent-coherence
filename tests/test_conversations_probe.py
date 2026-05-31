# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""Offline tests for the Q6 consistency probe's pure classification layer.

These exercise the SDK-free logic (`summarize_trials`, `gate_decision`,
`_percentile`, `run_probe`/`main` skip paths) without any network call or
third-party SDK, so they run on a bare ``[dev]`` install in the default
``pytest -q`` loop. The live measurement layer is covered separately by a
`live_api`-marked smoke test.
"""

from __future__ import annotations

import json

import pytest

from examples.conversations_stale_read.probe import (
    NO_STALENESS_OBSERVED,
    NO_TRIALS,
    SKIPPED,
    STALE_READS_OBSERVED,
    TrialResult,
    VendorVerdict,
    _percentile,
    gate_decision,
    main,
    run_probe,
    summarize_trials,
)


# --- summarize_trials ------------------------------------------------------


def test_summarize_trials_with_stale_reads_returns_stale_observed():
    trials = [
        TrialResult(observed_stale=False),
        TrialResult(observed_stale=True, convergence_latency_ms=120.0),
        TrialResult(observed_stale=True, convergence_latency_ms=80.0),
    ]
    verdict = summarize_trials("openai", trials)
    assert verdict.verdict == STALE_READS_OBSERVED
    assert verdict.observed_stale_count == 2
    assert verdict.trials == 3
    assert verdict.p50_latency_ms in (80.0, 120.0)  # nearest-rank over [80, 120]
    assert verdict.p99_latency_ms == 120.0


def test_summarize_trials_all_consistent_returns_no_staleness_observed():
    trials = [TrialResult(observed_stale=False) for _ in range(50)]
    verdict = summarize_trials("openai", trials)
    # Epistemic honesty: zero observed stale reads is NOT "strongly consistent".
    assert verdict.verdict == NO_STALENESS_OBSERVED
    assert verdict.observed_stale_count == 0
    assert verdict.p50_latency_ms is None
    assert verdict.p99_latency_ms is None


def test_summarize_trials_empty_returns_no_trials():
    verdict = summarize_trials("mistral", [])
    assert verdict.verdict == NO_TRIALS
    assert verdict.trials == 0
    assert verdict.observed_stale_count == 0


def test_summarize_trials_ignores_none_latencies_in_percentiles():
    # A stale trial that never converged contributes to the count but not latency.
    trials = [
        TrialResult(observed_stale=True, convergence_latency_ms=None),
        TrialResult(observed_stale=True, convergence_latency_ms=200.0),
    ]
    verdict = summarize_trials("openai", trials)
    assert verdict.observed_stale_count == 2
    assert verdict.p50_latency_ms == 200.0
    assert verdict.p99_latency_ms == 200.0


# --- _percentile -----------------------------------------------------------


def test_percentile_empty_returns_none():
    assert _percentile([], 50.0) is None


def test_percentile_nearest_rank():
    values = [10.0, 20.0, 30.0, 40.0, 50.0]
    assert _percentile(values, 50.0) == 30.0
    assert _percentile(values, 100.0) == 50.0


def test_percentile_rejects_out_of_range():
    with pytest.raises(ValueError):
        _percentile([1.0], 150.0)


# --- gate_decision ---------------------------------------------------------


def test_gate_proceed_when_any_vendor_reproduces():
    verdicts = [
        VendorVerdict("openai", 100, 3, 50.0, 90.0, STALE_READS_OBSERVED),
        VendorVerdict("mistral", 100, 0, None, None, NO_STALENESS_OBSERVED),
    ]
    assert gate_decision(verdicts) == "proceed"


def test_gate_pivot_when_no_vendor_reproduces():
    verdicts = [
        VendorVerdict("openai", 100, 0, None, None, NO_STALENESS_OBSERVED),
        VendorVerdict("mistral", 100, 0, None, None, NO_STALENESS_OBSERVED),
    ]
    assert gate_decision(verdicts) == "pivot"


def test_gate_pivot_when_all_skipped():
    verdicts = [
        VendorVerdict("openai", 0, 0, None, None, SKIPPED, skipped=True, skip_reason="OPENAI_API_KEY not set"),
    ]
    assert gate_decision(verdicts) == "pivot"


# --- run_probe / main skip paths (no SDK import when key absent) ------------


def test_run_probe_skips_vendor_without_key(monkeypatch):
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("MISTRAL_API_KEY", raising=False)
    report = run_probe(["openai", "mistral"], n_trials=10)
    assert report["gate"] == "pivot"
    assert report["vendors"]["openai"]["verdict"] == SKIPPED
    assert report["vendors"]["openai"]["skip_reason"] == "OPENAI_API_KEY not set"
    assert report["vendors"]["mistral"]["verdict"] == SKIPPED


def test_run_probe_report_is_json_serializable(monkeypatch):
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("MISTRAL_API_KEY", raising=False)
    report = run_probe(["openai"], n_trials=10)
    encoded = json.dumps(report)  # must not raise
    assert "schema" in json.loads(encoded)


def test_main_returns_2_when_no_keys(monkeypatch, capsys):
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("MISTRAL_API_KEY", raising=False)
    code = main(["--vendor", "both", "--trials", "5"])
    assert code == 2
    out = capsys.readouterr().out
    assert "OPENAI_API_KEY" in out and "MISTRAL_API_KEY" in out


def test_main_single_vendor_without_key_returns_2(monkeypatch):
    # Requesting only mistral with no MISTRAL_API_KEY = all requested vendors
    # missing = nothing measured = exit 2 (clear, not a silent success).
    monkeypatch.delenv("MISTRAL_API_KEY", raising=False)
    code = main(["--vendor", "mistral", "--trials", "5"])
    assert code == 2


def test_main_writes_verdict_with_injected_runner(monkeypatch, tmp_path):
    # Inject a fake runner so the orchestration + verdict-file-writing path is
    # exercised offline (no SDK, no network). Proves a real run exits 0 and
    # persists a verdict whose gate reflects the (synthetic) stale observation.
    from examples.conversations_stale_read import probe as probe_mod

    def fake_openai_runner(n_trials: int) -> list[TrialResult]:
        return [TrialResult(observed_stale=True, convergence_latency_ms=75.0) for _ in range(n_trials)]

    monkeypatch.setitem(probe_mod._RUNNERS, "openai", ("OPENAI_API_KEY", fake_openai_runner))
    monkeypatch.setenv("OPENAI_API_KEY", "sentinel-routed-to-fake-runner")
    out = tmp_path / "verdict.json"
    code = main(["--vendor", "openai", "--trials", "5", "--out", str(out)])
    assert code == 0
    written = json.loads(out.read_text())
    assert written["vendors"]["openai"]["verdict"] == STALE_READS_OBSERVED
    assert written["vendors"]["openai"]["observed_stale_count"] == 5
    assert written["gate"] == "proceed"
