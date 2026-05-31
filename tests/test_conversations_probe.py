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
    _measure_convergence,
    _percentile,
    gate_decision,
    main,
    run_probe,
    summarize_trials,
)


class _FakeTime:
    """Deterministic monotonic clock; sleep advances virtual time, no real wait."""

    def __init__(self) -> None:
        self.t = 0.0

    def monotonic(self) -> float:
        return self.t

    def sleep(self, seconds: float) -> None:
        self.t += seconds


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


# --- _measure_convergence (membership-based, pure with injected clock) ------


def test_measure_convergence_not_stale_when_payload_present_immediately():
    result = _measure_convergence(lambda: {"abc", "def"}, "abc", _FakeTime())
    assert result.observed_stale is False
    assert result.convergence_latency_ms is None


def test_measure_convergence_stale_then_converges_records_latency():
    calls = {"n": 0}

    def read_hashes() -> set[str]:
        calls["n"] += 1
        return {"target"} if calls["n"] >= 3 else set()  # absent on reads 1-2, present on 3

    result = _measure_convergence(read_hashes, "target", _FakeTime())
    assert result.observed_stale is True
    assert result.convergence_latency_ms is not None and result.convergence_latency_ms > 0


def test_measure_convergence_never_converges_returns_none_latency():
    result = _measure_convergence(lambda: {"other"}, "target", _FakeTime())
    assert result.observed_stale is True
    assert result.convergence_latency_ms is None  # timed out without seeing payload


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

    def fake_openai_runner(n_trials: int, delay_ms: int = 0) -> list[TrialResult]:
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


def test_run_probe_isolates_a_vendor_runner_failure(monkeypatch):
    # One vendor raising must not discard the other vendor's results nor crash.
    from examples.conversations_stale_read import probe as probe_mod

    def good_runner(n_trials: int, delay_ms: int = 0) -> list[TrialResult]:
        return [TrialResult(observed_stale=False) for _ in range(n_trials)]

    def boom_runner(n_trials: int, delay_ms: int = 0) -> list[TrialResult]:
        raise RuntimeError("conversation create failed")

    monkeypatch.setitem(probe_mod._RUNNERS, "openai", ("OPENAI_API_KEY", good_runner))
    monkeypatch.setitem(probe_mod._RUNNERS, "mistral", ("MISTRAL_API_KEY", boom_runner))
    monkeypatch.setenv("OPENAI_API_KEY", "x")
    monkeypatch.setenv("MISTRAL_API_KEY", "x")
    report = run_probe(["openai", "mistral"], n_trials=4)
    assert report["vendors"]["openai"]["verdict"] == NO_STALENESS_OBSERVED  # preserved
    assert report["vendors"]["mistral"]["verdict"] == "error"
    assert "conversation create failed" in report["vendors"]["mistral"]["error"]
    assert report["gate"] == "pivot"  # errored vendor is not evidence


def test_run_probe_marks_partial_results(monkeypatch):
    # A runner returning fewer than n_trials (mid-run rate limit) is summarized
    # over what completed, with a partial note — not dropped.
    from examples.conversations_stale_read import probe as probe_mod

    def partial_runner(n_trials: int, delay_ms: int = 0) -> list[TrialResult]:
        return [TrialResult(observed_stale=False) for _ in range(3)]  # only 3 of n

    monkeypatch.setitem(probe_mod._RUNNERS, "openai", ("OPENAI_API_KEY", partial_runner))
    monkeypatch.setenv("OPENAI_API_KEY", "x")
    report = run_probe(["openai"], n_trials=100)
    v = report["vendors"]["openai"]
    assert v["trials"] == 3
    assert v["verdict"] == NO_STALENESS_OBSERVED
    assert v["error"] == "partial: 3/100 trials before stop"
