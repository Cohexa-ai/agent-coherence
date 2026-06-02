# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""Q6 consistency probe (Phase 0 / Unit 1) — gate for the OpenAI/Mistral demo.

Measures, per vendor, whether two clients on the same ``conversation_id`` can
observe inconsistent views: client A appends a versioned item, awaits the server
ACK, then a *separate* client B (no client-side cache) reads. If B does not yet
see A's ACK'd write, that is an observed cross-client stale read.

Epistemic honesty (per the plan's adversarial review): N trials can *falsify*
consistency by observing a stale read, but can never *prove* strong consistency
— absence of an observed stale read within N trials and a short window is
reported as ``no_staleness_observed``, NOT "strongly consistent".

Layering: the pure classification layer (``TrialResult``, ``VendorVerdict``,
``summarize_trials``, ``_percentile``) imports no third-party SDK and is unit
tested offline. The live layer (``run_openai_trials``, ``run_mistral_trials``)
defers ``openai`` / ``mistralai`` imports into function bodies so a bare
``[dev]`` install can still import this module and exercise the pure layer.

Security: keys are read only from the environment; the emitted verdict contains
no credentials and no raw conversation content — only hashes are compared
in-memory and only counts/latencies are persisted.

Run:
    python -m examples.conversations_stale_read.probe --vendor both --trials 100
Requires ``OPENAI_API_KEY`` and/or ``MISTRAL_API_KEY`` in the environment and
``pip install "agent-coherence[openai,mistral]"``.
"""

from __future__ import annotations

import argparse
import dataclasses
import hashlib
import json
import os
import re
import sys
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Callable, Protocol


class _Clock(Protocol):
    """Minimal clock surface used by ``_measure_convergence`` (real ``time`` or a fake)."""

    def monotonic(self) -> float: ...

    def sleep(self, seconds: float) -> None: ...


# Redacts vendor key-like tokens (e.g. ``sk-proj-...``) from any error string before
# it is printed or persisted, so a stray auth-error body can't echo key material.
_KEY_TOKEN = re.compile(r"\b(sk-[A-Za-z0-9_\-]{6,}|[A-Za-z0-9_\-]{0,4}\*{2,}[A-Za-z0-9_\-]{2,})\b")


def _scrub(message: str) -> str:
    return _KEY_TOKEN.sub("<redacted>", message)

# --- Verdict vocabulary (epistemically honest) -----------------------------

STALE_READS_OBSERVED = "stale_reads_observed"  # pathology reproduces → proceed
NO_STALENESS_OBSERVED = "no_staleness_observed"  # failed to falsify → pivot consideration
NO_TRIALS = "no_trials"  # nothing measured (empty input)
SKIPPED = "skipped"  # key missing for this vendor
ERROR = "error"  # vendor runner failed before producing any trial

SCHEMA_VERSION = "ccs.q6probe.v1"

# Convergence polling bounds for the live layer (ms).
_POLL_INTERVAL_MS = 50
_POLL_TIMEOUT_MS = 5_000


# --- Pure classification layer (offline-testable, SDK-free) ----------------


@dataclass(frozen=True)
class TrialResult:
    """One write-then-cross-client-read measurement.

    ``observed_stale`` is True when client B did not see A's ACK'd write on its
    first read. ``convergence_latency_ms`` is how long until B converged (only
    meaningful when ``observed_stale``; ``None`` otherwise or if it never
    converged within the poll timeout).
    """

    observed_stale: bool
    convergence_latency_ms: float | None = None


@dataclass(frozen=True)
class VendorVerdict:
    """Per-vendor summary. ``to_dict`` feeds the machine-readable verdict file."""

    vendor: str
    trials: int
    observed_stale_count: int
    p50_latency_ms: float | None
    p99_latency_ms: float | None
    verdict: str
    skipped: bool = False
    skip_reason: str | None = None
    error: str | None = None  # set on total failure, or a "partial: N/M" note

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def _percentile(values: list[float], pct: float) -> float | None:
    """Nearest-rank percentile. ``None`` for an empty list. Pure."""
    if not values:
        return None
    if not 0.0 <= pct <= 100.0:
        raise ValueError(f"pct must be in [0, 100], got {pct}")
    ordered = sorted(values)
    # Nearest-rank: rank = ceil(pct/100 * N), 1-indexed.
    rank = max(1, -(-int(len(ordered) * pct) // 100)) if pct > 0 else 1
    rank = min(rank, len(ordered))
    return ordered[rank - 1]


def summarize_trials(vendor: str, trials: list[TrialResult]) -> VendorVerdict:
    """Map raw trial data to a per-vendor verdict. Pure — no I/O, no SDK.

    Verdict rule:
      - >= 1 observed stale read  -> STALE_READS_OBSERVED (pathology reproduces)
      - 0 observed stale reads    -> NO_STALENESS_OBSERVED (cannot prove
        consistency; only failed to falsify within N trials)
      - empty trial list          -> NO_TRIALS
    """
    if not trials:
        return VendorVerdict(
            vendor=vendor,
            trials=0,
            observed_stale_count=0,
            p50_latency_ms=None,
            p99_latency_ms=None,
            verdict=NO_TRIALS,
        )

    stale = [t for t in trials if t.observed_stale]
    latencies = [t.convergence_latency_ms for t in stale if t.convergence_latency_ms is not None]
    verdict = STALE_READS_OBSERVED if stale else NO_STALENESS_OBSERVED
    return VendorVerdict(
        vendor=vendor,
        trials=len(trials),
        observed_stale_count=len(stale),
        p50_latency_ms=_percentile(latencies, 50.0),
        p99_latency_ms=_percentile(latencies, 99.0),
        verdict=verdict,
    )


def gate_decision(verdicts: list[VendorVerdict]) -> str:
    """Roll up per-vendor verdicts to the Phase-0 gate decision. Pure.

    'proceed' if any vendor that produced a real measurement reproduced the
    pathology; otherwise 'pivot' (consider the Session-cache layer per the
    plan's Q6 pivot table). Skipped and errored vendors are not evidence.
    """
    measured = [v for v in verdicts if v.verdict in (STALE_READS_OBSERVED, NO_STALENESS_OBSERVED)]
    if not measured:
        return "pivot"
    return "proceed" if any(v.verdict == STALE_READS_OBSERVED for v in measured) else "pivot"


# --- Live measurement layer (deferred SDK imports) -------------------------


def _hash_content(payload: str) -> str:
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


_RECENT_WINDOW = 8  # how many most-recent messages B inspects for A's payload


def _run_trials(
    n_trials: int,
    delay_ms: int,
    append_and_read: Callable[[int], TrialResult],
) -> list[TrialResult]:
    """Drive ``n_trials`` of one vendor, preserving partial results on failure.

    A per-trial exception (e.g., HTTP 429 token-rate-limit) stops the loop and
    returns whatever completed, so one vendor's rate limit never discards
    another vendor's data nor crashes the whole probe.
    """
    results: list[TrialResult] = []
    for i in range(n_trials):
        try:
            results.append(append_and_read(i))
        except Exception as exc:  # noqa: BLE001 — partial-result preservation is the point
            print(f"  trial {i} stopped: {type(exc).__name__}: {_scrub(str(exc))}", file=sys.stderr)
            break
        if delay_ms:
            time.sleep(delay_ms / 1000.0)
    return results


def run_openai_trials(n_trials: int, delay_ms: int = 0) -> list[TrialResult]:
    """Q6a: measure OpenAI ``client.conversations`` cross-client consistency.

    OpenAI Conversations is *passive* state: ``items.create`` stores the exact
    item; a separate-client ``items.list`` should return it. Stale = B's read
    does not yet contain A's just-ACK'd payload. Verified live (SDK 2.x).
    """
    from openai import OpenAI  # deferred (third-party SDK; keep out of module-load path)

    writer = OpenAI()
    reader = OpenAI()  # separate client instance — no shared client-side cache
    conversation = writer.conversations.create()

    def append_and_read(i: int) -> TrialResult:
        payload = f"q6a-trial-{i}-{_hash_content(str(i))[:8]}"
        expected = _hash_content(payload)
        writer.conversations.items.create(
            conversation.id,
            items=[{"type": "message", "role": "user", "content": payload}],
        )  # await ACK
        return _measure_convergence(lambda: _openai_recent_hashes(reader, conversation.id), expected, time)

    return _run_trials(n_trials, delay_ms, append_and_read)


def _openai_recent_hashes(reader: Any, conversation_id: str) -> set[str]:
    page = reader.conversations.items.list(conversation_id, order="desc", limit=_RECENT_WINDOW)
    return {_hash_content(t) for t in (_extract_text(item) for item in page) if t is not None}


def run_mistral_trials(n_trials: int, delay_ms: int = 0) -> list[TrialResult]:
    """Q6b: measure Mistral ``client.beta.conversations`` cross-client consistency.

    Mistral's ``start``/``append`` *actively run a completion* (vs OpenAI's
    passive state), so each append yields [user:payload, assistant:reply] and the
    latest message is the assistant reply — NOT A's input. We therefore test
    membership: did B's read of the recent window contain A's user payload at
    all. This is the active-vs-passive asymmetry the capability matrix records.

    Because every append spends completion tokens, this vendor is token-rate
    limited (HTTP 429); use a small ``delay_ms`` and a lower trial count. The
    harness preserves partial results if the limit is still hit.
    """
    from mistralai.client import Mistral  # deferred (2.4.x is a namespace pkg; class lives in .client)

    api_key = os.environ["MISTRAL_API_KEY"]
    writer = Mistral(api_key=api_key)
    reader = Mistral(api_key=api_key)
    conversation = writer.beta.conversations.start(model="mistral-small-latest", inputs="q6b-init")
    cid = conversation.conversation_id

    def append_and_read(i: int) -> TrialResult:
        payload = f"q6b-trial-{i}-{_hash_content(str(i))[:8]}"
        expected = _hash_content(payload)
        writer.beta.conversations.append(conversation_id=cid, inputs=payload)  # await ACK (runs a completion)
        return _measure_convergence(lambda: _mistral_recent_hashes(reader, cid), expected, time)

    return _run_trials(n_trials, delay_ms, append_and_read)


def _mistral_recent_hashes(reader: Any, conversation_id: str) -> set[str]:
    msgs = reader.beta.conversations.get_messages(conversation_id=conversation_id)
    messages = getattr(msgs, "messages", None) or []
    return {
        _hash_content(t)
        for t in (_extract_text(entry) for entry in messages[-_RECENT_WINDOW:])
        if t is not None
    }


def _extract_text(item: object) -> str | None:
    """Best-effort text extraction from a vendor item/entry (shapes verified live)."""
    content = getattr(item, "content", None)
    if isinstance(content, str):
        return content
    if isinstance(content, list) and content:
        first = content[0]
        text = getattr(first, "text", None)
        if isinstance(text, str):
            return text
    return None


def _measure_convergence(read_hashes: Callable[[], set[str]], expected: str, clock: _Clock) -> TrialResult:
    """Read once; if B has not seen A's payload, poll to convergence or timeout.

    ``read_hashes`` returns the hash set of B's recent-window message contents;
    ``expected`` is the hash of A's just-ACK'd payload. Membership-based so it is
    correct for both passive (OpenAI) and active-completion (Mistral) vendors.
    """
    if expected in read_hashes():
        return TrialResult(observed_stale=False)
    start = clock.monotonic()
    deadline = start + _POLL_TIMEOUT_MS / 1000.0
    while clock.monotonic() < deadline:
        clock.sleep(_POLL_INTERVAL_MS / 1000.0)
        if expected in read_hashes():
            return TrialResult(observed_stale=True, convergence_latency_ms=(clock.monotonic() - start) * 1000.0)
    return TrialResult(observed_stale=True, convergence_latency_ms=None)  # did not converge in window


# --- Orchestration ---------------------------------------------------------

_RUNNERS: dict[str, tuple[str, Callable[[int, int], list[TrialResult]]]] = {
    "openai": ("OPENAI_API_KEY", run_openai_trials),
    "mistral": ("MISTRAL_API_KEY", run_mistral_trials),
}


def run_probe(vendors: list[str], n_trials: int, delay_ms: int = 0) -> dict[str, object]:
    """Run the probe for the requested vendors.

    Each vendor is isolated: a missing key skips it, and a runner exception
    records an ``error`` verdict without discarding other vendors' results or
    crashing the probe. A runner that returns fewer than ``n_trials`` results
    (partial, e.g. a mid-run rate limit) is summarized over what completed with
    a partial note attached.
    """
    verdicts: list[VendorVerdict] = []
    for vendor in vendors:
        env_var, runner = _RUNNERS[vendor]
        if not os.environ.get(env_var):
            verdicts.append(
                VendorVerdict(vendor, 0, 0, None, None, SKIPPED, skipped=True, skip_reason=f"{env_var} not set")
            )
            continue
        try:
            results = runner(n_trials, delay_ms)
        except Exception as exc:  # noqa: BLE001 — isolate one vendor's setup failure
            verdicts.append(
                VendorVerdict(vendor, 0, 0, None, None, ERROR, error=f"{type(exc).__name__}: {_scrub(str(exc))}")
            )
            continue
        verdict = summarize_trials(vendor, results)
        if len(results) < n_trials:
            # Covers a partial run AND a zero-result run (first trial failed): the
            # verdict carries an explicit note instead of looking like "never asked".
            verdict = dataclasses.replace(verdict, error=f"partial: {len(results)}/{n_trials} trials before stop")
        verdicts.append(verdict)
    return {
        "schema": SCHEMA_VERSION,
        "n_trials": n_trials,
        "vendors": {v.vendor: v.to_dict() for v in verdicts},
        "gate": gate_decision(verdicts),
    }


def _format_summary(report: dict[str, object]) -> str:
    lines = [f"Q6 consistency probe ({report['schema']}, n={report['n_trials']})", ""]
    for vendor, v in report["vendors"].items():  # type: ignore[union-attr]
        if v["skipped"]:
            lines.append(f"  {vendor:8s} SKIPPED — {v['skip_reason']}")
        elif v["verdict"] == ERROR:
            lines.append(f"  {vendor:8s} ERROR — {v['error']}")
        else:
            lat = f"p50={v['p50_latency_ms']}ms p99={v['p99_latency_ms']}ms" if v["observed_stale_count"] else "n/a"
            note = f"  [{v['error']}]" if v["error"] else ""
            lines.append(
                f"  {vendor:8s} {v['verdict']:22s} stale={v['observed_stale_count']}/{v['trials']} {lat}{note}"
            )
    lines += ["", f"  GATE: {report['gate']}  ('proceed' = pathology reproduces; 'pivot' = Session-cache layer)"]
    return "\n".join(lines)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Q6 OpenAI/Mistral Conversations consistency probe.")
    parser.add_argument("--vendor", choices=("openai", "mistral", "both"), default="both")
    parser.add_argument("--trials", type=int, default=100)
    parser.add_argument(
        "--delay-ms",
        type=int,
        default=0,
        help="Inter-trial delay (ms). Use for token-rate-limited vendors like Mistral.",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=Path(__file__).resolve().parent / "q6_verdict.json",
        help="Path for the machine-readable verdict (git-ignored).",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    vendors = ["openai", "mistral"] if args.vendor == "both" else [args.vendor]

    missing = [v for v in vendors if not os.environ.get(_RUNNERS[v][0])]
    if len(missing) == len(vendors):
        names = ", ".join(_RUNNERS[v][0] for v in vendors)
        print(f"No probe ran — set at least one of: {names}")
        return 2

    report = run_probe(vendors, args.trials, args.delay_ms)
    args.out.write_text(json.dumps(report, indent=2, sort_keys=True))
    print(_format_summary(report))
    print(f"\nVerdict written to {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

# Comparing notes on multi-agent coherence?
# https://github.com/hipvlady/agent-coherence/discussions
