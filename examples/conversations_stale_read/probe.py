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
import hashlib
import json
import os
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Callable

# --- Verdict vocabulary (epistemically honest) -----------------------------

STALE_READS_OBSERVED = "stale_reads_observed"  # pathology reproduces → proceed
NO_STALENESS_OBSERVED = "no_staleness_observed"  # failed to falsify → pivot consideration
NO_TRIALS = "no_trials"  # nothing measured (empty input)
SKIPPED = "skipped"  # key missing for this vendor

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

    'proceed' if any non-skipped vendor reproduced the pathology; otherwise
    'pivot' (consider the Session-cache layer per the plan's Q6 pivot table).
    """
    measured = [v for v in verdicts if not v.skipped]
    if not measured:
        return "pivot"
    return "proceed" if any(v.verdict == STALE_READS_OBSERVED for v in measured) else "pivot"


# --- Live measurement layer (deferred SDK imports) -------------------------


def _hash_content(payload: str) -> str:
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def run_openai_trials(n_trials: int) -> list[TrialResult]:
    """Q6a: measure OpenAI ``client.conversations`` cross-client consistency.

    Deferred imports keep the module SDK-free at collection time. Written to the
    documented 2.x ``client.conversations`` / ``client.conversations.items``
    shape; exact field access is verified on the first live run (SDK is 2.x).
    """
    import time

    from openai import OpenAI  # deferred

    writer = OpenAI()
    reader = OpenAI()  # separate client instance — no shared client-side cache
    conversation = writer.conversations.create()
    results: list[TrialResult] = []
    for i in range(n_trials):
        payload = f"q6a-trial-{i}-{_hash_content(str(i))[:8]}"
        expected = _hash_content(payload)
        writer.conversations.items.create(
            conversation.id,
            items=[{"type": "message", "role": "user", "content": payload}],
        )  # await ACK
        results.append(_measure_convergence(lambda: _latest_openai_hash(reader, conversation.id), expected, time))
    return results


def _latest_openai_hash(reader: object, conversation_id: str) -> str | None:
    page = reader.conversations.items.list(conversation_id, order="desc", limit=1)  # type: ignore[attr-defined]
    items = list(page)
    if not items:
        return None
    item = items[0]
    text = _extract_text(item)
    return _hash_content(text) if text is not None else None


def run_mistral_trials(n_trials: int) -> list[TrialResult]:
    """Q6b: measure Mistral ``client.beta.conversations`` cross-client consistency.

    Mistral's ``start()`` actively runs a completion (vs OpenAI's passive state),
    so the harness shares the *interface* (write-then-cross-client-read) but not
    one code path — see the capability matrix in README.md. Field access verified
    on first live run (SDK is 2.x, beta surface).
    """
    import time

    from mistralai import Mistral  # deferred

    api_key = os.environ["MISTRAL_API_KEY"]
    writer = Mistral(api_key=api_key)
    reader = Mistral(api_key=api_key)
    conversation = writer.beta.conversations.start(model="mistral-small-latest", inputs="q6b-init")
    results: list[TrialResult] = []
    for i in range(n_trials):
        payload = f"q6b-trial-{i}-{_hash_content(str(i))[:8]}"
        expected = _hash_content(payload)
        writer.beta.conversations.append(conversation_id=conversation.conversation_id, inputs=payload)  # await ACK
        results.append(
            _measure_convergence(
                lambda: _latest_mistral_hash(reader, conversation.conversation_id), expected, time
            )
        )
    return results


def _latest_mistral_hash(reader: object, conversation_id: str) -> str | None:
    convo = reader.beta.conversations.get(conversation_id=conversation_id)  # type: ignore[attr-defined]
    entries = getattr(convo, "entries", None) or getattr(convo, "outputs", None) or []
    if not entries:
        return None
    text = _extract_text(entries[-1])
    return _hash_content(text) if text is not None else None


def _extract_text(item: object) -> str | None:
    """Best-effort text extraction from a vendor item/entry (shape verified live)."""
    content = getattr(item, "content", None)
    if isinstance(content, str):
        return content
    if isinstance(content, list) and content:
        first = content[0]
        text = getattr(first, "text", None)
        if isinstance(text, str):
            return text
    return None


def _measure_convergence(read_latest_hash: Callable[[], str | None], expected: str, time_mod: object) -> TrialResult:
    """Read once; if stale, poll until convergence or timeout. Records latency."""
    first = read_latest_hash()
    if first == expected:
        return TrialResult(observed_stale=False)
    # Stale on first read — poll to convergence.
    start = time_mod.monotonic()  # type: ignore[attr-defined]
    deadline = start + _POLL_TIMEOUT_MS / 1000.0
    while time_mod.monotonic() < deadline:  # type: ignore[attr-defined]
        time_mod.sleep(_POLL_INTERVAL_MS / 1000.0)  # type: ignore[attr-defined]
        if read_latest_hash() == expected:
            return TrialResult(observed_stale=True, convergence_latency_ms=(time_mod.monotonic() - start) * 1000.0)  # type: ignore[attr-defined]
    return TrialResult(observed_stale=True, convergence_latency_ms=None)  # did not converge in window


# --- Orchestration ---------------------------------------------------------

_RUNNERS: dict[str, tuple[str, Callable[[int], list[TrialResult]]]] = {
    "openai": ("OPENAI_API_KEY", run_openai_trials),
    "mistral": ("MISTRAL_API_KEY", run_mistral_trials),
}


def run_probe(vendors: list[str], n_trials: int) -> dict[str, object]:
    """Run the probe for the requested vendors. Skips a vendor with no key."""
    verdicts: list[VendorVerdict] = []
    for vendor in vendors:
        env_var, runner = _RUNNERS[vendor]
        if not os.environ.get(env_var):
            verdicts.append(
                VendorVerdict(
                    vendor=vendor,
                    trials=0,
                    observed_stale_count=0,
                    p50_latency_ms=None,
                    p99_latency_ms=None,
                    verdict=SKIPPED,
                    skipped=True,
                    skip_reason=f"{env_var} not set",
                )
            )
            continue
        verdicts.append(summarize_trials(vendor, runner(n_trials)))
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
        else:
            lat = f"p50={v['p50_latency_ms']}ms p99={v['p99_latency_ms']}ms" if v["observed_stale_count"] else "n/a"
            lines.append(
                f"  {vendor:8s} {v['verdict']:22s} stale={v['observed_stale_count']}/{v['trials']} {lat}"
            )
    lines += ["", f"  GATE: {report['gate']}  ('proceed' = pathology reproduces; 'pivot' = Session-cache layer)"]
    return "\n".join(lines)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Q6 OpenAI/Mistral Conversations consistency probe.")
    parser.add_argument("--vendor", choices=("openai", "mistral", "both"), default="both")
    parser.add_argument("--trials", type=int, default=100)
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

    report = run_probe(vendors, args.trials)
    args.out.write_text(json.dumps(report, indent=2, sort_keys=True))
    print(_format_summary(report))
    print(f"\nVerdict written to {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

# Comparing notes on multi-agent coherence?
# https://github.com/hipvlady/agent-coherence/discussions
