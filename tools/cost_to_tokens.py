# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""Translate the cost-sweep's ``refetches_avoided`` into token + dollar savings.

The sweep (``tools/run_cost_sweep.py``) measures gating's savings as *re-fetches
avoided* — a proxy. This post-processor maps that proxy onto the headline cost
function the project's positioning rests on: **input-token spend + prompt-cache
preservation**. Every parameter is explicit and labeled; nothing is a hidden
magic number. Two lanes:

  1. Re-injection savings (PRIMARY, directly defensible): each avoided re-fetch
     is ``tokens_per_artifact`` input tokens NOT re-spent. Dollars at the input
     price.

  2. Prompt-cache preservation (OPT-IN, assumption-heavy → default OFF): a
     re-fetched artifact sitting inside a cached prefix invalidates everything
     after it, forcing the downstream suffix to be re-written (cache-write
     premium) instead of read (cache-read discount). Modeled only when you pass
     ``--prefix-tokens-after-artifact`` > 0, because the value depends on the
     caller's prompt structure. Left at 0, the headline is re-injection only.

This is a regime map in dollar units, NOT a field-measured invoice — the savings
scale linearly with ``tokens_per_artifact`` and the input price, both stated.

Reproduce:
    python tools/cost_to_tokens.py \
        --input benchmarks/results/cost_sweep_published.json \
        --output benchmarks/results/cost_tokens_published.json
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]

# Labeled default assumptions (overridable via CLI):
DEFAULT_TOKENS_PER_ARTIFACT = 1000  # a mid-size RAG document chunk / source artifact
DEFAULT_INPUT_PRICE_PER_MTOK = 3.00  # Claude Sonnet 4.6 input, $/Mtok (as of 2026-06)
DEFAULT_CACHE_WRITE_MULT = 1.25  # Anthropic 5-min-TTL prompt-cache write multiplier
DEFAULT_CACHE_READ_MULT = 0.10  # Anthropic prompt-cache read multiplier
DEFAULT_PREFIX_TOKENS_AFTER_ARTIFACT = 0  # cache-preservation lane OFF by default


def _resolve(p: str) -> Path:
    path = Path(p)
    return path if path.is_absolute() else REPO_ROOT / path


def translate(payload: dict[str, Any], *, tokens_per_artifact: int, input_price_per_mtok: float,
              cache_write_mult: float, cache_read_mult: float,
              prefix_tokens_after_artifact: int) -> dict[str, Any]:
    """Map each (sensitivity-invariant) rate row to per-session token + $ savings."""
    price_per_token = input_price_per_mtok / 1_000_000
    cache_delta = max(0.0, cache_write_mult - cache_read_mult)
    rows = []
    for r in payload["rows"]:
        if r["sensitivity"] != 0.0:  # savings is answer-sensitivity-invariant by design
            continue
        avoided = r["refetches_avoided"]
        reinjection_tokens = avoided * tokens_per_artifact
        reinjection_usd = reinjection_tokens * price_per_token
        cache_premium_tokens_equiv = avoided * prefix_tokens_after_artifact * cache_delta
        cache_premium_usd = cache_premium_tokens_equiv * price_per_token
        rows.append({
            "rate": r["rate"],
            "refetches_avoided": round(avoided, 2),
            "reinjection_tokens_saved": round(reinjection_tokens, 1),
            "reinjection_usd_saved": round(reinjection_usd, 5),
            "cache_premium_usd_saved": round(cache_premium_usd, 5),
            "total_usd_saved_per_session": round(reinjection_usd + cache_premium_usd, 5),
        })
    return {
        "translation": "cost-sweep refetches_avoided -> input-token + prompt-cache dollars",
        "provenance": payload.get("provenance"),
        "source_runs_per_point": payload.get("runs_per_point"),
        "assumptions": {
            "tokens_per_artifact": tokens_per_artifact,
            "input_price_per_mtok_usd": input_price_per_mtok,
            "cache_write_multiplier": cache_write_mult,
            "cache_read_multiplier": cache_read_mult,
            "prefix_tokens_after_artifact": prefix_tokens_after_artifact,
            "note": "Savings are PER SESSION and scale linearly with tokens_per_artifact "
                    "and input price. Cache-preservation lane is 0 unless "
                    "prefix_tokens_after_artifact > 0. A regime map in dollar units, not "
                    "a measured invoice.",
        },
        "rows": rows,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Translate cost-sweep savings to token/$ terms.")
    parser.add_argument("--input", default="benchmarks/results/cost_sweep_published.json")
    parser.add_argument("--output", default="benchmarks/results/cost_tokens_published.json")
    parser.add_argument("--tokens-per-artifact", type=int, default=DEFAULT_TOKENS_PER_ARTIFACT)
    parser.add_argument("--input-price-per-mtok", type=float, default=DEFAULT_INPUT_PRICE_PER_MTOK)
    parser.add_argument("--cache-write-multiplier", type=float, default=DEFAULT_CACHE_WRITE_MULT)
    parser.add_argument("--cache-read-multiplier", type=float, default=DEFAULT_CACHE_READ_MULT)
    parser.add_argument("--prefix-tokens-after-artifact", type=int,
                        default=DEFAULT_PREFIX_TOKENS_AFTER_ARTIFACT,
                        help="Cached tokens downstream of a typical artifact (enables the "
                             "prompt-cache-preservation lane). Default 0 = lane off.")
    args = parser.parse_args(argv)

    payload = json.loads(_resolve(args.input).read_text(encoding="utf-8"))
    result = translate(
        payload,
        tokens_per_artifact=args.tokens_per_artifact,
        input_price_per_mtok=args.input_price_per_mtok,
        cache_write_mult=args.cache_write_multiplier,
        cache_read_mult=args.cache_read_multiplier,
        prefix_tokens_after_artifact=args.prefix_tokens_after_artifact,
    )

    a = result["assumptions"]
    print(f"Assumptions: {a['tokens_per_artifact']} tok/artifact @ "
          f"${a['input_price_per_mtok_usd']}/Mtok input; cache lane "
          f"{'on' if a['prefix_tokens_after_artifact'] else 'off'}")
    for row in result["rows"]:
        print(f"  r={row['rate']:>4}: avoided={row['refetches_avoided']:>7.1f} "
              f"tokens_saved={row['reinjection_tokens_saved']:>10.0f} "
              f"$/session={row['total_usd_saved_per_session']:.4f}")

    out = _resolve(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(f"Wrote {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
