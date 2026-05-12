# refactor-demo

Two scripted sub-agents (planner + executor) collaborate on a shared task
spec through `CCSStore`. The planner re-plans mid-flight (writes v2 with a
fourth caller); the executor's behavior depends on whether write-side
coherence is on. Real `tsc` against a real TypeScript fixture turns the
correctness question into an on-screen build error.

Companion to the write-side-coherence positioning post (origin:
`docs/brainstorms/2026-05-12-cocoindex-positioning-coding-agent-wedge-requirements.md`).

## Prerequisites

- Python 3.11+ with `agent-coherence` installed in editable mode (see
  repo `README.md`).
- Node.js ≥18 and `npm` for the TypeScript fixture. The TS toolchain is
  not in CI — `tsc` is invoked locally only.

One-time fixture setup:

    cd examples/refactor_demo/fixture_repo_ts
    npm install
    npx tsc --noEmit          # should pass on the as-checked-in fixture

## Run

From the repo root:

    python -m examples.refactor_demo.main                     # --variant=with (default)
    python -m examples.refactor_demo.main --variant=no-invalidation
    python -m examples.refactor_demo.main --variant=context-cache

Each run copies the TS fixture to a fresh temp directory, runs the
LangGraph graph, invokes `npx tsc --noEmit` against the temp copy, and
prints the result. The source `fixture_repo_ts/` is never mutated.

## Variants

| Variant | Mechanism | What you should see |
| --- | --- | --- |
| `with` | Default `CCSStore`, lazy strategy. Executor reads v1 (cache SHARED), planner writes v2 (executor cache INVALID), executor re-reads at commit → fetches v2. | `cache_hit=False` on commit-time get; `committed spec v2`; `tsc: OK` |
| `no-invalidation` | `disable_invalidation(store)` patches the event bus' `publish_invalidation` to a no-op. Executor cache stays SHARED at v1; commit-time re-read returns cached v1. | `cache_hit=True` on commit-time get; `committed spec v1`; `tsc: FAIL` with `TS2305` on `src/utils/session.ts(4,10)` |
| `context-cache` | Executor never re-reads from the store. Commits from the v1 spec it captured at read time, mirroring LLM context-window behavior. | Only one executor `get` in the event stream; `committed spec v1`; `tsc: FAIL` with `TS2305` |

The protocol-level proof (the `no-invalidation` variant) is identical to
the with-coherence path except for a single line of bus suppression —
`strategies.disable_invalidation(store)`. The application-level
consequence (`tsc` failure on the missing caller) is the same as
`context-cache`, which is the audience-facing demo narrative.

## Tests

The behavior is asserted by `tests/test_refactor_demo.py`. Cache-state
and event-stream assertions cover all three variants; real-`tsc` tests
are gated on the Node toolchain being present.

    pytest tests/test_refactor_demo.py -v
