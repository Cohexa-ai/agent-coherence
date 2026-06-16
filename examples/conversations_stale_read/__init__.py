# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""Conversations API stale-read demo + Q6 consistency probe (Phase 0/1).

This package is the multi-vendor (OpenAI + Mistral) Conversations stale-read
artifact described in
``docs/plans/2026-05-31-001-feat-openai-stack-adapter-and-demo-plan.md``.

``probe`` is Phase 0 / Unit 1: an empirical harness that measures each vendor's
read-after-write consistency model (Q6a OpenAI, Q6b Mistral). Its verdict gates
whether the demo proceeds as drawn or pivots to the Session-cache layer.

Third-party SDK imports (``openai``, ``mistralai``, ``httpx``) are deferred into
function bodies so the pure classification logic stays importable on a bare
``[dev]`` install without triggering an ImportError at pytest collection time.
"""
