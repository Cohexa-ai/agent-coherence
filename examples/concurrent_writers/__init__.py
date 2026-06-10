# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""Concurrent lost-update demo for the v0.9.1 commit-CAS write path (Epic Piece #6).

Where ``examples/coherent_volume`` shows a *sequential* stale-overwrite (rung 1 —
invalidation-deny + reacquire), this shows the *concurrent* case (rung 2): two
threads race the same shared total, and ``CoherentVolume.write_cas`` (the
optimistic commit-CAS path) elects one winner while the loser is told it lost
(typed conflict) and re-applies on the fresh value — so both updates survive.

``broken.py`` reproduces the lost update under a true race over plain files;
``fixed.py`` runs the identical race through ``write_cas``. Run both side by side
with ``python -m examples.concurrent_writers.main``.

Scope: single-host (loopback coordinator + SQLite-WAL). Cross-host concurrency is
the demand-gated follow-on (roadmap § Epic — Cross-Host Concurrency, Pieces #3–#5).
"""
