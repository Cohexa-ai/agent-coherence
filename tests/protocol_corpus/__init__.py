# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""Cross-implementation protocol corpus.

Plan Unit 7 (v0.2): parametrized fixture-driven harness that POSTs identical
requests to both coordinator implementations (Python in-thread + Node subprocess)
and asserts JSON-equivalent responses after timestamp / UUID / PID / uptime
normalization.

Lives under ``tests/`` so the existing pytest test-discovery + the
``protocol_corpus`` pytest marker (see ``pyproject.toml``) can opt the corpus
into / out of default runs. Architecture checks scan ``src/ccs/`` only; this
package does not change any architectural layering.

See ``tests/protocol_corpus/harness.py`` for the harness implementation and
``tests/protocol_corpus/fixtures/warn_mode/*.json`` for the v0.1.1 warn-mode
wire-shape corpus (Unit 7a). Strict-mode fixtures (Unit 7b) land alongside
Unit 2's deny-branch tests."""
