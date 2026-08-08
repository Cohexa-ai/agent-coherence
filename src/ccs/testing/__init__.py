# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""In-repo deterministic substrate fakes and (soon) the packaged conformance corpus.

``ccs.testing`` ships INSIDE the package (``where = ["src"]``) so a foreign
implementation can import the corpus and the fakes without vendoring the test
tree. Registered as an ``interface``-layer namespace in
``ccs.hardening.architecture`` — the corpus imports adapters.

Current members:

- :mod:`ccs.testing.s3_local` — the deterministic S3-semantics fake
  (versioning, conditional writes, delete markers, legal hold) that
  ``CoherentObject`` runs against via its client-injection seam.
"""
