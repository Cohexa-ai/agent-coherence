# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""In-repo deterministic substrate fakes and the packaged conformance corpus.

``ccs.testing`` ships INSIDE the package (``where = ["src"]``) so a foreign
implementation can import the corpus and the fakes without vendoring the test
tree. Registered as an ``interface``-layer namespace in
``ccs.hardening.architecture`` — the corpus imports adapters.

Current members:

- :mod:`ccs.testing.s3_local` — the deterministic S3-semantics fake
  (versioning, conditional writes, delete markers, legal hold) that
  ``CoherentObject`` runs against via its client-injection seam.
- :mod:`ccs.testing.substrate_conformance` — the conformance corpus: the
  tier-honesty kit (``ConformanceBinding``, the in-memory fake, the
  split-comparand teeth) plus the Workspace-Versioning family
  (``WorkspaceConformanceBinding``, MUST-MATCH/DECLARED scenarios, the
  ``LocalWorkspaceBinding`` reference arm, the torn-cut-blind teeth stub).
  The in-repo runner under ``tests/conformance/`` imports FROM here through a
  thin shim; the corpus never imports from the test tree.

The corpus imports and runs WITHOUT pytest (its raise-expectations ride an
internal shim); only scenario SKIP reporting requires it — install via the
``conformance`` extra: ``pip install 'agent-coherence[conformance]'``.
"""
