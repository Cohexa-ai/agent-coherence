# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""RAG stale-memory write-back demo — an agent clobbers a moved memory record.

``broken.py`` reproduces the lost update over a plain memory file: an agent
edits a record from a snapshot a peer already superseded, erasing the peer's
update. ``fixed.py`` shows the same sequence denied (then recovered) through the
CoherentVolume explicit API. Run both side by side with
``python -m examples.rag_stale_memory.main``.
"""
