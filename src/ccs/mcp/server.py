# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""The ``stale-write-guard-fs`` MCP server: a stdio FastMCP binding over CoherentVolume.

**stdio invariant:** NEVER write to stdout — the MCP JSON-RPC stream owns fd 1.
All logging goes to stderr; the coordinator subprocess's stdout is redirected by
``connect_or_spawn``. **Serialization:** access to the single shared volume is
guarded by an ``asyncio.Lock`` and runs on the event-loop thread (no thread
offload), so FastMCP's coroutine dispatch cannot interleave two volume ops — the
volume's A5 thread-guard only sees *different threads*, not coroutine interleave,
so we serialize in code rather than rely on it.

Unit 2 ships the lifecycle skeleton (lifespan + serialization + fail-closed
construction). Tools are registered in Units 4 (read/write/reacquire/status) and
5 (write_cas).
"""

from __future__ import annotations

import asyncio
import logging
import sys
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass

from mcp.server.fastmcp import FastMCP

from ccs.adapters.claude_code.lifecycle import stop_coordinator
from ccs.adapters.coherent_volume import CoherentVolume
from ccs.mcp.session import SessionConfig, build_volume

logger = logging.getLogger(__name__)

SERVER_NAME = "stale-write-guard-fs"

# The honesty ceiling (origin §5 / R5): the server-level instructions, the
# per-tool descriptions (Units 4/5), and the deny ``structuredContent`` together
# bound what may be claimed. Annotations are untrusted hints; this prose is not.
INSTRUCTIONS = """\
stale-write-guard-fs guards a SINGLE-HOST workspace against silent lost updates
when two or more agents share a mutable file. It enforces VERSION LINEAGE via a
local coherence coordinator; it does NOT merge content for you.

Two guarantees, both single-host and fail-closed:
  1. Sequential stale-overwrite — if a peer committed a newer version, swg_write
     is DENIED (reason=stale_view). Recover with swg_reacquire, then write FROM
     the fresh bytes it returns.
  2. Concurrent same-key lost-update — swg_write_cas(path, expected_version,
     new_content) rejects a stale compare-and-set as a TYPED CONFLICT (not an
     auto-merge): you read, merge, and retry; the server never merges for you.

OUT OF GUARANTEE (do not rely on this server for): writers on DIFFERENT hosts or
across a synced/network mount; divergent-history reconciliation; semantic/content
correctness; any server-enforced auto-merge. These are NOT detected in v1 — a
heterogeneous multi-host setup looks identical to a guarded one (swg_status
reports heterogeneous_scope_detectable=false).

TRUST BOUNDARY: the server enforces that your write descends from a version you
read; it CANNOT verify you derived your content from the bytes you read. Same-uid
local processes can reach the coordinator directly, bypassing this server — the
model is single-uid, single-host.
"""


@dataclass
class ServerContext:
    """Lifespan-owned session state shared by every tool.

    ``lock`` serializes access to ``volume`` — every tool acquires it for the
    duration of its volume interaction (see module docstring).
    """

    volume: CoherentVolume
    config: SessionConfig
    lock: asyncio.Lock


@asynccontextmanager
async def lifespan(server: FastMCP) -> AsyncIterator[ServerContext]:
    """Own the coordinator for the server's lifetime.

    Enter → construct the strict-only volume (self-spawns/attaches; raises
    fail-closed if it can't). Exit → ``stop_coordinator``. Does NOT call
    ``connect_or_spawn`` — construction already did, so calling it again would
    double-spawn.
    """
    config = SessionConfig.from_env()
    volume = build_volume(config)  # blocking, fail-closed
    logger.info("stale-write-guard-fs attached: root=%s managed=%s", config.root, config.managed)
    try:
        yield ServerContext(volume=volume, config=config, lock=asyncio.Lock())
    finally:
        stop_coordinator(config.root)
        logger.info("stale-write-guard-fs coordinator stopped: root=%s", config.root)


def register_tools(server: FastMCP) -> None:
    """Register the ``swg_*`` tools.

    Populated in Unit 4 (``swg_read`` / ``swg_write`` / ``swg_reacquire`` /
    ``swg_status``) and Unit 5 (``swg_write_cas``). Unit 2 is the bare skeleton.
    """
    return None


def build_server() -> FastMCP:
    """Build the FastMCP server (lifespan wired, tools registered)."""
    server = FastMCP(name=SERVER_NAME, instructions=INSTRUCTIONS, lifespan=lifespan)
    register_tools(server)
    return server


def _configure_stderr_logging() -> None:
    """Route all logging to stderr — stdout is the JSON-RPC channel (stdio invariant)."""
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(logging.INFO)


def main() -> None:
    """Console-script entrypoint: run the server over stdio."""
    _configure_stderr_logging()
    build_server().run(transport="stdio")


if __name__ == "__main__":
    main()
