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

Tool logic lives in sync ``_do_*`` helpers (real ``volume`` + ``config`` in,
``CallToolResult`` out); the async tool wrappers are thin (lock + delegate), so
the contract is testable against a real coordinator without a FastMCP client.
"""

from __future__ import annotations

import asyncio
import logging
import sys
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field

from mcp.server.fastmcp import Context, FastMCP
from mcp.types import CallToolResult, TextContent, ToolAnnotations

from ccs.adapters.claude_code.lifecycle import stop_coordinator
from ccs.adapters.coherent_volume import CoherentVolume
from ccs.adapters.effect_gate import check_fence
from ccs.core.exceptions import HOLD_INPUT_VANISHED, CasVersionConflict, CoherenceError
from ccs.mcp.deny import cas_exhausted_result, coordinator_unavailable_result, deny_result
from ccs.mcp.session import SessionConfig, build_volume
from ccs.mcp.status import build_status
from ccs.mcp.uri import UriValidationError, validate_uri

logger = logging.getLogger(__name__)

SERVER_NAME = "stale-write-guard-fs"

# The honesty ceiling (origin §5 / R5): the server-level instructions, the
# per-tool descriptions, and the deny ``structuredContent`` together bound what
# may be claimed. Annotations are untrusted hints; this prose is not.
INSTRUCTIONS = """\
stale-write-guard-fs guards a SINGLE-HOST workspace against silent lost updates
when two or more agents share a mutable file. It enforces VERSION LINEAGE via a
local coherence coordinator; it does NOT merge content for you.

Three guarantees, all single-host and fail-closed:
  1. Sequential stale-overwrite — swg_write is DENIED (reason=stale_view) if the
     file changed since you read it: either a peer committed a newer version OR it
     was edited out-of-band (an editor, another tool, a shell script). Recover with
     swg_reacquire, then write FROM the fresh bytes it returns.
  2. Concurrent same-key lost-update — swg_write_cas(path, expected_version,
     new_content) rejects a stale compare-and-set as a TYPED CONFLICT (not an
     auto-merge): you read, merge, and retry; the server never merges for you.
  3. Effect fired on a superseded read — before ANY irreversible external
     action you decided from a swg_read (webhook, deploy, opened PR, posted
     message), call swg_gate(path, expected_version, expected_generation) with
     the pair that read returned. It DENIES (reason=stale_view) if the value
     moved OR the grant you read under was reclaimed while you were thinking —
     a reclaim leaves the version untouched, so the version alone cannot see
     it. On a deny, do not take the action: swg_reacquire, re-read, re-decide.
     The verdict is true as of that call; the dispatch after it is still yours.

OUT OF GUARANTEE (do not rely on this server for): writers on DIFFERENT hosts or
across a synced/network mount; divergent-history reconciliation; semantic/content
correctness; any server-enforced auto-merge. These are NOT detected in v1 — a
heterogeneous multi-host setup looks identical to a guarded one (swg_status
reports heterogeneous_scope_detectable=false).

TRUST BOUNDARY: the server enforces that your write descends from a version you
read; it CANNOT verify you derived your content from the bytes you read. Same-uid
local processes can reach the coordinator directly, bypassing this server — the
model is single-uid, single-host.

v1 guards UTF-8 TEXT artifacts (config, notes, memory, code); a non-text file is
reported, not silently mangled (binary support → v1.1).
"""

# Appended to every tool description so the honesty floor travels with each tool
# (a client that surfaces only descriptions still sees the scope).
_SCOPE_CLAUSE = (
    " SINGLE-HOST only. Out of guarantee and NOT detected in v1: writers on "
    "different hosts or across a synced/network mount, divergent-history "
    "reconciliation, semantic correctness, server-enforced auto-merge."
)

_READ_DESC = (
    "Read a workspace text file under coherence tracking. Returns {content, "
    "version, owner_generation}. The version is the comparand you pass to "
    "swg_write_cas; KEEP BOTH version and owner_generation and pass them to "
    "swg_gate before any irreversible external action you decide from this "
    "read (owner_generation=null means this coordinator does not report "
    "generations, so swg_gate will hold). A "
    "sticky-INVALID view returns fresh bytes but stays INVALID — use "
    "swg_reacquire to recover before writing." + _SCOPE_CLAUSE
)
_GATE_DESC = (
    "Verify a file is STILL unchanged and still under the same grant. Pass BOTH "
    "comparands from your earlier swg_read — expected_version AND "
    "expected_generation (its owner_generation). Call this "
    "immediately BEFORE any irreversible external action you decided from that "
    "read (sending a webhook, opening a PR, running a deploy, posting a "
    "message). Returns decision=proceed, or DENIES with reason=stale_view if the "
    "file moved OR the grant you read it under was reclaimed (the version alone "
    "cannot see a reclaim) OR either comparand is unconfirmed. On a deny: do NOT "
    "take the action — swg_reacquire, re-read, re-decide." + _SCOPE_CLAUSE
)
_WRITE_DESC = (
    "Write a workspace text file (acquire -> write -> commit). DENIED with "
    "reason=stale_view if the file changed since you read it — a peer commit OR an "
    "out-of-band edit (another tool/editor): recover with swg_reacquire, then write "
    "FROM its bytes. A mid-write preempt returns reason=commit_preempted (disk may "
    "hold un-versioned bytes; reacquire_and_reconcile)." + _SCOPE_CLAUSE
)
_REACQUIRE_DESC = (
    "Recover from a stale_view deny: re-mint identity and return the CURRENT "
    "bytes. You MUST write FROM these exact bytes — the server enforces version "
    "lineage, NOT that your content was derived from what you read." + _SCOPE_CLAUSE
)
_STATUS_DESC = (
    "Report coherence state: coordinator on|off|unknown (unknown is NOT off), "
    "per-path enforced|not_registered, is_attached/is_degraded/session_id, and "
    "heterogeneous_scope_detectable=false (a multi-host or differently-scoped "
    "setup is NOT distinguishable in v1)." + _SCOPE_CLAUSE
)

_WRITE_CAS_DESC = (
    "Concurrent same-key write via compare-and-set. You read (swg_read returns the "
    "version comparand; swg_reacquire does NOT — it is for swg_write recovery), "
    "MERGE, then call swg_write_cas(path, expected_version, "
    "new_content). Stale-write-rejected: if a peer committed since your read, the "
    "CAS is a TYPED CONFLICT (reason=version_mismatch, current_version returned) "
    "— NOT an auto-merge; re-read at current_version, re-merge, and retry. The "
    "per-session conflict counter bounds only a COOPERATING agent (one session, "
    "stops on retryable=false); it is NOT livelock-proof against a fresh session "
    "or one that ignores retryable=false." + _SCOPE_CLAUSE
)

_REACQUIRE_NOTE = "write FROM these exact bytes — the server enforces version lineage, not content derivation"

# The per-session bound on consecutive CAS conflicts for one path (the
# cooperating-agent livelock guard). Mirrors the adapter's MAX_CAS_REACQUIRES=8.
MAX_CAS_CONFLICTS = 8


@dataclass
class ServerContext:
    """Lifespan-owned session state shared by every tool.

    ``lock`` serializes access to ``volume`` — every tool acquires it for the
    duration of its volume interaction (see module docstring).
    """

    volume: CoherentVolume
    config: SessionConfig
    lock: asyncio.Lock
    # path → consecutive CAS-conflict count, the cooperating-agent livelock bound
    # for swg_write_cas (reset on a win; survives only within one stdio session).
    cas_conflicts: dict[str, int] = field(default_factory=dict)


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


# --- result builders ---------------------------------------------------------


def _ok_result(structured: dict, text: str) -> CallToolResult:
    return CallToolResult(
        isError=False,
        content=[TextContent(type="text", text=text)],
        structuredContent=structured,
    )


def _client_error_result(reason: str, recover: str, detail: str) -> CallToolResult:
    """A non-deny client/input error (invalid path, missing file, binary). Still
    a non-ignorable isError, but NOT a coherence deny (never ``stale_view``)."""
    return CallToolResult(
        isError=True,
        content=[TextContent(type="text", text=detail)],
        structuredContent={
            "reason": reason,
            "recover": recover,
            "retryable": False,
            "detail": detail,
        },
    )


def _decode_text(data: bytes) -> str | None:
    try:
        return data.decode("utf-8")
    except UnicodeDecodeError:
        return None


# --- tool logic (sync, testable: real volume + config in, CallToolResult out) -


def _do_read(volume: CoherentVolume, config: SessionConfig, path: str) -> CallToolResult:
    try:
        key = validate_uri(path, root=config.root)
    except UriValidationError as exc:
        return _client_error_result("invalid_path", "fix_path", str(exc))
    if not volume.is_attached:
        return coordinator_unavailable_result(f"coordinator unattached; cannot read {path}")
    try:
        data, version, owner_generation = volume.read_with_version_generation(key)
    except FileNotFoundError as exc:
        return _client_error_result("file_not_found", "check_path", str(exc))
    except CoherenceError as exc:
        return deny_result(exc)
    except OSError as exc:  # disk/permission failure → fail closed, never escape to FastMCP
        return _client_error_result("io_error", "none", str(exc))
    text = _decode_text(data)
    if text is None:
        return _client_error_result("binary_unsupported", "use_text", f"{key} is not UTF-8 text (v1 guards text only)")
    # owner_generation rides alongside the version as the SECOND comparand:
    # the version answers "is this value still current", the generation answers
    # "is the grant it was read under still standing". Both are what swg_gate
    # re-checks before an irreversible external action. ``null`` means the
    # coordinator could not confirm one (older daemon, deny, or degraded) — a
    # later swg_gate HOLDs on it rather than firing blind.
    return _ok_result(
        {
            "content": text,
            "version": version,
            "owner_generation": owner_generation,
            "encoding": "utf-8",
        },
        text,
    )


def _do_gate(
    volume: CoherentVolume,
    config: SessionConfig,
    path: str,
    expected_version: int,
    expected_generation: int | None,
) -> CallToolResult:
    """Re-validate a comparand from an earlier ``swg_read`` immediately before
    the agent takes an irreversible external action.

    This is the pull-based fence for the tool surface. ``gate()`` cannot be
    exposed as a tool — its ``decide``/``effect`` are Python callables, and an
    MCP agent's decision and effect live outside this process, between tool
    calls. So the agent holds the ``(version, owner_generation)`` pair from its
    read and asks HERE, one call before dispatching. Same guarantee, same
    honest boundary: the verdict is true as of this check, and the dispatch
    that follows is still the agent's own step."""
    try:
        key = validate_uri(path, root=config.root)
    except UriValidationError as exc:
        return _client_error_result("invalid_path", "fix_path", str(exc))
    if not volume.is_attached:
        return coordinator_unavailable_result(
            f"coordinator unattached; cannot verify {path} before an effect"
        )
    if expected_generation is None:
        # Distinct from a coherence deny: the agent supplied no authority
        # comparand, so there is nothing to verify. Returning the retryable
        # stale_view deny here would send a cooperating agent into an
        # unbounded reacquire loop that can never clear, since re-reading
        # cannot supply a comparand it failed to carry. Name the real problem.
        return _client_error_result(
            "missing_comparand",
            "reread",
            "swg_gate needs the owner_generation from your swg_read alongside "
            "expected_version; call swg_read and pass BOTH comparands back. "
            "If that read returned owner_generation=null, this coordinator "
            "does not report generations — restart it on a current version "
            "(re-reading will not help).",
        )
    try:
        check_fence(
            volume,
            key,
            expected_version=expected_version,
            expected_generation=expected_generation,
        )
    except CoherenceError as exc:
        cause = getattr(exc, "hold_cause", None)
        if cause == HOLD_INPUT_VANISHED:
            # The input is GONE. check_fence raises this as a StaleView (a
            # HOLD), but routing it through the deny mapping would advertise
            # recover="reacquire", retryable=true — telling the agent to
            # reacquire a file that does not exist, which swg_reacquire then
            # refuses. Answer with the same shape swg_read gives for a missing
            # file, so the advice matches reality.
            return _client_error_result("file_not_found", "check_path", str(exc))
        # Every other HOLD is a StaleView, so it maps through the SAME deny path
        # (and the same reason vocabulary) as every other refusal on this
        # surface — the agent already knows how to read it. Never a soft
        # "false", and retry-then-escalate is the correct advice for all of
        # them (see the hold_cause table in the guide).
        result = deny_result(exc)
        if cause is not None and isinstance(result.structuredContent, dict):
            # WHY it held, typed: the agent branches on a value, not on prose.
            result.structuredContent["hold_cause"] = cause
        return result
    except OSError as exc:
        return _client_error_result("io_error", "none", str(exc))
    return _ok_result(
        {
            "decision": "proceed",
            "path": key,
            "version": expected_version,
            "owner_generation": expected_generation,
        },
        f"{key} is unchanged at v{expected_version} under the same grant; proceed",
    )


def _do_write(volume: CoherentVolume, config: SessionConfig, path: str, content: str) -> CallToolResult:
    try:
        key = validate_uri(path, root=config.root)
    except UriValidationError as exc:
        return _client_error_result("invalid_path", "fix_path", str(exc))
    # is_attached re-check BEFORE the write — a mid-session endpoint loss must
    # fail closed here, never reach the adapter's best-effort unversioned write.
    if not volume.is_attached:
        return coordinator_unavailable_result(f"coordinator unattached; refusing write of {key}")
    try:
        volume.write(key, content.encode("utf-8"))
    except CoherenceError as exc:
        return deny_result(exc)
    except OSError as exc:  # disk/permission failure → fail closed, never escape to FastMCP
        return _client_error_result("io_error", "none", str(exc))
    return _ok_result({"ok": True, "path": key}, f"wrote {key}")


def _do_reacquire(volume: CoherentVolume, config: SessionConfig, path: str) -> CallToolResult:
    try:
        key = validate_uri(path, root=config.root)
    except UriValidationError as exc:
        return _client_error_result("invalid_path", "fix_path", str(exc))
    if not volume.is_attached:
        return coordinator_unavailable_result(f"coordinator unattached; cannot reacquire {key}")
    try:
        data = volume.reacquire(key)
    except FileNotFoundError as exc:
        return _client_error_result("file_not_found", "check_path", str(exc))
    except CoherenceError as exc:
        return deny_result(exc)
    except OSError as exc:  # disk/permission failure → fail closed, never escape to FastMCP
        return _client_error_result("io_error", "none", str(exc))
    text = _decode_text(data)
    if text is None:
        return _client_error_result("binary_unsupported", "use_text", f"{key} is not UTF-8 text (v1 guards text only)")
    return _ok_result({"content": text, "encoding": "utf-8", "note": _REACQUIRE_NOTE}, text)


def _do_status(volume: CoherentVolume, config: SessionConfig) -> CallToolResult:
    status = build_status(volume, config)
    return _ok_result(status, f"coordinator={status['coordinator']}")


def _do_write_cas(
    volume: CoherentVolume,
    config: SessionConfig,
    conflicts: dict[str, int],
    path: str,
    expected_version: int,
    new_content: str,
) -> CallToolResult:
    try:
        key = validate_uri(path, root=config.root)
    except UriValidationError as exc:
        return _client_error_result("invalid_path", "fix_path", str(exc))
    if not volume.is_attached:
        return coordinator_unavailable_result(f"coordinator unattached; refusing CAS of {key}")
    try:
        volume.write_cas_at(key, expected_version, new_content.encode("utf-8"))
    except CasVersionConflict as exc:
        # Bound a COOPERATING agent's retry loop: too many consecutive conflicts
        # on one path in this session → tell it to stop (retryable=false).
        count = conflicts.get(key, 0) + 1
        if count > MAX_CAS_CONFLICTS:
            conflicts.pop(key, None)
            return cas_exhausted_result(
                f"{key}: {count} consecutive CAS conflicts in this session — stop "
                "(the cooperating-agent livelock bound; coordinator-side fencing is v1.1)"
            )
        conflicts[key] = count
        return deny_result(exc)
    except FileNotFoundError as exc:
        return _client_error_result("file_not_found", "check_path", str(exc))
    except CoherenceError as exc:
        return deny_result(exc)
    except OSError as exc:  # disk/permission failure → fail closed, never escape to FastMCP
        return _client_error_result("io_error", "none", str(exc))
    conflicts.pop(key, None)  # a win resets the cooperating-agent conflict streak
    return _ok_result({"ok": True, "path": key}, f"committed {key}")


# --- registration ------------------------------------------------------------


def _server_context(ctx: Context) -> ServerContext:
    return ctx.request_context.lifespan_context


def register_tools(server: FastMCP) -> None:
    """Register the sequential ``swg_*`` tools under the serialization lock.

    ``swg_write_cas`` is the concurrent regime; ``swg_gate`` is the
    pull-based effect fence agents call before an irreversible dispatch.
    """

    @server.tool(
        name="swg_read",
        description=_READ_DESC,
        annotations=ToolAnnotations(readOnlyHint=False),  # pre-read mutates coordinator MESI state
        structured_output=False,
    )
    async def swg_read(path: str, ctx: Context) -> CallToolResult:
        sctx = _server_context(ctx)
        async with sctx.lock:
            return _do_read(sctx.volume, sctx.config, path)

    @server.tool(
        name="swg_write",
        description=_WRITE_DESC,
        annotations=ToolAnnotations(readOnlyHint=False),
        structured_output=False,
    )
    async def swg_write(path: str, content: str, ctx: Context) -> CallToolResult:
        sctx = _server_context(ctx)
        async with sctx.lock:
            return _do_write(sctx.volume, sctx.config, path, content)

    @server.tool(
        name="swg_reacquire",
        description=_REACQUIRE_DESC,
        annotations=ToolAnnotations(readOnlyHint=False),
        structured_output=False,
    )
    async def swg_reacquire(path: str, ctx: Context) -> CallToolResult:
        sctx = _server_context(ctx)
        async with sctx.lock:
            return _do_reacquire(sctx.volume, sctx.config, path)

    @server.tool(
        name="swg_status",
        description=_STATUS_DESC,
        annotations=ToolAnnotations(readOnlyHint=True),
        structured_output=False,
    )
    async def swg_status(ctx: Context) -> CallToolResult:
        sctx = _server_context(ctx)
        async with sctx.lock:
            return _do_status(sctx.volume, sctx.config)

    @server.tool(
        name="swg_gate",
        description=_GATE_DESC,
        annotations=ToolAnnotations(readOnlyHint=False),  # pre-read mutates coordinator MESI state
        structured_output=False,
    )
    async def swg_gate(
        path: str,
        expected_version: int,
        expected_generation: int | None,
        ctx: Context,
    ) -> CallToolResult:
        sctx = _server_context(ctx)
        async with sctx.lock:
            return _do_gate(
                sctx.volume, sctx.config, path, expected_version, expected_generation
            )

    @server.tool(
        name="swg_write_cas",
        description=_WRITE_CAS_DESC,
        annotations=ToolAnnotations(readOnlyHint=False),
        structured_output=False,
    )
    async def swg_write_cas(path: str, expected_version: int, new_content: str, ctx: Context) -> CallToolResult:
        sctx = _server_context(ctx)
        async with sctx.lock:
            return _do_write_cas(sctx.volume, sctx.config, sctx.cas_conflicts, path, expected_version, new_content)


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
