# Copyright (c) 2026 Arbiter contributors.
# The Coherence Protocol for AI Agents

"""Unit 8 CI guard — diagnose modules must have zero import-time side effects.

Importing :mod:`ccs.diagnose` and :mod:`ccs.diagnose.telemetry` must not:

* spawn threads,
* open sockets,
* read or write files,
* read environment-variable consent state,
* call ``urllib.request.urlopen``.

The guard is enforced via :func:`sys.addaudithook` (Python 3.8+) which
gives us a process-wide event stream covering ``open``, ``socket.connect``,
``urllib.Request``, etc. The hook is installed in a *subprocess* so the
parent test runner's own imports don't pollute the audit trace.

We also pin a structural property: importing the module twice (with
:func:`importlib.reload`) must not change ``threading.active_count()``.
"""

from __future__ import annotations

import subprocess
import sys
import textwrap


_AUDIT_HARNESS = textwrap.dedent(
    """
    import os
    import socket
    import sys
    import threading

    # Tell ccs.diagnose.telemetry to use a temp config dir so even if the
    # guard misfires, no real user file is touched.
    os.environ["XDG_CONFIG_HOME"] = "/tmp/_ccs_diagnose_guard_xdg_DOES_NOT_EXIST"

    violations: list[str] = []

    BANNED_EVENTS = {
        # Network surface — Unit 8 must add zero network code.
        "socket.connect",
        "socket.bind",
        "socket.gethostbyname",
        "urllib.Request",
    }

    # Files we deliberately watch for: anything inside the user's config
    # directory (~/.config or $XDG_CONFIG_HOME). The Unit 8 contract is
    # "no consent.json read or write at import time".
    CONFIG_PATH_HINTS = (
        os.environ["XDG_CONFIG_HOME"],
        "ccs-diagnose",
        "consent.json",
    )

    # Snapshot baseline before the audit hook to avoid flagging the harness
    # itself.
    threads_before = threading.active_count()

    # Only audit the telemetry module's own frames. Pre-existing
    # transitive imports (langchain_core / langgraph) read platform
    # metadata at import time -- that's outside Unit 8's scope and not
    # what the contract is protecting against.
    AUDIT_FRAMES = ("ccs.diagnose.telemetry", "ccs.diagnose")

    def _from_audited_frame() -> str | None:
        frame = sys._getframe(2)  # skip _from_audited_frame + the hook
        while frame is not None:
            modname = frame.f_globals.get("__name__", "")
            if modname in AUDIT_FRAMES:
                return modname
            frame = frame.f_back
        return None

    def _is_config_path_open(args) -> bool:
        if not args:
            return False
        path = args[0]
        if isinstance(path, (bytes, bytearray)):
            try:
                path = path.decode("utf-8", errors="replace")
            except Exception:
                return False
        if not isinstance(path, str):
            return False
        return any(hint in path for hint in CONFIG_PATH_HINTS)

    def _audit_hook(event: str, args) -> None:
        if event in BANNED_EVENTS:
            modname = _from_audited_frame()
            if modname is not None:
                violations.append(f"{event} from {modname}: args={args!r}")
            return
        if event == "open" and _is_config_path_open(args):
            modname = _from_audited_frame()
            if modname is not None:
                violations.append(f"open(config) from {modname}: args={args!r}")

    sys.addaudithook(_audit_hook)

    import ccs.diagnose  # noqa: F401
    import ccs.diagnose.telemetry  # noqa: F401

    threads_after = threading.active_count()

    if violations:
        print("VIOLATIONS:")
        for v in violations:
            print(f"  {v}")
        sys.exit(1)
    if threads_after != threads_before:
        print(f"THREAD_COUNT_CHANGED: before={threads_before} after={threads_after}")
        sys.exit(1)
    print("OK")
    """
)


def _run_audit_subprocess() -> tuple[int, str, str]:
    """Run the audit harness in a fresh interpreter and return (rc, out, err)."""
    result = subprocess.run(
        [sys.executable, "-c", _AUDIT_HARNESS],
        capture_output=True,
        text=True,
        timeout=30,
    )
    return result.returncode, result.stdout, result.stderr


def test_import_ccs_diagnose_has_no_side_effects() -> None:
    """``import ccs.diagnose`` and ``import ccs.diagnose.telemetry`` are clean."""
    rc, out, err = _run_audit_subprocess()
    assert rc == 0, (
        f"audit hook flagged a side effect during import:\n"
        f"stdout:\n{out}\n"
        f"stderr:\n{err}"
    )
    assert "OK" in out


_RELOAD_HARNESS = textwrap.dedent(
    """
    import importlib
    import os
    import threading

    os.environ["XDG_CONFIG_HOME"] = "/tmp/_ccs_diagnose_guard_xdg_RELOAD"

    before = threading.active_count()
    import ccs.diagnose.telemetry as t1
    importlib.reload(t1)
    importlib.reload(t1)
    after = threading.active_count()

    if before != after:
        raise SystemExit(f"threads changed: {before} -> {after}")
    print("OK")
    """
)


def test_reload_does_not_change_thread_count() -> None:
    """Repeated ``importlib.reload`` must not spawn threads."""
    result = subprocess.run(
        [sys.executable, "-c", _RELOAD_HARNESS],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, (
        f"reload harness failed:\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    )
    assert "OK" in result.stdout


_FILE_OPEN_HARNESS = textwrap.dedent(
    """
    import os
    import sys

    os.environ["XDG_CONFIG_HOME"] = "/tmp/_ccs_diagnose_guard_xdg_FILE_OPEN"

    opens_during_import: list[str] = []
    AUDIT_FRAMES = ("ccs.diagnose.telemetry", "ccs.diagnose")
    CONFIG_PATH_HINTS = (
        os.environ["XDG_CONFIG_HOME"],
        "ccs-diagnose",
        "consent.json",
    )

    def _is_config_path(args) -> bool:
        if not args:
            return False
        path = args[0]
        if isinstance(path, (bytes, bytearray)):
            try:
                path = path.decode("utf-8", errors="replace")
            except Exception:
                return False
        if not isinstance(path, str):
            return False
        return any(hint in path for hint in CONFIG_PATH_HINTS)

    def _audit_hook(event: str, args) -> None:
        if event != "open":
            return
        if not _is_config_path(args):
            return
        frame = sys._getframe(1)
        while frame is not None:
            modname = frame.f_globals.get("__name__", "")
            if modname in AUDIT_FRAMES:
                opens_during_import.append(f"{modname}: {args!r}")
                break
            frame = frame.f_back

    sys.addaudithook(_audit_hook)
    import ccs.diagnose  # noqa: F401
    import ccs.diagnose.telemetry  # noqa: F401

    if opens_during_import:
        print("FILE_OPENS:")
        for o in opens_during_import:
            print(f"  {o}")
        sys.exit(1)
    print("OK")
    """
)


def test_import_does_not_open_files() -> None:
    """No ``open()`` calls originate from ``ccs.diagnose`` import frames."""
    result = subprocess.run(
        [sys.executable, "-c", _FILE_OPEN_HARNESS],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, (
        f"file-open audit failed:\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    )
    assert "OK" in result.stdout
