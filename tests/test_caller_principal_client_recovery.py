# Copyright (c) 2026 agent-coherence contributors.
# The Coherence Protocol for AI Agents

"""The one-shot clients' caller-principal recovery (caller-principal plan R20).

The hook client and ``agent-coherence-status --self-test`` keep a session's
mint nonce and principal on disk under ``.coherence/`` — one process per
invocation leaves them no other place. A request the coordinator refuses with
a typed ``caller_principal_foreign`` / ``caller_principal_absent`` reason is
recovered by re-claiming with the SAME stored nonce: a principal that differs
from the one presented replaces the stored file (write-then-rename, 0600) and
the refused request is retried ONCE; the same principal, a claim bound under
another nonce, or a claim that does not confirm is reported and stops; a 404
retries once without the header. Nothing re-mints, nothing deletes the nonce,
and nothing prints a principal or a nonce.

What this buys on the hook surface is accident-resistance and a detectable
unbound caller, not separation between callers: every one of these files is
readable by any process of the same OS user.
"""

from __future__ import annotations

import http.server
import io
import json
import os
import threading
import time
import uuid
from pathlib import Path
from typing import Any

import pytest

from ccs.adapters.claude_code import auth
from ccs.adapters.claude_code.coordinator_server import (
    CoordinatorHTTPServer,
    caller_principal_identity,
)
from ccs.cli import coherence_hook_client, coherence_status

_PRINCIPAL_HEADER = "Coherence-Caller-Principal"  # frozen duplicate of the wire name
_FOREIGN = "caller_principal_foreign"  # frozen duplicates of the wire reasons
_ABSENT = "caller_principal_absent"
_CLAIMED = "caller_principal_claimed"


def _sid() -> str:
    return str(uuid.uuid4())


def _files(workspace: Path, sid: str) -> tuple[Path, Path]:
    base = workspace / ".coherence" / f"caller-principal-{caller_principal_identity(sid).hex}"
    return base.with_name(base.name + ".nonce"), base.with_name(base.name + ".principal")


def _value(tag: str) -> str:
    """A 43-character principal/nonce-shaped value, distinct per tag and not a
    palindrome (so a reversed or re-cut value can never pass for it)."""
    return (tag * 3 + "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOP")[:43]


def _drive(
    subcommand: str, payload: dict[str, Any], workspace: Path,
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> tuple[str, str]:
    monkeypatch.setattr("sys.stdin", io.StringIO(json.dumps(payload)))
    assert coherence_hook_client.main([subcommand, "--root", str(workspace)]) == 0
    captured = capsys.readouterr()
    return captured.out, captured.err


def _assert_nothing_leaks(text: str, *secrets: str) -> None:
    """R5 on the client side: no principal and no nonce in anything a client
    prints or logs. Asserts against the real values, so a leak cannot hide."""
    for secret in secrets:
        assert secret and secret not in text, "a principal or nonce reached client output"


# --- a scripted coordinator ---------------------------------------------------


class _Scripted(http.server.BaseHTTPRequestHandler):
    """Answers the claim from ``claims`` (one answer per claim, in order) and
    every other route through ``route(header)``; records every request as
    ``(path, header)`` for a route and ``(path, nonce)`` for a claim."""

    seen: list[tuple[str, str | None]] = []
    claims: list[tuple[int, dict[str, Any]]] = []
    route: Any = None

    def do_POST(self) -> None:  # noqa: N802 — stdlib name
        body = json.loads(self.rfile.read(int(self.headers.get("Content-Length", "0"))) or b"{}")
        if self.path == "/principal/claim":
            self.seen.append((self.path, body.get("mint_nonce")))
            status, answer = self.claims.pop(0)
        else:
            header = self.headers.get(_PRINCIPAL_HEADER)
            self.seen.append((self.path, header))
            status, answer = type(self).route(header)
        raw = json.dumps(answer).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def log_message(self, *_args: Any) -> None:
        return


def _refusal(reason: str) -> tuple[int, dict[str, Any]]:
    return 400, {"error": f"principal refused ({reason})", "reason": reason}


@pytest.fixture
def scripted(tmp_path: Path):
    """A git workspace wired to a scripted coordinator, with a stored nonce and
    principal for one session already on disk."""
    (tmp_path / ".git").mkdir()
    coherence = tmp_path / ".coherence"
    coherence.mkdir(mode=0o700)
    httpd = http.server.HTTPServer(("127.0.0.1", 0), _Scripted)
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    (coherence / "server.pid").write_text(f"12345\n{httpd.server_address[1]}\n")
    (coherence / "hook.secret").write_text("test-secret")
    sid = _sid()
    nonce_file, principal_file = _files(tmp_path, sid)
    nonce_file.write_text(_value("N") + "\n")
    principal_file.write_text(_value("P") + "\n")
    _Scripted.seen, _Scripted.claims, _Scripted.route = [], [], None
    try:
        yield tmp_path, sid
    finally:
        httpd.shutdown()
        httpd.server_close()


def test_a_refused_stored_principal_is_replaced_by_the_same_nonces_claim_and_retried_once(
    scripted, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    """The recovery path end to end on the wire: the refused request, ONE
    claim presenting the stored nonce (never a new one), the stored file
    replaced by the principal that claim returned, the request retried ONCE
    under it — and when the coordinator refuses the retry too, reported and
    stopped: no second claim, no third send. The nonce file is untouched and
    the replaced principal file is 0600."""
    workspace, sid = scripted
    nonce_file, principal_file = _files(workspace, sid)
    _Scripted.claims = [(200, {"ok": True, "principal": _value("Q")})]
    _Scripted.route = staticmethod(lambda _h: _refusal(_FOREIGN))

    out, err = _drive("session-stop", {"session_id": sid}, workspace, monkeypatch, capsys)

    assert _Scripted.seen == [
        ("/hooks/session-stop", _value("P")),
        ("/principal/claim", _value("N")),
        ("/hooks/session-stop", _value("Q")),
    ]
    assert out.strip() == "{}"
    assert _FOREIGN in err
    assert principal_file.read_text().strip() == _value("Q")
    assert principal_file.stat().st_mode & 0o777 == 0o600
    assert nonce_file.read_text().strip() == _value("N")
    _assert_nothing_leaks(out + err, _value("P"), _value("Q"), _value("N"))


def test_a_session_id_with_a_trailing_newline_is_never_claimed_for_nor_recovered(
    scripted, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    """The coordinator's shape check matches with ``$``, which in Python also
    admits one trailing newline; the Node client's does not. So neither client
    claims for such an id — not on the first hook, not in recovery, even with
    a nonce stored for it (as a client that did claim would have left) — and
    the two clients keep sharing one binding per session. The refusal is
    reported by its typed reason."""
    workspace, sid = scripted
    sid_nl = sid + "\n"
    nonce_file, _principal = _files(workspace, sid_nl)
    nonce_file.write_text(_value("N") + "\n")
    _Scripted.claims = [(200, {"ok": True, "principal": _value("Q")})]
    _Scripted.route = staticmethod(lambda _h: _refusal(_ABSENT))

    out, err = _drive("session-stop", {"session_id": sid_nl}, workspace, monkeypatch, capsys)

    assert _Scripted.seen == [("/hooks/session-stop", None)], "no claim, no retry"
    assert out.strip() == "{}"
    assert f"({_ABSENT})" in err and "malformed" in err
    assert nonce_file.read_text() == _value("N") + "\n"
    _assert_nothing_leaks(out + err, _value("N"), _value("Q"))


def test_a_recovered_request_is_answered_under_the_new_principal(
    scripted, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    """The success arm: the retry under the re-claimed principal is admitted
    and its answer is what the hook prints; nothing is reported."""
    workspace, sid = scripted
    _Scripted.claims = [(200, {"ok": True, "principal": _value("Q")})]
    _Scripted.route = staticmethod(
        lambda h: (200, {"ok": True}) if h == _value("Q") else _refusal(_FOREIGN)
    )

    out, err = _drive("session-stop", {"session_id": sid}, workspace, monkeypatch, capsys)

    assert json.loads(out) == {"ok": True}
    assert err == ""
    assert [path for path, _ in _Scripted.seen] == [
        "/hooks/session-stop", "/principal/claim", "/hooks/session-stop",
    ]


def test_a_reclaim_that_returns_the_presented_principal_reports_and_stops(
    scripted, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    """If the claim hands back the very principal that was refused, the refusal
    is not about staleness: report, and do not retry the request at all."""
    workspace, sid = scripted
    _, principal_file = _files(workspace, sid)
    _Scripted.claims = [(200, {"ok": True, "principal": _value("P")})]
    _Scripted.route = staticmethod(lambda _h: _refusal(_FOREIGN))

    out, err = _drive("session-stop", {"session_id": sid}, workspace, monkeypatch, capsys)

    assert _Scripted.seen == [
        ("/hooks/session-stop", _value("P")),
        ("/principal/claim", _value("N")),
    ]
    assert out.strip() == "{}" and _FOREIGN in err
    assert principal_file.read_text().strip() == _value("P")
    _assert_nothing_leaks(out + err, _value("P"), _value("N"))


def test_a_reclaim_refused_as_claimed_reports_and_never_re_mints(
    scripted, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    """Another claimant holds the session: report and stop. The stored nonce
    and principal stay exactly as they were — nothing is deleted, no new
    nonce is generated — and the request is not retried."""
    workspace, sid = scripted
    nonce_file, principal_file = _files(workspace, sid)
    _Scripted.claims = [(200, {"ok": False, "reason": _CLAIMED, "detail": "bound"})]
    _Scripted.route = staticmethod(lambda _h: _refusal(_FOREIGN))

    out, err = _drive("session-stop", {"session_id": sid}, workspace, monkeypatch, capsys)

    assert _Scripted.seen == [
        ("/hooks/session-stop", _value("P")),
        ("/principal/claim", _value("N")),
    ]
    assert out.strip() == "{}" and _CLAIMED in err
    assert nonce_file.read_text().strip() == _value("N")
    assert principal_file.read_text().strip() == _value("P")
    _assert_nothing_leaks(out + err, _value("P"), _value("N"))


def test_a_reclaim_answered_404_retries_once_without_the_header(
    scripted, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    """A 404 on the claim means the coordinator issues no principals now: the
    refused request is retried once WITHOUT the header."""
    workspace, sid = scripted
    _Scripted.claims = [(404, {"error": "not found"})]
    _Scripted.route = staticmethod(
        lambda h: (200, {"ok": True}) if h is None else _refusal(_FOREIGN)
    )

    out, err = _drive("session-stop", {"session_id": sid}, workspace, monkeypatch, capsys)

    assert _Scripted.seen == [
        ("/hooks/session-stop", _value("P")),
        ("/principal/claim", _value("N")),
        ("/hooks/session-stop", None),
    ]
    assert json.loads(out) == {"ok": True}


def test_an_absent_refusal_after_an_unconfirmed_claim_recovers_with_the_stored_nonce(
    scripted, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    """R20 inside one invocation: the claim's answer is lost (the watchdog's
    unconfirmed envelope), so the request goes out with no header and is
    refused as absent — the bind landed. The same stored nonce re-claims, the
    principal is stored, and the request is retried under it."""
    workspace, sid = scripted
    _, principal_file = _files(workspace, sid)
    principal_file.unlink()
    _Scripted.claims = [
        (200, {"ok": False, "degraded": True, "reason": "claim_unconfirmed"}),
        (200, {"ok": True, "principal": _value("Q")}),
    ]
    _Scripted.route = staticmethod(
        lambda h: (200, {"ok": True}) if h == _value("Q") else _refusal(_ABSENT)
    )

    out, _err = _drive("session-stop", {"session_id": sid}, workspace, monkeypatch, capsys)

    assert _Scripted.seen == [
        ("/principal/claim", _value("N")),
        ("/hooks/session-stop", None),
        ("/principal/claim", _value("N")),
        ("/hooks/session-stop", _value("Q")),
    ]
    assert json.loads(out) == {"ok": True}
    assert principal_file.read_text().strip() == _value("Q")


def test_a_loser_that_gave_up_on_a_stalled_winners_nonce_recovers_once_it_lands(
    scripted, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    """Two hooks race on a new session. The winner created the nonce file and
    stalled before writing it, past the loser's bounded wait: the loser gives
    up for now (no header) — it does not treat the half-written file as
    final. By the time its request is refused as absent (the winner's claim
    bound the session), the winner's write has landed; the loser's recovery
    reads the nonce THEN, claims with it, and its request is answered."""
    workspace, sid = scripted
    nonce_file, principal_file = _files(workspace, sid)
    principal_file.unlink()
    nonce_file.write_text("")  # the winner's exclusive create; its write has not landed
    monkeypatch.setattr(auth.time, "sleep", lambda _sec: None)  # the wait elapses first
    _Scripted.claims = [(200, {"ok": True, "principal": _value("Q")})]

    def route(header: str | None) -> tuple[int, dict[str, Any]]:
        if header == _value("Q"):
            return 200, {"ok": True}
        nonce_file.write_text(_value("N") + "\n")  # the stalled winner's write lands
        return _refusal(_ABSENT)

    _Scripted.route = staticmethod(route)

    out, err = _drive("session-stop", {"session_id": sid}, workspace, monkeypatch, capsys)

    assert _Scripted.seen == [
        ("/hooks/session-stop", None),
        ("/principal/claim", _value("N")),
        ("/hooks/session-stop", _value("Q")),
    ]
    assert json.loads(out) == {"ok": True}
    assert principal_file.read_text().strip() == _value("Q")
    _assert_nothing_leaks(out + err, _value("N"), _value("Q"))


def test_a_refusal_without_a_stored_nonce_is_reported_and_claims_nothing(
    scripted, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    """Recovery needs the nonce the binding was made with. Without one on
    disk the client has nothing to re-claim with: report and stop — never
    generate a nonce at recovery time, which would be a second claimant."""
    workspace, sid = scripted
    nonce_file, _ = _files(workspace, sid)
    nonce_file.unlink()
    _Scripted.route = staticmethod(lambda _h: _refusal(_FOREIGN))

    out, err = _drive("session-stop", {"session_id": sid}, workspace, monkeypatch, capsys)

    assert _Scripted.seen == [("/hooks/session-stop", _value("P"))]
    assert out.strip() == "{}" and _FOREIGN in err
    assert not nonce_file.exists()


def test_a_refusal_without_a_typed_reason_is_not_recovered(
    scripted, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    """Refusals are classified by the typed ``reason`` field, never by the
    prose: a 400 whose error text names the header and the reason but whose
    body carries no ``reason`` key is an ordinary validation failure — no
    recovery, no principal report, the silent ``{}`` every other rejected
    hook gets."""
    workspace, sid = scripted
    _Scripted.route = staticmethod(
        lambda _h: (400, {"error": f"the {_PRINCIPAL_HEADER} header ({_FOREIGN})"})
    )

    out, err = _drive("session-stop", {"session_id": sid}, workspace, monkeypatch, capsys)

    assert _Scripted.seen == [("/hooks/session-stop", _value("P"))]
    assert out.strip() == "{}"
    assert err == ""


# --- against a real coordinator: the binding store is reset under a session ---


def _start(workspace: Path) -> CoordinatorHTTPServer:
    server = CoordinatorHTTPServer(workspace, port=0, instance_id="principal-recovery")
    server.serve_in_thread()
    time.sleep(0.05)
    (workspace / ".coherence" / "server.pid").write_text(f"{os.getpid()}\n{server.port}\n")
    return server


def _reset_store(server: CoordinatorHTTPServer, workspace: Path) -> CoordinatorHTTPServer:
    """Stop the coordinator, delete ``state.db`` (the purge docs/security.md
    describes, and the corrupted-store recovery), and start a fresh one. The
    ``caller-principal-*`` files stay behind."""
    server.shutdown()
    for suffix in ("", "-wal", "-shm"):
        (workspace / ".coherence" / f"state.db{suffix}").unlink(missing_ok=True)
    return _start(workspace)


@pytest.fixture
def workspace(tmp_path: Path) -> Path:
    (tmp_path / ".git").mkdir()
    (tmp_path / "plan.md").write_text("plan v1")
    return tmp_path


def test_a_stored_principal_that_outlived_its_binding_store_is_recovered(
    workspace: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    """After ``state.db`` is deleted and recreated, a session that lives on
    (a long session, ``--resume``) presents a principal the fresh store never
    bound — refused as foreign on EVERY route. Its first hook re-claims with
    the stored nonce, which binds the session afresh; the stored principal is
    replaced by the new binding and the hook is answered. Every later hook
    presents the new one: exactly one claim for the whole recovery. Without
    this, coherence stayed off for that session on every route, for good."""
    server = _start(workspace)
    sid = _sid()
    edit = {"session_id": sid, "tool_input": {"file_path": str(workspace / "plan.md")}}
    try:
        out, _ = _drive("pre-edit", edit, workspace, monkeypatch, capsys)
        assert json.loads(out).get("ok") is True
        nonce_file, principal_file = _files(workspace, sid)
        nonce, old = nonce_file.read_text(), principal_file.read_text().strip()
        server = _reset_store(server, workspace)
        claims_before = server.endpoint_counters_snapshot()["principal_claim_total"]

        answers, errs = [], []
        for sub in ("pre-read", "pre-edit", "post-edit", "session-stop"):
            if sub == "post-edit":
                (workspace / "plan.md").write_text("plan v2")
            out, err = _drive(sub, edit, workspace, monkeypatch, capsys)
            answers.append(json.loads(out))
            errs.append(err)

        assert answers[0].get("status") == "fresh", answers
        assert all(a.get("ok") is True for a in answers[1:]), answers
        bound = server.registry.get_caller_principal(caller_principal_identity(sid))
        assert bound is not None and bound != old
        assert principal_file.read_text().strip() == bound
        assert principal_file.stat().st_mode & 0o777 == 0o600
        assert nonce_file.read_text() == nonce, "the nonce is never replaced"
        claims = server.endpoint_counters_snapshot()["principal_claim_total"] - claims_before
        assert claims == 1
        _assert_nothing_leaks("".join(errs), old, bound, nonce.strip())
    finally:
        server.shutdown()


def test_the_self_test_survives_a_reset_of_the_binding_store(
    workspace: Path, capsys: pytest.CaptureFixture[str],
) -> None:
    """``--self-test`` uses FIXED synthetic session ids and persists their
    principals like a hook does. After ``state.db`` is reset it must still
    pass — it used to fail at step 1 (``/hooks/pre-read returned HTTP 400``)
    on every run in that workspace — and it prints no principal or nonce."""
    server = _start(workspace)
    try:
        assert coherence_status._run_self_test(workspace) == 0
        server = _reset_store(server, workspace)
        assert coherence_status._run_self_test(workspace) == 0, capsys.readouterr().err
        assert coherence_status._run_self_test(workspace) == 0, capsys.readouterr().err
        captured = capsys.readouterr()
        stored = [
            p.read_text().strip()
            for p in (workspace / ".coherence").glob("caller-principal-*")
        ]
        assert len(stored) == 4
        _assert_nothing_leaks(captured.out + captured.err, *stored)
    finally:
        server.shutdown()


def test_a_torn_stored_principal_is_repaired_by_one_claim(
    workspace: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    """A ``.principal`` left empty (a write that never landed, or a
    truncation) reads as absent; the next hook claims with the stored nonce
    and gets the bound principal back. That claim REPAIRS the file, so the
    torn file costs one round trip, not one per hook for the rest of the
    session. Control: three hooks with an intact file claim nothing."""
    server = _start(workspace)
    sid = _sid()
    try:
        _drive("session-stop", {"session_id": sid}, workspace, monkeypatch, capsys)
        nonce_file, principal_file = _files(workspace, sid)
        bound = principal_file.read_text().strip()

        def claims_over_three_hooks() -> int:
            before = server.endpoint_counters_snapshot()["principal_claim_total"]
            for _ in range(3):
                out, _err = _drive(
                    "session-stop", {"session_id": sid}, workspace, monkeypatch, capsys
                )
                assert json.loads(out).get("ok") is True, out
            return server.endpoint_counters_snapshot()["principal_claim_total"] - before

        assert claims_over_three_hooks() == 0, "control: an intact file claims nothing"
        principal_file.write_text("")
        assert claims_over_three_hooks() == 1
        assert principal_file.read_text().strip() == bound
        assert principal_file.stat().st_mode & 0o777 == 0o600
    finally:
        server.shutdown()


# --- the mint-nonce file: an interrupted write is never overwritten ---------


@pytest.mark.parametrize(("age_sec", "waits"), [(0.0, True), (3600.0, False)], ids=["young", "old"])
def test_a_torn_nonce_file_is_waited_on_only_while_it_could_still_be_written(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, age_sec: float, waits: bool,
) -> None:
    """An empty nonce file is a racer's exclusive create whose write has not
    landed YET — or never will (killed, ENOSPC). A young one is waited on
    (the bounded retry: a loser must not treat a winner's in-progress write as
    final); an old one is treated as abandoned and reported at once instead of
    charging every later hook of the session the whole bounded wait — the
    outcome after the wait would be the same. Either way the file is never
    overwritten or removed."""
    (tmp_path / ".coherence").mkdir(mode=0o700)
    key = caller_principal_identity(_sid()).hex
    nonce_file = tmp_path / ".coherence" / f"caller-principal-{key}.nonce"
    nonce_file.touch(mode=0o600)
    stamp = time.time() - age_sec
    os.utime(nonce_file, (stamp, stamp))
    sleeps: list[float] = []
    monkeypatch.setattr(auth.time, "sleep", sleeps.append)

    with pytest.raises(auth.MintNonceUnavailable) as caught:
        auth.ensure_mint_nonce(tmp_path, key)

    assert bool(sleeps) is waits
    assert nonce_file.read_text() == ""
    # Named in full, with the operator's step, as the hook.secret error is and
    # as the Node client prints it: nothing repairs the file automatically.
    assert str(nonce_file) in str(caught.value)
    assert "remove" in str(caught.value)
