# Cross-host coherence demo — stale-write deny + effect ordering

Two clients coordinate **one** centralized coordinator over the network.

- **Scenario 1 — stale-write deny.** A stale write is denied by version-CAS
  *across the host boundary*, and the loser recovers via re-read + retry — no
  silent lost update.
- **Scenario 2 — effect ordering.** An effect gated on `config@vN` fires when the
  config is unchanged and is held when the config advanced under the agent —
  never fired on stale input.

**Honest scope (demo-grade).** Each limitation is named explicitly:

- **Single centralized coordinator.** A single point of failure, single-region —
  not distributed, replicated, or highly available. The production (HA) version
  is a separate effort.
- **Coordinator-version-only, not unmediated shared-filesystem coherence.** The
  shared artifact is **tracked** on the coordinator (so it is versioned) but
  **not strict** — the deny is the optimistic version-CAS; no strict
  read-enforcement is needed. The artifact is coordinated *through* the protocol,
  not by intercepting raw filesystem writes — catching a writer that **bypasses**
  the coordinator (e.g. a FUSE layer or kernel watcher) is out of scope
  (cooperative trust, not kernel enforcement).
- **Not cross-host fencing, partition tolerance, or durability/HA.** Durability
  beyond SQLite's `synchronous=NORMAL` is a separate decision; partition behavior
  is undefined.
- **Plaintext bearer token over a *trusted, encrypted* link** (e.g. WireGuard,
  not a bare VPC). Production hardening would add mTLS + certificate rotation.
  Single-user cooperative trust; no multi-tenant isolation.
- **Reduces, doesn't eliminate.** The gate only sees writes that go *through* the
  coordinator. An un-coordinated open, a write to a path the coordinator doesn't
  track, a value cached in another process, or a filesystem-level edit by a tool
  not party to the protocol — all invisible. It is best-effort point-in-time, not
  a lock; across hosts, network latency widens the window between version-check
  and effect.

All cross-host behavior is gated by `CCS_REMOTE_COORDINATOR`; the default
loopback path is unchanged.

## 1. Local smoke (any platform — proves the mechanism, not a host boundary)

```
python examples/cross_host/main.py
```

Spawns a loopback coordinator and runs both clients in one process. Exit `0` iff
Scenario 1's stale write was denied **and** recovery succeeded **and** Scenario
2's effect was held on a stale config. This proves the version-CAS deny +
recovery + effect gate; it does **not** cross a real host boundary.

### Negative control — the honest contract (`--baseline`)

```
python examples/cross_host/main.py --baseline
```

Runs the **negative control first** — the silent lost update and the stale effect
fire — *then* the with-coordination pass. Exit `0` iff **broken-must-lose AND
fixed-must-prevent**. The deny is *measured against its absence*, not asserted.
The baseline reproduces the classic un-coordinated, convention-only lost update —
a concrete failure this library prevents in the with-coordination pass.

## 2. Cross-host (the real demo — Linux netns or two VMs)

The genuine non-loopback bind (and `verify_host`'s validated-bind branch) runs
**only** here. Two ingredients:

**Host 1 — the coordinator**, bound to a private-range address and tracking the
shared key:

```
# Start a coordinator bound to a private-range interface (RFC-1918).
CCS_REMOTE_COORDINATOR=1 agent-coherence-coordinator --bind-host 10.0.0.1
# Track the shared key so it is versioned (tracked, not strict):
agent-coherence-track shared.txt
# Note the printed port and the secret file: <root>/.coherence/hook.secret
```

**Host 2 — the demo client**, pointing at host 1 over the encrypted link:

```
export CCS_REMOTE_COORDINATOR=1
export CCS_REMOTE_HOST=10.0.0.1
export CCS_REMOTE_PORT=<port from host 1>
export CCS_REMOTE_SECRET_FILE=/path/to/mounted/hook.secret   # provisioned out-of-band; never inline
export CCS_REMOTE_INSECURE=1   # acknowledge the plaintext-HTTP bearer over your encrypted link (see Security boundary)
python examples/cross_host/main.py
```

The client connects (never spawning a local coordinator), runs both clients (A
and B), and exits `0` on a denied-then-recovered stale write. The same two roles
run under the "Linux netns variant" below — set `CCS_REMOTE_INSECURE=1` there too.

`CCS_REMOTE_HOST` is non-loopback, so the client **fails closed** and refuses to
send its bearer over plaintext HTTP unless you set `CCS_REMOTE_INSECURE=1` — the
explicit acknowledgement that *you* have secured the link out-of-band (the
encrypted tunnel above). A loopback host needs no acknowledgement.

**Or terminate TLS in front and skip the ack.** If a TLS-terminating proxy sits in
front of the coordinator, point the client at it with verified https instead:

```
export CCS_REMOTE_TLS=1                                       # https with enforced certificate verification
export CCS_REMOTE_CA_FILE=/path/to/private-ca.pem            # trust your private CA (fail-closed: no symlink, not group/world-writable)
# CCS_REMOTE_INSECURE is NOT needed — a verified-https link satisfies the guard
```

Verification is fail-closed: an unverifiable certificate means the bearer is never
sent. Because the endpoint here is an IP literal, the proxy's certificate must
carry an IP subject alternative name (`subjectAltName = IP:10.0.0.1`) — a DNS-only
certificate fails closed against an IP endpoint — and on Python 3.13+ it must be
RFC 5280-strict. Use a private CA (not a self-signed certificate) and supply it via
`CCS_REMOTE_CA_FILE`. See the Security boundary below.

### Docker (recommended — genuine cross-container, one command, verified)

Two containers on a private-range bridge — **separate network namespaces, real
RFC-1918 IPs** — with one centralized coordinator. This is the genuine cross-host
topology (not loopback) and runs anywhere Docker does:

```
bash examples/cross_host/docker/run.sh
# or: docker compose -f examples/cross_host/docker/docker-compose.yml \
#       up --build --abort-on-container-exit --exit-code-from client
```

The coordinator binds to `172.28.0.2` (RFC-1918, validated) and serves; the client
connects from a *separate container* with `Host: 172.28.0.2` — exercising the
validated-bind allowlist branch for real, never spawning a local coordinator. The
bearer secret + port travel via a shared `.coherence` volume. Exit `0` on a
denied-then-recovered run (Scenario 1 deny + recover, Scenario 2 fire/hold).

### Linux netns variant

The same two roles also run in two `ip netns` joined by a `veth` pair (requires
`CAP_NET_ADMIN`). The Docker path above is the maintained one-command runner that
proves the same boundary with less host-specific setup; the raw-netns wrapper is
left as a Linux-host exercise.

## Security boundary

Before recording or sharing a cross-host run, the demo upholds these properties:
the relaxed Host-allowlist still rejects (403) any non-bind host; `bind_host` is
validated to RFC-1918/4193 (wildcard, loopback aliases, link-local, CGNAT, and
public addresses are all rejected); the bearer secret is provisioned via a
confidential channel (never an inline env var — it would leak in `ps` /
`docker inspect`); and the link is encrypted.

**Plaintext-bearer guard (fail-closed).** Without TLS the transport is plaintext
HTTP — the coordinator terminates no TLS itself; encryption is *your* out-of-band
responsibility (a WireGuard tunnel, a TLS-terminating proxy, or the isolated Docker
bridge here). To stop a silent leak, the client **refuses to send the bearer to a
non-loopback host** over plaintext. Two clean ways to satisfy it:

- **Verified https** — set `CCS_REMOTE_TLS=1` (see the Host-2 instructions above).
  A verified-https connection satisfies the guard automatically; no
  acknowledgement is needed. Certificate verification is enforced and fails
  closed — the bearer is never sent over an unverifiable link — with no way to
  turn verification off.
- **`CCS_REMOTE_INSECURE=1`** — the narrow case: a *plaintext* link you have
  secured yourself out-of-band. It *reduces* the silent-plaintext footgun; it does
  not *guarantee* encryption.

Set the plaintext ack **narrowly** (per-invocation or per-compose-service), not in
a persistent global shell profile — a forgotten global ack would
blanket-acknowledge every future non-loopback host.

**Coordinator-side bind guard (fail-closed).** A coordinator that binds beyond
loopback now refuses to start unless the operator asserts either
`CCS_TLS_TERMINATED=1` (a TLS-terminating front is present) or
`CCS_SERVE_INSECURE=1` (an acknowledged insecure link). These are **operator
assertions, not enforcement** — the coordinator cannot verify a proxy is really
there or that the link is really encrypted; it records the posture and serves. This
demo binds beyond loopback, so it sets `CCS_SERVE_INSECURE=1` (already wired into
the Docker compose service). Set it **narrowly** (per-compose-service), never as a
persistent global. Production hardening (TLS/mTLS termination) is tracked
separately; this cross-host mode remains experimental and default-off.
