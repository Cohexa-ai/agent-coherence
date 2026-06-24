# Cross-host coherence demo — slice 1 (stale-write deny) + slice 2 (effect-ordering)

Two clients coordinate **one** centralized coordinator over the network. **Slice 1
(artifact-coordination):** a stale write is denied by version-CAS *across the
boundary*, and the loser recovers via re-read + retry — no silent lost update.
**Slice 2 (effect-ordering, EO-1..EO-3):** an effect gated on `config@vN` FIRES
when the config is unchanged and is HELD when the config advanced under the
agent — never fired on stale input.

**Honest scope (Gate A, demo-grade) — owned by NR-number so each limit is named, not implied:**

- **NR-1 — Single centralized coordinator.** A SPOF, single-region. NOT distributed / replicated / HA. The HA version is the production rung, co-developed with a pilot — not papered over.
- **NR-2 — Coordinator-version-only, NOT unmediated shared-FS coherence.** The shared artifact is **tracked** on the coordinator (so it is versioned) but **not strict** — the deny is the OCC version-CAS; no strict read-enforcement is needed. The artifact is coordinated *through* the protocol, NOT by intercepting raw filesystem writes — catching a writer that **bypasses** the coordinator (FUSE / kernel watcher) is out (cooperative trust, not kernel enforcement).
- **NR-3 — NOT cross-host fencing, partition tolerance, or durability/HA.** `coordinator_epoch` is seeded-but-unused at the network layer; durability beyond sqlite's `synchronous=NORMAL` is a separate `synchronous=FULL` decision; partition behavior is undefined.
- **NR-4 — Plaintext bearer over a *trusted, encrypted* link** (WireGuard, not bare VPC). Production hardening = mTLS + cert rotation, co-developed. Single-uid cooperative trust; no multi-tenant isolation.
- **NR-5 — Reduces, doesn't eliminate.** The gate only sees writes that go *through* the coordinator. An un-coordinated open, a write to a path the coordinator doesn't `track`, a value cached in another process, an FS-level edit by a tool not party to the protocol — all invisible. Best-effort point-in-time, not a lock; across hosts, network latency widens the window between version-check and effect.

All cross-host behavior is gated by `CCS_REMOTE_COORDINATOR`; the default loopback path is unchanged.

## 1. Local smoke (any platform — proves the mechanism, not a host boundary)

```
python examples/cross_host/main.py
```

Spawns a loopback coordinator and runs both clients in one process. Exit `0` iff
slice-1's stale write was denied **and** recovery succeeded **and** slice-2's
effect was held on a stale config. This proves the version-CAS deny + recovery +
effect-gate; it does **not** cross a real host boundary.

### Negative control — the honest contract (`--baseline`)

```
python examples/cross_host/main.py --baseline
```

Runs the **negative control first** — slice-1 silent lost update + slice-2 stale
effect fire — *then* the with-CCS pass. Exit `0` iff **broken-must-lose AND
fixed-must-prevent**. This is the screen-share contract: the deny is *measured*
against its absence, not asserted. The baseline reproduces the classic
un-coordinated, convention-only lost-update — a concrete failure CCS prevents in
the with-CCS pass.

## 2. Cross-host (the real demo — Linux netns or two VMs)

The genuine non-loopback bind (and `verify_host`'s validated-bind branch) runs
**only** here. Two ingredients:

**Host 1 — the coordinator**, bound to a private-range address and tracking the
shared key:

```
# Start a coordinator bound to a private-range interface (RFC-1918).
CCS_REMOTE_COORDINATOR=1 agent-coherence-coordinator --bind-host 10.0.0.1
# Track the shared key so it is versioned (tracked != strict):
agent-coherence-track shared.txt
# Note the printed port and the secret file: <root>/.coherence/hook.secret
```

**Host 2 — the demo client**, pointing at host 1 over the encrypted link:

```
export CCS_REMOTE_COORDINATOR=1
export CCS_REMOTE_HOST=10.0.0.1
export CCS_REMOTE_PORT=<port from host 1>
export CCS_REMOTE_SECRET_FILE=/path/to/mounted/hook.secret   # provisioned out-of-band; never inline
python examples/cross_host/main.py
```

The client connects (never spawning a local coordinator), runs A/B, and exits
`0` on a denied-then-recovered stale write.

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
denied-then-recovered run. (Verified output: slice-1 deny + recover, slice-2
fire/hold.)

### Linux netns variant

The same two roles also run in two `ip netns` joined by a `veth` pair (requires
`CAP_NET_ADMIN`). The Docker path above is the maintained one-command runner that
proves the same boundary with less host-specific setup; the raw-netns wrapper is
left as a Linux-host exercise.

## Security boundary (R7)

Before recording/sharing a cross-host run: the relaxed Host-allowlist still 403s
any non-bind host; `bind_host` is validated to RFC-1918/4193 (wildcard, loopback
aliases, link-local, CGNAT, public all rejected); the secret is provisioned via
a confidential channel (not an inline env var — it would leak in `ps`/`docker
inspect`); the link is encrypted. See the `coherence-security-reviewer` pass.
