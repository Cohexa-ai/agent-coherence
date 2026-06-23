# Cross-host coherence demo (slice 1: stale-write deny across a host boundary)

Two clients coordinate **one** centralized coordinator over the network. A stale
write is denied by version-CAS *across the boundary*, and the loser recovers via
re-read + retry — no silent lost update.

**Honest scope (Gate A, demo-grade):**
- A **single centralized coordinator** — a SPOF, single-region. NOT distributed / replicated / HA.
- **Coordinator-version-only**: the shared artifact is **tracked** on the coordinator (so it is versioned) but **not strict** — the deny is the OCC version-CAS; no strict read-enforcement needed.
- **Plaintext bearer over a trusted, encrypted link** (WireGuard, not bare VPC). Production hardening = mTLS + cert rotation, co-developed.
- All cross-host behavior is gated by `CCS_REMOTE_COORDINATOR`; the default loopback path is unchanged.

## 1. Local smoke (any platform — proves the mechanism, not a host boundary)

```
python examples/cross_host/main.py
```

Spawns a loopback coordinator and runs both clients in one process. Exit `0` iff
the stale write was denied **and** recovery succeeded. This proves the
version-CAS deny + recovery; it does **not** cross a real host boundary.

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

### Linux netns variant

The same two roles run in two network namespaces joined by a `veth` pair (the
coordinator's namespace holds the RFC-1918 address). Requires `CAP_NET_ADMIN`
(root). A one-command `run_netns.sh` wrapper is a follow-up to add and **verify
on a Linux host** — it is intentionally omitted here rather than shipped
unverified (this demo was authored on macOS, where netns does not exist).

## Security boundary (R7)

Before recording/sharing a cross-host run: the relaxed Host-allowlist still 403s
any non-bind host; `bind_host` is validated to RFC-1918/4193 (wildcard, loopback
aliases, link-local, CGNAT, public all rejected); the secret is provisioned via
a confidential channel (not an inline env var — it would leak in `ps`/`docker
inspect`); the link is encrypted. See the `coherence-security-reviewer` pass.
