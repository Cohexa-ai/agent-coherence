# Changelog

All notable changes to `agent-coherence` are documented here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions adhere to [SemVer](https://semver.org/spec/v2.0.0.html).

Alpha — APIs may change before `v1.0`.

## [0.10.1] - 2026-06-24

A fully opt-in **cross-host coordination demo** (default-off `CCS_REMOTE_COORDINATOR`)
plus a library fix to the coordinator's Host-allowlist for IPv6 binds. The default
loopback path is byte-unchanged; all cross-host behavior is gated by the flag.

### Added

- **Cross-host coordination demo (`examples/cross_host/`), default-off.** Two
  clients coordinate one centralized coordinator across a host boundary: a stale
  write is denied by version-CAS *across the boundary* and the loser recovers via
  re-read + retry (slice 1, artifact-coordination); an effect gated on `config@vN`
  fires only when the config is unchanged and is held when it advanced (slice 2,
  effect-ordering). A `--baseline` negative-control mode runs the silent-lost-update
  and stale-effect-fire failures first, so the deny/HOLD is measured against its
  absence (`broken-must-lose AND fixed-must-prevent`). Loopback smoke runs anywhere;
  a Docker two-container runner (separate network namespaces, RFC-1918 bridge) and a
  Linux netns path exercise a genuine host boundary.
- **Opt-in remote-coordinator transport (experimental, demo-grade).** Gated entirely
  by `CCS_REMOTE_COORDINATOR` (default off): `CoherentVolume(remote_endpoint=…)`
  connect-only / never-spawn mode; a file-based bearer secret (`CCS_REMOTE_SECRET_FILE`,
  read with `O_NOFOLLOW`); a typed `RemoteAuthFailed`; and an
  `agent-coherence-coordinator --bind-host` flag with explicit RFC-1918/4193 bind
  validation plus a configurable Host-allowlist. Strict-only and fail-closed (deny /
  degrade / 401 / non-2xx all raise). The default loopback path is byte-unchanged.

### Fixed

- **Host-allowlist now parses bracketed IPv6 literals.** `verify_host` previously
  split on the first `:`, mangling `[fc00::1]:port` so every IPv6 Host was rejected.
  It now extracts the bracketed literal (rejecting junk after `]`), declines to trim
  whitespace/control characters, and matches IP entries on their normalized form so
  equivalent spellings resolve to the same allowlist entry. The loopback/IPv4 path is
  byte-unchanged and DNS-rebind protection is preserved (no aliased / IPv4-mapped /
  scoped / alt-radix spelling can admit a non-allowlisted host).

## [0.10.0] - 2026-06-23

Foreign-edit coordination on both the read and the write surface, plus the
`stale-write-guard-fs` MCP server. A managed file edited out-of-band (a human, a
script, a tool not on the coordinator) is now caught when an agent re-reads it
*and* when an agent writes over it.

### Added

- **MCP-C v1 — the `stale-write-guard-fs` MCP server.** A stdio
  [Model Context Protocol](https://modelcontextprotocol.io) server
  (`pip install agent-coherence[mcp]`; `stale-write-guard-fs` console script) that
  exposes the shipped single-host `CoherentVolume` coordination to any MCP client
  over five tools: `swg_read`, `swg_write`, `swg_reacquire`, `swg_write_cas`
  (single-shot version-checked CAS), and a 3-state `swg_status` (`on` / `off` /
  `unknown`). Per-session coordinator binding; a URI→key validator that rejects
  path traversal, `.coherence` access, and symlinks resolving into `.coherence`
  (info-disclosure); a typed deny-contract mapper that renders coherence terminals
  (e.g. `stale_view`) with `recover: reacquire`; fail-closed on IO errors.
  Strict / managed-path scoped. Ships with a red→green front-door demo (with a
  negative control); the MCP suite runs in CI (skipped on installs without the
  `mcp` extra).
- **Read-time foreign-edit deny (`on_stale_read`).** A SHARED holder that re-reads
  a managed file whose on-disk bytes changed out-of-band is now denied in strict
  mode — promoting the prior fail-open `hash_differs` advisory to an enforced deny.
  Opt in client-side with `CoherentVolume(on_stale_read="raise")` to surface it as
  `StaleView`; recover via `reacquire()`. A benign self-commit→disk-write lag
  window is suppressed (`shared_foreign_lag_suppressed_total` counter) so an
  instance never denies its own just-written bytes.
- **Pre-write content-CAS (`on_stale_write`).** `CoherentVolume(on_stale_write=…)`
  denies a write that would clobber a foreign / out-of-band edit (the managed file
  on disk changed since this instance last read/wrote it), surfaced as `StaleView`;
  recover via `reacquire()`. Plumbed through `install()` / `coherent_workspace()`.
  The MCP `swg_write` tool surfaces it as a typed `stale_view` deny.
- Vendor-neutral example demos: a shared-knowledge-base lost-update demo and a
  divergent-session memory-coherence demo (`examples/`).

### Changed

- **`CoherentVolume.write()` now guards by default** (`on_stale_write="raise"`): a
  write to a managed path whose on-disk bytes changed out-of-band since the last
  read/write raises `StaleView` instead of silently overwriting. Set
  `on_stale_write="allow"` to restore the prior clobber. (`write()` already raised
  `StaleView` on the INVALID-stale pre-edit; this extends it to the foreign-edit
  case.)
- **Strict mode now enforces read-time foreign-edit detection** (server-side): the
  coordinator denies a SHARED holder's re-read of a managed file whose supplied
  content hash diverges from the canonical, where it previously only advised. A
  behavior change for strict-mode deployments only; non-strict paths are unaffected.

## [0.9.3] - 2026-06-14

### Added

- **Bounded, durable version retention + read-at-version.** The coordinator can
  retain a bounded history of committed artifact versions and serve any retained
  `(artifact, version)` through `CoordinatorService.read_at_version(...)`,
  returning a typed `VersionedContent` or a `VersionedReadRejection` over six
  wire-stable reasons. Opt in per registry with `RetentionPolicy(max_versions=K,
  max_age_seconds=T)` — **off by default**. The in-memory registry retains in
  process; `SqliteArtifactRegistry` retains durably across a coordinator restart
  for in-process embedders, behind the store's first real schema-version bump
  (v1 → v2, applied automatically and atomically; durable content storage is
  opt-in and flips no existing deployment silently). `agent-coherence-replay
  resolve --db <state.db> --artifact <path|uuid> --version <n>` reads bytes at a
  version from a stored coordinator, content-safe by default (metadata only
  unless `--include-content` / `--output-file`). Read-at-version is an
  off-protocol read — it grants no MESI state and captures no read-generation
  fence claim (R6/R7). Formally modelled in `formal/tla/Retention.tla`
  (`NoCollectedRead` + a versioned-read-is-a-no-op action property), wired into
  `make tla-check`.
- **Reproducible temporal-cost sweep + token/$ translation** (benchmark tooling,
  no library API change). The change-rate × answer-sensitivity sweep
  (`tools/run_cost_sweep.py`) gains `--rates/--sensitivities/--runs` so a refined
  grid reproduces from committed code; the pre-registered savings-regime verdict
  is recorded in `benchmarks/cost_preregistration.md` (PASS at n=50, crossover
  r≈0.31). `tools/cost_to_tokens.py` translates the re-fetch-avoided proxy into
  input-token + prompt-cache dollar terms under explicit, labeled assumptions.

### Changed

- **`SqliteArtifactRegistry(retain_versions=True)` is now supported** —
  previously it raised `NotImplementedError`. Callers that relied on that raise
  as a feature gate ("durable retention impossible here") no longer get the
  signal from the constructor; consult `retention_meta()` (the persisted
  `(enabled, policy)` surface) or the new `RetentionPolicy` parameter to detect
  and control durable retention. Content bytes now land in `state.db` when (and
  only when) this flag is on — see `docs/security.md` for the 0600 posture.
- **`commit_cas(..., content=None)` under retention no longer records the prior
  body under the new version.** The old behavior silently retained the STALE
  previous body as the new version's snapshot (history poisoning, observable
  only through retention reads). A `content=None` WIN now records nothing for
  the new version: `get_content_at_version(new_version)` returns `None` and
  `read_at_version(new_version)` (once it is history) rejects `not_retained`.
  Relatedly, `get_content_at_version` is now annotated `str | bytes | None` on
  both registries — bytes bodies round-trip as `bytes` (they always did on the
  in-process path; the annotation was wrong).

### Fixed

- **Watchdog late-completion phantom grant / late grant-revocation (A6).** When
  a coordinator hook handler exceeded its 4s watchdog and returned
  `degraded: true`, its work kept running in the pool and its registry mutation
  could land afterward — the agent could be left holding an `EXCLUSIVE` grant it
  never saw (and its peers silently invalidated), or a late `session-stop`
  release could revoke a grant the registry had since handed to the next
  session. This was detection-only (`watchdog_late_completion_total`). The
  mutating handlers (`pre-edit`, `post-edit`, `post-edit-cas`, `session-stop`)
  now thread a per-request abort token into `CoordinatorService.write` /
  `invalidate` / `commit` / `commit_cas`; the watchdog sets it on timeout, and a
  new `registry.abort_guard` checks it the instant the late work wins the
  registry write lock (the reentrant `RLock` that serializes all mutations),
  raising `WatchdogAbandoned` so the mutation never lands. This closes the
  dominant case (work blocked on the lock under contention). A new
  `watchdog_late_aborts_total` counter (in `/status?detail=metrics`) records
  when the guard fires. Residual (documented): a `BEGIN IMMEDIATE` that begins
  after the check and then blocks on cross-process SQLite contention is still
  observed by `watchdog_late_completion_total`; the complete fix
  (response-and-visibility atomicity) is deferred to a fencing redesign.
- **Watchdog-degraded reads no longer silently pass as verified-fresh (A7).**
  When a `pre-read` / `pre-bash` / `pre-grep` handler exceeded its watchdog
  (e.g. its task burned the budget waiting in the executor queue under SQLite
  contention), it returned `{status: "fresh", degraded: true}` with no
  `hookSpecificOutput` — so the staleness check that never ran was
  indistinguishable from a confirmed-fresh read, and the model saw no warning.
  The degraded read envelope now carries an advisory `additionalContext` (the
  hook client passes it straight through) stating that freshness is unverified
  and the file should be re-read if shared. `status` stays `"fresh"` for wire
  back-compat. (Unbounded handler concurrency was already bounded by the
  handler-concurrency semaphore + watchdog queue-depth gate; this addresses the
  remaining *silent* suppression. Coupling the per-request deadline to dequeue
  time, so queue wait doesn't consume the work budget, is a separate deferred
  improvement.)
- **Coordinator spawn/idle lifecycle hardening (L1, L3, L5).**
  - **L1 — `rm -rf .coherence/` during coordinator construction.** The
    spawn-or-join loop revalidated the `server.pid` inode before acquiring the
    flock, but an external `rm -rf .coherence/ && recreate` landing during
    `CoordinatorHTTPServer` construction (SQLite open + TCP bind, >300ms cold)
    left the winner about to write the port into an orphaned `server.pid` that
    no concurrent reader could see — losers read the recreated, port-less file
    and degraded. The winner now re-validates the inode after construction and
    immediately before writing the port; on mismatch it tears down the
    just-bound coordinator (freeing the socket) and recovers on a fresh inode.
  - **L5 — idle/uptime now use a monotonic clock.** `idle_seconds` and
    `uptime_s` were computed from `time.time()` deltas, so an NTP step or a
    suspend/resume could misfire idle shutdown early or defer it. Both now use
    `time.monotonic()`; `time.time()` is reserved for operator-facing absolute
    timestamps.
  - **L3 — thundering-herd loser degrade is now observable.** When the loop
    exhausts its inode/retry budget under a cold-start herd and returns `-1`,
    a per-reason process-lifetime counter (`get_spawn_join_exhaustion_total()`
    / `_by_reason()`) records it — surfacing the otherwise-silent degrade and
    keeping the herd-exhaustion reasons distinct from the dying-coordinator
    probe failure. The retry budget itself is unchanged (a deterministic retune
    is deferred pending real-world p99 cold-start data).

## [0.9.2] — 2026-06-11

### Fixed

- **`CoherentVolume.write_cas` on-disk lost update under high same-key
  contention.** Surfaced by the concurrent-writers demo in CI (≥5 concurrent
  `write_cas` writers): the stale-deny recovery inside `write_cas` paired the
  bytes from `reacquire()` with a version comparand resolved by a *separate,
  later* read — and `reacquire()`'s read left the re-minted identity SHARED, so
  that follow-up read hit the coordinator's fresh-SHARED pre-read branch, which
  returns the version **without** re-checking the disk bytes' hash. A peer
  commit landing between the two reads paired STALE bytes with a FRESH version;
  the commit-CAS checks only the version, so `make_content` derived from the
  stale bytes and *won* — silently dropping the peer's update on disk (the
  protocol-level `NoLostUpdate` invariant held: the version-bump count was
  right, but the persisted value was wrong). Fixed by re-minting identity
  WITHOUT a read on every retry (`_remint()`), so each attempt's comparand
  `(bytes, version)` pair comes from ONE hash-checked None-state read; a
  stale-bytes/fresh-version split is now structurally unrepresentable, and a
  lagging-disk read is denied and retried rather than committed. The denied
  read no longer counts against the commit budget (`MAX_CAS_REACQUIRES` keeps
  its documented "total commit attempts" meaning); a never-clearing view is
  separately bounded by a consecutive-denied-streak limit, which also replaces
  the previous (misdiagnosed) "coordinator may be wedged" raise — that state is
  the transient disk-write-after-commit window, and now retries. Both
  terminals are typed (`CasRetriesExhausted` / `CoherenceError`); `write_cas`
  never returns success without its update landing. Regression:
  `tests/test_concurrent_writers_demo.py::test_fixed_under_higher_contention_holds_the_honest_invariant`.
- **Pre-read fresh-path `version` dropped when a preemption notice was
  surfaced** (`/hooks/pre-read`). The notice-surfacing wrapper rebuilt the
  fresh response as a literal dict, discarding the additive `version` field
  (and the new `hash_differs` field) whenever the reader had a pending
  preemption notice. An OCC writer sourcing `expected_version` from such a
  read fell back to `0` and burned one `version_mismatch` CAS round-trip
  before retrying — fail-safe, but wasteful after any preemption (a peer
  pre-edit or the stable-grant sweep). Fixed by spreading the `work()`
  payload so additive fresh-path keys survive notice attachment; the COR-03
  single-consumer notice-drain semantics are unchanged. Regression:
  `tests/test_claude_code_coordinator_server.py::test_a1_fresh_with_notice_preserves_version_field`.

### Added

- **Fresh-SHARED hash-mismatch signal** (`/hooks/pre-read`): a SHARED holder
  whose supplied `content_hash` mismatches the recorded artifact hash now gets
  an additive `hash_differs: true` field on the fresh response, and the
  coordinator bumps a new `fresh_shared_hash_mismatch_total` counter
  (`/status?detail=metrics`). Previously the fresh-SHARED fast path returned
  the version comparand with no validation — only INVALID/None-state reads
  were hash-checked. A peer commit would have left the session INVALID, so a
  mismatch implies an out-of-band write or commit→disk-write lag; the signal
  makes that observable. Warn-mode semantics unchanged: the read is still
  allowed, the key appears only when the mismatch fires, and sentinel
  recorded hashes (`""` seeds, `"f" * 64` launch-gate injection) never fire
  it.
- **Concurrent lost-update demo** (`examples/concurrent_writers/`): a true-race
  reproduction of the v0.9.1 commit-CAS write path. Two threads update a shared
  total concurrently; `broken.py` loses an update (last writer wins over a plain
  file), `fixed.py` runs the identical race through `CoherentVolume.write_cas` and
  preserves both (the loser is told `version_mismatch`, re-mints identity +
  re-reads, and re-applies on the winner's value via its `make_content` closure).
  The rung-2 (concurrent, single-host) analog of the rung-1 sequential
  `examples/coherent_volume` demo — it surfaces the race the invalidation-deny
  model cannot catch. Offline; the fixed case spawns a local coordinator
  subprocess. Run: `python -m examples.concurrent_writers.main`. Tests:
  `tests/test_concurrent_writers_demo.py`.

## [0.9.1] — 2026-06-10

Two write-correctness mechanisms land, both model-checked with TLA⁺: an
**optimistic commit-CAS write path** (`NoLostUpdate`) and a **read-generation
fence** (`NoStaleApply`).

**Optimistic concurrency (commit-CAS).** `CoordinatorService.commit_cas` and
`AgentRuntime.write_cas` commit a write only if the artifact version still
equals the version the writer read. Concurrent same-key writers resolve to one
winner; losers get a typed, retryable `ConflictDetail` (`version_mismatch` /
`other_holder`) instead of a silent lost update. The cross-process path ships
as the coordinator's `/hooks/post-edit-cas` endpoint plus
`CoherentVolume.write_cas` (bounded reacquire-and-retry, fail-closed). A caller
invalidated between its read and its CAS gets the stable
`caller_in_transient_state` reason — a lost race, not corruption. Spec:
`formal/tla/OCC.tla`.

**Read-generation fence.** Closes the reclaim-zombie window a version check
cannot see: a writer whose grant was evicted (crash-recovery sweep or the
transient-timeout fail-safe) can no longer land a commit while the version is
unchanged. Each artifact carries an `owner_generation` bumped on every
coordinator-side eviction; every claim captures a server-side
`read_generation`; both commit paths reject a superseded claim atomically with
the version persist — `StaleReadGeneration` on the pessimistic path,
`ConflictDetail` (`stale_read_generation`) on the CAS path; both retry-eligible
after a fresh read. No public write API accepts a generation argument
(CI-enforced). The SQLite schema gains the fence columns additively: pre-fence
databases upgrade in place on open, and older binaries can still open upgraded
databases. Spec: `formal/tla/Fencing.tla`.

**`CoherentVolume` fixes.** `write()` no longer skips the disk write when its
cached hash matches but the on-disk bytes diverged; and the EXCLUSIVE grant is
released on any error in the post-grant window, so an `OSError` mid-write can
no longer orphan a grant.

**Hardening and docs.** The replay recorder creates its session directory with
mode `0o700`. The README now leads with the verified guarantee set, and
`formal/tla/README.md` documents all four specs (MESI, CrashRecovery, OCC,
Fencing) with CI budgets and mutant-testing recipes.

## [0.9.0] — 2026-06-07

The first minor release since the v0.8 series. Three themes: the crash-recovery
default flips **ON**, a new **CoherentVolume** shared-workspace adapter, and a
temporal source-drift **cost benchmark**.

**Crash-recovery default flips ON.** Completes the deprecation cycle begun in
v0.8.3: `CrashRecoveryConfig().enabled` changes from `False` to `True`, so a bare
`CCSStore()` / `CoherenceAdapterCore()` now reclaims stale `MODIFIED`/`EXCLUSIVE`
grants automatically. Operators who depend on the v0.8.x default-disabled behavior
**must pass `CrashRecoveryConfig(enabled=False)` explicitly** to opt out.

### Changed

- **Breaking default — crash recovery is now ON.** `CrashRecoveryConfig.enabled`
  flipped `False` → `True`. Bare `CrashRecoveryConfig()`, `CCSStore()`, and
  `CoherenceAdapterCore()` now run the reclamation sweep. **Migration to keep
  v0.8.x behavior: pass `CrashRecoveryConfig(enabled=False)` explicitly.**
- **Default thresholds retuned** from simulation-anchored to batch-tick-realistic
  values: `heartbeat_timeout_ticks` 10 → 120, `max_hold_ticks` 1000 → 900. The old
  values remain settable explicitly. Calibrated so a bare `CCSStore` under realistic
  LLM workloads does not false-reclaim live agents. Benchmark token reductions are
  unchanged — see [`benchmarks/results/v0.9.0/attestation.md`](benchmarks/results/v0.9.0/attestation.md).
- **Migration caveat — `lease` strategy with `lease_ttl_ticks` ≥ 900.** Because the
  default is now enabled, the R11 composition rule (`max_hold_ticks` must exceed the
  strategy's inspectable lease TTL) is enforced at construction for bare `CCSStore()`
  / `CoherenceAdapterCore()`. A `lease` strategy with `lease_ttl_ticks` ≥ 900 (the new
  default `max_hold_ticks`) now raises `ValueError` at startup where v0.8.x silently
  skipped the check. Fix: pass `CrashRecoveryConfig(max_hold_ticks=…)` above your lease
  TTL, or `CrashRecoveryConfig(enabled=False)`.
- **Breaking — state-log byte-identity inverted (direction only).** A state-log produced with
  the `crash_recovery` block omitted is now byte-identical to one with an explicit
  `{enabled: true}` block, and diverges from `{enabled: false}`. CI that gates on
  state-log byte equality against v0.8.x output must now set `enabled=False`
  explicitly. The contract itself is unchanged — only which default it maps to.

### Added

- **Rate-limited reclamation sweep wired into `CoherenceAdapterCore`.** `read()` /
  `write()` invoke a thread-safe `_maybe_sweep(now_tick)` after recording the
  heartbeat; it reclaims stale grants at most once per `heartbeat_timeout_ticks // 2`
  ticks. `CCSStore.batch()` inherits once-per-batch sweep semantics from its shared
  per-batch tick — no separate state. The sweep is best-effort: a failure is logged
  and never crashes the adapter's read/write path.
- **Per-instance reclamation diagnostic.** The first time an adapter instance
  reclaims, it logs a one-shot `WARNING` on the `ccs.adapters.base` logger with
  structured `extra` fields (`trigger`, `agent_id_short`, `artifact_id_short`,
  `reclaim_count`); a companion `DEBUG` carries full UUIDs. Field names are stable
  for the v0.9 series. See the [crash-recovery guide](docs/guide.md#crash-recovery).
- **Transitional first-use warning.** The first `CrashRecoveryConfig` construction
  per process emits a one-shot `RuntimeWarning` naming the default change — a
  migration heads-up for anyone upgrading straight from v0.8.2 who skipped the v0.8.3
  `DeprecationWarning`. Removed in v0.10.0.
- **`CoherentVolume` — sequential coherence for a shared agent workspace**
  (`ccs.adapters.CoherentVolume`, plus `coherent_workspace()` / `install()` /
  `uninstall()`). An out-of-process coordinator client that gives multiple agents on
  one host, sharing files in a workspace, a coherent `read` / `write` / `reacquire`
  surface over the shipped local-HTTP coordinator (strict-mode `INVALID`-deny over
  SQLite-WAL). `write()` acquires EXCLUSIVE or **fails closed** when a peer commit has
  invalidated the writer; `reacquire()` recovers via a fresh identity + mandatory
  fresh read; writes are atomic (`tmp → fsync → os.replace`). An opt-in `install()` /
  `coherent_workspace()` shim patches `open()` / `pathlib` for no-call-site-change
  coordination (demo-grade). Scope: prevents stale-overwrite lost updates
  (single-spawner, sequential-conflict); concurrent-write serialization and
  multi-host are out of v1. Demo: `python -m examples.coherent_volume.main`.
- **Temporal source-drift cost benchmark.** A simulation-based, cost-only measurement
  (no LLM, no correctness oracle) of how many re-fetches/re-embeds coherence-gating
  avoids when an external source changes between an agent's turns, swept over
  change-rate × answer-sensitivity. Adds a flag-gated source-mutation step to
  `SimulationEngine` (default off; byte-identical when off, dedicated RNG),
  `BlindCacheStrategy` (the never-refresh cost floor), `source_refetches` /
  `wasted_refetches` metrics, `tools/run_cost_sweep.py`, and a CI drift gate
  (`make cost-benchmark-check` against `benchmarks/expected_cost.json`).

### Removed

- The v0.8.3 deprecation machinery: the falsy `_DefaultEnabledSentinel`, the
  bare-construction `DeprecationWarning`, and the internal `_default_disabled_config`
  helper. Bare `CrashRecoveryConfig()` is safe again — it now means "enabled".

## [0.8.4.3] — 2026-06-06

A patch release completing the `ccs-diagnose` heatmap report improvements
started in v0.8.4.2. No API, core-protocol, or adapter changes.

### Changed

- **`ccs-diagnose` Per-Artifact Heatmap note now bridges the two ranking
  criteria.** When the highest-rework artifact (Section 2, "The Event That
  Matters Most") differs from the highest-coordination-signal artifact
  (Section 3, heatmap row-1), a new sentence in the heatmap note explains
  that Section 2 ranks by rework impact (`divergent_reads`) while Section 3
  ranks by multi-writer coordination signal. Prevents reader confusion when
  comparing the two panels in a shared report.
- **Sort-key secondary difference documented.** `_build_heatmap_display_rows`
  and `ownership._row_sort_key` share the multi-writer-first top bucket; the
  secondary sort keys differ intentionally (`-divergent_reads` vs
  `-total_reads`). The docstring now calls this out explicitly, and a new
  machine-checked regression test pins the shared invariant.
- **Minor template cleanup.** The writer-count display cell's redundant
  `>= 1` outer guard simplified to `> 0`.

## [0.8.4.2] — 2026-06-06

A patch release improving the `ccs-diagnose` HTML report's Per-Artifact
Heatmap. No API, core-protocol, or adapter changes.

### Changed

- **`ccs-diagnose` Per-Artifact Heatmap ranks multi-writer artifacts first.**
  The heatmap previously ranked purely by `divergent_reads`, which surfaced
  single-writer artifacts — whose high `share` is expected pipeline ordering
  (readers handed the pre-write value) — above genuine *multi-writer*
  artifacts, the actual coordination signal. Display rows are now re-ranked
  multi-writer-first (mirroring the Ownership Map), gain a `writers` column
  with a `multi-writer` / `pipeline ordering` flag, and multi-writer rows are
  highlighted. This is a presentation-only re-rank at the render layer; the
  detection-layer ordering that drives the top-event callout and
  `_pick_top_event` is unchanged. Follows up the v0.8.4.1 `share` fix.

## [0.8.4.1] — 2026-06-05

A patch release fixing a display bug in the `ccs-diagnose` HTML report. No
API, core-protocol, or adapter changes.

### Fixed

- **`ccs-diagnose` Per-Artifact Heatmap `share` could exceed 100%.** The
  heatmap counted headline divergence *events* — which are ordered read
  *pairs* (`O(n²)`) — against the read *count*, so an artifact written late
  and read by many downstream nodes rendered shares like 600%, overflowing
  the CSS bar. `HeatmapRow.divergent_reads` now counts the distinct reads
  handed a divergent version (the `later_read` of ≥1 headline event), which
  is a subset of `total_reads`, so `share` is bounded to `[0, 100%]`. The
  report template also clamps the bar width and labels the column's
  witness-quality meaning. The headline-event count is unchanged and still
  drives the Reader-Pair Matrix.

## [0.8.4] — 2026-06-02

A patch release that adds the experimental OpenAI Agents SDK integration and
two packaging/UX fixes. The OpenAI Agents adapter is **experimental (0.x)** and
tracks the SDK's own 0.x surface; it brings coherence to the SDK `Session`
cache (the Q6 probe found the OpenAI and Mistral Conversations *servers*
read-after-write consistent, so the coherence value lives on the readers'
caches, not the server). No changes to the core protocol, the existing
LangGraph / CrewAI / AutoGen adapters, or the v0.8.3 crash-recovery deprecation
cycle — the v0.9.0 default flip is still the next behavioral change.

### Added

- **OpenAI Agents SDK coherence adapter (experimental, 0.x).**
  `OpenAIAgentsAdapter` (`ccs.adapters.openai_agents`, re-exported from
  `ccs.adapters`) brings coherence to the OpenAI Agents SDK. Two surfaces:
  `wrap_session(...)` composes over the SDK `Session` four-method protocol
  (`get_items` / `add_items` / `pop_item` / `clear_session`) and returns a
  drop-in `CoherenceSession` that invalidates peers on mutation and exposes
  `peer_mutated_since_read()`; `run_hooks(...)` returns a `RunHooks` that tracks
  the active agent across handoffs and refreshes coherence at agent-start /
  tool-start. Constructor parity with the other adapters (`strategy_name`,
  `core`, `crash_recovery`, `on_error`) plus `heartbeat` / `recover`; scope is
  in-process multi-agent (v1). The coherence target is the **Session cache**, not
  the Conversations server — the Q6 probe found the server consistent. See the
  [user guide](docs/guide.md#openai-agents-sdk-adapter-experimental).
- **New install extras:** `openai` (Conversations client + httpx), `openai-agents`
  (the adapter; pinned `>=0.17,<0.18`, composes `openai`), and `mistral`. `[all]`
  now includes `openai-agents` and `mistral`.
- **Conversations stale-read example** (`examples/conversations_stale_read/`): a
  deterministic, offline, no-keys reproducer of client-cache staleness over a
  consistent store, plus a live Q6 consistency probe (`probe.py`). The probe
  measured the OpenAI and Mistral Conversations servers read-after-write
  consistent (0 stale over 100 + 20 trials), which is why the demo isolates the
  client cache rather than the server.
- `CoherenceTopologyWarning` (`ccs.core.exceptions`, re-exported from
  `ccs.adapters`): emitted once when a server-side `conversation_id` is combined
  with a handoff, where the SDK disables handoff-history rewriting.
- `live_api` pytest marker for the paid OpenAI/Mistral live tests; excluded from
  the default `pytest -q` run (offline and free by default).

### Fixed

- The `otel` extra now also installs `opentelemetry-sdk`. The API package alone
  no-ops without an SDK, so OpenTelemetry metrics were not actually collected or
  exported when installing only `agent-coherence[otel]`.
- `agent-coherence-status` keeps the version column inline and prints a legend,
  fixing the wrapped/ambiguous status output.

## [0.8.3] — 2026-05-30

**First behavioral default-flip in the library's history.** v0.8.3 is a
deprecation-only release: it adds a one-shot `DeprecationWarning` to bare
`CrashRecoveryConfig()` construction announcing the v0.9.0 default flip
from `enabled=False` to `enabled=True`. **No behavior changes ship in
v0.8.3.** Downstream consumers get one release cycle to surface false-
reclaim issues under their own workloads before the flip lands.

This is novel for this repo — the only prior deprecation precedent
(`coordinator_uptime_s` rename + alias in v0.8.0) was a field rename, not
a behavior default change. Operators who depend on the v0.8.x
default-disabled behavior have two clear paths to silence the warning:

- **Recommended migration**: pass `CrashRecoveryConfig(enabled=True)` to
  opt in now and surface any false-reclaim issues under your workload
  before v0.9.0 ships.
- **Preserve current behavior**: pass `CrashRecoveryConfig(enabled=False)`
  explicitly. Crash recovery stays off; the warning stays silent.

The migration lands across two releases: v0.8.3 ships this deprecation
notice; v0.9.0 will flip the default and wire the crash-recovery sweep.

### Changed

- **Behavior change preview — v0.9.0 will flip
  `CrashRecoveryConfig().enabled` from `False` to `True`.** v0.8.3
  emits `DeprecationWarning` exactly once per process on the first
  bare `CrashRecoveryConfig()` construction. The warning names both
  silence paths (`enabled=True` opt-in or `enabled=False` opt-out) and
  the target release.
- Composition rule (R11) is unaffected: explicit
  `CrashRecoveryConfig(enabled=True, max_hold_ticks=…)` continues to
  validate against the longest inspectable strategy lease TTL via
  `validate_crash_recovery_config`. v0.9.0 will additionally retune
  `heartbeat_timeout_ticks` and `max_hold_ticks` from sim-anchor values
  (10 / 1000) to batch-tick-realistic defaults (120 / 900).

### Internal

- `CrashRecoveryConfig` distinguishes bare construction from explicit
  `enabled=False` via a *falsy* module-level sentinel default and a
  `__post_init__` normalization step that uses `object.__setattr__` to
  satisfy the `frozen=True` constraint. The sentinel is deliberately
  falsy so that any path which skips normalization — `importlib.reload`
  rebinding the module sentinel (gunicorn/uvicorn `--reload`, Jupyter
  autoreload), or a subclass `__post_init__` that omits `super()` —
  still reads as disabled rather than silently activating the sweep.
- The deprecation signal fires at most once per process (a thread-safe
  emit-once guard) on **two** channels: the `warnings` system *and* a
  WARNING-level log record on the `ccs.coordinator.service` logger. The
  second channel ensures the migration signal survives CPython's default
  `DeprecationWarning` filter, which suppresses warnings raised from
  non-`__main__` importers — i.e. virtually every SDK consumer. The
  sentinel, the guard, and the dual-channel emit are all removed in
  v0.9.0.
- A library-internal helper (in `ccs.coordinator.service`) lets
  library code paths (`ccs.simulation.engine`, `ccs.adapters.base`)
  construct the v0.8.x default-disabled config object without
  surfacing the deprecation warning to users. Removed in v0.9.0.
- Architecture-level regression gate
  (`tests/test_architecture.py::test_no_bare_crash_recovery_config_construction_in_src`)
  asserts no bare `CrashRecoveryConfig()` call sites exist in `src/`.
  Catches accidental re-introduction in future patches.

## [0.8.2] — 2026-05-28

Consolidated patch release covering both the v0.2 strict-mode track
(landed earlier on dev) and the D v1 LangGraph cycle replay tooling +
ce:review gated cluster (shipped to dev 2026-05-27 → 2026-05-28). Both
tracks are additive: new wire fields for v0.2 strict mode AND a new
CLI surface (`agent-coherence-replay`) + new module (`src/ccs/replay/`).
The `coordinator_uptime_s` deprecation alias from the `0.8.0` AC-02
plan stays in place through `0.8.x`; its removal continues to be
targeted for a future minor bump per the original SemVer commitment.

### Added — D v1 LangGraph cycle replay tooling (2026-05-27 → 2026-05-28 on dev)

- **`agent-coherence-replay` console script** — invariant replay CLI
  that walks a captured coordinator session and reports breaches of
  the **Core 4 invariants** (single-writer, monotonic-version,
  stale-read, lost-write). Five flags: `--json`, `--invariant <name>`
  (repeatable, choices: `single-writer` / `monotonic-version` /
  `stale-read` / `lost-write`), `--quiet`, `--include-ambiguous`,
  `--ambiguous-threshold N` (default 10). Five exit codes:
  - `0` — clean OR all SKIPPED reasons opted out via manifest `streams=`
    (also: `BrokenPipeError` — consumer closed the pipe early)
  - `1` — ≥1 CONFIRMED invariant breach
  - `2` — ≥1 SKIPPED for a stream declared but absent on disk (capture-
    side bug)
  - `3` — trace error (`ManifestMissingOrUnreadableError`,
    `MultiInstanceTraceError`, `TraceCorruptionError`,
    `SessionDirectoryNotFoundError`)
  - `4` — internal error (uncaught exception; CLI bug — file an issue)
- **`CCSStore.record_to(path, *, streams=None, **kwargs)` classmethod
  context manager** — one-line LangGraph capture. Writes
  `manifest.json` + per-stream JSONL (`state_log.jsonl`,
  `content_audit_log.jsonl`) to `path`. Mandatory `streams=` opt-out
  for compliance-constrained partners
  (`streams={'state_log'}` produces a state-log-only trace; stale-read
  invariant emits `INVARIANT SKIPPED — content_audit_log not captured`
  at replay). `manifest.json` carries `schema_version: 0` (explicitly
  unstable until the 30-day partner-feedback retro).
- **`record_callbacks(path, *, accept_unverified=False, ...)` helper**
  in `ccs.replay.recorder` — low-level entry point for direct
  `CoherenceAdapterCore` callback wiring (CrewAI / AutoGen). Raises
  `UnverifiedAdapterCaptureError` unless `accept_unverified=True` is
  passed; emits a stderr opt-in warning naming the D+1 smoke-test
  roadmap item. `CCSStore.record_to` sets the flag automatically
  (CCSStore is the verified adapter in v1).
- **`src/ccs/replay/` module** — new package: `recorder` (capture
  context manager + `RecordingSession`), `loader` (streaming JSONL
  loader + heap-merge by `(tick, stream_kind, sequence_number)`,
  detects `MULTI_INSTANCE_TRACE` and `TRACE_CORRUPTION_DUPLICATE_SEQ`),
  `predicates` (Core 4 invariant checkers + AMBIGUOUS classification
  for same-tick read/commit collisions + SKIPPED dispatch for missing
  streams), `formatters` (human + JSON emit, NDJSON schema in
  `--json` mode), and `errors` (`ReplayError` base with two-tier
  semantic split: `ReplayConfigurationError` for API misuse,
  `ReplayTraceError` for trace structural defects — `_TRACE_ERRORS`
  tuple in the CLI handler is now `(ReplayTraceError, OSError)`).
- **`CoherenceAdapterCore` public introspection** —
  `agent_names_snapshot()` and `artifact_names_snapshot()` return
  fresh `dict[UUID, str]` copies for downstream consumers
  (replay-recorder manifest finalization uses these instead of
  reaching into private attributes).
- **Capture-time safety** — `RecordingSession.__enter__` refuses if
  `session_dir/manifest.json` already exists
  (`SessionDirectoryNotEmptyError`) to prevent silent
  multi-coordinator-instance interleave; `__enter__` also wraps the
  manifest write in try/except so opened stream writers don't leak
  fds on disk-full / permission-error failures.
- **AMBIGUOUS classification for stale-read** — same-tick
  read/commit collisions emit `STALE_READ_AMBIGUOUS` (suppressed from
  per-finding output by default; count always reported in summary).
  `--include-ambiguous` opts in; `--ambiguous-threshold N` triggers a
  prominent summary callout when exceeded.
- **`--json` error envelope** — when `--json` is active and a
  trace error fires (exit 3), stdout receives one final NDJSON line:
  `{"kind":"error", "exit_code":3, "exception":"<ClassName>", "message":"..."}`.
  Keeps stdout self-contained for `--json` consumers. Stderr prose
  retained for human log tailing.
- **`docs/guide.md` §Replay (v0.8.2+)** — LangGraph quickstart +
  CLI surface reference + machine-readable output schema description.
- **Tests** — 79 new tests across `tests/test_replay_recorder.py`,
  `tests/test_replay_loader.py`, `tests/test_replay_predicates.py`,
  `tests/test_replay_errors.py`, `tests/test_cli_coherence_replay.py`,
  and `tests/integration/test_replay_e2e.py` (incl. a real-LangGraph
  fixture e2e test). Suite at dev tip: 1451 passed, 2 skipped,
  architecture check clean.

### Added — v0.2 strict mode (Python coordinator)

- **Per-artifact strict-mode opt-in** via `.coherence/strict_mode.yaml`
  (KTD-O). An artifact is strict iff its path matches both the
  `tracked_paths` set AND the new `strict_mode_paths` globs. Empty
  strict_mode_paths preserves v0.1.1 warn-mode for every artifact.
- **Handler decision-flip in all 4 PreToolUse handlers** (Read,
  Edit/Write, Bash, Grep) — `permissionDecision: "deny"` with the
  static reason template `STRICT_MODE_DENY_REASON_TEMPLATE` (KTD-P)
  fires when (strict + tracked + invalidated). First-time observers
  (state None on existing artifact) fall through to warn-mode allow
  per the semantic refinement during implementation.
- **`TERMINAL_DENIAL_CLASSES` security invariant** (KTD-U) — module-
  level `frozenset` enumerating denial classes that must never be
  converted to `permissionDecision: "allow"`. All 6 allow-emission
  call sites route through `emit_allow()` which asserts the invariant;
  AST-based meta-test grep-counts call sites in `coordinator_server.py`
  + `hook_payloads.py` so a future contributor adding a new allow
  path is forced to extend the parameter list.
- **`agent-coherence-migrate-deny` CLI** (KTD-R) — stricter sibling
  to `agent-coherence-migrate-rules`. STDOUT-only (never writes to
  settings.json), symlink-contained (canonical-path containment check),
  never invokes an LLM, never reads files outside resolved workspace
  root. Under-emit bias: only canonical phrasings trigger.
- **Strict-mode telemetry** (KTD-V minimal + KTD-J extension) —
  `strict_mode_denials_total`, `strict_mode_routed_around_via_bash_total`
  (Phase 0 H4 routing pattern detector with 30s window),
  `audit_log_mode_drift_total` counters surfaced via
  `/status?detail=metrics`. Minimal deny-only audit log appended as
  JSONL to `.coherence/audit.log` (mode 0o600, no schema_version, no
  command bodies, no user content).
- **Cross-implementation protocol corpus** (Unit 7) —
  `tests/protocol_corpus/` harness + 12 warn-mode + 8 strict-mode
  fixtures + opt-in `protocol_corpus` pytest marker + new
  `protocol-corpus` CI job. Catches Python ↔ Node coordinator
  wire-shape drift before it ships. Strict-mode fixtures are
  python-only (Node coordinator doesn't ship strict mode in v0.2).

### Changed

- **Hook payload builders** (`build_stale_response`,
  `build_collision_response`) now route through `emit_allow()` per
  the KTD-U structural invariant.
- **Static deny-reason text** for strict-mode replaces v0.1.1's
  per-invocation-varying warn-mode prose. Phase 0 H1 falsification
  inverted the original "varied text bounds retries" hypothesis on
  opus; static text byte-identical across retries is the right shape.

### Plugin compatibility

- v0.2 of the [agent-coherence-plugin](https://github.com/hipvlady/agent-coherence-plugin)
  consumes this library via its broad-beta launch package (plan Units
  8-11). The Node coordinator does NOT ship strict mode in v0.2 —
  strict-mode workspaces must use `coherence.coordinator_backend = "python"`.

## [0.8.1] — 2026-05-27

Single-fix patch.

### Fixed

- **`agent-coherence-track` / `-untrack` reject absolute paths.** The CLIs
  now normalize absolute paths that fall inside the workspace root to
  workspace-relative form before applying the server-side validator.
  Previously the validator rejected absolute paths outright, requiring
  callers to strip the workspace prefix manually even for paths the
  workspace clearly owns. Tracking paths outside the workspace remains
  rejected as before. (Equivalent fix landed on dev as commit `10f1e16`
  during the v0.2 strict-mode track; this 0.8.1 release ships the
  patch from main without dragging in the strict-mode work-in-flight.)

## [0.8.0] — 2026-05-23

**Stable release of the Claude Code plugin coordinator backend.** Promotes the `0.8.0a1` alpha pre-release to a final `0.8.0` after the v0.1.1 marketplace cohort + full ce-review remediation pass landed. Both the coordinator HTTP surface and the wire contract are now considered stable through the `0.8.x` minor line; breaking changes will bump to `0.9.0` per SemVer.

### Added — Marketplace cohort (Phase A–C of v0.1.1 plan)

- **Unit 4 watchdog hardening** — KTD-K `busy_timeout=1500ms` per multi-statement transaction analysis; KTD-N `/hooks/pre-bash` + `/hooks/pre-grep` with shlex-based path detection (closes the model-routing-around-Read H4 finding); KTD-G handler concurrency semaphore + queue-depth gate + three saturation counters.
- **Unit 5 lifecycle hardening** — KTD-H inode revalidation per retry (handles `rm -rf .coherence/ && recreate` mid-spawn races); KTD-I in-flight handler drain on `shutdown()` (5s deadline before SQLite close); KTD-L3 cold-start instrumentation surfaced via `coordinator.cold_start_duration_ms`.
- **Unit 6 residual risk fixes** — R10 `_agent_names` lock + public accessors; R11 `ensure_secret` bounded `O_EXCL` retry (fail-closed); R12 `/status` three-tier disclosure (`minimal` / `metrics` / `full` with `Coherence-Local-Operator: true` opt-in for the elevated tier); R14 `_append_policy_yaml` `fcntl.flock` discipline; R21 `MAX_REQUEST_BODY_BYTES = 64 KB` cap.
- **Unit 8 KTD-J telemetry** — per-endpoint counters (pre_read/pre_edit/post_edit/session_stop/pre_bash/pre_grep/policy_track/policy_untrack/status_total); product-signal counters (`intra_task_acquire_release_total`, `stale_warning_emitted_total`, `stale_warning_reread_total`); free-threading-safe via `threading.Lock`. New `coordinator_uptime_seconds` field (canonical) + `coordinator_uptime_s` deprecated alias for one release. `coordinator_backend` + `coordinator_version` fields for cross-backend dashboards.
- **Unit 8 `--self-test` smoke** — `agent-coherence-status --self-test` runs a four-step pre-read → pre-edit → post-edit → stale pre-read scenario against a live coordinator. Exit 0 on pass, 3 with actionable diagnostic on fail. Documented as the post-install validation step.
- **Unit 8 `--prepare-for-migration`** — `agent-coherence-coordinator --prepare-for-migration` enters a draining state that rejects new pre-edit (HTTP 503 with structured `migration in progress` error visible to the model), waits up to 5s for in-flight chains to complete, invalidates remaining M/E grants, then schedules shutdown. Eliminates the silent data-loss race when switching Python↔Node backends.
- **Unit 9 `agent-coherence-migrate-rules`** — scans CLAUDE.md for prose tool-class rules ("use rg, not grep", "never sudo") and proposes `permissions.deny` entries. Flag-only by default; `--apply` writes to `.claude/settings.local.json` after confirmation.

### Added — Stable-grant sweep preemption notice (ADV-004)

`enforce_stable_grant_timeouts` now records a preemption notice for every reclaimed agent (using a sentinel `SWEEP_RECLAMATION_PREEMPTER_ID`). When the victim's post-edit eventually arrives, the F4 enrichment path emits "your M/E grant was reclaimed by the coordinator sweep (heartbeat timeout or max-hold ceiling)" instead of a generic `CoherenceError` — eliminates a silent data-loss class for the alpha cohort's interactive workflows.

### Changed

- **`/status?detail=metrics`** is now the canonical surface for dashboard scrapers. The metrics-tier stability contract (additive in minor, removed only in major after one-release deprecation alias) is documented on `_handle_status`.
- **`StaleResponse` / `FreshResponse` / `PolicyUntrackResponse` TypedDicts** now match the actual wire shapes (AC-04/06/08 alignment).
- **`_run_or_degrade` accepts `degraded_response`** so `{ok:bool}`-shape endpoints (pre-edit, post-edit, session-stop) return `{ok:True, degraded:True}` on watchdog timeout instead of the `{status:fresh}` envelope used by pre-read shapes (AC-05).
- **`coordinator_uptime_s` field renamed to `coordinator_uptime_seconds`** per KTD-J `_seconds` convention. Old name kept as a deprecated alias through `0.8.x`; removal targeted for `0.9.0`.
- **Shutdown ordering** — `_drain_in_flight` now runs BEFORE `_server.server_close()` (COR-01); `_seq` rollback now fires on COMMIT failure (COR-02); shutdown wall-clock can exceed `IN_FLIGHT_DRAIN_TIMEOUT_SEC` when watchdog timeouts fire (COR-07; documented bound).

### Fixed

- **`resolve_or_register` re-fetch race** (COR-04) — concurrent `remove_artifact` between ROLLBACK and re-fetch now raises an informative `RuntimeError` chained from the original `IntegrityError` instead of an opaque "UNIQUE constraint failed" trace.
- **`artifact_names_under_prefix` TOCTOU** (REL-08) — combined the LIKE-prefix and exact-match queries into a single UNION under one lock.
- **G4 abort wedge** (REL-06) — added `shutdown_abort_count` on `_SpawnedEntry`; after 3 consecutive `coordinator.shutdown()` raises, escalate by releasing the flock + marking shutdown_done so a fresh spawn can proceed.
- **Hook secret rotation race** (ADV-003) — coordinator emits operator-visible WARNING on every 401 (with 60s dedupe) plus a new `auth_401_total` counter so silent auth failures become observable.
- **Plugin `hooks.json` Bash + Grep matchers** (cross-repo P0) — the plugin now actually invokes `/hooks/pre-bash` and `/hooks/pre-grep` rather than leaving the endpoints runtime-inert (companion plugin `v0.1.1`).

### Security

- **R12 `/status` disclosure tiers** make pasting `?detail=metrics` into bug reports safe — no absolute paths, no PIDs, no session identifiers. The `full` tier still exposes those but only with the `Coherence-Local-Operator: true` opt-in header (defense-in-depth within the Adversary 1 boundary).
- **`MAX_REQUEST_BODY_BYTES` cap** (R21) — coordinator rejects oversized request bodies before `rfile.read` so a hostile or buggy client cannot OOM the coordinator with a single oversized POST.
- **`ensure_secret` bounded retry** (R11) — fail-closed if the empty-file recovery branch can't acquire `O_EXCL` within 5 attempts, instead of silently `O_TRUNC`-overwriting a concurrent racer's valid secret.
- **`MIGRATION_DRAIN_TIMEOUT_SEC = 5.0`** — backend-switch operator path now refuses new writes during drain instead of relying on the prior 100ms scheduled-shutdown race.

### Internal

- **78 ce-review findings remediated** across 12 reviewer categories (adversarial, correctness, api-contract, reliability, kieran-python, maintainability, performance, project-standards, security, testing, agent-native, learnings). KP-3/KP-11/M-01/M-06 (large handler / file extractions) explicitly deferred with rationale documented in PR bodies.
- **PERF-1 `/status` batched snapshot** — `SqliteArtifactRegistry.status_snapshot()` collapses the per-artifact `2N` SELECTs into 2 SQL queries held under one lock.
- **PS-01..PS-04 risk-code test prefix audit** — `test_a4_*`, `test_a6_*`, `test_a7_*`, `test_a8_*`, `test_l1_*`, `test_l2_*` all present per the v0.1.1 plan's invariant naming policy.

## [0.8.0a1] — 2026-05-17

**Alpha pre-release.** This is the first release containing the Claude Code
plugin work (Phases 0 through F of the v0.1 plan). Packaged as a pre-release
(`a1`) so existing `pip install agent-coherence` users on the 0.7.x line are
not silently upgraded to a build whose entry points target a new use case.

To install: `pip install agent-coherence>=0.8.0a1` or
`pip install agent-coherence --pre`.

### Added — Claude Code plugin (v0.1 alpha)

The plugin lives at [hipvlady/agent-coherence-plugin](https://github.com/hipvlady/agent-coherence-plugin)
and depends on this library for the coordinator backend. New library entry
points wired in this release:

- **`agent-coherence-coordinator`** — lazy-spawn the per-workspace HTTP
  coordinator. Forks a detached subprocess via `subprocess.Popen` with
  `start_new_session=True` so the coordinator survives the launching
  shim's exit. Used by the plugin's `SessionStart` hook.
- **`agent-coherence-status`** — print tracked artifacts × per-session
  MESI states + policy summary. Backs `/agent-coherence status`.
- **`agent-coherence-track <path>...`** / **`agent-coherence-untrack <path>...`**
  — append paths to `.coherence/tracked.yaml` / `.coherence/ignored.yaml`
  and reload the live policy. Path-traversal validation matches the
  underlying TrackedArtifactPolicy.
- **`agent-coherence-hook-client {pre-read|pre-edit|post-edit|session-stop}`**
  — command-type hook handler that reads CC's hook payload from stdin,
  resolves the coordinator port + bearer from `.coherence/`, POSTs to the
  appropriate endpoint, forwards the response to stdout. Required because
  Claude Code v2.1.131's hooks.json schema validator rejects URL templates
  containing `${COHERENCE_PORT}` at LOAD time (Phase E.0 probe 2A finding),
  so HTTP-type hooks with templated URLs are not viable.

### Added — core library

- **`src/ccs/coordinator/sqlite_registry.py`** — `SqliteArtifactRegistry`,
  a drop-in replacement for `ArtifactRegistry` that persists state to
  SQLite-WAL across coordinator restarts. Preserves the 22-method public
  surface plus three plugin extensions: `resolve_or_register` (KTD-9
  first-observation seeding), `artifacts_held_by_agent` (KTD-11 Stop
  release), `evict_stale_notices` (F2 orphan-notice TTL eviction).
  Schema includes a `pending_notices` table for cross-session
  preemption surfacing.
- **`src/ccs/adapters/claude_code/`** — coordinator HTTP server,
  resolver, policy, auth, lifecycle, and hook payload contracts.
  ~2,800 lines net new, all gated by the existing architecture-layer
  rules (`tools/check_architecture.py` enforces).

### Added — tests

- `tests/test_claude_code_coordinator_server.py` — 63 tests including
  boundary validation, A1 preemption-notice surfacing, F1-F5
  hardening regression tests.
- `tests/test_claude_code_lifecycle.py` — 15 tests including the
  load-bearing 10-process race test (multiprocessing.Pool) and the
  G3/G4/G5/G6 hardening regression tests from the post-Unit-5
  adversarial review.
- `tests/test_claude_code_cli.py` — 21 tests covering all four CLI
  scripts including the detached-subprocess regression that the
  manual smoke surfaced (`8015f80`).
- `tests/test_claude_code_hook_client.py` — 12 tests for the
  command-type hook bridge.
- `tests/test_claude_code_contract.py` — 16 tests driven by real CC
  v2.1.131 stdin payloads recorded in `tests/fixtures/cc_hook_stdin/`.
  CI early-warning system for Claude Code version drift.
- `tests/test_claude_code_e2e.py` — 15 tests for bootstrap permissions,
  KTD-12 shared-secret auth (401/200/401), DNS-rebinding mitigation,
  KTD-13 state.db schema verification, coordinator-down graceful
  degradation, and subprocess-spawn integration.
- `tests/integration/test_warn_mode_behavior_change.py` + 40 scenarios —
  R7 hard-launch-gate harness (`@pytest.mark.launch_gate`). 4 categories
  × 10 scenarios × 10 phpmac-shape variants. Operator-runnable via
  `pytest -m launch_gate` (~$1.60, ~3 hours per N=40 run).

Total: 1101 passing, 2 skipped, 2 launch_gate deselected by default.

### Fixed — Claude Code plugin v0.1 hardening

- **A1 preemption notices** (`a76597a`) — F1 Stop hook pops + surfaces
  pending notices (canonical phpmac case where X never fires another
  pre-event); F2 orphan eviction with TTL; F3 10KB prose cap with
  newest-first coalescing; F4 single-consumer pop on post-edit
  failure; F5 UPSERT ordering uses wall-clock not commit-order.
- **Unit 5 lifecycle hardening** (`e545a4a`) — G2 self-probe budget,
  G3 entry short-circuit, G4 abort-on-shutdown-raise, G5 reorder (drop
  port BEFORE coordinator.shutdown), G6 per-coordinator shutdown mutex,
  G8 Windows ImportError guard, G9 retry budget bumped 30 → 60.
- **Unit 6 detached coordinator** (`8015f80`) — `agent-coherence-coordinator`
  now forks a detached subprocess so the coordinator survives the
  launching shim's exit. Previously the daemon-thread coordinator died
  with the parent CLI process (caught by manual hands-on smoke; tests
  passed because they spawn + assert in the same Python process).
- **KTD-13 .gitignore** — `_ensure_coherence_dir()` now writes
  `.coherence/.gitignore` containing `*` on first spawn. The README
  claimed this auto-gitignored but the code never did. Idempotent:
  doesn't clobber operator customizations.

### Changed

- **`pyproject.toml`** — registered `launch_gate` and `launch_gate_pilot`
  pytest markers; default `pytest -q` runs skip them via `addopts`.

## [0.7.1] — 2026-05-13

### Added

- **`examples/refactor_demo/`** — planner-executor demo for write-side coherence. Two scripted sub-agents share a task-spec artifact through `CCSStore`; three variants (`--variant=with` / `no-invalidation` / `context-cache`) exercise the protocol against a real TypeScript fixture under `fixture_repo_ts/`. Real `tsc` runs locally turn the coherence question into an on-screen build error (TS2305 in the failure variants, clean build with coherence on). The `disable_invalidation` helper in `examples/refactor_demo/strategies.py` is the canonical pattern for suppressing peer invalidations on a live `CCSStore` — strategy hooks (`invalidates_peers_on_commit`) are consumed only by the simulation engine; the real adapter path publishes invalidations unconditionally. See the module docstring for the full rationale.
- **`tests/test_refactor_demo.py`** — 10 tests covering all three variants, MESI cache-state assertions, cache-hit/miss event-stream contracts, fresh-store-per-invocation isolation, and end-to-end real-`tsc` invocation (Node-toolchain-gated).
- **CI Node toolchain** in the `test` job: `actions/setup-node@v4` + `npm ci` inside `examples/refactor_demo/fixture_repo_ts/` so the real-`tsc` end-to-end tests actually run in CI instead of silently skipping. Previously, the `_has_tsc()` gate evaluated False on every CI run because the fixture's `node_modules/` is gitignored and no Node was installed.

### Changed

- **README and `docs/guide.md` vocabulary** — two prose edits replacing anti-list nouns: README "`freshness needs`" → "`how aggressively cached reads should refresh`" (strategy-selection sentence); `docs/guide.md` "`regardless of heartbeat freshness`" → "`regardless of how recently the holder heartbeated`" (`max_hold_ticks` parameter doc).
- **`pyproject.toml`** — pytest `pythonpath = ["src", "."]` (was `["src"]`) so tests under `tests/` can `from examples.refactor_demo import …`.

### Removed

- **`ccs-check-release` console script** is no longer exposed as a `pip install` entry point. It was a maintainer-only pre-tag-push verifier that queried this repo's GitHub admin settings (hardcoded `hipvlady/agent-coherence` defaults); end users had no use case. The underlying script (`tools/check_release_readiness.py`) and its module (`ccs.hardening.release_readiness`) remain tracked — CI invokes the script directly during the release workflow preflight, and maintainers run the same path locally.

### Fixed

- **CI preflight branch-protection check** — skips gracefully on 403 in CI when `GITHUB_TOKEN` lacks `administration: read` (a fine-grained PAT scope that isn't available to Actions tokens by design). The check still PASSes/FAILs definitively when run locally via `ccs-check-release` with a properly-scoped PAT. The earlier attempt to grant the permission through `permissions:` was reverted because the permission name does not exist for Actions tokens.
- **`tools/check_readme_numbers.py`** no longer requires a `## Real-workload benchmarks` section heading to locate the benchmark table. The hook now falls back to extracting the table directly via its column-header line (`| Workload | Agents | Reads:Writes | Hit rate | Savings |`) and scans downward until the first non-table line. Works whether the README places the table at the top with no heading (current shape) or under a dedicated section heading (legacy shape).

## [0.7.0] — 2026-05-11

### Added

- **`ccs-diagnose` CLI (v0-preview)** — passive, zero-network stale-read detector for existing LangGraph graphs. Attaches a callback to your compiled graph, classifies its write pattern (`single_writer` / `shared_artifact` / `parallel_branch` / `mixed`), and reports artifacts whose reads were handed divergent versions across nodes. HTML + machine-readable JSON output. Ships under the `langgraph-v0-preview` classifier with an explicit `v1` promotion gate. Install with `pip install "agent-coherence[diagnose]"`. Full reference: [docs/ccs-diagnose.md](docs/ccs-diagnose.md).
- **Supply-chain hardening:** PyPI Trusted Publishers OIDC, PEP 740 attestations, CycloneDX SBOM attached to every GitHub Release, `requirements-diagnose.txt` hash-pinned for reproducible installs, `ccs-check-release` preflight verifier (rulesets-API-based), documented end-user trust contract at [docs/security.md](docs/security.md).
- **Console scripts:** `ccs-check-architecture`, `ccs-check-release` (plus `ccs-diagnose` and the prior `ccs-simulate`, `ccs-compare`, `ccs-benchmark`).
- New optional extras: `[diagnose]`, `[crewai]`, `[autogen]`. The `[all]` extra now covers everything including OTel + LangSmith + benchmark + diagnose.

### Changed

- **README rewritten** with vendor-neutral, framework-agnostic lead. Same library across LangGraph, CrewAI, AutoGen, and any custom orchestrator; same behavior across model providers.
- **Documentation reorganized:** `REPRODUCE.md` → `docs/reproduce.md` (tracked); `SECURITY.md` split into public trust contract at `docs/security.md` (tracked) and maintainer-only pre-release verification gate at the repo root (local-only). `reproduce.sh` → `scripts/reproduce.sh` (tracked); maintainer-only `scripts/configure-release-protections.sh` is local-only.
- `tests/conftest.py` adds a `collect_ignore_glob` guard so pytest collection succeeds when the `[diagnose]` extra is not installed.
- Tag-protection check in `release_readiness.py` migrated from the deprecated `/tags/protection` endpoint to the rulesets API.

### Fixed

- Production cal.com URL for the `ccs-diagnose` report CTA: `https://cal.com/agent-coherence`.
- `DiagnoseCallback` concurrency: `_track_namespace_step` / `_resolve_end_attribution` wrapped in `self._lock` with `RLock` for re-entry, in preparation for `AsyncDiagnoseCallback`.
- Calibration JSONL atomicity on macOS: replaced POSIX `PIPE_BUF` claim with `fcntl.flock`; added write-all loop for partial writes.
- `DEFAULT_BOOK_A_CALL_URL` / `DEFAULT_CONTACT_EMAIL` resolve from `CCS_DIAGNOSE_BOOK_A_CALL_URL` / `CCS_DIAGNOSE_CONTACT_EMAIL` env vars before falling back to hardcoded defaults. URL/email scheme allowlist still applies.

## [0.6.0] — 2026-05-10

### Added

- **Crash recovery for stale grants.** When an agent crashes (OOM-kill, segfault) or livelocks holding a `MODIFIED` or `EXCLUSIVE` grant, the coordinator reclaims the grant on a heartbeat-based sweep so other agents can proceed. Two reclaim triggers — `reclaim_heartbeat` (holder went silent) and `reclaim_max_hold` (held too long regardless of liveness) — surface in the state log. Composition fail-fast: `lease` strategy + crash recovery requires `max_hold_ticks > lease_ttl_ticks` or raises at startup. Every framework adapter — `LangGraphAdapter`, `CrewAIAdapter`, `AutoGenAdapter`, and `CCSStore` — accepts `crash_recovery=CrashRecoveryConfig(...)` and exposes `heartbeat()` / `recover()`.
- Behind feature flag (`CrashRecoveryConfig(enabled=False)` default).

## [0.5.0] — 2026-04-26

### Added

- **Per-agent content audit log.** Opt-in `content_audit_log=callback` records every content delivery (cache hit, fetch, broadcast, write, search) with SHA-256 hashes, gap-free sequence numbers, and `instance_id` cross-validated against the state log. Pairs with v0.4's `state_log` to give debuggers a complete picture: state transitions × content delivered.

## [0.4.1] — earlier 0.4 patch

### Fixed

- Misc cleanups to the v0.4 event-stream surface (see git log on `v0.4.1` tag).

## [0.4.0] — 2026-04 (initial 0.4)

### Added

- **Sequence-numbered event stream.** `sequence_number`, `instance_id`, `schema_version` on every state-log entry. `ccs.validation.validate_log` helper for gap and schema-drift detection.

## [0.3.0] — 2026-03

### Added

- **State transitions log.** Opt-in JSONL stream of every stable MESI state transition.
- **Reproducible benchmark harness.** `make benchmark` with committed baseline (`benchmarks/expected.json`).

## [0.2.0] — 2026-02

### Added

- **Inline benchmark mode.** `benchmark=True` + `print_benchmark_summary()`.
- **Telemetry.** OpenTelemetry + LangSmith adapters.
- **Graceful degradation.** `on_error="degrade"` + `CoherenceDegradedWarning`.

## [0.1.0] — initial release

### Added

- MESI-style cache coherence for shared artifacts in multi-agent LLM systems.
- Five synchronization strategies: `lazy`, `eager`, `lease`, `access_count`, `broadcast`.
- `CCSStore` (LangGraph `BaseStore` drop-in), `LangGraphAdapter`, `CrewAIAdapter`, `AutoGenAdapter`, `CoherenceAdapterCore`.
- Deterministic tick-driven simulation engine with scenario YAML loader.
- TLA+ formal model for protocol safety properties.

[0.7.0]: https://github.com/hipvlady/agent-coherence/releases/tag/v0.7.0
[0.6.0]: https://github.com/hipvlady/agent-coherence/releases/tag/v0.6.0
[0.5.0]: https://github.com/hipvlady/agent-coherence/releases/tag/v0.5.0
[0.4.1]: https://github.com/hipvlady/agent-coherence/releases/tag/v0.4.1
[0.4.0]: https://github.com/hipvlady/agent-coherence/releases/tag/v0.4.0
