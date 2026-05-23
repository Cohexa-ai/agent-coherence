# Changelog

All notable changes to `agent-coherence` are documented here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions adhere to [SemVer](https://semver.org/spec/v2.0.0.html).

Alpha — APIs may change before `v1.0`.

## [Unreleased]

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

### Plan reference

Full architectural rationale in
[`docs/plans/2026-05-13-001-feat-claude-code-coherence-plugin-v0.1-plan.md`](docs/plans/2026-05-13-001-feat-claude-code-coherence-plugin-v0.1-plan.md)
including Phase 0 buildability probes, KTD decisions (1-13), per-unit
adversarial review findings, and operator deliverables for v0.1
launch.

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
