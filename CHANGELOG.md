# Changelog

All notable changes to `agent-coherence` are documented here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions adhere to [SemVer](https://semver.org/spec/v2.0.0.html).

Alpha — APIs may change before `v1.0`.

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
