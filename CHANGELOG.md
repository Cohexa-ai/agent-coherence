# Changelog

All notable changes to `agent-coherence` are documented here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions adhere to [SemVer](https://semver.org/spec/v2.0.0.html).

Alpha — APIs may change before `v1.0`.

## [Unreleased]

### Fixed

- **The in-memory registry now carries the same concurrency contract as the
  durable one: thread-safe, one lock, held across every check-then-act
  sequence.** The old contract — "single-threaded, lock-free by contract, GIL
  per-access atomicity" — only ever covered a single dict access, while the
  paths above it compose multi-access sequences the GIL does not hold
  together. Racing the stable-grant sweep against a holder renewing its claim
  put the walk into the state the sweep's own comment called impossible
  ("M/E holder has no granted_at slot") and silently skipped max-hold
  enforcement; racing `fetch` against a peer `write` left two agents in a
  write state (`single_writer_violated`, 38 of 40 rounds); racing two
  optimistic committers double-applied one `expected_version`. All three
  reproduced on the SQLite arm too wherever the sequence lived in the service
  layer — per-call backend serialization cannot fix a multi-call decision.

  The capture-only `_capture_lock` is widened into `self._lock`, held by every
  public method except three named construction-time-immutable accessors;
  `abort_guard` now holds it across the caller's whole mutation exactly like
  the SQLite guard; `commit_cas` / `commit_all` decide and apply in one hold
  (the in-memory stand-in for `BEGIN IMMEDIATE`); and the service's five
  check-then-act sequences — `fetch`, `delete`, `register_artifact`, both
  sweeps — run under the guard, the sweeps holding per `(agent, artifact)`
  pair with the pair's state re-read inside the hold, never across the walk.
  Callbacks the registry accepts (`state_log`, `on_reclaim`) now run under the
  lock and carry that stated obligation. A structural test drives every
  public member of both registries against a tracking lock, with the surface
  frozen by name, so a new unlocked member fails CI by name. Uncontended
  overhead: ~168 ns per acquire, ~2.7 µs per commit cycle.

- **The read-generation fence is now an operation-class property, not a
  commit-path one — `invalidate` could both bypass it and suppress it.** The
  fence (`owner_generation` vs a committer's captured `read_generation`) was
  checked on the three commit paths and nowhere else. The other grant-revoking
  operation, `CoordinatorService.invalidate`, neither consulted it nor
  maintained it, and both halves were reachable on a single host in one process
  — the registry `RLock` serializes each mutation but not the decision to revoke
  against the revoke itself.

  *A revoke minted at one epoch could destroy a grant issued at a later one.*
  Invalidation signals are minted inside the registry lock and delivered after
  it is released, so a peer could be reclaimed, re-acquire and re-read in
  between; the stale signal then revoked the fresh grant. Reachable through
  `CoherenceAdapterCore` (the CCSStore/LangGraph base) with the concurrent
  callers it already documents, where nothing threads the A6 abort Event and
  `abort_guard` is therefore a plain lock acquire. A peer-issued invalidation is
  now pinned to the target's `last_observed_version`: a target that has already
  observed a version at least as new as the one announced is not behind the
  signal, so the signal is dropped as obsolete. The pin is deliberately narrow,
  because wrongly dropping an invalidation is far worse than wrongly applying
  one — a self-issued release (post-edit failure, session-stop, operator drain)
  is never pinned, an already-INVALID target is never pinned (its stranded
  SIA/EIA transient still needs clearing), and an absent observation is admitted
  rather than dropped, matching the commit-path fence's admit-on-absent rule.

  *A voluntary release could disarm the fence entirely.* `invalidate` moved an
  M/E holder to INVALID under a trigger outside `RECLAIM_TRIGGERS`, so the
  epoch never moved — and the sweep could not arm it later either, there being
  no M/E grant left to reclaim. Identical end-state, opposite verdict: a
  sweep-reclaimed holder's later commit at an unchanged version was rejected
  `stale_read_generation`, while an `invalidate`-released one was silently
  admitted. Epoch movement now keys on `EPOCH_BUMP_TRIGGERS` — every reclaim
  trigger plus `invalidate` — the rule being "a write claim was revoked without
  the version moving", which is the one condition version-CAS is structurally
  blind to. The version-moving peer invalidations (`write` / `commit`) are
  unchanged and still do not bump.

  Both registries; `backend_contract.py` R9 restated to match. Expect more
  `stale_read_generation` rejections where a release previously left the fence
  unarmed — that is the fix, not a regression.

  Two callers had to follow the new "decline" outcome, because `invalidate`
  could previously never be a no-op. `AgentRuntime.handle_invalidation` now asks
  the coordinator first and clamps the local cache only on a signal it actually
  applied — clamping first would drop the agent's view for a grant the
  coordinator then keeps, a split the pre-pin code could not produce.
  `AgentRuntime.handle_update` now clears the transient for the agents it grants
  SHARED, the way `fetch` already does: once an eager push has moved a peer back
  to SHARED the pin can legitimately decline the late invalidation that used to
  clear it, and a stranded transient stalls both the stable-grant sweep and that
  peer's next `commit_cas`.

  The backend conformance kit gains a MUST-MATCH release-bump scenario, so the
  epoch rule R9 now states is one a third-party backend can actually fail.

  The review of this branch then found the pin's un-fenced sibling: the eager
  UPDATE broadcast travels the same publish-after-lock boundary, and
  `handle_update` applied it unconditionally — a stale update could regress a
  recipient's view to older bytes marked valid, drop a fresher M/E claim with
  no epoch bump, and stamp `last_observed_version` with the registry's current
  version while the cache held the older body, poisoning the very comparand
  the revoke pin trusts. `handle_update` now drops a broadcast whose bytes are
  behind the world or whose recipient holds a live write claim (an update
  completes an INVALID/SHARED revalidation, never revokes M/E); only the
  transient clears survive a drop. In the same round, the in-memory read
  accessors became absent-artifact tolerant (None/{}/[] like sqlite's empty
  SELECT) so a delete landing between a sweep's snapshot and its per-pair
  hold skips the vanished pair instead of crashing the walk.

  Both defects were in the formal model too: `Fencing.tla` inherited the
  unguarded `CRInvalidateAction`, which *is* F2 encoded. `Fencing.tla` now bumps
  the epoch on a release and carries `NoSilentRevoke`; the pin and
  `NoZombieRevoke` live in a new sibling amendment, `ZombieRevoke.tla`, kept out
  of `Fencing` because its per-agent observation variable would cost every
  downstream spec (EffectGate, Retention, Snapshot) state space for a property
  none of them is about. `NoStaleApply` structurally cannot see F2 — it is
  defined relative to the very counter that fails to move — which is why the
  second invariant exists; confirmed by mutation. Both new invariants carry
  documented mutants (`formal/tla/README.md` recipes 16 and 17). `make
  tla-check` now sweeps nine specs. `MaxGen` is sized `MaxTicks + NumAgents` (mirroring `MaxVersion`): the release bump is not tick-bounded, so the old `MaxTicks` ceiling was reachable at clock 0, and a bump guard that fails DISABLES its action rather than violating anything — TLC silently stopped exploring revoke transitions while still reporting success.

## [0.14.0] - 2026-08-25

### Added

- **`swg_gate` — the effect fence on the MCP tool surface.** The
  `stale-write-guard-fs` server gains a sixth tool, closing the same
  reclaim-zombie hole for agents that speak MCP instead of Python. `gate()`
  itself cannot be a tool — its `decide`/`effect` are Python callables, while an
  MCP agent's decision and effect happen between tool calls, outside the
  process. So the agent carries the `(version, owner_generation)` pair its
  `swg_read` now returns and calls `swg_gate(path, expected_version,
  expected_generation)` immediately before anything irreversible (a webhook, a
  deploy, an opened PR, a posted message). It answers `decision: "proceed"`, or
  DENIES with the surface's existing `reason: "stale_view"` vocabulary when the
  value moved, when the grant the agent read under was reclaimed (the version
  alone cannot see that), or when either comparand is unconfirmed. The deny
  also carries a typed `hold_cause` naming WHICH class fired, so an agent
  branches on a value rather than on prose. Omitting the generation comparand
  is refused as its own typed agent error rather than a retryable deny, so a
  cooperating agent cannot loop on advice that could never clear. Same honest boundary as the
  Python gate: the verdict is true as of that call, and the dispatch after it
  is still the agent's own step. `swg_read`'s added `owner_generation` field is
  additive; every other tool is byte-unchanged.

- **Compaction-aware re-grounding — the Claude Code adapter re-anchors a
  session after context compaction.** When Claude Code compacts a session
  (auto-compaction or a manual `/compact`), the model's summary can silently
  drop what the session held and what peers changed around the boundary. The
  coordinator now answers `POST /hooks/session-start` with a bounded
  re-grounding payload: the grants the session held at compaction
  (event-anchored — "At compaction you held EXCLUSIVE on `plan.md` (v7) —
  re-acquire before writing.") and, for each artifact the session touched,
  the current coordinated version with a stale flag when a peer advanced it
  past the session's last-observed version. That comparand is durable — a
  nullable `agent_states.last_observed_version` column (registry schema v6),
  recorded in the same transaction as the grant or commit that observed it,
  with an `agent_states(agent_id)` index so the session-scoped read stays
  indexed rather than scanning. The `agent-coherence-hook-client` gains a
  `session-start` subcommand for Claude Code's `SessionStart` hook; it gates
  on `source: "compact"` client-side, so ordinary starts, resumes, and
  clears never reach the coordinator. The payload renders at the next user
  message and on `--resume`; a live autonomous loop additionally receives it
  on its next tool admit via a compact-pending flag claimed exactly once at
  the allow seam — delivery is bounded and contained: it attaches only to
  admit-shaped allow responses, and strict-mode deny bodies stay
  byte-identical and never carry it. Payload prose is byte-identical between
  the Python and Node coordinator backends, pinned by six new
  protocol-corpus fixtures; capped at three artifact lines plus an overflow
  summary; a session with no coordination state emits nothing. Fail-open by
  design: a coordinator that is down at the compact boundary emits `{}` and
  that compaction's re-grounding is lost — coordination never blocks the
  session. One benign duplicate is possible (a mid-loop delivery followed by
  the next user turn's render); the payload's closing line — a more recent
  read supersedes the notice — makes a second sighting harmless.

- **Workspace versioning & restore — `WorkspaceVersioner` checkpoint +
  restore over heterogeneous members.** Checkpoint a workspace whose members
  live in different backends — files (via the coordinator's version
  retention), S3 objects (via `CoherentObject`), and declared forward-only
  action surfaces — then bring it back with **per-member honesty** about what
  can and cannot come back. A checkpoint is a named manifest capturing, per
  member, a restore pointer (S3 versionId / file content-state version), a
  fingerprint, and an honest **restore tier** (`restorable` /
  `restorable-unpinned` / `forward_only`), taken as a **skew-declared cut**:
  the capture window `[window_min, window_max]` is recorded rather than
  hidden, and a post-capture verification pass flags any member that moved
  inside it (`dirty_during_window`). ABSENT is captured as a fact (distinct
  from present-and-empty), so restore includes **delete legs**. Restore
  drives one conditional write per member under a **termination contract**:
  every member reaches exactly one terminal outcome (`restored` /
  `converged` / `conflict` / `target_lost` / `forward_only_skipped` /
  `held_unconfirmed`), contended legs re-drive under a bounded budget (a
  sustained foreign writer wins honestly — never a livelock, never a
  clobber), progress is durable and crash-resumable, and restore is a
  *forward* commit carrying old bytes (versions strictly increase). A
  member the engine cannot drive — an unreadable path, or one that became
  a symlink, a hardlink with an outside co-owner, or a non-regular file —
  is absorbed as that member's `target_lost` so the run still concludes
  and reports honestly, instead of aborting and leaving the checkpoint
  stuck mid-restore; a re-restore of a concluded checkpoint rebuilds its
  registration answer from durable state (`refused` stays terminal, never
  re-attempted).
  Checkpoint pins are fail-closed and loud: S3 members are pinned with a
  legal hold on the captured version in *your* bucket; a bucket without
  Object Lock durably downgrades the member to `restorable-unpinned` with
  `pin_state="pin_unavailable"`; file-member pins verify the captured
  version against the bounded retention window (they cannot extend it), and
  an expired window surfaces as `target_lost`, never silently. Run it:
  `python -m examples.workspace_versioning.main` (offline, deterministic;
  `--baseline` demonstrates the loss first). Docs: guide § Workspace
  versioning & restore; security § workspace-checkpoint retention and S3
  credential posture.

- **`agent-coherence-workspace` CLI — `checkpoint` / `list` / `status` /
  `restore`.** The operator surface over the workspace engine for file and
  forward-only members, with `--root` / `--json` on every verb and a
  four-way exit-code contract: `0` clean, `1` validation error, `2` typed
  refusal (a non-UTF-8 member, an unknown checkpoint id, a persist failure,
  or a member path failing containment), `3` restore **concluded with
  absorbed outcomes** — the per-member report on stdout is the truth.
  `status` renders every member's `(restore_tier, pin_state)` pair,
  including `(restorable, unpinned)` labeled claimed-but-not-yet-backed;
  `checkpoint` output always carries the file-retention caveat. Member
  paths are re-validated on **every** read and write (restore replays paths
  persisted by an earlier invocation): a workspace escape, any symlink
  component, a hardlinked regular file with a co-owner outside the root, a
  non-regular file (FIFO, socket, device), or a `.coherence/**` self-target
  is refused — at capture as exit `2` with nothing persisted, inside a
  restore leg as that member's `target_lost`. Under `--json` every error
  path also emits a one-line JSON error envelope on stdout, so machine
  consumers never parse stderr prose. A restore that binds file members
  warns loudly when a live coordinator is serving the workspace (warn,
  never refuse — restored writes land outside its grant flow). Duplicate
  checkpoint names are disclosed with the prior ids, never refused: names
  are labels, `status`/`restore` target an id. The CLI owns its durable
  state at `<root>/.coherence/workspace.db`. S3 members ride the Python API
  (bindings carry credentials; the CLI refuses cleanly and points there).

- **Packaged conformance corpus — `ccs.testing.substrate_conformance`,
  now with a workspace family.** The substrate conformance kit moved from
  the test tree into the installable package, so foreign implementations can
  consume it as a dev dependency. New `WorkspaceConformanceBinding`
  protocol: implement it, declare capabilities honestly
  (`declares_versioned` / `declares_pinnable` /
  `declares_restart_survival`), and the suite runs **MUST-MATCH** scenarios
  every implementation must reproduce (one-winner restore arbitration,
  torn-cut detection, bounded termination, restore-as-forward-commit,
  ABSENT-is-a-fact) plus **DECLARED** scenarios pinned to the binding's own
  declarations — passing is by observable outcome, never by mimicking
  internals. The corpus imports and runs **without pytest**; the new
  `conformance` extra (`pip install "agent-coherence[conformance]"`) adds it
  so skipped scenarios report as skips under your own test runner.

- **`formal/tla/WorkspaceVersion.tla` — restore registration
  model-checked, in the CI sweep.** The seventh spec in the `tla-check`
  matrix. Models the restore-registration split (all-or-nothing commit for
  written file members; manifest-side records for deletes; the empty
  write-set path) and checks `NoPartialRestoreRegistered`,
  `NoVersionRegression`, and `ExactlyOnceRegistration` — including the
  crash-resume windows — on every push.

- **`CoherentObject` (S3) — additive version-axis extensions.** The shipped
  ETag CAS surface is unchanged; alongside it the binding now captures the
  S3 versionId from the same read/write responses (`read_versioned` /
  `cas_write_versioned`), reads a pinned historical version
  (`read_pinned`), deletes (unconditional-latest — minting a delete marker
  on a versioned bucket), and sets/queries/releases **legal holds** per
  version (`set_legal_hold` / `legal_hold_status` / `release_legal_hold`)
  with typed errors for unversioned buckets and missing Object Lock. A
  deterministic local S3-semantics fake (`ccs.testing.s3_local`) models
  exactly the verified subset for offline tests and demos.

- **Registry schema v5 — durable workspace-checkpoint tables.** New
  `workspace_checkpoints` + member tables (manifest header, per-member
  pointer/fingerprint/tier/pin/outcome state, restore progress), written in
  one transaction per registration. The v4→v5 migration runs in a single
  transaction, and the v3→v4 step now stamps with a fixed literal instead
  of the moving current-version constant — a v3-origin database walks
  v3→4→5 landing every schema at its correct stamp (regression-tested,
  including kill-mid-migration recovery).

### Changed

- **`atomic_publish`'s foreign-edit boundary is now named and test-pinned.**
  The multi-file publish path is version-OCC against the coordinator and
  never re-reads disk between the caller's read and materialization, so an
  edit that bypasses the volume entirely (a human in an editor, a formatter)
  is invisible to it and gets overwritten — while plain `write()` denies
  that same edit via the foreign-edit guards and a single-member publish
  wedges on its hash-checked comparand read. No behavior changed; the
  boundary is now stated in the API docs, the guide's scope notes, and the
  README, with a regression test pinning the asymmetry. Use `atomic_publish`
  only for file sets whose every contending writer goes through a volume.

### Fixed

- **`gate()` now HOLDs when the grant its input was read under is gone — not
  only when the version moved.** The effect-ordering wrapper's re-validation
  compared versions alone, and the version answers only "is the value still
  the one `decide` saw" — it never asks "is the grant it was read under still
  standing". A coordinator sweep that reclaims a stalled holder's grant
  advances the artifact's ownership generation **without** a version move, so
  a reclaimed (zombie) holder's escaping effect — the webhook, the deploy,
  the opened PR — fired straight through the gate on revoked authority; in
  strict mode it even fired through the deny (the deny's summary still
  carried the unchanged version, and the denied-read marker was discarded).
  This is the same distinction the v0.9.1 read-generation fence already draws
  at the commit seam, now applied at the effect boundary. `gate()` captures
  the `(version, owner_generation)` pair at decision time — one pair-atomic
  registry snapshot (`get_artifact_and_generation`, on both registry arms), so
  a concurrent sweep can never tear it — re-reads the pair at the boundary,
  and HOLDs if **either** moved. Fail-closed on both comparands: an
  unconfirmed version (`0`, degraded) or an unconfirmed generation (`None` —
  a strict deny, a degraded read, or an older coordinator daemon from before
  this fix) HOLDs loudly instead of reverting to the generation-blind check.
  `StaleView` now carries `expected_generation` / `current_generation`
  alongside the version pair (all default `None`), and the reclaim HOLD is
  recognizable: versions equal, generations apart ("grant reclaimed …
  version unchanged"), and every HOLD class carries a typed `hold_cause` so a
  caller branches on a value rather than on prose. The causes are honest about
  their own limits: five are specific and recoverable, while
  `generation_unconfirmed` is the residual bucket — retry first, and let a HOLD
  that survives a successful `reacquire()` be what points at the daemon. The pre-read response carries the pair only on the new
  `want_owner_generation` request opt-in — every shipped response shape is
  byte-unchanged for exact-shape status clients. Model-checked:
  `formal/tla/EffectGate.tla` proves `NoStaleAdmit` (the gate never admits an
  effect whose captured pair had moved as of the re-validate read — scoped to
  the re-validate point; the residual re-validate→fire window stays
  disclaimed and model-visible). Surfaced by an external practitioner running
  a step-granular bounded read fence; the v0.11.0 entry's "orders effects on
  the inputs they were computed from" was honest only about the value axis,
  never the authority axis. Run it:
  `python -m examples.gate_effect_ordering.main` (the reclaimed-lease act:
  version unchanged, authority revoked, deploy held). Docs: guide § `gate()`;
  README § Effect-ordering gate.

## [0.13.0] - 2026-07-19

### Added

- **BYO-substrate bindings — `CoherenceSubstrate` contract, `CoherentRow`
  (Postgres), `CoherentObject` (S3).** Coherence over shared state that lives in
  a store *you already run* — a Postgres row, an S3 object — instead of a store
  this library ships. The coordinator holds only coherence metadata (a monotonic
  version, per-agent MESI state, a fixed-width `content_hash`, an opaque
  substrate token); it never holds your bytes. A native conditional write
  (`UPDATE … WHERE version = ?`, S3 `If-Match`) already rejects a single lost
  update *at the moment you write* — the bindings add the cross-agent layer on
  top: a peer's commit marks your cached read **stale**, so your next
  binding-mediated read or write is denied *before* you act on state that
  already moved, and every substrate speaks the same typed conflict and the same
  `reacquire()` recovery. Ships with: the **Coherence Manifest**
  (allowlist-of-forms secret handling, resolved-address SSRF deny, tier
  visibility), **capability tiers** with a never-ship-a-store floor,
  coordinator-mediated cross-agent wiring (pull invalidation, two-part commit,
  divergence detection), offline demos (`examples/coherent_row`,
  `examples/coherent_object` — run `--baseline` first to watch the unguarded
  clobber), and a tier-honesty conformance kit. Install via the new extras:
  `pip install "agent-coherence[coherent-row]"` / `"agent-coherence[coherent-object]"`.
  Docs: [`docs/usage/byo-substrate.md`](docs/usage/byo-substrate.md), README
  § BYO substrate, guide API reference. Single-host scope unchanged.

- **Subagent identity (Claude Code adapter) — subagents as first-class
  coherence peers.** A Claude Code subagent's hook payload carries the parent
  `session_id` plus a distinct `agent_id`; folding both into the identity
  derivation gives each subagent its own coherence identity — correct
  `last_writer` attribution and sibling-collision detection, instead of every
  subagent blending into the parent session. With no `agent_id` the derivation
  is byte-identical to before, so main-thread behavior is unchanged. A new
  `subagent-stop` hook-client subcommand maps Claude Code's `SubagentStop`
  event to a scoped release of just that subagent's grants (`agent_id`
  required — absent it skips rather than stripping the parent's grants
  mid-session). The identity fold is mirrored byte-for-byte by the Node
  backend; parity is pinned by new protocol-corpus fixtures
  (sibling collision, attribution, scoped release).

- **Three lead-use-case demos** — runnable, offline, exit-0-gated:
  `examples/rag_stale_memory` (an agent caches a memory record, a peer appends
  a fact, the agent's stale write-back erases it — `broken.py` loses the
  update; `fixed.py` denies the stale write, reacquires, re-applies, and both
  facts survive), `examples/ci_merge_gate` (three clean PRs, three green CI
  runs, one broken product: a merge validated against a base SHA a peer
  already moved; `gate()` holds the merge and re-fires on the fresh base), and
  `examples/gate_effect_ordering` (a deploy planned from a base that moved
  before it fired; `gate()` holds and re-plans).

- **MCP Registry publish workflow (`.github/workflows/publish-mcp.yml`).**
  Publishes `server.json` to the Official MCP Registry via GitHub OIDC —
  repository identity, which is what grants the org namespace; the interactive
  `mcp-publisher login github` device flow cannot see org membership and only
  grants a personal namespace. Runs on `release: published` (after the PyPI
  job, so the package is live before the registry validates it) with a
  `workflow_dispatch` manual path, and syncs the manifest version from the
  release tag.

- **CCSStore read-side demo — `examples/ccsstore_read_side/`.** A three-act,
  offline, deterministic demo that makes the read/write split legible: Act 1
  shows the shipped read-side guarantee (a peer's commit invalidates the
  cached view, so the next `get()` is a fresh miss serving the new version —
  never a stale hit); Act 2 shows the documented boundary (`put()` is not
  version-CAS — a stale write-back lands, silently erasing the peer's update);
  Act 3 routes the same intent through `store.core.write_cas`, re-applying it
  against the freshly read version so nothing is lost. Runs via
  `python -m examples.ccsstore_read_side.demo` (or `uv run demo.py`
  standalone); exit 0 iff every invariant held, so it doubles as a CI gate.
  Tests: `tests/test_ccsstore_read_side_demo.py`.

### Fixed

- **MCP Registry namespace casing — `server.json` and the PyPI ownership tag.**
  Both used `io.github.cohexa-ai/…`, but the registry derives the namespace from
  GitHub's canonical organization name and grants `io.github.Cohexa-ai/*`, so
  publishing was rejected with a 403. Both now read
  `io.github.Cohexa-ai/stale-write-guard-fs`. The `mcp-name:` tag is matched
  case-sensitively against the README of the *published* PyPI package, so the
  corrected tag only takes effect for releases from this one onward.

- **Guide examples table: two broken commands.** `examples.divergent_memory`
  and `examples.shared_knowledge_base` ship `demo.py`, not `main.py`; the
  documented `python -m examples.<name>.main` commands failed. Corrected to
  `.demo`.

## [0.12.0] - 2026-07-14

Atomic multi-artifact publish (commit a *set* of files all-or-nothing), the
gate-independent slice of the cross-host TLS + networked-backend groundwork, and
the MCP Registry manifest for the `stale-write-guard-fs` server. This is the
first release published from the **`Cohexa-ai`** GitHub organization; the default
single-host path is unchanged.

### Added

- **Atomic multi-artifact publish — `atomic_publish` / `commit_all`.** Commit a
  *set* of artifacts all-or-nothing: either every member's version advances as
  one unit or none does, so a torn commit (some files updated, others not) is
  never a reachable state — the N-artifact generalization of the optimistic
  `NoLostUpdate` guarantee. Single-host, one coordinator; all-or-nothing at the
  **coordinator commit** (disk materialization is best-effort staged-rename after
  the commit, and a rename failing partway raises a typed
  `PublishMaterializationError` naming exactly which files landed). Not rollback
  of already-escaped effects, and not cross-session write-skew prevention. The
  `NoPartialPublish` property is formally specified in
  `formal/tla/AtomicPublish.tla` (that spec is held out of the CI `tla-check`
  matrix; the six other specs still run on every push). Run it:
  `python -m examples.atomic_publish.main` (offline, deterministic).
- **TLS transport guards + backend atomic-boundary contract.** The buildable,
  gate-independent slice of the cross-host work — **no networked backend is
  built**; a routed production deployment stays experimental and demand-gated.
  Adds client-side TLS with fail-closed verification (an `https://` coordinator
  URL verifies the certificate; a plaintext bearer token to a non-loopback host
  is refused with a typed `InsecureTransportRefused` unless explicitly
  acknowledged), a coordinator-side fail-closed routed-bind guard, and a
  backend-agnostic atomic-boundary contract with a Tier-1 conformance kit that
  both the in-memory and SQLite registries pass.
- **MCP Registry manifest (`server.json`).** The manifest to publish the
  `stale-write-guard-fs` MCP server to the [Official MCP
  Registry](https://registry.modelcontextprotocol.io/) — PyPI package
  `agent-coherence`, stdio transport.

### Changed

- **Repository moved to the `Cohexa-ai` GitHub organization.** All in-repo
  references now point at `Cohexa-ai/agent-coherence`, and the MCP Registry
  namespace is `io.github.cohexa-ai/stale-write-guard-fs`. Old `hipvlady/…` URLs
  301-redirect. This is the first release published from the org's Trusted
  Publisher: wheels from v0.12.0 attest `Cohexa-ai/agent-coherence` (wheels
  ≤ v0.11.0 immutably attest `hipvlady/agent-coherence`; the verification steps
  in `docs/security.md` are now version-scoped).
- **Documentation accuracy + completeness pass.** Clarified that `CCSStore` is
  read-side coherence — `put` is not version-CAS and does not deny a stale
  write-back; write-side lost-update prevention is `CoherentVolume` / `write_cas`.
  Added `.github/SECURITY.md`, a worktree/workspace-boundary note, MCP
  multi-session semantics, and "when you don't need this" guidance; scoped the
  front-page headline to single-host and marked the event-bus networked
  transports as roadmap.

### Fixed

- Synced `uv.lock` with the declared optional-dependency extras (`dev`, `all`).

## [0.11.0] - 2026-07-02

A read-side consistency layer that prevents read-skew within a coordinated
session, a builder-facing `gate()` wrapper that orders effects on the inputs they
were computed from, and a fail-closed guard that refuses to send a bearer token
over plaintext HTTP to a non-loopback host. The default single-host loopback path
is unchanged; all cross-host behavior stays gated by `CCS_REMOTE_COORDINATOR`.

### Added

- **Read-side transaction snapshots (coordinator).** A session pins a consistent
  cut of the tracked artifacts and serves reads from that cut, so an agent that
  reads several artifacts never sees a torn mix of old and new versions (read-skew)
  even while other writers advance those artifacts in the background. Reads
  (`session_read`) are non-mutating and serve **only** from the pinned cut — an
  artifact that was not in the captured read-set is refused, never silently served
  from live state. Commits (`session_commit`) validate optimistically against the
  pinned base. Pins have a bounded lifetime backed by a heartbeat lease and a
  liveness sweep: a session whose heartbeat goes stale (or that is lost to a
  coordinator restart) fails **closed** with a typed "session invalidated" signal —
  never a fall-through to live state. Exposed over HTTP session endpoints with
  per-session identity, isolation, and caps, plus a content-free audit trail. This
  prevents read-skew; it does not add write-skew prevention (concurrent writers
  still resolve through version-CAS).
- **`gate()` — builder-facing effect-ordering wrapper.** `from ccs.adapters import
  gate`. Agents don't only overwrite files — they fire *effects* (a deploy, a PR, a
  shell command) computed from inputs they read earlier. `gate(vol, path,
  decide=..., effect=...)` captures the input's version at decision time, re-reads
  at the effect boundary, and fires the effect only if the input is unchanged —
  otherwise it holds the effect (raising `StaleView`) before it runs. Plain Python,
  so the same call drops into a LangGraph node, a CrewAI task, or a raw script
  unchanged. It *orders* effects, it does not roll them back (fires pre-effect,
  single-host, cooperative). Run it: `python -m examples.effect_gate.main` (offline,
  deterministic; add `--baseline` to see the stale fire it catches).
- **Fail-closed plaintext-bearer guard (cross-host mode).** The remote transport is
  plaintext HTTP — encryption is the operator's out-of-band responsibility (a
  WireGuard tunnel or a TLS-terminating proxy). The client now refuses to send its
  bearer token to a non-loopback host unless `CCS_REMOTE_INSECURE=1` acknowledges
  the link is secured out-of-band, raising a typed `InsecureTransportRefused`
  otherwise — turning a silent plaintext-bearer footgun into an explicit opt-in.
  Loopback is byte-unchanged and the default-off `CCS_REMOTE_COORDINATOR` gating is
  unchanged. It *reduces* the silent footgun; it does not guarantee encryption. Set
  the acknowledgement **narrowly** (per-invocation / per-compose-service), never in
  a persistent global shell profile.

### Changed

- **Registry contract extracted behind a Protocol.** The coordinator's
  durable-registry surface is now defined by a `RegistryBase` / `SqliteExtended`
  Protocol pair, so an alternative backend can conform structurally without
  subclassing. Internal refactor; no user-facing API change. The durable SQLite
  state store gains a schema migration for per-session owner isolation (existing
  databases migrate on open, preserving `0600` permissions).

### Fixed

- **`gate()` holds on an unconfirmed (degraded) version.** When the coordinator
  returns a degraded/unconfirmed version at the effect boundary, `gate()` now holds
  the effect (fail-closed) rather than firing on an unverified input; the `decide`
  input is validated up front.
- **`StaleView` exposes `expected_version` / `current_version` uniformly**
  (defaulting to `None`), so callers can inspect the version mismatch consistently
  across the read and gate paths.
- **Deterministic IPv6 handling in the transport guard.** IPv4-mapped IPv6 forms
  (`::ffff:127.0.0.1`) are classified as non-loopback deterministically (independent
  of the interpreter's patch version), and `CoordinatorEndpoint.base_url` brackets
  IPv6 literals (`http://[::1]:port`) so an IPv6 host yields a valid URL.

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
  fence claim. Formally modelled in `formal/tla/Retention.tla`
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

- **Watchdog late-completion phantom grant / late grant-revocation.** When
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
- **Watchdog-degraded reads no longer silently pass as verified-fresh.**
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
- **Coordinator spawn/idle lifecycle hardening.**
  - **`rm -rf .coherence/` during coordinator construction.** The
    spawn-or-join loop revalidated the `server.pid` inode before acquiring the
    flock, but an external `rm -rf .coherence/ && recreate` landing during
    `CoordinatorHTTPServer` construction (SQLite open + TCP bind, >300ms cold)
    left the winner about to write the port into an orphaned `server.pid` that
    no concurrent reader could see — losers read the recreated, port-less file
    and degraded. The winner now re-validates the inode after construction and
    immediately before writing the port; on mismatch it tears down the
    just-bound coordinator (freeing the socket) and recovers on a fresh inode.
  - **Idle/uptime now use a monotonic clock.** `idle_seconds` and
    `uptime_s` were computed from `time.time()` deltas, so an NTP step or a
    suspend/resume could misfire idle shutdown early or defer it. Both now use
    `time.monotonic()`; `time.time()` is reserved for operator-facing absolute
    timestamps.
  - **Thundering-herd loser degrade is now observable.** When the loop
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
  payload so additive fresh-path keys survive notice attachment; the
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
  default is now enabled, the composition rule (`max_hold_ticks` must exceed the
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
cache (the consistency probe found the OpenAI and Mistral Conversations *servers*
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
  the Conversations server — the consistency probe found the server consistent. See the
  [user guide](docs/guide.md#openai-agents-sdk-adapter-experimental).
- **New install extras:** `openai` (Conversations client + httpx), `openai-agents`
  (the adapter; pinned `>=0.17,<0.18`, composes `openai`), and `mistral`. `[all]`
  now includes `openai-agents` and `mistral`.
- **Conversations stale-read example** (`examples/conversations_stale_read/`): a
  deterministic, offline, no-keys reproducer of client-cache staleness over a
  consistent store, plus a live consistency probe (`probe.py`). The probe
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
- The composition rule is unaffected: explicit
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
(landed earlier on dev) and the LangGraph cycle replay tooling +
review-gated cluster (shipped to dev 2026-05-27 → 2026-05-28). Both
tracks are additive: new wire fields for v0.2 strict mode AND a new
CLI surface (`agent-coherence-replay`) + new module (`src/ccs/replay/`).
The `coordinator_uptime_s` deprecation alias from `0.8.0`
stays in place through `0.8.x`; its removal continues to be
targeted for a future minor bump per the original SemVer commitment.

### Added — LangGraph cycle replay tooling (2026-05-27 → 2026-05-28 on dev)

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

- **Per-artifact strict-mode opt-in** via `.coherence/strict_mode.yaml`.
  An artifact is strict iff its path matches both the
  `tracked_paths` set AND the new `strict_mode_paths` globs. Empty
  strict_mode_paths preserves v0.1.1 warn-mode for every artifact.
- **Handler decision-flip in all 4 PreToolUse handlers** (Read,
  Edit/Write, Bash, Grep) — `permissionDecision: "deny"` with the
  static reason template `STRICT_MODE_DENY_REASON_TEMPLATE`
  fires when (strict + tracked + invalidated). First-time observers
  (state None on existing artifact) fall through to warn-mode allow
  per the semantic refinement during implementation.
- **`TERMINAL_DENIAL_CLASSES` security invariant** — module-
  level `frozenset` enumerating denial classes that must never be
  converted to `permissionDecision: "allow"`. All 6 allow-emission
  call sites route through `emit_allow()` which asserts the invariant;
  AST-based meta-test grep-counts call sites in `coordinator_server.py`
  + `hook_payloads.py` so a future contributor adding a new allow
  path is forced to extend the parameter list.
- **`agent-coherence-migrate-deny` CLI** — stricter sibling
  to `agent-coherence-migrate-rules`. STDOUT-only (never writes to
  settings.json), symlink-contained (canonical-path containment check),
  never invokes an LLM, never reads files outside resolved workspace
  root. Under-emit bias: only canonical phrasings trigger.
- **Strict-mode telemetry** —
  `strict_mode_denials_total`, `strict_mode_routed_around_via_bash_total`
  (routing pattern detector with 30s window),
  `audit_log_mode_drift_total` counters surfaced via
  `/status?detail=metrics`. Minimal deny-only audit log appended as
  JSONL to `.coherence/audit.log` (mode 0o600, no schema_version, no
  command bodies, no user content).
- **Cross-implementation protocol corpus** —
  `tests/protocol_corpus/` harness + 12 warn-mode + 8 strict-mode
  fixtures + opt-in `protocol_corpus` pytest marker + new
  `protocol-corpus` CI job. Catches Python ↔ Node coordinator
  wire-shape drift before it ships. Strict-mode fixtures are
  python-only (Node coordinator doesn't ship strict mode in v0.2).

### Changed

- **Hook payload builders** (`build_stale_response`,
  `build_collision_response`) now route through `emit_allow()` per
  the `TERMINAL_DENIAL_CLASSES` structural invariant.
- **Static deny-reason text** for strict-mode replaces v0.1.1's
  per-invocation-varying warn-mode prose. Falsification testing
  inverted the original "varied text bounds retries" hypothesis on
  opus; static text byte-identical across retries is the right shape.

### Plugin compatibility

- v0.2 of the [agent-coherence-plugin](https://github.com/Cohexa-ai/agent-coherence-plugin)
  consumes this library via its broad-beta launch package.
  The Node coordinator does NOT ship strict mode in v0.2 —
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

**Stable release of the Claude Code plugin coordinator backend.** Promotes the `0.8.0a1` alpha pre-release to a final `0.8.0` after the v0.1.1 marketplace cohort + full code-review remediation pass landed. Both the coordinator HTTP surface and the wire contract are now considered stable through the `0.8.x` minor line; breaking changes will bump to `0.9.0` per SemVer.

### Added — Marketplace cohort

- **Watchdog hardening** — `busy_timeout=1500ms` per multi-statement transaction analysis; `/hooks/pre-bash` + `/hooks/pre-grep` with shlex-based path detection (closes the model-routing-around-Read finding); handler concurrency semaphore + queue-depth gate + three saturation counters.
- **Lifecycle hardening** — inode revalidation per retry (handles `rm -rf .coherence/ && recreate` mid-spawn races); in-flight handler drain on `shutdown()` (5s deadline before SQLite close); cold-start instrumentation surfaced via `coordinator.cold_start_duration_ms`.
- **Residual risk fixes** — `_agent_names` lock + public accessors; `ensure_secret` bounded `O_EXCL` retry (fail-closed); `/status` three-tier disclosure (`minimal` / `metrics` / `full` with `Coherence-Local-Operator: true` opt-in for the elevated tier); `_append_policy_yaml` `fcntl.flock` discipline; `MAX_REQUEST_BODY_BYTES = 64 KB` cap.
- **Telemetry** — per-endpoint counters (pre_read/pre_edit/post_edit/session_stop/pre_bash/pre_grep/policy_track/policy_untrack/status_total); product-signal counters (`intra_task_acquire_release_total`, `stale_warning_emitted_total`, `stale_warning_reread_total`); free-threading-safe via `threading.Lock`. New `coordinator_uptime_seconds` field (canonical) + `coordinator_uptime_s` deprecated alias for one release. `coordinator_backend` + `coordinator_version` fields for cross-backend dashboards.
- **`--self-test` smoke** — `agent-coherence-status --self-test` runs a four-step pre-read → pre-edit → post-edit → stale pre-read scenario against a live coordinator. Exit 0 on pass, 3 with actionable diagnostic on fail. Documented as the post-install validation step.
- **`--prepare-for-migration`** — `agent-coherence-coordinator --prepare-for-migration` enters a draining state that rejects new pre-edit (HTTP 503 with structured `migration in progress` error visible to the model), waits up to 5s for in-flight chains to complete, invalidates remaining M/E grants, then schedules shutdown. Eliminates the silent data-loss race when switching Python↔Node backends.
- **`agent-coherence-migrate-rules`** — scans CLAUDE.md for prose tool-class rules ("use rg, not grep", "never sudo") and proposes `permissions.deny` entries. Flag-only by default; `--apply` writes to `.claude/settings.local.json` after confirmation.

### Added — Stable-grant sweep preemption notice

`enforce_stable_grant_timeouts` now records a preemption notice for every reclaimed agent (using a sentinel `SWEEP_RECLAMATION_PREEMPTER_ID`). When the victim's post-edit eventually arrives, the enrichment path emits "your M/E grant was reclaimed by the coordinator sweep (heartbeat timeout or max-hold ceiling)" instead of a generic `CoherenceError` — eliminates a silent data-loss class for the alpha cohort's interactive workflows.

### Changed

- **`/status?detail=metrics`** is now the canonical surface for dashboard scrapers. The metrics-tier stability contract (additive in minor, removed only in major after one-release deprecation alias) is documented on `_handle_status`.
- **`StaleResponse` / `FreshResponse` / `PolicyUntrackResponse` TypedDicts** now match the actual wire shapes.
- **`_run_or_degrade` accepts `degraded_response`** so `{ok:bool}`-shape endpoints (pre-edit, post-edit, session-stop) return `{ok:True, degraded:True}` on watchdog timeout instead of the `{status:fresh}` envelope used by pre-read shapes.
- **`coordinator_uptime_s` field renamed to `coordinator_uptime_seconds`** per the `_seconds` convention. Old name kept as a deprecated alias through `0.8.x`; removal targeted for `0.9.0`.
- **Shutdown ordering** — `_drain_in_flight` now runs BEFORE `_server.server_close()`; `_seq` rollback now fires on COMMIT failure; shutdown wall-clock can exceed `IN_FLIGHT_DRAIN_TIMEOUT_SEC` when watchdog timeouts fire (documented bound).

### Fixed

- **`resolve_or_register` re-fetch race** — concurrent `remove_artifact` between ROLLBACK and re-fetch now raises an informative `RuntimeError` chained from the original `IntegrityError` instead of an opaque "UNIQUE constraint failed" trace.
- **`artifact_names_under_prefix` TOCTOU** — combined the LIKE-prefix and exact-match queries into a single UNION under one lock.
- **Abort wedge** — added `shutdown_abort_count` on `_SpawnedEntry`; after 3 consecutive `coordinator.shutdown()` raises, escalate by releasing the flock + marking shutdown_done so a fresh spawn can proceed.
- **Hook secret rotation race** — coordinator emits operator-visible WARNING on every 401 (with 60s dedupe) plus a new `auth_401_total` counter so silent auth failures become observable.
- **Plugin `hooks.json` Bash + Grep matchers** (cross-repo P0) — the plugin now actually invokes `/hooks/pre-bash` and `/hooks/pre-grep` rather than leaving the endpoints runtime-inert (companion plugin `v0.1.1`).

### Security

- **`/status` disclosure tiers** make pasting `?detail=metrics` into bug reports safe — no absolute paths, no PIDs, no session identifiers. The `full` tier still exposes those but only with the `Coherence-Local-Operator: true` opt-in header (defense-in-depth).
- **`MAX_REQUEST_BODY_BYTES` cap** — coordinator rejects oversized request bodies before `rfile.read` so a hostile or buggy client cannot OOM the coordinator with a single oversized POST.
- **`ensure_secret` bounded retry** — fail-closed if the empty-file recovery branch can't acquire `O_EXCL` within 5 attempts, instead of silently `O_TRUNC`-overwriting a concurrent racer's valid secret.
- **`MIGRATION_DRAIN_TIMEOUT_SEC = 5.0`** — backend-switch operator path now refuses new writes during drain instead of relying on the prior 100ms scheduled-shutdown race.

### Internal

- **78 code-review findings remediated** across 12 reviewer categories (adversarial, correctness, api-contract, reliability, kieran-python, maintainability, performance, project-standards, security, testing, agent-native, learnings). Large handler / file extractions explicitly deferred with rationale documented in PR bodies.
- **`/status` batched snapshot** — `SqliteArtifactRegistry.status_snapshot()` collapses the per-artifact `2N` SELECTs into 2 SQL queries held under one lock.
- **Risk-code test prefix audit** — `test_a4_*`, `test_a6_*`, `test_a7_*`, `test_a8_*`, `test_l1_*`, `test_l2_*` all present per the invariant naming policy.

## [0.8.0a1] — 2026-05-17

**Alpha pre-release.** This is the first release containing the Claude Code
plugin work. Packaged as a pre-release
(`a1`) so existing `pip install agent-coherence` users on the 0.7.x line are
not silently upgraded to a build whose entry points target a new use case.

To install: `pip install agent-coherence>=0.8.0a1` or
`pip install agent-coherence --pre`.

### Added — Claude Code plugin (v0.1 alpha)

The plugin lives at [Cohexa-ai/agent-coherence-plugin](https://github.com/Cohexa-ai/agent-coherence-plugin)
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
  containing `${COHERENCE_PORT}` at LOAD time,
  so HTTP-type hooks with templated URLs are not viable.

### Added — core library

- **`src/ccs/coordinator/sqlite_registry.py`** — `SqliteArtifactRegistry`,
  a drop-in replacement for `ArtifactRegistry` that persists state to
  SQLite-WAL across coordinator restarts. Preserves the 22-method public
  surface plus three plugin extensions: `resolve_or_register`
  (first-observation seeding), `artifacts_held_by_agent` (Stop
  release), `evict_stale_notices` (orphan-notice TTL eviction).
  Schema includes a `pending_notices` table for cross-session
  preemption surfacing.
- **`src/ccs/adapters/claude_code/`** — coordinator HTTP server,
  resolver, policy, auth, lifecycle, and hook payload contracts.
  ~2,800 lines net new, all gated by the existing architecture-layer
  rules (`tools/check_architecture.py` enforces).

### Added — tests

- `tests/test_claude_code_coordinator_server.py` — 63 tests including
  boundary validation, preemption-notice surfacing, and
  hardening regression tests.
- `tests/test_claude_code_lifecycle.py` — 15 tests including the
  load-bearing 10-process race test (multiprocessing.Pool) and the
  hardening regression tests from the adversarial review.
- `tests/test_claude_code_cli.py` — 21 tests covering all four CLI
  scripts including the detached-subprocess regression that the
  manual smoke surfaced (`8015f80`).
- `tests/test_claude_code_hook_client.py` — 12 tests for the
  command-type hook bridge.
- `tests/test_claude_code_contract.py` — 16 tests driven by real CC
  v2.1.131 stdin payloads recorded in `tests/fixtures/cc_hook_stdin/`.
  CI early-warning system for Claude Code version drift.
- `tests/test_claude_code_e2e.py` — 15 tests for bootstrap permissions,
  shared-secret auth (401/200/401), DNS-rebinding mitigation,
  state.db schema verification, coordinator-down graceful
  degradation, and subprocess-spawn integration.
- `tests/integration/test_warn_mode_behavior_change.py` + 40 scenarios —
  hard-launch-gate harness (`@pytest.mark.launch_gate`). 4 categories
  × 10 scenarios × 10 variants. Operator-runnable via
  `pytest -m launch_gate` (~$1.60, ~3 hours per N=40 run).

Total: 1101 passing, 2 skipped, 2 launch_gate deselected by default.

### Fixed — Claude Code plugin v0.1 hardening

- **Preemption notices** (`a76597a`) — Stop hook pops + surfaces
  pending notices (canonical case where X never fires another
  pre-event); orphan eviction with TTL; 10KB prose cap with
  newest-first coalescing; single-consumer pop on post-edit
  failure; UPSERT ordering uses wall-clock not commit-order.
- **Lifecycle hardening** (`e545a4a`) — self-probe budget,
  entry short-circuit, abort-on-shutdown-raise, reorder (drop
  port BEFORE coordinator.shutdown), per-coordinator shutdown mutex,
  Windows ImportError guard, retry budget bumped 30 → 60.
- **Detached coordinator** (`8015f80`) — `agent-coherence-coordinator`
  now forks a detached subprocess so the coordinator survives the
  launching shim's exit. Previously the daemon-thread coordinator died
  with the parent CLI process (caught by manual hands-on smoke; tests
  passed because they spawn + assert in the same Python process).
- **.gitignore** — `_ensure_coherence_dir()` now writes
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

- **`ccs-check-release` console script** is no longer exposed as a `pip install` entry point. It was a maintainer-only pre-tag-push verifier that queried this repo's GitHub admin settings (hardcoded `Cohexa-ai/agent-coherence` defaults); end users had no use case. The underlying script (`tools/check_release_readiness.py`) and its module (`ccs.hardening.release_readiness`) remain tracked — CI invokes the script directly during the release workflow preflight, and maintainers run the same path locally.

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

[0.7.0]: https://github.com/Cohexa-ai/agent-coherence/releases/tag/v0.7.0
[0.6.0]: https://github.com/Cohexa-ai/agent-coherence/releases/tag/v0.6.0
[0.5.0]: https://github.com/Cohexa-ai/agent-coherence/releases/tag/v0.5.0
[0.4.1]: https://github.com/Cohexa-ai/agent-coherence/releases/tag/v0.4.1
[0.4.0]: https://github.com/Cohexa-ai/agent-coherence/releases/tag/v0.4.0
