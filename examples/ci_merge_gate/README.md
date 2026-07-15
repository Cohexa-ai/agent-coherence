# CI merge-gate demo

> "Three agents, three clean PRs, three green CI runs, one broken product."

An agentic-SDLC / CI symptom: an agent validates its PR against base@SHA-A, a
peer's PR merges first and moves the base to SHA-B, and the first agent's merge
fires against a base that no longer exists. Its green CI run never saw the
peer's changes, so the integration break lands **silently** — every individual
check was green. `gate()` re-reads the base pointer at the effect boundary and
**holds** the merge instead of firing on a validation that already went stale.

```
# requires Python 3.11+ — activate the project venv first:
cd /path/to/agent-coherence && source .venv/bin/activate
python -m examples.ci_merge_gate.main             # the with-gate demo
python -m examples.ci_merge_gate.main --baseline  # negative control first, then the gate
```

Offline, no API keys, no network, no git — the repo state is a mock (one JSON
file holds the base-branch SHA plus a commit log) and the "merge" is an
in-process ledger append. It spawns a local coordinator subprocess. Exit code
`0` iff the contract holds:

- **negative control (no gate, `--baseline`):** agent A merges on a validation
  that ran against `SHA-A` after peer B already moved the base to `SHA-B` — the
  failure the gate prevents, and
- **gated path:** that same merge is **held** (`StaleView`, carrying the
  expected/current versions), then fires against the fresh base (`SHA-B`) after
  `reacquire()`, and never merges on the stale validation.

## What it shows

Two actors share one versioned input, `ci/base.json` (base SHA + commit log).
Agent A reads `base@SHA-A`, runs its mock CI validation against it, and gates
the merge on that version. Peer B merges its own PR in between (`base -> SHA-B`).
The gate re-reads at the effect boundary, sees the version moved, and **holds**:
the merge never fires against the base that no longer exists. A reacquires the
fresh base, re-validates against `SHA-B`, and merges cleanly.

```python
from ccs.adapters import CoherentVolume, gate

vol = CoherentVolume(root, managed=("ci/**",))

def validate_pr(base_bytes):
    base = json.loads(base_bytes)
    return run_ci_and_decide_merge(base["sha"])

# fires the merge only if ci/base.json is unchanged since validate_pr read it;
# else raises StaleView BEFORE the merge runs.
gate(vol, "ci/base.json", decide=validate_pr, effect=do_merge)
```

`gate()` is plain Python, so it drops into any agent loop identically — a
LangGraph merge node, a CrewAI task, or a raw CI script:

```python
def merge_node(state):
    gate(vol, "ci/base.json", decide=lambda b: validate(state, b), effect=do_merge)
    return state
```

## Honest boundary

- **Ordering, not rollback.** The gate fires *pre-effect* and never undoes a
  fired merge. For an escaping effect (a real merge API call) there is a
  residual re-check -> fire window this layer **narrows but cannot close**.
- **Cooperative opt-in.** The agent calls the gate; it is not magic interception.
- **Single-host.** Both actors share one local coordinator with the same
  `managed` globs. Coordinating agents on separate machines is out of scope here.
- **Pure writes** use `CoherentVolume.write_cas_at(...)` directly — the atomic,
  no-window path.

## The same idea, one layer down

This is the check-then-act (TOCTOU) race serious CI tooling already fights:
Renovate hit it branch-side ([renovate#18804](https://github.com/renovatebot/renovate/issues/18804)),
Terraform refuses to apply a plan built on moved state (`Error: Saved plan is
stale`), and Atlantis has the same stale-plan problem for PR-driven applies.
This demo brings that freshness check to an agent's merge decision. Builder
example, not an enterprise CI product. Sibling demos: `examples/effect_gate`
(the bare primitive) and `examples/gate_effect_ordering` (deploy-framed).

---
Comparing notes on multi-agent coherence?
https://github.com/Cohexa-ai/agent-coherence/discussions
