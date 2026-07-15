# Deploy-on-moved-base gate demo

> "The deploy ran on a config/base that had already moved."

An agentic-SDLC / CI symptom: a build agent reads the release base, plans a
deploy from it, and fires — but by the time it fires, a peer promoted a new base.
The deploy ships the artifact planned from the **old** base, and the promotion is
silently skipped. `gate()` re-reads the base at the effect boundary and **holds**
the deploy instead of firing on state that already moved.

```
# requires Python 3.11+ — activate the project venv first:
cd /path/to/agent-coherence && source .venv/bin/activate
python -m examples.gate_effect_ordering.main
```

Offline, no API keys, no real deploy — it spawns a local coordinator subprocess
and the "deploy" is an in-process ledger append. Exit code `0` iff the contract
holds on **both** paths:

- **negative control (no gate):** the agent fires the deploy on the stale base
  (`app@sha-A`) after a peer promoted `app@sha-B` — the failure the gate prevents, and
- **gated path:** that same deploy is **held** (`StaleView`), then fires on the
  fresh base (`app@sha-B`) after `reacquire()`, and never ships the stale artifact.

## What it shows

Two actors share one versioned input, `build/base.json` (`{image, replicas}`).
Actor A reads `base@v1`, plans a deploy from it, and gates the deploy on that
version. Actor B promotes `base -> v2` in between. The gate re-reads at the effect
boundary, sees the version moved, and **holds**: the deploy never fires on the
base that no longer exists. A reacquires the fresh base, re-plans, and fires.

```python
from ccs.adapters import CoherentVolume, gate

vol = CoherentVolume(root, managed=("build/**",))

def plan_deploy(base_bytes):
    base = json.loads(base_bytes)
    return f"{base['image']} x{base['replicas']}"

# fires the deploy only if build/base.json is unchanged since plan_deploy read it;
# else raises StaleView BEFORE the deploy runs.
gate(vol, "build/base.json", decide=plan_deploy, effect=run_deploy)
```

`gate()` is plain Python, so it drops into any agent loop identically — a
LangGraph deploy node, a CrewAI task, or a raw CI script:

```python
def deploy_node(state):
    gate(vol, "build/base.json", decide=lambda b: plan(state, b), effect=run_deploy)
    return state
```

## Honest boundary

- **Ordering, not rollback.** The gate fires *pre-effect* and never undoes a
  fired deploy. For an escaping effect (a deploy, a PR, a charge) there is a
  residual re-validate -> fire window this layer **narrows but cannot eliminate**.
- **Cooperative opt-in.** The agent calls the gate; it is not magic interception.
- **Single-host.** Both actors share one local coordinator with the same
  `managed` globs. Coordinating agents on separate machines is out of scope here.
- **Pure writes** use `CoherentVolume.write_cas_at(...)` directly — the atomic,
  no-window path.

## The same idea, one layer down

Serious infra already does this freshness check. Terraform refuses to apply a
saved plan built on a state that has moved: `Error: Saved plan is stale`. This
demo brings that check to an agent's deploy. Builder example, not an enterprise
CI product. Sibling demo `examples/effect_gate` shows the same primitive on a
bare `replicas=` config.

---
Comparing notes on multi-agent coherence?
https://github.com/Cohexa-ai/agent-coherence/discussions
