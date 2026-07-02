# Effect-ordering gate demo

Fire an agent's effect only on the input it decided from. If the input moved in
between, hold the effect instead of firing it on stale state.

```
python -m examples.effect_gate.main             # the with-gate demo
python -m examples.effect_gate.main --baseline  # negative control first, then the gate
```

Offline, no API keys — it spawns a local coordinator subprocess. Exit code `0`
iff the contract holds: with the gate a stale deploy is **held** then fires on
fresh state after reacquire; with `--baseline`, the no-gate path fires on stale
input (the failure the gate prevents), so the hold is measured against its
absence rather than asserted.

## What it shows

An agent reads a shared config, decides a deploy, and gates the deploy on the
config version it just read. A peer changes the config before the deploy runs.
The gate re-reads at the effect boundary, sees the version moved, and **holds**:
the deploy never fires on the version that no longer exists. The agent reacquires
the fresh config, re-decides, and fires.

```python
from ccs.adapters import CoherentVolume, gate

vol = CoherentVolume(root, managed=("deploy/**",))

def decide(config_bytes):
    return plan_deploy(config_bytes)

# fires deploy(plan) only if deploy/config.txt is unchanged since decide() read it;
# else raises StaleView before the deploy runs.
gate(vol, "deploy/config.txt", decide=decide, effect=run_deploy)
```

It drops into any agent loop the same way — `gate()` is plain Python, so a
LangGraph node, a CrewAI task, or a raw script all call it identically:

```python
def deploy_node(state):
    gate(vol, "deploy/config.txt", decide=lambda c: plan(state, c), effect=run_deploy)
    return state
```

## Honest scope

- **Ordering, not rollback.** The gate fires pre-effect and never undoes an
  effect. For an escaping effect (a deploy, a PR, a charge) there is a residual
  window between the re-read and the fire that this layer cannot close — the gate
  *narrows* a stale fire, it does not *eliminate* it.
- **Cooperative.** The agent calls the gate; it is not magic interception.
- **Single-host.** The agent and the peer share one local coordinator with the
  same `managed` globs. Coordinating agents on separate machines is a harder
  problem and out of scope here.
- **Escaping effects.** A pure *write* effect uses `vol.write_cas_at(...)`
  directly — that is the atomic, no-window path.

## The same idea, one layer down

Serious infra already does this freshness check. Terraform refuses to apply a
saved plan built on a state that has moved and says so — `Error: Saved plan is
stale`. This demo brings that check to an agent's effect. It is a builder
example, not an enterprise CI product.
