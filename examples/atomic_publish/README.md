# Atomic multi-file publish demo

Publish a coherent SET of files together, or not at all. If a peer moves one of
them before the publish lands, hold the whole publish instead of leaving a torn
pair on disk.

```
python -m examples.atomic_publish.main             # the atomic-publish demo
python -m examples.atomic_publish.main --baseline  # negative control first, then atomic_publish
```

Offline, no API keys — it spawns a local coordinator subprocess. Exit code `0`
iff the contract holds: with `atomic_publish` a moved member **holds** the whole
publish (no file written) then lands atomically after a re-read; with
`--baseline`, publishing file-by-file lands one file and the other's CAS is
rejected — a **torn pair** on disk (the failure the atomic publish prevents), so
the hold is measured against its absence rather than asserted.

## What it shows

An agent edits a plan split across two files — a plan and its manifest — that must
stay consistent. It publishes them as ONE unit against the versions it read. A
peer moves the manifest first. `atomic_publish` sees a member moved and **holds**:
neither file is written, so the plan on disk never references a manifest that
already changed. The agent re-reads the fresh versions and publishes the set
atomically.

```python
from ccs.adapters import CoherentVolume

vol = CoherentVolume(root, managed=("proj/**",))

# lands BOTH files iff each is still at the version the agent read; else holds the
# whole publish (StaleView / CasVersionConflict) with NO file written.
vol.atomic_publish([
    ("proj/plan.md",     plan_version,     new_plan_bytes),
    ("proj/manifest.md", manifest_version, new_manifest_bytes),
])
```

It drops into any agent loop the same way — `atomic_publish()` is plain Python, so
a LangGraph node, a CrewAI task, or a raw script all call it identically:

```python
def publish_node(state):
    vol.atomic_publish(state["writes"])  # [(path, expected_version, content), ...]
    return state
```

## Honest scope

- **All-or-nothing at the coordinator.** Either every member's version advances or
  none does; a torn *commit* is never a reachable state. On a win the client then
  writes each file with an atomic `os.replace`.
- **Cooperative.** The agent calls `atomic_publish`; it is not magic interception.
- **Single-host.** The agents share one local coordinator with the same `managed`
  globs. Coordinating agents on separate machines is a harder problem and out of
  scope here.
- **Sizing.** A single-file publish takes the direct CAS path. A multi-file
  publish opens a consistent snapshot session so the versions it checks are
  captured at one point (no member read across a peer commit); that adds a small
  capture→commit window, and a lost race there is **held**, never a torn publish.
- **Recovery is a re-read.** A held publish clears by re-reading the fresh
  versions and publishing from them — never from bytes computed before the hold.

## The same idea, one layer down

A database already does this. A multi-row transaction `COMMIT`s whole or rolls
back — you never see half of it. This demo brings that all-or-nothing apply to a
set of files an agent publishes together. It is a builder example, not an
enterprise product.
